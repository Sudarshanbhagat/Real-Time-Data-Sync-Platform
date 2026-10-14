"""Async Kafka consumer for the `sync_events` topic.

The default handler only logs each event; the background processor
(`src/workers/sync_processor.py`) plugs in the handler that does the real work.

Offsets are committed automatically *after* each batch has been handled (not
on aiokafka's background timer), so a crash mid-batch redelivers the batch
instead of silently skipping it: at-least-once delivery. Handlers should
therefore be idempotent - `SyncEventMessage.event_id` is the dedup key.

Run standalone:  python -m src.kafka.consumer
"""

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from opentelemetry.context import Context
from pydantic import ValidationError

from src.config import get_settings
from src.kafka.schemas import SyncEventMessage
from src.observability.metrics import ErrorType, record_error
from src.observability.tracing import extract_context

logger = logging.getLogger(__name__)

MessageHandler = Callable[[SyncEventMessage], Awaitable[None]]


async def log_event(message: SyncEventMessage) -> None:
    """Default handler: log the event (mock processing until Phase 3)."""
    logger.info(
        "sync_event_consumed event_id=%s sync_id=%s entity_id=%s %s->%s",
        message.event_id,
        message.sync_id,
        message.entity_id,
        message.source_system,
        message.destination_system,
    )


def deserialize(raw: bytes | None) -> SyncEventMessage | None:
    """Decode a message value, or return None (and log) if it is malformed."""
    if raw is None:
        logger.warning("kafka_message_skipped reason=empty_value")
        record_error(ErrorType.MALFORMED_MESSAGE)
        return None
    try:
        return SyncEventMessage.model_validate_json(raw)
    except ValidationError as exc:
        logger.warning(
            "kafka_message_skipped reason=invalid_payload errors=%d preview=%r",
            exc.error_count(),
            raw[:200],
        )
        record_error(ErrorType.MALFORMED_MESSAGE)
        return None


@dataclass(frozen=True)
class ReceivedMessage:
    """A decoded message plus the producer's trace context (from its Kafka headers)."""

    message: SyncEventMessage
    trace_context: Context


class SyncEventConsumer:
    """Reads `SyncEventMessage`s as part of a consumer group."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
        auto_offset_reset: str = "earliest",
    ) -> None:
        """Configure the consumer; nothing connects until `start()`.

        `auto_offset_reset="earliest"` means a brand-new group starts from the
        beginning of the topic; an existing group always resumes from its
        committed offsets.
        """
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_sync_events_topic
        self.group_id = group_id or settings.kafka_consumer_group
        self.auto_offset_reset = auto_offset_reset
        self._consumer: Any = None  # AIOKafkaConsumer (aiokafka ships no type hints)
        self._stopping = asyncio.Event()
        self._has_uncommitted = False

    @property
    def is_running(self) -> bool:
        """True once `start()` has succeeded and until `stop()`."""
        return self._consumer is not None

    async def start(self) -> None:
        """Join the consumer group and subscribe to the topic."""
        if self._consumer is not None:
            return
        # Cleared before the await below, so a request_stop() that arrives while
        # we are still joining the group is not lost.
        self._stopping.clear()
        options = get_settings().kafka_client_options()
        options["bootstrap_servers"] = self.bootstrap_servers
        consumer = AIOKafkaConsumer(
            self.topic,
            **options,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=False,  # we commit after processing; see module docstring
        )
        try:
            await consumer.start()
        except KafkaError:
            await consumer.stop()
            logger.exception(
                "kafka_consumer_start_failed bootstrap_servers=%s", self.bootstrap_servers
            )
            raise
        self._consumer = consumer
        logger.info("kafka_consumer_started topic=%s group_id=%s", self.topic, self.group_id)

    def request_stop(self) -> None:
        """Ask a running `run()` loop to exit after its current batch (signal-safe)."""
        self._stopping.set()

    async def stop(self) -> None:
        """Leave the group and close the connection (idempotent).

        Call this from the task that owns the consumer, after `run()` returns;
        use `request_stop()` to end `run()` from elsewhere.
        """
        self._stopping.set()
        consumer, self._consumer = self._consumer, None
        if consumer is not None:
            await consumer.stop()
            logger.info("kafka_consumer_stopped group_id=%s", self.group_id)

    async def poll(self, timeout_ms: int = 1000, max_records: int = 100) -> list[SyncEventMessage]:
        """Fetch one batch and return its valid messages (call `commit()` once handled).

        Malformed messages are logged and skipped; their offsets are still
        committed with the batch, so one bad message cannot block a partition.
        """
        received = await self.poll_with_context(timeout_ms=timeout_ms, max_records=max_records)
        return [item.message for item in received]

    async def poll_with_context(
        self, timeout_ms: int = 1000, max_records: int = 100
    ) -> list[ReceivedMessage]:
        """Like `poll()`, but also returns each message's trace context from its headers."""
        if self._consumer is None:
            raise RuntimeError("Consumer is not started; call start() first")
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        if any(batches.values()):
            self._has_uncommitted = True
        return [
            ReceivedMessage(message, extract_context(record.headers))
            for records in batches.values()
            for record in records
            if (message := deserialize(record.value)) is not None
        ]

    async def commit(self) -> None:
        """Commit the offsets of everything fetched by `poll()` so far (no-op if none)."""
        if self._consumer is not None and self._has_uncommitted:
            await self._consumer.commit()
            self._has_uncommitted = False

    async def run(
        self, handler: MessageHandler = log_event, max_messages: int | None = None
    ) -> int:
        """Consume until `stop()` is called (or `max_messages` handled).

        Each batch is handled, then committed. If the handler raises, the batch
        is not committed and the error propagates, so it will be redelivered.
        Returns the number of messages handled.
        """
        await self.start()
        handled = 0
        while not self._stopping.is_set():
            messages = await self.poll()
            for message in messages:
                await handler(message)
                handled += 1
            await self.commit()
            if max_messages is not None and handled >= max_messages:
                break
        return handled


async def _main() -> None:
    """Run the consumer until Ctrl+C / SIGTERM."""
    consumer = SyncEventConsumer()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # add_signal_handler is unsupported on Windows; Ctrl+C then raises instead.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, consumer.request_stop)
    try:
        await consumer.run()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=get_settings().log_level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_main())
