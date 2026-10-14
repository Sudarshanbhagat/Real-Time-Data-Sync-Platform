"""Async Kafka producer that publishes sync events to the `sync_events` topic.

Messages are JSON (see `SyncEventMessage`) and keyed by `entity_id`, so every
event for one entity lands on the same partition and is consumed in order.
"""

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from opentelemetry.trace import SpanKind, Status, StatusCode

from src.config import get_settings
from src.kafka.schemas import SyncEventMessage
from src.observability.metrics import KAFKA_PUBLISH_LATENCY, ErrorType, record_error
from src.observability.tracing import get_tracer, inject_headers

logger = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """An event could not be delivered to Kafka (broker down, timeout, ...)."""


@dataclass(frozen=True)
class PublishResult:
    """Where a published message landed and how long delivery took."""

    topic: str
    partition: int
    offset: int
    latency_ms: float


class EventPublisher(Protocol):
    """Anything that can publish a sync event; lets tests swap in a fake."""

    async def publish_event(self, message: SyncEventMessage) -> PublishResult:
        """Deliver `message`, or raise `KafkaPublishError`."""
        ...


def serialize(message: SyncEventMessage) -> bytes:
    """Encode a message as UTF-8 JSON (ISO-8601 timestamps)."""
    return message.model_dump_json().encode("utf-8")


class SyncEventProducer:
    """Publishes `SyncEventMessage`s, waiting for broker acknowledgement.

    `start()` failing is not fatal: `publish_event()` reconnects lazily, so the
    API recovers on its own once Kafka comes back.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        client_id: str | None = None,
        timeout_ms: int | None = None,
    ) -> None:
        """Configure the producer; nothing connects until `start()`.

        Arguments default to the application settings.
        """
        settings = get_settings()
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_sync_events_topic
        self.client_id = client_id or settings.kafka_client_id
        self.timeout_s = (timeout_ms or settings.kafka_publish_timeout_ms) / 1000
        self._producer: Any = None  # AIOKafkaProducer (aiokafka ships no type hints)
        self._lock = asyncio.Lock()

    @property
    def is_connected(self) -> bool:
        """True once `start()` has succeeded and until `stop()`."""
        return self._producer is not None

    async def start(self) -> None:
        """Connect to the cluster. Raises `KafkaPublishError` if unreachable."""
        async with self._lock:
            if self._producer is not None:
                return
            options = get_settings().kafka_client_options()
            options["bootstrap_servers"] = self.bootstrap_servers
            producer = AIOKafkaProducer(
                **options,
                client_id=self.client_id,
                # Wait for all in-sync replicas, and let the broker drop duplicates
                # caused by the client's internal retries.
                acks="all",
                enable_idempotence=True,
                request_timeout_ms=int(self.timeout_s * 1000),
            )
            try:
                await asyncio.wait_for(producer.start(), timeout=self.timeout_s)
            except (KafkaError, TimeoutError, OSError) as exc:
                await producer.stop()
                logger.error(
                    "kafka_connect_failed bootstrap_servers=%s error=%r",
                    self.bootstrap_servers,
                    exc,
                )
                raise KafkaPublishError(
                    f"Cannot connect to Kafka at {self.bootstrap_servers}"
                ) from exc
            self._producer = producer
            logger.info(
                "kafka_producer_started bootstrap_servers=%s topic=%s",
                self.bootstrap_servers,
                self.topic,
            )

    async def stop(self) -> None:
        """Flush pending messages and close the connection (idempotent)."""
        async with self._lock:
            producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
            logger.info("kafka_producer_stopped")

    async def _discard(self) -> None:
        """Drop the client after a failed publish so the next one reconnects fresh.

        A send that timed out stays queued inside aiokafka; once the broker
        returns, the idempotent producer's sequence numbers no longer line up
        (`OutOfOrderSequenceNumber`) and that client never recovers. A new
        client gets a new producer ID and a clean state.
        """
        async with self._lock:
            producer, self._producer = self._producer, None
        if producer is not None:
            # Don't let a dead broker stall the request on flush.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(producer.stop(), timeout=self.timeout_s)
            logger.warning("kafka_producer_discarded reason=publish_failed")

    async def publish_event(self, message: SyncEventMessage) -> PublishResult:
        """Publish one event and wait until the broker has stored it.

        Raises `KafkaPublishError` (after logging it) on any delivery failure.
        """
        started = time.perf_counter()
        tracer = get_tracer()
        with tracer.start_as_current_span(
            "kafka.publish",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": self.topic,
                "sync.id": message.sync_id,
                "sync.entity_id": message.entity_id,
            },
        ) as span:
            try:
                result = await self._send(message, started)
            except KafkaPublishError as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                KAFKA_PUBLISH_LATENCY.labels(result="error").observe(time.perf_counter() - started)
                record_error(ErrorType.KAFKA_PUBLISH)
                raise
            span.set_attribute("messaging.kafka.destination.partition", result.partition)
            span.set_attribute("messaging.kafka.message.offset", result.offset)
        KAFKA_PUBLISH_LATENCY.labels(result="ok").observe(result.latency_ms / 1000)
        return result

    async def _send(self, message: SyncEventMessage, started: float) -> PublishResult:
        """Serialize and send `message` inside the current `kafka.publish` span."""
        tracer = get_tracer()
        # Injected here, in the `kafka.publish` span, so the processor's span
        # becomes a child of the publish (not of serialize/send).
        headers = inject_headers()
        try:
            if self._producer is None:
                await self.start()
            with tracer.start_as_current_span("kafka.serialize"):
                value = serialize(message)
            with tracer.start_as_current_span("kafka.send"):
                metadata = await asyncio.wait_for(
                    self._producer.send_and_wait(
                        self.topic,
                        value=value,
                        key=message.entity_id.encode("utf-8"),
                        headers=headers,
                    ),
                    timeout=self.timeout_s,
                )
        except (KafkaError, TimeoutError, KafkaPublishError) as exc:
            logger.error(
                "kafka_publish_failed topic=%s sync_id=%s entity_id=%s source=%s "
                "destination=%s error=%r",
                self.topic,
                message.sync_id,
                message.entity_id,
                message.source_system,
                message.destination_system,
                exc,
            )
            await self._discard()
            if isinstance(exc, KafkaPublishError):
                raise
            raise KafkaPublishError(f"Failed to publish to topic '{self.topic}'") from exc

        result = PublishResult(
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
            latency_ms=round((time.perf_counter() - started) * 1000, 3),
        )
        logger.info(
            "kafka_publish_ok topic=%s partition=%s offset=%s sync_id=%s entity_id=%s "
            "event_id=%s latency_ms=%.3f",
            result.topic,
            result.partition,
            result.offset,
            message.sync_id,
            message.entity_id,
            message.event_id,
            result.latency_ms,
        )
        return result


@lru_cache
def get_producer() -> SyncEventProducer:
    """Process-wide producer (FastAPI dependency; started/stopped in the app lifespan)."""
    return SyncEventProducer()
