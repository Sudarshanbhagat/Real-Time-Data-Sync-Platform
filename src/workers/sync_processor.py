"""Background processor: consumes `sync_events`, transforms, and finalises each sync.

For every Kafka message:

1. Load the sync event from the database (by `sync_id`).
2. Skip it if it is no longer `pending` (already processed: a redelivery).
3. Transform `source_data` to the canonical schema and validate it.
4. Store the result: `synced` + `canonical_data`, or `failed` + `error_message`.
5. Record the processing latency (message handled -> database committed).

Idempotency: step 4 is a conditional `UPDATE ... WHERE status = 'pending'`, so a
message delivered twice (consumer restart, rebalance, two workers) changes the
row at most once. Offsets are committed only after a whole batch is stored.

Throughput: a batch is processed concurrently across entities (bounded by
`concurrency`); events for one entity stay sequential, in publish order.

Failure handling:

- Invalid data           -> row marked `failed` with the reason; next message.
- Malformed Kafka value  -> logged and skipped by the consumer.
- Database error         -> retried; if it persists the batch is *not* committed,
                            the consumer is restarted with backoff, and the batch
                            is re-processed (no message is lost).
- Kafka unreachable      -> the supervisor keeps retrying with backoff; the API
                            is unaffected.

Runs inside the API process (started by the app lifespan) or standalone:
    python -m src.workers.sync_processor
"""

import asyncio
import contextlib
import logging
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC
from enum import StrEnum
from functools import lru_cache
from typing import Any, cast

from opentelemetry.context import Context
from opentelemetry.trace import SpanKind, Status, StatusCode
from sqlalchemy import update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import get_settings
from src.kafka.consumer import SyncEventConsumer
from src.kafka.schemas import SyncEventMessage
from src.models.base import utcnow
from src.models.sync_event import SyncEvent, SyncStatus
from src.observability.metrics import (
    PROCESSING_LATENCY,
    PROCESSOR_BATCH_SIZE,
    PROCESSOR_CONCURRENT,
    PROCESSOR_RUNNING,
    SYNC_EVENTS,
    SYNC_LATENCY,
    TRANSFORM_LATENCY,
    ErrorType,
    record_error,
)
from src.observability.tracing import get_tracer
from src.services.transformer import transform
from src.services.validator import TransformationError

logger = logging.getLogger(__name__)

# Keep enough samples for stable p99s without unbounded memory.
LATENCY_SAMPLES = 10_000


class ProcessingOutcome(StrEnum):
    """What handling one message did."""

    SYNCED = "synced"
    FAILED = "failed"
    DUPLICATE = "duplicate"  # row was already processed: redelivery, skipped
    MISSING = "missing"  # no row for this sync_id (e.g. database was reset)


class ProcessingError(Exception):
    """A message could not be stored; its batch must not be committed."""


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    """Nearest-rank percentile of an already sorted list (None if empty)."""
    if not sorted_values:
        return None
    rank = max(0, min(len(sorted_values) - 1, round(pct / 100 * len(sorted_values)) - 1))
    return round(sorted_values[rank], 3)


@dataclass
class ProcessorStats:
    """Counters and latency samples, shaped for a future Prometheus exporter."""

    synced: int = 0
    failed: int = 0
    duplicates: int = 0
    missing: int = 0
    db_errors: int = 0
    restarts: int = 0
    latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=LATENCY_SAMPLES))

    def record(self, outcome: ProcessingOutcome, latency_ms: float | None = None) -> None:
        """Count one handled message."""
        if outcome is ProcessingOutcome.SYNCED:
            self.synced += 1
        elif outcome is ProcessingOutcome.FAILED:
            self.failed += 1
        elif outcome is ProcessingOutcome.DUPLICATE:
            self.duplicates += 1
        else:
            self.missing += 1
        if latency_ms is not None:
            self.latencies_ms.append(latency_ms)

    @property
    def processed(self) -> int:
        """Messages that changed a row (synced or failed)."""
        return self.synced + self.failed

    def snapshot(self) -> dict[str, Any]:
        """JSON-friendly view: counts, error rate, latency percentiles."""
        ordered = sorted(self.latencies_ms)
        return {
            "processed": self.processed,
            "synced": self.synced,
            "failed": self.failed,
            "duplicates_skipped": self.duplicates,
            "missing": self.missing,
            "db_errors": self.db_errors,
            "restarts": self.restarts,
            "error_rate": round(self.failed / self.processed, 4) if self.processed else 0.0,
            "latency_ms": {
                "samples": len(ordered),
                "p50": _percentile(ordered, 50),
                "p95": _percentile(ordered, 95),
                "p99": _percentile(ordered, 99),
            },
        }


class SyncProcessor:
    """Supervised Kafka consumer that turns `pending` syncs into `synced`/`failed`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        *,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        group_id: str | None = None,
        db_retries: int = 3,
        restart_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        concurrency: int | None = None,
    ) -> None:
        """Configure the processor; nothing runs until `start()`.

        `session_factory` defaults to the application's database. Kafka settings
        default to the application settings (topic `sync_events`, group `sync_group`).
        `concurrency` caps how many entities of one batch are processed at once
        (default `SYNC_PROCESSOR_CONCURRENCY`); keep it within the DB pool size.
        """
        if session_factory is None:
            from src.database import SessionLocal

            session_factory = SessionLocal
        self.session_factory = session_factory
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.db_retries = db_retries
        self.restart_backoff_s = restart_backoff_s
        self.max_backoff_s = max_backoff_s
        self.concurrency = max(1, concurrency or get_settings().sync_processor_concurrency)
        self.stats = ProcessorStats()
        self._task: asyncio.Task[None] | None = None
        self._consumer: SyncEventConsumer | None = None
        self._stopping = asyncio.Event()

    @property
    def is_running(self) -> bool:
        """True while the background task is alive."""
        return self._task is not None and not self._task.done()

    # --- per-message processing ---------------------------------------------

    async def handle(
        self, message: SyncEventMessage, trace_context: Context | None = None
    ) -> ProcessingOutcome:
        """Process one message, retrying transient database errors.

        `trace_context` (from the message's Kafka headers) makes the
        `processor.consume` span a child of the API's `kafka.publish` span.
        Raises `ProcessingError` if the database stays unavailable, so the
        caller does not commit the offset and the message is redelivered.
        """
        with get_tracer().start_as_current_span(
            "processor.consume",
            context=trace_context,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "sync.id": message.sync_id,
                "sync.entity_id": message.entity_id,
                "sync.source_system": message.source_system,
            },
        ) as span:
            PROCESSOR_CONCURRENT.inc()
            try:
                outcome = await self._handle_with_retries(message)
            except ProcessingError as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise
            finally:
                PROCESSOR_CONCURRENT.dec()
            span.set_attribute("sync.outcome", outcome.value)
            return outcome

    async def _handle_with_retries(self, message: SyncEventMessage) -> ProcessingOutcome:
        """`handle()` body: retry `_process` on database errors."""
        started = time.perf_counter()
        for attempt in range(1, self.db_retries + 1):
            try:
                outcome = await self._process(message, started)
            except SQLAlchemyError as exc:
                self.stats.db_errors += 1
                record_error(ErrorType.DATABASE)
                logger.warning(
                    "sync_process_db_error sync_id=%s entity_id=%s attempt=%d/%d error=%r",
                    message.sync_id,
                    message.entity_id,
                    attempt,
                    self.db_retries,
                    exc,
                )
                if attempt == self.db_retries:
                    raise ProcessingError(
                        f"Database unavailable while processing sync {message.sync_id}"
                    ) from exc
                await asyncio.sleep(0.05 * 2**attempt)
                continue
            elapsed_s = time.perf_counter() - started
            processed = outcome in (ProcessingOutcome.SYNCED, ProcessingOutcome.FAILED)
            self.stats.record(outcome, round(elapsed_s * 1000, 3) if processed else None)
            PROCESSING_LATENCY.labels(outcome=outcome.value).observe(elapsed_s)
            return outcome
        raise AssertionError("unreachable")  # pragma: no cover

    def _transform(self, event: SyncEvent) -> dict[str, Any]:
        """Transform + validate one row, as `transform`/`validate` spans, return new values."""
        tracer = get_tracer()
        started = time.perf_counter()
        values: dict[str, Any]
        with tracer.start_as_current_span(
            "transform", attributes={"sync.source_system": event.source_system}
        ) as span:
            try:
                canonical = transform(event.source_data, event.source_system, event.entity_id)
            except TransformationError as exc:
                span.set_attribute("sync.validation_errors", len(exc.errors))
                span.set_status(Status(StatusCode.ERROR, "validation failed"))
                record_error(ErrorType.VALIDATION)
                values = {"status": SyncStatus.FAILED.value, "error_message": str(exc)}
            except Exception as exc:  # noqa: BLE001 - a transformer bug must not stall the topic
                logger.exception("sync_transform_crashed sync_id=%s", event.id)
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "transformer crashed"))
                record_error(ErrorType.TRANSFORM_CRASH)
                values = {
                    "status": SyncStatus.FAILED.value,
                    "error_message": f"internal transformation error: {exc!r}",
                }
            else:
                values = {
                    "status": SyncStatus.SYNCED.value,
                    "canonical_data": canonical,
                    "error_message": None,
                }
        TRANSFORM_LATENCY.labels(source_system=event.source_system).observe(
            time.perf_counter() - started
        )
        return values

    async def _process(self, message: SyncEventMessage, started: float) -> ProcessingOutcome:
        """One attempt: load, transform, conditionally update, commit."""
        tracer = get_tracer()
        async with self.session_factory() as session:
            with tracer.start_as_current_span("db.load"):
                event = await session.get(SyncEvent, message.sync_id)
            if event is None:
                logger.warning(
                    "sync_process_missing sync_id=%s entity_id=%s",
                    message.sync_id,
                    message.entity_id,
                )
                record_error(ErrorType.MISSING_ROW)
                return ProcessingOutcome.MISSING
            if event.status != SyncStatus.PENDING:
                logger.info(
                    "sync_process_skipped reason=already_%s sync_id=%s entity_id=%s",
                    event.status,
                    event.id,
                    event.entity_id,
                )
                return ProcessingOutcome.DUPLICATE

            values = self._transform(event)
            processed_at = utcnow()
            values["processed_at"] = processed_at
            values["processing_latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
            # Conditional on `pending`: a concurrent or repeated delivery that
            # passed the check above still cannot apply a second transition.
            with tracer.start_as_current_span("db.update"):
                result = cast(
                    CursorResult[Any],
                    await session.execute(
                        update(SyncEvent)
                        .where(
                            SyncEvent.id == event.id,
                            SyncEvent.status == SyncStatus.PENDING.value,
                        )
                        .values(**values)
                    ),
                )
                await session.commit()

        if result.rowcount == 0:
            logger.info("sync_process_skipped reason=concurrent_update sync_id=%s", event.id)
            return ProcessingOutcome.DUPLICATE

        outcome = ProcessingOutcome(values["status"])
        SYNC_EVENTS.labels(status=outcome.value, source_system=event.source_system).inc()
        created = (
            event.created_at if event.created_at.tzinfo else event.created_at.replace(tzinfo=UTC)
        )
        SYNC_LATENCY.labels(source_system=event.source_system).observe(
            max(0.0, (processed_at - created).total_seconds())
        )
        log = logger.info if outcome is ProcessingOutcome.SYNCED else logger.warning
        log(
            "sync_processed outcome=%s sync_id=%s entity_id=%s %s->%s latency_ms=%.3f%s",
            outcome,
            event.id,
            event.entity_id,
            event.source_system,
            event.destination_system,
            values["processing_latency_ms"],
            f" error={values['error_message']!r}" if values.get("error_message") else "",
        )
        return outcome

    # --- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        """Launch the supervised consume loop as a background task (idempotent)."""
        if self.is_running:
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self.run_forever(), name="sync-processor")

    def request_stop(self) -> None:
        """Ask the loop to exit after its current batch (signal-safe; see `stop()`)."""
        self._stopping.set()
        if self._consumer is not None:
            self._consumer.request_stop()

    async def stop(self, timeout_s: float = 10.0) -> None:
        """Finish the current batch, commit, leave the group; cancel if it takes too long."""
        self.request_stop()
        task, self._task = self._task, None
        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=timeout_s)
        except TimeoutError:
            logger.warning("sync_processor_stop_timeout timeout_s=%s; cancelled", timeout_s)
        logger.info("sync_processor_stopped stats=%s", self.stats.snapshot())

    async def run_forever(self) -> None:
        """Consume until `stop()`, restarting the consumer with backoff after any crash.

        A restart re-joins the group and resumes from the last committed offset,
        so the batch that was in flight is processed again (safely: see `_process`).
        """
        PROCESSOR_RUNNING.set(1)
        try:
            await self._supervise()
        finally:
            PROCESSOR_RUNNING.set(0)

    async def _supervise(self) -> None:
        """`run_forever()` body: the restart-with-backoff loop."""
        backoff = self.restart_backoff_s
        while not self._stopping.is_set():
            consumer = SyncEventConsumer(
                bootstrap_servers=self.bootstrap_servers, topic=self.topic, group_id=self.group_id
            )
            self._consumer = consumer
            try:
                await self._consume(consumer)
                backoff = self.restart_backoff_s
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - supervisor: log, back off, restart
                self.stats.restarts += 1
                record_error(ErrorType.PROCESSOR_CRASH)
                logger.error(
                    "sync_processor_crashed restart_in_s=%.1f restarts=%d error=%r",
                    backoff,
                    self.stats.restarts,
                    exc,
                )
            finally:
                self._consumer = None
                with contextlib.suppress(Exception):
                    await consumer.stop()
            if self._stopping.is_set():
                break
            # Sleep, but wake immediately if stop() is called.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=backoff)
            backoff = min(backoff * 2, self.max_backoff_s)

    async def _consume(self, consumer: SyncEventConsumer) -> None:
        """Poll -> handle the whole batch -> commit, until stopped."""
        await consumer.start()
        while not self._stopping.is_set():
            received = await consumer.poll_with_context()
            if received:
                PROCESSOR_BATCH_SIZE.set(len(received))
                try:
                    await self.handle_batch(
                        [item.message for item in received],
                        [item.trace_context for item in received],
                    )
                finally:
                    PROCESSOR_BATCH_SIZE.set(0)
            await consumer.commit()

    async def handle_batch(
        self,
        messages: list[SyncEventMessage],
        trace_contexts: list[Context] | None = None,
    ) -> None:
        """Handle one polled batch concurrently, preserving per-entity order.

        Messages are grouped by `entity_id` (the Kafka key): different entities
        run in parallel (up to `concurrency`), events of one entity run in the
        order they were published. If any message raises `ProcessingError` the
        rest of the batch is cancelled and the error propagates, so the batch is
        not committed and is redelivered (already-finished rows are then skipped).
        """
        contexts: list[Context | None] = (
            list(trace_contexts) if trace_contexts is not None else [None] * len(messages)
        )
        by_entity: dict[str, list[tuple[SyncEventMessage, Context | None]]] = {}
        for message, context in zip(messages, contexts, strict=True):
            by_entity.setdefault(message.entity_id, []).append((message, context))
        semaphore = asyncio.Semaphore(self.concurrency)

        async def run_entity(items: list[tuple[SyncEventMessage, Context | None]]) -> None:
            async with semaphore:
                for message, context in items:
                    if context is None:
                        await self.handle(message)
                    else:
                        await self.handle(message, context)

        async with asyncio.TaskGroup() as group:
            for entity_messages in by_entity.values():
                group.create_task(run_entity(entity_messages))


@lru_cache
def get_processor() -> SyncProcessor:
    """Process-wide processor started/stopped by the app lifespan."""
    return SyncProcessor()


async def _main() -> None:
    """Run the processor until Ctrl+C / SIGTERM."""
    processor = get_processor()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # add_signal_handler is unsupported on Windows; Ctrl+C then raises instead.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, processor.request_stop)
    processor.start()
    try:
        if processor._task is not None:
            await processor._task
    finally:
        await processor.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=get_settings().log_level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_main())
