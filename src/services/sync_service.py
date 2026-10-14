"""Business logic for recording syncs, kept separate from HTTP concerns."""

import hashlib
import json
import logging
import time
from dataclasses import dataclass

from opentelemetry import trace
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.kafka.producer import EventPublisher, KafkaPublishError
from src.kafka.schemas import SyncEventMessage
from src.models.base import utcnow
from src.models.sync_event import IdempotencyRecord, IdempotencyStatus, SyncEvent, SyncStatus
from src.observability.metrics import SYNC_EVENTS
from src.observability.tracing import get_tracer
from src.schemas.event import SyncRequest, SyncResponse

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """An idempotency key was reused with a different request body."""


class SyncInProgressError(Exception):
    """A request with the same idempotency key is still being processed."""


class EventPublishError(Exception):
    """The sync was saved but its Kafka event was not delivered.

    Retrying with the same idempotency key publishes it without creating a second sync.
    """

    def __init__(self, sync_id: int, entity_id: str) -> None:
        """Remember which sync is unpublished."""
        super().__init__(f"Sync {sync_id} for entity '{entity_id}' was saved but not published")
        self.sync_id = sync_id
        self.entity_id = entity_id


@dataclass(frozen=True)
class SyncOutcome:
    """Service result: the response payload plus whether it was a replay."""

    response: SyncResponse
    replayed: bool


def fingerprint(request: SyncRequest) -> str:
    """Stable SHA-256 of the request body, excluding the idempotency key itself."""
    payload = request.model_dump(mode="json", exclude={"idempotency_key"})
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode()).hexdigest()


async def _publish(
    publisher: EventPublisher, event: SyncEvent, idempotency_key: str | None
) -> float:
    """Publish `event` to Kafka, mark it published, and return the publish latency (ms).

    The caller commits `published_at`. Raises `EventPublishError` on failure.
    """
    message = SyncEventMessage.from_sync_event(event, idempotency_key)
    try:
        result = await publisher.publish_event(message)
    except KafkaPublishError as exc:
        # The producer has already logged the broker-side details.
        raise EventPublishError(event.id, event.entity_id) from exc
    event.published_at = utcnow()
    return result.latency_ms


async def _replay(
    session: AsyncSession, key: str, request_fp: str, publisher: EventPublisher
) -> SyncOutcome | None:
    """Return the cached outcome for `key`, or None if the key is unseen.

    If the original request saved the sync but failed to publish it, the event
    is published now, so a client retry closes the gap (still one message).
    Raises if the key exists but belongs to a different request or is unfinished.
    """
    record = await session.get(IdempotencyRecord, key)
    if record is None:
        return None
    if record.request_fingerprint != request_fp:
        raise IdempotencyConflictError(
            f"Idempotency key '{key}' was already used with a different request body"
        )
    if record.status != IdempotencyStatus.COMPLETED or record.result is None:
        raise SyncInProgressError(f"Request with idempotency key '{key}' is still processing")

    if record.sync_event_id is not None:
        event = await session.get(SyncEvent, record.sync_event_id)
        if event is not None and event.published_at is None:
            logger.info("Publishing previously unpublished sync_id=%s key=%s", event.id, key)
            await _publish(publisher, event, key)
            await session.commit()

    cached = SyncResponse.model_validate(record.result)
    return SyncOutcome(
        response=cached.model_copy(update={"idempotent_replay": True}), replayed=True
    )


async def create_sync(
    session: AsyncSession, request: SyncRequest, publisher: EventPublisher
) -> SyncOutcome:
    """Persist a `pending` sync event, then publish it to Kafka.

    The background processor (`src/workers/sync_processor.py`) consumes the
    event, transforms it and moves it to `synced` or `failed`.

    Idempotency: the event and its idempotency record are written in a single
    transaction. If two identical requests race, the second one violates the
    idempotency-key primary key, rolls back, and replays the first one's result -
    so exactly one sync event (and one Kafka message) is created per key.

    Ordering: the database commit happens *before* the publish, so Kafka never
    carries an event the database doesn't have. If publishing fails the row is
    kept with `published_at` NULL and `EventPublishError` is raised.
    """
    started = time.perf_counter()
    key = request.idempotency_key
    request_fp = fingerprint(request) if key else ""

    if key and (cached := await _replay(session, key, request_fp, publisher)):
        logger.info("Idempotent replay for key=%s entity_id=%s", key, request.entity_id)
        return cached

    event = SyncEvent(
        entity_id=request.entity_id,
        source_system=request.source_system.value,
        destination_system=request.destination_system.value,
        source_data=request.data,
        # Transformation happens asynchronously in the background processor.
        status=SyncStatus.PENDING.value,
    )
    tracer = get_tracer()
    persist_span = tracer.start_span("db.persist")
    session.add(event)
    try:
        with trace.use_span(persist_span, end_on_exit=False):
            await session.flush()  # assigns event.id without committing
    except BaseException:
        persist_span.end()
        raise

    # Provisional latency (validation, transformation, persistence); finalised after publish.
    event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
    response = SyncResponse(
        sync_id=event.id,
        entity_id=event.entity_id,
        status=SyncStatus.PENDING,
        latency_ms=event.latency_ms,
        message=(
            f"Accepted {request.entity_id} for sync from {event.source_system} to "
            f"{event.destination_system}; poll GET /api/v1/status/{request.entity_id}"
        ),
    )

    record: IdempotencyRecord | None = None
    if key:
        record = IdempotencyRecord(
            idempotency_key=key,
            request_fingerprint=request_fp,
            status=IdempotencyStatus.COMPLETED.value,
            result=response.model_dump(mode="json"),
            sync_event_id=event.id,
        )
        session.add(record)

    try:
        with trace.use_span(persist_span, end_on_exit=True):
            await session.commit()
    except IntegrityError:
        # Lost a race with a concurrent request using the same idempotency key.
        await session.rollback()
        if key and (cached := await _replay(session, key, request_fp, publisher)):
            logger.info("Concurrent duplicate resolved via replay for key=%s", key)
            return cached
        raise

    SYNC_EVENTS.labels(status="accepted", source_system=event.source_system).inc()
    publish_latency_ms = await _publish(publisher, event, key)

    # Total latency now includes the Kafka round-trip.
    event.latency_ms = round((time.perf_counter() - started) * 1000, 3)
    response = response.model_copy(
        update={"latency_ms": event.latency_ms, "publish_latency_ms": publish_latency_ms}
    )
    if record is not None:
        record.result = response.model_dump(mode="json")
    with tracer.start_as_current_span("db.mark_published"):
        await session.commit()

    logger.info(
        "Sync accepted sync_id=%s entity_id=%s %s->%s latency_ms=%.3f publish_latency_ms=%.3f",
        event.id,
        event.entity_id,
        event.source_system,
        event.destination_system,
        event.latency_ms,
        publish_latency_ms,
    )
    return SyncOutcome(response=response, replayed=False)


async def get_latest_sync(session: AsyncSession, entity_id: str) -> SyncEvent | None:
    """Return the most recent sync event for an entity, or None."""
    stmt = (
        select(SyncEvent)
        .where(SyncEvent.entity_id == entity_id)
        # id breaks ties when two events share a timestamp.
        .order_by(SyncEvent.created_at.desc(), SyncEvent.id.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()
