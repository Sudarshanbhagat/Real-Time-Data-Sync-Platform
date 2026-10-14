"""Full-pipeline tests: POST -> Kafka -> background processor -> database.

Each test runs a real `SyncProcessor` against the docker-compose broker on its
own throwaway topic and consumer group (skipped if Kafka is unreachable). The
database is a per-test SQLite *file*, because the API and the processor use
separate connections concurrently.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.database import get_db
from src.kafka.consumer import SyncEventConsumer
from src.kafka.producer import EventPublisher, SyncEventProducer, get_producer
from src.kafka.schemas import SyncEventMessage
from src.main import app
from src.models import Base, SyncEvent
from src.workers.sync_processor import ProcessingOutcome, SyncProcessor, get_processor
from tests.conftest import KAFKA_BOOTSTRAP

WAIT_S = 30.0


@pytest.fixture
async def file_session_factory(tmp_path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """A fresh SQLite database file shared by the API and the processor."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'e2e.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    await engine.dispose()


@pytest.fixture
def group_id() -> str:
    """Unique consumer group per test, so committed offsets never leak between tests."""
    return f"test_e2e_{uuid.uuid4().hex}"


@pytest.fixture
def make_processor(
    file_session_factory: async_sessionmaker[AsyncSession], kafka_topic: str, group_id: str
) -> Callable[[], SyncProcessor]:
    """Factory for processors on this test's topic/group (fast restart for tests)."""

    def _make() -> SyncProcessor:
        return SyncProcessor(
            file_session_factory,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic=kafka_topic,
            group_id=group_id,
            restart_backoff_s=0.2,
        )

    return _make


@pytest.fixture
async def processor(make_processor: Callable[[], SyncProcessor]) -> AsyncIterator[SyncProcessor]:
    """A running background processor; stopped (offsets committed) after the test."""
    proc = make_processor()
    proc.start()
    yield proc
    await proc.stop()


@pytest.fixture
async def api(
    file_session_factory: async_sessionmaker[AsyncSession],
    kafka_producer: SyncEventProducer,
    client: AsyncClient,
) -> AsyncClient:
    """HTTP client whose syncs go to the file DB and the per-test Kafka topic."""

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with file_session_factory() as session:
            yield session

    publisher: EventPublisher = kafka_producer
    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_producer] = lambda: publisher
    return client


async def wait_for(condition: Callable[[], Awaitable[bool]], timeout_s: float = WAIT_S) -> None:
    """Poll `condition` until it is true or fail after `timeout_s`."""

    async def _loop() -> None:
        while not await condition():  # noqa: ASYNC110 - polling the DB, no event to await
            await asyncio.sleep(0.1)

    await asyncio.wait_for(_loop(), timeout=timeout_s)


async def rows(factory: async_sessionmaker[AsyncSession]) -> list[SyncEvent]:
    """All sync events, oldest first."""
    async with factory() as session:
        return list((await session.scalars(select(SyncEvent).order_by(SyncEvent.id))).all())


async def wait_until_processed(
    factory: async_sessionmaker[AsyncSession], expected: int
) -> list[SyncEvent]:
    """Wait until `expected` rows exist and none is still pending."""

    async def done() -> bool:
        current = await rows(factory)
        return len(current) == expected and all(r.status != "pending" for r in current)

    await wait_for(done)
    return await rows(factory)


def body(entity_id: str, data: dict[str, Any], **extra: Any) -> dict[str, Any]:
    """A Salesforce -> HubSpot sync request."""
    return {
        "entity_id": entity_id,
        "source_system": "salesforce",
        "destination_system": "hubspot",
        "data": data,
        **extra,
    }


async def test_a_full_flow_post_to_canonical_data(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Test A: POST -> Kafka -> processor -> DB synced, visible via GET /status."""
    response = await api.post(
        "/api/v1/sync",
        json=body("acct_e2e", {"AccountName": "Acme", "AccountStatus": "Active", "Tier": "1"}),
    )
    assert response.status_code == 200
    assert response.json()["status"] == "pending"

    [event] = await wait_until_processed(file_session_factory, 1)

    assert event.status == "synced"
    assert event.canonical_data is not None
    assert event.canonical_data["name"] == "Acme"
    assert event.canonical_data["status"] == "Active"
    assert event.canonical_data["metadata"] == {"tier": "1"}
    assert event.processing_latency_ms is not None

    status = (await api.get("/api/v1/status/acct_e2e")).json()
    assert status["status"] == "synced"
    assert status["canonical_data"]["name"] == "Acme"
    assert status["processed_at"] is not None


async def test_b_duplicate_keys_give_one_canonical_record(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Test B: the same idempotency key 3x -> one row, one canonical record."""
    payload = body("acct_dup", {"AccountName": "Acme"}, idempotency_key="dup-1")
    for _ in range(3):
        assert (await api.post("/api/v1/sync", json=payload)).status_code == 200

    [event] = await wait_until_processed(file_session_factory, 1)

    assert event.status == "synced"
    assert processor.stats.synced == 1


async def test_c_invalid_data_is_stored_as_failed(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Test C: data the transformer rejects is kept as a failed sync with a reason."""
    response = await api.post(
        "/api/v1/sync", json=body("acct_bad", {"Industry": "Unknown", "Email": "nope"})
    )
    assert response.status_code == 200  # accepted; validation happens asynchronously

    [event] = await wait_until_processed(file_session_factory, 1)

    assert event.status == "failed"
    assert event.canonical_data is None
    assert event.error_message is not None and "'name' is missing" in event.error_message
    status = (await api.get("/api/v1/status/acct_bad")).json()
    assert status["status"] == "failed"
    assert "not a valid email" in status["error_message"]


async def test_d_transformation_error_does_not_block_next_event(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Test D: a failing event is logged and the following events still process."""
    cases: list[tuple[str, dict[str, Any]]] = [
        ("e1", {"AccountName": "First"}),
        ("e2", {"AccountName": 12345}),  # wrong type -> failed
        ("e3", {"AccountName": "Third"}),
    ]
    for entity_id, data in cases:
        assert (await api.post("/api/v1/sync", json=body(entity_id, data))).status_code == 200

    events = await wait_until_processed(file_session_factory, 3)

    assert [e.status for e in events] == ["synced", "failed", "synced"]
    assert processor.stats.snapshot()["error_rate"] == pytest.approx(1 / 3, abs=1e-3)


async def test_e_crash_before_commit_reprocesses_idempotently(
    api: AsyncClient,
    make_processor: Callable[[], SyncProcessor],
    file_session_factory: async_sessionmaker[AsyncSession],
    kafka_topic: str,
    group_id: str,
) -> None:
    """Test E: a worker dies mid-batch without committing; the restart redelivers
    the whole batch, finishes the rest and leaves already-done rows untouched."""
    for n in range(4):
        await api.post("/api/v1/sync", json=body(f"crash_{n}", {"AccountName": f"N{n}"}))

    # First worker: fetch the batch, process half, then "crash" (no commit).
    crashed = make_processor()
    consumer = SyncEventConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP, topic=kafka_topic, group_id=group_id
    )
    await consumer.start()
    batch: list[SyncEventMessage] = []
    try:

        async def fetch() -> None:
            while len(batch) < 4:
                batch.extend(await consumer.poll(timeout_ms=500))

        await asyncio.wait_for(fetch(), timeout=WAIT_S)
        for message in batch[:2]:
            assert await crashed.handle(message) is ProcessingOutcome.SYNCED
    finally:
        await consumer.stop()  # leaves the group with nothing committed
    before = {e.id: e.processed_at for e in await rows(file_session_factory) if e.processed_at}
    assert len(before) == 2

    # Restarted worker: same group, so Kafka redelivers all 4 messages.
    restarted = make_processor()
    restarted.start()
    try:
        events = await wait_until_processed(file_session_factory, 4)

        async def all_redelivered() -> bool:
            return restarted.stats.processed + restarted.stats.duplicates >= 4

        await wait_for(all_redelivered)
    finally:
        await restarted.stop()

    assert all(e.status == "synced" for e in events)
    assert restarted.stats.synced == 2
    assert restarted.stats.duplicates == 2
    # Rows processed before the crash were not processed a second time.
    assert {e.id: e.processed_at for e in events if e.id in before} == before


async def test_supervisor_restarts_consumer_after_db_outage(
    api: AsyncClient,
    make_processor: Callable[[], SyncProcessor],
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A database outage crashes the consume loop; the supervisor restarts it and the
    uncommitted message is redelivered and processed - nothing is lost."""
    proc = make_processor()
    real_process = proc._process
    failures = proc.db_retries  # exactly enough to exhaust one handle() call

    async def flaky(message: SyncEventMessage, started: float) -> ProcessingOutcome:
        nonlocal failures
        if failures > 0:
            failures -= 1
            raise OperationalError("UPDATE", {}, Exception("database is down"))
        return await real_process(message, started)

    with patch.object(proc, "_process", side_effect=flaky):
        proc.start()
        try:
            await api.post("/api/v1/sync", json=body("outage_1", {"AccountName": "Acme"}))
            [event] = await wait_until_processed(file_session_factory, 1)
        finally:
            await proc.stop()

    assert event.status == "synced"
    assert proc.stats.restarts == 1
    assert proc.stats.db_errors == proc.db_retries


async def test_processor_skips_malformed_kafka_messages(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
    kafka_producer: SyncEventProducer,
    kafka_topic: str,
) -> None:
    """A poison message on the topic is skipped; real events around it still process."""
    await api.post("/api/v1/sync", json=body("before", {"AccountName": "A"}))
    raw = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await raw.start()
    try:
        await raw.send_and_wait(kafka_topic, b"{not json")
    finally:
        await raw.stop()
    await api.post("/api/v1/sync", json=body("after", {"AccountName": "B"}))

    events = await wait_until_processed(file_session_factory, 2)

    assert [e.status for e in events] == ["synced", "synced"]
    async with file_session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(SyncEvent)) == 2


async def test_processor_metrics_endpoint(
    api: AsyncClient,
    processor: SyncProcessor,
    file_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """GET /health/processor exposes running state, counts and latency percentiles."""
    app.dependency_overrides[get_processor] = lambda: processor
    await api.post("/api/v1/sync", json=body("m1", {"AccountName": "A"}))
    await wait_until_processed(file_session_factory, 1)

    metrics = (await api.get("/health/processor")).json()

    assert metrics["running"] is True
    assert metrics["synced"] == 1
    assert metrics["error_rate"] == 0.0
    assert metrics["latency_ms"]["p50"] is not None
