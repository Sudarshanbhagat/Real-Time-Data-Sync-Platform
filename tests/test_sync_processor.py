"""Processor unit tests (SQLite, no Kafka): load, transform, update, skip, retry, stats."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.kafka.schemas import SyncEventMessage
from src.models import SyncEvent
from src.workers.sync_processor import (
    ProcessingError,
    ProcessingOutcome,
    ProcessorStats,
    SyncProcessor,
)


@pytest.fixture
def processor(session_factory: async_sessionmaker[AsyncSession]) -> SyncProcessor:
    """Processor bound to the test database (its Kafka loop is never started here)."""
    return SyncProcessor(session_factory)


@pytest.fixture
async def add_event(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[Any]:
    """Factory: insert a pending sync event and return its Kafka message."""

    async def _add(
        data: dict[str, Any], source_system: str = "salesforce", **columns: Any
    ) -> SyncEventMessage:
        async with session_factory() as session:
            event = SyncEvent(
                entity_id="acct_1",
                source_system=source_system,
                destination_system="stripe" if source_system != "stripe" else "hubspot",
                source_data=data,
                **columns,
            )
            session.add(event)
            await session.commit()
            return SyncEventMessage.from_sync_event(event)

    yield _add


async def reload(session_factory: async_sessionmaker[AsyncSession], sync_id: int) -> SyncEvent:
    """Fresh copy of a row (not the one cached in another session)."""
    async with session_factory() as session:
        event = await session.get(SyncEvent, sync_id)
        assert event is not None
        return event


async def test_valid_event_becomes_synced_with_canonical_data(
    processor: SyncProcessor, add_event: Any, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Pending -> synced; canonical_data, processed_at and latency are stored."""
    message = await add_event({"AccountName": "Acme", "AccountStatus": "Active"})

    outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.SYNCED
    event = await reload(session_factory, message.sync_id)
    assert event.status == "synced"
    assert event.canonical_data is not None
    assert event.canonical_data["name"] == "Acme"
    assert event.canonical_data["status"] == "Active"
    assert event.error_message is None
    assert event.processed_at is not None
    assert event.processing_latency_ms is not None and event.processing_latency_ms >= 0


async def test_invalid_event_is_stored_as_failed(
    processor: SyncProcessor, add_event: Any, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """Test C (unit): invalid data is kept as a failed sync with the reason, not dropped."""
    message = await add_event({"Email": "not-an-email"})

    outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.FAILED
    event = await reload(session_factory, message.sync_id)
    assert event.status == "failed"
    assert event.canonical_data is None
    assert event.error_message is not None
    assert "'name' is missing" in event.error_message
    assert "not a valid email" in event.error_message
    assert event.processed_at is not None


async def test_already_processed_event_is_skipped(
    processor: SyncProcessor, add_event: Any, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """A redelivered message does not re-transform or overwrite the row."""
    message = await add_event({"AccountName": "Acme"})
    assert await processor.handle(message) is ProcessingOutcome.SYNCED
    first = await reload(session_factory, message.sync_id)

    outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.DUPLICATE
    second = await reload(session_factory, message.sync_id)
    assert second.canonical_data == first.canonical_data
    assert second.processed_at == first.processed_at
    assert processor.stats.synced == 1 and processor.stats.duplicates == 1


async def test_failed_event_is_not_retried_by_redelivery(
    processor: SyncProcessor, add_event: Any
) -> None:
    """`failed` is terminal for the processor too: redelivery doesn't flip it."""
    message = await add_event({"Email": "x"})
    await processor.handle(message)

    assert await processor.handle(message) is ProcessingOutcome.DUPLICATE


async def test_conditional_update_prevents_double_transition(
    processor: SyncProcessor, add_event: Any, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """If another worker finishes between our read and our write, our update is a no-op.

    Emulated by letting a second worker process the row, then handing ours a stale
    copy that still says `pending`: only `WHERE status = 'pending'` stops it.
    """
    message = await add_event({"AccountName": "Acme"})
    assert await SyncProcessor(session_factory).handle(message) is ProcessingOutcome.SYNCED
    winner = await reload(session_factory, message.sync_id)

    stale = SyncEvent(
        id=message.sync_id,
        entity_id="acct_1",
        source_system="salesforce",
        destination_system="stripe",
        source_data={"AccountName": "Other value"},
        status="pending",
    )
    with patch.object(AsyncSession, "get", return_value=stale):
        outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.DUPLICATE
    event = await reload(session_factory, message.sync_id)
    assert event.status == "synced"
    assert event.canonical_data == winner.canonical_data  # not overwritten


async def test_unknown_sync_id_is_skipped(processor: SyncProcessor) -> None:
    """A message for a row that doesn't exist (e.g. DB reset) is logged and skipped."""
    message = SyncEventMessage.model_validate(
        {
            "sync_id": 999_999,
            "entity_id": "ghost",
            "source_system": "salesforce",
            "destination_system": "hubspot",
            "timestamp": "2026-10-12T00:00:00Z",
        }
    )

    assert await processor.handle(message) is ProcessingOutcome.MISSING
    assert processor.stats.missing == 1


async def test_transformer_crash_marks_failed_instead_of_stalling(
    processor: SyncProcessor, add_event: Any, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """An unexpected transformer exception fails that sync; the topic keeps moving."""
    message = await add_event({"AccountName": "Acme"})

    with patch("src.workers.sync_processor.transform", side_effect=KeyError("boom")):
        outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.FAILED
    event = await reload(session_factory, message.sync_id)
    assert event.error_message is not None
    assert "internal transformation error" in event.error_message


async def test_transient_db_error_is_retried(processor: SyncProcessor, add_event: Any) -> None:
    """A database blip is retried and the message still succeeds."""
    message = await add_event({"AccountName": "Acme"})
    real_process = processor._process
    calls = 0

    async def flaky(msg: SyncEventMessage, started: float) -> ProcessingOutcome:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("UPDATE", {}, Exception("connection reset"))
        return await real_process(msg, started)

    with patch.object(processor, "_process", side_effect=flaky):
        outcome = await processor.handle(message)

    assert outcome is ProcessingOutcome.SYNCED
    assert processor.stats.db_errors == 1


async def test_persistent_db_error_raises_so_offset_is_not_committed(
    processor: SyncProcessor, add_event: Any
) -> None:
    """If the DB stays down, handle() raises: the batch is redelivered, not lost."""
    message = await add_event({"AccountName": "Acme"})
    error = OperationalError("UPDATE", {}, Exception("db down"))

    with (
        patch.object(processor, "_process", side_effect=error),
        pytest.raises(ProcessingError),
    ):
        await processor.handle(message)

    assert processor.stats.db_errors == processor.db_retries


async def test_stats_snapshot_reports_error_rate_and_percentiles() -> None:
    """Metrics are ready for Phase 4: counts, error rate, p50/p95/p99."""
    stats = ProcessorStats()
    for latency in range(1, 101):
        stats.record(ProcessingOutcome.SYNCED, float(latency))
    for _ in range(25):
        stats.record(ProcessingOutcome.FAILED, 1.0)
    stats.record(ProcessingOutcome.DUPLICATE)

    snapshot = stats.snapshot()

    assert snapshot["processed"] == 125
    assert snapshot["duplicates_skipped"] == 1
    assert snapshot["error_rate"] == 0.2
    assert snapshot["latency_ms"]["samples"] == 125
    assert snapshot["latency_ms"]["p50"] is not None
    assert snapshot["latency_ms"]["p50"] <= snapshot["latency_ms"]["p95"]
    assert snapshot["latency_ms"]["p95"] <= snapshot["latency_ms"]["p99"] <= 100


def test_empty_stats_snapshot() -> None:
    """No traffic yet: zero error rate and null percentiles (not a crash)."""
    snapshot = ProcessorStats().snapshot()
    assert snapshot["error_rate"] == 0.0
    assert snapshot["latency_ms"]["p50"] is None


async def test_stop_without_start_is_safe(processor: SyncProcessor) -> None:
    """Shutdown is safe even if the processor never started (e.g. disabled)."""
    await processor.stop()
    assert processor.is_running is False


def message_for(entity_id: str, sync_id: int) -> SyncEventMessage:
    """A message that only needs to be routed, not stored."""
    return SyncEventMessage.model_validate(
        {
            "sync_id": sync_id,
            "entity_id": entity_id,
            "source_system": "salesforce",
            "destination_system": "hubspot",
            "timestamp": "2026-10-12T00:00:00Z",
        }
    )


async def test_batch_runs_entities_in_parallel_but_each_entity_in_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Different entities overlap; one entity's events never overlap or reorder."""
    processor = SyncProcessor(session_factory, concurrency=4)
    active: dict[str, int] = {}
    order: dict[str, list[int]] = {}
    peak = 0

    async def fake_handle(message: SyncEventMessage) -> ProcessingOutcome:
        nonlocal peak
        assert active.get(message.entity_id, 0) == 0, "same entity processed concurrently"
        active[message.entity_id] = 1
        peak = max(peak, sum(active.values()))
        await asyncio.sleep(0.01)
        order.setdefault(message.entity_id, []).append(message.sync_id)
        active[message.entity_id] = 0
        return ProcessingOutcome.SYNCED

    batch = [message_for(f"e{n % 4}", n) for n in range(12)]
    with patch.object(processor, "handle", side_effect=fake_handle):
        await processor.handle_batch(batch)

    assert peak > 1  # entities really ran concurrently
    assert order == {f"e{k}": [k, k + 4, k + 8] for k in range(4)}


async def test_batch_failure_propagates_so_batch_is_not_committed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A persistent DB error in one message fails the whole batch (redelivered later)."""
    processor = SyncProcessor(session_factory)

    async def fake_handle(message: SyncEventMessage) -> ProcessingOutcome:
        if message.entity_id == "bad":
            raise ProcessingError("db down")
        return ProcessingOutcome.SYNCED

    with (
        patch.object(processor, "handle", side_effect=fake_handle),
        pytest.raises(ExceptionGroup) as exc_info,
    ):
        await processor.handle_batch([message_for("ok", 1), message_for("bad", 2)])

    assert exc_info.group_contains(ProcessingError)


async def test_concurrency_defaults_from_settings(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """SYNC_PROCESSOR_CONCURRENCY sets the default; explicit values win; minimum 1."""
    assert SyncProcessor(session_factory).concurrency == 10
    assert SyncProcessor(session_factory, concurrency=3).concurrency == 3
    assert SyncProcessor(session_factory, concurrency=-5).concurrency == 1
