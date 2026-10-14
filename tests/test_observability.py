"""Metrics and tracing tests: counters/histograms move, /metrics serves them, spans nest.

Prometheus metrics are process-global, so each test asserts on the *change* in a
sample value rather than its absolute value.
"""

from collections.abc import Iterator
from typing import Any

import pytest
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.kafka.consumer import deserialize
from src.kafka.schemas import SyncEventMessage
from src.models import SyncEvent
from src.observability.metrics import ErrorType, record_error
from src.observability.tracing import extract_context, inject_headers
from src.workers.sync_processor import ProcessingOutcome, SyncProcessor
from tests.conftest import FakePublisher


def sample(name: str, **labels: str) -> float:
    """Current value of one metric sample (0.0 if it has never been observed)."""
    return REGISTRY.get_sample_value(name, labels) or 0.0


# One in-memory exporter for the whole session: OpenTelemetry allows setting the
# global tracer provider only once per process.
_EXPORTER = InMemorySpanExporter()
_PROVIDER = TracerProvider()
_PROVIDER.add_span_processor(SimpleSpanProcessor(_EXPORTER))
trace.set_tracer_provider(_PROVIDER)


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    """Captured spans, cleared before and after each test."""
    _EXPORTER.clear()
    yield _EXPORTER
    _EXPORTER.clear()


def by_name(finished: tuple[ReadableSpan, ...], name: str) -> ReadableSpan:
    """The single finished span called `name`."""
    matches = [span for span in finished if span.name == name]
    assert len(matches) == 1, f"expected one {name!r} span, got {[s.name for s in finished]}"
    return matches[0]


def parent_id(span: ReadableSpan) -> int | None:
    """Span ID of `span`'s parent, if any."""
    return span.parent.span_id if span.parent is not None else None


async def insert_pending(
    session_factory: async_sessionmaker[AsyncSession], data: dict[str, Any]
) -> SyncEventMessage:
    """A committed pending row and the message the API would publish for it."""
    async with session_factory() as session:
        event = SyncEvent(
            entity_id="obs_1",
            source_system="stripe",
            destination_system="hubspot",
            source_data=data,
        )
        session.add(event)
        await session.commit()
        return SyncEventMessage.from_sync_event(event)


# --- metrics --------------------------------------------------------------------


async def test_post_sync_updates_api_metrics(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """A successful POST counts one accepted sync and one API latency observation."""
    accepted = sample("sync_events_total", status="accepted", source_system="salesforce")
    api_count = sample("sync_api_latency_seconds_count", status="accepted")

    response = await client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 200
    assert sample("sync_events_total", status="accepted", source_system="salesforce") == (
        accepted + 1
    )
    assert sample("sync_api_latency_seconds_count", status="accepted") == api_count + 1


async def test_replay_is_not_counted_as_a_new_sync(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """An idempotent replay is a separate latency series and adds no accepted sync."""
    payload = {**sync_payload, "idempotency_key": "obs-replay-1"}
    await client.post("/api/v1/sync", json=payload)
    accepted = sample("sync_events_total", status="accepted", source_system="salesforce")
    replayed = sample("sync_api_latency_seconds_count", status="replayed")

    await client.post("/api/v1/sync", json=payload)

    assert sample("sync_events_total", status="accepted", source_system="salesforce") == accepted
    assert sample("sync_api_latency_seconds_count", status="replayed") == replayed + 1


async def test_kafka_outage_counts_publish_error(
    client: AsyncClient, fake_publisher: FakePublisher, sync_payload: dict[str, Any]
) -> None:
    """A failed publish shows up as an API error observation (the 500 response)."""
    fake_publisher.fail = True
    errors = sample("sync_api_latency_seconds_count", status="error")

    response = await client.post("/api/v1/sync", json=sync_payload)

    assert response.status_code == 500
    assert sample("sync_api_latency_seconds_count", status="error") == errors + 1


async def test_processor_metrics_for_synced_and_failed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Processor outcomes move sync_events_total, latency histograms and errors_total."""
    processor = SyncProcessor(session_factory)
    synced = sample("sync_events_total", status="synced", source_system="stripe")
    failed = sample("sync_events_total", status="failed", source_system="stripe")
    validation = sample("errors_total", type="validation")
    processing = sample("sync_processing_latency_seconds_count", outcome="synced")
    transform = sample("sync_transform_latency_seconds_count", source_system="stripe")
    end_to_end = sample("sync_latency_seconds_count", source_system="stripe")

    good = await insert_pending(session_factory, {"customer_name": "Linus"})
    bad = await insert_pending(session_factory, {"email": "nope"})
    assert await processor.handle(good) is ProcessingOutcome.SYNCED
    assert await processor.handle(bad) is ProcessingOutcome.FAILED

    assert sample("sync_events_total", status="synced", source_system="stripe") == synced + 1
    assert sample("sync_events_total", status="failed", source_system="stripe") == failed + 1
    assert sample("errors_total", type="validation") == validation + 1
    assert sample("sync_processing_latency_seconds_count", outcome="synced") == processing + 1
    assert sample("sync_transform_latency_seconds_count", source_system="stripe") == (transform + 2)
    assert sample("sync_latency_seconds_count", source_system="stripe") == end_to_end + 2
    assert sample("processor_concurrent_syncs") == 0  # back to idle


async def test_duplicate_delivery_does_not_double_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A redelivered message adds no synced count (idempotency shows in metrics too)."""
    processor = SyncProcessor(session_factory)
    message = await insert_pending(session_factory, {"customer_name": "Linus"})
    await processor.handle(message)
    synced = sample("sync_events_total", status="synced", source_system="stripe")

    assert await processor.handle(message) is ProcessingOutcome.DUPLICATE

    assert sample("sync_events_total", status="synced", source_system="stripe") == synced


def test_malformed_message_counts_error() -> None:
    """Poison messages are counted under errors_total{type="malformed_message"}."""
    before = sample("errors_total", type="malformed_message")

    assert deserialize(b"{not json") is None

    assert sample("errors_total", type="malformed_message") == before + 1


def test_record_error_uses_bounded_labels() -> None:
    """Every ErrorType is a valid label value."""
    for error_type in ErrorType:
        before = sample("errors_total", type=error_type.value)
        record_error(error_type)
        assert sample("errors_total", type=error_type.value) == before + 1


async def test_metrics_endpoint_serves_prometheus_text(
    client: AsyncClient, sync_payload: dict[str, Any]
) -> None:
    """GET /metrics returns the text exposition format with every pipeline metric."""
    await client.post("/api/v1/sync", json=sync_payload)

    response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    for name in (
        "sync_events_total",
        "sync_latency_seconds_bucket",
        "sync_api_latency_seconds_bucket",
        "sync_processing_latency_seconds",
        "sync_transform_latency_seconds",
        "sync_kafka_publish_latency_seconds",
        "processor_concurrent_syncs",
        "processor_batch_size",
        "processor_running",
        "errors_total",
    ):
        assert name in response.text, name


# --- tracing --------------------------------------------------------------------


async def test_post_sync_span_hierarchy(
    client: AsyncClient, sync_payload: dict[str, Any], spans: InMemorySpanExporter
) -> None:
    """POST /api/v1/sync (FastAPI's server span) is the root; `sync.accept` sits under
    it and parents the DB spans."""
    response = await client.post("/api/v1/sync", json=sync_payload)
    assert response.status_code == 200

    finished = spans.get_finished_spans()
    root = by_name(finished, "POST /api/v1/sync")
    accept = by_name(finished, "sync.accept")

    assert root.parent is None
    assert root.kind is trace.SpanKind.SERVER
    assert root.attributes is not None
    assert root.attributes["http.route"] == "/api/v1/sync"
    # sync.accept descends from the server span (via fastapi.endpoint).
    ids = {span.context.span_id: span for span in finished}
    ancestor = accept
    while ancestor.parent is not None and ancestor.parent.span_id in ids:
        ancestor = ids[ancestor.parent.span_id]
    assert ancestor is root
    for child in ("db.persist", "db.mark_published"):
        assert parent_id(by_name(finished, child)) == accept.context.span_id, child
    assert accept.attributes is not None
    assert accept.attributes["sync.id"] == response.json()["sync_id"]
    assert accept.attributes["sync.entity_id"] == "contact_12345"


async def test_processor_spans_continue_the_api_trace(
    session_factory: async_sessionmaker[AsyncSession], spans: InMemorySpanExporter
) -> None:
    """The processor span joins the producer's trace through the Kafka headers, and
    load/transform/validate/update spans hang off it."""
    message = await insert_pending(session_factory, {"customer_name": "Linus"})
    tracer = trace.get_tracer("test")
    with tracer.start_as_current_span("kafka.publish") as publish_span:
        headers = inject_headers()
    assert any(key == "traceparent" for key, _ in headers)

    await SyncProcessor(session_factory).handle(message, extract_context(headers))

    finished = spans.get_finished_spans()
    consume = by_name(finished, "processor.consume")
    assert consume.context.trace_id == publish_span.get_span_context().trace_id
    assert parent_id(consume) == publish_span.get_span_context().span_id
    for child in ("db.load", "transform", "db.update"):
        assert parent_id(by_name(finished, child)) == consume.context.span_id, child
    assert (
        parent_id(by_name(finished, "validate")) == by_name(finished, "transform").context.span_id
    )
    assert consume.attributes is not None
    assert consume.attributes["sync.outcome"] == "synced"


async def test_validation_failure_marks_transform_span_as_error(
    session_factory: async_sessionmaker[AsyncSession], spans: InMemorySpanExporter
) -> None:
    """A failed sync shows up as an error span in Jaeger, with the error count."""
    message = await insert_pending(session_factory, {"email": "nope"})

    await SyncProcessor(session_factory).handle(message)

    transform = by_name(spans.get_finished_spans(), "transform")
    assert transform.status.status_code is trace.StatusCode.ERROR
    assert transform.attributes is not None
    assert transform.attributes["sync.validation_errors"] == 2


def test_extract_context_tolerates_missing_or_garbage_headers() -> None:
    """Messages without (or with corrupt) trace headers just start a new trace."""
    for headers in (None, [], [("traceparent", b"\xff\xfe garbage")]):
        context = extract_context(headers)
        assert not trace.get_current_span(context).get_span_context().is_valid
