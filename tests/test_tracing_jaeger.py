"""Integration: POST /api/v1/sync -> trace exported over OTLP -> visible in Jaeger.

Needs the monitoring profile (`docker compose --profile monitoring up -d`);
skipped if Jaeger isn't reachable.

OpenTelemetry allows one global TracerProvider per process, and the tracing unit
tests (tests/test_observability.py) install it. This test attaches a real OTLP
exporter to that same provider for its duration, tags its request with a unique
entity_id, and then looks that trace up through Jaeger's query API.
"""

import asyncio
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

JAEGER_UI = os.environ.get("JAEGER_QUERY_URL", "http://localhost:16686")
OTLP_ENDPOINT = os.environ.get("JAEGER_OTLP_URL", "http://localhost:4318/v1/traces")
TEST_SERVICE = "sync-platform-test"

pytestmark = pytest.mark.jaeger


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


class _ToggleExporter(SpanExporter):
    """Forwards spans to OTLP only while `active` (span processors can't be removed)."""

    def __init__(self) -> None:
        self.active = False
        # Exported under its own service name, so test traces never mix with the app's.
        self._otlp = OTLPSpanExporter(endpoint=OTLP_ENDPOINT)
        self._resource = Resource.create({SERVICE_NAME: TEST_SERVICE})

    def export(self, spans: Any) -> SpanExportResult:
        if not self.active:
            return SpanExportResult.SUCCESS
        relabelled = [_with_resource(span, self._resource) for span in spans]
        return self._otlp.export(relabelled)

    def shutdown(self) -> None:
        self._otlp.shutdown()  # type: ignore[no-untyped-call]  # untyped in the SDK


def _with_resource(span: ReadableSpan, resource: Resource) -> ReadableSpan:
    """Copy of `span` reporting `resource` (so it lands under TEST_SERVICE)."""
    return ReadableSpan(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=resource,
        attributes=span.attributes,
        events=span.events,
        links=span.links,
        kind=span.kind,
        status=span.status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


_EXPORTER = _ToggleExporter()
_registered = False


@pytest.fixture
def jaeger_export() -> Iterator[None]:
    """Send this test's spans to the real Jaeger (skip if Jaeger isn't running)."""
    global _registered
    try:
        _get_json(f"{JAEGER_UI}/api/services")
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip(
            f"Jaeger not reachable at {JAEGER_UI} ({exc}); "
            "run docker compose --profile monitoring up -d"
        )
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        # Running alone: no SDK provider installed yet, so install one.
        provider = TracerProvider()
        trace.set_tracer_provider(provider)
        provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    if not _registered:
        provider.add_span_processor(SimpleSpanProcessor(_EXPORTER))
        _registered = True
    _EXPORTER.active = True
    yield
    _EXPORTER.active = False


async def test_post_sync_trace_appears_in_jaeger(
    client: AsyncClient, sync_payload: dict[str, Any], jaeger_export: None
) -> None:
    """The request's spans reach Jaeger with the expected parent/child structure."""
    entity_id = f"jaeger_{uuid.uuid4().hex[:10]}"

    response = await client.post("/api/v1/sync", json={**sync_payload, "entity_id": entity_id})
    assert response.status_code == 200

    tags = urllib.parse.quote(json.dumps({"sync.entity_id": entity_id}))
    url = f"{JAEGER_UI}/api/traces?service={TEST_SERVICE}&tags={tags}&limit=5&lookback=1h"
    spans: list[dict[str, Any]] = []
    for _ in range(60):  # Jaeger indexes asynchronously; allow a few seconds
        traces = _get_json(url)["data"]
        if traces:
            # The tag query returns the whole trace the tagged span belongs to.
            spans = traces[0]["spans"]
            names = {s["operationName"] for s in spans}
            if {"POST /api/v1/sync", "sync.accept", "db.persist", "db.mark_published"} <= names:
                break
        await asyncio.sleep(0.25)

    by_name = {s["operationName"]: s for s in spans}
    assert "sync.accept" in by_name, f"trace for {entity_id} not found in Jaeger"
    ids = {s["spanID"]: s for s in spans}

    def parent(span: dict[str, Any]) -> dict[str, Any] | None:
        refs = [r["spanID"] for r in span.get("references", []) if r["refType"] == "CHILD_OF"]
        return ids.get(refs[0]) if refs else None

    # sync.accept descends from FastAPI's server span; DB spans hang off sync.accept.
    ancestor = by_name["sync.accept"]
    while (up := parent(ancestor)) is not None:
        ancestor = up
    assert ancestor["operationName"] == "POST /api/v1/sync"
    for child in ("db.persist", "db.mark_published"):
        found = parent(by_name[child])
        assert found is not None and found["operationName"] == "sync.accept", child
    accept_tags = {t["key"]: t["value"] for t in by_name["sync.accept"]["tags"]}
    assert accept_tags["sync.id"] == response.json()["sync_id"]
