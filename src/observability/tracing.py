"""OpenTelemetry tracing, exported to Jaeger over OTLP/HTTP.

Jaeger v2 receives OpenTelemetry natively (OTLP on :4318), and the old
Jaeger-Thrift UDP agent protocol (:6831) is gone from Jaeger v2 and deprecated
in OpenTelemetry, so spans go out over OTLP. They show up the same way in
the Jaeger UI at http://localhost:16686.

Span layout:

    POST /api/v1/sync                       server span, opened by FastAPI's built-in
    │                                       OpenTelemetry support
    ├─ fastapi.dependencies / .serialization
    └─ fastapi.endpoint
       └─ sync.accept                       our span: entity, systems, sync_id attributes
          ├─ db.persist                     insert sync + idempotency record, commit
          ├─ kafka.publish                  producer span; context injected into headers
          │  ├─ kafka.serialize
          │  └─ kafka.send                  network send -> broker ack
          └─ db.mark_published

    processor.consume                       consumer span, child of kafka.publish via headers
    ├─ db.load
    ├─ transform
    │  └─ validate
    └─ db.update

The processor span continues the trace started by the API request, so Jaeger
shows one trace from HTTP request to database update.

Tracing is off unless `TRACING_ENABLED=true`. When it's off, every span here is
a no-op and costs next to nothing.
"""

import logging
from collections.abc import Mapping, Sequence

from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

from src.config import get_settings

logger = logging.getLogger(__name__)

TRACER_NAME = "sync_platform"

_provider: TracerProvider | None = None


def get_tracer() -> trace.Tracer:
    """Tracer for this application (a no-op tracer until `init_tracing` runs)."""
    return trace.get_tracer(TRACER_NAME)


def init_tracing(exporter: SpanExporter | None = None) -> TracerProvider | None:
    """Install the global tracer provider if tracing is enabled (idempotent).

    `exporter` overrides the OTLP exporter, so tests can capture spans in memory.
    Returns the provider, or None if tracing is disabled.
    """
    global _provider
    settings = get_settings()
    if _provider is not None:
        return _provider
    if not settings.tracing_enabled and exporter is None:
        return None
    if exporter is None:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        exporter = OTLPSpanExporter(endpoint=settings.otlp_traces_endpoint)
    provider = TracerProvider(resource=Resource.create({SERVICE_NAME: settings.otel_service_name}))
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _provider = provider
    logger.info(
        "tracing_enabled service=%s endpoint=%s",
        settings.otel_service_name,
        settings.otlp_traces_endpoint,
    )
    return provider


def shutdown_tracing() -> None:
    """Flush buffered spans and stop the exporter."""
    if _provider is not None:
        _provider.shutdown()


# Kafka headers are a list of (str, bytes). OpenTelemetry's propagators want a
# str -> str carrier, so convert at the edges.


def inject_headers() -> list[tuple[str, bytes]]:
    """W3C trace-context headers for the current span, ready for a Kafka message."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(key, value.encode("utf-8")) for key, value in carrier.items()]


def extract_context(headers: Sequence[tuple[str, bytes]] | None) -> Context:
    """Rebuild the producer's trace context from Kafka message headers."""
    carrier: Mapping[str, str] = {
        key: value.decode("utf-8", errors="replace") for key, value in (headers or [])
    }
    return propagate.extract(carrier)
