"""Prometheus metrics for the sync pipeline, exposed at `GET /metrics`.

Label values are always drawn from small fixed sets (statuses, source systems,
error types), never from entity IDs or messages, so series cardinality stays
bounded. The text of failed syncs' error messages lives in the database
(`sync_events.error_message`), not in Prometheus.

Metric map (names as scraped):

    sync_events_total{status,source_system}        counter    accepted (API), synced/failed
    sync_latency_seconds{source_system}            histogram  row created -> processed
    sync_api_latency_seconds{status}               histogram  POST /api/v1/sync server time
    sync_processing_latency_seconds{outcome}       histogram  message handled -> DB commit
    sync_transform_latency_seconds{source_system}  histogram  transform + validate only
    sync_kafka_publish_latency_seconds{result}     histogram  producer send -> broker ack
    processor_concurrent_syncs                     gauge      syncs being processed now
    processor_batch_size                           gauge      messages in the current batch
    processor_running                              gauge      1 while the processor is alive
    errors_total{type}                             counter    errors by type (see ErrorType)
"""

from enum import StrEnum

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# Buckets in seconds, chosen around the measured ranges: API ~5-150 ms, processor
# ~5-100 ms, Kafka publish ~2-15 ms, end to end up to a few seconds under load.
LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


class ErrorType(StrEnum):
    """Bounded set of `errors_total{type}` label values."""

    VALIDATION = "validation"  # source data failed the canonical schema (sync -> failed)
    TRANSFORM_CRASH = "transform_crash"  # unexpected exception in the transformer
    KAFKA_PUBLISH = "kafka_publish"  # producer could not deliver (broker down/timeout)
    DATABASE = "database"  # SQLAlchemy error in the API or processor
    MALFORMED_MESSAGE = "malformed_message"  # Kafka value not a valid SyncEventMessage
    MISSING_ROW = "missing_row"  # message for a sync_id not in the database
    PROCESSOR_CRASH = "processor_crash"  # consume loop died and was restarted


SYNC_EVENTS = Counter(
    "sync_events",
    "Sync lifecycle events by status: accepted (API), synced/failed (processor).",
    ["status", "source_system"],
)
SYNC_LATENCY = Histogram(
    "sync_latency_seconds",
    "End-to-end latency: sync row created by the API -> processed by the worker.",
    ["source_system"],
    buckets=LATENCY_BUCKETS,
)
API_LATENCY = Histogram(
    "sync_api_latency_seconds",
    "Server-side latency of POST /api/v1/sync (persist + publish).",
    ["status"],
    buckets=LATENCY_BUCKETS,
)
PROCESSING_LATENCY = Histogram(
    "sync_processing_latency_seconds",
    "Processor latency: Kafka message handled -> database committed.",
    ["outcome"],
    buckets=LATENCY_BUCKETS,
)
TRANSFORM_LATENCY = Histogram(
    "sync_transform_latency_seconds",
    "Time to transform and validate one record.",
    ["source_system"],
    buckets=LATENCY_BUCKETS,
)
KAFKA_PUBLISH_LATENCY = Histogram(
    "sync_kafka_publish_latency_seconds",
    "Kafka publish latency: send -> broker acknowledgement.",
    ["result"],
    buckets=LATENCY_BUCKETS,
)
PROCESSOR_CONCURRENT = Gauge(
    "processor_concurrent_syncs", "Syncs the background processor is handling right now."
)
PROCESSOR_BATCH_SIZE = Gauge(
    "processor_batch_size", "Messages in the batch the processor is currently handling."
)
PROCESSOR_RUNNING = Gauge(
    "processor_running", "1 while the background processor task is alive, else 0."
)
ERRORS = Counter("errors", "Errors by type.", ["type"])


def record_error(error_type: ErrorType) -> None:
    """Count one error of `error_type`."""
    ERRORS.labels(type=error_type.value).inc()


def render() -> tuple[bytes, str]:
    """Return the current metrics in Prometheus text format, with its content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
