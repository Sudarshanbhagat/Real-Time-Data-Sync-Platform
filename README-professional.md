# Real-Time Data Sync Platform

A backend integration service for synchronizing entity data between enterprise systems through an asynchronous, event-driven pipeline. The API persists incoming sync requests in PostgreSQL, publishes events to Kafka, and processes them in the background into a validated canonical schema.

**Live API:** [sync-platform-api.onrender.com](https://sync-platform-api.onrender.com) · **API schema:** [OpenAPI JSON](https://sync-platform-api.onrender.com/openapi.json)

## Overview

Integrations become difficult to operate when requests are duplicated, downstream systems disagree on field formats, or processing fails after a request is accepted. This project explores those problems with an explicit persistence boundary, Kafka-based asynchronous processing, idempotent request handling, canonical data transformation, and operational health endpoints.

### Key engineering decisions

- **PostgreSQL as the system of record:** accepted syncs and idempotency records are persisted before publication.
- **Kafka for asynchronous processing:** accepted events are published to the `sync_events` topic and consumed by a background processor.
- **Idempotent request handling:** retries with the same key and request body replay the original result instead of creating a second event.
- **Canonical schema:** source-specific payloads are normalized and validated before a sync is marked `synced`.
- **Failure-aware processing:** processing errors are recorded against the sync; database errors are retried, and already completed events are safe to encounter again.
- **Operational visibility:** health endpoints, Prometheus metrics, structured logs, and OpenTelemetry tracing support debugging and performance analysis.

## Architecture

```text
Client
  |
  | POST /api/v1/sync
  v
FastAPI service
  |
  |-- Persist sync + idempotency record
  |       |
  |       v
  |   PostgreSQL
  |
  |-- Publish event
          |
          v
     Kafka: sync_events
          |
          v
    Background processor
          |
          |-- Load persisted sync
          |-- Transform source payload
          |-- Validate canonical data
          |-- Record outcome and timing
          v
      PostgreSQL
          |
          v
GET /api/v1/status/{entity_id}
```

The request API and processor are decoupled: a successful submission is initially reported as `pending`; the status endpoint can then be used to observe the final `synced` or `failed` outcome.

## Technology stack

| Area | Technologies |
|---|---|
| API | Python, FastAPI, Pydantic |
| Persistence | PostgreSQL, SQLAlchemy 2 async, asyncpg |
| Event streaming | Apache Kafka, aiokafka |
| Background processing | Async Python worker |
| Observability | Prometheus, Grafana, OpenTelemetry, Jaeger |
| Local infrastructure | Docker Compose, Kafka UI, Redis |
| Testing and quality | pytest, httpx, Ruff, mypy, k6 |

## Live service

| Endpoint | Purpose |
|---|---|
| [`GET /health`](https://sync-platform-api.onrender.com/health) | Liveness check |
| [`GET /health/ready`](https://sync-platform-api.onrender.com/health/ready) | Readiness check, including database connectivity |
| [`GET /health/processor`](https://sync-platform-api.onrender.com/health/processor) | Background processor state and counters |
| [`GET /openapi.json`](https://sync-platform-api.onrender.com/openapi.json) | OpenAPI schema |

The API also exposes `POST /api/v1/sync` to submit a sync and `GET /api/v1/status/{entity_id}` to retrieve the latest status for an entity. The latter endpoints write or read application data respectively.

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check; does not verify dependencies |
| `GET` | `/health/ready` | Readiness check; returns an error if the database is unavailable |
| `GET` | `/health/processor` | Processor state, counters, and latency percentiles |
| `GET` | `/metrics` | Prometheus metrics in exposition format |
| `POST` | `/api/v1/sync` | Persist and publish a sync request |
| `GET` | `/api/v1/status/{entity_id}` | Retrieve the latest sync status and canonical data |

### Submit a sync

Example request for a Salesforce-to-HubSpot sync:

```bash
curl -X POST "https://sync-platform-api.onrender.com/api/v1/sync" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_id": "contact_12345",
    "source_system": "salesforce",
    "destination_system": "hubspot",
    "data": {
      "name": "Ada Lovelace",
      "first_name": "Ada",
      "last_name": "Lovelace",
      "email": "ada@example.com"
    },
    "idempotency_key": "sf-contact-12345-event-0001"
  }'
```

The API initially returns a `pending` result when the event has been accepted. Poll the status endpoint to observe the eventual outcome:

```bash
curl "https://sync-platform-api.onrender.com/api/v1/status/contact_12345"
```

A successful result includes `status: "synced"` and `canonical_data`. A validation failure is recorded as `failed` with an `error_message`.

### Idempotency

Provide an `idempotency_key` when submitting a request:

- Same key and same request body: the original result is replayed (`idempotent_replay: true`); a second sync event is not created.
- Same key with a different request body: the API returns `422`.
- Same key while the first request is still in flight: the API returns `409`.

The sync event and its idempotency record are committed in one database transaction, so concurrent duplicate requests resolve to one persisted event.

## Event processing and delivery semantics

Events are keyed by `entity_id`, preserving partition affinity for events belonging to the same entity. The producer uses Kafka acknowledgements and idempotence. The consumer commits offsets after handling a batch, which provides at-least-once delivery; processing must therefore tolerate redelivery.

The processor loads the persisted sync record, skips records that are no longer pending, transforms and validates the source payload, and stores the outcome and processing timestamps. A duplicate delivery for an already completed record is a no-op.

### Canonical data model

The canonical representation includes:

| Field | Meaning |
|---|---|
| `entity_id` | Entity identifier |
| `name` | Required normalized name |
| `email` | Email when available |
| `status` | Normalized source status when available |
| `metadata` | Unmapped source fields |
| `schema_version` | Canonical schema version |
| `source_system` | Originating system |
| `transformed_at` | Transformation timestamp |

Mappings are defined for Salesforce, HubSpot, Stripe, and custom payloads. For example, Salesforce name resolution checks `account_name`, then `name`, then `first_name` plus `last_name`. Validation failures are recorded with an explanatory error rather than silently treated as successful syncs.

### Failure handling

| Scenario | Behavior |
|---|---|
| Invalid source data | Sync is marked `failed` with a validation error |
| Unexpected transformation error | Sync is marked failed and the exception is logged |
| Database error during processing | Retries with backoff; an unhandled batch is not committed |
| Redelivery of an already completed sync | No-op; completed work is not applied again |
| Kafka unavailable during publication | The persisted record can remain unpublished; retry with the same idempotency key after recovery |
| Processor unavailable | Accepted records can remain pending until processing resumes |

Unpublished records created without an idempotency key do not currently have an automatic re-publisher; recovery may require operational intervention. Failed syncs are terminal in the processor: after correcting the payload, submit a new request with a new idempotency key.

## Deployed API testing and performance

The deployed API was tested using PowerShell HTTP requests and [k6](https://grafana.com/docs/k6/latest/). The load tests were short, controlled baseline runs against the live service, not maximum-capacity or soak tests.

### Health endpoint — `GET /health`

| Virtual users | Duration | Requests | Average latency | p95 latency | HTTP failures |
|---:|---:|---:|---:|---:|---:|
| 5 | 30 s | 123 | 245.76 ms | 250.80 ms | 0% |
| 10 | 30 s | 247 | 242.97 ms | 255.54 ms | 0% |
| 20 | 30 s | 495 | 243.53 ms | 272.42 ms | 0% |

All three runs completed with zero HTTP request failures and 100% successful checks. p95 latency remained below 273 ms across the tested concurrency levels.

### Database-backed status endpoint — `GET /api/v1/status/{entity_id}`

| Virtual users | Duration | Requests | Average latency | p95 latency | HTTP failures |
|---:|---:|---:|---:|---:|---:|
| 5 | 30 s | 120 | 262.04 ms | 294.52 ms | 0% |

All 120 requests returned HTTP 200, and all checks confirmed that the known entity remained in the `synced` state.

### Functional verification

| Test | Observed result |
|---|---|
| API liveness | `GET /health` returned HTTP 200 and `{"status":"healthy"}` |
| Database readiness | `GET /health/ready` returned HTTP 200 and reported `database: ok` |
| Processor health | Processor reported `running: true` |
| Sync submission | Synthetic event accepted with initial status `pending` |
| Asynchronous completion | Event subsequently reached `synced` |
| Status lookup | HTTP 200 with canonical data and no error |
| Idempotency replay | Duplicate request returned `idempotent_replay: true` and reused sync ID `3` |
| Processor metrics | At observation time: 1 processed, 1 synced, 0 failed, 0 database errors |

### How to interpret these results

- Each virtual user waits one second between iterations. The request rates therefore reflect this test configuration, not the service's maximum capacity.
- Latency includes network and hosting overhead; it is not a database-only or Kafka-only measurement.
- The idempotency result verifies API-level replay. The processor's `duplicates_skipped` counter was `0` at the observation time, so these results do not claim that the processor consumed a duplicate.
- These runs establish a baseline for the tested endpoints and conditions. They do not establish production-scale reliability, maximum throughput, or behavior under sustained load.

### Test evidence

Screenshots were captured for the endpoint checks, k6 summaries, successful sync, idempotency replay, and processor metrics. Add the image files to the repository before linking them here; GitHub cannot render screenshots that exist only on a local machine or in a chat.

Suggested location:

```text
docs/
  results/
    deployed-api/
      health-check.png
      database-readiness.png
      processor-health.png
      health-load-5vus.png
      health-load-10vus.png
      health-load-20vus.png
      entity-status-success.png
      status-load-5vus.png
      sync-submission.png
      sync-completed.png
      idempotency-replay.png
      processor-metrics-after-sync.png
      final-entity-status.png
```

## Local performance benchmarks

The following measurements were collected on a Windows 11 development laptop using Docker Desktop, a single app instance, and `SYNC_PROCESSOR_CONCURRENCY=10`. These local measurements are separate from the deployed API tests above.

| Workload | Throughput | Latency p50 / p95 / p99 |
|---|---:|---:|
| Processor only, 100 events | 114–119 events/s | 38–40 / 52–60 / 55–64 ms |
| Processor only, 1,000 events | 118 events/s | 42 / 61 / 88 ms |
| Processor with metrics and tracing, 1,000 events | 113 events/s | 43 / 71 / 76 ms |
| API via Windows host to container, sequential | 15.9 req/s | 58 / 90 / 96 ms |
| API via Windows host to container, concurrency 10 | 34.8 req/s | 246 / 501 / 623 ms |
| API via Windows host to container, concurrency 50 | 19.7 req/s | 1,747 / 6,845 / 10,818 ms |
| API from inside Linux container, concurrency 1 / 10 / 50 | 52 / 100 / 114 req/s | p50: 18 / 98 / 415 ms |

The Windows-host measurements include the Windows-to-Docker networking path and should not be interpreted as application-only throughput. The local and deployed results come from different environments and are not directly comparable.

## Observability

The service exposes Prometheus metrics at `/metrics`, including:

- `sync_events_total` — sync lifecycle counts
- `sync_latency_seconds` — end-to-end sync latency
- `sync_api_latency_seconds` — API acceptance, replay, and error latency
- `sync_processing_latency_seconds` — processor handling time
- `sync_kafka_publish_latency_seconds` — Kafka publish latency
- `processor_running` and `processor_concurrent_syncs` — processor state

The repository includes Grafana dashboard definitions for system overview, performance, and errors. OpenTelemetry traces can connect the HTTP request to asynchronous processing through trace-context propagation in Kafka headers. Tracing is configured through environment variables and OTLP.

For local monitoring, start the monitoring profile:

```bash
docker compose --profile monitoring up -d
```

| Tool | Local URL |
|---|---|
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

## Run locally

### Prerequisites

- Python 3.11 or newer (the project has been exercised with Python 3.14)
- Docker and Docker Compose
- Git

### Start dependencies and API

```bash
cp .env.example .env
docker compose up -d
pip install -r requirements-dev.txt
python scripts/setup_db.py
python scripts/create_topic.py
uvicorn src.main:app --reload
```

The API documentation is available at http://localhost:8000/docs. Kafka UI is available at http://localhost:8080.

On Windows PowerShell, use `curl.exe` rather than the `curl` alias when following shell examples, or put JSON request bodies in a file to avoid quoting issues.

### Run tests and quality checks

```bash
pytest tests/ -p no:cacheprovider
pytest tests/ -p no:cacheprovider -m "not kafka"
pytest tests/ -p no:cacheprovider -m kafka
pytest tests/ -p no:cacheprovider -m jaeger
ruff check . && ruff format --check .
mypy src tests scripts
```

Kafka integration tests require the Docker Compose broker. Jaeger integration tests require the monitoring stack to be running; tests configured to skip when those dependencies are unavailable should be interpreted accordingly.

### Run the local load test

```bash
python scripts/load_test_phase3.py --mode processor
python scripts/load_test_phase3.py --mode processor -n 1000
python scripts/load_test_phase3.py
python scripts/load_test_phase3.py -n 1000 -c 50
```

The processor mode measures Kafka consumption, transformation, and database update. The default API mode measures the HTTP request path as well.

## Docker

Build the image:

```bash
docker build -t sync-platform .
docker compose --profile app up -d app
```

The image runs as a non-root user and includes a health check on `/health`. The entrypoint handles schema setup and topic creation before starting the API. The same image can run the processor as a separate process:

```bash
docker run ... sync-platform python -m src.workers.sync_processor
```

## Repository layout

```text
src/
  main.py                 FastAPI application and lifecycle
  config.py               Environment-based configuration
  database.py             Async database engine and session setup
  kafka/
    schemas.py             Kafka event schema
    producer.py            Event publishing
    consumer.py            Kafka consumer
  models/                  Database models
  routes/                  Health and sync API routes
  schemas/                 Request and response schemas
  services/
    sync_service.py        Sync acceptance and idempotency
    transformer.py         Source-to-canonical mappings
    validator.py           Canonical data validation
  workers/
    sync_processor.py      Background event processor
  observability/
    metrics.py             Prometheus instrumentation
    tracing.py             OpenTelemetry setup
scripts/
  setup_db.py
  create_topic.py
  load_test_phase3.py
  load_test_cloud.py
  build_dashboards.py
monitoring/
  Grafana dashboards and provisioning
tests/
  API, Kafka, processor, transformation, observability, and end-to-end tests
```

## Further reading

- [Case study](CASE_STUDY.md)
- [Interview talking points](INTERVIEW_TALKING_POINTS.md)

## Known limitations and next steps

- Add an automatic re-publisher for persisted events that failed to publish to Kafka.
- Run longer-duration and higher-concurrency tests in an isolated environment before making capacity claims.
- Add committed screenshot artifacts and link them from the testing section.
- Validate recovery behavior and performance under dependency outages using repeatable automated tests.
