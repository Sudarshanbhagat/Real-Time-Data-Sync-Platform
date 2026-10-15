# Real-Time Data Sync Platform

Synchronises entity changes between enterprise systems (Salesforce, HubSpot, Stripe)
with idempotent, low-latency writes.

- **Phase 1 (done):** FastAPI service + PostgreSQL with idempotent writes.
- **Phase 2 (done):** every accepted sync is published to the Kafka topic `sync_events`.
- **Phase 3 (done):** a background processor consumes `sync_events`, maps each
  record to the canonical schema, validates it, and marks the sync `synced` or `failed`.
- **Phase 4 (current):** Prometheus metrics, Grafana dashboards, Jaeger tracing,
  a production Docker image and a Render + Aiven deployment blueprint.

Write-ups: [CASE_STUDY.md](CASE_STUDY.md) · [INTERVIEW_TALKING_POINTS.md](INTERVIEW_TALKING_POINTS.md)

## Stack

- Python 3.14 (3.11+ supported), FastAPI, SQLAlchemy 2 (async) + asyncpg, Pydantic v2, aiokafka
- PostgreSQL 15 (system of record), Kafka + Zookeeper, Redis, Kafka UI
- Observability: prometheus-client, OpenTelemetry SDK → Jaeger, Grafana
- pytest + httpx (API tests use in-memory SQLite; Kafka tests use the docker-compose broker)

## Quick start

```bash
cp .env.example .env
docker compose up -d                    # postgres, kafka, zookeeper, redis, kafka-ui
pip install -r requirements-dev.txt
python scripts/setup_db.py              # create tables / add new columns (--reset drops & recreates)
python scripts/create_topic.py          # create the sync_events topic (idempotent)
uvicorn src.main:app --reload
```

- API docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8080
- Metrics: http://localhost:8000/metrics
- With `docker compose --profile monitoring up -d`: Grafana http://localhost:3000,
  Prometheus http://localhost:9090, Jaeger http://localhost:16686 (see "Monitoring" below)

## API

| Method | Path                         | Description                                   |
|--------|------------------------------|-----------------------------------------------|
| GET    | `/health`                    | Liveness (no dependencies checked)            |
| GET    | `/health/ready`              | Readiness: 503 if the database is down        |
| GET    | `/health/processor`          | Background processor: running, counts, p50/p95/p99 |
| GET    | `/metrics`                   | Prometheus metrics (text exposition format)   |
| POST   | `/api/v1/sync`               | Accept a sync (`pending`) and publish it to Kafka |
| GET    | `/api/v1/status/{entity_id}` | Latest sync for an entity incl. `canonical_data` (404 if none) |

Example:

```bash
curl -X POST http://localhost:8000/api/v1/sync \
  -H "Content-Type: application/json" \
  -d '{
        "entity_id": "contact_12345",
        "source_system": "salesforce",
        "destination_system": "hubspot",
        "data": {"FirstName": "Ada", "LastName": "Lovelace", "Email": "ada@example.com"},
        "idempotency_key": "sf-contact_12345-evt-0001"
      }'
```

The response has `status: "pending"`: the sync is saved and queued. The background
processor usually finishes it within milliseconds; poll `GET /api/v1/status/{entity_id}`
to see `synced` (with `canonical_data`) or `failed` (with `error_message`).
`latency_ms` is the API's time (persist + publish), `publish_latency_ms` the Kafka
round-trip alone.

### Idempotency

Send an `idempotency_key` to make retries safe:

- Same key + same body → original result replayed (`idempotent_replay: true`,
  header `Idempotent-Replayed: true`); no new sync event and **no second Kafka message**.
- Same key + different body → `422`.
- Same key while the first request is still in flight → `409`.

The sync event and its idempotency record are committed in one transaction, so
concurrent duplicates resolve to exactly one event.

## Kafka (Phase 2)

### Event flow

```
POST /api/v1/sync ─► commit to Postgres ─► publish to `sync_events` ─► set published_at
                                                   │
                                                   ▼
                              consumer group `sync_group` (src/kafka/consumer.py)
```

- **Commit first, publish second.** Kafka never carries an event that isn't in the
  database. If the publish fails, the row stays with `sync_events.published_at = NULL`.
- **Kafka down → API still answers.** The sync is saved and the API returns `500`
  (JSON, within `KAFKA_PUBLISH_TIMEOUT_MS`, default 5 s). `/health`, `GET /status`
  and the rest of the app keep working.
- **Recovery.** Retrying with the same `idempotency_key` publishes the saved event
  (still exactly one message). The producer reconnects automatically on the next
  request. Unkeyed syncs that failed to publish stay `pending` and can be found with
  `SELECT id FROM sync_events WHERE published_at IS NULL` (indexed). There is no
  automatic re-publisher for them yet (planned for Phase 4).
- **Ordering.** Messages are keyed by `entity_id`, so all events for one entity go to
  the same partition and are consumed in order.
- **Delivery.** The producer uses `acks=all` + idempotence. The consumer commits
  offsets *after* handling each batch (at-least-once), so handlers must be idempotent:
  `event_id` is stable per sync and is the de-duplication key.

### Message schema (JSON)

```json
{
  "event_id": "65442417773e56da9dab4d825a2a37ab",
  "event_type": "sync.created",
  "schema_version": 2,
  "sync_id": 2,
  "entity_id": "contact_demo",
  "source_system": "salesforce",
  "destination_system": "hubspot",
  "source_data": {"FirstName": "Ada", "Email": "ada@example.com"},
  "canonical_data": null,
  "idempotency_key": "demo-1",
  "timestamp": "2026-10-12T05:45:03.387724Z"
}
```

Defined once in `src/kafka/schemas.py` (`SyncEventMessage`) and shared by producer and consumer.
In v2 (Phase 3) the message carries the raw `source_data`; `canonical_data` is
computed by the processor and stored in the database, so it is `null` on the wire.

### Kafka setup

The broker auto-creates topics, but create `sync_events` explicitly so it has
3 partitions (room for 3 parallel consumers in Phase 3):

```bash
# create (safe to re-run)
docker exec sync-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic sync_events --partitions 3 --replication-factor 1

# verify it exists
docker exec sync-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec sync-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic sync_events

# list messages (Ctrl+C to stop; --timeout-ms exits when idle)
docker exec sync-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sync_events --from-beginning --property print.key=true --timeout-ms 5000
```

> Kafka prints a warning about topic names that contain `_` or `.`. It's harmless
> unless you also create a topic named `sync.events`.

If you already ran Phase 1, run `python scripts/setup_db.py` once to add the new
`published_at` column (it is idempotent and does not touch existing data).

### Inspecting the topic with the logging consumer

```bash
python -m src.kafka.consumer         # Ctrl+C to stop
```

This only logs events (`sync_event_consumed ...`). **It joins the same group,
`sync_group`, as the Phase 3 processor**, so while it runs it takes partitions away
from the processor and the events it reads are never processed. Use it only with
the processor stopped, or use `kafka-console-consumer` (no group) instead.
Check the group's position and lag:

```bash
docker exec sync-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group sync_group
```

### Manual verification

```bash
# 1. Start the app
uvicorn src.main:app --reload            # log shows: kafka_producer_started ...

# 2. Sync something (run it twice: the 2nd is a replay)
curl -s -X POST http://localhost:8000/api/v1/sync -H "Content-Type: application/json" \
  -d '{"entity_id":"contact_demo","source_system":"salesforce","destination_system":"hubspot",
       "data":{"FirstName":"Ada","Email":"ada@example.com"},"idempotency_key":"demo-1"}'

# 3. See exactly one event for demo-1 in Kafka
docker exec sync-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sync_events --from-beginning --timeout-ms 5000 | grep demo-1

# 4. Outage drill
docker stop sync-kafka
curl ... -d '{... "idempotency_key":"demo-2"}'      # → 500 "saved but not published"
curl http://localhost:8000/health                  # → 200, app still up
docker start sync-kafka                            # wait ~20 s for it to be healthy
curl ... -d '{... "idempotency_key":"demo-2"}'      # → 200 replay; event now published once
```

On PowerShell use `curl.exe` (not the `curl` alias), and put the JSON body in a
file (`-d "@body.json"`) to avoid quoting issues.

### Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Startup log: `Kafka unreachable at startup` | Broker not running: `docker compose up -d kafka`; check `docker compose ps` says *healthy*. The app reconnects by itself on the next sync. |
| `POST /sync` returns 500 "saved but not published" | Kafka is down or slow (> `KAFKA_PUBLISH_TIMEOUT_MS`). The sync is saved; retry with the same `idempotency_key` once Kafka is back. |
| `KafkaConnectionError: Unable to bootstrap from localhost:9092` | Port mismatch (`KAFKA_PORT` in `.env`), or another process holds 9092. The app must use `localhost:9092`; containers use `kafka[...]` |
| Connects, then errors mentioning host `kafka` | You're using the internal listener from the host. Use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. |
| `UnknownTopicOrPartitionError` right after Kafka restarts | Broker is still loading; wait for *healthy* and retry. |
| Consumer reads nothing | The group already committed past those messages. Check lag with `kafka-consumer-groups --describe`; replay with `kafka-consumer-groups --bootstrap-server localhost:9092[...]` |
| Kafka tests show as *skipped* | Broker unreachable; they skip instead of failing. Start Docker and re-run. |
| `column sync_events.published_at does not exist` | Phase 1 database: run `python scripts/setup_db.py`. |

## Background processing (Phase 3)

### Pipeline

```
POST /api/v1/sync ─► row status=pending ─► Kafka `sync_events` ─► SyncProcessor (group sync_group)
                                                                    │ load row by sync_id
                                                                    │ skip unless pending
                                                                    │ transform + validate
                                                                    ▼
                                    UPDATE ... SET status=synced|failed, canonical_data | error_message,
                                           processed_at, processing_latency_ms
                                    WHERE id = :sync_id AND status = 'pending'
                                    → commit DB, then commit Kafka offsets for the batch
```

The processor starts with the API (FastAPI lifespan) and runs independently of
requests. On shutdown it finishes the current batch, commits offsets, and leaves
the group before the producer and DB pool close. To run it as its own process
(e.g. to scale it separately in Phase 4):

```bash
RUN_SYNC_PROCESSOR=false uvicorn src.main:app      # API only
python -m src.workers.sync_processor               # processor only (Ctrl+C to stop)
```

Several processor instances can run at once. Kafka splits the 3 partitions of
`sync_events` between them.

Within one instance, each polled batch is processed concurrently by entity:
different `entity_id`s run in parallel (up to `SYNC_PROCESSOR_CONCURRENCY`,
default 10), while events for the same entity run one after another in publish
order. The DB pool is shared with the API, so keep the concurrency below `DB_POOL_SIZE`.

### Canonical schema (v2)

| Field | Type | Rules |
|---|---|---|
| `entity_id` | string | required |
| `name` | string | required, non-empty, ≤ 255 chars |
| `email` | string | null | if present, must look like an address |
| `status` | string | null | ≤ 50 chars |
| `metadata` | object | every source field that was not mapped (keys snake_cased) |

Plus `schema_version`, `source_system` and `transformed_at`.

| Source | `name` from | `status` from |
|---|---|---|
| salesforce | `account_name`, else `name`, else `first_name` + `last_name` | `account_status`, else `status` |
| hubspot | `firstname` + `lastname`, else `name` | `hs_lead_status` |
| stripe | `customer_name`, else `name` | `status` |
| custom | `name` (passed through as-is; other keys go to `metadata` unchanged) | `status` |

`email` comes from `email` for all systems. Source keys are matched after
snake_case normalisation (`AccountName` = `account_name`). Mappings live in
`MAPPINGS` in `src/services/transformer.py`, and rules in `src/services/validator.py`.
To add a system, add a mapping and a value to `IntegrationSystem`.

### Error handling

| What goes wrong | What happens |
|---|---|
| Data fails validation (missing/null/blank name, wrong type, bad email) | Row → `failed`, `error_message` lists **every** problem; next message is processed |
| Unexpected transformer exception | Row → `failed` (`internal transformation error: ...`), logged with traceback |
| Message isn't valid JSON / schema | Logged (`kafka_message_skipped`) and skipped; offset still committed |
| Row for `sync_id` doesn't exist (DB reset) | Logged (`sync_process_missing`) and skipped |
| Same message delivered twice | Second delivery is a no-op (`sync_process_skipped reason=already_synced`) |
| Database error | Retried 3× with backoff. If it persists the batch is **not** committed; the consumer restarts with backoff (1 s → 30 s) and the batch is redelivered |
| Kafka down | Processor keeps retrying with backoff; API unaffected |
| Process killed mid-batch | Uncommitted batch is redelivered on restart; rows already done are skipped |

`failed` is terminal for the processor: it never retries a failed sync on its own.
To retry after fixing the data, send a new sync, using a new `idempotency_key`
because the old key replays the old result.

### Metrics

`GET /health/processor` returns (per process, since startup; numbers illustrative):

```json
{"running": true, "processed": 100, "synced": 100, "failed": 0, "duplicates_skipped": 0,
 "missing": 0, "db_errors": 0, "restarts": 0, "error_rate": 0.0,
 "latency_ms": {"samples": 100, "p50": 4.1, "p95": 9.8, "p99": 14.2}}
```

Per row, `processing_latency_ms` (Kafka message handled → DB committed) and
`processed_at` are stored, so latency can also be queried in SQL. Logs are
`key=value` (`sync_processed outcome=synced sync_id=... latency_ms=...`), ready to
be parsed or replaced by Prometheus counters in Phase 4.

### Upgrading from Phase 2

```bash
python scripts/setup_db.py      # adds processed_at, processing_latency_ms (idempotent)
```

Rows created before Phase 3 are already `synced` and are skipped if redelivered.
Messages published by Phase 2 (schema v1) still work: the processor reads the row
from the database, not the message body.

### Manual verification (Phase 3)

```bash
# 1. Start everything
docker compose up -d && python scripts/setup_db.py
uvicorn src.main:app                 # log: "Background sync processor started",
                                     #      kafka_consumer_started topic=sync_events group_id=sync_group

# 2. Valid sync -> accepted as pending
curl -s -X POST http://localhost:8000/api/v1/sync -H "Content-Type: application/json" \
  -d '{"entity_id":"acct_demo","source_system":"salesforce","destination_system":"hubspot",
       "data":{"AccountName":"Acme Corp","AccountStatus":"Active","Industry":"Mfg"},
       "idempotency_key":"p3-demo-1"}'
# → {"status":"pending", ...}

# 3. Event is in Kafka
docker exec sync-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic sync_events --from-beginning --timeout-ms 5000 | grep p3-demo-1

# 4. Processor picked it up (app log)
#    sync_processed outcome=synced sync_id=... entity_id=acct_demo salesforce->hubspot latency_ms=...

# 5. Synced, with canonical data
curl -s http://localhost:8000/api/v1/status/acct_demo
# → "status":"synced", "canonical_data":{"name":"Acme Corp","status":"Active",
#    "email":null,"metadata":{"industry":"Mfg"},...}, "processing_latency_ms":...

# 6. Same row in Postgres
docker exec sync-postgres psql -U sync_user -d sync_db -c \
  "SELECT id, status, canonical_data->>'name' AS name, processing_latency_ms
     FROM sync_events WHERE entity_id = 'acct_demo';"

# 7. Invalid data -> stored as failed, with the reason
curl -s -X POST http://localhost:8000/api/v1/sync -H "Content-Type: application/json" \
  -d '{"entity_id":"acct_bad","source_system":"stripe","destination_system":"hubspot",
       "data":{"email":"not-an-email"}}'
curl -s http://localhost:8000/api/v1/status/acct_bad
# → "status":"failed", "error_message":"'name' is missing (stripe expects one of:
#    customer_name, name); 'email' is not a valid email address: 'not-an-email'"

# 8. Metrics
curl -s http://localhost:8000/health/processor

# 9. Consumer group: lag should be 0
docker exec sync-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group sync_group
```

### Load test

With the app running (step 1 above):

```bash
python scripts/load_test_phase3.py --mode processor        # 100 events straight into Kafka
python scripts/load_test_phase3.py --mode processor -n 1000
python scripts/load_test_phase3.py                         # 100 events through POST /api/v1/sync
python scripts/load_test_phase3.py -n 1000 -c 50
```

- `--mode processor` inserts `pending` rows and publishes their events directly,
  then times how fast the running app's processor drains them. This measures the
  Phase 3 pipeline (Kafka consume → transform → DB update) on its own.
- `--mode api` (default) goes through HTTP, so it measures the API too.

Both use all four source systems, wait until every row has left `pending`, and
print throughput plus p50/p95/p99. They exit non-zero if anything failed or
throughput is below 50 events/s.

Measured on the development laptop (Windows 11, Docker Desktop, single app instance):

| Mode | Events | Throughput | Processing latency p50 / p95 / p99 |
|---|---|---|---|
| processor | 100 (×3 runs) | 114–119 events/s, ~0.85 s per 100 | 38–40 / 52–60 / 55–64 ms |
| processor | 1000 | 118 events/s | 42 / 61 / 88 ms |
| api | 100 | 15 events/s | 14 / 337 / 348 ms |

The API number is limited by HTTP on that machine, not by this app: a bare,
no-op ASGI app on the same laptop also manages only 29–43 req/s under concurrency.
Expect much higher on Linux. Phase 4 should re-measure in the deployment environment.

### Troubleshooting (Phase 3)

| Symptom | Cause / fix |
|---|---|
| Syncs stay `pending` | Processor not running. Check `GET /health/processor` → `running`; check `RUN_SYNC_PROCESSOR` isn't `false`; look for `sync_processor_crashed` in the log. |
| Some syncs stay `pending`, `published_at` is null | They were never published (Kafka was down and the client didn't retry). See "Recovery" above. |
| Syncs stay `pending` while `python -m src.kafka.consumer` runs | The logging consumer is in the same group and took the partitions. Stop it. Events it already read are lost to the processor. Re[...] |
| Log repeats `sync_processor_crashed ... restart_in_s=...` | Kafka or the DB is unreachable. The processor backs off up to 30 s and recovers on its own once the dependency is back. Nothing is co[...] |
| `sync_process_missing` warnings | Kafka has events for rows that no longer exist (DB was reset with `--reset`). Harmless. To clear them, reset `sync_group` offsets `--to-latest`. |
| Many `failed` syncs | Read `error_message`. Usually the source sends fields under different names than the mapping expects (see the table above). |
| `column sync_events.processed_at does not exist` | Run `python scripts/setup_db.py`. |
| Load test: `background processor is not running` | Start the app without `RUN_SYNC_PROCESSOR=false`, or run `python -m src.workers.sync_processor` alongside it. |
| Load test: `N of M still pending after 60s` | The processor is behind or stopped. Check `/health/processor` and the log. Raise `--timeout` for big runs. |
| `QueuePool limit ... reached` / timeouts under load | `SYNC_PROCESSOR_CONCURRENCY` plus API concurrency exceeds the DB pool. Raise `DB_POOL_SIZE` or lower the concurrency. |

## Monitoring (Phase 4)

```bash
docker compose --profile monitoring up -d       # Jaeger, Prometheus, Grafana
TRACING_ENABLED=true uvicorn src.main:app --host 0.0.0.0 --port 8000
python scripts/load_test_phase3.py -n 200       # generate some traffic
```

| UI | URL | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | Anonymous view enabled; admin login `admin` / `admin` (`GRAFANA_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:9090 | Status → Targets shows `local-dev` UP |
| Jaeger | http://localhost:16686 | Service `sync-platform` |

Use `--host 0.0.0.0` so the Prometheus container can scrape the app via
`host.docker.internal:8000`. The `container` target only comes up when the app runs
in Docker (`--profile app`). Until then it shows as DOWN, which is harmless.

### Metrics (`GET /metrics`)

Defined in `src/observability/metrics.py`. All labels come from fixed sets, so
cardinality is bounded: no entity IDs or error text.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `sync_events_total` | counter | `status` (accepted/synced/failed), `source_system` | Lifecycle events |
| `sync_latency_seconds` | histogram | `source_system` | End to end: row created → processed |
| `sync_api_latency_seconds` | histogram | `status` (accepted/replayed/error) | POST /api/v1/sync server time |
| `sync_processing_latency_seconds` | histogram | `outcome` | Processor: message handled → DB commit |
| `sync_transform_latency_seconds` | histogram | `source_system` | Transform + validate only |
| `sync_kafka_publish_latency_seconds` | histogram | `result` (ok/error) | Producer send → broker ack |
| `processor_concurrent_syncs` | gauge | | Syncs in flight in the processor |
| `processor_batch_size` | gauge | | Messages in the current Kafka batch |
| `processor_running` | gauge | | 1 while the processor task is alive |
| `errors_total` | counter | `type` | validation, transform_crash, kafka_publish, database, malformed_message, missing_row, processor_crash |

`/metrics` is unauthenticated, which is fine for a local demo. In production,
expose it only on the private network or behind the reverse proxy.

### Grafana dashboards

Provisioned automatically into the **Sync Platform** folder. Nothing needs importing.

| File | Dashboard | Panels |
|---|---|---|
| `monitoring/grafana_dashboard_overview.json` | System Overview | Syncs accepted, success rate, processor concurrency, processor up/down, throughput, concurrency & batch size, error rate (24h), [...] |
| `monitoring/grafana_dashboard_performance.json` | Performance | API / processor / Kafka / end-to-end p50-p95-p99, Kafka publish heatmap, transform p95 by system, throughput |
| `monitoring/grafana_dashboard_errors.json` | Errors | Error rate by type, failed syncs by source system, **top 10 error messages** and recent failures (from Postgres) |

The dashboards are generated by `scripts/build_dashboards.py`. Edit that file
and re-run it, rather than editing the JSON by hand. To import them into some
other Grafana instead: Dashboards → New → Import → upload the JSON. It needs a
Prometheus datasource with uid `prometheus`, and for the error-message tables a
PostgreSQL datasource with uid `syncdb`. Both are provisioned by
`monitoring/grafana/provisioning/`.

Error *messages* come from Postgres (`sync_events.error_message`) rather than
from Prometheus, because free-text labels would explode Prometheus' series count.

### Tracing (Jaeger)

Set `TRACING_ENABLED=true`. Spans go out over OTLP/HTTP to
`OTLP_TRACES_ENDPOINT` (default `http://localhost:4318/v1/traces`).

> The spec asked for the Jaeger agent on UDP 6831. That agent (Jaeger-Thrift
> over UDP) no longer exists in Jaeger v2, and OpenTelemetry has deprecated its
> exporter. OTLP is Jaeger's native protocol now. The UI and the traces you see
> are the same.

One trace covers the HTTP request *and* the asynchronous processing, because
the API puts W3C trace context into the Kafka message headers. A real trace from
this machine:

```
POST /api/v1/sync                          79.4 ms   FastAPI server span
  fastapi.endpoint                         58.1 ms
    sync.accept                            57.9 ms   entity, systems, sync_id attributes
      db.persist                           22.6 ms   insert + idempotency record + commit
      kafka.publish                         4.7 ms   producer span
        kafka.serialize                     0.1 ms
        kafka.send                          4.3 ms   network send -> broker ack
        processor.consume                  41.3 ms   consumer span, continues the trace
          db.load                          31.4 ms
          transform                         0.1 ms
            validate                        0.0 ms
          db.update                         8.7 ms
      db.mark_published                    19.5 ms
```

Failed validations mark the `transform` span as an error and record
`sync.validation_errors`. In Jaeger, search by tag `sync.entity_id=<id>` to find
the trace for one sync.

## Docker image

```bash
docker build -t sync-platform .
docker compose --profile app up -d app     # runs it against the compose Postgres/Kafka on :8001
```

- `python:3.14-slim`, runs as a non-root user (uid 10001), and has a `HEALTHCHECK` on `/health`.
- `docker/entrypoint.sh` runs the idempotent schema migration and topic creation
  before starting uvicorn (on `$PORT`, default 8000).
  - It writes Kafka client certificates from `KAFKA_SSL_*_PEM` env vars to files,
    since cloud platforms store secrets as env vars.
  - It turns Render's `postgres://` URL into `postgresql+asyncpg://`.
- The same image runs a standalone processor:
  `docker run ... sync-platform python -m src.workers.sync_processor`.
- `requirements.txt` pins the exact versions the test suite passed with.

I verified the image locally: it reached `healthy`; `/health`, `/health/ready`,
`POST /api/v1/sync`, `GET /status/{id}` (→ `synced`) and `/metrics` all
responded; it runs as `uid=10001(app)`; and the PEM-to-file handling and URL
rewrite in the entrypoint worked.

## Cloud deployment (Render + Aiven)

**Status: the deployment configuration is ready, but the service has not been
deployed.** Deploying needs your GitHub, Render and Aiven accounts. There is no
live URL yet. Fill it in below once you deploy.

Render runs the API and Postgres, and Aiven provides Kafka (Render has no Kafka, and
Aiven's free Kafka tier needs no credit card). As of 2026, the free tiers come with these limits:

- **Render web service (free):** 0.1 CPU and 512 MB. It sleeps after 15 min with no
  traffic, and the next request takes about 1 minute to wake it. Background workers have
  no free plan, so the processor runs inside the web service.
- **Render Postgres (free):** 1 GB, and it **expires 30 days after creation**. Upgrade or
  recreate it after that.
- **Aiven Kafka (free):** max 5 topics × 2 partitions, about 250 KiB/s, fixed retention.
  It powers off when idle; turn it back on in the console.

Steps:

1. **Push to GitHub.**
   ```bash
   git init && git add . && git commit -m "Real-time data sync platform"
   git remote add origin https://github.com/<you>/realtime-sync-platform.git
   git push -u origin main
   ```
   `.gitignore` already excludes `.env` and certificate files.
2. **Kafka on Aiven.** Create a free account → *Create service* → Apache Kafka → *Free* plan.
   - When it's running, go to *Overview* → *Connection information*. Note the *Service URI*
     (`host:port`) and download **ca.pem**, **service.cert** and **service.key`**.
   - Under *Topics* → *Add topic*, create `sync_events` with 2 partitions. The app also
     tries to create it at startup.
3. **Render blueprint.** In the Render dashboard: *New* → *Blueprint* → choose the repo.
   Render reads `render.yaml` and asks for the secret values:
   - `KAFKA_BOOTSTRAP_SERVERS` = the Aiven service URI
   - `KAFKA_SSL_CA_PEM`, `KAFKA_SSL_CERT_PEM`, `KAFKA_SSL_KEY_PEM` = the *full contents*
     of the three files, including the `-----BEGIN ...` lines
   - `OTLP_TRACES_ENDPOINT` = leave empty (tracing is off in the cloud)
4. **Deploy.** Render builds the Dockerfile, creates the database, and starts the
   service. In the log, look for `Database ready`, `Topic 'sync_events' ...`,
   `kafka_consumer_started` and `Uvicorn running`.
5. **Verify** (replace the URL):
   ```bash
   URL=https://sync-platform-api.onrender.com
   curl -s $URL/health                                   # {"status":"healthy"}
   curl -s $URL/health/ready                             # database ok
   curl -s -X POST $URL/api/v1/sync -H "Content-Type: application/json" \
     -d '{"entity_id":"cloud_demo","source_system":"salesforce","destination_system":"hubspot",
          "data":{"AccountName":"Acme Corp","AccountStatus":"Active"}}'
   curl -s $URL/api/v1/status/cloud_demo                 # "status":"synced", canonical_data
   curl -s $URL/metrics | grep sync_events_total
   curl -s $URL/health/processor
   ```
6. **Load test it:**
   `python scripts/load_test_cloud.py --url $URL --label "Render free" --full --json docs/results/load_render.json`.
   Then paste the table into "Performance" below.

**Live URL:** _not deployed yet: add it here after step 4._

Deployment troubleshooting:

| Symptom | Fix |
|---|---|
| Logs: `cannot reach Kafka ... SSL` / handshake errors | Check that the three PEM vars hold the whole file contents, and that `KAFKA_SECURITY_PROTOCOL=SSL`. |
| `TopicAuthorizationFailed` / topic missing | Create `sync_events` in the Aiven console (2 partitions). |
| First request takes ~1 min | Free instance waking up. The load test's warm-up handles it. |
| `remaining connection slots are reserved` | Free Postgres has few connections. Keep `DB_POOL_SIZE` small (5). |
| DB gone after a month | Render's free Postgres expired after 30 days. Recreate it, or move to a paid plan. |

## Performance

Everything here was measured on the development laptop: Windows 11, Docker Desktop,
a single app instance, and `SYNC_PROCESSOR_CONCURRENCY=10`. **No cloud numbers yet**,
because the service hasn't been deployed (see above).

| Measurement | Throughput | Latency p50 / p95 / p99 |
|---|---|---|
| Processor only: 100 events published straight to Kafka (`load_test_phase3.py --mode processor`) | **114–119 events/s** | 38–40 / 52–60 / 55–64 ms (Kafka consume → DB update) |
| Processor only: 1000 events | 118 events/s | 42 / 61 / 88 ms |
| Same, after Phase 4 instrumentation (metrics + tracing exported to Jaeger) | 107 events/s (100), 113 events/s (1000) | 42–43 / 64–71 / 74–76 ms |
| API from the Windows host → container, 1 request at a time | 15.9 req/s | 58 / 90 / 96 ms |
| API from the Windows host → container, 10 at a time | 34.8 req/s | 246 / 501 / 623 ms |
| API from the Windows host → container, 50 at a time (1000 requests) | 19.7 req/s | 1747 / 6845 / 10818 ms |
| API from **inside the Linux container** (no Windows networking), 1 / 10 / 50 at a time | **52 / 100 / 114 req/s** | p50 18 / 98 / 415 ms |
| Server-side processing during those runs (`/health/processor`) | | 10.8 / 19.0 / 24.2 ms |

How to read this table:

- The processor isn't what limits throughput. It handles over 110 events/s, and
  each event takes about 11 ms server-side.
- Full instrumentation with every span exported costs about 5% throughput
  (118 → 113 events/s for 1000 events). One 100-event run reached 281 events/s,
  an outlier I didn't use.
- The Windows-host numbers are capped by the Windows ↔ Docker Desktop network
  path. A *no-op* ASGI app reached through the same path also manages only 29–43 req/s.
  The same container, called from inside its own Linux network namespace, serves
  52–114 req/s with zero errors.
- On a Render free instance, expect lower numbers again: it has 0.1 CPU against the
  laptop's 16 cores. Run `load_test_cloud.py` there and add a column here.
  Don't assume the cloud will be faster.

Raw results: `docs/results/load_local_container.json`.

## Deployed API Testing & Performance Results

The deployed API was validated using PowerShell HTTP requests and [k6](https://grafana.com/docs/k6/latest/) to evaluate endpoint availability, database readiness, background processing, asynchronous synchronization, and idempotency.

**Test environment:** Deployed Render API  
**Load-test duration:** 30 seconds per run  
**Load generator:** k6  
**Test type:** Controlled baseline testing against the live API

### Performance benchmarks

#### Health endpoint — `GET /health`

| Virtual Users | Duration | Requests | Avg. Latency | p95 Latency | HTTP Failures |
|---:|---:|---:|---:|---:|---:|
| 5 | 30 s | 123 | 245.76 ms | 250.80 ms | 0% |
| 10 | 30 s | 247 | 242.97 ms | 255.54 ms | 0% |
| 20 | 30 s | 495 | 243.53 ms | 272.42 ms | 0% |

All three runs completed with zero HTTP request failures. The p95 response time remained below 273 ms across the tested concurrency levels.

#### Database-backed status endpoint — `GET /api/v1/status/{entity_id}`

| Virtual Users | Duration | Requests | Avg. Latency | p95 Latency | HTTP Failures |
|---:|---:|---:|---:|---:|---:|
| 5 | 30 s | 120 | 262.04 ms | 294.52 ms | 0% |

All 120 requests returned HTTP 200, and all checks confirmed that the target entity remained in the `synced` state.

### Functional verification

| Test | Result |
|---|---|
| API liveness — `GET /health` | Passed — HTTP 200 |
| Database readiness — `GET /health/ready` | Passed — HTTP 200; database reported `ok` |
| Background processor health | Passed — processor reported running |
| Valid sync submission | Passed — event accepted with initial status `pending` |
| Asynchronous sync completion | Passed — event reached `synced` |
| Database-backed status lookup | Passed — HTTP 200; canonical data returned |
| Idempotency replay | Passed — duplicate request returned `idempotent_replay: true` and reused sync ID `3` |
| Processor metrics after sync | Passed — 1 processed, 1 synced, 0 failed, 0 database errors at observation time |

### Methodology and limitations

- The k6 health and status tests used 5, 10, and 20 virtual users as configured above.
- Each virtual user waits one second between iterations. Therefore, the observed request rates represent this test configuration, not the maximum capacity of the service.
- These results are baseline measurements from the deployed environment, not a maximum-throughput benchmark or a guarantee of production-scale reliability.
- The synchronization test used a synthetic record and verified its transition from `pending` to `synced`. A repeated request with the same idempotency key returned the original sync ID.
- Latency measurements include network and hosting overhead; they should not be interpreted as database-only or Kafka-only latency.

### Test evidence

Screenshots of the test outputs are maintained with the project documentation. They include k6 benchmark summaries, API health checks, sync completion, idempotency replay, and processor metrics.

## Data model

- `sync_events`: raw `source_data`, `canonical_data`, `status` (`pending` → `synced`/`failed`),
  `error_message`, API `latency_ms`, `published_at`, `processed_at`, `processing_latency_ms`
- `idempotency_records`: key, request fingerprint (SHA-256), cached result
- `conflict_records`: field-level conflicts between systems (used in later phases)

## Development

```bash
pytest tests/ -p no:cacheprovider              # all tests (Kafka tests need docker compose up)
pytest tests/ -p no:cacheprovider -m "not kafka"   # skip real-Kafka tests
pytest tests/ -p no:cacheprovider -m kafka         # only real-Kafka tests
pytest tests/ -p no:cacheprovider -m jaeger        # only the Jaeger integration test
ruff check . && ruff format --check .
mypy src tests scripts                          # strict
```

Kafka tests skip if the broker isn't reachable, and the Jaeger test skips if
`--profile monitoring` isn't running.

`-p no:cacheprovider` is needed on OneDrive, which blocks `.pytest_cache`.

Real-Kafka tests each create a throwaway topic (`test_sync_events_<random>`) and
consumer group and delete the topic afterwards, so they never touch `sync_events`.

## Project layout

```
src/
  main.py            app, lifespan (DB check, producer + processor start/stop), middleware
  config.py          settings from env / .env
  database.py        async engine, session dependency, init_db
  kafka/
    schemas.py       SyncEventMessage: the JSON wire format
    producer.py      SyncEventProducer (aiokafka), FastAPI dependency get_producer()
    consumer.py      SyncEventConsumer (group sync_group); `python -m src.kafka.consumer`
    observability/
      metrics.py       Prometheus metrics (served at /metrics)
      tracing.py       OpenTelemetry setup, OTLP export, Kafka header propagation
  models/            SQLAlchemy ORM models
  schemas/           Pydantic request/response models
  routes/            health (+ /health/processor metrics) and sync endpoints
  services/
    sync_service.py  accept + publish (idempotency)
    transformer.py   per-system field mappings -> canonical record
    validator.py     canonical schema rules, TransformationError
  workers/
    sync_processor.py  background Kafka -> transform -> DB worker; `python -m src.workers.sync_processor`
scripts/
  setup_db.py          create / upgrade / reset the schema
  create_topic.py      ensure the sync_events topic exists (local or managed Kafka)
  load_test_phase3.py  N-event throughput + latency percentiles (needs DB access)
  load_test_cloud.py   HTTP-only load test for any URL (deployed instance)
  build_dashboards.py  generates the Grafana dashboard JSON
monitoring/
  grafana_dashboard_*.json   the three dashboards
  prometheus.yml             scrape config
  grafana/provisioning/      datasources (Prometheus, Postgres) + dashboard loader
docker/entrypoint.sh   migrate, create topic, write Kafka certs, start
Dockerfile, render.yaml, requirements*.txt
tests/
  conftest.py              DB, fake publisher, per-test Kafka topic + producer fixtures
  test_sync_api.py         API + idempotency + publishing + end-to-end through Kafka
  test_kafka_producer.py   serialization, error handling, real delivery
  test_kafka_consumer.py   init, malformed messages, round trip, offset management
  test_transformer.py      every system's mapping, validation errors, edge cases
  test_sync_processor.py   processing, skip/duplicate, retries, stats (no Kafka)
  test_end_to_end.py       POST -> Kafka -> processor -> DB, crash/restart, poison messages
  test_observability.py    metrics move correctly, /metrics output, span hierarchy
  test_tracing_jaeger.py   POST -> trace visible in a real Jaeger
  test_config.py           Kafka PLAINTEXT / SSL / SASL client options
```
