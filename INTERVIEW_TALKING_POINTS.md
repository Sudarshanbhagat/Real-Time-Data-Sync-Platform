# Interview talking points: real-time data sync platform

Every number below was measured during development, and the README's
*Performance* section has the raw data. If a number isn't here, I don't claim it.

---

### Q1. "Tell us about this project."

Companies keep the same customer in Salesforce, HubSpot and Stripe, and the
copies drift. I built a platform that takes a change from any of those systems,
records it durably, streams it through Kafka, and turns it into one canonical
record. The pipeline is a FastAPI service, then PostgreSQL, then Kafka, then an
async processor writing back to PostgreSQL. It's built to survive failures: a
Kafka outage, a database blip, a worker killed mid-batch, bad input data. None
of those lose data or apply a change twice.

One instance processes about 118 events per second, at p99 under 90 ms from
consume to database write. It has Prometheus metrics, Grafana dashboards, and
end-to-end tracing in Jaeger. There are 91 tests, including integration tests
against real Kafka and Jaeger.

### Q2. "How would you scale this 10x?"

First I'd find the bottleneck rather than guess. I measured it: the processor
spends about 11 ms per event, and the database round-trips dominate, not the
Python code.
1. **More partitions, more consumers.** Throughput scales with partitions. Each
   processor instance in `sync_group` takes a share. It's already a separate
   entry point (`python -m src.workers.sync_processor`), so it can be deployed
   and autoscaled on consumer lag, independently of the API.
2. **Fewer round-trips per event.** Batch the reads (`WHERE id IN (...)`) and
   the updates for each Kafka batch, and keep the conditional `status='pending'` guard.
3. **The database.** Use a connection pooler (PgBouncer), size the pool to the
   concurrency, and partition `sync_events` by time once it gets large.
4. **The API.** Run several uvicorn workers or replicas behind a load balancer.
   It's stateless, so that's easy.

Ordering still holds at 10x, because the partition key is `entity_id`.

### Q3. "What's the hardest problem you solved?"

**Recovering after a Kafka outage.** I tested it by stopping the broker while
the app was running. The API failed the way I designed it to: a 500, with the
sync saved. But after Kafka came back, every publish kept failing with
`OutOfOrderSequenceNumber`. The cause: a send that timed out stays queued
inside the idempotent producer, so its sequence numbers no longer match the
broker's, and that client never recovers. The fix is to throw away the
producer after any failed publish and reconnect fresh, which gives it a new
producer ID. I only found this because I ran the outage for real instead of
just mocking it.

*Backup story, if they want performance:* the API load test was stuck at about
15 req/s. Rather than optimise blindly, I benchmarked a no-op web app on the
same laptop and got the same ceiling. So the limit was the Windows/Docker
network path, not my code. From inside the container, the same image served 114 req/s.

### Q4. "How do you ensure exactly-once semantics?"

I want to be precise here: it's **at-least-once delivery plus idempotent
processing**, which gives *effectively-once* results. It isn't Kafka's
transactional exactly-once.
- **Client retries:** an `idempotency_key` plus a SHA-256 fingerprint of the
  request. A retry replays the stored result and publishes no second message.
  Reusing a key with a different body gets a 422.
- **Producer:** `acks=all` with `enable_idempotence`, so the broker drops
  duplicates caused by the client's internal retries.
- **Consumer:** offsets are committed only after the batch is in the database.
  The write is `UPDATE … WHERE id=:id AND status='pending'`, so a redelivered
  message (from a crash, a rebalance, or two workers) changes nothing.
- **Proof:** a test processes half a batch, kills the worker without
  committing, and restarts it. All four rows end up `synced`, and the two
  already-processed rows keep their original `processed_at`.

Why not Kafka transactions? The side effect is a Postgres write, and Kafka
transactions don't cover Postgres. An idempotent write is simpler and correct.

### Q5. "Describe the architecture."

There are two halves, connected by Kafka.

**API side** (`POST /api/v1/sync`):
1. Validate the request.
2. In one transaction, insert the row as `pending`, together with its
   idempotency record.
3. Commit.
4. Publish to `sync_events`, keyed by `entity_id`.
5. Set `published_at`.

The order matters: commit first, so Kafka never holds an event the database doesn't have.

**Processor side** (consumer group `sync_group`):
1. Poll a batch.
2. Group the messages by entity, and process different entities concurrently,
   up to 10 at a time.
3. For each message: load the row, skip it unless it's `pending`, transform it
   with that system's field mapping, validate it, and do the conditional update.
4. Commit the offsets after the whole batch.

**Observability:**
- Prometheus counters and histograms for every stage.
- OpenTelemetry spans, with the trace context passed in Kafka headers. So one
  Jaeger trace shows the HTTP request *and* the async processing.

**Deployment:** one Docker image. On Render plus Aiven Kafka, the entrypoint
migrates the schema, creates the topic, and writes the SSL client certificates
from secrets.

### Q6. "How do you handle failures?"

| Failure | Behaviour |
|---|---|
| Kafka down during a POST | Row saved, API returns 500 within the 5 s timeout. A retry with the same key publishes it exactly once, and the producer reconnects by itself |
| Invalid source data | Row marked `failed` with *every* problem listed. The next event is processed normally |
| Poison Kafka message | Logged, counted in `errors_total{type="malformed_message"}`, skipped, offset committed |
| Database blip | Retried 3× with backoff. If it persists, the batch isn't committed, the consumer restarts with backoff (1 s up to 30 s), and the batch is redelivered |
| Worker crash mid-batch | Uncommitted batch redelivered. Finished rows are skipped by the `pending` guard |
| Transformer bug (exception) | That sync is marked `failed` with the error, so the topic doesn't stall |

Every case has a test, and the Kafka outage was also tested live.

### Q7. "What's one decision you'd change?"

I'd use a **transactional outbox** instead of commit-then-publish. Right now,
if Kafka is down and the client *didn't* send an idempotency key, the row is
saved but nothing publishes it later. It sits `pending` with
`published_at IS NULL` (it's indexed, so it's easy to find, but nothing retries
it automatically). With an outbox, the event row is written in the same
transaction as the sync, and a relay publishes it. Then nothing can be
stranded, whatever the client does. I'd also run the processor as its own
service from the start. It sits inside the web process only because Render's
free tier has no background workers.

### Q8. "How do you measure success?"

- **Correctness:** no lost or duplicated syncs, checked by tests for crashes,
  redelivery and duplicate keys. Success rate is shown on the Grafana overview.
- **Latency:** p50/p95/p99 at each stage (API, Kafka publish, processing, end
  to end) from Prometheus histograms.
- **Throughput:** events/s accepted vs processed. If the two diverge, consumer
  lag is growing.
- **Errors by type:** validation, database, Kafka, malformed message, and so
  on. The *top error messages* table comes from Postgres. The text deliberately
  isn't stored in Prometheus, because free-text labels would explode cardinality.
- **Engineering hygiene:** 91 tests, mypy `--strict` and ruff kept clean
  through all four phases.

### Q9. "Show us the code." (what to open, in order)

1. `src/services/sync_service.py`: idempotency, commit-then-publish, the replay
   path that repairs a failed publish.
2. `src/workers/sync_processor.py`:
   - `handle_batch`: per-entity ordering with concurrency.
   - `_process`: the conditional `UPDATE … WHERE status='pending'`.
   - `run_forever`: restart with backoff.
3. `src/kafka/producer.py`: `_discard()` and the comment explaining the
   `OutOfOrderSequenceNumber` fix.
4. `src/services/transformer.py`: per-system mappings as data (`MAPPINGS`),
   validation that reports every error.
5. `tests/test_end_to_end.py::test_e_crash_before_commit_reprocesses_idempotently`.

### Q10. "Demo it." (about 5 minutes)

```bash
docker compose --profile monitoring up -d
TRACING_ENABLED=true uvicorn src.main:app --host 0.0.0.0 --port 8000
```
1. **Happy path:** POST a Salesforce account, then `GET /api/v1/status/<id>`.
   It shows `synced` with `canonical_data` (`name`, `status`, `metadata`).
2. **Bad data:** POST a Stripe record with a bad email. It shows `failed`, with both problems listed.
3. **Idempotency:** POST the same key three times. You get one row and one Kafka
   message, with `Idempotent-Replayed: true` on the replays.
4. **Load:** `python scripts/load_test_phase3.py --mode processor -n 500` → about 115 events/s.
5. **Grafana** (localhost:3000): throughput, success rate, and p99 on the
   performance dashboard. The errors dashboard shows the top error messages.
6. **Jaeger** (localhost:16686): open one trace and walk the spans, from
   `POST /api/v1/sync` through `kafka.publish` to `processor.consume`.
7. **Outage (optional):** `docker stop sync-kafka`, then POST. You get a 500 but
   `/health` stays at 200. Run `docker start sync-kafka` and retry: it returns
   200 and publishes once.

---

## Resume bullets

- Built a real-time data sync platform (Python, FastAPI, Kafka, PostgreSQL). It
  normalises Salesforce, HubSpot and Stripe records into one canonical schema,
  at **~118 events/s per instance, p99 < 90 ms** from Kafka consume to database write.
- Got **effectively-once processing on at-least-once Kafka delivery**, using
  idempotency keys, an idempotent producer, and conditional state-transition
  writes. It's proven by crash-and-redelivery integration tests. Per-entity
  ordering with parallel processing roughly doubled throughput (62 → 118 events/s).
- Instrumented the full pipeline with **Prometheus, Grafana and OpenTelemetry/Jaeger**:
  one trace runs across the HTTP request and the async Kafka consumer. Shipped
  it as a non-root Docker image with a Render + Aiven deployment blueprint, and
  91 tests with mypy `--strict`.

(After you deploy, you can add: "deployed on Render + Aiven Kafka, live demo at …".)
