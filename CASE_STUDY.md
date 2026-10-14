# Case study: a real-time data sync platform

**Links:** GitHub: `<add your repo URL>` · Live demo: `<add your Render URL once deployed>`

## The problem

Companies keep the same customer in several systems: Salesforce for sales,
HubSpot for marketing, Stripe for billing. Each system names fields its own way
(`AccountName`, `firstname`, `customer_name`). When they drift apart, people fix
records by hand, reports disagree, and customers get the wrong email or invoice.

Point-to-point integrations make this worse as systems are added: N systems
need up to N×(N−1) mappings, each with its own retry and error handling.

> If you quote a dollar cost for data silos (the brief suggested "$500K+"),
> cite the source you got it from. I haven't measured or sourced one here.

## What I built

A service that accepts a change from any source system, records it durably,
streams it through Kafka, and turns it into one **canonical record**. Each
destination then needs a single mapping: canonical → itself.

```
            ┌──────────── API (FastAPI) ────────────┐
client ──►  │ validate → commit row (pending)        │ ──► Kafka `sync_events`
            │ → publish event → mark published       │     (keyed by entity_id)
            └────────────────────────────────────────┘              │
                                                                     ▼
            ┌───────── Background processor ─────────┐   consumer group
PostgreSQL ◄│ load row → skip unless pending          │◄── `sync_group`
            │ → transform + validate → conditional    │
            │   UPDATE … WHERE status='pending'       │
            │ → commit DB, then commit Kafka offsets  │
            └─────────────────────────────────────────┘
       Prometheus /metrics · OpenTelemetry → Jaeger · Grafana dashboards
```

**Tech:** Python 3.14, FastAPI, SQLAlchemy 2 (async) + asyncpg, PostgreSQL 15,
Kafka (aiokafka), Pydantic v2, prometheus-client, OpenTelemetry → Jaeger, Grafana,
Docker, Render + Aiven (deployment config). Tooling: pytest (91 tests: unit tests plus
integration tests against real Kafka and Jaeger), ruff, mypy `--strict`.

## Key engineering decisions

**1. Effectively-once results on an at-least-once pipeline.**
Kafka gives at-least-once delivery here: offsets are committed only *after* a
batch is stored, so a crash means redelivery rather than loss. Duplicates are
harmless because every step is idempotent:
- **Client retries:** an `idempotency_key` with a request fingerprint means a
  retry replays the original result and publishes no second message.
- **Producer retries:** `acks=all` plus an idempotent Kafka producer.
- **Consumer redelivery:** the processor's write is
  `UPDATE … WHERE id = :id AND status = 'pending'`, so a sync changes state at
  most once, even with two workers or a restart mid-batch.

Strictly speaking this is *effectively-once state changes*, not Kafka
transactional exactly-once. A test kills a worker mid-batch, restarts it, and
checks that the already-done rows are left untouched.

**2. Commit first, publish second.** Kafka never carries an event the database
doesn't have. If publishing fails, the API returns a 500 but the row is kept, and a retry with
the same key publishes it exactly once.

**3. Parallel processing that keeps per-entity order.** Messages are keyed by
`entity_id`, so one entity's events share a partition. Inside each batch, the
processor runs *different* entities concurrently and *the same* entity
sequentially, in publish order. That raised throughput from 62 to about 118
events/s without reordering anyone's updates.

**4. Bad data is stored, not dropped.** Validation failures become `failed`
rows with every problem listed (`'name' is missing (stripe expects one of:
customer_name, name); 'email' is not a valid email address`). Poison Kafka
messages are logged, counted and skipped, so one bad record can't block a
partition.

**5. Observability from the start.**
- **Metrics:** bounded-cardinality Prometheus metrics for every stage, feeding
  three Grafana dashboards: overview, performance and errors.
- **Traces:** one trace runs from the HTTP request through Kafka into the
  processor, using W3C trace context carried in Kafka message headers.

## Results (measured, development laptop)

| | Result |
|---|---|
| Processor throughput | **114–119 events/s** (100 events), 118 events/s (1000 events), one instance |
| Processor latency (Kafka consume → DB update) | p50 38–42 ms, p95 52–61 ms, p99 55–88 ms |
| Server-side processing per event | p50 ≈ 11 ms, p99 ≈ 24 ms |
| API, measured inside the Linux container | 52 req/s at 1 concurrent request (p50 18 ms), 114 req/s at 50 concurrent |
| Error rate in the load tests | 0 of ~1,500 requests failed |
| Tests | 91 passing; ruff and mypy `--strict` clean |

**Not measured, so not claimed:**
- **Uptime / SLA.** It hasn't run long enough in production to state one.
- **Cloud throughput.** The Render deployment is configured but not yet
  deployed. A free instance has 0.1 CPU, so expect lower numbers than the laptop.
- **Kubernetes.** Not used. Containerised scaling is a natural next step; see below.

## What I'd do next

- **Transactional outbox:** write the event to an outbox table in the same
  transaction, and have a relay publish it. That removes the "saved but not
  published" window for clients that don't send idempotency keys.
- **Scale out:** more partitions, plus processors running as their own deployment
  (Kubernetes, or Render workers), autoscaled on consumer-group lag.
- **Dead-letter topic:** with a replay tool for poison messages.
- **Alerting:** Prometheus rules on error rate, consumer lag and `processor_running == 0`.
