"""Phase 3 load test: N syncs through the running app, measure processing.

Two modes, both against the *running* app and its background processor:

--mode api (default)
    Sends N POST /api/v1/sync requests, then waits until every row has left
    `pending`. Measures the whole pipeline, including the HTTP layer.

--mode processor
    Inserts N `pending` rows and publishes their events straight to Kafka (no
    HTTP), then times how fast the app's processor drains them. This isolates
    "Kafka consume -> transform -> DB update", which is what the 50 events/s
    target is about, from client/HTTP overhead on the test machine.

Both report throughput and p50/p95/p99 for processing latency (Kafka consume ->
DB update, per row) and end-to-end latency (row created -> processed).

Prerequisites: docker compose up -d; python scripts/setup_db.py;
uvicorn src.main:app (with the background processor enabled, the default).

Usage:
    python scripts/load_test_phase3.py                       # 100 events via the API
    python scripts/load_test_phase3.py --mode processor      # 100 events, processor only
    python scripts/load_test_phase3.py -n 1000 -c 50
"""

import argparse
import asyncio
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# Allow `python scripts/load_test_phase3.py` from the project root without installing.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402
from sqlalchemy import func, select, update  # noqa: E402

from src.database import SessionLocal, engine  # noqa: E402
from src.kafka.producer import SyncEventProducer  # noqa: E402
from src.kafka.schemas import SyncEventMessage  # noqa: E402
from src.models import SyncEvent  # noqa: E402
from src.models.base import utcnow  # noqa: E402


def sample(system: str, n: int) -> dict[str, Any]:
    """A valid payload for `system`, unique per `n`."""
    if system == "salesforce":
        return {"AccountName": f"Acme {n}", "AccountStatus": "Active"}
    if system == "hubspot":
        return {"firstname": f"Grace{n}", "hs_lead_status": "OPEN"}
    if system == "stripe":
        return {"customer_name": f"Linus {n}", "status": "active"}
    return {"name": f"Thing {n}", "metadata": {"n": n}}


# (source, destination) pairs, cycled so every source system is exercised.
ROUTES = [
    ("salesforce", "hubspot"),
    ("hubspot", "stripe"),
    ("stripe", "salesforce"),
    ("custom", "hubspot"),
]


def percentiles(values: list[float]) -> str:
    """Format p50/p95/p99/max of `values` (nearest rank)."""
    if not values:
        return "n/a"
    ordered = sorted(values)

    def pick(pct: float) -> float:
        return ordered[max(0, min(len(ordered) - 1, round(pct / 100 * len(ordered)) - 1))]

    return (
        f"p50={pick(50):.1f}ms  p95={pick(95):.1f}ms  p99={pick(99):.1f}ms  max={ordered[-1]:.1f}ms"
    )


async def send_all(url: str, run_id: str, count: int, concurrency: int) -> list[float]:
    """POST `count` syncs with bounded concurrency; return per-request latencies (ms)."""
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    failures: list[str] = []

    async with httpx.AsyncClient(base_url=url, timeout=30) as client:

        async def send(n: int) -> None:
            source, destination = ROUTES[n % len(ROUTES)]
            body: dict[str, Any] = {
                "entity_id": f"{run_id}_{n}",
                "source_system": source,
                "destination_system": destination,
                "data": sample(source, n),
                "idempotency_key": f"{run_id}-{n}",
            }
            async with semaphore:
                started = time.perf_counter()
                response = await client.post("/api/v1/sync", json=body)
                latencies.append((time.perf_counter() - started) * 1000)
            if response.status_code != 200:
                failures.append(f"#{n}: {response.status_code} {response.text[:120]}")

        await asyncio.gather(*(send(n) for n in range(count)))

    if failures:
        print(f"  {len(failures)} request(s) failed, e.g. {failures[0]}")
    return latencies


async def insert_pending(run_id: str, count: int) -> list[SyncEvent]:
    """Insert `count` pending rows (committed, so the processor can load them)."""
    async with SessionLocal() as session:
        events = []
        for n in range(count):
            source, destination = ROUTES[n % len(ROUTES)]
            events.append(
                SyncEvent(
                    entity_id=f"{run_id}_{n}",
                    source_system=source,
                    destination_system=destination,
                    source_data=sample(source, n),
                )
            )
        session.add_all(events)
        await session.commit()
        return events


async def publish_direct(events: list[SyncEvent]) -> None:
    """Publish the rows' events to Kafka, bypassing HTTP."""
    producer = SyncEventProducer()
    await producer.start()
    try:
        await asyncio.gather(
            *(producer.publish_event(SyncEventMessage.from_sync_event(e)) for e in events)
        )
    finally:
        await producer.stop()
    # Touch only published_at: the processor may already have finished these rows,
    # and writing back the stale objects would reset them to `pending`.
    async with SessionLocal() as session:
        await session.execute(
            update(SyncEvent)
            .where(SyncEvent.id.in_([e.id for e in events]))
            .values(published_at=utcnow())
        )
        await session.commit()


async def wait_processed(run_id: str, count: int, timeout_s: float) -> list[SyncEvent]:
    """Poll until all `count` rows of this run are out of `pending`."""
    like = f"{run_id}\\_%"
    deadline = time.perf_counter() + timeout_s
    while True:
        async with SessionLocal() as session:
            pending = await session.scalar(
                select(func.count())
                .select_from(SyncEvent)
                .where(SyncEvent.entity_id.like(like, escape="\\"), SyncEvent.status == "pending")
            )
            total = await session.scalar(
                select(func.count())
                .select_from(SyncEvent)
                .where(SyncEvent.entity_id.like(like, escape="\\"))
            )
            if total == count and pending == 0:
                result = await session.scalars(
                    select(SyncEvent).where(SyncEvent.entity_id.like(like, escape="\\"))
                )
                return list(result.all())
        if time.perf_counter() > deadline:
            raise TimeoutError(f"{pending} of {total} still pending after {timeout_s}s")
        await asyncio.sleep(0.05)


async def main(url: str, count: int, concurrency: int, timeout_s: float, mode: str) -> int:
    """Run the load test and print the report. Returns a process exit code."""
    async with httpx.AsyncClient(base_url=url, timeout=5) as client:
        try:
            metrics = (await client.get("/health/processor")).json()
        except httpx.HTTPError as exc:
            print(f"ERROR: API not reachable at {url} ({exc!r}). Start: uvicorn src.main:app")
            return 1
    if not metrics.get("running"):
        print("ERROR: background processor is not running (RUN_SYNC_PROCESSOR=false?)")
        return 1

    run_id = f"load{uuid.uuid4().hex[:8]}"
    api_latencies: list[float] = []
    if mode == "api":
        print(f"Sending {count} syncs to {url} (concurrency {concurrency}, run {run_id}) ...")
        started = time.perf_counter()
        api_latencies = await send_all(url, run_id, count, concurrency)
        sent = time.perf_counter()
    else:
        print(f"Publishing {count} events directly to Kafka (run {run_id}) ...")
        rows = await insert_pending(run_id, count)
        started = time.perf_counter()  # clock starts when events start hitting Kafka
        await publish_direct(rows)
        sent = time.perf_counter()
    events = await wait_processed(run_id, count, timeout_s)
    finished = time.perf_counter()
    await engine.dispose()

    synced = sum(e.status == "synced" for e in events)
    failed = sum(e.status == "failed" for e in events)
    processing = [e.processing_latency_ms for e in events if e.processing_latency_ms is not None]
    end_to_end = [
        (e.processed_at - e.created_at).total_seconds() * 1000
        for e in events
        if e.processed_at is not None
    ]
    total_s = finished - started

    print()
    print(f"Mode:              {mode}")
    print(f"Events:            {count}  (synced {synced}, failed {failed})")
    if mode == "api":
        rate = count / (sent - started)
        print(f"API:               {rate:.0f} req/s  {percentiles(api_latencies)}")
    else:
        print(f"Publish:           {count} events in {sent - started:.2f}s")
    print(f"Pipeline:          {count} events in {total_s:.2f}s = {count / total_s:.0f} events/s")
    print(f"Processing:        {percentiles(processing)}   (Kafka consume -> DB update)")
    if mode == "api":
        print(f"End-to-end:        {percentiles(end_to_end)}   (row created -> processed)")
    target_ok = count / total_s >= 50
    print(f"Target 50 ev/s:    {'PASS' if target_ok else 'FAIL'}")
    return 0 if target_ok and failed == 0 else 2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-n", "--count", type=int, default=100, help="number of syncs (100)")
    parser.add_argument("-c", "--concurrency", type=int, default=20, help="parallel requests")
    parser.add_argument("--url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--timeout", type=float, default=60, help="seconds to wait for processing")
    parser.add_argument(
        "--mode", choices=["api", "processor"], default="api", help="see description above"
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.url, args.count, args.concurrency, args.timeout, args.mode)))
