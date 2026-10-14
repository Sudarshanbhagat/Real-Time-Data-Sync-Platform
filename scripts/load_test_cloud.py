"""HTTP-only load test for a deployed instance (no database or Kafka access needed).

Runs against any base URL - the cloud deployment, the local container, or uvicorn:

    python scripts/load_test_cloud.py --url https://your-app.onrender.com
    python scripts/load_test_cloud.py --url http://localhost:8001 --label container
    python scripts/load_test_cloud.py --url ... --json results.json   # save results

Scenarios (each on fresh entity IDs, mixed across all four source systems):

    sequential   1 request at a time         -> best-case per-request latency
    c10          10 concurrent requests      -> moderate load
    c50          50 concurrent requests      -> saturation
    1000 x c50   (with --full)               -> sustained load

For every scenario it reports API throughput, latency p50/p95/p99 and error rate,
then polls GET /api/v1/status for a sample of the syncs to measure how long the
background processor took to finish them (pipeline throughput), and finally reads
GET /health/processor for the server-side processing percentiles.

Free-tier services sleep when idle: the first request after a while can take
~1 minute. The script sends a warm-up request (and waits for it) first.
"""

import argparse
import asyncio
import json
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import httpx

# (source, destination) pairs, cycled so every source system is exercised.
ROUTES = [
    ("salesforce", "hubspot"),
    ("hubspot", "stripe"),
    ("stripe", "salesforce"),
    ("custom", "hubspot"),
]


def sample(system: str, n: int) -> dict[str, Any]:
    """A valid payload for `system`, unique per `n`."""
    if system == "salesforce":
        return {"AccountName": f"Acme {n}", "AccountStatus": "Active"}
    if system == "hubspot":
        return {"firstname": f"Grace{n}", "hs_lead_status": "OPEN"}
    if system == "stripe":
        return {"customer_name": f"Linus {n}", "status": "active"}
    return {"name": f"Thing {n}", "metadata": {"n": n}}


def body(run_id: str, n: int) -> dict[str, Any]:
    """A valid sync request, unique per (run, n)."""
    source, destination = ROUTES[n % len(ROUTES)]
    data = sample(source, n)
    return {
        "entity_id": f"{run_id}_{n}",
        "source_system": source,
        "destination_system": destination,
        "data": data,
        "idempotency_key": f"{run_id}-{n}",
    }


def pct(values: list[float], p: float) -> float | None:
    """Nearest-rank percentile."""
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, min(len(ordered) - 1, round(p / 100 * len(ordered)) - 1))], 1)


@dataclass
class ScenarioResult:
    """One scenario's numbers (also serialised to --json)."""

    name: str
    requests: int
    concurrency: int
    duration_s: float
    throughput_rps: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    errors: int
    error_rate: float
    pipeline_s: float | None = None
    pipeline_eps: float | None = None
    synced: int = 0
    failed: int = 0
    status_codes: dict[str, int] = field(default_factory=dict)


async def run_scenario(
    client: httpx.AsyncClient, name: str, count: int, concurrency: int, wait_s: float
) -> ScenarioResult:
    """POST `count` syncs at `concurrency`, then wait for the processor to finish them."""
    run_id = f"cloud{uuid.uuid4().hex[:8]}"
    semaphore = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    codes: dict[str, int] = {}
    accepted: list[str] = []

    async def send(n: int) -> None:
        payload = body(run_id, n)
        async with semaphore:
            started = time.perf_counter()
            try:
                response = await client.post("/api/v1/sync", json=payload)
                code = str(response.status_code)
            except httpx.HTTPError as exc:
                code = type(exc).__name__
            latencies.append((time.perf_counter() - started) * 1000)
        codes[code] = codes.get(code, 0) + 1
        if code == "200":
            accepted.append(payload["entity_id"])

    started = time.perf_counter()
    await asyncio.gather(*(send(n) for n in range(count)))
    sent = time.perf_counter()
    errors = count - codes.get("200", 0)

    synced = failed = 0
    pipeline_s: float | None = None
    if accepted:
        # Poll until every accepted sync has left `pending` (or give up).
        remaining = set(accepted)
        deadline = time.perf_counter() + wait_s
        status_sem = asyncio.Semaphore(min(concurrency, 20))

        async def check(entity_id: str) -> str | None:
            async with status_sem:
                try:
                    response = await client.get(f"/api/v1/status/{entity_id}")
                except httpx.HTTPError:
                    return None
            return str(response.json().get("status")) if response.status_code == 200 else None

        while remaining and time.perf_counter() < deadline:
            batch = list(remaining)
            statuses = await asyncio.gather(*(check(e) for e in batch))
            for entity_id, status in zip(batch, statuses, strict=True):
                if status in ("synced", "failed"):
                    remaining.discard(entity_id)
                    synced += status == "synced"
                    failed += status == "failed"
            if remaining:
                await asyncio.sleep(0.25)
        if not remaining:
            pipeline_s = round(time.perf_counter() - started, 2)

    duration = sent - started
    return ScenarioResult(
        name=name,
        requests=count,
        concurrency=concurrency,
        duration_s=round(duration, 2),
        throughput_rps=round(count / duration, 1),
        p50_ms=pct(latencies, 50),
        p95_ms=pct(latencies, 95),
        p99_ms=pct(latencies, 99),
        errors=errors,
        error_rate=round(errors / count, 4),
        pipeline_s=pipeline_s,
        pipeline_eps=round(count / pipeline_s, 1) if pipeline_s else None,
        synced=synced,
        failed=failed,
        status_codes=codes,
    )


async def warm_up(client: httpx.AsyncClient) -> float:
    """Wake a sleeping instance; returns seconds until /health answered."""
    started = time.perf_counter()
    for _ in range(30):
        try:
            if (await client.get("/health", timeout=90)).status_code == 200:
                return time.perf_counter() - started
        except httpx.HTTPError:
            pass
        await asyncio.sleep(3)
    raise SystemExit("ERROR: /health never returned 200 - is the URL right / the service up?")


def print_table(label: str, results: list[ScenarioResult]) -> None:
    """Markdown table, ready to paste into the README."""
    print(f"\n### {label}\n")
    print(
        "| Scenario | Requests | Conc. | API req/s | p50 ms | p95 ms | p99 ms "
        "| Errors | Pipeline events/s |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for r in results:
        pipeline = f"{r.pipeline_eps}" if r.pipeline_eps else "n/a (timed out)"
        print(
            f"| {r.name} | {r.requests} | {r.concurrency} | {r.throughput_rps} | {r.p50_ms} "
            f"| {r.p95_ms} | {r.p99_ms} | {r.errors} ({r.error_rate:.1%}) | {pipeline} |"
        )


async def main(args: argparse.Namespace) -> int:
    """Run all scenarios and print/save the results."""
    limits = httpx.Limits(max_connections=60, max_keepalive_connections=60)
    async with httpx.AsyncClient(base_url=args.url, timeout=30, limits=limits) as client:
        wake_s = await warm_up(client)
        print(f"{args.url}: /health OK after {wake_s:.1f}s (warm-up)")

        plan = [
            ("100 sequential", 100, 1),
            ("100 x c10", 100, 10),
            ("100 x c50", 100, 50),
        ]
        if args.full:
            plan.append(("1000 x c50", 1000, 50))

        results = []
        for name, count, concurrency in plan:
            print(f"running {name} ...", flush=True)
            results.append(await run_scenario(client, name, count, concurrency, args.wait))

        try:
            processor = (await client.get("/health/processor")).json()
        except (httpx.HTTPError, ValueError):
            processor = {}

    print_table(args.label, results)
    if processor:
        lat = processor.get("latency_ms", {})
        print(
            f"\nServer-side processor (since start): processed={processor.get('processed')} "
            f"error_rate={processor.get('error_rate')} p50={lat.get('p50')}ms "
            f"p95={lat.get('p95')}ms p99={lat.get('p99')}ms"
        )
    if args.json:
        report = {
            "label": args.label,
            "url": args.url,
            "results": [asdict(r) for r in results],
            "processor": processor,
        }
        Path(args.json).write_text(json.dumps(report, indent=2), encoding="utf-8")  # noqa: ASYNC240 - tiny one-off write after the run
        print(f"saved {args.json}")
    return 0 if all(r.error_rate < 0.01 for r in results) else 2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--url", required=True, help="base URL, e.g. https://x.onrender.com")
    parser.add_argument("--label", default="deployment", help="name for the results table")
    parser.add_argument("--full", action="store_true", help="also run 1000 requests at c50")
    parser.add_argument(
        "--wait", type=float, default=120, help="max seconds to wait for processing"
    )
    parser.add_argument("--json", help="write results to this JSON file")
    sys.exit(asyncio.run(main(parser.parse_args())))
