"""HTTP-level inventory benchmark against a live seller (end-to-end: proxy-free
FastAPI + psycopg + Postgres). Complements db/run_pgbench.sh (DB only).

    pip install httpx
    python benchmarks/inventory/http_bench.py --url http://localhost:8101 --token $SELLER_ADMIN_TOKEN \
        --concurrency 1 8 32 --duration 15 --reps 3 --workload uniform hot

Workloads (all on fresh parts it creates, so seed data is untouched):
  uniform  reserve 1 unit of one of 40 parts, unique key per request
  hot      everyone reserves the same part (row-lock contention)
  oversell 200 concurrent reserves for a part with 50 units -> must be exactly 50 wins

Reports throughput, latency p50/p95/p99/max, error counts by status, and an
inventory-correctness check (DB stock vs. successful reservations).
Results -> benchmarks/results/inventory_http_<timestamp>.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import statistics
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

RESULTS = Path(__file__).resolve().parents[1] / "results"


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = max(0, min(len(values) - 1, round(p / 100 * len(values) + 0.5) - 1))
    return round(values[k], 2)


async def make_parts(client: httpx.AsyncClient, admin: dict, n: int, qty: int) -> list[str]:
    tag = uuid.uuid4().hex[:6]
    parts = [f"BENCH-{tag}-{i:02d}" for i in range(n)]
    for p in parts:
        (await client.post("/admin/inventory", json={"part_id": p, "add_quantity": qty}, headers=admin)).raise_for_status()
    return parts


async def stock(client: httpx.AsyncClient, admin: dict, parts: list[str]) -> dict[str, dict]:
    out = {}
    for p in parts:
        r = await client.get("/admin/inventory", params={"part_id": p}, headers=admin)
        out[p] = r.json()["items"][0]
    return out


async def run_timed(client, parts, concurrency, duration, qty_per_req=1):
    latencies: list[float] = []
    statuses: Counter = Counter()
    won: Counter = Counter()
    deadline = time.perf_counter() + duration
    i = 0

    async def worker(wid: int):
        nonlocal i
        while time.perf_counter() < deadline:
            i += 1
            part = parts[i % len(parts)]
            t0 = time.perf_counter()
            try:
                r = await client.post("/reservations", headers={"Idempotency-Key": f"bench-{uuid.uuid4().hex}"},
                                      json={"order_id": "bench", "line_id": f"w{wid}", "part_id": part,
                                            "quantity": qty_per_req})
                statuses[r.status_code] += 1
                if r.status_code == 201:
                    won[part] += qty_per_req
            except httpx.HTTPError as exc:
                statuses[type(exc).__name__] += 1
            latencies.append((time.perf_counter() - t0) * 1000)

    t_start = time.perf_counter()
    await asyncio.gather(*(worker(w) for w in range(concurrency)))
    return latencies, statuses, won, time.perf_counter() - t_start


async def bench_once(url, token, workload, concurrency, duration):
    admin = {"X-Admin-Token": token}
    limits = httpx.Limits(max_connections=concurrency + 4, max_keepalive_connections=concurrency + 4)
    async with httpx.AsyncClient(base_url=url, timeout=30, limits=limits) as client:
        if workload == "oversell":
            parts = await make_parts(client, admin, 1, 50)
            before = await stock(client, admin, parts)
            t0 = time.perf_counter()
            lat: list[float] = []

            async def one(k):
                s = time.perf_counter()
                r = await client.post("/reservations", headers={"Idempotency-Key": f"os-{parts[0]}-{k}"},
                                      json={"order_id": "bench", "line_id": str(k), "part_id": parts[0], "quantity": 1})
                lat.append((time.perf_counter() - s) * 1000)
                return r.status_code
            codes = await asyncio.gather(*(one(k) for k in range(200)))
            elapsed = time.perf_counter() - t0
            statuses, won = Counter(codes), Counter({parts[0]: codes.count(201)})
        else:
            parts = await make_parts(client, admin, 40 if workload == "uniform" else 1, 1_000_000_000)
            before = await stock(client, admin, parts)
            lat, statuses, won, elapsed = await run_timed(client, parts, concurrency, duration)
        after = await stock(client, admin, parts)

    violations = []
    for p in parts:
        taken = before[p]["available_quantity"] - after[p]["available_quantity"]
        if taken != won[p] or after[p]["available_quantity"] < 0:
            violations.append({"part": p, "db_taken": taken, "acknowledged_201": won[p]})
    if workload == "oversell" and won[parts[0]] != 50:
        violations.append({"part": parts[0], "expected_wins": 50, "wins": won[parts[0]]})

    total = sum(statuses.values())
    return {
        "workload": workload, "concurrency": concurrency, "duration_s": round(elapsed, 2),
        "requests": total, "throughput_rps": round(total / elapsed, 1) if elapsed else None,
        "successful_reservations": sum(won.values()),
        "latency_ms": {"p50": pct(lat, 50), "p95": pct(lat, 95), "p99": pct(lat, 99),
                       "max": round(max(lat), 2) if lat else None,
                       "mean": round(statistics.fmean(lat), 2) if lat else None},
        "status_counts": {str(k): v for k, v in statuses.items()},
        "errors": sum(v for k, v in statuses.items() if k not in (201, 409)),
        "inventory_correct": not violations, "violations": violations,
    }


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("SELLER_URL", "http://localhost:8101"))
    ap.add_argument("--token", default=os.environ.get("SELLER_ADMIN_TOKEN"))
    ap.add_argument("--workload", nargs="+", default=["uniform", "hot", "oversell"])
    ap.add_argument("--concurrency", nargs="+", type=int, default=[1, 8, 32])
    ap.add_argument("--duration", type=float, default=15)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--hardware", default="", help="free text, e.g. 'GCP e2-standard-2, 2 vCPU, 8 GB, pd-balanced'")
    a = ap.parse_args()
    if not a.token:
        ap.error("--token or SELLER_ADMIN_TOKEN required (admin endpoints create bench parts)")

    runs = []
    for w in a.workload:
        for c in ([0] if w == "oversell" else a.concurrency):
            for rep in range(1, a.reps + 1):
                r = await bench_once(a.url, a.token, w, c or 200, a.duration)
                r["rep"] = rep
                runs.append(r)
                print(f"{w:9s} c={r['concurrency']:<4} rep={rep}  {r['throughput_rps']} req/s  "
                      f"p50={r['latency_ms']['p50']}ms p99={r['latency_ms']['p99']}ms  "
                      f"errors={r['errors']}  correct={r['inventory_correct']}")

    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = RESULTS / f"inventory_http_{stamp}.json"
    out.write_text(json.dumps({
        "benchmark": "inventory_http", "timestamp": stamp, "target": a.url,
        "client_host": {"platform": platform.platform(), "python": platform.python_version(), "cpus": os.cpu_count()},
        "server_hardware": a.hardware, "duration_s": a.duration, "repetitions": a.reps, "runs": runs,
    }, indent=2))
    print(f"results -> {out}")


if __name__ == "__main__":
    asyncio.run(main())
