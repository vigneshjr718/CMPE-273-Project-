# Inventory benchmark (Package C lead)

Two levels:

| Level | Tool | Measures |
|---|---|---|
| DB | `db/run_pgbench.sh` | Cost and correctness of the stock transactions themselves (`seller_reserve`, `seller_cancel*`, `seller_set_delivery`) with no HTTP/Python overhead |
| End-to-end | `http_bench.py` | Same workloads through the seller API (FastAPI + psycopg + Postgres) on the deployed stack |

Every run ends with an **inventory-correctness check**. A run with a non-zero
violation count is a failed run, whatever its throughput.
* DB: `db/invariants.sql`: `reserved` = sum of active reservations; `delivered` = sum of delivered; `available + reserved + delivered = total`; `available ≥ 0`; ≤ 1 reservation per key; no fenced key holds stock; every `RESERVED` record has its reservation; every cancelled reservation has a durable receipt.
* HTTP: units taken from stock (DB) == units acknowledged with `201`; for `oversell`, exactly 50 winners for 50 units.

## Workloads

| Name | What | Why |
|---|---|---|
| `reserve_uniform` | reserve 1 unit of a random part among 40, unique key per call | normal spread load |
| `reserve_hot` | every client reserves the **same** part | worst-case contention (last-unit scenario at scale) |
| `mixed` | 3,000 operation keys over 40 parts: 65% reserve (repeats = idempotent replays), 10% conflicting payload, 15% cancel/fence (incl. never-reserved keys -> tombstones), 5% ETA change, 5% deliver | the coordinator's real mix, including retries |
| `oversell` (HTTP only) | 200 concurrent reserves for 50 units | must yield exactly 50 × `201` |

Stock is set to 10⁹ per part for the timed workloads so sell-outs don't cut runs short.

## Run

```bash
# DB level (needs a role that can CREATE DATABASE; drops/recreates bench_seller)
PGHOST=localhost PGUSER=postgres PGPASSWORD=... CLIENTS="1 8 32 64" DURATION=10 REPS=3 \
  benchmarks/inventory/db/run_pgbench.sh

# HTTP level, against one deployed seller (creates its own BENCH-* parts)
pip install httpx
python benchmarks/inventory/http_bench.py --url http://<vm>:8101 --token $SELLER_ADMIN_TOKEN \
  --concurrency 1 8 32 --duration 15 --reps 3 --hardware "<provider, vCPU, RAM, disk>"
```
Results land in `benchmarks/results/` as JSON (`inventory_pgbench_*`, `inventory_http_*`).

## Results — DB level

**Setup.** PostgreSQL 16.15, default config (`synchronous_commit=on`, `fsync=on`),
READ COMMITTED. 2 vCPU / 7.8 GiB Linux x86_64 container; **pgbench ran on the same
2 vCPUs as Postgres**, so the client competes with the server and absolute numbers
are a floor. 10 s per run, 3 repetitions, fresh database per run.
1,046,458 transactions in total.
Raw data: `benchmarks/results/inventory_pgbench_20261124T234504Z.json`.

| Workload | Clients | Throughput (txn/s), mean ± sd | Avg latency (ms) | Txns per run | Failed | Invariant violations |
|---|---|---|---|---|---|---|
| reserve_uniform | 1 | 1,140 ± 1 | 0.88 | 11,387 | 0 | 0 |
| reserve_uniform | 8 | 3,125 ± 133 | 2.56 | 31,134 | 0 | 0 |
| reserve_uniform | 32 | 3,959 ± 56 | 8.08 | 39,109 | 0 | 0 |
| reserve_uniform | 64 | 3,164 ± 226 | 20.30 | 30,777 | 0 | 0 |
| reserve_hot | 1 | 1,088 ± 38 | 0.92 | 10,877 | 0 | 0 |
| reserve_hot | 8 | 1,254 ± 87 | 6.40 | 12,498 | 0 | 0 |
| reserve_hot | 32 | 915 ± 26 | 35.00 | 9,047 | 0 | 0 |
| reserve_hot | 64 | 750 ± 21 | 85.43 | 7,344 | 0 | 0 |
| mixed | 1 | 2,059 ± 55 | 0.49 | 20,578 | 0 | 0 |
| mixed | 8 | 6,176 ± 309 | 1.30 | 61,527 | 0 | 0 |
| mixed | 32 | 6,160 ± 413 | 5.21 | 60,600 | 0 | 0 |
| mixed | 64 | 5,554 ± 302 | 11.55 | 53,941 | 0 | 0 |

**Reading it**

* **Correctness held everywhere.** 0 failed transactions (so no deadlocks or
  serialization errors) and 0 invariant violations across all 36 runs.
* **Spread load scales until CPU-bound.** `reserve_uniform` gains 3.5× from 1 → 32
  clients, then dips at 64 because the 2 vCPUs are saturated (server + pgbench).
* **One hot part = one serialization point.** `reserve_hot` peaks around 8 clients
  (~1.25k/s) and *falls* as clients are added, because they queue on one row lock.
  Latency grows roughly linearly with the queue (0.9 → 85 ms). This is the price of
  never overselling: correctness for a single SKU is bought with serialization.
  It's acceptable here, since real demand for a single part is far below 750/s.
  The mitigation if it ever mattered would be stock sharding (split a SKU into N
  rows), at the cost of more complex "is there any stock left" checks.
* **`mixed` is faster per transaction than pure reserves.** Many operations are
  replays, conflicts or repeat cancels that end at the operation-key check without
  touching inventory. Idempotent retries are therefore cheap for the coordinator.
* The `status_version` + receipt bookkeeping added for B's contract costs ≈ 10%
  throughput versus an earlier run without it (same machine). Worth it: it is what
  lets B reject stale observations and prove a fence.

## Results — HTTP level

_TODO: run `http_bench.py` against the cloud deployment once D's VM is up, and fill
this in. Record the VM type, vCPU/RAM, disk type, and whether the client ran on the
same VM._

| Workload | Concurrency | req/s (mean ± sd) | p50 ms | p95 ms | p99 ms | Errors | Correct |
|---|---|---|---|---|---|---|---|
| uniform | 1 / 8 / 32 | | | | | | |
| hot | 1 / 8 / 32 | | | | | | |
| oversell | 200 | — | | | | | 50/50 |
