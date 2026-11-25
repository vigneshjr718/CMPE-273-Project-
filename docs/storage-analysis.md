# Storage analysis

> Shared document (C + D). **Part 1 (seller DB workload and schema) is Package C.**
> Part 2 (deployed volumes, encryption, key handling, backup/restore) is D's to fill in.

## Part 1 — Seller database workload and schema (Package C)

### 1.1 What the seller stores and why

| Table | Purpose | Write pattern | Growth |
|---|---|---|---|
| `inventory` | Current stock per part: `total`, `available`, `reserved`, `delivered` | In-place UPDATE of one row per reserve, cancel or deliver | Fixed: ~40 rows per seller |
| `reservations` | One row per successful reserve; status + delivery state | INSERT once, then 1–3 UPDATEs (ship, deliver, cancel) | Append-mostly: 1 row per reservation |
| `idempotency_records` | Durable truth per operation key: outcome (`RESERVED`/`REJECTED`/`FENCED`), `status_version`, cancellation receipt | INSERT once; UPDATE on rejection, version bump or fence, always in the same transaction as the change | Append-mostly: 1 row per key ever seen |

Nothing is deleted. The idempotency guarantee ("same key → same outcome, even
after restart") is only as long as the record exists. Keeping records forever is
fine at our scale (§1.4). If we ever add a retention purge, it must be longer
than the coordinator's longest retry and reconciliation window.

### 1.2 Why a relational DB with row-level locking (and not, e.g., a KV store)

The seller's correctness requirements map directly onto PostgreSQL features:

| Requirement | Mechanism |
|---|---|
| Two requests for the last unit → only one succeeds | `UPDATE inventory … WHERE available_quantity >= q` takes the row lock; under READ COMMITTED the second transaction re-evaluates the predicate after the first commits and updates 0 rows |
| Stock never negative; units never created or lost | `CHECK (available >= 0)` and `CHECK (available + reserved + delivered = total)`. A buggy code path aborts instead of corrupting data |
| ≤ 1 stock effect per operation key, concurrent duplicates included | `idempotency_records.operation_key` PRIMARY KEY + `INSERT … ON CONFLICT DO NOTHING`: the second inserter **blocks** until the first commits, then sees its outcome |
| Reserve, stock change and idempotency record agree after a crash | All three in **one transaction** (one DB function call). There is no state where stock moved but the key has no record, or the reverse |
| Repeated cancel restores stock once | `SELECT … FOR UPDATE` on the reservation; the status check and restore run under the lock |
| Late reserve after the coordinator gave up | Tombstone row (`outcome = FENCED`, receipt) occupies the key, so the late request hits the conflict path |
| Stale or reordered observations are detectable | `status_version` bumped in the same transaction as each change; never derived from clocks |

All stock-changing logic lives in three PL/pgSQL functions
(`seller_reserve`, `seller_cancel_operation`, `seller_set_delivery`).
Each API write is **one round-trip and one transaction**. The exact production code
path is what the race tests exercise.

Lock order is always `idempotency_records` → `reservations` → `inventory`, so the
paths cannot deadlock each other. Across 1.05 M mixed concurrent operations
(1–64 clients, including concurrent cancel and deliver) there were 0 failed
transactions and 0 deadlocks.

### 1.3 Durability assumptions

* A `201`/`200` is sent only after COMMIT. With Postgres defaults
  (`synchronous_commit = on`, `fsync = on`) the WAL is flushed first, so an
  acknowledged reservation survives a crash or VM reboot **as long as the data
  directory is on a persistent volume** (D: Part 2).
* A 503 from the seller is only sent before commit (injected errors, DB down,
  statement timeout → rollback). A **timeout** may hide either outcome. That is why
  lookup-by-key exists.
* Fault rules are in memory on purpose. They are test scaffolding, not state.

### 1.4 Size estimate

Measured on PostgreSQL 16 after a 118k-operation stress run (so including
update bloat and all indexes):

| Table | Bytes per row (heap + indexes) |
|---|---|
| `reservations` | ~525 B |
| `idempotency_records` | ~195 B with an empty body; ~350 B with a real request body |
| `inventory` | negligible (40 rows) |

≈ **0.9 KB per reservation attempt** on the chosen seller. Even 100,000 attempts
per seller is < 100 MB. Storage volume is not a concern for this project. Durability
and isolation are.

### 1.5 Measured throughput (DB layer only)

From `benchmarks/inventory/db/run_pgbench.sh` (see `benchmarks/inventory/README.md`
for method, hardware and the full table). Short version:

* Reserves spread over many parts scale with clients until CPU-bound.
* Reserves on **one hot part** plateau at a single row's commit rate, whatever the
  concurrency. That is the expected cost of strict last-unit correctness: one
  serialization point per part. Latency rises with concurrency, throughput does not.
* Every run ended with **0 invariant violations** (no oversell, no lost units,
  no duplicate per key, no fenced key holding stock).

### 1.6 Isolation between services

`infra/db-init/init.sql` creates one role per service (`order_svc`, `coordinator`,
`tracker`, `seller1` … `seller5`), makes it
the owner of exactly one database, and revokes `CONNECT` from `PUBLIC`.
Verified through D's db-init runner: `seller1` → `seller2_db` and `coordinator` →
`seller3_db` give `FATAL: permission denied for database`. Passwords come from `infra/.env` as psql
variables (never in the repo) and are re-applied on every deploy, so rotation =
edit `.env` + redeploy. An empty or missing password aborts the role
transaction, so nothing changes.

---

## Part 2 — Deployment storage (Package D)

_TODO (D): persistent volumes for Postgres/Kafka, disk encryption + key handling,
backup/restore procedure and results, VM-reboot test, restart behaviour._
