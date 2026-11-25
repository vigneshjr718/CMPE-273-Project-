# Section C — Seller Services and Inventory

_Owner: Package C. Draft; reformat to A's section template when it's shared._

## C.1 Responsibility

The seller layer simulates five independent suppliers. Each instance answers
quotes, holds stock through reservations, reports delivery progress, and accepts
cancellations. The coordinator (B) is its only client. The seller's job is to
stay **correct under concurrency and partial failure**. It never oversells, never
applies an operation twice, and never "forgets" an acknowledged reservation, even
when replies are lost or the process restarts.

Five instances run from one image. Behaviour differs only through environment
(delivery time 24–216 hours, price multiplier 0.70–1.15, seeded stock), so the
coordinator faces real trade-offs. Seller-1 is fast but expensive. Seller-5 is
cheap but often too slow for a needed-by date. Every catalog part is stocked by
4 of the 5 sellers, so a replacement always exists in principle.

## C.2 API (contract: `contracts/openapi/seller-api.yaml`)

The wire shapes were aligned with the coordinator's adapters (B), which parse
replies strictly (`extra="forbid"`). The seller's test suite validates every reply
with verbatim copies of B's models, so a contract break fails in C's CI first.

| Endpoint | Purpose |
|---|---|
| `POST /quotes` | Non-binding, read-only stock + `estimated_delivery_at` for one line; B decides eligibility |
| `POST /reservations` + `Idempotency-Key` | Reserve stock; the key is B's persisted operation key. `201 RESERVED`, `200` replay, `409 REJECTED` with a reason |
| `GET /reservations/by-key/{key}` | Reconciliation after a timeout: what happened to this key? |
| `POST /reservation-operations/{key}/cancel` + cancellation key | **Terminal fencing** of the original operation, whether or not the seller has seen it: `CANCELLED` + durable receipt, `PENDING` (shipped), or `DELIVERED` |
| `GET /reservations/{id}/delivery` | Versioned delivery observation (`IN_TRANSIT`/`DELIVERED`, `status_version`), polled by B |
| `/admin/*` (token) | Fault injection, delivery demo controls, inventory inspection and restock |

## C.3 Design decisions

**One transaction per operation, in the database.** Each stock-changing request
is a single call to a PL/pgSQL function that claims the operation key, changes
stock, and records the outcome atomically. We chose this over application-level
multi-statement transactions for three reasons. There is no window in which stock
and the idempotency record disagree. Each write costs one network round-trip. And
the race tests exercise exactly the code path production runs.

**The last unit is serialized by a row lock, not by application logic.**
`UPDATE inventory SET available = available − q WHERE part_id = $1 AND available ≥ q`
is atomic. Under READ COMMITTED a waiting transaction re-checks the predicate
after the lock holder commits, so the loser updates zero rows and gets
`409 INSUFFICIENT_STOCK`. A `CHECK` constraint
(`available + reserved + delivered = total`, `available ≥ 0`) backs this up: if a
code path were wrong, it would abort rather than corrupt stock.

**Idempotency is durable and covers concurrent duplicates.** The operation key is
the primary key of `idempotency_records`. A duplicate request that arrives while
the original is still in flight blocks on that key until the original commits,
then replays its outcome. Same key with a different payload (SHA-256 of the
canonical body) returns `409`. Definitive rejections are stored too, so a retry
gets the same answer even if stock has changed since.

**Timeouts are UNKNOWN, so the seller provides lookup and fencing.** After a timeout
B asks `by-key`. A `404` there is only absence of evidence: the original request may
still be in flight. To make a late commit impossible, B fences the key. Fencing
writes a tombstone with a durable receipt in the same table that serializes
reserves, so the original request, if it shows up late, is rejected without touching
stock. The order doesn't matter: either the reserve commits first and the fence
cancels it, or the fence commits first and the reserve is rejected. Both orders were
tested with transactions held open deliberately.

**Cancellation is terminal, idempotent and provable.** A cancel locks the
operation's record, then its reservation (fixed lock order, so no deadlocks). Only a
`RESERVED → CANCELLED` transition restores stock, and the receipt is issued in the
same transaction. Repeated or concurrent cancels, under any cancellation key,
return the same receipt and restore 0. A shipped reservation answers `PENDING`,
because a dispatched delivery cannot be promised away. A delivered one answers
`DELIVERED`, so B never replaces a line that has already arrived.

**Versioned observations.** Each operation has one `status_version`, bumped in
the same transaction as every observable change (ETA moved, shipped, delivered,
cancelled or fenced) and unchanged on identical re-reads. B uses it instead of
wall-clock time to discard stale or reordered replies.

## C.4 Failure injection

Per-instance in-memory rules (`/admin/faults`, team token) let the team reproduce
every failure B must handle, deterministically (`remaining: N`) or with a
probability:

* **Delay** (slow seller): sleep before processing.
* **Error before commit** (unavailable seller): `503`, nothing changed, safe to retry.
* **Committed-but-reply-lost**: the change commits, then the reply is held past the
  client's timeout and finally `504 REPLY_LOST`. This is the case that requires
  reconciliation.
* **Dead seller**: stop the container.

## C.5 Verification

| Property | How verified | Result |
|---|---|---|
| Last unit: only one winner | 50 concurrent sessions, 1 unit (DB). 25 threads via service layer (pytest). 2 and 50 concurrent HTTP requests (e2e) | 1 winner, 49 `REJECTED INSUFFICIENT_STOCK` |
| Same key concurrently → one effect | 30 concurrent sessions with one key | 1 `RESERVED`, 29 `REPLAYED`, 1 reservation id, stock −5 once |
| Repeated cancel → one effect | 30 concurrent cancels, 30 different cancellation keys | total restored = 5 (the reserved qty), one receipt |
| Reserve in flight vs. fence | reserve held open 2 s, fence arrives at 0.5 s | fence waits, then cancels; stock net 0 |
| Fence in flight vs. late reserve | fence held open 2 s, reserve arrives at 0.5 s | reserve → `409 REJECTED OPERATION_CANCELLED`, stock untouched |
| Crash before commit | reserve held open, then rolled back; retry with same key | retry reserves normally (exactly one effect) |
| Cancel vs. deliver race | 20 concurrent pairs | exactly one wins each time; no deadlocks |
| Random mixed load | 1.05 M ops over 36 benchmark runs, 1–64 clients (reserve/replay/conflict/cancel/fence/ETA/deliver) | 0 failed txns, 0 deadlocks, 0 invariant violations |
| Restart | new app instance on same DB (pytest); container restart (e2e) | replay, 409-on-mismatch, tombstones and receipts all survive |
| Contract with B | every reply parsed with B's own pydantic models; B's request bodies parsed with C's | identical field sets; all parse |
| DB isolation | `init.sql` via D's db-init runner, twice; cross-DB connects; empty/missing password | idempotent; `permission denied` across services; bad config stops the run with no change |

## C.6 Benchmark

Full method and data: `benchmarks/inventory/README.md`. DB level, PostgreSQL 16,
2 vCPU, 10 s × 3 reps per cell, 1.05 M transactions in total:

| Workload | 1 client | 8 | 32 | 64 |
|---|---|---|---|---|
| reserve, 40 parts (txn/s) | 1,140 | 3,125 | 3,959 | 3,164 |
| reserve, 1 hot part (txn/s) | 1,088 | 1,254 | 915 | 750 |
| coordinator mix (txn/s) | 2,059 | 6,176 | 6,160 | 5,554 |

0 failed transactions and 0 inventory-invariant violations in all 36 runs.
A single hot part plateaus at one row's commit rate, and its latency grows with the
queue (0.9 → 85 ms). That is the deliberate cost of never overselling. Versioning
and receipts cost about 10% throughput compared with an unversioned build.

_TODO: add the HTTP-level table from the cloud VM (`http_bench.py`)._

## C.7 Limitations and future work

* Delivery progress is demo-driven (admin endpoint), not a real logistics feed.
* Fault rules are per process and in memory. With more than one replica per seller
  they would need to be shared.
* Idempotency records are kept forever. A retention policy must outlive B's
  longest retry and reconciliation window.
* Quotes are not reservations. Stock can change between quote and reserve, so B
  must treat `409 REJECTED INSUFFICIENT_STOCK` on reserve as normal.
* A reservation that was reserved and then cancelled cannot be expressed as a
  `ReservationReply`, so lookup-by-key returns `409 OPERATION_CANCELLED` for it.
  B already holds the cancellation receipt in that case.
