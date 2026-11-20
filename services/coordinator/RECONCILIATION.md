# Checkpoint 3: bounded reservation reconciliation

Based on pushed commit **cd5b597**. No new dependency or migration is needed.
Only coordinator-owned files change; .env and shared infra/contracts are excluded.
The existing PostgreSQL database must already be at 0002_reservation_flow.

## Test on Wendy's Mac
With the local database running and services/coordinator/.venv active:
```bash
COORDINATOR_POSTGRES_TESTS=1 python -m pytest -q
python -m app.tools.demo_reconciliation
```

Preparation environment: 73 passed, 4 PostgreSQL tests skipped because no local
PostgreSQL executable/Docker is available there. Opting into the real database
on Wendy's Mac should yield 77 passing tests if all checks succeed.
The PostgreSQL tests create unique synthetic test orders and remove only their
own generated records. Existing demo-order-001 remains unchanged.

The demo creates a NEW unique synthetic order by default. Expect:
- Before: order RECONCILIATION_REQUIRED, line UNKNOWN, one saved attempt/key.
- After: order and line RESERVED, SAME attempt/key, retry_count=1, version=7.
- seller_reserve_calls=1, seller_logical_reservations=1, seller_lookup_calls=1.
The controlled seller records a reservation and deliberately loses the response.
Lookup finds that exact record, so there is no second reservation request.
All coordinator state, locks and outbox records use real PostgreSQL.

The seller ledger is a controlled in-process fixture, NOT Package C or a
persistent seller service. It lasts only for that demo process. Do not claim this
demo proves C's real persistence or complete cross-process recovery. A fresh
coordinator against persisted state is exercised separately in the PostgreSQL
test using the same remote ledger fixture. Real C integration remains required.
If the demo process is interrupted, use a new demo ID on rerun; its synthetic
remote fixture ledger is not durable across processes. The old coordinator
attempt remains honestly unresolved rather than being erased/reset.

## Manually reconcile real seller APIs later
```bash
python -m app.tools.reconcile_order --order-id YOUR_ORDER_ID
```
This calls the provisional lookup/reserve HTTP adapter. Coordinate the exact
contracts with C before using it for real team integration. It does not send
cancellations or choose new sellers.

## Rules enforced
1. Read the saved attempt/key from the DB under the same per-order PostgreSQL lock.
2. Persist the reconciliation cycle/budget and crash-retry schedule BEFORE calls.
3. Query GET /reservations/by-key/{operation_key}.
4. If found, validate seller/key/part/quantity and save the confirmed result.
5. If 404/not-found, do NOT declare failure. Before the part's deadline, replay
   the original reserve with the SAME key, seller and immutable input. This
   relies on C's agreed durable, atomic idempotency semantics.
6. Lookup failure/malformed result is deferred, not interpreted as proof no
   reservation exists. Unknown write results likewise retain the original key.
7. Backoff after failure and retry budget survive new Reconciler instances and
   process restart. Calls before next_retry_at produce no new request/event.
8. Exhaustion stops at visible RECONCILIATION_REQUIRED. There is no infinite loop,
   silent reset or conflicting new seller reservation.
9. Once the deadline has expired, a missing result is NOT replayed into new late
   work. It stays reconciliation-required. A found existing reservation is
   recorded honestly; delivered/late status is a later polling concern.
10. Explicit REJECTED with agreed terminal no-effect evidence returns the line
    to QUOTING. It does not create a new attempt in the reconciliation method.
    The normal driver can quote again later.
11. Confirmation and its outgoing event commit together. If that transaction
    fails after remote success, the persisted attempt remains reconcilable.

## Budget/schedule
Defaults from Settings (existing .env need not change):
- COORDINATOR_RECONCILIATION_MAX_CYCLES=3
- COORDINATOR_RECONCILIATION_BACKOFF_SECONDS=1
Delay doubles per cycle, capped at 60 seconds. A cycle can include a lookup and
one same-key reserve replay; retry_count counts cycles, not new reservations.
Budget and a schedule are saved before the network call for crash safety.
After a known failed call, backoff is measured from failure completion, not
from its start. Exhausted lines require explicit intervention/compensation;
this driver does not automatically reset the budget.

Normal driver run_order and Reconciler.run_order are separate manual entry
points for now; automatic DB-backed scheduling/recovery is still to be added.
The normal driver leaves an interrupted RESERVING as UNKNOWN. Reconciliation
can then inspect/retry the saved operation under the budget above.

## Draft state/event changes for D review
RECONCILING can return to UNKNOWN when retry is deferred; it can return to
QUOTING only on explicit terminal rejection. These are guarded transitions.
Events line.reconciling, line.reconciliation_deferred, line.reservation_rejected,
line.reserved, and reconciliation.required share the previously proposed
per-order increasing version and outbox envelope.
Snapshots now include retry_count and next_retry_at.
No authoritative contracts/ schemas were edited; these remain review proposals.

## Remaining work
- Cancellation/fencing, safe replacement and delivery polling.
- Kafka consumer and outbox relay; all events are still only saved locally.
- Automatic pending-work/reconciliation scheduling and full restart recovery.
- Real A/C/D API integration, deployment, benchmarks, diagrams/report/demo.
