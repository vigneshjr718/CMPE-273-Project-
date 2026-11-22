# Confirmed cancellation and safe replacement

The seller fencing contract is provisional. Controlled demos validate coordinator
behavior, while live C integration and C's persistent fencing require joint checks.

## Local verification
From the repository root with the Python virtual environment created:

```bash
cd services/coordinator
source .venv/bin/activate
python -m alembic upgrade head
python -m pip check
COORDINATOR_POSTGRES_TESTS=1 COORDINATOR_KAFKA_TESTS=1 python -m pytest -q
python -m app.tools.demo_replacement
```

Existing PostgreSQL/Kafka containers must be running. Use the existing `.env` and
`COORDINATOR_KAFKA_BOOTSTRAP_SERVERS=localhost:19092` export as needed. There is
no dependency change. Migration **0004_cancellation** adds only cancellation_checks;
previous migrations/data, saved operation keys, delivery checks and outbox are
preserved. Restart an older HTTP process to load the new schema readiness check.

The demo uses a fresh synthetic order and real PostgreSQL with an in-process
seller ledger and controlled clock. Expected behavior: late seller-1 -> lost
cancellation reply -> CANCELLATION_PENDING with one reservation -> repeat skips
until due -> identical cancellation key recovers receipt -> seller-2 RESERVED
with a new attempt/key. Expected reserve_calls=2, cancel_calls=2, one terminal
fence. Recorded results are in VERIFICATION.md; this is controlled seller evidence.
The demo retains its unique order as evidence and does not contact seller HTTP
endpoints or publish Kafka records. Kafka is exercised by the separate opt-in test.

Production-adapter manual batch, only after C agrees/implements the proposal:

```bash
python -m app.tools.replace_order --order-id YOUR_ORDER_ID
```

This command intentionally requests cancellation of late RESERVED attempts and
unresolved UNKNOWN/RECONCILING/RECONCILIATION_REQUIRED reservation attempts. Use
`reconcile_order` instead when the goal is to recover an original uncertain
reservation without cancelling it. Timely reserved and delivered lines are skipped.
Invoking replacement is a cancellation decision, not a read-only status check.

## Cancellation contract proposed to C

POST `/reservation-operations/{original_operation_key}/cancel`, header
`Idempotency-Key: <persisted cancellation_key>`, JSON body order_id, line_id,
part_id, quantity, reservation_id (nullable for a lost original reservation reply).
Return HTTP 200 with evidence, or 202 with PENDING only:

```json
{
  "seller_id": "seller-1",
  "operation_key": "reserve-ORIGINAL_KEY",
  "cancellation_key": "cancel-STABLE_KEY",
  "part_id": "bumper",
  "quantity": 1,
  "reservation_id": "original-reservation",
  "status": "CANCELLED",
  "status_version": 2,
  "fenced": true,
  "cancellation_receipt": "durable-receipt-1",
  "delivered_at": null
}
```

CANCELLED must attest **no delivery has occurred and none can occur**, including
an older in-flight reserve/delivery operation and any later replay of the original
key. C must atomically fence the key and release any reservation, retaining a
durable tombstone even if reservation lookup currently returns not-found. Stock
release alone, a transient cancellation acknowledgement, or a TTL expiry is
insufficient. A physically dispatched delivery that cannot be stopped stays
PENDING; it cannot honestly return this terminal receipt. Fences/idempotency and
terminal replies survive C restart. B cannot implement those guarantees for C.

PENDING has fenced=false, no receipt and no delivery timestamp. DELIVERED has
fenced=false, no receipt, exact reservation identity and aware delivered_at; B
records delivery and never replaces that attempt. For an unknown original, C may
return a newly discovered reservation ID, or null only with a terminal key fence.
Every reply must identify the exact seller/original key/cancellation key/part/
quantity, and the reservation ID when B already knows it.

status_version is a monotonic C counter for the original reservation operation,
shared with its delivery observations (using the original key even before a reservation ID exists). Identical observations
keep their version. Changed payloads at the same version and lower versions are
rejected; a terminal change must exceed B's prior IN_TRANSIT version. C must
confirm this shared counter before integration. Never synthesize it from time.
An HTTP error, timeout, malformed/mismatched reply, 404 or unproved 202 is never
fencing proof. Responses are evidence only under this agreed seller guarantee;
a boolean or a receipt string from an incompatible endpoint is insufficient.

## Durable coordinator behavior

- Save cancellation intent/key, request count and next retry time before HTTP.
  The original reservation operation key and attempt stay intact.
- Hold the same PostgreSQL per-order advisory lock used by reservation,
  reconciliation and delivery drivers. Replies also check current ownership.
- Timeout/invalid/pending evidence leaves line CANCELLATION_PENDING and order
  RECONCILIATION_REQUIRED. Delivery/reconciliation/normal drivers cannot bypass it.
- Retry only the same persisted cancellation key, with bounded exponential
  backoff after the response. A crashed claim consumes budget. Exhaustion keeps
  the line pending, check REQUIRED and no next retry. Repeating the command or
  recreating the driver does not reset a REQUIRED check. Manual review with C is
  needed; no unsafe retry-reset endpoint is provided by the coordinator.
- Confirm terminal proof, old attempt CANCELLED, line REPLACING and line.cancelled
  event atomically. Persisted proof is revalidated before replacement, including
  when a normal driver resumes QUOTING after interruption.
- Requote using the current clock/deadline and exclude previously attempted
  sellers. A safe successor gets a fresh attempt/key; a rejection can try another
  seller. An ambiguous successor stays UNKNOWN for same-key reconciliation.
- No stock/deadline expiry yields UNFULFILLED; failed quote reads yield visible
  RECONCILIATION_REQUIRED and can be retried through the replacement command
  without recancelling. If a delivery wins the race, it becomes DELIVERED instead.
- A fresh driver resumes committed cancellation/quoting intent. Automatic scanning
  and the full worker process restart demo are described in SCHEDULER.md.

## Events and evidence for D

New proposed events: line.cancelling, line.cancellation_deferred, line.cancelled,
line.replacement_started. Existing line.reserving now includes replaces_attempt_id
(null for initial allocation), identifying the last cancelled predecessor.
Events retain B's per-order state_version and saved outbox/event ID protocol.
Cancellation snapshots expose status, key, count, error, receipt and next retry
for the current attempt. Old proof/history remains in cancellation_checks,
line_attempts and outbox after a replacement takes ownership of the line.

## Verification

Recorded verification and remaining integration limits are in VERIFICATION.md.
Temporary SQLite tests exercise actual coordinator transactions with controlled
sellers; opt-in PostgreSQL tests additionally verify committed cancellation
intent and peer-driver lock exclusion. They do not prove C's implementation or
end-to-end exactly-once behavior.
