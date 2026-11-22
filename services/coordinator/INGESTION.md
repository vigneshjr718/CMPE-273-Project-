# Durable order acceptance

The direct handler/CLI accepts orders transactionally. Kafka intake uses the same
handler; see [ORDER_CONSUMER.md](ORDER_CONSUMER.md). Migrate the current schema
before use; scheduling is covered in [SCHEDULER.md](SCHEDULER.md).

## Local verification
From services/coordinator with the Python 3.12 virtual environment active:
```bash
python -m pytest -q
python -m app.tools.accept_order --file examples/order-created.json
python -m app.tools.accept_order --file examples/order-created.json
```

With your saved .env and migrated PostgreSQL running, the first CLI invocation
should report outcome=created; the second should report duplicate_event.
If this sample was already accepted, both legitimately return duplicate_event.
The data is synthetic; the 2030 example dates are illustrative.

Verify committed results (using the example local PostgreSQL container):
```bash
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT order_id,status,state_version FROM workflows WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT count(*) FROM processed_events WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT event_type,state_version,published_at FROM outbox WHERE order_id='demo-order-001';"
```

Expected: one workflow at PENDING/version 1, one processed-event record, and one
unpublished workflow.accepted outbox record. Later workflow execution changes
these expected statuses/versions. Inspect these rows before any workflow worker
or outbox relay processes the order.

## What is guaranteed by this handler
- Workflow + inbox + outgoing outbox event + scheduling job are one local
  database transaction.
- Identical replay has no new workflow/outbox effect.
- A new event ID for an identical order records an additional inbox ID only.
- Reuse of an order/event ID for a different business payload is an explicit
  conflict, not an overwrite or successful duplicate.
- Replay returns current persisted status and does not reset later progress.
- Unique keys arbitrate concurrent inserts; a losing transaction retries from
  fresh state up to three times. Exhaustion propagates the DB error for safe
  transport retry, never acknowledgement.
- A returned success follows the DB commit. The Kafka consumer acknowledges
  Kafka only after this succeeds.

Idempotency compares the validated business payload; event metadata differences
such as a new occurrence timestamp do not change that payload. Preserve payload
line order/normalized representation until a canonical team contract is agreed.
Workflow.payload must remain the immutable accepted input; store execution
progress in separate state fields/tables instead of mutating that input.

## Proposed outgoing contract for D review
event_type=workflow.accepted, schema_version=1, producer=coordinator;
top-level event_id, occurred_at, order_id, state_version;
payload contains status=PENDING and the accepted lines.
One state_version per order, as proposed in integration-proposal.md.
Outbox.payload stores the complete wire envelope, not just the inner payload.
This local draft does not modify contracts/ or claim D has approved it.

## Verification scope
Local ingestion tests use actual temporary SQLite transactions and injected
failures. Opt-in PostgreSQL and Kafka tests additionally cover persistence,
concurrent acceptance and source-record replay. See [VERIFICATION.md](VERIFICATION.md)
for recorded results and the distinction between infrastructure and live-neighbor
integration evidence.
