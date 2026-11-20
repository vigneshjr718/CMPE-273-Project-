# Incremental checkpoint: durable order acceptance

Adds only new coordinator-owned files. No new dependencies or schema migrations
are needed beyond checkpoint 0. This is a direct handler/CLI, **not yet a running
Kafka consumer, reservation orchestrator or outbox publisher**.

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

Verify committed results (on Wendy's local development container):
```bash
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT order_id,status,state_version FROM workflows WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT count(*) FROM processed_events WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT event_type,state_version,published_at FROM outbox WHERE order_id='demo-order-001';"
```

Expected: one workflow at PENDING/version 1, one processed-event record, and one
unpublished workflow.accepted outbox record. Later workflow execution changes
these expected statuses/versions; these checks apply to this isolated checkpoint.

## What is guaranteed by this handler
- Workflow + inbox + outgoing outbox event are one local database transaction.
- Identical replay has no new workflow/outbox effect.
- A new event ID for an identical order records an additional inbox ID only.
- Reuse of an order/event ID for a different business payload is an explicit
  conflict, not an overwrite or successful duplicate.
- Replay returns current persisted status and does not reset later progress.
- Unique keys arbitrate concurrent inserts; a losing transaction retries from
  fresh state up to three times. Exhaustion propagates the DB error for safe
  transport retry, never acknowledgement.
- A returned success follows the DB commit. A later consumer must acknowledge
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

## Evidence and remaining checks
The automated ingestion tests use real SQLite transactions with controlled
inputs and injected failure. They do not establish PostgreSQL race/locking
behavior. Run the two CLI invocations on real PostgreSQL and inspect the rows
before claiming that manual PostgreSQL acceptance/replay check passed.
Additional real concurrent PostgreSQL and Kafka checks remain upcoming.

Checkpoint verification in the preparation environment: 42 tests passed,
including the original 33 foundation tests and nine new ingestion cases.
Wendy's previous checkpoint is pushed as f738f4c on
feature/package-b-coordinator; 33 foundation tests and PostgreSQL migration/
readiness were verified on her Mac. Her saved .env resolves port 55432 after
unsetting the temporary shell override. This incremental code still needs
the same test/CLI verification on her Mac before it is committed/pushed.
