# Persisted reservation workflow

## Setup and migration
Migration `0002_reservation_flow` introduced persisted line/attempt state. Apply
all current migrations before using any driver. Existing accepted input is retained.

From services/coordinator with .venv active and local PostgreSQL running:
```bash
python -m alembic upgrade head
COORDINATOR_POSTGRES_TESTS=1 python -m pytest -q
python -m app.tools.run_order --order-id demo-order-001 --demo
python -m app.tools.run_order --order-id demo-order-001 --demo
```

Your already accepted demo-order-001 should become RESERVED. Its single line is
assigned seller-1; one attempt/reservation ID is saved; state_version becomes 5.
Repeating the command should preserve the SAME reservation, attempt, version and
event count. If the sample has been advanced with different prior actions, inspect
its state instead of assuming these original counts apply.

**--demo uses controlled in-process seller responses. It does not run C's real
service or maintain real inventory.** The coordinator, migrations, transactions,
per-order lock and outbox records still use your actual PostgreSQL database.
Without --demo the HTTP adapter calls your configured seller URLs using the
provisional contract below; coordinate with C before relying on that integration.

## Test evidence and boundaries
See [VERIFICATION.md](VERIFICATION.md) for current recorded results. Opt-in
PostgreSQL tests use unique synthetic orders and delete only their generated IDs.

Coverage includes persisted-before-send evidence, outbox/state atomic rollback,
safe repeated execution, known rejection failover, lost/invalid replies, crash
between side effect and confirmation, partial outcomes and deadline problems.
Real tests check two independent PostgreSQL connections cannot hold one order
lock, four concurrent deliveries create one business effect, and two coordinator
instances do not make duplicate reservations. They do not establish a fully
distributed deployment's availability or correctness of C's real idempotency.
The process/session lock serializes live drivers; durable attempt identity and
seller idempotency are still essential if a process/connection fails.

## Manual database evidence
```bash
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT order_id,status,state_version FROM workflows WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT line_id,status,attempt_count,current_attempt_id FROM workflow_lines WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT seller_id,status,operation_key,reservation_id FROM line_attempts WHERE order_id='demo-order-001';"
docker exec partstream-coordinator-db psql -U coordinator -d coordinator_db -c "SELECT state_version,event_type,published_at FROM outbox WHERE order_id='demo-order-001' ORDER BY state_version;"
```

Initially expect five unpublished events:
1 workflow.accepted; 2 workflow.started; 3 line.quoting; 4 line.reserving;
5 line.reserved. Before running the outbox relay, published_at is NULL; see OUTBOX.md.

## New persistence
Migration 0002_reservation_flow adds workflow_lines and optional reservation
evidence fields to line_attempts. Existing orders/inbox/outbox rows are preserved.
Accepted Workflow.payload stays immutable. Per-line progress is separate.
Current readiness requires the migration head, including 0005_workflow_jobs.

All events use an increasing version **per order**. Workflow row locks serialize
version allocation. A failed state/event transaction rolls back both. Requests
are performed after the attempt/key transaction commits, never inside that
transaction. A separate PostgreSQL session advisory lock is held by the manual
driver across its short transactions and network calls; it is not a DB row lock
held in an idle transaction during the remote call.

## Provisional C reservation contract -- must be reviewed
POST /reservations
Header: Idempotency-Key: persisted operation_key
Body: order_id, line_id, part_id, quantity, needed_by.
Response fields: seller_id, operation_key, part_id, quantity, status.
- RESERVED requires reservation_id and timezone-aware estimated_delivery_at.
- REJECTED requires no reservation_id and must guarantee NO reservation exists
  or can later commit for this attempt. Only that explicit contractual evidence
  permits trying the next seller. A generic HTTP conflict is not enough.
- A successful response with mismatched key/seller/part/quantity is ambiguous.
- 200/201 accepted responses and 409 explicit business rejection are the local
  proposal; other/malformed errors remain UNKNOWN. Adapt to the final seller
  contract in app/clients/reservations.py, without rewriting the workflow.

Never infer safe failure from a timeout. UNKNOWN stops further reservation
attempts for that line. The reconciler can query/retry the same key (RECONCILIATION.md); terminal
fencing must precede a conflicting replacement (REPLACEMENT.md).
A restarted manual driver encountering saved RESERVING marks it UNKNOWN and
does not immediately resend/replace. The separate durable worker coordinates
reconciliation, polling and cancellation; see SCHEDULER.md.

## Provisional customer/event statuses -- D review required
All line reservations confirmed -> order RESERVED, not delivered/fulfilled.
All lines unfulfillable -> UNFULFILLED.
Mixed reserved/unfulfillable -> PARTIALLY_RESERVED.
Any ambiguous reservation or unavailable quote set -> RECONCILIATION_REQUIRED.
A confirmed reservation whose ETA exceeds the deadline remains recorded as
RESERVED, with reason DELIVERY_AFTER_DEADLINE and an order requiring attention;
it is never erased or misreported as delivered.

Line event payloads contain line_id, part_id, quantity, line_status, order status,
reason and current attempt/seller/reservation evidence where available. Envelope
matches the prior proposed workflow.accepted envelope. Full event/topic/status
contracts remain unapproved drafts; no shared contracts/ files were edited.

## Related operations
Use RECONCILIATION.md for unknown outcomes, REPLACEMENT.md for terminal fencing,
DELIVERY.md for delivery observations, and SCHEDULER.md for automatic resumption.
Live A/C/D integration and deployment/performance evidence remain pending.
