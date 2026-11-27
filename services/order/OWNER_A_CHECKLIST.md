# Owner A completion checklist

## What Package A owns

- Order submission and validation.
- Stable order and line IDs.
- Durable accepted-order storage.
- Duplicate submission handling with `Idempotency-Key`.
- Atomic order plus `order.created` outbox record.
- Kafka relay that retries pending outbox records.
- Minimal landing page, order form, and order lookup.
- Order API tests, Docker/configuration, health checks, logs, and documentation.
- Public routing and the end-to-end smoke test.
- Draft system-overview diagram and final report assembly.

## Files to commit

Commit the entire `services/order/` directory and these shared contracts:

```text
services/order/
contracts/order-api.yaml
contracts/events/order.created.json
```

Do not commit `.env`, `.venv`, `order.db`, or secrets.

## Local verification

```bash
cd services/order
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m compileall -q app
python -m alembic upgrade head
uvicorn app.main:app --port 8001
```

Expected test result: all tests pass. Visit `http://localhost:8001/` and
`http://localhost:8001/docs`.

## Evidence to capture

1. `/health` returns `200`.
2. POST `/orders` returns `202` with an `order_id` and line IDs.
3. Repeat the exact POST with the same `Idempotency-Key`; it returns the same
   order ID and does not create another order.
4. Reuse that key with a changed part or quantity; it returns `409`.
5. GET `/orders/{order_id}` returns the accepted order in `PROCESSING` state.
6. Show the database contains one order, its lines, and one pending outbox row.
7. With Kafka enabled, stop Kafka, submit an order, restart Kafka, and show the
   outbox row is eventually marked published.

## Integration decisions to send to the team

Ask B and D to confirm:

- `order.created` topic name and partition key. The current proposal uses the
  order ID as the Kafka key.
- The event envelope and schema version in `contracts/events/order.created.json`.
- B's consumer group and internal network name.
- D's final status API URL and response shape. Until then the UI honestly shows
  local `PROCESSING` status and does not claim that sellers have completed.
- The final PostgreSQL database name, role, password handling, and Compose
  service names.

## Suggested Git workflow

```bash
git checkout -b feature/package-a-order-service
git add services/order contracts/order-api.yaml contracts/events/order.created.json
git diff --cached --check
git commit -m "Add package A order service and customer flow"
git push -u origin feature/package-a-order-service
```

Open a pull request and ask D to review the Compose fragment and A/D to review
the API/status contract. Do not merge root Compose changes without agreeing on
the shared PostgreSQL, Kafka, and service names.

