# Order Service - Package A

Package A accepts a body shop parts list, validates it, stores the accepted
order and an `order.created` outbox event in one transaction, and returns a
stable order ID without waiting for sellers. The relay publishes the outbox
event to Kafka when Kafka is available; unpublished rows remain durable for a
later retry.

## Run locally

```bash
cd services/order
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
uvicorn app.main:app --reload --port 8001
```

For local tests, SQLite is used. Production should use PostgreSQL:

```bash
export ORDER_DATABASE_URL='postgresql+asyncpg://order:password@postgres:5432/order_db'
export ORDER_KAFKA_BOOTSTRAP_SERVERS='kafka:9092'
export ORDER_STATUS_BASE_URL='http://tracker:8004'
alembic upgrade head
uvicorn app.main:app --host 0.0.0.0 --port 8001
```

The API is documented at `/docs`. The customer page is `/`.

## API examples

```bash
curl -X POST http://localhost:8001/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-001' \
  -d '{"lines":[{"part_id":"front-bumper","quantity":1,"needed_by":"2030-01-15T17:00:00Z"}]}'

curl http://localhost:8001/orders/<order_id>
```

Repeated POSTs with the same key and identical payload return the original
order. Reusing a key with a different payload returns `409`.

## Integration contract

See `contracts/order-api.yaml`, `contracts/events/order.created.json`, and
`compose.fragment.yaml`. A owns order creation and the accepted-order database;
B consumes `order.created`; D supplies the final status API used by the UI.

