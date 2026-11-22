# Tracker + Status API (Package D)

Consumes `order.created` (A) and `workflow.events` (B) from Kafka and builds a read model of
order, line and seller progress. Serves it at `GET /status/orders/{order_id}`
(contract: [contracts/openapi/status-api.yaml](../../contracts/openapi/status-api.yaml)).

## Guarantees
- **DB first, then offset.** Each event is applied in one DB transaction, together with its
  `event_id` in `processed_events`. Only then is the Kafka offset committed (auto-commit is off).
  A crash in between causes a redelivery, which is a no-op.
- **Dedupe:** by `event_id` (`processed_events` primary key).
- **No regression:** B's events apply only if `state_version` is newer than the last one
  applied for that order. A late `order.created` fills in creation facts and never resets progress.
- **Producer authority:** `order.created` topic accepts producer `order` only; `workflow.events`
  accepts producer `coordinator` only. Violations are recorded and ignored.
- **Timestamps are never used for ordering.**
- **Invalid events:** an unparseable message stops the consumer, and its offset is not committed.
  `/ready` reports it. To skip and record such messages instead, set `TRACKER_SKIP_INVALID_EVENTS=true`.
- **Restarts:** if the consumer fails, it restarts in-process with backoff and resumes from the
  last committed offsets.

## Provisional input shapes
A's and B's event schemas are not in `contracts/` yet. `app/events.py` and `tests/mock_events.py`
copy the shapes from B's coordinator branch and B's draft `order.created` example.
Both files are marked MOCK/PROVISIONAL.

## Run tests (no Docker needed)
```bash
cd services/tracker
python3.12 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
python -m pytest -q
```
Tests use temporary SQLite files. To run them on PostgreSQL instead, set
`TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://...`. **This drops the tracker tables, so use a
disposable database only.** Setting it also enables `test_postgres_concurrency.py`, which covers
row locks and unique-key races that SQLite cannot reproduce. Example using the compose Postgres
(dev port; your local `POSTGRES_HOST_PORT` may differ):
```bash
docker exec -i partstream-postgres-1 psql -U postgres -c "CREATE DATABASE tracker_test OWNER tracker"
TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://tracker:local_only@localhost:5432/tracker_test python -m pytest -q
```

## Run
- In compose (from the repo root; see "Running PartStream" in the root README):
  `docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps up -d --build tracker`
- Locally: `cp .env.example .env`, then `python -m app.run`. This applies migrations, serves on
  :8004 and starts the consumer.
- Consumer only: `python -m app.consumers.event_consumer`.
- `/health` is liveness. `/ready` checks the DB, the migration version and the consumer state.
- To rebuild the projection, start a fresh consumer group (e.g. `TRACKER_CONSUMER_GROUP=tracker-v2`)
  on an empty DB. Topics keep events forever, so a full replay is possible.
