# Coordinator - Package B, checkpoint 0

This is Wendy's **component-local foundation**, following Alicia's structure.
No files belonging to A/C/D or root infrastructure are changed.

## Current status
Implemented: configuration, JSON application logs/request IDs, liveness/readiness,
SQLAlchemy table definitions and an Alembic migration, draft boundary models and
line-state rules, stable reservation-key generation, and concurrent quote-only
selection through an isolated HTTP adapter.

**Not implemented yet:** order Kafka consumer, durable workflow orchestration,
reservation writes, reconciliation, cancellation/replacement, delivery polling,
outbox relay and recovery workers. An available schema or a passing health check
does not mean the complete business workflow works. There is deliberately no
public endpoint that pretends to accept/process orders.

All incoming-event/quote shapes and topic names are **proposals**, not approved
team contracts. See [integration-proposal.md](integration-proposal.md).
Shared contract files should be a separate reviewed change later.

## Quick local verification (Python 3.12)
From the repository root:

```bash
cd services/coordinator
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

The tests do not require Docker or a running seller/DB/broker. They use controlled
HTTP fixtures and temporary **SQLite** databases for local schema/transaction
checks. These are not PostgreSQL locking or Kafka integration tests.

To start the foundation:

```bash
cp .env.example .env
python -m uvicorn app.main:app --port 8002 --no-access-log
```

Open http://localhost:8002/health and http://localhost:8002/docs.
Without a configured, migrated database, /ready correctly returns HTTP 503.
/ready checks this component's database and migration revision only. Kafka and
worker readiness must be added when workers exist. Both health responses identify
the current implementation as a foundation.

## PostgreSQL migrations
Ask C/D for the coordinator-only role/credentials and database connection. The
example credentials are synthetic local placeholders, not deployment secrets.
Configure COORDINATOR_DATABASE_URL in .env, then:

```bash
python -m alembic upgrade head
python -m uvicorn app.main:app --port 8002 --no-access-log
```

Run migrations as an explicit deployment step, once, before starting replicas.
No startup create_all or automatic destructive database reset is used.
The downgrade command removes coordinator-owned tables; use only on a disposable
test database. Do not run it against data you need.

## Container
The Docker build context is **services/coordinator**, not the repository root:

```bash
docker build -t partstream-coordinator:checkpoint0 .
docker run --rm --env-file .env -p 127.0.0.1:8002:8002 partstream-coordinator:checkpoint0
```

Inside a container, localhost refers to that container. Set database/seller/
broker addresses to the agreed Compose service names rather than host addresses.
Provide D the [compose.fragment.yaml](compose.fragment.yaml) example; D owns
merging it into infra/docker-compose.yml. No DB/broker/seller services are
created by this package.

## Dependencies
Requirements and transitive constraints record the Python 3.12 resolution tested
here. These are service-local candidates pending team version agreement.
requirements-dev.txt adds test dependencies; they are not installed by Docker.
The constraints file includes test-package pins but they are installed only by
requirements-dev.txt. Do not commit .env, virtual environments or test DBs.

## Verification and next checkpoint
See [CHECKPOINT.md](CHECKPOINT.md) for exact verification limits.
Next: accept order events transactionally, persist workflows, then connect
reservation/reconciliation adapters. Save an attempt/key before external writes.
