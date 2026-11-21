# Coordinator - Package B

Package B coordinates order fulfillment through durable workflows, seller adapters,
Kafka intake and outbox publication. It owns its service code and database state.

## Current status
Implemented: the component foundation below, transactional order acceptance
(see INGESTION.md), and a persisted normal reservation workflow driven manually
(see RESERVATIONS.md). Quote/reservation calls are isolated behind provisional
seller adapters. Every attempt/key is saved before the external reservation call;
confirmation and outgoing event are saved atomically. PostgreSQL session locks
serialize order drivers. Repeating a completed reservation flow does not reserve
again. Bounded lookup/same-key replay reconciliation is now implemented through
a separate manual driver (see RECONCILIATION.md). Its budget and retry schedule
are persisted, and uncertainty remains visible when it cannot be resolved.
The saved outbox can now be published by a separate bounded Kafka relay CLI
(see OUTBOX.md), using the original event ID/payload and order ID as the key.
Delivery observations can now be checked with a separate per-order polling CLI
(see DELIVERY.md). Poll schedules/backoff and versioned evidence are persisted.
Late or unconfirmed overdue delivery remains visible; confirmed delivery can
complete a line. Confirmed cancellation/fencing and safe replacement are now
available through a separate manual CLI (see REPLACEMENT.md). Unknown cancellation
stays visibly pending; replacement requires a persisted terminal fence receipt.

Incoming Kafka order intake is available through a separate bounded/watch CLI
(see ORDER_CONSUMER.md), with explicit offset commits after durable acceptance.
It creates PENDING workflows without invoking sellers.

Durable workflow scheduling is now implemented in a separate worker process
(see SCHEDULER.md). Intake creates a durable job atomically; due jobs resume
reservation/reconciliation/delivery/cancellation work under the same order lock.
A two-scenario OS-process restart demo has been verified against real PostgreSQL
with controlled HTTP sellers; see [VERIFICATION.md](VERIFICATION.md).

**Remaining:** live A/C/D integration, broader deployment/failure verification,
benchmarks and final diagrams/report. The manual driver conservatively marks an interrupted
RESERVING attempt UNKNOWN rather than creating a fresh reservation. There is
deliberately no public endpoint that pretends to accept/process orders.

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

The default tests do not require Docker or a running seller/DB/broker. They use controlled
HTTP fixtures and temporary **SQLite** databases for local schema/transaction
checks. Current opt-in PostgreSQL tests are documented in RECONCILIATION.md.
An additional opt-in PostgreSQL/Kafka replay check is documented in OUTBOX.md.

To start the foundation:

```bash
cp -n .env.example .env
python -m uvicorn app.main:app --port 8002 --no-access-log
```

Open http://localhost:8002/health and http://localhost:8002/docs.
Without a configured, migrated database, /ready correctly returns HTTP 503.
/ready checks this component's database and migration revision only. Kafka and
worker readiness must be added to the combined deployment later. The relay is a
separate CLI/process, so /ready does not certify its health or Kafka connectivity.
Both health responses identify the current HTTP implementation as a foundation;
workers=not_started_in_http_process makes the separate-process boundary explicit.

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
docker build -t partstream-coordinator:local .
docker run --rm --env-file .env -p 127.0.0.1:8002:8002 partstream-coordinator:local
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

## Guides and verification
- [INGESTION.md](INGESTION.md): transactional order acceptance and replay.
- [RESERVATIONS.md](RESERVATIONS.md): persisted attempts and normal allocation.
- [RECONCILIATION.md](RECONCILIATION.md): uncertain reservation recovery.
- [OUTBOX.md](OUTBOX.md): Kafka workflow event publication.
- [DELIVERY.md](DELIVERY.md): delivery observations and late detection.
- [REPLACEMENT.md](REPLACEMENT.md): terminal fencing and safe replacement.
- [ORDER_CONSUMER.md](ORDER_CONSUMER.md): Kafka intake and offset handling.
- [SCHEDULER.md](SCHEDULER.md): durable scheduling and worker restart recovery.
- [VERIFICATION.md](VERIFICATION.md): recorded results, reproduction and limits.

Run `alembic upgrade head` after schema updates. Migration `0005_workflow_jobs`
adds/backfills workflow jobs while preserving accepted orders, attempts,
cancellation/delivery evidence and saved outbox events.

Live A/C/D integration, broader deployment verification, benchmarks and final
diagrams/report remain pending against agreed team contracts.
