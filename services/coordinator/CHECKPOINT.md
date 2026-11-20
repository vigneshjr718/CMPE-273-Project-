# Package B checkpoint 0 handoff

Base repository: https://github.com/vigneshjr718/CMPE-273-Project-
Inspected main commit: 01da3c8ed9e9609f3d28e8f0750486e2b5cd0d03.
Local working branch: feature/package-b-coordinator.
All new project files are under services/coordinator/.
No remote push or PR has been made.

## Complete at this checkpoint
- Service-local foundation and documented interface assumptions.
- Live health vs dependency/schema readiness.
- JSON application logs/request IDs, bounded dependency probes.
- SQLAlchemy schema and versioned Alembic migration.
- Draft line-state guards and stable, attempt-specific operation keys.
- Concurrent quote selection, bounded fan-out/timeout, and isolated HTTP adapter.
- Meaningful schema/transaction, contract, state, HTTP and concurrency tests.

## Verification
CPython 3.12 in an isolated environment.
Test command: python -m pytest -q.
Result: 33 passed in 4.15 seconds; no warnings. pip check also passed.
Live Uvicorn smoke check: /health returned 200; /ready returned the expected
503 with no migrated PostgreSQL configured. Server terminated cleanly.
Migration-generation template compiles and git diff --check passed.
SQLite file-backed migrations up/down, SQL transaction rollback and uniqueness
checks use the actual Alembic migration/ORM, not a mocked DB.
PostgreSQL migration DDL compiles offline.
Quote HTTP uses controlled httpx fixtures; no real C implementation was tested.
No running PostgreSQL, Kafka, Docker or team-service integration has been tested
in this environment. Docker/postgres executables are unavailable here.
The Dockerfile/Compose fragment are supplied for verification on Wendy's Mac
or D's environment. Package B is NOT fully implemented or Sunday-complete yet.

## Next implementation checkpoint
1. Reconcile proposals with A/C/D contracts and current repository changes.
2. Implement atomic accepted-order consumption and order/line workflow state.
3. Implement durable reservation attempt execution and reconciliation.
4. Add Kafka consumer/outbox relay with replay-safe behavior.
5. Add cancellation/replacement, delivery polling, restart recovery and tests.
6. Run real PostgreSQL/Kafka plus teammate integrations and prepare report/demo.

## Presentation notes
- Concurrent reads avoid waiting sequentially for all sellers.
- Request timeout on a write means unknown outcome, not proven failure.
- Operation keys remain stable across retries but change for a new attempt.
- State transitions require durable evidence; draft enums are only guardrails.
- Outbox/inbox tables support later reliability logic; their existence alone
  does not implement it.
