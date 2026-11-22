# Coordinator verification

## B/D contract and deployment preparation — November 22, 2026

Base B code: f788c56. D PR #12 source:
81e2f9f7d8f1c5fd5b45b6ae004527d63052a309. Python 3.12.

- Full coordinator suite with the optional real D-source projection checks:
  **249 passed, 15 skipped in 26.23 seconds**. PostgreSQL/Kafka are unavailable
  in this preparation environment; their 15 opt-in checks were not rerun here.
- The 25 added checks include 12 emitted-event schema cases, 12 cross-component
  projection cases and one event-catalog drift check. B generates actual outbox
  records; D's real decoder/projector/API view processes them in a fresh Python
  interpreter against a temporary SQLite DB. Responses validate against D's
  OpenAPI schema. Sellers and broker delivery order are controlled.
- D's existing suite independently ran **41 passed, 3 skipped in 2.67 seconds**.
  Its PostgreSQL opt-ins were not run. No D source was changed.
- Compose v2.39.4 successfully rendered D's base file with B's integration overlay
  using synthetic example config. Migration dependency ordering, isolated worker
  profile and absence of worker host-port publication were checked. This is
  configuration validation, not a running Docker/cloud deployment.
- `pip check` and `git diff --check` passed. No runtime dependency/schema changes;
  jsonschema and PyYAML were added only to development requirements.
- Known D finding: cancellation followed by no stock produces UNFULFILLED with
  a single CANCELLED attempt, but D currently returns replaced=true. See
  TRACKER_INTEGRATION.md and contracts/status-values.md for the proposed meaning.
  Projection/schema compatibility checks do not claim that flag semantics passed.
- No live A producer, C seller, real B-to-D Kafka path, cloud rollout, shared
  contract approval, main merge or teammate message is claimed by these checks.

Reproduction and the deployment handoff are in TRACKER_INTEGRATION.md. The earlier
real PostgreSQL/Kafka results below remain historical evidence, not new runs.

Recorded November 20, 2026, against commit `5e529e8` on Python 3.12/macOS.
Results are a historical run, not a guarantee of future test counts or live-neighbor
compatibility. The full run used real coordinator PostgreSQL and Kafka.

## Reproduce
From services/coordinator, activate the virtual environment and use the configured
local PostgreSQL/Kafka services. Apply migrations before starting updated processes:

```bash
source .venv/bin/activate
python -m alembic upgrade head
python -m pip check
export COORDINATOR_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
COORDINATOR_POSTGRES_TESTS=1 COORDINATOR_KAFKA_TESTS=1 python -m pytest -q
python -m app.tools.demo_restart
```

Latest recorded full suite: **239 passed in 22.53 seconds**, with no skips.
`pip check`: no broken requirements found. Schema: `0005_workflow_jobs`.
Without infrastructure opt-ins, tests use temporary SQLite and controlled HTTP/
client fixtures and skip PostgreSQL/Kafka checks. Those local tests alone do not
establish PostgreSQL locking or broker integration.

## Worker process recovery
The demo used real PostgreSQL and controlled loopback HTTP sellers whose ledger
survived in the parent process. Each owned worker was killed with SIGKILL and a
fresh OS process resumed its committed state. Original attempt/key preserved:

| Interruption | Recovered result | Attempts | Job |
| --- | --- | --- | --- |
| RESERVING | FULFILLED, version 8 | One DELIVERED | COMPLETE, 2 runs |
| CANCELLATION_PENDING | FULFILLED, version 13 | CANCELLED, then DELIVERED | COMPLETE, 3 runs |

Killed/recovered PIDs were 4693/4694 and 4695/4696; exit codes -9/0 in both cases.
Total reservation HTTP calls/logical reservations: 3/3. Cancellation calls: 2;
terminal fences: 1. Synthetic orders used prefix `demo-restart-aaf8fead03c1`.
Cleanup was disabled, retaining demo evidence. See SCHEDULER.md for isolation and
the option to remove only a demo's generated rows.

## Other recorded scenarios
- Cancellation/replacement: 176 tests passed in 11.50 seconds at that revision.
  Controlled seller/clock demo progressed through late reservation, lost cancel
  reply, pending cancellation, terminal receipt and seller-2 reservation. Versions
  6/8/13; two reserve calls and two cancel calls; premature repeat skipped.
- Kafka intake: 218 tests passed in 13.17 seconds at that revision. Real DB/broker
  demo committed acceptance before an injected offset-commit failure. Fresh intake
  safely replayed; workflows/inbox/outbox totals were 2/3/2, broker next offsets
  partition 0=3 and partition 1=1. A fresh consumer handled zero further records.
  Poison JSON stopped intake at offset 3 and blocked later valid input.
- Foundation tests covered actual SQLite migrations/rollback/uniqueness, offline
  PostgreSQL DDL, and HTTP health 200 versus readiness 503 without a migrated DB.
  Foundation preparation suite: 33 passed in 4.15 seconds.

Earlier preparation runs without infrastructure were: cancellation/replacement
165 passed/11 skipped; Kafka intake 205 passed/13 skipped; scheduling/restart
224 passed/15 skipped. These are preparation results, superseded for real local
infrastructure coverage by the macOS runs above. Detailed development notes remain
in Git history; topic guides describe the current operation and contracts.

## Limits and outstanding evidence
- Controlled sellers do not prove C's durable idempotency, dispatch fencing,
  inventory correctness or survival across seller restart.
- Consumer-client recreation and worker process restart are distinct tests.
  Neither establishes broker/database failover, host restart, simultaneous
  deployment restart, high availability or end-to-end exactly-once behavior.
- HTTP /ready checks DB/schema only, not separate workers or Kafka connectivity.
- Live A/C/D contracts/integration, broader deployment checks, benchmarks and
  final diagrams/report remain pending. Instana instrumentation is deferred.

## Coordinator–tracker integration verification — November 22, 2026
- Full coordinator suite: 264 passed in 29.76 seconds; no skips.
- PostgreSQL/Kafka opt-ins and separate D-source compatibility checks enabled.
- pip check: No broken requirements found.
- B/D projection checks use temporary SQLite and controlled sellers.
- Live cloud/broker-to-tracker deployment remains pending.
