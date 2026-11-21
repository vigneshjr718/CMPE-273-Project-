# Checkpoint 8 preparation — November 20, 2026

Base: `2d4479f7dfb2c32e408fc3e9c572ce1176c9ef64`, development/PR#9 verified head.
Separate worktree; only services/coordinator/ changes. No dependency changes.
NEW additive migration0005_workflow_jobs must run before checks/process startup.
Existing business rows/evidence and .env are preserved; no background worker is
started by installing/migrating. Unfiltered workers would intentionally resume
historical unfinished orders, so use the isolated demo for verification.

Implemented: atomic intake job, migration backfill, bounded automatic scheduling,
saved claim/retry/next-due audit, shared PostgreSQL order ownership, safe normal/
unknown/delivery/late-cancellation/replacement resumption, visible parking on
exhaustion, manual-transition wake-up, graceful shutdown, and an owned-OS-process
kill/restart demo at reservation and cancellation reply boundaries.

Preparation verification (Python3.12):
- Full suite: **224 passed,15 skipped in16.91s**.
- `python -m pip check`: No broken requirements found.
- `git diff --check`: passed.
- Worker/demo CLI help and Python compilation: passed.
- Guarded installer verified on an isolated clone: refuses local coordinator
  edits before writing, applies exact payload and preserves existing .env.
- Eighteen scheduler/HTTP fixture tests exercise durable claims, saved backoff,
  bounded concurrency/batches, timeout/task cancellation, unchanged attempt keys,
  fencing-before-replacement, parked exhaustion, manual wake-up, idempotent intake
  and exact-order isolation. Migration test preserves saved cancellation/outbox
  evidence and backfills unfinished/complete jobs appropriately.

Docker/local PostgreSQL55432/Kafka19092 are unavailable here. Fifteen opt-in
infrastructure checks were skipped, including two new PostgreSQL checks for
scheduler peer exclusion and ACTUAL owned worker process death/restart. The latter
executes the same two-scenario demo with exact-ID cleanup. No actual PostgreSQL
migration or full process restart is claimed in this preparation environment.
Expected Mac counts: PostgreSQL only237 passed/2 Kafka skips; both opt-ins239
passed. These are expectations, not results. app.tools.demo_restart also awaits
Mac verification. It uses a surviving parent-memory HTTP seller ledger, not C.

Run SCHEDULER.md steps with other unfiltered workflow workers stopped. The demo
scans only its own newly generated orders. It kills only its own exact subprocess
handles and starts fresh PIDs against real PostgreSQL saved state. Parent fixture
and broker/database availability are not failed/restarted; no live C/A/D proof.
Kafka intake/outbox remain independent processes; the existing real broker checks
continue in the full suite. /ready remains HTTP DB/schema readiness only.

Next: Mac migrate/full tests/demo, record actual results, then scoped commit/push
and a frozen review branch based on pr/coordinator-kafka-order-consumer (PR#9).
No checkpoint8 commit/push/PR yet. Remaining: checkpoint9 live integration and
cross-component failure demos;10 benchmarks, numbered diagrams/report/handoff.
Instana deferred. No main merge or teammate message performed automatically.

## Checkpoint 8 Verification
- Installer applied 23 files; existing .env/database preserved.
- pip check: No broken requirements found.
- Full suite: 239 passed in 22.53s.
- Real PostgreSQL restart demo: both owned workers killed and recovered
  using fresh processes; original attempt/key preserved.
- Reservation case: FULFILLED/version8, one DELIVERED attempt.
- Cancellation case: FULFILLED/version13, CANCELLED then DELIVERED.
- Both jobs COMPLETE; run counts 2 and 3.
- Controlled HTTP seller; live C integration remains pending.
