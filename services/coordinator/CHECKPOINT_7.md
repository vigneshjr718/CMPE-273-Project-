# Checkpoint 7 preparation — November 20, 2026

Base: `18256269b1cf3aed58d9166a0a4f9b7d41f71c80`, fetched from development and
PR #8's frozen head. A separate worktree preserves checkpoint 6 work. Only
services/coordinator/ files change; no migration, dependency or .env replacement.
Current required schema: 0004_cancellation.

Implemented: incoming Kafka order transport, explicit next-offset commit after
transactional acceptance, per-partition ordering, bounded batches/timeouts,
assignment/rebalance guards, deliberate first-offset bootstrap, safe poison/
conflict stop, one-shot/watch CLI, graceful stop and an isolated real DB/broker
demo. Existing inbox/workflow/outbox transaction and replay handling are reused.
Acceptance creates PENDING workflows; automatic Saga scheduling remains next.

Preparation verification (Python 3.12):
- Full suite: **205 passed, 13 skipped in 11.73s**.
- `python -m pip check`: No broken requirements found.
- `git diff --check`: passed.
- `python -m app.tools.consume_orders --help`: passed.
- Guarded installer verified on an isolated clone: refuses local work before
  writing, applies the exact payload and preserves an existing .env.
- Forty new local tests exercise transaction rollback/timeout, explicit offsets,
  duplicate/conflicting deliveries, lost commit acknowledgement, poison records,
  independent partitions, rebalances and graceful/task-cancel shutdown.

Thirteen infrastructure checks were skipped: Docker is unavailable and local
PostgreSQL55432/Kafka19092 are not running in the preparation environment.
Two new opt-in checks cover real PostgreSQL offset-failure/replay and the real
PostgreSQL/Kafka two-partition demo. These and app.tools.demo_order_consumer
remain to be run on Wendy's Mac. Expected Mac counts: PostgreSQL only216 passed/
2 Kafka skips; both opt-ins218 passed. These are expectations, not verified results.

No fresh A/D contract/source exists in inspected remote branches. Topic/envelope/
key/group semantics remain provisional, documented in ORDER_CONSUMER.md and
integration-proposal.md. No live A/D integration or end-to-end exactly-once claim.
The demo recreates consumer clients against real persistent DB/broker state;
full OS-process restart proof remains checkpoint8. Poison/conflicting records
stay unacknowledged with safe coordinates in logs; no silent skip/unapproved DLQ.

Next: apply the guarded increment on feature/package-b-coordinator at1825626,
run Mac checks/demo, then record actual results before commit/push and a new
focused review branch/PR based on pr/coordinator-confirmed-cancellation-and-safe-replacement.
No checkpoint7 commit/push/PR has yet been created. Keep frozen PR heads unchanged.

218 passed in 13.17s with real PostgreSQL/Kafka; pip check passed.
Demo 672a8d5426064f8a8a1528f44b173fe3 verified DB-before-offset
replay, duplicate handling, partition offsets 0:3/1:1, and poison-input stop.
DB workflows/inbox/outbox: 2/3/2. Fresh consumer processed zero.
Later valid input stayed blocked. Full OS-process restart/live A/D remain pending.
