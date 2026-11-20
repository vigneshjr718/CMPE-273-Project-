# Checkpoint 6 preparation — November 20, 2026

Base: `4599229657cf4fabba81ff77f4380639c5efab76`, fetched from
`feature/package-b-coordinator`; PR #7 review head has the same base commit.
Implementation was prepared in a separate checkout. No verified work, frozen
review branch, teammate service, shared infrastructure or remote branch changed.

Implemented: bounded cancellation/fencing by original key, durable terminal
receipt validation, explicit pending/exhausted cancellation, delivery-vs-cancel
race handling, fresh safe replacement attempts, guarded quote resumption, manual
CLI and controlled demo. Additive migration: 0004_cancellation. No new dependency.
See REPLACEMENT.md and integration-proposal.md for the provisional C/D contracts.

Preparation checks (Python 3.12):

- Before changes: `python -m pytest -q` → 117 passed, 9 skipped in 9.95s.
- Final suite: `python -m pytest -q` → **165 passed, 11 skipped in 16.62s**.
- `python -m pip check` → No broken requirements found.
- `git diff --check` → passed.
- Guarded installer checked against an isolated clone: refuses local changes
  before writing; applies every intended file and preserves an existing .env.
- SQLite migration upgrades from saved checkpoint 5 preserve workflow version,
  reservation/key, delivery counter/schedule and outbox payload. PostgreSQL
  migration SQL compiles; this does not claim an actual PostgreSQL migration run.

Eleven opt-in checks require real PostgreSQL/Kafka, unavailable in this preparation
environment. These include two new PostgreSQL cancellation persistence/locking
checks. They and `python -m app.tools.demo_replacement` must be verified on the
Mac before calling checkpoint 6 verified or committing/pushing it. Expected
counts with PostgreSQL only: 175 passed/1 Kafka skip; with both: 176 passed.
Those counts are expectations, not results. No live C/A/D integration occurred.

Known remaining scope: incoming Kafka order consumer/offsets (7), automatic
scheduling and full process restart recovery (8), live integration/demonstrations
(9), benchmarks, numbered diagrams/report/final handoff (10). Instana deferred.
The terminal fencing guarantee depends on C's unapproved contract, not this
coordinator's fixture. Exhausted cancellations require review; no automatic
unsafe reset or conflicting replacement is provided.

After Mac verification: review only services/coordinator/, commit on the
existing development branch, then make a new focused review branch based on
pr/coordinator-delivery-polling-and-late-detection. Do not update PR #7's frozen
head or merge main automatically. No checkpoint 6 commit/PR has been created.

176 passed in 11.50s with real PostgreSQL/Kafka; pip check passed.
Demo demo-replacement-d21588d1341f: versions 6 → 8 → 13;
late → cancellation pending → confirmed fence → seller-2 reserved.
Two reserve calls, two cancellation calls; immediate repeat skipped.
Controlled sellers/clock; live C integration remains pending.
