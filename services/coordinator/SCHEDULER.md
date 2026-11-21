# Checkpoint 8: durable scheduling and worker process restart recovery

Base: `2d4479f7dfb2c32e408fc3e9c572ce1176c9ef64`, verified checkpoint7/PR#9.
Coordinator-only increment; no dependency change. **Additive migration
0005_workflow_jobs must run before tests or any updated process.** It creates
workflow_jobs and backfills accepted workflows, preserving existing business
rows, attempt keys, cancellation/delivery schedules and outbox evidence. Terminal
orders get COMPLETE jobs with no due time; unfinished orders get READY jobs.

## Mac verification

Apply the guarded installer from the repository root, then:

```bash
cd services/coordinator
source .venv/bin/activate
python -m alembic upgrade head
python -m pip check
export COORDINATOR_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
COORDINATOR_POSTGRES_TESTS=1 COORDINATOR_KAFKA_TESTS=1 python -m pytest -q
python -m app.tools.demo_restart
```

Keep the existing PostgreSQL/Kafka containers running. Restart any older HTTP,
consumer or relay process after upgrading; readiness now requires revision0005.
The installer does not replace .env, migrate, start background workers, commit,
push or touch teammates' infrastructure. Existing test cleanup handles the new
job FK and still deletes only its own unique synthetic orders.

## Actual OS-process demo, not object recreation

app.tools.demo_restart creates two unique orders in **real PostgreSQL**, starts a
loopback HTTP seller fixture in the parent, and launches worker subprocesses using
the same Python/environment. Only its own generated orders are scanned, using
--order-id; existing demo evidence/shared workflows are not processed.

1. Parent fixture commits a reservation and withholds its HTTP reply. B has saved
   RESERVING and a RUNNING job. The demo kills that exact owned worker process.
   A new PID waits for the durable job to become due, reloads the original attempt,
   queries the original key and records delivery. One attempt/one reservation call.
2. A second order becomes late. Parent fixture commits a terminal fence and
   withholds the cancellation reply. B has saved CANCELLATION_PENDING. The demo
   kills that worker, starts another PID, and retries the SAME cancellation key.
   Only the recovered receipt permits seller-2 reservation, followed by delivery.

Expected result: both orders FULFILLED, jobs COMPLETE with no next run, old
attempt/key preserved in history; case attempt counts1 and2; total reservation
HTTP calls3/logical reservations3, cancellation calls2/terminal fences1. Output
includes killed/recovered PIDs and exit codes. Killed workers have nonzero exit
codes; recovered workers stop gracefully with code0. These are expectations
until the actual Mac verification is recorded in CHECKPOINT_8.md.

The fixture's ledger is memory in the surviving parent, NOT proof C persists
fences or survives seller restart. B's state/schedule is real PostgreSQL. Kafka
is not used by this demo (it is exercised by the opt-in suite). This is a workflow
worker OS-process restart test, not broker/database failover, machine restart,
live-neighbor integration or an all-process simultaneous deployment restart.
Default retains the unique DB evidence; --cleanup removes only its generated rows.
All child processes/sockets are cleaned up by the demo. No unrelated process is
terminated. The local HTTP fixture is synthetic test code, not a production seller.

## Run the coordinator component

These are three separate processes using the same coordinator-owned database:

```bash
# Terminal 1: real keyed order.created -> durable PENDING workflow + job.
python -m app.tools.consume_orders --watch --bootstrap-earliest

# Terminal 2: scan persisted jobs and run due Saga/delivery work.
python -m app.tools.run_workflows --watch

# Terminal 3: publish saved outgoing events; preserve at-least-once relay rules.
python -m app.tools.publish_outbox --watch
```

The consumer's explicit bootstrap policy remains as documented in ORDER_CONSUMER.md.
The HTTP health process is separate; /ready certifies DB/schema, not these worker
processes or Kafka membership. Provide D the coordinator-owned compose.fragment.yaml
proposal for process commands/restart policy; D still owns shared infrastructure.
No hidden scheduler is started inside HTTP or Kafka intake.

Starting the scheduler opts into automatic reservation, reconciliation, delivery
polling and cancellation/replacement of known late reservations, using the existing
**provisional** C adapters. Review C's interfaces before running against live
neighbors. For initial verification, use the isolated demo. An unfiltered worker
also resumes historical unfinished demo orders backfilled by migration; choose
--order-id for an exact synthetic order to preserve older demonstration snapshots.
One bounded scan is the default; --watch repeats until SIGINT/SIGTERM.

```bash
python -m app.tools.run_workflows --order-id YOUR_SYNTHETIC_ORDER
```

## Durable execution and scheduling rules

- accept_order commits job+workflow+inbox+outbox in one transaction. Replayed
  creation never resets a completed job or an advanced workflow.
- Due scans order jobs by next_run_at/order_id, with bounded batch size and order
  concurrency. Each order obtains the SAME PostgreSQL advisory lock as the
  existing manual drivers; peers skip busy orders. No in-memory queue is required.
- Under that lock, a RUNNING claim/counter/deadline commits before external calls.
  next_run_at is now+order_timeout+retry_seconds. Normal transitions do not shorten
  an active job's claim. After process death the lock releases and another worker
  picks up the due persisted claim. This is a recovery cooldown, not a substitute
  for the lock: an expired timer never permits a conflicting live owner.
- Default abrupt-death pickup delay is at most approximately32s (30s order budget
  +2s retry), plus polling/load. The demo uses2s+0.1s. A long order is
  interrupted at its budget and safely resumes saved line/attempt progress.
- Normal/resumed work uses existing durable methods while holding one order lock.
  No nested advisory lock contexts. An interrupted RESERVING attempt becomes
  UNKNOWN and reconciles with its original key, never a fresh reservation key.
- UNKNOWN/RECONCILING use saved retry counts/times. Exhausted uncertainty is parked
  for review; the scheduler does not silently choose cancellation for an unknown
  reservation. Manual explicit fencing remains available through replace_order.
- Delivery uses saved poll/backoff/version evidence; late known reservations enter
  cancellation. Only persisted terminal fencing proof permits a successor. Pending
  cancellation follows its saved key/time/budget and parks if exhausted.
- Recovered explicit rejection can safely quote again. Quote-read failures retry
  with persisted capped exponential backoff. No stock/deadline expiry ends the line
  UNFULFILLED. A newly reserved successor is polled by the next due job pass.
- Successful runs schedule the earliest saved reservation/cancellation/delivery
  work, or mark COMPLETE/PARKED if there is nothing safely executable. A blocked
  line does not prevent scheduling another line's due work in that order.
- Unexpected exceptions/timeouts have saved bounded retries (default5) and capped
  backoff. Exhaustion parks the job with last_error and its counter intact. Quote
  read failures remain visible and continue bounded-frequency reads; they do not
  create ambiguous reservation effects. A manual state transition wakes a parked
  job atomically with its normal workflow/outbox transaction.
- SIGINT/SIGTERM stops selecting queued orders, finishes bounded active work and
  closes clients/engine. Abrupt task cancellation leaves the committed claim and
  attempt intact for restart. No forced marking as failed/cancelled/fulfilled.

Workflow state/outbox transactions remain authoritative; job audit counters do
not add new D-facing workflow statuses/events. An event may commit before the
next job timer is finalized; restart resumes using that saved state. The existing
outbox relay independently discovers committed pending events after its restart.

## Configuration and limits

Optional defaults: scheduler_batch_size20, scheduler_concurrency3,
scheduler_poll_seconds1, scheduler_order_timeout_seconds30,
scheduler_retry_seconds2, scheduler_max_failures5. Settings use COORDINATOR_ prefix.
Existing per-seller timeouts/concurrency, reconciliation/cancellation budgets and
delivery intervals are reused. Order concurrency bounds simultaneous order work;
quote fan-out is additionally bounded per order and delivery calls share a semaphore.
The worker uses direct HTTP to configured seller URLs (trust_env=false); ambient
HTTP proxy settings do not redirect internal/loopback calls.

A/C/D interfaces and deployment arrangements remain provisional; main still has
no neighbor implementation/contracts in the inspected branches. Scheduler tests
use controlled sellers; the restart demo uses controlled HTTP. Neither validates
C's physical dispatch/fencing guarantee. No exactly-once/HA guarantee is implied.
