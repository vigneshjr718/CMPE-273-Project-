# Incoming Kafka orders and offset handling

Intake accepts workflows into PENDING/version1 through the transactional handler.
The separate worker schedules reservation, reconciliation and delivery work; see
SCHEDULER.md. A/D topics and envelopes remain provisional, pending live integration.

## Local verification
From the repository root, with the Python virtual environment created:

```bash
cd services/coordinator
source .venv/bin/activate
python -m pip check
export COORDINATOR_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
COORDINATOR_POSTGRES_TESTS=1 COORDINATOR_KAFKA_TESTS=1 python -m pytest -q
python -m app.tools.demo_order_consumer
```

Keep existing PostgreSQL/Kafka containers running and current migrations applied
(run `python -m alembic upgrade head`; current head is 0005_workflow_jobs).
The real demo creates a fresh synthetic two-partition topic, consumer group and
orders. It does not read/reset shared topics/groups or contact any seller.
Expected sequence (recorded run in VERIFICATION.md):

1. Real broker delivers an order; workflow+inbox+outbox commit together. A
   controlled failure occurs before the Kafka offset commit. Broker offset is null.
2. Fresh consumer, same group, replays the saved order and handles explicit
   duplicates plus a second order on the other partition. Totals: processed4,
   created1, duplicate_event2, duplicate_order1. DB: workflows2/inbox3/outbox2.
3. Broker next offsets are partition0=3 and partition1=1. Another fresh member
   reads zero already handled records. Workflows remain PENDING/version1.
4. Invalid JSON at partition0/offset3 stops the consumer. A valid order at offset4
   is not accepted; committed next offset stays3. Expected error log at this
   deliberately injected boundary is part of the demo, not a failed assertion.

Default demo retains its unique topic/rows as evidence, including the deliberately
blocked later record. `--cleanup` deletes only that demo's exact generated DB rows
and topic; unique group metadata remains until Kafka expires/deletes it. The
opt-in test uses cleanup. The demo is fresh consumer recreation, not a full
OS-process restart demonstration or an actual producer/consumer network outage.
There are no seller calls, Saga execution or workflow-topic publishing here.
Kafka ingestion and PostgreSQL are real; the offset-commit failure is injected.

## Manual consumer commands

Pre-provision A/D's agreed incoming topic before starting a shared consumer.
The existing COORDINATOR_INCOMING_TOPIC and COORDINATOR_CONSUMER_GROUP select it.
Use an isolated local topic/group while contracts are provisional.

```bash
# One bounded poll/batch. First use of a group needs an explicit bootstrap choice.
python -m app.tools.consume_orders --bootstrap-earliest

# Continuous intake; SIGINT/SIGTERM finishes the current record, then closes.
python -m app.tools.consume_orders --watch --bootstrap-earliest

# Resume only broker-saved offsets; missing/out-of-range offsets fail closed.
python -m app.tools.consume_orders --watch
```

--bootstrap-earliest seeks to the earliest retained record **only for partitions
with no committed group offset**. It never resets an existing offset. Empty
partitions may still have no committed offset at the next startup; continue using
the explicit bootstrap option until they have records/commits, or let default
startup fail. This flag also makes rereading retained input an explicit operator
choice if group offsets have expired. It cannot recover input Kafka has deleted.
No automatic latest/earliest reset is configured: auto_offset_reset=none.

One-shot mode exits after one bounded poll, even if the topic was briefly empty;
it does not claim the topic is drained. Watch mode stays active until stopped or
an error occurs. Both start independently of the HTTP process; /ready continues
to check the database/schema, not consumer membership or Kafka connectivity.

## Proposed A/D wire boundary

- Topic: configured incoming_topic (default order.created); group: coordinator-v1.
- Kafka key: exact UTF-8 payload.order_id. Missing/mismatched keys fail visibly.
- Value: UTF-8 JSON matching the existing OrderCreatedEvent model in
  schemas.py / examples/order-created.json: event_id UUID, event_type order.created,
  schema_version1, producer order, aware occurred_at, payload with order_id and
  unique line IDs/positive quantities/aware needed_by timestamps.
- Reject tombstones, empty/oversized values, duplicate JSON keys, nonstandard
  numbers, unknown fields/versions/types, conflicting order/event identity.
  Headers do not override the envelope identity. Historical deadlines remain
  parseable; Saga eligibility uses the current time when scheduled later.
- Consumer reads committed transactional Kafka records and disables auto-commit.
  Adapt translations only after A/D review; no shared contracts were edited.

## Offset and failure rules

Every record follows: decode -> transactional accept_order -> commit explicit
{that_topic_partition: record.offset+1}. No bare commit() call, concurrent batch
commit or acknowledgement of prefetched/unhandled records is used. Records are
processed serially in each fetched batch; partitions have independent offsets.
Offset gaps are permitted (Kafka compaction/aborted records can leave them), but
records cannot move backwards within this consumer's assignment.

accept_order commits workflow+processed-event inbox+workflow.accepted outbox
atomically. An identical replay returns current state and adds no workflow/outbox
effect. A new event ID with the same order business payload adds an inbox ID only.
Conflicting payload/identity is a failure, never a successful duplicate.

- DB failure/timeout: do not attempt an offset commit. If the DB commit outcome
  itself was uncertain, replay resolves it using durable uniqueness/equality.
- Crash after DB commit/before offset: Kafka redelivers; existing inbox is reused.
- Commit failure/lost reply: stop. The broker may already have saved that processed
  offset, or may replay it. Either outcome is safe for the committed acceptance;
  no later records are handled by that failed consumer instance.
- Revoke/reassignment while fetching/handling: discard the stale batch, or leave
  the completed DB transaction replayable. Epoch/ownership checks guard the write
  and acknowledgement boundaries; callbacks never commit prefetched positions.
  Reassignment during a commit can acknowledge only that already durable record.
- SIGINT/SIGTERM: finish the current transaction/commit under bounded timeouts,
  stop before the next record and close without any implicit offset commit.
  Abrupt task cancellation also leaves unfinished offsets unacknowledged.
- Failure/interrupted batches cannot reuse their advanced in-memory positions:
  close and recreate the consumer from broker commits.

Malformed/conflicting input stops the process with nonzero status and safe
failure_type/topic/partition/offset logs. Earlier successful offsets remain saved;
the failing record and later unhandled records stay pending. There is deliberately
no unapproved dead-letter schema, silent skip, offset reset or fake success.
Investigate with A/D and choose an explicit reviewed repair/quarantine policy
before resuming that group. Broker retention and operations remain deployment
responsibilities; B's intake cannot recreate deleted source records.

## Configuration and verification scope

New optional settings (existing .env need not change): incoming_batch_size=10,
incoming_poll_ms=1000, incoming_max_event_bytes=1048576 (1 MiB cap),
incoming_database_timeout_seconds=10. Existing kafka_operation_timeout_seconds
bounds startup/poll/commit/cleanup requests; batch size informs max_poll_interval.

Local tests exercise actual temporary SQLite intake transactions and controlled
consumer/commit/rebalance failures. Opt-in tests add real PostgreSQL persistence
and a real PostgreSQL/Kafka two-partition demo. Recorded results and verification limits are in VERIFICATION.md. No end-to-end exactly-once, live A/D
integration, broker HA or all-process deployment recovery is claimed. The separate workflow-worker restart
demo is described in SCHEDULER.md.
