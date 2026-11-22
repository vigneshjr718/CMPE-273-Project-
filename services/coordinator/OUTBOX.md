# Checkpoint 4: PostgreSQL outbox -> Kafka

Based on Wendy's pushed checkpoint **db88e19**. All changes are in
services/coordinator/. No shared infra/contracts or teammates' code is changed.
Install the added pinned aiokafka dependency; no database migration is needed.
Keep the existing .env. New settings have defaults.

## First: install and test without Kafka
From the repository root after extracting the increment:
```bash
cd services/coordinator
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pip check
COORDINATOR_POSTGRES_TESTS=1 python -m pytest -q
```
The real broker test is separately opt-in, so one skipped test is intentional
at this step. Expected Mac result: **93 passed, 1 skipped**. With both opt-ins
enabled and a working broker, expect **94 passed**.
Preparation environment: **87 passed, 7 skipped** (no PostgreSQL/Docker there).
The PostgreSQL tests create/remove only their own unique test order records.

## Isolated local Kafka for Wendy's Mac
Use D's existing broker if available, setting the bootstrap address accordingly.
Otherwise this **development-only single broker** uses host port 19092 to avoid
colliding with common localhost:9092 labs. The advertised address must match the
address Python can reach; mapping the port alone is insufficient.

From services/coordinator/:
```bash
docker compose -f compose.kafka-demo.yaml up -d
docker compose -f compose.kafka-demo.yaml ps
```
This is an isolated, B-owned test Compose file, not D's shared infrastructure.
The same up command also starts it on later sessions. If Docker reports a
name/port conflict, inspect the
existing container; do not delete an unknown container or database.

Check broker startup (rerun if it is still starting):
```bash
docker exec partstream-coordinator-kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list
```
An empty topic list with exit code 0 is fine for a new broker. If it cannot
connect yet, inspect `docker logs --tail 60 partstream-coordinator-kafka`.
This local broker is not a replicated/cloud deployment and proves no broker
failover or durable-volume/backup guarantees. D still owns team deployment.

## Real PostgreSQL + Kafka replay demo
Keep the PostgreSQL container running. In the activated service terminal:
```bash
export COORDINATOR_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
python -m app.tools.demo_outbox
```
The program creates a unique synthetic order AND a unique one-partition topic
named coordinator-demo-<random>. It never drains another order's outbox and
does not create/change the shared workflow.events/order.created topics.
It calls the real coordinator normal path with controlled seller fixtures,
yielding five actual PostgreSQL outbox records.

It then publishes version 1 to real Kafka, deliberately fails before recording
the published marker, closes that producer, and constructs a fresh producer and
relay. Reading saved PostgreSQL state resends version 1 and then versions 2-5.
The test consumer reads Kafka and checks the original saved keys/payloads.

Expected evidence:
- infrastructure: real_postgresql_and_kafka
- seller_mode: controlled_demo
- workflow_status: RESERVED
- pending_after_simulated_marker_failure: 5
- published_outbox_rows: 5
- broker_records: 6
- distinct_event_ids: 5
- received_state_versions: [1, 1, 2, 3, 4, 5]
- first_event_replayed_with_same_id_and_payload: true
- subsequent_batch_published: 0

This is an injected failure at the DB-marker boundary and a fresh relay/producer
within one process. It is NOT a full OS-process kill/restart demonstration and
does not prove C's real stock handling or D's actual deduplication. The duplicate
version-1 broker record is intentional evidence of the at-least-once boundary.
The demo uses direct order intake, not an incoming Kafka order consumer.

Default demo records/topic remain for inspection. `--cleanup` instead removes
only the exact newly generated topic and synthetic order afterward, even when
the demo fails. Existing accepted demo-order-001 is never reset or deleted.

Run the full suite including the real broker check:
```bash
COORDINATOR_POSTGRES_TESTS=1 COORDINATOR_KAFKA_TESTS=1 python -m pytest -q
```
The Kafka test runs the same replay verification with cleanup=True, so it deletes
only its own generated topic/order. It needs broker topic create/delete rights.

## Actual relay process for team integration
Topics must already be provisioned by D (including appropriate replication,
retention and access policy); this relay never creates shared topics.
After agreeing on destination topics and broker addressing:
```bash
python -m app.tools.publish_outbox
python -m app.tools.publish_outbox --watch
```
The first command publishes at most one batch and exits. The second repeats
batches until SIGINT/SIGTERM; stop with Ctrl+C. An optional `--order-id ID` limits
all reads/updates to one exact order. Every row keeps its SAVED destination;
changing COORDINATOR_WORKFLOW_TOPIC does not reroute existing pending events.
Do not run both commands concurrently: a PostgreSQL advisory lock allows one
cooperative relay at a time per coordinator database.

Defaults:
- COORDINATOR_OUTBOX_BATCH_SIZE=50 (1-1000)
- COORDINATOR_OUTBOX_POLL_SECONDS=1
- COORDINATOR_KAFKA_OPERATION_TIMEOUT_SECONDS=10 (per start/send/stop)

The CLI exits nonzero on DB/publish failure, leaving uncertain events pending.
Restart it after resolving the dependency failure (D can configure container
restart policy). It does not silently discard bad events or continue past a
failed publish. Ctrl+C stops between records; the active call and producer
cleanup each have a timeout. Outbox rows are not garbage-collected in this step.

## Numbered publication and recovery flow
```mermaid
sequenceDiagram
    participant B as Coordinator
    participant DB as PostgreSQL
    participant R as Outbox relay
    participant K as Kafka
    participant D as Tracker
    B->>DB: 1. Commit state and outbox together
    R->>DB: 2. Read earliest pending order version
    R->>K: 3. Publish saved ID and order key
    K-->>R: 4. Acknowledge write
    alt Relay continues
        R->>DB: 5. Commit published marker
    else Failure before marker
        R->>DB: 5a. Reload pending record
        R->>K: 5b. Resend same event ID and payload
    end
    K-->>D: 6. Deliver; duplicates are possible
    Note over D: Inbox deduplication and version guard are D's responsibility
```

## Guarantees and limits
- Stable saved event ID/value and UTF-8 order_id key; no new event on retry.
- acks=all and producer idempotence enabled. This reduces producer retry
  duplicates but cannot atomically commit a PostgreSQL marker and Kafka append.
- A marker is saved only after broker acknowledgement. A crash/lost ack/failed
  marker transaction can cause replay; it must not silently lose a pending row.
- One global relay lock favors simple ordering/correctness over throughput.
  Lower unpublished versions block later versions of the same order, regardless
  of event timestamps. A publish error stops the batch; a bad row can block
  progress until corrected. Sharded leases/dead-letter policy are future work.
- Per-order increasing versions are preserved by cooperative live relays.
  D must still reject stale versions and deduplicate IDs transactionally: lock
  connection loss/network uncertainty/restarts are not end-to-end exactly-once.
- No SQL transaction is held across the Kafka request. The advisory lock uses a
  dedicated PostgreSQL connection and is released on exit/session loss.
- PLAINTEXT is a local/trusted-network setup. Managed Kafka SASL/TLS adapters
  and credentials are a deployment agreement, not implemented by this increment.
- The HTTP /ready endpoint covers its DB/schema only. The separate relay exits
  nonzero on failures; worker/Kafka readiness and automatic orchestration remain
  follow-up work. Inspect queue backlog and process status during deployment.

## References checked during implementation
- https://aiokafka.readthedocs.io/en/stable/producer.html
- https://aiokafka.readthedocs.io/en/stable/api.html
- https://kafka.apache.org/41/getting-started/docker/
- https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml

## Remaining Package B work
Incoming Kafka order consumer/offset handling; automatic pending-work scheduling;
cancellation/fencing and safe replacement; delivery polling; full process restart
demo; real A/C/D integration; benchmarks and final report/architecture handoff.
