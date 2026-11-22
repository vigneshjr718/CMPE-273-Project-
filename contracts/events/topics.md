# Kafka topics

**Owner:** D (coordinates). **Status:** DRAFT v1. Needs approval from A (order) and B (coordinator).
Any change needs a separate small PR approved by the affected producers and consumers.
Topics are created only by `infra/kafka/create-topics.sh`. Broker auto-creation is
disabled, so a misspelled topic name fails instead of silently creating a new topic.

## Topics

| Topic | Authoritative producer | Consumers | Event types | Key | Partitions |
|---|---|---|---|---|---|
| `order.created` | **order** (A) only, via its outbox | coordinator (B), tracker (D) | `order.created` | `order_id` | 3 |
| `workflow.events` | **coordinator** (B) only, via its outbox | tracker (D) | `workflow.*`, `line.*`, `reconciliation.required` (list defined by B in `contracts/events/workflow.*.json`) | `order_id` | 3 |

No other service publishes to these topics. Sellers (C) publish no Kafka events in v1.
B polls seller delivery status over REST and reports it in `workflow.events`.

## Partition key = `order_id`

- The message key is the UTF-8 bytes of `order_id`, with no prefix and no JSON.
- Kafka keeps order only within a partition. Keying by `order_id` sends all events of one
  order to the same partition, in the order the producer published them.
- There is **no ordering across topics**. The tracker may receive B's `workflow.accepted` before A's
  `order.created` for the same order. Consumers must tolerate that.
- **The partition count must not change** after go-live, because changing it remaps keys to
  different partitions. `create-topics.sh` fails if the count differs.

## Delivery semantics (all topics)

- **At-least-once.** Both producers use an outbox: publish, then mark as published. A crash
  between those steps re-sends the same event. Duplicates are expected.
- Every event has a unique `event_id`. A retried event keeps its original `event_id` and payload.
- Consumers must **deduplicate by `event_id` in the same DB transaction** as their state change,
  and **commit the Kafka offset only after that DB commit**.
- Producers use `acks=all` with idempotence enabled.

## Ordering and versions

- `workflow.events`: `state_version` is a positive integer that increases strictly **per
  order**, across all lines of that order (implemented by B). Consumers ignore any event whose
  version is less than or equal to the version they last applied for that order.
- `order.created`: one creation fact per order. Only A creates an order, so later events must
  never reset an order back to its creation state. *Open: whether A adds a `state_version`. See below.*
- Event timestamps (`occurred_at`) are informational only and are **never** used for ordering.

## Consumer groups

| Group | Service | Topics |
|---|---|---|
| `coordinator-v1` | coordinator (B) | `order.created` |
| `tracker-v1` | tracker (D) | `order.created`, `workflow.events` |

Starting a group from scratch (or bumping it to `-v2`) replays from the earliest retained offset.
Never reset an existing group's offsets without telling the owning team.

## Retention

`retention.ms = -1` (keep forever) on both topics. Reasons: the tracker can rebuild its
projection by replaying from offset 0, and the data volume is small (synthetic demo orders).
Kafka is **not** the system of record. Each producer's database is, and backups cover the
databases (`scripts/backup.sh`).

## Envelope

The common fields are defined in `contracts/envelope.schema.json` (owned by ALL, still pending).
The current shape on B's branch is:

```json
{
  "event_id": "uuid",
  "event_type": "line.reserved",
  "schema_version": 1,
  "producer": "coordinator",
  "occurred_at": "2030-01-01T00:00:00+00:00",
  "order_id": "order-42",
  "state_version": 7,
  "payload": { }
}
```

## Open items (need answers before v1 is final)

1. A: put `order_id` at the top level of the `order.created` envelope (matching B)?
   B's draft example currently has it only inside `payload`.
2. A: add `state_version` to `order.created`? D proposes a fixed `0`, so creation always sorts
   before B's first version (`1`).
3. B: confirm the final list of `workflow.events` event types and payloads (`workflow.*.json`).
4. Everyone: is a shared dead-letter *topic* needed? v1 has none. Each consumer handles
   unparseable messages itself. The tracker records them in its own `dead_letter_events` table,
   keeps consuming, and reports the count in `/ready` (it can be switched to stop instead).
   Other consumers choose and document their own policy.
