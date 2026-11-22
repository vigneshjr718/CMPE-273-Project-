# Package B local interface proposal - NOT YET APPROVED

No authoritative contracts/ files exist on the inspected main branch.
This document records assumptions for A/C/D review. Existing approved contracts,
when supplied, take precedence. Keep translations in the seller/event adapters.

## A -> B
Candidate JSON envelope:

```json
{
  "event_id": "b114e320-a162-4b3b-9124-e541ccdcf960",
  "event_type": "order.created",
  "schema_version": 1,
  "producer": "order",
  "occurred_at": "2030-01-01T00:00:00Z",
  "payload": {
    "order_id": "order-42",
    "lines": [{
      "line_id": "line-1",
      "part_id": "bumper",
      "quantity": 2,
      "needed_by": "2030-01-03T00:00:00Z"
    }]
  }
}
```

Proposal: IDs are nonempty ASCII identifiers, timestamps have timezone offsets,
line IDs are unique per order, quantity is a positive integer. The strict parser
currently rejects unknown fields so disagreement is visible. Historical events
remain parseable; eligibility is assessed separately. A different envelope is
a team decision, not a reason for A to rewrite its code blindly.

## C -> B quote adapter
POST /quotes with part_id, quantity, needed_by; response:
```json
{
  "seller_id": "seller-1",
  "part_id": "bumper",
  "available_quantity": 3,
  "estimated_delivery_at": "2030-01-02T12:00:00Z"
}
```

Choose enough stock and delivery time <= needed_by, earliest ETA first, seller
ID tie-breaker. Reject stale ETAs and mismatched seller/part identity. Preserve
failed seller queries as uncertainty; no eligible returned quote does not prove
no seller could fulfill. Selection has no reservation side effects.
The two-day policy vs per-line needed-by constraint remains unresolved.

Next adapter methods need C's contract: reserve, lookup by operation key,
cancel/fence by original operation key (even when no reservation ID was received),
and delivery/ETA lookup. A not-found lookup is not proof an in-flight request
cannot still commit. Confirm tombstone/fencing/cancellation guarantees before
implementing safe replacement. All write retries reuse the persisted logical
operation key. A replacement has a new attempt/key.

## B -> D
Proposal: monotonically increasing state_version **per order**, across all of B's
line/attempt events for that order. Serialize/lock that order's transitions when
implementing; emit one outbox event per order/version. D keeps separate producer
authority for A creation and B workflow events. Old creation must not reset B's
projection; old attempt events must not overwrite a newer replacement.
Topic names/order keys and actual workflow event envelope/names remain pending.
The schema's unique (order_id, state_version) constraint encodes this proposal.

## Draft state guards
UNKNOWN -> RECONCILING; no direct UNKNOWN -> UNFULFILLED or REPLACING.
RESERVED -> CANCELLATION_PENDING before REPLACING.
CANCELLATION_PENDING -> REPLACING requires durable proof of cancellation.
CANCELLATION_PENDING -> DELIVERED requires confirmed delivery instead.
RESERVING -> QUOTING is only safe after explicit proof the attempt was rejected
without side effects. Never use this path for a timeout.
RECONCILIATION_REQUIRED remains visible and can resume; it is not success.
DELIVERED/UNFULFILLED are terminal for this proposed line workflow.

State guards restrict allowed names; future orchestration must also verify the
evidence needed for each transition. An enum alone does not enforce a Saga.

## Message transaction rules for next checkpoint
- Consume order: inbox record + workflow creation in one local transaction.
- Transition: state + next order version + outbox event in one transaction.
- Offset acknowledgment follows durable local commit.
- Relay: publish saved event, then mark published. Crash may cause replay;
  downstream consumers must deduplicate event IDs transactionally.
- Pending durable work must resume after restart; Kafka is not the sole store.

## Requests to teammates (draft only, not sent)
A: Please supply order.created schema/example and required fields.
C: Please confirm quote, reserve, lookup and cancel/fencing wire contracts.
D: Please confirm topics, partition keys, envelope and version semantics, plus
coordinator_db role and internal connection/service names with C.
