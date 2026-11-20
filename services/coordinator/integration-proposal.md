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
UNKNOWN -> RECONCILING or CANCELLATION_PENDING; no direct UNKNOWN -> UNFULFILLED or REPLACING.
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

## Delivery observations: additional draft for C/D review

GET /reservations/{reservation_id}/delivery returns seller_id, operation_key,
reservation_id, part_id, quantity, status (IN_TRANSIT or DELIVERED), status_version,
estimated_delivery_at and delivered_at. IN_TRANSIT requires an aware ETA and no
delivery timestamp. DELIVERED requires an aware delivered_at timestamp.

status_version must be a strictly increasing integer PER RESERVATION when the
observation changes, stable on repeated identical reads and never reset while
that reservation exists, including after seller restart. B uses this counter,
not wall-clock timestamps, to reject stale/conflicting replies. This requirement
is a proposal to C, NOT a verified feature of C's implementation. If the agreed
API differs, translate it in app/clients/delivery.py and revisit version safety
before live integration. Do not derive an ordering counter from timestamps.

The adapter checks the exact saved seller/key/reservation/part/quantity. 404,
timeouts and malformed replies are observation failures, not cancellation or
delivery proof. Only known RESERVED attempts are polled. Cancellation and
replacement remain separate work; polling does not create reservation attempts.

Events proposed for D: line.delivery_updated, line.delivery_delayed,
line.delivery_check_deferred and line.delivered. Same per-order B state_version
and outbox protocol. Added per-line snapshot fields expose the last known
delivery observation, source version, poll failure and next scheduled check.
Repeated identical observations/failures do not create duplicate progress events.
Confirmed all-line delivery produces order FULFILLED; a mix of delivered and
unfulfilled lines produces PARTIALLY_FULFILLED. DELIVERED_AFTER_DEADLINE remains
visible as a lateness reason even when all actual deliveries are complete.


## Checkpoint 6: terminal fencing and replacement proposal

See [REPLACEMENT.md](REPLACEMENT.md) for the complete provisional cancellation
endpoint, payload, terminal receipt and shared source-version requirements.
No C implementation was available on the inspected remote branches on November
20, 2026; these guarantees remain unapproved and unverified against C.
UNKNOWN may enter CANCELLATION_PENDING when the manual replacement command
explicitly requests fencing by original operation key. No reservation ID is
needed to request that fence; 404 is never a substitute for terminal proof.

A CANCELLED response must prove no past delivery and no possible future effect
from the original operation, including in-flight requests and replays. If C
cannot guarantee this, cancellation remains PENDING and no safe replacement
is confirmed. B persists proof and revalidates it before every successor write.
A quote-only RECONCILIATION_REQUIRED line can resume QUOTING for replacement
only when all previous attempts are rejected or have valid cancellation proof.

Additional D events: line.cancelling, line.cancellation_deferred, line.cancelled,
line.replacement_started. Existing line.reserving adds replaces_attempt_id.
Current-attempt snapshots add cancellation status/key/count/error/receipt and
next retry. Prior attempts and receipts remain auditable after ownership changes.
All event/state transitions retain the same local transaction/outbox rules.


## Checkpoint 7: incoming Kafka transport proposal for A/D

See ORDER_CONSUMER.md for the full wire/failure policy. Incoming messages use
UTF-8 JSON OrderCreatedEvent and exact UTF-8 payload.order_id as the Kafka key.
Headers are not authoritative. Topics/group names remain provisional; no new
A/D source/contracts were available in inspected remote branches November20.

B disables auto-commit, reads committed Kafka transactions, and explicitly commits
only the current partition's next offset after transactional accept_order returns.
A duplicate event/order is durably handled according to the existing handler;
a conflicting business payload or invalid envelope stops intake without skipping
that record or later prefetched records. No dead-letter wire contract or automatic
offset reset is introduced. New groups require an explicit bootstrap choice.

Input is accepted into PENDING/version1; acceptance is decoupled from Saga
execution and outbox publication. Failures/rebalances can replay source records;
DB inbox/workflow/outbox transactionality protects the acceptance effect. This
is not a cross-system exactly-once claim. Scheduling and full process restart
recovery remain the next checkpoint. Live A/D compatibility is still pending.
