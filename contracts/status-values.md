# Coordinator status and event semantics

B's current producer behavior, published for A/C/D review, November 22, 2026.
This records the implementation; it does not claim other owners approved a new
wire contract. JSON schemas: [workflow.events](events/workflow.events.schema.json)
and [current order.created input](events/order.created.schema.json).

## Order status

The tracker may use `ACCEPTED` before B reports. **B never emits ACCEPTED.**
B's payload `status` is authoritative; D must not infer order status from a line's
event name, timestamp, or attempt outcome.

| Status | Meaning |
| --- | --- |
| PENDING | Acceptance transaction committed; no Saga execution required yet. |
| PROCESSING | Quoting, reserving or replacement work in progress. |
| RESERVED | Every line RESERVED or DELIVERED; at least one still RESERVED. |
| PARTIALLY_RESERVED | Lines RESERVED/DELIVERED/UNFULFILLED, including at least one RESERVED and one UNFULFILLED. |
| FULFILLED | Every line DELIVERED. |
| PARTIALLY_FULFILLED | Every line DELIVERED/UNFULFILLED, with at least one of each. |
| UNFULFILLED | Every line UNFULFILLED. |
| RECONCILIATION_REQUIRED | Uncertain line/cancellation, exhausted recovery, unavailable quotes, or known late/unconfirmed overdue reservation requiring attention. |

Reconciliation/late-reservation reasons take precedence in aggregation. Delivery
after the deadline is still delivery: `DELIVERED_AFTER_DEADLINE` preserves lateness
without changing a fully delivered order into an undelivered one.

## Line state and presentation

| State | Presentation |
| --- | --- |
| PENDING | Waiting for allocation. |
| QUOTING | Finding an eligible seller. |
| RESERVING | A saved attempt is being sent; success is not established. |
| RESERVED | Reservation confirmed; delivery not established. |
| UNKNOWN | Reservation result unknown; retain the same attempt/key. |
| RECONCILING | Checking the original reservation. |
| CANCELLATION_PENDING | Original reservation is not proven safely cancelled. |
| REPLACING | Terminal cancellation confirmed; looking for a successor. |
| DELIVERED | Terminal actual delivery, possibly late. |
| UNFULFILLED | Terminal inability to fulfill that line. |
| RECONCILIATION_REQUIRED | Automatic progress blocked; investigate/reconcile. |

UNKNOWN/RECONCILING/CANCELLATION_PENDING/RECONCILIATION_REQUIRED mean checking or
attention required, **not failed, cancelled or refunded**. REFUNDED is not a B
status. The last two terminal line states are DELIVERED and UNFULFILLED only.

Attempt summaries are different from line states: `line.cancelled` leaves the line
REPLACING while the old attempt is CANCELLED; `line.reservation_rejected` leaves
the line QUOTING while that attempt is REJECTED. Neither means the whole order was
cancelled/rejected. `line.replacement_started` still names the predecessor attempt;
the successor first appears in `line.reserving` with `replaces_attempt_id`.

## Delivery and flags for D

- B's `delivery_status` is null (no successful observation), IN_TRANSIT or DELIVERED.
  PENDING is not currently emitted as a seller delivery observation. A poll error
  can retain the last successful observation; null is not a failure.
- `reason` is an extensible machine-readable string or null, not a closed enum.
  Delivery reasons include DELIVERY_AFTER_DEADLINE,
  DELIVERY_DEADLINE_PASSED_UNCONFIRMED and DELIVERED_AFTER_DEADLINE. Other reasons
  include DEADLINE_EXPIRED, NO_ELIGIBLE_SELLER, QUOTES_UNAVAILABLE,
  RESERVATION_OUTCOME_UNKNOWN, INTERRUPTED_RESERVATION,
  EXPLICIT_RESERVATION_REJECTION, RECONCILED_EXPLICIT_REJECTION,
  CHECKING_ORIGINAL_RESERVATION, ORIGINAL_OUTCOME_STILL_UNKNOWN,
  RETRY_BUDGET_EXHAUSTED, AWAITING_TERMINAL_FENCE,
  CANCELLATION_BUDGET_EXHAUSTED and TERMINAL_CANCELLATION_CONFIRMED.
  Delivery/cancellation errors have separate fields; do not substitute error
  names for a closed reason enum. Unknown reasons should display generically.
- `terminal`: every line DELIVERED or UNFULFILLED. `pending`: its inverse.
- `reconciliation_required`: uncertainty/attention state or known late/unconfirmed
  overdue reservation. This does not mean that a terminal delivery was undone.
- Recommended `replaced`: a terminally cancelled predecessor **and a confirmed
  successor reservation**, not merely `line.cancelled`. A successor still RESERVING
  is an attempt at replacement. D currently treats any CANCELLED history as
  replaced; this needs D's agreement/correction (see B's TRACKER_INTEGRATION.md).

## Workflow event catalog

Every event has UUID event_id, schema_version=1, producer=coordinator,
timezone-aware occurred_at, top-level order_id, positive per-order state_version
and payload.status. The Kafka key is exact UTF-8 order_id. Events use the saved
outbox envelope/ID unchanged on retry; at-least-once delivery is intentional.

| Event | Meaning / attempt summary |
| --- | --- |
| workflow.accepted | PENDING, accepted input lines copied into payload.lines. |
| workflow.started | PROCESSING; no line/attempt fields required. |
| line.quoting | Line QUOTING; attempt_id may be null. |
| line.reserving | New attempt saved before HTTP call; IN_PROGRESS. |
| line.reserved | Confirmed attempt RESERVED, including recovered original. |
| line.reservation_rejected | Explicit no-effect rejection; attempt REJECTED, line QUOTING. |
| line.unknown | Attempt UNCERTAIN. |
| line.reconciling | Attempt UNCERTAIN, persisted retry budget/schedule. |
| line.reconciliation_deferred | Attempt still UNCERTAIN; next retry may be saved. |
| reconciliation.required | Attention required; may occur before any attempt. |
| line.unfulfilled | Line terminal UNFULFILLED; do not label an old cancelled/rejected attempt unfulfilled. |
| line.delivery_updated | New observation; line remains RESERVED. |
| line.delivery_delayed | Known late/overdue reservation; attention required. |
| line.delivery_check_deferred | Poll failed; previous observation can remain. |
| line.delivered | Actual delivery; attempt DELIVERED, including cancellation/delivery race. |
| line.cancelling | Attempt UNCERTAIN while awaiting terminal evidence. |
| line.cancellation_deferred | Cancellation not proven; attempt UNCERTAIN. |
| line.cancelled | Terminal fence persisted; old attempt CANCELLED, line REPLACING. |
| line.replacement_started | Line QUOTING; retains cancelled predecessor identity. |

Line events carry line_id, part_id, line_status, status and nullable attempt_id.
Quantity/ETA/reservation fields are not present on every reconciliation event;
absence is not an instruction to erase known values. Explicit null can clear a
value. Only workflow.accepted carries the original complete list of lines; a
version gap can leave line/attempt history incomplete until replay/rebuild.

## Responses to D's topic proposal

B's defaults agree with D: order.created -> coordinator-v1;
workflow.events -> tracker-v1; key=order_id; strictly increasing per-order B
versions, persisted event IDs, and DB-before-offset handling. B's first version is
1. A/D topics have no cross-topic ordering guarantee.

B currently accepts A's order_id **inside payload only**, with no top-level
order_id or state_version in order.created. The input schema forbids extras.
A adding top-level order_id or state_version=0 requires an agreed adapter/schema
change first; no silent change is made here. UUID order IDs are acceptable if
their string form meets B's identifier constraints; A chooses the business ID.

B stops visibly on poison/conflicting input, retaining the failing offset. D's
local dead-letter-and-continue policy is independent; B does not adopt it or
publish to a new shared dead-letter topic. First B group use requires deliberate
--bootstrap-earliest; saved offsets are never reset by that option.
