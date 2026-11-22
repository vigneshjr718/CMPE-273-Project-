"""MOCK EVENT FIXTURES. NOT AN APPROVED CONTRACT.

Neither contracts/events/order.created.json (owner A) nor contracts/events/workflow.*.json
(owner B) exist yet. These builders copy the shapes producers currently use:
  * order.created: B's draft example services/coordinator/examples/order-created.json
    (order_id only inside payload, no state_version).
  * workflow events: B's outbox envelope and payloads in services/coordinator/app/saga/*.py
    on branch pr/coordinator-durable-scheduling-and-restart-recovery (not yet on main).
Replace these with the contract examples when A and B publish them.
"""
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

ORDER_TOPIC = "order.created"
WORKFLOW_TOPIC = "workflow.events"
T0 = datetime(2026, 11, 23, 17, 0, tzinfo=timezone.utc)

DEFAULT_LINES = [
    {"line_id": "line-1", "part_id": "BMP-FR-2019CIV", "quantity": 1,
     "needed_by": "2026-11-25T17:00:00Z"},
    {"line_id": "line-2", "part_id": "HDL-LH-2019CIV", "quantity": 2,
     "needed_by": "2026-11-26T17:00:00Z"},
]


def encode(envelope: dict) -> bytes:
    return json.dumps(envelope).encode("utf-8")


def order_created(order_id: str, lines=None, *, event_id=None, at=T0,
                  producer="order", top_level_order_id=False) -> tuple[str, bytes]:
    envelope = {
        "event_id": event_id or str(uuid4()), "event_type": "order.created",
        "schema_version": 1, "producer": producer, "occurred_at": at.isoformat(),
        "payload": {"order_id": order_id, "lines": lines or DEFAULT_LINES},
    }
    if top_level_order_id:
        envelope["order_id"] = order_id
    return ORDER_TOPIC, encode(envelope)


class Workflow:
    """Emits B-style events for one order with a strictly increasing state_version."""

    def __init__(self, order_id: str, *, start=T0, lines=None):
        self.order_id, self.version = order_id, 0
        self.clock = start
        self.lines = lines or DEFAULT_LINES

    def emit(self, event_type: str, payload: dict, *, producer="coordinator",
             version: int | None = None) -> tuple[str, bytes]:
        self.version = version if version is not None else self.version + 1
        self.clock += timedelta(seconds=1)
        return WORKFLOW_TOPIC, encode({
            "event_id": str(uuid4()), "event_type": event_type, "schema_version": 1,
            "producer": producer, "occurred_at": self.clock.isoformat(),
            "order_id": self.order_id, "state_version": self.version, "payload": payload,
        })

    def accepted(self):
        return self.emit("workflow.accepted", {"status": "PENDING", "lines": self.lines})

    def started(self):
        return self.emit("workflow.started", {"status": "PROCESSING"})

    def line(self, event_type, line_id, line_status, status, *, attempt_id=None,
             seller_id=None, reason=None, version=None, **extra):
        part = next(item for item in self.lines if item["line_id"] == line_id)
        payload = {"line_id": line_id, "part_id": part["part_id"], "quantity": part["quantity"],
                   "line_status": line_status, "status": status, "reason": reason,
                   "attempt_id": attempt_id}
        if seller_id:
            payload.update(seller_id=seller_id, operation_key=f"reserve-{uuid4().hex}",
                           reservation_id=None)
        payload.update(extra)
        return self.emit(event_type, payload, version=version)

    def reserving(self, line_id, attempt_id, seller_id, replaces=None):
        part = next(item for item in self.lines if item["line_id"] == line_id)
        return self.emit("line.reserving", {
            "line_id": line_id, "part_id": part["part_id"], "quantity": part["quantity"],
            "line_status": "RESERVING",
            "status": "PROCESSING", "attempt_id": attempt_id, "seller_id": seller_id,
            "operation_key": f"reserve-{uuid4().hex}", "replaces_attempt_id": replaces})


# ---------------------------------------------------------------- scenarios
def just_accepted(order_id="ord-accepted"):
    return [order_created(order_id)]


def in_progress(order_id="ord-progress"):
    wf = Workflow(order_id)
    return [
        order_created(order_id),
        wf.accepted(), wf.started(),
        wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING"),
        wf.reserving("line-1", "att-1a", "seller-2"),
        wf.line("line.reserved", "line-1", "RESERVED", "PROCESSING", attempt_id="att-1a",
                seller_id="seller-2", estimated_delivery_at="2026-11-24T21:00:00+00:00"),
        wf.line("line.quoting", "line-2", "QUOTING", "PROCESSING"),
        wf.reserving("line-2", "att-2a", "seller-4"),
    ], wf


def fulfilled(order_id="ord-fulfilled"):
    events, wf = in_progress(order_id)
    events += [
        wf.line("line.reserved", "line-2", "RESERVED", "RESERVED", attempt_id="att-2a",
                seller_id="seller-4", estimated_delivery_at="2026-11-25T16:00:00+00:00"),
        wf.line("line.delivered", "line-1", "DELIVERED", "RESERVED", attempt_id="att-1a",
                seller_id="seller-2", delivery_status="DELIVERED",
                delivered_at="2026-11-24T20:40:00+00:00"),
        wf.line("line.delivered", "line-2", "DELIVERED", "FULFILLED", attempt_id="att-2a",
                seller_id="seller-4", delivery_status="DELIVERED",
                delivered_at="2026-11-25T15:10:00+00:00"),
    ]
    return events


def replaced(order_id="ord-replaced"):
    one_line = [DEFAULT_LINES[0]]
    wf = Workflow(order_id, lines=one_line)
    return [
        order_created(order_id, one_line),
        wf.accepted(), wf.started(),
        wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING"),
        wf.reserving("line-1", "att-old", "seller-3"),
        wf.line("line.reserved", "line-1", "RESERVED", "RESERVED", attempt_id="att-old",
                seller_id="seller-3", estimated_delivery_at="2026-11-25T10:00:00+00:00"),
        wf.line("line.delivery_delayed", "line-1", "RESERVED", "RECONCILIATION_REQUIRED",
                attempt_id="att-old", seller_id="seller-3", reason="DELIVERY_AFTER_DEADLINE",
                delivery_status="DELAYED", estimated_delivery_at="2026-11-27T10:00:00+00:00"),
        wf.line("line.cancelling", "line-1", "CANCELLATION_PENDING", "RECONCILIATION_REQUIRED",
                attempt_id="att-old", seller_id="seller-3", reason="DELIVERY_AFTER_DEADLINE"),
        wf.line("line.cancelled", "line-1", "REPLACING", "PROCESSING", attempt_id="att-old",
                seller_id="seller-3", reason="DELIVERY_AFTER_DEADLINE"),
        wf.line("line.replacement_started", "line-1", "REPLACING", "PROCESSING",
                attempt_id="att-old", reason="DELIVERY_AFTER_DEADLINE"),
        wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING", attempt_id="att-old"),
        wf.reserving("line-1", "att-new", "seller-1", replaces="att-old"),
        wf.line("line.reserved", "line-1", "RESERVED", "RESERVED", attempt_id="att-new",
                seller_id="seller-1", estimated_delivery_at="2026-11-25T08:00:00+00:00"),
    ]


def unfulfilled(order_id="ord-unfulfilled"):
    one_line = [DEFAULT_LINES[0]]
    wf = Workflow(order_id, lines=one_line)
    return [
        order_created(order_id, one_line),
        wf.accepted(), wf.started(),
        wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING"),
        wf.line("line.unfulfilled", "line-1", "UNFULFILLED", "UNFULFILLED",
                reason="NO_ELIGIBLE_SELLER"),
    ]


def reconciliation_required(order_id="ord-reconcile"):
    one_line = [DEFAULT_LINES[0]]
    wf = Workflow(order_id, lines=one_line)
    return [
        order_created(order_id, one_line),
        wf.accepted(), wf.started(),
        wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING"),
        wf.reserving("line-1", "att-lost", "seller-5"),
        wf.line("line.unknown", "line-1", "UNKNOWN", "RECONCILIATION_REQUIRED",
                attempt_id="att-lost", seller_id="seller-5"),
        wf.line("line.reconciling", "line-1", "RECONCILING", "RECONCILIATION_REQUIRED",
                attempt_id="att-lost", seller_id="seller-5"),
        wf.line("reconciliation.required", "line-1", "RECONCILIATION_REQUIRED",
                "RECONCILIATION_REQUIRED", attempt_id="att-lost", seller_id="seller-5",
                reason="RETRY_BUDGET_EXHAUSTED"),
    ]
