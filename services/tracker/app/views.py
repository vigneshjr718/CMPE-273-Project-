"""Builds the Status API view from the read model (no writes)."""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.events import as_utc
from app.models import LineStatus, OrderStatus, StatusHistory
from app.schemas import Attempt, Flags, LineView, OrderView, SelectedSeller

TERMINAL_LINE_STATES = {"DELIVERED", "UNFULFILLED"}
# Outcome is not proven; the UI must say "checking", never "failed". TODO(B): confirm.
UNCERTAIN_LINE_STATES = {"UNKNOWN", "RECONCILING", "CANCELLATION_PENDING", "RECONCILIATION_REQUIRED"}
RECONCILIATION_REASONS = {"DELIVERY_AFTER_DEADLINE", "DELIVERY_DEADLINE_PASSED_UNCONFIRMED"}
# No seller is currently committed to the line in these states.
NO_SELLER_STATES = {"ACCEPTED", "PENDING", "QUOTING", "REPLACING", "UNFULFILLED"}


def _attempts(history: list[StatusHistory]) -> list[Attempt]:
    attempts: dict[str, dict] = {}
    for row in history:  # ordered by state_version
        item = attempts.setdefault(row.attempt_id, {
            "attempt_id": row.attempt_id, "seller_id": None, "outcome": "IN_PROGRESS",
            "reason": None, "started_at": as_utc(row.occurred_at), "updated_at": None})
        item["seller_id"] = item["seller_id"] or row.seller_id
        item["updated_at"] = as_utc(row.occurred_at)
        if row.attempt_outcome:
            item["outcome"], item["reason"] = row.attempt_outcome, row.reason
    return [Attempt(**item) for item in attempts.values()]


def _line_view(line: LineStatus, history: list[StatusHistory]) -> LineView:
    attempts = _attempts(history)
    terminal = line.status in TERMINAL_LINE_STATES
    current = next((a for a in attempts if a.attempt_id == line.current_attempt_id), None)
    selected = None
    if (line.current_attempt_id and line.seller_id and line.status not in NO_SELLER_STATES
            and not (current and current.outcome in {"CANCELLED", "REJECTED"})):
        selected = SelectedSeller(
            seller_id=line.seller_id, attempt_id=line.current_attempt_id,
            estimated_delivery_at=as_utc(line.estimated_delivery_at),
            delivery_status=line.delivery_status, delivered_at=as_utc(line.delivered_at))
    return LineView(
        line_id=line.line_id, part_id=line.part_id, quantity=line.quantity,
        needed_by=as_utc(line.needed_by), status=line.status, reason=line.reason,
        flags=Flags(
            pending=not terminal,
            reconciliation_required=(line.status in UNCERTAIN_LINE_STATES
                                     or line.reason in RECONCILIATION_REASONS),
            replaced=any(a.outcome == "CANCELLED" for a in attempts),
            terminal=terminal),
        selected_seller=selected, attempt_count=line.attempt_count, attempts=attempts,
        last_updated=as_utc(line.last_updated))


async def load_order_view(session: AsyncSession, order_id: str) -> OrderView | None:
    order = await session.get(OrderStatus, order_id)
    if order is None:
        return None
    lines = list(await session.scalars(
        select(LineStatus).where(LineStatus.order_id == order_id).order_by(LineStatus.line_id)))
    history = list(await session.scalars(
        select(StatusHistory)
        .where(StatusHistory.order_id == order_id, StatusHistory.attempt_id.is_not(None))
        .order_by(StatusHistory.state_version, StatusHistory.id)))
    views = [_line_view(line, [h for h in history if h.line_id == line.line_id]) for line in lines]
    terminal = bool(views) and all(v.flags.terminal for v in views)
    return OrderView(
        order_id=order.order_id, status=order.status,
        flags=Flags(
            pending=not terminal,
            reconciliation_required=(order.status == "RECONCILIATION_REQUIRED"
                                     or any(v.flags.reconciliation_required for v in views)),
            replaced=any(v.flags.replaced for v in views),
            terminal=terminal),
        order_created_seen=order.order_created_seen, state_version=order.state_version,
        created_at=as_utc(order.created_at), last_updated=as_utc(order.last_updated),
        lines=views)
