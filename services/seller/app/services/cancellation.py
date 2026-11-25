"""Terminal fencing of an original operation key (B: app/clients/cancellation.py).

One transaction calling seller_cancel_operation(). Works whether or not the seller
has seen the key; stock is restored exactly once; the receipt is durable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import Settings


@dataclass
class CancelOutcome:
    http_status: int                 # 200 terminal, 202 pending, 409 mismatch
    error_code: str | None
    reply: dict[str, Any] | None     # CancellationReply when 200/202
    committed_change: bool           # THIS call changed durable state


_SQL = text(
    "SELECT http_status, error_code, status, reservation_id, part_id, quantity, status_version,"
    "       fenced, receipt, delivered_at, already_cancelled, stock_restored"
    " FROM seller_cancel_operation(:key, :cancel_key, :reason, :order_id, :line_id, :part_id, :qty,"
    "                              CAST(:rid AS uuid))"
)


def cancel_operation(engine: Engine, settings: Settings, key: str, cancellation_key: str,
                     body: dict[str, Any], reason: str | None = None) -> CancelOutcome:
    with engine.begin() as conn:
        row = conn.execute(_SQL, {
            "key": key, "cancel_key": cancellation_key, "reason": reason,
            "order_id": body["order_id"], "line_id": body["line_id"],
            "part_id": body["part_id"], "qty": body["quantity"],
            "rid": body.get("reservation_id"),
        }).mappings().one()
    if row["http_status"] not in (200, 202):
        return CancelOutcome(row["http_status"], row["error_code"], None, False)
    reply = {
        "seller_id": settings.seller_id,
        "operation_key": key,
        "cancellation_key": cancellation_key,
        "part_id": row["part_id"],
        "quantity": row["quantity"],
        "reservation_id": str(row["reservation_id"]) if row["reservation_id"] else None,
        "status": row["status"],
        "status_version": row["status_version"],
        "fenced": row["fenced"],
        "cancellation_receipt": str(row["receipt"]) if row["receipt"] else None,
        "delivered_at": row["delivered_at"],
    }
    changed = row["status"] == "CANCELLED" and not row["already_cancelled"]
    return CancelOutcome(row["http_status"], None, reply, changed)
