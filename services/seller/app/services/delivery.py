"""Persisted delivery observations (B: app/clients/delivery.py) plus demo controls."""
from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import Settings
from app.services.inventory import get_reservation_row


def delivery_reply(row: Any, seller_id: str) -> dict[str, Any]:
    """Only for RESERVED/DELIVERED rows. PENDING and SHIPPED are both IN_TRANSIT to B."""
    delivered = row["status"] == "DELIVERED"
    return {
        "seller_id": seller_id,
        "operation_key": row["operation_key"],
        "reservation_id": str(row["reservation_id"]),
        "part_id": row["part_id"],
        "quantity": row["quantity"],
        "status": "DELIVERED" if delivered else "IN_TRANSIT",
        "status_version": row["status_version"],
        "estimated_delivery_at": row["estimated_delivery_at"],
        "delivered_at": row["delivered_at"] if delivered else None,
    }


def get_delivery_row(engine: Engine, reservation_id: uuid.UUID | str):
    with engine.connect() as conn:
        return get_reservation_row(conn, reservation_id)


_SET_SQL = text(
    "SELECT http_status, error_code FROM seller_set_delivery(CAST(:rid AS uuid), :status, :eta)"
)


def update_delivery(engine: Engine, settings: Settings, reservation_id: uuid.UUID,
                    delivery_status: str | None, eta, delay_hours: float | None):
    """Admin/demo: returns (http_status, error_code, joined row or None)."""
    with engine.begin() as conn:
        if delay_hours:
            current = get_reservation_row(conn, reservation_id)
            if current is None:
                return 404, "RESERVATION_NOT_FOUND", None
            eta = (eta or current["estimated_delivery_at"]) + timedelta(hours=delay_hours)
        row = conn.execute(_SET_SQL, {"rid": str(reservation_id), "status": delivery_status, "eta": eta}).mappings().one()
        if row["http_status"] != 200:
            return row["http_status"], row["error_code"], None
        return 200, None, get_reservation_row(conn, reservation_id)
