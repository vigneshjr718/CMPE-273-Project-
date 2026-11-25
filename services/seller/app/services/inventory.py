"""Quotes, reservations and stock.

Every stock change is ONE transaction calling ONE DB function
(migrations/002_functions.sql). Reserve = idempotency claim + conditional
decrement + reservation insert, atomically.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.engine import Connection, Engine

from app.config import Settings
from app.models import idempotency_records, inventory, reservations
from app.services.idempotency import request_hash


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def estimated_delivery(settings: Settings, at: datetime | None = None) -> datetime:
    return (at or utc_now()) + timedelta(hours=settings.delivery_hours)


def compute_is_late(status: str, eta: datetime, needed_by: datetime | None) -> bool:
    """Late = an active (not delivered/cancelled) reservation whose ETA is after needed_by."""
    return status == "RESERVED" and needed_by is not None and eta > needed_by


def _as_uuid(value: uuid.UUID | str) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


# ---- read helpers ---------------------------------------------------------------
_JOINED = (
    select(reservations, idempotency_records.c.status_version, idempotency_records.c.cancellation_receipt)
    .join(idempotency_records, idempotency_records.c.operation_key == reservations.c.operation_key)
)


def get_reservation_row(conn: Connection, reservation_id: uuid.UUID | str):
    """Reservation joined with its operation record (status_version, receipt)."""
    try:
        rid = _as_uuid(reservation_id)
    except ValueError:
        return None
    return conn.execute(_JOINED.where(reservations.c.reservation_id == rid)).mappings().first()


def reservation_view(row: Any, seller_id: str) -> dict[str, Any]:
    """Full internal view (GET /reservations/{id}; debugging and demos)."""
    return {
        "reservation_id": str(row["reservation_id"]),
        "seller_id": seller_id,
        "operation_key": row["operation_key"],
        "order_id": row["order_id"],
        "line_id": row["line_id"],
        "part_id": row["part_id"],
        "quantity": row["quantity"],
        "unit_price_cents": row["unit_price_cents"],
        "status": row["status"],
        "delivery_status": row["delivery_status"],
        "needed_by": row["needed_by"],
        "estimated_delivery_at": row["estimated_delivery_at"],
        "is_late": compute_is_late(row["status"], row["estimated_delivery_at"], row["needed_by"]),
        "status_version": row["status_version"],
        "cancellation_receipt": str(row["cancellation_receipt"]) if row["cancellation_receipt"] else None,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "cancelled_at": row["cancelled_at"],
        "delivered_at": row["delivered_at"],
    }


def reserved_reply(row: Any, seller_id: str) -> dict[str, Any]:
    """ReservationReply for a key that holds a (live or delivered) reservation."""
    return {
        "seller_id": seller_id,
        "operation_key": row["operation_key"],
        "part_id": row["part_id"],
        "quantity": row["quantity"],
        "status": "RESERVED",
        "reservation_id": str(row["reservation_id"]),
        "estimated_delivery_at": row["estimated_delivery_at"],
        "reason": None,
    }


def rejected_reply(seller_id: str, key: str, part_id: str, quantity: int, reason: str) -> dict[str, Any]:
    return {
        "seller_id": seller_id,
        "operation_key": key,
        "part_id": part_id,
        "quantity": quantity,
        "status": "REJECTED",
        "reservation_id": None,
        "estimated_delivery_at": None,
        "reason": reason,
    }


# ---- quotes -----------------------------------------------------------------------
def quote(engine: Engine, settings: Settings, part_id: str) -> dict[str, Any]:
    """Non-binding: holds nothing. Unknown part -> available_quantity 0."""
    with engine.connect() as conn:
        available = conn.execute(
            select(inventory.c.available_quantity).where(inventory.c.part_id == part_id)
        ).scalar()
    return {
        "seller_id": settings.seller_id,
        "part_id": part_id,
        "available_quantity": available or 0,
        "estimated_delivery_at": estimated_delivery(settings),
    }


# ---- reserve ----------------------------------------------------------------------
@dataclass
class ReserveOutcome:
    result: str          # RESERVED | REPLAYED | REJECTED | KEY_REUSED | OPERATION_CANCELLED
    http_status: int
    error_code: str | None
    row: Any | None      # joined reservation row when one exists

    @property
    def committed_change(self) -> bool:
        """True only when THIS call changed durable state (matters for 'lost reply' faults)."""
        return self.result == "RESERVED"


_RESERVE_SQL = text(
    "SELECT result, http_status, error_code, reservation_id FROM seller_reserve("
    " :key, :hash, CAST(:body AS jsonb), :order_id, :line_id, :part_id, :qty,"
    " :needed_by, :eta, CAST(:new_id AS uuid))"
)


def reserve(engine: Engine, settings: Settings, key: str, payload: dict[str, Any]) -> ReserveOutcome:
    """payload = ReservationRequest.model_dump(mode='json')."""
    needed_by = datetime.fromisoformat(payload["needed_by"]) if payload.get("needed_by") else None
    with engine.begin() as conn:
        row = conn.execute(_RESERVE_SQL, {
            "key": key,
            "hash": request_hash(payload),
            "body": json.dumps(payload, sort_keys=True),
            "order_id": payload["order_id"],
            "line_id": payload["line_id"],
            "part_id": payload["part_id"],
            "qty": payload["quantity"],
            "needed_by": needed_by,
            "eta": estimated_delivery(settings),
            "new_id": str(uuid.uuid4()),
        }).mappings().one()
        res = get_reservation_row(conn, row["reservation_id"]) if row["reservation_id"] else None
    return ReserveOutcome(row["result"], row["http_status"], row["error_code"], res)


# ---- inventory admin ----------------------------------------------------------------
def list_inventory(engine: Engine, part_id: str | None = None) -> list[dict[str, Any]]:
    q = select(inventory).order_by(inventory.c.part_id)
    if part_id:
        q = q.where(inventory.c.part_id == part_id)
    with engine.connect() as conn:
        return [
            {k: r[k] for k in ("part_id", "description", "total_quantity", "available_quantity",
                               "reserved_quantity", "delivered_quantity", "unit_price_cents")}
            for r in conn.execute(q).mappings().all()
        ]


_UPSERT_SQL = text(
    "INSERT INTO inventory (part_id, description, unit_price_cents, total_quantity, available_quantity)"
    " VALUES (:part_id, :description, :price, :qty, :qty)"
    " ON CONFLICT (part_id) DO UPDATE SET"
    "   total_quantity     = inventory.total_quantity     + EXCLUDED.total_quantity,"
    "   available_quantity = inventory.available_quantity + EXCLUDED.available_quantity,"
    "   description        = COALESCE(:new_description, inventory.description),"
    "   unit_price_cents   = COALESCE(:new_price, inventory.unit_price_cents),"
    "   updated_at         = now()"
)


def restock(engine: Engine, part_id: str, add_quantity: int, description: str | None,
            unit_price_cents: int | None) -> dict[str, Any]:
    """Add units (or create the part). Used by tests, benchmarks and demos."""
    with engine.begin() as conn:
        conn.execute(_UPSERT_SQL, {
            "part_id": part_id,
            "description": description or f"Demo part {part_id}",
            "price": unit_price_cents or 1000,
            "qty": add_quantity,
            "new_description": description,
            "new_price": unit_price_cents,
        })
    return list_inventory(engine, part_id)[0]
