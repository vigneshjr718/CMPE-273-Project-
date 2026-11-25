"""Operation-key helpers: validation, request fingerprint, and lookup-by-key.

The durable record per key lives in `idempotency_records` and is written by the
seller_reserve / seller_cancel_operation DB functions in the same transaction
as the stock change, so it survives restarts and can never disagree with stock.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")


def is_valid_key(key: str | None) -> bool:
    return key is not None and KEY_PATTERN.fullmatch(key) is not None


def canonical_json(payload: dict[str, Any]) -> str:
    """Stable JSON: sorted keys, no whitespace, dates already as ISO strings."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def request_hash(payload: dict[str, Any]) -> str:
    """Fingerprint of the business payload. Same key + different hash -> 409."""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass
class Lookup:
    """What lookup-by-key should answer.

    kind: RESERVED (live or delivered reservation) | REJECTED (definitive, no stock
    was taken: rejected, or fenced before any reserve) | CANCELLED (was reserved,
    then cancelled -> not representable as a ReservationReply) | NOT_FOUND.
    """
    kind: str
    reply: dict[str, Any] | None = None


def lookup_kind(outcome: str, reservation_status: str | None) -> str:
    if outcome == "RESERVED":
        return "CANCELLED" if reservation_status == "CANCELLED" else "RESERVED"
    return "REJECTED"  # REJECTED or FENCED


def lookup_by_key(conn, key: str, seller_id: str) -> Lookup:
    from sqlalchemy import select

    from app.models import idempotency_records as ir
    from app.services.inventory import get_reservation_row, rejected_reply, reserved_reply

    rec = conn.execute(select(ir).where(ir.c.operation_key == key)).mappings().first()
    if rec is None:
        return Lookup("NOT_FOUND")
    row = get_reservation_row(conn, rec["reservation_id"]) if rec["reservation_id"] else None
    kind = lookup_kind(rec["outcome"], row["status"] if row else None)
    if kind == "RESERVED":
        return Lookup(kind, reserved_reply(row, seller_id))
    if kind == "REJECTED":
        reason = "OPERATION_CANCELLED" if rec["outcome"] == "FENCED" else rec["error_code"]
        return Lookup(kind, rejected_reply(seller_id, key, rec["part_id"], rec["quantity"], reason))
    return Lookup(kind)
