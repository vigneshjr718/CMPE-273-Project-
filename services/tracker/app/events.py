"""Decoding of incoming Kafka events.

PROVISIONAL: neither contracts/envelope.schema.json, contracts/events/order.created.json (A)
nor contracts/events/workflow.*.json (B) exist yet. This parser accepts:
  - B's workflow envelope as implemented on B's coordinator branch (top-level order_id +
    state_version), and
  - A's order.created as in B's draft example (order_id only inside payload) OR with a
    top-level order_id (proposed in contracts/events/topics.md).
Tighten this once the contracts land.
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

ORDER_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


class InvalidEvent(ValueError):
    """The message cannot be interpreted. It is never applied."""


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    producer: str
    schema_version: int
    occurred_at: datetime
    order_id: str
    state_version: int | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class CreationLine:
    line_id: str
    part_id: str
    quantity: int
    needed_by: datetime


def as_utc(value: datetime | None) -> datetime | None:
    """SQLite returns naive datetimes; everything the tracker stores is UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise InvalidEvent("timestamp must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise InvalidEvent("invalid ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise InvalidEvent("timestamp must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def _text(data: dict, key: str, max_length: int) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise InvalidEvent(f"{key} must be a non-empty string of at most {max_length} characters")
    return value


def _int(value: Any, key: str, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise InvalidEvent(f"{key} must be an integer >= {minimum}")
    return value


def decode(raw: bytes | None) -> Event:
    if raw is None:
        raise InvalidEvent("empty (tombstone) message")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidEvent("message is not UTF-8 JSON") from error
    if not isinstance(data, dict):
        raise InvalidEvent("envelope must be a JSON object")
    payload = data.get("payload")
    if not isinstance(payload, dict):
        raise InvalidEvent("payload must be a JSON object")

    top, inner = data.get("order_id"), payload.get("order_id")
    if top is not None and inner is not None and top != inner:
        raise InvalidEvent("envelope and payload order_id disagree")
    order_id = top if top is not None else inner
    if not isinstance(order_id, str) or not ORDER_ID.fullmatch(order_id):
        raise InvalidEvent("missing or malformed order_id")

    state_version = data.get("state_version")
    if state_version is not None:
        state_version = _int(state_version, "state_version", 0)

    return Event(
        event_id=_text(data, "event_id", 128),
        event_type=_text(data, "event_type", 80),
        producer=_text(data, "producer", 40),
        schema_version=_int(data.get("schema_version"), "schema_version", 1),
        occurred_at=parse_datetime(data.get("occurred_at")),
        order_id=order_id,
        state_version=state_version,
        payload=payload,
    )


def creation_lines(payload: dict[str, Any]) -> list[CreationLine]:
    lines = payload.get("lines")
    if not isinstance(lines, list) or not lines:
        raise InvalidEvent("lines must be a non-empty list")
    result, seen = [], set()
    for item in lines:
        if not isinstance(item, dict):
            raise InvalidEvent("each line must be an object")
        line = CreationLine(
            line_id=_text(item, "line_id", 128),
            part_id=_text(item, "part_id", 128),
            quantity=_int(item.get("quantity"), "quantity", 1),
            needed_by=parse_datetime(item.get("needed_by")),
        )
        if line.line_id in seen:
            raise InvalidEvent("duplicate line_id")
        seen.add(line.line_id)
        result.append(line)
    return result
