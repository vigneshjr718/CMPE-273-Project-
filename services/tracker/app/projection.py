"""Applies Kafka events to the tracker read model.

Guarantees (each event is handled in ONE database transaction):
  * Dedupe: event_id is inserted into processed_events in the same transaction as the
    state change, so a redelivered event has no second effect.
  * No regression: a coordinator event applies only if its per-order state_version is
    greater than the last applied one. A late order.created never resets B's progress.
  * Producer authority: order.created topic -> producer "order" only; workflow topic ->
    producer "coordinator" only. Anything else is recorded and ignored.
  * Timestamps (occurred_at) are stored for display and never used for ordering.
"""
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from sqlalchemy import exists, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.events import (CreationLine, Event, InvalidEvent, creation_lines, decode,
                        parse_datetime)
from app.models import LineStatus, OrderStatus, ProcessedEvent, StatusHistory

logger = logging.getLogger("tracker.projection")

ORDER_PRODUCER = "order"
COORDINATOR_PRODUCER = "coordinator"
ORDER_CREATED = "order.created"

# Tracker-only states before the coordinator has reported (see status-api.yaml).
ACCEPTED = "ACCEPTED"
COORDINATOR_PENDING = "PENDING"

# TODO(B): confirm against contracts/status-values.md once published.
ATTEMPT_OUTCOME_BY_EVENT = {
    "line.reserving": "IN_PROGRESS",
    "line.reserved": "RESERVED",
    "line.reservation_rejected": "REJECTED",
    "line.unknown": "UNCERTAIN",
    "line.reconciling": "UNCERTAIN",
    "line.reconciliation_deferred": "UNCERTAIN",
    "reconciliation.required": "UNCERTAIN",
    "line.cancelling": "UNCERTAIN",
    "line.cancellation_deferred": "UNCERTAIN",
    "line.cancelled": "CANCELLED",
    "line.delivered": "DELIVERED",
}


class Outcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"            # not recorded again; the first record stands
    STALE = "stale"
    REJECTED_AUTHORITY = "rejected_authority"
    DUPLICATE_CREATION = "duplicate_creation"
    INVALID_SKIPPED = "invalid_skipped"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _opt_str(payload: dict[str, Any], key: str, max_length: int) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value and len(value) <= max_length:
        return value
    return None


def _opt_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _opt_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_datetime(value)
    except InvalidEvent:
        return None


class Projector:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], *, order_topic: str,
                 workflow_topic: str, clock: Callable[[], datetime] = utc_now):
        if order_topic == workflow_topic:
            raise ValueError("order and workflow topics must differ")
        self.sessions = sessions
        self.order_topic, self.workflow_topic = order_topic, workflow_topic
        self.clock = clock

    # ------------------------------------------------------------------ entry points
    async def handle(self, topic: str, partition: int, offset: int, raw: bytes | None) -> Outcome:
        """Decode and apply one message. Raises InvalidEvent if it cannot be interpreted.

        Returns only after the database transaction has COMMITTED, so the caller may then
        commit the Kafka offset.
        """
        event = decode(raw)
        authorized = self._authorized(topic, event)
        lines: list[CreationLine] | None = None
        if authorized and topic == self.order_topic:
            lines = creation_lines(event.payload)
        if authorized and topic == self.workflow_topic and not event.state_version:
            raise InvalidEvent("coordinator events need state_version >= 1")

        for attempt in range(3):
            try:
                async with self.sessions() as session, session.begin():
                    if await session.get(ProcessedEvent, event.event_id) is not None:
                        outcome = Outcome.DUPLICATE
                    else:
                        record = ProcessedEvent(
                            event_id=event.event_id, topic=topic, kafka_partition=partition,
                            kafka_offset=offset, event_type=event.event_type,
                            producer=event.producer, order_id=event.order_id,
                            outcome="pending", processed_at=self.clock())
                        session.add(record)
                        await session.flush()  # unique event_id is the backstop for races
                        if not authorized:
                            outcome = Outcome.REJECTED_AUTHORITY
                        elif topic == self.order_topic:
                            outcome = await self._apply_creation(session, event, lines or [])
                        else:
                            outcome = await self._apply_workflow(session, event)
                        record.outcome = outcome.value
                break
            except IntegrityError:
                # A concurrent writer inserted the same event/order first. Retry: the
                # next pass sees it and becomes a duplicate or a normal update.
                if attempt == 2:
                    raise
        log = logger.warning if outcome in (Outcome.REJECTED_AUTHORITY,
                                            Outcome.DUPLICATE_CREATION) else logger.info
        log("event_processed", extra={
            "event_id": event.event_id, "event_type": event.event_type,
            "producer": event.producer, "order_id": event.order_id, "topic": topic,
            "partition": partition, "offset": offset, "outcome": outcome.value,
            "state_version": event.state_version})
        return outcome

    async def record_invalid(self, topic: str, partition: int, offset: int) -> None:
        """Skip-mode only: remember that this offset held an uninterpretable message."""
        marker = f"invalid:{topic}:{partition}:{offset}"
        async with self.sessions() as session, session.begin():
            if await session.get(ProcessedEvent, marker) is None:
                session.add(ProcessedEvent(
                    event_id=marker, topic=topic, kafka_partition=partition,
                    kafka_offset=offset, outcome=Outcome.INVALID_SKIPPED.value,
                    processed_at=self.clock()))

    # ------------------------------------------------------------------ rules
    def _authorized(self, topic: str, event: Event) -> bool:
        if topic == self.order_topic:
            return event.producer == ORDER_PRODUCER and event.event_type == ORDER_CREATED
        if topic == self.workflow_topic:
            return event.producer == COORDINATOR_PRODUCER and event.event_type != ORDER_CREATED
        return False

    async def _lock_order(self, session: AsyncSession, order_id: str) -> OrderStatus | None:
        result = await session.execute(
            select(OrderStatus).where(OrderStatus.order_id == order_id).with_for_update())
        return result.scalar_one_or_none()

    async def _lines(self, session: AsyncSession, order_id: str) -> dict[str, LineStatus]:
        rows = await session.scalars(select(LineStatus).where(LineStatus.order_id == order_id))
        return {row.line_id: row for row in rows}

    async def _apply_creation(self, session: AsyncSession, event: Event,
                              lines: list[CreationLine]) -> Outcome:
        """A's order.created: authoritative creation facts. Never touches B's progress."""
        now = self.clock()
        order = await self._lock_order(session, event.order_id)
        if order is None:
            order = OrderStatus(
                order_id=event.order_id, status=ACCEPTED, order_created_seen=False,
                state_version=0, last_event_id=event.event_id,
                last_updated=event.occurred_at, projected_at=now)
            session.add(order)
            await session.flush()  # parent row before FK-dependent lines
        elif order.order_created_seen:
            # Creation is a single fact. A second creation event (new event_id) for the
            # same order must not reset anything.
            return Outcome.DUPLICATE_CREATION

        order.order_created_seen = True
        order.creation_event_id = event.event_id
        order.created_at = event.occurred_at
        order.projected_at = now
        if order.state_version == 0:
            order.last_event_id, order.last_updated = event.event_id, event.occurred_at

        existing = await self._lines(session, event.order_id)
        for line in lines:
            row = existing.get(line.line_id)
            if row is None:
                session.add(LineStatus(
                    order_id=event.order_id, line_id=line.line_id, part_id=line.part_id,
                    quantity=line.quantity, needed_by=line.needed_by, creation_source="order",
                    status=ACCEPTED if order.state_version == 0 else COORDINATOR_PENDING,
                    attempt_count=0, last_updated=event.occurred_at))
            else:  # B's copy arrived first; A's facts win, B's progress is kept
                row.part_id, row.quantity, row.needed_by = line.part_id, line.quantity, line.needed_by
                row.creation_source = "order"

        session.add(self._history(event, order, state_version=0, now=now))
        return Outcome.APPLIED

    async def _apply_workflow(self, session: AsyncSession, event: Event) -> Outcome:
        """B's workflow/line event: applied only if its state_version is newer."""
        now = self.clock()
        version = event.state_version or 0
        order = await self._lock_order(session, event.order_id)
        if order is None:  # B's event arrived before A's order.created (no cross-topic order)
            order = OrderStatus(
                order_id=event.order_id, status=COORDINATOR_PENDING, order_created_seen=False,
                state_version=0, last_event_id=event.event_id,
                last_updated=event.occurred_at, projected_at=now)
            session.add(order)
            await session.flush()

        if version <= order.state_version:
            logger.info("stale_event_ignored", extra={
                "event_id": event.event_id, "order_id": event.order_id,
                "state_version": version, "current_version": order.state_version})
            return Outcome.STALE
        if version > order.state_version + 1:
            # Not fatal: B's payloads carry full line state. Attempt history may have a hole.
            logger.warning("state_version_gap", extra={
                "event_id": event.event_id, "order_id": event.order_id,
                "state_version": version, "current_version": order.state_version})

        payload = event.payload
        order.state_version = version
        order.last_event_id, order.last_updated, order.projected_at = event.event_id, event.occurred_at, now
        status = _opt_str(payload, "status", 40)
        if status:
            order.status = status

        lines = await self._lines(session, event.order_id)
        if event.event_type == "workflow.accepted" and "lines" in payload:
            try:
                copied = creation_lines(payload)
            except InvalidEvent:
                logger.warning("workflow_accepted_lines_unreadable",
                               extra={"event_id": event.event_id, "order_id": event.order_id})
                copied = []
            for line in copied:
                row = lines.get(line.line_id)
                if row is None:
                    row = LineStatus(
                        order_id=event.order_id, line_id=line.line_id, part_id=line.part_id,
                        quantity=line.quantity, needed_by=line.needed_by,
                        creation_source="coordinator", status=COORDINATOR_PENDING,
                        attempt_count=0, last_updated=event.occurred_at)
                    session.add(row)
                    lines[line.line_id] = row
                elif row.creation_source != "order":
                    row.part_id, row.quantity, row.needed_by = line.part_id, line.quantity, line.needed_by
        for row in lines.values():  # the coordinator now owns the order
            if row.status == ACCEPTED:
                row.status = COORDINATOR_PENDING

        line_id = _opt_str(payload, "line_id", 128)
        attempt_id = _opt_str(payload, "attempt_id", 128)
        outcome = ATTEMPT_OUTCOME_BY_EVENT.get(event.event_type) if attempt_id else None
        if line_id:
            await self._apply_line(session, event, lines, line_id, attempt_id)

        session.add(self._history(event, order, state_version=version, now=now,
                                  line_id=line_id, attempt_id=attempt_id,
                                  attempt_outcome=outcome))
        return Outcome.APPLIED

    async def _apply_line(self, session: AsyncSession, event: Event,
                          lines: dict[str, LineStatus], line_id: str,
                          attempt_id: str | None) -> None:
        payload = event.payload
        row = lines.get(line_id)
        if row is None:
            row = LineStatus(
                order_id=event.order_id, line_id=line_id,
                part_id=_opt_str(payload, "part_id", 128), quantity=_opt_int(payload, "quantity"),
                needed_by=None, creation_source="coordinator", status=COORDINATOR_PENDING,
                attempt_count=0, last_updated=event.occurred_at)
            session.add(row)
            lines[line_id] = row
        elif row.creation_source != "order":
            row.part_id = row.part_id or _opt_str(payload, "part_id", 128)
            row.quantity = row.quantity or _opt_int(payload, "quantity")

        line_status = _opt_str(payload, "line_status", 40)
        if line_status:
            row.status = line_status

        if attempt_id and attempt_id != row.current_attempt_id:
            seen = await session.scalar(select(exists().where(
                StatusHistory.order_id == event.order_id, StatusHistory.line_id == line_id,
                StatusHistory.attempt_id == attempt_id)))
            if not seen:  # a NEW attempt (first try or replacement) becomes current
                row.attempt_count += 1
                row.current_attempt_id = attempt_id
                row.seller_id = row.estimated_delivery_at = None
                row.delivery_status = row.delivered_at = None
                row.reason = None  # B clears the line reason when a new attempt starts
            # else: an event about an OLDER attempt; it must not take over the line.

        if "reason" in payload:
            row.reason = _opt_str(payload, "reason", 80)

        if attempt_id and attempt_id == row.current_attempt_id:
            seller_id = _opt_str(payload, "seller_id", 128)
            if seller_id:
                row.seller_id = seller_id
            if "estimated_delivery_at" in payload:
                row.estimated_delivery_at = _opt_datetime(payload["estimated_delivery_at"])
            if "delivery_status" in payload:
                row.delivery_status = _opt_str(payload, "delivery_status", 40)
            if "delivered_at" in payload:
                row.delivered_at = _opt_datetime(payload["delivered_at"])
        row.last_updated = event.occurred_at

    def _history(self, event: Event, order: OrderStatus, *, state_version: int, now: datetime,
                 line_id: str | None = None, attempt_id: str | None = None,
                 attempt_outcome: str | None = None) -> StatusHistory:
        payload = event.payload
        return StatusHistory(
            event_id=event.event_id, order_id=event.order_id, state_version=state_version,
            event_type=event.event_type, producer=event.producer, line_id=line_id,
            attempt_id=attempt_id, seller_id=_opt_str(payload, "seller_id", 128),
            line_status=_opt_str(payload, "line_status", 40), order_status=order.status,
            reason=_opt_str(payload, "reason", 80), attempt_outcome=attempt_outcome,
            occurred_at=event.occurred_at, applied_at=now, payload=payload)
