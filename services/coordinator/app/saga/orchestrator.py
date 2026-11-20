"""Durable normal reservation flow; unknown outcomes stop for reconciliation."""
import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import AsyncContextManager
from uuid import uuid4

import httpx
from sqlalchemy import select

from app.clients.reservations import ReservationClient, validate_reply
from app.models import DeliveryCheck, LineAttempt, OutboxEvent, Workflow, WorkflowLine
from app.saga.keys import reservation_key
from app.saga.selection import collect_quotes
from app.saga.states import LineState, require_transition
from app.schemas import OrderCreatedPayload, OrderLine, utc_now


def as_utc(value: datetime) -> datetime:
    # SQLite test driver drops offsets; application timestamps are stored as UTC.
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def aggregate(lines: list[WorkflowLine]) -> str:
    states = {line.status for line in lines}
    if any(line.reason in {"DELIVERY_AFTER_DEADLINE", "DELIVERY_DEADLINE_PASSED_UNCONFIRMED"}
           for line in lines) or states & {
        "UNKNOWN", "RECONCILING", "RECONCILIATION_REQUIRED", "CANCELLATION_PENDING",
    }:
        return "RECONCILIATION_REQUIRED"
    if states == {"DELIVERED"}:
        return "FULFILLED"
    if states <= {"RESERVED", "DELIVERED"}:
        return "RESERVED"
    if states == {"UNFULFILLED"}:
        return "UNFULFILLED"
    if states <= {"DELIVERED", "UNFULFILLED"}:
        return "PARTIALLY_FULFILLED"
    if states <= {"RESERVED", "DELIVERED", "UNFULFILLED"}:
        return "PARTIALLY_RESERVED"
    return "PROCESSING"


class Coordinator:
    def __init__(
        self, sessions, client: ReservationClient, seller_ids: list[str], *,
        lock: Callable[[str], AsyncContextManager],
        workflow_topic: str, timeout_seconds=2.0, concurrency=5, clock=utc_now,
    ):
        if not seller_ids or len(set(seller_ids)) != len(seller_ids):
            raise ValueError("configure unique nonempty seller IDs")
        if timeout_seconds <= 0 or concurrency < 1 or not workflow_topic.strip():
            raise ValueError("invalid timeout, concurrency or topic")
        self.sessions = sessions
        self.client = client
        self.seller_ids = seller_ids
        self.lock = lock
        self.topic = workflow_topic
        self.timeout = timeout_seconds
        self.concurrency = concurrency
        self.clock = clock

    async def _workflow(self, session, order_id):
        workflow = (await session.scalars(
            select(Workflow).where(Workflow.order_id == order_id).with_for_update()
        )).one_or_none()
        if workflow is None:
            raise ValueError("order has not been accepted")
        return workflow

    async def _emit(self, session, workflow, event_type, payload):
        emitted_at = self.clock()
        workflow.state_version += 1
        workflow.updated_at = emitted_at
        event_id = str(uuid4())
        session.add(OutboxEvent(
            event_id=event_id, order_id=workflow.order_id, topic=self.topic,
            event_type=event_type, state_version=workflow.state_version,
            created_at=emitted_at, payload={
                "event_id": event_id, "event_type": event_type,
                "schema_version": 1, "producer": "coordinator",
                "occurred_at": emitted_at.isoformat(),
                "order_id": workflow.order_id,
                "state_version": workflow.state_version, "payload": payload,
            },
        ))

    async def _initialize(self, order_id):
        async with self.sessions.begin() as session:
            workflow = await self._workflow(session, order_id)
            source = OrderCreatedPayload.model_validate(workflow.payload)
            existing = list(await session.scalars(
                select(WorkflowLine).where(WorkflowLine.order_id == order_id)
            ))
            if existing:
                if {
                    (r.line_id, r.part_id, r.quantity, as_utc(r.needed_by)) for r in existing
                } != {
                    (r.line_id, r.part_id, r.quantity, as_utc(r.needed_by)) for r in source.lines
                }:
                    raise RuntimeError("persisted line set conflicts with accepted order")
            else:
                for line in source.lines:
                    session.add(WorkflowLine(
                        order_id=order_id, line_id=line.line_id, part_id=line.part_id,
                        quantity=line.quantity, needed_by=as_utc(line.needed_by),
                        status="PENDING", attempt_count=0,
                    ))
                workflow.status = "PROCESSING"
                await self._emit(session, workflow, "workflow.started",
                                 {"status": "PROCESSING"})
            return source.lines

    async def _line_state(self, order_id, line_id):
        async with self.sessions() as session:
            row = await session.get(WorkflowLine, (order_id, line_id))
            if row is None:
                raise RuntimeError("missing workflow line")
            return row.status, row.current_attempt_id

    async def _set_state(
        self, order_id, line_id, target, event_type, *, reason=None,
        attempt_id=None, reply=None,
    ):
        async with self.sessions.begin() as session:
            workflow = await self._workflow(session, order_id)
            row = await session.get(WorkflowLine, (order_id, line_id))
            if row is None:
                raise RuntimeError("missing workflow line")
            require_transition(LineState(row.status), LineState(target))
            row.status, row.reason = target, reason
            if attempt_id:
                if row.current_attempt_id != attempt_id:
                    raise RuntimeError("attempt no longer owns this workflow line")
                attempt = await session.get(LineAttempt, attempt_id)
                if attempt is None:
                    raise RuntimeError("missing persisted attempt")
                attempt.status = "REJECTED" if reply and reply.status == "REJECTED" else target
                attempt.reason = reason
                if reply:
                    attempt.response_payload = reply.model_dump(mode="json")
                    attempt.reservation_id = reply.reservation_id
                    attempt.next_retry_at = None
                    attempt.estimated_delivery_at = (
                        as_utc(reply.estimated_delivery_at)
                        if reply.estimated_delivery_at else None
                    )
            await session.flush()
            lines = list(await session.scalars(
                select(WorkflowLine).where(WorkflowLine.order_id == order_id)
            ))
            workflow.status = aggregate(lines)
            payload = {
                "line_id": line_id, "part_id": row.part_id,
                "quantity": row.quantity, "line_status": target,
                "status": workflow.status, "reason": reason,
                "attempt_id": row.current_attempt_id,
            }
            if row.current_attempt_id:
                current = await session.get(LineAttempt, row.current_attempt_id)
                payload.update(seller_id=current.seller_id,
                               operation_key=current.operation_key,
                               reservation_id=current.reservation_id)
                if current.estimated_delivery_at:
                    payload["estimated_delivery_at"] = as_utc(current.estimated_delivery_at).isoformat()
            await self._emit(session, workflow, event_type, payload)

    async def _prepare_attempt(self, order_id, line: OrderLine, seller_id):
        async with self.sessions.begin() as session:
            workflow = await self._workflow(session, order_id)
            row = await session.get(WorkflowLine, (order_id, line.line_id))
            require_transition(LineState(row.status), LineState.RESERVING)
            number = row.attempt_count + 1
            key = reservation_key(order_id, line.line_id, seller_id, number)
            attempt_id = str(uuid4())
            attempt = LineAttempt(
                attempt_id=attempt_id, order_id=order_id, line_id=line.line_id,
                seller_id=seller_id, attempt_number=number, operation_key=key,
                status="RESERVING", retry_count=0, created_at=self.clock(),
            )
            session.add(attempt)
            row.status, row.current_attempt_id = "RESERVING", attempt_id
            row.attempt_count, row.reason = number, None
            workflow.status = "PROCESSING"
            await self._emit(session, workflow, "line.reserving", {
                "line_id": line.line_id, "part_id": line.part_id,
                "quantity": line.quantity, "line_status": "RESERVING",
                "status": "PROCESSING", "attempt_id": attempt_id,
                "seller_id": seller_id, "operation_key": key,
            })
            # Both commit BEFORE the seller request is even invoked.
            return attempt_id, key

    async def _process_line(self, order_id, line):
        state, old_attempt = await self._line_state(order_id, line.line_id)
        if state == "RESERVING":
            # Restart may have happened before or after the external side effect.
            await self._set_state(
                order_id, line.line_id, "UNKNOWN", "line.unknown",
                reason="INTERRUPTED_RESERVATION", attempt_id=old_attempt,
            )
            return
        if state not in {"PENDING", "QUOTING"}:
            return
        if state == "PENDING":
            await self._set_state(order_id, line.line_id, "QUOTING", "line.quoting")
        result = await collect_quotes(
            self.client, self.seller_ids, line, now=self.clock(),
            timeout_seconds=self.timeout, concurrency=self.concurrency,
        )
        if not result.eligible:
            reason = ("DEADLINE_EXPIRED" if line.needed_by < self.clock()
                      else "QUOTES_UNAVAILABLE" if result.failures
                      else "NO_ELIGIBLE_SELLER")
            target = "RECONCILIATION_REQUIRED" if result.failures else "UNFULFILLED"
            await self._set_state(order_id, line.line_id, target,
                                  "reconciliation.required" if result.failures else "line.unfulfilled",
                                  reason=reason)
            return
        for quote in result.eligible:
            if line.needed_by < self.clock():
                break
            if quote.estimated_delivery_at < self.clock():
                continue
            attempt_id, key = await self._prepare_attempt(order_id, line, quote.seller_id)
            try:
                reply = await asyncio.wait_for(
                    self.client.reserve(quote.seller_id, order_id, line, key),
                    timeout=self.timeout,
                )
                validate_reply(reply, quote.seller_id, line, key)
            except (TimeoutError, httpx.HTTPError, ValueError):
                await self._set_state(
                    order_id, line.line_id, "UNKNOWN", "line.unknown",
                    reason="RESERVATION_OUTCOME_UNKNOWN", attempt_id=attempt_id,
                )
                return  # Never switch sellers on an ambiguous write outcome.
            if reply.status == "REJECTED":
                await self._set_state(
                    order_id, line.line_id, "QUOTING", "line.reservation_rejected",
                    reason="EXPLICIT_RESERVATION_REJECTION", attempt_id=attempt_id,
                    reply=reply,
                )
                continue
            await self._set_state(
                order_id, line.line_id, "RESERVED", "line.reserved",
                reason=("DELIVERY_AFTER_DEADLINE"
                        if reply.estimated_delivery_at > line.needed_by else None),
                attempt_id=attempt_id, reply=reply,
            )
            return
        await self._set_state(
            order_id, line.line_id,
            "RECONCILIATION_REQUIRED" if result.failures else "UNFULFILLED",
            "reconciliation.required" if result.failures else "line.unfulfilled",
            reason="QUOTES_UNAVAILABLE" if result.failures else "NO_ELIGIBLE_SELLER",
        )

    async def run_order(self, order_id):
        async with self.lock(order_id):
            lines = await self._initialize(order_id)
            for line in lines:
                await self._process_line(order_id, line)
            return await self.snapshot(order_id)

    async def snapshot(self, order_id):
        async with self.sessions() as session:
            workflow = await session.get(Workflow, order_id)
            if workflow is None:
                raise ValueError("order has not been accepted")
            lines = list(await session.scalars(
                select(WorkflowLine).where(WorkflowLine.order_id == order_id)
                .order_by(WorkflowLine.line_id)
            ))
            values = []
            for row in lines:
                attempt = (await session.get(LineAttempt, row.current_attempt_id)
                           if row.current_attempt_id else None)
                delivery = (await session.get(DeliveryCheck, row.current_attempt_id)
                            if row.current_attempt_id else None)
                observation = delivery.last_payload if delivery and delivery.last_payload else {}
                values.append({
                    "line_id": row.line_id, "part_id": row.part_id,
                    "quantity": row.quantity, "status": row.status,
                    "reason": row.reason, "attempt_count": row.attempt_count,
                    "attempt_id": row.current_attempt_id,
                    "seller_id": attempt.seller_id if attempt else None,
                    "operation_key": attempt.operation_key if attempt else None,
                    "reservation_id": attempt.reservation_id if attempt else None,
                    "estimated_delivery_at": (as_utc(attempt.estimated_delivery_at).isoformat()
                                               if attempt and attempt.estimated_delivery_at else None),
                    "retry_count": attempt.retry_count if attempt else 0,
                    "next_retry_at": (as_utc(attempt.next_retry_at).isoformat()
                                      if attempt and attempt.next_retry_at else None),
                    "delivery_status": observation.get("status"),
                    "delivery_status_version": delivery.status_version if delivery else None,
                    "delivered_at": observation.get("delivered_at"),
                    "delivery_poll_count": delivery.poll_count if delivery else 0,
                    "delivery_poll_failures": delivery.failure_count if delivery else 0,
                    "delivery_poll_error": delivery.last_error if delivery else None,
                    "next_delivery_poll_at": (as_utc(delivery.next_poll_at).isoformat()
                                              if delivery and delivery.next_poll_at else None),
                })
            return {
                "order_id": order_id, "status": workflow.status,
                "state_version": workflow.state_version, "lines": values,
            }
