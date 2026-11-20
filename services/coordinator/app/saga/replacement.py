"""Manual bounded cancellation/fencing, then replacement under the order lock."""
import asyncio
from dataclasses import dataclass
from datetime import timedelta
import logging

import httpx
from sqlalchemy import select

from app.clients.cancellation import require_cancelled, validate_cancellation
from app.models import CancellationCheck, DeliveryCheck, LineAttempt, WorkflowLine
from app.saga.keys import cancellation_key
from app.saga.orchestrator import aggregate, as_utc
from app.saga.states import LineState, require_transition
from app.schemas import OrderLine

logger = logging.getLogger("coordinator.replacement")


@dataclass(frozen=True)
class CancelRequest:
    order_id: str
    line: OrderLine
    attempt_id: str
    seller_id: str
    operation_key: str
    cancellation_key: str
    reservation_id: str | None


class ReplacementCoordinator:
    def __init__(self, coordinator, client, *, max_cycles=3, backoff_seconds=1):
        if not 1 <= max_cycles <= 10 or not 0 < backoff_seconds <= 60:
            raise ValueError("invalid cancellation retry budget")
        self.c, self.client = coordinator, client
        self.max_cycles, self.backoff = max_cycles, backoff_seconds

    def _delay(self, count):
        return min(60, self.backoff * 2 ** (count - 1))

    async def _event(self, session, workflow, row, attempt, check, event_type):
        await session.flush()
        lines = list(await session.scalars(select(WorkflowLine).where(
            WorkflowLine.order_id == row.order_id)))
        workflow.status = aggregate(lines)
        proof = check.last_payload or {}
        await self.c._emit(session, workflow, event_type, {
            "line_id": row.line_id, "part_id": row.part_id, "quantity": row.quantity,
            "line_status": row.status, "status": workflow.status, "reason": row.reason,
            "attempt_id": attempt.attempt_id, "seller_id": attempt.seller_id,
            "operation_key": attempt.operation_key, "reservation_id": attempt.reservation_id,
            "cancellation_key": check.cancellation_key,
            "cancellation_status": check.status, "cancellation_requests": check.request_count,
            "cancellation_error": check.last_error,
            "cancellation_receipt": proof.get("cancellation_receipt"),
            "source_status_version": check.status_version,
            "delivered_at": proof.get("delivered_at"),
            "delivery_status": "DELIVERED" if row.status == "DELIVERED" else None,
            "delivery_status_version": check.status_version if row.status == "DELIVERED" else None,
            "estimated_delivery_at": (as_utc(attempt.estimated_delivery_at).isoformat()
                                      if attempt.estimated_delivery_at else None),
            "next_cancellation_retry_at": (as_utc(check.next_retry_at).isoformat()
                                            if check.next_retry_at else None),
        })

    async def _begin(self, order_id, line):
        c = self.c
        async with c.sessions.begin() as session:
            workflow = await c._workflow(session, order_id)
            row = await session.get(WorkflowLine, (order_id, line.line_id))
            attempt = await session.get(LineAttempt, row.current_attempt_id) if row.current_attempt_id else None
            if attempt is None:
                return None
            if attempt.order_id != order_id or attempt.line_id != line.line_id:
                raise RuntimeError("cancellation attempt belongs to another line")
            check = await session.get(CancellationCheck, attempt.attempt_id)
            if check is None:
                late = row.status == attempt.status == "RESERVED" and row.reason in {
                    "DELIVERY_AFTER_DEADLINE", "DELIVERY_DEADLINE_PASSED_UNCONFIRMED"}
                uncertain = row.status in {"UNKNOWN", "RECONCILING", "RECONCILIATION_REQUIRED"} and attempt.status in {
                    "UNKNOWN", "RECONCILING", "RECONCILIATION_REQUIRED"}
                if not (late or uncertain):
                    return None
                require_transition(LineState(row.status), LineState.CANCELLATION_PENDING)
                row.status = attempt.status = "CANCELLATION_PENDING"
                row.reason = attempt.reason = "AWAITING_TERMINAL_FENCE"
                attempt.next_retry_at = None
                check = CancellationCheck(
                    attempt_id=attempt.attempt_id, order_id=order_id, line_id=line.line_id,
                    cancellation_key=cancellation_key(attempt.attempt_id), status="PENDING",
                    request_count=0, next_retry_at=c.clock())
                session.add(check)
                delivery = await session.get(DeliveryCheck, attempt.attempt_id)
                if delivery:
                    delivery.next_poll_at = None
            if check.order_id != order_id or check.line_id != line.line_id:
                raise RuntimeError("cancellation schedule belongs to another line")
            if row.status != "CANCELLATION_PENDING" or check.status != "PENDING":
                return None
            if check.request_count >= self.max_cycles:
                check.status, check.next_retry_at = "REQUIRED", None
                check.last_error = row.reason = attempt.reason = "CANCELLATION_BUDGET_EXHAUSTED"
                await self._event(session, workflow, row, attempt, check, "line.cancellation_deferred")
                return None
            if check.next_retry_at is None or as_utc(check.next_retry_at) > c.clock():
                return None
            check.request_count += 1
            check.next_retry_at = c.clock() + timedelta(seconds=self._delay(check.request_count))
            await self._event(session, workflow, row, attempt, check, "line.cancelling")
            # Commit key, intent, budget and schedule BEFORE the external write.
            return CancelRequest(order_id, line, attempt.attempt_id, attempt.seller_id,
                                 attempt.operation_key, check.cancellation_key, attempt.reservation_id)

    async def _fetch(self, request):
        try:
            reply = await asyncio.wait_for(self.client.cancel(
                request.seller_id, request.order_id, request.line, request.operation_key,
                request.cancellation_key, request.reservation_id), self.c.timeout)
            reply = validate_cancellation(
                reply, seller_id=request.seller_id, operation_key=request.operation_key,
                cancellation_key=request.cancellation_key, line=request.line,
                reservation_id=request.reservation_id)
            return reply, None
        except (TimeoutError, httpx.HTTPError):
            return None, "CANCELLATION_OUTCOME_UNKNOWN"
        except ValueError:
            return None, "INVALID_CANCELLATION_EVIDENCE"

    async def _record(self, request, reply, error):
        c = self.c
        async with c.sessions.begin() as session:
            workflow = await c._workflow(session, request.order_id)
            row = await session.get(WorkflowLine, (request.order_id, request.line.line_id))
            if row.current_attempt_id != request.attempt_id or row.status != "CANCELLATION_PENDING":
                return
            attempt = await session.get(LineAttempt, request.attempt_id)
            check = await session.get(CancellationCheck, request.attempt_id)
            if check.status != "PENDING":
                return
            delivery = await session.get(DeliveryCheck, request.attempt_id)
            payload = reply.model_dump(mode="json") if reply else None
            if reply and reply.delivered_at:
                payload["delivered_at"] = as_utc(reply.delivered_at).isoformat()
            if reply and check.status_version is not None:
                if reply.status_version < check.status_version:
                    error = "STALE_CANCELLATION_EVIDENCE"
                elif reply.status_version == check.status_version and payload != check.last_payload:
                    error = "CONFLICTING_CANCELLATION_VERSION"
            if reply and delivery and delivery.status_version is not None:
                # A terminal change must follow the previously observed IN_TRANSIT.
                if reply.status_version < delivery.status_version or (
                    reply.status != "PENDING" and reply.status_version == delivery.status_version
                ):
                    error = "STALE_CANCELLATION_EVIDENCE"
                if (delivery.last_payload or {}).get("status") == "DELIVERED" and reply.status == "CANCELLED":
                    error = "CONFLICTING_DELIVERY_PROOF"
            if error is None:
                check.last_payload, check.status_version = payload, reply.status_version
            if error or reply.status == "PENDING":
                exhausted = check.request_count >= self.max_cycles
                check.status = "REQUIRED" if exhausted else "PENDING"
                check.last_error = error or "SELLER_CANCELLATION_PENDING"
                row.reason = attempt.reason = (
                    "CANCELLATION_BUDGET_EXHAUSTED" if exhausted else check.last_error)
                check.next_retry_at = (None if exhausted else c.clock() + timedelta(
                    seconds=self._delay(check.request_count)))
                await self._event(session, workflow, row, attempt, check, "line.cancellation_deferred")
            else:
                check.next_retry_at, check.last_error = None, None
                if reply.reservation_id:
                    attempt.reservation_id = reply.reservation_id
                if reply.status == "CANCELLED":
                    require_transition(LineState(row.status), LineState.REPLACING)
                    check.status, attempt.status, row.status = "CONFIRMED", "CANCELLED", "REPLACING"
                    row.reason = attempt.reason = "TERMINAL_CANCELLATION_CONFIRMED"
                    event_type = "line.cancelled"
                else:
                    require_transition(LineState(row.status), LineState.DELIVERED)
                    check.status, row.status, attempt.status = "DELIVERED", "DELIVERED", "DELIVERED"
                    row.reason = attempt.reason = ("DELIVERED_AFTER_DEADLINE"
                        if reply.delivered_at > request.line.needed_by else None)
                    if delivery is None:
                        delivery = DeliveryCheck(attempt_id=attempt.attempt_id,
                            order_id=request.order_id, line_id=row.line_id, poll_count=0, failure_count=0)
                        session.add(delivery)
                    delivery.status_version, delivery.next_poll_at = reply.status_version, None
                    delivery.last_success_at, delivery.last_error = c.clock(), None
                    delivery.last_payload = {name: payload[name] for name in (
                        "seller_id", "operation_key", "reservation_id", "part_id", "quantity",
                        "status", "status_version", "delivered_at")}
                    delivery.last_payload["estimated_delivery_at"] = None
                    event_type = "line.delivered"
                await self._event(session, workflow, row, attempt, check, event_type)
        logger.info("cancellation_check_completed", extra={
            "order_id": request.order_id, "line_id": request.line.line_id,
            "attempt_id": request.attempt_id, "failure_type": error})

    async def _resume(self, order_id, line):
        c = self.c
        state, _ = await c._line_state(order_id, line.line_id)
        if state not in {"REPLACING", "QUOTING", "RECONCILIATION_REQUIRED"}:
            return
        async with c.sessions() as session:
            previous = list(await session.scalars(select(LineAttempt).where(
                LineAttempt.order_id == order_id, LineAttempt.line_id == line.line_id)))
            had_cancellation = False
            for old in previous:
                if old.status == "CANCELLED":
                    require_cancelled(old, await session.get(CancellationCheck, old.attempt_id), line)
                    had_cancellation = True
                elif old.status != "REJECTED":
                    return
            if not had_cancellation:
                return
        if state != "QUOTING":
            await c._set_state(order_id, line.line_id, "QUOTING", "line.replacement_started")
        await c._process_line(order_id, line)

    async def run_order(self, order_id):
        c = self.c
        async with c.lock(order_id):
            lines = await c._initialize(order_id)
            for line in lines:
                # Interrupted replacement writes also need fencing/reconciliation.
                state, attempt_id = await c._line_state(order_id, line.line_id)
                if state == "RESERVING":
                    await c._set_state(order_id, line.line_id, "UNKNOWN", "line.unknown",
                                       reason="INTERRUPTED_RESERVATION", attempt_id=attempt_id)
                request = await self._begin(order_id, line)
                if request:
                    await self._record(request, *await self._fetch(request))
                await self._resume(order_id, line)
            return await c.snapshot(order_id)
