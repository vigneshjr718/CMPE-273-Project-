"""Bounded durable reconciliation. Never creates a new attempt or operation key."""
import asyncio
from datetime import timedelta

import httpx
from sqlalchemy import select

from app.clients.reservations import validate_reply
from app.models import LineAttempt, Workflow, WorkflowLine
from app.saga.orchestrator import Coordinator, aggregate, as_utc
from app.saga.states import LineState, require_transition
from app.schemas import OrderCreatedPayload


class Reconciler:
    def __init__(self, coordinator: Coordinator, *, max_cycles=3, backoff_seconds=1.0):
        if not 1 <= max_cycles <= 10 or not 0 < backoff_seconds <= 60:
            raise ValueError("invalid reconciliation budget")
        if not callable(getattr(coordinator.client, "lookup", None)):
            raise ValueError("reconciliation requires lookup-by-key support")
        self.coordinator = coordinator
        self.max_cycles = max_cycles
        self.backoff = backoff_seconds

    async def _begin_cycle(self, order_id, line):
        c = self.coordinator
        async with c.sessions.begin() as session:
            workflow = await c._workflow(session, order_id)
            row = await session.get(WorkflowLine, (order_id, line.line_id))
            if row is None or row.status not in {"UNKNOWN", "RECONCILING"}:
                return "skip", None
            attempt = await session.get(LineAttempt, row.current_attempt_id)
            if (attempt is None or attempt.order_id != order_id
                or attempt.line_id != line.line_id):
                raise RuntimeError("missing or mismatched saved reservation attempt")
            if attempt.retry_count >= self.max_cycles:
                return "exhausted", attempt
            if attempt.next_retry_at and as_utc(attempt.next_retry_at) > c.clock():
                return "not_due", attempt
            if row.status != "RECONCILING":
                require_transition(LineState(row.status), LineState.RECONCILING)
            row.status = attempt.status = "RECONCILING"
            row.reason = attempt.reason = "CHECKING_ORIGINAL_RESERVATION"
            attempt.retry_count += 1
            # Budget and schedule persist BEFORE any lookup/replayed write.
            delay = min(60.0, self.backoff * (2 ** (attempt.retry_count - 1)))
            attempt.next_retry_at = c.clock() + timedelta(seconds=delay)
            workflow.status = "RECONCILIATION_REQUIRED"
            await c._emit(session, workflow, "line.reconciling", {
                "line_id": row.line_id, "part_id": row.part_id,
                "line_status": "RECONCILING", "status": workflow.status,
                "attempt_id": attempt.attempt_id, "seller_id": attempt.seller_id,
                "operation_key": attempt.operation_key,
                "retry_count": attempt.retry_count,
            })
            return "started", attempt

    async def _defer(self, order_id, line, attempt_id, reason, *, force_required=False):
        c = self.coordinator
        async with c.sessions.begin() as session:
            workflow = await c._workflow(session, order_id)
            row = await session.get(WorkflowLine, (order_id, line.line_id))
            if row is None or row.current_attempt_id != attempt_id:
                raise RuntimeError("attempt no longer owns this line")
            attempt = await session.get(LineAttempt, attempt_id)
            if attempt is None:
                raise RuntimeError("saved attempt is missing")
            required = force_required or attempt.retry_count >= self.max_cycles
            target = "RECONCILIATION_REQUIRED" if required else "UNKNOWN"
            require_transition(LineState(row.status), LineState(target))
            row.status = attempt.status = target
            row.reason = attempt.reason = (
                reason if force_required else "RETRY_BUDGET_EXHAUSTED" if required else reason
            )
            if required:
                attempt.next_retry_at = None
            else:
                # Backoff starts after this failed call, even if it was slow.
                delay = min(60.0, self.backoff * (2 ** (attempt.retry_count - 1)))
                attempt.next_retry_at = c.clock() + timedelta(seconds=delay)
            await session.flush()
            lines = list(await session.scalars(
                select(WorkflowLine).where(WorkflowLine.order_id == order_id)))
            workflow.status = aggregate(lines)
            await c._emit(session, workflow,
                          "reconciliation.required" if required else "line.reconciliation_deferred", {
                "line_id": line.line_id, "part_id": line.part_id,
                "line_status": target, "status": workflow.status,
                "attempt_id": attempt_id, "seller_id": attempt.seller_id,
                "operation_key": attempt.operation_key,
                "reason": row.reason, "retry_count": attempt.retry_count,
                "next_retry_at": (as_utc(attempt.next_retry_at).isoformat()
                                  if attempt.next_retry_at else None),
            })

    async def _reconcile_line(self, order_id, line):
        c = self.coordinator
        state, attempt_id = await c._line_state(order_id, line.line_id)
        if state == "RESERVING":
            await c._set_state(order_id, line.line_id, "UNKNOWN", "line.unknown",
                               attempt_id=attempt_id, reason="INTERRUPTED_RESERVATION")
        elif state not in {"UNKNOWN", "RECONCILING"}:
            return
        action, attempt = await self._begin_cycle(order_id, line)
        if action in {"skip", "not_due"}:
            return
        if action == "exhausted":
            current, _ = await c._line_state(order_id, line.line_id)
            if current == "UNKNOWN":
                await c._set_state(order_id, line.line_id, "RECONCILING", "line.reconciling",
                                   reason="RETRY_BUDGET_EXHAUSTED", attempt_id=attempt.attempt_id)
            await self._defer(order_id, line, attempt.attempt_id, "RETRY_BUDGET_EXHAUSTED",
                              force_required=True)
            return
        try:
            reply = await asyncio.wait_for(
                c.client.lookup(attempt.seller_id, attempt.operation_key),
                timeout=c.timeout,
            )
            if reply is None:
                # NOT FOUND cannot prove an older in-flight request cannot commit.
                # Replay only the SAME key, relying on C's durable atomic idempotency.
                if line.needed_by < c.clock():
                    await self._defer(
                        order_id, line, attempt.attempt_id,
                        "DEADLINE_EXPIRED_OUTCOME_UNKNOWN", force_required=True,
                    )
                    return
                reply = await asyncio.wait_for(
                    c.client.reserve(attempt.seller_id, order_id, line, attempt.operation_key),
                    timeout=c.timeout,
                )
            validate_reply(reply, attempt.seller_id, line, attempt.operation_key)
        except (TimeoutError, httpx.HTTPError, ValueError):
            await self._defer(order_id, line, attempt.attempt_id, "ORIGINAL_OUTCOME_STILL_UNKNOWN")
            return
        if reply.status == "REJECTED":
            # Explicit terminal/no-effect proof resolves uncertainty, not a timeout.
            await c._set_state(
                order_id, line.line_id, "QUOTING", "line.reservation_rejected",
                reason="RECONCILED_EXPLICIT_REJECTION", attempt_id=attempt.attempt_id,
                reply=reply,
            )
            return
        await c._set_state(
            order_id, line.line_id, "RESERVED", "line.reserved",
            reason=("DELIVERY_AFTER_DEADLINE"
                    if reply.estimated_delivery_at > line.needed_by else None),
            attempt_id=attempt.attempt_id, reply=reply,
        )

    async def run_order(self, order_id):
        c = self.coordinator
        async with c.lock(order_id):
            async with c.sessions() as session:
                workflow = await session.get(Workflow, order_id)
                if workflow is None:
                    raise ValueError("order has not been accepted")
                lines = OrderCreatedPayload.model_validate(workflow.payload).lines
                persisted_ids = set(await session.scalars(select(WorkflowLine.line_id).where(
                    WorkflowLine.order_id == order_id)))
                if persisted_ids != {line.line_id for line in lines} and not (
                    not persisted_ids and workflow.status == "PENDING"
                ):
                    raise RuntimeError("persisted line set conflicts with accepted order")
            for line in lines:
                if line.line_id in persisted_ids:
                    await self._reconcile_line(order_id, line)
            return await c.snapshot(order_id)
