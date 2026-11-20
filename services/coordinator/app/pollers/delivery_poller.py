"""Poll known reservations; never cancel, replace or infer delivery from a timeout."""
import asyncio
from dataclasses import dataclass
from datetime import timedelta
import logging

import httpx
from sqlalchemy import select

from app.clients.delivery import DeliveryClient, DeliveryReply
from app.models import DeliveryCheck, LineAttempt, WorkflowLine
from app.saga.orchestrator import aggregate, as_utc
from app.saga.states import LineState, require_transition
from app.schemas import OrderLine

logger = logging.getLogger("coordinator.delivery")
OVERDUE = "DELIVERY_DEADLINE_PASSED_UNCONFIRMED"
LATE = "DELIVERY_AFTER_DEADLINE"


@dataclass(frozen=True)
class PollRequest:
    order_id: str
    line: OrderLine
    attempt_id: str
    seller_id: str
    operation_key: str
    reservation_id: str


class DeliveryPoller:
    def __init__(self, coordinator, client: DeliveryClient, *, interval_seconds=10,
                 concurrency=5):
        if not 0 < interval_seconds <= 300 or not 1 <= concurrency <= 20:
            raise ValueError("invalid delivery poll interval or concurrency")
        self.c, self.client, self.interval = coordinator, client, interval_seconds
        self.semaphore = asyncio.Semaphore(concurrency)

    async def _claim(self, order_id):
        requests = []
        now = self.c.clock()
        async with self.c.sessions.begin() as session:
            await self.c._workflow(session, order_id)
            lines = list(await session.scalars(select(WorkflowLine).where(
                WorkflowLine.order_id == order_id, WorkflowLine.status == "RESERVED"
            ).order_by(WorkflowLine.line_id)))
            for row in lines:
                attempt = await session.get(LineAttempt, row.current_attempt_id)
                if (attempt is None or attempt.status != "RESERVED"
                        or attempt.order_id != order_id or attempt.line_id != row.line_id
                        or not attempt.reservation_id):
                    raise RuntimeError("reserved line has inconsistent attempt evidence")
                check = await session.get(DeliveryCheck, attempt.attempt_id)
                if check is None:
                    check = DeliveryCheck(
                        attempt_id=attempt.attempt_id, order_id=order_id,
                        line_id=row.line_id, poll_count=0, failure_count=0,
                        next_poll_at=now,
                    )
                    session.add(check)
                if check.order_id != order_id or check.line_id != row.line_id:
                    raise RuntimeError("delivery schedule belongs to another line")
                if check.next_poll_at is None or as_utc(check.next_poll_at) > now:
                    continue
                check.poll_count += 1
                check.last_attempt_at = now
                # Save before IO so a crash/restart does not immediately hammer C.
                check.next_poll_at = now + timedelta(seconds=self.interval)
                requests.append(PollRequest(
                    order_id, OrderLine(line_id=row.line_id, part_id=row.part_id,
                                        quantity=row.quantity, needed_by=as_utc(row.needed_by)),
                    attempt.attempt_id, attempt.seller_id, attempt.operation_key,
                    attempt.reservation_id,
                ))
        return requests

    async def _fetch(self, request):
        try:
            async with self.semaphore:
                reply = await asyncio.wait_for(self.client.delivery(
                    request.seller_id, request.reservation_id), self.c.timeout)
            reply = DeliveryReply.model_validate(reply)
            if (reply.seller_id, reply.operation_key, reply.reservation_id,
                reply.part_id, reply.quantity) != (
                request.seller_id, request.operation_key, request.reservation_id,
                request.line.part_id, request.line.quantity,
            ):
                raise ValueError("delivery observation does not identify the saved attempt")
            return reply, None
        except (TimeoutError, httpx.HTTPError):
            return None, "DELIVERY_LOOKUP_UNAVAILABLE"
        except ValueError:
            return None, "INVALID_DELIVERY_OBSERVATION"

    async def _record(self, request, reply, error):
        now = self.c.clock()
        async with self.c.sessions.begin() as session:
            workflow = await self.c._workflow(session, request.order_id)
            row = await session.get(WorkflowLine, (request.order_id, request.line.line_id))
            check = await session.get(DeliveryCheck, request.attempt_id)
            # Protect against stale replies if ownership changes outside this driver.
            if row.status != "RESERVED" or row.current_attempt_id != request.attempt_id:
                return
            attempt = await session.get(LineAttempt, request.attempt_id)
            previous_error, previous_reason = check.last_error, row.reason
            payload = reply.model_dump(mode="json") if reply else None
            if reply:
                for name in ("estimated_delivery_at", "delivered_at"):
                    value = getattr(reply, name)
                    payload[name] = as_utc(value).isoformat() if value else None
            if reply and check.status_version is not None:
                if reply.status_version < check.status_version:
                    error = "STALE_DELIVERY_OBSERVATION"
                elif reply.status_version == check.status_version and payload != check.last_payload:
                    error = "CONFLICTING_DELIVERY_VERSION"
            event_type = None
            if error:
                check.failure_count += 1
                check.last_error = error
                delay = min(self.interval * 2 ** min(check.failure_count - 1, 8),
                            max(self.interval, 60))
                check.next_poll_at = now + timedelta(seconds=delay)
                if now > request.line.needed_by and row.reason not in {LATE, OVERDUE}:
                    row.reason = OVERDUE
                if previous_error != error or previous_reason != row.reason:
                    event_type = "line.delivery_check_deferred"
            else:
                changed = check.last_payload != payload
                check.last_payload, check.status_version = payload, reply.status_version
                check.last_success_at, check.last_error, check.failure_count = now, None, 0
                check.next_poll_at = now + timedelta(seconds=self.interval)
                if reply.status == "DELIVERED":
                    require_transition(LineState(row.status), LineState.DELIVERED)
                    row.status = attempt.status = "DELIVERED"
                    row.reason = ("DELIVERED_AFTER_DEADLINE"
                                  if reply.delivered_at > request.line.needed_by else None)
                    check.next_poll_at = None
                    event_type = "line.delivered"
                else:
                    attempt.estimated_delivery_at = as_utc(reply.estimated_delivery_at)
                    row.reason = (LATE if reply.estimated_delivery_at > request.line.needed_by
                                  else OVERDUE if now > request.line.needed_by else None)
                    if changed or previous_error or previous_reason != row.reason:
                        event_type = "line.delivery_delayed" if row.reason else "line.delivery_updated"
            attempt.reason = row.reason
            if event_type:
                await session.flush()
                lines = list(await session.scalars(select(WorkflowLine).where(
                    WorkflowLine.order_id == request.order_id)))
                workflow.status = aggregate(lines)
                observation = check.last_payload or {}
                await self.c._emit(session, workflow, event_type, {
                    "line_id": row.line_id, "part_id": row.part_id, "quantity": row.quantity,
                    "line_status": row.status, "status": workflow.status, "reason": row.reason,
                    "attempt_id": request.attempt_id, "seller_id": request.seller_id,
                    "operation_key": request.operation_key,
                    "reservation_id": request.reservation_id,
                    "delivery_status": observation.get("status"),
                    "delivery_status_version": check.status_version,
                    "delivery_check_error": check.last_error,
                    "estimated_delivery_at": (as_utc(attempt.estimated_delivery_at).isoformat()
                                               if attempt.estimated_delivery_at else None),
                    "delivered_at": observation.get("delivered_at"),
                })
        logger.info("delivery_check_completed", extra={
            "order_id": request.order_id, "line_id": request.line.line_id,
            "attempt_id": request.attempt_id, "failure_type": error,
        })

    async def run_order(self, order_id):
        async with self.c.lock(order_id):
            requests = await self._claim(order_id)
            results = await asyncio.gather(*[self._fetch(r) for r in requests],
                                           return_exceptions=True)
            for request, result in zip(requests, results):
                if isinstance(result, BaseException):
                    raise result
                await self._record(request, *result)
            return await self.c.snapshot(order_id)
