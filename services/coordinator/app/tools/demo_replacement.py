"""Real PostgreSQL, controlled seller/clock: lost fence reply then safe replacement."""
import asyncio
from datetime import timedelta
import json
from uuid import uuid4

import httpx
from sqlalchemy import select

from app.clients.cancellation import CancellationReply
from app.clients.delivery import DeliveryReply
from app.clients.reservations import ReservationReply
from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.models import CancellationCheck, LineAttempt
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.saga.replacement import ReplacementCoordinator
from app.schemas import OrderCreatedEvent, Quote, utc_now


class FencingDemoSellers:
    """In-process ledger only. Not C, not persistent seller restart evidence."""
    def __init__(self, clock):
        self.clock = clock
        self.reservations, self.fences = {}, {}
        self.reserve_calls = self.cancel_calls = 0
        self.lose_first_reply = True

    async def quote(self, seller_id, line):
        return Quote(seller_id=seller_id, part_id=line.part_id, available_quantity=100,
                     estimated_delivery_at=self.clock() + timedelta(
                         hours=1 if seller_id == "seller-1" else 2))

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.reserve_calls += 1
        if operation_key in self.fences:
            raise ValueError("original operation is terminally fenced")
        if operation_key not in self.reservations:
            self.reservations[operation_key] = ReservationReply(
                seller_id=seller_id, operation_key=operation_key, part_id=line.part_id,
                quantity=line.quantity, status="RESERVED", reservation_id="demo-" + uuid4().hex,
                estimated_delivery_at=self.clock() + timedelta(hours=2))
        return self.reservations[operation_key]

    async def delivery(self, seller_id, reservation_id):
        saved = next(reply for reply in self.reservations.values()
                     if reply.reservation_id == reservation_id)
        return DeliveryReply(
            seller_id=seller_id, operation_key=saved.operation_key,
            reservation_id=reservation_id, part_id=saved.part_id, quantity=saved.quantity,
            status="IN_TRANSIT", status_version=1,
            estimated_delivery_at=self.clock() + timedelta(days=3))

    async def cancel(self, seller_id, order_id, line, operation_key,
                     cancellation_key, reservation_id):
        self.cancel_calls += 1
        if operation_key not in self.fences:
            self.fences[operation_key] = CancellationReply(
                seller_id=seller_id, operation_key=operation_key,
                cancellation_key=cancellation_key, part_id=line.part_id,
                quantity=line.quantity, reservation_id=reservation_id, status="CANCELLED",
                status_version=2, fenced=True, cancellation_receipt="fence-" + uuid4().hex)
        if self.lose_first_reply:
            self.lose_first_reply = False
            raise httpx.ReadError("controlled: cancellation committed but response lost")
        return self.fences[operation_key]


async def run_demo():
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        if engine.dialect.name != "postgresql":
            raise ValueError("demo requires migrated PostgreSQL")
        now = [utc_now()]
        clock = lambda: now[0]
        order_id = "demo-replacement-" + uuid4().hex[:12]
        event = OrderCreatedEvent.model_validate({
            "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
            "producer": "order", "occurred_at": clock(),
            "payload": {"order_id": order_id, "lines": [{"line_id": "line-1", "part_id": "bumper",
                "quantity": 1, "needed_by": clock() + timedelta(days=2)}]},
        })
        sessions = make_sessions(engine)
        await accept_order(event, sessions, workflow_topic=settings.workflow_topic)
        seller = FencingDemoSellers(clock)
        def coordinator():
            return Coordinator(sessions, seller, ["seller-1", "seller-2"],
                lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic, clock=clock)
        await coordinator().run_order(order_id)
        before = await DeliveryPoller(coordinator(), seller).run_order(order_id)
        pending = await ReplacementCoordinator(coordinator(), seller).run_order(order_id)
        assert pending["lines"][0]["status"] == "CANCELLATION_PENDING"
        assert seller.reserve_calls == 1
        assert await ReplacementCoordinator(coordinator(), seller).run_order(order_id) == pending
        now[0] += timedelta(seconds=2)
        after = await ReplacementCoordinator(coordinator(), seller).run_order(order_id)
        assert await ReplacementCoordinator(coordinator(), seller).run_order(order_id) == after
        async with sessions() as session:
            attempts = list(await session.scalars(select(LineAttempt).where(
                LineAttempt.order_id == order_id).order_by(LineAttempt.attempt_number)))
            proof = await session.get(CancellationCheck, attempts[0].attempt_id)
            receipt = proof.last_payload
        assert after["status"] == "RESERVED" and after["lines"][0]["seller_id"] == "seller-2"
        assert [a.status for a in attempts] == ["CANCELLED", "RESERVED"]
        assert attempts[0].operation_key != attempts[1].operation_key
        assert seller.reserve_calls == seller.cancel_calls == 2 and len(seller.fences) == 1
        return {"infrastructure": "real_postgresql", "seller_mode": "controlled_in_process",
                "clock_mode": "controlled_time_advance", "before": before,
                "after_lost_cancellation_reply": pending, "after_replacement": after,
                "original_terminal_fence": receipt, "immediate_repeat_skipped": True,
                "seller_reserve_calls": seller.reserve_calls, "seller_cancel_calls": seller.cancel_calls}
    finally:
        await engine.dispose()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run_demo()), indent=2))
