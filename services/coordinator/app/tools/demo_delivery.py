"""Real coordinator DB, controlled sellers/time; no cancellation or replacement."""
import asyncio
from datetime import timedelta
import json
from uuid import uuid4

from app.clients.delivery import DeliveryReply
from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.schemas import OrderCreatedEvent, utc_now
from app.tools.demo_sellers import DemoSellers


class DeliveryDemoSellers(DemoSellers):
    def __init__(self, clock):
        self.clock, self.records = clock, {}
        self.reserve_calls, self.delivery_calls = 0, 0
        self.finish_bumper = False

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.reserve_calls += 1
        reply = await super().reserve(seller_id, order_id, line, operation_key)
        self.records[reply.reservation_id] = (reply, line)
        return reply

    async def delivery(self, seller_id, reservation_id):
        self.delivery_calls += 1
        saved, line = self.records[reservation_id]
        delivered = line.part_id != "bumper" or self.finish_bumper
        return DeliveryReply(
            seller_id=seller_id, operation_key=saved.operation_key,
            reservation_id=reservation_id, part_id=line.part_id, quantity=line.quantity,
            status="DELIVERED" if delivered else "IN_TRANSIT",
            status_version=2 if self.finish_bumper else 1,
            estimated_delivery_at=None if delivered else line.needed_by + timedelta(hours=1),
            delivered_at=self.clock() if delivered else None,
        )


async def run_demo():
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        if engine.dialect.name != "postgresql":
            raise ValueError("demo requires migrated PostgreSQL")
        now = [utc_now()]
        clock = lambda: now[0]
        order_id = "demo-delivery-" + uuid4().hex[:12]
        event = OrderCreatedEvent.model_validate({
            "event_id": str(uuid4()), "event_type": "order.created",
            "schema_version": 1, "producer": "order", "occurred_at": clock(),
            "payload": {"order_id": order_id, "lines": [
                {"line_id": f"line-{i}", "part_id": part, "quantity": 1,
                 "needed_by": clock() + timedelta(days=2)}
                for i, part in enumerate(("bumper", "mirror"), 1)
            ]},
        })
        sessions = make_sessions(engine)
        await accept_order(event, sessions, workflow_topic=settings.workflow_topic)
        seller = DeliveryDemoSellers(clock)
        def coordinator():
            return Coordinator(
                sessions, seller, ["seller-1"],
                lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic, clock=clock,
            )
        before = await coordinator().run_order(order_id)
        first = await DeliveryPoller(coordinator(), seller).run_order(order_id)
        calls = seller.delivery_calls
        await DeliveryPoller(coordinator(), seller).run_order(order_id)
        no_early_poll = seller.delivery_calls == calls
        now[0] += timedelta(seconds=11)  # Controlled clock; no blocking sleep.
        seller.finish_bumper = True
        after = await DeliveryPoller(coordinator(), seller).run_order(order_id)
        assert first["status"] == "RECONCILIATION_REQUIRED"
        assert first["lines"][0]["reason"] == "DELIVERY_AFTER_DEADLINE"
        assert after["status"] == "FULFILLED"
        assert no_early_poll and seller.reserve_calls == 2 and seller.delivery_calls == 3
        return {
            "infrastructure": "real_postgresql", "seller_mode": "controlled_demo",
            "clock_mode": "controlled_time_advance", "before": before,
            "after_first_poll": first, "after_due_poll": after,
            "immediate_repeat_skipped": no_early_poll,
            "seller_reserve_calls": seller.reserve_calls,
            "seller_delivery_calls": seller.delivery_calls,
        }
    finally:
        await engine.dispose()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run_demo()), indent=2))
