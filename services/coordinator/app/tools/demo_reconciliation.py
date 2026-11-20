"""Controlled lost-reply demonstration using the real local coordinator DB."""
import argparse
import asyncio
from datetime import timedelta
import json
from uuid import uuid4

import httpx
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.models import Workflow
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.saga.reconciliation import Reconciler
from app.schemas import OrderCreatedEvent, utc_now
from app.tools.demo_sellers import DemoSellers


class LostReplySellers(DemoSellers):
    """Remote ledger fixture lasts for this one process, not a real seller DB."""
    def __init__(self):
        self.records = {}
        self.reserve_calls = 0
        self.lookup_calls = 0
        self.lose_next_reply = True

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.reserve_calls += 1
        reply = self.records.get(operation_key)
        if reply is None:
            reply = await super().reserve(seller_id, order_id, line, operation_key)
            self.records[operation_key] = reply
        if self.lose_next_reply:
            self.lose_next_reply = False
            raise httpx.ReadError("controlled reply loss after reservation")
        return reply

    async def lookup(self, seller_id, operation_key):
        self.lookup_calls += 1
        return self.records.get(operation_key)


async def run(order_id):
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    sessions = make_sessions(engine)
    try:
        await check_database(engine)
        async with sessions() as session:
            if await session.get(Workflow, order_id):
                raise ValueError("demo order already exists; choose a new --order-id")
        now = utc_now()
        incoming = OrderCreatedEvent.model_validate({
            "event_id": str(uuid4()), "event_type": "order.created",
            "schema_version": 1, "producer": "order", "occurred_at": now,
            "payload": {"order_id": order_id, "lines": [{
                "line_id": "line-1", "part_id": "bumper", "quantity": 1,
                "needed_by": now + timedelta(days=2),
            }]},
        })
        await accept_order(incoming, sessions, workflow_topic=settings.workflow_topic)
        seller = LostReplySellers()
        coordinator = Coordinator(
            sessions, seller, list(settings.seller_urls),
            lock=lambda oid: postgres_order_lock(engine, oid),
            workflow_topic=settings.workflow_topic,
            timeout_seconds=settings.seller_timeout_seconds,
            concurrency=settings.max_seller_concurrency,
        )
        before = await coordinator.run_order(order_id)
        after = await Reconciler(
            coordinator, max_cycles=settings.reconciliation_max_cycles,
            backoff_seconds=settings.reconciliation_backoff_seconds,
        ).run_order(order_id)
        print(json.dumps({
            "seller_mode": "controlled_lost_reply",
            "before": before, "after": after,
            "seller_reserve_calls": seller.reserve_calls,
            "seller_logical_reservations": len(seller.records),
            "seller_lookup_calls": seller.lookup_calls,
        }, indent=2))
        return 0
    except SQLAlchemyError as error:
        print(json.dumps({"error": "database_operation_failed",
                          "error_type": type(error).__name__}))
        return 4
    except (ValueError, RuntimeError) as error:
        print(json.dumps({"error": "demo_failed", "message": str(error)}))
        return 3
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id", default="demo-lost-reply-" + uuid4().hex[:12])
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.order_id)))


if __name__ == "__main__":
    main()
