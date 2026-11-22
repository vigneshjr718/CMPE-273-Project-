"""Opt-in PostgreSQL checks. Creates/deletes ONLY unique test-order records."""
import asyncio
from datetime import datetime, timedelta, timezone
import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select

from app.clients.reservations import ReservationReply
from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.models import LineAttempt, OutboxEvent, ProcessedEvent, Workflow, WorkflowLine
from app.saga.locking import OrderBusy, postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.schemas import OrderCreatedEvent, Quote


@pytest_asyncio.fixture
async def postgres():
    if os.getenv("COORDINATOR_POSTGRES_TESTS") != "1":
        pytest.skip("set COORDINATOR_POSTGRES_TESTS=1 to use the local migrated PostgreSQL")
    url = Settings().database_url.get_secret_value()
    if not url.startswith("postgresql+asyncpg://"):
        pytest.fail("PostgreSQL integration checks require a PostgreSQL URL")
    engine = make_engine(url)
    ids = []
    try:
        await check_database(engine)
        yield engine, make_sessions(engine), ids
    finally:
        try:
            if ids:
                async with engine.begin() as connection:
                    # Exact generated IDs only, not a database/table-wide reset.
                    for model in (OutboxEvent, ProcessedEvent, LineAttempt, WorkflowLine, Workflow):
                        await connection.execute(delete(model).where(model.order_id.in_(ids)))
        finally:
            await engine.dispose()


def make_event(ids):
    order_id = "coordinator-test-" + str(uuid4())
    ids.append(order_id)
    now = datetime.now(timezone.utc)
    event = OrderCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
        "producer": "order", "occurred_at": now,
        "payload": {"order_id": order_id, "lines": [{
            "line_id": "line-1", "part_id": "bumper", "quantity": 1,
            "needed_by": now + timedelta(days=2),
        }]},
    })
    return event


async def test_postgres_session_lock_excludes_another_connection(postgres):
    engine, _, ids = postgres
    order_id = make_event(ids).payload.order_id
    async with postgres_order_lock(engine, order_id):
        with pytest.raises(OrderBusy):
            async with postgres_order_lock(engine, order_id):
                pytest.fail("two connections obtained the same order lock")
    # Lock is released back to the pool after the context exits.
    async with postgres_order_lock(engine, order_id):
        pass


async def test_postgres_concurrent_order_delivery_has_one_business_effect(postgres):
    _, sessions, ids = postgres
    event = make_event(ids)
    results = await asyncio.gather(*[
        accept_order(event, sessions, workflow_topic="workflow.events")
        for _ in range(4)
    ])
    assert sum(r.outcome == "created" for r in results) == 1
    async with sessions() as session:
        for model in (Workflow, ProcessedEvent, OutboxEvent):
            count = await session.scalar(select(func.count()).select_from(model).where(
                model.order_id == event.payload.order_id))
            assert count == 1


async def test_postgres_two_coordinators_do_not_duplicate_reservation(postgres):
    engine, sessions, ids = postgres
    event = make_event(ids)
    await accept_order(event, sessions, workflow_topic="workflow.events")
    entered, release = asyncio.Event(), asyncio.Event()

    class ControlledSeller:
        calls = 0
        async def quote(self, seller_id, line):
            return Quote(seller_id=seller_id, part_id=line.part_id,
                         available_quantity=3,
                         estimated_delivery_at=datetime.now(timezone.utc) + timedelta(hours=1))

        async def reserve(self, seller_id, order_id, line, operation_key):
            self.calls += 1
            async with sessions() as session:
                persisted = (await session.scalars(select(LineAttempt).where(
                    LineAttempt.operation_key == operation_key))).one()
                assert persisted.status == "RESERVING"
            entered.set()
            await release.wait()
            return ReservationReply(
                seller_id=seller_id, operation_key=operation_key,
                part_id=line.part_id, quantity=line.quantity, status="RESERVED",
                reservation_id="test-reservation",
                estimated_delivery_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )

    seller = ControlledSeller()
    def coordinator():
        return Coordinator(
            sessions, seller, ["seller-1"],
            lock=lambda oid: postgres_order_lock(engine, oid),
            workflow_topic="workflow.events", timeout_seconds=10,
        )
    first = asyncio.create_task(coordinator().run_order(event.payload.order_id))
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        with pytest.raises(OrderBusy):
            await coordinator().run_order(event.payload.order_id)
        release.set()
        result = await first
        assert result["status"] == "RESERVED"
        replay = await coordinator().run_order(event.payload.order_id)
        assert replay == result
        assert seller.calls == 1
        async with sessions() as session:
            count = await session.scalar(select(func.count()).select_from(LineAttempt).where(
                LineAttempt.order_id == event.payload.order_id))
            assert count == 1
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)


async def test_postgres_new_coordinator_reconciles_saved_unknown(postgres):
    from app.saga.reconciliation import Reconciler
    from app.tools.demo_reconciliation import LostReplySellers

    engine, sessions, ids = postgres
    event = make_event(ids)
    await accept_order(event, sessions, workflow_topic="workflow.events")
    seller = LostReplySellers()
    def coordinator():
        return Coordinator(
            sessions, seller, ["seller-1", "seller-2"],
            lock=lambda oid: postgres_order_lock(engine, oid),
            workflow_topic="workflow.events", timeout_seconds=10,
        )
    before = await coordinator().run_order(event.payload.order_id)
    assert before["lines"][0]["status"] == "UNKNOWN"
    # Fresh driver/session reads the durable attempt; remote ledger is a fixture.
    after = await Reconciler(coordinator()).run_order(event.payload.order_id)
    assert after["status"] == "RESERVED"
    assert after["lines"][0]["attempt_id"] == before["lines"][0]["attempt_id"]
    assert after["lines"][0]["operation_key"] == before["lines"][0]["operation_key"]
    assert after["lines"][0]["retry_count"] == 1
    assert after["state_version"] == 7
    assert seller.reserve_calls == seller.lookup_calls == len(seller.records) == 1
    async with sessions() as session:
        for model, expected in ((LineAttempt, 1), (OutboxEvent, 7)):
            count = await session.scalar(select(func.count()).select_from(model).where(
                model.order_id == event.payload.order_id))
            assert count == expected
