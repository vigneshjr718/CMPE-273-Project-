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
from app.models import CancellationCheck, DeliveryCheck, LineAttempt, OutboxEvent, ProcessedEvent, Workflow, WorkflowJob, WorkflowLine
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
                    for model in (WorkflowJob, CancellationCheck, DeliveryCheck, OutboxEvent, ProcessedEvent, LineAttempt, WorkflowLine, Workflow):
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


async def test_postgres_relay_lock_excludes_another_connection(postgres):
    from app.outbox.locking import RelayBusy, postgres_relay_lock
    engine, _, _ = postgres
    async with postgres_relay_lock(engine):
        with pytest.raises(RelayBusy):
            async with postgres_relay_lock(engine):
                pytest.fail("two relay connections obtained the same lock")
    async with postgres_relay_lock(engine):
        pass


async def test_postgres_outbox_recovers_acknowledged_event_after_marker_failure(postgres):
    from app.outbox.locking import postgres_relay_lock
    from app.outbox.publisher import OutboxRelay
    from app.tools.demo_outbox import FailBeforeMarker, SimulatedMarkerFailure

    engine, sessions, ids = postgres
    event = make_event(ids)
    order_id = event.payload.order_id
    await accept_order(event, sessions, workflow_topic="workflow.events")
    records = []
    class AcknowledgingBroker:
        async def publish(self, record):
            # Broker fixture only; the companion opt-in Kafka test uses a real broker.
            records.append(record)
    kwargs = {"lock": lambda: postgres_relay_lock(engine)}
    with pytest.raises(SimulatedMarkerFailure):
        await FailBeforeMarker(sessions, AcknowledgingBroker(), **kwargs).run_once(order_id=order_id)
    async with sessions() as session:
        row = (await session.scalars(select(OutboxEvent).where(
            OutboxEvent.order_id == order_id))).one()
        assert row.published_at is None
    assert await OutboxRelay(sessions, AcknowledgingBroker(), **kwargs).run_once(order_id=order_id) == 1
    assert records[0] == records[1]
    async with sessions() as session:
        row = (await session.scalars(select(OutboxEvent).where(
            OutboxEvent.order_id == order_id))).one()
        assert row.published_at is not None


async def test_real_postgres_kafka_outbox_round_trip_and_replay(postgres):
    if os.getenv("COORDINATOR_KAFKA_TESTS") != "1":
        pytest.skip("set COORDINATOR_KAFKA_TESTS=1 for the real broker replay check")
    from app.tools.demo_outbox import run_demo
    result = await run_demo(Settings(), cleanup=True)
    assert result["published_outbox_rows"] == 5
    assert result["broker_records"] == 6
    assert result["distinct_event_ids"] == 5
    assert result["received_state_versions"] == [1, 1, 2, 3, 4, 5]
    assert result["first_event_replayed_with_same_id_and_payload"]


async def test_postgres_delivery_schedule_survives_new_driver_and_completes(postgres):
    from app.pollers.delivery_poller import DeliveryPoller
    from app.tools.demo_delivery import DeliveryDemoSellers

    engine, sessions, ids = postgres
    event = make_event(ids)
    order_id = event.payload.order_id
    await accept_order(event, sessions, workflow_topic="workflow.events")
    now = [datetime.now(timezone.utc)]
    seller = DeliveryDemoSellers(lambda: now[0])
    def coordinator():
        return Coordinator(
            sessions, seller, ["seller-1"],
            lock=lambda oid: postgres_order_lock(engine, oid),
            workflow_topic="workflow.events", clock=lambda: now[0], timeout_seconds=10,
        )
    await coordinator().run_order(order_id)
    first = await DeliveryPoller(coordinator(), seller).run_order(order_id)
    assert first["status"] == "RECONCILIATION_REQUIRED"
    assert first["lines"][0]["reason"] == "DELIVERY_AFTER_DEADLINE"
    assert first["state_version"] == 6
    assert await DeliveryPoller(coordinator(), seller).run_order(order_id) == first
    assert seller.delivery_calls == 1
    now[0] += timedelta(seconds=11)
    seller.finish_bumper = True
    result = await DeliveryPoller(coordinator(), seller).run_order(order_id)
    assert result["status"] == "FULFILLED" and result["state_version"] == 7
    assert result["lines"][0]["attempt_id"] == first["lines"][0]["attempt_id"]
    assert result["lines"][0]["next_delivery_poll_at"] is None
    assert seller.reserve_calls == 1 and seller.delivery_calls == 2
    async with sessions() as session:
        check = (await session.scalars(select(DeliveryCheck).where(
            DeliveryCheck.order_id == order_id))).one()
        assert check.poll_count == 2 and check.status_version == 2


async def test_postgres_delivery_claim_saved_before_http_and_order_lock_excludes_peer(postgres):
    from app.pollers.delivery_poller import DeliveryPoller
    from app.tools.demo_delivery import DeliveryDemoSellers

    engine, sessions, ids = postgres
    event = make_event(ids)
    order_id = event.payload.order_id
    await accept_order(event, sessions, workflow_topic="workflow.events")
    entered, release = asyncio.Event(), asyncio.Event()
    class BlockingSeller(DeliveryDemoSellers):
        async def delivery(self, seller_id, reservation_id):
            async with sessions() as session:
                check = (await session.scalars(select(DeliveryCheck).where(
                    DeliveryCheck.order_id == order_id))).one()
                assert check.poll_count == 1 and check.next_poll_at is not None
            entered.set()
            await release.wait()
            return await super().delivery(seller_id, reservation_id)
    seller = BlockingSeller(lambda: datetime.now(timezone.utc))
    def coordinator():
        return Coordinator(sessions, seller, ["seller-1"],
                           lock=lambda oid: postgres_order_lock(engine, oid),
                           workflow_topic="workflow.events", timeout_seconds=10)
    await coordinator().run_order(order_id)
    first = asyncio.create_task(DeliveryPoller(coordinator(), seller).run_order(order_id))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        with pytest.raises(OrderBusy):
            await DeliveryPoller(coordinator(), seller).run_order(order_id)
        release.set()
        await first
        assert seller.delivery_calls == 1
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)


async def test_postgres_lost_cancellation_reply_recovers_same_key_then_replaces(postgres):
    from app.pollers.delivery_poller import DeliveryPoller
    from app.saga.replacement import ReplacementCoordinator
    from app.tools.demo_replacement import FencingDemoSellers

    engine, sessions, ids = postgres
    event = make_event(ids)
    order_id = event.payload.order_id
    now = [datetime.now(timezone.utc)]
    seller = FencingDemoSellers(lambda: now[0])
    def coordinator():
        return Coordinator(sessions, seller, ["seller-1", "seller-2"],
            lock=lambda oid: postgres_order_lock(engine, oid),
            workflow_topic="workflow.events", clock=lambda: now[0], timeout_seconds=10)
    await accept_order(event, sessions, workflow_topic="workflow.events")
    await coordinator().run_order(order_id)
    await DeliveryPoller(coordinator(), seller).run_order(order_id)
    pending = await ReplacementCoordinator(coordinator(), seller).run_order(order_id)
    assert pending["lines"][0]["status"] == "CANCELLATION_PENDING" and seller.reserve_calls == 1
    assert await ReplacementCoordinator(coordinator(), seller).run_order(order_id) == pending
    now[0] += timedelta(seconds=2)
    result = await ReplacementCoordinator(coordinator(), seller).run_order(order_id)
    assert result["status"] == "RESERVED" and result["lines"][0]["seller_id"] == "seller-2"
    assert await ReplacementCoordinator(coordinator(), seller).run_order(order_id) == result
    assert seller.reserve_calls == seller.cancel_calls == 2
    async with sessions() as session:
        attempts = list(await session.scalars(select(LineAttempt).where(
            LineAttempt.order_id == order_id).order_by(LineAttempt.attempt_number)))
        check = await session.get(CancellationCheck, attempts[0].attempt_id)
        assert [a.status for a in attempts] == ["CANCELLED", "RESERVED"]
        assert check.request_count == 2 and check.status == "CONFIRMED"
        assert check.last_payload["operation_key"] == attempts[0].operation_key
        assert check.last_payload["fenced"] is True


async def test_postgres_cancellation_claim_commits_before_http_and_blocks_other_drivers(postgres):
    from app.pollers.delivery_poller import DeliveryPoller
    from app.saga.replacement import ReplacementCoordinator
    from app.tools.demo_replacement import FencingDemoSellers

    engine, sessions, ids = postgres
    event = make_event(ids)
    order_id = event.payload.order_id
    entered, release = asyncio.Event(), asyncio.Event()
    class BlockingSeller(FencingDemoSellers):
        async def cancel(self, *args):
            async with sessions() as session:
                check = (await session.scalars(select(CancellationCheck).where(
                    CancellationCheck.order_id == order_id))).one()
                assert check.request_count == 1 and check.next_retry_at is not None
                attempt = await session.get(LineAttempt, check.attempt_id)
                assert attempt.status == "CANCELLATION_PENDING"
            entered.set()
            await release.wait()
            return await super().cancel(*args)
    seller = BlockingSeller(lambda: datetime.now(timezone.utc))
    seller.lose_first_reply = False
    def coordinator():
        return Coordinator(sessions, seller, ["seller-1", "seller-2"],
            lock=lambda oid: postgres_order_lock(engine, oid), workflow_topic="workflow.events",
            timeout_seconds=10)
    await accept_order(event, sessions, workflow_topic="workflow.events")
    await coordinator().run_order(order_id)
    await DeliveryPoller(coordinator(), seller).run_order(order_id)
    first = asyncio.create_task(ReplacementCoordinator(coordinator(), seller).run_order(order_id))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        for driver in (coordinator(), DeliveryPoller(coordinator(), seller),
                       ReplacementCoordinator(coordinator(), seller)):
            with pytest.raises(OrderBusy):
                await driver.run_order(order_id)
        release.set()
        assert (await first)["status"] == "RESERVED"
        assert seller.cancel_calls == 1 and seller.reserve_calls == 2
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)


async def test_postgres_kafka_intake_commit_failure_replays_one_durable_acceptance(postgres):
    from app.consumers.kafka_orders import KafkaOrderIntake, OrderAssignment
    from app.tools.demo_order_consumer import FailBeforeOffsetCommit, SimulatedOffsetFailure
    from test_kafka_orders import Broker, P0, record

    _, sessions, ids = postgres
    event = make_event(ids)
    async def intake(broker):
        ownership = OrderAssignment()
        await ownership.on_partitions_assigned([P0])
        return KafkaOrderIntake(broker, ownership, sessions,
            incoming_topic="order.created", workflow_topic="workflow.events")
    broker = Broker([{P0: [record(event)]}])
    with pytest.raises(SimulatedOffsetFailure):
        await (await intake(FailBeforeOffsetCommit(broker))).run_once()
    async with sessions() as session:
        row = await session.get(Workflow, event.payload.order_id)
        assert (row.status, row.state_version) == ("PENDING", 1)
    replay = Broker([{P0: [record(event)]}])
    result = await (await intake(replay)).run_once()
    assert result.duplicate_event == 1 and replay.commits == [{P0: 1}]
    async with sessions() as session:
        for model in (Workflow, ProcessedEvent, OutboxEvent):
            assert await session.scalar(select(func.count()).select_from(model).where(
                model.order_id == event.payload.order_id)) == 1


async def test_real_postgres_kafka_intake_replay_offsets_and_poison_stop(postgres):
    if os.getenv("COORDINATOR_KAFKA_TESTS") != "1":
        pytest.skip("set COORDINATOR_KAFKA_TESTS=1 for real Kafka intake/offset checks")
    from app.tools.demo_order_consumer import run_demo
    result = await run_demo(Settings(), cleanup=True)
    assert result["offset_after_simulated_commit_failure"] is None
    assert result["recovered_intake"] == dict(processed=4, created=1, duplicate_event=2, duplicate_order=1)
    assert result["committed_offsets"] == {"0": 3, "1": 1}
    assert (result["workflow_count"], result["inbox_count"], result["outbox_count"]) == (2, 3, 2)
    assert result["fresh_consumer_processed"] == 0
    assert result["poison_record_stopped_at_offset"] == 3
    assert result["later_valid_order_was_not_accepted"] is True


async def test_postgres_scheduler_claim_is_committed_and_excludes_peer_drivers(postgres):
    from app.tools.run_workflows import make_scheduler
    from test_workflow_scheduler import Sellers

    engine,sessions,ids=postgres
    event=make_event(ids); order_id=event.payload.order_id
    await accept_order(event,sessions,workflow_topic="workflow.events")
    entered,release=asyncio.Event(),asyncio.Event()
    seller=Sellers(lambda: datetime.now(timezone.utc))
    original=seller.reserve
    async def blocked(*args):
        async with sessions() as session:
            job=await session.get(WorkflowJob,order_id)
            assert job.status=="RUNNING" and job.run_count==1 and job.next_run_at
        result=await original(*args)
        entered.set()
        await release.wait()
        return result
    seller.reserve=blocked
    def coordinator():
        return Coordinator(sessions,seller,["seller-1","seller-2"],
            lock=lambda oid:postgres_order_lock(engine,oid),workflow_topic="workflow.events",timeout_seconds=10)
    def worker():
        return make_scheduler(Settings(_env_file=None),coordinator(),seller,seller)
    first=asyncio.create_task(worker().run_once(order_id=order_id))
    try:
        await asyncio.wait_for(entered.wait(),10)
        assert await worker().run_order(order_id)=="busy"
        with pytest.raises(OrderBusy):
            await coordinator().run_order(order_id)
        release.set()
        assert (await first)["complete"]==1
        assert len(seller.reserve_calls)==1
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first,return_exceptions=True)


async def test_postgres_owned_process_restart_recovers_reservation_and_cancellation(postgres):
    from app.tools.demo_restart import run_demo
    result=await run_demo(Settings(),cleanup=True)
    assert result["restart_mode"]=="owned_worker_os_process_kill_and_fresh_start"
    assert [case["attempt_count"] for case in result["cases"]]==[1,2]
    assert all(case["final_status"]=="FULFILLED" and case["original_key_preserved"] for case in result["cases"])
    assert result["reservation_http_calls"]==3 and result["logical_reservations"]==3
    assert result["cancellation_http_calls"]==2 and result["terminal_fences"]==1
