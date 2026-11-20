from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event as sql_event, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.consumers.order_created import IngestionConflict, accept_order
from app.models import Base, OutboxEvent, ProcessedEvent, Workflow
from app.schemas import OrderCreatedEvent


@pytest_asyncio.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ingestion.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture
def order_event(line, now):
    return OrderCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "order.created",
        "schema_version": 1, "producer": "order", "occurred_at": now,
        "payload": {"order_id": "o-42", "lines": [line.model_dump()]},
    })


async def counts(sessions):
    async with sessions() as session:
        return tuple([
            await session.scalar(select(func.count()).select_from(model))
            for model in (Workflow, ProcessedEvent, OutboxEvent)
        ])


async def test_acceptance_persists_workflow_inbox_and_outbox(sessions, order_event):
    result = await accept_order(order_event, sessions, workflow_topic="workflow.events")
    assert result.outcome == "created"
    assert await counts(sessions) == (1, 1, 1)
    async with sessions() as session:
        workflow = await session.get(Workflow, "o-42")
        assert workflow.payload["lines"][0]["quantity"] == 2
        assert (workflow.status, workflow.state_version) == ("PENDING", 1)
        outgoing = (await session.scalars(select(OutboxEvent))).one()
        assert outgoing.published_at is None
        assert outgoing.payload["event_id"] == outgoing.event_id
        assert outgoing.payload["state_version"] == 1
        assert outgoing.payload["payload"]["status"] == "PENDING"


async def test_replayed_event_is_duplicate_without_new_effects(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    result = await accept_order(order_event, sessions, workflow_topic="workflow.events")
    assert result.outcome == "duplicate_event"
    assert await counts(sessions) == (1, 1, 1)


async def test_different_event_same_order_records_inbox_only(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    other = order_event.model_copy(update={"event_id": uuid4()})
    result = await accept_order(other, sessions, workflow_topic="workflow.events")
    assert result.outcome == "duplicate_order"
    assert await counts(sessions) == (1, 2, 1)


@pytest.mark.parametrize("same_event_id", [True, False])
async def test_changed_order_payload_is_rejected_atomically(sessions, order_event, same_event_id):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    changed = order_event.model_dump(mode="json")
    changed["payload"]["lines"][0]["quantity"] = 99
    if not same_event_id:
        changed["event_id"] = str(uuid4())
    with pytest.raises(IngestionConflict):
        await accept_order(
            OrderCreatedEvent.model_validate(changed), sessions,
            workflow_topic="workflow.events",
        )
    assert await counts(sessions) == (1, 1, 1)


async def test_event_id_cannot_be_reused_for_another_order(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    changed = order_event.model_dump(mode="json")
    changed["payload"]["order_id"] = "o-different"
    with pytest.raises(IngestionConflict):
        await accept_order(
            OrderCreatedEvent.model_validate(changed), sessions,
            workflow_topic="workflow.events",
        )
    assert await counts(sessions) == (1, 1, 1)


async def test_replay_does_not_reset_advanced_workflow(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    async with sessions.begin() as session:
        workflow = await session.get(Workflow, "o-42")
        workflow.status = "PROCESSING"
        workflow.state_version = 7
    result = await accept_order(order_event, sessions, workflow_topic="workflow.events")
    assert (result.status, result.state_version) == ("PROCESSING", 7)
    assert await counts(sessions) == (1, 1, 1)


async def test_failure_after_parent_flush_rolls_back_all_records(sessions, order_event):
    def fail_on_outbox(session, *_):
        if any(isinstance(row, OutboxEvent) for row in session.new):
            raise RuntimeError("simulated failure before outbox insert")
    sql_event.listen(Session, "before_flush", fail_on_outbox)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            await accept_order(order_event, sessions, workflow_topic="workflow.events")
    finally:
        sql_event.remove(Session, "before_flush", fail_on_outbox)
    assert await counts(sessions) == (0, 0, 0)
    # Delivery can be retried after the transaction was rolled back.
    result = await accept_order(order_event, sessions, workflow_topic="workflow.events")
    assert result.outcome == "created"
    assert await counts(sessions) == (1, 1, 1)


async def test_empty_topic_cannot_create_partial_workflow(sessions, order_event):
    with pytest.raises(ValueError, match="workflow_topic"):
        await accept_order(order_event, sessions, workflow_topic="")
    assert await counts(sessions) == (0, 0, 0)
