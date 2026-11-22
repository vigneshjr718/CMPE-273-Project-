"""DB-commit-then-offset-commit behaviour, crash replay, and stop/restart catch-up."""
import asyncio

import pytest
from sqlalchemy import func, select

from app.consumers.event_consumer import ConsumerStatus, EventConsumer, supervise
from app.events import InvalidEvent
from app.models import ProcessedEvent, StatusHistory
from tests import mock_events as ev
from tests.conftest import (FakeBroker, FakeSource, SimulatedCrash, apply_all, make_projector,
                            snapshot)


async def reference(reference_sessions, events) -> dict:
    """Expected state: every distinct event applied once, in producer order."""
    seen, unique = set(), []
    for topic, raw in events:
        if raw not in seen:
            seen.add(raw)
            unique.append((topic, raw))
    await apply_all(make_projector(reference_sessions), unique)
    return await snapshot(reference_sessions)


async def start(consumer: EventConsumer):
    await consumer.source.start()
    return consumer


async def test_crash_between_db_commit_and_offset_commit_replays_safely(
        sessions, reference_sessions):
    events = ev.fulfilled() + ev.replaced()
    broker = FakeBroker()
    broker.produce_all(events)

    # 1st run: dies on the 5th record AFTER its DB transaction committed.
    first = await start(EventConsumer(FakeSource(broker, crash_on_commit=5),
                                      make_projector(sessions)))
    with pytest.raises(SimulatedCrash):
        await first.drain()
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 5
    assert sum(broker.committed.values()) == 4  # 5th offset never committed

    # Restart: resumes from committed offsets, so record 5 is delivered AGAIN.
    second = await start(EventConsumer(FakeSource(broker), make_projector(sessions)))
    await second.drain()

    assert second.status.outcomes.get("duplicate") == 1
    assert broker.committed == broker.end_offsets()
    assert await snapshot(sessions) == await reference(reference_sessions, events)


async def test_stop_restart_catches_up_without_duplicates_or_regression(
        sessions, reference_sessions):
    batch_one = ev.fulfilled("ord-a") + ev.reconciliation_required("ord-b")[:3]
    rest_b = ev.reconciliation_required("ord-b")[3:]
    batch_two = ev.unfulfilled("ord-c") + rest_b

    broker = FakeBroker()
    broker.produce_all(batch_one)
    first = await start(EventConsumer(FakeSource(broker), make_projector(sessions)))
    await first.drain()
    await first.source.stop()  # tracker stopped

    # While it is down: new events, plus producer replays of events it already has
    # (same event_id/payload, as after an outbox relay crash).
    broker.produce_all(batch_two)
    broker.produce_all(batch_one[2:5])

    second = await start(EventConsumer(FakeSource(broker, max_records=2),
                                       make_projector(sessions)))
    await second.drain()

    assert broker.committed == broker.end_offsets()  # fully caught up
    assert second.status.outcomes.get("duplicate") == 3
    assert await snapshot(sessions) == await reference(reference_sessions, batch_one + batch_two)
    async with sessions() as session:
        history = await session.scalar(select(func.count()).select_from(StatusHistory))
    assert history == len(batch_one) + len(batch_two)


async def test_invalid_event_stops_without_committing_its_offset(sessions):
    broker = FakeBroker(partitions=1)
    good = ev.order_created("ord-p")
    broker.produce(*good)
    broker.produce(ev.ORDER_TOPIC, b"{broken", key="ord-p")
    consumer = await start(EventConsumer(FakeSource(broker), make_projector(sessions)))
    with pytest.raises(InvalidEvent):
        await consumer.drain()
    assert broker.committed == {(ev.ORDER_TOPIC, 0): 1}  # stuck before the bad record


async def test_invalid_event_skip_mode_records_and_continues(sessions):
    broker = FakeBroker(partitions=1)
    broker.produce(ev.ORDER_TOPIC, b"{broken", key="ord-s")
    broker.produce(*ev.order_created("ord-s"))
    consumer = await start(EventConsumer(FakeSource(broker), make_projector(sessions),
                                         skip_invalid=True))
    await consumer.drain()
    assert broker.committed == {(ev.ORDER_TOPIC, 0): 2}
    async with sessions() as session:
        assert (await session.get(ProcessedEvent, f"invalid:{ev.ORDER_TOPIC}:0:0")).outcome \
            == "invalid_skipped"


async def test_supervisor_restarts_after_failure_and_finishes(sessions, reference_sessions):
    events = ev.fulfilled("ord-sup")
    broker = FakeBroker()
    broker.produce_all(events)
    sources = iter([FakeSource(broker, crash_on_commit=3), FakeSource(broker)])
    status, stop = ConsumerStatus(), asyncio.Event()
    task = asyncio.create_task(supervise(lambda: next(sources), make_projector(sessions),
                                         status=status, stop=stop, skip_invalid=False,
                                         backoff=0.01, backoff_max=0.02))
    deadline = asyncio.get_running_loop().time() + 15
    while broker.committed != broker.end_offsets():
        assert asyncio.get_running_loop().time() < deadline, "consumer did not catch up"
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert status.restarts == 1 and status.last_error_type == "SimulatedCrash"
    assert status.state == "stopped"
    assert await snapshot(sessions) == await reference(reference_sessions, events)
