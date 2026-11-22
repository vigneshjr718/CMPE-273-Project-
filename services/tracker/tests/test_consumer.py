"""DB-commit-then-offset-commit behaviour, crash replay, and stop/restart catch-up."""
import asyncio
import json

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.consumers.event_consumer import ConsumerStatus, EventConsumer, supervise
from app.events import InvalidEvent
from app.models import DEAD_LETTER_MAX_BYTES, DeadLetterEvent, ProcessedEvent, StatusHistory
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


async def dead_letters(sessions):
    async with sessions() as session:
        return list(await session.scalars(select(DeadLetterEvent).order_by(DeadLetterEvent.id)))


async def test_default_policy_dead_letters_and_keeps_consuming(sessions, reference_sessions):
    """Poison messages between good ones: all good events are applied, offsets advance to
    the end, and each poison message is recorded once with enough context to debug it."""
    broker = FakeBroker(partitions=1)
    good = ev.fulfilled("ord-dl")
    no_version = json.loads(ev.Workflow("ord-dl").started()[1])
    del no_version["state_version"]
    poison = [
        (ev.ORDER_TOPIC, b"{broken"),
        (ev.WORKFLOW_TOPIC, json.dumps(no_version).encode()),
        (ev.ORDER_TOPIC, None),  # tombstone
    ]
    sequence = good[:3] + poison[:1] + good[3:6] + poison[1:] + good[6:]
    for topic, raw in sequence:
        if raw is None:
            broker.logs[(topic, 0)].append(None)  # tombstone (null value)
        else:
            broker.produce(topic, raw, key="ord-dl")

    consumer = await start(EventConsumer(FakeSource(broker), make_projector(sessions)))
    await consumer.drain()  # default policy: dead_letter

    assert broker.committed == broker.end_offsets()
    assert consumer.status.outcomes["dead_lettered"] == 3
    assert await snapshot(sessions) == await reference(reference_sessions, good)
    rows = sorted(await dead_letters(sessions), key=lambda r: (r.topic, r.kafka_offset))
    assert [(r.topic, r.raw_value, r.order_id, r.truncated) for r in rows] == [
        (ev.ORDER_TOPIC, b"{broken", None, False),
        (ev.ORDER_TOPIC, None, None, False),
        (ev.WORKFLOW_TOPIC, json.dumps(no_version).encode(), "ord-dl", False),
    ]
    assert "not UTF-8 JSON" in rows[0].reason
    assert "tombstone" in rows[1].reason and rows[1].raw_size == 0
    assert "state_version" in rows[2].reason and rows[2].event_id == no_version["event_id"]


async def test_dead_letter_is_not_duplicated_on_redelivery(sessions):
    """Crash after the dead-letter row commits but before the offset: replay adds no row."""
    broker = FakeBroker(partitions=1)
    broker.produce(*ev.order_created("ord-dl2"))
    broker.produce(ev.ORDER_TOPIC, b"\xff\xfe not utf8", key="ord-dl2")
    first = await start(EventConsumer(FakeSource(broker, crash_on_commit=2),
                                      make_projector(sessions)))
    with pytest.raises(SimulatedCrash):
        await first.drain()
    second = await start(EventConsumer(FakeSource(broker), make_projector(sessions)))
    await second.drain()
    assert broker.committed == {(ev.ORDER_TOPIC, 0): 2}
    assert len(await dead_letters(sessions)) == 1


async def test_dead_letter_truncates_oversized_payload(sessions):
    raw = b"x" * (DEAD_LETTER_MAX_BYTES + 10)
    assert await make_projector(sessions).dead_letter(ev.ORDER_TOPIC, 0, 7, raw, "bad")
    row = (await dead_letters(sessions))[0]
    assert row.truncated and row.raw_size == len(raw) and len(row.raw_value) == DEAD_LETTER_MAX_BYTES


async def test_stop_policy_halts_without_committing_its_offset(sessions):
    broker = FakeBroker(partitions=1)
    broker.produce(*ev.order_created("ord-p"))
    broker.produce(ev.ORDER_TOPIC, b"{broken", key="ord-p")
    broker.produce(*ev.order_created("ord-p2"))
    consumer = await start(EventConsumer(FakeSource(broker), make_projector(sessions),
                                         dead_letter_invalid=False))
    with pytest.raises(InvalidEvent):
        await consumer.drain()
    assert broker.committed == {(ev.ORDER_TOPIC, 0): 1}  # stuck before the bad record
    assert await dead_letters(sessions) == []


async def test_stop_policy_via_settings_keeps_supervisor_restarting(sessions):
    """The config switch reaches the running consumer: with policy=stop the supervisor
    keeps failing on the same record instead of moving past it."""
    settings = Settings(database_url="sqlite+aiosqlite://", invalid_event_policy="stop")
    assert settings.dead_letter_invalid is False
    assert Settings(database_url="sqlite+aiosqlite://").dead_letter_invalid is True

    broker = FakeBroker(partitions=1)
    broker.produce(ev.ORDER_TOPIC, b"{broken", key="ord-stop")
    status, stop = ConsumerStatus(), asyncio.Event()
    task = asyncio.create_task(supervise(lambda: FakeSource(broker), make_projector(sessions),
                                         status=status, stop=stop,
                                         dead_letter_invalid=settings.dead_letter_invalid,
                                         backoff=0.01, backoff_max=0.02))
    deadline = asyncio.get_running_loop().time() + 15
    while status.restarts < 2:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert status.last_error_type == "InvalidEvent"
    assert broker.committed == {} and await dead_letters(sessions) == []


async def test_supervisor_restarts_after_failure_and_finishes(sessions, reference_sessions):
    events = ev.fulfilled("ord-sup")
    broker = FakeBroker()
    broker.produce_all(events)
    sources = iter([FakeSource(broker, crash_on_commit=3), FakeSource(broker)])
    status, stop = ConsumerStatus(), asyncio.Event()
    task = asyncio.create_task(supervise(lambda: next(sources), make_projector(sessions),
                                         status=status, stop=stop, dead_letter_invalid=True,
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
