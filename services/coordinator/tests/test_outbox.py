import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event as sql_event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Base, OutboxEvent, Workflow
from app.outbox.kafka import KafkaPublisher, kafka_publisher
from app.outbox.publisher import OutboxDataError, OutboxRelay


@asynccontextmanager
async def no_lock():
    # Test-only. Production entry points always use PostgreSQL advisory locking.
    yield


@pytest_asyncio.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'outbox.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def seed(sessions, now, *, order_id="order-a", versions=(1, 2, 3)):
    async with sessions.begin() as session:
        session.add(Workflow(order_id=order_id, payload={}, status="PROCESSING",
                             state_version=max(versions)))
        await session.flush()
        for version in versions:
            event_id = str(uuid4())
            session.add(OutboxEvent(
                event_id=event_id, order_id=order_id, topic="workflow.events",
                event_type="line.quoting", state_version=version,
                # Deliberately reversed wall-clock order: versions must win.
                created_at=now - timedelta(seconds=version),
                payload={"event_id": event_id, "order_id": order_id,
                         "state_version": version, "event_type": "line.quoting",
                         "producer": "coordinator", "schema_version": 1,
                         "occurred_at": now.isoformat(), "payload": {"status": "PROCESSING"}},
            ))


async def rows(sessions):
    async with sessions() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(
            OutboxEvent.order_id, OutboxEvent.state_version)))


class RecordingPublisher:
    def __init__(self):
        self.records = []

    async def publish(self, record):
        self.records.append(record)


def relay(sessions, publisher, **kwargs):
    return OutboxRelay(sessions, publisher, lock=no_lock, **kwargs)


async def test_publish_in_version_order_and_mark_only_after_ack(sessions, now):
    await seed(sessions, now)
    class CheckingPublisher(RecordingPublisher):
        async def publish(self, record):
            current = next(r for r in await rows(sessions) if r.event_id == record.event_id)
            assert current.published_at is None
            assert json.loads(record.value) == current.payload
            await super().publish(record)
    publisher = CheckingPublisher()
    assert await relay(sessions, publisher).run_once() == 3
    assert [r.state_version for r in publisher.records] == [1, 2, 3]
    assert all(r.key == b"order-a" for r in publisher.records)
    assert all(r.published_at is not None for r in await rows(sessions))
    assert await relay(sessions, publisher).run_once() == 0
    assert len(publisher.records) == 3


async def test_broker_error_leaves_event_pending_and_stops_later_versions(sessions, now):
    await seed(sessions, now)
    publisher = AsyncMock()
    publisher.publish.side_effect = ConnectionError("broker down")
    with pytest.raises(ConnectionError):
        await relay(sessions, publisher).run_once()
    assert publisher.publish.await_count == 1
    assert all(r.published_at is None for r in await rows(sessions))
    recovered = RecordingPublisher()
    assert await relay(sessions, recovered).run_once() == 3
    assert [r.state_version for r in recovered.records] == [1, 2, 3]


async def test_acknowledged_send_with_failed_db_commit_replays_identical_event(sessions, now):
    await seed(sessions, now)
    publisher = RecordingPublisher()
    def fail_marker(session):
        if any(isinstance(r, OutboxEvent) for r in session.dirty):
            raise RuntimeError("simulated marker commit failure")
    sql_event.listen(Session, "before_commit", fail_marker)
    try:
        with pytest.raises(RuntimeError, match="marker"):
            await relay(sessions, publisher).run_once()
    finally:
        sql_event.remove(Session, "before_commit", fail_marker)
    assert all(r.published_at is None for r in await rows(sessions))
    assert len(publisher.records) == 1
    first = publisher.records[0]
    # A new relay has no in-memory knowledge of the previous send.
    assert await relay(sessions, publisher).run_once() == 3
    assert publisher.records[1] == first
    assert [r.state_version for r in publisher.records] == [1, 1, 2, 3]


async def test_ambiguous_publish_timeout_keeps_same_event_for_retry(sessions, now):
    await seed(sessions, now, versions=(1,))
    class LostAck(RecordingPublisher):
        async def publish(self, record):
            await super().publish(record)
            await asyncio.Event().wait()
    first = LostAck()
    with pytest.raises(TimeoutError):
        await relay(sessions, first, timeout_seconds=.02).run_once()
    assert (await rows(sessions))[0].published_at is None
    second = RecordingPublisher()
    assert await relay(sessions, second).run_once() == 1
    assert first.records == second.records


async def test_cancellation_does_not_mark_event_published(sessions, now):
    await seed(sessions, now, versions=(1,))
    publisher = AsyncMock()
    publisher.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await relay(sessions, publisher).run_once()
    assert (await rows(sessions))[0].published_at is None


async def test_exact_order_filter_and_bounded_batches(sessions, now):
    await seed(sessions, now)
    await seed(sessions, now, order_id="order-b")
    publisher = RecordingPublisher()
    runner = relay(sessions, publisher)
    assert await runner.run_once(limit=1, order_id="order-b") == 1
    assert await runner.run_once(limit=1, order_id="order-b") == 1
    assert [r.state_version for r in publisher.records] == [1, 2]
    assert {r.order_id for r in publisher.records} == {"order-b"}
    assert all(r.published_at is None for r in await rows(sessions) if r.order_id == "order-a")
    assert await runner.run_once(order_id="absent") == 0


async def test_bad_envelope_is_not_silently_skipped(sessions, now):
    await seed(sessions, now)
    async with sessions.begin() as session:
        row = (await session.scalars(select(OutboxEvent).where(
            OutboxEvent.state_version == 1))).one()
        row.payload = {**row.payload, "order_id": "wrong"}
    publisher = RecordingPublisher()
    with pytest.raises(OutboxDataError):
        await relay(sessions, publisher).run_once()
    assert not publisher.records
    assert all(r.published_at is None for r in await rows(sessions))


async def test_stop_signal_finishes_current_ack_and_leaves_rest_pending(sessions, now):
    await seed(sessions, now)
    stop = asyncio.Event()
    class StopPublisher(RecordingPublisher):
        async def publish(self, record):
            await super().publish(record)
            stop.set()
    assert await relay(sessions, StopPublisher()).run_once(stop=stop) == 1
    assert [r.published_at is not None for r in await rows(sessions)] == [True, False, False]


async def test_lock_contention_sends_nothing(sessions, now):
    from app.outbox.locking import RelayBusy
    await seed(sessions, now)
    @asynccontextmanager
    async def busy():
        raise RelayBusy()
        yield
    publisher = RecordingPublisher()
    with pytest.raises(RelayBusy):
        await OutboxRelay(sessions, publisher, lock=busy).run_once()
    assert not publisher.records


@pytest.mark.parametrize("limit", [0, 1001])
async def test_invalid_batch_size_does_not_publish(sessions, limit):
    with pytest.raises(ValueError):
        await relay(sessions, RecordingPublisher()).run_once(limit=limit)


async def test_kafka_adapter_waits_for_ack_and_uses_saved_key_value(sessions, now):
    await seed(sessions, now, versions=(1,))
    producer = AsyncMock()
    assert await relay(sessions, KafkaPublisher(producer)).run_once() == 1
    args, kwargs = producer.send_and_wait.await_args
    assert args == ("workflow.events",)
    assert kwargs["key"] == b"order-a"
    assert json.loads(kwargs["value"])["event_id"] == (await rows(sessions))[0].event_id
    assert kwargs["headers"] == [("event_id", (await rows(sessions))[0].event_id.encode())]


async def test_producer_config_and_cleanup_after_publish_failure():
    producer = AsyncMock()
    seen = {}
    def factory(**kwargs):
        seen.update(kwargs)
        return producer
    with pytest.raises(RuntimeError):
        async with kafka_publisher(Settings(), producer_factory=factory):
            raise RuntimeError("simulated publish error")
    assert seen["enable_idempotence"] is True and seen["acks"] == "all"
    producer.start.assert_awaited_once()
    producer.stop.assert_awaited_once()


async def test_shutdown_is_bounded_when_idempotent_producer_cannot_flush():
    producer = AsyncMock()
    async def hang():
        await asyncio.Event().wait()
    producer.stop.side_effect = hang
    async with asyncio.timeout(1):
        async with kafka_publisher(
            Settings(kafka_operation_timeout_seconds=.02),
            producer_factory=lambda **_: producer,
        ):
            pass
    producer.stop.assert_awaited_once()
