"""Test database setup.

Default: a temporary SQLite file per test (no Docker needed). This checks logic and
transaction boundaries, NOT PostgreSQL row locking.
Optional: TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://... runs the same tests on
PostgreSQL. It DROPS and recreates the tracker tables, so use a disposable database only.
"""
import os
from collections import defaultdict
from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from app.consumers.event_consumer import SourceRecord
from app.db import make_sessions
from app.events import as_utc
from app.models import Base, LineStatus, OrderStatus, ProcessedEvent, StatusHistory
from app.projection import Projector
from tests.mock_events import ORDER_TOPIC, WORKFLOW_TOPIC

FIXED_NOW = datetime(2026, 11, 30, tzinfo=timezone.utc)


async def _engine(url: str):
    engine = create_async_engine(url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    return engine


@pytest.fixture
async def sessions(tmp_path):
    url = os.environ.get("TRACKER_TEST_DATABASE_URL") or f"sqlite+aiosqlite:///{tmp_path / 'tracker.db'}"
    engine = await _engine(url)
    yield make_sessions(engine)
    await engine.dispose()


@pytest.fixture
async def reference_sessions(tmp_path):
    """A second, independent DB used to compute the expected projection."""
    engine = await _engine(f"sqlite+aiosqlite:///{tmp_path / 'reference.db'}")
    yield make_sessions(engine)
    await engine.dispose()


def make_projector(sessions) -> Projector:
    return Projector(sessions, order_topic=ORDER_TOPIC, workflow_topic=WORKFLOW_TOPIC,
                     clock=lambda: FIXED_NOW)


@pytest.fixture
def projector(sessions):
    return make_projector(sessions)


async def apply_all(projector, events):
    outcomes = []
    for offset, (topic, raw) in enumerate(events):
        outcomes.append(await projector.handle(topic, 0, offset, raw))
    return outcomes


def _row(obj, skip=()):
    data = {}
    for column in obj.__table__.columns:
        if column.name in skip:
            continue
        value = getattr(obj, column.name)
        data[column.name] = as_utc(value) if isinstance(value, datetime) else value
    return data


async def snapshot(sessions) -> dict:
    """Comparable dump of the read model (ignores wall-clock and Kafka-position columns)."""
    async with sessions() as session:
        orders = [_row(o, {"projected_at"}) for o in await session.scalars(
            select(OrderStatus).order_by(OrderStatus.order_id))]
        lines = [_row(l) for l in await session.scalars(
            select(LineStatus).order_by(LineStatus.order_id, LineStatus.line_id))]
        history = [_row(h, {"id", "applied_at"}) for h in await session.scalars(
            select(StatusHistory).order_by(StatusHistory.order_id, StatusHistory.state_version,
                                           StatusHistory.event_id))]
        processed = [_row(p, {"kafka_partition", "kafka_offset", "processed_at"})
                     for p in await session.scalars(
                         select(ProcessedEvent).order_by(ProcessedEvent.event_id))]
    return {"orders": orders, "lines": lines, "history": history, "processed": processed}


class FakeBroker:
    """In-memory Kafka: keyed partitioning, per-partition order, committed offsets."""

    def __init__(self, partitions=3):
        self.partitions = partitions
        self.logs: dict[tuple[str, int], list[bytes]] = defaultdict(list)
        self.committed: dict[tuple[str, int], int] = {}

    def produce(self, topic: str, raw: bytes, key: str | None = None):
        import json, zlib
        if key is None:
            try:
                data = json.loads(raw)
                key = data.get("order_id") or data["payload"]["order_id"]
            except Exception:
                key = "invalid"
        partition = zlib.crc32(key.encode()) % self.partitions
        self.logs[(topic, partition)].append(raw)

    def produce_all(self, events):
        for topic, raw in events:
            self.produce(topic, raw)

    def end_offsets(self):
        return {tp: len(log) for tp, log in self.logs.items()}


class SimulatedCrash(RuntimeError):
    pass


class FakeSource:
    """EventSource over FakeBroker. Starts at the COMMITTED offsets, like a real restart."""

    def __init__(self, broker: FakeBroker, *, max_records=3, crash_on_commit: int | None = None):
        self.broker, self.max_records = broker, max_records
        self.crash_on_commit = crash_on_commit  # crash INSTEAD of the Nth offset commit
        self.commits = 0
        self.position: dict = {}

    async def start(self):
        self.position = dict(self.broker.committed)

    async def stop(self):
        pass

    async def poll(self):
        import asyncio
        await asyncio.sleep(0.001)  # like getmany(timeout_ms): yield to the event loop
        out = []
        for (topic, partition), log in sorted(self.broker.logs.items()):
            start = self.position.get((topic, partition), 0)
            for offset in range(start, min(len(log), start + self.max_records - len(out))):
                out.append(SourceRecord(topic, partition, offset, log[offset]))
                self.position[(topic, partition)] = offset + 1
            if len(out) >= self.max_records:
                break
        return out

    async def commit(self, record):
        self.commits += 1
        if self.crash_on_commit is not None and self.commits == self.crash_on_commit:
            raise SimulatedCrash("process died after DB commit, before offset commit")
        self.broker.committed[(record.topic, record.partition)] = record.offset + 1
