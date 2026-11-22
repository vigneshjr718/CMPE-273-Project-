"""At-least-once relay: broker acknowledgement precedes the published marker."""
import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import json
import logging
from typing import Protocol

from sqlalchemy import exists, select
from sqlalchemy.orm import aliased

from app.models import OutboxEvent
from app.schemas import utc_now

logger = logging.getLogger("coordinator.outbox")


@dataclass(frozen=True)
class OutgoingRecord:
    event_id: str
    order_id: str
    state_version: int
    topic: str
    key: bytes
    value: bytes


class EventPublisher(Protocol):
    async def publish(self, record: OutgoingRecord) -> None:
        """Return only after a successful broker acknowledgement."""


class OutboxDataError(ValueError):
    """Saved event is inconsistent. Keep it pending for explicit investigation."""


def encode_record(row: OutboxEvent) -> OutgoingRecord:
    envelope = row.payload
    expected = {"event_id": row.event_id, "order_id": row.order_id,
                "state_version": row.state_version, "event_type": row.event_type,
                "producer": "coordinator", "schema_version": 1}
    if not isinstance(envelope, dict) or any(envelope.get(k) != v
                                             for k, v in expected.items()):
        raise OutboxDataError("saved event envelope disagrees with its outbox row")
    if not row.topic or not row.topic.strip():
        raise OutboxDataError("saved event has no destination topic")
    return OutgoingRecord(
        row.event_id, row.order_id, row.state_version, row.topic,
        row.order_id.encode("utf-8"),
        json.dumps(envelope, sort_keys=True, separators=(",", ":"),
                   ensure_ascii=False, allow_nan=False).encode("utf-8"),
    )


class OutboxRelay:
    def __init__(self, sessions, publisher: EventPublisher, *, lock: Callable,
                 timeout_seconds: float = 10, clock=utc_now):
        if timeout_seconds <= 0:
            raise ValueError("publish timeout must be positive")
        self.sessions, self.publisher = sessions, publisher
        self.lock, self.timeout, self.clock = lock, timeout_seconds, clock

    async def run_once(self, *, limit: int = 50, order_id: str | None = None,
                       stop: asyncio.Event | None = None) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("batch size must be between 1 and 1000")
        count = 0
        async with self.lock():
            for _ in range(limit):
                if stop is not None and stop.is_set():
                    break
                record = await self._next(order_id)
                if record is None:
                    break
                # No SQL transaction is open during network IO. Timeout/cancellation
                # is uncertain publication, so leave the saved event pending.
                await asyncio.wait_for(self.publisher.publish(record), self.timeout)
                await self._mark_published(record.event_id)
                count += 1
                logger.info("outbox_event_published", extra={
                    "event_id": record.event_id, "order_id": record.order_id,
                    "state_version": record.state_version,
                })
        return count

    async def _next(self, order_id):
        older = aliased(OutboxEvent)
        statement = select(OutboxEvent).where(
            OutboxEvent.published_at.is_(None),
            ~exists(select(older.event_id).where(
                older.order_id == OutboxEvent.order_id,
                older.published_at.is_(None),
                older.state_version < OutboxEvent.state_version,
            )),
        )
        if order_id is not None:
            statement = statement.where(OutboxEvent.order_id == order_id)
        statement = statement.order_by(OutboxEvent.created_at,
                                       OutboxEvent.order_id,
                                       OutboxEvent.state_version).limit(1)
        async with self.sessions() as session:
            row = (await session.scalars(statement)).one_or_none()
            return None if row is None else encode_record(row)

    async def _mark_published(self, event_id):
        async with self.sessions.begin() as session:
            row = await session.get(OutboxEvent, event_id, with_for_update=True)
            if row is None:
                raise RuntimeError("acknowledged outbox event disappeared")
            if row.published_at is None:
                row.published_at = self.clock()
