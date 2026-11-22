"""Kafka -> projection, with "DB commit first, then offset commit".

For each record, in partition order:
  1. Projector.handle() applies it and COMMITS the DB transaction (event_id recorded).
  2. Only then is the Kafka offset (record.offset + 1) committed.

A crash between 1 and 2 means Kafka redelivers the record after restart; the
processed_events row makes that redelivery a no-op. A crash before 1 loses nothing,
because neither the DB nor the offset moved. Auto-commit is disabled.

Run standalone:  python -m app.consumers.event_consumer
(Normally it runs inside the API process, started by app.main's lifespan.)
"""
import asyncio
import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from app.events import InvalidEvent
from app.projection import Outcome, Projector

logger = logging.getLogger("tracker.consumer")


@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | None


class EventSource(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def poll(self) -> list[SourceRecord]:
        """Next records, each partition's records in offset order."""
    async def commit(self, record: SourceRecord) -> None:
        """Durably commit offset record.offset + 1 for record's partition."""


class KafkaEventSource:
    """aiokafka adapter: manual commits, start from earliest when the group is new."""

    def __init__(self, settings):
        self.settings = settings
        self._consumer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaConsumer
        self._consumer = AIOKafkaConsumer(
            self.settings.order_topic, self.settings.workflow_topic,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id=self.settings.consumer_group, client_id="tracker",
            enable_auto_commit=False, auto_offset_reset="earliest")
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            consumer, self._consumer = self._consumer, None
            await consumer.stop()

    async def poll(self) -> list[SourceRecord]:
        batches = await self._consumer.getmany(
            timeout_ms=self.settings.consumer_poll_timeout_ms,
            max_records=self.settings.consumer_max_records)
        return [SourceRecord(r.topic, r.partition, r.offset, r.value)
                for records in batches.values() for r in records]

    async def commit(self, record: SourceRecord) -> None:
        from aiokafka import TopicPartition
        await self._consumer.commit({TopicPartition(record.topic, record.partition): record.offset + 1})


@dataclass
class ConsumerStatus:
    state: str = "stopped"          # starting | running | restarting | stopped
    restarts: int = 0
    last_error_type: str | None = None
    outcomes: dict[str, int] = field(default_factory=dict)
    last_commit_at: datetime | None = None

    def snapshot(self) -> dict:
        return {"state": self.state, "restarts": self.restarts,
                "last_error_type": self.last_error_type, "outcomes": dict(self.outcomes),
                "last_commit_at": self.last_commit_at.isoformat() if self.last_commit_at else None}


class EventConsumer:
    def __init__(self, source: EventSource, projector: Projector, *,
                 dead_letter_invalid: bool = True, status: ConsumerStatus | None = None):
        self.source, self.projector = source, projector
        self.dead_letter_invalid = dead_letter_invalid
        self.status = status or ConsumerStatus()

    async def run_once(self) -> int:
        """Process one polled batch. Returns the number of records handled."""
        records = await self.source.poll()
        for record in records:
            outcome = await self._apply(record)      # 1. DB transaction committed
            await self.source.commit(record)         # 2. then the offset
            self.status.outcomes[outcome.value] = self.status.outcomes.get(outcome.value, 0) + 1
            self.status.last_commit_at = datetime.now(timezone.utc)
        return len(records)

    async def drain(self) -> int:
        """Run until a poll returns nothing (tests/tools)."""
        total = 0
        while count := await self.run_once():
            total += count
        return total

    async def _apply(self, record: SourceRecord) -> Outcome:
        try:
            return await self.projector.handle(record.topic, record.partition, record.offset,
                                               record.value)
        except InvalidEvent as error:
            context = {"topic": record.topic, "partition": record.partition,
                       "offset": record.offset, "reason": str(error)}
            if not self.dead_letter_invalid:
                logger.error("invalid_event_stopping", extra=context)
                raise  # policy "stop": the offset is NOT committed
            # Same DB-then-offset rule: the dead-letter row commits before the offset does.
            await self.projector.dead_letter(record.topic, record.partition, record.offset,
                                             record.value, str(error))
            logger.error("invalid_event_dead_lettered", extra=context)
            return Outcome.DEAD_LETTERED


async def supervise(source_factory: Callable[[], EventSource], projector: Projector, *,
                    status: ConsumerStatus, stop: asyncio.Event, dead_letter_invalid: bool,
                    backoff: float = 1.0, backoff_max: float = 30.0) -> None:
    """Keep a consumer running. On any failure: close it, wait, and start a fresh one,
    which resumes from the last COMMITTED offsets (the replay is safe by dedupe)."""
    delay = backoff
    while not stop.is_set():
        source = source_factory()
        try:
            status.state = "starting"
            await source.start()
            status.state = "running"
            consumer = EventConsumer(source, projector, dead_letter_invalid=dead_letter_invalid,
                                     status=status)
            while not stop.is_set():
                if await consumer.run_once():
                    delay = backoff
        except asyncio.CancelledError:
            raise
        except Exception as error:
            status.state = "restarting"
            status.restarts += 1
            status.last_error_type = type(error).__name__
            logger.error("consumer_failed_restarting", exc_info=True, extra={
                "failure_type": type(error).__name__, "restarts": status.restarts,
                "delay_seconds": delay})
        finally:
            try:
                await source.stop()
            except Exception as error:
                logger.warning("consumer_stop_failed", extra={"failure_type": type(error).__name__})
        if stop.is_set():
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
        delay = min(delay * 2, backoff_max)
    status.state = "stopped"


async def _main() -> None:
    from app.config import Settings
    from app.db import make_engine, make_sessions
    from app.logging_config import configure_logging

    settings = Settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url.get_secret_value())
    projector = Projector(make_sessions(engine), order_topic=settings.order_topic,
                          workflow_topic=settings.workflow_topic)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass
    try:
        await supervise(lambda: KafkaEventSource(settings), projector, status=ConsumerStatus(),
                        stop=stop, dead_letter_invalid=settings.dead_letter_invalid,
                        backoff=settings.consumer_restart_backoff_seconds,
                        backoff_max=settings.consumer_restart_backoff_max_seconds)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(_main())
