import asyncio
from dataclasses import asdict
import json
import logging
from types import SimpleNamespace
from uuid import uuid4

from aiokafka import TopicPartition
from aiokafka.errors import CommitFailedError
import pytest
from sqlalchemy import event as sql_event, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.consumers.kafka_orders import (
    AssignmentChanged, InvalidOrderRecord, KafkaOrderIntake, OrderAssignment,
    decode_order, kafka_order_source,
)
from app.consumers.order_created import IngestionConflict, accept_order
from app.logging_config import JsonFormatter
from app.models import OutboxEvent, Workflow
from test_order_ingestion import sessions, order_event, counts

TOPIC = "order.created"
P0, P1 = TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)


def record(event, offset=0, partition=P0, **changes):
    return SimpleNamespace(topic=partition.topic, partition=partition.partition,
        offset=offset, key=event.payload.order_id.encode(), value=event.model_dump_json().encode(), **changes)


class Broker:
    def __init__(self, batches=()):
        self.batches, self.commits, self.before_commit = list(batches), [], None
        self.commit_mode, self.fetch_mode, self.fetch_args = "ok", "ok", []

    async def getmany(self, **kwargs):
        self.fetch_args.append(kwargs)
        if self.fetch_mode == "timeout":
            await asyncio.Event().wait()
        return self.batches.pop(0) if self.batches else {}

    async def commit(self, offsets):
        if self.before_commit:
            await self.before_commit(offsets)
        if self.commit_mode == "error":
            raise CommitFailedError()
        if self.commit_mode == "timeout":
            await asyncio.Event().wait()
        self.commits.append(offsets)
        if self.commit_mode == "lost_ack":
            raise TimeoutError("commit reply lost")


async def worker(sessions, broker, **kwargs):
    assignment = OrderAssignment(timeout_seconds=.1)
    await assignment.on_partitions_assigned([P0, P1])
    return KafkaOrderIntake(broker, assignment, sessions, incoming_topic=TOPIC,
        workflow_topic="workflow.events", timeout_seconds=.02, poll_ms=1, **kwargs)


async def test_ack_is_explicit_next_offset_after_real_database_commit(sessions, order_event):
    broker = Broker([{P0: [record(order_event, 7)]}])
    async def committed(offsets):
        assert await counts(sessions) == (1, 1, 1)
        async with sessions() as session:
            row = await session.get(Workflow, order_event.payload.order_id)
            assert row.status == "PENDING" and row.state_version == 1
        assert offsets == {P0: 8}
    broker.before_commit = committed
    result = await (await worker(sessions, broker)).run_once()
    assert asdict(result) == dict(processed=1, created=1, duplicate_event=0, duplicate_order=0, stopped=False)
    assert broker.commits == [{P0: 8}]
    assert broker.fetch_args == [{"timeout_ms": 1, "max_records": 10}]


async def test_duplicate_records_and_duplicate_order_keep_one_workflow_event(sessions, order_event):
    another = order_event.model_copy(update={"event_id": uuid4()})
    broker = Broker([{P0: [record(order_event, 0), record(order_event, 1), record(another, 2)]}])
    result = await (await worker(sessions, broker)).run_once()
    assert (result.created, result.duplicate_event, result.duplicate_order) == (1, 1, 1)
    assert await counts(sessions) == (1, 2, 1)
    assert broker.commits == [{P0: 1}, {P0: 2}, {P0: 3}]


@pytest.mark.parametrize("mode", ["error", "timeout", "lost_ack"])
async def test_failed_or_ambiguous_offset_commit_replays_durable_acceptance(sessions, order_event, mode):
    broker = Broker([{P0: [record(order_event)]}])
    broker.commit_mode = mode
    intake = await worker(sessions, broker)
    with pytest.raises((CommitFailedError, TimeoutError)):
        await intake.run_once()
    assert await counts(sessions) == (1, 1, 1)
    with pytest.raises(RuntimeError, match="restart"):
        await intake.run_once()
    restarted = Broker([{P0: [record(order_event)]}])
    result = await (await worker(sessions, restarted)).run_once()
    assert result.duplicate_event == 1 and await counts(sessions) == (1, 1, 1)
    assert restarted.commits == [{P0: 1}]


async def test_database_failure_rolls_back_and_no_offset_is_acknowledged(sessions, order_event):
    broker = Broker([{P0: [record(order_event)]}])
    def fail(session, *_):
        if any(isinstance(row, OutboxEvent) for row in session.new):
            raise RuntimeError("injected database commit failure")
    sql_event.listen(Session, "before_flush", fail)
    try:
        with pytest.raises(RuntimeError, match="database commit"):
            await (await worker(sessions, broker)).run_once()
    finally:
        sql_event.remove(Session, "before_flush", fail)
    assert not broker.commits and await counts(sessions) == (0, 0, 0)


async def test_conflicting_input_blocks_its_offset_and_following_records(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    data = order_event.model_dump(mode="json")
    data["payload"]["lines"][0]["quantity"] += 1
    changed = type(order_event).model_validate(data)
    later = order_event.model_copy(update={"event_id": uuid4()})
    broker = Broker([{P0: [record(changed, 0), record(later, 1)], P1: [record(later, 0, P1)]}])
    with pytest.raises(IngestionConflict):
        await (await worker(sessions, broker)).run_once()
    assert not broker.commits and await counts(sessions) == (1, 1, 1)


async def test_poison_record_does_not_commit_prefetched_later_offsets(sessions, order_event):
    bad = record(order_event, 1)
    bad.value = b"not-json"
    broker = Broker([{P0: [record(order_event), bad, record(order_event, 2)],
                      P1: [record(order_event, 9, P1)]}])
    with pytest.raises(InvalidOrderRecord):
        await (await worker(sessions, broker)).run_once()
    assert broker.commits == [{P0: 1}]
    assert await counts(sessions) == (1, 1, 1)


async def test_partitions_use_independent_offsets_and_allow_kafka_offset_gaps(sessions, order_event):
    broker = Broker([{P0: [record(order_event, 2), record(order_event, 5)],
                      P1: [record(order_event, 0, P1)]}])
    result = await (await worker(sessions, broker)).run_once()
    assert result.processed == 3
    assert broker.commits == [{P0: 3}, {P0: 6}, {P1: 1}]


async def test_reordered_records_cannot_rewind_a_partition_commit(sessions, order_event):
    broker = Broker([{P0: [record(order_event, 2), record(order_event, 1)]}])
    with pytest.raises(InvalidOrderRecord):
        await (await worker(sessions, broker)).run_once()
    assert broker.commits == [{P0: 3}]


async def test_database_commit_after_revoke_remains_replayable_without_ack(sessions, order_event, monkeypatch):
    import app.consumers.kafka_orders as module
    broker = Broker([{P0: [record(order_event)]}])
    intake = await worker(sessions, broker)
    original = module.accept_order
    async def revoke_after_commit(*args, **kwargs):
        result = await original(*args, **kwargs)
        await intake.assignment.on_partitions_revoked([P0, P1])
        await intake.assignment.on_partitions_assigned([P0, P1])
        return result
    monkeypatch.setattr(module, "accept_order", revoke_after_commit)
    with pytest.raises(AssignmentChanged):
        await intake.run_once()
    assert await counts(sessions) == (1, 1, 1) and not broker.commits


async def test_revoke_during_commit_cannot_process_prefetched_successors(sessions, order_event):
    broker = Broker([{P0: [record(order_event, 0), record(order_event, 1)]}])
    intake = await worker(sessions, broker)
    async def revoked(_):
        await intake.assignment.on_partitions_revoked([P0])
    broker.before_commit = revoked
    with pytest.raises(AssignmentChanged):
        await intake.run_once()
    assert broker.commits == [{P0: 1}]


async def test_revoke_during_fetch_discards_entire_batch(sessions, order_event):
    broker = Broker()
    intake = await worker(sessions, broker)
    async def fetch(**_):
        await intake.assignment.on_partitions_revoked([P0, P1])
        await intake.assignment.on_partitions_assigned([P0, P1])
        return {P0: [record(order_event)]}
    broker.getmany = fetch
    with pytest.raises(AssignmentChanged):
        await intake.run_once()
    assert not broker.commits and await counts(sessions) == (0, 0, 0)


async def test_unowned_partition_is_rejected_before_database_write(sessions, order_event):
    broker = Broker([{TopicPartition(TOPIC, 2): [record(order_event, partition=TopicPartition(TOPIC, 2))]}])
    with pytest.raises(AssignmentChanged):
        await (await worker(sessions, broker)).run_once()
    assert await counts(sessions) == (0, 0, 0)


async def test_graceful_stop_commits_current_record_only(sessions, order_event):
    stop = asyncio.Event()
    broker = Broker([{P0: [record(order_event, 0), record(order_event, 1)]}])
    async def stopping(_):
        stop.set()
    broker.before_commit = stopping
    intake = await worker(sessions, broker)
    result = await intake.run_once(stop=stop)
    assert result.processed == 1 and result.stopped
    assert broker.commits == [{P0: 1}]
    with pytest.raises(RuntimeError):
        await intake.run_once()


async def test_cancellation_after_acceptance_leaves_offset_unacknowledged(sessions, order_event):
    broker = Broker([{P0: [record(order_event)]}])
    async def cancelled(_):
        raise asyncio.CancelledError()
    broker.before_commit = cancelled
    with pytest.raises(asyncio.CancelledError):
        await (await worker(sessions, broker)).run_once()
    assert await counts(sessions) == (1, 1, 1) and not broker.commits


@pytest.mark.parametrize("change", [
    {"value": None}, {"value": b""}, {"value": b"\xff"}, {"value": b"{}"},
    {"value": b'{"payload":null,"payload":null}'}, {"value": b'{"payload":NaN}'},
    {"key": None}, {"key": b"another-order"}, {"topic": "other"},
    {"partition": 3}, {"offset": -1}, {"offset": True},
])
def test_invalid_record_boundaries(order_event, change):
    row = record(order_event)
    row.__dict__.update(change)
    with pytest.raises(InvalidOrderRecord):
        decode_order(row, P0, TOPIC, 1048576)


def test_size_limit_is_applied_before_json_decoding(order_event):
    with pytest.raises(InvalidOrderRecord):
        decode_order(record(order_event), P0, TOPIC, 10)


class SourceBroker:
    def __init__(self, **kwargs):
        self.kwargs, self.stopped, self.seeks = kwargs, False, []
        self.offsets = {P0: None, P1: 9}
        self.fail_start = False
    def subscribe(self, topics, listener):
        self.topics, self.listener = topics, listener
    async def start(self):
        if self.fail_start:
            raise RuntimeError("unavailable")
        await self.listener.on_partitions_assigned([P0, P1])
    async def stop(self):
        self.stopped = True
    async def committed(self, partition):
        return self.offsets[partition]
    async def seek_to_beginning(self, *partitions):
        self.seeks.extend(partitions)
        return True


@pytest.mark.parametrize("bootstrap", [False, True])
async def test_source_disables_auto_commit_and_never_resets_existing_offsets(bootstrap):
    made = []
    def factory(**kwargs):
        made.append(SourceBroker(**kwargs))
        return made[-1]
    settings = Settings(_env_file=None)
    async with kafka_order_source(settings, bootstrap_earliest=bootstrap, consumer_factory=factory) as (consumer, ownership):
        assert consumer.kwargs["enable_auto_commit"] is False
        assert consumer.kwargs["auto_offset_reset"] == "none"
        assert consumer.kwargs["isolation_level"] == "read_committed"
        assert consumer.kwargs["group_id"] == settings.consumer_group
        assert consumer.topics == [TOPIC]
        assert consumer.seeks == ([P0] if bootstrap else [])
        ownership.require(ownership.epoch, P0)
    assert made[0].stopped


async def test_startup_failure_closes_consumer():
    made = []
    def factory(**kwargs):
        broker = SourceBroker(**kwargs)
        broker.fail_start = True
        made.append(broker)
        return broker
    with pytest.raises(RuntimeError):
        async with kafka_order_source(Settings(_env_file=None), consumer_factory=factory):
            pytest.fail("startup should fail")
    assert made[0].stopped


async def test_failed_initial_offset_lookup_cannot_start_intake():
    def factory(**kwargs):
        broker = SourceBroker(**kwargs)
        async def fail(_):
            raise TimeoutError()
        broker.committed = fail
        return broker
    with pytest.raises(AssignmentChanged):
        async with kafka_order_source(Settings(_env_file=None), bootstrap_earliest=True, consumer_factory=factory):
            pytest.fail("callback failure must not be hidden")


def test_safe_logs_include_coordinates_without_raw_payload_or_error_text():
    row = logging.LogRecord("coordinator.order_consumer", logging.ERROR, "", 1,
                            "order_consumer_stopped", (), None)
    row.topic, row.partition, row.offset = TOPIC, 2, 10
    row.failure_type, row.payload, row.password = "InvalidOrderRecord", "secret event", "secret credential"
    result = json.loads(JsonFormatter().format(row))
    assert (result["topic"], result["partition"], result["offset"]) == (TOPIC, 2, 10)
    assert "secret" not in json.dumps(result)


async def test_database_timeout_does_not_commit_offset(sessions, order_event, monkeypatch):
    import app.consumers.kafka_orders as module
    async def hanging(*args, **kwargs):
        await asyncio.Event().wait()
    monkeypatch.setattr(module, "accept_order", hanging)
    broker = Broker([{P0: [record(order_event)]}])
    with pytest.raises(TimeoutError):
        await (await worker(sessions, broker, database_timeout_seconds=.01)).run_once()
    assert not broker.commits and await counts(sessions) == (0, 0, 0)


async def test_fetch_timeout_stops_without_acknowledging(sessions):
    broker = Broker()
    broker.fetch_mode = "timeout"
    intake = await worker(sessions, broker)
    with pytest.raises(TimeoutError):
        await intake.run_once()
    assert intake.closed and not broker.commits


async def test_preexisting_advanced_workflow_is_not_reset_by_kafka_replay(sessions, order_event):
    await accept_order(order_event, sessions, workflow_topic="workflow.events")
    async with sessions.begin() as session:
        row = await session.get(Workflow, order_event.payload.order_id)
        row.status, row.state_version = "RESERVED", 5
    broker = Broker([{P0: [record(order_event)]}])
    result = await (await worker(sessions, broker)).run_once()
    assert result.duplicate_event == 1 and broker.commits == [{P0: 1}]
    async with sessions() as session:
        row = await session.get(Workflow, order_event.payload.order_id)
        assert (row.status, row.state_version) == ("RESERVED", 5)


async def test_stop_before_poll_and_empty_batch_do_not_commit_offsets(sessions):
    broker = Broker()
    intake = await worker(sessions, broker)
    assert (await intake.run_once()).processed == 0
    stop = asyncio.Event()
    stop.set()
    assert (await intake.run_once(stop=stop)).stopped
    assert len(broker.fetch_args) == 1 and not broker.commits


async def test_consumer_stop_occurs_on_context_failure():
    made = []
    def factory(**kwargs):
        made.append(SourceBroker(**kwargs))
        return made[-1]
    with pytest.raises(InvalidOrderRecord):
        async with kafka_order_source(Settings(_env_file=None), consumer_factory=factory):
            raise InvalidOrderRecord()
    assert made[0].stopped


async def test_demo_sends_durable_database_before_real_offset_attempt(sessions, order_event):
    from app.tools.demo_order_consumer import FailBeforeOffsetCommit, SimulatedOffsetFailure
    broker = Broker([{P0: [record(order_event)]}])
    intake = await worker(sessions, FailBeforeOffsetCommit(broker))
    with pytest.raises(SimulatedOffsetFailure):
        await intake.run_once()
    assert await counts(sessions) == (1, 1, 1) and not broker.commits
