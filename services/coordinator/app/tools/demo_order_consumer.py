"""Real PostgreSQL/Kafka intake: committed DB, lost offset, replay, poison-record stop."""
import argparse
import asyncio
from dataclasses import asdict
from datetime import timedelta
import json
from uuid import uuid4

from aiokafka import TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import delete, func, select

from app.config import Settings
from app.consumers.kafka_orders import InvalidOrderRecord, kafka_order_source
from app.db import check_database, make_engine, make_sessions
from app.models import (
    CancellationCheck, DeliveryCheck, LineAttempt, OutboxEvent, ProcessedEvent, Workflow, WorkflowJob, WorkflowLine,
)
from app.outbox.kafka import kafka_publisher
from app.schemas import OrderCreatedEvent, utc_now
from app.tools.consume_orders import make_intake


class SimulatedOffsetFailure(RuntimeError):
    pass


class FailBeforeOffsetCommit:
    """Inject after real DB acceptance, before the real consumer sends a commit."""
    def __init__(self, consumer):
        self.consumer = consumer
    async def getmany(self, **kwargs):
        return await self.consumer.getmany(**kwargs)
    async def commit(self, offsets):
        raise SimulatedOffsetFailure()


async def run_demo(settings, *, cleanup=False):
    token = uuid4().hex
    topic, group = "coordinator-intake-demo-" + token, "coordinator-intake-group-" + token
    ids = ["demo-intake-" + token + suffix for suffix in ("-a", "-b", "-blocked")]
    config = settings.model_copy(update={"incoming_topic": topic, "consumer_group": group,
        "incoming_poll_ms": 100, "incoming_batch_size": 10})
    engine = make_engine(settings.database_url.get_secret_value())
    if engine.dialect.name != "postgresql":
        await engine.dispose()
        raise ValueError("demo requires migrated PostgreSQL")
    sessions, timeout = make_sessions(engine), settings.kafka_operation_timeout_seconds
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers,
                                request_timeout_ms=max(1000, int(timeout * 1000)))
    created = False
    p0, p1 = TopicPartition(topic, 0), TopicPartition(topic, 1)
    def event(order_id):
        now = utc_now()
        return OrderCreatedEvent.model_validate({
            "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
            "producer": "order", "occurred_at": now,
            "payload": {"order_id": order_id, "lines": [{"line_id": "line-1", "part_id": "bumper",
                "quantity": 1, "needed_by": now + timedelta(days=2)}]},
        })
    original, other, blocked = [event(order_id) for order_id in ids]
    duplicate_order = original.model_copy(update={"event_id": uuid4()})
    async def db_counts():
        async with sessions() as session:
            return [await session.scalar(select(func.count()).select_from(model).where(
                model.order_id.in_(ids))) for model in (Workflow, ProcessedEvent, OutboxEvent)]
    try:
        await check_database(engine)
        await asyncio.wait_for(admin.start(), timeout)
        await asyncio.wait_for(admin.create_topics([
            NewTopic(name=topic, num_partitions=2, replication_factor=1)]), timeout)
        created = True
        async with kafka_publisher(settings) as publisher:
            async def send(value, partition, key):
                await asyncio.wait_for(publisher.producer.send_and_wait(
                    topic, partition=partition, key=key, value=value), timeout)
            await send(original.model_dump_json().encode(), 0, ids[0].encode())
            # Real group, DB and source record. Only offset-commit failure is injected.
            async with kafka_order_source(config, bootstrap_earliest=True) as (consumer, assignment):
                failed = make_intake(config, FailBeforeOffsetCommit(consumer), assignment, sessions)
                try:
                    async with asyncio.timeout(timeout * 3):
                        while True:
                            await failed.run_once()
                except SimulatedOffsetFailure:
                    pass
                else:
                    raise AssertionError("failure injection did not run")
                committed_after_failure = await asyncio.wait_for(consumer.committed(p0), timeout)
                assert committed_after_failure is None
                assert await db_counts() == [1, 1, 1]

            # One record replays from the original broker offset; the others are
            # explicit duplicate deliveries/new IDs and an independent partition.
            await send(original.model_dump_json().encode(), 0, ids[0].encode())
            await send(duplicate_order.model_dump_json().encode(), 0, ids[0].encode())
            await send(other.model_dump_json().encode(), 1, ids[1].encode())
            totals = dict(processed=0, created=0, duplicate_event=0, duplicate_order=0)
            async with kafka_order_source(config, bootstrap_earliest=True) as (consumer, assignment):
                intake = make_intake(config, consumer, assignment, sessions)
                async with asyncio.timeout(timeout * 3):
                    while totals["processed"] < 4:
                        batch = asdict(await intake.run_once())
                        for name in totals:
                            totals[name] += batch[name]
                offsets = [await asyncio.wait_for(consumer.committed(tp), timeout) for tp in (p0, p1)]
                assert totals == dict(processed=4, created=1, duplicate_event=2, duplicate_order=1)
                assert offsets == [3, 1] and await db_counts() == [2, 3, 2]

            # Fresh group member resumes broker commits; no event is resent here.
            async with kafka_order_source(config) as (consumer, assignment):
                fresh = make_intake(config, consumer, assignment, sessions)
                repeated = await fresh.run_once()
                assert repeated.processed == 0

            await send(b'{"invalid":', 0, ids[2].encode())
            await send(blocked.model_dump_json().encode(), 0, ids[2].encode())
            async with kafka_order_source(config) as (consumer, assignment):
                intake = make_intake(config, consumer, assignment, sessions)
                try:
                    async with asyncio.timeout(timeout * 3):
                        while True:
                            await intake.run_once()
                except InvalidOrderRecord:
                    pass
                else:
                    raise AssertionError("invalid input did not stop the consumer")
                poison_offset = await asyncio.wait_for(consumer.committed(p0), timeout)
                assert poison_offset == 3
                async with sessions() as session:
                    assert await session.get(Workflow, ids[2]) is None
                    workflows = list(await session.scalars(select(Workflow).where(Workflow.order_id.in_(ids))))
                    assert all(w.status == "PENDING" and w.state_version == 1 for w in workflows)
            return {"infrastructure": "real_postgresql_and_kafka", "seller_mode": "not_used",
                "topic": topic, "consumer_group": group, "order_ids": ids,
                "offset_after_simulated_commit_failure": committed_after_failure,
                "database_rows_after_simulated_failure": [1, 1, 1],
                "recovered_intake": totals, "committed_offsets": {"0": offsets[0], "1": offsets[1]},
                "workflow_count": 2, "inbox_count": 3, "outbox_count": 2,
                "workflow_status": "PENDING", "workflow_version": 1,
                "fresh_consumer_processed": repeated.processed,
                "poison_record_stopped_at_offset": poison_offset,
                "later_valid_order_was_not_accepted": True,
                "cleanup": cleanup}
    finally:
        try:
            if cleanup:
                async with engine.begin() as connection:
                    for model in (WorkflowJob, CancellationCheck, DeliveryCheck, OutboxEvent, ProcessedEvent,
                                  LineAttempt, WorkflowLine, Workflow):
                        await connection.execute(delete(model).where(model.order_id.in_(ids)))
                if created:
                    await asyncio.wait_for(admin.delete_topics([topic]), timeout)
        finally:
            try:
                await asyncio.wait_for(admin.close(), timeout)
            finally:
                await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup", action="store_true", help="remove only this demo's generated DB rows/topic")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run_demo(Settings(), cleanup=args.cleanup)), indent=2))


if __name__ == "__main__":
    main()
