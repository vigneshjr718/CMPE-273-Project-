"""Real PostgreSQL/Kafka replay demo; sellers remain an explicit fixture."""
import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlalchemy import delete, func, select

from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.models import DeliveryCheck, LineAttempt, OutboxEvent, ProcessedEvent, Workflow, WorkflowJob, WorkflowLine
from app.outbox.kafka import kafka_publisher
from app.outbox.locking import postgres_relay_lock
from app.outbox.publisher import OutboxRelay, encode_record
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.schemas import OrderCreatedEvent
from app.tools.demo_sellers import DemoSellers


class SimulatedMarkerFailure(RuntimeError):
    """Broker acknowledged, but the local published-marker transaction was lost."""


class FailBeforeMarker(OutboxRelay):
    async def _mark_published(self, event_id):
        raise SimulatedMarkerFailure()


async def run_demo(settings, *, cleanup=False):
    """All mutations scoped to this freshly generated order/topic, even in tests."""
    token = uuid4().hex
    order_id, topic = "demo-outbox-" + token, "coordinator-demo-" + token
    engine = make_engine(settings.database_url.get_secret_value())
    if engine.dialect.name != "postgresql":
        await engine.dispose()
        raise ValueError("demo requires the migrated real PostgreSQL database")
    sessions = make_sessions(engine)
    timeout = settings.kafka_operation_timeout_seconds
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers,
                                request_timeout_ms=max(1000, int(timeout * 1000)))
    topic_created = False
    try:
        await check_database(engine)
        await asyncio.wait_for(admin.start(), timeout)
        await asyncio.wait_for(admin.create_topics([
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
        ]), timeout)
        topic_created = True
        now = datetime.now(timezone.utc)
        event = OrderCreatedEvent.model_validate({
            "event_id": str(uuid4()), "event_type": "order.created",
            "schema_version": 1, "producer": "order", "occurred_at": now,
            "payload": {"order_id": order_id, "lines": [{
                "line_id": "line-1", "part_id": "bumper", "quantity": 1,
                "needed_by": now + timedelta(days=2),
            }]},
        })
        await accept_order(event, sessions, workflow_topic=topic)
        workflow = await Coordinator(
            sessions, DemoSellers(), ["seller-1"],
            lock=lambda oid: postgres_order_lock(engine, oid), workflow_topic=topic,
        ).run_order(order_id)
        async with sessions() as session:
            expected = [encode_record(row) for row in await session.scalars(
                select(OutboxEvent).where(OutboxEvent.order_id == order_id)
                .order_by(OutboxEvent.state_version))]
        assert len(expected) == 5, "unexpected normal workflow event count"
        assert workflow["status"] == "RESERVED"

        # Controlled failure at the actual durable-DB/broker boundary.
        async with kafka_publisher(settings) as publisher:
            failed = FailBeforeMarker(
                sessions, publisher, lock=lambda: postgres_relay_lock(engine),
                timeout_seconds=timeout,
            )
            try:
                await failed.run_once(order_id=order_id)
            except SimulatedMarkerFailure:
                pass
            else:
                raise AssertionError("failure injection did not run")
        async with sessions() as session:
            pending_after_failure = await session.scalar(
                select(func.count()).select_from(OutboxEvent).where(
                    OutboxEvent.order_id == order_id, OutboxEvent.published_at.is_(None)))
        assert pending_after_failure == 5

        # Fresh producer/relay, saved IDs/payloads; no hidden in-memory sent cache.
        async with kafka_publisher(settings) as publisher:
            restarted = OutboxRelay(
                sessions, publisher, lock=lambda: postgres_relay_lock(engine),
                timeout_seconds=timeout,
            )
            replayed = await restarted.run_once(order_id=order_id)
            empty_batch = await restarted.run_once(order_id=order_id)
        assert replayed == 5 and empty_batch == 0

        consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers, group_id=None,
            enable_auto_commit=False, auto_offset_reset="earliest",
            request_timeout_ms=max(1000, int(timeout * 1000)),
        )
        try:
            await asyncio.wait_for(consumer.start(), timeout)
            partition = TopicPartition(topic, 0)
            consumer.assign([partition])
            consumer.seek(partition, 0)
            records = []
            async with asyncio.timeout(timeout * 2):
                for _ in range(6):
                    records.append(await consumer.getone())
            wanted = [expected[0], *expected]
            assert [(r.key, r.value) for r in records] == [(r.key, r.value) for r in wanted]
        finally:
            await asyncio.wait_for(consumer.stop(), timeout)
        async with sessions() as session:
            published = await session.scalar(select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.order_id == order_id, OutboxEvent.published_at.is_not(None)))
        assert published == 5
        return {
            "infrastructure": "real_postgresql_and_kafka",
            "seller_mode": "controlled_demo", "order_id": order_id, "topic": topic,
            "workflow_status": workflow["status"],
            "pending_after_simulated_marker_failure": pending_after_failure,
            "published_outbox_rows": published, "broker_records": len(records),
            "distinct_event_ids": len({r.event_id for r in wanted}),
            "received_state_versions": [json.loads(r.value)["state_version"] for r in records],
            "first_event_replayed_with_same_id_and_payload": (
                records[0].value == records[1].value),
            "subsequent_batch_published": empty_batch,
            "cleanup": cleanup,
        }
    finally:
        try:
            if cleanup:
                async with engine.begin() as connection:
                    for model in (WorkflowJob, DeliveryCheck, OutboxEvent, ProcessedEvent, LineAttempt, WorkflowLine, Workflow):
                        await connection.execute(delete(model).where(model.order_id == order_id))
                if topic_created:
                    await asyncio.wait_for(admin.delete_topics([topic]), timeout)
        finally:
            try:
                await asyncio.wait_for(admin.close(), timeout)
            finally:
                await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup", action="store_true",
                        help="remove only this demo's generated order and topic afterward")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run_demo(Settings(), cleanup=args.cleanup)), indent=2))


if __name__ == "__main__":
    main()
