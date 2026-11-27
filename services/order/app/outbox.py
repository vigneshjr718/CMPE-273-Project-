"""Best-effort Kafka relay; the database row is the durable source of truth."""
import asyncio
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from app.models import OutboxEvent

async def publish_pending(session_factory, bootstrap_servers: str, enabled: bool) -> int:
    if not enabled:
        return 0
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    count = 0
    try:
        async with session_factory.begin() as session:
            rows = list((await session.scalars(select(OutboxEvent).where(OutboxEvent.published_at.is_(None)).order_by(OutboxEvent.created_at).limit(100))).all())
            for row in rows:
                await producer.send_and_wait(row.topic, json_bytes(row.payload), key=row.order_id.encode())
                row.published_at = datetime.now(timezone.utc)
                count += 1
    finally:
        await producer.stop()
    return count

def json_bytes(payload: dict) -> bytes:
    import json
    return json.dumps(payload, separators=(",", ":"), default=str).encode()

