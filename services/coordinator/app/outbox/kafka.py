"""Small adapter; never serialize credentials or regenerate an event identity."""
import asyncio
from contextlib import asynccontextmanager
import logging

from aiokafka import AIOKafkaProducer

logger = logging.getLogger("coordinator.kafka")


class KafkaPublisher:
    def __init__(self, producer):
        self.producer = producer

    async def publish(self, record):
        await self.producer.send_and_wait(
            record.topic, key=record.key, value=record.value,
            headers=[("event_id", record.event_id.encode("ascii"))],
        )


@asynccontextmanager
async def kafka_publisher(settings, *, producer_factory=AIOKafkaProducer):
    timeout = settings.kafka_operation_timeout_seconds
    producer = producer_factory(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="partstream-coordinator-outbox",
        enable_idempotence=True, acks="all",
        request_timeout_ms=max(1000, int(timeout * 1000)),
    )
    try:
        await asyncio.wait_for(producer.start(), timeout)
        yield KafkaPublisher(producer)
    finally:
        # Idempotent delivery can otherwise wait indefinitely during an outage.
        # This CLI exits on a publish error; asyncio.run cancels remaining tasks.
        try:
            await asyncio.wait_for(producer.stop(), timeout)
        except TimeoutError:
            logger.warning("kafka_shutdown_timed_out")
