"""Serial durable order intake with explicit per-partition Kafka acknowledgements."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import logging

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener

from app.consumers.order_created import accept_order
from app.schemas import OrderCreatedEvent

logger = logging.getLogger("coordinator.order_consumer")


class InvalidOrderRecord(ValueError):
    pass


class AssignmentChanged(RuntimeError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidOrderRecord("duplicate JSON field")
        result[key] = value
    return result


def _invalid_constant(_):
    raise InvalidOrderRecord("nonstandard JSON number")


def decode_order(record, partition, topic, max_bytes):
    if (partition.topic != topic or record.topic != partition.topic
            or record.partition != partition.partition
            or type(record.partition) is not int or record.partition < 0
            or type(record.offset) is not int or record.offset < 0):
        raise InvalidOrderRecord("invalid Kafka record coordinates")
    if not isinstance(record.value, bytes) or not 0 < len(record.value) <= max_bytes:
        raise InvalidOrderRecord("missing or oversized Kafka value")
    try:
        payload = json.loads(record.value.decode("utf-8"), object_pairs_hook=_unique_object,
                             parse_constant=_invalid_constant)
        event = OrderCreatedEvent.model_validate(payload)
    except (ValueError, RecursionError) as error:
        raise InvalidOrderRecord("invalid order envelope") from error
    # PROVISIONAL A/D contract: producer uses exact UTF-8 order_id as Kafka key.
    if record.key != event.payload.order_id.encode("utf-8"):
        raise InvalidOrderRecord("Kafka key does not match order_id")
    return event


class OrderAssignment(ConsumerRebalanceListener):
    """Never acknowledge from callbacks; unfinished work is safe to replay."""
    def __init__(self, *, bootstrap_earliest=False, timeout_seconds=10):
        self.consumer = None
        self.bootstrap_earliest, self.timeout = bootstrap_earliest, timeout_seconds
        self.epoch, self.partitions, self.error = 0, frozenset(), None
        self.ready = asyncio.Event()

    async def on_partitions_revoked(self, revoked):
        self.epoch += 1
        self.partitions = frozenset()
        self.ready.clear()

    async def on_partitions_assigned(self, assigned):
        self.epoch += 1
        self.ready.clear()
        self.partitions, self.error = frozenset(assigned), None
        try:
            if self.bootstrap_earliest and assigned:
                async with asyncio.timeout(self.timeout):
                    offsets = await asyncio.gather(*[self.consumer.committed(tp) for tp in assigned])
                    new = [tp for tp, offset in zip(assigned, offsets) if offset is None]
                    if new and not await self.consumer.seek_to_beginning(*new):
                        raise TimeoutError("initial offset lookup did not complete")
        except Exception as error:
            # aiokafka may log callback failures; retain an explicit fail-closed guard.
            self.error = type(error).__name__
            self.partitions = frozenset()
        finally:
            self.ready.set()

    async def wait_ready(self):
        await asyncio.wait_for(self.ready.wait(), self.timeout)
        if self.error:
            raise AssignmentChanged("partition initialization failed")

    def require(self, epoch, partition=None):
        if (self.error or not self.ready.is_set() or self.epoch != epoch
                or (partition is not None and partition not in self.partitions)):
            raise AssignmentChanged("Kafka partition ownership changed; replay from committed offsets")


@dataclass
class IntakeBatch:
    processed: int = 0
    created: int = 0
    duplicate_event: int = 0
    duplicate_order: int = 0
    stopped: bool = False


class KafkaOrderIntake:
    def __init__(self, consumer, assignment, sessions, *, incoming_topic, workflow_topic,
                 batch_size=10, poll_ms=1000, timeout_seconds=10,
                 database_timeout_seconds=10, max_event_bytes=1048576):
        if (not incoming_topic.strip() or not workflow_topic.strip()
                or incoming_topic == workflow_topic or not 1 <= batch_size <= 100
                or not 1 <= poll_ms <= 10000 or timeout_seconds <= 0
                or database_timeout_seconds <= 0 or not 1 <= max_event_bytes <= 1048576):
            raise ValueError("invalid order consumer configuration")
        self.consumer, self.assignment, self.sessions = consumer, assignment, sessions
        self.topic, self.workflow_topic = incoming_topic, workflow_topic
        self.batch_size, self.poll_ms, self.timeout = batch_size, poll_ms, timeout_seconds
        self.db_timeout, self.max_bytes = database_timeout_seconds, max_event_bytes
        self.epoch, self.last_offsets, self.closed = None, {}, False

    async def run_once(self, *, stop=None):
        if self.closed:
            raise RuntimeError("restart the consumer after failure or interrupted batch")
        result = IntakeBatch()
        current_partition, current_record, event = None, None, None
        try:
            if stop is not None and stop.is_set():
                result.stopped, self.closed = True, True
                return result
            await self.assignment.wait_ready()
            epoch = self.assignment.epoch
            if self.epoch is not None and self.epoch != epoch:
                raise AssignmentChanged("consumer assignment changed between batches")
            self.epoch = epoch
            records = await asyncio.wait_for(self.consumer.getmany(
                timeout_ms=self.poll_ms, max_records=self.batch_size),
                self.timeout + self.poll_ms / 1000)
            self.assignment.require(epoch)
            for partition, messages in records.items():
                for record in messages:
                    current_partition, current_record, event = partition, record, None
                    if stop is not None and stop.is_set():
                        result.stopped, self.closed = True, True
                        return result
                    self.assignment.require(epoch, partition)
                    event = decode_order(record, partition, self.topic, self.max_bytes)
                    if record.offset <= self.last_offsets.get(partition, -1):
                        raise InvalidOrderRecord("non-increasing partition offsets")
                    # accept_order returns only after inbox/workflow/outbox commit.
                    accepted = await asyncio.wait_for(accept_order(
                        event, self.sessions, workflow_topic=self.workflow_topic), self.db_timeout)
                    self.assignment.require(epoch, partition)
                    # Never use consumer.commit() without an explicit offset map:
                    # getmany may already have advanced past unhandled records.
                    await asyncio.wait_for(self.consumer.commit({partition: record.offset + 1}),
                                           self.timeout)
                    self.assignment.require(epoch, partition)
                    self.last_offsets[partition] = record.offset
                    result.processed += 1
                    setattr(result, accepted.outcome, getattr(result, accepted.outcome) + 1)
                    logger.info("order_record_committed", extra={
                        "order_id": accepted.order_id, "event_id": str(event.event_id),
                        "state_version": accepted.state_version, "topic": partition.topic,
                        "partition": partition.partition, "offset": record.offset,
                        "outcome": accepted.outcome})
            return result
        except BaseException as error:
            # Prefetched positions cannot be reused after an unhandled record.
            self.closed = True
            context = {"failure_type": type(error).__name__}
            if current_partition is not None:
                context.update(topic=current_partition.topic, partition=current_partition.partition,
                               offset=current_record.offset)
            if event is not None:
                context.update(order_id=event.payload.order_id, event_id=str(event.event_id))
            logger.error("order_consumer_stopped", extra=context)
            raise


@asynccontextmanager
async def kafka_order_source(settings, *, bootstrap_earliest=False,
                             consumer_factory=AIOKafkaConsumer):
    timeout = settings.kafka_operation_timeout_seconds
    if (not settings.incoming_topic.strip() or not settings.workflow_topic.strip()
            or not settings.consumer_group.strip()
            or settings.incoming_topic == settings.workflow_topic):
        raise ValueError("configure a distinct incoming topic and nonempty consumer group")
    assignment = OrderAssignment(bootstrap_earliest=bootstrap_earliest, timeout_seconds=timeout)
    consumer = consumer_factory(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group, client_id="partstream-coordinator-order-intake",
        enable_auto_commit=False, auto_offset_reset="none", isolation_level="read_committed",
        max_poll_records=settings.incoming_batch_size,
        max_partition_fetch_bytes=settings.incoming_max_event_bytes,
        request_timeout_ms=max(1000, int(timeout * 1000)),
        max_poll_interval_ms=max(300000, int((settings.incoming_batch_size * (
            settings.incoming_database_timeout_seconds + timeout) + 30) * 1000)),
    )
    assignment.consumer = consumer
    try:
        consumer.subscribe([settings.incoming_topic], listener=assignment)
        await asyncio.wait_for(consumer.start(), timeout * 2)
        await assignment.wait_ready()
        yield consumer, assignment
    finally:
        try:
            await asyncio.wait_for(consumer.stop(), timeout)
        except TimeoutError:
            logger.warning("order_consumer_shutdown_timed_out")
