"""Transactional acceptance; Kafka acknowledgement is owned by the transport."""
import asyncio
from dataclasses import dataclass
from typing import Literal
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import OutboxEvent, ProcessedEvent, Workflow
from app.schemas import OrderCreatedEvent, utc_now


class IngestionConflict(ValueError):
    """Do not acknowledge the source event; arrange explicit conflict handling."""


@dataclass(frozen=True)
class Acceptance:
    order_id: str
    status: str
    state_version: int
    outcome: Literal["created", "duplicate_event", "duplicate_order"]


async def accept_order(
    event: OrderCreatedEvent,
    sessions: async_sessionmaker[AsyncSession],
    *,
    workflow_topic: str,
) -> Acceptance:
    if not workflow_topic.strip():
        raise ValueError("workflow_topic must not be empty")
    order_id = event.payload.order_id
    event_id = str(event.event_id)
    payload = event.payload.model_dump(mode="json")

    # Unique DB keys arbitrate concurrent insertions. Any losing transaction
    # rolls back in full, then re-reads the winning committed business record.
    for attempt in range(3):
        try:
            async with sessions.begin() as session:
                processed = await session.get(ProcessedEvent, event_id)
                workflow = await session.get(Workflow, order_id)
                if processed:
                    if (
                        processed.order_id != order_id or workflow is None
                        or workflow.payload != payload
                    ):
                        raise IngestionConflict(
                            "event_id reused with a different order or business payload"
                        )
                    return Acceptance(order_id, workflow.status,
                                      workflow.state_version, "duplicate_event")
                if workflow:
                    if workflow.payload != payload:
                        raise IngestionConflict(
                            "order_id reused with a different business payload"
                        )
                    session.add(ProcessedEvent(event_id=event_id, order_id=order_id))
                    return Acceptance(order_id, workflow.status,
                                      workflow.state_version, "duplicate_order")

                now = utc_now()
                workflow = Workflow(
                    order_id=order_id, payload=payload, status="PENDING",
                    state_version=1, created_at=now, updated_at=now,
                )
                session.add(workflow)
                await session.flush()  # Parent exists before FK-dependent inserts.
                session.add(ProcessedEvent(event_id=event_id, order_id=order_id))
                outgoing_id = str(uuid4())
                session.add(OutboxEvent(
                    event_id=outgoing_id, order_id=order_id,
                    topic=workflow_topic, event_type="workflow.accepted",
                    state_version=1, created_at=now, payload={
                        "event_id": outgoing_id,
                        "event_type": "workflow.accepted",
                        "schema_version": 1,
                        "producer": "coordinator",
                        "occurred_at": now.isoformat(),
                        "order_id": order_id,
                        "state_version": 1,
                        "payload": {
                            "status": "PENDING",
                            "lines": payload["lines"],
                        },
                    },
                ))
                # __aexit__ commits all three before returning to the caller.
                return Acceptance(order_id, "PENDING", 1, "created")
        except IntegrityError:
            if attempt == 2:
                raise  # Leave event unacknowledged; transport must retry safely.
            await asyncio.sleep(0)
    raise AssertionError("unreachable")
