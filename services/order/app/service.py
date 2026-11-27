import hashlib
import json
from datetime import timezone
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select

from app.models import Order, OrderLine, OutboxEvent
from app.schemas import OrderCreateRequest, OrderCreatedPayload, StoredOrder, StoredOrderLine, utc_now


def fingerprint(request: OrderCreateRequest) -> str:
    body = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def as_stored(order: Order, lines: list[OrderLine]) -> StoredOrder:
    def aware(value):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return StoredOrder(
        order_id=order.order_id, status=order.status, created_at=order.created_at,
        lines=[StoredOrderLine(line_id=x.line_id, part_id=x.part_id, quantity=x.quantity, needed_by=aware(x.needed_by)) for x in lines],
    )


async def accept_order(session, request: OrderCreateRequest, idempotency_key: str, topic: str) -> tuple[StoredOrder, bool]:
    if not idempotency_key or len(idempotency_key) > 256:
        raise HTTPException(400, "Idempotency-Key is required and must be 1-256 characters")
    digest = fingerprint(request)
    existing = await session.scalar(select(Order).where(Order.idempotency_key == idempotency_key))
    if existing:
        if existing.request_fingerprint != digest:
            raise HTTPException(409, "Idempotency-Key was already used with a different payload")
        lines = list((await session.scalars(select(OrderLine).where(OrderLine.order_id == existing.order_id))).all())
        return as_stored(existing, lines), True

    now = utc_now()
    order_id = f"ord-{uuid4()}"
    lines = [OrderLine(line_id=f"{order_id}-line-{i}", order_id=order_id, part_id=line.part_id,
                       quantity=line.quantity, needed_by=line.needed_by)
             for i, line in enumerate(request.lines, start=1)]
    order = Order(order_id=order_id, idempotency_key=idempotency_key, request_fingerprint=digest,
                  status="PROCESSING", payload=request.model_dump(mode="json"), created_at=now)
    payload = OrderCreatedPayload(order_id=order_id, lines=[StoredOrderLine(
        line_id=x.line_id, part_id=x.part_id, quantity=x.quantity, needed_by=x.needed_by) for x in lines])
    event_id = str(uuid4())
    envelope = {
        "event_id": event_id,
        "event_type": "order.created",
        "schema_version": 1,
        "producer": "order",
        "occurred_at": now.isoformat(),
        "payload": payload.model_dump(mode="json"),
    }
    event = OutboxEvent(event_id=event_id, order_id=order_id, topic=topic,
                        event_type="order.created", payload=envelope, created_at=now)
    session.add(order)
    session.add_all(lines)
    session.add(event)
    await session.flush()
    return as_stored(order, lines), False

