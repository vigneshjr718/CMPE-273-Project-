from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator

Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OrderLineInput(ContractModel):
    part_id: Identifier
    quantity: int = Field(strict=True, gt=0, le=10000)
    needed_by: AwareDatetime


class OrderCreateRequest(ContractModel):
    lines: list[OrderLineInput] = Field(min_length=1, max_length=100)


class StoredOrderLine(OrderLineInput):
    line_id: Identifier


class StoredOrder(ContractModel):
    order_id: Identifier
    status: str
    lines: list[StoredOrderLine]
    created_at: datetime


class OrderCreatedPayload(ContractModel):
    order_id: Identifier
    lines: list[StoredOrderLine]

    @model_validator(mode="after")
    def unique_lines(self):
        ids = [line.line_id for line in self.lines]
        if len(ids) != len(set(ids)):
            raise ValueError("line_id must be unique within the order")
        return self


class OrderCreatedEvent(ContractModel):
    event_id: UUID
    event_type: str = "order.created"
    schema_version: int = 1
    producer: str = "order"
    occurred_at: datetime
    payload: OrderCreatedPayload


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

