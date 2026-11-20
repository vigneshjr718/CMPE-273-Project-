"""PROVISIONAL local boundary models. Replace/adapt to approved contracts."""
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator,
)

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OrderLine(ContractModel):
    line_id: Identifier
    part_id: Identifier
    quantity: int = Field(strict=True, gt=0, le=10000)
    needed_by: AwareDatetime
    # Do not reject historical deadlines here: replay must remain parseable.
    # The orchestrator will assess eligibility using the current time.


class OrderCreatedPayload(ContractModel):
    order_id: Identifier
    lines: list[OrderLine] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def unique_lines(self):
        ids = [line.line_id for line in self.lines]
        if len(ids) != len(set(ids)):
            raise ValueError("line_id must be unique within the order")
        return self


class OrderCreatedEvent(ContractModel):
    event_id: UUID
    event_type: Literal["order.created"]
    schema_version: Literal[1]
    producer: Literal["order"]
    occurred_at: AwareDatetime
    payload: OrderCreatedPayload


class Quote(ContractModel):
    seller_id: Identifier
    part_id: Identifier
    available_quantity: int = Field(strict=True, ge=0)
    estimated_delivery_at: AwareDatetime


def utc_now() -> datetime:
    from datetime import timezone
    return datetime.now(timezone.utc)
