"""VERBATIM copies of Package B's seller-facing reply models.

Source: vigneshjr718/CMPE-273-Project- branch pr/coordinator-complete-integration
  services/coordinator/app/schemas.py              (Identifier, ContractModel, Quote)
  services/coordinator/app/clients/reservations.py (ReservationReply)
  services/coordinator/app/clients/cancellation.py (CancellationReply)
  services/coordinator/app/clients/delivery.py     (DeliveryReply)

test_api.py validates every seller response against these, so a seller change that
would break B's coordinator fails here first. If B changes its adapters, re-copy.
"""
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints, model_validator,
)

Identifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Quote(ContractModel):
    seller_id: Identifier
    part_id: Identifier
    available_quantity: int = Field(strict=True, ge=0)
    estimated_delivery_at: AwareDatetime


class ReservationReply(ContractModel):
    seller_id: Identifier
    operation_key: Identifier
    part_id: Identifier
    quantity: int = Field(strict=True, gt=0)
    status: Literal["RESERVED", "REJECTED"]
    reservation_id: Identifier | None = None
    estimated_delivery_at: AwareDatetime | None = None
    reason: str | None = Field(default=None, max_length=80)

    @model_validator(mode="after")
    def require_evidence(self):
        if self.status == "RESERVED" and (
            self.reservation_id is None or self.estimated_delivery_at is None
        ):
            raise ValueError("RESERVED needs reservation_id and delivery estimate")
        if self.status == "REJECTED" and self.reservation_id is not None:
            raise ValueError("REJECTED must prove no reservation was created")
        return self


class CancellationReply(ContractModel):
    seller_id: Identifier
    operation_key: str = Field(min_length=1, max_length=80)
    cancellation_key: str = Field(min_length=1, max_length=80)
    part_id: Identifier
    quantity: int = Field(strict=True, gt=0)
    reservation_id: Identifier | None = None
    status: Literal["PENDING", "CANCELLED", "DELIVERED"]
    # Same monotonic source counter as delivery, per original operation.
    status_version: int = Field(strict=True, ge=1)
    fenced: bool = Field(strict=True)
    cancellation_receipt: Identifier | None = None
    delivered_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def evidence(self):
        if self.status == "CANCELLED":
            if not self.fenced or not self.cancellation_receipt or self.delivered_at:
                raise ValueError("cancellation requires a terminal fence receipt and no delivery")
        else:
            if self.fenced or self.cancellation_receipt:
                raise ValueError("only terminal cancellation can attest fencing")
            if self.status == "DELIVERED":
                if not self.reservation_id or not self.delivered_at:
                    raise ValueError("delivery requires reservation identity and timestamp")
            elif self.delivered_at:
                raise ValueError("pending cancellation cannot attest delivery")
        return self


class DeliveryReply(ContractModel):
    seller_id: Identifier
    operation_key: str = Field(min_length=1, max_length=80)
    reservation_id: Identifier
    part_id: Identifier
    quantity: int = Field(strict=True, gt=0)
    status: Literal["IN_TRANSIT", "DELIVERED"]
    # Monotonic per reservation, not a timestamp or cross-service counter.
    status_version: int = Field(strict=True, ge=1)
    estimated_delivery_at: AwareDatetime | None = None
    delivered_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def evidence(self):
        if self.status == "IN_TRANSIT":
            if self.estimated_delivery_at is None or self.delivered_at is not None:
                raise ValueError("in-transit observation requires ETA and no delivery proof")
        elif self.delivered_at is None:
            raise ValueError("delivered observation requires delivered_at")
        return self
