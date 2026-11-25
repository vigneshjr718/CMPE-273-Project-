"""Pydantic request/response models.

The wire shapes match Package B's coordinator adapters exactly
(services/coordinator/app/clients/*.py: Quote, ReservationReply,
CancellationReply, DeliveryReply). B parses with extra="forbid", so reply models
here must not grow extra fields. See contracts/openapi/seller-api.yaml.
"""
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

# Same identifier rule as B's schemas.Identifier and A's order.created schema.
Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Reply(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ---- quotes -----------------------------------------------------------------
class QuoteRequest(_Request):
    part_id: Identifier
    quantity: int = Field(gt=0, le=100_000)
    needed_by: AwareDatetime
    order_id: Identifier | None = None
    line_id: Identifier | None = None


class QuoteReply(_Reply):
    seller_id: str
    part_id: str
    available_quantity: int
    estimated_delivery_at: datetime


# ---- reservations -------------------------------------------------------------
class ReservationRequest(_Request):
    order_id: Identifier
    line_id: Identifier
    part_id: Identifier
    quantity: int = Field(gt=0, le=100_000)
    needed_by: AwareDatetime | None = None


class ReservationReply(_Reply):
    """Reserve result and lookup-by-key result (B: app/clients/reservations.py)."""
    seller_id: str
    operation_key: str
    part_id: str
    quantity: int
    status: Literal["RESERVED", "REJECTED"]
    reservation_id: str | None = None
    estimated_delivery_at: datetime | None = None
    reason: str | None = None


# ---- cancellation / fencing -----------------------------------------------------
class CancelOperationRequest(_Request):
    order_id: Identifier
    line_id: Identifier
    part_id: Identifier
    quantity: int = Field(gt=0, le=100_000)
    reservation_id: Identifier | None = None


class CancellationReply(_Reply):
    """B: app/clients/cancellation.py."""
    seller_id: str
    operation_key: str
    cancellation_key: str
    part_id: str
    quantity: int
    reservation_id: str | None
    status: Literal["PENDING", "CANCELLED", "DELIVERED"]
    status_version: int
    fenced: bool
    cancellation_receipt: str | None
    delivered_at: datetime | None


# ---- delivery -------------------------------------------------------------------
class DeliveryReply(_Reply):
    """B: app/clients/delivery.py."""
    seller_id: str
    operation_key: str
    reservation_id: str
    part_id: str
    quantity: int
    status: Literal["IN_TRANSIT", "DELIVERED"]
    status_version: int
    estimated_delivery_at: datetime | None
    delivered_at: datetime | None


# ---- internal / debugging view ----------------------------------------------------
class ReservationView(BaseModel):
    reservation_id: str
    seller_id: str
    operation_key: str
    order_id: str
    line_id: str
    part_id: str
    quantity: int
    unit_price_cents: int
    status: Literal["RESERVED", "CANCELLED", "DELIVERED"]
    delivery_status: Literal["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]
    needed_by: datetime | None
    estimated_delivery_at: datetime
    is_late: bool
    status_version: int
    cancellation_receipt: str | None
    created_at: datetime
    updated_at: datetime
    cancelled_at: datetime | None
    delivered_at: datetime | None


# ---- admin ------------------------------------------------------------------------
class DeliveryUpdate(_Request):
    delivery_status: Literal["PENDING", "SHIPPED", "DELIVERED"] | None = None
    estimated_delivery_at: AwareDatetime | None = None
    delay_hours: float | None = Field(default=None, gt=0, le=24 * 365)


class FaultRuleCreate(_Request):
    endpoint: Literal["*", "quotes", "reserve", "cancel", "lookup", "delivery"]
    delay_ms: int = Field(0, ge=0, le=120_000)
    error_rate: float = Field(0.0, ge=0, le=1)
    lost_reply_rate: float = Field(0.0, ge=0, le=1)
    lost_reply_hang_ms: int = Field(30_000, ge=0, le=300_000)
    remaining: int | None = Field(default=None, ge=1)


class InventoryItem(BaseModel):
    part_id: str
    description: str
    total_quantity: int
    available_quantity: int
    reserved_quantity: int
    delivered_quantity: int
    unit_price_cents: int


class InventoryUpsert(_Request):
    part_id: Identifier
    add_quantity: int = Field(ge=0, le=1_000_000_000)
    description: str | None = Field(default=None, max_length=200)
    unit_price_cents: int | None = Field(default=None, ge=1)
