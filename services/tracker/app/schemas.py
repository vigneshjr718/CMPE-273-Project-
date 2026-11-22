"""Response models. Must match contracts/openapi/status-api.yaml (checked by tests/test_api.py)."""
from datetime import datetime

from pydantic import BaseModel


class Flags(BaseModel):
    pending: bool
    reconciliation_required: bool
    replaced: bool
    terminal: bool


class SelectedSeller(BaseModel):
    seller_id: str
    attempt_id: str
    estimated_delivery_at: datetime | None
    delivery_status: str | None
    delivered_at: datetime | None


class Attempt(BaseModel):
    attempt_id: str
    seller_id: str | None
    outcome: str
    reason: str | None
    started_at: datetime | None
    updated_at: datetime


class LineView(BaseModel):
    line_id: str
    part_id: str | None
    quantity: int | None
    needed_by: datetime | None
    status: str
    reason: str | None
    flags: Flags
    selected_seller: SelectedSeller | None
    attempt_count: int
    attempts: list[Attempt]
    last_updated: datetime


class OrderView(BaseModel):
    order_id: str
    status: str
    flags: Flags
    order_created_seen: bool
    state_version: int
    created_at: datetime | None
    last_updated: datetime
    lines: list[LineView]


class ErrorBody(BaseModel):
    code: str
    message: str
    retry_after_seconds: int | None = None
