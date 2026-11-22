"""PROVISIONAL C adapter: versioned observations for an existing reservation."""
from typing import Literal, Protocol
from urllib.parse import quote

from pydantic import AwareDatetime, Field, model_validator

from app.schemas import ContractModel, Identifier


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


class DeliveryClient(Protocol):
    async def delivery(self, seller_id: str, reservation_id: str) -> DeliveryReply: ...


class HttpDeliveryClient:
    def __init__(self, client, seller_urls):
        self.client, self.seller_urls = client, seller_urls

    async def delivery(self, seller_id, reservation_id):
        response = await self.client.get(
            self.seller_urls[seller_id].rstrip("/") + "/reservations/"
            + quote(reservation_id, safe="") + "/delivery")
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("delivery endpoint did not return an observation")
        return DeliveryReply.model_validate(response.json())
