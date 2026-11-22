"""PROVISIONAL C contract. Keep all wire translations in this adapter."""
from typing import Literal, Protocol

import httpx
from pydantic import AwareDatetime, Field, model_validator

from app.clients.seller_client import HttpQuoteClient, QuoteClient
from app.schemas import ContractModel, Identifier, OrderLine


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


class ReservationClient(QuoteClient, Protocol):
    async def reserve(
        self, seller_id: str, order_id: str, line: OrderLine, operation_key: str,
    ) -> ReservationReply: ...


class HttpReservationClient(HttpQuoteClient):
    async def reserve(
        self, seller_id: str, order_id: str, line: OrderLine, operation_key: str,
    ) -> ReservationReply:
        response = await self.client.post(
            self.seller_urls[seller_id].rstrip("/") + "/reservations",
            headers={"Idempotency-Key": operation_key},
            json={
                "order_id": order_id, "line_id": line.line_id,
                "part_id": line.part_id, "quantity": line.quantity,
                "needed_by": line.needed_by.isoformat(),
            },
        )
        # A generic 4xx/5xx is not evidence of whether a side effect happened.
        if response.status_code not in (200, 201, 409):
            response.raise_for_status()
            raise ValueError("unexpected reservation response")
        reply = ReservationReply.model_validate(response.json())
        if response.status_code == 409 and reply.status != "REJECTED":
            raise ValueError("ambiguous conflict response")
        return reply
