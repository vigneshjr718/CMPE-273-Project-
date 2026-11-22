"""PROVISIONAL C contract: durable terminal fencing of an original operation."""
from typing import Literal, Protocol
from urllib.parse import quote

from pydantic import AwareDatetime, Field, model_validator

from app.schemas import ContractModel, Identifier


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


def validate_cancellation(reply, *, seller_id, operation_key, cancellation_key,
                          line, reservation_id):
    reply = CancellationReply.model_validate(reply)
    if (reply.seller_id, reply.operation_key, reply.cancellation_key,
        reply.part_id, reply.quantity) != (
        seller_id, operation_key, cancellation_key, line.part_id, line.quantity,
    ) or (reservation_id is not None and reply.reservation_id != reservation_id):
        raise ValueError("cancellation evidence does not identify the saved attempt")
    return reply


def require_cancelled(attempt, check, line):
    """Revalidate persisted proof before any replacement reservation."""
    if attempt.status != "CANCELLED" or check is None or check.status != "CONFIRMED":
        raise ValueError("replacement requires persisted terminal cancellation")
    if (check.attempt_id, check.order_id, check.line_id) != (
        attempt.attempt_id, attempt.order_id, attempt.line_id,
    ) or attempt.line_id != line.line_id:
        raise ValueError("cancellation proof belongs to another attempt or line")
    reply = validate_cancellation(
        check.last_payload, seller_id=attempt.seller_id,
        operation_key=attempt.operation_key, cancellation_key=check.cancellation_key,
        line=line, reservation_id=attempt.reservation_id,
    )
    if reply.status != "CANCELLED" or reply.status_version != check.status_version:
        raise ValueError("missing or inconsistent terminal fence proof")
    return reply


class CancellationClient(Protocol):
    async def cancel(self, seller_id: str, order_id: str, line,
                     operation_key: str, cancellation_key: str,
                     reservation_id: str | None) -> CancellationReply: ...


class HttpCancellationClient:
    def __init__(self, client, seller_urls):
        self.client, self.seller_urls = client, seller_urls

    async def cancel(self, seller_id, order_id, line, operation_key,
                     cancellation_key, reservation_id):
        response = await self.client.post(
            self.seller_urls[seller_id].rstrip("/") + "/reservation-operations/"
            + quote(operation_key, safe="") + "/cancel",
            headers={"Idempotency-Key": cancellation_key},
            json={"order_id": order_id, "line_id": line.line_id,
                  "part_id": line.part_id, "quantity": line.quantity,
                  "reservation_id": reservation_id},
        )
        response.raise_for_status()
        if response.status_code not in (200, 202):
            raise ValueError("cancellation endpoint did not return evidence")
        reply = CancellationReply.model_validate(response.json())
        if response.status_code == 202 and reply.status != "PENDING":
            raise ValueError("202 is not terminal cancellation evidence")
        return reply
