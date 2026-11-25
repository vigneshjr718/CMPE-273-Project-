from __future__ import annotations

from fastapi import APIRouter, Request

from app.api.deps import before
from app.errors import ApiError
from app.schemas import DeliveryReply
from app.services.delivery import delivery_reply, get_delivery_row

router = APIRouter(tags=["delivery"])


@router.get("/reservations/{reservation_id}/delivery", response_model=DeliveryReply)
def delivery_status(reservation_id: str, request: Request):
    """Polled by the coordinator. Versioned observation; read-only.

    A cancelled reservation has no delivery observation: 409 RESERVATION_CANCELLED
    (an observation failure for the caller, never delivery proof).
    """
    before(request, "delivery")
    row = get_delivery_row(request.app.state.engine, reservation_id)
    if row is None:
        raise ApiError(404, "RESERVATION_NOT_FOUND", "Reservation not found", {"reservation_id": reservation_id})
    if row["status"] == "CANCELLED":
        raise ApiError(409, "RESERVATION_CANCELLED", "Reservation was cancelled",
                       {"reservation_id": reservation_id, "status_version": row["status_version"]})
    return delivery_reply(row, request.app.state.settings.seller_id)
