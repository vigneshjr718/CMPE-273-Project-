"""Team-only admin endpoints (X-Admin-Token): fault injection, delivery demo controls,
inventory inspection/restock. Disabled entirely when ADMIN_TOKEN is unset."""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Depends, Request, Response

from app.api.deps import require_admin
from app.errors import ApiError
from app.faults import FaultRule
from app.schemas import DeliveryUpdate, FaultRuleCreate, InventoryItem, InventoryUpsert, ReservationView
from app.services import inventory
from app.services.delivery import update_delivery

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])
log = logging.getLogger("seller.admin")


# ---- faults -------------------------------------------------------------------------
@router.get("/faults")
def list_faults(request: Request):
    return {"faults": [r.to_dict() for r in request.app.state.faults.list()]}


@router.post("/faults", status_code=201)
def add_fault(body: FaultRuleCreate, request: Request):
    rule = request.app.state.faults.add(FaultRule(**body.model_dump()))
    log.warning("fault rule added", extra={"rule": rule.to_dict()})
    return rule.to_dict()


@router.delete("/faults", status_code=204)
def clear_faults(request: Request):
    request.app.state.faults.clear()
    log.warning("fault rules cleared")
    return Response(status_code=204)


# ---- delivery demo controls ------------------------------------------------------------
@router.post("/reservations/{reservation_id}/delivery", response_model=ReservationView)
def set_delivery(reservation_id: uuid.UUID, body: DeliveryUpdate, request: Request):
    """Demo: make a delivery late (delay_hours / estimated_delivery_at), ship it, or deliver it.
    Every real change bumps the operation's status_version."""
    if body.delivery_status is None and body.estimated_delivery_at is None and body.delay_hours is None:
        raise ApiError(422, "VALIDATION_ERROR", "Provide delivery_status, estimated_delivery_at or delay_hours")
    settings = request.app.state.settings
    status, code, row = update_delivery(request.app.state.engine, settings, reservation_id,
                                        body.delivery_status, body.estimated_delivery_at, body.delay_hours)
    if status != 200:
        raise ApiError(status, code or "ERROR", {
            "RESERVATION_NOT_FOUND": "Reservation not found",
            "RESERVATION_CANCELLED": "Reservation is cancelled",
            "INVALID_TRANSITION": "Delivery status cannot move that way",
        }.get(code or "", "Update failed"), {"reservation_id": str(reservation_id)})
    log.info("delivery updated", extra={"reservation_id": str(reservation_id), **body.model_dump(mode="json")})
    return inventory.reservation_view(row, settings.seller_id)


# ---- inventory -------------------------------------------------------------------------
@router.get("/inventory")
def get_inventory(request: Request, part_id: str | None = None):
    items = inventory.list_inventory(request.app.state.engine, part_id)
    return {"seller_id": request.app.state.settings.seller_id, "items": items}


@router.post("/inventory", response_model=InventoryItem)
def restock(body: InventoryUpsert, request: Request):
    item = inventory.restock(request.app.state.engine, body.part_id, body.add_quantity, body.description,
                             body.unit_price_cents)
    log.info("restock", extra={"part_id": body.part_id, "add_quantity": body.add_quantity})
    return item
