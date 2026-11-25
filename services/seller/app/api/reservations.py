"""Reserve, lookup-by-key, cancel/fence by operation key, and the internal reservation view.

Wire shapes follow Package B's adapters (services/coordinator/app/clients/).
NOTE: /reservations/by-key/ routes are registered before /reservations/{reservation_id}
routes so an operation key can never be mistaken for a reservation id.
"""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Header, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from app.api.deps import after_commit, before
from app.errors import ApiError
from app.schemas import (
    CancellationReply, CancelOperationRequest, ReservationReply, ReservationRequest, ReservationView,
)
from app.services import cancellation, inventory
from app.services.idempotency import is_valid_key, lookup_by_key

router = APIRouter(tags=["reservations"])
log = logging.getLogger("seller.reservations")


def _header_key(value: str | None, name: str) -> str:
    if not is_valid_key(value):
        raise ApiError(400, "IDEMPOTENCY_KEY_REQUIRED",
                       f"{name} header is required: 1-200 chars of [A-Za-z0-9._:-]")
    return value  # type: ignore[return-value]


def _path_key(key: str) -> None:
    if not is_valid_key(key):
        raise ApiError(422, "VALIDATION_ERROR", "Invalid operation key")


def _json(model_cls, data: dict, status: int, headers: dict | None = None) -> JSONResponse:
    return JSONResponse(jsonable_encoder(model_cls(**data)), status_code=status, headers=headers)


# ---- reserve ----------------------------------------------------------------------
@router.post("/reservations", status_code=201, response_model=ReservationReply,
             responses={200: {"model": ReservationReply}, 409: {"model": ReservationReply}})
def create_reservation(body: ReservationRequest, request: Request,
                       idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    key = _header_key(idempotency_key, "Idempotency-Key")
    decision = before(request, "reserve")
    settings = request.app.state.settings
    out = inventory.reserve(request.app.state.engine, settings, key, body.model_dump(mode="json"))

    log.info("reserve", extra={"operation_key": key, "result": out.result, "part_id": body.part_id,
                               "quantity": body.quantity, "order_id": body.order_id, "line_id": body.line_id,
                               "reservation_id": str(out.row["reservation_id"]) if out.row else None})
    after_commit(decision, out.committed_change, "reserve")

    if out.result in ("RESERVED", "REPLAYED") and out.row is not None:
        reply = inventory.reserved_reply(out.row, settings.seller_id)
        if out.result == "RESERVED":
            return _json(ReservationReply, reply, 201)
        return _json(ReservationReply, reply, 200, {"Idempotent-Replayed": "true"})
    if out.result == "KEY_REUSED":
        # Caller bug: same key, different payload. Not a REJECTED reply on purpose.
        raise ApiError(409, "IDEMPOTENCY_KEY_REUSED",
                       "This Idempotency-Key was already used with a different payload",
                       {"operation_key": key})
    # REJECTED (INSUFFICIENT_STOCK / PART_NOT_FOUND) or OPERATION_CANCELLED (fenced key):
    # definitive, no stock taken by this key -> 409 with status REJECTED.
    reason = out.error_code or "REJECTED"
    return _json(ReservationReply,
                 inventory.rejected_reply(settings.seller_id, key, body.part_id, body.quantity, reason), 409)


# ---- by operation key (registered BEFORE /{reservation_id}) --------------------------
@router.get("/reservations/by-key/{operation_key}", response_model=ReservationReply)
def get_by_key(operation_key: str, request: Request):
    _path_key(operation_key)
    before(request, "lookup")
    with request.app.state.engine.connect() as conn:
        found = lookup_by_key(conn, operation_key, request.app.state.settings.seller_id)
    if found.kind == "NOT_FOUND":
        raise ApiError(404, "KEY_NOT_FOUND",
                       "No record of this operation key. Retry the reserve with the same key, or fence it.",
                       {"operation_key": operation_key})
    if found.kind == "CANCELLED":
        raise ApiError(409, "OPERATION_CANCELLED",
                       "This operation was reserved and later cancelled; use the cancellation endpoint "
                       "for the terminal receipt", {"operation_key": operation_key})
    return found.reply


@router.post("/reservation-operations/{operation_key}/cancel", response_model=CancellationReply,
             responses={202: {"model": CancellationReply}})
def cancel_operation(operation_key: str, body: CancelOperationRequest, request: Request,
                     idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    _path_key(operation_key)
    cancellation_key = _header_key(idempotency_key, "Idempotency-Key (cancellation key)")
    if body.reservation_id is not None:
        try:
            uuid.UUID(body.reservation_id)
        except ValueError:
            raise ApiError(409, "OPERATION_MISMATCH", "reservation_id does not belong to this operation",
                           {"operation_key": operation_key}) from None
    decision = before(request, "cancel")
    out = cancellation.cancel_operation(request.app.state.engine, request.app.state.settings,
                                        operation_key, cancellation_key, body.model_dump())
    log.info("cancel_operation", extra={"operation_key": operation_key, "cancellation_key": cancellation_key,
                                        "http_status": out.http_status,
                                        "status": out.reply["status"] if out.reply else out.error_code})
    after_commit(decision, out.committed_change, "cancel")
    if out.reply is None:
        raise ApiError(out.http_status, out.error_code or "ERROR",
                       "Request does not match the recorded operation (part, quantity or reservation id)",
                       {"operation_key": operation_key})
    return _json(CancellationReply, out.reply, out.http_status)


# ---- internal view by reservation id ---------------------------------------------------
@router.get("/reservations/{reservation_id}", response_model=ReservationView)
def get_reservation(reservation_id: uuid.UUID, request: Request):
    with request.app.state.engine.connect() as conn:
        row = inventory.get_reservation_row(conn, reservation_id)
    if row is None:
        raise ApiError(404, "RESERVATION_NOT_FOUND", "Reservation not found", {"reservation_id": str(reservation_id)})
    return inventory.reservation_view(row, request.app.state.settings.seller_id)
