"""GET /status/orders/{order_id}, as defined in contracts/openapi/status-api.yaml."""
import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from app.events import ORDER_ID
from app.schemas import ErrorBody, OrderView
from app.views import load_order_view

router = APIRouter(tags=["status"])
logger = logging.getLogger("tracker.api")
NO_STORE = {"Cache-Control": "no-store"}


def _error(status: int, code: str, message: str, retry_after: int | None = None) -> JSONResponse:
    headers = dict(NO_STORE)
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    body = ErrorBody(code=code, message=message, retry_after_seconds=retry_after)
    return JSONResponse(status_code=status, content=body.model_dump(exclude_none=True),
                        headers=headers)


@router.get(
    "/status/orders/{order_id}",
    response_model=OrderView,
    responses={400: {"model": ErrorBody}, 404: {"model": ErrorBody}, 503: {"model": ErrorBody}},
)
async def get_order_status(order_id: str, request: Request):
    if not ORDER_ID.fullmatch(order_id):
        return _error(400, "INVALID_ORDER_ID",
                      "order_id must be 1-128 characters of A-Z, a-z, 0-9, '-' or '_'.")
    settings = request.app.state.settings
    try:
        async with request.app.state.sessions() as session:
            view = await asyncio.wait_for(load_order_view(session, order_id),
                                          timeout=settings.dependency_timeout_seconds)
    except (SQLAlchemyError, OSError, TimeoutError) as error:
        logger.warning("status_store_unavailable", extra={
            "order_id": order_id, "failure_type": type(error).__name__})
        return _error(503, "STATUS_UNAVAILABLE", "Status store temporarily unavailable.", 5)
    if view is None:
        # The tracker cannot prove the order does not exist; it may not be projected yet.
        return _error(404, "ORDER_NOT_YET_VISIBLE",
                      "No status events have been processed for this order yet. "
                      "If it was just submitted, it is still being processed.",
                      settings.not_visible_retry_after_seconds)
    return JSONResponse(content=view.model_dump(mode="json"), headers=NO_STORE)
