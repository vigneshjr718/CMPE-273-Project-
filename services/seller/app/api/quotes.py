from __future__ import annotations

from fastapi import APIRouter, Request

from app.api.deps import before
from app.schemas import QuoteReply, QuoteRequest
from app.services import inventory

router = APIRouter(tags=["quotes"])


@router.post("/quotes", response_model=QuoteReply)
def create_quote(body: QuoteRequest, request: Request):
    """Non-binding, read-only: holds no stock, safe to retry. One line per request.

    The coordinator decides eligibility itself (enough stock AND
    estimated_delivery_at <= needed_by); `needed_by` is accepted for logging only.
    """
    before(request, "quotes")
    return inventory.quote(request.app.state.engine, request.app.state.settings, body.part_id)
