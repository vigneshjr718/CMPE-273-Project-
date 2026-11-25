from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.errors import error_body

router = APIRouter(tags=["health"])


@router.get("/health")
def health(request: Request):
    settings = request.app.state.settings
    try:
        with request.app.state.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        return JSONResponse(error_body("SELLER_UNAVAILABLE", "Database unreachable", {"error": str(exc)[:200]}),
                            status_code=503)
    return {"status": "ok", "service": "seller", "seller_id": settings.seller_id, "db": "ok"}
