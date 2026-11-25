"""Error envelope: {"error": {"code", "message", "details"}} for every non-2xx."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, OperationalError

log = logging.getLogger("seller.errors")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: dict[str, Any] | None = None,
                 headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}
        self.headers = headers


def error_body(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details or {}}}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(error_body(exc.code, exc.message, exc.details), status_code=exc.status,
                            headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [{"loc": list(e.get("loc", [])), "msg": e.get("msg"), "type": e.get("type")} for e in exc.errors()]
        return JSONResponse(error_body("VALIDATION_ERROR", "Request validation failed", {"errors": errors}),
                            status_code=422)

    @app.exception_handler(OperationalError)
    async def _db_down(request: Request, exc: OperationalError) -> JSONResponse:
        # Raised before/at commit -> the transaction did not commit.
        log.warning("database unavailable", extra={"path": request.url.path, "error": str(exc.orig)})
        return JSONResponse(error_body("SELLER_UNAVAILABLE", "Database unavailable"), status_code=503)

    @app.exception_handler(DBAPIError)
    async def _db_error(request: Request, exc: DBAPIError) -> JSONResponse:
        # e.g. statement_timeout (QueryCanceled). Transaction rolled back.
        log.error("database error", extra={"path": request.url.path, "error": str(exc.orig)})
        return JSONResponse(error_body("SELLER_UNAVAILABLE", "Database error; nothing was committed"),
                            status_code=503)
