"""Seller service: FastAPI app, router registration, startup/shutdown.

    uvicorn app.main:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api import admin_faults, delivery, health, quotes, reservations
from app.config import Settings, get_settings
from app.db import make_engine
from app.errors import install_error_handlers
from app.faults import FaultRegistry
from app.logging_setup import setup_logging
from app.migrate import run_migrations

log = logging.getLogger("seller")


def create_app(settings: Settings | None = None, *, run_startup: bool = True) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings)
        app.state.engine = engine
        if run_startup:
            _startup(engine, settings)
        log.info("seller started", extra={"delivery_hours": settings.delivery_hours,
                                          "admin_enabled": bool(settings.admin_token)})
        try:
            yield
        finally:
            engine.dispose()
            log.info("seller stopped")

    app = FastAPI(title=f"PartStream Seller ({settings.seller_id})", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.faults = FaultRegistry()
    install_error_handlers(app)

    @app.middleware("http")
    async def request_log(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            if request.url.path != "/health":
                log.info("request", extra={
                    "request_id": request_id, "method": request.method, "path": request.url.path,
                    "status": status, "duration_ms": round((time.perf_counter() - start) * 1000, 2),
                    "operation_key": request.headers.get("idempotency-key"),
                })

    # Order matters: reservations (incl. /by-key/ routes) before delivery.
    app.include_router(health.router)
    app.include_router(quotes.router)
    app.include_router(reservations.router)
    app.include_router(delivery.router)
    app.include_router(admin_faults.router)
    return app


def _startup(engine, settings: Settings) -> None:
    """Migrate, then seed (insert-if-missing). Retries while Postgres is still starting."""
    from seed.generate_seed import apply as apply_seed

    for attempt in range(1, 31):
        try:
            applied = run_migrations(engine)
            if applied:
                log.info("migrations applied", extra={"migrations": applied})
            break
        except Exception as exc:
            log.warning("database not ready", extra={"attempt": attempt, "error": str(exc)[:200]})
            time.sleep(min(2 * attempt, 10))
    else:
        raise RuntimeError("database never became ready")

    if settings.seed_on_startup:
        inserted = apply_seed(engine, settings.seed_base, settings.seller_number, settings.price_multiplier)
        log.info("seed applied", extra={"inserted_rows": inserted})


app = create_app()  # uvicorn app.main:app  (no DB connection until startup)
