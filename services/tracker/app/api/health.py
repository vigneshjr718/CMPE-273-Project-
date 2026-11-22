import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import func, select

from app.models import DeadLetterEvent

router = APIRouter(tags=["ops"])
logger = logging.getLogger("tracker.health")


@router.get("/health")
async def health():
    """Liveness: the process is up. Does not check dependencies."""
    return {"status": "ok", "service": "tracker"}


@router.get("/ready")
async def ready(request: Request):
    """Readiness: DB reachable with current migrations, and (if enabled) consumer running."""
    state = request.app.state
    checks = {}
    try:
        await asyncio.wait_for(state.database_probe(),
                               timeout=state.settings.dependency_timeout_seconds)
        checks["database_and_migrations"] = "passed"
    except Exception as error:  # report, never crash the probe
        logger.warning("database_readiness_failed", extra={"failure_type": type(error).__name__})
        checks["database_and_migrations"] = "failed"

    # Informational: dead letters do not make the tracker unready (it keeps consuming),
    # but a non-zero count means a producer sent something the tracker could not read.
    try:
        async with state.sessions() as session:
            checks["dead_letter_events"] = await asyncio.wait_for(
                session.scalar(select(func.count()).select_from(DeadLetterEvent)),
                timeout=state.settings.dependency_timeout_seconds)
    except Exception as error:
        logger.warning("dead_letter_count_failed", extra={"failure_type": type(error).__name__})
        checks["dead_letter_events"] = None

    consumer = state.consumer_status
    if state.settings.consumer_enabled:
        checks["consumer"] = consumer.snapshot()
    ok = checks["database_and_migrations"] == "passed" and (
        not state.settings.consumer_enabled or consumer.state == "running")
    body = {"status": "ready" if ok else "not_ready", "service": "tracker", "checks": checks}
    return JSONResponse(status_code=200 if ok else 503, content=body)
