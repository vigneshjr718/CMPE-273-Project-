import asyncio
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()
logger = logging.getLogger("coordinator.health")


@router.get("/health")
async def health():
    return {"service": "coordinator", "status": "alive", "implementation": "foundation"}


@router.get("/ready")
async def readiness(request: Request):
    try:
        await asyncio.wait_for(
            request.app.state.database_probe(),
            timeout=request.app.state.settings.dependency_timeout_seconds,
        )
    except Exception as error:
        logger.warning("database_readiness_failed",
                       extra={"failure_type": type(error).__name__})
        return JSONResponse(status_code=503, content={
            "service": "coordinator", "status": "not_ready",
            "checks": {"database_and_migrations": "failed"},
        })
    return {
        "service": "coordinator", "status": "ready",
        "implementation": "foundation",
        "checks": {"database_and_migrations": "passed"},
        "workers": "not_implemented",
    }
