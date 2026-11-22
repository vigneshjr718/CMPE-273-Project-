import logging
import re
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request

from app.api.health import router
from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging

REQUEST_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
logger = logging.getLogger("coordinator.http")


def create_app(settings: Settings | None = None, database_probe=None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings.database_url.get_secret_value())
        app.state.sessions = make_sessions(engine)

        async def probe():
            await check_database(engine)

        app.state.database_probe = database_probe or probe
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="PartStream Coordinator - foundation", version="0.1.0",
                  lifespan=lifespan)
    app.state.settings = settings

    @app.middleware("http")
    async def request_logging(request: Request, call_next):
        supplied = request.headers.get("X-Request-ID", "")
        request_id = supplied if REQUEST_ID.fullmatch(supplied) else str(uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        logger.info("http_request_completed", extra={
            "request_id": request_id, "status_code": response.status_code,
        })
        return response

    app.include_router(router)
    return app


app = create_app()
