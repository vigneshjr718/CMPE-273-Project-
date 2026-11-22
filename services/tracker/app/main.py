import asyncio
import logging
import re
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request

from app.api import health, status
from app.config import Settings
from app.consumers.event_consumer import ConsumerStatus, KafkaEventSource, supervise
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging
from app.projection import Projector

REQUEST_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
logger = logging.getLogger("tracker.http")


def create_app(settings: Settings | None = None, source_factory=None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = make_engine(settings.database_url.get_secret_value())
        sessions = make_sessions(engine)
        app.state.sessions = sessions
        app.state.database_probe = lambda: check_database(engine)
        stop, task = asyncio.Event(), None
        if settings.consumer_enabled:
            projector = Projector(sessions, order_topic=settings.order_topic,
                                  workflow_topic=settings.workflow_topic)
            factory = source_factory or (lambda: KafkaEventSource(settings))
            task = asyncio.create_task(supervise(
                factory, projector, status=app.state.consumer_status, stop=stop,
                skip_invalid=settings.skip_invalid_events,
                backoff=settings.consumer_restart_backoff_seconds,
                backoff_max=settings.consumer_restart_backoff_max_seconds))
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                try:
                    await asyncio.wait_for(task, timeout=10)
                except TimeoutError:
                    task.cancel()
            await engine.dispose()

    app = FastAPI(title="PartStream Status API (tracker)", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.consumer_status = ConsumerStatus()

    @app.middleware("http")
    async def request_logging(request: Request, call_next):
        supplied = request.headers.get("X-Request-ID", "")
        request_id = supplied if REQUEST_ID.fullmatch(supplied) else str(uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        logger.info("http_request_completed", extra={
            "request_id": request_id, "status_code": response.status_code})
        return response

    app.include_router(health.router)
    app.include_router(status.router)
    return app
