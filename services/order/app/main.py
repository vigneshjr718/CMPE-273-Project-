import json
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import select

from app.config import Settings
from app.db import create_tables, make_engine, make_sessions
from app.models import Order, OrderLine
from app.outbox import publish_pending
from app.schemas import OrderCreateRequest, StoredOrder
from app.service import accept_order, as_stored


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    sessions = make_sessions(engine)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await create_tables(engine)
        app.state.sessions = sessions
        app.state.settings = settings
        relay_task = None
        if settings.kafka_enabled:
            async def relay_loop():
                while True:
                    try:
                        await publish_pending(sessions, settings.kafka_bootstrap_servers, True)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        # Keep the API available during broker outages. Rows stay pending.
                        pass
                    await asyncio.sleep(2)
            relay_task = asyncio.create_task(relay_loop())
        yield
        if relay_task:
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass
        await engine.dispose()

    app = FastAPI(title="PartStream Order Service", version="1.0.0", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"service": "order", "status": "alive"}

    @app.get("/ready")
    async def ready(request: Request):
        try:
            async with sessions() as session:
                await session.execute(select(Order).limit(1))
            return {"service": "order", "status": "ready"}
        except Exception:
            return JSONResponse(status_code=503, content={"service": "order", "status": "not_ready"})

    @app.get("/")
    async def landing():
        return FileResponse(Path(__file__).parents[1] / "web" / "index.html")

    @app.post("/orders", response_model=StoredOrder, status_code=202)
    async def create_order(request: OrderCreateRequest, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
        async with sessions.begin() as session:
            result, replay = await accept_order(session, request, idempotency_key or "", settings.created_topic)
        return result

    @app.get("/orders/{order_id}", response_model=StoredOrder)
    async def get_order(order_id: str):
        async with sessions() as session:
            order = await session.get(Order, order_id)
            if not order:
                raise HTTPException(404, "order not found")
            lines = list((await session.scalars(select(OrderLine).where(OrderLine.order_id == order_id))).all())
            return as_stored(order, lines)

    return app


app = create_app()

