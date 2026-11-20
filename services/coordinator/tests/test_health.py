import asyncio
import json
import logging

import httpx

from app.config import Settings
from app.logging_config import JsonFormatter
from app.main import create_app


def settings():
    return Settings(_env_file=None, database_url="sqlite+aiosqlite:///:memory:")


async def test_liveness_does_not_require_database_and_failure_is_not_ready():
    async def failing():
        raise RuntimeError("postgresql://secret-password")
    app = create_app(settings(), database_probe=failing)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/health")).json()["implementation"] == "foundation"
            response = await client.get("/ready")
            assert response.status_code == 503
            assert "secret-password" not in response.text


async def test_successful_probe_and_request_id():
    async def passing():
        pass
    app = create_app(settings(), database_probe=passing)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/ready", headers={"X-Request-ID": "order-42"})
            assert response.status_code == 200
            assert response.json()["workers"] == "not_implemented"
            assert response.headers["X-Request-ID"] == "order-42"
            response = await client.get("/health", headers={"X-Request-ID": "bad value"})
            assert response.headers["X-Request-ID"] != "bad value"


async def test_readiness_probe_has_a_deadline():
    async def hanging():
        await asyncio.Event().wait()
    config = settings()
    config.dependency_timeout_seconds = 0.01
    app = create_app(config, database_probe=hanging)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/ready")).status_code == 503


async def test_database_and_missing_schema_are_not_ready():
    app = create_app(settings())
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/health")).status_code == 200
            assert (await client.get("/ready")).status_code == 503


def test_json_formatter_emits_only_allowed_context():
    record = logging.LogRecord("coordinator", logging.INFO, "", 1, "completed", (), None)
    record.order_id = "o-1"
    record.database_url = "secret"
    payload = json.loads(JsonFormatter().format(record))
    assert payload["order_id"] == "o-1"
    assert "secret" not in json.dumps(payload)
