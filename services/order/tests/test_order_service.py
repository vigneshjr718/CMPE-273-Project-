from datetime import datetime, timezone, timedelta
import pytest
from httpx import ASGITransport, AsyncClient
from app.config import Settings
from app.main import create_app

@pytest.fixture
def app(tmp_path):
    return create_app(Settings(_env_file=None, database_url=f"sqlite+aiosqlite:///{tmp_path / 'order.db'}"))

def body():
    return {"lines": [{"part_id": "bumper", "quantity": 1,
             "needed_by": (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()}]}

async def test_order_is_accepted_and_duplicate_is_replayed(app):
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            request_body = body()
            first = await client.post("/orders", json=request_body, headers={"Idempotency-Key": "key-1"})
            second = await client.post("/orders", json=request_body, headers={"Idempotency-Key": "key-1"})
            assert first.status_code == second.status_code == 202
            assert first.json()["order_id"] == second.json()["order_id"]
            assert len(first.json()["lines"]) == 1

async def test_conflicting_reuse_is_rejected(app):
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            await client.post("/orders", json=body(), headers={"Idempotency-Key": "key-2"})
            changed = {"lines": [{"part_id": "hood", "quantity": 1,
                       "needed_by": (datetime.now(timezone.utc) + timedelta(days=5)).isoformat()}]}
            response = await client.post("/orders", json=changed, headers={"Idempotency-Key": "key-2"})
            assert response.status_code == 409

async def test_validation_and_health(app):
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/health")).json()["status"] == "alive"
            assert (await client.post("/orders", json={"lines": []}, headers={"Idempotency-Key": "x"})).status_code == 422
            assert (await client.post("/orders", json=body())).status_code == 400

