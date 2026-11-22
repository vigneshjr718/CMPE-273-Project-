"""Status API responses, validated against contracts/openapi/status-api.yaml."""
from pathlib import Path

import httpx
import jsonschema
import pytest
import yaml

from app.config import Settings
from app.consumers.event_consumer import ConsumerStatus
from app.main import create_app
from tests import mock_events as ev
from tests.conftest import apply_all

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "openapi" / "status-api.yaml"


@pytest.fixture(scope="module")
def contract():
    if not CONTRACT.exists():
        pytest.skip("contracts/openapi/status-api.yaml not found")
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


def validate(contract, schema_name, body):
    schema = {"$schema": "https://json-schema.org/draft/2020-12/schema",
              "components": contract["components"],
              "$ref": f"#/components/schemas/{schema_name}"}
    jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()).validate(body)


@pytest.fixture
async def client(sessions):
    settings = Settings(database_url="sqlite+aiosqlite://", consumer_enabled=False)
    app = create_app(settings)
    # ASGITransport does not run the lifespan; wire the state directly.
    app.state.sessions = sessions

    async def probe():
        return None
    app.state.database_probe = probe
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://tracker") as http:
        yield http


def _events(name):
    events = getattr(ev, name)()
    return events[0] if isinstance(events, tuple) else events


@pytest.mark.parametrize("scenario, order_id, status, flags", [
    ("just_accepted", "ord-accepted", "ACCEPTED",
     dict(pending=True, reconciliation_required=False, replaced=False, terminal=False)),
    ("in_progress", "ord-progress", "PROCESSING",
     dict(pending=True, reconciliation_required=False, replaced=False, terminal=False)),
    ("fulfilled", "ord-fulfilled", "FULFILLED",
     dict(pending=False, reconciliation_required=False, replaced=False, terminal=True)),
    ("replaced", "ord-replaced", "RESERVED",
     dict(pending=True, reconciliation_required=False, replaced=True, terminal=False)),
    ("unfulfilled", "ord-unfulfilled", "UNFULFILLED",
     dict(pending=False, reconciliation_required=False, replaced=False, terminal=True)),
    ("reconciliation_required", "ord-reconcile", "RECONCILIATION_REQUIRED",
     dict(pending=True, reconciliation_required=True, replaced=False, terminal=False)),
])
async def test_scenarios_match_contract(client, projector, contract, scenario, order_id,
                                        status, flags):
    await apply_all(projector, _events(scenario))
    response = await client.get(f"/status/orders/{order_id}")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    validate(contract, "OrderStatus", body)
    assert body["status"] == status
    assert body["flags"] == flags


async def test_in_progress_details(client, projector):
    await apply_all(projector, _events("in_progress"))
    body = (await client.get("/status/orders/ord-progress")).json()
    one, two = body["lines"]
    assert body["state_version"] == 7 and body["created_at"] == "2026-11-23T17:00:00Z"
    assert one["selected_seller"]["seller_id"] == "seller-2"
    assert one["selected_seller"]["estimated_delivery_at"] == "2026-11-24T21:00:00Z"
    assert two["status"] == "RESERVING" and two["attempts"][0]["outcome"] == "IN_PROGRESS"
    assert "operation_key" not in str(body)  # internal keys never leave the tracker


async def test_unfulfilled_has_no_seller(client, projector):
    await apply_all(projector, _events("unfulfilled"))
    line = (await client.get("/status/orders/ord-unfulfilled")).json()["lines"][0]
    assert line["selected_seller"] is None and line["reason"] == "NO_ELIGIBLE_SELLER"


async def test_unknown_order_is_not_yet_visible_not_lost(client, contract):
    response = await client.get("/status/orders/ord-never-seen")
    assert response.status_code == 404
    assert response.headers["retry-after"] == "2"
    body = response.json()
    validate(contract, "Error", body)
    assert body["code"] == "ORDER_NOT_YET_VISIBLE"


async def test_malformed_order_id_is_400(client, contract):
    response = await client.get("/status/orders/bad%20id!")
    assert response.status_code == 400
    validate(contract, "Error", response.json())
    assert response.json()["code"] == "INVALID_ORDER_ID"


async def test_database_failure_is_503(client, sessions, contract):
    class Broken:
        def __call__(self):
            raise OSError("connection refused")
    client._transport.app.state.sessions = Broken()
    response = await client.get("/status/orders/ord-any")
    assert response.status_code == 503
    validate(contract, "Error", response.json())


async def test_health_and_ready(client):
    assert (await client.get("/health")).json() == {"status": "ok", "service": "tracker"}
    ready = await client.get("/ready")
    assert ready.status_code == 200  # consumer disabled in this test app


async def test_ready_fails_when_consumer_not_running(sessions):
    settings = Settings(database_url="sqlite+aiosqlite://", consumer_enabled=True)
    app = create_app(settings)
    app.state.sessions = sessions

    async def probe():
        return None
    app.state.database_probe = probe
    app.state.consumer_status = ConsumerStatus(state="restarting", restarts=3,
                                               last_error_type="KafkaConnectionError")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://tracker") as http:
        response = await http.get("/ready")
    assert response.status_code == 503
    assert response.json()["checks"]["consumer"]["last_error_type"] == "KafkaConnectionError"
