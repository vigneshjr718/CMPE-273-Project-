"""Actual B outbox -> D decoder/projector/view; SQLite, controlled seller, no Kafka.

Set COORDINATOR_TRACKER_SOURCE to a checkout's services/tracker directory.
The optional D process uses a temporary DB and inherits no service credentials.
"""
from copy import deepcopy
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import httpx
from jsonschema import Draft202012Validator, FormatChecker
import pytest
from sqlalchemy import select

from app.models import OutboxEvent
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.reconciliation import Reconciler
from app.saga.replacement import ReplacementCoordinator
from test_delivery_polling import setup, sessions
from test_replacement import prepare

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / "contracts/events/workflow.events.schema.json"
PROBE = Path(__file__).with_name("tracker_projection_probe.py")


async def corpus(sessions, now, scenario):
    if scenario in {"replacement", "cancel_pending", "cancel_no_stock", "cancel_delivery"}:
        c, seller, clock = await prepare(sessions, now)
        if scenario == "cancel_pending":
            seller.mode = "lost_reply"
        elif scenario == "cancel_no_stock":
            seller.quote_mode = "no_stock"
        elif scenario == "cancel_delivery":
            seller.mode = "delivered"
        await ReplacementCoordinator(c, seller).run_order("delivery-test")
    else:
        c, seller, clock = await setup(sessions, now)
        if scenario == "delivered":
            seller.delivered.add("bumper")
        elif scenario == "late":
            seller.eta = now + timedelta(days=3)
        elif scenario == "overdue":
            seller.mode = "error"
            clock.advance(3 * 86400)
        if scenario in {"delivered", "late", "overdue", "in_transit"}:
            await DeliveryPoller(c, seller).run_order("delivery-test")
        elif scenario in {"unknown", "reconciled", "exhausted"}:
            # Simulate interrupted write after durable attempt creation. Persist
            # the uncertainty through B's own transition/event transaction.
            # Use a fresh order whose reservation call loses its response.
            from app.consumers.order_created import accept_order
            from app.schemas import OrderCreatedEvent
            event = OrderCreatedEvent.model_validate({
                "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
                "producer": "order", "occurred_at": now,
                "payload": {"order_id": "uncertain-test", "lines": [{
                    "line_id": "line-1", "part_id": "bumper", "quantity": 1,
                    "needed_by": now + timedelta(days=2)}]}})
            await accept_order(event, sessions, workflow_topic="workflow.events")
            reserve = seller.reserve
            async def lose_reply(*args):
                await reserve(*args)
                raise httpx.ReadError("controlled lost reservation reply")
            seller.reserve = lose_reply
            await c.run_order("uncertain-test")
            if scenario == "reconciled":
                async def lookup(seller_id, key):
                    return next(r for r in seller.records.values() if r.operation_key == key)
                seller.lookup = lookup
                await Reconciler(c).run_order("uncertain-test")
            elif scenario == "exhausted":
                async def unavailable(*args):
                    raise httpx.ReadError("controlled unavailable lookup")
                seller.lookup = unavailable
                await Reconciler(c, max_cycles=1).run_order("uncertain-test")
            return await exported(c, sessions, "uncertain-test")
    return await exported(c, sessions, "delivery-test")


async def exported(c, sessions, order_id):
    async with sessions() as session:
        events = list(await session.scalars(select(OutboxEvent).where(
            OutboxEvent.order_id == order_id).order_by(OutboxEvent.state_version)))
    return {"order_id": order_id, "events": [e.payload for e in events],
            "expected": await c.snapshot(order_id)}


SCENARIOS = ["reserved", "in_transit", "delivered", "late", "overdue", "unknown",
             "reconciled", "exhausted", "replacement", "cancel_pending",
             "cancel_no_stock", "cancel_delivery"]


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_real_outbox_matches_published_schema(sessions, now, scenario):
    data = await corpus(sessions, now, scenario)
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=FormatChecker())
    for event in data["events"]:
        validator.validate(event)


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_real_outbox_projects_into_tracker(sessions, now, scenario):
    path = os.getenv("COORDINATOR_TRACKER_SOURCE")
    if not path:
        pytest.skip("set COORDINATOR_TRACKER_SOURCE to services/tracker checkout")
    tracker = Path(path).resolve()
    assert (tracker / "app/projection.py").is_file(), "not a tracker checkout"
    data = await corpus(sessions, now, scenario)
    messages = [{"topic": "workflow.events", "event": e} for e in data["events"]]
    final_index = len(messages) - 1
    # Late A creation must enrich immutable input without resetting B progress.
    creation = {"event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
                "producer": "order", "occurred_at": now.isoformat(), "payload": {
                    "order_id": data["order_id"], "lines": [{"line_id": "line-1",
                    "part_id": "bumper", "quantity": 1,
                    "needed_by": (now + timedelta(days=2)).isoformat()}]}}
    stale = deepcopy(data["events"][1])
    stale["event_id"] = str(uuid4())
    messages += [{"topic": "workflow.events", "event": data["events"][-1]},
                 {"topic": "workflow.events", "event": stale},
                 {"topic": "order.created", "event": creation}]
    env = {k: v for k, v in os.environ.items() if not k.startswith(("COORDINATOR_", "TRACKER_"))}
    env["PYTHONPATH"] = str(tracker)
    result = subprocess.run([sys.executable, str(PROBE)], cwd=tracker, env=env,
                            input=json.dumps({**data, "messages": messages}),
                            text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr
    projected = json.loads(result.stdout)
    evidence_dir = os.getenv("COORDINATOR_TRACKER_EVIDENCE_DIR")
    if evidence_dir:
        evidence = Path(evidence_dir)
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / f"{scenario}.json").write_text(json.dumps({
            "scenario": scenario, "coordinator": data, "tracker": projected}, indent=2) + "\n")
    assert projected["outcomes"][-3:] == ["duplicate", "stale", "applied"]
    assert projected["history"] == len(data["events"]) + 1  # A creation audit
    for index in (final_index, final_index + 1, final_index + 2, final_index + 3):
        view = projected["views"][index]
        assert (view["status"], view["state_version"]) == (
            data["expected"]["status"], data["expected"]["state_version"])
        line, expected = view["lines"][0], data["expected"]["lines"][0]
        assert (line["status"], line["attempt_count"]) == (expected["status"], expected["attempt_count"])
        assert line["reason"] == expected["reason"]
        current = line["selected_seller"]
        if current:
            assert current["attempt_id"] == expected["attempt_id"]
            assert current["seller_id"] == expected["seller_id"]
        assert view["flags"]["terminal"] == (expected["status"] in {"DELIVERED", "UNFULFILLED"})
    assert projected["views"][-1]["order_created_seen"] is True


def test_event_catalog_matches_producer_literals():
    import ast
    import re
    catalog = set(json.loads(SCHEMA.read_text())["properties"]["event_type"]["enum"])
    emitted = set()
    for path in (ROOT / "services/coordinator/app").rglob("*.py"):
        if "tools" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value != "workflow.events" and re.fullmatch(
                    r"(?:workflow|line)\.[a-z_]+|reconciliation\.required", node.value
                ):
                    emitted.add(node.value)
    assert emitted == catalog
