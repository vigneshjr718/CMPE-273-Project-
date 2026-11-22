"""Projection rules: dedupe, no regression, producer authority, no timestamp ordering."""
import json
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.events import InvalidEvent
from app.models import ProcessedEvent, StatusHistory
from app.projection import Outcome
from app.views import load_order_view
from tests import mock_events as ev
from tests.conftest import apply_all, snapshot


async def view(sessions, order_id):
    async with sessions() as session:
        return await load_order_view(session, order_id)


async def count(sessions, model):
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(model))


# ---------------------------------------------------------------- duplicates
async def test_duplicate_event_has_single_effect(sessions, projector):
    events, _ = ev.in_progress()
    await apply_all(projector, events)
    before = await snapshot(sessions)

    # Redeliver every event (same event_id + payload), as an outbox replay would.
    outcomes = await apply_all(projector, events)

    assert outcomes == [Outcome.DUPLICATE] * len(events)
    assert await snapshot(sessions) == before
    assert await count(sessions, StatusHistory) == len(events)
    assert await count(sessions, ProcessedEvent) == len(events)


async def test_duplicate_creation_with_new_event_id_does_not_reset(sessions, projector):
    events, _ = ev.in_progress("ord-dup-create")
    await apply_all(projector, events)
    again = ev.order_created("ord-dup-create")  # same order, NEW event_id

    assert await projector.handle(*again[:1], 0, 99, again[1]) == Outcome.DUPLICATE_CREATION
    order = await view(sessions, "ord-dup-create")
    assert order.status == "PROCESSING" and order.state_version == 7


# ---------------------------------------------------------------- stale / ordering
async def test_stale_workflow_event_is_ignored(sessions, projector):
    wf = ev.Workflow("ord-stale")
    accepted, started = wf.accepted(), wf.started()
    reserving = wf.reserving("line-1", "att-1", "seller-2")         # v3
    reserved = wf.line("line.reserved", "line-1", "RESERVED", "PROCESSING",
                       attempt_id="att-1", seller_id="seller-2")     # v4
    await apply_all(projector, [ev.order_created("ord-stale"), accepted, started, reserving, reserved])
    before = await snapshot(sessions)

    # Distinct event_id but an OLD version (e.g. a lagging relay) must not regress state.
    old = wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING", version=2)
    assert await projector.handle(*old[:1], 0, 50, old[1]) == Outcome.STALE

    order = await view(sessions, "ord-stale")
    assert order.state_version == 4
    assert order.lines[0].status == "RESERVED"
    after = await snapshot(sessions)
    assert after["orders"] == before["orders"] and after["lines"] == before["lines"]
    assert after["history"] == before["history"]  # stale events are not history


async def test_out_of_order_delivery_keeps_newest_state(sessions, projector):
    wf = ev.Workflow("ord-ooo")
    v1, v2 = wf.accepted(), wf.started()
    v3 = wf.reserving("line-1", "att-1", "seller-2")
    v4 = wf.line("line.reserved", "line-1", "RESERVED", "PROCESSING", attempt_id="att-1",
                 seller_id="seller-2")
    outcomes = await apply_all(projector, [ev.order_created("ord-ooo"), v1, v2, v4, v3])
    assert outcomes[-2:] == [Outcome.APPLIED, Outcome.STALE]
    order = await view(sessions, "ord-ooo")
    assert order.state_version == 4 and order.lines[0].status == "RESERVED"


async def test_version_not_timestamp_decides(sessions, projector):
    wf = ev.Workflow("ord-clock")
    await apply_all(projector, [ev.order_created("ord-clock"), wf.accepted()])
    # Newer version with an OLDER timestamp is applied ...
    wf.clock -= timedelta(hours=5)
    newer = wf.started()
    assert await projector.handle(*newer[:1], 0, 10, newer[1]) == Outcome.APPLIED
    # ... and an older version with a NEWER timestamp is not.
    wf.clock += timedelta(days=1)
    older = wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING", version=1)
    assert await projector.handle(*older[:1], 0, 11, older[1]) == Outcome.STALE
    assert (await view(sessions, "ord-clock")).status == "PROCESSING"


async def test_late_order_created_does_not_regress_coordinator_state(sessions, projector):
    """Topics are not ordered with each other: B's events may arrive before A's creation."""
    events, _ = ev.in_progress("ord-late-create")
    creation, workflow = events[0], events[1:]
    await apply_all(projector, workflow)
    early = await view(sessions, "ord-late-create")
    assert early.order_created_seen is False and early.created_at is None
    assert early.status == "PROCESSING" and early.state_version == 7

    assert await projector.handle(*creation[:1], 0, 0, creation[1]) == Outcome.APPLIED
    order = await view(sessions, "ord-late-create")
    assert order.order_created_seen is True and order.created_at is not None
    assert order.status == "PROCESSING" and order.state_version == 7
    assert [l.status for l in order.lines] == ["RESERVED", "RESERVING"]


# ---------------------------------------------------------------- producer authority
@pytest.mark.parametrize("make", [
    # A coordinator event on the order topic.
    lambda: (ev.ORDER_TOPIC, ev.Workflow("ord-auth").started()[1]),
    # order.created from the wrong producer.
    lambda: ev.order_created("ord-auth", producer="coordinator"),
    # A creation event published on the workflow topic.
    lambda: (ev.WORKFLOW_TOPIC, ev.order_created("ord-auth")[1]),
    # A workflow event claiming to come from the order service.
    lambda: ev.Workflow("ord-auth").emit("workflow.started", {"status": "FULFILLED"},
                                         producer="order"),
])
async def test_unauthorized_producer_is_recorded_but_ignored(sessions, projector, make):
    topic, raw = make()
    assert await projector.handle(topic, 0, 0, raw) == Outcome.REJECTED_AUTHORITY
    assert await view(sessions, "ord-auth") is None
    async with sessions() as session:
        record = await session.get(ProcessedEvent, json.loads(raw)["event_id"])
    assert record.outcome == "rejected_authority"


# ---------------------------------------------------------------- invalid input
@pytest.mark.parametrize("raw", [
    b"not json", b"[]", None,
    json.dumps({"event_id": "e1", "event_type": "order.created", "schema_version": 1,
                "producer": "order", "occurred_at": "2026-11-23T17:00:00",  # no timezone
                "payload": {"order_id": "o1", "lines": ev.DEFAULT_LINES}}).encode(),
    json.dumps({"event_id": "e2", "event_type": "order.created", "schema_version": 1,
                "producer": "order", "occurred_at": "2026-11-23T17:00:00Z",
                "payload": {"order_id": "o1", "lines": []}}).encode(),
])
async def test_invalid_events_raise_and_change_nothing(sessions, projector, raw):
    with pytest.raises(InvalidEvent):
        await projector.handle(ev.ORDER_TOPIC, 0, 0, raw)
    assert await count(sessions, ProcessedEvent) == 0


async def test_coordinator_event_without_version_is_invalid(projector):
    topic, raw = ev.Workflow("ord-nov").started()
    data = json.loads(raw)
    del data["state_version"]
    with pytest.raises(InvalidEvent):
        await projector.handle(topic, 0, 0, json.dumps(data).encode())


async def test_top_level_order_id_variant_is_accepted(sessions, projector):
    topic, raw = ev.order_created("ord-top", top_level_order_id=True)
    assert await projector.handle(topic, 0, 0, raw) == Outcome.APPLIED
    assert (await view(sessions, "ord-top")).status == "ACCEPTED"


# ---------------------------------------------------------------- scenario results
async def test_replacement_history(sessions, projector):
    await apply_all(projector, ev.replaced())
    line = (await view(sessions, "ord-replaced")).lines[0]
    assert line.status == "RESERVED" and line.reason is None
    assert line.attempt_count == 2
    assert [(a.seller_id, a.outcome) for a in line.attempts] == [
        ("seller-3", "CANCELLED"), ("seller-1", "RESERVED")]
    assert line.attempts[0].reason == "DELIVERY_AFTER_DEADLINE"
    assert line.selected_seller.seller_id == "seller-1"
    assert line.flags.replaced and not line.flags.reconciliation_required
