"""Concurrency behaviour that only a real PostgreSQL can show (row locks, unique-key races).

Runs only with TRACKER_TEST_DATABASE_URL=postgresql+asyncpg://... (SQLite serializes writers).
"""
import asyncio
import os

import pytest
from sqlalchemy import func, select

from app.models import LineStatus, ProcessedEvent, StatusHistory
from app.projection import Outcome
from app.views import load_order_view
from tests import mock_events as ev
from tests.conftest import make_projector

pytestmark = pytest.mark.skipif(
    not os.environ.get("TRACKER_TEST_DATABASE_URL", "").startswith("postgresql"),
    reason="needs TRACKER_TEST_DATABASE_URL pointing at PostgreSQL")


async def test_same_event_delivered_concurrently_applies_once(sessions):
    topic, raw = ev.order_created("ord-race-dup")
    # Separate projectors = separate tracker replicas racing on one event.
    outcomes = await asyncio.gather(*[
        make_projector(sessions).handle(topic, 0, 0, raw) for _ in range(6)])
    assert sorted(outcomes) == sorted([Outcome.APPLIED] + [Outcome.DUPLICATE] * 5)
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(StatusHistory)) == 1
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


async def test_creation_and_workflow_race_on_new_order(sessions):
    """order.created and workflow.events are separate topics, so two consumers can create
    the same order row at the same moment. Both events must land; neither may be lost."""
    for attempt in range(10):  # repeat to hit different interleavings
        order_id = f"ord-race-{attempt}"
        wf = ev.Workflow(order_id)
        creation, accepted = ev.order_created(order_id), wf.accepted()
        outcomes = await asyncio.gather(
            make_projector(sessions).handle(*creation[:1], 0, 0, creation[1]),
            make_projector(sessions).handle(*accepted[:1], 1, 0, accepted[1]))
        assert outcomes == [Outcome.APPLIED, Outcome.APPLIED]
        async with sessions() as session:
            order = await load_order_view(session, order_id)
            lines = list(await session.scalars(
                select(LineStatus).where(LineStatus.order_id == order_id)))
        assert order.order_created_seen and order.state_version == 1
        assert order.status == "PENDING"
        assert {l.line_id: (l.status, l.creation_source) for l in lines} == {
            "line-1": ("PENDING", "order"), "line-2": ("PENDING", "order")}


async def test_concurrent_versions_for_one_order_never_regress(sessions):
    """Two replicas handed v2 and v3 of one order at once: the lock serializes them;
    whichever order they land in, the final version is 3 and v2 is never applied after it."""
    wf = ev.Workflow("ord-race-ver")
    projector = make_projector(sessions)
    for topic, raw in [ev.order_created("ord-race-ver"), wf.accepted()]:
        await projector.handle(topic, 0, 0, raw)
    v2 = wf.started()
    v3 = wf.line("line.quoting", "line-1", "QUOTING", "PROCESSING")
    outcomes = await asyncio.gather(
        make_projector(sessions).handle(*v3[:1], 0, 3, v3[1]),
        make_projector(sessions).handle(*v2[:1], 1, 2, v2[1]))
    assert Outcome.APPLIED in outcomes
    async with sessions() as session:
        order = await load_order_view(session, "ord-race-ver")
    assert order.state_version == 3
    assert order.lines[0].status == "QUOTING"
