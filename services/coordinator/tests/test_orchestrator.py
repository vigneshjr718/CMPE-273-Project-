import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select, event as sql_event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.clients.reservations import ReservationReply, HttpReservationClient
from app.consumers.order_created import accept_order
from app.models import Base, LineAttempt, OutboxEvent, Workflow, WorkflowLine
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.schemas import OrderCreatedEvent, Quote


class LocalTestLocks:
    """Test only. Production injects PostgreSQL session advisory locks."""
    def __init__(self):
        self.locks = {}

    @asynccontextmanager
    async def __call__(self, order_id):
        async with self.locks.setdefault(order_id, asyncio.Lock()):
            yield


@pytest_asyncio.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'flow.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


@pytest.fixture
def event(line, now):
    return OrderCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
        "producer": "order", "occurred_at": now,
        "payload": {"order_id": "o-flow", "lines": [line.model_dump()]},
    })


class Sellers:
    def __init__(self, now, sessions):
        self.now, self.sessions = now, sessions
        self.calls = []
        self.mode = "normal"
        self.reject = set()
        self.unavailable = set()
        self.empty = False
        self.crash_after_commit = False

    async def quote(self, seller_id, line):
        if seller_id in self.unavailable:
            raise httpx.ConnectError("not available")
        return Quote(
            seller_id=seller_id, part_id=line.part_id,
            available_quantity=0 if self.empty else 10,
            estimated_delivery_at=self.now + timedelta(hours=int(seller_id[-1])),
        )

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.calls.append((seller_id, operation_key))
        # A new independent DB session must see the attempt BEFORE the call.
        async with self.sessions() as session:
            attempt = (await session.scalars(select(LineAttempt).where(
                LineAttempt.operation_key == operation_key))).one()
            assert attempt.status == "RESERVING"
            row = await session.get(WorkflowLine, (order_id, line.line_id))
            assert row.current_attempt_id == attempt.attempt_id
        if self.mode == "timeout":
            await asyncio.Event().wait()
        if self.mode == "network_error":
            raise httpx.ReadError("reply lost")
        if self.mode == "crash":
            raise RuntimeError("process interrupted after remote side effect")
        rejected = seller_id in self.reject
        return ReservationReply(
            seller_id=seller_id,
            operation_key="wrong-key" if self.mode == "wrong_key" else operation_key,
            part_id=line.part_id, quantity=line.quantity,
            status="REJECTED" if rejected else "RESERVED",
            reservation_id=None if rejected else "reservation-" + seller_id,
            estimated_delivery_at=None if rejected else (
                self.now + timedelta(days=3 if self.mode == "late_eta" else 1)
            ),
        )


def make_coordinator(sessions, sellers, now, locks=None):
    return Coordinator(
        sessions, sellers, ["s1", "s2", "s3"], lock=locks or LocalTestLocks(),
        workflow_topic="workflow.events", timeout_seconds=0.1,
        concurrency=3, clock=lambda: now,
    )


async def accept(event, sessions):
    await accept_order(event, sessions, workflow_topic="workflow.events")


async def inspect_rows(sessions):
    async with sessions() as session:
        attempts = list(await session.scalars(
            select(LineAttempt).order_by(LineAttempt.attempt_number)))
        events = list(await session.scalars(
            select(OutboxEvent).order_by(OutboxEvent.state_version)))
        return attempts, events


async def test_normal_flow_persists_attempt_before_call_and_does_not_repeat(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    coordinator = make_coordinator(sessions, sellers, now)
    first = await coordinator.run_order("o-flow")
    assert first["status"] == "RESERVED"
    assert first["lines"][0]["seller_id"] == "s1"
    assert first["lines"][0]["reservation_id"] == "reservation-s1"
    assert len(sellers.calls) == 1
    attempts, events = await inspect_rows(sessions)
    assert len(attempts) == 1 and attempts[0].status == "RESERVED"
    assert [e.state_version for e in events] == [1, 2, 3, 4, 5]
    assert events[-1].event_type == "line.reserved"
    assert all(e.published_at is None for e in events)
    second = await coordinator.run_order("o-flow")
    assert second == first
    assert len(sellers.calls) == 1
    assert len((await inspect_rows(sessions))[1]) == 5


async def test_definitive_rejection_can_try_another_seller(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.reject.add("s1")
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "RESERVED"
    assert result["lines"][0]["seller_id"] == "s2"
    assert len(sellers.calls) == 2
    assert sellers.calls[0][1] != sellers.calls[1][1]
    attempts, _ = await inspect_rows(sessions)
    assert [a.status for a in attempts] == ["REJECTED", "RESERVED"]


@pytest.mark.parametrize("mode", ["timeout", "network_error", "wrong_key"])
async def test_unknown_outcome_never_switches_seller(sessions, event, now, mode):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.mode = mode
    coordinator = make_coordinator(sessions, sellers, now)
    first = await coordinator.run_order("o-flow")
    assert first["status"] == "RECONCILIATION_REQUIRED"
    assert first["lines"][0]["status"] == "UNKNOWN"
    assert len(sellers.calls) == 1
    assert (await inspect_rows(sessions))[0][0].status == "UNKNOWN"
    # Re-running does not silently reset uncertainty or reserve elsewhere.
    await coordinator.run_order("o-flow")
    assert len(sellers.calls) == 1


async def test_no_stock_without_failed_quotes_is_unfulfilled(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.empty = True
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "UNFULFILLED"
    assert not sellers.calls
    assert not (await inspect_rows(sessions))[0]


async def test_failed_quotes_are_not_misrepresented_as_no_stock(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.empty = True
    sellers.unavailable.add("s1")
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "RECONCILIATION_REQUIRED"
    assert result["lines"][0]["reason"] == "QUOTES_UNAVAILABLE"
    assert not sellers.calls


async def test_crash_after_attempt_commit_is_unknown_on_resume(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.mode = "crash"
    with pytest.raises(RuntimeError, match="interrupted"):
        await make_coordinator(sessions, sellers, now).run_order("o-flow")
    attempts, _ = await inspect_rows(sessions)
    original_key = attempts[0].operation_key
    assert attempts[0].status == "RESERVING"
    sellers.mode = "normal"
    # A new coordinator sees the saved in-flight operation; it cannot assume failure.
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["lines"][0]["status"] == "UNKNOWN"
    assert result["lines"][0]["operation_key"] == original_key
    assert len(sellers.calls) == 1


async def test_late_confirmed_reservation_is_visible_not_falsely_fulfilled(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    sellers.mode = "late_eta"
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "RECONCILIATION_REQUIRED"
    assert result["lines"][0]["status"] == "RESERVED"
    assert result["lines"][0]["reason"] == "DELIVERY_AFTER_DEADLINE"


async def test_mixed_stock_produces_an_explicit_partial_order(sessions, event, now):
    data = event.model_dump(mode="json")
    second = {**data["payload"]["lines"][0], "line_id": "line-2", "part_id": "hood"}
    data["payload"]["lines"].append(second)
    event = OrderCreatedEvent.model_validate(data)
    await accept(event, sessions)
    class SplitSellers(Sellers):
        async def quote(self, seller_id, line):
            quote = await super().quote(seller_id, line)
            return quote.model_copy(update={"available_quantity":
                                           0 if line.part_id == "hood" else 10})
    sellers = SplitSellers(now, sessions)
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "PARTIALLY_RESERVED"
    assert [r["status"] for r in result["lines"]] == ["RESERVED", "UNFULFILLED"]


async def test_different_lines_are_reserved_from_different_sellers(sessions, event, now):
    data = event.model_dump(mode="json")
    data["payload"]["lines"].append({
        **data["payload"]["lines"][0], "line_id": "line-2", "part_id": "hood",
    })
    await accept(OrderCreatedEvent.model_validate(data), sessions)
    class SplitSellers(Sellers):
        async def quote(self, seller_id, line):
            quote = await super().quote(seller_id, line)
            eligible = seller_id == ("s1" if line.part_id == "bumper" else "s2")
            return quote.model_copy(update={"available_quantity": 10 if eligible else 0})
    sellers = SplitSellers(now, sessions)
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["status"] == "RESERVED"
    assert [(r["line_id"], r["seller_id"]) for r in result["lines"]] == [
        ("line-1", "s1"), ("line-2", "s2"),
    ]
    attempts, events = await inspect_rows(sessions)
    assert len(attempts) == 2
    assert [e.state_version for e in events] == list(range(1, 9))


async def test_test_lock_serializes_two_coordinators(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    locks = LocalTestLocks()
    a = make_coordinator(sessions, sellers, now, locks)
    b = make_coordinator(sessions, sellers, now, locks)
    results = await asyncio.gather(a.run_order("o-flow"), b.run_order("o-flow"))
    assert all(r["status"] == "RESERVED" for r in results)
    assert len(sellers.calls) == 1


async def test_transition_outbox_failure_rolls_back_confirmation(sessions, event, now):
    await accept(event, sessions)
    sellers = Sellers(now, sessions)
    def fail_confirmation(session, *_):
        if any(isinstance(r, OutboxEvent) and r.event_type == "line.reserved"
               for r in session.new):
            raise RuntimeError("cannot save confirmation event")
    sql_event.listen(Session, "before_flush", fail_confirmation)
    try:
        with pytest.raises(RuntimeError, match="confirmation"):
            await make_coordinator(sessions, sellers, now).run_order("o-flow")
    finally:
        sql_event.remove(Session, "before_flush", fail_confirmation)
    attempts, events = await inspect_rows(sessions)
    assert attempts[0].status == "RESERVING"
    assert events[-1].event_type == "line.reserving"
    # The side effect may exist but no confirmation committed; next run preserves uncertainty.
    result = await make_coordinator(sessions, sellers, now).run_order("o-flow")
    assert result["lines"][0]["status"] == "UNKNOWN"
    assert len(sellers.calls) == 1


async def test_production_lock_rejects_sqlite(sessions):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        with pytest.raises(RuntimeError, match="PostgreSQL"):
            async with postgres_order_lock(engine, "o1"):
                pytest.fail("unsafe production fallback")
    finally:
        await engine.dispose()


async def test_http_reservation_adapter_sends_idempotency_and_accepts_explicit_rejection(line):
    def respond(request):
        assert request.headers["Idempotency-Key"] == "reserve-key"
        assert str(request.url) == "http://s1/reservations"
        return httpx.Response(409, json={
            "seller_id": "s1", "operation_key": "reserve-key",
            "part_id": line.part_id, "quantity": line.quantity, "status": "REJECTED",
        })
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = HttpReservationClient(http, {"s1": "http://s1"})
        reply = await client.reserve("s1", "o1", line, "reserve-key")
        assert reply.status == "REJECTED"


async def test_http_conflict_without_no_effect_evidence_remains_ambiguous(line):
    def respond(request):
        return httpx.Response(409, json={"detail": "something happened"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(ValueError):
            await HttpReservationClient(http, {"s1": "http://s1"}).reserve(
                "s1", "o1", line, "key")
