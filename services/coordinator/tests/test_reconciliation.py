import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event as sql_event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.clients.reconciliation import HttpReconciliationClient
from app.clients.reservations import ReservationReply
from app.consumers.order_created import accept_order
from app.models import Base, LineAttempt, OutboxEvent
from app.saga.orchestrator import Coordinator
from app.saga.reconciliation import Reconciler
from app.schemas import OrderCreatedEvent, Quote


class Clock:
    def __init__(self, now):
        self.value = now
    def __call__(self):
        return self.value
    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class Locks:
    def __init__(self):
        self.items = {}
    @asynccontextmanager
    async def __call__(self, order_id):
        async with self.items.setdefault(order_id, asyncio.Lock()):
            yield


@pytest_asyncio.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'reconcile.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


class Seller:
    def __init__(self, clock):
        self.clock = clock
        self.records = {}
        self.writes = []
        self.lookups = []
        self.lose_reply = True
        self.lookup_mode = "normal"
        self.write_mode = "normal"

    async def quote(self, seller_id, line):
        return Quote(seller_id=seller_id, part_id=line.part_id,
                     available_quantity=5,
                     estimated_delivery_at=self.clock() + timedelta(hours=1))

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.writes.append((seller_id, operation_key))
        if self.write_mode == "timeout":
            await asyncio.Event().wait()
        reply = self.records.get(operation_key)
        if reply is None:
            reply = ReservationReply(
                seller_id=seller_id, operation_key=operation_key,
                part_id=line.part_id, quantity=line.quantity, status="RESERVED",
                reservation_id="reservation-1",
                estimated_delivery_at=self.clock() + timedelta(hours=1),
            )
            self.records[operation_key] = reply
        if self.lose_reply:
            self.lose_reply = False
            raise httpx.ReadError("response lost after remote commit")
        return reply

    async def lookup(self, seller_id, operation_key):
        self.lookups.append((seller_id, operation_key))
        if self.lookup_mode == "error":
            raise httpx.ConnectError("seller unreachable")
        if self.lookup_mode == "slow_error":
            self.clock.advance(5)
            raise httpx.ReadError("lookup failed after consuming time")
        if self.lookup_mode == "crash":
            raise RuntimeError("reconciliation process interrupted")
        if self.lookup_mode == "not_found":
            return None
        if self.lookup_mode == "rejected":
            return ReservationReply(seller_id=seller_id, operation_key=operation_key,
                                    part_id="bumper", quantity=2, status="REJECTED")
        reply = self.records.get(operation_key)
        if self.lookup_mode == "wrong_key":
            return reply.model_copy(update={"operation_key": "another-key"})
        return reply


async def setup(sessions, line, now):
    event = OrderCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "order.created",
        "schema_version": 1, "producer": "order", "occurred_at": now,
        "payload": {"order_id": "o-reconcile", "lines": [line.model_dump()]},
    })
    await accept_order(event, sessions, workflow_topic="workflow.events")
    clock = Clock(now)
    seller = Seller(clock)
    coordinator = Coordinator(
        sessions, seller, ["s1", "s2"], lock=Locks(),
        workflow_topic="workflow.events", timeout_seconds=0.03, clock=clock,
    )
    return coordinator, seller, clock


async def unknown(sessions, line, now, *, timeout=False):
    c, seller, clock = await setup(sessions, line, now)
    if timeout:
        seller.write_mode = "timeout"
    initial = await c.run_order("o-reconcile")
    assert initial["lines"][0]["status"] == "UNKNOWN"
    return c, seller, clock, initial


async def rows(sessions):
    async with sessions() as session:
        attempts = list(await session.scalars(select(LineAttempt)))
        events = list(await session.scalars(
            select(OutboxEvent).order_by(OutboxEvent.state_version)))
        return attempts, events


async def test_lookup_recovers_lost_reply_without_another_write(sessions, line, now):
    c, seller, _, before = await unknown(sessions, line, now)
    after = await Reconciler(c).run_order("o-reconcile")
    assert after["status"] == "RESERVED"
    assert after["state_version"] == 7
    assert after["lines"][0]["attempt_id"] == before["lines"][0]["attempt_id"]
    assert after["lines"][0]["operation_key"] == before["lines"][0]["operation_key"]
    assert after["lines"][0]["retry_count"] == 1
    assert after["lines"][0]["next_retry_at"] is None
    assert len(seller.writes) == len(seller.lookups) == len(seller.records) == 1
    attempts, events = await rows(sessions)
    assert len(attempts) == 1
    assert [e.state_version for e in events] == list(range(1, 8))
    assert await Reconciler(c).run_order("o-reconcile") == after
    assert len(seller.lookups) == 1


async def test_stale_not_found_replays_original_key_without_duplicate_effect(sessions, line, now):
    c, seller, _, before = await unknown(sessions, line, now)
    seller.lookup_mode = "not_found"
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["status"] == "RESERVED"
    assert seller.writes == [seller.writes[0], seller.writes[0]]
    assert len(seller.records) == 1
    assert result["lines"][0]["operation_key"] == before["lines"][0]["operation_key"]
    assert len((await rows(sessions))[0]) == 1


async def test_not_yet_created_reservation_reuses_saved_attempt(sessions, line, now):
    c, seller, _, before = await unknown(sessions, line, now, timeout=True)
    seller.write_mode = "normal"
    seller.lose_reply = False
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["status"] == "RESERVED"
    assert seller.writes[0] == seller.writes[1]
    assert result["lines"][0]["attempt_id"] == before["lines"][0]["attempt_id"]
    assert len(seller.records) == 1


async def test_backoff_survives_new_reconciler_and_then_clears(sessions, line, now):
    c, seller, clock, _ = await unknown(sessions, line, now)
    seller.lookup_mode = "error"
    first = await Reconciler(c).run_order("o-reconcile")
    assert first["lines"][0]["status"] == "UNKNOWN"
    assert first["lines"][0]["retry_count"] == 1
    assert first["lines"][0]["next_retry_at"] is not None
    again = await Reconciler(c).run_order("o-reconcile")
    assert again == first and len(seller.lookups) == 1
    clock.advance(1)
    seller.lookup_mode = "normal"
    recovered = await Reconciler(c).run_order("o-reconcile")
    assert recovered["status"] == "RESERVED"
    assert recovered["lines"][0]["retry_count"] == 2
    assert recovered["lines"][0]["next_retry_at"] is None


async def test_backoff_is_measured_after_a_slow_failure(sessions, line, now):
    c, seller, clock, _ = await unknown(sessions, line, now)
    seller.lookup_mode = "slow_error"
    result = await Reconciler(c).run_order("o-reconcile")
    retry_at = datetime.fromisoformat(result["lines"][0]["next_retry_at"])
    assert retry_at == clock() + timedelta(seconds=1)
    await Reconciler(c).run_order("o-reconcile")
    assert len(seller.lookups) == 1


async def test_retry_budget_is_durable_and_exhaustion_does_not_switch_sellers(sessions, line, now):
    c, seller, clock, before = await unknown(sessions, line, now)
    seller.lookup_mode = "error"
    for _ in range(3):
        result = await Reconciler(c, max_cycles=3).run_order("o-reconcile")
        clock.advance(60)
    assert result["lines"][0]["status"] == "RECONCILIATION_REQUIRED"
    assert result["lines"][0]["retry_count"] == 3
    assert result["lines"][0]["reason"] == "RETRY_BUDGET_EXHAUSTED"
    assert result["lines"][0]["operation_key"] == before["lines"][0]["operation_key"]
    for _ in range(3):
        await Reconciler(c).run_order("o-reconcile")
    assert len(seller.lookups) == 3
    assert len(seller.writes) == 1
    assert len((await rows(sessions))[0]) == 1


async def test_expired_unknown_is_not_replayed_as_new_late_work(sessions, line, now):
    c, seller, clock, _ = await unknown(sessions, line, now, timeout=True)
    seller.lookup_mode = "not_found"
    clock.advance(3 * 24 * 3600)
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["lines"][0]["status"] == "RECONCILIATION_REQUIRED"
    assert result["lines"][0]["reason"] == "DEADLINE_EXPIRED_OUTCOME_UNKNOWN"
    assert len(seller.writes) == 1


async def test_wrong_lookup_identity_cannot_confirm_the_attempt(sessions, line, now):
    c, seller, _, _ = await unknown(sessions, line, now)
    seller.lookup_mode = "wrong_key"
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["lines"][0]["status"] == "UNKNOWN"
    assert result["lines"][0]["reservation_id"] is None
    assert len(seller.writes) == 1


async def test_explicit_no_effect_rejection_returns_line_to_quoting(sessions, line, now):
    c, seller, _, _ = await unknown(sessions, line, now, timeout=True)
    seller.lookup_mode = "rejected"
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["status"] == "PROCESSING"
    assert result["lines"][0]["status"] == "QUOTING"
    assert result["lines"][0]["next_retry_at"] is None
    assert (await rows(sessions))[0][0].status == "REJECTED"
    assert len(seller.writes) == 1


async def test_interrupted_reconciliation_respects_saved_budget_and_schedule(sessions, line, now):
    c, seller, clock, before = await unknown(sessions, line, now)
    seller.lookup_mode = "crash"
    with pytest.raises(RuntimeError, match="interrupted"):
        await Reconciler(c).run_order("o-reconcile")
    state = await c.snapshot("o-reconcile")
    assert state["lines"][0]["status"] == "RECONCILING"
    assert state["lines"][0]["retry_count"] == 1
    seller.lookup_mode = "normal"
    await Reconciler(c).run_order("o-reconcile")
    assert len(seller.lookups) == 1
    clock.advance(1)
    recovered = await Reconciler(c).run_order("o-reconcile")
    assert recovered["status"] == "RESERVED"
    assert recovered["lines"][0]["retry_count"] == 2
    assert recovered["lines"][0]["attempt_id"] == before["lines"][0]["attempt_id"]
    assert len(seller.writes) == 1


async def test_confirmation_event_failure_rolls_back_result_and_can_resume(sessions, line, now):
    c, seller, clock, _ = await unknown(sessions, line, now)
    def fail(session, *_):
        if any(isinstance(r, OutboxEvent) and r.event_type == "line.reserved"
               for r in session.new):
            raise RuntimeError("confirmation transaction failed")
    sql_event.listen(Session, "before_flush", fail)
    try:
        with pytest.raises(RuntimeError, match="transaction failed"):
            await Reconciler(c).run_order("o-reconcile")
    finally:
        sql_event.remove(Session, "before_flush", fail)
    attempts, events = await rows(sessions)
    assert attempts[0].status == "RECONCILING"
    assert events[-1].event_type == "line.reconciling"
    clock.advance(1)
    assert (await Reconciler(c).run_order("o-reconcile"))["status"] == "RESERVED"
    assert len(seller.writes) == 1


async def test_pending_unmaterialized_order_has_nothing_to_reconcile(sessions, line, now):
    c, seller, _, = await setup(sessions, line, now)
    result = await Reconciler(c).run_order("o-reconcile")
    assert result["status"] == "PENDING" and result["lines"] == []
    assert not seller.writes and not seller.lookups


@pytest.mark.parametrize("status", [404, 202, 500])
async def test_lookup_http_does_not_invent_terminal_failure(status):
    def respond(request):
        assert request.method == "GET"
        assert request.url.path == "/reservations/by-key/key"
        return httpx.Response(status, json={"detail": "not a terminal reply"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = HttpReconciliationClient(http, {"s1": "http://seller"})
        if status == 404:
            assert await client.lookup("s1", "key") is None
        else:
            with pytest.raises((ValueError, httpx.HTTPError)):
                await client.lookup("s1", "key")
