import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event as sql_event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from app.clients.delivery import DeliveryReply, HttpDeliveryClient
from app.clients.reservations import ReservationReply
from app.consumers.order_created import accept_order
from app.models import Base, DeliveryCheck, LineAttempt, OutboxEvent, WorkflowLine
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.orchestrator import Coordinator
from app.schemas import OrderCreatedEvent, Quote


class Clock:
    def __init__(self, now):
        self.value = now
    def __call__(self):
        return self.value
    def advance(self, seconds=11):
        self.value += timedelta(seconds=seconds)


class Locks:
    def __init__(self):
        self.locks = {}
    @asynccontextmanager
    async def __call__(self, order_id):
        async with self.locks.setdefault(order_id, asyncio.Lock()):
            yield


@pytest_asyncio.fixture
async def sessions(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'delivery.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


class Seller:
    def __init__(self, clock):
        self.clock, self.eta = clock, clock() + timedelta(hours=1)
        self.records, self.calls, self.writes = {}, [], []
        self.mode, self.version, self.delivered = "normal", 1, set()
        self.active = self.peak = 0

    async def quote(self, seller_id, line):
        return Quote(seller_id=seller_id, part_id=line.part_id, available_quantity=100,
                     estimated_delivery_at=self.eta)

    async def reserve(self, seller_id, order_id, line, operation_key):
        self.writes.append(operation_key)
        reply = ReservationReply(
            seller_id=seller_id, operation_key=operation_key,
            part_id=line.part_id, quantity=line.quantity, status="RESERVED",
            reservation_id="reservation-" + line.line_id, estimated_delivery_at=self.eta)
        self.records[reply.reservation_id] = reply
        return reply

    async def delivery(self, seller_id, reservation_id):
        self.calls.append(reservation_id)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0)
            if self.mode == "timeout":
                await asyncio.Event().wait()
            if self.mode == "error":
                raise httpx.ReadError("seller unreachable")
            if self.mode == "slow_error":
                self.clock.advance(20)
                raise httpx.ReadError("slow lookup failed")
            if self.mode == "crash":
                raise RuntimeError("poller interrupted")
            original = self.records[reservation_id]
            delivered = original.part_id in self.delivered
            reply = DeliveryReply(
                seller_id=seller_id, operation_key=original.operation_key,
                reservation_id=reservation_id, part_id=original.part_id,
                quantity=original.quantity, status="DELIVERED" if delivered else "IN_TRANSIT",
                status_version=self.version,
                estimated_delivery_at=None if delivered else self.eta,
                delivered_at=self.clock() if delivered else None,
            ).model_dump(mode="json")
            if self.mode == "wrong_identity":
                reply["operation_key"] = "wrong-key"
            return reply
        finally:
            self.active -= 1


async def setup(sessions, now, *, parts=("bumper",)):
    clock = Clock(now)
    seller = Seller(clock)
    event = OrderCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "order.created", "schema_version": 1,
        "producer": "order", "occurred_at": now,
        "payload": {"order_id": "delivery-test", "lines": [
            {"line_id": f"line-{i}", "part_id": part, "quantity": 1,
             "needed_by": now + timedelta(days=2)}
            for i, part in enumerate(parts, 1)
        ]},
    })
    await accept_order(event, sessions, workflow_topic="workflow.events")
    c = Coordinator(sessions, seller, ["seller-1"], lock=Locks(), clock=clock,
                    workflow_topic="workflow.events", timeout_seconds=.03)
    await c.run_order("delivery-test")
    return c, seller, clock


async def state(sessions):
    async with sessions() as session:
        return (
            list(await session.scalars(select(DeliveryCheck))),
            list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.state_version))),
        )


async def test_due_schedule_and_same_reply_survive_fresh_poller(sessions, now):
    c, seller, clock = await setup(sessions, now)
    first = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert first["status"] == "RESERVED"
    assert first["lines"][0]["delivery_poll_count"] == 1
    assert first["state_version"] == 6
    assert await DeliveryPoller(c, seller).run_order("delivery-test") == first
    assert len(seller.calls) == 1
    clock.advance()
    second = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert len(seller.calls) == 2
    assert second["state_version"] == first["state_version"]  # no duplicate progress event
    assert second["lines"][0]["delivery_poll_count"] == 2
    assert len(seller.writes) == 1


async def test_late_delivery_is_visible_without_cancellation_or_replacement(sessions, now):
    c, seller, _ = await setup(sessions, now)
    before = await c.snapshot("delivery-test")
    seller.eta = now + timedelta(days=3)
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    line = result["lines"][0]
    assert result["status"] == "RECONCILIATION_REQUIRED"
    assert line["status"] == "RESERVED" and line["reason"] == "DELIVERY_AFTER_DEADLINE"
    assert line["attempt_id"] == before["lines"][0]["attempt_id"]
    assert line["attempt_count"] == len(seller.writes) == 1
    assert (await state(sessions))[1][-1].event_type == "line.delivery_delayed"


async def test_delivered_is_terminal_and_clears_poll_schedule(sessions, now):
    c, seller, clock = await setup(sessions, now)
    seller.delivered.add("bumper")
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert result["status"] == "FULFILLED"
    assert result["lines"][0]["status"] == "DELIVERED"
    assert result["lines"][0]["next_delivery_poll_at"] is None
    clock.advance(100)
    assert await DeliveryPoller(c, seller).run_order("delivery-test") == result
    assert len(seller.calls) == 1


async def test_delivered_and_reserved_lines_are_not_prematurely_fulfilled(sessions, now):
    c, seller, _ = await setup(sessions, now, parts=("bumper", "mirror"))
    seller.delivered.add("mirror")
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert result["status"] == "RESERVED"
    assert [line["status"] for line in result["lines"]] == ["RESERVED", "DELIVERED"]


@pytest.mark.parametrize("mode,expected", [
    ("error", "DELIVERY_LOOKUP_UNAVAILABLE"),
    ("timeout", "DELIVERY_LOOKUP_UNAVAILABLE"),
    ("wrong_identity", "INVALID_DELIVERY_OBSERVATION"),
])
async def test_failed_observation_keeps_known_reservation_and_defers(sessions, now, mode, expected):
    c, seller, clock = await setup(sessions, now)
    seller.mode = mode
    first = await DeliveryPoller(c, seller).run_order("delivery-test")
    line = first["lines"][0]
    assert line["status"] == "RESERVED" and line["delivery_status"] is None
    assert line["delivery_poll_error"] == expected
    assert line["delivery_poll_failures"] == 1
    clock.advance(10)
    second = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert second["state_version"] == first["state_version"]
    assert second["lines"][0]["delivery_poll_failures"] == 2
    await DeliveryPoller(c, seller).run_order("delivery-test")
    assert len(seller.calls) == 2
    clock.advance(20)
    seller.mode = "normal"
    recovered = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert recovered["lines"][0]["delivery_poll_error"] is None
    assert recovered["lines"][0]["delivery_poll_failures"] == 0
    assert len(seller.writes) == 1


async def test_backoff_is_measured_after_a_slow_lookup_failure(sessions, now):
    c, seller, clock = await setup(sessions, now)
    seller.mode = "slow_error"
    await DeliveryPoller(c, seller).run_order("delivery-test")
    check = (await state(sessions))[0][0]
    from app.saga.orchestrator import as_utc
    assert as_utc(check.next_poll_at) == clock() + timedelta(seconds=10)


async def test_missing_delivery_proof_after_deadline_is_explicit(sessions, now):
    c, seller, clock = await setup(sessions, now)
    seller.mode = "error"
    clock.advance(3 * 24 * 3600)
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert result["status"] == "RECONCILIATION_REQUIRED"
    assert result["lines"][0]["reason"] == "DELIVERY_DEADLINE_PASSED_UNCONFIRMED"
    assert result["lines"][0]["status"] == "RESERVED"


@pytest.mark.parametrize("conflict", [False, True])
async def test_stale_or_conflicting_source_version_cannot_overwrite_eta(sessions, now, conflict):
    c, seller, clock = await setup(sessions, now)
    seller.version = 2
    first = await DeliveryPoller(c, seller).run_order("delivery-test")
    clock.advance()
    seller.version = 2 if conflict else 1
    seller.eta = now + timedelta(days=3)
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert result["lines"][0]["estimated_delivery_at"] == first["lines"][0]["estimated_delivery_at"]
    assert result["lines"][0]["delivery_status_version"] == 2
    assert result["lines"][0]["delivery_poll_error"] == (
        "CONFLICTING_DELIVERY_VERSION" if conflict else "STALE_DELIVERY_OBSERVATION")


async def test_interrupted_poll_claim_is_durable_and_resumes_when_due(sessions, now):
    c, seller, clock = await setup(sessions, now)
    seller.mode = "crash"
    with pytest.raises(RuntimeError, match="interrupted"):
        await DeliveryPoller(c, seller).run_order("delivery-test")
    checks, events = await state(sessions)
    assert checks[0].poll_count == 1 and checks[0].next_poll_at is not None
    assert len(events) == 5
    seller.mode = "normal"
    await DeliveryPoller(c, seller).run_order("delivery-test")
    assert len(seller.calls) == 1
    clock.advance()
    assert (await DeliveryPoller(c, seller).run_order("delivery-test"))["lines"][0]["delivery_poll_count"] == 2


async def test_delivery_state_and_event_rollback_together(sessions, now):
    c, seller, clock = await setup(sessions, now)
    seller.delivered.add("bumper")
    def fail(session, *_):
        if any(isinstance(row, OutboxEvent) and row.event_type == "line.delivered"
               for row in session.new):
            raise RuntimeError("delivery event commit failed")
    sql_event.listen(Session, "before_flush", fail)
    try:
        with pytest.raises(RuntimeError, match="commit failed"):
            await DeliveryPoller(c, seller).run_order("delivery-test")
    finally:
        sql_event.remove(Session, "before_flush", fail)
    snapshot = await c.snapshot("delivery-test")
    assert snapshot["lines"][0]["status"] == "RESERVED"
    assert snapshot["lines"][0]["delivery_status"] is None
    assert len((await state(sessions))[1]) == 5
    clock.advance()
    assert (await DeliveryPoller(c, seller).run_order("delivery-test"))["status"] == "FULFILLED"


async def test_delivery_calls_have_bounded_concurrency(sessions, now):
    c, seller, _ = await setup(sessions, now, parts=tuple(f"part-{i}" for i in range(8)))
    await DeliveryPoller(c, seller, concurrency=2).run_order("delivery-test")
    assert seller.peak == 2 and len(seller.calls) == 8


async def test_late_delivered_part_is_reported_as_arrived_with_lateness(sessions, now):
    c, seller, clock = await setup(sessions, now)
    clock.advance(3 * 24 * 3600)
    seller.delivered.add("bumper")
    result = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert result["status"] == "FULFILLED"
    assert result["lines"][0]["reason"] == "DELIVERED_AFTER_DEADLINE"


async def test_stale_attempt_reply_cannot_mark_a_replacing_line_delivered(sessions, now):
    c, seller, _ = await setup(sessions, now)
    poller = DeliveryPoller(c, seller)
    requests = await poller._claim("delivery-test")
    seller.delivered.add("bumper")
    reply, error = await poller._fetch(requests[0])
    async with sessions.begin() as session:
        line = await session.get(WorkflowLine, ("delivery-test", "line-1"))
        line.status = "REPLACING"  # Controlled simulation of an external ownership change.
    await poller._record(requests[0], reply, error)
    assert (await c.snapshot("delivery-test"))["lines"][0]["status"] == "REPLACING"
    assert len((await state(sessions))[1]) == 5


async def test_unknown_reservation_is_not_polled_as_known_delivery(sessions, now):
    c, seller, _ = await setup(sessions, now)
    async with sessions.begin() as session:
        line = await session.get(WorkflowLine, ("delivery-test", "line-1"))
        line.status = "UNKNOWN"
        attempt = await session.get(LineAttempt, line.current_attempt_id)
        attempt.status = "UNKNOWN"
    await DeliveryPoller(c, seller).run_order("delivery-test")
    assert not seller.calls and not (await state(sessions))[0]


async def test_equivalent_timestamp_offsets_do_not_conflict_at_same_version(sessions, now):
    from datetime import timezone
    c, seller, clock = await setup(sessions, now)
    first = await DeliveryPoller(c, seller).run_order("delivery-test")
    clock.advance()
    seller.eta = seller.eta.astimezone(timezone(timedelta(hours=-8)))
    second = await DeliveryPoller(c, seller).run_order("delivery-test")
    assert second["state_version"] == first["state_version"]
    assert second["lines"][0]["delivery_poll_error"] is None


@pytest.mark.parametrize("states,expected", [
    (["DELIVERED", "UNFULFILLED"], "PARTIALLY_FULFILLED"),
    (["DELIVERED", "RESERVED", "UNFULFILLED"], "PARTIALLY_RESERVED"),
])
def test_partial_delivery_does_not_claim_every_line_arrived(states, expected):
    from types import SimpleNamespace
    from app.saga.orchestrator import aggregate
    assert aggregate([SimpleNamespace(status=status, reason=None) for status in states]) == expected


@pytest.mark.parametrize("status", [202, 404, 500])
async def test_http_errors_are_not_delivery_proof(status):
    def respond(request):
        assert request.method == "GET" and request.url.path == "/reservations/res-1/delivery"
        return httpx.Response(status, json={"detail": "no delivery proof"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises((ValueError, httpx.HTTPError)):
            await HttpDeliveryClient(http, {"seller-1": "http://seller"}).delivery("seller-1", "res-1")


@pytest.mark.parametrize("change", [
    {"status_version": True}, {"status_version": 0},
    {"estimated_delivery_at": None}, {"delivered_at": "2030-01-01T00:00:00Z"},
    {"estimated_delivery_at": "2030-01-02T00:00:00"},
    {"status": "DELIVERED", "estimated_delivery_at": None, "delivered_at": None},
])
def test_delivery_boundary_rejects_ambiguous_or_invalid_evidence(change):
    with pytest.raises(ValueError):
        DeliveryReply.model_validate({
            "seller_id": "s1", "operation_key": "key", "reservation_id": "r1",
            "part_id": "bumper", "quantity": 1, "status": "IN_TRANSIT", "status_version": 1,
            "estimated_delivery_at": "2030-01-02T00:00:00Z", **change,
        })
