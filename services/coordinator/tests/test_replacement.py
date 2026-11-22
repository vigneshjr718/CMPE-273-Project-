"""Controlled C behavior; these fixtures do not certify C's actual fencing."""
import asyncio
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import event as sql_event, select
from sqlalchemy.orm import Session

from app.clients.cancellation import CancellationReply, HttpCancellationClient
from app.clients.reservations import ReservationReply
from app.models import CancellationCheck, LineAttempt, OutboxEvent, WorkflowLine
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.orchestrator import Coordinator, as_utc
from app.saga.reconciliation import Reconciler
from app.saga.replacement import ReplacementCoordinator
from app.schemas import Quote
from test_delivery_polling import setup, sessions

ORDER = "delivery-test"


class FencingSeller:
    def __init__(self, original, clock):
        self.original, self.clock = original, clock
        self.mode, self.version, self.quote_mode = "cancelled", 2, "normal"
        self.calls, self.writes, self.records, self.fences, self.receipts = [], [], {}, set(), {}

    async def quote(self, seller_id, line):
        if self.quote_mode == "error":
            raise httpx.ReadError("quotes unavailable")
        return Quote(seller_id=seller_id, part_id=line.part_id,
                     available_quantity=0 if self.quote_mode == "no_stock" else 10,
                     estimated_delivery_at=self.clock() + timedelta(hours=2))

    async def reserve(self, seller_id, order_id, line, key):
        assert key not in self.fences, "fenced original cannot be replayed"
        self.writes.append((seller_id, key))
        if key not in self.records:
            self.records[key] = ReservationReply(
                seller_id=seller_id, operation_key=key, part_id=line.part_id,
                quantity=line.quantity, status="RESERVED", reservation_id="new-reservation",
                estimated_delivery_at=self.clock() + timedelta(hours=2))
        if self.mode == "new_unknown":
            raise httpx.ReadError("lost replacement response")
        return self.records[key]

    async def lookup(self, seller_id, key):
        return self.records.get(key)

    async def cancel(self, seller_id, order_id, line, key, cancel_key, reservation_id):
        self.calls.append((key, cancel_key, reservation_id))
        if self.mode == "timeout":
            await asyncio.Event().wait()
        if self.mode == "error":
            raise httpx.ReadError("unavailable")
        if self.mode == "404":
            httpx.Response(404, request=httpx.Request("POST", "http://seller/cancel")).raise_for_status()
        if self.mode == "crash":
            raise RuntimeError("interrupted cancellation")
        status = "PENDING" if self.mode in {"pending", "slow_pending"} else (
            "DELIVERED" if self.mode == "delivered" else "CANCELLED")
        if self.mode == "slow_pending":
            self.clock.advance(20)
        if status == "CANCELLED":
            self.fences.add(key)
        reply = dict(
            seller_id=seller_id, operation_key=key, cancellation_key=cancel_key,
            part_id=line.part_id, quantity=line.quantity, reservation_id=reservation_id,
            status=status, status_version=self.version, fenced=status == "CANCELLED",
            cancellation_receipt="receipt-1" if status == "CANCELLED" else None,
            delivered_at=self.clock() if status == "DELIVERED" else None,
        )
        if self.mode == "wrong_identity":
            reply["operation_key"] = "another-operation"
        if self.mode == "wrong_reservation":
            reply["reservation_id"] = "another-reservation"
        if self.mode == "missing_fence":
            reply["fenced"] = False
        if self.mode == "lost_reply":
            self.receipts[cancel_key] = reply
            raise httpx.ReadError("terminal cancellation committed, reply lost")
        return self.receipts.get(cancel_key, reply)


async def prepare(sessions, now):
    c, original, clock = await setup(sessions, now)
    original.eta = now + timedelta(days=3)
    await DeliveryPoller(c, original).run_order(ORDER)
    seller = FencingSeller(original, clock)
    c.client, c.seller_ids = seller, ["seller-1", "seller-2", "seller-3"]
    return c, seller, clock


async def saved(sessions):
    async with sessions() as session:
        return (list(await session.scalars(select(CancellationCheck))),
                list(await session.scalars(select(LineAttempt).order_by(LineAttempt.attempt_number))),
                list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.state_version))))


def fresh(c):
    return Coordinator(c.sessions, c.client, c.seller_ids, lock=c.lock, clock=c.clock,
                       workflow_topic=c.topic, timeout_seconds=c.timeout)


async def test_confirmed_fence_then_one_replacement_with_new_key(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    checks, attempts, events = await saved(sessions)
    assert result["status"] == "RESERVED" and result["lines"][0]["attempt_count"] == 2
    assert result["lines"][0]["seller_id"] == "seller-2"
    assert [a.status for a in attempts] == ["CANCELLED", "RESERVED"]
    assert attempts[0].operation_key != attempts[1].operation_key
    assert checks[0].status == "CONFIRMED" and checks[0].last_payload["fenced"] is True
    assert [e.event_type for e in events][-5:] == [
        "line.cancelling", "line.cancelled", "line.replacement_started", "line.reserving", "line.reserved"]
    assert [e.state_version for e in events] == list(range(1, result["state_version"] + 1))
    assert await ReplacementCoordinator(fresh(c), seller).run_order(ORDER) == result
    assert await c.run_order(ORDER) == result
    assert len(seller.writes) == len(seller.calls) == 1


@pytest.mark.parametrize("mode,error", [
    ("timeout", "CANCELLATION_OUTCOME_UNKNOWN"), ("error", "CANCELLATION_OUTCOME_UNKNOWN"),
    ("404", "CANCELLATION_OUTCOME_UNKNOWN"), ("wrong_identity", "INVALID_CANCELLATION_EVIDENCE"),
    ("wrong_reservation", "INVALID_CANCELLATION_EVIDENCE"),
    ("missing_fence", "INVALID_CANCELLATION_EVIDENCE"), ("pending", "SELLER_CANCELLATION_PENDING"),
])
async def test_ambiguity_blocks_all_other_drivers(sessions, now, mode, error):
    c, seller, _ = await prepare(sessions, now)
    seller.mode = mode
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    row = result["lines"][0]
    assert row["status"] == "CANCELLATION_PENDING" and row["cancellation_error"] == error
    assert result["status"] == "RECONCILIATION_REQUIRED" and row["next_cancellation_retry_at"]
    assert await c.run_order(ORDER) == result
    assert await DeliveryPoller(c, seller.original).run_order(ORDER) == result
    assert await Reconciler(c).run_order(ORDER) == result
    assert not seller.writes and len((await saved(sessions))[1]) == 1


@pytest.mark.parametrize("mode", ["lost_reply", "crash"])
async def test_uncertain_or_interrupted_call_recovers_same_persisted_key(sessions, now, mode):
    c, seller, clock = await prepare(sessions, now)
    seller.mode = mode
    if mode == "crash":
        with pytest.raises(RuntimeError):
            await ReplacementCoordinator(c, seller).run_order(ORDER)
    else:
        await ReplacementCoordinator(c, seller).run_order(ORDER)
    check = (await saved(sessions))[0][0]
    assert check.request_count == 1 and check.next_retry_at and not seller.writes
    seller.mode = "cancelled"
    await ReplacementCoordinator(fresh(c), seller).run_order(ORDER)
    assert len(seller.calls) == 1
    clock.advance(1)
    assert (await ReplacementCoordinator(fresh(c), seller).run_order(ORDER))["status"] == "RESERVED"
    assert seller.calls[0] == seller.calls[1] and len(seller.writes) == 1


async def test_budget_and_backoff_persist_after_slow_pending_response(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    seller.mode = "slow_pending"
    await ReplacementCoordinator(c, seller, max_cycles=2).run_order(ORDER)
    assert as_utc((await saved(sessions))[0][0].next_retry_at) == clock() + timedelta(seconds=1)
    clock.advance(1)
    seller.mode = "error"
    result = await ReplacementCoordinator(fresh(c), seller, max_cycles=2).run_order(ORDER)
    assert result["lines"][0]["cancellation_status"] == "REQUIRED"
    assert result["lines"][0]["next_cancellation_retry_at"] is None
    clock.advance(100)
    seller.mode = "cancelled"
    assert await ReplacementCoordinator(fresh(c), seller).run_order(ORDER) == result
    assert len(seller.calls) == 2 and not seller.writes


async def test_final_interrupted_claim_remains_pending_for_review(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    seller.mode = "crash"
    with pytest.raises(RuntimeError):
        await ReplacementCoordinator(c, seller, max_cycles=1).run_order(ORDER)
    result = await ReplacementCoordinator(fresh(c), seller, max_cycles=1).run_order(ORDER)
    assert result["lines"][0]["status"] == "CANCELLATION_PENDING"
    assert result["lines"][0]["cancellation_status"] == "REQUIRED" and not seller.writes


async def test_delivery_wins_race_and_blocks_replacement(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    seller.mode = "delivered"
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["status"] == "FULFILLED" and result["lines"][0]["delivery_status"] == "DELIVERED"
    assert result["lines"][0]["next_delivery_poll_at"] is None and not seller.writes
    assert await ReplacementCoordinator(fresh(c), seller).run_order(ORDER) == result


@pytest.mark.parametrize("version", [1, 0])
async def test_stale_or_invalid_source_version_cannot_replace(sessions, now, version):
    c, seller, _ = await prepare(sessions, now)
    seller.version = version
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["lines"][0]["status"] == "CANCELLATION_PENDING" and not seller.writes


async def test_same_source_version_with_changed_payload_is_not_terminal_proof(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    seller.mode = "pending"
    await ReplacementCoordinator(c, seller).run_order(ORDER)
    clock.advance(1)
    seller.mode = "cancelled"
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["lines"][0]["cancellation_error"] == "CONFLICTING_CANCELLATION_VERSION"
    assert not seller.writes


async def test_unknown_without_reservation_id_can_be_fenced_by_original_key(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    async with sessions.begin() as session:
        row = await session.get(WorkflowLine, (ORDER, "line-1"))
        row.status = "UNKNOWN"
        attempt = await session.get(LineAttempt, row.current_attempt_id)
        attempt.status, attempt.reservation_id = "UNKNOWN", None
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["status"] == "RESERVED" and seller.calls[0][2] is None
    with pytest.raises(AssertionError, match="fenced original"):
        await seller.reserve("seller-1", ORDER, None, seller.calls[0][0])


@pytest.mark.parametrize("mode,expected", [("no_stock", "UNFULFILLED"), ("error", "RECONCILIATION_REQUIRED")])
async def test_no_eligible_replacement_and_failed_quotes_are_explicit(sessions, now, mode, expected):
    c, seller, _ = await prepare(sessions, now)
    seller.quote_mode = mode
    assert (await ReplacementCoordinator(c, seller).run_order(ORDER))["status"] == expected
    assert (await saved(sessions))[1][0].status == "CANCELLED" and not seller.writes
    if mode == "error":
        seller.quote_mode = "normal"
        assert (await ReplacementCoordinator(fresh(c), seller).run_order(ORDER))["status"] == "RESERVED"
        assert len(seller.calls) == 1


async def test_expired_deadline_blocks_new_reservation(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    clock.advance(3 * 24 * 3600)
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["status"] == "UNFULFILLED" and not seller.writes
    assert result["lines"][0]["reason"] == "DEADLINE_EXPIRED"


async def test_ambiguous_successor_uses_existing_reconciler_with_same_key(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    seller.mode = "new_unknown"
    first = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert first["lines"][0]["status"] == "UNKNOWN" and first["lines"][0]["attempt_count"] == 2
    assert await c.run_order(ORDER) == first
    result = await Reconciler(fresh(c)).run_order(ORDER)
    assert result["status"] == "RESERVED" and len(seller.writes) == 1
    assert result["lines"][0]["operation_key"] == first["lines"][0]["operation_key"]


async def test_terminal_proof_and_event_rollback_together(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    def fail(session, *_):
        if any(isinstance(row, OutboxEvent) and row.event_type == "line.cancelled" for row in session.new):
            raise RuntimeError("cancel event commit failed")
    sql_event.listen(Session, "before_flush", fail)
    try:
        with pytest.raises(RuntimeError, match="commit failed"):
            await ReplacementCoordinator(c, seller).run_order(ORDER)
    finally:
        sql_event.remove(Session, "before_flush", fail)
    checks, attempts, events = await saved(sessions)
    assert checks[0].last_payload is None and checks[0].status == "PENDING"
    assert attempts[0].status == "CANCELLATION_PENDING" and not seller.writes
    assert events[-1].event_type == "line.cancelling"
    clock.advance(1)
    assert (await ReplacementCoordinator(fresh(c), seller).run_order(ORDER))["status"] == "RESERVED"
    assert seller.calls[0] == seller.calls[1]


async def test_resume_after_confirmation_does_not_cancel_again(sessions, now, monkeypatch):
    c, seller, _ = await prepare(sessions, now)
    worker = ReplacementCoordinator(c, seller)
    async def interrupted(*args):
        raise RuntimeError("crash before replacement")
    monkeypatch.setattr(worker, "_resume", interrupted)
    with pytest.raises(RuntimeError):
        await worker.run_order(ORDER)
    assert (await c.snapshot(ORDER))["lines"][0]["status"] == "REPLACING" and not seller.writes
    assert (await ReplacementCoordinator(fresh(c), seller).run_order(ORDER))["status"] == "RESERVED"
    assert len(seller.calls) == len(seller.writes) == 1


@pytest.mark.parametrize("old_state", ["CANCELLATION_PENDING", "CANCELLED"])
async def test_normal_driver_cannot_bypass_missing_fence(sessions, now, old_state):
    c, seller, _ = await prepare(sessions, now)
    async with sessions.begin() as session:
        row = await session.get(WorkflowLine, (ORDER, "line-1"))
        row.status = "QUOTING"
        (await session.get(LineAttempt, row.current_attempt_id)).status = old_state
    with pytest.raises(ValueError):
        await c.run_order(ORDER)
    assert not seller.writes


async def test_stale_reply_cannot_overwrite_new_attempt_owner(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    worker = ReplacementCoordinator(c, seller)
    request = await worker._begin(ORDER, (await c._initialize(ORDER))[0])
    reply, error = await worker._fetch(request)
    async with sessions.begin() as session:
        (await session.get(WorkflowLine, (ORDER, "line-1"))).current_attempt_id = "another-attempt"
    await worker._record(request, reply, error)
    assert (await saved(sessions))[0][0].status == "PENDING" and not seller.writes


async def test_intent_commits_before_http_and_order_lock_serializes_workers(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    entered, release = asyncio.Event(), asyncio.Event()
    original_cancel = seller.cancel
    async def blocking(*args):
        checks, attempts, _ = await saved(sessions)
        assert checks[0].request_count == 1 and checks[0].next_retry_at
        assert attempts[0].status == "CANCELLATION_PENDING"
        entered.set()
        await release.wait()
        return await original_cancel(*args)
    seller.cancel, c.timeout = blocking, 5
    first = asyncio.create_task(ReplacementCoordinator(c, seller).run_order(ORDER))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(ReplacementCoordinator(fresh(c), seller).run_order(ORDER))
    try:
        await asyncio.sleep(0)
        assert not second.done()
        release.set()
        assert await first == await second
        assert len(seller.calls) == len(seller.writes) == 1
    finally:
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)


async def test_timely_and_delivered_lines_are_not_cancelled(sessions, now):
    c, original, clock = await setup(sessions, now)
    seller = FencingSeller(original, clock)
    await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert not seller.calls
    original.delivered.add("bumper")
    await DeliveryPoller(c, original).run_order(ORDER)
    await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert not seller.calls


@pytest.mark.parametrize("status", [202, 204, 404, 409, 500])
async def test_http_errors_and_ack_without_evidence_are_not_proof(status, line):
    def respond(request):
        assert request.method == "POST" and request.url.path == "/reservation-operations/reserve-key/cancel"
        assert request.headers["Idempotency-Key"] == "cancel-key"
        return httpx.Response(status, json={"detail": "no terminal proof"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises((ValueError, httpx.HTTPError)):
            await HttpCancellationClient(http, {"s1": "http://seller"}).cancel(
                "s1", "o1", line, "reserve-key", "cancel-key", None)


@pytest.mark.parametrize("change", [
    {"fenced": False}, {"fenced": "true"}, {"cancellation_receipt": None},
    {"status_version": True}, {"status_version": 0},
    {"delivered_at": "2030-01-01T00:00:00Z"}, {"status": "PENDING"}, {"status": "DELIVERED"},
])
def test_boundary_requires_terminal_fence_receipt(change):
    with pytest.raises(ValueError):
        CancellationReply.model_validate(dict(
            seller_id="s1", operation_key="key", cancellation_key="cancel-key", part_id="bumper",
            quantity=1, status="CANCELLED", status_version=1, fenced=True, cancellation_receipt="receipt-1") | change)


async def test_pending_response_can_advance_to_valid_terminal_proof(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    seller.mode = "pending"
    await ReplacementCoordinator(c, seller).run_order(ORDER)
    clock.advance(1)
    seller.mode, seller.version = "cancelled", 3
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["status"] == "RESERVED" and len(seller.writes) == 1


async def test_replacement_rejects_one_seller_and_uses_next_without_recancelling(sessions, now):
    c, seller, _ = await prepare(sessions, now)
    original = seller.reserve
    async def reserve(seller_id, order_id, line, key):
        if seller_id == "seller-2":
            return ReservationReply(seller_id=seller_id, operation_key=key,
                part_id=line.part_id, quantity=line.quantity, status="REJECTED")
        return await original(seller_id, order_id, line, key)
    seller.reserve = reserve
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["lines"][0]["seller_id"] == "seller-3"
    assert [a.status for a in (await saved(sessions))[1]] == ["CANCELLED", "REJECTED", "RESERVED"]
    assert len(seller.calls) == 1


async def test_delivered_other_line_keeps_original_attempt_during_replacement(sessions, now):
    c, original, clock = await setup(sessions, now, parts=("bumper", "mirror"))
    original.eta = now + timedelta(days=3)
    original.delivered.add("mirror")
    before = await DeliveryPoller(c, original).run_order(ORDER)
    seller = FencingSeller(original, clock)
    c.client, c.seller_ids = seller, ["seller-1", "seller-2"]
    result = await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert result["status"] == "RESERVED" and len(seller.calls) == 1
    assert result["lines"][1] == before["lines"][1]
    assert result["lines"][0]["attempt_count"] == 2


async def test_crash_before_successor_request_never_creates_third_attempt_without_fencing(sessions, now):
    c, seller, clock = await prepare(sessions, now)
    original = seller.reserve
    async def crash(*args):
        raise RuntimeError("crash after saved successor intent")
    seller.reserve = crash
    with pytest.raises(RuntimeError):
        await ReplacementCoordinator(c, seller).run_order(ORDER)
    assert (await c.snapshot(ORDER))["lines"][0]["status"] == "RESERVING"
    seller.reserve = original
    result = await fresh(c).run_order(ORDER)
    assert result["lines"][0]["status"] == "UNKNOWN"
    assert result["lines"][0]["attempt_count"] == 2 and not seller.writes


@pytest.mark.parametrize("status,result", [(200, "CANCELLED"), (202, "PENDING")])
async def test_http_adapter_parses_proof_and_sends_original_identity(status, result, line):
    import json
    def respond(request):
        body = json.loads(request.content)
        assert body == {"order_id": "o1", "line_id": line.line_id, "part_id": line.part_id,
                        "quantity": line.quantity, "reservation_id": None}
        return httpx.Response(status, json=dict(
            seller_id="s1", operation_key="reserve-key", cancellation_key="cancel-key",
            part_id=line.part_id, quantity=line.quantity, status=result, status_version=1,
            fenced=result == "CANCELLED", cancellation_receipt="receipt" if result == "CANCELLED" else None))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        reply = await HttpCancellationClient(http, {"s1": "http://seller"}).cancel(
            "s1", "o1", line, "reserve-key", "cancel-key", None)
    assert reply.status == result
