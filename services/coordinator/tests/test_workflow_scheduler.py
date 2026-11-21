import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from app.clients.cancellation import CancellationReply
from app.clients.delivery import DeliveryReply
from app.clients.reservations import ReservationReply
from app.config import Settings
from app.consumers.order_created import accept_order
from app.models import CancellationCheck, LineAttempt, OutboxEvent, Workflow, WorkflowJob
from app.saga.locking import OrderBusy
from app.saga.orchestrator import Coordinator, as_utc
from app.saga.replacement import ReplacementCoordinator
from app.schemas import OrderCreatedEvent, Quote
from app.tools.run_workflows import make_scheduler
from app.workers.workflows import WorkflowScheduler
from test_delivery_polling import Clock, Locks, sessions


class Sellers:
    def __init__(self, clock):
        self.clock, self.records, self.fences = clock, {}, {}
        self.reserve_calls, self.lookup_calls, self.cancel_calls, self.delivery_calls, self.quote_calls = [], [], [], [], []
        self.quote_error = self.lookup_error = self.lose_reserve = False
        self.cancel_mode, self.late, self.crash_reserve = "confirmed", False, False

    async def quote(self, seller_id, line):
        self.quote_calls.append(seller_id)
        if self.quote_error:
            raise httpx.ReadError("quotes unavailable")
        return Quote(seller_id=seller_id, part_id=line.part_id, available_quantity=10,
                     estimated_delivery_at=self.clock() + timedelta(hours=int(seller_id[-1])))

    async def reserve(self, seller_id, order_id, line, key):
        self.reserve_calls.append(key)
        assert key not in self.fences
        if key not in self.records:
            self.records[key] = (ReservationReply(seller_id=seller_id, operation_key=key,
                part_id=line.part_id, quantity=line.quantity, status="RESERVED",
                reservation_id="res-" + uuid4().hex, estimated_delivery_at=self.clock()+timedelta(hours=1)), line)
        if self.crash_reserve:
            self.crash_reserve = False
            raise RuntimeError("crash after remote commit")
        if self.lose_reserve:
            self.lose_reserve = False
            raise httpx.ReadError("lost response")
        return self.records[key][0]

    async def lookup(self, seller_id, key):
        self.lookup_calls.append(key)
        if self.lookup_error:
            raise httpx.ReadError("lookup unavailable")
        return self.records.get(key, (None,))[0]

    async def delivery(self, seller_id, reservation_id):
        self.delivery_calls.append(reservation_id)
        saved,line = next(pair for pair in self.records.values() if pair[0].reservation_id==reservation_id)
        late = self.late and seller_id == "seller-1"
        return DeliveryReply(seller_id=seller_id, operation_key=saved.operation_key,
            reservation_id=reservation_id, part_id=line.part_id, quantity=line.quantity,
            status="IN_TRANSIT" if late else "DELIVERED", status_version=1,
            estimated_delivery_at=line.needed_by+timedelta(days=1) if late else None,
            delivered_at=None if late else self.clock())

    async def cancel(self, seller_id, order_id, line, key, cancel_key, reservation_id):
        self.cancel_calls.append((key,cancel_key))
        pending = self.cancel_mode == "pending"
        if key not in self.fences or pending:
            reply=CancellationReply(seller_id=seller_id, operation_key=key,
                cancellation_key=cancel_key, reservation_id=reservation_id, part_id=line.part_id,
                quantity=line.quantity, status="PENDING" if pending else "CANCELLED",
                status_version=2, fenced=not pending, cancellation_receipt=None if pending else "receipt-1")
            if not pending:
                self.fences[key]=reply
            return reply
        return self.fences[key]


async def setup(sessions, now, **settings):
    clock, lock = Clock(now), Locks()
    seller = Sellers(clock)
    event=OrderCreatedEvent.model_validate({"event_id":str(uuid4()),"event_type":"order.created",
        "schema_version":1,"producer":"order","occurred_at":now,"payload":{"order_id":"scheduled",
        "lines":[{"line_id":"line-1","part_id":"bumper","quantity":1,"needed_by":now+timedelta(days=2)}]}})
    await accept_order(event,sessions,workflow_topic="workflow.events")
    c=Coordinator(sessions,seller,["seller-1","seller-2"],lock=lock,clock=clock,
                  workflow_topic="workflow.events",timeout_seconds=.03)
    config=Settings(_env_file=None,**settings)
    return c,seller,clock,config


def scheduler(c,seller,config):
    return make_scheduler(config,c,seller,seller)


async def job(sessions):
    async with sessions() as session:
        return await session.get(WorkflowJob,"scheduled")


async def test_intake_atomically_schedules_and_worker_completes_order(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    before=await job(sessions)
    assert before.status=="READY" and before.run_count==0
    result=await scheduler(c,seller,config).run_once()
    assert result=={"selected":1,"complete":1}
    assert (await c.snapshot("scheduled"))["status"]=="FULFILLED"
    row=await job(sessions)
    assert row.status=="COMPLETE" and row.next_run_at is None and row.run_count==1
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    assert len(seller.reserve_calls)==1


async def test_unknown_reconciles_same_key_after_saved_retry_time(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    seller.lose_reserve=seller.lookup_error=True
    await scheduler(c,seller,config).run_once()
    state=await c.snapshot("scheduled")
    assert state["lines"][0]["status"]=="UNKNOWN"
    assert as_utc((await job(sessions)).next_run_at)==clock()+timedelta(seconds=1)
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    clock.advance(1); seller.lookup_error=False
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    assert seller.lookup_calls==seller.reserve_calls*2 and not seller.cancel_calls
    assert (await c.snapshot("scheduled"))["lines"][0]["attempt_id"]==state["lines"][0]["attempt_id"]


async def test_exhausted_unknown_is_parked_without_automatic_cancellation(sessions,now):
    c,seller,clock,config=await setup(sessions,now,reconciliation_max_cycles=1)
    seller.lose_reserve=seller.lookup_error=True
    result=await scheduler(c,seller,config).run_once()
    assert result["parked"]==1 and not seller.cancel_calls
    assert (await job(sessions)).last_error=="MANUAL_REVIEW_REQUIRED"
    clock.advance(100)
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    assert len(seller.reserve_calls)==1


async def test_late_reservation_requires_fence_before_scheduled_successor(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    seller.late=True
    assert (await scheduler(c,seller,config).run_once())["waiting"]==1
    first=await c.snapshot("scheduled")
    assert first["lines"][0]["seller_id"]=="seller-2" and first["lines"][0]["attempt_count"]==2
    assert len(seller.fences)==len(seller.cancel_calls)==1
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    async with sessions() as session:
        attempts=list(await session.scalars(select(LineAttempt).order_by(LineAttempt.attempt_number)))
        assert [a.status for a in attempts]==["CANCELLED","DELIVERED"]


async def test_pending_cancellation_budget_parks_without_replacement(sessions,now):
    c,seller,clock,config=await setup(sessions,now,cancellation_max_cycles=2)
    seller.late,seller.cancel_mode=True,"pending"
    await scheduler(c,seller,config).run_once()
    assert len(seller.cancel_calls)==1 and len(seller.reserve_calls)==1
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    clock.advance(1)
    assert (await scheduler(c,seller,config).run_once())["parked"]==1
    assert seller.cancel_calls[0]==seller.cancel_calls[1]
    assert (await c.snapshot("scheduled"))["lines"][0]["status"]=="CANCELLATION_PENDING"
    assert (await job(sessions)).next_run_at is None


async def test_quote_failures_back_off_durably_then_recover(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    seller.quote_error=True
    await scheduler(c,seller,config).run_once()
    first=await job(sessions)
    assert first.failure_count==1 and as_utc(first.next_run_at)==clock()+timedelta(seconds=2)
    clock.advance(2)
    await scheduler(c,seller,config).run_once()
    second=await job(sessions)
    assert second.failure_count==2 and as_utc(second.next_run_at)==clock()+timedelta(seconds=4)
    seller.quote_error=False; clock.advance(4)
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    assert (await job(sessions)).failure_count==0 and len(seller.reserve_calls)==1


async def test_unexpected_failure_resumes_saved_attempt_without_new_key(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    seller.crash_reserve=True
    assert (await scheduler(c,seller,config).run_once())["failed"]==1
    state=await c.snapshot("scheduled")
    assert state["lines"][0]["status"]=="RESERVING"
    assert (await job(sessions)).last_error=="RuntimeError"
    clock.advance(2)
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    assert len(seller.reserve_calls)==1 and len(seller.lookup_calls)==1
    assert (await c.snapshot("scheduled"))["lines"][0]["operation_key"]==state["lines"][0]["operation_key"]


async def test_cancelled_task_leaves_committed_claim_then_resumes_when_due(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    entered=asyncio.Event(); original=seller.reserve
    async def blocked(*args):
        await original(*args)
        entered.set()
        await asyncio.Event().wait()
    seller.reserve=blocked; c.timeout=10
    first=asyncio.create_task(scheduler(c,seller,config).run_once())
    await asyncio.wait_for(entered.wait(),2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    claim=await job(sessions)
    assert claim.status=="RUNNING" and claim.run_count==1
    assert as_utc(claim.next_run_at)==clock()+timedelta(seconds=32)
    seller.reserve=original
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    clock.advance(32)
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    assert len(seller.reserve_calls)==1


async def test_manual_progress_wakes_parked_job_atomically(sessions,now):
    c,seller,clock,config=await setup(sessions,now,reconciliation_max_cycles=1)
    seller.lose_reserve=seller.lookup_error=True
    await scheduler(c,seller,config).run_once()
    assert (await job(sessions)).status=="PARKED"
    await ReplacementCoordinator(c,seller).run_order("scheduled")
    assert (await job(sessions)).status=="READY"
    assert (await scheduler(c,seller,config).run_once())["complete"]==1


async def test_scheduler_respects_batch_limit_stop_and_order_lock(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    class Never:
        async def run_locked(self,*args):
            pytest.fail("driver must not execute")
    @asynccontextmanager
    async def busy(_):
        raise OrderBusy()
        yield
    c.lock=busy
    worker=WorkflowScheduler(c,Never(),batch_size=1)
    assert await worker.run_once()=={"selected":1,"busy":1}
    assert (await job(sessions)).run_count==0
    stop=asyncio.Event(); stop.set()
    assert await worker.run_once(stop=stop)=={"selected":1,"stopped":1}


async def test_claim_is_committed_before_driver_and_order_timeout_is_persisted(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    class Hanging:
        async def run_locked(self,order_id):
            claim=await job(sessions)
            assert claim.status=="RUNNING" and claim.run_count==1 and claim.next_run_at
            await asyncio.Event().wait()
    worker=WorkflowScheduler(c,Hanging(),order_timeout_seconds=.02)
    assert (await worker.run_once())["failed"]==1
    assert (await job(sessions)).last_error=="TimeoutError"


async def test_duplicate_intake_does_not_reopen_completed_job(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    await scheduler(c,seller,config).run_once()
    async with sessions() as session:
        payload=(await session.get(Workflow,"scheduled")).payload
    event=OrderCreatedEvent.model_validate(dict(event_id=str(uuid4()),event_type="order.created",
        schema_version=1,producer="order",occurred_at=clock(),payload=payload))
    assert (await accept_order(event,sessions,workflow_topic="workflow.events")).outcome=="duplicate_order"
    saved=await job(sessions)
    assert saved.status=="COMPLETE" and saved.run_count==1 and saved.next_run_at is None
    assert await scheduler(c,seller,config).run_once()=={"selected":0}


async def test_job_insert_rolls_back_with_inbox_workflow_and_outbox(sessions,now):
    from sqlalchemy import event as sql_event,func
    from sqlalchemy.orm import Session
    def fail(session,*_):
        if any(isinstance(row,OutboxEvent) for row in session.new):
            raise RuntimeError("outbox failed")
    sql_event.listen(Session,"before_flush",fail)
    try:
        with pytest.raises(RuntimeError):
            await setup(sessions,now)
    finally:
        sql_event.remove(Session,"before_flush",fail)
    async with sessions() as session:
        for model in (Workflow,WorkflowJob,OutboxEvent):
            assert await session.scalar(select(func.count()).select_from(model))==0


async def test_due_scan_bounds_parallel_orders_and_stops_before_queued_jobs(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    async with sessions.begin() as session:
        for i in range(4):
            oid="other-"+str(i)
            session.add(Workflow(order_id=oid,payload={}))
            await session.flush()
            session.add(WorkflowJob(order_id=oid,next_run_at=clock()))
    class Counting:
        active=peak=calls=0
        async def run_locked(self,oid):
            self.active+=1; self.calls+=1; self.peak=max(self.peak,self.active)
            await asyncio.sleep(.02)
            self.active-=1
    driver=Counting()
    result=await WorkflowScheduler(c,driver,concurrency=2,batch_size=3).run_once()
    assert result["selected"]==3 and driver.calls==3 and driver.peak==2
    async with sessions() as session:
        assert len(list(await session.scalars(select(WorkflowJob).where(WorkflowJob.run_count==0))))==2


async def test_loopback_fixture_keeps_committed_ledger_when_reply_is_lost(now):
    from app.clients.reconciliation import HttpReconciliationClient
    from app.clients.cancellation import HttpCancellationClient
    from app.schemas import OrderLine
    from app.tools.restart_sellers import RestartSellers
    fixture=RestartSellers("reserve-case","cancel-case")
    urls=fixture.start()
    line=OrderLine(line_id="l1",part_id="bumper",quantity=1,needed_by=now+timedelta(days=2))
    try:
        async with httpx.AsyncClient(timeout=.5,trust_env=False) as http:
            client=HttpReconciliationClient(http,urls)
            with pytest.raises(httpx.ReadTimeout):
                await client.reserve("seller-1","reserve-case",line,"original-key")
            assert fixture.reserve_committed.is_set()
            reply=await client.lookup("seller-1","original-key")
            assert reply.status=="RESERVED" and len(fixture.records)==1
            reply=await client.reserve("seller-1","cancel-case",line,"cancelled-original")
            cancel=HttpCancellationClient(http,urls)
            with pytest.raises(httpx.ReadTimeout):
                await cancel.cancel("seller-1","cancel-case",line,"cancelled-original","cancel-key",reply.reservation_id)
            proof=await cancel.cancel("seller-1","cancel-case",line,"cancelled-original","cancel-key",reply.reservation_id)
            assert proof.status=="CANCELLED" and proof.fenced and len(fixture.fences)==1
    finally:
        await asyncio.to_thread(fixture.close)


async def test_expired_pending_order_completes_unfulfilled_without_a_write(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    clock.advance(3*24*3600)
    assert (await scheduler(c,seller,config).run_once())["complete"]==1
    assert (await c.snapshot("scheduled"))["status"]=="UNFULFILLED"
    assert not seller.reserve_calls


async def test_unexpected_error_budget_is_durable_and_requires_review(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    class Broken:
        async def run_locked(self,oid):
            raise RuntimeError("controlled driver fault")
    assert (await WorkflowScheduler(c,Broken(),max_failures=2).run_once())["failed"]==1
    clock.advance(2)
    assert (await WorkflowScheduler(c,Broken(),max_failures=2).run_once())["failed"]==1
    row=await job(sessions)
    assert row.failure_count==2 and row.status=="PARKED" and row.next_run_at is None
    clock.advance(100)
    assert await WorkflowScheduler(c,Broken()).run_once()=={"selected":0}


async def test_replacement_quote_outage_does_not_retry_twice_in_same_job_pass(sessions,now):
    c,seller,clock,config=await setup(sessions,now)
    seller.late=True
    original=seller.cancel
    async def cancel(*args):
        result=await original(*args)
        seller.quote_error=True
        return result
    seller.cancel=cancel
    await scheduler(c,seller,config).run_once()
    quoted=len(seller.quote_calls)
    assert len(seller.reserve_calls)==1 and len(seller.fences)==1
    assert await scheduler(c,seller,config).run_once()=={"selected":0}
    clock.advance(2)
    await scheduler(c,seller,config).run_once()
    assert len(seller.quote_calls)==quoted+1
    assert len(seller.reserve_calls)==1 and len(seller.cancel_calls)==1
