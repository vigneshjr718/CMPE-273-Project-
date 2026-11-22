"""Kill only owned worker processes, then recover real PostgreSQL workflow state."""
import argparse
import asyncio
from datetime import timedelta
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

from sqlalchemy import delete, select

from app.config import Settings
from app.consumers.order_created import accept_order
from app.db import check_database, make_engine, make_sessions
from app.models import (CancellationCheck, DeliveryCheck, LineAttempt, OutboxEvent,
                        ProcessedEvent, Workflow, WorkflowJob, WorkflowLine)
from app.schemas import OrderCreatedEvent, utc_now
from app.tools.restart_sellers import RestartSellers


async def run_demo(settings, *, cleanup=False):
    token=uuid4().hex[:12]
    ids=["demo-restart-"+token+suffix for suffix in ("-reserve","-cancel")]
    engine=make_engine(settings.database_url.get_secret_value())
    if engine.dialect.name!="postgresql":
        await engine.dispose()
        raise ValueError("restart demo requires migrated real PostgreSQL")
    sessions=make_sessions(engine)
    fixture=RestartSellers(*ids)
    urls=fixture.start()
    children=[]
    async def launch(oid):
        env={**os.environ,"COORDINATOR_DATABASE_URL":settings.database_url.get_secret_value(),
            "COORDINATOR_SELLER_URLS":json.dumps(urls),"COORDINATOR_SCHEDULER_POLL_SECONDS":"0.05",
            "COORDINATOR_SCHEDULER_ORDER_TIMEOUT_SECONDS":"2","COORDINATOR_SCHEDULER_RETRY_SECONDS":"0.1",
            "COORDINATOR_SCHEDULER_CONCURRENCY":"1","COORDINATOR_SELLER_TIMEOUT_SECONDS":"8",
            "COORDINATOR_DELIVERY_POLL_SECONDS":"0.1","COORDINATOR_RECONCILIATION_BACKOFF_SECONDS":"0.1",
            "COORDINATOR_CANCELLATION_BACKOFF_SECONDS":"0.1","COORDINATOR_WORKFLOW_TOPIC":settings.workflow_topic}
        process=await asyncio.create_subprocess_exec(sys.executable,"-m","app.tools.run_workflows",
            "--watch","--order-id",oid,cwd=Path(__file__).resolve().parents[2],env=env,
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
        output=asyncio.create_task(process.communicate())
        children.append((process,output))
        return process,output
    async def stop(child, *, kill=False):
        process,output=child
        if process.returncode is None:
            process.kill() if kill else process.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(output),5)
        except TimeoutError:
            process.kill()
            await asyncio.wait_for(asyncio.shield(output),5)
        if not kill and process.returncode!=0:
            raise RuntimeError("recovery worker did not stop gracefully")
    async def rows(oid):
        async with sessions() as session:
            workflow=await session.get(Workflow,oid)
            attempts=list(await session.scalars(select(LineAttempt).where(
                LineAttempt.order_id==oid).order_by(LineAttempt.attempt_number)))
            job=await session.get(WorkflowJob,oid)
            line=await session.get(WorkflowLine,(oid,"line-1"))
            return workflow,attempts,job,line
    async def fulfilled(oid,child):
        async with asyncio.timeout(20):
            while True:
                values=await rows(oid)
                if values[0].status=="FULFILLED" and values[2].status=="COMPLETE":
                    return values
                if child[0].returncode is not None:
                    raise RuntimeError("recovery worker exited before completion")
                await asyncio.sleep(.05)
    async def accepted(oid):
        now=utc_now()
        event=OrderCreatedEvent.model_validate({"event_id":str(uuid4()),"event_type":"order.created",
            "schema_version":1,"producer":"order","occurred_at":now,"payload":{"order_id":oid,
            "lines":[{"line_id":"line-1","part_id":"bumper","quantity":1,"needed_by":now+timedelta(days=2)}]}})
        await accept_order(event,sessions,workflow_topic=settings.workflow_topic)
    try:
        await check_database(engine)
        evidence=[]
        for oid,signal,expected in ((ids[0],fixture.reserve_committed,"RESERVING"),
                                    (ids[1],fixture.cancel_committed,"CANCELLATION_PENDING")):
            await accepted(oid)
            first=await launch(oid)
            if not await asyncio.to_thread(signal.wait,10):
                raise TimeoutError("controlled external commit boundary not reached")
            before,attempts,before_job,line=await rows(oid)
            assert line.status==expected and before_job.status=="RUNNING"
            original_key,original_id=attempts[0].operation_key,attempts[0].attempt_id
            await stop(first,kill=True)
            assert first[0].returncode!=0
            fresh=await launch(oid)
            after,attempts,job,line=await fulfilled(oid,fresh)
            await stop(fresh)
            assert first[0].pid!=fresh[0].pid
            assert attempts[0].operation_key==original_key and attempts[0].attempt_id==original_id
            assert job.run_count>=2 and job.next_run_at is None
            if oid==ids[0]:
                assert len(attempts)==1 and attempts[0].status=="DELIVERED"
                assert sum(o==oid for o,_ in fixture.reserve_calls)==1
                assert original_key in fixture.lookup_calls
            else:
                assert [a.status for a in attempts]==["CANCELLED","DELIVERED"]
                calls=[call for call in fixture.cancel_calls if call[0]==oid]
                assert len(calls)==2 and calls[0]==calls[1]
                assert sum(o==oid for o,_ in fixture.reserve_calls)==2
                assert original_key in fixture.fences
            evidence.append(dict(order_id=oid,interrupted_line_status=expected,
                killed_pid=first[0].pid,recovered_pid=fresh[0].pid,
                killed_returncode=first[0].returncode,recovered_returncode=fresh[0].returncode,
                original_attempt_preserved=True,original_key_preserved=True,
                final_status=after.status,final_version=after.state_version,
                attempt_count=len(attempts),attempt_statuses=[a.status for a in attempts],
                job_status=job.status,job_run_count=job.run_count))
        assert len(fixture.fences)==1
        return dict(infrastructure="real_postgresql",seller_mode="controlled_loopback_http_parent_ledger",
            restart_mode="owned_worker_os_process_kill_and_fresh_start",cases=evidence,
            reservation_http_calls=len(fixture.reserve_calls),logical_reservations=len(fixture.records),
            cancellation_http_calls=len(fixture.cancel_calls),terminal_fences=len(fixture.fences),cleanup=cleanup)
    finally:
        for process,output in children:
            if process.returncode is None:
                process.kill()
            await asyncio.wait_for(asyncio.shield(output),5)
        await asyncio.to_thread(fixture.close)
        try:
            if cleanup:
                async with engine.begin() as connection:
                    for model in (WorkflowJob,CancellationCheck,DeliveryCheck,OutboxEvent,ProcessedEvent,
                                  LineAttempt,WorkflowLine,Workflow):
                        await connection.execute(delete(model).where(model.order_id.in_(ids)))
        finally:
            await engine.dispose()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleanup",action="store_true",help="remove only the generated demo order records")
    args=parser.parse_args()
    try:
        result=asyncio.run(run_demo(Settings(),cleanup=args.cleanup))
    except Exception as error:
        print(json.dumps({"error":"restart_demo_failed","error_type":type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result,indent=2))


if __name__=="__main__":
    main()
