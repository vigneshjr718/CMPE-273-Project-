import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import inspect, select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.db import check_database
from app.models import OutboxEvent, Workflow

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def migrated_database(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'coordinator.db'}"
    env = {**os.environ, "COORDINATOR_DATABASE_URL": url}
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                   cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    return url


async def test_upgrade_readiness_and_downgrade(migrated_database):
    engine = create_async_engine(migrated_database)
    try:
        await check_database(engine)
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda c: inspect(c).get_table_names())
        assert {"workflows", "line_attempts", "processed_events", "outbox"} <= set(names)
    finally:
        await engine.dispose()
    subprocess.run([sys.executable, "-m", "alembic", "downgrade", "base"],
                   cwd=ROOT, env={**os.environ, "COORDINATOR_DATABASE_URL": migrated_database},
                   check=True, capture_output=True, text=True)
    engine = create_async_engine(migrated_database)
    try:
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda c: inspect(c).get_table_names())
        assert set(names) == {"alembic_version"}
    finally:
        await engine.dispose()


async def test_local_transaction_rolls_back_workflow_and_outbox(migrated_database):
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine)
    try:
        with pytest.raises(RuntimeError):
            async with sessions.begin() as session:
                session.add(Workflow(order_id="o1", payload={}))
                await session.flush()
                session.add(OutboxEvent(order_id="o1", topic="workflow.events",
                                       event_type="line.reserved", state_version=1,
                                       payload={}))
                await session.flush()
                raise RuntimeError("simulate failure before commit")
        async with sessions() as session:
            assert await session.get(Workflow, "o1") is None
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
    finally:
        await engine.dispose()


async def test_order_and_event_version_uniqueness(migrated_database):
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine)
    try:
        async with sessions.begin() as session:
            session.add(Workflow(order_id="o1", payload={}))
        with pytest.raises(IntegrityError):
            async with sessions.begin() as session:
                session.add(Workflow(order_id="o1", payload={}))
        async with sessions.begin() as session:
            session.add(OutboxEvent(order_id="o1", topic="workflow.events",
                                   event_type="line.reserved", state_version=1, payload={}))
        with pytest.raises(IntegrityError):
            async with sessions.begin() as session:
                session.add(OutboxEvent(order_id="o1", topic="workflow.events",
                                       event_type="line.replaced", state_version=1, payload={}))
    finally:
        await engine.dispose()


def test_postgres_migration_sql_compiles():
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=ROOT, env={**os.environ, "COORDINATOR_DATABASE_URL":
                      "postgresql+asyncpg://u:p@localhost/coordinator_db"},
        check=True, capture_output=True, text=True,
    )
    assert "CREATE TABLE workflows" in result.stdout
    assert "CREATE TABLE outbox" in result.stdout
    assert "CREATE TABLE delivery_checks" in result.stdout
    assert "CREATE TABLE cancellation_checks" in result.stdout
    assert "CREATE TABLE workflow_jobs" in result.stdout


async def test_upgrade_from_reservations_preserves_saved_business_state(tmp_path):
    from datetime import datetime, timezone
    from app.models import DeliveryCheck, LineAttempt, WorkflowLine

    url = f"sqlite+aiosqlite:///{tmp_path / 'existing-reservation.db'}"
    env = {**os.environ, "COORDINATOR_DATABASE_URL": url}
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "0002_reservation_flow"],
                   cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    try:
        async with sessions.begin() as session:
            session.add(Workflow(order_id="saved-order", payload={}, status="RESERVED", state_version=5))
            await session.flush()
            session.add(LineAttempt(
                attempt_id="saved-attempt", order_id="saved-order", line_id="line-1",
                seller_id="seller-1", attempt_number=1, operation_key="saved-key",
                status="RESERVED", reservation_id="saved-reservation"))
            session.add(WorkflowLine(
                order_id="saved-order", line_id="line-1", part_id="bumper", quantity=1,
                needed_by=datetime(2030, 1, 2, tzinfo=timezone.utc), status="RESERVED",
                current_attempt_id="saved-attempt", attempt_count=1))
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                       cwd=ROOT, env=env, check=True, capture_output=True, text=True)
        await check_database(engine)
        async with sessions() as session:
            workflow = await session.get(Workflow, "saved-order")
            assert workflow.status == "RESERVED" and workflow.state_version == 5
            attempt = await session.get(LineAttempt, "saved-attempt")
            assert attempt.operation_key == "saved-key" and attempt.reservation_id == "saved-reservation"
            assert await session.scalar(select(func.count()).select_from(DeliveryCheck)) == 0
    finally:
        await engine.dispose()


async def test_cancellation_upgrade_preserves_checkpoint5_delivery_and_outbox(tmp_path):
    from datetime import datetime, timezone
    from app.models import CancellationCheck, DeliveryCheck, LineAttempt, WorkflowLine

    url = f"sqlite+aiosqlite:///{tmp_path / 'checkpoint5.db'}"
    env = {**os.environ, "COORDINATOR_DATABASE_URL": url}
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "0003_delivery_polling"],
                   cwd=ROOT, env=env, check=True, capture_output=True, text=True)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine)
    try:
        async with sessions.begin() as session:
            session.add(Workflow(order_id="saved-cp5", payload={}, status="RESERVED", state_version=6))
            await session.flush()
            session.add(LineAttempt(attempt_id="saved-attempt", order_id="saved-cp5", line_id="l1",
                seller_id="seller-1", attempt_number=1, operation_key="existing-key",
                status="RESERVED", reservation_id="existing-reservation"))
            session.add(WorkflowLine(order_id="saved-cp5", line_id="l1", part_id="bumper", quantity=1,
                needed_by=datetime(2030, 1, 2, tzinfo=timezone.utc), status="RESERVED",
                current_attempt_id="saved-attempt", attempt_count=1))
            session.add(OutboxEvent(event_id="existing-event", order_id="saved-cp5", topic="workflow.events",
                event_type="line.delivery_updated", state_version=6, payload={"saved": "unchanged"}))
            await session.flush()
            session.add(DeliveryCheck(attempt_id="saved-attempt", order_id="saved-cp5", line_id="l1",
                poll_count=2, failure_count=0, status_version=1, last_payload={"status": "IN_TRANSIT"},
                next_poll_at=datetime(2030, 1, 1, tzinfo=timezone.utc)))
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"],
                       cwd=ROOT, env=env, check=True, capture_output=True, text=True)
        await check_database(engine)
        async with sessions() as session:
            assert (await session.get(Workflow, "saved-cp5")).state_version == 6
            assert (await session.get(LineAttempt, "saved-attempt")).operation_key == "existing-key"
            check = await session.get(DeliveryCheck, "saved-attempt")
            assert check.poll_count == 2 and check.next_poll_at and check.status_version == 1
            assert (await session.get(OutboxEvent, "existing-event")).payload == {"saved": "unchanged"}
            assert await session.scalar(select(func.count()).select_from(CancellationCheck)) == 0
    finally:
        await engine.dispose()


async def test_scheduler_upgrade_backfills_jobs_without_changing_saved_evidence(tmp_path):
    from datetime import datetime,timedelta,timezone
    from app.models import CancellationCheck,LineAttempt,WorkflowJob
    url=f"sqlite+aiosqlite:///{tmp_path / 'checkpoint7.db'}"
    env={**os.environ,"COORDINATOR_DATABASE_URL":url}
    subprocess.run([sys.executable,"-m","alembic","upgrade","0004_cancellation"],
                   cwd=ROOT,env=env,check=True,capture_output=True,text=True)
    engine=create_async_engine(url); sessions=async_sessionmaker(engine)
    saved=datetime(2030,1,1,tzinfo=timezone.utc)
    try:
        async with sessions.begin() as session:
            session.add(Workflow(order_id="pending-fence",payload={},status="RECONCILIATION_REQUIRED",
                                 state_version=8,updated_at=saved))
            session.add(Workflow(order_id="completed",payload={},status="FULFILLED",state_version=6))
            await session.flush()
            session.add(LineAttempt(attempt_id="saved-attempt",order_id="pending-fence",line_id="l1",
                seller_id="seller-1",attempt_number=1,operation_key="original-key",status="CANCELLATION_PENDING"))
            await session.flush()
            session.add(CancellationCheck(attempt_id="saved-attempt",order_id="pending-fence",line_id="l1",
                cancellation_key="stable-cancel-key",status="PENDING",request_count=1,
                next_retry_at=saved+timedelta(seconds=4)))
            session.add(OutboxEvent(event_id="saved-outgoing",order_id="pending-fence",topic="workflow.events",
                event_type="line.cancelling",state_version=8,payload={"saved":"unchanged"}))
        subprocess.run([sys.executable,"-m","alembic","upgrade","head"],cwd=ROOT,env=env,
                       check=True,capture_output=True,text=True)
        await check_database(engine)
        async with sessions() as session:
            job=await session.get(WorkflowJob,"pending-fence")
            assert job.status=="READY" and job.run_count==0 and job.next_run_at
            terminal=await session.get(WorkflowJob,"completed")
            assert terminal.status=="COMPLETE" and terminal.next_run_at is None
            assert (await session.get(Workflow,"pending-fence")).state_version==8
            assert (await session.get(LineAttempt,"saved-attempt")).operation_key=="original-key"
            check=await session.get(CancellationCheck,"saved-attempt")
            assert check.request_count==1 and check.cancellation_key=="stable-cancel-key" and check.next_retry_at
            assert (await session.get(OutboxEvent,"saved-outgoing")).payload=={"saved":"unchanged"}
    finally:
        await engine.dispose()
