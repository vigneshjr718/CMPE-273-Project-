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
