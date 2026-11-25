"""Fixtures for DB-backed tests.

Set TEST_DATABASE_URL to a THROWAWAY Postgres database (tables get truncated), e.g.

    docker compose up -d postgres
    createdb -h localhost -U postgres seller_test
    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/seller_test pytest

Without it, DB tests are skipped and only tests/test_unit.py runs.
"""
from __future__ import annotations

import os

import pytest

DB_URL = os.environ.get("TEST_DATABASE_URL")
ADMIN = {"X-Admin-Token": "test-admin"}


@pytest.fixture(scope="session")
def settings():
    if not DB_URL:
        pytest.skip("TEST_DATABASE_URL not set")
    from app.config import Settings

    return Settings(
        database_url=DB_URL, seller_id="seller-1", delivery_hours=48, admin_token="test-admin",
        seed_on_startup=False, db_pool_size=25, db_max_overflow=25,
    )


@pytest.fixture(scope="session")
def engine(settings):
    from app.db import make_engine
    from app.migrate import run_migrations

    eng = make_engine(settings)
    run_migrations(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def db(engine):
    """Empty tables before each test."""
    from sqlalchemy import text

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE idempotency_records, reservations, inventory"))
    return engine


@pytest.fixture()
def make_client(settings, db):
    """Factory: each call = a fresh app instance (simulates a process restart)."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    clients = []

    def _make():
        c = TestClient(create_app(settings, run_startup=False))
        c.__enter__()
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.__exit__(None, None, None)


@pytest.fixture()
def client(make_client):
    return make_client()


@pytest.fixture()
def stock(db):
    """stock("PART-X", 5) creates/restocks; stock("PART-X") reads the row."""
    from app.services import inventory

    def _stock(part_id: str, add: int | None = None) -> dict:
        if add is not None:
            return inventory.restock(db, part_id, add, None, 1000)
        rows = inventory.list_inventory(db, part_id)
        return rows[0] if rows else {}

    return _stock
