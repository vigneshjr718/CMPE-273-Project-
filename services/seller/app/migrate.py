"""Tiny SQL migration runner: applies migrations/NNN_*.sql once each, in order.

Safe with several replicas starting at once (advisory lock). Each file runs in
its own transaction together with its schema_migrations row.

    python -m app.migrate          # apply pending migrations and exit
"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.engine import Engine

log = logging.getLogger("seller.migrate")

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_LOCK_ID = 727_001  # arbitrary, constant per service


def pending_files(applied: set[str]) -> list[Path]:
    return [p for p in sorted(MIGRATIONS_DIR.glob("*.sql")) if p.name not in applied]


def run_migrations(engine: Engine) -> list[str]:
    raw = engine.raw_connection()  # raw psycopg connection: multi-statement files need it
    try:
        dbapi = raw.driver_connection
        dbapi.autocommit = True
        with dbapi.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (_LOCK_ID,))
            try:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS schema_migrations ("
                    " version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
                )
                cur.execute("SELECT version FROM schema_migrations")
                applied = {row[0] for row in cur.fetchall()}
                done: list[str] = []
                for path in pending_files(applied):
                    sql = path.read_text(encoding="utf-8")
                    with dbapi.transaction():
                        cur.execute(sql)  # no params -> simple protocol, multi-statement OK
                        cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))
                    log.info("migration applied", extra={"migration": path.name})
                    done.append(path.name)
                return done
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_ID,))
    finally:
        # Never hand an autocommit connection back to SQLAlchemy's pool.
        try:
            raw.driver_connection.autocommit = False
        except Exception:  # broken connection: drop it instead of pooling it
            raw.invalidate()
        raw.close()


if __name__ == "__main__":
    from app.config import get_settings
    from app.db import make_engine
    from app.logging_setup import setup_logging

    s = get_settings()
    setup_logging(s)
    applied_now = run_migrations(make_engine(s))
    print(f"applied: {applied_now or 'nothing (up to date)'}")
