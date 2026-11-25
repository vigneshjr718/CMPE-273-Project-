"""DB engine. Sync SQLAlchemy + psycopg 3; FastAPI runs sync endpoints in a threadpool."""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from app.config import Settings


def make_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.sqlalchemy_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            # A stuck lock wait must not hang a request forever; a statement timeout
            # rolls the transaction back, so the caller sees a clean 503 (nothing committed).
            "options": f"-c statement_timeout={settings.db_statement_timeout_ms}",
            "application_name": settings.seller_id,
        },
    )
