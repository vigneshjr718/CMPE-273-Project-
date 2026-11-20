"""Serialize cooperative relays without holding a SQL transaction over Kafka."""
from contextlib import asynccontextmanager
import hashlib

from sqlalchemy import text


class RelayBusy(RuntimeError):
    pass


@asynccontextmanager
async def postgres_relay_lock(engine):
    if engine.dialect.name != "postgresql":
        raise RuntimeError("production outbox publishing requires PostgreSQL locking")
    key = int.from_bytes(hashlib.sha256(b"partstream-outbox-relay:v1").digest()[:8],
                         "big", signed=True)
    async with engine.connect() as connection:
        acquired = False
        try:
            acquired = bool((await connection.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
            )).scalar_one())
            await connection.commit()
            if not acquired:
                raise RelayBusy("another outbox relay is active")
            yield
        finally:
            if acquired:
                try:
                    unlocked = (await connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": key}
                    )).scalar_one()
                    await connection.commit()
                    if not unlocked:
                        raise RuntimeError("outbox lock ownership was lost")
                except BaseException:
                    await connection.invalidate()
                    raise
