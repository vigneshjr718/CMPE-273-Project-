"""One active order driver across processes on the same PostgreSQL database."""
from contextlib import asynccontextmanager
import hashlib

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


class OrderBusy(RuntimeError):
    pass


@asynccontextmanager
async def postgres_order_lock(engine: AsyncEngine, order_id: str):
    if engine.dialect.name != "postgresql":
        raise RuntimeError("production order execution requires PostgreSQL locking")
    # Namespace plus stable signed 64-bit key. Collisions serialize harmlessly.
    key = int.from_bytes(
        hashlib.sha256(("partstream-order:" + order_id).encode()).digest()[:8],
        "big", signed=True,
    )
    async with engine.connect() as connection:
        acquired = False
        try:
            acquired = bool((await connection.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": key}
            )).scalar_one())
            await connection.commit()
            if not acquired:
                raise OrderBusy("another worker is processing this order")
            yield
        finally:
            if acquired:
                try:
                    unlocked = (await connection.execute(
                        text("SELECT pg_advisory_unlock(:key)"), {"key": key}
                    )).scalar_one()
                    await connection.commit()
                    if not unlocked:
                        raise RuntimeError("order lock ownership was lost")
                except BaseException:
                    # Never return a connection with a possibly held session lock.
                    await connection.invalidate()
                    raise
