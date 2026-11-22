from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

SCHEMA_REVISION = "0003_delivery_polling"


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True, echo=False)


def make_sessions(engine: AsyncEngine):
    return async_sessionmaker(engine, expire_on_commit=False)


async def check_database(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
        result = await connection.execute(text("SELECT version_num FROM alembic_version"))
        if result.scalar_one() != SCHEMA_REVISION:
            raise RuntimeError("coordinator migrations are not current")
