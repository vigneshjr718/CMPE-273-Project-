from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

SCHEMA_REVISION = "0002_dead_letter"


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_pre_ping=True, echo=False)


def make_sessions(engine: AsyncEngine):
    return async_sessionmaker(engine, expire_on_commit=False)


async def check_database(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT version_num FROM alembic_version"))
        if result.scalar_one() != SCHEMA_REVISION:
            raise RuntimeError("tracker migrations are not current")
