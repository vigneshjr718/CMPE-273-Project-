from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base


def make_engine(url: str):
    return create_async_engine(url, pool_pre_ping=True)


def make_sessions(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_tables(engine):
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

