import asyncio

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.config import Settings
from app.models import Base

target_metadata = Base.metadata


def database_url() -> str:
    # Tests may pass a URL via Config.set_main_option("sqlalchemy.url", ...).
    return (context.config.get_main_option("sqlalchemy.url")
            or Settings().database_url.get_secret_value())


def configure(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def offline():
    context.configure(url=database_url(), target_metadata=target_metadata,
                      literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = create_async_engine(database_url(), poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(configure)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    offline()
else:
    asyncio.run(online())
