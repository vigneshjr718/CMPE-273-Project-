"""The Alembic migration creates exactly the tables/columns the ORM models expect."""
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

from app.db import SCHEMA_REVISION
from app.models import Base

SERVICE_ROOT = Path(__file__).resolve().parents[1]


def test_migration_matches_models_and_downgrades(tmp_path):
    db = tmp_path / "migrated.db"
    config = Config(str(SERVICE_ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{db}")
    command.upgrade(config, "head")

    engine = create_engine(f"sqlite:///{db}")
    inspector = inspect(engine)
    for table in Base.metadata.sorted_tables:
        migrated = {c["name"] for c in inspector.get_columns(table.name)}
        assert migrated == {c.name for c in table.columns}, table.name
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar() \
            == SCHEMA_REVISION

    command.downgrade(config, "base")
    assert set(inspect(engine).get_table_names()) == {"alembic_version"}
    engine.dispose()
