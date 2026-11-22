"""Run D's real projection in a fresh interpreter, avoiding the shared `app` name.

Invoked by test_tracker_compatibility.py with cwd/PYTHONPATH set to D's service.
Uses only a temporary SQLite database; never uses D's configured database.
"""
import asyncio
import json
from pathlib import Path
import sys
import tempfile

from jsonschema import Draft202012Validator, FormatChecker
import yaml

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base, StatusHistory
from app.projection import Projector
from app.views import load_order_view


async def probe(corpus):
    with tempfile.TemporaryDirectory(prefix="tracker-projection-") as directory:
        engine = create_async_engine(f"sqlite+aiosqlite:///{Path(directory) / 'tracker.db'}")
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            projector = Projector(sessions, order_topic="order.created", workflow_topic="workflow.events")
            spec = yaml.safe_load((Path.cwd().parents[1] / "contracts/openapi/status-api.yaml").read_text())
            response_schema = {"$ref": "#/components/schemas/OrderStatus", "components": spec["components"]}
            validator = Draft202012Validator(response_schema, format_checker=FormatChecker())
            views, outcomes = [], []
            for offset, item in enumerate(corpus["messages"]):
                result = await projector.handle(item["topic"], 0, offset,
                                                json.dumps(item["event"]).encode())
                outcomes.append(str(result))
                async with sessions() as session:
                    view = await load_order_view(session, corpus["order_id"])
                    rendered = view.model_dump(mode="json") if view else None
                    if rendered:
                        validator.validate(rendered)
                    views.append(rendered)
            async with sessions() as session:
                history = await session.scalar(select(func.count()).select_from(StatusHistory))
            return {"views": views, "outcomes": outcomes, "history": history}
        finally:
            await engine.dispose()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(probe(json.load(sys.stdin)))))
