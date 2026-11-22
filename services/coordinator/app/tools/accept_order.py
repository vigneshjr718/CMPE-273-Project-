"""Local development entry point; not a Kafka consumer or public orders API."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.consumers.order_created import IngestionConflict, accept_order
from app.db import check_database, make_engine, make_sessions
from app.schemas import OrderCreatedEvent


async def run(path: Path) -> int:
    try:
        event = OrderCreatedEvent.model_validate_json(path.read_text())
    except (OSError, ValidationError, ValueError) as error:
        print(json.dumps({"error": "invalid_event_file",
                          "error_type": type(error).__name__}))
        return 2
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        result = await accept_order(
            event, make_sessions(engine), workflow_topic=settings.workflow_topic
        )
        print(json.dumps(asdict(result)))
        return 0
    except IngestionConflict as error:
        print(json.dumps({"error": "ingestion_conflict", "message": str(error)}))
        return 3
    except SQLAlchemyError as error:
        print(json.dumps({"error": "database_operation_failed",
                          "error_type": type(error).__name__,
                          "message": "Verify database configuration and migrations."}))
        return 4
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.file)))


if __name__ == "__main__":
    main()
