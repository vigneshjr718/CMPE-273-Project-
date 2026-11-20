"""Run one persisted order. --demo uses controlled synthetic seller responses."""
import argparse
import asyncio
import json

import httpx
from sqlalchemy.exc import SQLAlchemyError

from app.clients.reservations import HttpReservationClient
from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.saga.locking import OrderBusy, postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.tools.demo_sellers import DemoSellers


async def run(order_id, demo):
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        async with httpx.AsyncClient(timeout=settings.seller_timeout_seconds) as http:
            client = DemoSellers() if demo else HttpReservationClient(http, settings.seller_urls)
            coordinator = Coordinator(
                make_sessions(engine), client, list(settings.seller_urls),
                lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic,
                timeout_seconds=settings.seller_timeout_seconds,
                concurrency=settings.max_seller_concurrency,
            )
            result = await coordinator.run_order(order_id)
            result["seller_mode"] = "controlled_demo" if demo else "real_http"
            print(json.dumps(result))
            return 0
    except OrderBusy:
        print(json.dumps({"error": "order_busy", "message": "Retry this order later."}))
        return 75
    except SQLAlchemyError as error:
        print(json.dumps({"error": "database_operation_failed",
                          "error_type": type(error).__name__}))
        return 4
    except (ValueError, RuntimeError) as error:
        print(json.dumps({"error": "workflow_execution_failed",
                          "error_type": type(error).__name__, "message": str(error)}))
        return 3
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id", required=True)
    parser.add_argument("--demo", action="store_true",
                        help="use synthetic in-process sellers, not Package C")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.order_id, args.demo)))


if __name__ == "__main__":
    main()
