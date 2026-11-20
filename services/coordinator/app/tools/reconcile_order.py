"""Manually reconcile one accepted order against the configured real seller APIs."""
import argparse
import asyncio
import json

import httpx
from sqlalchemy.exc import SQLAlchemyError

from app.clients.reconciliation import HttpReconciliationClient
from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.saga.locking import OrderBusy, postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.saga.reconciliation import Reconciler


async def run(order_id):
    settings = Settings()
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        async with httpx.AsyncClient(timeout=settings.seller_timeout_seconds) as http:
            coordinator = Coordinator(
                make_sessions(engine),
                HttpReconciliationClient(http, settings.seller_urls),
                list(settings.seller_urls),
                lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic,
                timeout_seconds=settings.seller_timeout_seconds,
                concurrency=settings.max_seller_concurrency,
            )
            result = await Reconciler(
                coordinator, max_cycles=settings.reconciliation_max_cycles,
                backoff_seconds=settings.reconciliation_backoff_seconds,
            ).run_order(order_id)
            print(json.dumps(result))
            return 0
    except OrderBusy:
        print(json.dumps({"error": "order_busy"}))
        return 75
    except SQLAlchemyError as error:
        print(json.dumps({"error": "database_operation_failed",
                          "error_type": type(error).__name__}))
        return 4
    except (ValueError, RuntimeError) as error:
        print(json.dumps({"error": "reconciliation_failed",
                          "error_type": type(error).__name__, "message": str(error)}))
        return 3
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id", required=True)
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args.order_id)))


if __name__ == "__main__":
    main()
