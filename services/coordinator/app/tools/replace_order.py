"""Request fencing for late/unknown attempts, then safely replace confirmed cancellations."""
import argparse
import asyncio
import json

import httpx

from app.clients.cancellation import HttpCancellationClient
from app.clients.reservations import HttpReservationClient
from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.saga.replacement import ReplacementCoordinator


async def run(order_id):
    settings = Settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url.get_secret_value())
    try:
        await check_database(engine)
        async with httpx.AsyncClient(timeout=settings.seller_timeout_seconds) as http:
            c = Coordinator(
                make_sessions(engine), HttpReservationClient(http, settings.seller_urls),
                list(settings.seller_urls), lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic,
                timeout_seconds=settings.seller_timeout_seconds,
                concurrency=settings.max_seller_concurrency,
            )
            return await ReplacementCoordinator(
                c, HttpCancellationClient(http, settings.seller_urls),
                max_cycles=settings.cancellation_max_cycles,
                backoff_seconds=settings.cancellation_backoff_seconds,
            ).run_order(order_id)
    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args.order_id))
    except Exception as error:
        print(json.dumps({"error": "replacement_failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
