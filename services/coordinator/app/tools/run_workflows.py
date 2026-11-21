"""Run one bounded durable workflow scan, or watch until SIGINT/SIGTERM."""
import argparse
import asyncio
import json
import signal

import httpx

from app.clients.cancellation import HttpCancellationClient
from app.clients.delivery import HttpDeliveryClient
from app.clients.reconciliation import HttpReconciliationClient
from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging
from app.pollers.delivery_poller import DeliveryPoller
from app.saga.locking import postgres_order_lock
from app.saga.orchestrator import Coordinator
from app.saga.reconciliation import Reconciler
from app.saga.replacement import ReplacementCoordinator
from app.workers.workflows import WorkflowDriver, WorkflowScheduler


def make_scheduler(settings, coordinator, delivery_client, cancellation_client):
    driver = WorkflowDriver(coordinator, Reconciler(coordinator,
        max_cycles=settings.reconciliation_max_cycles, backoff_seconds=settings.reconciliation_backoff_seconds),
        DeliveryPoller(coordinator, delivery_client, interval_seconds=settings.delivery_poll_seconds,
                       concurrency=settings.delivery_poll_concurrency),
        ReplacementCoordinator(coordinator, cancellation_client, max_cycles=settings.cancellation_max_cycles,
                               backoff_seconds=settings.cancellation_backoff_seconds))
    return WorkflowScheduler(coordinator, driver, batch_size=settings.scheduler_batch_size,
        concurrency=settings.scheduler_concurrency, order_timeout_seconds=settings.scheduler_order_timeout_seconds,
        retry_seconds=settings.scheduler_retry_seconds, max_failures=settings.scheduler_max_failures)


async def run(args):
    settings = Settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url.get_secret_value())
    stop, loop = asyncio.Event(), asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await asyncio.wait_for(check_database(engine), settings.dependency_timeout_seconds)
        if engine.dialect.name != "postgresql":
            raise ValueError("workflow worker requires PostgreSQL order locking")
        async with httpx.AsyncClient(timeout=settings.seller_timeout_seconds, trust_env=False) as http:
            coordinator = Coordinator(make_sessions(engine), HttpReconciliationClient(http, settings.seller_urls),
                list(settings.seller_urls), lock=lambda oid: postgres_order_lock(engine, oid),
                workflow_topic=settings.workflow_topic, timeout_seconds=settings.seller_timeout_seconds,
                concurrency=settings.max_seller_concurrency)
            scheduler = make_scheduler(settings, coordinator, HttpDeliveryClient(http, settings.seller_urls),
                                       HttpCancellationClient(http, settings.seller_urls))
            while not stop.is_set():
                result = await scheduler.run_once(order_id=args.order_id, stop=stop)
                print(json.dumps(result), flush=True)
                if not args.watch:
                    break
                try:
                    await asyncio.wait_for(stop.wait(), settings.scheduler_poll_seconds)
                except TimeoutError:
                    pass
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--order-id", help="optional exact order filter, including synthetic demos")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as error:
        print(json.dumps({"error": "workflow_worker_stopped", "error_type": type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
