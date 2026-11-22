"""One bounded batch by default; --watch runs a separate relay process."""
import argparse
import asyncio
import json
import logging
import signal

from app.config import Settings
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging
from app.outbox.kafka import kafka_publisher
from app.outbox.locking import postgres_relay_lock
from app.outbox.publisher import OutboxRelay


async def run(args):
    settings = Settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url.get_secret_value())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)
    try:
        await check_database(engine)
        async with kafka_publisher(settings) as publisher:
            relay = OutboxRelay(
                make_sessions(engine), publisher,
                lock=lambda: postgres_relay_lock(engine),
                timeout_seconds=settings.kafka_operation_timeout_seconds,
            )
            while not stop.is_set():
                count = await relay.run_once(
                    limit=settings.outbox_batch_size, order_id=args.order_id, stop=stop)
                print(json.dumps({"published": count, "watch": args.watch}), flush=True)
                if not args.watch:
                    break
                try:
                    await asyncio.wait_for(stop.wait(), settings.outbox_poll_seconds)
                except TimeoutError:
                    pass
    finally:
        for name in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(name)
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--order-id", help="optional exact order filter")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as error:
        logging.getLogger("coordinator.outbox").error(
            "outbox_relay_stopped", extra={"failure_type": type(error).__name__})
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
