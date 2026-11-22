"""Consume one bounded Kafka batch, or watch; only durable intake commits offsets."""
import argparse
import asyncio
from dataclasses import asdict
import json
import signal

from app.config import Settings
from app.consumers.kafka_orders import KafkaOrderIntake, kafka_order_source
from app.db import check_database, make_engine, make_sessions
from app.logging_config import configure_logging


def make_intake(settings, consumer, assignment, sessions):
    return KafkaOrderIntake(
        consumer, assignment, sessions, incoming_topic=settings.incoming_topic,
        workflow_topic=settings.workflow_topic, batch_size=settings.incoming_batch_size,
        poll_ms=settings.incoming_poll_ms, timeout_seconds=settings.kafka_operation_timeout_seconds,
        database_timeout_seconds=settings.incoming_database_timeout_seconds,
        max_event_bytes=settings.incoming_max_event_bytes)


async def run(*, watch=False, bootstrap_earliest=False):
    settings = Settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings.database_url.get_secret_value())
    stop, loop, signals = asyncio.Event(), asyncio.get_running_loop(), []
    summary = {"batches": 0, "processed": 0, "created": 0,
               "duplicate_event": 0, "duplicate_order": 0, "stopped": False}
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            signals.append(sig)
        await asyncio.wait_for(check_database(engine), settings.dependency_timeout_seconds)
        async with kafka_order_source(settings, bootstrap_earliest=bootstrap_earliest) as source:
            intake = make_intake(settings, *source, make_sessions(engine))
            while not stop.is_set():
                batch = asdict(await intake.run_once(stop=stop))
                summary["batches"] += 1
                for name, value in batch.items():
                    if name == "stopped":
                        summary[name] = value
                    else:
                        summary[name] += value
                if not watch or batch["stopped"]:
                    break
            summary["stopped"] = stop.is_set()
        return summary
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--bootstrap-earliest", action="store_true",
                        help="initialize only partitions without a committed group offset")
    args = parser.parse_args()
    try:
        result = asyncio.run(run(watch=args.watch, bootstrap_earliest=args.bootstrap_earliest))
    except Exception as error:
        print(json.dumps({"error": "order_consumer_failed", "error_type": type(error).__name__,
            "action": "Inspect the safe topic/partition/offset log; restart from broker offsets after resolving the cause."}))
        raise SystemExit(1) from None
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
