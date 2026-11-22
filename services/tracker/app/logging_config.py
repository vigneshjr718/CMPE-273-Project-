import json
import logging
from datetime import datetime, timezone

CONTEXT_KEYS = (
    "request_id", "status_code", "event_id", "event_type", "producer", "order_id",
    "line_id", "topic", "partition", "offset", "outcome", "state_version",
    "current_version", "failure_type", "reason", "restarts", "delay_seconds",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "tracker",
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in CONTEXT_KEYS:
            if hasattr(record, key):
                data[key] = getattr(record, key)
        # Never log event payloads, DSNs or raw exception text (may contain credentials).
        if record.exc_info:
            data["exception_type"] = record.exc_info[0].__name__
        return json.dumps(data, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # Route uvicorn/alembic through the same JSON handler.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "alembic", "aiokafka"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
    logging.getLogger("aiokafka").setLevel(max(logging.WARNING, logging.getLevelName(level)))
