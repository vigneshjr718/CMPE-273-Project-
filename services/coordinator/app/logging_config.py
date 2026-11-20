import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "coordinator",
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("request_id", "order_id", "line_id", "attempt_id", "status_code",
                    "event_id", "state_version", "failure_type", "topic", "partition",
                    "offset", "outcome"):
            if hasattr(record, key):
                data[key] = getattr(record, key)
        # Never serialize request bodies, DSNs, headers or arbitrary exception text.
        if record.exc_info:
            data["exception_type"] = record.exc_info[0].__name__
        return json.dumps(data)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("coordinator")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
