"""Structured JSON logging with mandatory fields: timestamp, service_name, level, message."""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Formats log records as JSON with required fields."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service_name": self.service_name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


def setup_logging(service_name: str, level: str = "INFO") -> logging.Logger:
    """Configure the root logger with structured JSON output.

    Configures the root logger so that all child loggers (including
    ``src.worker_web.worker``, ``src.worker_web.parsers.fl_ru``, etc.)
    automatically inherit the handler and output to stdout.

    Args:
        service_name: Name of the service (used in JSON ``service_name`` field).
        level: Log level string (e.g. 'INFO', 'DEBUG', 'ERROR').

    Returns:
        The root logger instance.
    """
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers on repeated calls
    if not any(isinstance(h, logging.StreamHandler) and h.stream is sys.stdout for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter(service_name))
        root.addHandler(handler)

    return root
