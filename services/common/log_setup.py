"""Structured JSON logging shared by every service.

Design goals:
  * One JSON object per line (JSON Lines / NDJSON) -> trivially parseable by
    Fluent Bit in Stage 2 and by anything downstream.
  * A fixed core schema on every record: timestamp, service_name, level,
    correlation_id, message. Plus arbitrary contextual fields (latency_ms,
    status_code, region, endpoint, user_id, ...) passed per call.
  * The correlation_id is pulled from the request-scoped ContextVar, so callers
    never have to pass it explicitly -> impossible to forget, always consistent.
  * Logs go to BOTH stdout (nice for `docker logs`) and a per-service file
    under LOG_DIR (Fluent Bit tails the file in Stage 2).

Why not just print JSON? Using the stdlib `logging` module gives us levels,
filtering, and the same API every Python dev expects, while the custom
Formatter guarantees the wire format.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sys
from pathlib import Path

from .tracing import get_correlation_id

# LogRecord attributes that the stdlib sets itself. Anything a caller passes via
# `extra=` that is NOT in this set is treated as a contextual field and merged
# into the JSON output. This lets us write `logger.info("ok", extra={...})`
# without enumerating field names here.
_STANDARD_RECORD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName",
}


class JsonLogFormatter(logging.Formatter):
    """Render a LogRecord as a single-line JSON object with our core schema."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        # ISO8601 in UTC with a trailing Z, e.g. 2026-07-14T09:00:00.123456Z.
        ts = _dt.datetime.fromtimestamp(
            record.created, tz=_dt.timezone.utc
        ).isoformat().replace("+00:00", "Z")

        payload = {
            "timestamp": ts,
            "service_name": self.service_name,
            # Normalise Python's "WARNING" to the spec's "WARN".
            "level": "WARN" if record.levelname == "WARNING" else record.levelname,
            # Read the correlation id lazily, at format time, so it reflects the
            # request context in which the log call was actually made.
            "correlation_id": get_correlation_id(),
            "message": record.getMessage(),
        }

        # Merge any contextual fields the caller attached via extra=.
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_"):
                payload[key] = value

        # Include exception text if this was logger.exception / exc_info=True.
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(service_name: str, log_dir: str | None = None) -> logging.Logger:
    """Configure and return the logger for a service.

    Idempotent: safe to call once per process at startup.
    """
    log_dir = log_dir or os.getenv("LOG_DIR", "./logs")
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(service_name)
    logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "DEBUG").upper()))
    logger.propagate = False  # don't double-log through the root logger

    # Guard against duplicate handlers if this runs more than once.
    if logger.handlers:
        return logger

    formatter = JsonLogFormatter(service_name)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    file_handler = logging.FileHandler(
        Path(log_dir) / f"{service_name}.log", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
