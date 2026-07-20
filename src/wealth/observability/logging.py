"""Structured JSON logging for canonical events."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import TextIO, cast

from pydantic import JsonValue

from wealth.domain.events import DomainEvent


class JsonFormatter(logging.Formatter):
    """Format standard-library log records as deterministic JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize selected safe fields and an optional canonical event."""

        payload: dict[str, JsonValue] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        raw_event = getattr(record, "domain_event", None)
        if raw_event is not None:
            payload["event"] = cast(JsonValue, raw_event)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


class StandardLibraryEventLogger:
    """Event-logging adapter backed by the Python logging module."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def record(self, event: DomainEvent) -> None:
        """Log a validated event using JSON-safe values."""

        self._logger.info(
            "domain_event_recorded",
            extra={"domain_event": event.model_dump(mode="json")},
        )


def configure_json_logger(
    *,
    name: str = "wealth",
    stream: TextIO | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Create an isolated logger with exactly one JSON stream handler."""

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(level)

    handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger
