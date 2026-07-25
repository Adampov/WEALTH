"""Foundation ports for deterministic application behavior."""

from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

from wealth.domain.events import DomainEvent


class ClockContractError(ValueError):
    """Reject injected clock output outside the canonical fixed UTC zone."""


def require_utc_clock(value: object) -> datetime:
    """Return an existing fixed-UTC clock value unchanged or fail closed."""

    if (
        not isinstance(value, datetime)
        or value.tzinfo is not UTC
        or value.utcoffset() != timedelta(0)
    ):
        raise ClockContractError(
            "clock must return a datetime with tzinfo exactly datetime.UTC and zero offset"
        )
    return value


class Clock(Protocol):
    """Provide canonical UTC time without coupling business logic to the wall clock."""

    def now(self) -> datetime:
        """Return the current timestamp with ``tzinfo`` exactly ``datetime.UTC``."""


class Sleeper(Protocol):
    """Delay retryable work behind an injectable boundary."""

    def sleep(self, seconds: float) -> None:
        """Block for a finite, non-negative duration."""


class IdGenerator(Protocol):
    """Create unique identifiers behind an injectable boundary."""

    def new(self) -> UUID:
        """Return a new identifier."""


class EventStore(Protocol):
    """Persist canonical events."""

    def append(self, event: DomainEvent) -> None:
        """Append one immutable event."""


class EventLogger(Protocol):
    """Emit structured, audit-friendly event logs."""

    def record(self, event: DomainEvent) -> None:
        """Record one event without changing it."""
