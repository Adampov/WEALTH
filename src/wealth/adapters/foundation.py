"""Safe foundation adapters that perform no network or financial action."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import isfinite
from time import sleep
from uuid import UUID, uuid4

from wealth.domain.events import DomainEvent


@dataclass(slots=True)
class InMemoryEventStore:
    """Append-only store used to prove the event boundary before database selection."""

    _events: list[DomainEvent] = field(default_factory=list)

    def append(self, event: DomainEvent) -> None:
        """Append an immutable event."""

        self._events.append(event)

    def all(self) -> tuple[DomainEvent, ...]:
        """Return an immutable snapshot of stored events."""

        return tuple(self._events)


@dataclass(frozen=True, slots=True)
class SystemClock:
    """UTC wall-clock adapter for non-replay runtime entry points."""

    def now(self) -> datetime:
        """Return the current timezone-aware UTC timestamp."""

        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SystemSleeper:
    """Apply bounded application delays without hiding them in provider adapters."""

    def sleep(self, seconds: float) -> None:
        """Sleep for a validated non-negative duration."""

        if not isfinite(seconds) or seconds < 0:
            raise ValueError("sleep duration must be finite and non-negative")
        sleep(seconds)


@dataclass(frozen=True, slots=True)
class Uuid4Generator:
    """Random UUID adapter for non-replay runtime entry points."""

    def new(self) -> UUID:
        """Return a random UUID."""

        return uuid4()
