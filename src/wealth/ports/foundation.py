"""Foundation ports for deterministic application behavior."""

from datetime import datetime
from typing import Protocol
from uuid import UUID

from wealth.domain.events import DomainEvent


class Clock(Protocol):
    """Provide time without coupling business logic to the wall clock."""

    def now(self) -> datetime:
        """Return the current timezone-aware timestamp."""


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
