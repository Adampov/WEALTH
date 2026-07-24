"""Ports for collector service shutdown and durable heartbeat evidence."""

from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from wealth.domain.collector_service import (
    CollectorServiceHeartbeat,
    CollectorServiceHeartbeatQuery,
)


class CollectorServiceHeartbeatWriteStatus(StrEnum):
    """Idempotent outcomes for one heartbeat append."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class CollectorServiceHeartbeatWriteResult(BaseModel):
    """Expose heartbeat identity and current durable sequence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: CollectorServiceHeartbeatWriteStatus
    run_id: UUID
    heartbeat_id: UUID
    current_sequence: int


class CollectorServiceHeartbeatStore(Protocol):
    """Persist append-only heartbeat evidence and one validated current projection."""

    def append(
        self,
        heartbeat: CollectorServiceHeartbeat,
    ) -> CollectorServiceHeartbeatWriteResult:
        """Append the exact next heartbeat or report an explicit conflict."""

    def current(self, run_id: UUID) -> CollectorServiceHeartbeat | None:
        """Return the validated latest heartbeat for one service run."""

    def observations(
        self,
        query: CollectorServiceHeartbeatQuery,
    ) -> tuple[CollectorServiceHeartbeat, ...]:
        """Return bounded heartbeat history from sequence one."""


class ShutdownSignal(Protocol):
    """Expose immediate and interruptible process-stop state."""

    def requested(self) -> bool:
        """Return whether graceful shutdown has been requested."""

    def wait(self, timeout_seconds: float) -> bool:
        """Wait until timeout or shutdown; return true when shutdown wins."""
