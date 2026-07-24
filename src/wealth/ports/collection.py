"""Persistence boundary for durable collection control state."""

from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from wealth.domain.collection import (
    CollectionHealthSummary,
    HistoricalCollectionJob,
    SourceHealthObservation,
)


class CollectionCheckpointWriteStatus(StrEnum):
    """Explicit outcomes for optimistic checkpoint writes."""

    INSERTED = "inserted"
    UPDATED = "updated"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class CollectionCheckpointWriteResult(BaseModel):
    """Return checkpoint persistence status without hiding concurrency."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: CollectionCheckpointWriteStatus
    job_id: UUID
    current_version: int


class CollectionCheckpointStore(Protocol):
    """Persist checkpoint transitions and health evidence atomically."""

    def create(self, job: HistoricalCollectionJob) -> CollectionCheckpointWriteResult:
        """Insert one pending job or return an explicit duplicate/conflict."""

    def get(self, job_id: UUID) -> HistoricalCollectionJob | None:
        """Reload and validate the current checkpoint."""

    def transition(
        self,
        job: HistoricalCollectionJob,
        *,
        expected_version: int,
        health: SourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        """Compare-and-swap one state transition and optional health record."""

    def health_for_job(self, job_id: UUID) -> tuple[SourceHealthObservation, ...]:
        """Return append-only health evidence in observation order."""

    def health_summary(self, job_id: UUID) -> CollectionHealthSummary:
        """Return aggregate health counters without loading full evidence."""
