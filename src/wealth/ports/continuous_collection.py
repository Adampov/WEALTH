"""Persistence boundary for restart-safe continuous collection cursors."""

from enum import StrEnum
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from wealth.domain.continuous_collection import ContinuousCollectionCheckpoint


class ContinuousCollectionWriteStatus(StrEnum):
    """Explicit outcomes for optimistic continuous checkpoint writes."""

    INSERTED = "inserted"
    UPDATED = "updated"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class ContinuousCollectionWriteResult(BaseModel):
    """Expose durable write and concurrency outcomes."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: ContinuousCollectionWriteStatus
    collection_id: UUID
    current_version: int


class ContinuousCollectionCheckpointStore(Protocol):
    """Persist one current cursor plus append-only transition evidence."""

    def create(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> ContinuousCollectionWriteResult:
        """Insert pristine state or return an explicit duplicate or conflict."""

    def get(self, collection_id: UUID) -> ContinuousCollectionCheckpoint | None:
        """Reload and validate one current continuous cursor."""

    def transition(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        *,
        expected_version: int,
    ) -> ContinuousCollectionWriteResult:
        """Compare-and-swap one validated state transition."""
