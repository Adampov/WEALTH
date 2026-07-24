"""Persistence boundary for reconciliation evidence and quality metrics."""

from enum import StrEnum
from typing import Protocol, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator

from wealth.domain.reconciliation_history import (
    ReconciliationHistorySummary,
    ReconciliationObservation,
    ReconciliationObservationQuery,
    ReconciliationSummaryQuery,
)


class ReconciliationWriteStatus(StrEnum):
    """Idempotent outcomes for append-only observation writes."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class ReconciliationWriteConflictCode(StrEnum):
    """Machine-readable reasons an observation could not be appended."""

    OBSERVATION_ID_REUSE = "observation_id_reuse"
    COMPARISON_KEY_REUSE = "comparison_key_reuse"


class ReconciliationWriteResult(BaseModel):
    """Return append status without hiding identity conflicts."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: ReconciliationWriteStatus
    observation_id: UUID
    conflict_code: ReconciliationWriteConflictCode | None = None

    @model_validator(mode="after")
    def conflict_evidence_matches_status(self) -> Self:
        """Require a reason exactly when the write conflicts."""

        if self.status is ReconciliationWriteStatus.CONFLICT:
            if self.conflict_code is None:
                raise ValueError("conflict result requires conflict_code")
        elif self.conflict_code is not None:
            raise ValueError("non-conflict result cannot contain conflict_code")
        return self


class ReconciliationHistoryStore(Protocol):
    """Persist and query reconciliation evidence without silent replacement."""

    def append(self, observation: ReconciliationObservation) -> ReconciliationWriteResult:
        """Insert once or report an explicit duplicate or conflict."""

    def get(self, observation_id: UUID) -> ReconciliationObservation | None:
        """Reload and validate one observation by identity."""

    def observations(
        self,
        query: ReconciliationObservationQuery,
    ) -> tuple[ReconciliationObservation, ...]:
        """Return a bounded, recorded-time-ordered history slice."""

    def summarize(
        self,
        query: ReconciliationSummaryQuery,
    ) -> ReconciliationHistorySummary | None:
        """Return indexed quality metrics, or None for an unknown comparison key."""
