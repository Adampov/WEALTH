"""Provider-independent contracts for deterministic candle reconciliation."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.quality import CandleSequenceReport, CandleStream, DataQualityStatus

BasisPoints = Annotated[Decimal, Field(ge=Decimal("0"), le=Decimal("10000"))]


class CandleReconciliationStatus(StrEnum):
    """Overall outcome for one bounded cross-source comparison."""

    PASS = "pass"
    DIVERGENT = "divergent"
    BLOCKED = "blocked"


class CandleReconciliationIssueCode(StrEnum):
    """Machine-readable reconciliation findings."""

    PRIMARY_MISSING = "primary_missing"
    REFERENCE_MISSING = "reference_missing"
    OPEN_PRICE_DIVERGENCE = "open_price_divergence"
    HIGH_PRICE_DIVERGENCE = "high_price_divergence"
    LOW_PRICE_DIVERGENCE = "low_price_divergence"
    CLOSE_PRICE_DIVERGENCE = "close_price_divergence"
    BASE_VOLUME_DIVERGENCE = "base_volume_divergence"


class CandleReconciliationPolicy(BaseModel):
    """Explicit tolerances for venue-specific candle differences."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    max_price_difference_bps: BasisPoints
    max_base_volume_difference_bps: BasisPoints | None = None


class CandleIntervalComparison(BaseModel):
    """Symmetric differences for one interval present in both sources."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    open_time: AwareDatetime
    primary_record_id: UUID
    reference_record_id: UUID
    open_difference_bps: BasisPoints
    high_difference_bps: BasisPoints
    low_difference_bps: BasisPoints
    close_difference_bps: BasisPoints
    base_volume_difference_bps: BasisPoints


class CandleReconciliationIssue(BaseModel):
    """One missing-source or threshold-divergence finding."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: CandleReconciliationIssueCode
    open_time: AwareDatetime
    primary_record_id: UUID | None = None
    reference_record_id: UUID | None = None
    difference_bps: BasisPoints | None = None
    limit_bps: BasisPoints | None = None

    @model_validator(mode="after")
    def evidence_matches_issue_type(self) -> Self:
        """Require complete evidence without inventing values for missing records."""

        if self.code is CandleReconciliationIssueCode.PRIMARY_MISSING:
            if self.primary_record_id is not None:
                raise ValueError("primary-missing issue cannot identify a primary record")
            if self.difference_bps is not None or self.limit_bps is not None:
                raise ValueError("missing-record issue cannot contain a numeric difference")
            return self
        if self.code is CandleReconciliationIssueCode.REFERENCE_MISSING:
            if self.reference_record_id is not None:
                raise ValueError("reference-missing issue cannot identify a reference record")
            if self.difference_bps is not None or self.limit_bps is not None:
                raise ValueError("missing-record issue cannot contain a numeric difference")
            return self
        if self.primary_record_id is None or self.reference_record_id is None:
            raise ValueError("divergence issue must identify both source records")
        if self.difference_bps is None or self.limit_bps is None:
            raise ValueError("divergence issue must contain its difference and limit")
        if self.difference_bps <= self.limit_bps:
            raise ValueError("divergence issue must exceed its configured limit")
        return self


class CandleReconciliationReport(BaseModel):
    """Deterministic evidence for one selected cross-source candle window."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    comparison_key: str = Field(min_length=1, max_length=128)
    primary_stream: CandleStream
    reference_stream: CandleStream
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime
    policy: CandleReconciliationPolicy
    primary_quality: CandleSequenceReport
    reference_quality: CandleSequenceReport
    compared_count: int = Field(ge=0)
    status: CandleReconciliationStatus
    comparisons: tuple[CandleIntervalComparison, ...] = ()
    issues: tuple[CandleReconciliationIssue, ...] = ()

    @field_validator("comparison_key")
    @classmethod
    def comparison_key_is_canonical(cls, value: str) -> str:
        """Reject ambiguous comparison identifiers."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("comparison_key must not contain whitespace")
        return value

    @model_validator(mode="after")
    def report_is_self_consistent(self) -> Self:
        """Ensure status, streams, quality evidence, and counts cannot disagree."""

        if self.primary_stream == self.reference_stream:
            raise ValueError("reconciliation requires two distinct streams")
        primary_market = (
            self.primary_stream.instrument,
            self.primary_stream.instrument_type,
            self.primary_stream.timeframe,
        )
        reference_market = (
            self.reference_stream.instrument,
            self.reference_stream.instrument_type,
            self.reference_stream.timeframe,
        )
        if primary_market != reference_market:
            raise ValueError("reconciliation streams must describe the same canonical market")
        if self.window_end_exclusive <= self.window_start:
            raise ValueError("reconciliation window end must be after its start")
        if self.primary_quality.stream != self.primary_stream:
            raise ValueError("primary quality evidence must match the primary stream")
        if self.reference_quality.stream != self.reference_stream:
            raise ValueError("reference quality evidence must match the reference stream")
        for quality in (self.primary_quality, self.reference_quality):
            if (
                quality.window_start != self.window_start
                or quality.window_end_exclusive != self.window_end_exclusive
            ):
                raise ValueError("quality evidence must cover the reconciliation window")
        if self.compared_count != len(self.comparisons):
            raise ValueError("compared_count must match the interval comparisons")
        comparison_times = tuple(comparison.open_time for comparison in self.comparisons)
        if comparison_times != tuple(sorted(set(comparison_times))):
            raise ValueError("interval comparisons must have unique ascending open times")
        if any(
            not self.window_start <= comparison.open_time < self.window_end_exclusive
            for comparison in self.comparisons
        ):
            raise ValueError("interval comparison is outside the reconciliation window")

        quality_blocked = (
            self.primary_quality.status is DataQualityStatus.FAIL
            or self.reference_quality.status is DataQualityStatus.FAIL
        )
        if quality_blocked and self.status is not CandleReconciliationStatus.BLOCKED:
            raise ValueError("failed source quality must block reconciliation")
        if not quality_blocked and self.status is CandleReconciliationStatus.BLOCKED:
            raise ValueError("blocked reconciliation requires failed source quality")
        if (
            not quality_blocked
            and self.issues
            and self.status is not CandleReconciliationStatus.DIVERGENT
        ):
            raise ValueError("unblocked findings require divergent status")
        if (
            not quality_blocked
            and not self.issues
            and self.status is not CandleReconciliationStatus.PASS
        ):
            raise ValueError("clean unblocked reconciliation must pass")
        return self

    @property
    def compared_open_times(self) -> tuple[datetime, ...]:
        """Return the intervals backed by records from both sources."""

        return tuple(comparison.open_time for comparison in self.comparisons)
