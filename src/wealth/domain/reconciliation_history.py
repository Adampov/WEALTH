"""Durable evidence contracts for reconciliation history and metrics."""

from datetime import timedelta
from hashlib import sha256
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.quality import CandleStream
from wealth.domain.reconciliation import (
    CandleReconciliationIssueCode,
    CandleReconciliationReport,
)

MAX_RECONCILIATION_REPORT_BYTES = 64 * 1024 * 1024
MAX_RECONCILIATION_QUERY_DURATION = timedelta(days=366)


class ReconciliationObservation(BaseModel):
    """One immutable capture of a deterministic reconciliation report."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    observation_id: UUID
    recorded_at: AwareDatetime
    report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    report: CandleReconciliationReport
    lineage: tuple[str, ...] = Field(min_length=1)

    @field_validator("lineage")
    @classmethod
    def lineage_is_explicit(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Reject missing or invisible provenance references."""

        if any(not reference.strip() for reference in value):
            raise ValueError("lineage references must be non-empty")
        return value

    @model_validator(mode="after")
    def evidence_is_bounded_and_consistent(self) -> Self:
        """Verify timing, size, and digest before evidence reaches storage."""

        if self.recorded_at < self.report.window_end_exclusive:
            raise ValueError("observation cannot be recorded before its report window closes")
        report_bytes = self.report_bytes
        if len(report_bytes) > MAX_RECONCILIATION_REPORT_BYTES:
            raise ValueError("reconciliation report exceeds the durable evidence limit")
        if sha256(report_bytes).hexdigest() != self.report_sha256:
            raise ValueError("report_sha256 must match the exact report JSON")
        return self

    @property
    def report_bytes(self) -> bytes:
        """Return the canonical UTF-8 report representation used by the digest."""

        return self.report.model_dump_json().encode("utf-8")


class ReconciliationObservationQuery(BaseModel):
    """Bounded ordered read of observations for one comparison series."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    comparison_key: str = Field(min_length=1, max_length=128)
    recorded_start: AwareDatetime
    recorded_end_exclusive: AwareDatetime
    limit: int = Field(default=100, ge=1, le=1_000)

    @field_validator("comparison_key")
    @classmethod
    def comparison_key_is_canonical(cls, value: str) -> str:
        """Reject ambiguous identifiers at the query boundary."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("comparison_key must not contain whitespace")
        return value

    @model_validator(mode="after")
    def query_window_is_bounded(self) -> Self:
        """Prevent empty, inverted, or operationally unbounded reads."""

        _validate_query_window(self.recorded_start, self.recorded_end_exclusive)
        return self


class ReconciliationSummaryQuery(BaseModel):
    """Bounded aggregate request for one comparison series."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    comparison_key: str = Field(min_length=1, max_length=128)
    recorded_start: AwareDatetime
    recorded_end_exclusive: AwareDatetime

    @field_validator("comparison_key")
    @classmethod
    def comparison_key_is_canonical(cls, value: str) -> str:
        """Reject ambiguous identifiers at the summary boundary."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("comparison_key must not contain whitespace")
        return value

    @model_validator(mode="after")
    def query_window_is_bounded(self) -> Self:
        """Prevent empty, inverted, or operationally unbounded aggregates."""

        _validate_query_window(self.recorded_start, self.recorded_end_exclusive)
        return self


class ReconciliationIssueCount(BaseModel):
    """Aggregate count for one machine-readable reconciliation finding."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: CandleReconciliationIssueCode
    count: int = Field(gt=0)


class ReconciliationHistorySummary(BaseModel):
    """Indexed quality metrics for one immutable comparison series."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    comparison_key: str = Field(min_length=1, max_length=128)
    primary_stream: CandleStream
    reference_stream: CandleStream
    recorded_start: AwareDatetime
    recorded_end_exclusive: AwareDatetime
    observation_count: int = Field(ge=0)
    pass_count: int = Field(ge=0)
    divergent_count: int = Field(ge=0)
    blocked_count: int = Field(ge=0)
    primary_quality_failure_count: int = Field(ge=0)
    reference_quality_failure_count: int = Field(ge=0)
    compared_interval_count: int = Field(ge=0)
    first_recorded_at: AwareDatetime | None = None
    last_recorded_at: AwareDatetime | None = None
    issue_counts: tuple[ReconciliationIssueCount, ...] = ()

    @model_validator(mode="after")
    def summary_is_self_consistent(self) -> Self:
        """Reject aggregates whose status, time, or issue totals are inconsistent."""

        _validate_query_window(self.recorded_start, self.recorded_end_exclusive)
        if self.primary_stream == self.reference_stream:
            raise ValueError("history summary requires distinct source streams")
        if self.observation_count != (self.pass_count + self.divergent_count + self.blocked_count):
            raise ValueError("status counts must equal observation_count")
        if self.primary_quality_failure_count > self.observation_count:
            raise ValueError("primary quality failures cannot exceed observation_count")
        if self.reference_quality_failure_count > self.observation_count:
            raise ValueError("reference quality failures cannot exceed observation_count")
        issue_codes = tuple(issue.code for issue in self.issue_counts)
        if issue_codes != tuple(sorted(set(issue_codes), key=lambda code: code.value)):
            raise ValueError("issue_counts must have unique machine-code order")
        if self.observation_count == 0:
            if self.first_recorded_at is not None or self.last_recorded_at is not None:
                raise ValueError("empty summary cannot have observation timestamps")
            if self.compared_interval_count or self.issue_counts:
                raise ValueError("empty summary cannot contain comparison metrics")
            return self
        if self.first_recorded_at is None or self.last_recorded_at is None:
            raise ValueError("non-empty summary requires first and last timestamps")
        if self.first_recorded_at > self.last_recorded_at:
            raise ValueError("summary timestamps must be ordered")
        if not self.recorded_start <= self.first_recorded_at < self.recorded_end_exclusive:
            raise ValueError("first observation is outside the summary window")
        if not self.recorded_start <= self.last_recorded_at < self.recorded_end_exclusive:
            raise ValueError("last observation is outside the summary window")
        return self


def _validate_query_window(
    recorded_start: AwareDatetime, recorded_end_exclusive: AwareDatetime
) -> None:
    if recorded_end_exclusive <= recorded_start:
        raise ValueError("recorded_end_exclusive must be after recorded_start")
    if recorded_end_exclusive - recorded_start > MAX_RECONCILIATION_QUERY_DURATION:
        raise ValueError("reconciliation query exceeds the maximum duration")
