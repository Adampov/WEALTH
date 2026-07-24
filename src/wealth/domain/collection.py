"""Durable collection checkpoints, leases, and source-health evidence."""

from enum import StrEnum
from math import isfinite
from typing import Literal, Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from wealth.domain.market import CandleTimeframe, InstrumentType


class CollectionJobStatus(StrEnum):
    """Lifecycle states for one bounded historical collection job."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class SourceHealthStatus(StrEnum):
    """Provider health inferred from one page-level collection outcome."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class HistoricalCollectionJob(BaseModel):
    """Current durable checkpoint for one immutable collection request."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    job_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime
    next_window_start: AwareDatetime
    status: CollectionJobStatus
    created_at: AwareDatetime
    updated_at: AwareDatetime
    version: int = Field(ge=1)
    lease_owner: str | None = Field(default=None, min_length=1, max_length=128)
    lease_expires_at: AwareDatetime | None = None
    pages_completed: int = Field(default=0, ge=0)
    candles_completed: int = Field(default=0, ge=0)
    total_attempts: int = Field(default=0, ge=0)
    last_failure_code: str | None = Field(default=None, min_length=1, max_length=128)
    last_stop_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator(
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "lease_owner",
        "last_failure_code",
        "last_stop_reason",
    )
    @classmethod
    def identifiers_are_unambiguous(cls, value: str | None) -> str | None:
        """Reject implicit normalization and unsafe whitespace."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("collection identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def checkpoint_invariants_hold(self) -> Self:
        """Make progress, lease, and terminal-state semantics unambiguous."""

        if self.window_end_exclusive <= self.window_start:
            raise ValueError("collection window end must be after its start")
        if not self.window_start <= self.next_window_start <= self.window_end_exclusive:
            raise ValueError("collection cursor must remain inside its immutable window")
        if (self.next_window_start - self.window_start) % self.timeframe.duration:
            raise ValueError("collection cursor must align to its timeframe")
        expected_candles = (self.next_window_start - self.window_start) // self.timeframe.duration
        if self.candles_completed != expected_candles:
            raise ValueError("candles_completed must equal durable cursor progress")
        if self.total_attempts < self.pages_completed:
            raise ValueError("total_attempts cannot be below completed page count")
        if self.created_at > self.updated_at:
            raise ValueError("collection timestamps must not regress")

        has_owner = self.lease_owner is not None
        has_expiry = self.lease_expires_at is not None
        if has_owner != has_expiry:
            raise ValueError("collection lease owner and expiry must be set together")
        if self.status is CollectionJobStatus.RUNNING:
            if not has_owner or self.lease_expires_at is None:
                raise ValueError("running collection job requires a lease")
            if self.lease_expires_at <= self.updated_at:
                raise ValueError("running collection lease must expire after updated_at")
        elif has_owner:
            raise ValueError("only a running collection job may hold a lease")

        if self.status is CollectionJobStatus.COMPLETED:
            if self.next_window_start != self.window_end_exclusive:
                raise ValueError("completed collection job must cover its full window")
        elif self.next_window_start == self.window_end_exclusive:
            raise ValueError("full collection window must be marked completed")

        if self.status is CollectionJobStatus.FAILED:
            if self.last_failure_code is None:
                raise ValueError("failed collection job requires a machine failure code")
        elif self.last_failure_code is not None or self.last_stop_reason is not None:
            raise ValueError("failure details are only valid on failed collection jobs")
        return self


class SourceHealthObservation(BaseModel):
    """Append-only health evidence for one attempted provider page."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    observation_id: UUID
    job_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    timeframe: CandleTimeframe
    page_start: AwareDatetime
    page_end_exclusive: AwareDatetime
    observed_at: AwareDatetime
    status: SourceHealthStatus
    accepted: bool
    attempts: int = Field(ge=1)
    retry_delays_seconds: tuple[float, ...] = ()
    failure_code: str | None = Field(default=None, min_length=1, max_length=128)
    stop_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("source", "venue", "instrument", "failure_code", "stop_reason")
    @classmethod
    def fields_are_unambiguous(cls, value: str | None) -> str | None:
        """Keep metric dimensions and machine codes canonical."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("health fields must not contain whitespace")
        return value

    @field_validator("retry_delays_seconds")
    @classmethod
    def retry_delays_are_safe(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        """Reject impossible delay metrics."""

        if any(not isfinite(delay) or delay < 0 for delay in value):
            raise ValueError("retry delays must be finite and non-negative")
        return value

    @model_validator(mode="after")
    def observation_invariants_hold(self) -> Self:
        """Tie status and retry metrics to the recorded page outcome."""

        if self.page_end_exclusive <= self.page_start:
            raise ValueError("health page end must be after its start")
        if (self.page_end_exclusive - self.page_start) % self.timeframe.duration:
            raise ValueError("health page must cover exact timeframe intervals")
        if len(self.retry_delays_seconds) != self.attempts - 1:
            raise ValueError("health retry delays must describe attempts after the first")
        if self.accepted:
            if self.failure_code is not None or self.stop_reason is not None:
                raise ValueError("accepted health observation cannot carry a failure")
            expected_status = (
                SourceHealthStatus.HEALTHY if self.attempts == 1 else SourceHealthStatus.DEGRADED
            )
            if self.status is not expected_status:
                raise ValueError("accepted health status must reflect whether retries occurred")
        else:
            if self.status is SourceHealthStatus.HEALTHY or self.failure_code is None:
                raise ValueError("rejected health observation requires a non-healthy failure")
        return self


class CollectionHealthSummary(BaseModel):
    """Compact, query-efficient health totals for one collection job."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    job_id: UUID
    observation_count: int = Field(ge=0)
    healthy_count: int = Field(ge=0)
    degraded_count: int = Field(ge=0)
    unavailable_count: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    total_attempts: int = Field(ge=0)

    @model_validator(mode="after")
    def totals_are_consistent(self) -> Self:
        """Prevent summaries whose dimensions disagree."""

        if (
            self.healthy_count + self.degraded_count + self.unavailable_count
            != self.observation_count
        ):
            raise ValueError("health status counts must equal observation_count")
        if self.accepted_count > self.observation_count:
            raise ValueError("accepted_count cannot exceed observation_count")
        if self.total_attempts < self.observation_count:
            raise ValueError("every observation must contain at least one attempt")
        return self


def validate_collection_transition(
    previous: HistoricalCollectionJob,
    current: HistoricalCollectionJob,
) -> None:
    """Reject stale, regressive, or unauthorized checkpoint transitions."""

    immutable_fields = (
        "job_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "timeframe",
        "window_start",
        "window_end_exclusive",
        "created_at",
    )
    if any(getattr(previous, field) != getattr(current, field) for field in immutable_fields):
        raise ValueError("collection transition changed immutable job identity")
    if current.version != previous.version + 1:
        raise ValueError("collection transition version must increase by exactly one")
    if current.updated_at < previous.updated_at:
        raise ValueError("collection transition time must not regress")
    if current.next_window_start < previous.next_window_start:
        raise ValueError("collection cursor must not regress")
    if (
        current.pages_completed < previous.pages_completed
        or current.candles_completed < previous.candles_completed
        or current.total_attempts < previous.total_attempts
    ):
        raise ValueError("collection counters must not regress")

    allowed = {
        CollectionJobStatus.PENDING: {CollectionJobStatus.RUNNING},
        CollectionJobStatus.PAUSED: {CollectionJobStatus.RUNNING},
        CollectionJobStatus.FAILED: {CollectionJobStatus.RUNNING},
        CollectionJobStatus.RUNNING: {
            CollectionJobStatus.RUNNING,
            CollectionJobStatus.COMPLETED,
            CollectionJobStatus.FAILED,
            CollectionJobStatus.PAUSED,
        },
        CollectionJobStatus.COMPLETED: set(),
    }
    if current.status not in allowed[previous.status]:
        raise ValueError("collection status transition is not allowed")

    if previous.status is CollectionJobStatus.RUNNING:
        if (
            current.status
            in {
                CollectionJobStatus.COMPLETED,
                CollectionJobStatus.FAILED,
                CollectionJobStatus.PAUSED,
            }
            and current.lease_owner is not None
        ):
            raise ValueError("terminal or paused transition must release its lease")
        if (
            current.status is CollectionJobStatus.RUNNING
            and current.lease_owner != previous.lease_owner
            and previous.lease_expires_at is not None
            and previous.lease_expires_at > current.updated_at
        ):
            raise ValueError("active collection lease cannot be taken by another worker")
