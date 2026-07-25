"""Restart-safe checkpoint and health contracts for public trade collection."""

from datetime import timedelta
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

from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.market import InstrumentType

MAX_DURABLE_COUNTER = 2**63 - 1
MAX_PUBLIC_TRADE_INVOCATION_REQUESTS = 1_024
MAX_PUBLIC_TRADE_INVOCATION_RECORDS = 100_000
MAX_PUBLIC_TRADE_RETRY_DELAY_SECONDS = 300.0
MAX_PUBLIC_TRADE_LEASE_DURATION = timedelta(hours=1)


class PublicTradeCollectionCheckpoint(BaseModel):
    """Current durable state for one immutable bounded public-trade range."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    job_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    policy_fingerprint: str = Field(min_length=1, max_length=128)
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime
    next_window_start: AwareDatetime
    pending_window_end_exclusive: AwareDatetime | None = None
    status: CollectionJobStatus
    created_at: AwareDatetime
    updated_at: AwareDatetime
    version: int = Field(ge=1, le=MAX_DURABLE_COUNTER)
    lease_owner: str | None = Field(default=None, min_length=1, max_length=128)
    lease_token: UUID | None = None
    lease_expires_at: AwareDatetime | None = None
    windows_completed: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    records_completed: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    source_requests: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    window_traces: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    retry_attempts: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    splits_completed: int = Field(default=0, ge=0, le=MAX_DURABLE_COUNTER)
    last_failure_code: str | None = Field(default=None, min_length=1, max_length=128)
    last_stop_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator(
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "policy_fingerprint",
        "lease_owner",
        "last_failure_code",
        "last_stop_reason",
    )
    @classmethod
    def identifiers_are_unambiguous(cls, value: str | None) -> str | None:
        """Reject identifiers that would require implicit normalization."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("public trade collection identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def checkpoint_invariants_hold(self) -> Self:
        """Tie cursor, counters, lease, and lifecycle into one trusted state."""

        if self.window_end_exclusive <= self.window_start:
            raise ValueError("collection window end must be after its start")
        if any(
            timestamp.microsecond % 1_000
            for timestamp in (
                self.window_start,
                self.window_end_exclusive,
                self.next_window_start,
                *(
                    ()
                    if self.pending_window_end_exclusive is None
                    else (self.pending_window_end_exclusive,)
                ),
            )
        ):
            raise ValueError("public trade collection boundaries must align to milliseconds")
        if not self.window_start <= self.next_window_start <= self.window_end_exclusive:
            raise ValueError("collection cursor must remain inside its immutable window")
        if self.pending_window_end_exclusive is not None and not (
            self.next_window_start < self.pending_window_end_exclusive <= self.window_end_exclusive
        ):
            raise ValueError("pending collection window must begin at the durable cursor")
        if self.created_at > self.updated_at:
            raise ValueError("collection timestamps must not regress")
        if self.records_completed > 0 and self.windows_completed == 0:
            raise ValueError("completed records require at least one completed window")
        if self.window_traces < self.windows_completed + self.splits_completed:
            raise ValueError("window traces cannot be below completed windows plus splits")
        if self.source_requests != self.window_traces + self.retry_attempts:
            raise ValueError("source requests must equal window traces plus retry attempts")

        has_owner = self.lease_owner is not None
        has_token = self.lease_token is not None
        has_expiry = self.lease_expires_at is not None
        if not has_owner == has_token == has_expiry:
            raise ValueError("collection lease owner, token, and expiry must be set together")
        if self.status is CollectionJobStatus.RUNNING:
            if not has_owner or self.lease_expires_at is None:
                raise ValueError("running collection job requires a lease")
            if self.lease_expires_at <= self.updated_at:
                raise ValueError("running collection lease must expire after updated_at")
            if self.lease_expires_at - self.updated_at > MAX_PUBLIC_TRADE_LEASE_DURATION:
                raise ValueError("running collection lease cannot exceed one hour")
        elif has_owner:
            raise ValueError("only a running collection job may hold a lease")

        if self.status is CollectionJobStatus.COMPLETED:
            if self.next_window_start != self.window_end_exclusive:
                raise ValueError("completed collection job must cover its full window")
            if self.pending_window_end_exclusive is not None:
                raise ValueError("completed collection job cannot retain a pending window")
        elif self.next_window_start == self.window_end_exclusive:
            raise ValueError("full collection window must be marked completed")
        if (
            self.status is CollectionJobStatus.PENDING
            and self.pending_window_end_exclusive is not None
        ):
            raise ValueError("pending collection job cannot retain attempted-window state")

        if self.status is CollectionJobStatus.FAILED:
            if (
                self.last_failure_code is None
                or self.last_stop_reason is None
                or self.pending_window_end_exclusive is None
            ):
                raise ValueError(
                    "failed collection job requires failure, stop reason, and pending window"
                )
        elif self.status is CollectionJobStatus.PAUSED:
            if (
                self.last_failure_code is not None
                or self.last_stop_reason is None
                or self.pending_window_end_exclusive is None
            ):
                raise ValueError(
                    "paused collection job requires stop reason and pending window without failure"
                )
        elif self.last_failure_code is not None or self.last_stop_reason is not None:
            raise ValueError("stop details are only valid on failed or paused collection jobs")
        return self


class PublicTradeSourceHealthObservation(BaseModel):
    """Append-only outcome evidence for one bounded range invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    observation_id: UUID
    job_id: UUID
    checkpoint_version: int = Field(ge=2, le=MAX_DURABLE_COUNTER)
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    range_start: AwareDatetime
    range_end_exclusive: AwareDatetime
    next_window_start: AwareDatetime
    pending_window_end_exclusive: AwareDatetime | None = None
    observed_at: AwareDatetime
    status: SourceHealthStatus
    accepted: bool
    source_requests: int = Field(ge=1, le=MAX_PUBLIC_TRADE_INVOCATION_REQUESTS)
    window_traces: int = Field(ge=1, le=MAX_PUBLIC_TRADE_INVOCATION_REQUESTS)
    windows_completed: int = Field(ge=0, le=MAX_PUBLIC_TRADE_INVOCATION_REQUESTS)
    records_completed: int = Field(ge=0, le=MAX_PUBLIC_TRADE_INVOCATION_RECORDS)
    splits_completed: int = Field(ge=0, le=MAX_PUBLIC_TRADE_INVOCATION_REQUESTS)
    retry_delays_seconds: tuple[float, ...] = Field(
        default=(),
        max_length=MAX_PUBLIC_TRADE_INVOCATION_REQUESTS - 1,
    )
    failure_code: str | None = Field(default=None, min_length=1, max_length=128)
    stop_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator(
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "failure_code",
        "stop_reason",
    )
    @classmethod
    def fields_are_unambiguous(cls, value: str | None) -> str | None:
        """Keep metric dimensions and machine codes canonical."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("public trade source health fields must not contain whitespace")
        return value

    @field_validator("retry_delays_seconds")
    @classmethod
    def retry_delays_are_safe(cls, value: tuple[float, ...]) -> tuple[float, ...]:
        """Reject impossible wait evidence."""

        if any(
            not isfinite(delay) or delay < 0 or delay > MAX_PUBLIC_TRADE_RETRY_DELAY_SECONDS
            for delay in value
        ):
            raise ValueError("retry delays must be finite and inside the bounded retry policy")
        if not isfinite(sum(value)):
            raise ValueError("retry delay total must be finite")
        return value

    @model_validator(mode="after")
    def observation_invariants_hold(self) -> Self:
        """Tie progress, adaptation, and failure classification together."""

        if self.range_end_exclusive <= self.range_start:
            raise ValueError("source health range end must be after its start")
        if any(
            timestamp.microsecond % 1_000
            for timestamp in (
                self.range_start,
                self.range_end_exclusive,
                self.next_window_start,
                *(
                    ()
                    if self.pending_window_end_exclusive is None
                    else (self.pending_window_end_exclusive,)
                ),
            )
        ):
            raise ValueError("source health range boundaries must align to milliseconds")
        if not self.range_start <= self.next_window_start <= self.range_end_exclusive:
            raise ValueError("source health cursor must remain inside its attempted range")
        if self.pending_window_end_exclusive is not None and not (
            self.next_window_start < self.pending_window_end_exclusive <= self.range_end_exclusive
        ):
            raise ValueError("health pending window must begin at its safe resume cursor")
        if self.records_completed > 0 and self.windows_completed == 0:
            raise ValueError("health records require at least one completed window")
        if self.window_traces < self.windows_completed + self.splits_completed:
            raise ValueError("health traces cannot be below completed windows plus splits")
        if self.source_requests != self.window_traces + len(self.retry_delays_seconds):
            raise ValueError("health requests must equal window traces plus retry attempts")
        terminal_trace_count = self.window_traces - self.windows_completed - self.splits_completed
        if terminal_trace_count > 1:
            raise ValueError("health can contain at most one terminal window trace")

        if self.accepted:
            if self.next_window_start != self.range_end_exclusive:
                raise ValueError("accepted source health must cover its complete range")
            if (
                self.pending_window_end_exclusive is not None
                or self.failure_code is not None
                or self.stop_reason is not None
            ):
                raise ValueError("accepted source health cannot carry a failure")
            if self.windows_completed == 0:
                raise ValueError("accepted source health requires a completed window")
            if self.window_traces != self.windows_completed + self.splits_completed:
                raise ValueError("accepted source health cannot contain a terminal trace")
            expected_status = (
                SourceHealthStatus.HEALTHY
                if (
                    self.splits_completed == 0
                    and not self.retry_delays_seconds
                    and self.source_requests == self.windows_completed
                )
                else SourceHealthStatus.DEGRADED
            )
            if self.status is not expected_status:
                raise ValueError("accepted source health must reflect retries or adaptive splits")
        else:
            if self.pending_window_end_exclusive is None or self.stop_reason is None:
                raise ValueError("rejected source health requires stop and pending-window evidence")
            if self.failure_code is None:
                expected_status = (
                    SourceHealthStatus.HEALTHY
                    if self.splits_completed == 0 and not self.retry_delays_seconds
                    else SourceHealthStatus.DEGRADED
                )
                if self.status is not expected_status:
                    raise ValueError(
                        "controlled pause health must reflect retries or adaptive splits"
                    )
            else:
                if terminal_trace_count != 1:
                    raise ValueError("failed source health requires one terminal trace")
                if self.status is SourceHealthStatus.HEALTHY:
                    raise ValueError("failed source health cannot be healthy")
        return self


class PublicTradeCollectionHealthSummary(BaseModel):
    """Compact validated aggregate evidence for one public-trade job."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    job_id: UUID
    observation_count: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    healthy_count: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    degraded_count: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    unavailable_count: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    accepted_count: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_source_requests: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_window_traces: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_retry_attempts: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_windows_completed: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_records_completed: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_splits_completed: int = Field(ge=0, le=MAX_DURABLE_COUNTER)
    total_retry_delay_seconds: float = Field(ge=0)

    @model_validator(mode="after")
    def totals_are_consistent(self) -> Self:
        """Reject summaries whose status and work totals are impossible."""

        if (
            self.healthy_count + self.degraded_count + self.unavailable_count
            != self.observation_count
        ):
            raise ValueError("health status counts must equal observation_count")
        if self.accepted_count > self.observation_count:
            raise ValueError("accepted_count cannot exceed observation_count")
        if self.total_window_traces < (self.total_windows_completed + self.total_splits_completed):
            raise ValueError("summary traces cannot be below windows plus splits")
        if self.total_source_requests != self.total_window_traces + self.total_retry_attempts:
            raise ValueError("summary requests must equal traces plus retries")
        if not isfinite(self.total_retry_delay_seconds):
            raise ValueError("summary retry delay must be finite")
        return self


def validate_public_trade_collection_transition(
    previous: PublicTradeCollectionCheckpoint,
    current: PublicTradeCollectionCheckpoint,
) -> None:
    """Reject stale, regressive, or unauthorized checkpoint transitions."""

    immutable_fields = (
        "job_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "policy_fingerprint",
        "window_start",
        "window_end_exclusive",
        "created_at",
    )
    if any(getattr(previous, field) != getattr(current, field) for field in immutable_fields):
        raise ValueError("public trade transition changed immutable job identity")
    if current.version != previous.version + 1:
        raise ValueError("public trade transition version must increase by exactly one")
    if current.updated_at < previous.updated_at:
        raise ValueError("public trade transition time must not regress")
    if current.next_window_start < previous.next_window_start:
        raise ValueError("public trade collection cursor must not regress")

    counters = (
        "windows_completed",
        "records_completed",
        "source_requests",
        "window_traces",
        "retry_attempts",
        "splits_completed",
    )
    if any(getattr(current, name) < getattr(previous, name) for name in counters):
        raise ValueError("public trade collection counters must not regress")

    cursor_advanced = current.next_window_start > previous.next_window_start
    windows_advanced = current.windows_completed > previous.windows_completed
    if cursor_advanced != windows_advanced:
        raise ValueError("cursor and completed-window progress must advance together")
    if current.records_completed > previous.records_completed and not windows_advanced:
        raise ValueError("completed records cannot advance without a completed window")

    window_delta = current.windows_completed - previous.windows_completed
    split_delta = current.splits_completed - previous.splits_completed
    trace_delta = current.window_traces - previous.window_traces
    retry_delta = current.retry_attempts - previous.retry_attempts
    request_delta = current.source_requests - previous.source_requests
    if trace_delta < window_delta + split_delta:
        raise ValueError("transition trace delta cannot be below windows plus splits")
    terminal_trace_delta = trace_delta - window_delta - split_delta
    if terminal_trace_delta > 1:
        raise ValueError("transition can contain at most one terminal window trace")
    if current.status is CollectionJobStatus.FAILED and terminal_trace_delta != 1:
        raise ValueError("failed transition requires one terminal window trace")
    if (
        current.status in {CollectionJobStatus.RUNNING, CollectionJobStatus.COMPLETED}
        and terminal_trace_delta != 0
    ):
        raise ValueError("accepted transition cannot contain a terminal window trace")
    if request_delta != trace_delta + retry_delta:
        raise ValueError("transition requests must equal window traces plus retry attempts")

    work_changed = (
        cursor_advanced
        or any(getattr(current, name) != getattr(previous, name) for name in counters)
        or current.pending_window_end_exclusive != previous.pending_window_end_exclusive
    )
    if (
        previous.status is CollectionJobStatus.RUNNING
        and work_changed
        and (previous.lease_expires_at is None or previous.lease_expires_at <= current.updated_at)
    ):
        raise ValueError("expired public trade collection lease cannot record work")

    if (
        previous.pending_window_end_exclusive is not None
        and current.next_window_start > previous.pending_window_end_exclusive
    ):
        raise ValueError("public trade transition skipped beyond the pending window")
    if (
        previous.pending_window_end_exclusive is not None
        and current.pending_window_end_exclusive is not None
        and current.pending_window_end_exclusive > previous.pending_window_end_exclusive
    ):
        raise ValueError("public trade transition expanded its pending window")

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
        raise ValueError("public trade collection status transition is not allowed")

    if previous.status is not CollectionJobStatus.RUNNING and work_changed:
        raise ValueError("claiming a public trade job cannot record collection work")

    if previous.status is CollectionJobStatus.RUNNING:
        terminal_or_paused = {
            CollectionJobStatus.COMPLETED,
            CollectionJobStatus.FAILED,
            CollectionJobStatus.PAUSED,
        }
        lease_is_active = (
            previous.lease_expires_at is not None and previous.lease_expires_at > current.updated_at
        )
        if current.status in terminal_or_paused:
            if current.lease_owner is not None or current.lease_token is not None:
                raise ValueError("terminal or paused transition must release its lease")
            if not lease_is_active:
                raise ValueError("expired public trade collection lease cannot finalize work")
        elif current.status is CollectionJobStatus.RUNNING:
            if lease_is_active and (
                current.lease_owner != previous.lease_owner
                or current.lease_token != previous.lease_token
            ):
                raise ValueError("active public trade collection lease cannot be taken over")
            if not lease_is_active and current.lease_token == previous.lease_token:
                raise ValueError("expired public trade collection lease requires a new token")
