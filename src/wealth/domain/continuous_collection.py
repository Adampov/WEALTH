"""Durable contracts for supervised continuous candle polling."""

from datetime import UTC, datetime
from enum import StrEnum
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

UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ContinuousCollectionStatus(StrEnum):
    """Operator-visible lifecycle for one supervised candle stream."""

    ACTIVE = "active"
    PAUSED = "paused"


class ContinuousCollectionRequest(BaseModel):
    """Immutable identity and initial cursor for one continuously polled stream."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe
    window_start: AwareDatetime

    @field_validator("source", "venue", "instrument", "provider_symbol")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject identifiers that would require implicit normalization."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("continuous collection identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def start_is_aligned(self) -> Self:
        """Require the first open time to use the timeframe's UTC grid."""

        if (self.window_start.astimezone(UTC) - UTC_EPOCH) % self.timeframe.duration:
            raise ValueError("continuous collection start must align to the timeframe UTC grid")
        return self


class ContinuousCollectionCheckpoint(BaseModel):
    """Current restart-safe cursor and reconnect state for one stream."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    collection_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe
    window_start: AwareDatetime
    next_window_start: AwareDatetime
    status: ContinuousCollectionStatus
    created_at: AwareDatetime
    updated_at: AwareDatetime
    version: int = Field(ge=1)
    active_job_id: UUID | None = None
    active_window_end_exclusive: AwareDatetime | None = None
    cycles_completed: int = Field(default=0, ge=0)
    candles_completed: int = Field(default=0, ge=0)
    consecutive_failures: int = Field(default=0, ge=0)
    next_retry_at: AwareDatetime | None = None
    last_failure_code: str | None = Field(default=None, min_length=1, max_length=128)
    last_stop_reason: str | None = Field(default=None, min_length=1, max_length=128)
    pause_reason: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator(
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "last_failure_code",
        "last_stop_reason",
        "pause_reason",
    )
    @classmethod
    def text_fields_are_canonical(cls, value: str | None) -> str | None:
        """Keep stream dimensions and machine reasons unambiguous."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("continuous collection fields must not contain whitespace")
        return value

    @model_validator(mode="after")
    def checkpoint_is_consistent(self) -> Self:
        """Tie the durable cursor, active work, retries, and pause state together."""

        if (self.window_start.astimezone(UTC) - UTC_EPOCH) % self.timeframe.duration:
            raise ValueError("continuous collection start must align to the timeframe UTC grid")
        if self.next_window_start < self.window_start:
            raise ValueError("continuous collection cursor cannot precede its start")
        if (self.next_window_start - self.window_start) % self.timeframe.duration:
            raise ValueError("continuous collection cursor must align to its timeframe")
        expected_candles = (self.next_window_start - self.window_start) // self.timeframe.duration
        if self.candles_completed != expected_candles:
            raise ValueError("candles_completed must equal durable continuous cursor progress")
        if self.created_at > self.updated_at:
            raise ValueError("continuous collection timestamps must not regress")

        has_job = self.active_job_id is not None
        has_job_end = self.active_window_end_exclusive is not None
        if has_job != has_job_end:
            raise ValueError("active job identity and window end must be set together")
        if self.active_window_end_exclusive is not None:
            if self.active_window_end_exclusive <= self.next_window_start:
                raise ValueError("active collection window must advance the durable cursor")
            if (
                self.active_window_end_exclusive - self.next_window_start
            ) % self.timeframe.duration:
                raise ValueError("active collection window must align to its timeframe")

        has_failure = self.consecutive_failures > 0
        failure_fields = self.last_failure_code is not None and self.last_stop_reason is not None
        if has_failure != failure_fields:
            raise ValueError("continuous failure count and evidence must be set together")
        if has_failure and not has_job:
            raise ValueError("continuous failure evidence requires an active recoverable job")

        if self.next_retry_at is not None:
            if self.status is not ContinuousCollectionStatus.ACTIVE or not has_failure:
                raise ValueError("next retry requires an active failed collection")
            if self.next_retry_at < self.updated_at:
                raise ValueError("continuous retry time cannot precede its checkpoint update")
        elif self.status is ContinuousCollectionStatus.ACTIVE and has_failure:
            raise ValueError("active failed collection requires a next retry time")

        if self.status is ContinuousCollectionStatus.PAUSED:
            if self.pause_reason is None:
                raise ValueError("paused continuous collection requires a reason")
            if self.next_retry_at is not None:
                raise ValueError("paused continuous collection cannot schedule an automatic retry")
        elif self.pause_reason is not None:
            raise ValueError("only a paused continuous collection can have a pause reason")
        return self


def validate_continuous_collection_transition(
    previous: ContinuousCollectionCheckpoint,
    current: ContinuousCollectionCheckpoint,
) -> None:
    """Reject stale, regressive, or unexplained continuous cursor changes."""

    immutable_fields = (
        "collection_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "timeframe",
        "window_start",
        "created_at",
    )
    if any(getattr(previous, field) != getattr(current, field) for field in immutable_fields):
        raise ValueError("continuous collection transition changed immutable identity")
    if current.version != previous.version + 1:
        raise ValueError("continuous collection version must increase by exactly one")
    if current.updated_at < previous.updated_at:
        raise ValueError("continuous collection transition time must not regress")
    if current.next_window_start < previous.next_window_start:
        raise ValueError("continuous collection cursor must not regress")
    if (
        current.cycles_completed < previous.cycles_completed
        or current.candles_completed < previous.candles_completed
    ):
        raise ValueError("continuous collection counters must not regress")

    cursor_advanced = current.next_window_start > previous.next_window_start
    if cursor_advanced:
        if previous.status is not ContinuousCollectionStatus.ACTIVE:
            raise ValueError("paused continuous collection must resume before cursor advancement")
        if (
            previous.active_window_end_exclusive is None
            or current.next_window_start != previous.active_window_end_exclusive
        ):
            raise ValueError("continuous cursor must advance to the exact active window end")
        if current.status is not ContinuousCollectionStatus.ACTIVE:
            raise ValueError("successful continuous cursor advancement must remain active")
        if current.active_job_id is not None:
            raise ValueError("completed continuous work must clear its active job")
        if current.cycles_completed != previous.cycles_completed + 1:
            raise ValueError("cursor advancement must complete exactly one polling cycle")
        if current.consecutive_failures != 0:
            raise ValueError("successful continuous work must clear consecutive failures")
    else:
        if current.cycles_completed != previous.cycles_completed:
            raise ValueError("cycle count cannot change without cursor advancement")
        if current.candles_completed != previous.candles_completed:
            raise ValueError("candle count cannot change without cursor advancement")

    if previous.active_job_id is None and current.active_job_id is not None:
        if current.status is not ContinuousCollectionStatus.ACTIVE:
            raise ValueError("only an active continuous collection can attach work")
        if cursor_advanced or current.consecutive_failures != previous.consecutive_failures:
            raise ValueError("attaching continuous work cannot also change progress or failures")
    elif previous.active_job_id is not None:
        if current.active_job_id not in {previous.active_job_id, None}:
            raise ValueError("continuous active job identity cannot be replaced")
        if current.active_job_id is None and not cursor_advanced:
            raise ValueError("continuous active job can clear only after exact cursor advancement")
        if (
            current.active_job_id == previous.active_job_id
            and current.active_window_end_exclusive != previous.active_window_end_exclusive
        ):
            raise ValueError("continuous active job window cannot change after attachment")

    if current.consecutive_failures > previous.consecutive_failures:
        if current.consecutive_failures != previous.consecutive_failures + 1:
            raise ValueError("continuous failures must increase one attempt at a time")
        if previous.status is not ContinuousCollectionStatus.ACTIVE:
            raise ValueError("only an active continuous collection can record a failure")
        if current.active_job_id != previous.active_job_id:
            raise ValueError("failure evidence must remain attached to the same job")
    elif current.consecutive_failures < previous.consecutive_failures:
        resumed = (
            previous.status is ContinuousCollectionStatus.PAUSED
            and current.status is ContinuousCollectionStatus.ACTIVE
            and current.consecutive_failures == 0
        )
        if not cursor_advanced and not resumed:
            raise ValueError("continuous failures can clear only after success or operator resume")
    elif (
        current.last_failure_code != previous.last_failure_code
        or current.last_stop_reason != previous.last_stop_reason
    ):
        raise ValueError("continuous failure evidence cannot change without a new failure")

    if (
        previous.status is ContinuousCollectionStatus.ACTIVE
        and current.status is ContinuousCollectionStatus.ACTIVE
        and current.consecutive_failures == previous.consecutive_failures > 0
        and current.next_retry_at != previous.next_retry_at
    ):
        raise ValueError("scheduled continuous retry cannot move without a new failure")
