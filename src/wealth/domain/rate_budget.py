"""Provider-independent request-budget contracts and metrics."""

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

MAX_RATE_BUDGET_CAPACITY = 1_000_000
MAX_RATE_BUDGET_PERIOD_SECONDS = 3_600


class RateBudgetDecisionStatus(StrEnum):
    """Whether one idempotent request reservation may proceed."""

    GRANTED = "granted"
    DENIED = "denied"


class RateBudgetPolicy(BaseModel):
    """One bounded GCRA request budget shared by cooperating workers."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    budget_key: str = Field(min_length=1, max_length=128)
    capacity: int = Field(ge=1, le=MAX_RATE_BUDGET_CAPACITY)
    period_seconds: int = Field(ge=1, le=MAX_RATE_BUDGET_PERIOD_SECONDS)

    @field_validator("budget_key")
    @classmethod
    def budget_key_is_unambiguous(cls, value: str) -> str:
        """Reject implicit normalization in the shared coordination key."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("budget_key must not contain whitespace")
        return value

    @property
    def interval_microseconds(self) -> int:
        """Return a conservative integer emission interval."""

        period_microseconds = self.period_seconds * 1_000_000
        return (period_microseconds + self.capacity - 1) // self.capacity


class RateBudgetRequest(BaseModel):
    """One idempotent attempt to reserve shared request capacity."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    reservation_id: UUID
    budget_key: str = Field(min_length=1, max_length=128)
    requested_at: AwareDatetime
    cost: int = Field(ge=1, le=MAX_RATE_BUDGET_CAPACITY)

    @field_validator("budget_key")
    @classmethod
    def budget_key_is_unambiguous(cls, value: str) -> str:
        """Keep the request identity canonical."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("budget_key must not contain whitespace")
        return value


class RateBudgetDecision(BaseModel):
    """Durable result of one shared request-budget reservation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    reservation_id: UUID
    budget_key: str = Field(min_length=1, max_length=128)
    requested_at: AwareDatetime
    cost: int = Field(ge=1, le=MAX_RATE_BUDGET_CAPACITY)
    capacity: int = Field(ge=1, le=MAX_RATE_BUDGET_CAPACITY)
    period_seconds: int = Field(ge=1, le=MAX_RATE_BUDGET_PERIOD_SECONDS)
    status: RateBudgetDecisionStatus
    reason_code: Literal["granted", "budget_exhausted"]
    retry_after_seconds: int | None = Field(default=None, ge=1)
    available_capacity: int = Field(ge=0)
    theoretical_arrival_at: AwareDatetime

    @model_validator(mode="after")
    def decision_invariants_hold(self) -> Self:
        """Tie status, cost, capacity, and retry evidence together."""

        if self.cost > self.capacity:
            raise ValueError("rate-budget request cost cannot exceed capacity")
        if self.available_capacity > self.capacity:
            raise ValueError("available_capacity cannot exceed capacity")
        if self.theoretical_arrival_at < self.requested_at:
            raise ValueError("theoretical arrival time cannot precede the request")
        if self.status is RateBudgetDecisionStatus.GRANTED:
            if self.reason_code != "granted" or self.retry_after_seconds is not None:
                raise ValueError("granted rate-budget decision cannot require a retry")
        elif self.reason_code != "budget_exhausted" or self.retry_after_seconds is None:
            raise ValueError("denied rate-budget decision requires bounded retry evidence")
        return self


class RateBudgetReservationResult(BaseModel):
    """Expose whether a durable decision was newly recorded or replayed."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    decision: RateBudgetDecision
    replayed: bool = False


class RateBudgetSummary(BaseModel):
    """Query-efficient aggregate evidence for one shared budget."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    budget_key: str = Field(min_length=1, max_length=128)
    reservation_count: int = Field(ge=0)
    granted_count: int = Field(ge=0)
    denied_count: int = Field(ge=0)
    total_requested_cost: int = Field(ge=0)
    total_retry_after_seconds: int = Field(ge=0)
    maximum_retry_after_seconds: int = Field(ge=0)

    @model_validator(mode="after")
    def summary_invariants_hold(self) -> Self:
        """Reject aggregate counters that cannot describe real decisions."""

        if self.granted_count + self.denied_count != self.reservation_count:
            raise ValueError("rate-budget status counts must equal reservation_count")
        if self.total_requested_cost < self.reservation_count:
            raise ValueError("every rate-budget reservation must cost at least one")
        if self.maximum_retry_after_seconds > self.total_retry_after_seconds:
            raise ValueError("maximum retry delay cannot exceed the total")
        return self
