"""Canonical domain-event contracts."""

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, model_validator


class Environment(StrEnum):
    """Runtime environments that must remain distinguishable in every record."""

    DEVELOPMENT = "development"
    TEST = "test"
    RESEARCH = "research"
    PAPER = "paper"
    LIVE = "live"


class EventType(StrEnum):
    """Event types available in the foundation slice."""

    SYSTEM_HEALTH = "system.health"


class DomainEvent(BaseModel):
    """Strict, immutable, versioned event shared across application boundaries."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    event_id: UUID
    correlation_id: UUID
    event_type: EventType
    source: str = Field(min_length=1, max_length=128)
    environment: Environment
    event_time: AwareDatetime
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    payload: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def timestamps_are_monotonic(self) -> Self:
        """Reject records that claim processing happened before observation."""

        if self.event_time > self.observed_at:
            raise ValueError("event_time must not be after observed_at")
        if self.observed_at > self.processed_at:
            raise ValueError("observed_at must not be after processed_at")
        return self

    @property
    def timestamp(self) -> datetime:
        """Return the canonical event timestamp as a standard datetime."""

        return self.event_time
