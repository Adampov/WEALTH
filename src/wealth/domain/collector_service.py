"""Durable lifecycle evidence for the continuous collector service."""

from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator


class CollectorCycleStatus(StrEnum):
    """Stable service-facing projection of one continuous polling outcome."""

    ADVANCED = "advanced"
    CAUGHT_UP = "caught_up"
    WAITING = "waiting"
    RETRY_SCHEDULED = "retry_scheduled"
    PAUSED = "paused"
    ALREADY_RUNNING = "already_running"
    CHECKPOINT_CONFLICT = "checkpoint_conflict"
    LOST_LEASE = "lost_lease"


class CollectorServiceStatus(StrEnum):
    """Lifecycle states retained for one local service invocation."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    PAUSED = "paused"
    FAILED = "failed"
    CYCLE_LIMIT = "cycle_limit"


TERMINAL_COLLECTOR_SERVICE_STATUSES = frozenset(
    {
        CollectorServiceStatus.STOPPED,
        CollectorServiceStatus.PAUSED,
        CollectorServiceStatus.FAILED,
        CollectorServiceStatus.CYCLE_LIMIT,
    }
)


class CollectorServiceHeartbeat(BaseModel):
    """One immutable service lifecycle and liveness observation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    heartbeat_id: UUID
    run_id: UUID
    collection_id: UUID
    worker_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=1)
    observed_at: AwareDatetime
    status: CollectorServiceStatus
    cycles_attempted: int = Field(ge=0)
    checkpoint_version: int = Field(ge=1)
    next_window_start: AwareDatetime
    last_cycle_status: CollectorCycleStatus | None = None
    reason_code: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("worker_id", "reason_code")
    @classmethod
    def text_fields_are_canonical(cls, value: str | None) -> str | None:
        """Keep worker and machine-reason dimensions unambiguous."""

        if value is not None and (
            value != value.strip() or any(character.isspace() for character in value)
        ):
            raise ValueError("collector service fields must not contain whitespace")
        return value

    @model_validator(mode="after")
    def heartbeat_matches_status(self) -> Self:
        """Tie lifecycle status to sequence, cycle, and terminal evidence."""

        safe_cycles = {
            CollectorCycleStatus.ADVANCED,
            CollectorCycleStatus.CAUGHT_UP,
            CollectorCycleStatus.WAITING,
            CollectorCycleStatus.RETRY_SCHEDULED,
        }
        failure_cycles = {
            CollectorCycleStatus.ALREADY_RUNNING,
            CollectorCycleStatus.CHECKPOINT_CONFLICT,
            CollectorCycleStatus.LOST_LEASE,
        }
        if self.status is CollectorServiceStatus.STARTING:
            if (
                self.sequence != 1
                or self.cycles_attempted != 0
                or self.last_cycle_status is not None
                or self.reason_code is not None
            ):
                raise ValueError("starting heartbeat must be pristine sequence one")
        elif self.status is CollectorServiceStatus.RUNNING:
            if (
                self.cycles_attempted < 1
                or self.last_cycle_status not in safe_cycles
                or self.reason_code is not None
            ):
                raise ValueError("running heartbeat requires one safe completed cycle")
        elif self.status is CollectorServiceStatus.STOPPED:
            if self.reason_code != "shutdown_requested":
                raise ValueError("stopped service requires shutdown_requested reason")
            if self.last_cycle_status is not None and self.last_cycle_status not in safe_cycles:
                raise ValueError("stopped service cannot follow an unsafe cycle")
        elif self.status is CollectorServiceStatus.PAUSED:
            if (
                self.last_cycle_status is not CollectorCycleStatus.PAUSED
                or self.reason_code is None
            ):
                raise ValueError("paused service requires paused cycle and reason")
        elif self.status is CollectorServiceStatus.FAILED:
            if self.last_cycle_status not in failure_cycles or self.reason_code is None:
                raise ValueError("failed service requires an operational failure cycle and reason")
        elif self.status is CollectorServiceStatus.CYCLE_LIMIT and (
            self.cycles_attempted < 1
            or self.last_cycle_status not in safe_cycles
            or self.reason_code != "cycle_limit_reached"
        ):
            raise ValueError("cycle-limit service exit requires bounded completion evidence")
        return self


class CollectorServiceHeartbeatQuery(BaseModel):
    """Bounded ordered heartbeat history for one service invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    run_id: UUID
    limit: int = Field(default=100, ge=1, le=1_000)


def validate_collector_service_transition(
    previous: CollectorServiceHeartbeat,
    current: CollectorServiceHeartbeat,
) -> None:
    """Reject missing, regressive, or post-terminal heartbeat transitions."""

    identity = ("run_id", "collection_id", "worker_id")
    if any(getattr(previous, field) != getattr(current, field) for field in identity):
        raise ValueError("collector service transition changed immutable run identity")
    if previous.status in TERMINAL_COLLECTOR_SERVICE_STATUSES:
        raise ValueError("terminal collector service run cannot emit another heartbeat")
    if current.status is CollectorServiceStatus.STARTING:
        raise ValueError("starting heartbeat is valid only as the first run observation")
    if current.sequence != previous.sequence + 1:
        raise ValueError("collector service heartbeat sequence must increase by exactly one")
    if current.observed_at < previous.observed_at:
        raise ValueError("collector service heartbeat time must not regress")
    if current.cycles_attempted not in {
        previous.cycles_attempted,
        previous.cycles_attempted + 1,
    }:
        raise ValueError("collector service cycles must remain stable or increase by one")
    if current.checkpoint_version < previous.checkpoint_version:
        raise ValueError("collector service checkpoint version must not regress")
    if current.next_window_start < previous.next_window_start:
        raise ValueError("collector service market cursor must not regress")
    if current.cycles_attempted == previous.cycles_attempted + 1:
        if current.last_cycle_status is None:
            raise ValueError("attempted service cycle requires a cycle outcome")
    elif previous.last_cycle_status != current.last_cycle_status and not (
        previous.status is CollectorServiceStatus.STARTING
        and current.status is CollectorServiceStatus.STOPPED
    ):
        raise ValueError("service exit without a new cycle must retain its last outcome")
