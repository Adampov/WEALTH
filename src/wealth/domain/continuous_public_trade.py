"""Pure contracts for planning closed-window continuous public-trade work."""

import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.market import InstrumentType

MAX_CONTRACT_INTEGER = 2**63 - 1
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
SHA256_FINGERPRINT_PATTERN = r"^sha256:[0-9a-f]{64}$"
_SHA256_FINGERPRINT = re.compile(SHA256_FINGERPRINT_PATTERN)


def _require_builtin_int(value: object, field_name: str) -> int:
    """Return an exact built-in integer without coercion."""

    if type(value) is not int:
        raise ValueError(f"{field_name} must be an exact built-in int")
    return value


def _require_builtin_string(value: object, field_name: str) -> str:
    """Return an exact built-in string without coercion."""

    if type(value) is not str:
        raise ValueError(f"{field_name} must be an exact built-in str")
    return value


def _require_fingerprint(value: object, field_name: str) -> str:
    """Return one canonical lowercase SHA-256 fingerprint."""

    fingerprint = _require_builtin_string(value, field_name)
    if _SHA256_FINGERPRINT.fullmatch(fingerprint) is None:
        raise ValueError(f"{field_name} must be a canonical sha256 fingerprint")
    return fingerprint


def _epoch_milliseconds(now: datetime) -> int:
    """Convert one exact UTC datetime to integer epoch milliseconds."""

    if type(now) is not datetime or now.tzinfo is not UTC:
        raise ValueError("now must be an exact datetime whose tzinfo is datetime.UTC")
    delta = now - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


class ContinuousPublicTradeStreamStatus(StrEnum):
    """Durable operator-controlled state of one public-trade stream."""

    ACTIVE = "active"
    PAUSED = "paused"


class ContinuousPublicTradeServiceStatus(StrEnum):
    """Finite lifecycle states for a possible future service invocation."""

    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    PAUSED = "paused"
    FAILED = "failed"
    RUN_LIMIT = "run_limit"


class ContinuousPublicTradePlanStatus(StrEnum):
    """Side-effect-free outcome of one closed-window planning decision."""

    HELD = "held"
    WAITING = "waiting"
    ATTACHED_JOB = "attached_job"


class ContinuousPublicTradeTransitionKind(StrEnum):
    """Explicit reason for one validated stream-checkpoint transition."""

    RETAIN = "retain"
    ATTACH = "attach"
    CHILD_COMPLETED = "child_completed"
    MANUAL_HOLD = "manual_hold"
    MANUAL_RESUME = "manual_resume"


class ContinuousPublicTradePolicy(BaseModel):
    """Finite immutable bounds for pure continuous public-trade planning."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    schema_version: Literal["1.0"] = "1.0"
    window_size_ms: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    settlement_lag_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    max_catchup_span_ms: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_jobs_per_invocation: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_requests_per_job: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_records_per_job: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    policy_fingerprint: str = Field(pattern=SHA256_FINGERPRINT_PATTERN)

    @field_validator(
        "window_size_ms",
        "settlement_lag_ms",
        "max_catchup_span_ms",
        "max_jobs_per_invocation",
        "max_requests_per_job",
        "max_records_per_job",
        mode="before",
    )
    @classmethod
    def integer_fields_are_exact(cls, value: object, info: object) -> int:
        """Reject booleans, integer subclasses, and coerced numeric values."""

        field_name = getattr(info, "field_name", "policy integer")
        return _require_builtin_int(value, str(field_name))

    @field_validator("schema_version", mode="before")
    @classmethod
    def schema_version_is_exact(cls, value: object) -> str:
        """Reject coerced or subclassed schema-version strings."""

        return _require_builtin_string(value, "schema_version")

    @field_validator("policy_fingerprint", mode="before")
    @classmethod
    def fingerprint_is_exact(cls, value: object) -> str:
        """Require one complete lowercase policy fingerprint."""

        return _require_fingerprint(value, "policy_fingerprint")

    @model_validator(mode="after")
    def catchup_span_uses_complete_windows(self) -> Self:
        """Keep every finite catch-up range on the selected epoch grid."""

        if self.max_catchup_span_ms % self.window_size_ms:
            raise ValueError("max_catchup_span_ms must be a multiple of window_size_ms")
        return self


class ContinuousPublicTradeAttachment(BaseModel):
    """Exact immutable bounded-child identity attached to one stream cursor."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    job_id: UUID
    window_start_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    window_end_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    policy_fingerprint: str = Field(pattern=SHA256_FINGERPRINT_PATTERN)
    creation_fingerprint: str = Field(pattern=SHA256_FINGERPRINT_PATTERN)

    @field_validator("window_start_epoch_ms", "window_end_epoch_ms", mode="before")
    @classmethod
    def boundaries_are_exact_integers(cls, value: object, info: object) -> int:
        """Reject coerced or subclassed millisecond boundaries."""

        field_name = getattr(info, "field_name", "attachment boundary")
        return _require_builtin_int(value, str(field_name))

    @field_validator("policy_fingerprint", "creation_fingerprint", mode="before")
    @classmethod
    def fingerprints_are_exact(cls, value: object, info: object) -> str:
        """Require complete lowercase attachment fingerprints."""

        field_name = getattr(info, "field_name", "attachment fingerprint")
        return _require_fingerprint(value, str(field_name))

    @model_validator(mode="after")
    def range_is_nonempty(self) -> Self:
        """Require one advancing half-open bounded-child range."""

        if self.window_end_epoch_ms <= self.window_start_epoch_ms:
            raise ValueError("attachment end must be after its start")
        return self


class ContinuousPublicTradeStreamCheckpoint(BaseModel):
    """Immutable-identity cursor and optional exact child attachment."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    schema_version: Literal["1.0"] = "1.0"
    stream_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    request_variant: str = Field(min_length=1, max_length=128)
    policy_fingerprint: str = Field(pattern=SHA256_FINGERPRINT_PATTERN)
    stream_start_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    cursor_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    status: ContinuousPublicTradeStreamStatus
    pause_reason: str | None = Field(default=None, min_length=1, max_length=128)
    attachment: ContinuousPublicTradeAttachment | None = None
    version: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)

    @field_validator("stream_start_epoch_ms", "cursor_epoch_ms", "version", mode="before")
    @classmethod
    def integer_fields_are_exact(cls, value: object, info: object) -> int:
        """Reject coerced or subclassed checkpoint integers."""

        field_name = getattr(info, "field_name", "checkpoint integer")
        return _require_builtin_int(value, str(field_name))

    @field_validator(
        "schema_version",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "request_variant",
        "pause_reason",
        mode="before",
    )
    @classmethod
    def text_fields_are_exact(cls, value: object, info: object) -> str | None:
        """Keep checkpoint text exact and free of implicit normalization."""

        if value is None:
            return None
        field_name = str(getattr(info, "field_name", "checkpoint text"))
        text = _require_builtin_string(value, field_name)
        if text != text.strip() or any(character.isspace() for character in text):
            raise ValueError(f"{field_name} must not contain whitespace")
        return text

    @field_validator("policy_fingerprint", mode="before")
    @classmethod
    def fingerprint_is_exact(cls, value: object) -> str:
        """Require one complete lowercase stream policy fingerprint."""

        return _require_fingerprint(value, "policy_fingerprint")

    @model_validator(mode="after")
    def checkpoint_invariants_hold(self) -> Self:
        """Tie progress, manual hold state, and child identity together."""

        if self.cursor_epoch_ms < self.stream_start_epoch_ms:
            raise ValueError("stream cursor cannot precede stream start")
        if self.status is ContinuousPublicTradeStreamStatus.PAUSED:
            if self.pause_reason is None:
                raise ValueError("paused stream requires a manual pause reason")
        elif self.pause_reason is not None:
            raise ValueError("active stream cannot carry a pause reason")
        if self.attachment is not None:
            if self.attachment.window_start_epoch_ms != self.cursor_epoch_ms:
                raise ValueError("attached child must begin exactly at the durable cursor")
            if self.attachment.policy_fingerprint != self.policy_fingerprint:
                raise ValueError("attached child policy fingerprint must match its stream")
        return self


class ContinuousPublicTradePlan(BaseModel):
    """One pure planning result that performs and authorizes no action."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, validate_default=True)

    schema_version: Literal["1.0"] = "1.0"
    status: ContinuousPublicTradePlanStatus
    stream_id: UUID
    policy_fingerprint: str = Field(pattern=SHA256_FINGERPRINT_PATTERN)
    cursor_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    latest_eligible_end_epoch_ms: int = Field(
        ge=-MAX_CONTRACT_INTEGER,
        le=MAX_CONTRACT_INTEGER,
    )
    attachment: ContinuousPublicTradeAttachment | None = None

    @field_validator("cursor_epoch_ms", "latest_eligible_end_epoch_ms", mode="before")
    @classmethod
    def integer_fields_are_exact(cls, value: object, info: object) -> int:
        """Reject coerced or subclassed plan boundaries."""

        field_name = getattr(info, "field_name", "plan boundary")
        return _require_builtin_int(value, str(field_name))

    @field_validator("schema_version", mode="before")
    @classmethod
    def schema_version_is_exact(cls, value: object) -> str:
        """Reject coerced or subclassed schema-version strings."""

        return _require_builtin_string(value, "schema_version")

    @field_validator("policy_fingerprint", mode="before")
    @classmethod
    def fingerprint_is_exact(cls, value: object) -> str:
        """Require one complete lowercase plan policy fingerprint."""

        return _require_fingerprint(value, "policy_fingerprint")

    @model_validator(mode="after")
    def outcome_is_consistent(self) -> Self:
        """Tie plan status to an optional exact immutable attachment."""

        if self.attachment is not None:
            if self.attachment.window_start_epoch_ms != self.cursor_epoch_ms:
                raise ValueError("planned attachment must begin at the durable cursor")
            if self.attachment.policy_fingerprint != self.policy_fingerprint:
                raise ValueError("planned attachment policy fingerprint must match the plan")
        if self.status is ContinuousPublicTradePlanStatus.ATTACHED_JOB:
            if self.attachment is None:
                raise ValueError("attached-job plan requires an attachment")
        elif self.status is ContinuousPublicTradePlanStatus.WAITING and (
            self.attachment is not None or self.cursor_epoch_ms < self.latest_eligible_end_epoch_ms
        ):
            raise ValueError("waiting plan requires no attachment and no eligible cursor range")
        return self


def _validate_planning_inputs(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
    policy: ContinuousPublicTradePolicy,
) -> None:
    """Validate cross-model policy identity and UTC-grid bounds."""

    if type(checkpoint) is not ContinuousPublicTradeStreamCheckpoint:
        raise ValueError("checkpoint must be an exact continuous public-trade checkpoint")
    if type(policy) is not ContinuousPublicTradePolicy:
        raise ValueError("policy must be an exact continuous public-trade policy")
    if checkpoint.policy_fingerprint != policy.policy_fingerprint:
        raise ValueError("checkpoint and policy fingerprints must match")
    if checkpoint.stream_start_epoch_ms % policy.window_size_ms:
        raise ValueError("stream start must align to the policy's epoch grid")
    if checkpoint.cursor_epoch_ms % policy.window_size_ms:
        raise ValueError("stream cursor must align to the policy's epoch grid")
    if checkpoint.attachment is not None:
        attachment = checkpoint.attachment
        if (
            attachment.window_start_epoch_ms % policy.window_size_ms
            or attachment.window_end_epoch_ms % policy.window_size_ms
        ):
            raise ValueError("attached child range must align to the policy's epoch grid")
        if (
            attachment.window_end_epoch_ms - attachment.window_start_epoch_ms
            > policy.max_catchup_span_ms
        ):
            raise ValueError("attached child range exceeds the finite catch-up span")


def plan_continuous_public_trade_window(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
    policy: ContinuousPublicTradePolicy,
    now: datetime,
    *,
    candidate_job_id: UUID | None = None,
    candidate_creation_fingerprint: str | None = None,
) -> ContinuousPublicTradePlan:
    """Select held, waiting, or one exact bounded child without side effects."""

    _validate_planning_inputs(checkpoint, policy)
    now_epoch_ms = _epoch_milliseconds(now)
    unsettled_boundary = now_epoch_ms - policy.settlement_lag_ms
    latest_eligible_end_epoch_ms = (
        unsettled_boundary // policy.window_size_ms
    ) * policy.window_size_ms
    if not -MAX_CONTRACT_INTEGER <= latest_eligible_end_epoch_ms <= MAX_CONTRACT_INTEGER:
        raise ValueError("latest eligible end is outside the representable contract range")

    if checkpoint.status is ContinuousPublicTradeStreamStatus.PAUSED:
        return ContinuousPublicTradePlan(
            status=ContinuousPublicTradePlanStatus.HELD,
            stream_id=checkpoint.stream_id,
            policy_fingerprint=policy.policy_fingerprint,
            cursor_epoch_ms=checkpoint.cursor_epoch_ms,
            latest_eligible_end_epoch_ms=latest_eligible_end_epoch_ms,
            attachment=checkpoint.attachment,
        )
    if checkpoint.attachment is not None:
        return ContinuousPublicTradePlan(
            status=ContinuousPublicTradePlanStatus.ATTACHED_JOB,
            stream_id=checkpoint.stream_id,
            policy_fingerprint=policy.policy_fingerprint,
            cursor_epoch_ms=checkpoint.cursor_epoch_ms,
            latest_eligible_end_epoch_ms=latest_eligible_end_epoch_ms,
            attachment=checkpoint.attachment,
        )
    if checkpoint.cursor_epoch_ms >= latest_eligible_end_epoch_ms:
        return ContinuousPublicTradePlan(
            status=ContinuousPublicTradePlanStatus.WAITING,
            stream_id=checkpoint.stream_id,
            policy_fingerprint=policy.policy_fingerprint,
            cursor_epoch_ms=checkpoint.cursor_epoch_ms,
            latest_eligible_end_epoch_ms=latest_eligible_end_epoch_ms,
            attachment=None,
        )
    if type(candidate_job_id) is not UUID:
        raise ValueError("due work requires an exact candidate UUID")
    creation_fingerprint = _require_fingerprint(
        candidate_creation_fingerprint,
        "candidate_creation_fingerprint",
    )
    target_end_epoch_ms = min(
        latest_eligible_end_epoch_ms,
        checkpoint.cursor_epoch_ms + policy.max_catchup_span_ms,
    )
    attachment = ContinuousPublicTradeAttachment(
        job_id=candidate_job_id,
        window_start_epoch_ms=checkpoint.cursor_epoch_ms,
        window_end_epoch_ms=target_end_epoch_ms,
        policy_fingerprint=policy.policy_fingerprint,
        creation_fingerprint=creation_fingerprint,
    )
    return ContinuousPublicTradePlan(
        status=ContinuousPublicTradePlanStatus.ATTACHED_JOB,
        stream_id=checkpoint.stream_id,
        policy_fingerprint=policy.policy_fingerprint,
        cursor_epoch_ms=checkpoint.cursor_epoch_ms,
        latest_eligible_end_epoch_ms=latest_eligible_end_epoch_ms,
        attachment=attachment,
    )


def _checkpoint_fields_unchanged(
    previous: ContinuousPublicTradeStreamCheckpoint,
    current: ContinuousPublicTradeStreamCheckpoint,
    *,
    allowed_changes: frozenset[str],
) -> bool:
    """Return whether every field outside an explicit change set is equal."""

    return all(
        field_name in allowed_changes
        or getattr(previous, field_name) == getattr(current, field_name)
        for field_name in ContinuousPublicTradeStreamCheckpoint.model_fields
    )


def validate_continuous_public_trade_stream_transition(
    previous: ContinuousPublicTradeStreamCheckpoint,
    current: ContinuousPublicTradeStreamCheckpoint,
    kind: ContinuousPublicTradeTransitionKind,
    *,
    policy: ContinuousPublicTradePolicy,
    completed_job_id: UUID | None = None,
) -> None:
    """Reject unexplained, regressive, or authority-ambiguous stream changes."""

    _validate_planning_inputs(previous, policy)
    _validate_planning_inputs(current, policy)
    if (
        type(previous) is not ContinuousPublicTradeStreamCheckpoint
        or type(current) is not ContinuousPublicTradeStreamCheckpoint
    ):
        raise ValueError("stream transition requires exact checkpoint models")
    if type(kind) is not ContinuousPublicTradeTransitionKind:
        raise ValueError("stream transition kind must be explicit")
    immutable_fields = (
        "stream_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "request_variant",
        "policy_fingerprint",
        "stream_start_epoch_ms",
    )
    if any(getattr(previous, name) != getattr(current, name) for name in immutable_fields):
        raise ValueError("stream transition changed immutable identity or policy")
    if current.version != previous.version + 1:
        raise ValueError("stream checkpoint version must increase by exactly one")
    if (
        kind is not ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
        and completed_job_id is not None
    ):
        raise ValueError("completed_job_id is valid only for child completion")

    if kind is ContinuousPublicTradeTransitionKind.RETAIN:
        if not _checkpoint_fields_unchanged(
            previous,
            current,
            allowed_changes=frozenset({"version"}),
        ):
            raise ValueError("retain transition may change only the version")
        return

    if kind is ContinuousPublicTradeTransitionKind.ATTACH:
        if (
            previous.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or current.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or previous.attachment is not None
            or current.attachment is None
            or not _checkpoint_fields_unchanged(
                previous,
                current,
                allowed_changes=frozenset({"attachment", "version"}),
            )
        ):
            raise ValueError("attach transition must add one child without progress")
        return

    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        attachment = previous.attachment
        if (
            type(completed_job_id) is not UUID
            or attachment is None
            or completed_job_id != attachment.job_id
            or previous.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or current.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or current.attachment is not None
            or current.cursor_epoch_ms != attachment.window_end_epoch_ms
            or not _checkpoint_fields_unchanged(
                previous,
                current,
                allowed_changes=frozenset({"cursor_epoch_ms", "attachment", "version"}),
            )
        ):
            raise ValueError("child completion must clear and advance the exact attached job")
        return

    if kind is ContinuousPublicTradeTransitionKind.MANUAL_HOLD:
        if (
            previous.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or current.status is not ContinuousPublicTradeStreamStatus.PAUSED
            or not _checkpoint_fields_unchanged(
                previous,
                current,
                allowed_changes=frozenset({"status", "pause_reason", "version"}),
            )
        ):
            raise ValueError("manual hold cannot change progress or attachment")
        return

    if kind is ContinuousPublicTradeTransitionKind.MANUAL_RESUME:
        if (
            previous.status is not ContinuousPublicTradeStreamStatus.PAUSED
            or current.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or not _checkpoint_fields_unchanged(
                previous,
                current,
                allowed_changes=frozenset({"status", "pause_reason", "version"}),
            )
        ):
            raise ValueError("manual resume cannot change progress or attachment")
        return

    raise ValueError("unsupported continuous public-trade transition kind")


def validate_continuous_public_trade_service_transition(
    previous: ContinuousPublicTradeServiceStatus | None,
    current: ContinuousPublicTradeServiceStatus,
) -> None:
    """Allow only creation, running entry, and one finite terminal transition."""

    if previous is not None and type(previous) is not ContinuousPublicTradeServiceStatus:
        raise ValueError("previous service status must be explicit")
    if type(current) is not ContinuousPublicTradeServiceStatus:
        raise ValueError("current service status must be explicit")
    if previous is None:
        if current is not ContinuousPublicTradeServiceStatus.STARTING:
            raise ValueError("service lifecycle must begin with STARTING")
        return
    if previous is ContinuousPublicTradeServiceStatus.STARTING:
        if current is not ContinuousPublicTradeServiceStatus.RUNNING:
            raise ValueError("STARTING may transition only to RUNNING")
        return
    if previous is ContinuousPublicTradeServiceStatus.RUNNING:
        terminal_statuses = {
            ContinuousPublicTradeServiceStatus.STOPPED,
            ContinuousPublicTradeServiceStatus.PAUSED,
            ContinuousPublicTradeServiceStatus.FAILED,
            ContinuousPublicTradeServiceStatus.RUN_LIMIT,
        }
        if current not in terminal_statuses:
            raise ValueError("RUNNING may transition only to one terminal status")
        return
    raise ValueError("terminal service status cannot transition")
