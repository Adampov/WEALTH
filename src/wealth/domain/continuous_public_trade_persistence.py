"""Pure canonical persistence contracts for continuous public-trade streams.

This module deliberately performs no I/O and is not imported by any runtime composition.  It
freezes the version-one projections selected by ADR 0029 without selecting a port, repository,
physical schema, clock, evidence store, or execution authority.
"""

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Final, Literal, Never, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from wealth.domain.canonical_utc import (
    CanonicalUtcError,
    from_epoch_microseconds,
    parse_canonical_utc,
    serialize_canonical_utc,
    to_epoch_microseconds,
)
from wealth.domain.collection import CollectionJobStatus
from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradeAttachment,
    ContinuousPublicTradePlan,
    ContinuousPublicTradePlanStatus,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeStreamCheckpoint,
    ContinuousPublicTradeStreamStatus,
    ContinuousPublicTradeTransitionKind,
    plan_continuous_public_trade_window,
    validate_continuous_public_trade_stream_transition,
)
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import PublicTradeCollectionCheckpoint

__all__ = [
    "MAX_CHILD_CREATION_BYTES",
    "MAX_ENVELOPE_BYTES",
    "MAX_JSON_DEPTH",
    "MAX_JSON_INTEGER_DIGITS",
    "MAX_JSON_KEY_BYTES",
    "MAX_JSON_MEMBERS",
    "MAX_JSON_STRING_LEXICAL_BYTES",
    "MAX_RAW_RECORD_BYTES",
    "MAX_SUCCESSOR_ENVELOPE_HEX_CHARS",
    "PROVISIONAL_CHILD_CREATION_FINGERPRINT",
    "ContinuousPublicTradeChildCreationPayloadV1",
    "ContinuousPublicTradeEvidenceKind",
    "ContinuousPublicTradeEvidenceOutcome",
    "ContinuousPublicTradeEvidenceReferenceV1",
    "ContinuousPublicTradeEvidenceScopeV1",
    "ContinuousPublicTradePersistenceContractError",
    "ContinuousPublicTradePersistenceErrorCode",
    "ContinuousPublicTradePolicyProjectionV1",
    "ContinuousPublicTradeStreamCreationRecordV1",
    "ContinuousPublicTradeStreamEnvelopeV1",
    "ContinuousPublicTradeStreamTransitionRecordV1",
    "child_creation_fingerprint",
    "decode_child_creation_payload",
    "decode_evidence_scope",
    "decode_stream_creation_record",
    "decode_stream_envelope",
    "decode_stream_transition_record",
    "encode_child_creation_payload",
    "encode_evidence_scope",
    "encode_stream_creation_record",
    "encode_stream_envelope",
    "encode_stream_transition_record",
    "evidence_scope_digest",
    "finalize_continuous_public_trade_attachment",
    "initial_stream_history_root",
    "next_stream_history_root",
    "project_continuous_public_trade_policy",
    "stream_creation_digest",
    "stream_envelope_digest",
    "stream_transition_digest",
    "validate_stream_creation_record_scope",
    "validate_stream_load_bindings",
    "validate_stream_transition_link",
    "validate_stream_transition_record_scopes",
]

MAX_RAW_RECORD_BYTES: Final[int] = 65_536
MAX_CHILD_CREATION_BYTES: Final[int] = 8_192
MAX_ENVELOPE_BYTES: Final[int] = 16_384
MAX_SUCCESSOR_ENVELOPE_HEX_CHARS: Final[int] = 32_768
MAX_JSON_STRING_LEXICAL_BYTES: Final[int] = 8_192
MAX_JSON_DEPTH: Final[int] = 16
MAX_JSON_MEMBERS: Final[int] = 128
MAX_JSON_KEY_BYTES: Final[int] = 64
MAX_JSON_INTEGER_DIGITS: Final[int] = 19

CHILD_CREATION_RECORD_TYPE: Final[Literal["wealth.continuous_public_trade.child_creation"]] = (
    "wealth.continuous_public_trade.child_creation"
)
STREAM_ENVELOPE_RECORD_TYPE: Final[Literal["wealth.continuous_public_trade.stream_envelope"]] = (
    "wealth.continuous_public_trade.stream_envelope"
)
STREAM_CREATION_RECORD_TYPE: Final[Literal["wealth.continuous_public_trade.stream_creation"]] = (
    "wealth.continuous_public_trade.stream_creation"
)
STREAM_TRANSITION_RECORD_TYPE: Final[
    Literal["wealth.continuous_public_trade.stream_transition"]
] = "wealth.continuous_public_trade.stream_transition"
MODEL_VERSION: Final[Literal["1.0"]] = "1.0"
SERIALIZATION_VERSION: Final[Literal[1]] = 1

PROVISIONAL_CHILD_CREATION_FINGERPRINT: Final[str] = "sha256:" + ("0" * 64)

_CHILD_CREATION_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.child_creation/v1\x00"
_STREAM_ENVELOPE_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.stream_record/v1\x00"
_STREAM_CREATION_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.stream_creation/v1\x00"
_STREAM_TRANSITION_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.stream_transition/v1\x00"
_EVIDENCE_SCOPE_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.evidence_scope/v1\x00"
_HISTORY_ROOT_INITIAL_DOMAIN: Final[bytes] = (
    b"wealth.continuous_public_trade.history_root/v1\x00\x01"
)
_HISTORY_ROOT_NEXT_DOMAIN: Final[bytes] = b"wealth.continuous_public_trade.history_root/v1\x00\x02"

_SHA256_PATTERN: Final[re.Pattern[str]] = re.compile(r"sha256:[0-9a-f]{64}")
_LOWERCASE_HEX_PATTERN: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]*")
_CANONICAL_UUID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
_VISIBLE_ASCII_PATTERN: Final[re.Pattern[str]] = re.compile(r"[\x21-\x7e]{1,128}")


class ContinuousPublicTradePersistenceErrorCode(StrEnum):
    """Stable sanitized failure classes exposed by the pure codec boundary."""

    RAW_INPUT = "raw_input"
    DUPLICATE_KEY = "duplicate_key"
    UNSUPPORTED_VERSION = "unsupported_version"
    MALFORMED_RECORD = "malformed_record"
    NON_CANONICAL = "non_canonical"
    INCONSISTENT = "inconsistent"


class ContinuousPublicTradePersistenceContractError(ValueError):
    """One fail-closed public exception that never echoes rejected content."""

    def __init__(self, code: ContinuousPublicTradePersistenceErrorCode) -> None:
        self.code = code
        super().__init__("continuous public-trade persistence contract rejected the supplied value")


def _fail(code: ContinuousPublicTradePersistenceErrorCode) -> Never:
    raise ContinuousPublicTradePersistenceContractError(code)


def _require_exact_string(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
    forbid_whitespace: bool = False,
) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be an exact built-in string")
    text = value
    if len(text) < minimum or (maximum is not None and len(text) > maximum):
        raise ValueError(f"{field_name} is outside its finite length bound")
    if forbid_whitespace and any(character.isspace() for character in text):
        raise ValueError(f"{field_name} must not contain whitespace")
    return text


def _require_exact_int(
    value: object,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an exact built-in integer")
    integer = value
    if minimum is not None and integer < minimum:
        raise ValueError(f"{field_name} is below its finite lower bound")
    if maximum is not None and integer > maximum:
        raise ValueError(f"{field_name} exceeds its finite upper bound")
    return integer


def _require_fingerprint(value: object, field_name: str) -> str:
    fingerprint = _require_exact_string(value, field_name)
    if _SHA256_PATTERN.fullmatch(fingerprint) is None:
        raise ValueError(f"{field_name} must be one canonical lowercase SHA-256 value")
    return fingerprint


def _require_exact_uuid(value: object, field_name: str) -> UUID:
    if type(value) is not UUID:
        raise ValueError(f"{field_name} must be an exact UUID")
    return value


def _require_exact_enum[E: StrEnum](
    value: object,
    enum_type: type[E],
    field_name: str,
) -> E:
    if type(value) is not enum_type:
        raise ValueError(f"{field_name} must be an exact {enum_type.__name__}")
    return value


def _require_exact_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is not UTC:
        raise ValueError(f"{field_name} must be an exact datetime.UTC datetime")
    try:
        serialized = serialize_canonical_utc(value)
        parsed = parse_canonical_utc(serialized)
    except CanonicalUtcError as error:
        raise ValueError(f"{field_name} must be an exact datetime.UTC datetime") from error
    if parsed != value:
        raise ValueError(f"{field_name} must be an exact datetime.UTC datetime")
    return value


def _require_reason(value: object, field_name: str) -> str:
    return _require_exact_string(
        value,
        field_name,
        minimum=1,
        maximum=128,
        forbid_whitespace=True,
    )


def _require_reference_id(value: object) -> str:
    evidence_id = _require_exact_string(value, "evidence_id")
    if _VISIBLE_ASCII_PATTERN.fullmatch(evidence_id) is None:
        raise ValueError("evidence_id must contain 1 through 128 visible ASCII characters")
    return evidence_id


def _epoch_milliseconds_to_datetime(value: object, field_name: str) -> datetime:
    milliseconds = _require_exact_int(
        value,
        field_name,
        minimum=0,
        maximum=MAX_CONTRACT_INTEGER,
    )
    try:
        result = from_epoch_microseconds(milliseconds * 1_000)
    except CanonicalUtcError as error:
        raise ValueError(
            f"{field_name} cannot be represented by the child datetime model"
        ) from error
    if to_epoch_microseconds(result) != milliseconds * 1_000:
        raise ValueError(f"{field_name} did not round-trip exactly")
    return result


def _datetime_to_epoch_milliseconds(value: object, field_name: str) -> int:
    timestamp = _require_exact_utc(value, field_name)
    try:
        microseconds = to_epoch_microseconds(timestamp)
    except CanonicalUtcError as error:
        raise ValueError(f"{field_name} cannot be projected exactly") from error
    if microseconds < 0 or microseconds % 1_000:
        raise ValueError(f"{field_name} must be a non-negative exact epoch millisecond")
    milliseconds = microseconds // 1_000
    if milliseconds > MAX_CONTRACT_INTEGER:
        raise ValueError(f"{field_name} exceeds the stream contract range")
    return milliseconds


class _StrictContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
        revalidate_instances="always",
    )


def _require_exact_declared_model_fields(
    value: BaseModel,
    model_type: type[BaseModel],
    field_name: str,
) -> None:
    """Reject bypass-constructed external values carrying undeclared hidden attributes."""

    try:
        stored_fields = value.__dict__
        supplied_fields = value.__pydantic_fields_set__
        extra_fields = value.__pydantic_extra__
        private_fields = value.__pydantic_private__
    except AttributeError as error:
        raise ValueError(f"{field_name} does not expose exact model storage") from error
    declared_fields = frozenset(model_type.model_fields)
    if (
        type(stored_fields) is not dict
        or frozenset(stored_fields) != declared_fields
        or type(supplied_fields) is not set
        or not supplied_fields <= declared_fields
        or (extra_fields is not None and (type(extra_fields) is not dict or extra_fields))
        or (private_fields is not None and (type(private_fields) is not dict or private_fields))
    ):
        raise ValueError(f"{field_name} contains undeclared or incomplete model fields")


def _require_no_private_model_storage(value: BaseModel, field_name: str) -> None:
    """Reject ignored private state anywhere in a canonical contract object graph."""

    pending: list[tuple[BaseModel, str]] = [(value, field_name)]
    visited: set[int] = set()
    while pending:
        current, current_name = pending.pop()
        identity = id(current)
        if identity in visited:
            raise ValueError(f"{current_name} contains recursive model storage")
        visited.add(identity)
        try:
            stored_fields = current.__dict__
            private_fields = current.__pydantic_private__
        except AttributeError as error:
            raise ValueError(f"{current_name} does not expose exact model storage") from error
        if type(stored_fields) is not dict or (
            private_fields is not None and (type(private_fields) is not dict or private_fields)
        ):
            raise ValueError(f"{current_name} contains undeclared private model state")
        pending.extend(
            (nested, f"{current_name}.{nested_name}")
            for nested_name, nested in stored_fields.items()
            if isinstance(nested, BaseModel)
        )


class ContinuousPublicTradePolicyProjectionV1(_StrictContractModel):
    """Frozen version-one projection of every TASK-059 stream-policy field."""

    schema_version: Literal["1.0"] = "1.0"
    window_size_ms: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    settlement_lag_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    max_catchup_span_ms: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_jobs_per_invocation: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_requests_per_job: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    max_records_per_job: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    policy_fingerprint: str

    @field_validator("schema_version", mode="before")
    @classmethod
    def schema_version_is_exact(cls, value: object) -> str:
        return _require_exact_string(value, "schema_version")

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
    def integers_are_exact(cls, value: object, info: object) -> int:
        return _require_exact_int(
            value,
            str(getattr(info, "field_name", "policy integer")),
        )

    @field_validator("policy_fingerprint", mode="before")
    @classmethod
    def fingerprint_is_exact(cls, value: object) -> str:
        return _require_fingerprint(value, "policy_fingerprint")

    @model_validator(mode="after")
    def complete_policy_is_valid(self) -> Self:
        ContinuousPublicTradePolicy(**self.model_dump())
        return self


class ContinuousPublicTradeEvidenceKind(StrEnum):
    """Closed evidence kinds permitted by the version-one record contract."""

    STREAM_CREATE_AUTHORITY = "STREAM_CREATE_AUTHORITY"
    STREAM_TRANSITION_AUTHORITY = "STREAM_TRANSITION_AUTHORITY"
    CHILD_COMPLETION = "CHILD_COMPLETION"


class ContinuousPublicTradeEvidenceOutcome(StrEnum):
    """Closed evidence outcomes permitted by the version-one record contract."""

    APPROVED = "APPROVED"
    ACCEPTED = "ACCEPTED"


class ContinuousPublicTradeEvidenceReferenceV1(_StrictContractModel):
    """Bounded scalar-only pointer to separately governed evidence."""

    reference_version: Literal[1] = 1
    evidence_kind: ContinuousPublicTradeEvidenceKind
    evidence_id: str
    evidence_digest: str
    scope_digest: str
    outcome: ContinuousPublicTradeEvidenceOutcome
    valid_from: datetime
    expires_at: datetime | None = None

    @field_validator("reference_version", mode="before")
    @classmethod
    def version_is_exact(cls, value: object) -> int:
        return _require_exact_int(value, "reference_version")

    @field_validator("evidence_kind", mode="before")
    @classmethod
    def kind_is_exact(cls, value: object) -> ContinuousPublicTradeEvidenceKind:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeEvidenceKind,
            "evidence_kind",
        )

    @field_validator("evidence_id", mode="before")
    @classmethod
    def identifier_is_exact(cls, value: object) -> str:
        return _require_reference_id(value)

    @field_validator("evidence_digest", "scope_digest", mode="before")
    @classmethod
    def digests_are_exact(cls, value: object, info: object) -> str:
        return _require_fingerprint(value, str(getattr(info, "field_name", "digest")))

    @field_validator("outcome", mode="before")
    @classmethod
    def outcome_is_exact(cls, value: object) -> ContinuousPublicTradeEvidenceOutcome:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeEvidenceOutcome,
            "outcome",
        )

    @field_validator("valid_from", "expires_at", mode="before")
    @classmethod
    def timestamps_are_exact(cls, value: object, info: object) -> datetime | None:
        if value is None:
            return None
        return _require_exact_utc(value, str(getattr(info, "field_name", "evidence time")))

    @model_validator(mode="after")
    def reference_is_consistent(self) -> Self:
        expected_outcome = (
            ContinuousPublicTradeEvidenceOutcome.ACCEPTED
            if self.evidence_kind is ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
            else ContinuousPublicTradeEvidenceOutcome.APPROVED
        )
        if self.outcome is not expected_outcome:
            raise ValueError("evidence kind and outcome do not agree")
        if self.expires_at is not None and self.expires_at <= self.valid_from:
            raise ValueError("evidence expiry must be strictly after valid_from")
        return self


class ContinuousPublicTradeEvidenceScopeV1(_StrictContractModel):
    """Exact domain-separated scope whose digest is retained by an evidence reference."""

    evidence_kind: ContinuousPublicTradeEvidenceKind
    stream_id: UUID
    transition_kind: ContinuousPublicTradeTransitionKind | None
    prior_version: int | None
    prior_envelope_digest: str | None
    prior_history_root: str | None
    successor_version: int
    successor_envelope_digest: str | None
    child_job_id: UUID | None
    child_policy_fingerprint: str | None
    child_creation_fingerprint: str | None
    reason_code: str | None
    stream_policy: ContinuousPublicTradePolicyProjectionV1 | None

    @field_validator("evidence_kind", mode="before")
    @classmethod
    def evidence_kind_is_exact(cls, value: object) -> ContinuousPublicTradeEvidenceKind:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeEvidenceKind,
            "evidence_kind",
        )

    @field_validator("stream_id", "child_job_id", mode="before")
    @classmethod
    def uuids_are_exact(cls, value: object, info: object) -> UUID | None:
        if value is None:
            return None
        return _require_exact_uuid(value, str(getattr(info, "field_name", "UUID")))

    @field_validator("transition_kind", mode="before")
    @classmethod
    def transition_kind_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeTransitionKind | None:
        if value is None:
            return None
        return _require_exact_enum(
            value,
            ContinuousPublicTradeTransitionKind,
            "transition_kind",
        )

    @field_validator("prior_version", "successor_version", mode="before")
    @classmethod
    def versions_are_exact(cls, value: object, info: object) -> int | None:
        if value is None:
            return None
        return _require_exact_int(
            value,
            str(getattr(info, "field_name", "version")),
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )

    @field_validator(
        "prior_envelope_digest",
        "prior_history_root",
        "successor_envelope_digest",
        "child_policy_fingerprint",
        "child_creation_fingerprint",
        mode="before",
    )
    @classmethod
    def optional_digests_are_exact(cls, value: object, info: object) -> str | None:
        if value is None:
            return None
        return _require_fingerprint(value, str(getattr(info, "field_name", "digest")))

    @field_validator("reason_code", mode="before")
    @classmethod
    def reason_is_exact(cls, value: object) -> str | None:
        if value is None:
            return None
        return _require_reason(value, "reason_code")

    @field_validator("stream_policy", mode="before")
    @classmethod
    def policy_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradePolicyProjectionV1 | None:
        if value is None:
            return None
        if type(value) is not ContinuousPublicTradePolicyProjectionV1:
            raise ValueError("stream_policy must be an exact policy projection")
        return value

    @model_validator(mode="after")
    def scope_matrix_is_exact(self) -> Self:
        prior_values = (
            self.prior_version,
            self.prior_envelope_digest,
            self.prior_history_root,
        )
        child_values = (
            self.child_job_id,
            self.child_policy_fingerprint,
            self.child_creation_fingerprint,
        )

        if self.evidence_kind is ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY:
            if (
                self.transition_kind is not None
                or any(value is not None for value in prior_values)
                or any(value is not None for value in child_values)
                or self.successor_version != 1
                or self.successor_envelope_digest is None
                or self.reason_code is not None
                or self.stream_policy is None
            ):
                raise ValueError("stream-create evidence scope has inconsistent bindings")
            return self

        if self.transition_kind is None:
            raise ValueError("non-create evidence scope requires a transition kind")
        if any(value is None for value in prior_values):
            raise ValueError("transition evidence scope requires every prior binding")
        if self.prior_version is None or self.successor_version != self.prior_version + 1:
            raise ValueError("transition scope versions must be contiguous")
        if self.stream_policy is not None:
            raise ValueError("transition evidence scope cannot carry a stream policy")

        if self.evidence_kind is ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY:
            if self.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH:
                if (
                    self.successor_envelope_digest is not None
                    or self.child_job_id is None
                    or self.child_policy_fingerprint is None
                    or self.child_creation_fingerprint is not None
                    or self.reason_code is not None
                ):
                    raise ValueError("attach authority scope has inconsistent bindings")
                return self
            if self.successor_envelope_digest is None or any(
                value is not None for value in child_values
            ):
                raise ValueError("transition authority scope has inconsistent bindings")
            reason_required = self.transition_kind in {
                ContinuousPublicTradeTransitionKind.RETAIN,
                ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
            }
            if reason_required != (self.reason_code is not None):
                raise ValueError("transition authority reason does not match its kind")
            return self

        if self.evidence_kind is ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION:
            if (
                self.transition_kind is not ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
                or self.successor_envelope_digest is None
                or any(value is None for value in child_values)
                or self.reason_code is not None
            ):
                raise ValueError("child-completion scope has inconsistent bindings")
            return self

        raise ValueError("unsupported evidence scope kind")


def _validate_checkpoint_scalar_types(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
) -> None:
    if type(checkpoint) is not ContinuousPublicTradeStreamCheckpoint:
        raise ValueError("checkpoint must be an exact TASK-059 checkpoint")
    _require_exact_declared_model_fields(
        checkpoint,
        ContinuousPublicTradeStreamCheckpoint,
        "checkpoint",
    )
    exact_strings = (
        "schema_version",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "request_variant",
        "policy_fingerprint",
    )
    if any(type(getattr(checkpoint, name)) is not str for name in exact_strings):
        raise ValueError("checkpoint contains a polymorphic string")
    if checkpoint.pause_reason is not None and type(checkpoint.pause_reason) is not str:
        raise ValueError("checkpoint pause_reason must be an exact string")
    if type(checkpoint.stream_id) is not UUID:
        raise ValueError("checkpoint stream_id must be an exact UUID")
    if type(checkpoint.instrument_type) is not InstrumentType:
        raise ValueError("checkpoint instrument_type must be exact")
    if type(checkpoint.status) is not ContinuousPublicTradeStreamStatus:
        raise ValueError("checkpoint status must be exact")
    if any(
        type(getattr(checkpoint, name)) is not int
        for name in ("stream_start_epoch_ms", "cursor_epoch_ms", "version")
    ):
        raise ValueError("checkpoint contains a polymorphic integer")
    attachment = checkpoint.attachment
    if attachment is not None:
        if type(attachment) is not ContinuousPublicTradeAttachment:
            raise ValueError("checkpoint attachment must be exact")
        _require_exact_declared_model_fields(
            attachment,
            ContinuousPublicTradeAttachment,
            "checkpoint attachment",
        )
        if type(attachment.job_id) is not UUID:
            raise ValueError("attachment job_id must be an exact UUID")
        if any(
            type(getattr(attachment, name)) is not int
            for name in ("window_start_epoch_ms", "window_end_epoch_ms")
        ):
            raise ValueError("attachment contains a polymorphic integer")
        if any(
            type(getattr(attachment, name)) is not str
            for name in ("policy_fingerprint", "creation_fingerprint")
        ):
            raise ValueError("attachment contains a polymorphic fingerprint")
    try:
        ContinuousPublicTradeStreamCheckpoint.model_validate(checkpoint.model_dump(round_trip=True))
    except ValidationError as error:
        raise ValueError("checkpoint failed TASK-059 validation") from error


def _validated_task059_policy(value: object) -> ContinuousPublicTradePolicy:
    """Rebuild one exact TASK-059 policy so bypass constructors cannot reach public helpers."""

    if type(value) is not ContinuousPublicTradePolicy:
        raise ValueError("policy must be an exact TASK-059 policy")
    try:
        _require_exact_declared_model_fields(
            value,
            ContinuousPublicTradePolicy,
            "policy",
        )
        if _require_exact_string(value.schema_version, "schema_version") != MODEL_VERSION:
            raise ValueError("policy has an unsupported model version")
        return ContinuousPublicTradePolicy(
            schema_version=MODEL_VERSION,
            window_size_ms=_require_exact_int(
                value.window_size_ms,
                "window_size_ms",
            ),
            settlement_lag_ms=_require_exact_int(
                value.settlement_lag_ms,
                "settlement_lag_ms",
            ),
            max_catchup_span_ms=_require_exact_int(
                value.max_catchup_span_ms,
                "max_catchup_span_ms",
            ),
            max_jobs_per_invocation=_require_exact_int(
                value.max_jobs_per_invocation,
                "max_jobs_per_invocation",
            ),
            max_requests_per_job=_require_exact_int(
                value.max_requests_per_job,
                "max_requests_per_job",
            ),
            max_records_per_job=_require_exact_int(
                value.max_records_per_job,
                "max_records_per_job",
            ),
            policy_fingerprint=_require_fingerprint(
                value.policy_fingerprint,
                "policy_fingerprint",
            ),
        )
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ValueError("policy failed exact TASK-059 validation") from error


def _validate_checkpoint_policy_bindings(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
    policy: ContinuousPublicTradePolicy,
) -> ContinuousPublicTradePolicy:
    """Reproduce TASK-059's policy/grid checks without demanding a due-work candidate."""

    _validate_checkpoint_scalar_types(checkpoint)
    validated_policy = _validated_task059_policy(policy)
    if checkpoint.policy_fingerprint != validated_policy.policy_fingerprint:
        raise ValueError("checkpoint and policy fingerprints must match")
    if checkpoint.stream_start_epoch_ms % validated_policy.window_size_ms:
        raise ValueError("stream start must align to the policy's epoch grid")
    if checkpoint.cursor_epoch_ms % validated_policy.window_size_ms:
        raise ValueError("stream cursor must align to the policy's epoch grid")
    attachment = checkpoint.attachment
    if attachment is None:
        return validated_policy
    if (
        attachment.window_start_epoch_ms % validated_policy.window_size_ms
        or attachment.window_end_epoch_ms % validated_policy.window_size_ms
    ):
        raise ValueError("attached child range must align to the policy's epoch grid")
    if (
        attachment.window_end_epoch_ms - attachment.window_start_epoch_ms
        > validated_policy.max_catchup_span_ms
    ):
        raise ValueError("attached child range exceeds the finite catch-up span")
    return validated_policy


def _validate_child_checkpoint_scalar_types(
    checkpoint: PublicTradeCollectionCheckpoint,
) -> None:
    if type(checkpoint) is not PublicTradeCollectionCheckpoint:
        raise ValueError("child_checkpoint must be an exact bounded-child checkpoint")
    _require_exact_declared_model_fields(
        checkpoint,
        PublicTradeCollectionCheckpoint,
        "child_checkpoint",
    )
    exact_strings = (
        "schema_version",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "policy_fingerprint",
    )
    if any(type(getattr(checkpoint, name)) is not str for name in exact_strings):
        raise ValueError("child checkpoint contains a polymorphic string")
    for name in ("lease_owner", "last_failure_code", "last_stop_reason"):
        value = getattr(checkpoint, name)
        if value is not None and type(value) is not str:
            raise ValueError("child checkpoint contains a polymorphic optional string")
    if type(checkpoint.job_id) is not UUID:
        raise ValueError("child checkpoint job_id must be an exact UUID")
    if checkpoint.lease_token is not None and type(checkpoint.lease_token) is not UUID:
        raise ValueError("child checkpoint lease_token must be an exact UUID")
    if type(checkpoint.instrument_type) is not InstrumentType:
        raise ValueError("child checkpoint instrument_type must be exact")
    if type(checkpoint.status) is not CollectionJobStatus:
        raise ValueError("child checkpoint status must be exact")
    datetime_fields = (
        "window_start",
        "window_end_exclusive",
        "next_window_start",
        "created_at",
        "updated_at",
    )
    for name in datetime_fields:
        _require_exact_utc(getattr(checkpoint, name), name)
    for name in ("pending_window_end_exclusive", "lease_expires_at"):
        value = getattr(checkpoint, name)
        if value is not None:
            _require_exact_utc(value, name)
    integer_fields = (
        "version",
        "windows_completed",
        "records_completed",
        "source_requests",
        "window_traces",
        "retry_attempts",
        "splits_completed",
    )
    if any(type(getattr(checkpoint, name)) is not int for name in integer_fields):
        raise ValueError("child checkpoint contains a polymorphic integer")
    try:
        PublicTradeCollectionCheckpoint.model_validate(checkpoint.model_dump(round_trip=True))
    except ValidationError as error:
        raise ValueError("child checkpoint failed bounded-child validation") from error


def _validate_pristine_child_checkpoint(
    checkpoint: PublicTradeCollectionCheckpoint,
) -> None:
    _validate_child_checkpoint_scalar_types(checkpoint)
    if (
        checkpoint.schema_version != MODEL_VERSION
        or checkpoint.status is not CollectionJobStatus.PENDING
        or checkpoint.next_window_start != checkpoint.window_start
        or checkpoint.pending_window_end_exclusive is not None
        or checkpoint.created_at != checkpoint.updated_at
        or checkpoint.version != 1
        or checkpoint.lease_owner is not None
        or checkpoint.lease_token is not None
        or checkpoint.lease_expires_at is not None
        or checkpoint.windows_completed != 0
        or checkpoint.records_completed != 0
        or checkpoint.source_requests != 0
        or checkpoint.window_traces != 0
        or checkpoint.retry_attempts != 0
        or checkpoint.splits_completed != 0
        or checkpoint.last_failure_code is not None
        or checkpoint.last_stop_reason is not None
    ):
        raise ValueError("child checkpoint is not the complete pristine version-one value")
    _datetime_to_epoch_milliseconds(checkpoint.window_start, "window_start")
    _datetime_to_epoch_milliseconds(
        checkpoint.window_end_exclusive,
        "window_end_exclusive",
    )
    _datetime_to_epoch_milliseconds(checkpoint.next_window_start, "next_window_start")


class ContinuousPublicTradeChildCreationPayloadV1(_StrictContractModel):
    """Complete reconstructable pristine bounded-child creation material."""

    record_type: Literal["wealth.continuous_public_trade.child_creation"] = (
        CHILD_CREATION_RECORD_TYPE
    )
    model_version: Literal["1.0"] = MODEL_VERSION
    serialization_version: Literal[1] = SERIALIZATION_VERSION
    stream_id: UUID
    request_variant: str = Field(min_length=1, max_length=128)
    stream_policy_fingerprint: str
    child_checkpoint: PublicTradeCollectionCheckpoint

    @field_validator("record_type", "model_version", "request_variant", mode="before")
    @classmethod
    def strings_are_exact(cls, value: object, info: object) -> str:
        field_name = str(getattr(info, "field_name", "payload string"))
        if field_name == "request_variant":
            return _require_exact_string(
                value,
                field_name,
                minimum=1,
                maximum=128,
                forbid_whitespace=True,
            )
        return _require_exact_string(value, field_name)

    @field_validator("serialization_version", mode="before")
    @classmethod
    def serialization_version_is_exact(cls, value: object) -> int:
        return _require_exact_int(value, "serialization_version")

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("stream_policy_fingerprint", mode="before")
    @classmethod
    def stream_fingerprint_is_exact(cls, value: object) -> str:
        return _require_fingerprint(value, "stream_policy_fingerprint")

    @field_validator("child_checkpoint", mode="before")
    @classmethod
    def child_checkpoint_is_exact(cls, value: object) -> PublicTradeCollectionCheckpoint:
        if type(value) is not PublicTradeCollectionCheckpoint:
            raise ValueError("child_checkpoint must be an exact bounded-child checkpoint")
        return value

    @model_validator(mode="after")
    def child_is_pristine(self) -> Self:
        _validate_pristine_child_checkpoint(self.child_checkpoint)
        return self


class ContinuousPublicTradeStreamEnvelopeV1(_StrictContractModel):
    """Exact version-one stream checkpoint and optional attached child material."""

    record_type: Literal["wealth.continuous_public_trade.stream_envelope"] = (
        STREAM_ENVELOPE_RECORD_TYPE
    )
    serialization_version: Literal[1] = SERIALIZATION_VERSION
    checkpoint: ContinuousPublicTradeStreamCheckpoint
    child_creation_payload: ContinuousPublicTradeChildCreationPayloadV1 | None = None

    @field_validator("record_type", mode="before")
    @classmethod
    def record_type_is_exact(cls, value: object) -> str:
        return _require_exact_string(value, "record_type")

    @field_validator("serialization_version", mode="before")
    @classmethod
    def serialization_version_is_exact(cls, value: object) -> int:
        return _require_exact_int(value, "serialization_version")

    @field_validator("checkpoint", mode="before")
    @classmethod
    def checkpoint_is_exact(cls, value: object) -> ContinuousPublicTradeStreamCheckpoint:
        if type(value) is not ContinuousPublicTradeStreamCheckpoint:
            raise ValueError("checkpoint must be an exact TASK-059 checkpoint")
        return value

    @field_validator("child_creation_payload", mode="before")
    @classmethod
    def child_payload_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeChildCreationPayloadV1 | None:
        if value is None:
            return None
        if type(value) is not ContinuousPublicTradeChildCreationPayloadV1:
            raise ValueError("child_creation_payload must be an exact payload")
        return value

    @model_validator(mode="after")
    def envelope_bindings_are_exact(self) -> Self:
        checkpoint = self.checkpoint
        _validate_checkpoint_scalar_types(checkpoint)
        payload = self.child_creation_payload
        attachment = checkpoint.attachment
        if (payload is None) != (attachment is None):
            raise ValueError("child creation payload must exist exactly when attachment exists")
        if payload is None or attachment is None:
            return self

        _validate_pristine_child_checkpoint(payload.child_checkpoint)
        child = payload.child_checkpoint
        if (
            payload.stream_id != checkpoint.stream_id
            or payload.request_variant != checkpoint.request_variant
            or payload.stream_policy_fingerprint != checkpoint.policy_fingerprint
            or child.job_id != attachment.job_id
            or child.source != checkpoint.source
            or child.venue != checkpoint.venue
            or child.instrument != checkpoint.instrument
            or child.provider_symbol != checkpoint.provider_symbol
            or child.instrument_type is not checkpoint.instrument_type
            or _datetime_to_epoch_milliseconds(child.window_start, "window_start")
            != attachment.window_start_epoch_ms
            or _datetime_to_epoch_milliseconds(
                child.window_end_exclusive,
                "window_end_exclusive",
            )
            != attachment.window_end_epoch_ms
        ):
            raise ValueError("child creation payload does not match the stream attachment")
        if child_creation_fingerprint(payload) != attachment.creation_fingerprint:
            raise ValueError("attachment creation fingerprint does not match its payload")
        return self


def _reference_is_valid_at(
    reference: ContinuousPublicTradeEvidenceReferenceV1,
    recorded_at: datetime,
) -> bool:
    return reference.valid_from <= recorded_at and (
        reference.expires_at is None or recorded_at < reference.expires_at
    )


class ContinuousPublicTradeStreamCreationRecordV1(_StrictContractModel):
    """Immutable version-one stream creation evidence record."""

    record_type: Literal["wealth.continuous_public_trade.stream_creation"] = (
        STREAM_CREATION_RECORD_TYPE
    )
    model_version: Literal["1.0"] = MODEL_VERSION
    serialization_version: Literal[1] = SERIALIZATION_VERSION
    stream_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    request_variant: str = Field(min_length=1, max_length=128)
    stream_start_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)
    stream_policy: ContinuousPublicTradePolicyProjectionV1
    prior_version: None = None
    prior_envelope_digest: None = None
    successor_version: Literal[1] = 1
    successor_envelope_hex: str
    successor_envelope_digest: str
    create_authority_reference: ContinuousPublicTradeEvidenceReferenceV1
    recorded_at: datetime

    @field_validator(
        "record_type",
        "model_version",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "request_variant",
        "successor_envelope_hex",
        mode="before",
    )
    @classmethod
    def strings_are_exact(cls, value: object, info: object) -> str:
        field_name = str(getattr(info, "field_name", "creation string"))
        if field_name in {
            "source",
            "venue",
            "instrument",
            "provider_symbol",
            "request_variant",
        }:
            limits = {
                "source": 128,
                "venue": 64,
                "instrument": 64,
                "provider_symbol": 64,
                "request_variant": 128,
            }
            return _require_exact_string(
                value,
                field_name,
                minimum=1,
                maximum=limits[field_name],
                forbid_whitespace=True,
            )
        return _require_exact_string(value, field_name)

    @field_validator(
        "serialization_version",
        "stream_start_epoch_ms",
        "successor_version",
        mode="before",
    )
    @classmethod
    def integers_are_exact(cls, value: object, info: object) -> int:
        return _require_exact_int(
            value,
            str(getattr(info, "field_name", "creation integer")),
        )

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("instrument_type", mode="before")
    @classmethod
    def instrument_type_is_exact(cls, value: object) -> InstrumentType:
        return _require_exact_enum(value, InstrumentType, "instrument_type")

    @field_validator("stream_policy", mode="before")
    @classmethod
    def stream_policy_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradePolicyProjectionV1:
        if type(value) is not ContinuousPublicTradePolicyProjectionV1:
            raise ValueError("stream_policy must be an exact policy projection")
        return value

    @field_validator("successor_envelope_digest", mode="before")
    @classmethod
    def successor_digest_is_exact(cls, value: object) -> str:
        return _require_fingerprint(value, "successor_envelope_digest")

    @field_validator("create_authority_reference", mode="before")
    @classmethod
    def reference_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeEvidenceReferenceV1:
        if type(value) is not ContinuousPublicTradeEvidenceReferenceV1:
            raise ValueError("create_authority_reference must be exact")
        return value

    @field_validator("recorded_at", mode="before")
    @classmethod
    def recorded_at_is_exact(cls, value: object) -> datetime:
        return _require_exact_utc(value, "recorded_at")

    @model_validator(mode="after")
    def creation_record_is_consistent(self) -> Self:
        reference = self.create_authority_reference
        if (
            reference.evidence_kind is not ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY
            or reference.outcome is not ContinuousPublicTradeEvidenceOutcome.APPROVED
            or not _reference_is_valid_at(reference, self.recorded_at)
        ):
            raise ValueError("create authority is inconsistent or invalid at recorded_at")
        successor = _decode_successor_envelope_hex(self.successor_envelope_hex)
        checkpoint = successor.checkpoint
        if (
            stream_envelope_digest(successor) != self.successor_envelope_digest
            or checkpoint.version != 1
            or checkpoint.status is not ContinuousPublicTradeStreamStatus.ACTIVE
            or checkpoint.cursor_epoch_ms != checkpoint.stream_start_epoch_ms
            or checkpoint.pause_reason is not None
            or checkpoint.attachment is not None
            or successor.child_creation_payload is not None
            or checkpoint.stream_id != self.stream_id
            or checkpoint.source != self.source
            or checkpoint.venue != self.venue
            or checkpoint.instrument != self.instrument
            or checkpoint.provider_symbol != self.provider_symbol
            or checkpoint.instrument_type is not self.instrument_type
            or checkpoint.request_variant != self.request_variant
            or checkpoint.stream_start_epoch_ms != self.stream_start_epoch_ms
            or checkpoint.policy_fingerprint != self.stream_policy.policy_fingerprint
        ):
            raise ValueError("stream creation record does not bind its pristine successor")
        policy = ContinuousPublicTradePolicy(**self.stream_policy.model_dump())
        try:
            _validate_checkpoint_policy_bindings(checkpoint, policy)
        except ValueError as error:
            raise ValueError("creation checkpoint does not satisfy its complete policy") from error
        return self


class ContinuousPublicTradeStreamTransitionRecordV1(_StrictContractModel):
    """Immutable version-one stream checkpoint transition evidence record."""

    record_type: Literal["wealth.continuous_public_trade.stream_transition"] = (
        STREAM_TRANSITION_RECORD_TYPE
    )
    model_version: Literal["1.0"] = MODEL_VERSION
    serialization_version: Literal[1] = SERIALIZATION_VERSION
    stream_id: UUID
    prior_version: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    successor_version: int = Field(ge=2, le=MAX_CONTRACT_INTEGER)
    transition_kind: ContinuousPublicTradeTransitionKind
    prior_history_root: str
    prior_envelope_digest: str
    successor_envelope_hex: str
    successor_envelope_digest: str
    reason_code: str | None = None
    transition_authority_reference: ContinuousPublicTradeEvidenceReferenceV1
    child_completion_reference: ContinuousPublicTradeEvidenceReferenceV1 | None = None
    recorded_at: datetime

    @field_validator(
        "record_type",
        "model_version",
        "successor_envelope_hex",
        mode="before",
    )
    @classmethod
    def strings_are_exact(cls, value: object, info: object) -> str:
        return _require_exact_string(
            value,
            str(getattr(info, "field_name", "transition string")),
        )

    @field_validator(
        "serialization_version",
        "prior_version",
        "successor_version",
        mode="before",
    )
    @classmethod
    def integers_are_exact(cls, value: object, info: object) -> int:
        return _require_exact_int(
            value,
            str(getattr(info, "field_name", "transition integer")),
        )

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("transition_kind", mode="before")
    @classmethod
    def transition_kind_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeTransitionKind:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeTransitionKind,
            "transition_kind",
        )

    @field_validator(
        "prior_history_root",
        "prior_envelope_digest",
        "successor_envelope_digest",
        mode="before",
    )
    @classmethod
    def digests_are_exact(cls, value: object, info: object) -> str:
        return _require_fingerprint(value, str(getattr(info, "field_name", "digest")))

    @field_validator("reason_code", mode="before")
    @classmethod
    def reason_is_exact(cls, value: object) -> str | None:
        if value is None:
            return None
        return _require_reason(value, "reason_code")

    @field_validator(
        "transition_authority_reference",
        "child_completion_reference",
        mode="before",
    )
    @classmethod
    def references_are_exact(
        cls,
        value: object,
        info: object,
    ) -> ContinuousPublicTradeEvidenceReferenceV1 | None:
        if value is None:
            return None
        if type(value) is not ContinuousPublicTradeEvidenceReferenceV1:
            raise ValueError(f"{getattr(info, 'field_name', 'reference')} must be exact")
        return value

    @field_validator("recorded_at", mode="before")
    @classmethod
    def recorded_at_is_exact(cls, value: object) -> datetime:
        return _require_exact_utc(value, "recorded_at")

    @model_validator(mode="after")
    def transition_record_is_consistent(self) -> Self:
        if self.successor_version != self.prior_version + 1:
            raise ValueError("transition versions must be contiguous")
        reason_required = self.transition_kind in {
            ContinuousPublicTradeTransitionKind.RETAIN,
            ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
        }
        if reason_required != (self.reason_code is not None):
            raise ValueError("transition reason does not match its kind")

        authority = self.transition_authority_reference
        if (
            authority.evidence_kind
            is not ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY
            or authority.outcome is not ContinuousPublicTradeEvidenceOutcome.APPROVED
            or not _reference_is_valid_at(authority, self.recorded_at)
        ):
            raise ValueError("transition authority is inconsistent or invalid at recorded_at")

        completion = self.child_completion_reference
        requires_completion = (
            self.transition_kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
        )
        if requires_completion != (completion is not None):
            raise ValueError("child completion reference does not match transition kind")
        if completion is not None and (
            completion.evidence_kind is not ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
            or completion.outcome is not ContinuousPublicTradeEvidenceOutcome.ACCEPTED
            or not _reference_is_valid_at(completion, self.recorded_at)
        ):
            raise ValueError("child completion evidence is inconsistent at recorded_at")

        successor = _decode_successor_envelope_hex(self.successor_envelope_hex)
        if (
            stream_envelope_digest(successor) != self.successor_envelope_digest
            or successor.checkpoint.stream_id != self.stream_id
            or successor.checkpoint.version != self.successor_version
        ):
            raise ValueError("transition record does not bind its exact successor")
        if (
            self.transition_kind is ContinuousPublicTradeTransitionKind.MANUAL_HOLD
            and successor.checkpoint.pause_reason != self.reason_code
        ):
            raise ValueError("manual-hold reason must equal the successor pause reason")
        if self.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH:
            payload = successor.child_creation_payload
            if payload is None or (
                payload.child_checkpoint.created_at != self.recorded_at
                or payload.child_checkpoint.updated_at != self.recorded_at
            ):
                raise ValueError("attach child timestamps must equal recorded_at")
        return self


def _validated_contract_value[ContractT: _StrictContractModel](
    value: object,
    model_type: type[ContractT],
) -> ContractT:
    """Revalidate exact contract instances so bypass constructors cannot reach public boundaries."""

    if type(value) is not model_type:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    try:
        _require_exact_declared_model_fields(value, model_type, "contract value")
        _require_no_private_model_storage(value, "contract value")
        validated = model_type.model_validate(value)
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error
    if type(validated) is not model_type:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    return validated


def _policy_projection(
    policy: ContinuousPublicTradePolicyProjectionV1,
) -> dict[str, object]:
    return {
        "max_catchup_span_ms": policy.max_catchup_span_ms,
        "max_jobs_per_invocation": policy.max_jobs_per_invocation,
        "max_records_per_job": policy.max_records_per_job,
        "max_requests_per_job": policy.max_requests_per_job,
        "policy_fingerprint": policy.policy_fingerprint,
        "schema_version": policy.schema_version,
        "settlement_lag_ms": policy.settlement_lag_ms,
        "window_size_ms": policy.window_size_ms,
    }


def _attachment_projection(
    attachment: ContinuousPublicTradeAttachment | None,
) -> dict[str, object] | None:
    if attachment is None:
        return None
    return {
        "creation_fingerprint": attachment.creation_fingerprint,
        "job_id": str(attachment.job_id),
        "policy_fingerprint": attachment.policy_fingerprint,
        "window_end_epoch_ms": attachment.window_end_epoch_ms,
        "window_start_epoch_ms": attachment.window_start_epoch_ms,
    }


def _stream_checkpoint_projection(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
) -> dict[str, object]:
    return {
        "attachment": _attachment_projection(checkpoint.attachment),
        "cursor_epoch_ms": checkpoint.cursor_epoch_ms,
        "instrument": checkpoint.instrument,
        "instrument_type": checkpoint.instrument_type.value,
        "pause_reason": checkpoint.pause_reason,
        "policy_fingerprint": checkpoint.policy_fingerprint,
        "provider_symbol": checkpoint.provider_symbol,
        "request_variant": checkpoint.request_variant,
        "schema_version": checkpoint.schema_version,
        "source": checkpoint.source,
        "status": checkpoint.status.value,
        "stream_id": str(checkpoint.stream_id),
        "stream_start_epoch_ms": checkpoint.stream_start_epoch_ms,
        "venue": checkpoint.venue,
        "version": checkpoint.version,
    }


def _child_checkpoint_projection(
    checkpoint: PublicTradeCollectionCheckpoint,
) -> dict[str, object]:
    return {
        "created_at": serialize_canonical_utc(checkpoint.created_at),
        "instrument": checkpoint.instrument,
        "instrument_type": checkpoint.instrument_type.value,
        "job_id": str(checkpoint.job_id),
        "last_failure_code": checkpoint.last_failure_code,
        "last_stop_reason": checkpoint.last_stop_reason,
        "lease_expires_at": (
            None
            if checkpoint.lease_expires_at is None
            else serialize_canonical_utc(checkpoint.lease_expires_at)
        ),
        "lease_owner": checkpoint.lease_owner,
        "lease_token": (None if checkpoint.lease_token is None else str(checkpoint.lease_token)),
        "next_window_start": serialize_canonical_utc(checkpoint.next_window_start),
        "pending_window_end_exclusive": (
            None
            if checkpoint.pending_window_end_exclusive is None
            else serialize_canonical_utc(checkpoint.pending_window_end_exclusive)
        ),
        "policy_fingerprint": checkpoint.policy_fingerprint,
        "provider_symbol": checkpoint.provider_symbol,
        "records_completed": checkpoint.records_completed,
        "retry_attempts": checkpoint.retry_attempts,
        "schema_version": checkpoint.schema_version,
        "source": checkpoint.source,
        "source_requests": checkpoint.source_requests,
        "splits_completed": checkpoint.splits_completed,
        "status": checkpoint.status.value,
        "updated_at": serialize_canonical_utc(checkpoint.updated_at),
        "venue": checkpoint.venue,
        "version": checkpoint.version,
        "window_end_exclusive": serialize_canonical_utc(checkpoint.window_end_exclusive),
        "window_start": serialize_canonical_utc(checkpoint.window_start),
        "window_traces": checkpoint.window_traces,
        "windows_completed": checkpoint.windows_completed,
    }


def _child_creation_projection(
    payload: ContinuousPublicTradeChildCreationPayloadV1,
) -> dict[str, object]:
    return {
        "child_checkpoint": _child_checkpoint_projection(payload.child_checkpoint),
        "model_version": payload.model_version,
        "record_type": payload.record_type,
        "request_variant": payload.request_variant,
        "serialization_version": payload.serialization_version,
        "stream_id": str(payload.stream_id),
        "stream_policy_fingerprint": payload.stream_policy_fingerprint,
    }


def _stream_envelope_projection(
    envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> dict[str, object]:
    return {
        "checkpoint": _stream_checkpoint_projection(envelope.checkpoint),
        "child_creation_payload": (
            None
            if envelope.child_creation_payload is None
            else _child_creation_projection(envelope.child_creation_payload)
        ),
        "record_type": envelope.record_type,
        "serialization_version": envelope.serialization_version,
    }


def _reference_projection(
    reference: ContinuousPublicTradeEvidenceReferenceV1,
) -> dict[str, object]:
    return {
        "evidence_digest": reference.evidence_digest,
        "evidence_id": reference.evidence_id,
        "evidence_kind": reference.evidence_kind.value,
        "expires_at": (
            None if reference.expires_at is None else serialize_canonical_utc(reference.expires_at)
        ),
        "outcome": reference.outcome.value,
        "reference_version": reference.reference_version,
        "scope_digest": reference.scope_digest,
        "valid_from": serialize_canonical_utc(reference.valid_from),
    }


def _scope_projection(
    scope: ContinuousPublicTradeEvidenceScopeV1,
) -> dict[str, object]:
    return {
        "child_creation_fingerprint": scope.child_creation_fingerprint,
        "child_job_id": None if scope.child_job_id is None else str(scope.child_job_id),
        "child_policy_fingerprint": scope.child_policy_fingerprint,
        "evidence_kind": scope.evidence_kind.value,
        "prior_envelope_digest": scope.prior_envelope_digest,
        "prior_history_root": scope.prior_history_root,
        "prior_version": scope.prior_version,
        "reason_code": scope.reason_code,
        "stream_id": str(scope.stream_id),
        "stream_policy": (
            None if scope.stream_policy is None else _policy_projection(scope.stream_policy)
        ),
        "successor_envelope_digest": scope.successor_envelope_digest,
        "successor_version": scope.successor_version,
        "transition_kind": (None if scope.transition_kind is None else scope.transition_kind.value),
    }


def _stream_creation_projection(
    record: ContinuousPublicTradeStreamCreationRecordV1,
) -> dict[str, object]:
    return {
        "create_authority_reference": _reference_projection(record.create_authority_reference),
        "instrument": record.instrument,
        "instrument_type": record.instrument_type.value,
        "model_version": record.model_version,
        "prior_envelope_digest": record.prior_envelope_digest,
        "prior_version": record.prior_version,
        "provider_symbol": record.provider_symbol,
        "record_type": record.record_type,
        "recorded_at": serialize_canonical_utc(record.recorded_at),
        "request_variant": record.request_variant,
        "serialization_version": record.serialization_version,
        "source": record.source,
        "stream_id": str(record.stream_id),
        "stream_policy": _policy_projection(record.stream_policy),
        "stream_start_epoch_ms": record.stream_start_epoch_ms,
        "successor_envelope_digest": record.successor_envelope_digest,
        "successor_envelope_hex": record.successor_envelope_hex,
        "successor_version": record.successor_version,
        "venue": record.venue,
    }


def _stream_transition_projection(
    record: ContinuousPublicTradeStreamTransitionRecordV1,
) -> dict[str, object]:
    return {
        "child_completion_reference": (
            None
            if record.child_completion_reference is None
            else _reference_projection(record.child_completion_reference)
        ),
        "model_version": record.model_version,
        "prior_envelope_digest": record.prior_envelope_digest,
        "prior_history_root": record.prior_history_root,
        "prior_version": record.prior_version,
        "reason_code": record.reason_code,
        "record_type": record.record_type,
        "recorded_at": serialize_canonical_utc(record.recorded_at),
        "serialization_version": record.serialization_version,
        "stream_id": str(record.stream_id),
        "successor_envelope_digest": record.successor_envelope_digest,
        "successor_envelope_hex": record.successor_envelope_hex,
        "successor_version": record.successor_version,
        "transition_authority_reference": _reference_projection(
            record.transition_authority_reference
        ),
        "transition_kind": record.transition_kind.value,
    }


def _canonical_json_bytes(projection: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            projection,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error


class _ObjectScanState:
    __slots__ = ("pending_key",)

    def __init__(self) -> None:
        self.pending_key: bytes | None = None


_HEX_DIGIT_BYTES: Final[frozenset[int]] = frozenset(b"0123456789abcdefABCDEF")
_JSON_WHITESPACE_BYTES: Final[frozenset[int]] = frozenset(b" \t\r\n")
_NUMBER_DELIMITER_BYTES: Final[frozenset[int]] = frozenset(b" \t\r\n,}]")


def _scan_unicode_escape(raw: bytes, escape_index: int, string_end: int) -> tuple[int, int]:
    if escape_index + 6 > string_end:
        _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
    digits = raw[escape_index + 2 : escape_index + 6]
    if len(digits) != 4 or any(byte not in _HEX_DIGIT_BYTES for byte in digits):
        _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
    codepoint = int(digits.decode("ascii"), 16)
    return codepoint, escape_index + 6


def _scan_json_string(raw: bytes, start: int) -> tuple[int, bool]:
    index = start + 1
    had_escape = False
    while index < len(raw):
        byte = raw[index]
        if byte == 0x22:
            return index + 1, had_escape
        if byte < 0x20:
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        if byte != 0x5C:
            index += 1
            continue
        had_escape = True
        if index + 1 >= len(raw):
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        escape = raw[index + 1]
        if escape in b'"\\/bfnrt':
            index += 2
            continue
        if escape != ord("u"):
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        codepoint, next_index = _scan_unicode_escape(raw, index, len(raw))
        if 0xD800 <= codepoint <= 0xDBFF:
            if next_index + 6 > len(raw) or raw[next_index : next_index + 2] != b"\\u":
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            low, paired_index = _scan_unicode_escape(raw, next_index, len(raw))
            if not 0xDC00 <= low <= 0xDFFF:
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            index = paired_index
            continue
        if 0xDC00 <= codepoint <= 0xDFFF:
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        index = next_index
    _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)


def _next_non_whitespace(raw: bytes, index: int) -> int:
    while index < len(raw) and raw[index] in _JSON_WHITESPACE_BYTES:
        index += 1
    return index


def _lexically_scan_json(
    raw: object,
    *,
    document_limit: int,
    allow_successor_envelope_hex: bool,
) -> bytes:
    if type(raw) is not bytes:
        _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
    document = raw
    if len(document) > MAX_RAW_RECORD_BYTES or len(document) > document_limit:
        _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)

    stack: list[_ObjectScanState] = []
    members = 0
    index = 0
    while index < len(document):
        byte = document[index]
        if byte == 0x7B:
            stack.append(_ObjectScanState())
            if len(stack) > MAX_JSON_DEPTH:
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            index += 1
            continue
        if byte == 0x7D:
            if not stack:
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            stack.pop()
            index += 1
            continue
        if byte in (0x5B, 0x5D):
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        if byte == 0x22:
            token_start = index + 1
            token_end, had_escape = _scan_json_string(document, index)
            lexical_length = token_end - token_start - 1
            next_index = _next_non_whitespace(document, token_end)
            is_key = next_index < len(document) and document[next_index] == 0x3A
            if is_key:
                if not stack or had_escape or lexical_length > MAX_JSON_KEY_BYTES:
                    _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
                key = document[token_start : token_end - 1]
                if any(byte < 0x20 or byte > 0x7E for byte in key):
                    _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
                members += 1
                if members > MAX_JSON_MEMBERS:
                    _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
                stack[-1].pending_key = key
            else:
                active_key = None if not stack else stack[-1].pending_key
                string_limit = (
                    MAX_SUCCESSOR_ENVELOPE_HEX_CHARS
                    if allow_successor_envelope_hex and active_key == b"successor_envelope_hex"
                    else MAX_JSON_STRING_LEXICAL_BYTES
                )
                if lexical_length > string_limit:
                    _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            index = token_end
            continue
        if byte == 0x2C:
            if stack:
                stack[-1].pending_key = None
            index += 1
            continue
        if byte == 0x2D or 0x30 <= byte <= 0x39:
            number_end = index + 1
            while (
                number_end < len(document) and document[number_end] not in _NUMBER_DELIMITER_BYTES
            ):
                number_end += 1
            token = document[index:number_end]
            if any(character in token for character in b".eE"):
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            unsigned = token[1:] if token.startswith(b"-") else token
            if (
                not unsigned
                or any(character < 0x30 or character > 0x39 for character in unsigned)
                or len(unsigned) > MAX_JSON_INTEGER_DIGITS
            ):
                _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
            index = number_end
            continue
        if byte >= 0x80 and not stack:
            _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
        index += 1

    if stack:
        _fail(ContinuousPublicTradePersistenceErrorCode.RAW_INPUT)
    return document


class _DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_pairs(pairs: Iterable[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError
        result[key] = value
    return result


def _reject_float_or_constant(_: str) -> Never:
    raise ValueError


def _load_json_document(
    raw: object,
    *,
    document_limit: int,
    allow_successor_envelope_hex: bool,
) -> tuple[bytes, object]:
    document = _lexically_scan_json(
        raw,
        document_limit=document_limit,
        allow_successor_envelope_hex=allow_successor_envelope_hex,
    )
    try:
        text = document.decode("utf-8", errors="strict")
        loaded = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_float=_reject_float_or_constant,
            parse_constant=_reject_float_or_constant,
        )
    except _DuplicateKeyError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.DUPLICATE_KEY
        ) from error
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.RAW_INPUT
        ) from error
    return document, loaded


def _expect_object(
    value: object,
    expected_keys: frozenset[str],
) -> dict[str, object]:
    if type(value) is not dict:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    result = cast(dict[str, object], value)
    if frozenset(result) != expected_keys:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return result


def _expect_string(value: object) -> str:
    if type(value) is not str:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return value


def _expect_optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _expect_string(value)


def _expect_integer(value: object) -> int:
    if type(value) is not int:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return value


def _expect_optional_integer(value: object) -> int | None:
    if value is None:
        return None
    return _expect_integer(value)


def _expect_literal(value: object, expected: str | int) -> None:
    if type(value) is not type(expected):
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    if value != expected:
        _fail(ContinuousPublicTradePersistenceErrorCode.UNSUPPORTED_VERSION)


def _expect_null(value: object) -> None:
    if value is not None:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)


def _parse_uuid(value: object) -> UUID:
    text = _expect_string(value)
    if _CANONICAL_UUID_PATTERN.fullmatch(text) is None:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    try:
        parsed = UUID(text)
    except ValueError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
        ) from error
    if type(parsed) is not UUID or str(parsed) != text:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return parsed


def _parse_optional_uuid(value: object) -> UUID | None:
    if value is None:
        return None
    return _parse_uuid(value)


def _parse_enum[E: StrEnum](value: object, enum_type: type[E]) -> E:
    text = _expect_string(value)
    try:
        result = enum_type(text)
    except ValueError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
        ) from error
    if type(result) is not enum_type:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return result


def _parse_optional_transition_kind(
    value: object,
) -> ContinuousPublicTradeTransitionKind | None:
    if value is None:
        return None
    return _parse_enum(value, ContinuousPublicTradeTransitionKind)


def _parse_datetime(value: object) -> datetime:
    text = _expect_string(value)
    try:
        result = parse_canonical_utc(text)
    except CanonicalUtcError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
        ) from error
    if type(result) is not datetime or serialize_canonical_utc(result) != text:
        _fail(ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD)
    return result


def _parse_optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value)


_POLICY_KEYS: Final[frozenset[str]] = frozenset(
    {
        "schema_version",
        "window_size_ms",
        "settlement_lag_ms",
        "max_catchup_span_ms",
        "max_jobs_per_invocation",
        "max_requests_per_job",
        "max_records_per_job",
        "policy_fingerprint",
    }
)
_ATTACHMENT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "job_id",
        "window_start_epoch_ms",
        "window_end_epoch_ms",
        "policy_fingerprint",
        "creation_fingerprint",
    }
)
_STREAM_CHECKPOINT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "schema_version",
        "stream_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "request_variant",
        "policy_fingerprint",
        "stream_start_epoch_ms",
        "cursor_epoch_ms",
        "status",
        "pause_reason",
        "attachment",
        "version",
    }
)
_CHILD_CHECKPOINT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "schema_version",
        "job_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "policy_fingerprint",
        "window_start",
        "window_end_exclusive",
        "next_window_start",
        "pending_window_end_exclusive",
        "status",
        "created_at",
        "updated_at",
        "version",
        "lease_owner",
        "lease_token",
        "lease_expires_at",
        "windows_completed",
        "records_completed",
        "source_requests",
        "window_traces",
        "retry_attempts",
        "splits_completed",
        "last_failure_code",
        "last_stop_reason",
    }
)
_CHILD_CREATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "record_type",
        "model_version",
        "serialization_version",
        "stream_id",
        "request_variant",
        "stream_policy_fingerprint",
        "child_checkpoint",
    }
)
_STREAM_ENVELOPE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "record_type",
        "serialization_version",
        "checkpoint",
        "child_creation_payload",
    }
)
_REFERENCE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "reference_version",
        "evidence_kind",
        "evidence_id",
        "evidence_digest",
        "scope_digest",
        "outcome",
        "valid_from",
        "expires_at",
    }
)
_SCOPE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "evidence_kind",
        "stream_id",
        "transition_kind",
        "prior_version",
        "prior_envelope_digest",
        "prior_history_root",
        "successor_version",
        "successor_envelope_digest",
        "child_job_id",
        "child_policy_fingerprint",
        "child_creation_fingerprint",
        "reason_code",
        "stream_policy",
    }
)
_STREAM_CREATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "record_type",
        "model_version",
        "serialization_version",
        "stream_id",
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "instrument_type",
        "request_variant",
        "stream_start_epoch_ms",
        "stream_policy",
        "prior_version",
        "prior_envelope_digest",
        "successor_version",
        "successor_envelope_hex",
        "successor_envelope_digest",
        "create_authority_reference",
        "recorded_at",
    }
)
_STREAM_TRANSITION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "record_type",
        "model_version",
        "serialization_version",
        "stream_id",
        "prior_version",
        "successor_version",
        "transition_kind",
        "prior_history_root",
        "prior_envelope_digest",
        "successor_envelope_hex",
        "successor_envelope_digest",
        "reason_code",
        "transition_authority_reference",
        "child_completion_reference",
        "recorded_at",
    }
)


def _parse_policy_projection(
    value: object,
) -> ContinuousPublicTradePolicyProjectionV1:
    item = _expect_object(value, _POLICY_KEYS)
    _expect_literal(item["schema_version"], MODEL_VERSION)
    return ContinuousPublicTradePolicyProjectionV1(
        schema_version=MODEL_VERSION,
        window_size_ms=_expect_integer(item["window_size_ms"]),
        settlement_lag_ms=_expect_integer(item["settlement_lag_ms"]),
        max_catchup_span_ms=_expect_integer(item["max_catchup_span_ms"]),
        max_jobs_per_invocation=_expect_integer(item["max_jobs_per_invocation"]),
        max_requests_per_job=_expect_integer(item["max_requests_per_job"]),
        max_records_per_job=_expect_integer(item["max_records_per_job"]),
        policy_fingerprint=_expect_string(item["policy_fingerprint"]),
    )


def _parse_attachment(value: object) -> ContinuousPublicTradeAttachment | None:
    if value is None:
        return None
    item = _expect_object(value, _ATTACHMENT_KEYS)
    return ContinuousPublicTradeAttachment(
        job_id=_parse_uuid(item["job_id"]),
        window_start_epoch_ms=_expect_integer(item["window_start_epoch_ms"]),
        window_end_epoch_ms=_expect_integer(item["window_end_epoch_ms"]),
        policy_fingerprint=_expect_string(item["policy_fingerprint"]),
        creation_fingerprint=_expect_string(item["creation_fingerprint"]),
    )


def _parse_stream_checkpoint(value: object) -> ContinuousPublicTradeStreamCheckpoint:
    item = _expect_object(value, _STREAM_CHECKPOINT_KEYS)
    _expect_literal(item["schema_version"], MODEL_VERSION)
    return ContinuousPublicTradeStreamCheckpoint(
        schema_version=MODEL_VERSION,
        stream_id=_parse_uuid(item["stream_id"]),
        source=_expect_string(item["source"]),
        venue=_expect_string(item["venue"]),
        instrument=_expect_string(item["instrument"]),
        provider_symbol=_expect_string(item["provider_symbol"]),
        instrument_type=_parse_enum(item["instrument_type"], InstrumentType),
        request_variant=_expect_string(item["request_variant"]),
        policy_fingerprint=_expect_string(item["policy_fingerprint"]),
        stream_start_epoch_ms=_expect_integer(item["stream_start_epoch_ms"]),
        cursor_epoch_ms=_expect_integer(item["cursor_epoch_ms"]),
        status=_parse_enum(item["status"], ContinuousPublicTradeStreamStatus),
        pause_reason=_expect_optional_string(item["pause_reason"]),
        attachment=_parse_attachment(item["attachment"]),
        version=_expect_integer(item["version"]),
    )


def _parse_child_checkpoint(value: object) -> PublicTradeCollectionCheckpoint:
    item = _expect_object(value, _CHILD_CHECKPOINT_KEYS)
    _expect_literal(item["schema_version"], MODEL_VERSION)
    return PublicTradeCollectionCheckpoint(
        schema_version=MODEL_VERSION,
        job_id=_parse_uuid(item["job_id"]),
        source=_expect_string(item["source"]),
        venue=_expect_string(item["venue"]),
        instrument=_expect_string(item["instrument"]),
        provider_symbol=_expect_string(item["provider_symbol"]),
        instrument_type=_parse_enum(item["instrument_type"], InstrumentType),
        policy_fingerprint=_expect_string(item["policy_fingerprint"]),
        window_start=_parse_datetime(item["window_start"]),
        window_end_exclusive=_parse_datetime(item["window_end_exclusive"]),
        next_window_start=_parse_datetime(item["next_window_start"]),
        pending_window_end_exclusive=_parse_optional_datetime(item["pending_window_end_exclusive"]),
        status=_parse_enum(item["status"], CollectionJobStatus),
        created_at=_parse_datetime(item["created_at"]),
        updated_at=_parse_datetime(item["updated_at"]),
        version=_expect_integer(item["version"]),
        lease_owner=_expect_optional_string(item["lease_owner"]),
        lease_token=_parse_optional_uuid(item["lease_token"]),
        lease_expires_at=_parse_optional_datetime(item["lease_expires_at"]),
        windows_completed=_expect_integer(item["windows_completed"]),
        records_completed=_expect_integer(item["records_completed"]),
        source_requests=_expect_integer(item["source_requests"]),
        window_traces=_expect_integer(item["window_traces"]),
        retry_attempts=_expect_integer(item["retry_attempts"]),
        splits_completed=_expect_integer(item["splits_completed"]),
        last_failure_code=_expect_optional_string(item["last_failure_code"]),
        last_stop_reason=_expect_optional_string(item["last_stop_reason"]),
    )


def _parse_child_creation_payload(
    value: object,
) -> ContinuousPublicTradeChildCreationPayloadV1:
    item = _expect_object(value, _CHILD_CREATION_KEYS)
    _expect_literal(item["record_type"], CHILD_CREATION_RECORD_TYPE)
    _expect_literal(item["model_version"], MODEL_VERSION)
    _expect_literal(item["serialization_version"], SERIALIZATION_VERSION)
    return ContinuousPublicTradeChildCreationPayloadV1(
        record_type=CHILD_CREATION_RECORD_TYPE,
        model_version=MODEL_VERSION,
        serialization_version=SERIALIZATION_VERSION,
        stream_id=_parse_uuid(item["stream_id"]),
        request_variant=_expect_string(item["request_variant"]),
        stream_policy_fingerprint=_expect_string(item["stream_policy_fingerprint"]),
        child_checkpoint=_parse_child_checkpoint(item["child_checkpoint"]),
    )


def _parse_stream_envelope(value: object) -> ContinuousPublicTradeStreamEnvelopeV1:
    item = _expect_object(value, _STREAM_ENVELOPE_KEYS)
    _expect_literal(item["record_type"], STREAM_ENVELOPE_RECORD_TYPE)
    _expect_literal(item["serialization_version"], SERIALIZATION_VERSION)
    payload_value = item["child_creation_payload"]
    return ContinuousPublicTradeStreamEnvelopeV1(
        record_type=STREAM_ENVELOPE_RECORD_TYPE,
        serialization_version=SERIALIZATION_VERSION,
        checkpoint=_parse_stream_checkpoint(item["checkpoint"]),
        child_creation_payload=(
            None if payload_value is None else _parse_child_creation_payload(payload_value)
        ),
    )


def _parse_reference(value: object) -> ContinuousPublicTradeEvidenceReferenceV1:
    item = _expect_object(value, _REFERENCE_KEYS)
    _expect_literal(item["reference_version"], 1)
    return ContinuousPublicTradeEvidenceReferenceV1(
        reference_version=1,
        evidence_kind=_parse_enum(
            item["evidence_kind"],
            ContinuousPublicTradeEvidenceKind,
        ),
        evidence_id=_expect_string(item["evidence_id"]),
        evidence_digest=_expect_string(item["evidence_digest"]),
        scope_digest=_expect_string(item["scope_digest"]),
        outcome=_parse_enum(
            item["outcome"],
            ContinuousPublicTradeEvidenceOutcome,
        ),
        valid_from=_parse_datetime(item["valid_from"]),
        expires_at=_parse_optional_datetime(item["expires_at"]),
    )


def _parse_scope(value: object) -> ContinuousPublicTradeEvidenceScopeV1:
    item = _expect_object(value, _SCOPE_KEYS)
    policy_value = item["stream_policy"]
    return ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=_parse_enum(
            item["evidence_kind"],
            ContinuousPublicTradeEvidenceKind,
        ),
        stream_id=_parse_uuid(item["stream_id"]),
        transition_kind=_parse_optional_transition_kind(item["transition_kind"]),
        prior_version=_expect_optional_integer(item["prior_version"]),
        prior_envelope_digest=_expect_optional_string(item["prior_envelope_digest"]),
        prior_history_root=_expect_optional_string(item["prior_history_root"]),
        successor_version=_expect_integer(item["successor_version"]),
        successor_envelope_digest=_expect_optional_string(item["successor_envelope_digest"]),
        child_job_id=_parse_optional_uuid(item["child_job_id"]),
        child_policy_fingerprint=_expect_optional_string(item["child_policy_fingerprint"]),
        child_creation_fingerprint=_expect_optional_string(item["child_creation_fingerprint"]),
        reason_code=_expect_optional_string(item["reason_code"]),
        stream_policy=(None if policy_value is None else _parse_policy_projection(policy_value)),
    )


def _parse_stream_creation_record(
    value: object,
) -> ContinuousPublicTradeStreamCreationRecordV1:
    item = _expect_object(value, _STREAM_CREATION_KEYS)
    _expect_literal(item["record_type"], STREAM_CREATION_RECORD_TYPE)
    _expect_literal(item["model_version"], MODEL_VERSION)
    _expect_literal(item["serialization_version"], SERIALIZATION_VERSION)
    _expect_null(item["prior_version"])
    _expect_null(item["prior_envelope_digest"])
    _expect_literal(item["successor_version"], 1)
    return ContinuousPublicTradeStreamCreationRecordV1(
        record_type=STREAM_CREATION_RECORD_TYPE,
        model_version=MODEL_VERSION,
        serialization_version=SERIALIZATION_VERSION,
        stream_id=_parse_uuid(item["stream_id"]),
        source=_expect_string(item["source"]),
        venue=_expect_string(item["venue"]),
        instrument=_expect_string(item["instrument"]),
        provider_symbol=_expect_string(item["provider_symbol"]),
        instrument_type=_parse_enum(item["instrument_type"], InstrumentType),
        request_variant=_expect_string(item["request_variant"]),
        stream_start_epoch_ms=_expect_integer(item["stream_start_epoch_ms"]),
        stream_policy=_parse_policy_projection(item["stream_policy"]),
        prior_version=None,
        prior_envelope_digest=None,
        successor_version=1,
        successor_envelope_hex=_expect_string(item["successor_envelope_hex"]),
        successor_envelope_digest=_expect_string(item["successor_envelope_digest"]),
        create_authority_reference=_parse_reference(item["create_authority_reference"]),
        recorded_at=_parse_datetime(item["recorded_at"]),
    )


def _parse_stream_transition_record(
    value: object,
) -> ContinuousPublicTradeStreamTransitionRecordV1:
    item = _expect_object(value, _STREAM_TRANSITION_KEYS)
    _expect_literal(item["record_type"], STREAM_TRANSITION_RECORD_TYPE)
    _expect_literal(item["model_version"], MODEL_VERSION)
    _expect_literal(item["serialization_version"], SERIALIZATION_VERSION)
    completion_value = item["child_completion_reference"]
    return ContinuousPublicTradeStreamTransitionRecordV1(
        record_type=STREAM_TRANSITION_RECORD_TYPE,
        model_version=MODEL_VERSION,
        serialization_version=SERIALIZATION_VERSION,
        stream_id=_parse_uuid(item["stream_id"]),
        prior_version=_expect_integer(item["prior_version"]),
        successor_version=_expect_integer(item["successor_version"]),
        transition_kind=_parse_enum(
            item["transition_kind"],
            ContinuousPublicTradeTransitionKind,
        ),
        prior_history_root=_expect_string(item["prior_history_root"]),
        prior_envelope_digest=_expect_string(item["prior_envelope_digest"]),
        successor_envelope_hex=_expect_string(item["successor_envelope_hex"]),
        successor_envelope_digest=_expect_string(item["successor_envelope_digest"]),
        reason_code=_expect_optional_string(item["reason_code"]),
        transition_authority_reference=_parse_reference(item["transition_authority_reference"]),
        child_completion_reference=(
            None if completion_value is None else _parse_reference(completion_value)
        ),
        recorded_at=_parse_datetime(item["recorded_at"]),
    )


def _encode_projection(
    projection: Mapping[str, object],
    *,
    document_limit: int,
    allow_successor_envelope_hex: bool,
) -> bytes:
    canonical = _canonical_json_bytes(projection)
    try:
        _lexically_scan_json(
            canonical,
            document_limit=document_limit,
            allow_successor_envelope_hex=allow_successor_envelope_hex,
        )
    except ContinuousPublicTradePersistenceContractError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error
    return canonical


def _decode_document[DecodedT](
    raw: object,
    *,
    document_limit: int,
    allow_successor_envelope_hex: bool,
    parser: Callable[[object], DecodedT],
    encoder: Callable[[DecodedT], bytes],
) -> DecodedT:
    document, loaded = _load_json_document(
        raw,
        document_limit=document_limit,
        allow_successor_envelope_hex=allow_successor_envelope_hex,
    )
    try:
        value = parser(loaded)
    except ContinuousPublicTradePersistenceContractError:
        raise
    except (ValidationError, ValueError, TypeError, OverflowError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
        ) from error
    canonical = encoder(value)
    if document != canonical:
        _fail(ContinuousPublicTradePersistenceErrorCode.NON_CANONICAL)
    return value


def _domain_digest(domain: bytes, canonical: bytes) -> str:
    return f"sha256:{hashlib.sha256(domain + canonical).hexdigest()}"


def _raw_digest(value: object, field_name: str) -> bytes:
    digest = _require_fingerprint(value, field_name)
    try:
        result = bytes.fromhex(digest.removeprefix("sha256:"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be one canonical SHA-256 value") from error
    if len(result) != hashlib.sha256().digest_size:
        raise ValueError(f"{field_name} must decode to exactly 32 bytes")
    return result


def project_continuous_public_trade_policy(
    policy: ContinuousPublicTradePolicy,
) -> ContinuousPublicTradePolicyProjectionV1:
    """Freeze every TASK-059 policy field without deriving its caller fingerprint."""

    try:
        validated = _validated_task059_policy(policy)
        return ContinuousPublicTradePolicyProjectionV1(
            schema_version=validated.schema_version,
            window_size_ms=validated.window_size_ms,
            settlement_lag_ms=validated.settlement_lag_ms,
            max_catchup_span_ms=validated.max_catchup_span_ms,
            max_jobs_per_invocation=validated.max_jobs_per_invocation,
            max_requests_per_job=validated.max_requests_per_job,
            max_records_per_job=validated.max_records_per_job,
            policy_fingerprint=validated.policy_fingerprint,
        )
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error


def encode_child_creation_payload(
    payload: ContinuousPublicTradeChildCreationPayloadV1,
) -> bytes:
    """Return exact version-one canonical bytes for reconstructable child creation."""

    payload = _validated_contract_value(
        payload,
        ContinuousPublicTradeChildCreationPayloadV1,
    )
    return _encode_projection(
        _child_creation_projection(payload),
        document_limit=MAX_CHILD_CREATION_BYTES,
        allow_successor_envelope_hex=False,
    )


def decode_child_creation_payload(
    raw: object,
) -> ContinuousPublicTradeChildCreationPayloadV1:
    """Decode only exact canonical version-one child-creation bytes."""

    return _decode_document(
        raw,
        document_limit=MAX_CHILD_CREATION_BYTES,
        allow_successor_envelope_hex=False,
        parser=_parse_child_creation_payload,
        encoder=encode_child_creation_payload,
    )


def encode_stream_envelope(envelope: ContinuousPublicTradeStreamEnvelopeV1) -> bytes:
    """Return exact version-one canonical stream-envelope bytes."""

    envelope = _validated_contract_value(
        envelope,
        ContinuousPublicTradeStreamEnvelopeV1,
    )
    return _encode_projection(
        _stream_envelope_projection(envelope),
        document_limit=MAX_ENVELOPE_BYTES,
        allow_successor_envelope_hex=False,
    )


def decode_stream_envelope(raw: object) -> ContinuousPublicTradeStreamEnvelopeV1:
    """Decode only exact canonical version-one stream-envelope bytes."""

    return _decode_document(
        raw,
        document_limit=MAX_ENVELOPE_BYTES,
        allow_successor_envelope_hex=False,
        parser=_parse_stream_envelope,
        encoder=encode_stream_envelope,
    )


def encode_evidence_scope(scope: ContinuousPublicTradeEvidenceScopeV1) -> bytes:
    """Return exact canonical bytes for one version-one evidence scope."""

    scope = _validated_contract_value(
        scope,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    return _encode_projection(
        _scope_projection(scope),
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=False,
    )


def decode_evidence_scope(raw: object) -> ContinuousPublicTradeEvidenceScopeV1:
    """Decode only exact canonical version-one evidence-scope bytes."""

    return _decode_document(
        raw,
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=False,
        parser=_parse_scope,
        encoder=encode_evidence_scope,
    )


def encode_stream_creation_record(
    record: ContinuousPublicTradeStreamCreationRecordV1,
) -> bytes:
    """Return exact canonical bytes for version-one stream creation."""

    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamCreationRecordV1,
    )
    return _encode_projection(
        _stream_creation_projection(record),
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=True,
    )


def decode_stream_creation_record(
    raw: object,
) -> ContinuousPublicTradeStreamCreationRecordV1:
    """Decode only exact canonical version-one stream-creation bytes."""

    return _decode_document(
        raw,
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=True,
        parser=_parse_stream_creation_record,
        encoder=encode_stream_creation_record,
    )


def encode_stream_transition_record(
    record: ContinuousPublicTradeStreamTransitionRecordV1,
) -> bytes:
    """Return exact canonical bytes for one version-one stream transition."""

    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamTransitionRecordV1,
    )
    return _encode_projection(
        _stream_transition_projection(record),
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=True,
    )


def decode_stream_transition_record(
    raw: object,
) -> ContinuousPublicTradeStreamTransitionRecordV1:
    """Decode only exact canonical version-one stream-transition bytes."""

    return _decode_document(
        raw,
        document_limit=MAX_RAW_RECORD_BYTES,
        allow_successor_envelope_hex=True,
        parser=_parse_stream_transition_record,
        encoder=encode_stream_transition_record,
    )


def child_creation_fingerprint(
    payload: ContinuousPublicTradeChildCreationPayloadV1,
) -> str:
    """Bind complete child-creation material to its distinct versioned domain."""

    return _domain_digest(_CHILD_CREATION_DOMAIN, encode_child_creation_payload(payload))


def stream_envelope_digest(envelope: ContinuousPublicTradeStreamEnvelopeV1) -> str:
    """Bind exact current stream bytes to the stream-record domain."""

    return _domain_digest(_STREAM_ENVELOPE_DOMAIN, encode_stream_envelope(envelope))


def stream_creation_digest(record: ContinuousPublicTradeStreamCreationRecordV1) -> str:
    """Bind exact creation evidence to the stream-creation domain."""

    return _domain_digest(_STREAM_CREATION_DOMAIN, encode_stream_creation_record(record))


def stream_transition_digest(
    record: ContinuousPublicTradeStreamTransitionRecordV1,
) -> str:
    """Bind exact transition evidence to the stream-transition domain."""

    return _domain_digest(
        _STREAM_TRANSITION_DOMAIN,
        encode_stream_transition_record(record),
    )


def evidence_scope_digest(scope: ContinuousPublicTradeEvidenceScopeV1) -> str:
    """Bind an exact authority/completion scope to its distinct versioned domain."""

    return _domain_digest(_EVIDENCE_SCOPE_DOMAIN, encode_evidence_scope(scope))


def initial_stream_history_root(
    record: ContinuousPublicTradeStreamCreationRecordV1,
) -> str:
    """Commit the version-one creation bytes as the first rolling history root."""

    return _domain_digest(
        _HISTORY_ROOT_INITIAL_DOMAIN,
        encode_stream_creation_record(record),
    )


def next_stream_history_root(
    prior_history_root: object,
    record: ContinuousPublicTradeStreamTransitionRecordV1,
) -> str:
    """Extend one exact prior root with one exact contiguous transition record."""

    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamTransitionRecordV1,
    )
    try:
        prior_raw = _raw_digest(prior_history_root, "prior_history_root")
    except ValueError as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error
    if record.prior_history_root != prior_history_root:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    return _domain_digest(
        _HISTORY_ROOT_NEXT_DOMAIN,
        prior_raw + encode_stream_transition_record(record),
    )


def _decode_successor_envelope_hex(
    value: object,
) -> ContinuousPublicTradeStreamEnvelopeV1:
    text = _require_exact_string(value, "successor_envelope_hex")
    if (
        len(text) > MAX_SUCCESSOR_ENVELOPE_HEX_CHARS
        or len(text) % 2
        or _LOWERCASE_HEX_PATTERN.fullmatch(text) is None
    ):
        raise ValueError("successor_envelope_hex is not canonical bounded lowercase hex")
    try:
        raw = bytes.fromhex(text)
    except ValueError as error:
        raise ValueError("successor_envelope_hex is not valid hexadecimal") from error
    if len(raw) > MAX_ENVELOPE_BYTES or raw.hex() != text:
        raise ValueError("successor_envelope_hex does not preserve exact bounded bytes")
    try:
        envelope = decode_stream_envelope(raw)
    except ContinuousPublicTradePersistenceContractError as error:
        raise ValueError("successor_envelope_hex does not contain a canonical envelope") from error
    if encode_stream_envelope(envelope).hex() != text:
        raise ValueError("successor_envelope_hex is not an exact canonical round trip")
    return envelope


def _plans_match_except_creation_fingerprint(
    provisional: ContinuousPublicTradePlan,
    finalized: ContinuousPublicTradePlan,
) -> bool:
    if (
        provisional.schema_version != finalized.schema_version
        or provisional.status is not finalized.status
        or provisional.stream_id != finalized.stream_id
        or provisional.policy_fingerprint != finalized.policy_fingerprint
        or provisional.cursor_epoch_ms != finalized.cursor_epoch_ms
        or provisional.latest_eligible_end_epoch_ms != finalized.latest_eligible_end_epoch_ms
    ):
        return False
    first = provisional.attachment
    second = finalized.attachment
    if first is None or second is None:
        return first is second
    return (
        first.job_id == second.job_id
        and first.window_start_epoch_ms == second.window_start_epoch_ms
        and first.window_end_epoch_ms == second.window_end_epoch_ms
        and first.policy_fingerprint == second.policy_fingerprint
    )


def finalize_continuous_public_trade_attachment(
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
    policy: ContinuousPublicTradePolicy,
    *,
    candidate_job_id: UUID,
    child_policy_fingerprint: str,
    now: datetime,
) -> tuple[
    ContinuousPublicTradePlan,
    ContinuousPublicTradeChildCreationPayloadV1 | None,
]:
    """Run ADR-0029's pure two-pass planner without persisting its provisional value."""

    try:
        policy = _validate_checkpoint_policy_bindings(checkpoint, policy)
        job_id = _require_exact_uuid(candidate_job_id, "candidate_job_id")
        child_fingerprint = _require_fingerprint(
            child_policy_fingerprint,
            "child_policy_fingerprint",
        )
        command_time = _require_exact_utc(now, "now")
        provisional = plan_continuous_public_trade_window(
            checkpoint,
            policy,
            command_time,
            candidate_job_id=job_id,
            candidate_creation_fingerprint=PROVISIONAL_CHILD_CREATION_FINGERPRINT,
        )
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error

    if (
        provisional.status is not ContinuousPublicTradePlanStatus.ATTACHED_JOB
        or checkpoint.attachment is not None
    ):
        return provisional, None
    attachment = provisional.attachment
    if (
        attachment is None
        or attachment.creation_fingerprint != PROVISIONAL_CHILD_CREATION_FINGERPRINT
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)

    try:
        window_start = _epoch_milliseconds_to_datetime(
            attachment.window_start_epoch_ms,
            "window_start_epoch_ms",
        )
        window_end = _epoch_milliseconds_to_datetime(
            attachment.window_end_epoch_ms,
            "window_end_epoch_ms",
        )
        child = PublicTradeCollectionCheckpoint(
            schema_version=MODEL_VERSION,
            job_id=job_id,
            source=checkpoint.source,
            venue=checkpoint.venue,
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            policy_fingerprint=child_fingerprint,
            window_start=window_start,
            window_end_exclusive=window_end,
            next_window_start=window_start,
            pending_window_end_exclusive=None,
            status=CollectionJobStatus.PENDING,
            created_at=command_time,
            updated_at=command_time,
            version=1,
            lease_owner=None,
            lease_token=None,
            lease_expires_at=None,
            windows_completed=0,
            records_completed=0,
            source_requests=0,
            window_traces=0,
            retry_attempts=0,
            splits_completed=0,
            last_failure_code=None,
            last_stop_reason=None,
        )
        payload = ContinuousPublicTradeChildCreationPayloadV1(
            stream_id=checkpoint.stream_id,
            request_variant=checkpoint.request_variant,
            stream_policy_fingerprint=checkpoint.policy_fingerprint,
            child_checkpoint=child,
        )
        real_fingerprint = child_creation_fingerprint(payload)
        finalized = plan_continuous_public_trade_window(
            checkpoint,
            policy,
            command_time,
            candidate_job_id=job_id,
            candidate_creation_fingerprint=real_fingerprint,
        )
    except (
        ContinuousPublicTradePersistenceContractError,
        ValidationError,
        ValueError,
        TypeError,
        OverflowError,
    ) as error:
        if isinstance(error, ContinuousPublicTradePersistenceContractError):
            raise
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error

    if (
        not _plans_match_except_creation_fingerprint(provisional, finalized)
        or finalized.attachment is None
        or finalized.attachment.creation_fingerprint != real_fingerprint
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    return finalized, payload


def validate_stream_creation_record_scope(
    record: ContinuousPublicTradeStreamCreationRecordV1,
    scope: ContinuousPublicTradeEvidenceScopeV1,
) -> None:
    """Require one exact governed-create scope and matching retained scope digest."""

    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamCreationRecordV1,
    )
    scope = _validated_contract_value(
        scope,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    expected = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=record.stream_id,
        transition_kind=None,
        prior_version=None,
        prior_envelope_digest=None,
        prior_history_root=None,
        successor_version=1,
        successor_envelope_digest=record.successor_envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=record.stream_policy,
    )
    if scope != expected or record.create_authority_reference.scope_digest != evidence_scope_digest(
        scope
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)


def validate_stream_load_bindings(
    creation_record: ContinuousPublicTradeStreamCreationRecordV1,
    current_envelope: ContinuousPublicTradeStreamEnvelopeV1,
    *,
    effective_stream_policy: ContinuousPublicTradePolicy,
    effective_child_policy_fingerprint: str | None,
) -> None:
    """Validate exact immutable identity and caller policy bindings for one pure load result."""

    creation_record = _validated_contract_value(
        creation_record,
        ContinuousPublicTradeStreamCreationRecordV1,
    )
    current_envelope = _validated_contract_value(
        current_envelope,
        ContinuousPublicTradeStreamEnvelopeV1,
    )
    try:
        stream_policy = _validated_task059_policy(effective_stream_policy)
        checkpoint = current_envelope.checkpoint
        _validate_checkpoint_policy_bindings(checkpoint, stream_policy)
        if project_continuous_public_trade_policy(stream_policy) != creation_record.stream_policy:
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
        if (
            checkpoint.stream_id != creation_record.stream_id
            or checkpoint.source != creation_record.source
            or checkpoint.venue != creation_record.venue
            or checkpoint.instrument != creation_record.instrument
            or checkpoint.provider_symbol != creation_record.provider_symbol
            or checkpoint.instrument_type is not creation_record.instrument_type
            or checkpoint.request_variant != creation_record.request_variant
            or checkpoint.stream_start_epoch_ms != creation_record.stream_start_epoch_ms
        ):
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
        if checkpoint.version == 1 and (
            encode_stream_envelope(current_envelope).hex() != creation_record.successor_envelope_hex
            or stream_envelope_digest(current_envelope) != creation_record.successor_envelope_digest
        ):
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
        payload = current_envelope.child_creation_payload
        if payload is None:
            if effective_child_policy_fingerprint is not None:
                _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
            return
        child_fingerprint = _require_fingerprint(
            effective_child_policy_fingerprint,
            "effective_child_policy_fingerprint",
        )
        if payload.child_checkpoint.policy_fingerprint != child_fingerprint:
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    except ContinuousPublicTradePersistenceContractError:
        raise
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error


def _expected_transition_authority_scope(
    record: ContinuousPublicTradeStreamTransitionRecordV1,
    successor_envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> ContinuousPublicTradeEvidenceScopeV1:
    child_job_id: UUID | None = None
    child_policy_fingerprint: str | None = None
    successor_digest: str | None = record.successor_envelope_digest
    if record.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH:
        attachment = successor_envelope.checkpoint.attachment
        payload = successor_envelope.child_creation_payload
        if attachment is None or payload is None:
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
        child_job_id = attachment.job_id
        child_policy_fingerprint = payload.child_checkpoint.policy_fingerprint
        successor_digest = None
    return ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=record.stream_id,
        transition_kind=record.transition_kind,
        prior_version=record.prior_version,
        prior_envelope_digest=record.prior_envelope_digest,
        prior_history_root=record.prior_history_root,
        successor_version=record.successor_version,
        successor_envelope_digest=successor_digest,
        child_job_id=child_job_id,
        child_policy_fingerprint=child_policy_fingerprint,
        child_creation_fingerprint=None,
        reason_code=record.reason_code,
        stream_policy=None,
    )


def _expected_child_completion_scope(
    record: ContinuousPublicTradeStreamTransitionRecordV1,
    prior_envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> ContinuousPublicTradeEvidenceScopeV1:
    attachment = prior_envelope.checkpoint.attachment
    payload = prior_envelope.child_creation_payload
    if attachment is None or payload is None:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    return ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
        stream_id=record.stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
        prior_version=record.prior_version,
        prior_envelope_digest=record.prior_envelope_digest,
        prior_history_root=record.prior_history_root,
        successor_version=record.successor_version,
        successor_envelope_digest=record.successor_envelope_digest,
        child_job_id=attachment.job_id,
        child_policy_fingerprint=payload.child_checkpoint.policy_fingerprint,
        child_creation_fingerprint=attachment.creation_fingerprint,
        reason_code=None,
        stream_policy=None,
    )


def validate_stream_transition_record_scopes(
    prior_envelope: ContinuousPublicTradeStreamEnvelopeV1,
    record: ContinuousPublicTradeStreamTransitionRecordV1,
    transition_authority_scope: ContinuousPublicTradeEvidenceScopeV1,
    child_completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None,
) -> None:
    """Bind retained transition references to their exact kind-specific scopes."""

    prior_envelope = _validated_contract_value(
        prior_envelope,
        ContinuousPublicTradeStreamEnvelopeV1,
    )
    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamTransitionRecordV1,
    )
    transition_authority_scope = _validated_contract_value(
        transition_authority_scope,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    if child_completion_scope is not None:
        child_completion_scope = _validated_contract_value(
            child_completion_scope,
            ContinuousPublicTradeEvidenceScopeV1,
        )
    prior_checkpoint = prior_envelope.checkpoint
    if (
        prior_checkpoint.stream_id != record.stream_id
        or prior_checkpoint.version != record.prior_version
        or stream_envelope_digest(prior_envelope) != record.prior_envelope_digest
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    successor = _decode_successor_envelope_hex(record.successor_envelope_hex)
    expected_authority = _expected_transition_authority_scope(
        record,
        successor,
    )
    if (
        transition_authority_scope != expected_authority
        or record.transition_authority_reference.scope_digest
        != evidence_scope_digest(transition_authority_scope)
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)

    completion_reference = record.child_completion_reference
    if completion_reference is None:
        if child_completion_scope is not None:
            _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
        return
    if child_completion_scope is None:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)
    expected_completion = _expected_child_completion_scope(record, prior_envelope)
    if (
        child_completion_scope != expected_completion
        or completion_reference.scope_digest != evidence_scope_digest(child_completion_scope)
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)


def validate_stream_transition_link(
    prior_envelope: ContinuousPublicTradeStreamEnvelopeV1,
    record: ContinuousPublicTradeStreamTransitionRecordV1,
    *,
    policy: ContinuousPublicTradePolicy,
    prior_history_root: str,
    prior_recorded_at: datetime,
    transition_authority_scope: ContinuousPublicTradeEvidenceScopeV1,
    child_completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None,
) -> None:
    """Validate one exact causal TASK-059 transition without performing any action."""

    prior_envelope = _validated_contract_value(
        prior_envelope,
        ContinuousPublicTradeStreamEnvelopeV1,
    )
    record = _validated_contract_value(
        record,
        ContinuousPublicTradeStreamTransitionRecordV1,
    )
    try:
        policy = _validated_task059_policy(policy)
        expected_prior_root = _require_fingerprint(
            prior_history_root,
            "prior_history_root",
        )
        prior_time = _require_exact_utc(prior_recorded_at, "prior_recorded_at")
    except (ValidationError, ValueError, TypeError, OverflowError, AttributeError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error
    prior_checkpoint = prior_envelope.checkpoint
    successor = _decode_successor_envelope_hex(record.successor_envelope_hex)
    successor_checkpoint = successor.checkpoint
    if (
        record.stream_id != prior_checkpoint.stream_id
        or record.prior_version != prior_checkpoint.version
        or record.prior_envelope_digest != stream_envelope_digest(prior_envelope)
        or record.prior_history_root != expected_prior_root
        or record.successor_version != successor_checkpoint.version
        or record.successor_envelope_digest != stream_envelope_digest(successor)
        or record.recorded_at < prior_time
    ):
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)

    previous_payload = prior_envelope.child_creation_payload
    current_payload = successor.child_creation_payload
    if record.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH:
        payload_transition_is_valid = previous_payload is None and current_payload is not None
    elif record.transition_kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        payload_transition_is_valid = previous_payload is not None and current_payload is None
    else:
        payload_transition_is_valid = previous_payload == current_payload
    if not payload_transition_is_valid:
        _fail(ContinuousPublicTradePersistenceErrorCode.INCONSISTENT)

    completed_job_id = (
        None
        if record.transition_kind is not ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
        or prior_checkpoint.attachment is None
        else prior_checkpoint.attachment.job_id
    )
    try:
        validate_continuous_public_trade_stream_transition(
            prior_checkpoint,
            successor_checkpoint,
            record.transition_kind,
            policy=policy,
            completed_job_id=completed_job_id,
        )
    except (ValidationError, ValueError, TypeError, OverflowError) as error:
        raise ContinuousPublicTradePersistenceContractError(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT
        ) from error
    validate_stream_transition_record_scopes(
        prior_envelope,
        record,
        transition_authority_scope,
        child_completion_scope,
    )
