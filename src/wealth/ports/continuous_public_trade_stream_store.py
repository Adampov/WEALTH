"""Pure logical store-port contracts for continuous public-trade streams.

The values in this module describe a future lower-level atomic persistence boundary.  They perform
no I/O, sample no clock, construct no successor, validate no external evidence body, and grant no
authority.  A finalized TASK-061 record contains the sole canonical successor accepted by a write
command.
"""

import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Never, Protocol, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeStreamCheckpoint,
    ContinuousPublicTradeTransitionKind,
)
from wealth.domain.continuous_public_trade_persistence import (
    MAX_ENVELOPE_BYTES,
    MAX_RAW_RECORD_BYTES,
    ContinuousPublicTradeEvidenceKind,
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradeStreamCreationRecordV1,
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeStreamTransitionRecordV1,
    decode_stream_creation_record,
    decode_stream_envelope,
    decode_stream_transition_record,
    encode_evidence_scope,
    encode_stream_creation_record,
    encode_stream_envelope,
    encode_stream_transition_record,
    evidence_scope_digest,
    initial_stream_history_root,
    next_stream_history_root,
    project_continuous_public_trade_policy,
    stream_creation_digest,
    stream_envelope_digest,
    stream_transition_digest,
    validate_stream_creation_record_scope,
    validate_stream_load_bindings,
    validate_stream_transition_link,
    validate_stream_transition_record_scopes,
)
from wealth.domain.market import InstrumentType

__all__ = [
    "ContinuousPublicTradeStreamAuditAtTailResultV1",
    "ContinuousPublicTradeStreamAuditContinuationQueryV1",
    "ContinuousPublicTradeStreamAuditContinuationV1",
    "ContinuousPublicTradeStreamAuditNotFoundResultV1",
    "ContinuousPublicTradeStreamAuditOutcome",
    "ContinuousPublicTradeStreamAuditPageResultV1",
    "ContinuousPublicTradeStreamAuditPageV1",
    "ContinuousPublicTradeStreamAuditQueryV1",
    "ContinuousPublicTradeStreamAuditRejectedResultV1",
    "ContinuousPublicTradeStreamAuditResultV1",
    "ContinuousPublicTradeStreamAuditStartQueryV1",
    "ContinuousPublicTradeStreamAuditUnavailableResultV1",
    "ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1",
    "ContinuousPublicTradeStreamCompareAndSwapCommandV1",
    "ContinuousPublicTradeStreamCompareAndSwapOutcome",
    "ContinuousPublicTradeStreamCompareAndSwapReceiptV1",
    "ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1",
    "ContinuousPublicTradeStreamCompareAndSwapResultV1",
    "ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1",
    "ContinuousPublicTradeStreamCreateAcceptedResultV1",
    "ContinuousPublicTradeStreamCreateCommandV1",
    "ContinuousPublicTradeStreamCreateOutcome",
    "ContinuousPublicTradeStreamCreateReceiptV1",
    "ContinuousPublicTradeStreamCreateRejectedResultV1",
    "ContinuousPublicTradeStreamCreateResultV1",
    "ContinuousPublicTradeStreamCreateUnavailableResultV1",
    "ContinuousPublicTradeStreamCurrentViewV1",
    "ContinuousPublicTradeStreamExpectationV1",
    "ContinuousPublicTradeStreamIdentityV1",
    "ContinuousPublicTradeStreamLoadFoundResultV1",
    "ContinuousPublicTradeStreamLoadNotFoundResultV1",
    "ContinuousPublicTradeStreamLoadOutcome",
    "ContinuousPublicTradeStreamLoadQueryV1",
    "ContinuousPublicTradeStreamLoadRejectedResultV1",
    "ContinuousPublicTradeStreamLoadResultV1",
    "ContinuousPublicTradeStreamLoadUnavailableResultV1",
    "ContinuousPublicTradeStreamStore",
    "ContinuousPublicTradeStreamStoreContractError",
    "ContinuousPublicTradeStreamStoreContractErrorCode",
    "ContinuousPublicTradeStreamStoreRetryDisposition",
    "ContinuousPublicTradeStreamStoredCreationV1",
    "ContinuousPublicTradeStreamStoredEnvelopeV1",
    "ContinuousPublicTradeStreamStoredHistoryEntryV1",
    "ContinuousPublicTradeStreamStoredTransitionV1",
    "validate_continuous_public_trade_stream_audit_page",
]

_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")


class ContinuousPublicTradeStreamStoreContractErrorCode(StrEnum):
    """Sanitized invalid-caller-value classes at a future port method boundary."""

    MALFORMED_VALUE = "malformed_value"
    INCONSISTENT_VALUE = "inconsistent_value"


class ContinuousPublicTradeStreamStoreContractError(ValueError):
    """Fail closed without copying rejected caller material into the public message."""

    def __init__(self, code: ContinuousPublicTradeStreamStoreContractErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


def _fail(code: ContinuousPublicTradeStreamStoreContractErrorCode) -> Never:
    raise ContinuousPublicTradeStreamStoreContractError(code)


def _require_exact_int(
    value: object,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an exact built-in integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} is below its lower bound")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field_name} exceeds its upper bound")
    return value


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
    if len(value) < minimum or (maximum is not None and len(value) > maximum):
        raise ValueError(f"{field_name} is outside its length bounds")
    if forbid_whitespace and (
        value != value.strip() or any(character.isspace() for character in value)
    ):
        raise ValueError(f"{field_name} must not contain whitespace")
    return value


def _require_digest(value: object, field_name: str) -> str:
    digest = _require_exact_string(value, field_name)
    if _SHA256_PATTERN.fullmatch(digest) is None:
        raise ValueError(f"{field_name} must be one canonical SHA-256 digest")
    return digest


def _require_exact_bytes(
    value: object,
    field_name: str,
    *,
    maximum: int,
) -> bytes:
    if type(value) is not bytes:
        raise ValueError(f"{field_name} must be exact immutable bytes")
    if not value or len(value) > maximum:
        raise ValueError(f"{field_name} is outside its byte bounds")
    return value


def _require_exact_uuid(value: object, field_name: str) -> UUID:
    if type(value) is not UUID:
        raise ValueError(f"{field_name} must be an exact UUID")
    return value


def _require_exact_enum[EnumT: StrEnum](
    value: object,
    enum_type: type[EnumT],
    field_name: str,
) -> EnumT:
    if type(value) is not enum_type:
        raise ValueError(f"{field_name} must be an exact {enum_type.__name__}")
    return value


def _require_exact_model_storage(value: BaseModel, model_type: type[BaseModel]) -> None:
    try:
        stored_fields = value.__dict__
        supplied_fields = value.__pydantic_fields_set__
        extra_fields = value.__pydantic_extra__
        private_fields = value.__pydantic_private__
    except AttributeError as error:
        raise ValueError("model does not expose exact storage") from error
    declared_fields = frozenset(model_type.model_fields)
    required_fields = frozenset(
        name for name, field in model_type.model_fields.items() if field.is_required()
    )
    if (
        type(stored_fields) is not dict
        or frozenset(stored_fields) != declared_fields
        or type(supplied_fields) is not set
        or not supplied_fields <= declared_fields
        or not required_fields <= supplied_fields
        or (extra_fields is not None and (type(extra_fields) is not dict or extra_fields))
        or (private_fields is not None and (type(private_fields) is not dict or private_fields))
    ):
        raise ValueError("model contains incomplete or undeclared storage")


def _require_no_hidden_model_storage(value: BaseModel) -> None:
    active: set[int] = set()
    validated: set[int] = set()

    def visit(current: BaseModel) -> None:
        identity = id(current)
        if identity in active:
            raise ValueError("model graph contains recursive storage")
        if identity in validated:
            return

        active.add(identity)
        try:
            _require_exact_model_storage(current, type(current))
            for nested in current.__dict__.values():
                if isinstance(nested, BaseModel):
                    visit(nested)
                elif type(nested) is tuple:
                    for item in nested:
                        if isinstance(item, BaseModel):
                            visit(item)
        finally:
            active.remove(identity)
        validated.add(identity)

    visit(value)


def _revalidate_exact_model[ModelT: BaseModel](
    value: object,
    model_type: type[ModelT],
) -> ModelT:
    if type(value) is not model_type:
        raise ValueError("value has an unexpected exact model type")
    _require_no_hidden_model_storage(value)
    fields = {name: getattr(value, name) for name in model_type.model_fields}
    validated = model_type(**fields)
    _require_no_hidden_model_storage(validated)
    return validated


class _StrictPortModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        strict=True,
        validate_default=True,
        revalidate_instances="always",
    )

    @classmethod
    def revalidate_at_boundary(cls, value: object) -> Self:
        """Revalidate bypass-created caller values before a future adapter does any work."""

        try:
            return _revalidate_exact_model(value, cls)
        except ContinuousPublicTradeStreamStoreContractError:
            raise
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            RecursionError,
        ) as error:
            raise ContinuousPublicTradeStreamStoreContractError(
                ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE
            ) from error


def _require_nested_port_model[ModelT: _StrictPortModel](
    value: object,
    model_type: type[ModelT],
) -> ModelT:
    return _revalidate_exact_model(value, model_type)


def _require_task061_model[ModelT: BaseModel](
    value: object,
    model_type: type[ModelT],
    field_name: str,
) -> ModelT:
    if type(value) is not model_type:
        raise ValueError(f"{field_name} must be an exact {model_type.__name__}")
    _require_no_hidden_model_storage(value)
    return value


class ContinuousPublicTradeStreamStoreRetryDisposition(StrEnum):
    """Descriptive only; no member authorizes or performs a retry."""

    NOT_REQUIRED = "not_required"
    DO_NOT_RETRY = "do_not_retry"
    EXACT_REQUEST_ONLY = "exact_request_only"


class ContinuousPublicTradeStreamCreateOutcome(StrEnum):
    """Closed store-local create classifications."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    UNSUPPORTED_VERSION = "unsupported_version"
    CORRUPT = "corrupt"
    UNAVAILABLE = "unavailable"


class ContinuousPublicTradeStreamLoadOutcome(StrEnum):
    """Closed exact-identity current-load classifications."""

    FOUND = "found"
    NOT_FOUND = "not_found"
    IDENTITY_CONFLICT = "identity_conflict"
    UNSUPPORTED_VERSION = "unsupported_version"
    CORRUPT = "corrupt"
    UNAVAILABLE = "unavailable"


class ContinuousPublicTradeStreamCompareAndSwapOutcome(StrEnum):
    """Closed store-local compare-and-swap classifications."""

    UPDATED = "updated"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    UNSUPPORTED_VERSION = "unsupported_version"
    CORRUPT = "corrupt"
    UNAVAILABLE = "unavailable"


class ContinuousPublicTradeStreamAuditOutcome(StrEnum):
    """Closed bounded-history classifications."""

    PAGE = "page"
    AT_TAIL = "at_tail"
    NOT_FOUND = "not_found"
    IDENTITY_CONFLICT = "identity_conflict"
    ANCHOR_CONFLICT = "anchor_conflict"
    UNSUPPORTED_VERSION = "unsupported_version"
    CORRUPT = "corrupt"
    UNAVAILABLE = "unavailable"


class ContinuousPublicTradeStreamIdentityV1(_StrictPortModel):
    """Complete immutable expected stream identity."""

    stream_contract_version: Literal[1] = 1
    stream_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    request_variant: str = Field(min_length=1, max_length=128)
    policy_fingerprint: str
    stream_start_epoch_ms: int = Field(ge=0, le=MAX_CONTRACT_INTEGER)

    @field_validator("stream_contract_version", "stream_start_epoch_ms", mode="before")
    @classmethod
    def integers_are_exact(cls, value: object, info: object) -> int:
        field_name = str(getattr(info, "field_name", "identity integer"))
        return _require_exact_int(
            value,
            field_name,
            minimum=0 if field_name == "stream_start_epoch_ms" else 1,
            maximum=MAX_CONTRACT_INTEGER,
        )

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator(
        "source",
        "venue",
        "instrument",
        "provider_symbol",
        "request_variant",
        mode="before",
    )
    @classmethod
    def strings_are_exact(cls, value: object, info: object) -> str:
        field_name = str(getattr(info, "field_name", "identity string"))
        maximums = {
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
            maximum=maximums[field_name],
            forbid_whitespace=True,
        )

    @field_validator("instrument_type", mode="before")
    @classmethod
    def instrument_type_is_exact(cls, value: object) -> InstrumentType:
        return _require_exact_enum(value, InstrumentType, "instrument_type")

    @field_validator("policy_fingerprint", mode="before")
    @classmethod
    def policy_fingerprint_is_exact(cls, value: object) -> str:
        return _require_digest(value, "policy_fingerprint")


class ContinuousPublicTradeStreamExpectationV1(_StrictPortModel):
    """Complete expected identity and effective policy inputs for one store call."""

    identity: ContinuousPublicTradeStreamIdentityV1
    effective_stream_policy: ContinuousPublicTradePolicy
    effective_child_policy_fingerprint: str | None = None

    @field_validator("identity", mode="before")
    @classmethod
    def identity_is_exact(cls, value: object) -> ContinuousPublicTradeStreamIdentityV1:
        return _require_nested_port_model(value, ContinuousPublicTradeStreamIdentityV1)

    @field_validator("effective_stream_policy", mode="before")
    @classmethod
    def stream_policy_is_exact(cls, value: object) -> ContinuousPublicTradePolicy:
        policy = _require_task061_model(
            value,
            ContinuousPublicTradePolicy,
            "effective_stream_policy",
        )
        project_continuous_public_trade_policy(policy)
        return policy

    @field_validator("effective_child_policy_fingerprint", mode="before")
    @classmethod
    def child_policy_fingerprint_is_exact(cls, value: object) -> str | None:
        if value is None:
            return None
        return _require_digest(value, "effective_child_policy_fingerprint")

    @model_validator(mode="after")
    def stream_policy_matches_identity(self) -> Self:
        if self.identity.policy_fingerprint != self.effective_stream_policy.policy_fingerprint:
            raise ValueError("effective stream policy does not match expected identity")
        return self


class ContinuousPublicTradeStreamStoredEnvelopeV1(_StrictPortModel):
    """One exact retained TASK-061 envelope, original bytes, and digest."""

    envelope: ContinuousPublicTradeStreamEnvelopeV1
    canonical_bytes: bytes = Field(min_length=1, max_length=MAX_ENVELOPE_BYTES)
    envelope_digest: str

    @field_validator("envelope", mode="before")
    @classmethod
    def envelope_is_exact(cls, value: object) -> ContinuousPublicTradeStreamEnvelopeV1:
        return _require_task061_model(
            value,
            ContinuousPublicTradeStreamEnvelopeV1,
            "envelope",
        )

    @field_validator("canonical_bytes", mode="before")
    @classmethod
    def bytes_are_exact(cls, value: object) -> bytes:
        return _require_exact_bytes(
            value,
            "canonical_bytes",
            maximum=MAX_ENVELOPE_BYTES,
        )

    @field_validator("envelope_digest", mode="before")
    @classmethod
    def digest_is_exact(cls, value: object) -> str:
        return _require_digest(value, "envelope_digest")

    @model_validator(mode="after")
    def retained_envelope_is_exact(self) -> Self:
        if (
            encode_stream_envelope(self.envelope) != self.canonical_bytes
            or decode_stream_envelope(self.canonical_bytes) != self.envelope
            or stream_envelope_digest(self.envelope) != self.envelope_digest
        ):
            raise ValueError("stored envelope bytes, value, and digest disagree")
        return self


class ContinuousPublicTradeStreamStoredCreationV1(_StrictPortModel):
    """One exact retained stream-creation history entry."""

    record: ContinuousPublicTradeStreamCreationRecordV1
    canonical_bytes: bytes = Field(min_length=1, max_length=MAX_RAW_RECORD_BYTES)
    record_digest: str
    successor_envelope: ContinuousPublicTradeStreamStoredEnvelopeV1
    history_root: str
    create_authority_scope: ContinuousPublicTradeEvidenceScopeV1

    @field_validator("record", mode="before")
    @classmethod
    def record_is_exact(cls, value: object) -> ContinuousPublicTradeStreamCreationRecordV1:
        return _require_task061_model(
            value,
            ContinuousPublicTradeStreamCreationRecordV1,
            "record",
        )

    @field_validator("canonical_bytes", mode="before")
    @classmethod
    def bytes_are_exact(cls, value: object) -> bytes:
        return _require_exact_bytes(
            value,
            "canonical_bytes",
            maximum=MAX_RAW_RECORD_BYTES,
        )

    @field_validator("record_digest", "history_root", mode="before")
    @classmethod
    def digests_are_exact(cls, value: object, info: object) -> str:
        return _require_digest(value, str(getattr(info, "field_name", "digest")))

    @field_validator("successor_envelope", mode="before")
    @classmethod
    def successor_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredEnvelopeV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredEnvelopeV1,
        )

    @field_validator("create_authority_scope", mode="before")
    @classmethod
    def scope_is_exact(cls, value: object) -> ContinuousPublicTradeEvidenceScopeV1:
        scope = _require_task061_model(
            value,
            ContinuousPublicTradeEvidenceScopeV1,
            "create_authority_scope",
        )
        encode_evidence_scope(scope)
        return scope

    @model_validator(mode="after")
    def retained_creation_is_exact(self) -> Self:
        record = self.record
        if (
            encode_stream_creation_record(record) != self.canonical_bytes
            or decode_stream_creation_record(self.canonical_bytes) != record
            or stream_creation_digest(record) != self.record_digest
            or initial_stream_history_root(record) != self.history_root
            or record.successor_envelope_hex != self.successor_envelope.canonical_bytes.hex()
            or record.successor_envelope_digest != self.successor_envelope.envelope_digest
        ):
            raise ValueError("stored creation material disagrees")
        validate_stream_creation_record_scope(record, self.create_authority_scope)
        return self


class ContinuousPublicTradeStreamStoredTransitionV1(_StrictPortModel):
    """One exact retained stream-transition history entry."""

    record: ContinuousPublicTradeStreamTransitionRecordV1
    canonical_bytes: bytes = Field(min_length=1, max_length=MAX_RAW_RECORD_BYTES)
    record_digest: str
    successor_envelope: ContinuousPublicTradeStreamStoredEnvelopeV1
    history_root: str
    transition_authority_scope: ContinuousPublicTradeEvidenceScopeV1
    child_completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None

    @field_validator("record", mode="before")
    @classmethod
    def record_is_exact(cls, value: object) -> ContinuousPublicTradeStreamTransitionRecordV1:
        return _require_task061_model(
            value,
            ContinuousPublicTradeStreamTransitionRecordV1,
            "record",
        )

    @field_validator("canonical_bytes", mode="before")
    @classmethod
    def bytes_are_exact(cls, value: object) -> bytes:
        return _require_exact_bytes(
            value,
            "canonical_bytes",
            maximum=MAX_RAW_RECORD_BYTES,
        )

    @field_validator("record_digest", "history_root", mode="before")
    @classmethod
    def digests_are_exact(cls, value: object, info: object) -> str:
        return _require_digest(value, str(getattr(info, "field_name", "digest")))

    @field_validator("successor_envelope", mode="before")
    @classmethod
    def successor_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredEnvelopeV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredEnvelopeV1,
        )

    @field_validator(
        "transition_authority_scope",
        "child_completion_scope",
        mode="before",
    )
    @classmethod
    def scopes_are_exact(
        cls,
        value: object,
        info: object,
    ) -> ContinuousPublicTradeEvidenceScopeV1 | None:
        if value is None:
            return None
        scope = _require_task061_model(
            value,
            ContinuousPublicTradeEvidenceScopeV1,
            str(getattr(info, "field_name", "scope")),
        )
        encode_evidence_scope(scope)
        return scope

    @model_validator(mode="after")
    def retained_transition_is_exact(self) -> Self:
        record = self.record
        if (
            encode_stream_transition_record(record) != self.canonical_bytes
            or decode_stream_transition_record(self.canonical_bytes) != record
            or stream_transition_digest(record) != self.record_digest
            or next_stream_history_root(record.prior_history_root, record) != self.history_root
            or record.successor_envelope_hex != self.successor_envelope.canonical_bytes.hex()
            or record.successor_envelope_digest != self.successor_envelope.envelope_digest
        ):
            raise ValueError("stored transition material disagrees")
        _validate_transition_scopes_without_predecessor(self)
        return self


def _validate_transition_scopes_without_predecessor(
    stored: ContinuousPublicTradeStreamStoredTransitionV1,
) -> None:
    record = stored.record
    successor = stored.successor_envelope.envelope
    authority = stored.transition_authority_scope
    common_authority_mismatch = (
        authority.evidence_kind is not ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY
        or authority.stream_id != record.stream_id
        or authority.transition_kind is not record.transition_kind
        or authority.prior_version != record.prior_version
        or authority.prior_envelope_digest != record.prior_envelope_digest
        or authority.prior_history_root != record.prior_history_root
        or authority.successor_version != record.successor_version
        or authority.reason_code != record.reason_code
        or authority.stream_policy is not None
        or record.transition_authority_reference.scope_digest != evidence_scope_digest(authority)
    )
    if common_authority_mismatch:
        raise ValueError("transition-authority scope disagrees with its record")

    if record.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH:
        attachment = successor.checkpoint.attachment
        payload = successor.child_creation_payload
        if (
            attachment is None
            or payload is None
            or authority.successor_envelope_digest is not None
            or authority.child_job_id != attachment.job_id
            or authority.child_policy_fingerprint != payload.child_checkpoint.policy_fingerprint
            or authority.child_creation_fingerprint is not None
        ):
            raise ValueError("attach-authority scope disagrees with its successor")
    elif (
        authority.successor_envelope_digest != record.successor_envelope_digest
        or authority.child_job_id is not None
        or authority.child_policy_fingerprint is not None
        or authority.child_creation_fingerprint is not None
    ):
        raise ValueError("transition-authority scope disagrees with its successor")

    completion = stored.child_completion_scope
    completion_reference = record.child_completion_reference
    if completion is None:
        if completion_reference is not None:
            raise ValueError("child-completion scope is missing")
        return
    if (
        completion_reference is None
        or completion.evidence_kind is not ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
        or completion.transition_kind is not ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
        or completion.stream_id != record.stream_id
        or completion.prior_version != record.prior_version
        or completion.prior_envelope_digest != record.prior_envelope_digest
        or completion.prior_history_root != record.prior_history_root
        or completion.successor_version != record.successor_version
        or completion.successor_envelope_digest != record.successor_envelope_digest
        or completion.reason_code is not None
        or completion.stream_policy is not None
        or completion_reference.scope_digest != evidence_scope_digest(completion)
    ):
        raise ValueError("child-completion scope disagrees with its record")


type ContinuousPublicTradeStreamStoredHistoryEntryV1 = (
    ContinuousPublicTradeStreamStoredCreationV1 | ContinuousPublicTradeStreamStoredTransitionV1
)


def _identity_matches_checkpoint(
    identity: ContinuousPublicTradeStreamIdentityV1,
    checkpoint: ContinuousPublicTradeStreamCheckpoint,
) -> bool:
    return (
        identity.stream_id == checkpoint.stream_id
        and identity.source == checkpoint.source
        and identity.venue == checkpoint.venue
        and identity.instrument == checkpoint.instrument
        and identity.provider_symbol == checkpoint.provider_symbol
        and identity.instrument_type is checkpoint.instrument_type
        and identity.request_variant == checkpoint.request_variant
        and identity.policy_fingerprint == checkpoint.policy_fingerprint
        and identity.stream_start_epoch_ms == checkpoint.stream_start_epoch_ms
    )


def _identity_matches_creation(
    identity: ContinuousPublicTradeStreamIdentityV1,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> bool:
    record = creation.record
    return (
        identity.stream_id == record.stream_id
        and identity.source == record.source
        and identity.venue == record.venue
        and identity.instrument == record.instrument
        and identity.provider_symbol == record.provider_symbol
        and identity.instrument_type is record.instrument_type
        and identity.request_variant == record.request_variant
        and identity.policy_fingerprint == record.stream_policy.policy_fingerprint
        and identity.stream_start_epoch_ms == record.stream_start_epoch_ms
        and _identity_matches_checkpoint(
            identity,
            creation.successor_envelope.envelope.checkpoint,
        )
    )


class ContinuousPublicTradeStreamCreateCommandV1(_StrictPortModel):
    """One fully finalized stream-creation artifact for a future atomic insert."""

    expectation: ContinuousPublicTradeStreamExpectationV1
    creation: ContinuousPublicTradeStreamStoredCreationV1

    @field_validator("expectation", mode="before")
    @classmethod
    def expectation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamExpectationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamExpectationV1,
        )

    @field_validator("creation", mode="before")
    @classmethod
    def creation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredCreationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredCreationV1,
        )

    @model_validator(mode="after")
    def creation_is_cross_bound(self) -> Self:
        expectation = self.expectation
        creation = self.creation
        if (
            expectation.effective_child_policy_fingerprint is not None
            or not _identity_matches_creation(expectation.identity, creation)
            or project_continuous_public_trade_policy(expectation.effective_stream_policy)
            != creation.record.stream_policy
        ):
            raise ValueError("create expectation disagrees with finalized creation")
        validate_stream_load_bindings(
            creation.record,
            creation.successor_envelope.envelope,
            effective_stream_policy=expectation.effective_stream_policy,
            effective_child_policy_fingerprint=None,
        )
        return self


class ContinuousPublicTradeStreamLoadQueryV1(_StrictPortModel):
    """One complete exact-identity current-load expectation."""

    expectation: ContinuousPublicTradeStreamExpectationV1

    @field_validator("expectation", mode="before")
    @classmethod
    def expectation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamExpectationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamExpectationV1,
        )


class ContinuousPublicTradeStreamCompareAndSwapCommandV1(_StrictPortModel):
    """One finalized transition with exact prior CAS expectations and no alternate successor."""

    expectation: ContinuousPublicTradeStreamExpectationV1
    expected_version: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    expected_envelope_digest: str
    expected_history_root: str
    transition: ContinuousPublicTradeStreamStoredTransitionV1

    @field_validator("expectation", mode="before")
    @classmethod
    def expectation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamExpectationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamExpectationV1,
        )

    @field_validator("expected_version", mode="before")
    @classmethod
    def expected_version_is_exact(cls, value: object) -> int:
        return _require_exact_int(
            value,
            "expected_version",
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )

    @field_validator(
        "expected_envelope_digest",
        "expected_history_root",
        mode="before",
    )
    @classmethod
    def expected_digests_are_exact(cls, value: object, info: object) -> str:
        return _require_digest(value, str(getattr(info, "field_name", "expected digest")))

    @field_validator("transition", mode="before")
    @classmethod
    def transition_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredTransitionV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredTransitionV1,
        )

    @model_validator(mode="after")
    def compare_and_swap_is_cross_bound(self) -> Self:
        expectation = self.expectation
        identity = expectation.identity
        transition = self.transition
        record = transition.record
        successor = transition.successor_envelope.envelope
        child_fingerprint = expectation.effective_child_policy_fingerprint
        if (
            self.expected_version != record.prior_version
            or self.expected_envelope_digest != record.prior_envelope_digest
            or self.expected_history_root != record.prior_history_root
            or record.stream_id != identity.stream_id
            or not _identity_matches_checkpoint(identity, successor.checkpoint)
        ):
            raise ValueError("CAS expectation disagrees with finalized transition")

        payload = successor.child_creation_payload
        if payload is not None:
            if (
                child_fingerprint is None
                or payload.child_checkpoint.policy_fingerprint != child_fingerprint
            ):
                raise ValueError("CAS child policy disagrees with attached successor")
        elif record.transition_kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
            completion_scope = transition.child_completion_scope
            if (
                child_fingerprint is None
                or completion_scope is None
                or completion_scope.child_policy_fingerprint != child_fingerprint
            ):
                raise ValueError("CAS child policy disagrees with completed predecessor")
        elif child_fingerprint is not None:
            raise ValueError("unattached CAS cannot carry a child-policy expectation")
        return self


class ContinuousPublicTradeStreamAuditContinuationV1(_StrictPortModel):
    """One exact validated history anchor returned by an earlier page."""

    stream_id: UUID
    through_version: int = Field(ge=1, le=MAX_CONTRACT_INTEGER)
    through_envelope_digest: str
    through_history_root: str

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("through_version", mode="before")
    @classmethod
    def through_version_is_exact(cls, value: object) -> int:
        return _require_exact_int(
            value,
            "through_version",
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )

    @field_validator(
        "through_envelope_digest",
        "through_history_root",
        mode="before",
    )
    @classmethod
    def digests_are_exact(cls, value: object, info: object) -> str:
        return _require_digest(value, str(getattr(info, "field_name", "continuation digest")))


class ContinuousPublicTradeStreamAuditStartQueryV1(_StrictPortModel):
    """Request the bounded creation-first audit page for one exact stream."""

    expectation: ContinuousPublicTradeStreamExpectationV1
    limit: int = Field(ge=1, le=100)

    @field_validator("expectation", mode="before")
    @classmethod
    def expectation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamExpectationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamExpectationV1,
        )

    @field_validator("limit", mode="before")
    @classmethod
    def limit_is_exact(cls, value: object) -> int:
        return _require_exact_int(value, "limit", minimum=1, maximum=100)


class ContinuousPublicTradeStreamAuditContinuationQueryV1(_StrictPortModel):
    """Request one bounded page after an exact prior continuation anchor."""

    expectation: ContinuousPublicTradeStreamExpectationV1
    continuation: ContinuousPublicTradeStreamAuditContinuationV1
    limit: int = Field(ge=1, le=100)

    @field_validator("expectation", mode="before")
    @classmethod
    def expectation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamExpectationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamExpectationV1,
        )

    @field_validator("continuation", mode="before")
    @classmethod
    def continuation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamAuditContinuationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamAuditContinuationV1,
        )

    @field_validator("limit", mode="before")
    @classmethod
    def limit_is_exact(cls, value: object) -> int:
        return _require_exact_int(value, "limit", minimum=1, maximum=100)

    @model_validator(mode="after")
    def continuation_matches_identity(self) -> Self:
        if self.continuation.stream_id != self.expectation.identity.stream_id:
            raise ValueError("audit continuation stream does not match expectation")
        return self


type ContinuousPublicTradeStreamAuditQueryV1 = (
    ContinuousPublicTradeStreamAuditStartQueryV1
    | ContinuousPublicTradeStreamAuditContinuationQueryV1
)


def _revalidate_history_entry(
    value: object,
) -> ContinuousPublicTradeStreamStoredHistoryEntryV1:
    if type(value) is ContinuousPublicTradeStreamStoredCreationV1:
        return _revalidate_exact_model(
            value,
            ContinuousPublicTradeStreamStoredCreationV1,
        )
    if type(value) is ContinuousPublicTradeStreamStoredTransitionV1:
        return _revalidate_exact_model(
            value,
            ContinuousPublicTradeStreamStoredTransitionV1,
        )
    raise ValueError("history entry has an unexpected exact type")


def _entry_stream_id(entry: ContinuousPublicTradeStreamStoredHistoryEntryV1) -> UUID:
    return entry.record.stream_id


def _entry_version(entry: ContinuousPublicTradeStreamStoredHistoryEntryV1) -> int:
    return entry.record.successor_version


def _entry_recorded_at(
    entry: ContinuousPublicTradeStreamStoredHistoryEntryV1,
) -> datetime:
    return entry.record.recorded_at


def _validate_direct_history_link(
    predecessor: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    current: ContinuousPublicTradeStreamStoredTransitionV1,
) -> None:
    record = current.record
    if (
        _entry_stream_id(predecessor) != record.stream_id
        or _entry_version(predecessor) != record.prior_version
        or predecessor.successor_envelope.envelope_digest != record.prior_envelope_digest
        or predecessor.history_root != record.prior_history_root
        or _entry_recorded_at(predecessor) > record.recorded_at
    ):
        raise ValueError("direct predecessor does not match transition")
    validate_stream_transition_record_scopes(
        predecessor.successor_envelope.envelope,
        record,
        current.transition_authority_scope,
        current.child_completion_scope,
    )


def _policy_from_creation(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> ContinuousPublicTradePolicy:
    return ContinuousPublicTradePolicy(**creation.record.stream_policy.model_dump())


def _validate_full_history_link(
    predecessor: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    current: ContinuousPublicTradeStreamStoredTransitionV1,
    policy: ContinuousPublicTradePolicy,
) -> None:
    _validate_direct_history_link(predecessor, current)
    validate_stream_transition_link(
        predecessor.successor_envelope.envelope,
        current.record,
        policy=policy,
        prior_history_root=predecessor.history_root,
        prior_recorded_at=_entry_recorded_at(predecessor),
        transition_authority_scope=current.transition_authority_scope,
        child_completion_scope=current.child_completion_scope,
    )


class ContinuousPublicTradeStreamCreateReceiptV1(_StrictPortModel):
    """The exact historical creation entry accepted by create."""

    creation: ContinuousPublicTradeStreamStoredCreationV1

    @field_validator("creation", mode="before")
    @classmethod
    def creation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredCreationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredCreationV1,
        )


class ContinuousPublicTradeStreamCompareAndSwapReceiptV1(_StrictPortModel):
    """The exact historical transition entry accepted by compare-and-swap."""

    transition: ContinuousPublicTradeStreamStoredTransitionV1

    @field_validator("transition", mode="before")
    @classmethod
    def transition_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredTransitionV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredTransitionV1,
        )


class ContinuousPublicTradeStreamCurrentViewV1(_StrictPortModel):
    """One constant-size structurally validated current stream view."""

    creation: ContinuousPublicTradeStreamStoredCreationV1
    current: ContinuousPublicTradeStreamStoredHistoryEntryV1
    predecessor: ContinuousPublicTradeStreamStoredHistoryEntryV1 | None = None

    @field_validator("creation", mode="before")
    @classmethod
    def creation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredCreationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamStoredCreationV1,
        )

    @field_validator("current", "predecessor", mode="before")
    @classmethod
    def history_entries_are_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredHistoryEntryV1 | None:
        if value is None:
            return None
        return _revalidate_history_entry(value)

    @model_validator(mode="after")
    def bounded_view_is_consistent(self) -> Self:
        creation = self.creation
        current = self.current
        predecessor = self.predecessor
        policy = _policy_from_creation(creation)
        if _entry_stream_id(current) != creation.record.stream_id:
            raise ValueError("current entry does not match creation stream")
        if type(current) is ContinuousPublicTradeStreamStoredCreationV1:
            if current != creation or predecessor is not None:
                raise ValueError("version-one current view has an invalid predecessor")
        else:
            if not isinstance(current, ContinuousPublicTradeStreamStoredTransitionV1):
                raise ValueError("later current entry must be an exact transition")
            if predecessor is None:
                raise ValueError("later current view requires one direct predecessor")
            if current.record.successor_version == 2:
                if type(predecessor) is not ContinuousPublicTradeStreamStoredCreationV1:
                    raise ValueError("version-two predecessor must be stream creation")
                if predecessor != creation:
                    raise ValueError("version-two predecessor must be the creation entry")
            elif type(predecessor) is not ContinuousPublicTradeStreamStoredTransitionV1:
                raise ValueError("later predecessor must be the preceding transition")
            _validate_full_history_link(predecessor, current, policy)

        envelope = current.successor_envelope.envelope
        payload = envelope.child_creation_payload
        validate_stream_load_bindings(
            creation.record,
            envelope,
            effective_stream_policy=policy,
            effective_child_policy_fingerprint=(
                None if payload is None else payload.child_checkpoint.policy_fingerprint
            ),
        )
        return self


class ContinuousPublicTradeStreamAuditPageV1(_StrictPortModel):
    """One bounded structurally contiguous page with at most one predecessor overlap.

    Continuation pages do not retain creation policy.  Call
    ``validate_continuous_public_trade_stream_audit_page`` to bind a page to its exact query and
    validate every transition under that query's complete effective policy.
    """

    predecessor_overlap: ContinuousPublicTradeStreamStoredHistoryEntryV1 | None = None
    records: tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...] = Field(
        min_length=1,
        max_length=100,
    )
    continuation: ContinuousPublicTradeStreamAuditContinuationV1

    @field_validator("predecessor_overlap", mode="before")
    @classmethod
    def predecessor_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoredHistoryEntryV1 | None:
        if value is None:
            return None
        return _revalidate_history_entry(value)

    @field_validator("records", mode="before")
    @classmethod
    def records_are_exact(
        cls,
        value: object,
    ) -> tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...]:
        if type(value) is not tuple or not 1 <= len(value) <= 100:
            raise ValueError("audit records must be an exact bounded tuple")
        return tuple(_revalidate_history_entry(entry) for entry in value)

    @field_validator("continuation", mode="before")
    @classmethod
    def continuation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamAuditContinuationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamAuditContinuationV1,
        )

    @model_validator(mode="after")
    def page_is_contiguous(self) -> Self:
        predecessor = self.predecessor_overlap
        records = self.records
        policy: ContinuousPublicTradePolicy | None = None
        first = records[0]
        if predecessor is None:
            if type(first) is not ContinuousPublicTradeStreamStoredCreationV1:
                raise ValueError("first audit page must begin with stream creation")
            policy = _policy_from_creation(first)
        elif type(first) is not ContinuousPublicTradeStreamStoredTransitionV1:
            raise ValueError("continuation page may add only transition entries")

        previous = predecessor
        for index, entry in enumerate(records):
            if index and type(entry) is ContinuousPublicTradeStreamStoredCreationV1:
                raise ValueError("stream creation may occur only at the first history position")
            if previous is not None:
                if type(entry) is not ContinuousPublicTradeStreamStoredTransitionV1:
                    raise ValueError("history successor must be a transition")
                if policy is None and type(previous) is ContinuousPublicTradeStreamStoredCreationV1:
                    policy = _policy_from_creation(previous)
                if policy is None:
                    _validate_direct_history_link(previous, entry)
                else:
                    _validate_full_history_link(previous, entry, policy)
            previous = entry

        final = records[-1]
        if (
            self.continuation.stream_id != _entry_stream_id(final)
            or self.continuation.through_version != _entry_version(final)
            or self.continuation.through_envelope_digest != final.successor_envelope.envelope_digest
            or self.continuation.through_history_root != final.history_root
        ):
            raise ValueError("audit continuation does not describe the final new record")
        return self


def validate_continuous_public_trade_stream_audit_page(
    query: ContinuousPublicTradeStreamAuditQueryV1,
    page: ContinuousPublicTradeStreamAuditPageV1,
    /,
) -> None:
    """Cross-bind one exact audit query and page without performing I/O or granting authority."""

    exact_query: ContinuousPublicTradeStreamAuditQueryV1
    if type(query) is ContinuousPublicTradeStreamAuditStartQueryV1:
        exact_query = ContinuousPublicTradeStreamAuditStartQueryV1.revalidate_at_boundary(query)
    elif type(query) is ContinuousPublicTradeStreamAuditContinuationQueryV1:
        exact_query = ContinuousPublicTradeStreamAuditContinuationQueryV1.revalidate_at_boundary(
            query
        )
    else:
        _fail(ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE)
    exact_page = ContinuousPublicTradeStreamAuditPageV1.revalidate_at_boundary(page)

    try:
        if len(exact_page.records) > exact_query.limit:
            _fail(ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE)

        expectation = exact_query.expectation
        identity = expectation.identity
        policy = expectation.effective_stream_policy
        previous: ContinuousPublicTradeStreamStoredHistoryEntryV1
        transitions: tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...]
        if type(exact_query) is ContinuousPublicTradeStreamAuditStartQueryV1:
            creation = exact_page.records[0]
            if (
                exact_page.predecessor_overlap is not None
                or type(creation) is not ContinuousPublicTradeStreamStoredCreationV1
                or not _identity_matches_creation(identity, creation)
                or project_continuous_public_trade_policy(policy) != creation.record.stream_policy
            ):
                _fail(ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE)
            validate_stream_load_bindings(
                creation.record,
                creation.successor_envelope.envelope,
                effective_stream_policy=policy,
                effective_child_policy_fingerprint=None,
            )
            previous = creation
            transitions = exact_page.records[1:]
        else:
            if not isinstance(
                exact_query,
                ContinuousPublicTradeStreamAuditContinuationQueryV1,
            ):
                _fail(ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE)
            predecessor = exact_page.predecessor_overlap
            anchor = exact_query.continuation
            if (
                predecessor is None
                or anchor.stream_id != _entry_stream_id(predecessor)
                or anchor.through_version != _entry_version(predecessor)
                or anchor.through_envelope_digest != predecessor.successor_envelope.envelope_digest
                or anchor.through_history_root != predecessor.history_root
                or not _identity_matches_checkpoint(
                    identity,
                    predecessor.successor_envelope.envelope.checkpoint,
                )
            ):
                _fail(ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE)
            previous = predecessor
            transitions = exact_page.records

        for transition in transitions:
            if type(transition) is not ContinuousPublicTradeStreamStoredTransitionV1:
                _fail(ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE)
            _validate_full_history_link(previous, transition, policy)
            previous = transition
    except ContinuousPublicTradeStreamStoreContractError:
        raise
    except (
        ValidationError,
        ValueError,
        TypeError,
        AttributeError,
        OverflowError,
        RecursionError,
    ) as error:
        raise ContinuousPublicTradeStreamStoreContractError(
            ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE
        ) from error


class _CreateResultBase(_StrictPortModel):
    stream_id: UUID
    outcome: ContinuousPublicTradeStreamCreateOutcome
    retry_disposition: ContinuousPublicTradeStreamStoreRetryDisposition

    @model_validator(mode="before")
    @classmethod
    def supplied_outcome_is_exact(cls, value: object) -> object:
        if (
            type(value) is dict
            and "outcome" in value
            and type(value["outcome"]) is not ContinuousPublicTradeStreamCreateOutcome
        ):
            raise ValueError("outcome must be an exact create outcome")
        return value

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("retry_disposition", mode="before")
    @classmethod
    def retry_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoreRetryDisposition:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeStreamStoreRetryDisposition,
            "retry_disposition",
        )

    @model_validator(mode="after")
    def outcome_is_exact(self) -> Self:
        _require_exact_enum(
            self.outcome,
            ContinuousPublicTradeStreamCreateOutcome,
            "outcome",
        )
        return self


class ContinuousPublicTradeStreamCreateAcceptedResultV1(_CreateResultBase):
    """An inserted or exact historical duplicate creation receipt."""

    outcome: Literal[
        ContinuousPublicTradeStreamCreateOutcome.INSERTED,
        ContinuousPublicTradeStreamCreateOutcome.DUPLICATE,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    receipt: ContinuousPublicTradeStreamCreateReceiptV1

    @field_validator("receipt", mode="before")
    @classmethod
    def receipt_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamCreateReceiptV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamCreateReceiptV1,
        )

    @model_validator(mode="after")
    def receipt_matches_stream(self) -> Self:
        if self.receipt.creation.record.stream_id != self.stream_id:
            raise ValueError("create receipt does not match result stream")
        return self


class ContinuousPublicTradeStreamCreateRejectedResultV1(_CreateResultBase):
    """A conclusive fail-closed create disagreement with no success payload."""

    outcome: Literal[
        ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
        ContinuousPublicTradeStreamCreateOutcome.UNSUPPORTED_VERSION,
        ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )


class ContinuousPublicTradeStreamCreateUnavailableResultV1(_CreateResultBase):
    """Storage could not establish one coherent create classification."""

    outcome: Literal[ContinuousPublicTradeStreamCreateOutcome.UNAVAILABLE] = (
        ContinuousPublicTradeStreamCreateOutcome.UNAVAILABLE
    )
    retry_disposition: Literal[
        ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    ] = ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY


type ContinuousPublicTradeStreamCreateResultV1 = Annotated[
    ContinuousPublicTradeStreamCreateAcceptedResultV1
    | ContinuousPublicTradeStreamCreateRejectedResultV1
    | ContinuousPublicTradeStreamCreateUnavailableResultV1,
    Field(discriminator="outcome"),
]


class _LoadResultBase(_StrictPortModel):
    stream_id: UUID
    outcome: ContinuousPublicTradeStreamLoadOutcome
    retry_disposition: ContinuousPublicTradeStreamStoreRetryDisposition

    @model_validator(mode="before")
    @classmethod
    def supplied_outcome_is_exact(cls, value: object) -> object:
        if (
            type(value) is dict
            and "outcome" in value
            and type(value["outcome"]) is not ContinuousPublicTradeStreamLoadOutcome
        ):
            raise ValueError("outcome must be an exact load outcome")
        return value

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("retry_disposition", mode="before")
    @classmethod
    def retry_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoreRetryDisposition:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeStreamStoreRetryDisposition,
            "retry_disposition",
        )

    @model_validator(mode="after")
    def outcome_is_exact(self) -> Self:
        _require_exact_enum(
            self.outcome,
            ContinuousPublicTradeStreamLoadOutcome,
            "outcome",
        )
        return self


class ContinuousPublicTradeStreamLoadFoundResultV1(_LoadResultBase):
    """A bounded structurally validated current view."""

    outcome: Literal[ContinuousPublicTradeStreamLoadOutcome.FOUND] = (
        ContinuousPublicTradeStreamLoadOutcome.FOUND
    )
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    view: ContinuousPublicTradeStreamCurrentViewV1

    @field_validator("view", mode="before")
    @classmethod
    def view_is_exact(cls, value: object) -> ContinuousPublicTradeStreamCurrentViewV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamCurrentViewV1,
        )

    @model_validator(mode="after")
    def view_matches_stream(self) -> Self:
        if self.view.creation.record.stream_id != self.stream_id:
            raise ValueError("current view does not match result stream")
        return self


class ContinuousPublicTradeStreamLoadNotFoundResultV1(_LoadResultBase):
    """Both UUID and natural identity were absent in one coherent view."""

    outcome: Literal[ContinuousPublicTradeStreamLoadOutcome.NOT_FOUND] = (
        ContinuousPublicTradeStreamLoadOutcome.NOT_FOUND
    )
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )


class ContinuousPublicTradeStreamLoadRejectedResultV1(_LoadResultBase):
    """A conclusive fail-closed current-load disagreement."""

    outcome: Literal[
        ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT,
        ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION,
        ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )


class ContinuousPublicTradeStreamLoadUnavailableResultV1(_LoadResultBase):
    """Storage could not establish one coherent current-load classification."""

    outcome: Literal[ContinuousPublicTradeStreamLoadOutcome.UNAVAILABLE] = (
        ContinuousPublicTradeStreamLoadOutcome.UNAVAILABLE
    )
    retry_disposition: Literal[
        ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    ] = ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY


type ContinuousPublicTradeStreamLoadResultV1 = Annotated[
    ContinuousPublicTradeStreamLoadFoundResultV1
    | ContinuousPublicTradeStreamLoadNotFoundResultV1
    | ContinuousPublicTradeStreamLoadRejectedResultV1
    | ContinuousPublicTradeStreamLoadUnavailableResultV1,
    Field(discriminator="outcome"),
]


class _CompareAndSwapResultBase(_StrictPortModel):
    stream_id: UUID
    outcome: ContinuousPublicTradeStreamCompareAndSwapOutcome
    retry_disposition: ContinuousPublicTradeStreamStoreRetryDisposition

    @model_validator(mode="before")
    @classmethod
    def supplied_outcome_is_exact(cls, value: object) -> object:
        if (
            type(value) is dict
            and "outcome" in value
            and type(value["outcome"]) is not ContinuousPublicTradeStreamCompareAndSwapOutcome
        ):
            raise ValueError("outcome must be an exact compare-and-swap outcome")
        return value

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("retry_disposition", mode="before")
    @classmethod
    def retry_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoreRetryDisposition:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeStreamStoreRetryDisposition,
            "retry_disposition",
        )

    @model_validator(mode="after")
    def outcome_is_exact(self) -> Self:
        _require_exact_enum(
            self.outcome,
            ContinuousPublicTradeStreamCompareAndSwapOutcome,
            "outcome",
        )
        return self


class ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(_CompareAndSwapResultBase):
    """An updated or exact historical duplicate transition receipt."""

    outcome: Literal[
        ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED,
        ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    receipt: ContinuousPublicTradeStreamCompareAndSwapReceiptV1

    @field_validator("receipt", mode="before")
    @classmethod
    def receipt_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamCompareAndSwapReceiptV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamCompareAndSwapReceiptV1,
        )

    @model_validator(mode="after")
    def receipt_matches_stream(self) -> Self:
        if self.receipt.transition.record.stream_id != self.stream_id:
            raise ValueError("transition receipt does not match result stream")
        return self


class ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(_CompareAndSwapResultBase):
    """A conclusive fail-closed compare-and-swap disagreement."""

    outcome: Literal[
        ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT,
        ContinuousPublicTradeStreamCompareAndSwapOutcome.UNSUPPORTED_VERSION,
        ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )


class ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1(_CompareAndSwapResultBase):
    """Storage could not establish one coherent CAS classification."""

    outcome: Literal[ContinuousPublicTradeStreamCompareAndSwapOutcome.UNAVAILABLE] = (
        ContinuousPublicTradeStreamCompareAndSwapOutcome.UNAVAILABLE
    )
    retry_disposition: Literal[
        ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    ] = ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY


type ContinuousPublicTradeStreamCompareAndSwapResultV1 = Annotated[
    ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1
    | ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1
    | ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1,
    Field(discriminator="outcome"),
]


class _AuditResultBase(_StrictPortModel):
    stream_id: UUID
    outcome: ContinuousPublicTradeStreamAuditOutcome
    retry_disposition: ContinuousPublicTradeStreamStoreRetryDisposition

    @model_validator(mode="before")
    @classmethod
    def supplied_outcome_is_exact(cls, value: object) -> object:
        if (
            type(value) is dict
            and "outcome" in value
            and type(value["outcome"]) is not ContinuousPublicTradeStreamAuditOutcome
        ):
            raise ValueError("outcome must be an exact audit outcome")
        return value

    @field_validator("stream_id", mode="before")
    @classmethod
    def stream_id_is_exact(cls, value: object) -> UUID:
        return _require_exact_uuid(value, "stream_id")

    @field_validator("retry_disposition", mode="before")
    @classmethod
    def retry_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamStoreRetryDisposition:
        return _require_exact_enum(
            value,
            ContinuousPublicTradeStreamStoreRetryDisposition,
            "retry_disposition",
        )

    @model_validator(mode="after")
    def outcome_is_exact(self) -> Self:
        _require_exact_enum(
            self.outcome,
            ContinuousPublicTradeStreamAuditOutcome,
            "outcome",
        )
        return self


class ContinuousPublicTradeStreamAuditPageResultV1(_AuditResultBase):
    """One nonempty bounded page of new history records."""

    outcome: Literal[ContinuousPublicTradeStreamAuditOutcome.PAGE] = (
        ContinuousPublicTradeStreamAuditOutcome.PAGE
    )
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    page: ContinuousPublicTradeStreamAuditPageV1

    @field_validator("page", mode="before")
    @classmethod
    def page_is_exact(cls, value: object) -> ContinuousPublicTradeStreamAuditPageV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamAuditPageV1,
        )

    @model_validator(mode="after")
    def page_matches_stream(self) -> Self:
        if self.page.continuation.stream_id != self.stream_id:
            raise ValueError("audit page does not match result stream")
        return self


class ContinuousPublicTradeStreamAuditAtTailResultV1(_AuditResultBase):
    """The exact validated continuation already identifies the retained tail."""

    outcome: Literal[ContinuousPublicTradeStreamAuditOutcome.AT_TAIL] = (
        ContinuousPublicTradeStreamAuditOutcome.AT_TAIL
    )
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    continuation: ContinuousPublicTradeStreamAuditContinuationV1

    @field_validator("continuation", mode="before")
    @classmethod
    def continuation_is_exact(
        cls,
        value: object,
    ) -> ContinuousPublicTradeStreamAuditContinuationV1:
        return _require_nested_port_model(
            value,
            ContinuousPublicTradeStreamAuditContinuationV1,
        )

    @model_validator(mode="after")
    def continuation_matches_stream(self) -> Self:
        if self.continuation.stream_id != self.stream_id:
            raise ValueError("tail continuation does not match result stream")
        return self


class ContinuousPublicTradeStreamAuditNotFoundResultV1(_AuditResultBase):
    """Both UUID and natural identity were absent in one coherent audit view."""

    outcome: Literal[ContinuousPublicTradeStreamAuditOutcome.NOT_FOUND] = (
        ContinuousPublicTradeStreamAuditOutcome.NOT_FOUND
    )
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )


class ContinuousPublicTradeStreamAuditRejectedResultV1(_AuditResultBase):
    """A conclusive fail-closed audit disagreement."""

    outcome: Literal[
        ContinuousPublicTradeStreamAuditOutcome.IDENTITY_CONFLICT,
        ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT,
        ContinuousPublicTradeStreamAuditOutcome.UNSUPPORTED_VERSION,
        ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
    ]
    retry_disposition: Literal[ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY] = (
        ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )


class ContinuousPublicTradeStreamAuditUnavailableResultV1(_AuditResultBase):
    """Storage could not establish one coherent audit classification."""

    outcome: Literal[ContinuousPublicTradeStreamAuditOutcome.UNAVAILABLE] = (
        ContinuousPublicTradeStreamAuditOutcome.UNAVAILABLE
    )
    retry_disposition: Literal[
        ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    ] = ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY


type ContinuousPublicTradeStreamAuditResultV1 = Annotated[
    ContinuousPublicTradeStreamAuditPageResultV1
    | ContinuousPublicTradeStreamAuditAtTailResultV1
    | ContinuousPublicTradeStreamAuditNotFoundResultV1
    | ContinuousPublicTradeStreamAuditRejectedResultV1
    | ContinuousPublicTradeStreamAuditUnavailableResultV1,
    Field(discriminator="outcome"),
]


class ContinuousPublicTradeStreamStore(Protocol):
    """Unused provider-independent logical boundary for one future atomic stream store."""

    def create(
        self,
        command: ContinuousPublicTradeStreamCreateCommandV1,
        /,
    ) -> ContinuousPublicTradeStreamCreateResultV1:
        """Classify or atomically accept one finalized stream creation."""

    def load_current(
        self,
        query: ContinuousPublicTradeStreamLoadQueryV1,
        /,
    ) -> ContinuousPublicTradeStreamLoadResultV1:
        """Return one bounded exact-identity current view or a closed failure."""

    def compare_and_swap(
        self,
        command: ContinuousPublicTradeStreamCompareAndSwapCommandV1,
        /,
    ) -> ContinuousPublicTradeStreamCompareAndSwapResultV1:
        """Classify or atomically accept the sole finalized transition record."""

    def audit_page(
        self,
        query: ContinuousPublicTradeStreamAuditQueryV1,
        /,
    ) -> ContinuousPublicTradeStreamAuditResultV1:
        """Return at most 100 new records plus at most one predecessor overlap."""
