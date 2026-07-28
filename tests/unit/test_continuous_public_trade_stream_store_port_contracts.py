"""Pure contract tests for the unused continuous public-trade stream-store port."""

from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast, get_type_hints
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

import wealth.ports.continuous_public_trade_stream_store as store_contract
from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeStreamCheckpoint,
    ContinuousPublicTradeStreamStatus,
    ContinuousPublicTradeTransitionKind,
)
from wealth.domain.continuous_public_trade_persistence import (
    ContinuousPublicTradeEvidenceKind,
    ContinuousPublicTradeEvidenceOutcome,
    ContinuousPublicTradeEvidenceReferenceV1,
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradeStreamCreationRecordV1,
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeStreamTransitionRecordV1,
    encode_stream_creation_record,
    encode_stream_envelope,
    encode_stream_transition_record,
    evidence_scope_digest,
    finalize_continuous_public_trade_attachment,
    initial_stream_history_root,
    next_stream_history_root,
    project_continuous_public_trade_policy,
    stream_creation_digest,
    stream_envelope_digest,
    stream_transition_digest,
    validate_stream_transition_link,
)
from wealth.domain.market import InstrumentType

STREAM_ID = UUID("00000000-0000-4000-8000-000000000062")
OTHER_STREAM_ID = UUID("00000000-0000-4000-8000-000000000063")
CHILD_ID = UUID("00000000-0000-4000-8000-000000000059")
POLICY_FINGERPRINT = "sha256:" + ("1" * 64)
CHILD_POLICY_FINGERPRINT = "sha256:" + ("2" * 64)
EVIDENCE_DIGEST = "sha256:" + ("3" * 64)
OTHER_DIGEST = "sha256:" + ("4" * 64)
COMMAND_TIME = datetime(2026, 7, 28, 8, 30, 0, 123456, tzinfo=UTC)


class IntSubclass(int):
    """Hostile integer subclass."""


class StringSubclass(str):
    """Hostile string subclass."""


class BytesSubclass(bytes):
    """Hostile bytes subclass."""


def policy(**updates: object) -> ContinuousPublicTradePolicy:
    values: dict[str, object] = {
        "window_size_ms": 1_000,
        "settlement_lag_ms": 250,
        "max_catchup_span_ms": 5_000,
        "max_jobs_per_invocation": 3,
        "max_requests_per_job": 100,
        "max_records_per_job": 10_000,
        "policy_fingerprint": POLICY_FINGERPRINT,
    }
    values.update(updates)
    return ContinuousPublicTradePolicy.model_validate(values)


def checkpoint(**updates: object) -> ContinuousPublicTradeStreamCheckpoint:
    values: dict[str, object] = {
        "stream_id": STREAM_ID,
        "source": "binance.public-rest",
        "venue": "BINANCE",
        "instrument": "BTC-USDT",
        "provider_symbol": "BTCUSDT",
        "instrument_type": InstrumentType.SPOT,
        "request_variant": "aggregate-trades",
        "policy_fingerprint": POLICY_FINGERPRINT,
        "stream_start_epoch_ms": 0,
        "cursor_epoch_ms": 0,
        "status": ContinuousPublicTradeStreamStatus.ACTIVE,
        "version": 1,
    }
    values.update(updates)
    return ContinuousPublicTradeStreamCheckpoint.model_validate(values)


def copy_checkpoint(
    current: ContinuousPublicTradeStreamCheckpoint,
    **updates: object,
) -> ContinuousPublicTradeStreamCheckpoint:
    values = current.model_dump()
    values.update(updates)
    return ContinuousPublicTradeStreamCheckpoint.model_validate(values)


def identity_from(
    current: ContinuousPublicTradeStreamCheckpoint,
    **updates: object,
) -> store_contract.ContinuousPublicTradeStreamIdentityV1:
    values: dict[str, object] = {
        "stream_id": current.stream_id,
        "source": current.source,
        "venue": current.venue,
        "instrument": current.instrument,
        "provider_symbol": current.provider_symbol,
        "instrument_type": current.instrument_type,
        "request_variant": current.request_variant,
        "policy_fingerprint": current.policy_fingerprint,
        "stream_start_epoch_ms": current.stream_start_epoch_ms,
    }
    values.update(updates)
    return store_contract.ContinuousPublicTradeStreamIdentityV1.model_validate(values)


def expectation_for(
    current: ContinuousPublicTradeStreamCheckpoint,
    *,
    child_policy_fingerprint: str | None = None,
    effective_policy: ContinuousPublicTradePolicy | None = None,
) -> store_contract.ContinuousPublicTradeStreamExpectationV1:
    return store_contract.ContinuousPublicTradeStreamExpectationV1(
        identity=identity_from(current),
        effective_stream_policy=effective_policy or policy(),
        effective_child_policy_fingerprint=child_policy_fingerprint,
    )


def evidence_reference(
    kind: ContinuousPublicTradeEvidenceKind,
    scope_digest: str,
    *,
    recorded_at: datetime,
    suffix: str,
) -> ContinuousPublicTradeEvidenceReferenceV1:
    return ContinuousPublicTradeEvidenceReferenceV1(
        evidence_kind=kind,
        evidence_id=f"{kind.value.lower()}-{suffix}",
        evidence_digest=EVIDENCE_DIGEST,
        scope_digest=scope_digest,
        outcome=(
            ContinuousPublicTradeEvidenceOutcome.ACCEPTED
            if kind is ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
            else ContinuousPublicTradeEvidenceOutcome.APPROVED
        ),
        valid_from=recorded_at - timedelta(minutes=1),
        expires_at=recorded_at + timedelta(minutes=1),
    )


def stored_envelope(
    envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> store_contract.ContinuousPublicTradeStreamStoredEnvelopeV1:
    return store_contract.ContinuousPublicTradeStreamStoredEnvelopeV1(
        envelope=envelope,
        canonical_bytes=encode_stream_envelope(envelope),
        envelope_digest=stream_envelope_digest(envelope),
    )


def creation_entry(
    *,
    initial_checkpoint: ContinuousPublicTradeStreamCheckpoint | None = None,
    recorded_at: datetime = COMMAND_TIME,
) -> store_contract.ContinuousPublicTradeStreamStoredCreationV1:
    initial = initial_checkpoint or checkpoint()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(checkpoint=initial)
    envelope_digest = stream_envelope_digest(envelope)
    stream_policy = project_continuous_public_trade_policy(policy())
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=initial.stream_id,
        transition_kind=None,
        prior_version=None,
        prior_envelope_digest=None,
        prior_history_root=None,
        successor_version=1,
        successor_envelope_digest=envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=stream_policy,
    )
    record = ContinuousPublicTradeStreamCreationRecordV1(
        stream_id=initial.stream_id,
        source=initial.source,
        venue=initial.venue,
        instrument=initial.instrument,
        provider_symbol=initial.provider_symbol,
        instrument_type=initial.instrument_type,
        request_variant=initial.request_variant,
        stream_start_epoch_ms=initial.stream_start_epoch_ms,
        stream_policy=stream_policy,
        successor_envelope_hex=encode_stream_envelope(envelope).hex(),
        successor_envelope_digest=envelope_digest,
        create_authority_reference=evidence_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
            evidence_scope_digest(scope),
            recorded_at=recorded_at,
            suffix="create",
        ),
        recorded_at=recorded_at,
    )
    return store_contract.ContinuousPublicTradeStreamStoredCreationV1(
        record=record,
        canonical_bytes=encode_stream_creation_record(record),
        record_digest=stream_creation_digest(record),
        successor_envelope=stored_envelope(envelope),
        history_root=initial_stream_history_root(record),
        create_authority_scope=scope,
    )


def _next_successor(
    prior: ContinuousPublicTradeStreamEnvelopeV1,
    kind: ContinuousPublicTradeTransitionKind,
    *,
    recorded_at: datetime,
    reason_code: str | None,
    child_id: UUID,
) -> ContinuousPublicTradeStreamEnvelopeV1:
    current = prior.checkpoint
    next_version = current.version + 1
    if kind is ContinuousPublicTradeTransitionKind.RETAIN:
        return ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(current, version=next_version),
            child_creation_payload=prior.child_creation_payload,
        )
    if kind is ContinuousPublicTradeTransitionKind.ATTACH:
        plan, payload = finalize_continuous_public_trade_attachment(
            current,
            policy(),
            candidate_job_id=child_id,
            child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
            now=recorded_at,
        )
        assert plan.attachment is not None and payload is not None
        return ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                current,
                attachment=plan.attachment,
                version=next_version,
            ),
            child_creation_payload=payload,
        )
    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        attachment = current.attachment
        assert attachment is not None and prior.child_creation_payload is not None
        return ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                current,
                cursor_epoch_ms=attachment.window_end_epoch_ms,
                attachment=None,
                version=next_version,
            )
        )
    if kind is ContinuousPublicTradeTransitionKind.MANUAL_HOLD:
        assert reason_code is not None
        return ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                current,
                status=ContinuousPublicTradeStreamStatus.PAUSED,
                pause_reason=reason_code,
                version=next_version,
            ),
            child_creation_payload=prior.child_creation_payload,
        )
    assert kind is ContinuousPublicTradeTransitionKind.MANUAL_RESUME
    return ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            current,
            status=ContinuousPublicTradeStreamStatus.ACTIVE,
            pause_reason=None,
            version=next_version,
        ),
        child_creation_payload=prior.child_creation_payload,
    )


def transition_entry(
    prior: store_contract.ContinuousPublicTradeStreamStoredEnvelopeV1,
    *,
    prior_history_root: str,
    kind: ContinuousPublicTradeTransitionKind,
    recorded_at: datetime,
    prior_recorded_at: datetime,
    reason_code: str | None = None,
    child_id: UUID = CHILD_ID,
) -> store_contract.ContinuousPublicTradeStreamStoredTransitionV1:
    successor_value = _next_successor(
        prior.envelope,
        kind,
        recorded_at=recorded_at,
        reason_code=reason_code,
        child_id=child_id,
    )
    successor = stored_envelope(successor_value)
    current = prior.envelope.checkpoint
    successor_checkpoint = successor.envelope.checkpoint
    successor_attachment = successor_checkpoint.attachment
    successor_payload = successor.envelope.child_creation_payload
    authority_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=current.stream_id,
        transition_kind=kind,
        prior_version=current.version,
        prior_envelope_digest=prior.envelope_digest,
        prior_history_root=prior_history_root,
        successor_version=successor_checkpoint.version,
        successor_envelope_digest=(
            None
            if kind is ContinuousPublicTradeTransitionKind.ATTACH
            else successor.envelope_digest
        ),
        child_job_id=(
            successor_attachment.job_id
            if kind is ContinuousPublicTradeTransitionKind.ATTACH
            and successor_attachment is not None
            else None
        ),
        child_policy_fingerprint=(
            successor_payload.child_checkpoint.policy_fingerprint
            if kind is ContinuousPublicTradeTransitionKind.ATTACH and successor_payload is not None
            else None
        ),
        child_creation_fingerprint=None,
        reason_code=reason_code,
        stream_policy=None,
    )
    completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None
    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        prior_attachment = current.attachment
        prior_payload = prior.envelope.child_creation_payload
        assert prior_attachment is not None and prior_payload is not None
        completion_scope = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
            stream_id=current.stream_id,
            transition_kind=kind,
            prior_version=current.version,
            prior_envelope_digest=prior.envelope_digest,
            prior_history_root=prior_history_root,
            successor_version=successor_checkpoint.version,
            successor_envelope_digest=successor.envelope_digest,
            child_job_id=prior_attachment.job_id,
            child_policy_fingerprint=prior_payload.child_checkpoint.policy_fingerprint,
            child_creation_fingerprint=prior_attachment.creation_fingerprint,
            reason_code=None,
            stream_policy=None,
        )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=current.stream_id,
        prior_version=current.version,
        successor_version=successor_checkpoint.version,
        transition_kind=kind,
        prior_history_root=prior_history_root,
        prior_envelope_digest=prior.envelope_digest,
        successor_envelope_hex=successor.canonical_bytes.hex(),
        successor_envelope_digest=successor.envelope_digest,
        reason_code=reason_code,
        transition_authority_reference=evidence_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(authority_scope),
            recorded_at=recorded_at,
            suffix=f"transition-{successor_checkpoint.version}",
        ),
        child_completion_reference=(
            None
            if completion_scope is None
            else evidence_reference(
                ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
                evidence_scope_digest(completion_scope),
                recorded_at=recorded_at,
                suffix=f"completion-{successor_checkpoint.version}",
            )
        ),
        recorded_at=recorded_at,
    )
    result = store_contract.ContinuousPublicTradeStreamStoredTransitionV1(
        record=record,
        canonical_bytes=encode_stream_transition_record(record),
        record_digest=stream_transition_digest(record),
        successor_envelope=successor,
        history_root=next_stream_history_root(prior_history_root, record),
        transition_authority_scope=authority_scope,
        child_completion_scope=completion_scope,
    )
    validate_stream_transition_link(
        prior.envelope,
        record,
        policy=policy(),
        prior_history_root=prior_history_root,
        prior_recorded_at=prior_recorded_at,
        transition_authority_scope=authority_scope,
        child_completion_scope=completion_scope,
    )
    return result


def unchecked_retain_transition(
    prior: store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
    *,
    declared_prior_version: int | None = None,
    cursor_epoch_ms: int | None = None,
    suffix: str,
) -> store_contract.ContinuousPublicTradeStreamStoredTransitionV1:
    """Build a canonically self-consistent record without validating its predecessor lifecycle."""

    prior_envelope = prior.successor_envelope
    prior_checkpoint = prior_envelope.envelope.checkpoint
    prior_version = (
        prior.record.successor_version if declared_prior_version is None else declared_prior_version
    )
    checkpoint_updates: dict[str, object] = {"version": prior_version + 1}
    if cursor_epoch_ms is not None:
        checkpoint_updates["cursor_epoch_ms"] = cursor_epoch_ms
    successor_value = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(prior_checkpoint, **checkpoint_updates),
        child_creation_payload=prior_envelope.envelope.child_creation_payload,
    )
    successor = stored_envelope(successor_value)
    recorded_at = prior.record.recorded_at + timedelta(microseconds=1)
    reason_code = f"unchecked-retain-{suffix}"
    authority_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=prior_checkpoint.stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_version=prior_version,
        prior_envelope_digest=prior_envelope.envelope_digest,
        prior_history_root=prior.history_root,
        successor_version=successor_value.checkpoint.version,
        successor_envelope_digest=successor.envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=reason_code,
        stream_policy=None,
    )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=prior_checkpoint.stream_id,
        prior_version=prior_version,
        successor_version=successor_value.checkpoint.version,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_history_root=prior.history_root,
        prior_envelope_digest=prior_envelope.envelope_digest,
        successor_envelope_hex=successor.canonical_bytes.hex(),
        successor_envelope_digest=successor.envelope_digest,
        reason_code=reason_code,
        transition_authority_reference=evidence_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(authority_scope),
            recorded_at=recorded_at,
            suffix=suffix,
        ),
        recorded_at=recorded_at,
    )
    return store_contract.ContinuousPublicTradeStreamStoredTransitionV1(
        record=record,
        canonical_bytes=encode_stream_transition_record(record),
        record_digest=stream_transition_digest(record),
        successor_envelope=successor,
        history_root=next_stream_history_root(prior.history_root, record),
        transition_authority_scope=authority_scope,
    )


def retain_history(
    length: int,
) -> tuple[
    tuple[store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1, ...],
    dict[int, store_contract.ContinuousPublicTradeStreamStoredEnvelopeV1],
]:
    assert length >= 1
    creation = creation_entry()
    records: list[store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1] = [creation]
    envelopes = {1: creation.successor_envelope}
    prior = creation.successor_envelope
    prior_root = creation.history_root
    prior_time = creation.record.recorded_at
    for version in range(2, length + 1):
        recorded_at = COMMAND_TIME + timedelta(microseconds=version - 1)
        transition = transition_entry(
            prior,
            prior_history_root=prior_root,
            kind=ContinuousPublicTradeTransitionKind.RETAIN,
            reason_code=f"retain-{version}",
            recorded_at=recorded_at,
            prior_recorded_at=prior_time,
        )
        records.append(transition)
        prior = transition.successor_envelope
        envelopes[version] = prior
        prior_root = transition.history_root
        prior_time = recorded_at
    return tuple(records), envelopes


def natural_identity(
    identity: store_contract.ContinuousPublicTradeStreamIdentityV1,
) -> tuple[str, str, str, str, InstrumentType, str]:
    return (
        identity.source,
        identity.venue,
        identity.instrument,
        identity.provider_symbol,
        identity.instrument_type,
        identity.request_variant,
    )


def continuation_for(
    entry: store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
) -> store_contract.ContinuousPublicTradeStreamAuditContinuationV1:
    return store_contract.ContinuousPublicTradeStreamAuditContinuationV1(
        stream_id=entry.record.stream_id,
        through_version=entry.record.successor_version,
        through_envelope_digest=entry.successor_envelope.envelope_digest,
        through_history_root=entry.history_root,
    )


@dataclass(slots=True)
class _MemoryStream:
    expectation: store_contract.ContinuousPublicTradeStreamExpectationV1
    history: list[store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1]


@dataclass(slots=True)
class PureStreamStoreSpy:
    """Test-only deterministic logical store; it performs no I/O or clock work."""

    streams: dict[UUID, _MemoryStream] = field(default_factory=dict)
    natural_ids: dict[tuple[str, str, str, str, InstrumentType, str], UUID] = field(
        default_factory=dict
    )
    calls: list[tuple[str, object]] = field(default_factory=list)
    write_count: int = 0
    history_reads: list[int] = field(default_factory=list)
    forced_fault: str | None = None

    @staticmethod
    def _create_receipt(
        creation: store_contract.ContinuousPublicTradeStreamStoredCreationV1,
    ) -> store_contract.ContinuousPublicTradeStreamCreateReceiptV1:
        return store_contract.ContinuousPublicTradeStreamCreateReceiptV1(creation=creation)

    @staticmethod
    def _transition_receipt(
        transition: store_contract.ContinuousPublicTradeStreamStoredTransitionV1,
    ) -> store_contract.ContinuousPublicTradeStreamCompareAndSwapReceiptV1:
        return store_contract.ContinuousPublicTradeStreamCompareAndSwapReceiptV1(
            transition=transition
        )

    def _validated_retained_stream(
        self,
        stream_id: UUID,
        retained: _MemoryStream,
    ) -> tuple[
        store_contract.ContinuousPublicTradeStreamCurrentViewV1,
        store_contract.ContinuousPublicTradeStreamExpectationV1,
    ]:
        if type(retained.history) is not list or not retained.history:
            raise ValueError("retained history must be one nonempty exact list")
        if (
            type(retained.history[0])
            is not store_contract.ContinuousPublicTradeStreamStoredCreationV1
        ):
            raise ValueError("retained history must begin with exact creation")

        retained_create = store_contract.ContinuousPublicTradeStreamCreateCommandV1(
            expectation=retained.expectation,
            creation=retained.history[0],
        )
        creation = retained_create.creation
        current = retained.history[-1]
        predecessor = None if len(retained.history) == 1 else retained.history[-2]
        view = store_contract.ContinuousPublicTradeStreamCurrentViewV1(
            creation=creation,
            current=current,
            predecessor=predecessor,
        )
        if (
            creation.record.stream_id != stream_id
            or view.current.record.successor_version != len(retained.history)
            or self.natural_ids.get(natural_identity(retained_create.expectation.identity))
            != stream_id
        ):
            raise ValueError("retained current, history, or natural identity is incoherent")
        return view, retained_create.expectation

    @staticmethod
    def _base_expectation_matches(
        expected: store_contract.ContinuousPublicTradeStreamExpectationV1,
        retained_expectation: store_contract.ContinuousPublicTradeStreamExpectationV1,
        creation: store_contract.ContinuousPublicTradeStreamStoredCreationV1,
    ) -> bool:
        return (
            expected.identity == retained_expectation.identity
            and project_continuous_public_trade_policy(expected.effective_stream_policy)
            == creation.record.stream_policy
        )

    @classmethod
    def _current_expectation_matches(
        cls,
        expected: store_contract.ContinuousPublicTradeStreamExpectationV1,
        retained_expectation: store_contract.ContinuousPublicTradeStreamExpectationV1,
        view: store_contract.ContinuousPublicTradeStreamCurrentViewV1,
    ) -> bool:
        current = view.current.successor_envelope.envelope
        payload = current.child_creation_payload
        actual_child = None if payload is None else payload.child_checkpoint.policy_fingerprint
        return (
            cls._base_expectation_matches(
                expected,
                retained_expectation,
                view.creation,
            )
            and expected.effective_child_policy_fingerprint == actual_child
        )

    @classmethod
    def _cas_expectation_matches(
        cls,
        expected: store_contract.ContinuousPublicTradeStreamExpectationV1,
        retained_expectation: store_contract.ContinuousPublicTradeStreamExpectationV1,
        view: store_contract.ContinuousPublicTradeStreamCurrentViewV1,
        transition: store_contract.ContinuousPublicTradeStreamStoredTransitionV1,
    ) -> bool:
        current_payload = view.current.successor_envelope.envelope.child_creation_payload
        applicable_child = (
            None if current_payload is None else current_payload.child_checkpoint.policy_fingerprint
        )
        if (
            applicable_child is None
            and transition.record.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH
        ):
            proposed_payload = transition.successor_envelope.envelope.child_creation_payload
            applicable_child = (
                None
                if proposed_payload is None
                else proposed_payload.child_checkpoint.policy_fingerprint
            )
        return (
            cls._base_expectation_matches(
                expected,
                retained_expectation,
                view.creation,
            )
            and expected.effective_child_policy_fingerprint == applicable_child
        )

    @staticmethod
    def _revalidate_history_entry(
        entry: store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
    ) -> store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1:
        if type(entry) is store_contract.ContinuousPublicTradeStreamStoredCreationV1:
            return (
                store_contract.ContinuousPublicTradeStreamStoredCreationV1.revalidate_at_boundary(
                    entry
                )
            )
        if type(entry) is store_contract.ContinuousPublicTradeStreamStoredTransitionV1:
            return (
                store_contract.ContinuousPublicTradeStreamStoredTransitionV1.revalidate_at_boundary(
                    entry
                )
            )
        raise ValueError("retained history entry has an unexpected type")

    @classmethod
    def _validate_historical_transition(
        cls,
        retained: _MemoryStream,
        index: int,
        transition: store_contract.ContinuousPublicTradeStreamStoredTransitionV1,
        *,
        policy_value: ContinuousPublicTradePolicy,
    ) -> store_contract.ContinuousPublicTradeStreamStoredTransitionV1:
        if index < 1:
            raise ValueError("historical transition has no predecessor")
        predecessor = cls._revalidate_history_entry(retained.history[index - 1])
        exact_transition = (
            store_contract.ContinuousPublicTradeStreamStoredTransitionV1.revalidate_at_boundary(
                transition
            )
        )
        if (
            predecessor.record.successor_version != index
            or exact_transition.record.successor_version != index + 1
        ):
            raise ValueError("retained history position and version disagree")
        validate_stream_transition_link(
            predecessor.successor_envelope.envelope,
            exact_transition.record,
            policy=policy_value,
            prior_history_root=predecessor.history_root,
            prior_recorded_at=cls._entry_recorded_at(predecessor),
            transition_authority_scope=exact_transition.transition_authority_scope,
            child_completion_scope=exact_transition.child_completion_scope,
        )
        return exact_transition

    @staticmethod
    def _entry_recorded_at(
        entry: store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
    ) -> datetime:
        return entry.record.recorded_at

    @staticmethod
    def _create_corrupt(
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1:
        return store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
            stream_id=stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
        )

    @staticmethod
    def _load_corrupt(
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1:
        return store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
            stream_id=stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
        )

    @staticmethod
    def _cas_corrupt(
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1:
        return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
            stream_id=stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
        )

    @staticmethod
    def _audit_corrupt(
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1:
        return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
            stream_id=stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
        )

    def _create_fault(
        self,
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamCreateResultV1 | None:
        if self.forced_fault == "unavailable":
            return store_contract.ContinuousPublicTradeStreamCreateUnavailableResultV1(
                stream_id=stream_id
            )
        if self.forced_fault == "unsupported":
            return store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.UNSUPPORTED_VERSION,
            )
        if self.forced_fault == "corrupt":
            return store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
            )
        return None

    def _load_fault(
        self,
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamLoadResultV1 | None:
        if self.forced_fault == "unavailable":
            return store_contract.ContinuousPublicTradeStreamLoadUnavailableResultV1(
                stream_id=stream_id
            )
        if self.forced_fault == "unsupported":
            return store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION,
            )
        if self.forced_fault == "corrupt":
            return store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
            )
        return None

    def _cas_fault(
        self,
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamCompareAndSwapResultV1 | None:
        if self.forced_fault == "unavailable":
            return store_contract.ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1(
                stream_id=stream_id
            )
        if self.forced_fault == "unsupported":
            return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UNSUPPORTED_VERSION,
            )
        if self.forced_fault == "corrupt":
            return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
            )
        return None

    def _audit_fault(
        self,
        stream_id: UUID,
    ) -> store_contract.ContinuousPublicTradeStreamAuditResultV1 | None:
        if self.forced_fault == "unavailable":
            return store_contract.ContinuousPublicTradeStreamAuditUnavailableResultV1(
                stream_id=stream_id
            )
        if self.forced_fault == "unsupported":
            return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.UNSUPPORTED_VERSION,
            )
        if self.forced_fault == "corrupt":
            return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
            )
        return None

    def create(
        self,
        command: store_contract.ContinuousPublicTradeStreamCreateCommandV1,
        /,
    ) -> store_contract.ContinuousPublicTradeStreamCreateResultV1:
        command = store_contract.ContinuousPublicTradeStreamCreateCommandV1.revalidate_at_boundary(
            command
        )
        self.calls.append(("create", command))
        identity = command.expectation.identity
        fault = self._create_fault(identity.stream_id)
        if fault is not None:
            return fault

        retained = self.streams.get(identity.stream_id)
        retained_natural_stream = self.natural_ids.get(natural_identity(identity))
        if retained is not None:
            try:
                view, _ = self._validated_retained_stream(identity.stream_id, retained)
                existing = view.creation
                if existing == command.creation:
                    return store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1(
                        stream_id=identity.stream_id,
                        outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.DUPLICATE,
                        receipt=self._create_receipt(existing),
                    )
                return store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
                    stream_id=identity.stream_id,
                    outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                AttributeError,
                OverflowError,
                IndexError,
                RecursionError,
            ):
                return self._create_corrupt(identity.stream_id)
        if retained_natural_stream is not None:
            conflicting = self.streams.get(retained_natural_stream)
            if conflicting is None:
                return self._create_corrupt(identity.stream_id)
            try:
                self._validated_retained_stream(retained_natural_stream, conflicting)
            except (
                ValidationError,
                ValueError,
                TypeError,
                AttributeError,
                OverflowError,
                IndexError,
                RecursionError,
            ):
                return self._create_corrupt(identity.stream_id)
            return store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
            )

        self.streams[identity.stream_id] = _MemoryStream(
            expectation=command.expectation,
            history=[command.creation],
        )
        self.natural_ids[natural_identity(identity)] = identity.stream_id
        self.write_count += 1
        return store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1(
            stream_id=identity.stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED,
            receipt=self._create_receipt(command.creation),
        )

    def load_current(
        self,
        query: store_contract.ContinuousPublicTradeStreamLoadQueryV1,
        /,
    ) -> store_contract.ContinuousPublicTradeStreamLoadResultV1:
        query = store_contract.ContinuousPublicTradeStreamLoadQueryV1.revalidate_at_boundary(query)
        self.calls.append(("load_current", query))
        identity = query.expectation.identity
        fault = self._load_fault(identity.stream_id)
        if fault is not None:
            return fault

        retained = self.streams.get(identity.stream_id)
        if retained is None:
            conflicting_stream_id = self.natural_ids.get(natural_identity(identity))
            if conflicting_stream_id is not None:
                conflicting = self.streams.get(conflicting_stream_id)
                if conflicting is None:
                    return self._load_corrupt(identity.stream_id)
                try:
                    self._validated_retained_stream(conflicting_stream_id, conflicting)
                except (
                    ValidationError,
                    ValueError,
                    TypeError,
                    AttributeError,
                    OverflowError,
                    IndexError,
                    RecursionError,
                ):
                    return self._load_corrupt(identity.stream_id)
                return store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
                    stream_id=identity.stream_id,
                    outcome=store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT,
                )
            return store_contract.ContinuousPublicTradeStreamLoadNotFoundResultV1(
                stream_id=identity.stream_id
            )
        try:
            view, retained_expectation = self._validated_retained_stream(
                identity.stream_id,
                retained,
            )
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            IndexError,
            RecursionError,
        ):
            return self._load_corrupt(identity.stream_id)
        if not self._current_expectation_matches(
            query.expectation,
            retained_expectation,
            view,
        ):
            return store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT,
            )

        self.history_reads.append(2 if view.predecessor is None else 3)
        return store_contract.ContinuousPublicTradeStreamLoadFoundResultV1(
            stream_id=identity.stream_id,
            view=view,
        )

    def compare_and_swap(
        self,
        command: store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1,
        /,
    ) -> store_contract.ContinuousPublicTradeStreamCompareAndSwapResultV1:
        command = store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1.revalidate_at_boundary(
            command
        )
        self.calls.append(("compare_and_swap", command))
        identity = command.expectation.identity
        fault = self._cas_fault(identity.stream_id)
        if fault is not None:
            return fault

        retained = self.streams.get(identity.stream_id)
        if retained is None:
            conflicting_stream_id = self.natural_ids.get(natural_identity(identity))
            if conflicting_stream_id is not None:
                conflicting = self.streams.get(conflicting_stream_id)
                if conflicting is None:
                    return self._cas_corrupt(identity.stream_id)
                try:
                    self._validated_retained_stream(conflicting_stream_id, conflicting)
                except (
                    ValidationError,
                    ValueError,
                    TypeError,
                    AttributeError,
                    OverflowError,
                    IndexError,
                    RecursionError,
                ):
                    return self._cas_corrupt(identity.stream_id)
            return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT,
            )
        try:
            view, retained_expectation = self._validated_retained_stream(
                identity.stream_id,
                retained,
            )
            for index, historical in enumerate(retained.history[1:], start=1):
                exact_historical = self._revalidate_history_entry(historical)
                if (
                    type(exact_historical)
                    is not store_contract.ContinuousPublicTradeStreamStoredTransitionV1
                ):
                    raise ValueError("retained post-creation history must contain transitions")
                if exact_historical == command.transition:
                    exact_historical = self._validate_historical_transition(
                        retained,
                        index,
                        exact_historical,
                        policy_value=retained_expectation.effective_stream_policy,
                    )
                    if not self._base_expectation_matches(
                        command.expectation,
                        retained_expectation,
                        view.creation,
                    ):
                        return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                            stream_id=identity.stream_id,
                            outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT,
                        )
                    return store_contract.ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(
                        stream_id=identity.stream_id,
                        outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE,
                        receipt=self._transition_receipt(exact_historical),
                    )
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            IndexError,
            RecursionError,
        ):
            return self._cas_corrupt(identity.stream_id)

        current = view.current
        if (
            not self._cas_expectation_matches(
                command.expectation,
                retained_expectation,
                view,
                command.transition,
            )
            or current.record.successor_version != command.expected_version
            or current.successor_envelope.envelope_digest != command.expected_envelope_digest
            or current.history_root != command.expected_history_root
        ):
            return store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT,
            )

        validate_stream_transition_link(
            current.successor_envelope.envelope,
            command.transition.record,
            policy=command.expectation.effective_stream_policy,
            prior_history_root=current.history_root,
            prior_recorded_at=self._entry_recorded_at(current),
            transition_authority_scope=command.transition.transition_authority_scope,
            child_completion_scope=command.transition.child_completion_scope,
        )
        retained.history.append(command.transition)
        self.write_count += 1
        return store_contract.ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(
            stream_id=identity.stream_id,
            outcome=store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED,
            receipt=self._transition_receipt(command.transition),
        )

    def audit_page(
        self,
        query: store_contract.ContinuousPublicTradeStreamAuditQueryV1,
        /,
    ) -> store_contract.ContinuousPublicTradeStreamAuditResultV1:
        exact_query: store_contract.ContinuousPublicTradeStreamAuditQueryV1
        if type(query) is store_contract.ContinuousPublicTradeStreamAuditStartQueryV1:
            exact_query = (
                store_contract.ContinuousPublicTradeStreamAuditStartQueryV1.revalidate_at_boundary(
                    query
                )
            )
        elif type(query) is store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1:
            exact_query = store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1.revalidate_at_boundary(
                query
            )
        else:
            raise store_contract.ContinuousPublicTradeStreamStoreContractError(
                store_contract.ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE
            )
        self.calls.append(("audit_page", exact_query))
        identity = exact_query.expectation.identity
        fault = self._audit_fault(identity.stream_id)
        if fault is not None:
            return fault

        retained = self.streams.get(identity.stream_id)
        if retained is None:
            conflicting_stream_id = self.natural_ids.get(natural_identity(identity))
            if conflicting_stream_id is not None:
                conflicting = self.streams.get(conflicting_stream_id)
                if conflicting is None:
                    return self._audit_corrupt(identity.stream_id)
                try:
                    self._validated_retained_stream(conflicting_stream_id, conflicting)
                except (
                    ValidationError,
                    ValueError,
                    TypeError,
                    AttributeError,
                    OverflowError,
                    IndexError,
                    RecursionError,
                ):
                    return self._audit_corrupt(identity.stream_id)
                return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=identity.stream_id,
                    outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.IDENTITY_CONFLICT,
                )
            return store_contract.ContinuousPublicTradeStreamAuditNotFoundResultV1(
                stream_id=identity.stream_id
            )
        try:
            view, retained_expectation = self._validated_retained_stream(
                identity.stream_id,
                retained,
            )
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            IndexError,
            RecursionError,
        ):
            return self._audit_corrupt(identity.stream_id)
        if not self._current_expectation_matches(
            exact_query.expectation,
            retained_expectation,
            view,
        ):
            return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.IDENTITY_CONFLICT,
            )

        if type(exact_query) is store_contract.ContinuousPublicTradeStreamAuditStartQueryV1:
            try:
                new_records = tuple(retained.history[: exact_query.limit])
                page = store_contract.ContinuousPublicTradeStreamAuditPageV1(
                    predecessor_overlap=None,
                    records=new_records,
                    continuation=continuation_for(new_records[-1]),
                )
                store_contract.validate_continuous_public_trade_stream_audit_page(
                    exact_query,
                    page,
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                AttributeError,
                OverflowError,
                IndexError,
                RecursionError,
            ):
                return self._audit_corrupt(identity.stream_id)
            self.history_reads.append(len(new_records))
            return store_contract.ContinuousPublicTradeStreamAuditPageResultV1(
                stream_id=identity.stream_id,
                page=page,
            )

        assert isinstance(
            exact_query,
            store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1,
        )
        continuation = exact_query.continuation
        index = continuation.through_version - 1
        tail_version = view.current.record.successor_version
        if continuation.through_version > tail_version:
            return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT,
            )
        if index < 0 or index >= len(retained.history):
            return self._audit_corrupt(identity.stream_id)
        try:
            predecessor = self._revalidate_history_entry(retained.history[index])
            retained_continuation = continuation_for(predecessor)
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            IndexError,
            RecursionError,
        ):
            return self._audit_corrupt(identity.stream_id)
        if retained_continuation != continuation:
            return store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=identity.stream_id,
                outcome=store_contract.ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT,
            )
        if index == len(retained.history) - 1:
            self.history_reads.append(1)
            return store_contract.ContinuousPublicTradeStreamAuditAtTailResultV1(
                stream_id=identity.stream_id,
                continuation=continuation,
            )

        try:
            new_records = tuple(retained.history[index + 1 : index + 1 + exact_query.limit])
            page = store_contract.ContinuousPublicTradeStreamAuditPageV1(
                predecessor_overlap=predecessor,
                records=new_records,
                continuation=continuation_for(new_records[-1]),
            )
            store_contract.validate_continuous_public_trade_stream_audit_page(
                exact_query,
                page,
            )
        except (
            ValidationError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
            IndexError,
            RecursionError,
        ):
            return self._audit_corrupt(identity.stream_id)
        self.history_reads.append(1 + len(new_records))
        return store_contract.ContinuousPublicTradeStreamAuditPageResultV1(
            stream_id=identity.stream_id,
            page=page,
        )


def create_command(
    creation: store_contract.ContinuousPublicTradeStreamStoredCreationV1 | None = None,
) -> store_contract.ContinuousPublicTradeStreamCreateCommandV1:
    stored = creation or creation_entry()
    return store_contract.ContinuousPublicTradeStreamCreateCommandV1(
        expectation=expectation_for(stored.successor_envelope.envelope.checkpoint),
        creation=stored,
    )


def load_query(
    current: ContinuousPublicTradeStreamCheckpoint | None = None,
    *,
    child_policy_fingerprint: str | None = None,
    effective_policy: ContinuousPublicTradePolicy | None = None,
) -> store_contract.ContinuousPublicTradeStreamLoadQueryV1:
    value = current or checkpoint()
    return store_contract.ContinuousPublicTradeStreamLoadQueryV1(
        expectation=expectation_for(
            value,
            child_policy_fingerprint=child_policy_fingerprint,
            effective_policy=effective_policy,
        )
    )


def compare_and_swap_command(
    prior: store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
    transition: store_contract.ContinuousPublicTradeStreamStoredTransitionV1,
    *,
    child_policy_fingerprint: str | None = None,
    effective_policy: ContinuousPublicTradePolicy | None = None,
) -> store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1:
    return store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1(
        expectation=expectation_for(
            prior.successor_envelope.envelope.checkpoint,
            child_policy_fingerprint=child_policy_fingerprint,
            effective_policy=effective_policy,
        ),
        expected_version=prior.record.successor_version,
        expected_envelope_digest=prior.successor_envelope.envelope_digest,
        expected_history_root=prior.history_root,
        transition=transition,
    )


def audit_start_query(
    current: ContinuousPublicTradeStreamCheckpoint | None = None,
    *,
    limit: int = 100,
    child_policy_fingerprint: str | None = None,
    effective_policy: ContinuousPublicTradePolicy | None = None,
) -> store_contract.ContinuousPublicTradeStreamAuditStartQueryV1:
    value = current or checkpoint()
    return store_contract.ContinuousPublicTradeStreamAuditStartQueryV1(
        expectation=expectation_for(
            value,
            child_policy_fingerprint=child_policy_fingerprint,
            effective_policy=effective_policy,
        ),
        limit=limit,
    )


def audit_continuation_query(
    current: ContinuousPublicTradeStreamCheckpoint,
    continuation: store_contract.ContinuousPublicTradeStreamAuditContinuationV1,
    *,
    limit: int = 100,
    child_policy_fingerprint: str | None = None,
) -> store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1:
    return store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1(
        expectation=expectation_for(
            current,
            child_policy_fingerprint=child_policy_fingerprint,
        ),
        continuation=continuation,
        limit=limit,
    )


def seeded_spy(
    history: tuple[
        store_contract.ContinuousPublicTradeStreamStoredHistoryEntryV1,
        ...,
    ],
) -> PureStreamStoreSpy:
    creation = cast(
        store_contract.ContinuousPublicTradeStreamStoredCreationV1,
        history[0],
    )
    initial_checkpoint = creation.successor_envelope.envelope.checkpoint
    expectation = expectation_for(initial_checkpoint)
    spy = PureStreamStoreSpy()
    spy.streams[initial_checkpoint.stream_id] = _MemoryStream(
        expectation=expectation,
        history=list(history),
    )
    spy.natural_ids[natural_identity(expectation.identity)] = initial_checkpoint.stream_id
    return spy


def test_protocol_has_only_four_exact_positional_boundaries() -> None:
    expected = {
        "create": (
            "command",
            store_contract.ContinuousPublicTradeStreamCreateCommandV1,
            store_contract.ContinuousPublicTradeStreamCreateResultV1,
        ),
        "load_current": (
            "query",
            store_contract.ContinuousPublicTradeStreamLoadQueryV1,
            store_contract.ContinuousPublicTradeStreamLoadResultV1,
        ),
        "compare_and_swap": (
            "command",
            store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1,
            store_contract.ContinuousPublicTradeStreamCompareAndSwapResultV1,
        ),
        "audit_page": (
            "query",
            store_contract.ContinuousPublicTradeStreamAuditQueryV1,
            store_contract.ContinuousPublicTradeStreamAuditResultV1,
        ),
    }
    public_methods = {
        name
        for name, value in vars(store_contract.ContinuousPublicTradeStreamStore).items()
        if not name.startswith("_") and inspect.isfunction(value)
    }
    assert public_methods == set(expected)

    for method_name, (argument_name, argument_type, return_type) in expected.items():
        method = getattr(store_contract.ContinuousPublicTradeStreamStore, method_name)
        signature = inspect.signature(method)
        parameters = tuple(signature.parameters.values())
        assert tuple(parameter.name for parameter in parameters) == ("self", argument_name)
        assert parameters[0].kind is inspect.Parameter.POSITIONAL_ONLY
        assert parameters[1].kind is inspect.Parameter.POSITIONAL_ONLY
        assert parameters[1].default is inspect.Parameter.empty
        hints = get_type_hints(method)
        assert hints[argument_name] == argument_type
        assert hints["return"] == return_type

    typed_spy: store_contract.ContinuousPublicTradeStreamStore = PureStreamStoreSpy()
    assert isinstance(typed_spy, PureStreamStoreSpy)


def test_commands_have_no_alternate_successor_clock_or_unbounded_surface() -> None:
    assert set(store_contract.ContinuousPublicTradeStreamCreateCommandV1.model_fields) == {
        "expectation",
        "creation",
    }
    assert set(store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1.model_fields) == {
        "expectation",
        "expected_version",
        "expected_envelope_digest",
        "expected_history_root",
        "transition",
    }
    assert set(store_contract.ContinuousPublicTradeStreamAuditStartQueryV1.model_fields) == {
        "expectation",
        "limit",
    }
    assert set(store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1.model_fields) == {
        "expectation",
        "continuation",
        "limit",
    }
    public_fields = {
        name
        for value in (
            store_contract.ContinuousPublicTradeStreamCreateCommandV1,
            store_contract.ContinuousPublicTradeStreamLoadQueryV1,
            store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1,
            store_contract.ContinuousPublicTradeStreamAuditStartQueryV1,
            store_contract.ContinuousPublicTradeStreamAuditContinuationQueryV1,
        )
        for name in value.model_fields
    }
    forbidden_fragments = {
        "clock",
        "now",
        "path",
        "database",
        "table",
        "timeout",
        "retry_after",
        "retention",
        "capacity",
        "successor_bytes",
        "successor_digest",
        "child_payload",
        "recorded_at",
    }
    assert not public_fields.intersection(forbidden_fragments)
    assert not {
        "get",
        "delete",
        "upsert",
        "repair",
        "replay",
        "all_history",
        "iter_history",
    }.intersection(vars(store_contract.ContinuousPublicTradeStreamStore))


def test_identity_expectation_and_values_are_strict_frozen_and_bounded() -> None:
    current = checkpoint()
    identity = identity_from(current)
    expectation = expectation_for(current)
    assert identity.stream_contract_version == 1
    assert expectation.identity is not identity
    assert expectation.identity == identity

    with pytest.raises(ValidationError):
        identity.stream_start_epoch_ms = 1
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.model_dump(),
                "stream_start_epoch_ms": True,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.model_dump(),
                "stream_start_epoch_ms": MAX_CONTRACT_INTEGER + 1,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.model_dump(),
                "source": " binance",
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.model_dump(),
                "instrument_type": InstrumentType.SPOT.value,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.model_dump(),
                "stream_id": str(STREAM_ID),
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamExpectationV1(
            identity=identity,
            effective_stream_policy=policy(policy_fingerprint=OTHER_DIGEST),
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamExpectationV1.model_validate(
            {
                "identity": identity,
                "effective_stream_policy": policy(),
                "undeclared": True,
            }
        )


def test_direct_pydantic_validation_error_does_not_echo_hostile_input() -> None:
    identity = identity_from(checkpoint())
    hostile = "SECRET-CALLER-MATERIAL-MUST-NOT-ECHO "
    with pytest.raises(ValidationError) as caught:
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.__dict__,
                "source": hostile,
            }
        )
    assert hostile not in str(caught.value)
    assert "input_value" not in str(caught.value)


def test_stored_wrappers_preserve_exact_task061_bytes_digests_roots_and_scopes() -> None:
    creation = creation_entry()
    transition = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    assert creation.canonical_bytes == encode_stream_creation_record(creation.record)
    assert creation.record_digest == stream_creation_digest(creation.record)
    assert creation.history_root == initial_stream_history_root(creation.record)
    assert creation.successor_envelope.canonical_bytes == bytes.fromhex(
        creation.record.successor_envelope_hex
    )
    assert transition.canonical_bytes == encode_stream_transition_record(transition.record)
    assert transition.record_digest == stream_transition_digest(transition.record)
    assert transition.history_root == next_stream_history_root(
        creation.history_root,
        transition.record,
    )
    assert transition.successor_envelope.canonical_bytes == bytes.fromhex(
        transition.record.successor_envelope_hex
    )

    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredEnvelopeV1(
            envelope=creation.successor_envelope.envelope,
            canonical_bytes=creation.successor_envelope.canonical_bytes + b" ",
            envelope_digest=creation.successor_envelope.envelope_digest,
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredCreationV1(
            **{
                **creation.__dict__,
                "record_digest": OTHER_DIGEST,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredCreationV1(
            **{
                **creation.__dict__,
                "history_root": OTHER_DIGEST,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredTransitionV1(
            **{
                **transition.__dict__,
                "canonical_bytes": transition.canonical_bytes + b" ",
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredTransitionV1(
            **{
                **transition.__dict__,
                "history_root": OTHER_DIGEST,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredTransitionV1(
            **{
                **transition.__dict__,
                "transition_authority_scope": transition.transition_authority_scope.model_copy(
                    update={"prior_history_root": OTHER_DIGEST}
                ),
            }
        )


def test_create_and_cas_commands_cross_bind_every_finalized_prior_value() -> None:
    creation = creation_entry()
    create = create_command(creation)
    transition = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    command = compare_and_swap_command(creation, transition)
    assert command.expected_version == transition.record.prior_version
    assert command.expected_envelope_digest == transition.record.prior_envelope_digest
    assert command.expected_history_root == transition.record.prior_history_root
    assert create.creation == creation

    for field_name, invalid in (
        ("expected_version", transition.record.prior_version + 1),
        ("expected_envelope_digest", OTHER_DIGEST),
        ("expected_history_root", OTHER_DIGEST),
    ):
        with pytest.raises(ValidationError):
            store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1(
                **{
                    **command.__dict__,
                    field_name: invalid,
                }
            )

    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateCommandV1(
            expectation=store_contract.ContinuousPublicTradeStreamExpectationV1(
                identity=create.expectation.identity,
                effective_stream_policy=policy(),
                effective_child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
            ),
            creation=creation,
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateCommandV1(
            expectation=expectation_for(
                creation.successor_envelope.envelope.checkpoint,
                effective_policy=policy(max_jobs_per_invocation=4),
            ),
            creation=creation,
        )


def test_result_variants_enforce_exact_payload_and_retry_matrices() -> None:
    creation = creation_entry()
    create_receipt = store_contract.ContinuousPublicTradeStreamCreateReceiptV1(creation=creation)
    transition = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    transition_receipt = store_contract.ContinuousPublicTradeStreamCompareAndSwapReceiptV1(
        transition=transition
    )
    view = store_contract.ContinuousPublicTradeStreamCurrentViewV1(
        creation=creation,
        current=creation,
    )
    page = store_contract.ContinuousPublicTradeStreamAuditPageV1(
        predecessor_overlap=None,
        records=(creation,),
        continuation=continuation_for(creation),
    )

    for create_accepted_outcome in (
        store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED,
        store_contract.ContinuousPublicTradeStreamCreateOutcome.DUPLICATE,
    ):
        accepted_result = store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1(
            stream_id=STREAM_ID,
            outcome=create_accepted_outcome,
            receipt=create_receipt,
        )
        assert (
            accepted_result.retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
        )
    for create_rejected_outcome in (
        store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
        store_contract.ContinuousPublicTradeStreamCreateOutcome.UNSUPPORTED_VERSION,
        store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
    ):
        rejected_result = store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1(
            stream_id=STREAM_ID,
            outcome=create_rejected_outcome,
        )
        assert (
            rejected_result.retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
        )
    assert (
        store_contract.ContinuousPublicTradeStreamCreateUnavailableResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    )

    assert (
        store_contract.ContinuousPublicTradeStreamLoadFoundResultV1(
            stream_id=STREAM_ID,
            view=view,
        ).outcome
        is store_contract.ContinuousPublicTradeStreamLoadOutcome.FOUND
    )
    assert (
        store_contract.ContinuousPublicTradeStreamLoadNotFoundResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    for load_rejected_outcome in (
        store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT,
        store_contract.ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION,
        store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
    ):
        assert (
            store_contract.ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=STREAM_ID,
                outcome=load_rejected_outcome,
            ).retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
        )
    assert (
        store_contract.ContinuousPublicTradeStreamLoadUnavailableResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    )

    for cas_accepted_outcome in (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED,
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE,
    ):
        assert (
            store_contract.ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(
                stream_id=STREAM_ID,
                outcome=cas_accepted_outcome,
                receipt=transition_receipt,
            ).retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
        )
    for cas_rejected_outcome in (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT,
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UNSUPPORTED_VERSION,
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
    ):
        assert (
            store_contract.ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                stream_id=STREAM_ID,
                outcome=cas_rejected_outcome,
            ).retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
        )
    assert (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    )

    assert (
        store_contract.ContinuousPublicTradeStreamAuditPageResultV1(
            stream_id=STREAM_ID,
            page=page,
        ).outcome
        is store_contract.ContinuousPublicTradeStreamAuditOutcome.PAGE
    )
    assert (
        store_contract.ContinuousPublicTradeStreamAuditAtTailResultV1(
            stream_id=STREAM_ID,
            continuation=page.continuation,
        ).outcome
        is store_contract.ContinuousPublicTradeStreamAuditOutcome.AT_TAIL
    )
    assert (
        store_contract.ContinuousPublicTradeStreamAuditNotFoundResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.NOT_REQUIRED
    )
    for audit_rejected_outcome in (
        store_contract.ContinuousPublicTradeStreamAuditOutcome.IDENTITY_CONFLICT,
        store_contract.ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT,
        store_contract.ContinuousPublicTradeStreamAuditOutcome.UNSUPPORTED_VERSION,
        store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
    ):
        assert (
            store_contract.ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=STREAM_ID,
                outcome=audit_rejected_outcome,
            ).retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
        )
    assert (
        store_contract.ContinuousPublicTradeStreamAuditUnavailableResultV1(
            stream_id=STREAM_ID
        ).retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY
    )

    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1.model_validate(
            {
                "stream_id": STREAM_ID,
                "outcome": store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
                "receipt": create_receipt,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateRejectedResultV1.model_validate(
            {
                "stream_id": STREAM_ID,
                "outcome": store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT,
                "receipt": create_receipt,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamLoadFoundResultV1(
            stream_id=OTHER_STREAM_ID,
            view=view,
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditAtTailResultV1(
            stream_id=OTHER_STREAM_ID,
            continuation=page.continuation,
        )


def test_pure_fake_create_is_atomic_exactly_idempotent_and_never_upserts() -> None:
    spy = PureStreamStoreSpy()
    command = create_command()

    inserted = spy.create(command)
    duplicate = spy.create(command)
    assert inserted.outcome is store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED
    assert duplicate.outcome is store_contract.ContinuousPublicTradeStreamCreateOutcome.DUPLICATE
    assert inserted.receipt.creation == command.creation
    assert duplicate.receipt.creation == command.creation
    assert spy.write_count == 1
    assert [name for name, _ in spy.calls] == ["create", "create"]

    same_id_different_bytes = create_command(
        creation_entry(recorded_at=COMMAND_TIME + timedelta(seconds=1))
    )
    conflict = spy.create(same_id_different_bytes)
    assert conflict.outcome is store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT
    assert spy.write_count == 1

    other_creation = creation_entry(initial_checkpoint=checkpoint(stream_id=OTHER_STREAM_ID))
    natural_identity_conflict = spy.create(create_command(other_creation))
    assert (
        natural_identity_conflict.outcome
        is store_contract.ContinuousPublicTradeStreamCreateOutcome.CONFLICT
    )
    assert OTHER_STREAM_ID not in spy.streams
    assert spy.write_count == 1


def test_pure_fake_all_five_transitions_form_one_exact_chain_and_replay_historically() -> None:
    spy = PureStreamStoreSpy()
    creation_command = create_command()
    assert (
        spy.create(creation_command).outcome
        is store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED
    )
    retained = spy.streams[STREAM_ID]
    replay_command: store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1 | None = None
    sequence = (
        (ContinuousPublicTradeTransitionKind.ATTACH, None),
        (ContinuousPublicTradeTransitionKind.MANUAL_HOLD, "operator-hold"),
        (ContinuousPublicTradeTransitionKind.MANUAL_RESUME, None),
        (ContinuousPublicTradeTransitionKind.CHILD_COMPLETED, None),
        (ContinuousPublicTradeTransitionKind.RETAIN, "scheduled-retain"),
    )
    for offset, (kind, reason) in enumerate(sequence, start=1):
        prior = retained.history[-1]
        transition = transition_entry(
            prior.successor_envelope,
            prior_history_root=prior.history_root,
            kind=kind,
            reason_code=reason,
            recorded_at=COMMAND_TIME + timedelta(seconds=offset),
            prior_recorded_at=prior.record.recorded_at,
        )
        child_fingerprint = (
            CHILD_POLICY_FINGERPRINT
            if kind
            in {
                ContinuousPublicTradeTransitionKind.ATTACH,
                ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
                ContinuousPublicTradeTransitionKind.MANUAL_RESUME,
                ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
            }
            else None
        )
        command = compare_and_swap_command(
            prior,
            transition,
            child_policy_fingerprint=child_fingerprint,
        )
        result = spy.compare_and_swap(command)
        assert (
            result.outcome
            is store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED
        )
        assert result.receipt.transition == transition
        if kind is ContinuousPublicTradeTransitionKind.ATTACH:
            replay_command = command

    assert replay_command is not None
    replay = spy.compare_and_swap(replay_command)
    assert (
        replay.outcome is store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE
    )
    assert replay.receipt.transition == replay_command.transition
    assert len(retained.history) == 6
    assert retained.history[-1].record.successor_version == 6
    assert spy.write_count == 6
    historical_create_replay = spy.create(creation_command)
    assert (
        historical_create_replay.outcome
        is store_contract.ContinuousPublicTradeStreamCreateOutcome.DUPLICATE
    )
    assert spy.write_count == 6


def test_two_competing_cas_writers_have_one_winner_and_no_retry_pressure() -> None:
    creation = creation_entry()
    spy = seeded_spy((creation,))
    first = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="first-writer",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    second = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="second-writer",
        recorded_at=COMMAND_TIME + timedelta(microseconds=2),
        prior_recorded_at=COMMAND_TIME,
    )
    first_command = compare_and_swap_command(creation, first)
    second_command = compare_and_swap_command(creation, second)

    winner = spy.compare_and_swap(first_command)
    loser = spy.compare_and_swap(second_command)
    replay = spy.compare_and_swap(first_command)
    assert winner.outcome is store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED
    assert loser.outcome is store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT
    assert (
        loser.retry_disposition
        is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )
    assert (
        replay.outcome is store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE
    )
    assert spy.write_count == 1
    assert len(spy.streams[STREAM_ID].history) == 2


def test_historical_cas_duplicate_requires_complete_creation_policy_and_allows_later_tail() -> None:
    history, _ = retain_history(3)
    creation = cast(store_contract.ContinuousPublicTradeStreamStoredCreationV1, history[0])
    historical = cast(store_contract.ContinuousPublicTradeStreamStoredTransitionV1, history[1])
    spy = seeded_spy(history)

    drifted_replay = compare_and_swap_command(
        creation,
        historical,
        effective_policy=policy(max_jobs_per_invocation=4),
    )
    conflict = spy.compare_and_swap(drifted_replay)
    assert conflict.outcome is (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT
    )
    assert conflict.retry_disposition is (
        store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )

    exact_replay = spy.compare_and_swap(compare_and_swap_command(creation, historical))
    assert exact_replay.outcome is (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE
    )
    assert exact_replay.receipt.transition == historical
    assert history[-1].record.successor_version == 3
    assert spy.write_count == 0


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("source", "coinbase.public-rest"),
        ("venue", "COINBASE"),
        ("instrument", "ETH-USD"),
        ("provider_symbol", "ETHUSD"),
        ("request_variant", "trades-v2"),
        ("policy_fingerprint", OTHER_DIGEST),
        ("stream_start_epoch_ms", 1_000),
    ],
)
def test_load_distinguishes_each_identity_mismatch_from_absence(
    field_name: str,
    replacement: object,
) -> None:
    creation = creation_entry()
    spy = seeded_spy((creation,))
    original = identity_from(checkpoint())
    changed = store_contract.ContinuousPublicTradeStreamIdentityV1(
        **{
            **original.__dict__,
            field_name: replacement,
        }
    )
    effective_policy = (
        policy(policy_fingerprint=OTHER_DIGEST) if field_name == "policy_fingerprint" else policy()
    )
    result = spy.load_current(
        store_contract.ContinuousPublicTradeStreamLoadQueryV1(
            expectation=store_contract.ContinuousPublicTradeStreamExpectationV1(
                identity=changed,
                effective_stream_policy=effective_policy,
            )
        )
    )
    assert result.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT
    assert result.retry_disposition is (
        store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
    )


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("window_size_ms", 500),
        ("settlement_lag_ms", 251),
        ("max_catchup_span_ms", 6_000),
        ("max_jobs_per_invocation", 4),
        ("max_requests_per_job", 101),
        ("max_records_per_job", 10_001),
    ],
)
def test_load_rejects_complete_policy_drift_under_reused_fingerprint(
    field_name: str,
    replacement: int,
) -> None:
    creation = creation_entry()
    spy = seeded_spy((creation,))
    drifted = policy(**{field_name: replacement})
    result = spy.load_current(load_query(effective_policy=drifted))
    assert result.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT


def test_load_child_policy_and_natural_identity_outcomes_are_exact() -> None:
    creation = creation_entry()
    attach = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.ATTACH,
        recorded_at=COMMAND_TIME + timedelta(seconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    spy = seeded_spy((creation, attach))
    current = attach.successor_envelope.envelope.checkpoint

    found = spy.load_current(
        load_query(
            current,
            child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
        )
    )
    missing_child = spy.load_current(load_query(current))
    wrong_child = spy.load_current(load_query(current, child_policy_fingerprint=OTHER_DIGEST))
    assert found.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.FOUND
    assert found.view.current == attach
    assert found.view.predecessor == creation
    assert (
        missing_child.outcome
        is store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT
    )
    assert (
        wrong_child.outcome
        is store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT
    )
    assert spy.history_reads[-1] == 3

    other_identity = identity_from(checkpoint(), stream_id=OTHER_STREAM_ID)
    natural_conflict = spy.load_current(
        store_contract.ContinuousPublicTradeStreamLoadQueryV1(
            expectation=store_contract.ContinuousPublicTradeStreamExpectationV1(
                identity=other_identity,
                effective_stream_policy=policy(),
            )
        )
    )
    absent_identity = identity_from(
        checkpoint(),
        stream_id=OTHER_STREAM_ID,
        provider_symbol="NOT-RETAINED",
    )
    absent = spy.load_current(
        store_contract.ContinuousPublicTradeStreamLoadQueryV1(
            expectation=store_contract.ContinuousPublicTradeStreamExpectationV1(
                identity=absent_identity,
                effective_stream_policy=policy(),
            )
        )
    )
    assert (
        natural_conflict.outcome
        is store_contract.ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT
    )
    assert absent.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.NOT_FOUND


def test_current_view_is_constant_size_at_versions_one_two_and_later() -> None:
    for length in (1, 2, 5):
        history, _ = retain_history(length)
        creation = cast(
            store_contract.ContinuousPublicTradeStreamStoredCreationV1,
            history[0],
        )
        predecessor = None if length == 1 else history[-2]
        view = store_contract.ContinuousPublicTradeStreamCurrentViewV1(
            creation=creation,
            current=history[-1],
            predecessor=predecessor,
        )
        assert view.current.record.successor_version == length
        if length == 1:
            assert view.predecessor is None
        elif length == 2:
            assert type(view.predecessor) is (
                store_contract.ContinuousPublicTradeStreamStoredCreationV1
            )
        else:
            assert type(view.predecessor) is (
                store_contract.ContinuousPublicTradeStreamStoredTransitionV1
            )


@pytest.mark.parametrize(
    ("history_length", "limit", "expected_count"),
    [
        (1, 1, 1),
        (100, 100, 100),
        (101, 100, 100),
    ],
)
def test_audit_first_page_has_creation_no_overlap_and_no_lookahead(
    history_length: int,
    limit: int,
    expected_count: int,
) -> None:
    history, _ = retain_history(history_length)
    spy = seeded_spy(history)
    current = history[-1].successor_envelope.envelope.checkpoint
    result = spy.audit_page(audit_start_query(current, limit=limit))
    assert result.outcome is store_contract.ContinuousPublicTradeStreamAuditOutcome.PAGE
    assert result.page.predecessor_overlap is None
    assert len(result.page.records) == expected_count
    assert type(result.page.records[0]) is (
        store_contract.ContinuousPublicTradeStreamStoredCreationV1
    )
    assert result.page.records == history[:expected_count]
    assert result.page.continuation == continuation_for(history[expected_count - 1])
    assert spy.history_reads[-1] == expected_count
    assert spy.history_reads[-1] <= 100


def test_audit_101_boundary_uses_one_overlap_then_exact_tail_without_empty_page() -> None:
    history, _ = retain_history(101)
    spy = seeded_spy(history)
    current = history[-1].successor_envelope.envelope.checkpoint

    first = spy.audit_page(audit_start_query(current, limit=100))
    assert type(first) is store_contract.ContinuousPublicTradeStreamAuditPageResultV1
    second = spy.audit_page(
        audit_continuation_query(
            current,
            first.page.continuation,
            limit=100,
        )
    )
    assert type(second) is store_contract.ContinuousPublicTradeStreamAuditPageResultV1
    assert second.page.predecessor_overlap == history[99]
    assert second.page.records == (history[100],)
    assert second.page.continuation == continuation_for(history[100])
    assert spy.history_reads[-1] == 2

    tail = spy.audit_page(
        audit_continuation_query(
            current,
            second.page.continuation,
            limit=100,
        )
    )
    assert type(tail) is store_contract.ContinuousPublicTradeStreamAuditAtTailResultV1
    assert tail.continuation == second.page.continuation
    assert not hasattr(tail, "page")
    assert spy.history_reads[-1] == 1
    assert max(spy.history_reads) <= 101


def test_audit_continuation_anchor_conflict_is_not_corruption_or_absence() -> None:
    history, _ = retain_history(3)
    spy = seeded_spy(history)
    current = history[-1].successor_envelope.envelope.checkpoint
    valid = continuation_for(history[0])
    for update in (
        {"through_version": 2},
        {"through_envelope_digest": OTHER_DIGEST},
        {"through_history_root": OTHER_DIGEST},
    ):
        changed = store_contract.ContinuousPublicTradeStreamAuditContinuationV1(
            **{
                **valid.__dict__,
                **update,
            }
        )
        result = spy.audit_page(
            audit_continuation_query(
                current,
                changed,
                limit=1,
            )
        )
        assert (
            result.outcome is store_contract.ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT
        )
        assert (
            result.retry_disposition
            is store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY
        )


def test_audit_page_rejects_gaps_wrong_overlap_mutable_sequences_and_bad_tail() -> None:
    history, _ = retain_history(3)
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditPageV1(
            predecessor_overlap=None,
            records=(history[0], history[2]),
            continuation=continuation_for(history[2]),
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditPageV1(
            predecessor_overlap=history[0],
            records=(history[2],),
            continuation=continuation_for(history[2]),
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditPageV1.model_validate(
            {
                "predecessor_overlap": None,
                "records": [history[0]],
                "continuation": continuation_for(history[0]),
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditPageV1(
            predecessor_overlap=None,
            records=(history[0],),
            continuation=continuation_for(history[1]),
        )


@pytest.mark.parametrize("invalid_limit", [False, True, 0, 101, IntSubclass(1)])
def test_audit_limits_reject_bool_subclass_and_out_of_range(
    invalid_limit: object,
) -> None:
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamAuditStartQueryV1.model_validate(
            {
                "expectation": expectation_for(checkpoint()),
                "limit": invalid_limit,
            }
        )


@given(st.integers(min_value=1, max_value=100))
def test_every_legal_audit_limit_is_preserved_exactly(limit: int) -> None:
    query = audit_start_query(limit=limit)
    assert query.limit == limit
    assert type(query.limit) is int


@given(
    history_length=st.integers(min_value=1, max_value=20),
    limit=st.integers(min_value=1, max_value=100),
)
@settings(deadline=None, max_examples=25)
def test_audit_property_never_exceeds_requested_or_absolute_read_bound(
    history_length: int,
    limit: int,
) -> None:
    history, _ = retain_history(history_length)
    spy = seeded_spy(history)
    current = history[-1].successor_envelope.envelope.checkpoint
    first = spy.audit_page(audit_start_query(current, limit=limit))
    assert type(first) is store_contract.ContinuousPublicTradeStreamAuditPageResultV1
    assert 1 <= len(first.page.records) <= limit
    assert spy.history_reads[-1] <= limit
    if len(first.page.records) < history_length:
        continued = spy.audit_page(
            audit_continuation_query(
                current,
                first.page.continuation,
                limit=limit,
            )
        )
        assert type(continued) is store_contract.ContinuousPublicTradeStreamAuditPageResultV1
        assert 1 <= len(continued.page.records) <= limit
        assert spy.history_reads[-1] == 1 + len(continued.page.records)
        assert spy.history_reads[-1] <= 101


def test_retained_v1_v3_gap_is_corrupt_for_load_audit_and_duplicate_paths() -> None:
    creation = creation_entry()
    gap = unchecked_retain_transition(
        creation,
        declared_prior_version=2,
        suffix="v1-v3-gap",
    )
    current = gap.successor_envelope.envelope.checkpoint
    spy = seeded_spy((creation, gap))
    gap_replay = store_contract.ContinuousPublicTradeStreamCompareAndSwapCommandV1(
        expectation=expectation_for(current),
        expected_version=gap.record.prior_version,
        expected_envelope_digest=gap.record.prior_envelope_digest,
        expected_history_root=gap.record.prior_history_root,
        transition=gap,
    )

    load_result = spy.load_current(load_query(current))
    start_result = spy.audit_page(audit_start_query(current, limit=2))
    continuation_result = spy.audit_page(
        audit_continuation_query(
            current,
            continuation_for(creation),
            limit=1,
        )
    )
    create_replay = spy.create(create_command(creation))
    cas_replay = spy.compare_and_swap(gap_replay)

    assert load_result.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT
    assert start_result.outcome is store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT
    assert continuation_result.outcome is (
        store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT
    )
    assert create_replay.outcome is store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT
    assert cas_replay.outcome is (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT
    )
    assert spy.write_count == 0


def test_retained_lifecycle_corruption_after_overlap_is_never_page_or_duplicate() -> None:
    history, _ = retain_history(2)
    creation = cast(store_contract.ContinuousPublicTradeStreamStoredCreationV1, history[0])
    predecessor = cast(store_contract.ContinuousPublicTradeStreamStoredTransitionV1, history[1])
    corrupt = unchecked_retain_transition(
        predecessor,
        cursor_epoch_ms=1_000,
        suffix="illegal-retain-progress",
    )
    current = corrupt.successor_envelope.envelope.checkpoint
    continuation = continuation_for(predecessor)
    query = audit_continuation_query(current, continuation, limit=1)
    structurally_contiguous_page = store_contract.ContinuousPublicTradeStreamAuditPageV1(
        predecessor_overlap=predecessor,
        records=(corrupt,),
        continuation=continuation_for(corrupt),
    )
    with pytest.raises(store_contract.ContinuousPublicTradeStreamStoreContractError) as caught:
        store_contract.validate_continuous_public_trade_stream_audit_page(
            query,
            structurally_contiguous_page,
        )
    assert caught.value.code is (
        store_contract.ContinuousPublicTradeStreamStoreContractErrorCode.INCONSISTENT_VALUE
    )

    spy = seeded_spy((creation, predecessor, corrupt))
    load_result = spy.load_current(load_query(current))
    start_result = spy.audit_page(audit_start_query(current, limit=3))
    continuation_result = spy.audit_page(query)
    create_replay = spy.create(create_command(creation))
    historical_replay = spy.compare_and_swap(compare_and_swap_command(creation, predecessor))

    assert load_result.outcome is store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT
    assert start_result.outcome is store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT
    assert continuation_result.outcome is (
        store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT
    )
    assert create_replay.outcome is store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT
    assert historical_replay.outcome is (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT
    )
    assert spy.write_count == 0


@pytest.mark.parametrize(
    (
        "operation",
        "fault",
        "expected_outcome",
        "expected_retry",
    ),
    [
        (
            "create",
            "unsupported",
            store_contract.ContinuousPublicTradeStreamCreateOutcome.UNSUPPORTED_VERSION,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "create",
            "corrupt",
            store_contract.ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "create",
            "unavailable",
            store_contract.ContinuousPublicTradeStreamCreateOutcome.UNAVAILABLE,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY,
        ),
        (
            "load",
            "unsupported",
            store_contract.ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "load",
            "corrupt",
            store_contract.ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "load",
            "unavailable",
            store_contract.ContinuousPublicTradeStreamLoadOutcome.UNAVAILABLE,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY,
        ),
        (
            "cas",
            "unsupported",
            store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UNSUPPORTED_VERSION,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "cas",
            "corrupt",
            store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "cas",
            "unavailable",
            store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UNAVAILABLE,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY,
        ),
        (
            "audit",
            "unsupported",
            store_contract.ContinuousPublicTradeStreamAuditOutcome.UNSUPPORTED_VERSION,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "audit",
            "corrupt",
            store_contract.ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.DO_NOT_RETRY,
        ),
        (
            "audit",
            "unavailable",
            store_contract.ContinuousPublicTradeStreamAuditOutcome.UNAVAILABLE,
            store_contract.ContinuousPublicTradeStreamStoreRetryDisposition.EXACT_REQUEST_ONLY,
        ),
    ],
)
def test_storage_failures_remain_closed_outcomes_and_never_become_absence(
    operation: str,
    fault: str,
    expected_outcome: object,
    expected_retry: store_contract.ContinuousPublicTradeStreamStoreRetryDisposition,
) -> None:
    creation = creation_entry()
    transition = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    spy = seeded_spy((creation,))
    spy.forced_fault = fault
    result: Any
    if operation == "create":
        result = spy.create(create_command(creation))
    elif operation == "load":
        result = spy.load_current(load_query())
    elif operation == "cas":
        result = spy.compare_and_swap(compare_and_swap_command(creation, transition))
    else:
        result = spy.audit_page(audit_start_query(limit=1))
    assert result.outcome is expected_outcome
    assert result.retry_disposition is expected_retry
    assert not hasattr(result, "receipt")
    assert not hasattr(result, "view")
    assert not hasattr(result, "page")
    assert spy.write_count == 0


def test_attach_and_completion_commands_require_exact_applicable_child_policy() -> None:
    creation = creation_entry()
    attach = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.ATTACH,
        recorded_at=COMMAND_TIME + timedelta(seconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    valid_attach = compare_and_swap_command(
        creation,
        attach,
        child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
    )
    assert valid_attach.expectation.effective_child_policy_fingerprint == CHILD_POLICY_FINGERPRINT
    for invalid_child in (None, OTHER_DIGEST):
        with pytest.raises(ValidationError):
            compare_and_swap_command(
                creation,
                attach,
                child_policy_fingerprint=invalid_child,
            )

    completion = transition_entry(
        attach.successor_envelope,
        prior_history_root=attach.history_root,
        kind=ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
        recorded_at=COMMAND_TIME + timedelta(seconds=2),
        prior_recorded_at=COMMAND_TIME + timedelta(seconds=1),
    )
    assert compare_and_swap_command(
        attach,
        completion,
        child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
    )
    for invalid_child in (None, OTHER_DIGEST):
        with pytest.raises(ValidationError):
            compare_and_swap_command(
                attach,
                completion,
                child_policy_fingerprint=invalid_child,
            )

    retain = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(seconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    with pytest.raises(ValidationError):
        compare_and_swap_command(
            creation,
            retain,
            child_policy_fingerprint=CHILD_POLICY_FINGERPRINT,
        )


def _assert_boundary_rejects_before_call(
    action: Any,
    spy: PureStreamStoreSpy,
) -> None:
    with pytest.raises(store_contract.ContinuousPublicTradeStreamStoreContractError) as caught:
        action()
    assert (
        caught.value.code
        is store_contract.ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE
    )
    assert str(caught.value) == "malformed_value"
    assert spy.calls == []
    assert spy.write_count == 0


def test_method_boundary_rejects_model_copy_construct_and_top_level_hidden_storage() -> None:
    valid = create_command()

    copied = valid.model_copy(
        update={"creation": valid.creation.model_copy(update={"history_root": OTHER_DIGEST})}
    )
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(copied), spy)

    partial = store_contract.ContinuousPublicTradeStreamCreateCommandV1.model_construct(
        expectation=valid.expectation
    )
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(partial), spy)

    hidden = valid.model_copy(deep=True)
    hidden.__dict__["undeclared_secret"] = "must-not-echo"
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(hidden), spy)

    private = valid.model_copy(deep=True)
    object.__setattr__(private, "__pydantic_private__", {"_secret": "must-not-echo"})
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(private), spy)


def test_method_boundary_recursively_rejects_nested_task059_and_task061_bypasses() -> None:
    valid = create_command()

    hidden_record = valid.model_copy(deep=True)
    hidden_record.creation.record.__dict__["undeclared_secret"] = "must-not-echo"
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(hidden_record), spy)

    hidden_checkpoint = valid.model_copy(deep=True)
    hidden_checkpoint.creation.successor_envelope.envelope.checkpoint.__dict__[
        "undeclared_secret"
    ] = "must-not-echo"
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(hidden_checkpoint), spy)

    invalid_policy = valid.model_copy(deep=True)
    object.__setattr__(
        invalid_policy.expectation,
        "effective_stream_policy",
        invalid_policy.expectation.effective_stream_policy.model_copy(
            update={"max_jobs_per_invocation": True}
        ),
    )
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(invalid_policy), spy)

    hidden_scope = valid.model_copy(deep=True)
    hidden_scope.creation.create_authority_scope.__dict__["undeclared_secret"] = "must-not-echo"
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(hidden_scope), spy)


def test_exact_model_subclasses_and_scalar_subclasses_are_rejected() -> None:
    identity = identity_from(checkpoint())
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.__dict__,
                "source": StringSubclass(identity.source),
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamIdentityV1(
            **{
                **identity.__dict__,
                "stream_start_epoch_ms": IntSubclass(0),
            }
        )
    creation = creation_entry()
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamStoredCreationV1(
            **{
                **creation.__dict__,
                "canonical_bytes": BytesSubclass(creation.canonical_bytes),
            }
        )

    class CreateCommandSubclass(store_contract.ContinuousPublicTradeStreamCreateCommandV1):
        pass

    valid = create_command()
    subclass = CreateCommandSubclass(
        expectation=valid.expectation,
        creation=valid.creation,
    )
    spy = PureStreamStoreSpy()
    _assert_boundary_rejects_before_call(lambda: spy.create(subclass), spy)


def test_result_revalidation_rejects_bypass_private_and_wrong_enum_storage() -> None:
    creation = creation_entry()
    result = store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1(
        stream_id=STREAM_ID,
        outcome=store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED,
        receipt=store_contract.ContinuousPublicTradeStreamCreateReceiptV1(creation=creation),
    )
    hidden = result.model_copy(deep=True)
    hidden.receipt.creation.__dict__["undeclared_secret"] = "must-not-echo"
    with pytest.raises(store_contract.ContinuousPublicTradeStreamStoreContractError) as caught:
        type(result).revalidate_at_boundary(hidden)
    assert caught.value.code is (
        store_contract.ContinuousPublicTradeStreamStoreContractErrorCode.MALFORMED_VALUE
    )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1.model_validate(
            {
                "stream_id": STREAM_ID,
                "outcome": store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED.value,
                "receipt": result.receipt,
            }
        )
    with pytest.raises(ValidationError):
        store_contract.ContinuousPublicTradeStreamCreateAcceptedResultV1.model_validate(
            {
                "stream_id": str(STREAM_ID),
                "outcome": store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED,
                "receipt": result.receipt,
            }
        )


@given(index=st.integers(min_value=0, max_value=63))
@settings(deadline=None, max_examples=32)
def test_one_nibble_anchor_mutation_is_never_accepted_as_a_page(index: int) -> None:
    history, _ = retain_history(3)
    spy = seeded_spy(history)
    current = history[-1].successor_envelope.envelope.checkpoint
    continuation = continuation_for(history[0])
    digest = continuation.through_history_root
    offset = len("sha256:") + index
    replacement = "0" if digest[offset] != "0" else "1"
    changed_digest = digest[:offset] + replacement + digest[offset + 1 :]
    changed = store_contract.ContinuousPublicTradeStreamAuditContinuationV1(
        **{
            **continuation.__dict__,
            "through_history_root": changed_digest,
        }
    )
    result = spy.audit_page(
        audit_continuation_query(
            current,
            changed,
            limit=1,
        )
    )
    assert result.outcome is store_contract.ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT


def test_module_and_fake_have_no_clock_io_database_or_runtime_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_path = Path(store_contract.__file__)
    parsed = ast.parse(module_path.read_text(encoding="utf-8"))
    banned_imports = {
        "os",
        "pathlib",
        "sqlite3",
        "socket",
        "time",
        "urllib",
        "requests",
        "httpx",
        "wealth.adapters",
        "wealth.application",
    }
    imported: set[str] = set()
    called_names: set[str] = set()
    for node in ast.walk(parsed):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_names.add(node.func.attr)
    assert not {
        name
        for name in imported
        if any(name == banned or name.startswith(f"{banned}.") for banned in banned_imports)
    }
    assert not {"open", "connect", "sleep", "now", "utcnow", "uuid4"}.intersection(called_names)

    import builtins
    import socket
    import sqlite3
    import time

    def forbidden(*_: object, **__: object) -> Any:
        raise AssertionError("unexpected external or clock capability")

    creation = creation_entry()
    transition = transition_entry(
        creation.successor_envelope,
        prior_history_root=creation.history_root,
        kind=ContinuousPublicTradeTransitionKind.RETAIN,
        reason_code="scheduled-retain",
        recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        prior_recorded_at=COMMAND_TIME,
    )
    create = create_command(creation)
    load = load_query()
    cas = compare_and_swap_command(creation, transition)
    audit = audit_start_query(limit=1)
    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(time, "time", forbidden)

    spy = PureStreamStoreSpy()
    assert spy.create(create).outcome is (
        store_contract.ContinuousPublicTradeStreamCreateOutcome.INSERTED
    )
    assert spy.load_current(load).outcome is (
        store_contract.ContinuousPublicTradeStreamLoadOutcome.FOUND
    )
    assert spy.compare_and_swap(cas).outcome is (
        store_contract.ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED
    )
    assert spy.audit_page(audit).outcome is (
        store_contract.ContinuousPublicTradeStreamAuditOutcome.PAGE
    )


def test_no_runtime_source_imports_the_unused_port() -> None:
    module_path = Path(store_contract.__file__).resolve()
    source_root = module_path.parents[2]
    needle = "wealth.ports.continuous_public_trade_stream_store"
    importers = []
    for path in source_root.rglob("*.py"):
        if path.resolve() != module_path and needle in path.read_text(encoding="utf-8"):
            importers.append(path)
    assert importers == []
