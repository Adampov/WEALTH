"""Contract tests for pure continuous public-trade persistence values and codecs."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

import wealth.domain.continuous_public_trade_persistence as persistence
from wealth.domain.continuous_public_trade import (
    ContinuousPublicTradeAttachment,
    ContinuousPublicTradePlanStatus,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeStreamCheckpoint,
    ContinuousPublicTradeStreamStatus,
    ContinuousPublicTradeTransitionKind,
)
from wealth.domain.continuous_public_trade_persistence import (
    MAX_CHILD_CREATION_BYTES,
    MAX_ENVELOPE_BYTES,
    MAX_JSON_DEPTH,
    MAX_JSON_INTEGER_DIGITS,
    MAX_JSON_KEY_BYTES,
    MAX_JSON_MEMBERS,
    MAX_JSON_STRING_LEXICAL_BYTES,
    MAX_RAW_RECORD_BYTES,
    MAX_SUCCESSOR_ENVELOPE_HEX_CHARS,
    PROVISIONAL_CHILD_CREATION_FINGERPRINT,
    ContinuousPublicTradeChildCreationPayloadV1,
    ContinuousPublicTradeEvidenceKind,
    ContinuousPublicTradeEvidenceOutcome,
    ContinuousPublicTradeEvidenceReferenceV1,
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradePersistenceContractError,
    ContinuousPublicTradePersistenceErrorCode,
    ContinuousPublicTradePolicyProjectionV1,
    ContinuousPublicTradeStreamCreationRecordV1,
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeStreamTransitionRecordV1,
    child_creation_fingerprint,
    decode_child_creation_payload,
    decode_evidence_scope,
    decode_stream_creation_record,
    decode_stream_envelope,
    decode_stream_transition_record,
    encode_child_creation_payload,
    encode_evidence_scope,
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
    validate_stream_creation_record_scope,
    validate_stream_load_bindings,
    validate_stream_transition_link,
    validate_stream_transition_record_scopes,
)
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import PublicTradeCollectionCheckpoint

STREAM_ID = UUID("00000000-0000-4000-8000-000000000061")
CHILD_ID = UUID("00000000-0000-4000-8000-000000000059")
POLICY_FP = "sha256:" + ("1" * 64)
CHILD_POLICY_FP = "sha256:" + ("2" * 64)
EVIDENCE_DIGEST = "sha256:" + ("3" * 64)
OTHER_DIGEST = "sha256:" + ("4" * 64)
COMMAND_TIME = datetime(2026, 7, 27, 12, 34, 56, 123456, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

DOMAINS = {
    "child": b"wealth.continuous_public_trade.child_creation/v1\x00",
    "envelope": b"wealth.continuous_public_trade.stream_record/v1\x00",
    "creation": b"wealth.continuous_public_trade.stream_creation/v1\x00",
    "transition": b"wealth.continuous_public_trade.stream_transition/v1\x00",
    "scope": b"wealth.continuous_public_trade.evidence_scope/v1\x00",
    "initial": b"wealth.continuous_public_trade.history_root/v1\x00\x01",
    "next": b"wealth.continuous_public_trade.history_root/v1\x00\x02",
}


class IntSubclass(int):
    """Hostile integer subclass."""


class StringSubclass(str):
    """Hostile string subclass."""


class DatetimeSubclass(datetime):
    """Hostile datetime subclass."""


def policy(**updates: object) -> ContinuousPublicTradePolicy:
    values: dict[str, object] = {
        "window_size_ms": 1_000,
        "settlement_lag_ms": 250,
        "max_catchup_span_ms": 5_000,
        "max_jobs_per_invocation": 3,
        "max_requests_per_job": 100,
        "max_records_per_job": 10_000,
        "policy_fingerprint": POLICY_FP,
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
        "policy_fingerprint": POLICY_FP,
        "stream_start_epoch_ms": 0,
        "cursor_epoch_ms": 10_000,
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


def due_payload(
    current: ContinuousPublicTradeStreamCheckpoint | None = None,
    *,
    at: datetime = COMMAND_TIME,
) -> tuple[ContinuousPublicTradeAttachment, ContinuousPublicTradeChildCreationPayloadV1]:
    plan, payload = finalize_continuous_public_trade_attachment(
        current or checkpoint(),
        policy(),
        candidate_job_id=CHILD_ID,
        child_policy_fingerprint=CHILD_POLICY_FP,
        now=at,
    )
    assert plan.status is ContinuousPublicTradePlanStatus.ATTACHED_JOB
    assert plan.attachment is not None and payload is not None
    return plan.attachment, payload


def unattached_envelope(
    **updates: object,
) -> ContinuousPublicTradeStreamEnvelopeV1:
    return ContinuousPublicTradeStreamEnvelopeV1(checkpoint=checkpoint(**updates))


def attached_envelope(
    current: ContinuousPublicTradeStreamCheckpoint | None = None,
    *,
    version: int = 2,
    at: datetime = COMMAND_TIME,
) -> ContinuousPublicTradeStreamEnvelopeV1:
    prior = current or checkpoint()
    attachment, payload = due_payload(prior, at=at)
    return ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(prior, attachment=attachment, version=version),
        child_creation_payload=payload,
    )


def reference(
    kind: ContinuousPublicTradeEvidenceKind,
    scope_digest: str,
    *,
    expires_at: datetime | None = None,
) -> ContinuousPublicTradeEvidenceReferenceV1:
    return ContinuousPublicTradeEvidenceReferenceV1(
        evidence_kind=kind,
        evidence_id=f"evidence-{kind.value.lower()}",
        evidence_digest=EVIDENCE_DIGEST,
        scope_digest=scope_digest,
        outcome=(
            ContinuousPublicTradeEvidenceOutcome.ACCEPTED
            if kind is ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
            else ContinuousPublicTradeEvidenceOutcome.APPROVED
        ),
        valid_from=COMMAND_TIME - timedelta(minutes=1),
        expires_at=expires_at,
    )


def creation_fixture() -> tuple[
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradeStreamCreationRecordV1,
]:
    envelope = unattached_envelope(cursor_epoch_ms=0)
    projection = project_continuous_public_trade_policy(policy())
    envelope_digest = stream_envelope_digest(envelope)
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=STREAM_ID,
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
        stream_policy=projection,
    )
    record = ContinuousPublicTradeStreamCreationRecordV1(
        stream_id=STREAM_ID,
        source=envelope.checkpoint.source,
        venue=envelope.checkpoint.venue,
        instrument=envelope.checkpoint.instrument,
        provider_symbol=envelope.checkpoint.provider_symbol,
        instrument_type=envelope.checkpoint.instrument_type,
        request_variant=envelope.checkpoint.request_variant,
        stream_start_epoch_ms=envelope.checkpoint.stream_start_epoch_ms,
        stream_policy=projection,
        successor_envelope_hex=encode_stream_envelope(envelope).hex(),
        successor_envelope_digest=envelope_digest,
        create_authority_reference=reference(
            ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
            evidence_scope_digest(scope),
        ),
        recorded_at=COMMAND_TIME,
    )
    return envelope, scope, record


def transition_fixture(
    kind: ContinuousPublicTradeTransitionKind,
) -> tuple[
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradeEvidenceScopeV1 | None,
    ContinuousPublicTradeStreamTransitionRecordV1,
]:
    initial, _, creation = creation_fixture()
    prior = initial
    prior_root = initial_stream_history_root(creation)
    reason: str | None = None
    if kind is ContinuousPublicTradeTransitionKind.ATTACH:
        successor = attached_envelope(prior.checkpoint)
    elif kind is ContinuousPublicTradeTransitionKind.RETAIN:
        reason = "scheduled-retain"
        successor = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(prior.checkpoint, version=2)
        )
    elif kind is ContinuousPublicTradeTransitionKind.MANUAL_HOLD:
        reason = "operator-hold"
        successor = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                prior.checkpoint,
                status=ContinuousPublicTradeStreamStatus.PAUSED,
                pause_reason=reason,
                version=2,
            )
        )
    elif kind is ContinuousPublicTradeTransitionKind.MANUAL_RESUME:
        prior = unattached_envelope(
            status=ContinuousPublicTradeStreamStatus.PAUSED,
            pause_reason="operator-hold",
        )
        successor = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                prior.checkpoint,
                status=ContinuousPublicTradeStreamStatus.ACTIVE,
                pause_reason=None,
                version=2,
            )
        )
    else:
        prior = attached_envelope(prior.checkpoint, version=1)
        assert prior.checkpoint.attachment is not None
        successor = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=copy_checkpoint(
                prior.checkpoint,
                cursor_epoch_ms=prior.checkpoint.attachment.window_end_epoch_ms,
                attachment=None,
                version=2,
            )
        )
    prior_digest = stream_envelope_digest(prior)
    successor_digest = stream_envelope_digest(successor)
    attachment = successor.checkpoint.attachment
    payload = successor.child_creation_payload
    authority = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=STREAM_ID,
        transition_kind=kind,
        prior_version=1,
        prior_envelope_digest=prior_digest,
        prior_history_root=prior_root,
        successor_version=2,
        successor_envelope_digest=(
            None if kind is ContinuousPublicTradeTransitionKind.ATTACH else successor_digest
        ),
        child_job_id=(
            attachment.job_id
            if kind is ContinuousPublicTradeTransitionKind.ATTACH and attachment
            else None
        ),
        child_policy_fingerprint=(
            payload.child_checkpoint.policy_fingerprint
            if kind is ContinuousPublicTradeTransitionKind.ATTACH and payload
            else None
        ),
        child_creation_fingerprint=None,
        reason_code=reason,
        stream_policy=None,
    )
    completion: ContinuousPublicTradeEvidenceScopeV1 | None = None
    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        assert prior.checkpoint.attachment is not None
        assert prior.child_creation_payload is not None
        completion = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
            stream_id=STREAM_ID,
            transition_kind=kind,
            prior_version=1,
            prior_envelope_digest=prior_digest,
            prior_history_root=prior_root,
            successor_version=2,
            successor_envelope_digest=successor_digest,
            child_job_id=prior.checkpoint.attachment.job_id,
            child_policy_fingerprint=(
                prior.child_creation_payload.child_checkpoint.policy_fingerprint
            ),
            child_creation_fingerprint=prior.checkpoint.attachment.creation_fingerprint,
            reason_code=None,
            stream_policy=None,
        )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=STREAM_ID,
        prior_version=1,
        successor_version=2,
        transition_kind=kind,
        prior_history_root=prior_root,
        prior_envelope_digest=prior_digest,
        successor_envelope_hex=encode_stream_envelope(successor).hex(),
        successor_envelope_digest=successor_digest,
        reason_code=reason,
        transition_authority_reference=reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(authority),
        ),
        child_completion_reference=(
            None
            if completion is None
            else reference(
                ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
                evidence_scope_digest(completion),
            )
        ),
        recorded_at=COMMAND_TIME,
    )
    return prior, authority, completion, record


def chained_transition(
    prior: ContinuousPublicTradeStreamEnvelopeV1,
    successor: ContinuousPublicTradeStreamEnvelopeV1,
    kind: ContinuousPublicTradeTransitionKind,
    *,
    prior_history_root: str,
    recorded_at: datetime,
    reason_code: str | None = None,
) -> tuple[
    ContinuousPublicTradeEvidenceScopeV1,
    ContinuousPublicTradeEvidenceScopeV1 | None,
    ContinuousPublicTradeStreamTransitionRecordV1,
]:
    """Build one exact transition whose causal fields come from its direct predecessor."""

    prior_digest = stream_envelope_digest(prior)
    successor_digest = stream_envelope_digest(successor)
    successor_attachment = successor.checkpoint.attachment
    successor_payload = successor.child_creation_payload
    authority = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=prior.checkpoint.stream_id,
        transition_kind=kind,
        prior_version=prior.checkpoint.version,
        prior_envelope_digest=prior_digest,
        prior_history_root=prior_history_root,
        successor_version=successor.checkpoint.version,
        successor_envelope_digest=(
            None if kind is ContinuousPublicTradeTransitionKind.ATTACH else successor_digest
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
    completion: ContinuousPublicTradeEvidenceScopeV1 | None = None
    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        prior_attachment = prior.checkpoint.attachment
        prior_payload = prior.child_creation_payload
        assert prior_attachment is not None and prior_payload is not None
        completion = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
            stream_id=prior.checkpoint.stream_id,
            transition_kind=kind,
            prior_version=prior.checkpoint.version,
            prior_envelope_digest=prior_digest,
            prior_history_root=prior_history_root,
            successor_version=successor.checkpoint.version,
            successor_envelope_digest=successor_digest,
            child_job_id=prior_attachment.job_id,
            child_policy_fingerprint=prior_payload.child_checkpoint.policy_fingerprint,
            child_creation_fingerprint=prior_attachment.creation_fingerprint,
            reason_code=None,
            stream_policy=None,
        )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=prior.checkpoint.stream_id,
        prior_version=prior.checkpoint.version,
        successor_version=successor.checkpoint.version,
        transition_kind=kind,
        prior_history_root=prior_history_root,
        prior_envelope_digest=prior_digest,
        successor_envelope_hex=encode_stream_envelope(successor).hex(),
        successor_envelope_digest=successor_digest,
        reason_code=reason_code,
        transition_authority_reference=reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(authority),
        ),
        child_completion_reference=(
            None
            if completion is None
            else reference(
                ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
                evidence_scope_digest(completion),
            )
        ),
        recorded_at=recorded_at,
    )
    return authority, completion, record


def oracle(domain: bytes, payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(domain + payload).hexdigest()


def mutate_json(raw: bytes, **updates: object) -> bytes:
    value = json.loads(raw)
    assert isinstance(value, dict)
    value.update(updates)
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def assert_error(
    code: ContinuousPublicTradePersistenceErrorCode,
    action: Callable[[], object],
) -> None:
    with pytest.raises(ContinuousPublicTradePersistenceContractError) as caught:
        action()
    assert caught.value.code is code
    assert str(caught.value) == (
        "continuous public-trade persistence contract rejected the supplied value"
    )


def test_policy_projection_is_complete_frozen_and_keeps_caller_digest() -> None:
    source = policy()
    projection = project_continuous_public_trade_policy(source)
    assert projection == ContinuousPublicTradePolicyProjectionV1(**source.model_dump())
    assert projection.model_dump() == source.model_dump()
    with pytest.raises(ValidationError):
        projection.window_size_ms = 2_000


@pytest.mark.parametrize(
    "field",
    [
        "window_size_ms",
        "settlement_lag_ms",
        "max_catchup_span_ms",
        "max_jobs_per_invocation",
        "max_requests_per_job",
        "max_records_per_job",
    ],
)
@pytest.mark.parametrize("invalid", [True, 1.0, "1", IntSubclass(1)])
def test_projection_rejects_non_exact_integers(field: str, invalid: object) -> None:
    values = policy().model_dump()
    values[field] = invalid
    with pytest.raises(ValidationError):
        ContinuousPublicTradePolicyProjectionV1.model_validate(values)


def test_two_pass_finalizer_covers_held_waiting_existing_and_new_due_paths() -> None:
    held, held_payload = finalize_continuous_public_trade_attachment(
        checkpoint(
            status=ContinuousPublicTradeStreamStatus.PAUSED,
            pause_reason="operator-hold",
        ),
        policy(),
        candidate_job_id=CHILD_ID,
        child_policy_fingerprint=CHILD_POLICY_FP,
        now=COMMAND_TIME,
    )
    waiting, waiting_payload = finalize_continuous_public_trade_attachment(
        checkpoint(),
        policy(),
        candidate_job_id=CHILD_ID,
        child_policy_fingerprint=CHILD_POLICY_FP,
        now=EPOCH + timedelta(milliseconds=10_250),
    )
    existing, _ = due_payload()
    attached, attached_payload = finalize_continuous_public_trade_attachment(
        checkpoint(attachment=existing),
        policy(),
        candidate_job_id=UUID(int=999),
        child_policy_fingerprint=OTHER_DIGEST,
        now=COMMAND_TIME + timedelta(days=1),
    )
    due, payload = finalize_continuous_public_trade_attachment(
        checkpoint(),
        policy(),
        candidate_job_id=CHILD_ID,
        child_policy_fingerprint=CHILD_POLICY_FP,
        now=COMMAND_TIME,
    )
    assert (held.status, held_payload) == (ContinuousPublicTradePlanStatus.HELD, None)
    assert (waiting.status, waiting_payload) == (
        ContinuousPublicTradePlanStatus.WAITING,
        None,
    )
    assert attached.attachment is existing and attached_payload is None
    assert due.attachment is not None and payload is not None
    assert due.attachment.creation_fingerprint != PROVISIONAL_CHILD_CREATION_FINGERPRINT
    assert due.attachment.creation_fingerprint == child_creation_fingerprint(payload)
    child = payload.child_checkpoint
    assert payload.stream_policy_fingerprint == POLICY_FP
    assert child.policy_fingerprint == CHILD_POLICY_FP
    assert child.created_at == child.updated_at == COMMAND_TIME
    assert child.window_start == child.next_window_start == EPOCH + timedelta(seconds=10)
    assert child.window_end_exclusive == EPOCH + timedelta(seconds=15)
    assert child.version == 1
    assert child.pending_window_end_exclusive is None
    assert child.lease_owner is None
    assert child.lease_token is None
    assert child.lease_expires_at is None
    assert (
        child.windows_completed,
        child.records_completed,
        child.source_requests,
        child.window_traces,
        child.retry_attempts,
        child.splits_completed,
    ) == (0, 0, 0, 0, 0, 0)


def test_two_pass_finalizer_calls_planner_zero_then_real_with_same_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    original = persistence.plan_continuous_public_trade_window  # type: ignore[attr-defined]

    def spy(*args: object, **kwargs: object) -> object:
        calls.append((args, kwargs))
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(persistence, "plan_continuous_public_trade_window", spy)
    plan, payload = finalize_continuous_public_trade_attachment(
        checkpoint(),
        policy(),
        candidate_job_id=CHILD_ID,
        child_policy_fingerprint=CHILD_POLICY_FP,
        now=COMMAND_TIME,
    )
    assert payload is not None and plan.attachment is not None
    assert len(calls) == 2
    assert calls[0][0] == calls[1][0]
    assert calls[0][1]["candidate_job_id"] == calls[1][1]["candidate_job_id"] == CHILD_ID
    assert calls[0][1]["candidate_creation_fingerprint"] == PROVISIONAL_CHILD_CREATION_FINGERPRINT
    assert calls[1][1]["candidate_creation_fingerprint"] == child_creation_fingerprint(payload)


def test_two_pass_finalizer_rejects_second_pass_plan_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = persistence.plan_continuous_public_trade_window  # type: ignore[attr-defined]
    call_count = 0

    def drifting_planner(*args: object, **kwargs: object) -> object:
        nonlocal call_count
        call_count += 1
        result = original(*args, **kwargs)  # type: ignore[arg-type]
        if call_count == 2:
            return result.model_copy(
                update={
                    "latest_eligible_end_epoch_ms": (result.latest_eligible_end_epoch_ms + 1_000)
                }
            )
        return result

    monkeypatch.setattr(
        persistence,
        "plan_continuous_public_trade_window",
        drifting_planner,
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: finalize_continuous_public_trade_attachment(
            checkpoint(),
            policy(),
            candidate_job_id=CHILD_ID,
            child_policy_fingerprint=CHILD_POLICY_FP,
            now=COMMAND_TIME,
        ),
    )
    assert call_count == 2


@pytest.mark.parametrize(
    ("job_id", "child_fp", "now"),
    [
        ("not-uuid", CHILD_POLICY_FP, COMMAND_TIME),
        (CHILD_ID, "sha256:" + ("A" * 64), COMMAND_TIME),
        (CHILD_ID, CHILD_POLICY_FP, datetime(2026, 1, 1)),
        (CHILD_ID, CHILD_POLICY_FP, DatetimeSubclass(2026, 1, 1, tzinfo=UTC)),
    ],
)
def test_finalizer_rejects_non_exact_inputs(
    job_id: object,
    child_fp: object,
    now: object,
) -> None:
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: finalize_continuous_public_trade_attachment(
            checkpoint(),
            policy(),
            candidate_job_id=job_id,  # type: ignore[arg-type]
            child_policy_fingerprint=child_fp,  # type: ignore[arg-type]
            now=now,  # type: ignore[arg-type]
        ),
    )


def test_payload_envelope_golden_shape_round_trip_and_independent_oracles() -> None:
    attachment, payload = due_payload()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=checkpoint(attachment=attachment, version=2),
        child_creation_payload=payload,
    )
    child_wire = encode_child_creation_payload(payload)
    envelope_wire = encode_stream_envelope(envelope)
    assert child_wire.startswith(b'{"child_checkpoint":{"created_at":"2026-07-27T12:34:56.123456Z"')
    assert b" " not in child_wire and b"\n" not in child_wire
    assert not child_wire.startswith(b"\xef\xbb\xbf")
    assert encode_child_creation_payload(decode_child_creation_payload(child_wire)) == child_wire
    assert encode_stream_envelope(decode_stream_envelope(envelope_wire)) == envelope_wire
    assert child_creation_fingerprint(payload) == oracle(DOMAINS["child"], child_wire)
    assert stream_envelope_digest(envelope) == oracle(DOMAINS["envelope"], envelope_wire)


def test_maximum_astral_identifiers_fit_attached_payload_and_envelope_caps() -> None:
    astral = "\U0001f4b1"
    maximum_checkpoint = checkpoint(
        source=astral * 128,
        venue=astral * 64,
        instrument=astral * 64,
        provider_symbol=astral * 64,
        request_variant=astral * 128,
    )
    attachment, payload = due_payload(maximum_checkpoint)
    envelope = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            maximum_checkpoint,
            attachment=attachment,
            version=2,
        ),
        child_creation_payload=payload,
    )
    child_wire = encode_child_creation_payload(payload)
    envelope_wire = encode_stream_envelope(envelope)
    assert len(child_wire) <= MAX_CHILD_CREATION_BYTES
    assert len(envelope_wire) <= MAX_ENVELOPE_BYTES
    # Version one has no canonical padding field. Even maximal escaped identifiers remain
    # structurally below the envelope cap, so no valid current model can manufacture an exact
    # 32,768-character successor hex merely to exercise that lexical equality branch.
    assert MAX_SUCCESSOR_ENVELOPE_HEX_CHARS == 2 * MAX_ENVELOPE_BYTES
    assert len(envelope_wire.hex()) < MAX_SUCCESSOR_ENVELOPE_HEX_CHARS
    assert decode_child_creation_payload(child_wire) == payload
    assert decode_stream_envelope(envelope_wire) == envelope


@given(st.integers(min_value=1, max_value=10**9))
def test_policy_projection_codec_property_preserves_every_integer(value: int) -> None:
    operating_policy = policy(
        window_size_ms=1,
        settlement_lag_ms=value,
        max_catchup_span_ms=value,
        max_jobs_per_invocation=value,
        max_requests_per_job=value,
        max_records_per_job=value,
    )
    projection = project_continuous_public_trade_policy(operating_policy)
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=STREAM_ID,
        transition_kind=None,
        prior_version=None,
        prior_envelope_digest=None,
        prior_history_root=None,
        successor_version=1,
        successor_envelope_digest=OTHER_DIGEST,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=projection,
    )
    assert decode_evidence_scope(encode_evidence_scope(scope)) == scope


def test_scope_has_hardcoded_wire_round_trip_and_digest() -> None:
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=STREAM_ID,
        transition_kind=None,
        prior_version=None,
        prior_envelope_digest=None,
        prior_history_root=None,
        successor_version=1,
        successor_envelope_digest=OTHER_DIGEST,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=project_continuous_public_trade_policy(policy()),
    )
    wire = encode_evidence_scope(scope)
    expected = (
        b'{"child_creation_fingerprint":null,"child_job_id":null,'
        b'"child_policy_fingerprint":null,"evidence_kind":"STREAM_CREATE_AUTHORITY",'
        b'"prior_envelope_digest":null,"prior_history_root":null,"prior_version":null,'
        b'"reason_code":null,"stream_id":"00000000-0000-4000-8000-000000000061",'
        b'"stream_policy":{"max_catchup_span_ms":5000,"max_jobs_per_invocation":3,'
        b'"max_records_per_job":10000,"max_requests_per_job":100,'
        b'"policy_fingerprint":"sha256:'
        + (b"1" * 64)
        + b'","schema_version":"1.0","settlement_lag_ms":250,"window_size_ms":1000},'
        b'"successor_envelope_digest":"sha256:'
        + (b"4" * 64)
        + b'","successor_version":1,"transition_kind":null}'
    )
    assert wire == expected
    assert decode_evidence_scope(wire) == scope
    assert evidence_scope_digest(scope) == oracle(DOMAINS["scope"], wire)


def test_creation_codec_scope_digest_and_initial_root() -> None:
    _, scope, record = creation_fixture()
    wire = encode_stream_creation_record(record)
    assert decode_stream_creation_record(wire) == record
    validate_stream_creation_record_scope(record, scope)
    assert stream_creation_digest(record) == oracle(DOMAINS["creation"], wire)
    assert initial_stream_history_root(record) == oracle(DOMAINS["initial"], wire)
    assert stream_creation_digest(record) != initial_stream_history_root(record)


@pytest.mark.parametrize("kind", list(ContinuousPublicTradeTransitionKind))
def test_every_legal_transition_round_trips_scopes_links_and_roots(
    kind: ContinuousPublicTradeTransitionKind,
) -> None:
    prior, authority, completion, record = transition_fixture(kind)
    wire = encode_stream_transition_record(record)
    assert decode_stream_transition_record(wire) == record
    validate_stream_transition_record_scopes(prior, record, authority, completion)
    validate_stream_transition_link(
        prior,
        record,
        policy=policy(),
        prior_history_root=record.prior_history_root,
        prior_recorded_at=COMMAND_TIME,
        transition_authority_scope=authority,
        child_completion_scope=completion,
    )
    assert stream_transition_digest(record) == oracle(DOMAINS["transition"], wire)
    raw_root = bytes.fromhex(record.prior_history_root.removeprefix("sha256:"))
    assert next_stream_history_root(record.prior_history_root, record) == oracle(
        DOMAINS["next"],
        raw_root + wire,
    )


def test_contiguous_create_attach_hold_resume_complete_retain_history_chain() -> None:
    current, creation_scope, creation = creation_fixture()
    validate_stream_creation_record_scope(creation, creation_scope)
    current_root = initial_stream_history_root(creation)
    prior_time = creation.recorded_at
    observed_roots = [current_root]

    attach_time = COMMAND_TIME + timedelta(seconds=1)
    attachment, payload = due_payload(current.checkpoint, at=attach_time)
    attached = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            current.checkpoint,
            attachment=attachment,
            version=2,
        ),
        child_creation_payload=payload,
    )
    held = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            attached.checkpoint,
            status=ContinuousPublicTradeStreamStatus.PAUSED,
            pause_reason="operator-hold",
            version=3,
        ),
        child_creation_payload=payload,
    )
    resumed = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            held.checkpoint,
            status=ContinuousPublicTradeStreamStatus.ACTIVE,
            pause_reason=None,
            version=4,
        ),
        child_creation_payload=payload,
    )
    completed = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            resumed.checkpoint,
            cursor_epoch_ms=attachment.window_end_epoch_ms,
            attachment=None,
            version=5,
        )
    )
    retained = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(completed.checkpoint, version=6)
    )
    steps = [
        (
            ContinuousPublicTradeTransitionKind.ATTACH,
            attached,
            attach_time,
            None,
        ),
        (
            ContinuousPublicTradeTransitionKind.MANUAL_HOLD,
            held,
            COMMAND_TIME + timedelta(seconds=2),
            "operator-hold",
        ),
        (
            ContinuousPublicTradeTransitionKind.MANUAL_RESUME,
            resumed,
            COMMAND_TIME + timedelta(seconds=3),
            None,
        ),
        (
            ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
            completed,
            COMMAND_TIME + timedelta(seconds=4),
            None,
        ),
        (
            ContinuousPublicTradeTransitionKind.RETAIN,
            retained,
            COMMAND_TIME + timedelta(seconds=5),
            "scheduled-retain",
        ),
    ]

    for kind, successor, recorded_at, reason_code in steps:
        authority, completion_scope, record = chained_transition(
            current,
            successor,
            kind,
            prior_history_root=current_root,
            recorded_at=recorded_at,
            reason_code=reason_code,
        )
        assert record.prior_version == current.checkpoint.version
        assert record.successor_version == successor.checkpoint.version
        assert record.prior_envelope_digest == stream_envelope_digest(current)
        assert record.prior_history_root == current_root
        validate_stream_transition_record_scopes(
            current,
            record,
            authority,
            completion_scope,
        )
        validate_stream_transition_link(
            current,
            record,
            policy=policy(),
            prior_history_root=current_root,
            prior_recorded_at=prior_time,
            transition_authority_scope=authority,
            child_completion_scope=completion_scope,
        )
        transition_wire = encode_stream_transition_record(record)
        expected_root = oracle(
            DOMAINS["next"],
            bytes.fromhex(current_root.removeprefix("sha256:")) + transition_wire,
        )
        current_root = next_stream_history_root(current_root, record)
        assert current_root == expected_root
        observed_roots.append(current_root)
        current = successor
        prior_time = recorded_at

    assert current == retained
    assert current.checkpoint.version == 6
    assert len(set(observed_roots)) == 6


def test_attach_scope_breaks_digest_cycle_and_completion_requires_second_scope() -> None:
    prior, authority, completion, attach = transition_fixture(
        ContinuousPublicTradeTransitionKind.ATTACH
    )
    successor = decode_stream_envelope(bytes.fromhex(attach.successor_envelope_hex))
    assert authority.successor_envelope_digest is None
    assert authority.child_job_id == CHILD_ID
    assert authority.child_policy_fingerprint == CHILD_POLICY_FP
    assert authority.child_creation_fingerprint is None
    assert attach.successor_envelope_digest == stream_envelope_digest(successor)
    assert completion is None

    prior, authority, completion, completed = transition_fixture(
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
    )
    assert completion is not None and prior.checkpoint.attachment is not None
    assert completion.child_job_id == prior.checkpoint.attachment.job_id
    assert completion.child_creation_fingerprint == prior.checkpoint.attachment.creation_fingerprint
    assert completed.child_completion_reference is not None
    assert (
        completed.child_completion_reference.outcome
        is ContinuousPublicTradeEvidenceOutcome.ACCEPTED
    )
    assert completion.prior_history_root == authority.prior_history_root


def test_scope_validators_reject_same_fingerprint_policy_mismatch_and_missing_scope() -> None:
    _, scope, record = creation_fixture()
    assert scope.stream_policy is not None
    mismatched = ContinuousPublicTradePolicyProjectionV1(
        **{**scope.stream_policy.model_dump(), "max_requests_per_job": 101}
    )
    assert mismatched.policy_fingerprint == record.stream_policy.policy_fingerprint
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_creation_record_scope(
            record,
            scope.model_copy(update={"stream_policy": mismatched}),
        ),
    )
    prior, authority, _, completed = transition_fixture(
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_transition_record_scopes(
            prior,
            completed,
            authority,
            None,
        ),
    )


def test_load_bindings_require_complete_policies_child_policy_and_exact_v1_envelope() -> None:
    initial, _, creation = creation_fixture()
    validate_stream_load_bindings(
        creation,
        initial,
        effective_stream_policy=policy(),
        effective_child_policy_fingerprint=None,
    )
    mismatched_policy = policy(max_requests_per_job=101)
    assert mismatched_policy.policy_fingerprint == POLICY_FP
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_load_bindings(
            creation,
            initial,
            effective_stream_policy=mismatched_policy,
            effective_child_policy_fingerprint=None,
        ),
    )
    different_v1 = unattached_envelope(cursor_epoch_ms=1_000)
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_load_bindings(
            creation,
            different_v1,
            effective_stream_policy=policy(),
            effective_child_policy_fingerprint=None,
        ),
    )
    attached = attached_envelope(initial.checkpoint)
    validate_stream_load_bindings(
        creation,
        attached,
        effective_stream_policy=policy(),
        effective_child_policy_fingerprint=CHILD_POLICY_FP,
    )
    for child_fingerprint in (None, OTHER_DIGEST):
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
            partial(
                validate_stream_load_bindings,
                creation,
                attached,
                effective_stream_policy=policy(),
                effective_child_policy_fingerprint=child_fingerprint,
            ),
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("stream_id", UUID(int=999)),
        ("source", "other.public-rest"),
        ("venue", "OTHER"),
        ("instrument", "ETH-USDT"),
        ("provider_symbol", "ETHUSDT"),
        ("instrument_type", InstrumentType.PERPETUAL_FUTURE),
        ("request_variant", "other-trades"),
        ("policy_fingerprint", OTHER_DIGEST),
        ("stream_start_epoch_ms", 1_000),
    ],
)
def test_load_rejects_each_immutable_creation_identity_mutation(
    field: str,
    replacement: object,
) -> None:
    initial, _, creation = creation_fixture()
    updates: dict[str, object] = {"version": 2, field: replacement}
    if field == "stream_start_epoch_ms":
        updates["cursor_epoch_ms"] = replacement
    hostile = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(initial.checkpoint, **updates)
    )

    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_load_bindings(
            creation,
            hostile,
            effective_stream_policy=policy(),
            effective_child_policy_fingerprint=None,
        ),
    )


def test_public_encoders_reject_model_copy_construct_and_partial_nested_bypasses() -> None:
    attachment, payload = due_payload()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=checkpoint(attachment=attachment),
        child_creation_payload=payload,
    )
    _, scope, creation = creation_fixture()
    _, _, _, transition = transition_fixture(ContinuousPublicTradeTransitionKind.RETAIN)
    hostile_values: list[tuple[Callable[[Any], bytes], object]] = [
        (
            encode_child_creation_payload,
            payload.model_copy(update={"serialization_version": True}),
        ),
        (
            encode_stream_envelope,
            envelope.model_copy(
                update={
                    "checkpoint": ContinuousPublicTradeStreamCheckpoint.model_construct(
                        stream_id=STREAM_ID
                    )
                }
            ),
        ),
        (
            encode_child_creation_payload,
            payload.model_copy(
                update={
                    "child_checkpoint": PublicTradeCollectionCheckpoint.model_construct(
                        job_id=CHILD_ID
                    )
                }
            ),
        ),
        (
            encode_evidence_scope,
            scope.model_copy(update={"successor_version": True}),
        ),
        (
            encode_stream_creation_record,
            creation.model_construct(),
        ),
        (
            encode_stream_transition_record,
            transition.model_copy(update={"prior_version": True}),
        ),
    ]
    for encoder, hostile in hostile_values:
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
            partial(encoder, hostile),
        )


def test_external_models_with_hidden_extra_storage_are_rejected() -> None:
    hostile_policy = policy().model_copy(update={"evil": 1})
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: project_continuous_public_trade_policy(hostile_policy),
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: finalize_continuous_public_trade_attachment(
            checkpoint().model_copy(update={"evil": 1}),
            policy(),
            candidate_job_id=CHILD_ID,
            child_policy_fingerprint=CHILD_POLICY_FP,
            now=COMMAND_TIME,
        ),
    )
    attachment, payload = due_payload()
    hostile_attachment = attachment.model_copy(update={"evil": 1})
    hostile_checkpoint = checkpoint(attachment=attachment).model_copy(
        update={"attachment": hostile_attachment}
    )
    with pytest.raises(ValidationError):
        ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=hostile_checkpoint,
            child_creation_payload=payload,
        )
    hostile_child = payload.child_checkpoint.model_copy(update={"evil": 1})
    hostile_payload = payload.model_copy(update={"child_checkpoint": hostile_child})
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: encode_child_creation_payload(hostile_payload),
    )


def test_top_level_codec_values_with_injected_private_state_are_rejected() -> None:
    attachment, payload = due_payload()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=checkpoint(attachment=attachment),
        child_creation_payload=payload,
    )
    _, scope, creation = creation_fixture()
    _, _, _, transition = transition_fixture(ContinuousPublicTradeTransitionKind.RETAIN)
    hostile_values: list[tuple[Callable[[Any], bytes], object]] = [
        (encode_child_creation_payload, payload),
        (encode_stream_envelope, envelope),
        (encode_evidence_scope, scope),
        (encode_stream_creation_record, creation),
        (encode_stream_transition_record, transition),
    ]
    for encoder, hostile in hostile_values:
        object.__setattr__(hostile, "__pydantic_private__", {"evil": 1})
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
            partial(encoder, hostile),
        )


def test_envelope_rejects_injected_private_state_on_nested_task059_checkpoint() -> None:
    nested_checkpoint = checkpoint()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(checkpoint=nested_checkpoint)
    object.__setattr__(nested_checkpoint, "__pydantic_private__", {"evil": 1})

    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        partial(encode_stream_envelope, envelope),
    )


def test_full_range_unattached_is_preserved_but_unrepresentable_child_fails_closed() -> None:
    maximum = 2**63 - 1
    full_range = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=checkpoint(
            stream_start_epoch_ms=maximum,
            cursor_epoch_ms=maximum,
        )
    )
    assert decode_stream_envelope(encode_stream_envelope(full_range)) == full_range

    task059_attachment = ContinuousPublicTradeAttachment(
        job_id=CHILD_ID,
        window_start_epoch_ms=10**18,
        window_end_epoch_ms=10**18 + 1,
        policy_fingerprint=POLICY_FP,
        creation_fingerprint=OTHER_DIGEST,
    )
    task059_attached = checkpoint(
        stream_start_epoch_ms=10**18,
        cursor_epoch_ms=10**18,
        attachment=task059_attachment,
    )
    with pytest.raises(ValidationError):
        ContinuousPublicTradeStreamEnvelopeV1(checkpoint=task059_attached)


def test_transition_scope_rejects_predecessor_transplant() -> None:
    prior, authority, completion, record = transition_fixture(
        ContinuousPublicTradeTransitionKind.RETAIN
    )
    unrelated = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(prior.checkpoint, stream_id=UUID(int=999))
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_transition_record_scopes(
            unrelated,
            record,
            authority,
            completion,
        ),
    )


@pytest.mark.parametrize(
    "updates",
    [
        {"window_size_ms": True},
        {"max_jobs_per_invocation": False},
        {"settlement_lag_ms": "bad"},
    ],
)
def test_load_and_link_reject_forged_task059_policies(
    updates: dict[str, object],
) -> None:
    initial, _, creation = creation_fixture()
    forged = policy().model_copy(update=updates)
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_load_bindings(
            creation,
            initial,
            effective_stream_policy=forged,
            effective_child_policy_fingerprint=None,
        ),
    )
    prior, authority, completion, record = transition_fixture(
        ContinuousPublicTradeTransitionKind.RETAIN
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_transition_link(
            prior,
            record,
            policy=forged,
            prior_history_root=record.prior_history_root,
            prior_recorded_at=COMMAND_TIME,
            transition_authority_scope=authority,
            child_completion_scope=completion,
        ),
    )


@pytest.mark.parametrize(
    "mutation",
    [
        {"prior_version": 2},
        {"stream_id": UUID(int=999)},
        {"prior_envelope_digest": OTHER_DIGEST},
        {"recorded_at": COMMAND_TIME - timedelta(microseconds=1)},
    ],
)
def test_link_rejects_stale_conflicting_and_regressive_records(
    mutation: dict[str, object],
) -> None:
    prior, authority, completion, record = transition_fixture(
        ContinuousPublicTradeTransitionKind.RETAIN
    )
    hostile = record.model_copy(update=mutation)
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_transition_link(
            prior,
            hostile,
            policy=policy(),
            prior_history_root=record.prior_history_root,
            prior_recorded_at=COMMAND_TIME,
            transition_authority_scope=authority,
            child_completion_scope=completion,
        ),
    )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("stream_id", UUID(int=999)),
        ("source", "other.public-rest"),
        ("venue", "OTHER"),
        ("instrument", "ETH-USDT"),
        ("provider_symbol", "ETHUSDT"),
        ("instrument_type", InstrumentType.PERPETUAL_FUTURE),
        ("request_variant", "other-trades"),
        ("policy_fingerprint", OTHER_DIGEST),
        ("stream_start_epoch_ms", 1_000),
    ],
)
def test_link_rejects_each_immutable_identity_field_mutation(
    field: str,
    replacement: object,
) -> None:
    prior = unattached_envelope(cursor_epoch_ms=10_000)
    successor = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(prior.checkpoint, version=2)
    )
    authority, completion, record = chained_transition(
        prior,
        successor,
        ContinuousPublicTradeTransitionKind.RETAIN,
        prior_history_root=OTHER_DIGEST,
        recorded_at=COMMAND_TIME + timedelta(seconds=1),
        reason_code="scheduled-retain",
    )
    hostile_successor = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            successor.checkpoint,
            **{field: replacement},
        )
    )
    hostile_record = record.model_copy(
        update={
            "successor_envelope_hex": encode_stream_envelope(hostile_successor).hex(),
            "successor_envelope_digest": stream_envelope_digest(hostile_successor),
        }
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: validate_stream_transition_link(
            prior,
            hostile_record,
            policy=policy(),
            prior_history_root=OTHER_DIGEST,
            prior_recorded_at=COMMAND_TIME,
            transition_authority_scope=authority,
            child_completion_scope=completion,
        ),
    )


@pytest.mark.parametrize(
    ("encoder", "decoder", "value"),
    [
        (encode_child_creation_payload, decode_child_creation_payload, due_payload()[1]),
        (encode_stream_envelope, decode_stream_envelope, attached_envelope()),
        (encode_evidence_scope, decode_evidence_scope, creation_fixture()[1]),
        (encode_stream_creation_record, decode_stream_creation_record, creation_fixture()[2]),
        (
            encode_stream_transition_record,
            decode_stream_transition_record,
            transition_fixture(ContinuousPublicTradeTransitionKind.ATTACH)[3],
        ),
    ],
)
def test_all_codecs_reject_noncanonical_duplicate_missing_extra_and_unknown_version(
    encoder: Callable[[Any], bytes],
    decoder: Callable[[object], object],
    value: object,
) -> None:
    canonical = encoder(value)
    parsed = json.loads(canonical)
    assert isinstance(parsed, dict)
    first_key = next(iter(parsed))
    duplicate = b'{"' + first_key.encode() + b'":null,' + canonical[1:]
    missing = dict(parsed)
    missing.pop(first_key)
    extra = {**parsed, "unexpected": None}
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.NON_CANONICAL,
        lambda: decoder(b" " + canonical),
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.DUPLICATE_KEY,
        lambda: decoder(duplicate),
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        lambda: decoder(json.dumps(missing, separators=(",", ":"), sort_keys=True).encode()),
    )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        lambda: decoder(json.dumps(extra, separators=(",", ":"), sort_keys=True).encode()),
    )
    version = "serialization_version" if "serialization_version" in parsed else "successor_version"
    expected_version_code = (
        ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
        if decoder is decode_evidence_scope
        else ContinuousPublicTradePersistenceErrorCode.UNSUPPORTED_VERSION
    )
    assert_error(
        expected_version_code,
        lambda: decoder(mutate_json(canonical, **{version: 999})),
    )


@pytest.mark.parametrize(
    "raw",
    [b"", b"\xef\xbb\xbf{}", b"\xff", b'{"x":NaN}', b'{"x":1.0}', b'{"x":1e1}'],
)
def test_raw_failures_are_one_sanitized_boundary(raw: bytes) -> None:
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.RAW_INPUT,
        lambda: decode_evidence_scope(raw),
    )


def test_semantically_equivalent_noncanonical_json_is_rejected() -> None:
    _, scope, _ = creation_fixture()
    wire = encode_evidence_scope(scope)
    parsed = json.loads(wire, object_pairs_hook=dict)
    reordered = json.dumps(
        dict(reversed(list(parsed.items()))),
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    alternate_escape = wire.replace(
        b"STREAM_CREATE_AUTHORITY",
        b"\\u0053TREAM_CREATE_AUTHORITY",
    )
    for hostile in (reordered, wire + b"\n", alternate_escape):
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.NON_CANONICAL,
            partial(decode_evidence_scope, hostile),
        )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        partial(
            decode_evidence_scope,
            wire.replace(b'"successor_version":1', b'"successor_version":-0'),
        ),
    )


def test_six_digest_domains_are_not_substitutable() -> None:
    attachment, payload = due_payload()
    envelope = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=checkpoint(attachment=attachment),
        child_creation_payload=payload,
    )
    _, scope, creation = creation_fixture()
    _, _, _, transition = transition_fixture(ContinuousPublicTradeTransitionKind.RETAIN)
    canonical_values = [
        encode_child_creation_payload(payload),
        encode_stream_envelope(envelope),
        encode_evidence_scope(scope),
        encode_stream_creation_record(creation),
        encode_stream_transition_record(transition),
    ]
    domains = [
        DOMAINS["child"],
        DOMAINS["envelope"],
        DOMAINS["scope"],
        DOMAINS["creation"],
        DOMAINS["transition"],
    ]
    expected = [
        child_creation_fingerprint(payload),
        stream_envelope_digest(envelope),
        evidence_scope_digest(scope),
        stream_creation_digest(creation),
        stream_transition_digest(transition),
    ]
    for index, (canonical, digest) in enumerate(zip(canonical_values, expected, strict=True)):
        for other_index, domain in enumerate(domains):
            if other_index != index:
                assert oracle(domain, canonical) != digest
    assert initial_stream_history_root(creation) != stream_creation_digest(creation)


def test_exact_parser_limits_and_limit_plus_one() -> None:
    cases = [
        (
            decode_evidence_scope,
            b" " * (MAX_RAW_RECORD_BYTES - 2) + b"{}",
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_child_creation_payload,
            b" " * (MAX_CHILD_CREATION_BYTES - 2) + b"{}",
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_stream_envelope,
            b" " * (MAX_ENVELOPE_BYTES - 2) + b"{}",
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_evidence_scope,
            b'{"x":"' + (b"a" * MAX_JSON_STRING_LEXICAL_BYTES) + b'"}',
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_evidence_scope,
            (b'{"x":' * MAX_JSON_DEPTH) + b"null" + (b"}" * MAX_JSON_DEPTH),
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_evidence_scope,
            b"{" + b",".join(f'"k{i}":null'.encode() for i in range(MAX_JSON_MEMBERS)) + b"}",
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_evidence_scope,
            b'{"' + (b"k" * MAX_JSON_KEY_BYTES) + b'":null}',
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
        (
            decode_evidence_scope,
            b'{"x":' + (b"9" * MAX_JSON_INTEGER_DIGITS) + b"}",
            ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        ),
    ]
    for decoder, exact, exact_code in cases:
        assert_error(exact_code, partial(decoder, exact))
        if decoder is decode_evidence_scope and len(exact) == MAX_RAW_RECORD_BYTES:
            excessive = exact + b" "
        elif b'"x":"' in exact:
            excessive = exact[:-2] + b'a"}'
        elif exact.startswith(b'{"x":{"x":'):
            excessive = b'{"x":' + exact + b"}"
        elif exact.count(b":null") == MAX_JSON_MEMBERS:
            excessive = exact[:-1] + b',"overflow":null}'
        elif exact.startswith(b'{"kk'):
            excessive = exact[: 2 + MAX_JSON_KEY_BYTES] + b"k" + exact[2 + MAX_JSON_KEY_BYTES :]
        elif b'"x":999' in exact:
            excessive = exact[:-1] + b"9}"
        else:
            excessive = exact + b" "
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.RAW_INPUT,
            partial(decoder, excessive),
        )


@pytest.mark.parametrize(
    "mutator",
    [
        str.upper,
        lambda value: value + "0",
        lambda _value: "g0",
        lambda _value: "0" * (MAX_SUCCESSOR_ENVELOPE_HEX_CHARS + 2),
    ],
)
def test_successor_hex_rejects_uppercase_odd_nonhex_and_oversize(
    mutator: Callable[[str], str],
) -> None:
    _, _, record = creation_fixture()
    mutated = mutator(record.successor_envelope_hex)
    expected = (
        ContinuousPublicTradePersistenceErrorCode.RAW_INPUT
        if len(mutated) > MAX_SUCCESSOR_ENVELOPE_HEX_CHARS
        else ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD
    )
    assert_error(
        expected,
        lambda: decode_stream_creation_record(
            mutate_json(
                encode_stream_creation_record(record),
                successor_envelope_hex=mutated,
            )
        ),
    )


def test_decode_rejects_bool_polymorphic_and_nonbytes() -> None:
    _, scope, _ = creation_fixture()
    wire = encode_evidence_scope(scope)
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.MALFORMED_RECORD,
        lambda: decode_evidence_scope(mutate_json(wire, successor_version=True)),
    )
    for raw in (bytearray(wire), StringSubclass(wire.decode())):
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.RAW_INPUT,
            partial(decode_evidence_scope, raw),
        )


def test_envelope_rejects_absent_extra_or_mismatched_creation_material() -> None:
    attachment, payload = due_payload()
    for values in (
        {"checkpoint": checkpoint(attachment=attachment)},
        {"checkpoint": checkpoint(), "child_creation_payload": payload},
        {
            "checkpoint": checkpoint(attachment=attachment),
            "child_creation_payload": payload.model_copy(
                update={"stream_policy_fingerprint": OTHER_DIGEST}
            ),
        },
    ):
        with pytest.raises(ValidationError):
            ContinuousPublicTradeStreamEnvelopeV1(**values)


def test_evidence_reference_rejects_kind_outcome_mismatch_and_empty_interval() -> None:
    approved = reference(
        ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        OTHER_DIGEST,
    )
    accepted = reference(
        ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
        OTHER_DIGEST,
    )
    hostile_values = [
        {
            **approved.model_dump(),
            "outcome": ContinuousPublicTradeEvidenceOutcome.ACCEPTED,
        },
        {
            **accepted.model_dump(),
            "outcome": ContinuousPublicTradeEvidenceOutcome.APPROVED,
        },
        {
            **approved.model_dump(),
            "expires_at": approved.valid_from,
        },
    ]
    for hostile in hostile_values:
        with pytest.raises(ValidationError):
            ContinuousPublicTradeEvidenceReferenceV1(**hostile)


def test_evidence_reference_identifier_visible_ascii_bounds() -> None:
    maximal = ContinuousPublicTradeEvidenceReferenceV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        evidence_id="~" * 128,
        evidence_digest=EVIDENCE_DIGEST,
        scope_digest=OTHER_DIGEST,
        outcome=ContinuousPublicTradeEvidenceOutcome.APPROVED,
        valid_from=COMMAND_TIME,
    )
    assert maximal.evidence_id == "~" * 128

    for evidence_id in ("", "a" * 129, "\x00", "\x1f", "\x7f", "has space", "line\nbreak"):
        with pytest.raises(ValidationError):
            ContinuousPublicTradeEvidenceReferenceV1(
                evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
                evidence_id=evidence_id,
                evidence_digest=EVIDENCE_DIGEST,
                scope_digest=OTHER_DIGEST,
                outcome=ContinuousPublicTradeEvidenceOutcome.APPROVED,
                valid_from=COMMAND_TIME,
            )


def test_evidence_scope_rejects_illegal_kind_nullability_and_reason_combinations() -> None:
    _, create_scope, _ = creation_fixture()
    _, attach_scope, _, _ = transition_fixture(ContinuousPublicTradeTransitionKind.ATTACH)
    _, retain_scope, _, _ = transition_fixture(ContinuousPublicTradeTransitionKind.RETAIN)
    _, hold_scope, _, _ = transition_fixture(ContinuousPublicTradeTransitionKind.MANUAL_HOLD)
    _, _, completion_scope, _ = transition_fixture(
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED
    )
    assert completion_scope is not None

    hostile_values = [
        {**create_scope.model_dump(), "stream_policy": None},
        {**create_scope.model_dump(), "prior_version": 1},
        {**attach_scope.model_dump(), "successor_envelope_digest": OTHER_DIGEST},
        {**attach_scope.model_dump(), "child_job_id": None},
        {**attach_scope.model_dump(), "child_policy_fingerprint": None},
        {**attach_scope.model_dump(), "child_creation_fingerprint": OTHER_DIGEST},
        {**attach_scope.model_dump(), "reason_code": "not-allowed"},
        {**retain_scope.model_dump(), "reason_code": None},
        {**hold_scope.model_dump(), "reason_code": None},
        {**completion_scope.model_dump(), "child_creation_fingerprint": None},
        {**completion_scope.model_dump(), "successor_envelope_digest": None},
        {**completion_scope.model_dump(), "reason_code": "not-allowed"},
        {
            **completion_scope.model_dump(),
            "transition_kind": ContinuousPublicTradeTransitionKind.ATTACH,
        },
    ]
    for hostile in hostile_values:
        with pytest.raises(ValidationError):
            ContinuousPublicTradeEvidenceScopeV1(**hostile)


def test_attach_record_rejects_child_timestamps_different_from_recorded_at() -> None:
    prior, _, creation = creation_fixture()
    prior_root = initial_stream_history_root(creation)
    attachment, payload = due_payload(prior.checkpoint, at=COMMAND_TIME)
    successor = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=copy_checkpoint(
            prior.checkpoint,
            attachment=attachment,
            version=2,
        ),
        child_creation_payload=payload,
    )

    with pytest.raises(ValidationError):
        chained_transition(
            prior,
            successor,
            ContinuousPublicTradeTransitionKind.ATTACH,
            prior_history_root=prior_root,
            recorded_at=COMMAND_TIME + timedelta(microseconds=1),
        )


def test_reference_and_record_creation_validation_fail_closed() -> None:
    for updates in (
        {"evidence_id": "contains space"},
        {"evidence_id": StringSubclass("evidence")},
        {"reference_version": True},
        {"evidence_digest": "sha256:" + ("A" * 64)},
        {"valid_from": datetime(2026, 1, 1)},
        {"expires_at": COMMAND_TIME - timedelta(minutes=2)},
    ):
        values: dict[str, object] = {
            "evidence_kind": ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
            "evidence_id": "evidence",
            "evidence_digest": EVIDENCE_DIGEST,
            "scope_digest": OTHER_DIGEST,
            "outcome": ContinuousPublicTradeEvidenceOutcome.APPROVED,
            "valid_from": COMMAND_TIME - timedelta(minutes=1),
        }
        values.update(updates)
        with pytest.raises(ValidationError):
            ContinuousPublicTradeEvidenceReferenceV1.model_validate(values)

    _, _, creation = creation_fixture()
    expired = creation.create_authority_reference.model_copy(update={"expires_at": COMMAND_TIME})
    with pytest.raises(ValidationError):
        ContinuousPublicTradeStreamCreationRecordV1(
            **{**creation.model_dump(), "create_authority_reference": expired}
        )


def test_history_root_rejects_hostile_prior_digest() -> None:
    _, _, _, transition = transition_fixture(ContinuousPublicTradeTransitionKind.RETAIN)
    for invalid in (OTHER_DIGEST.upper(), "sha256:00", StringSubclass(OTHER_DIGEST)):
        assert_error(
            ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
            partial(next_stream_history_root, invalid, transition),
        )
    assert_error(
        ContinuousPublicTradePersistenceErrorCode.INCONSISTENT,
        lambda: next_stream_history_root(OTHER_DIGEST, transition),
    )
