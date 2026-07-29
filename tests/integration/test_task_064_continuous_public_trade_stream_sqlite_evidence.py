"""Generated-data integration evidence for the TASK-064 SQLite harness."""

from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import os
import selectors
import signal
import sqlite3
import threading
import time
import tracemalloc
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pytest

from wealth.domain.continuous_public_trade import (
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
    decode_child_creation_payload,
    decode_stream_creation_record,
    decode_stream_envelope,
    decode_stream_transition_record,
    encode_child_creation_payload,
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
from wealth.ports.continuous_public_trade_stream_store import (
    ContinuousPublicTradeStreamAuditContinuationQueryV1,
    ContinuousPublicTradeStreamAuditContinuationV1,
    ContinuousPublicTradeStreamAuditOutcome,
    ContinuousPublicTradeStreamAuditPageResultV1,
    ContinuousPublicTradeStreamAuditStartQueryV1,
    ContinuousPublicTradeStreamCompareAndSwapCommandV1,
    ContinuousPublicTradeStreamCompareAndSwapOutcome,
    ContinuousPublicTradeStreamCreateCommandV1,
    ContinuousPublicTradeStreamCreateOutcome,
    ContinuousPublicTradeStreamExpectationV1,
    ContinuousPublicTradeStreamIdentityV1,
    ContinuousPublicTradeStreamLoadOutcome,
    ContinuousPublicTradeStreamLoadQueryV1,
    ContinuousPublicTradeStreamStoredCreationV1,
    ContinuousPublicTradeStreamStoredEnvelopeV1,
    ContinuousPublicTradeStreamStoredHistoryEntryV1,
    ContinuousPublicTradeStreamStoredTransitionV1,
)

if TYPE_CHECKING:
    import support.continuous_public_trade_stream_sqlite_harness as harness
else:
    import tests.support.continuous_public_trade_stream_sqlite_harness as harness

RECORDED_AT = datetime(2026, 7, 29, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _active_task064_pytest_root(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    with harness._pytest_root_scope(tmp_path, node_id=request.node.nodeid):
        yield


def _digest(label: str) -> str:
    return f"sha256:{hashlib.sha256(label.encode('ascii')).hexdigest()}"


def _policy(
    seed: int = 1,
    *,
    window_size_ms: int = 1_000,
) -> ContinuousPublicTradePolicy:
    return ContinuousPublicTradePolicy(
        window_size_ms=window_size_ms,
        settlement_lag_ms=250,
        max_catchup_span_ms=5_000,
        max_jobs_per_invocation=3,
        max_requests_per_job=100,
        max_records_per_job=10_000,
        policy_fingerprint=_digest(f"policy-{seed}"),
    )


def _checkpoint(
    seed: int = 1,
    *,
    stream_start_epoch_ms: int = 0,
    policy: ContinuousPublicTradePolicy | None = None,
    stream_id_seed: int | None = None,
    identity_seed: int | None = None,
) -> ContinuousPublicTradeStreamCheckpoint:
    effective_policy = policy or _policy(seed)
    uuid_seed = seed if stream_id_seed is None else stream_id_seed
    atom_seed = seed if identity_seed is None else identity_seed
    return ContinuousPublicTradeStreamCheckpoint(
        stream_id=UUID(f"00000000-0000-4000-8000-{uuid_seed:012d}"),
        source=f"source-{atom_seed}",
        venue=f"VENUE-{atom_seed}",
        instrument=f"ASSET-{atom_seed}-USD",
        provider_symbol=f"ASSET{atom_seed}USD",
        instrument_type=InstrumentType.SPOT,
        request_variant=f"public-trades-{atom_seed}",
        policy_fingerprint=effective_policy.policy_fingerprint,
        stream_start_epoch_ms=stream_start_epoch_ms,
        cursor_epoch_ms=stream_start_epoch_ms,
        status=ContinuousPublicTradeStreamStatus.ACTIVE,
        version=1,
    )


def _reference(
    kind: ContinuousPublicTradeEvidenceKind,
    scope_digest: str,
    *,
    suffix: str,
    recorded_at: datetime,
) -> ContinuousPublicTradeEvidenceReferenceV1:
    return ContinuousPublicTradeEvidenceReferenceV1(
        evidence_kind=kind,
        evidence_id=f"task064-{suffix}",
        evidence_digest=_digest(f"evidence-{suffix}"),
        scope_digest=scope_digest,
        outcome=(
            ContinuousPublicTradeEvidenceOutcome.ACCEPTED
            if kind is ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION
            else ContinuousPublicTradeEvidenceOutcome.APPROVED
        ),
        valid_from=recorded_at - timedelta(minutes=1),
        expires_at=recorded_at + timedelta(minutes=1),
    )


def _stored_envelope(
    envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> ContinuousPublicTradeStreamStoredEnvelopeV1:
    return ContinuousPublicTradeStreamStoredEnvelopeV1(
        envelope=envelope,
        canonical_bytes=encode_stream_envelope(envelope),
        envelope_digest=stream_envelope_digest(envelope),
    )


def _creation(
    seed: int = 1,
    *,
    stream_start_epoch_ms: int = 0,
    stream_id_seed: int | None = None,
    identity_seed: int | None = None,
) -> tuple[ContinuousPublicTradePolicy, ContinuousPublicTradeStreamStoredCreationV1]:
    policy = _policy(
        seed,
        window_size_ms=1 if stream_start_epoch_ms == (2**63) - 1 else 1_000,
    )
    initial = _checkpoint(
        seed,
        stream_start_epoch_ms=stream_start_epoch_ms,
        policy=policy,
        stream_id_seed=stream_id_seed,
        identity_seed=identity_seed,
    )
    successor = _stored_envelope(ContinuousPublicTradeStreamEnvelopeV1(checkpoint=initial))
    projection = project_continuous_public_trade_policy(policy)
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=initial.stream_id,
        transition_kind=None,
        prior_version=None,
        prior_envelope_digest=None,
        prior_history_root=None,
        successor_version=1,
        successor_envelope_digest=successor.envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=projection,
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
        stream_policy=projection,
        successor_envelope_hex=successor.canonical_bytes.hex(),
        successor_envelope_digest=successor.envelope_digest,
        create_authority_reference=_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
            evidence_scope_digest(scope),
            suffix=f"create-{seed}",
            recorded_at=RECORDED_AT,
        ),
        recorded_at=RECORDED_AT,
    )
    stored = ContinuousPublicTradeStreamStoredCreationV1(
        record=record,
        canonical_bytes=encode_stream_creation_record(record),
        record_digest=stream_creation_digest(record),
        successor_envelope=successor,
        history_root=initial_stream_history_root(record),
        create_authority_scope=scope,
    )
    return policy, stored


def _retain(
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    policy: ContinuousPublicTradePolicy,
    *,
    reason: str = "retained-for-task064-evidence",
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    prior_checkpoint = prior.successor_envelope.envelope.checkpoint
    successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
        prior_checkpoint.model_dump() | {"version": prior_checkpoint.version + 1}
    )
    successor = _stored_envelope(
        ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=successor_checkpoint,
            child_creation_payload=prior.successor_envelope.envelope.child_creation_payload,
        )
    )
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=prior_checkpoint.stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_version=prior_checkpoint.version,
        prior_envelope_digest=prior.successor_envelope.envelope_digest,
        prior_history_root=prior.history_root,
        successor_version=successor_checkpoint.version,
        successor_envelope_digest=successor.envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=reason,
        stream_policy=None,
    )
    recorded_at = RECORDED_AT + timedelta(microseconds=successor_checkpoint.version)
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=prior_checkpoint.stream_id,
        prior_version=prior_checkpoint.version,
        successor_version=successor_checkpoint.version,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_history_root=prior.history_root,
        prior_envelope_digest=prior.successor_envelope.envelope_digest,
        successor_envelope_hex=successor.canonical_bytes.hex(),
        successor_envelope_digest=successor.envelope_digest,
        reason_code=reason,
        transition_authority_reference=_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(scope),
            suffix=f"retain-{successor_checkpoint.version}",
            recorded_at=recorded_at,
        ),
        recorded_at=recorded_at,
    )
    stored = ContinuousPublicTradeStreamStoredTransitionV1(
        record=record,
        canonical_bytes=encode_stream_transition_record(record),
        record_digest=stream_transition_digest(record),
        successor_envelope=successor,
        history_root=next_stream_history_root(prior.history_root, record),
        transition_authority_scope=scope,
        child_completion_scope=None,
    )
    validate_stream_transition_link(
        prior.successor_envelope.envelope,
        record,
        policy=policy,
        prior_history_root=prior.history_root,
        prior_recorded_at=prior.record.recorded_at,
        transition_authority_scope=scope,
        child_completion_scope=None,
    )
    return stored


def _with_recorded_at(
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    recorded_at: datetime,
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    record = transition.record.model_copy(update={"recorded_at": recorded_at})
    return transition.model_copy(
        update={
            "record": record,
            "canonical_bytes": encode_stream_transition_record(record),
            "record_digest": stream_transition_digest(record),
            "history_root": next_stream_history_root(prior.history_root, record),
        }
    )


def _natural_key(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> bytes:
    record = creation.record
    return harness.natural_identity_key(
        source=record.source,
        venue=record.venue,
        instrument=record.instrument,
        provider_symbol=record.provider_symbol,
        instrument_type=record.instrument_type.value,
        request_variant=record.request_variant,
    )


def _truncate_generated_wal(token: harness.StoreToken) -> None:
    connection, _ = harness._connect(token, writer=True)
    try:
        assert tuple(harness._fetch_one(connection, "PRAGMA wal_checkpoint(TRUNCATE)")) == (
            0,
            0,
            0,
        )
    finally:
        harness._close_preserving_primary(connection)


def _complete_evidence_gates() -> tuple[tuple[str, harness.EvidenceDisposition, str | None], ...]:
    return tuple(
        (name, harness.EvidenceDisposition.PASS, None) for name in harness.GENERATED_EVIDENCE_GATES
    ) + tuple(
        (
            name,
            harness.EvidenceDisposition.NOT_APPLICABLE,
            harness.TARGET_NOT_APPLICABLE_REASON,
        )
        for name in harness.TARGET_NOT_APPLICABLE_GATES
    )


def _evidence_report(
    summary: harness.VerificationSummary,
    *,
    backup_manifest: harness.BackupManifest,
    recorded_at: str,
    query_rows: int,
    maximum_open_cursors: int,
    latency_samples_ns: tuple[int, ...] = (1, 2, 3, 4, 5),
    peak_traced_memory_bytes: int = 0,
) -> harness.EvidenceReport:
    profile = summary.profile
    return harness.EvidenceReport(
        report_version=1,
        task_id=harness.TASK_ID,
        contract_generation=harness.TASK_CONTRACT_GENERATION,
        contract_digest=harness.TASK_CONTRACT_DIGEST,
        schema_fingerprint=summary.schema_fingerprint,
        application_id=harness.APPLICATION_ID,
        user_version=harness.USER_VERSION,
        schema_generation=harness.SCHEMA_GENERATION,
        page_size=harness.PAGE_SIZE,
        storage_marker=harness.STORAGE_MARKER.decode("ascii"),
        python_version=profile.python_version,
        sqlite_version=profile.sqlite_version,
        sqlite_source_id=profile.sqlite_source_id,
        threadsafety=profile.threadsafety,
        compile_options=profile.compile_options,
        connection_profiles=summary.connection_profiles,
        environment_class="generated-linux-pytest",
        evidence_recorded_at_utc=recorded_at,
        workload_seed=harness.WORKLOAD_SEED,
        workload_runs=harness.WORKLOAD_RUNS,
        record_size_matrix=harness.RECORD_SIZE_MATRIX,
        workload_matrix=harness.WORKLOAD_MATRIX,
        maximum_operation_latency_ns=harness.MAX_OPERATION_LATENCY_NS,
        maximum_database_bytes=harness.MAX_TEST_DATABASE_BYTES,
        maximum_wal_bytes=harness.MAX_TEST_WAL_BYTES,
        maximum_traced_memory_bytes=harness.MAX_TEST_TRACED_MEMORY_BYTES,
        maximum_open_cursors_threshold=harness.MAX_TEST_OPEN_CURSORS,
        maximum_page_count=harness.MAX_PAGE_COUNT,
        wal_autocheckpoint_pages=harness.WAL_AUTOCHECKPOINT_PAGES,
        stream_rows=summary.stream_count,
        history_rows=summary.history_count,
        query_rows=query_rows,
        database_bytes=summary.database_bytes,
        wal_bytes=summary.wal_bytes,
        page_count=summary.page_count,
        freelist_count=summary.freelist_count,
        maximum_open_cursors=maximum_open_cursors,
        peak_traced_memory_bytes=peak_traced_memory_bytes,
        latency_samples_ns=latency_samples_ns,
        backup_manifest=backup_manifest,
        gates=_complete_evidence_gates(),
    )


def _expectation(
    policy: ContinuousPublicTradePolicy,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    *,
    source: str | None = None,
) -> ContinuousPublicTradeStreamExpectationV1:
    record = creation.record
    return ContinuousPublicTradeStreamExpectationV1(
        identity=ContinuousPublicTradeStreamIdentityV1(
            stream_id=record.stream_id,
            source=record.source if source is None else source,
            venue=record.venue,
            instrument=record.instrument,
            provider_symbol=record.provider_symbol,
            instrument_type=record.instrument_type,
            request_variant=record.request_variant,
            policy_fingerprint=policy.policy_fingerprint,
            stream_start_epoch_ms=record.stream_start_epoch_ms,
        ),
        effective_stream_policy=policy,
        effective_child_policy_fingerprint=None,
    )


def _insert_stream_only(
    connection: sqlite3.Connection,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> int:
    policy = creation.record.stream_policy
    cursor = connection.execute(
        harness._INSERT_STREAM_SQL,
        (
            creation.record.stream_id.bytes,
            _natural_key(creation),
            1,
            1,
            creation.canonical_bytes,
            creation.record_digest.encode("ascii"),
            creation.history_root.encode("ascii"),
            policy.schema_version.encode("ascii"),
            policy.window_size_ms,
            policy.settlement_lag_ms,
            policy.max_catchup_span_ms,
            policy.max_jobs_per_invocation,
            policy.max_requests_per_job,
            policy.max_records_per_job,
            policy.policy_fingerprint.encode("ascii"),
            creation.record.stream_start_epoch_ms,
            1,
            creation.canonical_bytes,
            creation.record_digest.encode("ascii"),
            creation.successor_envelope.canonical_bytes,
            creation.successor_envelope.envelope_digest.encode("ascii"),
            creation.history_root.encode("ascii"),
        ),
    )
    try:
        assert cursor.lastrowid is not None
        return cursor.lastrowid
    finally:
        cursor.close()


def test_create_cas_current_and_bounded_audit_round_trip(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation()
    natural_key = _natural_key(creation)

    inserted = harness.create_stream(token, creation, policy)
    assert inserted.classification is harness.StoreClassification.INSERTED
    assert inserted.committed
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.DUPLICATE
    )
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
    )
    assert current.classification is harness.StoreClassification.FOUND
    assert current.query_evidence.history_rows == 1
    assert current.current == creation

    second = _retain(creation, policy)
    third = _retain(second, policy)
    assert harness.compare_and_swap_stream(token, second).classification is (
        harness.StoreClassification.UPDATED
    )
    assert harness.compare_and_swap_stream(token, second).classification is (
        harness.StoreClassification.DUPLICATE
    )
    assert harness.compare_and_swap_stream(token, third).classification is (
        harness.StoreClassification.UPDATED
    )
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
    )
    assert current.query_evidence.history_rows == 3
    assert current.creation == creation
    assert current.predecessor == second
    assert current.current == third

    initial = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=1,
    )
    assert initial.classification is harness.StoreClassification.PAGE
    assert initial.overlap_count == 0
    assert initial.new_count == 1
    assert initial.entries == (creation,)
    continued = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            creation.record.successor_version,
            creation.successor_envelope.envelope_digest,
            creation.history_root,
        ),
    )
    assert continued.entries == (creation, second, third)
    assert continued.overlap_count == 1
    assert continued.new_count == 2
    tail = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            third.record.successor_version,
            third.successor_envelope.envelope_digest,
            third.history_root,
        ),
    )
    assert tail.classification is harness.StoreClassification.AT_TAIL
    assert tail.entries == (third,)
    assert tail.overlap_count == 1
    assert tail.new_count == 0
    bad_anchor = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            creation.record.successor_version,
            _digest("wrong-anchor-envelope"),
            _digest("wrong-anchor-root"),
        ),
    )
    assert bad_anchor.classification is harness.StoreClassification.ANCHOR_CONFLICT
    ahead_anchor = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            third.record.successor_version + 1,
            third.successor_envelope.envelope_digest,
            third.history_root,
        ),
    )
    assert ahead_anchor.classification is harness.StoreClassification.ANCHOR_CONFLICT

    summary = harness.verify_store(token)
    assert summary.stream_count == 1
    assert summary.history_count == 3


def test_task062_shaped_facade_revalidates_and_returns_exact_port_models(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    store = harness.TestOnlySQLiteStreamStorePrototype(token)
    policy, creation = _creation(seed=29)
    expectation = _expectation(policy, creation)
    create_command = ContinuousPublicTradeStreamCreateCommandV1(
        expectation=expectation,
        creation=creation,
    )
    inserted = store.create(create_command)
    assert inserted.outcome is ContinuousPublicTradeStreamCreateOutcome.INSERTED
    assert store.create(create_command).outcome is (
        ContinuousPublicTradeStreamCreateOutcome.DUPLICATE
    )

    load_query = ContinuousPublicTradeStreamLoadQueryV1(expectation=expectation)
    loaded = store.load_current(load_query)
    assert loaded.outcome is ContinuousPublicTradeStreamLoadOutcome.FOUND

    transition = _retain(creation, policy)
    cas_command = ContinuousPublicTradeStreamCompareAndSwapCommandV1(
        expectation=expectation,
        expected_version=transition.record.prior_version,
        expected_envelope_digest=transition.record.prior_envelope_digest,
        expected_history_root=transition.record.prior_history_root,
        transition=transition,
    )
    updated = store.compare_and_swap(cas_command)
    assert updated.outcome is ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED
    assert store.compare_and_swap(cas_command).outcome is (
        ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE
    )

    first = store.audit_page(
        ContinuousPublicTradeStreamAuditStartQueryV1(
            expectation=expectation,
            limit=1,
        )
    )
    assert first.outcome is ContinuousPublicTradeStreamAuditOutcome.PAGE
    assert isinstance(first, ContinuousPublicTradeStreamAuditPageResultV1)
    continuation = first.page.continuation
    second = store.audit_page(
        ContinuousPublicTradeStreamAuditContinuationQueryV1(
            expectation=expectation,
            continuation=continuation,
            limit=100,
        )
    )
    assert second.outcome is ContinuousPublicTradeStreamAuditOutcome.PAGE
    assert isinstance(second, ContinuousPublicTradeStreamAuditPageResultV1)
    tail_continuation = second.page.continuation
    tail = store.audit_page(
        ContinuousPublicTradeStreamAuditContinuationQueryV1(
            expectation=expectation,
            continuation=tail_continuation,
            limit=100,
        )
    )
    assert tail.outcome is ContinuousPublicTradeStreamAuditOutcome.AT_TAIL

    wrong_identity = _expectation(
        policy,
        creation,
        source=f"{creation.record.source}-different",
    )
    assert (
        store.load_current(
            ContinuousPublicTradeStreamLoadQueryV1(expectation=wrong_identity)
        ).outcome
        is ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT
    )
    bad_anchor = ContinuousPublicTradeStreamAuditContinuationV1(
        stream_id=creation.record.stream_id,
        through_version=transition.record.successor_version,
        through_envelope_digest=_digest("facade-bad-anchor-envelope"),
        through_history_root=_digest("facade-bad-anchor-root"),
    )
    assert (
        store.audit_page(
            ContinuousPublicTradeStreamAuditContinuationQueryV1(
                expectation=expectation,
                continuation=bad_anchor,
                limit=100,
            )
        ).outcome
        is ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT
    )


def test_audit_rejects_a_short_required_initial_and_continuation_range(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=34)
    entries: list[ContinuousPublicTradeStreamStoredHistoryEntryV1] = [creation]
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    for _ in range(3):
        transition = _retain(entries[-1], policy)
        assert harness.compare_and_swap_stream(token, transition).classification is (
            harness.StoreClassification.UPDATED
        )
        entries.append(transition)

    connection, _ = harness._connect(token, writer=True)
    try:
        connection.set_authorizer(None)
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_schema "
            "WHERE type = 'trigger' AND name = 'trg_history_no_delete'"
        ).fetchone()
        assert trigger_row is not None
        trigger_sql = cast(str, trigger_row[0])
        connection.execute("PRAGMA foreign_keys = OFF").close()
        connection.execute("DROP TRIGGER trg_history_no_delete").close()
        connection.execute(
            "DELETE FROM continuous_public_trade_history WHERE successor_version = 3"
        ).close()
        connection.execute(trigger_sql).close()
        connection.execute("PRAGMA foreign_keys = ON").close()
    finally:
        connection.close()

    natural_key = _natural_key(creation)
    with pytest.raises(harness.HarnessFailure) as initial:
        harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=natural_key,
            limit=3,
        )
    assert initial.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as continuation:
        harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=natural_key,
            limit=2,
            continuation=(
                creation.record.successor_version,
                creation.successor_envelope.envelope_digest,
                creation.history_root,
            ),
        )
    assert continuation.value.code is harness.HarnessFailureCode.CORRUPT


def test_audit_page_reaching_tail_requires_exact_stream_tail_witness(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=35)
    retained = _retain(creation, policy, reason="retained-tail")
    alternate = _retain(creation, policy, reason="alternate-tail")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(token, retained).classification is (
        harness.StoreClassification.UPDATED
    )

    connection, _ = harness._connect(token, writer=True)
    try:
        connection.set_authorizer(None)
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_schema "
            "WHERE type = 'trigger' AND name = 'trg_history_no_update'"
        ).fetchone()
        assert trigger_row is not None
        trigger_sql = cast(str, trigger_row[0])
        connection.execute("PRAGMA foreign_keys = OFF").close()
        connection.execute("DROP TRIGGER trg_history_no_update").close()
        connection.execute(
            """
            UPDATE continuous_public_trade_history
            SET record_canonical_bytes = ?,
                record_digest = ?,
                successor_envelope_canonical_bytes = ?,
                successor_envelope_digest = ?,
                prior_version = ?,
                prior_envelope_digest = ?,
                prior_history_root = ?,
                predecessor_record_canonical_bytes = ?,
                predecessor_record_digest = ?,
                successor_history_root = ?
            WHERE successor_version = 2
            """,
            (
                alternate.canonical_bytes,
                alternate.record_digest.encode("ascii"),
                alternate.successor_envelope.canonical_bytes,
                alternate.successor_envelope.envelope_digest.encode("ascii"),
                alternate.record.prior_version,
                alternate.record.prior_envelope_digest.encode("ascii"),
                alternate.record.prior_history_root.encode("ascii"),
                creation.canonical_bytes,
                creation.record_digest.encode("ascii"),
                alternate.history_root.encode("ascii"),
            ),
        ).close()
        connection.execute(trigger_sql).close()
        connection.execute("PRAGMA foreign_keys = ON").close()
    finally:
        connection.close()

    with pytest.raises(harness.HarnessFailure) as corrupt:
        harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=_natural_key(creation),
            limit=100,
        )
    assert corrupt.value.code is harness.HarnessFailureCode.CORRUPT


def test_create_and_identity_conflicts_validate_both_retained_streams(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy_a, creation_a = _creation(seed=30)
    policy_b, creation_b = _creation(seed=31)
    assert harness.create_stream(token, creation_a, policy_a).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.create_stream(token, creation_b, policy_b).classification is (
        harness.StoreClassification.INSERTED
    )

    same_uuid_policy, same_uuid = _creation(
        seed=32,
        stream_id_seed=30,
    )
    assert (
        harness.create_stream(
            token,
            same_uuid,
            same_uuid_policy,
        ).classification
        is harness.StoreClassification.CONFLICT
    )
    same_key_policy, same_key = _creation(
        seed=30,
        stream_id_seed=33,
        identity_seed=30,
    )
    assert (
        harness.create_stream(
            token,
            same_key,
            same_key_policy,
        ).classification
        is harness.StoreClassification.CONFLICT
    )

    two_row_conflict = harness.load_current(
        token,
        stream_id=creation_a.record.stream_id,
        natural_key=_natural_key(creation_b),
    )
    assert two_row_conflict.classification is harness.StoreClassification.IDENTITY_CONFLICT
    assert two_row_conflict.query_evidence.stream_rows == 2
    assert two_row_conflict.query_evidence.history_rows == 2
    audit_conflict = harness.audit_history(
        token,
        stream_id=creation_a.record.stream_id,
        natural_key=_natural_key(creation_b),
        limit=100,
    )
    assert audit_conflict.classification is harness.StoreClassification.IDENTITY_CONFLICT
    assert harness.verify_store(token).stream_count == 2


def test_compare_and_swap_competing_successor_and_historical_duplicate(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=34)
    accepted = _retain(creation, policy, reason="accepted-retain")
    competing = _retain(creation, policy, reason="competing-retain")
    third = _retain(accepted, policy, reason="later-retain")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(token, accepted).classification is (
        harness.StoreClassification.UPDATED
    )
    assert harness.compare_and_swap_stream(token, competing).classification is (
        harness.StoreClassification.CONFLICT
    )
    assert harness.compare_and_swap_stream(token, third).classification is (
        harness.StoreClassification.UPDATED
    )
    assert harness.compare_and_swap_stream(token, accepted).classification is (
        harness.StoreClassification.DUPLICATE
    )
    assert harness.verify_store(token).history_count == 3


def test_compare_and_swap_rejects_a_self_consistent_time_regression(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=340)
    valid = _retain(creation, policy, reason="time-regression")
    regressed = _with_recorded_at(
        creation,
        valid,
        creation.record.recorded_at - timedelta(microseconds=1),
    )
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    with pytest.raises(harness.HarnessFailure) as rejected:
        harness.compare_and_swap_stream(token, regressed)
    assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.verify_store(token).history_count == 1


def test_load_and_verify_reject_a_persisted_time_regression(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=341)
    valid = _retain(creation, policy, reason="persisted-time-regression")
    regressed = _with_recorded_at(
        creation,
        valid,
        creation.record.recorded_at - timedelta(microseconds=1),
    )
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(token, valid).classification is (
        harness.StoreClassification.UPDATED
    )
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.set_authorizer(None)
        trigger_sql = {
            cast(str, row["name"]): cast(str, row["sql"])
            for row in connection.execute(
                """
                SELECT name, sql
                FROM sqlite_schema
                WHERE type = 'trigger'
                  AND name IN ('trg_history_no_update', 'trg_stream_current_update')
                ORDER BY name
                """
            ).fetchall()
        }
        assert set(trigger_sql) == {
            "trg_history_no_update",
            "trg_stream_current_update",
        }
        connection.execute("BEGIN IMMEDIATE").close()
        connection.execute("DROP TRIGGER trg_history_no_update").close()
        connection.execute("DROP TRIGGER trg_stream_current_update").close()
        connection.execute(
            """
            UPDATE continuous_public_trade_history
            SET record_canonical_bytes = ?,
                record_digest = ?,
                successor_history_root = ?
            WHERE successor_version = 2
            """,
            (
                regressed.canonical_bytes,
                regressed.record_digest.encode("ascii"),
                regressed.history_root.encode("ascii"),
            ),
        ).close()
        connection.execute(
            """
            UPDATE continuous_public_trade_stream
            SET current_record_canonical_bytes = ?,
                current_record_digest = ?,
                current_history_root = ?
            WHERE stream_uuid = ?
            """,
            (
                regressed.canonical_bytes,
                regressed.record_digest.encode("ascii"),
                regressed.history_root.encode("ascii"),
                creation.record.stream_id.bytes,
            ),
        ).close()
        for name in sorted(trigger_sql):
            connection.execute(trigger_sql[name]).close()
        connection.execute("COMMIT").close()
    finally:
        connection.close()
    with pytest.raises(harness.HarnessFailure) as loaded:
        harness.load_current(
            token,
            stream_id=creation.record.stream_id,
            natural_key=_natural_key(creation),
        )
    assert loaded.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as verified:
        harness.verify_store(token)
    assert verified.value.code is harness.HarnessFailureCode.CORRUPT


def test_verify_rejects_one_coherent_history_row_beyond_the_current_tail(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=342)
    second = _retain(creation, policy, reason="second")
    third = _retain(second, policy, reason="extra-third")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(token, second).classification is (
        harness.StoreClassification.UPDATED
    )
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.set_authorizer(None)
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_schema "
            "WHERE type = 'trigger' AND name = 'trg_history_transition_pending'"
        ).fetchone()
        assert trigger_row is not None
        trigger_sql = cast(str, trigger_row[0])
        stream_row = connection.execute(
            "SELECT stream_row_id FROM continuous_public_trade_stream WHERE stream_uuid = ?",
            (creation.record.stream_id.bytes,),
        ).fetchone()
        assert stream_row is not None
        connection.execute("BEGIN IMMEDIATE").close()
        connection.execute("DROP TRIGGER trg_history_transition_pending").close()
        connection.execute(
            harness._INSERT_HISTORY_SQL,
            (
                stream_row["stream_row_id"],
                third.record.successor_version,
                b"transition",
                b"1.0",
                1,
                third.canonical_bytes,
                third.record_digest.encode("ascii"),
                third.successor_envelope.canonical_bytes,
                third.successor_envelope.envelope_digest.encode("ascii"),
                third.record.prior_version,
                third.record.prior_envelope_digest.encode("ascii"),
                third.record.prior_history_root.encode("ascii"),
                second.canonical_bytes,
                second.record_digest.encode("ascii"),
                third.history_root.encode("ascii"),
            ),
        ).close()
        connection.execute(trigger_sql).close()
        connection.execute("COMMIT").close()
    finally:
        connection.close()
    with pytest.raises(harness.HarnessFailure) as verified:
        harness.verify_store(token)
    assert verified.value.code is harness.HarnessFailureCode.CORRUPT


def test_malformed_values_are_rejected_before_any_database_open(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=35)
    forged_token = replace(token, _nonce=b"f" * 32)
    forged_record = creation.record.model_copy(update={"stream_start_epoch_ms": True})
    forged_creation = creation.model_copy(update={"record": forged_record})
    with pytest.raises(harness.HarnessFailure) as malformed:
        harness.create_stream(forged_token, forged_creation, policy)
    assert malformed.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as invalid_identity:
        harness.load_current(
            forged_token,
            stream_id=cast(UUID, "not-a-uuid"),
            natural_key=b"not-a-natural-key",
        )
    assert invalid_identity.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.verify_store(token).history_count == 0


def test_online_backup_restore_generation_copy_and_report(tmp_path: Path) -> None:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=2)
    transition = _retain(creation, policy)
    assert harness.create_stream(source, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(source, transition).classification is (
        harness.StoreClassification.UPDATED
    )

    backup, manifest = harness.online_backup(
        source,
        tmp_path,
        evidence_recorded_at_utc="2026-07-29T06:00:00.000000Z",
    )
    restored, restored_manifest = harness.online_backup(
        backup,
        tmp_path,
        evidence_recorded_at_utc="2026-07-29T06:01:00.000000Z",
    )
    copied = harness.same_format_generation_copy(source, tmp_path)
    source_summary = harness.verify_store(source)
    destination_summary = harness.verify_store(backup)
    assert manifest.source_generation_id != manifest.destination_generation_id
    assert restored_manifest.source_generation_id == manifest.destination_generation_id
    assert manifest.schema_fingerprint == destination_summary.schema_fingerprint
    assert manifest.sqlite_source_id == harness.ACCEPTED_SQLITE_SOURCE_ID
    assert manifest.page_size == harness.PAGE_SIZE
    assert manifest.source_page_count == source_summary.page_count
    assert manifest.source_page_count == manifest.destination_page_count
    assert manifest.destination_page_count == destination_summary.page_count
    assert manifest.checkpoint_outcome == (0, 0, 0)
    assert manifest.evidence_recorded_at_utc == "2026-07-29T06:00:00.000000Z"
    assert manifest.source_streams == 1
    assert manifest.source_history_rows == 2
    assert manifest.destination_streams == 1
    assert manifest.destination_history_rows == 2
    assert tuple(item[0] for item in manifest.files) in {
        ("store.sqlite3",),
        ("store.sqlite3", "store.sqlite3-shm", "store.sqlite3-wal"),
    }
    assert manifest.finalization_outcome == (
        "TRUNCATE_CHECKPOINT_CLOSED_STANDALONE_MAIN"
        if len(manifest.files) == 1
        else "TRUNCATE_CHECKPOINT_CLOSED_COMPLETE_FILE_SET"
    )
    assert all(size >= 0 and digest.startswith("sha256:") for _, size, digest in manifest.files)
    assert manifest.per_stream_tails == (
        (
            str(creation.record.stream_id),
            transition.record.successor_version,
            transition.successor_envelope.envelope_digest,
            transition.history_root,
        ),
    )
    assert restored_manifest.per_stream_tails == manifest.per_stream_tails
    assert harness.verify_store(restored).history_count == 2
    assert harness.verify_store(copied).history_count == 2

    backup_summary = harness.verify_store(backup)
    maximum_open_cursors = harness.measured_open_cursor_evidence(
        backup,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    complete_report = _evidence_report(
        backup_summary,
        backup_manifest=manifest,
        recorded_at=manifest.evidence_recorded_at_utc,
        query_rows=0,
        maximum_open_cursors=maximum_open_cursors,
    )
    with pytest.raises(harness.HarnessFailure) as incomplete:
        harness.write_evidence_report(
            tmp_path,
            report=replace(complete_report, gates=complete_report.gates[:-1]),
        )
    assert incomplete.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as arbitrary_pragma:
        harness.write_evidence_report(
            tmp_path,
            report=replace(
                complete_report,
                connection_profiles=(
                    replace(
                        complete_report.connection_profiles[0],
                        pragmas=(
                            *complete_report.connection_profiles[0].pragmas,
                            ("path", str(tmp_path)),
                        ),
                    ),
                    complete_report.connection_profiles[1],
                ),
            ),
        )
    assert arbitrary_pragma.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as malformed_manifest:
        harness.write_evidence_report(
            tmp_path,
            report=replace(
                complete_report,
                backup_manifest=replace(
                    manifest,
                    destination_history_rows=manifest.destination_history_rows + 1,
                ),
            ),
        )
    assert malformed_manifest.value.code is harness.HarnessFailureCode.CORRUPT
    forged_source_page_count = (
        manifest.source_page_count + 1 if manifest.source_page_count < harness.MAX_PAGE_COUNT else 1
    )
    with pytest.raises(harness.HarnessFailure) as forged_page_count:
        harness.write_evidence_report(
            tmp_path,
            report=replace(
                complete_report,
                backup_manifest=replace(
                    manifest,
                    source_page_count=forged_source_page_count,
                ),
            ),
        )
    assert forged_page_count.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as malformed_report_time:
        harness.write_evidence_report(
            tmp_path,
            report=replace(
                complete_report,
                evidence_recorded_at_utc="2026-07-29T06:00:00Z",
            ),
        )
    assert malformed_report_time.value.code is harness.HarnessFailureCode.CORRUPT
    report = harness.write_evidence_report(
        tmp_path,
        report=complete_report,
    )
    assert report.parent == tmp_path
    assert stat_mode(report) == 0o600
    assert str(tmp_path) not in report.read_text(encoding="utf-8")
    report_document = json.loads(report.read_text(encoding="utf-8"))
    assert report_document["backup_manifest"] == {
        "source_generation_id": manifest.source_generation_id,
        "destination_generation_id": manifest.destination_generation_id,
        "schema_fingerprint": manifest.schema_fingerprint,
        "sqlite_source_id": manifest.sqlite_source_id,
        "page_size": manifest.page_size,
        "source_page_count": manifest.source_page_count,
        "destination_page_count": manifest.destination_page_count,
        "checkpoint_outcome": [0, 0, 0],
        "finalization_outcome": manifest.finalization_outcome,
        "evidence_recorded_at_utc": manifest.evidence_recorded_at_utc,
        "source_streams": manifest.source_streams,
        "source_history_rows": manifest.source_history_rows,
        "destination_streams": manifest.destination_streams,
        "destination_history_rows": manifest.destination_history_rows,
        "files": [list(item) for item in manifest.files],
        "per_stream_tails": [list(item) for item in manifest.per_stream_tails],
    }


def test_online_backup_rejects_malformed_evidence_time_before_destination(
    tmp_path: Path,
) -> None:
    source = harness.bootstrap_store(tmp_path)
    token_nonces = frozenset(harness._TOKEN_REGISTRY)
    with pytest.raises(harness.HarnessFailure) as malformed:
        harness.online_backup(
            source,
            tmp_path,
            evidence_recorded_at_utc="2026-07-29T06:00:00Z",
        )
    assert malformed.value.code is harness.HarnessFailureCode.CORRUPT
    assert frozenset(harness._TOKEN_REGISTRY) == token_nonces
    assert harness.verify_store(source).history_count == 0


def test_online_backup_post_copy_failure_preserves_primary_and_cleans_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=22)
    assert harness.create_stream(source, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    created_destinations: list[harness.StoreToken] = []
    original_bootstrap = harness.bootstrap_store

    def capture_destination(root: Path) -> harness.StoreToken:
        destination = original_bootstrap(root)
        created_destinations.append(destination)
        return destination

    primary = harness.HarnessFailure(harness.HarnessFailureCode.BOUNDS_EXCEEDED)

    def fail_manifest(_token: harness.StoreToken) -> tuple[tuple[str, int, str], ...]:
        raise primary

    monkeypatch.setattr(harness, "bootstrap_store", capture_destination)
    monkeypatch.setattr(harness, "_closed_file_manifest", fail_manifest)
    with pytest.raises(harness.HarnessFailure) as failed:
        harness.online_backup(
            source,
            tmp_path,
            evidence_recorded_at_utc="2026-07-29T06:02:00.000000Z",
        )
    assert failed.value is primary
    assert created_destinations
    destination = created_destinations[-1]
    assert destination._nonce not in harness._TOKEN_REGISTRY
    assert not destination._generation_root.exists()
    assert harness.verify_store(source).history_count == 1


def stat_mode(path: Path) -> int:
    return os.stat(path, follow_symlinks=False).st_mode & 0o777


def test_frozen_minimum_typical_and_maximum_contract_shape_record_sizes() -> None:
    stream_id = UUID("00000000-0000-4000-8000-000000000064")
    child_id = UUID("00000000-0000-4000-8000-000000000059")
    policy_fingerprint = "sha256:" + ("1" * 64)
    evidence_digest = "sha256:" + ("2" * 64)
    child_policy_fingerprint = "sha256:" + ("3" * 64)

    def authority(
        kind: ContinuousPublicTradeEvidenceKind,
        scope: ContinuousPublicTradeEvidenceScopeV1,
        evidence_id: str,
    ) -> ContinuousPublicTradeEvidenceReferenceV1:
        return ContinuousPublicTradeEvidenceReferenceV1(
            evidence_kind=kind,
            evidence_id=evidence_id,
            evidence_digest=evidence_digest,
            scope_digest=evidence_scope_digest(scope),
            outcome=ContinuousPublicTradeEvidenceOutcome.APPROVED,
            valid_from=RECORDED_AT - timedelta(minutes=1),
            expires_at=RECORDED_AT + timedelta(minutes=1),
        )

    def creation_bytes(
        policy: ContinuousPublicTradePolicy,
        checkpoint: ContinuousPublicTradeStreamCheckpoint,
        evidence_id: str,
    ) -> tuple[
        ContinuousPublicTradeStreamCreationRecordV1,
        bytes,
        bytes,
        str,
    ]:
        envelope = ContinuousPublicTradeStreamEnvelopeV1(checkpoint=checkpoint)
        envelope_bytes = encode_stream_envelope(envelope)
        envelope_digest = stream_envelope_digest(envelope)
        projection = project_continuous_public_trade_policy(policy)
        scope = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
            stream_id=stream_id,
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
            stream_id=stream_id,
            source=checkpoint.source,
            venue=checkpoint.venue,
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            request_variant=checkpoint.request_variant,
            stream_start_epoch_ms=checkpoint.stream_start_epoch_ms,
            stream_policy=projection,
            successor_envelope_hex=envelope_bytes.hex(),
            successor_envelope_digest=envelope_digest,
            create_authority_reference=authority(
                ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
                scope,
                evidence_id,
            ),
            recorded_at=RECORDED_AT,
        )
        return (
            record,
            encode_stream_creation_record(record),
            envelope_bytes,
            initial_stream_history_root(record),
        )

    minimum_policy = ContinuousPublicTradePolicy(
        window_size_ms=1,
        settlement_lag_ms=0,
        max_catchup_span_ms=1,
        max_jobs_per_invocation=1,
        max_requests_per_job=1,
        max_records_per_job=1,
        policy_fingerprint=policy_fingerprint,
    )
    minimum_checkpoint = ContinuousPublicTradeStreamCheckpoint(
        stream_id=stream_id,
        source="a",
        venue="a",
        instrument="a",
        provider_symbol="a",
        instrument_type=InstrumentType.SPOT,
        request_variant="a",
        policy_fingerprint=policy_fingerprint,
        stream_start_epoch_ms=0,
        cursor_epoch_ms=0,
        status=ContinuousPublicTradeStreamStatus.ACTIVE,
        version=1,
    )
    minimum_creation, minimum_creation_bytes, minimum_envelope_bytes, minimum_root = creation_bytes(
        minimum_policy, minimum_checkpoint, "a"
    )
    minimum_successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
        minimum_checkpoint.model_dump() | {"version": 2}
    )
    minimum_successor = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=minimum_successor_checkpoint
    )
    minimum_successor_bytes = encode_stream_envelope(minimum_successor)
    minimum_successor_digest = stream_envelope_digest(minimum_successor)
    minimum_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_version=1,
        prior_envelope_digest=minimum_creation.successor_envelope_digest,
        prior_history_root=minimum_root,
        successor_version=2,
        successor_envelope_digest=minimum_successor_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code="a",
        stream_policy=None,
    )
    minimum_transition = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=stream_id,
        prior_version=1,
        successor_version=2,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_history_root=minimum_root,
        prior_envelope_digest=minimum_creation.successor_envelope_digest,
        successor_envelope_hex=minimum_successor_bytes.hex(),
        successor_envelope_digest=minimum_successor_digest,
        reason_code="a",
        transition_authority_reference=authority(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            minimum_scope,
            "a",
        ),
        recorded_at=RECORDED_AT,
    )
    minimum_transition_bytes = encode_stream_transition_record(minimum_transition)
    assert (
        len(minimum_creation_bytes),
        len(minimum_transition_bytes),
        len(minimum_envelope_bytes),
        len(minimum_successor_bytes),
        0,
    ) == harness.RECORD_SIZE_MATRIX[0][2:]

    typical_policy, typical_creation = _creation(seed=harness.WORKLOAD_SEED)
    typical_transition = _retain(typical_creation, typical_policy)
    assert (
        len(typical_creation.canonical_bytes),
        len(typical_transition.canonical_bytes),
        len(typical_creation.successor_envelope.canonical_bytes),
        len(typical_transition.successor_envelope.canonical_bytes),
        0,
    ) == harness.RECORD_SIZE_MATRIX[1][2:]

    maximum_integer = (2**63) - 1
    astral = "\U0001f4b1"
    maximum_policy = ContinuousPublicTradePolicy(
        window_size_ms=1,
        settlement_lag_ms=0,
        max_catchup_span_ms=maximum_integer,
        max_jobs_per_invocation=maximum_integer,
        max_requests_per_job=maximum_integer,
        max_records_per_job=maximum_integer,
        policy_fingerprint=policy_fingerprint,
    )
    maximum_checkpoint = ContinuousPublicTradeStreamCheckpoint(
        stream_id=stream_id,
        source=astral * 128,
        venue=astral * 64,
        instrument=astral * 64,
        provider_symbol=astral * 64,
        instrument_type=InstrumentType.SPOT,
        request_variant=astral * 128,
        policy_fingerprint=policy_fingerprint,
        stream_start_epoch_ms=0,
        cursor_epoch_ms=0,
        status=ContinuousPublicTradeStreamStatus.ACTIVE,
        version=1,
    )
    maximum_creation, maximum_creation_bytes, maximum_envelope_bytes, maximum_root = creation_bytes(
        maximum_policy, maximum_checkpoint, "~" * 128
    )
    plan, payload = finalize_continuous_public_trade_attachment(
        maximum_checkpoint,
        maximum_policy,
        candidate_job_id=child_id,
        child_policy_fingerprint=child_policy_fingerprint,
        now=RECORDED_AT,
    )
    assert plan.attachment is not None
    assert payload is not None
    maximum_successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
        maximum_checkpoint.model_dump() | {"attachment": plan.attachment, "version": 2}
    )
    maximum_successor = ContinuousPublicTradeStreamEnvelopeV1(
        checkpoint=maximum_successor_checkpoint,
        child_creation_payload=payload,
    )
    maximum_successor_bytes = encode_stream_envelope(maximum_successor)
    maximum_successor_digest = stream_envelope_digest(maximum_successor)
    maximum_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.ATTACH,
        prior_version=1,
        prior_envelope_digest=maximum_creation.successor_envelope_digest,
        prior_history_root=maximum_root,
        successor_version=2,
        successor_envelope_digest=None,
        child_job_id=plan.attachment.job_id,
        child_policy_fingerprint=payload.child_checkpoint.policy_fingerprint,
        child_creation_fingerprint=None,
        reason_code=None,
        stream_policy=None,
    )
    maximum_transition = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=stream_id,
        prior_version=1,
        successor_version=2,
        transition_kind=ContinuousPublicTradeTransitionKind.ATTACH,
        prior_history_root=maximum_root,
        prior_envelope_digest=maximum_creation.successor_envelope_digest,
        successor_envelope_hex=maximum_successor_bytes.hex(),
        successor_envelope_digest=maximum_successor_digest,
        reason_code=None,
        transition_authority_reference=authority(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            maximum_scope,
            "~" * 128,
        ),
        recorded_at=RECORDED_AT,
    )
    maximum_transition_bytes = encode_stream_transition_record(maximum_transition)
    child_bytes = encode_child_creation_payload(payload)
    assert (
        len(maximum_creation_bytes),
        len(maximum_transition_bytes),
        len(maximum_envelope_bytes),
        len(maximum_successor_bytes),
        len(child_bytes),
    ) == harness.RECORD_SIZE_MATRIX[2][2:]
    assert (
        encode_stream_creation_record(decode_stream_creation_record(maximum_creation_bytes))
        == maximum_creation_bytes
    )
    assert (
        encode_stream_transition_record(decode_stream_transition_record(maximum_transition_bytes))
        == maximum_transition_bytes
    )
    assert (
        encode_stream_envelope(decode_stream_envelope(maximum_successor_bytes))
        == maximum_successor_bytes
    )
    assert encode_child_creation_payload(decode_child_creation_payload(child_bytes)) == child_bytes
    assert len(maximum_creation_bytes) <= 65_536
    assert len(maximum_transition_bytes) <= 65_536
    assert len(maximum_successor_bytes) <= 16_384
    assert len(child_bytes) <= 8_192
    assert len(maximum_successor_bytes.hex()) <= 32_768


@pytest.mark.parametrize(
    "stream_start_epoch_ms",
    [0, (2**63) - 1],
)
def test_stream_start_integer_boundaries_round_trip(
    tmp_path: Path,
    stream_start_epoch_ms: int,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(
        seed=3 if stream_start_epoch_ms == 0 else 4,
        stream_start_epoch_ms=stream_start_epoch_ms,
    )
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    loaded = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    assert loaded.creation is not None
    assert loaded.creation.record.stream_start_epoch_ms == stream_start_epoch_ms


def test_direct_causal_maximum_is_validated_and_binds_as_sql_integer(
    tmp_path: Path,
) -> None:
    policy, creation = _creation(seed=36)
    prior_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
        creation.successor_envelope.envelope.checkpoint.model_dump() | {"version": (2**63) - 2}
    )
    successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
        prior_checkpoint.model_dump() | {"version": (2**63) - 1}
    )
    prior_envelope = _stored_envelope(
        ContinuousPublicTradeStreamEnvelopeV1(checkpoint=prior_checkpoint)
    )
    successor_envelope = _stored_envelope(
        ContinuousPublicTradeStreamEnvelopeV1(checkpoint=successor_checkpoint)
    )
    prior_root = _digest("direct-max-prior-root")
    reason = "direct-max-boundary"
    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=prior_checkpoint.stream_id,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_version=prior_checkpoint.version,
        prior_envelope_digest=prior_envelope.envelope_digest,
        prior_history_root=prior_root,
        successor_version=successor_checkpoint.version,
        successor_envelope_digest=successor_envelope.envelope_digest,
        child_job_id=None,
        child_policy_fingerprint=None,
        child_creation_fingerprint=None,
        reason_code=reason,
        stream_policy=None,
    )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=prior_checkpoint.stream_id,
        prior_version=prior_checkpoint.version,
        successor_version=successor_checkpoint.version,
        transition_kind=ContinuousPublicTradeTransitionKind.RETAIN,
        prior_history_root=prior_root,
        prior_envelope_digest=prior_envelope.envelope_digest,
        successor_envelope_hex=successor_envelope.canonical_bytes.hex(),
        successor_envelope_digest=successor_envelope.envelope_digest,
        reason_code=reason,
        transition_authority_reference=_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(scope),
            suffix="direct-max",
            recorded_at=RECORDED_AT,
        ),
        recorded_at=RECORDED_AT,
    )
    validate_stream_transition_link(
        prior_envelope.envelope,
        record,
        policy=policy,
        prior_history_root=prior_root,
        prior_recorded_at=RECORDED_AT - timedelta(microseconds=1),
        transition_authority_scope=scope,
        child_completion_scope=None,
    )
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        harness._verify_operation_snapshot(connection, token, writer=False)
        row = connection.execute(
            "SELECT typeof(?), ?, typeof(?), ?",
            (
                record.prior_version,
                record.prior_version,
                record.successor_version,
                record.successor_version,
            ),
        ).fetchone()
        assert row is not None
        connection.execute("COMMIT").close()
    finally:
        connection.close()
    assert tuple(row) == (
        "integer",
        (2**63) - 2,
        "integer",
        (2**63) - 1,
    )


def test_digest_checks_reject_embedded_nul_and_non_ascii(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    connection.set_authorizer(None)
    malformed_values = (
        b"sha256:" + (b"0" * 10) + b"\x00" + (b"Z" * 53),
        b"sha256:" + (b"\xff" * 64),
    )
    try:
        for malformed in malformed_values:
            connection.execute("BEGIN IMMEDIATE").close()
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO stream_tail_commit_guard (
                        stream_row_id,
                        successor_version,
                        record_digest,
                        unresolved_singleton_key
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (9_999, 2, malformed, 0),
                ).close()
            connection.execute("ROLLBACK").close()
    finally:
        connection.close()
    assert harness.verify_store(token).history_count == 0


@pytest.mark.parametrize(
    "statement",
    [
        "DROP TRIGGER trg_history_no_delete",
        "CREATE TABLE forbidden_table (value INTEGER)",
        "CREATE VIRTUAL TABLE forbidden_fts USING fts5(value)",
        "ATTACH DATABASE ':memory:' AS forbidden",
        "PRAGMA writable_schema = ON",
        "SELECT load_extension('forbidden')",
    ],
)
def test_operation_authorizer_rejects_every_forbidden_sql_family(
    tmp_path: Path,
    statement: str,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            connection.execute(statement).close()
    finally:
        connection.close()
    assert harness.verify_store(token).stream_count == 0


def test_transition_history_cannot_commit_without_current_tail(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=5)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.execute("BEGIN IMMEDIATE").close()
        stream = connection.execute(
            """
            SELECT *
            FROM continuous_public_trade_stream
            WHERE stream_uuid = ?
            """,
            (creation.record.stream_id.bytes,),
        ).fetchone()
        assert stream is not None
        connection.execute(
            harness._INSERT_HISTORY_SQL,
            (
                stream["stream_row_id"],
                transition.record.successor_version,
                b"transition",
                b"1.0",
                1,
                transition.canonical_bytes,
                transition.record_digest.encode("ascii"),
                transition.successor_envelope.canonical_bytes,
                transition.successor_envelope.envelope_digest.encode("ascii"),
                transition.record.prior_version,
                transition.record.prior_envelope_digest.encode("ascii"),
                transition.record.prior_history_root.encode("ascii"),
                creation.canonical_bytes,
                creation.record_digest.encode("ascii"),
                transition.history_root.encode("ascii"),
            ),
        ).close()
        with pytest.raises(sqlite3.DatabaseError):
            connection.execute("DELETE FROM stream_tail_commit_guard").close()
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("COMMIT").close()
        connection.execute("ROLLBACK").close()
    finally:
        connection.close()
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    assert current.current == creation
    assert harness.verify_store(token).history_count == 1


def test_deferred_creation_requires_both_stream_and_history_rows(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    _, creation = _creation(seed=40)
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.execute("BEGIN IMMEDIATE").close()
        _insert_stream_only(connection, creation)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("COMMIT").close()
        connection.execute("ROLLBACK").close()

        connection.execute("BEGIN IMMEDIATE").close()
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                harness._INSERT_HISTORY_SQL,
                (
                    99_999,
                    1,
                    b"creation",
                    b"1.0",
                    1,
                    creation.canonical_bytes,
                    creation.record_digest.encode("ascii"),
                    creation.successor_envelope.canonical_bytes,
                    creation.successor_envelope.envelope_digest.encode("ascii"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    creation.history_root.encode("ascii"),
                ),
            ).close()
        connection.execute("ROLLBACK").close()
    finally:
        connection.close()
    assert harness.verify_store(token).history_count == 0


def test_metadata_history_identity_and_tail_are_immutable(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=41)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    connection, _ = harness._connect(token, writer=True)
    statements = (
        "UPDATE stream_store_metadata SET page_size = 8192",
        "DELETE FROM stream_store_metadata",
        (
            "UPDATE continuous_public_trade_history "
            "SET serialization_version = 1 WHERE successor_version = 1"
        ),
        "DELETE FROM continuous_public_trade_history",
        ("UPDATE continuous_public_trade_stream SET stream_contract_version = 2"),
        "DELETE FROM continuous_public_trade_stream",
        ("UPDATE continuous_public_trade_stream SET current_version = current_version + 2"),
    )
    try:
        for statement in statements:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement).close()
    finally:
        connection.close()
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    assert current.current == creation
    assert harness.verify_store(token).history_count == 1


def test_gap_and_wrong_predecessor_transition_rows_roll_back(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=42)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    connection, _ = harness._connect(token, writer=True)
    stream = connection.execute(
        """
        SELECT stream_row_id
        FROM continuous_public_trade_stream
        WHERE stream_uuid = ?
        """,
        (creation.record.stream_id.bytes,),
    ).fetchone()
    assert stream is not None
    base_values = [
        stream["stream_row_id"],
        transition.record.successor_version,
        b"transition",
        b"1.0",
        1,
        transition.canonical_bytes,
        transition.record_digest.encode("ascii"),
        transition.successor_envelope.canonical_bytes,
        transition.successor_envelope.envelope_digest.encode("ascii"),
        transition.record.prior_version,
        transition.record.prior_envelope_digest.encode("ascii"),
        transition.record.prior_history_root.encode("ascii"),
        creation.canonical_bytes,
        creation.record_digest.encode("ascii"),
        transition.history_root.encode("ascii"),
    ]
    hostile_values = []
    wrong_predecessor = list(base_values)
    wrong_predecessor[12] = creation.canonical_bytes + b" "
    hostile_values.append(wrong_predecessor)
    wrong_digest = list(base_values)
    wrong_digest[13] = _digest("wrong-predecessor").encode("ascii")
    hostile_values.append(wrong_digest)
    wrong_root = list(base_values)
    wrong_root[11] = _digest("wrong-root").encode("ascii")
    hostile_values.append(wrong_root)
    gap = list(base_values)
    gap[1] = 3
    gap[9] = 2
    hostile_values.append(gap)
    try:
        for values in hostile_values:
            connection.execute("BEGIN IMMEDIATE").close()
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    harness._INSERT_HISTORY_SQL,
                    values,
                ).close()
            connection.execute("ROLLBACK").close()
    finally:
        connection.close()
    assert harness.verify_store(token).history_count == 1


def test_unsupported_generation_and_short_page_are_rejected(
    tmp_path: Path,
) -> None:
    marker_token = harness.bootstrap_store(tmp_path)
    marker_policy, marker_creation = _creation(seed=43)
    assert (
        harness.create_stream(
            marker_token,
            marker_creation,
            marker_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    marker_connection, _ = harness._connect(marker_token, writer=True)
    try:
        marker_connection.execute("PRAGMA user_version = 2").close()
    finally:
        marker_connection.close()
    with pytest.raises(harness.HarnessFailure) as marker_error:
        harness.verify_store(marker_token)
    assert marker_error.value.code is harness.HarnessFailureCode.UNSUPPORTED_VERSION
    store = harness.TestOnlySQLiteStreamStorePrototype(marker_token)
    unsupported = store.load_current(
        ContinuousPublicTradeStreamLoadQueryV1(
            expectation=_expectation(marker_policy, marker_creation)
        )
    )
    assert unsupported.outcome is ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION

    zero_token = harness.bootstrap_store(tmp_path)
    zero_connection, _ = harness._connect(zero_token, writer=True)
    try:
        zero_connection.execute("PRAGMA user_version = 0").close()
    finally:
        zero_connection.close()
    with pytest.raises(harness.HarnessFailure) as zero_error:
        harness.verify_store(zero_token)
    assert zero_error.value.code is harness.HarnessFailureCode.CORRUPT

    short_token = harness.bootstrap_store(tmp_path)
    checkpoint_connection, _ = harness._connect(short_token, writer=True)
    try:
        checkpoint_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").close()
    finally:
        checkpoint_connection.close()
    os.truncate(short_token._database_path, harness.PAGE_SIZE - 1)
    with pytest.raises(harness.HarnessFailure) as short_error:
        harness.verify_store(short_token)
    assert short_error.value.code is harness.HarnessFailureCode.CORRUPT


def test_path_token_and_permission_guards_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(harness.HarnessFailure) as relative:
        harness.bootstrap_store(Path("relative"))
    assert relative.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    alias = tmp_path.parent / f"{tmp_path.name}-alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    try:
        with pytest.raises(harness.HarnessFailure):
            harness.bootstrap_store(alias)
    finally:
        alias.unlink()

    reconstructed = Path(str(tmp_path))
    assert reconstructed == tmp_path
    assert reconstructed is not tmp_path
    with pytest.raises(harness.HarnessFailure) as reconstructed_root:
        harness.bootstrap_store(reconstructed)
    assert reconstructed_root.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    token = harness.bootstrap_store(tmp_path)
    forged = replace(token, _nonce=b"x" * 32)
    with pytest.raises(harness.HarnessFailure) as invalid:
        harness.verify_store(forged)
    assert invalid.value.code is harness.HarnessFailureCode.INVALID_TOKEN

    hardlink = token._database_path.with_name("forbidden-hardlink.sqlite3")
    os.link(token._database_path, hardlink)
    try:
        with pytest.raises(harness.HarnessFailure) as linked:
            harness.verify_store(token)
        assert linked.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        hardlink.unlink()

    unexpected = token._generation_root / "unexpected-entry"
    unexpected.write_bytes(b"not-owned")
    try:
        with pytest.raises(harness.HarnessFailure) as unexpected_entry:
            harness.verify_store(token)
        assert unexpected_entry.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        unexpected.unlink()

    os.chmod(token._database_path, 0o400)
    try:
        with pytest.raises(harness.HarnessFailure) as readonly:
            harness.verify_store(token)
        assert readonly.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        os.chmod(token._database_path, 0o600)
    assert harness.verify_store(token).stream_count == 0

    root_mode = stat_mode(tmp_path)
    os.chmod(tmp_path, root_mode ^ 0o020)
    try:
        with pytest.raises(harness.HarnessFailure) as widened_root:
            harness.verify_store(token)
        assert widened_root.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        os.chmod(tmp_path, root_mode)
    assert harness.verify_store(token).stream_count == 0

    missing = harness.bootstrap_store(tmp_path)
    missing._database_path.unlink()
    with pytest.raises(harness.HarnessFailure) as absent:
        harness.verify_store(missing)
    assert absent.value.code is harness.HarnessFailureCode.UNAVAILABLE

    replaced = harness.bootstrap_store(tmp_path)
    replaced._database_path.unlink()
    replacement_descriptor = os.open(
        replaced._database_path,
        os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    os.close(replacement_descriptor)
    with pytest.raises(harness.HarnessFailure) as swapped:
        harness.verify_store(replaced)
    assert swapped.value.code is harness.HarnessFailureCode.UNAVAILABLE


def test_live_connection_path_revalidation_is_lock_neutral(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)

    def unexpected_open(*_args: object, **_kwargs: object) -> int:
        raise AssertionError("live SQLite-family revalidation must not open another descriptor")

    try:
        connection.execute("BEGIN IMMEDIATE").close()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "open", unexpected_open)
            harness._verify_operation_authority(connection, token)
            assert (
                harness._operation_file_size(
                    connection,
                    token,
                    "store.sqlite3",
                    required=True,
                    maximum=harness.MAX_TEST_DATABASE_BYTES,
                )
                > 0
            )
            assert (
                harness._operation_file_size(
                    connection,
                    token,
                    "store.sqlite3-wal",
                    required=False,
                    maximum=harness.MAX_TEST_WAL_BYTES,
                )
                >= 0
            )
        connection.execute("ROLLBACK").close()
    finally:
        if connection.in_transaction:
            with contextlib.suppress(sqlite3.Error):
                connection.execute("ROLLBACK").close()
        connection.close()


def test_pytest_root_registration_expires_and_rejects_wrong_process_or_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" (", maxsplit=1)[0]
    scoped_root = tmp_path / "separately-scoped-root"
    scoped_root.mkdir(mode=0o700)
    with harness._pytest_root_scope(scoped_root, node_id=node_id):
        token = harness.bootstrap_store(scoped_root)
        assert harness.verify_store(token).stream_count == 0
        child_read, child_write = os.pipe()
        child_process_id = os.fork()
        if child_process_id == 0:
            os.close(child_read)
            child_ok = False
            try:
                child_ok = harness.verify_store(token).stream_count == 0
                os.write(child_write, b"V" if child_ok else b"E")
            except BaseException:
                with contextlib.suppress(OSError):
                    os.write(child_write, b"E")
            finally:
                with contextlib.suppress(OSError):
                    os.close(child_write)
            os._exit(0 if child_ok else 70)
        os.close(child_write)
        child_payload = os.read(child_read, 1)
        os.close(child_read)
        _, child_status = os.waitpid(child_process_id, 0)
        assert child_payload == b"V"
        assert os.WIFEXITED(child_status)
        assert os.WEXITSTATUS(child_status) == 0
        monkeypatch.setattr(os, "getpid", lambda: -1)
        with pytest.raises(harness.HarnessFailure) as wrong_process:
            harness.bootstrap_store(scoped_root)
        assert wrong_process.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        with pytest.raises(harness.HarnessFailure) as wrong_token_process:
            harness.verify_store(token)
        assert wrong_token_process.value.code is harness.HarnessFailureCode.INVALID_TOKEN
        monkeypatch.undo()
        monkeypatch.setenv("PYTEST_CURRENT_TEST", f"{node_id}::wrong (call)")
        with pytest.raises(harness.HarnessFailure) as wrong_node:
            harness.bootstrap_store(scoped_root)
        assert wrong_node.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        with pytest.raises(harness.HarnessFailure) as wrong_token_node:
            harness.verify_store(token)
        assert wrong_token_node.value.code is harness.HarnessFailureCode.INVALID_TOKEN
        monkeypatch.undo()
        late_ready_read, late_ready_write = os.pipe()
        late_control_read, late_control_write = os.pipe()
        late_result_read, late_result_write = os.pipe()
        late_child_process_id = os.fork()
        if late_child_process_id == 0:
            os.close(late_ready_read)
            os.close(late_control_write)
            os.close(late_result_read)
            packet = b"E"
            try:
                os.write(late_ready_write, b"R")
                if os.read(late_control_read, 1) != b"C":
                    os._exit(71)
                try:
                    harness.verify_store(token)
                except harness.HarnessFailure as error:
                    if error.code is harness.HarnessFailureCode.INVALID_TOKEN:
                        packet = b"I"
                else:
                    packet = b"V"
            except BaseException:
                packet = b"E"
            with contextlib.suppress(OSError):
                os.write(late_result_write, packet)
            os._exit(0 if packet == b"I" else 70)
        os.close(late_ready_write)
        os.close(late_control_read)
        os.close(late_result_write)
        late_ready_selector = selectors.DefaultSelector()
        late_ready_selector.register(late_ready_read, selectors.EVENT_READ)
        try:
            assert late_ready_selector.select(timeout=10)
            assert os.read(late_ready_read, 1) == b"R"
        finally:
            late_ready_selector.close()
            os.close(late_ready_read)
    with pytest.raises(harness.HarnessFailure) as expired:
        harness.bootstrap_store(scoped_root)
    assert expired.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    with pytest.raises(harness.HarnessFailure) as expired_token:
        harness.verify_store(token)
    assert expired_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    late_result_selector = selectors.DefaultSelector()
    late_result_selector.register(late_result_read, selectors.EVENT_READ)
    late_payload = b""
    late_status = 0
    late_child_reaped = False
    try:
        os.write(late_control_write, b"C")
        os.close(late_control_write)
        assert late_result_selector.select(timeout=10)
        late_payload = os.read(late_result_read, 1)
        _, late_status = os.waitpid(late_child_process_id, 0)
        late_child_reaped = True
    finally:
        late_result_selector.close()
        with contextlib.suppress(OSError):
            os.close(late_result_read)
        with contextlib.suppress(OSError):
            os.close(late_control_write)
        if not late_child_reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(late_child_process_id, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(late_child_process_id, 0)
    assert late_payload == b"I"
    assert os.WIFEXITED(late_status)
    assert os.WEXITSTATUS(late_status) == 0
    harness._remove_owned_files(token)
    scoped_root.rmdir()


def test_inflight_create_rolls_back_when_fixture_authority_expires(
    tmp_path: Path,
) -> None:
    node_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" (", maxsplit=1)[0]
    scoped_root = tmp_path / "inflight-revocation-root"
    scoped_root.mkdir(mode=0o700)
    policy, creation = _creation(seed=77)
    with harness._pytest_root_scope(scoped_root, node_id=node_id):
        token = harness.bootstrap_store(scoped_root)
        ready_read, ready_write = os.pipe()
        control_read, control_write = os.pipe()
        result_read, result_write = os.pipe()
        child_process_id = os.fork()
        if child_process_id == 0:
            os.close(ready_read)
            os.close(control_write)
            os.close(result_read)
            commit_seen = False
            rollback_called = False
            rollback_clean = False
            real_execute = harness._MeteredConnection.execute
            real_rollback = harness._rollback_best_effort

            def observed_execute(
                connection: harness._MeteredConnection,
                sql: str,
                parameters: Any = (),
                /,
            ) -> sqlite3.Cursor:
                nonlocal commit_seen
                if sql == "COMMIT":
                    commit_seen = True
                return real_execute(connection, sql, parameters)

            def observed_rollback(connection: sqlite3.Connection) -> None:
                nonlocal rollback_called, rollback_clean
                rollback_called = True
                real_rollback(connection)
                rollback_clean = not connection.in_transaction

            def pause_after_first_insert(seam: str) -> None:
                if seam == "between_stream_insert_and_creation_insert":
                    os.write(ready_write, b"R")
                    if os.read(control_read, 1) != b"C":
                        raise AssertionError("bounded revocation control failed")

            failure_code: harness.HarnessFailureCode | None = None
            with pytest.MonkeyPatch.context() as child_patch:
                child_patch.setattr(harness._MeteredConnection, "execute", observed_execute)
                child_patch.setattr(harness, "_rollback_best_effort", observed_rollback)
                try:
                    harness.create_stream(
                        token,
                        creation,
                        policy,
                        seam_hook=pause_after_first_insert,
                    )
                except harness.HarnessFailure as error:
                    failure_code = error.code
            packet = (
                b"I"
                if (
                    failure_code is harness.HarnessFailureCode.INVALID_TOKEN
                    and rollback_called
                    and rollback_clean
                    and not commit_seen
                )
                else b"E"
            )
            with contextlib.suppress(OSError):
                os.write(result_write, packet)
            os._exit(0 if packet == b"I" else 70)
        os.close(ready_write)
        os.close(control_read)
        os.close(result_write)
        ready_selector = selectors.DefaultSelector()
        ready_selector.register(ready_read, selectors.EVENT_READ)
        try:
            assert ready_selector.select(timeout=10)
            assert os.read(ready_read, 1) == b"R"
        finally:
            ready_selector.close()
            os.close(ready_read)

    result_selector = selectors.DefaultSelector()
    result_selector.register(result_read, selectors.EVENT_READ)
    payload = b""
    child_status = 0
    child_reaped = False
    try:
        os.write(control_write, b"C")
        os.close(control_write)
        assert result_selector.select(timeout=10)
        payload = os.read(result_read, 1)
        _, child_status = os.waitpid(child_process_id, 0)
        child_reaped = True
    finally:
        result_selector.close()
        with contextlib.suppress(OSError):
            os.close(result_read)
        with contextlib.suppress(OSError):
            os.close(control_write)
        if not child_reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child_process_id, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(child_process_id, 0)
    assert payload == b"I"
    assert os.WIFEXITED(child_status)
    assert os.WEXITSTATUS(child_status) == 0
    harness._remove_owned_files(token)
    scoped_root.rmdir()


def test_registered_pytest_root_inode_replacement_is_rejected(tmp_path: Path) -> None:
    retained_root = tmp_path.with_name(f"{tmp_path.name}-retained")
    original_mode = stat_mode(tmp_path)
    tmp_path.rename(retained_root)
    tmp_path.mkdir(mode=original_mode)
    try:
        with pytest.raises(harness.HarnessFailure) as replaced:
            harness.bootstrap_store(tmp_path)
        assert replaced.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    finally:
        tmp_path.rmdir()
        retained_root.rename(tmp_path)


def test_path_resolution_races_are_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_resolve(_path: Path, *, strict: bool = False) -> Path:
        del strict
        raise FileNotFoundError("sensitive-generated-path")

    monkeypatch.setattr(Path, "resolve", fail_resolve)
    with pytest.raises(harness.HarnessFailure) as bootstrap:
        harness.bootstrap_store(tmp_path)
    assert bootstrap.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    assert str(bootstrap.value) == "invalid_bootstrap_root"
    assert bootstrap.value.__cause__ is None

    monkeypatch.undo()
    token = harness.bootstrap_store(tmp_path)
    monkeypatch.setattr(Path, "resolve", fail_resolve)
    with pytest.raises(harness.HarnessFailure) as operation:
        harness.verify_store(token)
    assert operation.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert str(operation.value) == "unavailable"
    assert operation.value.__cause__ is None


def test_manifest_rejects_unexpected_entries_and_allowed_name_aliases(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    unexpected = token._generation_root / "unexpected-directory"
    unexpected.mkdir()
    try:
        with pytest.raises(harness.HarnessFailure) as directory:
            harness._closed_file_manifest(token)
        assert directory.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        unexpected.rmdir()

    sentinel = tmp_path / "manifest-sentinel"
    sentinel.write_bytes(b"sentinel-must-not-be-hashed")
    alias = token._generation_root / "store.sqlite3-shm"
    retained = tmp_path / "store.sqlite3-shm-retained"
    alias.rename(retained)
    alias.symlink_to(sentinel)
    try:
        with pytest.raises(harness.HarnessFailure) as linked:
            harness._closed_file_manifest(token)
        assert linked.value.code is harness.HarnessFailureCode.UNAVAILABLE
        with pytest.raises(harness.HarnessFailure) as operation:
            harness.verify_store(token)
        assert operation.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        alias.unlink()
        retained.rename(alias)


def test_cleanup_never_follows_a_replaced_generation_alias(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    original_generation = token._generation_root
    retained_generation = original_generation.with_name(f"{original_generation.name}-retained")
    sentinel_root = tmp_path / "cleanup-sentinel"
    sentinel_root.mkdir()
    sentinel = sentinel_root / "store.sqlite3"
    sentinel.write_bytes(b"sentinel-must-survive")
    original_generation.rename(retained_generation)
    original_generation.symlink_to(sentinel_root, target_is_directory=True)
    try:
        harness._remove_owned_files(token)
        assert sentinel.read_bytes() == b"sentinel-must-survive"
    finally:
        original_generation.unlink()
        retained_generation.rename(original_generation)
        harness._remove_unregistered_generation(
            original_generation,
            original_generation / "store.sqlite3",
        )
        sentinel.unlink()
        sentinel_root.rmdir()


def test_cleanup_failures_preserve_primary_and_otherwise_are_sanitized() -> None:
    class FailingCloser:
        def __init__(self, label: str) -> None:
            self.label = label
            self.attempts = 0

        def close(self) -> None:
            self.attempts += 1
            raise RuntimeError(f"sensitive-{self.label}")

    primary = ValueError("exact-primary")
    primary_connection = FailingCloser("primary-connection")
    try:
        try:
            raise primary
        finally:
            harness._close_preserving_primary(cast(sqlite3.Connection, primary_connection))
    except ValueError as caught:
        assert caught is primary
    assert primary_connection.attempts == 1

    primary_cursor = FailingCloser("primary-cursor")
    try:
        try:
            raise primary
        finally:
            harness._close_cursor_preserving_primary(cast(sqlite3.Cursor, primary_cursor))
    except ValueError as caught:
        assert caught is primary
    assert primary_cursor.attempts == 1

    standalone = FailingCloser("standalone")
    with pytest.raises(harness.HarnessFailure) as sanitized:
        harness._close_checked(cast(sqlite3.Connection, standalone))
    assert sanitized.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert str(sanitized.value) == "unavailable"
    assert sanitized.value.__cause__ is None

    first = FailingCloser("first")
    second = FailingCloser("second")
    with pytest.raises(harness.HarnessFailure) as first_failure:
        harness._close_many_preserving_primary(
            cast(sqlite3.Connection, first),
            cast(sqlite3.Connection, second),
        )
    assert first_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert first.attempts == 1
    assert second.attempts == 1

    cursor = FailingCloser("cursor")
    with pytest.raises(harness.HarnessFailure) as cursor_failure:
        harness._close_cursor_preserving_primary(cast(sqlite3.Cursor, cursor))
    assert cursor_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert str(cursor_failure.value) == "unavailable"

    measurement_primary = LookupError("measurement-primary")
    try:
        with harness.measure_open_cursors() as measurement:
            measurement.current = 1
            raise measurement_primary
    except LookupError as caught:
        assert caught is measurement_primary


def test_busy_writer_is_one_attempt_and_sanitized(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=6)
    blocker, _ = harness._connect(token, writer=True)
    blocker.execute("BEGIN IMMEDIATE").close()
    try:
        with pytest.raises(harness.HarnessFailure) as busy:
            harness.create_stream(token, creation, policy)
        assert busy.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert str(busy.value) == "unavailable"
        assert busy.value.__cause__ is None
    finally:
        blocker.execute("ROLLBACK").close()
        blocker.close()
    assert harness.verify_store(token).history_count == 0


@pytest.mark.parametrize(
    ("seam", "expected_code"),
    [
        ("readonly", sqlite3.SQLITE_READONLY),
        ("busy", sqlite3.SQLITE_BUSY),
    ],
)
def test_fresh_process_readonly_and_busy_result_code_probes_are_sanitized(
    tmp_path: Path,
    seam: str,
    expected_code: int,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    evidence = harness.sqlite_result_code_fault_evidence(token, seam=seam)
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.sqlite_errorcode == expected_code
    assert evidence.acknowledgement_bytes == 4
    assert evidence.reopened_state is harness.ReopenedState.OLD
    assert evidence.reason is None
    assert harness.verify_store(token).history_count == 0


@pytest.mark.parametrize(
    "code",
    [
        sqlite3.SQLITE_LOCKED,
        sqlite3.SQLITE_LOCKED_SHAREDCACHE,
        sqlite3.SQLITE_LOCKED_VTAB,
    ],
)
def test_locked_result_codes_are_mapping_only_and_fail_closed(code: int) -> None:
    assert harness.sqlite_result_failure_code(code) is harness.HarnessFailureCode.UNAVAILABLE


def test_locked_probe_cannot_be_manufactured_on_the_compliant_path(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    with pytest.raises(harness.HarnessFailure) as rejected:
        harness.sqlite_result_code_fault_evidence(token, seam="locked")
    assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.verify_store(token).history_count == 0


def test_fresh_process_create_writers_really_overlap_at_the_writer_lock(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=69)
    evidence = harness.fresh_process_writer_contention_evidence(
        token,
        operation="create",
        creation=creation,
        policy=policy,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.winner_outcome is harness.StoreClassification.INSERTED
    assert evidence.contender_outcome is harness.HarnessFailureCode.UNAVAILABLE
    assert evidence.contender_sqlite_errorcode == sqlite3.SQLITE_BUSY
    assert evidence.process_boundary == "two-forked-overlapping-writers"
    assert (evidence.stream_count, evidence.history_count) == (1, 1)


def test_fresh_process_cas_writers_really_overlap_at_the_writer_lock(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=70)
    transition = _retain(creation, policy, reason="overlapping-cas")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.fresh_process_writer_contention_evidence(
        token,
        operation="compare_and_swap",
        transition=transition,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.winner_outcome is harness.StoreClassification.UPDATED
    assert evidence.contender_outcome is harness.HarnessFailureCode.UNAVAILABLE
    assert evidence.contender_sqlite_errorcode == sqlite3.SQLITE_BUSY
    assert evidence.process_boundary == "two-forked-overlapping-writers"
    assert (evidence.stream_count, evidence.history_count) == (1, 2)


def test_fresh_process_identical_create_writers_classify_insert_then_duplicate(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=7)
    evidence = harness.fresh_process_two_writer_evidence(
        token,
        operation="create",
        creations=(creation, creation),
        policy=policy,
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.outcomes == (
        harness.StoreClassification.INSERTED,
        harness.StoreClassification.DUPLICATE,
    )
    assert evidence.process_boundary == "two-forked-sequential-classifiers"
    assert harness.verify_store(token).history_count == 1


def test_fresh_process_same_identity_create_writers_classify_insert_then_conflict(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=72, stream_id_seed=72, identity_seed=72)
    competing_policy, competing = _creation(
        seed=72,
        stream_id_seed=720,
        identity_seed=72,
    )
    assert competing_policy == policy
    evidence = harness.fresh_process_two_writer_evidence(
        token,
        operation="create",
        creations=(creation, competing),
        policy=policy,
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.outcomes == (
        harness.StoreClassification.INSERTED,
        harness.StoreClassification.CONFLICT,
    )
    assert harness.verify_store(token).history_count == 1


def test_fresh_process_identical_cas_writers_classify_update_then_duplicate(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=73)
    transition = _retain(creation, policy, reason="identical-cas")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.fresh_process_two_writer_evidence(
        token,
        operation="compare_and_swap",
        transitions=(transition, transition),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.outcomes == (
        harness.StoreClassification.UPDATED,
        harness.StoreClassification.DUPLICATE,
    )
    assert harness.verify_store(token).history_count == 2


def test_fresh_process_competing_cas_writers_classify_update_then_conflict(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=74)
    accepted = _retain(creation, policy, reason="accepted-cas")
    competing = _retain(creation, policy, reason="competing-cas")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.fresh_process_two_writer_evidence(
        token,
        operation="compare_and_swap",
        transitions=(accepted, competing),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.outcomes == (
        harness.StoreClassification.UPDATED,
        harness.StoreClassification.CONFLICT,
    )
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    assert current.current == accepted
    assert harness.verify_store(token).history_count == 2


def test_partial_multichild_fork_failures_are_bounded_and_unproven(
    tmp_path: Path,
) -> None:
    real_fork = os.fork
    real_pipe = os.pipe

    def descriptor_count() -> int:
        return len(os.listdir("/proc/self/fd"))

    def failing_fork(fail_on: int) -> Callable[[], int]:
        calls = 0

        def invoke() -> int:
            nonlocal calls
            calls += 1
            if calls == fail_on:
                raise OSError("bounded synthetic fork failure")
            return real_fork()

        return invoke

    def always_failing_fork() -> int:
        raise OSError("bounded synthetic fork failure")

    def failing_pipe(fail_on: int) -> Callable[[], tuple[int, int]]:
        calls = 0

        def invoke() -> tuple[int, int]:
            nonlocal calls
            calls += 1
            if calls == fail_on:
                raise OSError("bounded synthetic pipe failure")
            return real_pipe()

        return invoke

    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=75)
    before_descriptors = descriptor_count()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "pipe", failing_pipe(2))
        ipc_failure = harness.sqlite_result_code_fault_evidence(
            token,
            seam="readonly",
        )
    assert ipc_failure.disposition is harness.EvidenceDisposition.UNPROVEN
    assert ipc_failure.reason == "ipc_setup_failed"
    assert descriptor_count() == before_descriptors

    spawned_probe_processes: list[int] = []
    fork_calls = 0

    def fail_busy_holder_fork() -> int:
        nonlocal fork_calls
        fork_calls += 1
        if fork_calls == 2:
            raise OSError("bounded synthetic holder fork failure")
        process_id = real_fork()
        if process_id > 0:
            spawned_probe_processes.append(process_id)
        return process_id

    before_descriptors = descriptor_count()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", fail_busy_holder_fork)
        busy_spawn_failure = harness.sqlite_result_code_fault_evidence(
            token,
            seam="busy",
        )
    assert busy_spawn_failure.disposition is harness.EvidenceDisposition.UNPROVEN
    assert busy_spawn_failure.reason == "fork_spawn_failed"
    assert descriptor_count() == before_descriptors
    assert len(spawned_probe_processes) == 1
    with pytest.raises(ChildProcessError):
        os.waitpid(spawned_probe_processes[0], os.WNOHANG)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", failing_fork(2))
        contention = harness.fresh_process_writer_contention_evidence(
            token,
            operation="create",
            creation=creation,
            policy=policy,
            natural_key=_natural_key(creation),
        )
    assert contention.disposition is harness.EvidenceDisposition.UNPROVEN
    assert contention.process_boundary == "fork_spawn_failed"

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", failing_fork(2))
        classifiers = harness.fresh_process_two_writer_evidence(
            token,
            operation="create",
            creations=(creation, creation),
            policy=policy,
        )
    assert classifiers.disposition is harness.EvidenceDisposition.UNPROVEN
    assert classifiers.process_boundary == "fork_spawn_failed"

    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    transition = _retain(creation, policy, reason="spawn-failure-wal")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", failing_fork(3))
        wal = harness.wal_concurrency_evidence(
            token,
            transitions=(transition,),
            natural_key=_natural_key(creation),
        )
    assert wal.disposition is harness.EvidenceDisposition.UNPROVEN
    assert wal.process_boundary == "fork_spawn_failed"
    assert harness.verify_store(token).history_count == 1

    second_policy, second_creation = _creation(seed=76)
    single_process_faults: tuple[
        Callable[[], harness.FaultEvidence],
        ...,
    ] = (
        lambda: harness.sqlite_result_code_fault_evidence(token, seam="readonly"),
        lambda: harness.fresh_process_kill_evidence(
            token,
            seam="after_lock",
            creation=second_creation,
            policy=second_policy,
            natural_key=_natural_key(second_creation),
        ),
        lambda: harness.true_during_commit_evidence(
            token,
            transition=transition,
            natural_key=_natural_key(creation),
        ),
        lambda: harness.ioerr_write_evidence(
            token,
            transition=transition,
            natural_key=_natural_key(creation),
        ),
        lambda: harness.max_page_count_evidence(
            token,
            transition=transition,
            natural_key=_natural_key(creation),
        ),
    )
    for invoke in single_process_faults:
        before_descriptors = descriptor_count()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fork", always_failing_fork)
            fault = invoke()
        assert fault.disposition is harness.EvidenceDisposition.UNPROVEN
        assert fault.reason == "fork_spawn_failed"
        assert fault.sqlite_errorcode is None
        assert fault.reopened_state is harness.ReopenedState.UNAVAILABLE
        assert descriptor_count() == before_descriptors
        assert harness.verify_store(token).history_count == 1


def test_query_row_bounds_at_limits_one_and_one_hundred(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=8)
    entries: list[ContinuousPublicTradeStreamStoredHistoryEntryV1] = [creation]
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    for index in range(102):
        transition = _retain(entries[-1], policy)
        assert harness.compare_and_swap_stream(token, transition).classification is (
            harness.StoreClassification.UPDATED
        )
        entries.append(transition)
        if (index + 1) % 20 == 0:
            _truncate_generated_wal(token)
    natural_key = _natural_key(creation)
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
    )
    assert current.query_evidence.history_rows == 3
    assert current.creation == entries[0]
    assert current.predecessor == entries[101]
    assert current.current == entries[102]

    first = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=1,
    )
    hundred = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
    )
    continuation = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            creation.record.successor_version,
            creation.successor_envelope.envelope_digest,
            creation.history_root,
        ),
    )
    tail_entry = entries[-1]
    tail = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            tail_entry.record.successor_version,
            tail_entry.successor_envelope.envelope_digest,
            tail_entry.history_root,
        ),
    )
    assert first.entries == (entries[0],)
    assert first.query_evidence.history_rows == 1
    assert hundred.entries == tuple(entries[:100])
    assert hundred.query_evidence.history_rows == 100
    assert continuation.entries == tuple(entries[:101])
    assert continuation.query_evidence.history_rows == 101
    assert tail.entries == (tail_entry,)
    assert tail.query_evidence.history_rows == 1
    assert tail.classification is harness.StoreClassification.AT_TAIL
    plans = harness.query_plan_evidence(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
    )
    assert {
        "identity",
        "current",
        "audit_initial_1",
        "audit_continuation_1",
        "audit_initial_100",
        "audit_continuation_100",
    } == set(plans)
    for name, details in plans.items():
        joined = " ".join(details)
        assert "SCAN continuous_public_trade_history" not in joined
        if name != "identity":
            assert "ux_cpt_history_stream_version" in joined
    assert "ux_cpt_stream_uuid" in " ".join(plans["identity"])
    assert "ux_cpt_stream_natural_key" in " ".join(plans["identity"])
    summary = harness.verify_store(token)
    assert summary.stream_count == 1
    assert summary.history_count == 103


def test_verify_store_keyset_pages_more_than_one_hundred_streams(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    for offset, seed in enumerate(range(1000, 1101), start=1):
        policy, creation = _creation(seed=seed)
        assert harness.create_stream(token, creation, policy).classification is (
            harness.StoreClassification.INSERTED
        )
        if offset % 20 == 0:
            _truncate_generated_wal(token)
    summary = harness.verify_store(token)
    assert summary.stream_count == 101
    assert summary.history_count == 101


@pytest.mark.parametrize(
    "seam",
    [
        "before_transaction",
        "after_lock",
        "between_stream_insert_and_creation_insert",
        "between_creation_insert_and_create_commit",
        "after_commit_before_acknowledgement",
    ],
)
def test_fresh_process_create_kill_seams(
    tmp_path: Path,
    seam: str,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=20)
    evidence = harness.fresh_process_kill_evidence(
        token,
        seam=seam,
        creation=creation,
        policy=policy,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.acknowledgement_bytes == 0
    assert evidence.reopened_state is (
        harness.ReopenedState.NEW
        if seam == "after_commit_before_acknowledgement"
        else harness.ReopenedState.OLD
    )


@pytest.mark.parametrize(
    "seam",
    [
        "before_transaction",
        "after_lock",
        "between_transition_insert_and_current_update",
        "between_current_update_and_compare_and_swap_commit",
        "after_commit_before_acknowledgement",
    ],
)
def test_fresh_process_compare_and_swap_kill_seams(
    tmp_path: Path,
    seam: str,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=21)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.fresh_process_kill_evidence(
        token,
        seam=seam,
        transition=transition,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.acknowledgement_bytes == 0
    assert evidence.reopened_state is (
        harness.ReopenedState.NEW
        if seam == "after_commit_before_acknowledgement"
        else harness.ReopenedState.OLD
    )


def test_true_during_commit_is_observed_inside_wal_write(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=22)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.true_during_commit_evidence(
        token,
        transition=transition,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.reopened_state in {
        harness.ReopenedState.OLD,
        harness.ReopenedState.NEW,
    }
    assert evidence.acknowledgement_bytes == 0
    assert evidence.observed_syscall is not None


def test_injected_ioerr_write_reopens_exact_old_state(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=23)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.ioerr_write_evidence(
        token,
        transition=transition,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.sqlite_errorcode == 778
    assert evidence.reopened_state is harness.ReopenedState.OLD
    assert evidence.acknowledgement_bytes == 0


def test_max_page_count_reports_sqlite_full_and_exact_old_state(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=24)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    evidence = harness.max_page_count_evidence(
        token,
        transition=transition,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.sqlite_errorcode == sqlite3.SQLITE_FULL
    assert evidence.reopened_state is harness.ReopenedState.OLD
    assert evidence.observed_syscall == "forked-sqlite-page-allocation"


@pytest.mark.parametrize(
    ("fault_stage", "expected_writes", "expected_samples", "expected_reader_snapshot"),
    (
        pytest.param("reader_ready", 0, 0, 0, id="reader-ready"),
        pytest.param("reader_snapshot", 1, 1, 0, id="reader-snapshot"),
        pytest.param("checkpoint_truncate", 1, 1, 1, id="checkpoint-truncate"),
    ),
)
def test_wal_protocol_failure_closes_descriptors_and_reaps_exact_children(
    tmp_path: Path,
    fault_stage: str,
    expected_writes: int,
    expected_samples: int,
    expected_reader_snapshot: int,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=124)
    transition = _retain(creation, policy, reason=f"protocol-fault-{fault_stage}")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    parent_process_id = os.getpid()
    real_fork = os.fork
    real_pipe = os.pipe
    real_read_packet = harness._read_process_packet
    child_process_ids: list[int] = []
    pipe_descriptors: list[int] = []

    def recording_fork() -> int:
        process_id = real_fork()
        if os.getpid() == parent_process_id and process_id > 0:
            child_process_ids.append(process_id)
        return process_id

    def recording_pipe() -> tuple[int, int]:
        descriptors = real_pipe()
        if os.getpid() == parent_process_id:
            pipe_descriptors.extend(descriptors)
        return descriptors

    def faulting_read_packet(stage: str, descriptor: int, size: int) -> bytes:
        if os.getpid() == parent_process_id and stage == fault_stage:
            return b""
        return real_read_packet(stage, descriptor, size)

    before_descriptors = len(os.listdir("/proc/self/fd"))
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fork", recording_fork)
            patch.setattr(os, "pipe", recording_pipe)
            patch.setattr(harness, "_read_process_packet", faulting_read_packet)
            evidence = harness.wal_concurrency_evidence(
                token,
                transitions=(transition,),
                natural_key=_natural_key(creation),
            )
        assert evidence.disposition is harness.EvidenceDisposition.FAIL
        assert evidence.process_boundary == "three-forked-role-children"
        assert evidence.writes == expected_writes
        assert len(evidence.checkpoint_samples) == expected_samples
        assert evidence.reader_snapshot_version == expected_reader_snapshot
        assert len(child_process_ids) == 3
        assert len(set(child_process_ids)) == 3
        for process_id in child_process_ids:
            with pytest.raises(ChildProcessError):
                os.waitpid(process_id, os.WNOHANG)
        assert len(pipe_descriptors) == 12
        for descriptor in pipe_descriptors:
            with pytest.raises(OSError) as closed:
                os.fstat(descriptor)
            assert closed.value.errno == errno.EBADF
        assert len(os.listdir("/proc/self/fd")) == before_descriptors
        expected_final_version = 1 if fault_stage == "reader_ready" else 2
        assert evidence.final_version == expected_final_version
        assert harness.verify_store(token).history_count == expected_final_version
    finally:
        for process_id in child_process_ids:
            while True:
                try:
                    waited_process_id, _ = os.waitpid(process_id, os.WNOHANG)
                except InterruptedError:
                    continue
                except (ChildProcessError, OSError):
                    break
                if waited_process_id == 0:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(process_id, signal.SIGKILL)
                    with contextlib.suppress(ChildProcessError, OSError):
                        os.waitpid(process_id, 0)
                break
        for descriptor in pipe_descriptors:
            with contextlib.suppress(OSError):
                os.close(descriptor)


def test_long_reader_writer_and_passive_checkpointer_are_bounded(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=25)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation
    for _ in range(8):
        transition = _retain(prior, policy)
        transitions.append(transition)
        prior = transition
    evidence = harness.wal_concurrency_evidence(
        token,
        transitions=transitions,
        natural_key=_natural_key(creation),
    )
    assert evidence.disposition is harness.EvidenceDisposition.PASS
    assert evidence.reader_snapshot_version == 1
    assert evidence.final_version == 9
    assert evidence.writes == 8
    assert evidence.process_boundary == "three-forked-role-children"
    assert 0 < evidence.maximum_wal_bytes <= harness.MAX_TEST_WAL_BYTES
    assert evidence.checkpoint_samples[-1][1] > evidence.checkpoint_samples[0][1]
    assert all(0 <= checkpointed <= log for _, log, checkpointed in evidence.checkpoint_samples)


def test_online_backup_remains_coherent_during_one_concurrent_append(
    tmp_path: Path,
) -> None:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=26)
    second = _retain(creation, policy)
    concurrent_transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = second
    for index in range(16):
        transition = _retain(
            prior,
            policy,
            reason=f"backup-growth-{index:02d}-" + ("x" * 96),
        )
        concurrent_transitions.append(transition)
        prior = transition
    assert harness.create_stream(source, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(source, second).classification is (
        harness.StoreClassification.UPDATED
    )
    source_page_count_before = harness.verify_store(source).page_count
    start_writer = threading.Event()
    writer_done = threading.Event()
    writer_outcome: list[harness.StoreClassification] = []
    writer_errors: list[BaseException] = []

    def writer() -> None:
        try:
            if not start_writer.wait(timeout=40):
                raise AssertionError("bounded backup writer was never started")
            for transition in concurrent_transitions:
                writer_outcome.append(
                    harness.compare_and_swap_stream(source, transition).classification
                )
        except BaseException as error:
            writer_errors.append(error)
        finally:
            writer_done.set()

    thread = threading.Thread(target=writer)
    thread.start()

    def backup_progress() -> None:
        start_writer.set()
        if not writer_done.wait(timeout=40):
            raise AssertionError("bounded backup writer did not finish")

    try:
        backup, manifest = harness.online_backup(
            source,
            tmp_path,
            evidence_recorded_at_utc="2026-07-29T06:15:00.000000Z",
            progress_hook=backup_progress,
        )
    finally:
        start_writer.set()
        thread.join(timeout=40)
    assert not thread.is_alive()
    assert not writer_errors
    assert writer_outcome == [harness.StoreClassification.UPDATED] * len(concurrent_transitions)
    assert 2 <= manifest.source_history_rows <= 2 + len(concurrent_transitions)
    backup_summary = harness.verify_store(backup)
    assert backup_summary.history_count == manifest.source_history_rows
    assert manifest.source_page_count == manifest.destination_page_count
    assert manifest.destination_page_count == backup_summary.page_count
    source_summary = harness.verify_store(source)
    assert source_summary.history_count == 2 + len(concurrent_transitions)
    assert source_summary.page_count > source_page_count_before


def test_finite_typical_workload_measurements_and_sanitized_report(
    tmp_path: Path,
) -> None:
    assert harness.WORKLOAD_MATRIX == (
        ("minimum", 1, 1, 1),
        ("typical", 3, 9, 10),
        ("maximum_query", 1, 103, 100),
    )
    token = harness.bootstrap_store(tmp_path)
    retained: list[
        tuple[
            ContinuousPublicTradeStreamStoredCreationV1,
            bytes,
        ]
    ] = []
    for offset in range(3):
        policy, creation = _creation(seed=harness.WORKLOAD_SEED + offset)
        assert harness.create_stream(token, creation, policy).classification is (
            harness.StoreClassification.INSERTED
        )
        prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation
        for _ in range(8):
            transition = _retain(prior, policy)
            assert (
                harness.compare_and_swap_stream(
                    token,
                    transition,
                ).classification
                is harness.StoreClassification.UPDATED
            )
            prior = transition
        retained.append((creation, _natural_key(creation)))

    latency_samples: list[int] = []
    for _ in range(harness.WORKLOAD_RUNS):
        started = time.monotonic_ns()
        loaded = harness.load_current(
            token,
            stream_id=retained[0][0].record.stream_id,
            natural_key=retained[0][1],
        )
        latency_samples.append(time.monotonic_ns() - started)
        assert loaded.classification is harness.StoreClassification.FOUND
        assert loaded.query_evidence.history_rows == 3

    tracemalloc.start()
    try:
        measured = harness.load_current(
            token,
            stream_id=retained[0][0].record.stream_id,
            natural_key=retained[0][1],
        )
        _, peak_memory = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert measured.classification is harness.StoreClassification.FOUND
    assert measured.query_evidence.history_rows == 3
    report_token, report_manifest = harness.online_backup(
        token,
        tmp_path,
        evidence_recorded_at_utc="2026-07-29T06:45:00.000000Z",
    )
    summary = harness.verify_store(report_token)
    assert len(latency_samples) == harness.WORKLOAD_RUNS
    assert max(latency_samples) <= harness.MAX_OPERATION_LATENCY_NS
    assert peak_memory <= harness.MAX_TEST_TRACED_MEMORY_BYTES
    assert summary.database_bytes <= harness.MAX_TEST_DATABASE_BYTES
    assert summary.wal_bytes <= harness.MAX_TEST_WAL_BYTES
    maximum_open_cursors = harness.measured_open_cursor_evidence(
        report_token,
        stream_id=retained[0][0].record.stream_id,
        natural_key=retained[0][1],
    )
    report = harness.write_evidence_report(
        tmp_path,
        report=_evidence_report(
            summary,
            backup_manifest=report_manifest,
            recorded_at=report_manifest.evidence_recorded_at_utc,
            query_rows=4,
            maximum_open_cursors=maximum_open_cursors,
            peak_traced_memory_bytes=peak_memory,
            latency_samples_ns=tuple(latency_samples),
        ),
    )
    report_text = report.read_text(encoding="utf-8")
    assert str(tmp_path) not in report_text
    username = os.environ.get("USER")
    assert username is None or username not in report_text
