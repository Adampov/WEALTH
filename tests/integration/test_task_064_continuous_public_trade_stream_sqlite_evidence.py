"""Generated-data integration evidence for the TASK-064 SQLite harness."""

from __future__ import annotations

import ast
import contextlib
import errno
import hashlib
import json
import os
import selectors
import signal
import sqlite3
import stat
import time
from collections.abc import Callable, Iterator, Sequence
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


def _corrupt_creation_history_record(
    token: harness.StoreToken,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> None:
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.set_authorizer(None)
        trigger_row = connection.execute(
            "SELECT sql FROM sqlite_schema "
            "WHERE type = 'trigger' AND name = 'trg_history_no_update'"
        ).fetchone()
        assert trigger_row is not None
        trigger_sql = cast(str, trigger_row[0])
        connection.execute("BEGIN IMMEDIATE").close()
        try:
            connection.execute("DROP TRIGGER trg_history_no_update").close()
            cursor = connection.execute(
                """
                UPDATE continuous_public_trade_history
                SET record_canonical_bytes = ?
                WHERE stream_row_id = (
                    SELECT stream_row_id
                    FROM continuous_public_trade_stream
                    WHERE stream_uuid = ?
                )
                  AND successor_version = 1
                """,
                (
                    b"{malformed-retained-creation",
                    creation.record.stream_id.bytes,
                ),
            )
            try:
                assert cursor.rowcount == 1
            finally:
                cursor.close()
            connection.execute(trigger_sql).close()
            connection.execute("COMMIT").close()
        except BaseException:
            connection.execute("ROLLBACK").close()
            raise
    finally:
        connection.close()


def _evidence_report(
    summary: harness.VerificationSummary,
    *,
    recorded_at: str,
    evidence: harness.GeneratedEvidenceAggregate,
) -> harness.EvidenceReport:
    profile = summary.profile
    workload = evidence.workload_thresholds
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
        query_evidence=workload.query_evidence,
        database_bytes=summary.database_bytes,
        wal_bytes=summary.wal_bytes,
        page_count=summary.page_count,
        freelist_count=summary.freelist_count,
        maximum_open_cursors=workload.maximum_open_cursors,
        peak_traced_memory_bytes=workload.peak_traced_memory_bytes,
        latency_samples_ns=workload.latency_samples_ns,
        backup_manifest=evidence.backup_restore.backup_manifest,
        evidence=evidence,
    )


def _capture_rejection(
    label: str,
    *,
    pytest_root: Path | None = None,
    token: harness.StoreToken | None = None,
    stream_id: UUID | None = None,
    natural_key: bytes | None = None,
    limit: int | None = None,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
) -> tuple[str, harness.HarnessFailureCode]:
    return harness.capture_harness_rejection(
        label,
        pytest_root=pytest_root,
        token=token,
        stream_id=stream_id,
        natural_key=natural_key,
        limit=limit,
        expectation=expectation,
    )


def _capture_sqlite_rejection(
    label: str,
    connection: sqlite3.Connection,
    *,
    parameters: Sequence[object] = (),
) -> tuple[str, harness.HarnessFailureCode]:
    return harness.capture_sqlite_rejection(
        label,
        connection,
        parameters=parameters,
    )


def _report_bootstrap_path_evidence(
    tmp_path: Path,
    token: harness.StoreToken,
) -> harness.BootstrapPathEvidence:
    checks: list[tuple[str, harness.HarnessFailureCode]] = [
        _capture_rejection(
            "relative_root",
            pytest_root=Path("relative"),
        ),
        _capture_rejection(
            "unregistered_root",
            pytest_root=tmp_path / "unregistered-root",
        ),
        _capture_rejection(
            "reconstructed_root",
            pytest_root=Path(str(tmp_path)),
        ),
        _capture_rejection(
            "sibling_root",
            pytest_root=tmp_path.with_name(f"{tmp_path.name}-sibling"),
        ),
        _capture_rejection(
            "nested_root",
            pytest_root=tmp_path / "nested-root",
        ),
    ]
    alias = tmp_path.with_name(f"{tmp_path.name}-report-alias")
    alias.symlink_to(tmp_path, target_is_directory=True)
    try:
        checks.append(
            _capture_rejection(
                "symlink_root",
                pytest_root=alias,
            )
        )
    finally:
        alias.unlink()

    checks.append(
        _capture_rejection(
            "forged_token",
            token=replace(token, _nonce=b"\x00" * 32),
        )
    )

    node_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" (", maxsplit=1)[0]
    scoped_root = tmp_path / "report-scoped-root"
    scoped_root.mkdir(mode=0o700)
    with harness._pytest_root_scope(scoped_root, node_id=node_id):
        scoped_token = harness.bootstrap_store(scoped_root)
        checks.extend(
            (
                _capture_rejection(
                    "wrong_process_root",
                    pytest_root=scoped_root,
                ),
                _capture_rejection(
                    "wrong_process_token",
                    token=scoped_token,
                ),
                _capture_rejection(
                    "wrong_node_root",
                    pytest_root=scoped_root,
                ),
                _capture_rejection(
                    "wrong_node_token",
                    token=scoped_token,
                ),
            )
        )
    checks.extend(
        (
            _capture_rejection(
                "expired_root",
                pytest_root=scoped_root,
            ),
            _capture_rejection(
                "expired_token",
                token=scoped_token,
            ),
        )
    )
    harness._remove_owned_files(scoped_token)
    scoped_root.rmdir()

    registered_root = tmp_path / "report-replaced-root"
    registered_root.mkdir(mode=0o700)
    retained_root = tmp_path / "report-replaced-root-retained"
    with harness._pytest_root_scope(registered_root, node_id=node_id):
        registered_root.rename(retained_root)
        registered_root.mkdir(mode=0o700)
        try:
            checks.append(
                _capture_rejection(
                    "replaced_registered_root",
                    pytest_root=registered_root,
                )
            )
        finally:
            registered_root.rmdir()
            retained_root.rename(registered_root)
    registered_root.rmdir()

    hardlink_token = harness.bootstrap_store(tmp_path)
    hardlink = tmp_path / "forbidden-hardlink.sqlite3"
    os.link(hardlink_token._database_path, hardlink)
    try:
        checks.append(
            _capture_rejection(
                "hardlink_database",
                token=hardlink_token,
            )
        )
    finally:
        hardlink.unlink()

    unexpected_token = harness.bootstrap_store(tmp_path)
    unexpected = unexpected_token._generation_root / "unexpected-entry"
    unexpected.write_bytes(b"not-owned")
    try:
        checks.append(
            _capture_rejection(
                "unexpected_entry",
                token=unexpected_token,
            )
        )
    finally:
        unexpected.unlink()

    readonly_token = harness.bootstrap_store(tmp_path)
    os.chmod(readonly_token._database_path, 0o400)
    try:
        checks.append(
            _capture_rejection(
                "readonly_database",
                token=readonly_token,
            )
        )
    finally:
        os.chmod(readonly_token._database_path, 0o600)

    root_mode = stat_mode(tmp_path)
    os.chmod(tmp_path, root_mode ^ 0o020)
    try:
        checks.append(
            _capture_rejection(
                "widened_root",
                token=token,
            )
        )
    finally:
        os.chmod(tmp_path, root_mode)

    missing_token = harness.bootstrap_store(tmp_path)
    missing_token._database_path.unlink()
    checks.append(
        _capture_rejection(
            "missing_database",
            token=missing_token,
        )
    )

    replaced_token = harness.bootstrap_store(tmp_path)
    replaced_token._database_path.unlink()
    replacement_descriptor = os.open(
        replaced_token._database_path,
        os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    os.close(replacement_descriptor)
    checks.append(
        _capture_rejection(
            "replaced_database",
            token=replaced_token,
        )
    )

    alias_token = harness.bootstrap_store(tmp_path)
    allowed_alias = alias_token._generation_root / "store.sqlite3-shm"
    retained_allowed = tmp_path / "report-retained-shm"
    sentinel = tmp_path / "report-alias-sentinel"
    sentinel.write_bytes(b"sentinel")
    if allowed_alias.exists():
        allowed_alias.rename(retained_allowed)
    allowed_alias.symlink_to(sentinel)
    try:
        checks.append(
            _capture_rejection(
                "allowed_name_symlink",
                token=alias_token,
            )
        )
    finally:
        allowed_alias.unlink()
        if retained_allowed.exists():
            retained_allowed.rename(allowed_alias)
        sentinel.unlink()

    checks.append(
        _capture_rejection(
            "path_resolution_bootstrap",
            pytest_root=tmp_path,
        )
    )
    resolution_token = harness.bootstrap_store(tmp_path)
    checks.append(
        _capture_rejection(
            "path_resolution_operation",
            token=resolution_token,
        )
    )

    return harness.BootstrapPathEvidence(
        token=token,
        rejections=harness.RejectionEvidence(tuple(checks)),
    )


def _report_corruption_evidence(tmp_path: Path) -> harness.RejectionEvidence:
    checks: list[tuple[str, harness.HarnessFailureCode]] = []

    digest_token = harness.bootstrap_store(tmp_path)
    digest_connection, _ = harness._connect(digest_token, writer=True)
    try:
        digest_connection.set_authorizer(None)
        digest_connection.execute("BEGIN IMMEDIATE").close()
        checks.append(
            _capture_sqlite_rejection(
                "digest_byte_guard",
                digest_connection,
                parameters=(
                    99_001,
                    2,
                    b"sha256:" + (b"0" * 10) + b"\x00" + (b"f" * 53),
                    0,
                ),
            )
        )
        digest_connection.execute("ROLLBACK").close()
    finally:
        digest_connection.close()

    authorizer_token = harness.bootstrap_store(tmp_path)
    authorizer_connection, _ = harness._connect(authorizer_token, writer=True)
    try:
        checks.append(
            _capture_sqlite_rejection(
                "forbidden_schema_sql",
                authorizer_connection,
            )
        )
    finally:
        authorizer_connection.close()

    tail_token = harness.bootstrap_store(tmp_path)
    tail_policy, tail_creation = _creation(seed=4_901)
    tail_transition = _retain(
        tail_creation,
        tail_policy,
        reason="report-tail-binding",
    )
    assert (
        harness.create_stream(
            tail_token,
            tail_creation,
            tail_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    tail_connection, _ = harness._connect(tail_token, writer=True)
    try:
        tail_connection.execute("BEGIN IMMEDIATE").close()
        stream = tail_connection.execute(
            """
            SELECT stream_row_id
            FROM continuous_public_trade_stream
            WHERE stream_uuid = ?
            """,
            (tail_creation.record.stream_id.bytes,),
        ).fetchone()
        assert stream is not None
        tail_connection.execute(
            harness._INSERT_HISTORY_SQL,
            (
                stream["stream_row_id"],
                tail_transition.record.successor_version,
                b"transition",
                b"1.0",
                1,
                tail_transition.canonical_bytes,
                tail_transition.record_digest.encode("ascii"),
                tail_transition.successor_envelope.canonical_bytes,
                tail_transition.successor_envelope.envelope_digest.encode("ascii"),
                tail_transition.record.prior_version,
                tail_transition.record.prior_envelope_digest.encode("ascii"),
                tail_transition.record.prior_history_root.encode("ascii"),
                tail_creation.canonical_bytes,
                tail_creation.record_digest.encode("ascii"),
                tail_transition.history_root.encode("ascii"),
            ),
        ).close()
        checks.append(
            _capture_sqlite_rejection(
                "transition_without_current_tail",
                tail_connection,
            )
        )
        tail_connection.execute("ROLLBACK").close()
    finally:
        tail_connection.close()

    deferred_token = harness.bootstrap_store(tmp_path)
    _, deferred_creation = _creation(seed=4_902)
    deferred_connection, _ = harness._connect(deferred_token, writer=True)
    try:
        deferred_connection.execute("BEGIN IMMEDIATE").close()
        _insert_stream_only(deferred_connection, deferred_creation)
        checks.append(
            _capture_sqlite_rejection(
                "stream_without_creation_history",
                deferred_connection,
            )
        )
        deferred_connection.execute("ROLLBACK").close()

        deferred_connection.execute("BEGIN IMMEDIATE").close()
        checks.append(
            _capture_sqlite_rejection(
                "orphan_creation_history",
                deferred_connection,
                parameters=(
                    99_902,
                    1,
                    b"creation",
                    b"1.0",
                    1,
                    deferred_creation.canonical_bytes,
                    deferred_creation.record_digest.encode("ascii"),
                    deferred_creation.successor_envelope.canonical_bytes,
                    deferred_creation.successor_envelope.envelope_digest.encode("ascii"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    deferred_creation.history_root.encode("ascii"),
                ),
            )
        )
        deferred_connection.execute("ROLLBACK").close()
    finally:
        deferred_connection.close()

    immutable_token = harness.bootstrap_store(tmp_path)
    immutable_policy, immutable_creation = _creation(seed=4_903)
    assert (
        harness.create_stream(
            immutable_token,
            immutable_creation,
            immutable_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    immutable_connection, _ = harness._connect(immutable_token, writer=True)
    try:
        immutable_statements = (
            (
                "metadata_update",
                "UPDATE stream_store_metadata SET page_size = 8192",
            ),
            ("metadata_delete", "DELETE FROM stream_store_metadata"),
            (
                "history_update",
                "UPDATE continuous_public_trade_history "
                "SET serialization_version = 1 WHERE successor_version = 1",
            ),
            ("history_delete", "DELETE FROM continuous_public_trade_history"),
            (
                "stream_identity_update",
                "UPDATE continuous_public_trade_stream SET stream_contract_version = 2",
            ),
            ("stream_delete", "DELETE FROM continuous_public_trade_stream"),
            (
                "current_tail_jump",
                "UPDATE continuous_public_trade_stream SET current_version = current_version + 2",
            ),
        )
        for label, statement in immutable_statements:
            del statement
            checks.append(
                _capture_sqlite_rejection(
                    label,
                    immutable_connection,
                )
            )
    finally:
        immutable_connection.close()

    predecessor_token = harness.bootstrap_store(tmp_path)
    predecessor_policy, predecessor_creation = _creation(seed=4_904)
    predecessor_transition = _retain(
        predecessor_creation,
        predecessor_policy,
        reason="report-predecessor-binding",
    )
    assert (
        harness.create_stream(
            predecessor_token,
            predecessor_creation,
            predecessor_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    predecessor_connection, _ = harness._connect(predecessor_token, writer=True)
    stream = predecessor_connection.execute(
        """
        SELECT stream_row_id
        FROM continuous_public_trade_stream
        WHERE stream_uuid = ?
        """,
        (predecessor_creation.record.stream_id.bytes,),
    ).fetchone()
    assert stream is not None
    base_values: list[object] = [
        stream["stream_row_id"],
        predecessor_transition.record.successor_version,
        b"transition",
        b"1.0",
        1,
        predecessor_transition.canonical_bytes,
        predecessor_transition.record_digest.encode("ascii"),
        predecessor_transition.successor_envelope.canonical_bytes,
        predecessor_transition.successor_envelope.envelope_digest.encode("ascii"),
        predecessor_transition.record.prior_version,
        predecessor_transition.record.prior_envelope_digest.encode("ascii"),
        predecessor_transition.record.prior_history_root.encode("ascii"),
        predecessor_creation.canonical_bytes,
        predecessor_creation.record_digest.encode("ascii"),
        predecessor_transition.history_root.encode("ascii"),
    ]
    hostile_values: list[tuple[str, list[object]]] = []
    wrong_predecessor = list(base_values)
    wrong_predecessor[12] = predecessor_creation.canonical_bytes + b" "
    hostile_values.append(("wrong_predecessor_bytes", wrong_predecessor))
    wrong_digest = list(base_values)
    wrong_digest[13] = _digest("report-wrong-predecessor").encode("ascii")
    hostile_values.append(("wrong_predecessor_digest", wrong_digest))
    wrong_root = list(base_values)
    wrong_root[11] = _digest("report-wrong-root").encode("ascii")
    hostile_values.append(("wrong_predecessor_root", wrong_root))
    gap = list(base_values)
    gap[1] = 3
    gap[9] = 2
    hostile_values.append(("history_gap", gap))
    try:
        for label, values in hostile_values:
            predecessor_connection.execute("BEGIN IMMEDIATE").close()
            checks.append(
                _capture_sqlite_rejection(
                    label,
                    predecessor_connection,
                    parameters=values,
                )
            )
            predecessor_connection.execute("ROLLBACK").close()
    finally:
        predecessor_connection.close()

    unsupported_token = harness.bootstrap_store(tmp_path)
    unsupported_connection, _ = harness._connect(unsupported_token, writer=True)
    try:
        unsupported_connection.execute("PRAGMA user_version = 2").close()
    finally:
        unsupported_connection.close()
    checks.append(
        _capture_rejection(
            "unsupported_generation",
            token=unsupported_token,
        )
    )

    zero_token = harness.bootstrap_store(tmp_path)
    zero_connection, _ = harness._connect(zero_token, writer=True)
    try:
        zero_connection.execute("PRAGMA user_version = 0").close()
    finally:
        zero_connection.close()
    checks.append(
        _capture_rejection(
            "malformed_generation",
            token=zero_token,
        )
    )

    short_token = harness.bootstrap_store(tmp_path)
    short_connection, _ = harness._connect(short_token, writer=True)
    try:
        short_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").close()
    finally:
        short_connection.close()
    os.truncate(short_token._database_path, harness.PAGE_SIZE - 1)
    checks.append(
        _capture_rejection(
            "short_page",
            token=short_token,
        )
    )

    overflow_token = harness.bootstrap_store(tmp_path)
    overflow_policy, overflow_creation = _creation(seed=4_905)
    assert (
        harness.create_stream(
            overflow_token,
            overflow_creation,
            overflow_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    checks.append(
        _capture_rejection(
            "audit_limit_overflow",
            token=overflow_token,
            stream_id=overflow_creation.record.stream_id,
            natural_key=_natural_key(overflow_creation),
            limit=101,
        )
    )

    retained_token = harness.bootstrap_store(tmp_path)
    retained_policy, retained_creation = _creation(seed=4_906)
    assert (
        harness.create_stream(
            retained_token,
            retained_creation,
            retained_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    _corrupt_creation_history_record(retained_token, retained_creation)
    checks.append(
        _capture_rejection(
            "retained_history_canonical_bytes",
            token=retained_token,
        )
    )

    two_candidate_token = harness.bootstrap_store(tmp_path)
    two_policy_a, two_creation_a = _creation(seed=4_907)
    two_policy_b, two_creation_b = _creation(seed=4_908)
    assert (
        harness.create_stream(
            two_candidate_token,
            two_creation_a,
            two_policy_a,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    assert (
        harness.create_stream(
            two_candidate_token,
            two_creation_b,
            two_policy_b,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    _corrupt_creation_history_record(two_candidate_token, two_creation_b)
    checks.append(
        _capture_rejection(
            "two_candidate_corrupt_precedence",
            token=two_candidate_token,
            stream_id=two_creation_a.record.stream_id,
            natural_key=_natural_key(two_creation_b),
            limit=100,
        )
    )

    expectation_token = harness.bootstrap_store(tmp_path)
    expectation_policy, expectation_creation = _creation(seed=4_909)
    assert (
        harness.create_stream(
            expectation_token,
            expectation_creation,
            expectation_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    _corrupt_creation_history_record(expectation_token, expectation_creation)
    checks.append(
        _capture_rejection(
            "expectation_conflict_corrupt_precedence",
            token=expectation_token,
            stream_id=expectation_creation.record.stream_id,
            natural_key=_natural_key(expectation_creation),
            limit=100,
            expectation=_expectation(
                expectation_policy,
                expectation_creation,
                source="report-mismatching-source",
            ),
        )
    )

    return harness.RejectionEvidence(tuple(checks))


def _report_fresh_process_evidence(
    tmp_path: Path,
) -> harness.FreshProcessEvidenceAggregate:
    create_faults: list[harness.FaultEvidence] = []
    for offset, seam in enumerate(harness._CREATE_KILL_SEAM_ORDER):
        token = harness.bootstrap_store(tmp_path)
        policy, creation = _creation(seed=5_000 + offset)
        create_faults.append(
            harness.fresh_process_kill_evidence(
                token,
                seam=seam,
                creation=creation,
                policy=policy,
                natural_key=_natural_key(creation),
            )
        )

    compare_and_swap_faults: list[harness.FaultEvidence] = []
    for offset, seam in enumerate(harness._CAS_KILL_SEAM_ORDER):
        token = harness.bootstrap_store(tmp_path)
        policy, creation = _creation(seed=5_100 + offset)
        transition = _retain(
            creation,
            policy,
            reason=f"report-cas-fault-{offset}",
        )
        assert harness.create_stream(token, creation, policy).classification is (
            harness.StoreClassification.INSERTED
        )
        compare_and_swap_faults.append(
            harness.fresh_process_kill_evidence(
                token,
                seam=seam,
                transition=transition,
                natural_key=_natural_key(creation),
            )
        )

    result_code_faults = tuple(
        harness.sqlite_result_code_fault_evidence(
            harness.bootstrap_store(tmp_path),
            seam=seam,
        )
        for seam in ("readonly", "busy")
    )

    during_token = harness.bootstrap_store(tmp_path)
    during_policy, during_creation = _creation(seed=5_200)
    during_transition = _retain(
        during_creation,
        during_policy,
        reason="report-true-during-commit",
    )
    assert (
        harness.create_stream(
            during_token,
            during_creation,
            during_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    during_commit = harness.true_during_commit_evidence(
        during_token,
        transition=during_transition,
        natural_key=_natural_key(during_creation),
    )

    ioerr_token = harness.bootstrap_store(tmp_path)
    ioerr_policy, ioerr_creation = _creation(seed=5_201)
    ioerr_transition = _retain(
        ioerr_creation,
        ioerr_policy,
        reason="report-ioerr",
    )
    assert (
        harness.create_stream(
            ioerr_token,
            ioerr_creation,
            ioerr_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    ioerr = harness.ioerr_write_evidence(
        ioerr_token,
        transition=ioerr_transition,
        natural_key=_natural_key(ioerr_creation),
    )

    full_token = harness.bootstrap_store(tmp_path)
    full_policy, full_creation = _creation(seed=5_202)
    full_transition = _retain(
        full_creation,
        full_policy,
        reason="report-full",
    )
    assert (
        harness.create_stream(
            full_token,
            full_creation,
            full_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    full = harness.max_page_count_evidence(
        full_token,
        transition=full_transition,
        natural_key=_natural_key(full_creation),
    )

    create_contention_token = harness.bootstrap_store(tmp_path)
    create_contention_policy, create_contention = _creation(seed=5_300)
    create_contention_evidence = harness.fresh_process_writer_contention_evidence(
        create_contention_token,
        operation="create",
        creation=create_contention,
        policy=create_contention_policy,
        natural_key=_natural_key(create_contention),
    )
    cas_contention_token = harness.bootstrap_store(tmp_path)
    cas_contention_policy, cas_contention = _creation(seed=5_301)
    cas_contention_transition = _retain(
        cas_contention,
        cas_contention_policy,
        reason="report-cas-contention",
    )
    assert (
        harness.create_stream(
            cas_contention_token,
            cas_contention,
            cas_contention_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    cas_contention_evidence = harness.fresh_process_writer_contention_evidence(
        cas_contention_token,
        operation="compare_and_swap",
        transition=cas_contention_transition,
        natural_key=_natural_key(cas_contention),
    )

    duplicate_create_token = harness.bootstrap_store(tmp_path)
    duplicate_create_policy, duplicate_create = _creation(seed=5_400)
    duplicate_create_evidence = harness.fresh_process_two_writer_evidence(
        duplicate_create_token,
        operation="create",
        creations=(duplicate_create, duplicate_create),
        policy=duplicate_create_policy,
    )
    conflict_create_token = harness.bootstrap_store(tmp_path)
    conflict_create_policy, conflict_create = _creation(seed=5_401)
    _, competing_create = _creation(
        seed=5_401,
        stream_id_seed=54_010,
        identity_seed=5_401,
    )
    conflict_create_evidence = harness.fresh_process_two_writer_evidence(
        conflict_create_token,
        operation="create",
        creations=(conflict_create, competing_create),
        policy=conflict_create_policy,
    )
    duplicate_cas_token = harness.bootstrap_store(tmp_path)
    duplicate_cas_policy, duplicate_cas_creation = _creation(seed=5_402)
    duplicate_cas = _retain(
        duplicate_cas_creation,
        duplicate_cas_policy,
        reason="report-duplicate-cas",
    )
    assert (
        harness.create_stream(
            duplicate_cas_token,
            duplicate_cas_creation,
            duplicate_cas_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    duplicate_cas_evidence = harness.fresh_process_two_writer_evidence(
        duplicate_cas_token,
        operation="compare_and_swap",
        transitions=(duplicate_cas, duplicate_cas),
    )
    conflict_cas_token = harness.bootstrap_store(tmp_path)
    conflict_cas_policy, conflict_cas_creation = _creation(seed=5_403)
    accepted_cas = _retain(
        conflict_cas_creation,
        conflict_cas_policy,
        reason="report-accepted-cas",
    )
    competing_cas = _retain(
        conflict_cas_creation,
        conflict_cas_policy,
        reason="report-competing-cas",
    )
    assert (
        harness.create_stream(
            conflict_cas_token,
            conflict_cas_creation,
            conflict_cas_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    conflict_cas_evidence = harness.fresh_process_two_writer_evidence(
        conflict_cas_token,
        operation="compare_and_swap",
        transitions=(accepted_cas, competing_cas),
    )

    wal_token = harness.bootstrap_store(tmp_path)
    wal_policy, wal_creation = _creation(seed=5_500)
    assert (
        harness.create_stream(
            wal_token,
            wal_creation,
            wal_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    wal_transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    wal_prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = wal_creation
    for offset in range(8):
        transition = _retain(
            wal_prior,
            wal_policy,
            reason=f"report-wal-{offset}",
        )
        wal_transitions.append(transition)
        wal_prior = transition
    wal = harness.wal_concurrency_evidence(
        wal_token,
        transitions=wal_transitions,
        natural_key=_natural_key(wal_creation),
    )

    return harness.FreshProcessEvidenceAggregate(
        create_faults=tuple(create_faults),
        compare_and_swap_faults=tuple(compare_and_swap_faults),
        result_code_faults=result_code_faults,
        true_during_commit=during_commit,
        ioerr_write=ioerr,
        max_page_count=full,
        writer_contentions=(
            create_contention_evidence,
            cas_contention_evidence,
        ),
        two_writers=(
            duplicate_create_evidence,
            conflict_create_evidence,
            duplicate_cas_evidence,
            conflict_cas_evidence,
        ),
        wal_concurrency=wal,
    )


def _report_atomicity_evidence(
    tmp_path: Path,
    fresh_process: harness.FreshProcessEvidenceAggregate,
    *,
    substitute_semantics: bool = False,
) -> harness.AtomicityClassificationEvidence:
    token = harness.bootstrap_store(tmp_path)
    policy_a, creation_a = _creation(seed=5_600)
    inserted = harness.create_stream(token, creation_a, policy_a)
    transition_a_v2 = _retain(
        creation_a,
        policy_a,
        reason="report-atomicity-a-v2",
    )
    updated = harness.compare_and_swap_stream(token, transition_a_v2)
    transition_a_v3 = _retain(
        transition_a_v2,
        policy_a,
        reason="report-atomicity-a-v3",
    )
    harness.compare_and_swap_stream(token, transition_a_v3)
    duplicate_create = harness.create_stream(token, creation_a, policy_a)
    duplicate_compare_and_swap = harness.compare_and_swap_stream(token, transition_a_v3)

    policy_b, creation_b = _creation(seed=5_601)
    harness.create_stream(token, creation_b, policy_b)
    transition_b_v2 = _retain(
        creation_b,
        policy_b,
        reason="report-atomicity-b-v2",
    )
    harness.compare_and_swap_stream(token, transition_b_v2)
    transition_b_v3 = _retain(
        transition_b_v2,
        policy_b,
        reason="report-atomicity-b-v3",
    )
    harness.compare_and_swap_stream(token, transition_b_v3)

    mixed_policy, mixed_creation = (
        _creation(
            seed=5_601,
            stream_id_seed=5_601,
            identity_seed=5_600,
        )
        if substitute_semantics
        else _creation(
            seed=5_600,
            stream_id_seed=5_600,
            identity_seed=5_601,
        )
    )
    conflicting_create = harness.create_stream(
        token,
        mixed_creation,
        mixed_policy,
    )
    conflict_prior, conflict_policy, conflict_expectation = (
        (transition_b_v3, policy_b, _expectation(policy_a, creation_a))
        if substitute_semantics
        else (transition_a_v3, policy_a, _expectation(policy_b, creation_b))
    )
    conflict_transition = _retain(
        conflict_prior,
        conflict_policy,
        reason=(
            "report-atomicity-substituted-v4-conflict"
            if substitute_semantics
            else "report-atomicity-a-v4-conflict"
        ),
    )
    conflicting_compare_and_swap = harness.compare_and_swap_stream(
        token,
        conflict_transition,
        expectation=conflict_expectation,
    )
    return harness.AtomicityClassificationEvidence(
        mutations=(
            inserted,
            duplicate_create,
            conflicting_create,
            updated,
            duplicate_compare_and_swap,
            conflicting_compare_and_swap,
        ),
        two_writers=fresh_process.two_writers,
        unknown_acknowledgements=(
            fresh_process.create_faults[-1],
            fresh_process.compare_and_swap_faults[-1],
        ),
    )


def _report_bounded_query_evidence(
    token: harness.StoreToken,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    tmp_path: Path,
) -> tuple[harness.CurrentSlice, harness.BoundedQueryEvidenceAggregate]:
    natural_key = _natural_key(creation)
    projection = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
    )
    missing_policy, missing_creation = _creation(seed=5_700)
    del missing_policy
    missing = harness.load_current(
        token,
        stream_id=missing_creation.record.stream_id,
        natural_key=_natural_key(missing_creation),
    )
    initial_one = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=1,
    )
    harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=10,
    )
    continued_one = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=1,
        continuation=(
            creation.record.successor_version,
            creation.successor_envelope.envelope_digest,
            creation.history_root,
        ),
    )
    assert projection.current is not None
    tail = projection.current
    at_tail = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=100,
        continuation=(
            tail.record.successor_version,
            tail.successor_envelope.envelope_digest,
            tail.history_root,
        ),
    )
    anchor_conflict = harness.audit_history(
        token,
        stream_id=creation.record.stream_id,
        natural_key=natural_key,
        limit=1,
        continuation=(
            creation.record.successor_version,
            _digest("report-wrong-audit-anchor"),
            creation.history_root,
        ),
    )

    conflict_token = harness.bootstrap_store(tmp_path)
    conflict_policy_a, conflict_creation_a = _creation(seed=5_701)
    conflict_policy_b, conflict_creation_b = _creation(seed=5_702)
    assert (
        harness.create_stream(
            conflict_token,
            conflict_creation_a,
            conflict_policy_a,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    conflict_a_v2 = _retain(
        conflict_creation_a,
        conflict_policy_a,
        reason="report-bounded-conflict-a-v2",
    )
    assert (
        harness.compare_and_swap_stream(conflict_token, conflict_a_v2).classification
        is harness.StoreClassification.UPDATED
    )
    conflict_a_v3 = _retain(
        conflict_a_v2,
        conflict_policy_a,
        reason="report-bounded-conflict-a-v3",
    )
    assert (
        harness.compare_and_swap_stream(conflict_token, conflict_a_v3).classification
        is harness.StoreClassification.UPDATED
    )
    assert (
        harness.create_stream(
            conflict_token,
            conflict_creation_b,
            conflict_policy_b,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    conflict_b_v2 = _retain(
        conflict_creation_b,
        conflict_policy_b,
        reason="report-bounded-conflict-b-v2",
    )
    assert (
        harness.compare_and_swap_stream(conflict_token, conflict_b_v2).classification
        is harness.StoreClassification.UPDATED
    )
    conflict_b_v3 = _retain(
        conflict_b_v2,
        conflict_policy_b,
        reason="report-bounded-conflict-b-v3",
    )
    assert (
        harness.compare_and_swap_stream(conflict_token, conflict_b_v3).classification
        is harness.StoreClassification.UPDATED
    )
    current_conflict = harness.load_current(
        conflict_token,
        stream_id=conflict_creation_a.record.stream_id,
        natural_key=_natural_key(conflict_creation_b),
    )
    audit_conflict = harness.audit_history(
        conflict_token,
        stream_id=conflict_creation_a.record.stream_id,
        natural_key=_natural_key(conflict_creation_b),
        limit=100,
    )

    maximum_token = harness.bootstrap_store(tmp_path)
    maximum_policy, maximum_creation = _creation(seed=5_703)
    assert (
        harness.create_stream(
            maximum_token,
            maximum_creation,
            maximum_policy,
        ).classification
        is harness.StoreClassification.INSERTED
    )
    maximum_prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = maximum_creation
    for offset in range(102):
        maximum_transition = _retain(
            maximum_prior,
            maximum_policy,
            reason=f"report-maximum-query-{offset:03d}",
        )
        assert (
            harness.compare_and_swap_stream(
                maximum_token,
                maximum_transition,
            ).classification
            is harness.StoreClassification.UPDATED
        )
        maximum_prior = maximum_transition
        if (offset + 1) % 20 == 0:
            _truncate_generated_wal(maximum_token)
    initial_hundred = harness.audit_history(
        maximum_token,
        stream_id=maximum_creation.record.stream_id,
        natural_key=_natural_key(maximum_creation),
        limit=100,
    )
    continued_hundred = harness.audit_history(
        maximum_token,
        stream_id=maximum_creation.record.stream_id,
        natural_key=_natural_key(maximum_creation),
        limit=100,
        continuation=(
            maximum_creation.record.successor_version,
            maximum_creation.successor_envelope.envelope_digest,
            maximum_creation.history_root,
        ),
    )
    plans = harness.query_plan_evidence(
        maximum_token,
        stream_id=maximum_creation.record.stream_id,
        natural_key=_natural_key(maximum_creation),
    )
    plan_names = (
        "identity",
        "current",
        "audit_initial_1",
        "audit_continuation_1",
        "audit_initial_100",
        "audit_continuation_100",
    )
    return projection, harness.BoundedQueryEvidenceAggregate(
        queries=(
            ("current_found", projection.classification, projection.query_evidence),
            ("current_not_found", missing.classification, missing.query_evidence),
            (
                "current_identity_conflict",
                current_conflict.classification,
                current_conflict.query_evidence,
            ),
            (
                "audit_identity_conflict",
                audit_conflict.classification,
                audit_conflict.query_evidence,
            ),
            ("audit_initial_1", initial_one.classification, initial_one.query_evidence),
            (
                "audit_continuation_1",
                continued_one.classification,
                continued_one.query_evidence,
            ),
            (
                "audit_initial_100",
                initial_hundred.classification,
                initial_hundred.query_evidence,
            ),
            (
                "audit_continuation_100",
                continued_hundred.classification,
                continued_hundred.query_evidence,
            ),
            ("audit_at_tail", at_tail.classification, at_tail.query_evidence),
            (
                "audit_anchor_conflict",
                anchor_conflict.classification,
                anchor_conflict.query_evidence,
            ),
        ),
        plans=tuple((name, plans[name]) for name in plan_names),
    )


def _report_generation_copy_evidence(
    source: harness.StoreToken,
    tmp_path: Path,
) -> harness.GenerationCopyEvidence:
    source_summary = harness.verify_store(source)
    source_generation_id = harness._generation_evidence_id(source)
    source_tails = harness._tail_manifest(source)
    destination = harness.same_format_generation_copy(source, tmp_path)
    return harness.GenerationCopyEvidence(
        source_token=source,
        destination_token=destination,
        source_generation_id=source_generation_id,
        destination_generation_id=harness._generation_evidence_id(destination),
        source_summary=source_summary,
        destination_summary=harness.verify_store(destination),
        source_tails=source_tails,
        destination_tails=harness._tail_manifest(destination),
    )


def _report_concurrent_backup_evidence(
    tmp_path: Path,
) -> harness.ConcurrentBackupEvidence:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=5_800)
    second = _retain(
        creation,
        policy,
        reason="report-concurrent-backup-prior",
    )
    assert (
        harness.create_stream(source, creation, policy).classification
        is harness.StoreClassification.INSERTED
    )
    assert (
        harness.compare_and_swap_stream(source, second).classification
        is harness.StoreClassification.UPDATED
    )
    transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = second
    for offset in range(16):
        transition = _retain(
            prior,
            policy,
            reason=f"report-concurrent-backup-{offset:02d}-" + ("x" * 96),
        )
        transitions.append(transition)
        prior = transition
    return harness.concurrent_write_backup_evidence(
        source,
        tmp_path,
        transitions=tuple(transitions),
        evidence_recorded_at_utc="2026-07-29T06:47:00.000000Z",
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
    historical_duplicate = harness.compare_and_swap_stream(token, third)
    assert historical_duplicate.classification is harness.StoreClassification.DUPLICATE
    assert historical_duplicate.stream_rows == 1
    assert historical_duplicate.history_rows == 5
    assert historical_duplicate.rows_materialized == 6
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
    prior_a: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation_a
    prior_b: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation_b
    for version in (2, 3):
        transition_a = _retain(prior_a, policy_a, reason=f"mature-conflict-a-v{version}")
        transition_b = _retain(prior_b, policy_b, reason=f"mature-conflict-b-v{version}")
        assert harness.compare_and_swap_stream(token, transition_a).classification is (
            harness.StoreClassification.UPDATED
        )
        assert harness.compare_and_swap_stream(token, transition_b).classification is (
            harness.StoreClassification.UPDATED
        )
        prior_a = transition_a
        prior_b = transition_b

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
    assert two_row_conflict.query_evidence.history_rows == 6
    assert two_row_conflict.query_evidence.decoded_rows == 6
    audit_conflict = harness.audit_history(
        token,
        stream_id=creation_a.record.stream_id,
        natural_key=_natural_key(creation_b),
        limit=100,
    )
    assert audit_conflict.classification is harness.StoreClassification.IDENTITY_CONFLICT
    assert audit_conflict.query_evidence.stream_rows == 2
    assert audit_conflict.query_evidence.history_rows == 6
    assert audit_conflict.query_evidence.decoded_rows == 6
    assert harness.verify_store(token).stream_count == 2


def test_audit_identity_conflict_cannot_mask_corrupt_candidate_history(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy_a, creation_a = _creation(seed=301)
    policy_b, creation_b = _creation(seed=302)
    assert harness.create_stream(token, creation_a, policy_a).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.create_stream(token, creation_b, policy_b).classification is (
        harness.StoreClassification.INSERTED
    )
    _corrupt_creation_history_record(token, creation_b)

    with pytest.raises(harness.HarnessFailure) as corrupt:
        harness.audit_history(
            token,
            stream_id=creation_a.record.stream_id,
            natural_key=_natural_key(creation_b),
            limit=100,
        )
    assert corrupt.value.code is harness.HarnessFailureCode.CORRUPT


def test_audit_expectation_conflict_cannot_mask_corrupt_candidate_history(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=303)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    _corrupt_creation_history_record(token, creation)

    with pytest.raises(harness.HarnessFailure) as corrupt:
        harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=_natural_key(creation),
            limit=100,
            expectation=_expectation(
                policy,
                creation,
                source="coherent-but-different-source",
            ),
        )
    assert corrupt.value.code is harness.HarnessFailureCode.CORRUPT


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


def test_expectation_conflicts_cannot_mask_transition_link_corruption(
    tmp_path: Path,
) -> None:
    single = harness.bootstrap_store(tmp_path)
    policy_a, creation_a = _creation(seed=342)
    a_v2 = _retain(creation_a, policy_a, reason="expectation-corrupt-a-v2")
    a_v3 = _retain(a_v2, policy_a, reason="expectation-corrupt-a-v3")
    valid_a_v4 = _retain(a_v3, policy_a, reason="expectation-corrupt-a-v4")
    regressed_a_v4 = _with_recorded_at(
        a_v3,
        valid_a_v4,
        a_v3.record.recorded_at - timedelta(microseconds=1),
    )
    assert (
        harness.create_stream(single, creation_a, policy_a).classification
        is harness.StoreClassification.INSERTED
    )
    assert (
        harness.compare_and_swap_stream(single, a_v2).classification
        is harness.StoreClassification.UPDATED
    )
    assert (
        harness.compare_and_swap_stream(single, a_v3).classification
        is harness.StoreClassification.UPDATED
    )
    with pytest.raises(harness.HarnessFailure) as single_candidate:
        harness.compare_and_swap_stream(
            single,
            regressed_a_v4,
            expectation=_expectation(_policy(seed=3_420), creation_a),
        )
    assert single_candidate.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.verify_store(single).history_count == 3

    mature = harness.bootstrap_store(tmp_path)
    policy_b, creation_b = _creation(seed=343)
    b_v2 = _retain(creation_b, policy_b, reason="expectation-corrupt-b-v2")
    b_v3 = _retain(b_v2, policy_b, reason="expectation-corrupt-b-v3")
    for creation, policy, second, third in (
        (creation_a, policy_a, a_v2, a_v3),
        (creation_b, policy_b, b_v2, b_v3),
    ):
        assert (
            harness.create_stream(mature, creation, policy).classification
            is harness.StoreClassification.INSERTED
        )
        assert (
            harness.compare_and_swap_stream(mature, second).classification
            is harness.StoreClassification.UPDATED
        )
        assert (
            harness.compare_and_swap_stream(mature, third).classification
            is harness.StoreClassification.UPDATED
        )
    with pytest.raises(harness.HarnessFailure) as two_candidates:
        harness.compare_and_swap_stream(
            mature,
            regressed_a_v4,
            expectation=_expectation(policy_b, creation_b),
        )
    assert two_candidates.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.verify_store(mature).history_count == 6


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


def test_report_collectors_reject_cross_root_before_delegate_and_preserve_slots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def exact_inventory(root: Path) -> tuple[tuple[str, str, int, str], ...]:
        result: list[tuple[str, str, int, str]] = []
        for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
            details = path.lstat()
            if path.is_file():
                result.append(
                    (
                        path.relative_to(root).as_posix(),
                        "file",
                        details.st_size,
                        hashlib.sha256(path.read_bytes()).hexdigest(),
                    )
                )
            else:
                result.append(
                    (
                        path.relative_to(root).as_posix(),
                        "directory",
                        details.st_mode,
                        "",
                    )
                )
        return tuple(result)

    def generation_set(root: Path) -> tuple[str, ...]:
        return tuple(
            sorted(
                path.relative_to(root).as_posix()
                for path in root.rglob("continuous-public-trade-v1-*")
                if path.is_dir()
            )
        )

    run = harness.begin_generated_evidence_run(tmp_path)
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=2_401)
    prior = _retain(
        creation,
        policy,
        reason="collector-preflight-prior-" + ("x" * 96),
    )
    assert harness.create_stream(source, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(source, prior).classification is (
        harness.StoreClassification.UPDATED
    )
    transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    for index in range(harness.CONCURRENT_BACKUP_TRANSITIONS):
        transition = _retain(
            prior,
            policy,
            reason=f"collector-preflight-{index:02d}-" + ("x" * 96),
        )
        transitions.append(transition)
        prior = transition
    exact_transitions = tuple(transitions)
    ledger = harness._validated_evidence_run(run)

    chain_calls = 0
    backup_calls = 0
    copy_calls = 0
    real_chain = harness._validated_applicable_transition_chain
    real_backup = harness.concurrent_write_backup_evidence
    real_copy = harness.same_format_generation_copy

    def counted_chain(
        token: harness.StoreToken,
        candidate: object,
    ) -> tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...]:
        nonlocal chain_calls
        chain_calls += 1
        return real_chain(token, candidate)

    def counted_backup(
        token: harness.StoreToken,
        root: Path,
        *,
        transitions: tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...],
        evidence_recorded_at_utc: str,
    ) -> harness.ConcurrentBackupEvidence:
        nonlocal backup_calls
        backup_calls += 1
        return real_backup(
            token,
            root,
            transitions=transitions,
            evidence_recorded_at_utc=evidence_recorded_at_utc,
        )

    def counted_copy(
        token: harness.StoreToken,
        root: Path,
    ) -> harness.StoreToken:
        nonlocal copy_calls
        copy_calls += 1
        return real_copy(token, root)

    monkeypatch.setattr(harness, "_validated_applicable_transition_chain", counted_chain)
    monkeypatch.setattr(harness, "concurrent_write_backup_evidence", counted_backup)
    monkeypatch.setattr(harness, "same_format_generation_copy", counted_copy)

    node_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" (", maxsplit=1)[0]
    foreign_backup_root = tmp_path / "foreign-backup-root"
    foreign_backup_root.mkdir(mode=0o700)
    with harness._pytest_root_scope(foreign_backup_root, node_id=node_id):
        foreign_source = harness.bootstrap_store(foreign_backup_root)
        before_summary = harness.verify_store(source)
        before_tails = harness._tail_manifest(source)
        before_manifest = harness._closed_file_manifest(source)
        before_inventory = exact_inventory(tmp_path)
        before_generations = generation_set(tmp_path)
        before_ledger = (
            dict(ledger.observations),
            dict(ledger.operation_runs),
            dict(ledger.rejection_runs),
        )
        for candidate_token, candidate_root in (
            (foreign_source, tmp_path),
            (source, foreign_backup_root),
            (foreign_source, foreign_backup_root),
        ):
            with pytest.raises(harness.HarnessFailure) as rejected:
                harness.collect_backup_restore_evidence(
                    run,
                    candidate_token,
                    candidate_root,
                    transitions=exact_transitions,
                    backup_recorded_at_utc="2026-07-29T06:35:00.000000Z",
                    restore_recorded_at_utc="2026-07-29T06:36:00.000000Z",
                )
            assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
        assert (chain_calls, backup_calls) == (0, 0)
        assert exact_inventory(tmp_path) == before_inventory
        assert generation_set(tmp_path) == before_generations
        assert harness._closed_file_manifest(source) == before_manifest
        assert harness._tail_manifest(source) == before_tails
        assert harness.verify_store(source) == before_summary
        assert (
            dict(ledger.observations),
            dict(ledger.operation_runs),
            dict(ledger.rejection_runs),
        ) == before_ledger

    backup_restore = harness.collect_backup_restore_evidence(
        run,
        source,
        tmp_path,
        transitions=exact_transitions,
        backup_recorded_at_utc="2026-07-29T06:35:00.000000Z",
        restore_recorded_at_utc="2026-07-29T06:36:00.000000Z",
    )
    assert (chain_calls, backup_calls) == (4, 1)
    report_token = backup_restore.backup_token
    with harness.bootstrap_path_operation_scope(run):
        _report_bootstrap_path_evidence(tmp_path, report_token)
    harness.finalize_bootstrap_path_evidence(run, report_token)

    before_summary = harness.verify_store(report_token)
    before_tails = harness._tail_manifest(report_token)
    before_manifest = harness._closed_file_manifest(report_token)
    before_inventory = exact_inventory(tmp_path)
    before_generations = generation_set(tmp_path)
    before_ledger = (
        dict(ledger.observations),
        dict(ledger.operation_runs),
        dict(ledger.rejection_runs),
    )
    with pytest.raises(harness.HarnessFailure) as wrong_same_root_source:
        harness.collect_generation_copy_evidence(
            run,
            backup_restore.source_token,
            tmp_path,
        )
    assert wrong_same_root_source.value.code is harness.HarnessFailureCode.CORRUPT
    assert copy_calls == 0
    assert exact_inventory(tmp_path) == before_inventory
    assert generation_set(tmp_path) == before_generations
    assert harness._closed_file_manifest(report_token) == before_manifest
    assert harness._tail_manifest(report_token) == before_tails
    assert harness.verify_store(report_token) == before_summary
    assert (
        dict(ledger.observations),
        dict(ledger.operation_runs),
        dict(ledger.rejection_runs),
    ) == before_ledger

    foreign_copy_root = tmp_path / "foreign-copy-root"
    foreign_copy_root.mkdir(mode=0o700)
    with harness._pytest_root_scope(foreign_copy_root, node_id=node_id):
        foreign_report = harness.bootstrap_store(foreign_copy_root)
        before_summary = harness.verify_store(report_token)
        before_tails = harness._tail_manifest(report_token)
        before_manifest = harness._closed_file_manifest(report_token)
        before_inventory = exact_inventory(tmp_path)
        before_generations = generation_set(tmp_path)
        before_ledger = (
            dict(ledger.observations),
            dict(ledger.operation_runs),
            dict(ledger.rejection_runs),
        )
        for candidate_token, candidate_root in (
            (foreign_report, tmp_path),
            (report_token, foreign_copy_root),
            (foreign_report, foreign_copy_root),
        ):
            with pytest.raises(harness.HarnessFailure) as rejected:
                harness.collect_generation_copy_evidence(
                    run,
                    candidate_token,
                    candidate_root,
                )
            assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
        assert copy_calls == 0
        assert exact_inventory(tmp_path) == before_inventory
        assert generation_set(tmp_path) == before_generations
        assert harness._closed_file_manifest(report_token) == before_manifest
        assert harness._tail_manifest(report_token) == before_tails
        assert harness.verify_store(report_token) == before_summary
        assert (
            dict(ledger.observations),
            dict(ledger.operation_runs),
            dict(ledger.rejection_runs),
        ) == before_ledger

    copied = harness.collect_generation_copy_evidence(
        run,
        report_token,
        tmp_path,
    )
    assert copy_calls == 1
    assert copied.source_token is report_token
    assert copied.destination_tails == copied.source_tails
    assert (
        copied.destination_summary.stream_count,
        copied.destination_summary.history_count,
        copied.destination_summary.schema_fingerprint,
        copied.destination_summary.page_count,
    ) == (
        copied.source_summary.stream_count,
        copied.source_summary.history_count,
        copied.source_summary.schema_fingerprint,
        copied.source_summary.page_count,
    )

    removed_tokens: list[harness.StoreToken] = []
    real_remove_new_token = harness._remove_new_collector_token

    def remove_then_signal_first_failure(
        active_run: Any,
        token: object,
        *,
        retained_nonces: frozenset[bytes],
    ) -> None:
        assert type(token) is harness.StoreToken
        removed_tokens.append(token)
        real_remove_new_token(
            active_run,
            token,
            retained_nonces=retained_nonces,
        )
        if len(removed_tokens) == 1:
            raise harness.HarnessFailure(harness.HarnessFailureCode.UNAVAILABLE)

    monkeypatch.setattr(
        harness,
        "_remove_new_collector_token",
        remove_then_signal_first_failure,
    )
    with pytest.raises(harness.HarnessFailure) as total_cleanup:
        harness._rollback_backup_restore_outputs(
            run,
            {"source_token": source},
            backup_restore,
        )
    assert total_cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert removed_tokens == [
        backup_restore.restore_token,
        backup_restore.backup_token,
    ]
    assert harness.verify_store(source).history_count == (
        backup_restore.source_summary.history_count
    )


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


def test_direct_backup_and_copy_apis_reject_cross_scope_before_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=5_850)
    assert (
        harness.create_stream(source, creation, policy).classification
        is harness.StoreClassification.INSERTED
    )
    transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation
    for offset in range(harness.CONCURRENT_BACKUP_TRANSITIONS):
        transition = _retain(
            prior,
            policy,
            reason=f"direct-preflight-{offset:02d}",
        )
        transitions.append(transition)
        prior = transition
    exact_transitions = tuple(transitions)
    node_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" (", maxsplit=1)[0]
    foreign_root = tmp_path / "direct-foreign-root"
    foreign_root.mkdir(mode=0o700)
    foreign_token: harness.StoreToken | None = None
    with harness._pytest_root_scope(foreign_root, node_id=node_id):
        foreign_token = harness.bootstrap_store(foreign_root)
        source_summary = harness.verify_store(source)
        source_tails = harness._tail_manifest(source)
        source_files = harness._closed_file_manifest(source)
        source_inventory = tuple(sorted(path.name for path in tmp_path.iterdir()))
        verify_calls = 0
        bootstrap_calls = 0
        pipe_calls = 0
        real_verify = harness.verify_store
        real_bootstrap = harness.bootstrap_store
        real_open_pipes = harness._open_pipes

        def counted_verify(token: harness.StoreToken) -> harness.VerificationSummary:
            nonlocal verify_calls
            verify_calls += 1
            return real_verify(token)

        def counted_bootstrap(root: Path) -> harness.StoreToken:
            nonlocal bootstrap_calls
            bootstrap_calls += 1
            return real_bootstrap(root)

        def counted_pipes(count: int) -> tuple[tuple[int, int], ...]:
            nonlocal pipe_calls
            pipe_calls += 1
            return real_open_pipes(count)

        monkeypatch.setattr(harness, "verify_store", counted_verify)
        monkeypatch.setattr(harness, "bootstrap_store", counted_bootstrap)
        monkeypatch.setattr(harness, "_open_pipes", counted_pipes)
        for candidate_token, candidate_root in (
            (foreign_token, tmp_path),
            (source, foreign_root),
        ):
            with pytest.raises(harness.HarnessFailure) as online_rejected:
                harness.online_backup(
                    candidate_token,
                    candidate_root,
                    evidence_recorded_at_utc="2026-07-29T06:40:00.000000Z",
                )
            assert online_rejected.value.code is harness.HarnessFailureCode.CORRUPT
            with pytest.raises(harness.HarnessFailure) as concurrent_rejected:
                harness.concurrent_write_backup_evidence(
                    candidate_token,
                    candidate_root,
                    transitions=exact_transitions,
                    evidence_recorded_at_utc="2026-07-29T06:41:00.000000Z",
                )
            assert concurrent_rejected.value.code is harness.HarnessFailureCode.CORRUPT
            with pytest.raises(harness.HarnessFailure) as copy_rejected:
                harness.same_format_generation_copy(
                    candidate_token,
                    candidate_root,
                )
            assert copy_rejected.value.code is harness.HarnessFailureCode.CORRUPT
        assert (verify_calls, bootstrap_calls, pipe_calls) == (0, 0, 0)
        assert tuple(sorted(path.name for path in tmp_path.iterdir())) == source_inventory
        monkeypatch.setattr(harness, "verify_store", real_verify)
        monkeypatch.setattr(harness, "bootstrap_store", real_bootstrap)
        monkeypatch.setattr(harness, "_open_pipes", real_open_pipes)
        assert harness.verify_store(source) == source_summary
        assert harness._tail_manifest(source) == source_tails
        assert harness._closed_file_manifest(source) == source_files
        valid_copy = harness.same_format_generation_copy(source, tmp_path)
        assert harness.verify_store(valid_copy).history_count == source_summary.history_count
        harness._remove_owned_files(valid_copy)
    assert foreign_token is not None
    harness._remove_owned_files(foreign_token)


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

    hardlink = tmp_path / "forbidden-hardlink.sqlite3"
    os.link(token._database_path, hardlink)
    try:
        assert set(os.listdir(token._generation_root)) <= {
            "store.sqlite3",
            "store.sqlite3-wal",
            "store.sqlite3-shm",
        }
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


def test_rejection_capture_does_not_accept_caller_callbacks(tmp_path: Path) -> None:
    def caller_callback() -> object:
        raise sqlite3.IntegrityError("caller-manufactured")

    with pytest.raises(TypeError):
        cast(Any, harness.capture_harness_rejection)(
            "relative_root",
            invoke=caller_callback,
        )
    with pytest.raises(TypeError):
        cast(Any, harness.capture_sqlite_rejection)(
            "digest_byte_guard",
            invoke=caller_callback,
        )
    assert not (tmp_path / "task064-evidence.json").exists()


def test_direct_run_constructor_cannot_mint_evidence_authority(tmp_path: Path) -> None:
    for removed_issuer in (
        "_operation_evidence_executor",
        "_operation_evidence_executor_unsealed",
        "_build_operation_evidence_authority",
        "_OPERATION_PRODUCERS_FROZEN",
        "_whole_gate_collector",
        "_whole_gate_collector_unsealed",
        "_build_whole_gate_authority",
        "_GATE_COLLECTORS_FROZEN",
        "_registered_rejection_scenario",
        "_REGISTERED_REJECTION_SCENARIOS",
        "_build_rejection_scenario_authority",
        "_GATE_OPERATION_ALLOWLIST",
        "_FRESH_OPERATION_PRODUCER_SEQUENCE",
        "_ATOMICITY_OPERATION_PRODUCER_SEQUENCE",
        "_BOUNDED_OPERATION_PRODUCER_SEQUENCE",
        "_GATE_OPERATION_PRODUCER_SEQUENCES",
        "_canonical_operation_sequence",
        "_canonical_rejection_scenario",
        "_validate_canonical_rejection_scenario",
        "_canonical_rejection_scenario_snapshot",
        "_private_gate_receipt_digest",
        "_validate_issued_operation_observation",
        "_validate_issued_rejection_observation",
        "_seal_generated_evidence_run_unsealed",
        "_validate_evidence_receipt",
        "_validate_evidence_receipt_unbound",
        "_validate_issued_evidence_receipt",
        "_consume_issued_evidence_receipt",
        "_write_evidence_report_unbound",
        "_build_evidence_report_writer",
        "_build_private_gate_receipt_authority",
        "_build_rejection_evidence_authority",
        "_build_evidence_receipt_authority",
    ):
        assert not hasattr(harness, removed_issuer)
    run = harness.begin_generated_evidence_run(tmp_path)
    ledger = run._pytest_registration.evidence_ledger
    forged = replace(run)
    ledger.run = forged
    context_token = harness._ACTIVE_EVIDENCE_RUN.set(forged)
    try:
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._validated_evidence_run(forged)
        assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
    finally:
        harness._ACTIVE_EVIDENCE_RUN.reset(context_token)
        ledger.run = run
    assert harness._validated_evidence_run(run) is ledger


def test_rejection_scenario_mutation_cannot_rebind_outcome(tmp_path: Path) -> None:
    run = harness.begin_generated_evidence_run(tmp_path)
    ledger = harness._validated_evidence_run(run)
    bootstrap_calls = 0
    real_bootstrap_store = harness.bootstrap_store

    def counted_bootstrap_store(pytest_root: Path) -> harness.StoreToken:
        nonlocal bootstrap_calls
        bootstrap_calls += 1
        return real_bootstrap_store(pytest_root)

    with (
        pytest.MonkeyPatch.context() as patch,
        pytest.raises(harness.HarnessFailure) as mutated_scenario,
        harness.bootstrap_path_operation_scope(run),
    ):
        patch.setattr(harness, "bootstrap_store", counted_bootstrap_store)
        state = harness._ACTIVE_REJECTION_COLLECTOR.get()
        assert state is not None
        scenario = state.scenarios[0]
        original_code = scenario.code
        object.__setattr__(
            scenario,
            "code",
            harness.HarnessFailureCode.INVALID_TOKEN,
        )
        try:
            harness.capture_harness_rejection(
                "relative_root",
                pytest_root=Path("relative"),
            )
        finally:
            object.__setattr__(scenario, "code", original_code)
    assert mutated_scenario.value.code is harness.HarnessFailureCode.CORRUPT
    assert bootstrap_calls == 0
    assert scenario.code is original_code
    assert harness._ACTIVE_REJECTION_COLLECTOR.get() is None
    assert ledger.operation_runs == {}
    assert ledger.rejection_runs == {}


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
        with pytest.raises(harness.HarnessFailure) as replaced_alias:
            harness._remove_owned_files(token)
        assert replaced_alias.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert sentinel.read_bytes() == b"sentinel-must-survive"
        assert token._nonce in harness._TOKEN_REGISTRY
    finally:
        original_generation.unlink()
        retained_generation.rename(original_generation)
        harness._remove_owned_files(token)
        sentinel.unlink()
        sentinel_root.rmdir()
    assert token._nonce not in harness._TOKEN_REGISTRY
    assert not original_generation.exists()


@pytest.mark.parametrize(
    "fault",
    ("unlink", "rmdir", "generation_close", "root_close"),
)
def test_owned_cleanup_failures_retain_retry_authority(
    tmp_path: Path,
    fault: str,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    registered = harness._TOKEN_REGISTRY[token._nonce]
    real_unlink = os.unlink
    real_rmdir = os.rmdir
    real_close = os.close
    injected = False

    def fail_unlink(
        path: str | bytes,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        if not injected and path == "store.sqlite3":
            injected = True
            raise OSError(errno.EIO, "injected owned unlink failure")
        real_unlink(path, *args, **kwargs)

    def fail_rmdir(
        path: str | bytes,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        nonlocal injected
        if not injected and path == token._generation_root.name:
            injected = True
            raise OSError(errno.EIO, "injected owned rmdir failure")
        real_rmdir(path, *args, **kwargs)

    target_inode = (
        registered.generation_inode if fault == "generation_close" else registered.pytest_root_inode
    )

    def fail_close(descriptor: int) -> None:
        nonlocal injected
        details = os.fstat(descriptor)
        if (
            not injected
            and details.st_dev
            == (
                registered.generation_device
                if fault == "generation_close"
                else registered.pytest_root_device
            )
            and details.st_ino == target_inode
        ):
            injected = True
            real_close(descriptor)
            raise OSError(errno.EIO, "injected owned close uncertainty")
        real_close(descriptor)

    with pytest.MonkeyPatch.context() as patch:
        if fault == "unlink":
            patch.setattr(os, "unlink", fail_unlink)
        elif fault == "rmdir":
            patch.setattr(os, "rmdir", fail_rmdir)
        else:
            patch.setattr(os, "close", fail_close)
        with pytest.raises(harness.HarnessFailure) as cleanup:
            harness._remove_owned_files(token)
    assert injected
    assert cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert harness._TOKEN_REGISTRY.get(token._nonce) is registered
    harness._remove_owned_files(token)
    assert token._nonce not in harness._TOKEN_REGISTRY
    assert not token._generation_root.exists()


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


def test_process_lifecycle_helpers_are_bounded_exact_and_fail_closed(
    tmp_path: Path,
) -> None:
    source = Path(harness.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    waitpid_sites: list[tuple[str, ast.Call]] = []

    class WaitpidVisitor(ast.NodeVisitor):
        current_function = ""

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            prior = self.current_function
            self.current_function = node.name
            self.generic_visit(node)
            self.current_function = prior

        def visit_Call(self, node: ast.Call) -> None:
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "os"
                and node.func.attr == "waitpid"
            ):
                waitpid_sites.append((self.current_function, node))
            self.generic_visit(node)

    WaitpidVisitor().visit(tree)
    assert waitpid_sites
    assert {function for function, _ in waitpid_sites} <= {
        "_wait_for_owned_process_event",
        "_terminate_and_reap_processes",
    }
    assert all(
        len(call.args) == 2
        and not (isinstance(call.args[1], ast.Constant) and call.args[1].value == 0)
        for _, call in waitpid_sites
    )
    assert "os.WNOHANG" in source
    assert "os.WUNTRACED" in source

    real_waitpid = os.waitpid
    system_calls: list[tuple[str, int]] = []

    def forbidden_waitpid(process_id: int, options: int) -> tuple[int, int]:
        system_calls.append(("wait", process_id))
        return real_waitpid(process_id, options)

    def forbidden_kill(process_id: int, signal_number: int) -> None:
        del signal_number
        system_calls.append(("kill", process_id))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", forbidden_waitpid)
        patch.setattr(os, "kill", forbidden_kill)
        for invalid_process_id in (0, -1, True):
            with pytest.raises(harness.HarnessFailure) as invalid_wait:
                harness._wait_for_owned_process_event(cast(Any, invalid_process_id))
            assert invalid_wait.value.code is harness.HarnessFailureCode.CORRUPT
            with pytest.raises(harness.HarnessFailure) as invalid_terminate:
                harness._terminate_and_reap_processes((cast(Any, invalid_process_id),))
            assert invalid_terminate.value.code is harness.HarnessFailureCode.CORRUPT
    assert not system_calls

    child_process_id = os.fork()
    if child_process_id == 0:
        os._exit(0)
    interrupted = False

    def interrupt_once(process_id: int, options: int) -> tuple[int, int]:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise InterruptedError
        return real_waitpid(process_id, options)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", interrupt_once)
        child_status = harness._wait_for_owned_process(
            child_process_id,
            timeout_seconds=1.0,
        )
    assert interrupted
    assert child_status is not None
    assert os.WIFEXITED(child_status)
    assert os.WEXITSTATUS(child_status) == 0
    with pytest.raises(ChildProcessError):
        os.waitpid(child_process_id, os.WNOHANG)

    ticks = -0.01

    def advancing_monotonic() -> float:
        nonlocal ticks
        ticks += 0.01
        return ticks

    def always_interrupted(_process_id: int, _options: int) -> tuple[int, int]:
        raise InterruptedError

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", always_interrupted)
        patch.setattr(time, "monotonic", advancing_monotonic)
        assert (
            harness._wait_for_owned_process_event(
                123_456,
                timeout_seconds=0.02,
            )
            is None
        )

    attempted_signals: list[int] = []

    def record_signal(process_id: int, _signal_number: int) -> None:
        attempted_signals.append(process_id)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            os,
            "waitpid",
            lambda _process_id, _options: (_ for _ in ()).throw(
                OSError(errno.EIO, "unproven ownership")
            ),
        )
        patch.setattr(os, "kill", record_signal)
        assert not harness._terminate_and_reap_processes((123_459,))
    assert attempted_signals == []

    ownership_ticks = -11.0

    def expiring_ownership_clock() -> float:
        nonlocal ownership_ticks
        ownership_ticks += 11.0
        return ownership_ticks

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", always_interrupted)
        patch.setattr(time, "monotonic", expiring_ownership_clock)
        patch.setattr(os, "kill", record_signal)
        assert not harness._terminate_and_reap_processes((123_460,))
    assert attempted_signals == []

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            os,
            "waitpid",
            lambda process_id, _options: (process_id + 1, 0),
        )
        patch.setattr(os, "kill", record_signal)
        assert not harness._terminate_and_reap_processes((123_461,))
    assert attempted_signals == []

    stopped_status = (signal.SIGSTOP << 8) | 0x7F
    assert os.WIFSTOPPED(stopped_status)
    observed_options: list[int] = []

    def stopped_waitpid(process_id: int, options: int) -> tuple[int, int]:
        observed_options.append(options)
        return process_id, stopped_status

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", stopped_waitpid)
        assert (
            harness._wait_for_owned_process_event(
                123_457,
                include_stopped=True,
            )
            == stopped_status
        )
    assert observed_options == [os.WNOHANG | os.WUNTRACED]

    ticks = -0.01
    nonterminal_observations = 0

    def stopped_then_pending(process_id: int, options: int) -> tuple[int, int]:
        nonlocal nonterminal_observations
        del options
        nonterminal_observations += 1
        return (process_id, stopped_status) if nonterminal_observations == 1 else (0, 0)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "waitpid", stopped_then_pending)
        patch.setattr(time, "monotonic", advancing_monotonic)
        assert (
            harness._wait_for_owned_process_event(
                123_458,
                timeout_seconds=0.02,
            )
            is None
        )
    assert nonterminal_observations >= 1

    stopped_child = os.fork()
    if stopped_child == 0:
        os.kill(os.getpid(), signal.SIGSTOP)
        os._exit(99)
    observed_stop = harness._wait_for_owned_process_event(
        stopped_child,
        timeout_seconds=1.0,
        include_stopped=True,
    )
    assert observed_stop is not None
    assert os.WIFSTOPPED(observed_stop)
    assert harness._terminate_and_reap_processes((stopped_child,))
    with pytest.raises(ChildProcessError):
        os.waitpid(stopped_child, os.WNOHANG)

    partial_read, partial_write = os.pipe()
    partial_child = os.fork()
    if partial_child == 0:
        os.close(partial_read)
        os.write(partial_write, b"ab")
        os.write(partial_write, b"cd")
        os.close(partial_write)
        os._exit(0)
    os.close(partial_write)
    real_read = os.read
    read_interrupted = False

    def interrupt_partial_read(descriptor: int, size: int) -> bytes:
        nonlocal read_interrupted
        if not read_interrupted:
            read_interrupted = True
            raise InterruptedError
        return real_read(descriptor, min(size, 2))

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "read", interrupt_partial_read)
        assert harness._read_process_packet("partial_packet", partial_read, 4) == b"abcd"
    os.close(partial_read)
    partial_status = harness._wait_for_owned_process(partial_child, timeout_seconds=1.0)
    assert partial_status is not None
    assert os.WIFEXITED(partial_status)
    assert os.WEXITSTATUS(partial_status) == 0

    descriptor_count_before = len(os.listdir("/proc/self/fd"))
    read_descriptor, write_descriptor = os.pipe()
    stalled_process_id = os.fork()
    if stalled_process_id == 0:
        os.close(read_descriptor)
        os.write(write_descriptor, b"R")
        while True:
            signal.pause()
    os.close(write_descriptor)
    assert harness._read_process_packet("stalled_child_ready", read_descriptor, 1) == b"R"
    assert (
        harness._wait_or_terminate_owned_process(
            stalled_process_id,
            timeout_seconds=0.05,
        )
        is None
    )
    os.close(read_descriptor)
    with pytest.raises(ChildProcessError):
        os.waitpid(stalled_process_id, os.WNOHANG)
    assert len(os.listdir("/proc/self/fd")) == descriptor_count_before

    reaped_process_id = os.fork()
    if reaped_process_id == 0:
        os._exit(0)
    real_terminate = harness._terminate_and_reap_processes

    def reap_then_report_false(process_ids: Sequence[int]) -> bool:
        assert real_terminate(process_ids)
        return False

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            harness,
            "_terminate_and_reap_processes",
            reap_then_report_false,
        )
        with pytest.raises(harness.HarnessFailure) as uncertain_reap:
            harness._finalize_process_resources((), (reaped_process_id,))
    assert uncertain_reap.value.code is harness.HarnessFailureCode.UNAVAILABLE
    with pytest.raises(ChildProcessError):
        os.waitpid(reaped_process_id, os.WNOHANG)

    descriptor_count_before = len(os.listdir("/proc/self/fd"))
    read_descriptor, write_descriptor = os.pipe()
    cleanup_process_id = os.fork()
    if cleanup_process_id == 0:
        while True:
            signal.pause()
    real_close_descriptors = harness._close_descriptors

    def close_then_report_false(descriptors: Sequence[int]) -> bool:
        assert real_close_descriptors(descriptors)
        return False

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_close_descriptors", close_then_report_false)
        with pytest.raises(harness.HarnessFailure) as uncertain_descriptors:
            harness._finalize_process_resources(
                (read_descriptor, write_descriptor),
                (cleanup_process_id,),
            )
    assert uncertain_descriptors.value.code is harness.HarnessFailureCode.UNAVAILABLE
    with pytest.raises(ChildProcessError):
        os.waitpid(cleanup_process_id, os.WNOHANG)
    assert len(os.listdir("/proc/self/fd")) == descriptor_count_before
    assert tmp_path.is_dir()


def test_every_forked_evidence_api_rejects_live_connection_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=7_450)
    transition = _retain(creation, policy, reason="live-connection-fork-guard")
    assert (
        harness.create_stream(token, creation, policy).classification
        is harness.StoreClassification.INSERTED
    )
    before = harness.verify_store(token)
    connection, _ = harness._connect(token, writer=True)
    fork_calls = 0

    def forbidden_fork() -> int:
        nonlocal fork_calls
        fork_calls += 1
        raise AssertionError("fork must not run with a live SQLite connection")

    monkeypatch.setattr(os, "fork", forbidden_fork)
    calls: tuple[Callable[[], object], ...] = (
        lambda: harness.sqlite_result_code_fault_evidence(token, seam="readonly"),
        lambda: harness.fresh_process_writer_contention_evidence(
            token,
            operation="create",
            natural_key=_natural_key(creation),
            creation=creation,
            policy=policy,
        ),
        lambda: harness.fresh_process_two_writer_evidence(
            token,
            operation="create",
            creations=(creation, creation),
            policy=policy,
        ),
        lambda: harness.fresh_process_kill_evidence(
            token,
            seam="before_transaction",
            creation=creation,
            policy=policy,
            natural_key=_natural_key(creation),
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
        lambda: harness.wal_concurrency_evidence(
            token,
            transitions=(transition,),
            natural_key=_natural_key(creation),
        ),
        lambda: harness.concurrent_write_backup_evidence(
            token,
            tmp_path,
            transitions=(transition,) * harness.CONCURRENT_BACKUP_TRANSITIONS,
            evidence_recorded_at_utc="2026-07-29T06:42:00.000000Z",
        ),
    )
    try:
        for invoke in calls:
            with pytest.raises(harness.HarnessFailure) as guarded:
                invoke()
            assert guarded.value.code is harness.HarnessFailureCode.UNPROVEN
    finally:
        connection.close()
    assert fork_calls == 0
    assert harness.verify_store(token) == before


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


def test_ioerr_evidence_rejects_nonzero_child_exit_after_exact_reap(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=230)
    transition = _retain(creation, policy)
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    real_wait = harness._wait_or_terminate_owned_process
    reaped_process_ids: list[int] = []

    def return_nonzero_after_reap(
        process_id: int,
        *,
        timeout_seconds: float = 10.0,
    ) -> int | None:
        status = real_wait(process_id, timeout_seconds=timeout_seconds)
        assert status is not None
        reaped_process_ids.append(process_id)
        return 1 << 8

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            harness,
            "_wait_or_terminate_owned_process",
            return_nonzero_after_reap,
        )
        evidence = harness.ioerr_write_evidence(
            token,
            transition=transition,
            natural_key=_natural_key(creation),
        )
    assert evidence.disposition is harness.EvidenceDisposition.UNPROVEN
    assert evidence.reason == "ioerr_child_protocol_failed"
    assert len(reaped_process_ids) == 1
    with pytest.raises(ChildProcessError):
        os.waitpid(reaped_process_ids[0], os.WNOHANG)
    current = harness.load_current(
        token,
        stream_id=creation.record.stream_id,
        natural_key=_natural_key(creation),
    )
    assert current.current == creation


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
    evidence = harness.concurrent_write_backup_evidence(
        source,
        tmp_path,
        transitions=tuple(concurrent_transitions),
        evidence_recorded_at_utc="2026-07-29T06:15:00.000000Z",
    )
    assert evidence.source_token is source
    assert evidence.progress_observations == 1
    assert len(evidence.writer_mutations) == harness.CONCURRENT_BACKUP_TRANSITIONS
    assert all(
        mutation.classification is harness.StoreClassification.UPDATED
        for mutation in evidence.writer_mutations
    )
    assert evidence.source_after.page_count > evidence.source_before.page_count
    assert (
        evidence.source_before.page_count
        <= evidence.backup_manifest.source_page_count
        <= evidence.source_after.page_count
    )
    assert evidence.backup_summary.history_count == (
        evidence.backup_manifest.destination_history_rows
    )


def test_concurrent_backup_ready_failure_reaps_exact_child(
    tmp_path: Path,
) -> None:
    source = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=27)
    second = _retain(creation, policy)
    assert harness.create_stream(source, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(source, second).classification is (
        harness.StoreClassification.UPDATED
    )
    transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = second
    for index in range(harness.CONCURRENT_BACKUP_TRANSITIONS):
        transition = _retain(
            prior,
            policy,
            reason=f"ready-failure-{index:02d}-" + ("x" * 96),
        )
        transitions.append(transition)
        prior = transition
    before = harness.verify_store(source)
    generation_names = {
        path.name
        for path in tmp_path.iterdir()
        if path.is_dir() and path.name.startswith("continuous-public-trade-v1-")
    }
    real_fork = os.fork
    parent_child_ids: list[int] = []

    def recording_fork() -> int:
        process_id = real_fork()
        if process_id > 0:
            parent_child_ids.append(process_id)
        return process_id

    real_read_packet = harness._read_process_packet

    def fail_ready(stage: str, descriptor: int, size: int) -> bytes:
        if stage == "concurrent_backup_ready":
            raise OSError(errno.EIO, "injected ready failure")
        return real_read_packet(stage, descriptor, size)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", recording_fork)
        patch.setattr(harness, "_read_process_packet", fail_ready)
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness.concurrent_write_backup_evidence(
                source,
                tmp_path,
                transitions=tuple(transitions),
                evidence_recorded_at_utc="2026-07-29T06:16:00.000000Z",
            )
    assert rejected.value.code is harness.HarnessFailureCode.UNPROVEN
    assert len(parent_child_ids) == 1
    with pytest.raises(ChildProcessError):
        os.waitpid(parent_child_ids[0], os.WNOHANG)
    after = harness.verify_store(source)
    assert (after.stream_count, after.history_count) == (
        before.stream_count,
        before.history_count,
    )
    assert {
        path.name
        for path in tmp_path.iterdir()
        if path.is_dir() and path.name.startswith("continuous-public-trade-v1-")
    } == generation_names


def test_finite_typical_workload_measurements_and_sanitized_report(
    tmp_path: Path,
) -> None:
    assert harness.WORKLOAD_MATRIX == (
        ("minimum", 1, 1, 1),
        ("typical", 3, 9, 10),
        ("maximum_query", 1, 103, 100),
    )
    evidence_run = harness.begin_generated_evidence_run(tmp_path)
    token = harness.bootstrap_store(tmp_path)
    retained: list[
        tuple[
            ContinuousPublicTradeStreamStoredCreationV1,
            bytes,
            ContinuousPublicTradeStreamStoredHistoryEntryV1,
        ]
    ] = []
    for offset in range(3):
        policy, creation = _creation(seed=harness.WORKLOAD_SEED + offset)
        created = harness.create_stream(token, creation, policy)
        assert created.classification is harness.StoreClassification.INSERTED
        prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = creation
        for _ in range(8):
            transition = _retain(prior, policy)
            updated = harness.compare_and_swap_stream(
                token,
                transition,
            )
            assert updated.classification is harness.StoreClassification.UPDATED
            prior = transition
        retained.append((creation, _natural_key(creation), prior))

    concurrent_transitions: list[ContinuousPublicTradeStreamStoredTransitionV1] = []
    concurrent_prior = retained[0][2]
    first_policy = _policy(harness.WORKLOAD_SEED)
    for offset in range(harness.CONCURRENT_BACKUP_TRANSITIONS):
        concurrent_transition = _retain(
            concurrent_prior,
            first_policy,
            reason=f"report-concurrent-primary-{offset:02d}-" + ("x" * 96),
        )
        concurrent_transitions.append(concurrent_transition)
        concurrent_prior = concurrent_transition
    backup_restore = harness.collect_backup_restore_evidence(
        evidence_run,
        token,
        tmp_path,
        transitions=tuple(concurrent_transitions),
        backup_recorded_at_utc="2026-07-29T06:45:00.000000Z",
        restore_recorded_at_utc="2026-07-29T06:46:00.000000Z",
    )
    report_token = backup_restore.backup_token
    report_manifest = backup_restore.backup_manifest
    restore_manifest = backup_restore.restore_manifest
    summary = harness.collect_schema_identity_evidence(evidence_run, report_token)
    with harness.bootstrap_path_operation_scope(evidence_run):
        _report_bootstrap_path_evidence(tmp_path, report_token)
    bootstrap_path = harness.finalize_bootstrap_path_evidence(
        evidence_run,
        report_token,
    )
    runtime_controls = harness.collect_runtime_connection_controls_evidence(
        evidence_run,
        report_token,
        summary,
    )
    projection = harness.collect_projection_roundtrip_evidence(
        evidence_run,
        report_token,
        stream_id=retained[0][0].record.stream_id,
        natural_key=retained[0][1],
    )
    with harness.schema_corruption_operation_scope(evidence_run):
        _report_corruption_evidence(tmp_path)
    corruption = harness.finalize_schema_corruption_evidence(
        evidence_run,
        report_token,
    )
    with harness.fresh_process_operation_scope(evidence_run):
        _report_fresh_process_evidence(tmp_path)
    fresh_process = harness.finalize_fresh_process_evidence(
        evidence_run,
        report_token,
    )
    evidence_ledger = harness._validated_evidence_run(evidence_run)
    atomicity_ordinal = harness.GENERATED_EVIDENCE_GATES.index("atomicity_classification")
    harness.__dict__["_ATOMICITY_OPERATION_PRODUCER_SEQUENCE"] = ("bootstrap_store",)
    harness.__dict__["_GATE_OPERATION_PRODUCER_SEQUENCES"] = {
        "atomicity_classification": ("bootstrap_store",)
    }
    try:
        with harness.atomicity_operation_scope(evidence_run):
            _report_atomicity_evidence(tmp_path, fresh_process)
    finally:
        harness.__dict__.pop("_ATOMICITY_OPERATION_PRODUCER_SEQUENCE")
        harness.__dict__.pop("_GATE_OPERATION_PRODUCER_SEQUENCES")
    atomicity_operations = evidence_ledger.operation_runs["atomicity_classification"]
    historical_binding_substitution = list(atomicity_operations)
    historical_binding_substitution[5] = replace(
        historical_binding_substitution[5],
        input_binding=atomicity_operations[8].input_binding,
        input_digest=atomicity_operations[8].input_digest,
    )
    evidence_ledger.operation_runs["atomicity_classification"] = tuple(
        historical_binding_substitution
    )
    with pytest.raises(harness.HarnessFailure) as historical_substitution:
        harness.finalize_atomicity_evidence(evidence_run, report_token)
    assert historical_substitution.value.code is harness.HarnessFailureCode.CORRUPT
    assert atomicity_ordinal not in evidence_ledger.observations
    evidence_ledger.operation_runs["atomicity_classification"] = atomicity_operations
    for operation_index, substituted_tag in (
        (5, "DUPLICATE:stream_rows=1:history_rows=5"),
        (9, "CONFLICT:stream_rows=2:history_rows=6"),
        (10, "CONFLICT:stream_rows=2:history_rows=6"),
    ):
        substituted_operations = list(atomicity_operations)
        substituted_operations[operation_index] = replace(
            substituted_operations[operation_index],
            operation_tag=substituted_tag,
        )
        evidence_ledger.operation_runs["atomicity_classification"] = tuple(substituted_operations)
        with pytest.raises(harness.HarnessFailure) as semantic_substitution:
            harness.finalize_atomicity_evidence(evidence_run, report_token)
        assert semantic_substitution.value.code is harness.HarnessFailureCode.CORRUPT
        assert atomicity_ordinal not in evidence_ledger.observations
    evidence_ledger.operation_runs["atomicity_classification"] = atomicity_operations
    atomicity = harness.finalize_atomicity_evidence(
        evidence_run,
        report_token,
    )
    assert tuple(
        (item.classification, item.stream_rows, item.history_rows) for item in atomicity.mutations
    ) == (
        (harness.StoreClassification.INSERTED, 0, 0),
        (harness.StoreClassification.DUPLICATE, 1, 3),
        (harness.StoreClassification.CONFLICT, 2, 6),
        (harness.StoreClassification.UPDATED, 1, 2),
        (harness.StoreClassification.DUPLICATE, 1, 5),
        (harness.StoreClassification.CONFLICT, 2, 6),
    )
    with harness.bounded_query_operation_scope(evidence_run):
        _report_bounded_query_evidence(
            report_token,
            retained[0][0],
            tmp_path,
        )
    bounded_operations = evidence_ledger.operation_runs["bounded_queries"]
    missing_binding = bounded_operations[1].input_binding
    assert isinstance(missing_binding, harness._QueryOperationBinding)
    hostile_missing_expectation = _expectation(
        first_policy,
        retained[0][0],
    )
    hostile_missing_binding = harness._operation_input_binding(
        "load_current",
        {
            "stream_id": missing_binding.stream_id,
            "natural_key": missing_binding.natural_key,
            "expectation": hostile_missing_expectation,
        },
    )
    assert isinstance(hostile_missing_binding, harness._QueryOperationBinding)
    missing_expectation_operations = list(bounded_operations)
    missing_expectation_operations[1] = replace(
        missing_expectation_operations[1],
        input_binding=hostile_missing_binding,
        input_digest=harness._evidence_payload_digest(hostile_missing_binding),
    )
    evidence_ledger.operation_runs["bounded_queries"] = tuple(missing_expectation_operations)
    with pytest.raises(harness.HarnessFailure) as missing_expectation_substitution:
        harness.finalize_bounded_query_evidence(
            evidence_run,
            report_token,
        )
    assert missing_expectation_substitution.value.code is harness.HarnessFailureCode.CORRUPT
    same_shape_binding = bounded_operations[2].input_binding
    assert isinstance(same_shape_binding, harness._QueryOperationBinding)
    wrong_stream_same_shape_binding = replace(
        same_shape_binding,
        stream_id=retained[1][0].record.stream_id,
        natural_key=retained[1][1],
    )
    same_shape_operations = list(bounded_operations)
    same_shape_operations[2] = replace(
        same_shape_operations[2],
        input_binding=wrong_stream_same_shape_binding,
        input_digest=harness._evidence_payload_digest(
            wrong_stream_same_shape_binding,
        ),
    )
    evidence_ledger.operation_runs["bounded_queries"] = tuple(same_shape_operations)
    with pytest.raises(harness.HarnessFailure) as same_shape_query_splice:
        harness.finalize_bounded_query_evidence(
            evidence_run,
            report_token,
        )
    assert same_shape_query_splice.value.code is harness.HarnessFailureCode.CORRUPT
    spliced_operations = list(bounded_operations)
    spliced_operations[2] = replace(
        spliced_operations[2],
        input_binding=bounded_operations[3].input_binding,
        input_digest=bounded_operations[3].input_digest,
    )
    evidence_ledger.operation_runs["bounded_queries"] = tuple(spliced_operations)
    with pytest.raises(harness.HarnessFailure) as coherent_query_splice:
        harness.finalize_bounded_query_evidence(
            evidence_run,
            report_token,
        )
    assert coherent_query_splice.value.code is harness.HarnessFailureCode.CORRUPT
    evidence_ledger.operation_runs["bounded_queries"] = bounded_operations
    bounded_queries = harness.finalize_bounded_query_evidence(
        evidence_run,
        report_token,
    )
    assert tuple(
        (
            bounded_queries.queries[index][2].stream_rows,
            bounded_queries.queries[index][2].history_rows,
            bounded_queries.queries[index][2].decoded_rows,
        )
        for index in (2, 3)
    ) == ((2, 6, 6), (2, 6, 6))
    shallow_queries = list(bounded_queries.queries)
    for index in (2, 3):
        name, classification, query = shallow_queries[index]
        shallow_queries[index] = (
            name,
            classification,
            replace(query, history_rows=2, decoded_rows=2),
        )
    with pytest.raises(harness.HarnessFailure) as shallow_conflicts:
        harness._validate_bounded_query_report_evidence(
            replace(bounded_queries, queries=tuple(shallow_queries)),
            projection,
        )
    assert shallow_conflicts.value.code is harness.HarnessFailureCode.CORRUPT
    closed_mapping = harness.collect_closed_error_mapping_evidence(
        evidence_run,
        report_token,
    )
    generation_copy = harness.collect_generation_copy_evidence(
        evidence_run,
        report_token,
        tmp_path,
    )
    workload = harness.collect_workload_threshold_evidence(
        evidence_run,
        report_token,
        stream_id=retained[0][0].record.stream_id,
        natural_key=retained[0][1],
    )
    assert len(workload.latency_samples_ns) == harness.WORKLOAD_RUNS
    assert max(workload.latency_samples_ns) <= harness.MAX_OPERATION_LATENCY_NS
    assert workload.peak_traced_memory_bytes <= harness.MAX_TEST_TRACED_MEMORY_BYTES
    assert summary.database_bytes <= harness.MAX_TEST_DATABASE_BYTES
    assert summary.wal_bytes <= harness.MAX_TEST_WAL_BYTES
    evidence = harness.GeneratedEvidenceAggregate(
        schema_identity=summary,
        bootstrap_path_ownership=bootstrap_path,
        runtime_connection_controls=runtime_controls,
        projection_roundtrip=projection,
        schema_constraints_corruption=corruption,
        atomicity_classification=atomicity,
        fresh_process_faults=fresh_process,
        bounded_queries=bounded_queries,
        closed_error_mapping=closed_mapping,
        backup_restore=backup_restore,
        generation_copy=generation_copy,
        workload_thresholds=workload,
    )
    complete_report = _evidence_report(
        summary,
        recorded_at=report_manifest.evidence_recorded_at_utc,
        evidence=evidence,
    )
    evidence_receipt = harness.seal_generated_evidence_run(
        evidence_run,
        evidence=evidence,
    )
    report_path = tmp_path / "task064-evidence.json"
    forged_receipt = replace(evidence_receipt)
    evidence_ledger.receipt = forged_receipt
    fake_validator_calls = 0
    fake_issued_validator_calls = 0

    def fake_receipt_validator(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal fake_validator_calls
        fake_validator_calls += 1
        return evidence_ledger

    def fake_issued_receipt_validator(*_args: Any, **_kwargs: Any) -> bool:
        nonlocal fake_issued_validator_calls
        fake_issued_validator_calls += 1
        return True

    with (
        pytest.MonkeyPatch.context() as patch,
        pytest.raises(harness.HarnessFailure) as direct_receipt_mint,
    ):
        patch.setattr(
            harness,
            "_validate_evidence_receipt",
            fake_receipt_validator,
            raising=False,
        )
        patch.setattr(
            harness,
            "_validate_issued_evidence_receipt",
            fake_issued_receipt_validator,
            raising=False,
        )
        harness.write_evidence_report(
            tmp_path,
            receipt=forged_receipt,
            report=complete_report,
        )
    assert direct_receipt_mint.value.code is harness.HarnessFailureCode.CORRUPT
    assert fake_validator_calls == 0
    assert fake_issued_validator_calls == 0
    assert not report_path.exists()
    evidence_ledger.receipt = evidence_receipt
    with pytest.raises(harness.HarnessFailure) as wrong_report_type:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=cast(Any, object()),
        )
    assert wrong_report_type.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()
    assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))
    assert not evidence_ledger.consumed
    wrong_nested_reports = (
        replace(complete_report, evidence=cast(Any, object())),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                schema_identity=cast(Any, object()),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                projection_roundtrip=replace(
                    projection,
                    creation=cast(Any, object()),
                ),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                bounded_queries=replace(
                    bounded_queries,
                    plans=(
                        cast(Any, object()),
                        *bounded_queries.plans[1:],
                    ),
                ),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                fresh_process_faults=replace(
                    fresh_process,
                    create_faults=(
                        cast(Any, object()),
                        *fresh_process.create_faults[1:],
                    ),
                ),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                fresh_process_faults=replace(
                    fresh_process,
                    wal_concurrency=replace(
                        fresh_process.wal_concurrency,
                        checkpoint_samples=cast(Any, object()),
                    ),
                ),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                backup_restore=replace(
                    backup_restore,
                    concurrent_write=replace(
                        backup_restore.concurrent_write,
                        source_before=replace(
                            backup_restore.concurrent_write.source_before,
                            profile=cast(Any, object()),
                        ),
                    ),
                ),
            ),
        ),
        replace(
            complete_report,
            evidence=replace(
                evidence,
                backup_restore=replace(
                    backup_restore,
                    concurrent_write=replace(
                        backup_restore.concurrent_write,
                        source_before=replace(
                            backup_restore.concurrent_write.source_before,
                            page_count=cast(Any, object()),
                        ),
                    ),
                ),
            ),
        ),
        replace(
            complete_report,
            backup_manifest=replace(
                report_manifest,
                files=(
                    (
                        cast(Any, object()),
                        report_manifest.files[0][1],
                        report_manifest.files[0][2],
                    ),
                    *report_manifest.files[1:],
                ),
            ),
        ),
        replace(
            complete_report,
            latency_samples_ns=cast(Any, object()),
        ),
    )
    for wrong_nested_report in wrong_nested_reports:
        with pytest.raises(harness.HarnessFailure) as wrong_nested:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=wrong_nested_report,
            )
        assert wrong_nested.value.code is harness.HarnessFailureCode.CORRUPT
        assert not report_path.exists()
        assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))
        assert not evidence_ledger.consumed
    backup_file_name, backup_file_size, backup_file_digest = report_manifest.files[0]
    forged_backup_digest = _digest("forged-report-backup-file")
    assert forged_backup_digest != backup_file_digest
    forged_backup_manifest = replace(
        report_manifest,
        files=(
            (backup_file_name, backup_file_size, forged_backup_digest),
            *report_manifest.files[1:],
        ),
    )
    restore_file_name, restore_file_size, restore_file_digest = restore_manifest.files[0]
    forged_restore_digest = _digest("forged-report-restore-file")
    assert forged_restore_digest != restore_file_digest
    forged_restore_manifest = replace(
        restore_manifest,
        files=(
            (restore_file_name, restore_file_size, forged_restore_digest),
            *restore_manifest.files[1:],
        ),
    )

    hostile_evidence = (
        replace(
            evidence,
            fresh_process_faults=replace(
                fresh_process,
                create_faults=fresh_process.create_faults[:-1],
            ),
        ),
        replace(
            evidence,
            fresh_process_faults=replace(
                fresh_process,
                create_faults=(
                    *fresh_process.create_faults[:-1],
                    fresh_process.create_faults[0],
                ),
            ),
        ),
        replace(
            evidence,
            fresh_process_faults=replace(
                fresh_process,
                create_faults=(
                    fresh_process.create_faults[1],
                    fresh_process.create_faults[0],
                    *fresh_process.create_faults[2:],
                ),
            ),
        ),
        replace(
            evidence,
            fresh_process_faults=replace(
                fresh_process,
                create_faults=(
                    replace(
                        fresh_process.create_faults[0],
                        disposition=harness.EvidenceDisposition.FAIL,
                        reason="observed_failure",
                    ),
                    *fresh_process.create_faults[1:],
                ),
            ),
        ),
        replace(
            evidence,
            schema_constraints_corruption=harness.RejectionEvidence(()),
        ),
        replace(
            evidence,
            atomicity_classification=replace(
                atomicity,
                mutations=(
                    *atomicity.mutations[:4],
                    replace(atomicity.mutations[4], stream_rows=2, history_rows=4),
                    atomicity.mutations[5],
                ),
            ),
        ),
        replace(
            evidence,
            atomicity_classification=replace(
                atomicity,
                mutations=(
                    *atomicity.mutations[:2],
                    replace(atomicity.mutations[2], history_rows=2),
                    *atomicity.mutations[3:],
                ),
            ),
        ),
        replace(
            evidence,
            atomicity_classification=replace(
                atomicity,
                mutations=(
                    *atomicity.mutations[:5],
                    replace(atomicity.mutations[5], history_rows=2),
                ),
            ),
        ),
        replace(
            evidence,
            generation_copy=replace(
                generation_copy,
                destination_tails=(),
            ),
        ),
        replace(
            evidence,
            backup_restore=replace(
                evidence.backup_restore,
                backup_manifest=forged_backup_manifest,
            ),
        ),
        replace(
            evidence,
            backup_restore=replace(
                evidence.backup_restore,
                restore_manifest=forged_restore_manifest,
            ),
        ),
    )
    for hostile in hostile_evidence:
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=_evidence_report(
                    summary,
                    recorded_at=report_manifest.evidence_recorded_at_utc,
                    evidence=hostile,
                ),
            )
        assert rejected.value.code is harness.HarnessFailureCode.CORRUPT
        assert not report_path.exists()

    malformed_query = replace(
        workload.query_evidence,
        decoded_rows=workload.query_evidence.decoded_rows - 1,
    )
    malformed_query_evidence = replace(
        evidence,
        projection_roundtrip=replace(
            projection,
            query_evidence=malformed_query,
        ),
        bounded_queries=replace(
            bounded_queries,
            queries=(
                (
                    "current_found",
                    projection.classification,
                    malformed_query,
                ),
                *bounded_queries.queries[1:],
            ),
        ),
        workload_thresholds=replace(
            evidence.workload_thresholds,
            query_evidence=malformed_query,
        ),
    )
    with pytest.raises(harness.HarnessFailure) as malformed_query_rejected:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=_evidence_report(
                summary,
                recorded_at=report_manifest.evidence_recorded_at_utc,
                evidence=malformed_query_evidence,
            ),
        )
    assert malformed_query_rejected.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()

    with pytest.raises(harness.HarnessFailure) as arbitrary_pragma:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
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
    assert not report_path.exists()
    with pytest.raises(harness.HarnessFailure) as malformed_manifest:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=replace(
                complete_report,
                backup_manifest=replace(
                    report_manifest,
                    destination_history_rows=report_manifest.destination_history_rows + 1,
                ),
            ),
        )
    assert malformed_manifest.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()
    forged_source_page_count = (
        report_manifest.source_page_count + 1
        if report_manifest.source_page_count < harness.MAX_PAGE_COUNT
        else 1
    )
    with pytest.raises(harness.HarnessFailure) as forged_page_count:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=replace(
                complete_report,
                backup_manifest=replace(
                    report_manifest,
                    source_page_count=forged_source_page_count,
                ),
            ),
        )
    assert forged_page_count.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()
    with pytest.raises(harness.HarnessFailure) as malformed_report_time:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=replace(
                complete_report,
                evidence_recorded_at_utc="2026-07-29T06:45:00Z",
            ),
        )
    assert malformed_report_time.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()

    def assert_no_report_staging_file() -> None:
        assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))

    real_write = os.write
    write_calls = 0

    def partial_report_write(descriptor: int, payload: Any) -> int:
        nonlocal write_calls
        write_calls += 1
        if write_calls == 1:
            partial_size = max(1, len(payload) // 2)
            return real_write(descriptor, payload[:partial_size])
        raise OSError(errno.EIO, "injected report write failure")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "write", partial_report_write)
        with pytest.raises(harness.HarnessFailure) as partial_write:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
    assert partial_write.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert not report_path.exists()
    assert_no_report_staging_file()

    real_fsync = os.fsync
    fsync_calls = 0

    def fail_first_report_fsync(descriptor: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 1:
            raise OSError(errno.EIO, "injected report fsync failure")
        real_fsync(descriptor)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fsync", fail_first_report_fsync)
        with pytest.raises(harness.HarnessFailure) as staged_fsync:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
    assert staged_fsync.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert not report_path.exists()
    assert_no_report_staging_file()

    real_stage_close = os.close
    staged_receipt_mutated = False

    def close_after_staged_receipt_mutation(descriptor: int) -> None:
        nonlocal staged_receipt_mutated
        details = os.fstat(descriptor)
        try:
            descriptor_target = Path(os.readlink(f"/proc/self/fd/{descriptor}")).name
        except OSError:
            descriptor_target = ""
        if (
            stat.S_ISREG(details.st_mode)
            and descriptor_target.startswith(".task064-evidence-")
            and descriptor_target.endswith(".tmp")
            and not staged_receipt_mutated
        ):
            evidence_ledger.receipt = forged_receipt
            staged_receipt_mutated = True
        real_stage_close(descriptor)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "close", close_after_staged_receipt_mutation)
        try:
            with pytest.raises(harness.HarnessFailure) as final_receipt_recheck:
                harness.write_evidence_report(
                    tmp_path,
                    receipt=evidence_receipt,
                    report=complete_report,
                )
        finally:
            evidence_ledger.receipt = evidence_receipt
    assert staged_receipt_mutated
    assert final_receipt_recheck.value.code is harness.HarnessFailureCode.CORRUPT
    assert not report_path.exists()
    assert_no_report_staging_file()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            os,
            "link",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                OSError(errno.EIO, "injected report publish failure")
            ),
        )
        with pytest.raises(harness.HarnessFailure) as publish_failure:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
    assert publish_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert not report_path.exists()
    assert_no_report_staging_file()

    real_link = os.link

    def link_then_fail(*args: Any, **kwargs: Any) -> None:
        real_link(*args, **kwargs)
        raise OSError(errno.EIO, "injected post-link report publish failure")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "link", link_then_fail)
        with pytest.raises(harness.HarnessFailure) as post_link_failure:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
    assert post_link_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert not report_path.exists()
    assert_no_report_staging_file()
    assert not evidence_ledger.consumed

    collision_bytes = b"preexisting-report-sentinel\n"
    report_path.write_bytes(collision_bytes)
    try:
        with pytest.raises(harness.HarnessFailure) as report_collision:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
        assert report_collision.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert report_path.read_bytes() == collision_bytes
        assert_no_report_staging_file()
    finally:
        report_path.unlink()

    real_report_close = os.close
    root_identity = tmp_path.lstat()
    ledger_mutated_during_root_close = False

    def close_after_ledger_mutation(descriptor: int) -> None:
        nonlocal ledger_mutated_during_root_close
        details = os.fstat(descriptor)
        if (
            stat.S_ISDIR(details.st_mode)
            and details.st_dev == root_identity.st_dev
            and details.st_ino == root_identity.st_ino
            and report_path.exists()
        ):
            evidence_ledger.receipt = forged_receipt
            evidence_ledger.consumed = False
            ledger_mutated_during_root_close = True
        real_report_close(descriptor)

    fake_consume_calls = 0

    def fake_receipt_consumer(*_args: Any, **_kwargs: Any) -> None:
        nonlocal fake_consume_calls
        fake_consume_calls += 1

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "close", close_after_ledger_mutation)
        patch.setattr(
            harness,
            "_consume_issued_evidence_receipt",
            fake_receipt_consumer,
            raising=False,
        )
        report = harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=complete_report,
        )
    assert ledger_mutated_during_root_close
    assert fake_consume_calls == 0
    assert not evidence_ledger.consumed
    assert report == report_path
    assert stat_mode(report) == 0o600
    assert report.stat().st_nlink == 1
    assert_no_report_staging_file()
    report_text = report.read_text(encoding="utf-8")
    assert str(tmp_path) not in report_text
    username = os.environ.get("USER")
    assert username is None or username not in report_text
    report_document = json.loads(report_text)
    assert report_document["measurements"]["query_rows"] == (
        workload.query_evidence.stream_rows + workload.query_evidence.history_rows
    )
    assert report_document["gates"] == [
        *[
            {
                "name": name,
                "disposition": harness.EvidenceDisposition.PASS.value,
                "reason": None,
            }
            for name in harness.GENERATED_EVIDENCE_GATES
        ],
        *[
            {
                "name": name,
                "disposition": harness.EvidenceDisposition.NOT_APPLICABLE.value,
                "reason": harness.TARGET_NOT_APPLICABLE_REASON,
            }
            for name in harness.TARGET_NOT_APPLICABLE_GATES
        ],
    ]
    assert report_document["backup_manifest"] == {
        "source_generation_id": report_manifest.source_generation_id,
        "destination_generation_id": report_manifest.destination_generation_id,
        "schema_fingerprint": report_manifest.schema_fingerprint,
        "sqlite_source_id": report_manifest.sqlite_source_id,
        "page_size": report_manifest.page_size,
        "source_page_count": report_manifest.source_page_count,
        "destination_page_count": report_manifest.destination_page_count,
        "checkpoint_outcome": [0, 0, 0],
        "finalization_outcome": report_manifest.finalization_outcome,
        "evidence_recorded_at_utc": report_manifest.evidence_recorded_at_utc,
        "source_streams": report_manifest.source_streams,
        "source_history_rows": report_manifest.source_history_rows,
        "destination_streams": report_manifest.destination_streams,
        "destination_history_rows": report_manifest.destination_history_rows,
        "files": [list(item) for item in report_manifest.files],
        "per_stream_tails": [list(item) for item in report_manifest.per_stream_tails],
    }
    with pytest.raises(harness.HarnessFailure) as consumed_receipt:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=complete_report,
        )
    assert consumed_receipt.value.code is harness.HarnessFailureCode.CORRUPT
    evidence_ledger.consumed = False
    evidence_ledger.recording = False
    evidence_ledger.receipt = evidence_receipt
    with pytest.raises(harness.HarnessFailure) as reopened_receipt:
        harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=complete_report,
        )
    assert reopened_receipt.value.code is harness.HarnessFailureCode.CORRUPT
    evidence_ledger.consumed = True
    assert report.read_text(encoding="utf-8") == report_text
