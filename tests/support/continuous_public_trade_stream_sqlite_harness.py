"""Generated-data SQLite evidence harness for TASK-064.

This module is test support only.  It owns every database it opens, never accepts SQL or URI
options from a caller, and is intentionally not importable from production source.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import math
import mmap
import os
import resource
import secrets
import selectors
import signal
import sqlite3
import stat
import struct
import sys
import time
import tracemalloc
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, fields, is_dataclass
from datetime import UTC, datetime
from enum import Enum, StrEnum
from functools import wraps
from pathlib import Path
from typing import Any, Final, ParamSpec, TypeVar, cast
from uuid import UUID

from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradePolicy,
)
from wealth.domain.continuous_public_trade_persistence import (
    ContinuousPublicTradeStreamCreationRecordV1,
    ContinuousPublicTradeStreamEnvelopeV1,
    ContinuousPublicTradeStreamTransitionRecordV1,
    decode_stream_creation_record,
    decode_stream_envelope,
    decode_stream_transition_record,
    encode_stream_creation_record,
    encode_stream_envelope,
    encode_stream_transition_record,
    initial_stream_history_root,
    next_stream_history_root,
    project_continuous_public_trade_policy,
    stream_creation_digest,
    stream_envelope_digest,
    stream_transition_digest,
    validate_stream_load_bindings,
    validate_stream_transition_link,
)
from wealth.ports.continuous_public_trade_stream_store import (
    ContinuousPublicTradeStreamAuditAtTailResultV1,
    ContinuousPublicTradeStreamAuditContinuationQueryV1,
    ContinuousPublicTradeStreamAuditContinuationV1,
    ContinuousPublicTradeStreamAuditNotFoundResultV1,
    ContinuousPublicTradeStreamAuditOutcome,
    ContinuousPublicTradeStreamAuditPageResultV1,
    ContinuousPublicTradeStreamAuditPageV1,
    ContinuousPublicTradeStreamAuditQueryV1,
    ContinuousPublicTradeStreamAuditRejectedResultV1,
    ContinuousPublicTradeStreamAuditResultV1,
    ContinuousPublicTradeStreamAuditStartQueryV1,
    ContinuousPublicTradeStreamAuditUnavailableResultV1,
    ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1,
    ContinuousPublicTradeStreamCompareAndSwapCommandV1,
    ContinuousPublicTradeStreamCompareAndSwapOutcome,
    ContinuousPublicTradeStreamCompareAndSwapReceiptV1,
    ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1,
    ContinuousPublicTradeStreamCompareAndSwapResultV1,
    ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1,
    ContinuousPublicTradeStreamCreateAcceptedResultV1,
    ContinuousPublicTradeStreamCreateCommandV1,
    ContinuousPublicTradeStreamCreateOutcome,
    ContinuousPublicTradeStreamCreateReceiptV1,
    ContinuousPublicTradeStreamCreateRejectedResultV1,
    ContinuousPublicTradeStreamCreateResultV1,
    ContinuousPublicTradeStreamCreateUnavailableResultV1,
    ContinuousPublicTradeStreamCurrentViewV1,
    ContinuousPublicTradeStreamExpectationV1,
    ContinuousPublicTradeStreamLoadFoundResultV1,
    ContinuousPublicTradeStreamLoadNotFoundResultV1,
    ContinuousPublicTradeStreamLoadOutcome,
    ContinuousPublicTradeStreamLoadQueryV1,
    ContinuousPublicTradeStreamLoadRejectedResultV1,
    ContinuousPublicTradeStreamLoadResultV1,
    ContinuousPublicTradeStreamLoadUnavailableResultV1,
    ContinuousPublicTradeStreamStoredCreationV1,
    ContinuousPublicTradeStreamStoredHistoryEntryV1,
    ContinuousPublicTradeStreamStoredTransitionV1,
    validate_continuous_public_trade_stream_audit_page,
)

TASK_ID: Final = "TASK-064"
TASK_CONTRACT_GENERATION: Final = 3
TASK_CONTRACT_DIGEST: Final = "86e3650608f2f1c96a9aa272b2b9cd597bc3d5ac188a39937afb974536d11ccb"
ACCEPTED_PYTHON_VERSION: Final = "3.13.14"
ACCEPTED_SQLITE_VERSION: Final = "3.53.1"
ACCEPTED_THREADSAFETY: Final = 3
SCHEMA_DESCRIPTOR_DOMAIN: Final = b"wealth.continuous_public_trade.stream_store_schema/v1\x00"
NATURAL_IDENTITY_DOMAIN: Final = b"wealth.continuous_public_trade.natural_identity_key/v1\x00"
STORAGE_MARKER: Final = b"wealth.continuous_public_trade.stream_store/sqlite/v1"
APPLICATION_ID: Final = 0x57505431  # WPT1: WEALTH public-trade generation one.
USER_VERSION: Final = 1
SCHEMA_GENERATION: Final = 1
NATURAL_IDENTITY_KEY_VERSION: Final = 1
PAGE_SIZE: Final = 4096
MAX_PAGE_COUNT: Final = 16_384
WAL_AUTOCHECKPOINT_PAGES: Final = 4
MAX_TEST_WAL_BYTES: Final = 2 * 1024 * 1024
MAX_TEST_DATABASE_BYTES: Final = 16 * 1024 * 1024
MAX_TEST_TRACED_MEMORY_BYTES: Final = 64 * 1024 * 1024
MAX_TEST_OPEN_CURSORS: Final = 8
WORKLOAD_SEED: Final = 64_064
WORKLOAD_RUNS: Final = 5
CONCURRENT_BACKUP_TRANSITIONS: Final = 16
MAX_OPERATION_LATENCY_NS: Final = 2_000_000_000
WORKLOAD_MATRIX: Final = (
    ("minimum", 1, 1, 1),
    ("typical", 3, 9, 10),
    ("maximum_query", 1, 103, 100),
)
RECORD_SIZE_MATRIX: Final = (
    ("minimum", "RETAIN", 2_283, 2_118, 529, 529, 0),
    ("typical", "RETAIN", 2_511, 2_291, 594, 594, 0),
    ("maximum_contract_shape", "ATTACH", 18_595, 26_512, 5_900, 12_662, 6_467),
)
GENERATED_EVIDENCE_GATES: Final = (
    "schema_identity",
    "bootstrap_path_ownership",
    "runtime_connection_controls",
    "projection_roundtrip",
    "schema_constraints_corruption",
    "atomicity_classification",
    "fresh_process_faults",
    "bounded_queries",
    "closed_error_mapping",
    "backup_restore",
    "generation_copy",
    "workload_thresholds",
)
TARGET_NOT_APPLICABLE_GATES: Final = (
    "target_filesystem_power_loss",
    "target_permissions_alias_locking_sync",
    "target_sqlite_runtime",
    "operational_capacity_checkpoint_latency",
    "production_backup_retention_rpo_rto",
    "production_monitoring_stop_thresholds",
    "incompatible_generation_migration",
)
TARGET_NOT_APPLICABLE_REASON: Final = "outside_task_target_deployment_evidence"
_ALLOWED_TASK064_TEST_MODULES: Final = (
    "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
    "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
)
ACCEPTED_SQLITE_SOURCE_ID: Final = (
    "2026-05-05 10:34:17 c88b22011a54b4f6fbd149e9f8e4de77658ce58143a1af0e3785e4e6475127e9"
)
ACCEPTED_COMPILE_OPTIONS: Final = (
    "ATOMIC_INTRINSICS=1",
    "COMPILER=clang-22.1.3",
    "DEFAULT_AUTOVACUUM",
    "DEFAULT_CACHE_SIZE=-2000",
    "DEFAULT_FILE_FORMAT=4",
    "DEFAULT_JOURNAL_SIZE_LIMIT=-1",
    "DEFAULT_MMAP_SIZE=0",
    "DEFAULT_PAGE_SIZE=4096",
    "DEFAULT_PCACHE_INITSZ=20",
    "DEFAULT_RECURSIVE_TRIGGERS",
    "DEFAULT_SECTOR_SIZE=4096",
    "DEFAULT_SYNCHRONOUS=2",
    "DEFAULT_WAL_AUTOCHECKPOINT=1000",
    "DEFAULT_WAL_SYNCHRONOUS=2",
    "DEFAULT_WORKER_THREADS=0",
    "DIRECT_OVERFLOW_READ",
    "ENABLE_DBSTAT_VTAB",
    "ENABLE_FTS3",
    "ENABLE_FTS3_PARENTHESIS",
    "ENABLE_FTS4",
    "ENABLE_FTS5",
    "ENABLE_GEOPOLY",
    "ENABLE_MATH_FUNCTIONS",
    "ENABLE_PERCENTILE",
    "ENABLE_RTREE",
    "MALLOC_SOFT_LIMIT=1024",
    "MAX_ATTACHED=10",
    "MAX_COLUMN=2000",
    "MAX_COMPOUND_SELECT=500",
    "MAX_DEFAULT_PAGE_SIZE=8192",
    "MAX_EXPR_DEPTH=1000",
    "MAX_FUNCTION_ARG=1000",
    "MAX_LENGTH=1000000000",
    "MAX_LIKE_PATTERN_LENGTH=50000",
    "MAX_MMAP_SIZE=0x7fff0000",
    "MAX_PAGE_COUNT=0xfffffffe",
    "MAX_PAGE_SIZE=65536",
    "MAX_SQL_LENGTH=1000000000",
    "MAX_TRIGGER_DEPTH=1000",
    "MAX_VARIABLE_NUMBER=32766",
    "MAX_VDBE_OP=250000000",
    "MAX_WORKER_THREADS=8",
    "MUTEX_PTHREADS",
    "SYSTEM_MALLOC",
    "TEMP_STORE=1",
    "THREADSAFE=1",
)
SCHEMA_OBJECT_ORDER: Final = (
    "stream_store_metadata",
    "continuous_public_trade_stream",
    "continuous_public_trade_history",
    "stream_tail_commit_guard",
    "ux_cpt_stream_uuid",
    "ux_cpt_stream_natural_key",
    "ux_cpt_history_stream_version",
    "ux_cpt_history_record_binding",
    "ux_cpt_history_tail_binding",
    "trg_metadata_no_update",
    "trg_metadata_no_delete",
    "trg_stream_insert_shape",
    "trg_stream_current_update",
    "trg_stream_transition_finalize",
    "trg_stream_no_delete",
    "trg_history_insert_binding",
    "trg_history_transition_pending",
    "trg_history_no_update",
    "trg_history_no_delete",
)

_FIXTURE_ROOT: Final = (
    Path(__file__).resolve().parents[1] / "fixtures" / "continuous_public_trade_stream_store" / "v1"
)
SCHEMA_PATH: Final = _FIXTURE_ROOT / "schema.sql"
SCHEMA_DESCRIPTOR_PATH: Final = _FIXTURE_ROOT / "schema_descriptor.json"
SCHEMA_FINGERPRINT_PATH: Final = _FIXTURE_ROOT / "schema_fingerprint.txt"

_DIGEST_PREFIX: Final = b"sha256:"
_DIGEST_BYTES: Final = 71
_ENTRY_CREATION: Final = b"creation"
_ENTRY_TRANSITION: Final = b"transition"
_MODEL_VERSION: Final = b"1.0"
_DATABASE_BASENAME: Final = "store.sqlite3"
_GENERATION_PREFIX: Final = "continuous-public-trade-v1-"
_OWNED_DATABASE_FILENAMES: Final = frozenset(
    {
        _DATABASE_BASENAME,
        f"{_DATABASE_BASENAME}-wal",
        f"{_DATABASE_BASENAME}-shm",
    }
)

_PTRACE_SYSCALL: Final = 24
_PTRACE_SEIZE: Final = 0x4206
_PTRACE_INTERRUPT: Final = 0x4207
_PTRACE_GET_SYSCALL_INFO: Final = 0x420E
_PTRACE_O_TRACESYSGOOD: Final = 0x00000001
_PTRACE_SYSCALL_INFO_ENTRY: Final = 1
_PTRACE_SYSCALL_INFO_EXIT: Final = 2
_X86_64_PWRITE64: Final = 18
_AUDIT_ARCH_X86_64: Final = 0xC000003E


class _PtraceSyscallEntry(ctypes.Structure):
    _fields_ = [
        ("number", ctypes.c_uint64),
        ("arguments", ctypes.c_uint64 * 6),
    ]


class _PtraceSyscallExit(ctypes.Structure):
    _fields_ = [
        ("return_value", ctypes.c_int64),
        ("is_error", ctypes.c_uint8),
        ("padding", ctypes.c_uint8 * 7),
    ]


class _PtraceSyscallPayload(ctypes.Union):
    _fields_ = [  # noqa: RUF012 - ctypes requires this mutable class descriptor.
        ("entry", _PtraceSyscallEntry),
        ("exit", _PtraceSyscallExit),
        ("padding", ctypes.c_uint8 * 64),
    ]


class _PtraceSyscallInfo(ctypes.Structure):
    _fields_ = [
        ("operation", ctypes.c_uint8),
        ("padding", ctypes.c_uint8 * 3),
        ("architecture", ctypes.c_uint32),
        ("instruction_pointer", ctypes.c_uint64),
        ("stack_pointer", ctypes.c_uint64),
        ("payload", _PtraceSyscallPayload),
    ]


def _ptrace_raw(
    library: ctypes.CDLL,
    request: int,
    process_id: int,
    address: object,
    data: object,
) -> int:
    result = library.ptrace(request, process_id, address, data)
    return result if type(result) is int else -1


class EvidenceDisposition(StrEnum):
    """Closed evidence disposition vocabulary."""

    PASS = "PASS"
    FAIL = "FAIL"
    UNPROVEN = "UNPROVEN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ReopenedState(StrEnum):
    """Closed state observed after a process or storage fault."""

    OLD = "OLD"
    NEW = "NEW"
    DUPLICATE = "DUPLICATE"
    UNAVAILABLE = "UNAVAILABLE"


class StoreClassification(StrEnum):
    """Closed harness-local operation classifications."""

    INSERTED = "INSERTED"
    UPDATED = "UPDATED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"
    ANCHOR_CONFLICT = "ANCHOR_CONFLICT"
    NOT_FOUND = "NOT_FOUND"
    FOUND = "FOUND"
    PAGE = "PAGE"
    AT_TAIL = "AT_TAIL"
    UNSUPPORTED_VERSION = "UNSUPPORTED_VERSION"
    CORRUPT = "CORRUPT"
    UNAVAILABLE = "UNAVAILABLE"


class HarnessFailureCode(StrEnum):
    """Sanitized failure codes; exception text never contains retained data or a path."""

    INVALID_BOOTSTRAP_ROOT = "invalid_bootstrap_root"
    INVALID_TOKEN = "invalid_token"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED_VERSION = "unsupported_version"
    CORRUPT = "corrupt"
    BOUNDS_EXCEEDED = "bounds_exceeded"
    UNPROVEN = "unproven"


class HarnessFailure(RuntimeError):
    """One sanitized harness failure."""

    def __init__(
        self,
        code: HarnessFailureCode,
        *,
        sqlite_errorcode: int | None = None,
    ) -> None:
        self.code = code
        self.sqlite_errorcode = sqlite_errorcode
        super().__init__(code.value)


@dataclass(frozen=True, slots=True)
class StoreToken:
    """Opaque same-bootstrap ownership token for one pytest-owned generation."""

    _nonce: bytes
    _pytest_root: Path
    _generation_root: Path
    _database_path: Path
    _device: int
    _inode: int
    _uid: int
    _mode: int
    _link_count: int


@dataclass(frozen=True, slots=True)
class _EvidenceObservation:
    """One exact whole-gate collector return retained for the pytest scope."""

    value: object
    payload_digest: str
    registration: _ActivePytestRoot
    producer: _EvidenceProducer
    run: _EvidenceRun
    process_id: int
    token_roles: tuple[tuple[str, bytes], ...]
    private_receipt_digest: str | None


@dataclass(frozen=True, slots=True)
class _EvidenceProducer:
    """Closure-only whole-gate producer authority with one immutable slot."""

    capability: object
    name: str
    gate: str
    ordinal: int


@dataclass(frozen=True, slots=True)
class _RejectionScenario:
    """Fixed rejection label, outcome, and authorized executor contract."""

    capability: object
    label: str
    kind: str
    code: HarnessFailureCode
    executor_names: tuple[str, ...]
    normalized_sql: str | None = None


@dataclass(frozen=True, slots=True)
class _RejectionObservation:
    """Actual rejection bound to one registered scenario capability."""

    scenario: _RejectionScenario
    code: HarnessFailureCode
    sqlite_errorcode: int | None
    run: _EvidenceRun
    process_id: int
    ordinal: int
    target_nonce: bytes | None
    call_digest: str


@dataclass(slots=True)
class _RejectionCollectorState:
    """Exact scenario sequence owned by one active whole-gate collector."""

    run: _EvidenceRun
    gate: str
    scenarios: tuple[_RejectionScenario, ...]
    observations: list[_RejectionObservation]


@dataclass(frozen=True, slots=True)
class _OperationObservation:
    """One closure-issued low-level executor result in a fixed gate scope."""

    producer_name: str
    operation_tag: str
    result: object
    payload_digest: str
    sequence: int
    token_nonces: tuple[bytes, ...]


@dataclass(slots=True)
class _GateOperationState:
    """Low-level receipts captured for one exact gate and run."""

    run: _EvidenceRun
    gate: str
    observations: list[_OperationObservation]


@dataclass(slots=True)
class _EvidenceLedger:
    """Private per-pytest-root evidence state; never shared across test scopes."""

    nonce: bytes
    observations: dict[int, _EvidenceObservation]
    operation_runs: dict[str, tuple[_OperationObservation, ...]]
    rejection_runs: dict[str, tuple[_RejectionObservation, ...]]
    run: _EvidenceRun | None = None
    receipt: _EvidenceReceipt | None = None
    recording: bool = False
    consumed: bool = False
    closed: bool = False


@dataclass(frozen=True, slots=True)
class _ActivePytestRoot:
    """Fixture-scoped authority for one repository-controlled pytest root."""

    path_object: Path
    resolved_path: Path
    device: int
    inode: int
    uid: int
    mode: int
    process_id: int
    node_id: str
    nonce: bytes
    revocation_flag: mmap.mmap
    evidence_ledger: _EvidenceLedger


@dataclass(frozen=True, slots=True)
class _EvidenceRun:
    """Unforgeable object-identity capability for one active evidence execution."""

    _constructor: object
    _pytest_registration: _ActivePytestRoot
    _nonce: bytes


@dataclass(frozen=True, slots=True)
class _GateEvidenceReceipt:
    """One sealed generated-gate payload bound to its exact live object."""

    _constructor: object
    gate: str
    payload: object
    payload_digest: str


@dataclass(frozen=True, slots=True)
class _EvidenceReceipt:
    """Consume-on-success authority for one exact generated evidence aggregate."""

    _constructor: object
    run: _EvidenceRun
    evidence: GeneratedEvidenceAggregate
    evidence_digest: str
    gates: tuple[_GateEvidenceReceipt, ...]


@dataclass(slots=True)
class CursorMeasurement:
    """Measured Python DB-API cursor ownership within one explicit evidence scope."""

    current: int = 0
    maximum: int = 0


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    """Exact runtime/profile evidence captured from a coherent open."""

    role: str
    python_version: str
    sqlite_version: str
    sqlite_source_id: str
    threadsafety: int
    compile_options: tuple[str, ...]
    dbconfig: tuple[tuple[str, bool], ...]
    defensive_available: bool
    defensive_enabled: bool
    pragmas: tuple[tuple[str, str | int], ...]
    limits: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ConnectionControlProfile:
    """Exact role-specific controls observed on one accepted connection."""

    role: str
    dbconfig: tuple[tuple[str, bool], ...]
    defensive_available: bool
    defensive_enabled: bool
    limits: tuple[tuple[str, int], ...]
    pragmas: tuple[tuple[str, str | int], ...]


@dataclass(frozen=True, slots=True)
class QueryEvidence:
    """Bounded SQL/row materialization evidence."""

    statements: tuple[str, ...]
    stream_rows: int
    history_rows: int
    decoded_rows: int


@dataclass(frozen=True, slots=True)
class AuditSlice:
    """One exact bounded history interval."""

    classification: StoreClassification
    entries: tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...]
    overlap_count: int
    new_count: int
    query_evidence: QueryEvidence


@dataclass(frozen=True, slots=True)
class AuditBounds:
    """Overflow-safe inclusive range and materialization ceiling."""

    low_version: int
    high_version: int
    row_limit: int
    overlap_count: int


@dataclass(frozen=True, slots=True)
class VerificationSummary:
    """Fresh-open structural verification evidence."""

    profile: RuntimeProfile
    connection_profiles: tuple[ConnectionControlProfile, ...]
    stream_count: int
    history_count: int
    page_count: int
    freelist_count: int
    database_bytes: int
    wal_bytes: int
    schema_fingerprint: str


@dataclass(frozen=True, slots=True)
class BackupManifest:
    """Test-local verified closed-file backup manifest."""

    source_generation_id: str
    destination_generation_id: str
    schema_fingerprint: str
    sqlite_source_id: str
    page_size: int
    source_page_count: int
    destination_page_count: int
    checkpoint_outcome: tuple[int, int, int]
    finalization_outcome: str
    evidence_recorded_at_utc: str
    source_streams: int
    source_history_rows: int
    destination_streams: int
    destination_history_rows: int
    files: tuple[tuple[str, int, str], ...]
    per_stream_tails: tuple[tuple[str, int, str, str], ...]


@dataclass(frozen=True, slots=True)
class FaultEvidence:
    """One process-fault seam result."""

    seam: str
    disposition: EvidenceDisposition
    sqlite_errorcode: int | None
    observed_syscall: str | None
    acknowledgement_bytes: int
    reopened_state: ReopenedState
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class TwoWriterEvidence:
    """Two fresh writer-process classifications with no hidden retry."""

    operation: str
    outcomes: tuple[StoreClassification | HarnessFailureCode, ...]
    disposition: EvidenceDisposition
    process_boundary: str
    stream_count: int
    history_count: int


@dataclass(frozen=True, slots=True)
class WriterContentionEvidence:
    """One real overlapping winner and one no-retry fresh-process contender."""

    operation: str
    winner_outcome: StoreClassification | HarnessFailureCode | None
    contender_outcome: StoreClassification | HarnessFailureCode | None
    contender_sqlite_errorcode: int | None
    disposition: EvidenceDisposition
    process_boundary: str
    stream_count: int
    history_count: int


@dataclass(frozen=True, slots=True)
class WalConcurrencyEvidence:
    """Finite long-reader, writer, and passive-checkpointer evidence."""

    disposition: EvidenceDisposition
    initial_version: int
    reader_snapshot_version: int
    final_version: int
    writes: int
    checkpoint_samples: tuple[tuple[int, int, int], ...]
    maximum_wal_bytes: int
    process_boundary: str


@dataclass(frozen=True, slots=True)
class RejectionEvidence:
    """Exact ordered sanitized rejection or closed-mapping observations."""

    checks: tuple[tuple[str, HarnessFailureCode], ...]


@dataclass(frozen=True, slots=True)
class BootstrapPathEvidence:
    """Live bootstrap authority plus exact hostile-boundary rejections."""

    token: StoreToken
    rejections: RejectionEvidence


@dataclass(frozen=True, slots=True)
class FreshProcessEvidenceAggregate:
    """Actual returned packets for the complete generated process-fault matrix."""

    create_faults: tuple[FaultEvidence, ...]
    compare_and_swap_faults: tuple[FaultEvidence, ...]
    result_code_faults: tuple[FaultEvidence, ...]
    true_during_commit: FaultEvidence
    ioerr_write: FaultEvidence
    max_page_count: FaultEvidence
    writer_contentions: tuple[WriterContentionEvidence, ...]
    two_writers: tuple[TwoWriterEvidence, ...]
    wal_concurrency: WalConcurrencyEvidence


@dataclass(frozen=True, slots=True)
class BoundedQueryEvidenceAggregate:
    """Actual bounded query packets and their exact planner observations."""

    queries: tuple[tuple[str, StoreClassification, QueryEvidence], ...]
    plans: tuple[tuple[str, tuple[str, ...]], ...]


@dataclass(frozen=True, slots=True)
class AtomicityClassificationEvidence:
    """Committed writes plus duplicate/conflict and unknown-ack classifications."""

    mutations: tuple[MutationEvidence, ...]
    two_writers: tuple[TwoWriterEvidence, ...]
    unknown_acknowledgements: tuple[FaultEvidence, ...]


@dataclass(frozen=True, slots=True)
class ConcurrentBackupEvidence:
    """One backup snapshot overlapped by actual committed source mutations."""

    source_token: StoreToken
    backup_token: StoreToken
    backup_manifest: BackupManifest
    source_before: VerificationSummary
    source_after: VerificationSummary
    backup_summary: VerificationSummary
    writer_mutations: tuple[MutationEvidence, ...]
    progress_observations: int
    writer_process_boundary: str


@dataclass(frozen=True, slots=True)
class BackupRestoreEvidence:
    """Cross-validated source, backup, and isolated-restore observations."""

    source_token: StoreToken
    backup_token: StoreToken
    restore_token: StoreToken
    backup_manifest: BackupManifest
    restore_manifest: BackupManifest
    source_summary: VerificationSummary
    backup_summary: VerificationSummary
    restore_summary: VerificationSummary
    concurrent_write: ConcurrentBackupEvidence


@dataclass(frozen=True, slots=True)
class GenerationCopyEvidence:
    """Same-format generation-copy identity, summary, and tail observations."""

    source_token: StoreToken
    destination_token: StoreToken
    source_generation_id: str
    destination_generation_id: str
    source_summary: VerificationSummary
    destination_summary: VerificationSummary
    source_tails: tuple[tuple[str, int, str, str], ...]
    destination_tails: tuple[tuple[str, int, str, str], ...]


@dataclass(frozen=True, slots=True)
class WorkloadEvidence:
    """Actual finite-workload query and resource measurements."""

    query_evidence: QueryEvidence
    maximum_open_cursors: int
    peak_traced_memory_bytes: int
    latency_samples_ns: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class GeneratedEvidenceAggregate:
    """Named generated-gate evidence; callers never supply gate dispositions."""

    schema_identity: VerificationSummary
    bootstrap_path_ownership: BootstrapPathEvidence
    runtime_connection_controls: tuple[ConnectionControlProfile, ...]
    projection_roundtrip: CurrentSlice
    schema_constraints_corruption: RejectionEvidence
    atomicity_classification: AtomicityClassificationEvidence
    fresh_process_faults: FreshProcessEvidenceAggregate
    bounded_queries: BoundedQueryEvidenceAggregate
    closed_error_mapping: RejectionEvidence
    backup_restore: BackupRestoreEvidence
    generation_copy: GenerationCopyEvidence
    workload_thresholds: WorkloadEvidence


@dataclass(frozen=True, slots=True)
class EvidenceReport:
    """Strict sanitized generated report contract."""

    report_version: int
    task_id: str
    contract_generation: int
    contract_digest: str
    schema_fingerprint: str
    application_id: int
    user_version: int
    schema_generation: int
    page_size: int
    storage_marker: str
    python_version: str
    sqlite_version: str
    sqlite_source_id: str
    threadsafety: int
    compile_options: tuple[str, ...]
    connection_profiles: tuple[ConnectionControlProfile, ...]
    environment_class: str
    evidence_recorded_at_utc: str
    workload_seed: int
    workload_runs: int
    record_size_matrix: tuple[tuple[str, str, int, int, int, int, int], ...]
    workload_matrix: tuple[tuple[str, int, int, int], ...]
    maximum_operation_latency_ns: int
    maximum_database_bytes: int
    maximum_wal_bytes: int
    maximum_traced_memory_bytes: int
    maximum_open_cursors_threshold: int
    maximum_page_count: int
    wal_autocheckpoint_pages: int
    stream_rows: int
    history_rows: int
    query_evidence: QueryEvidence
    database_bytes: int
    wal_bytes: int
    page_count: int
    freelist_count: int
    maximum_open_cursors: int
    peak_traced_memory_bytes: int
    latency_samples_ns: tuple[int, ...]
    backup_manifest: BackupManifest
    evidence: GeneratedEvidenceAggregate


@dataclass(frozen=True, slots=True)
class MutationEvidence:
    """One exact create or compare-and-swap classification."""

    classification: StoreClassification
    statements: tuple[str, ...]
    rows_materialized: int
    committed: bool


@dataclass(frozen=True, slots=True)
class CurrentSlice:
    """One bounded current-state result and its query budget evidence."""

    classification: StoreClassification
    creation: ContinuousPublicTradeStreamStoredCreationV1 | None
    predecessor: ContinuousPublicTradeStreamStoredHistoryEntryV1 | None
    current: ContinuousPublicTradeStreamStoredHistoryEntryV1 | None
    query_evidence: QueryEvidence


@dataclass(frozen=True, slots=True)
class _RegisteredIdentity:
    pytest_root: Path
    pytest_registration: _ActivePytestRoot
    generation_root: Path
    database_path: Path
    pytest_root_device: int
    pytest_root_inode: int
    pytest_root_uid: int
    pytest_root_mode: int
    generation_device: int
    generation_inode: int
    generation_uid: int
    generation_mode: int
    device: int
    inode: int
    uid: int
    mode: int
    link_count: int


@dataclass(frozen=True, slots=True)
class _PinnedFile:
    name: str
    descriptor: int
    device: int
    inode: int
    uid: int
    mode: int
    link_count: int


@dataclass(frozen=True, slots=True)
class _OperationPathSnapshot:
    root_descriptor: int
    generation_descriptor: int
    files: tuple[_PinnedFile, ...]


_TOKEN_REGISTRY: dict[bytes, _RegisteredIdentity] = {}
_ACTIVE_PYTEST_ROOTS: dict[int, _ActivePytestRoot] = {}
_REVOKED_PYTEST_ROOTS: dict[int, _ActivePytestRoot] = {}
_CONNECTION_PATH_SNAPSHOTS: dict[int, _OperationPathSnapshot] = {}
_CONNECTION_EVIDENCE_BINDINGS: dict[int, tuple[bytes, bool]] = {}
_ACTIVE_CURSOR_MEASUREMENT: ContextVar[CursorMeasurement | None] = ContextVar(
    "task064_cursor_measurement",
    default=None,
)
_ACTIVE_EVIDENCE_RUN: ContextVar[_EvidenceRun | None] = ContextVar(
    "task064_evidence_run",
    default=None,
)
_ACTIVE_REJECTION_COLLECTOR: ContextVar[_RejectionCollectorState | None] = ContextVar(
    "task064_rejection_collector",
    default=None,
)
_ACTIVE_GATE_OPERATIONS: ContextVar[_GateOperationState | None] = ContextVar(
    "task064_gate_operations",
    default=None,
)
_ACTIVE_OPERATION_EXECUTOR_DEPTH: ContextVar[int] = ContextVar(
    "task064_operation_executor_depth",
    default=0,
)
_EVIDENCE_CAPABILITY_CONSTRUCTOR: Final = object()
_GENERATION_COUNTER = 0

_Parameters = ParamSpec("_Parameters")
_Result = TypeVar("_Result")


def _normalized_evidence_payload(value: object) -> object:
    """Return a strict type-tagged value suitable for a private payload digest."""

    if value is None:
        return ["none"]
    if isinstance(value, Enum):
        return [
            "enum",
            f"{type(value).__module__}.{type(value).__qualname__}",
            value.value,
        ]
    if type(value) is bool:
        return ["bool", value]
    if type(value) is int:
        return ["int", value]
    if type(value) is float:
        if not math.isfinite(value):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return ["float", value.hex()]
    if type(value) is str:
        return ["str", value]
    if type(value) is bytes:
        return ["bytes", value.hex()]
    if isinstance(value, UUID):
        return ["uuid", str(value)]
    if isinstance(value, Path):
        return ["path", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if type(value) in (
        ContinuousPublicTradeStreamStoredCreationV1,
        ContinuousPublicTradeStreamStoredTransitionV1,
    ):
        try:
            stored_value = cast(
                ContinuousPublicTradeStreamStoredCreationV1
                | ContinuousPublicTradeStreamStoredTransitionV1,
                value,
            )
            dumped = stored_value.model_dump(mode="json")
        except (AttributeError, TypeError, ValueError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        return [
            "pydantic",
            f"{type(value).__module__}.{type(value).__qualname__}",
            _normalized_evidence_payload(dumped),
        ]
    if type(value) is tuple:
        return ["tuple", [_normalized_evidence_payload(item) for item in value]]
    if type(value) is list:
        return ["list", [_normalized_evidence_payload(item) for item in value]]
    if isinstance(value, Mapping):
        normalized_items = [
            (
                _normalized_evidence_payload(key),
                _normalized_evidence_payload(item),
            )
            for key, item in value.items()
        ]
        normalized_items.sort(
            key=lambda item: json.dumps(
                item[0],
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return ["mapping", normalized_items]
    if is_dataclass(value) and not isinstance(value, type):
        return [
            "dataclass",
            f"{type(value).__module__}.{type(value).__qualname__}",
            [
                [field.name, _normalized_evidence_payload(getattr(value, field.name))]
                for field in fields(value)
            ],
        ]
    raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _evidence_payload_digest(value: object) -> str:
    try:
        raw = json.dumps(
            _normalized_evidence_payload(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (AttributeError, TypeError, ValueError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    return hashlib.sha256(raw).hexdigest()


def _collect_evidence_registrations(
    value: object,
    registrations: list[_ActivePytestRoot],
    token_nonces: list[bytes],
) -> None:
    if type(value) is StoreToken:
        registered = _TOKEN_REGISTRY.get(value._nonce)
        if registered is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if registered.pytest_registration not in registrations:
            registrations.append(registered.pytest_registration)
        if value._nonce not in token_nonces:
            token_nonces.append(value._nonce)
        return
    if isinstance(value, Path):
        registration = _ACTIVE_PYTEST_ROOTS.get(id(value))
        if registration is not None and registration not in registrations:
            registrations.append(registration)
        return
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _collect_evidence_registrations(
                getattr(value, field.name),
                registrations,
                token_nonces,
            )
        return
    if type(value) in (tuple, list):
        for item in cast(tuple[object, ...] | list[object], value):
            _collect_evidence_registrations(item, registrations, token_nonces)
        return
    if isinstance(value, Mapping):
        for item in value.values():
            _collect_evidence_registrations(item, registrations, token_nonces)


_GATE_PRODUCERS: dict[int, tuple[str, str, int]] = {}
_GATE_COLLECTORS_FROZEN = False
_OPERATION_PRODUCERS: dict[int, str] = {}
_OPERATION_PRODUCERS_FROZEN = False
_GATE_OPERATION_ALLOWLIST: Final = {
    "fresh_process_faults": frozenset(
        {
            "bootstrap_store",
            "create_stream",
            "fresh_process_kill_evidence",
            "sqlite_result_code_fault_evidence",
            "true_during_commit_evidence",
            "ioerr_write_evidence",
            "max_page_count_evidence",
            "fresh_process_writer_contention_evidence",
            "fresh_process_two_writer_evidence",
            "wal_concurrency_evidence",
        }
    ),
    "atomicity_classification": frozenset(
        {"bootstrap_store", "create_stream", "compare_and_swap_stream"}
    ),
    "bounded_queries": frozenset(
        {
            "bootstrap_store",
            "create_stream",
            "compare_and_swap_stream",
            "load_current",
            "audit_history",
            "query_plan_evidence",
        }
    ),
}
_FRESH_OPERATION_PRODUCER_SEQUENCE: Final = (
    *(("bootstrap_store", "fresh_process_kill_evidence") * 5),
    *(("bootstrap_store", "create_stream", "fresh_process_kill_evidence") * 5),
    *(("bootstrap_store", "sqlite_result_code_fault_evidence") * 2),
    "bootstrap_store",
    "create_stream",
    "true_during_commit_evidence",
    "bootstrap_store",
    "create_stream",
    "ioerr_write_evidence",
    "bootstrap_store",
    "create_stream",
    "max_page_count_evidence",
    "bootstrap_store",
    "fresh_process_writer_contention_evidence",
    "bootstrap_store",
    "create_stream",
    "fresh_process_writer_contention_evidence",
    *(("bootstrap_store", "fresh_process_two_writer_evidence") * 2),
    *(("bootstrap_store", "create_stream", "fresh_process_two_writer_evidence") * 2),
    "bootstrap_store",
    "create_stream",
    "wal_concurrency_evidence",
)
_ATOMICITY_OPERATION_PRODUCER_SEQUENCE: Final = (
    "bootstrap_store",
    "create_stream",
    "create_stream",
    "create_stream",
    "compare_and_swap_stream",
    "compare_and_swap_stream",
    "compare_and_swap_stream",
)
_BOUNDED_OPERATION_PRODUCER_SEQUENCE: Final = (
    "load_current",
    "load_current",
    *(("audit_history",) * 5),
    "bootstrap_store",
    "create_stream",
    "create_stream",
    "load_current",
    "audit_history",
    "bootstrap_store",
    "create_stream",
    *(("compare_and_swap_stream",) * 102),
    "audit_history",
    "audit_history",
    "query_plan_evidence",
)
_GATE_OPERATION_PRODUCER_SEQUENCES: Final = {
    "fresh_process_faults": _FRESH_OPERATION_PRODUCER_SEQUENCE,
    "atomicity_classification": _ATOMICITY_OPERATION_PRODUCER_SEQUENCE,
    "bounded_queries": _BOUNDED_OPERATION_PRODUCER_SEQUENCE,
}


def _operation_evidence_tag(
    producer_name: str,
    arguments: tuple[object, ...],
    keywords: Mapping[str, object],
    result: object,
) -> str:
    if producer_name == "fresh_process_kill_evidence":
        operation = "create" if keywords.get("creation") is not None else "compare_and_swap"
        return f"{operation}:{keywords.get('seam')}"
    if producer_name == "sqlite_result_code_fault_evidence":
        return cast(str, keywords.get("seam"))
    if producer_name in {
        "fresh_process_writer_contention_evidence",
        "fresh_process_two_writer_evidence",
    }:
        operation_value = keywords.get("operation")
        if type(operation_value) is not str:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        outcomes = getattr(result, "outcomes", None)
        suffix = (
            ""
            if outcomes is None
            else ":"
            + ",".join(
                outcome.value if isinstance(outcome, Enum) else str(outcome) for outcome in outcomes
            )
        )
        return f"{operation_value}{suffix}"
    if producer_name in {
        "true_during_commit_evidence",
        "ioerr_write_evidence",
        "max_page_count_evidence",
        "wal_concurrency_evidence",
        "query_plan_evidence",
    }:
        return producer_name
    if producer_name in {"create_stream", "compare_and_swap_stream", "load_current"}:
        classification = getattr(result, "classification", None)
        if not isinstance(classification, Enum):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        classification_value = classification.value
        if type(classification_value) is not str:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return classification_value
    if producer_name == "bootstrap_store":
        if type(result) is not StoreToken:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return "bootstrap_store"
    if producer_name == "audit_history":
        classification = getattr(result, "classification", None)
        if not isinstance(classification, Enum):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return (
            f"{classification.value}:limit={keywords.get('limit')}:"
            f"continuation={keywords.get('continuation') is not None}"
        )
    raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _operation_evidence_executor(  # noqa: UP047 - ParamSpec preserves signatures.
    function: Callable[_Parameters, _Result],
) -> Callable[_Parameters, _Result]:
    """Bind a low-level executor to a closure-held receipt producer."""

    if _OPERATION_PRODUCERS_FROZEN:
        raise RuntimeError("TASK064 operation producers are frozen")
    capability = object()
    producer_name = function.__name__
    _OPERATION_PRODUCERS[id(capability)] = producer_name

    @wraps(function)
    def wrapped(
        *args: _Parameters.args,
        **kwargs: _Parameters.kwargs,
    ) -> _Result:
        state = _ACTIVE_GATE_OPERATIONS.get()
        depth = _ACTIVE_OPERATION_EXECUTOR_DEPTH.get()
        if state is None or depth > 0:
            return function(*args, **kwargs)
        _validated_evidence_run(state.run)
        expected_sequence = _GATE_OPERATION_PRODUCER_SEQUENCES.get(state.gate)
        allowed = _GATE_OPERATION_ALLOWLIST.get(state.gate)
        if (
            _ACTIVE_EVIDENCE_RUN.get() is not state.run
            or _OPERATION_PRODUCERS.get(id(capability)) != producer_name
            or expected_sequence is None
            or allowed is None
            or producer_name not in allowed
            or len(state.observations) >= len(expected_sequence)
            or expected_sequence[len(state.observations)] != producer_name
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        registration = state.run._pytest_registration
        registrations: list[_ActivePytestRoot] = []
        token_nonces: list[bytes] = []
        for value in (
            *cast(tuple[object, ...], args),
            *cast(Mapping[str, object], kwargs).values(),
        ):
            _collect_evidence_registrations(
                value,
                registrations,
                token_nonces,
            )
        if registrations and any(observed is not registration for observed in registrations):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        depth_token = _ACTIVE_OPERATION_EXECUTOR_DEPTH.set(depth + 1)
        try:
            result = function(*args, **kwargs)
        finally:
            _ACTIVE_OPERATION_EXECUTOR_DEPTH.reset(depth_token)
        result_registrations: list[_ActivePytestRoot] = []
        result_token_nonces: list[bytes] = []
        _collect_evidence_registrations(
            result,
            result_registrations,
            result_token_nonces,
        )
        if any(observed is not registration for observed in result_registrations):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        for nonce in result_token_nonces:
            if nonce not in token_nonces:
                token_nonces.append(nonce)
        state.observations.append(
            _OperationObservation(
                producer_name=producer_name,
                operation_tag=_operation_evidence_tag(
                    producer_name,
                    cast(tuple[object, ...], args),
                    cast(Mapping[str, object], kwargs),
                    result,
                ),
                result=result,
                payload_digest=_evidence_payload_digest(result),
                sequence=len(state.observations),
                token_nonces=tuple(token_nonces),
            )
        )
        return result

    return wrapped


def _private_gate_receipt_digest(
    run: _EvidenceRun,
    gate: str,
) -> str | None:
    """Bind one whole-gate result to its exact private operation/rejection receipts."""

    ledger = _validated_evidence_run(run)
    operations = ledger.operation_runs.get(gate)
    rejections = ledger.rejection_runs.get(gate)
    if operations is None and rejections is None:
        return None
    return _evidence_payload_digest(
        (
            gate,
            run._nonce,
            ()
            if operations is None
            else tuple(
                (
                    observation.producer_name,
                    observation.operation_tag,
                    observation.payload_digest,
                    observation.sequence,
                    observation.token_nonces,
                )
                for observation in operations
            ),
            ()
            if rejections is None
            else tuple(
                (
                    id(observation.scenario.capability),
                    observation.scenario.label,
                    observation.code,
                    observation.sqlite_errorcode,
                    observation.process_id,
                    observation.ordinal,
                    observation.target_nonce,
                    observation.call_digest,
                )
                for observation in rejections
            ),
        )
    )


def _whole_gate_collector(
    gate: str,
    *,
    token_roles: Callable[
        [tuple[object, ...], Mapping[str, object], object],
        tuple[tuple[str, StoreToken], ...],
    ],
) -> Callable[
    [Callable[_Parameters, _Result]],
    Callable[_Parameters, _Result],
]:
    """Create one frozen closure-held producer for one generated gate slot."""

    if (
        _GATE_COLLECTORS_FROZEN
        or gate not in GENERATED_EVIDENCE_GATES
        or not callable(token_roles)
        or any(metadata[1] == gate for metadata in _GATE_PRODUCERS.values())
    ):
        raise RuntimeError("invalid TASK064 gate collector registration")
    producer = _EvidenceProducer(
        capability=object(),
        name="",
        gate=gate,
        ordinal=GENERATED_EVIDENCE_GATES.index(gate),
    )

    def decorate(
        function: Callable[_Parameters, _Result],
    ) -> Callable[_Parameters, _Result]:
        registered = _EvidenceProducer(
            capability=producer.capability,
            name=function.__name__,
            gate=producer.gate,
            ordinal=producer.ordinal,
        )
        producer_metadata = (
            registered.name,
            registered.gate,
            registered.ordinal,
        )
        _GATE_PRODUCERS[id(registered.capability)] = producer_metadata

        @wraps(function)
        def wrapped(
            *args: _Parameters.args,
            **kwargs: _Parameters.kwargs,
        ) -> _Result:
            if not args or type(args[0]) is not _EvidenceRun:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            run = args[0]
            ledger = _validated_evidence_run(run)
            if (
                _GATE_PRODUCERS.get(id(registered.capability)) != producer_metadata
                or _ACTIVE_EVIDENCE_RUN.get() is not run
                or ledger.closed
                or ledger.consumed
                or not ledger.recording
                or registered.ordinal in ledger.observations
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            result = function(*args, **kwargs)
            registrations: list[_ActivePytestRoot] = []
            token_nonces: list[bytes] = []
            for value in (
                *args[1:],
                *cast(Mapping[str, object], kwargs).values(),
                result,
            ):
                _collect_evidence_registrations(
                    value,
                    registrations,
                    token_nonces,
                )
            registration = run._pytest_registration
            named_tokens = token_roles(
                args[1:],
                cast(Mapping[str, object], kwargs),
                result,
            )
            if (
                any(observed is not registration for observed in registrations)
                or registration.process_id != os.getpid()
                or _ACTIVE_PYTEST_ROOTS.get(id(registration.path_object)) is not registration
                or type(named_tokens) is not tuple
                or not named_tokens
                or len(named_tokens) != len({name for name, _ in named_tokens})
                or any(
                    type(name) is not str
                    or not name
                    or type(token) is not StoreToken
                    or _TOKEN_REGISTRY.get(token._nonce) is None
                    or _TOKEN_REGISTRY[token._nonce].pytest_registration is not registration
                    for name, token in named_tokens
                )
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            ledger.observations[registered.ordinal] = _EvidenceObservation(
                value=result,
                payload_digest=_evidence_payload_digest(result),
                registration=registration,
                producer=registered,
                run=run,
                process_id=os.getpid(),
                token_roles=tuple((name, token._nonce) for name, token in named_tokens),
                private_receipt_digest=_private_gate_receipt_digest(run, registered.gate),
            )
            return result

        return wrapped

    return decorate


def _close_descriptors(descriptors: Sequence[int]) -> None:
    for descriptor in descriptors:
        with suppress(OSError):
            os.close(descriptor)


def _open_pipes(count: int) -> tuple[tuple[int, int], ...]:
    pipes: list[tuple[int, int]] = []
    try:
        for _ in range(count):
            pipes.append(os.pipe())
    except OSError:
        _close_descriptors(tuple(descriptor for pipe in pipes for descriptor in pipe))
        raise
    return tuple(pipes)


def _wait_for_owned_process(process_id: int, *, timeout_seconds: float = 10.0) -> int | None:
    """Boundedly reap one exact child without ever signaling a numeric PID."""

    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            waited_process_id, status = os.waitpid(process_id, os.WNOHANG)
        except InterruptedError:
            continue
        except (ChildProcessError, OSError):
            return None
        if waited_process_id == process_id:
            return status
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.01)


def _terminate_and_reap_processes(process_ids: Sequence[int]) -> bool:
    """Kill and boundedly reap only PIDs still proven to be our unreaped children."""

    remaining: set[int] = set()
    cleanup_ok = True
    for process_id in dict.fromkeys(process_ids):
        while True:
            try:
                waited_process_id, _ = os.waitpid(process_id, os.WNOHANG)
            except InterruptedError:
                continue
            except ChildProcessError:
                break
            except OSError:
                cleanup_ok = False
                break
            if waited_process_id == process_id:
                break
            remaining.add(process_id)
            break
    for process_id in tuple(remaining):
        try:
            os.kill(process_id, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            cleanup_ok = False
    deadline = time.monotonic() + 10.0
    while remaining and time.monotonic() < deadline:
        for process_id in tuple(remaining):
            while True:
                try:
                    waited_process_id, _ = os.waitpid(process_id, os.WNOHANG)
                except InterruptedError:
                    continue
                except ChildProcessError:
                    remaining.discard(process_id)
                except OSError:
                    cleanup_ok = False
                    remaining.discard(process_id)
                else:
                    if waited_process_id == process_id:
                        remaining.discard(process_id)
                break
        if remaining:
            time.sleep(0.01)
    return cleanup_ok and not remaining


def _read_process_packet(_stage: str, descriptor: int, size: int) -> bytes:
    """Read one exact finite child packet with a shared deadline."""

    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + 10.0
    payload = bytearray()
    try:
        selector.register(descriptor, selectors.EVENT_READ)
        while len(payload) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not selector.select(timeout=remaining):
                break
            chunk = os.read(descriptor, size - len(payload))
            if not chunk:
                break
            payload.extend(chunk)
        return bytes(payload)
    finally:
        selector.close()


def _write_process_packet(descriptor: int, payload: bytes) -> None:
    """Write one finite child packet without accepting a partial acknowledgement."""

    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError(errno.EIO, "short process packet write")
        view = view[written:]


def _unproven_process_fault(seam: str, *, reason: str) -> FaultEvidence:
    return FaultEvidence(
        seam=seam,
        disposition=EvidenceDisposition.UNPROVEN,
        sqlite_errorcode=None,
        observed_syscall=None,
        acknowledgement_bytes=0,
        reopened_state=ReopenedState.UNAVAILABLE,
        reason=reason,
    )


def _next_generation_name() -> str:
    global _GENERATION_COUNTER
    _GENERATION_COUNTER += 1
    return f"{_GENERATION_PREFIX}{_GENERATION_COUNTER:06d}-{secrets.token_hex(8)}"


def _require_exact_int(value: object, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return value


def _digest_bytes(value: str) -> bytes:
    if (
        type(value) is not str
        or len(value) != _DIGEST_BYTES
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return value.encode("ascii")


def _decode_digest(value: object) -> str:
    if type(value) is not bytes:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        text = value.decode("ascii")
    except UnicodeDecodeError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    _digest_bytes(text)
    return text


def natural_identity_key(
    *,
    source: str,
    venue: str,
    instrument: str,
    provider_symbol: str,
    instrument_type: str,
    request_variant: str,
) -> bytes:
    """Return the exact reversible ADR-0031 natural identity key."""

    atoms = (
        source,
        venue,
        instrument,
        provider_symbol,
        instrument_type,
        request_variant,
    )
    result = bytearray(NATURAL_IDENTITY_DOMAIN)
    for atom in atoms:
        if type(atom) is not str:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        encoded = atom.encode("utf-8", "surrogatepass")
        if not encoded or len(encoded) > 0xFFFFFFFF:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        result.extend(len(encoded).to_bytes(4, "big"))
        result.extend(encoded)
    return bytes(result)


def decode_natural_identity_key(value: object) -> tuple[str, str, str, str, str, str]:
    """Decode and prove an exact round trip for one natural identity key."""

    if type(value) is not bytes or not value.startswith(NATURAL_IDENTITY_DOMAIN):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    offset = len(NATURAL_IDENTITY_DOMAIN)
    atoms: list[str] = []
    for _ in range(6):
        if offset + 4 > len(value):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        size = int.from_bytes(value[offset : offset + 4], "big")
        offset += 4
        if size == 0 or offset + size > len(value):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            atom = value[offset : offset + size].decode("utf-8", "surrogatepass")
        except UnicodeDecodeError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        atoms.append(atom)
        offset += size
    if offset != len(value):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decoded = cast(tuple[str, str, str, str, str, str], tuple(atoms))
    if (
        natural_identity_key(
            source=decoded[0],
            venue=decoded[1],
            instrument=decoded[2],
            provider_symbol=decoded[3],
            instrument_type=decoded[4],
            request_variant=decoded[5],
        )
        != value
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return decoded


def normalize_schema_sql(value: str) -> str:
    """Normalize one SQLite object statement without rewriting quoted text."""

    if type(value) is not str or "\x00" in value or "--" in value or "/*" in value:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    output: list[str] = []
    quote: str | None = None
    bracket = False
    pending_space = False
    index = 0
    while index < len(value):
        character = value[index]
        if quote is not None:
            output.append(character)
            if character == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    output.append(value[index + 1])
                    index += 1
                else:
                    quote = None
            index += 1
            continue
        if bracket:
            output.append(character)
            if character == "]":
                bracket = False
            index += 1
            continue
        if character in {"'", '"', "`"}:
            if pending_space and output:
                output.append(" ")
            pending_space = False
            quote = character
            output.append(character)
        elif character == "[":
            if pending_space and output:
                output.append(" ")
            pending_space = False
            bracket = True
            output.append(character)
        elif character in " \t\r\n\f\v":
            pending_space = bool(output)
        else:
            if pending_space and output:
                output.append(" ")
            pending_space = False
            output.append(character)
        index += 1
    if quote is not None or bracket:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    normalized = "".join(output).strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    return normalized


def canonical_descriptor_bytes(descriptor: Mapping[str, object]) -> bytes:
    """Return the frozen no-newline canonical JSON descriptor bytes."""

    return json.dumps(
        descriptor,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")


def schema_fingerprint(descriptor: Mapping[str, object]) -> str:
    """Return the domain-separated schema descriptor fingerprint."""

    digest = hashlib.sha256(
        SCHEMA_DESCRIPTOR_DOMAIN + canonical_descriptor_bytes(descriptor)
    ).hexdigest()
    return f"sha256:{digest}"


def _quote_identifier(value: str) -> str:
    if value not in SCHEMA_OBJECT_ORDER:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return '"' + value.replace('"', '""') + '"'


def _fetch_all(
    connection: sqlite3.Connection,
    sql: str,
    parameters: Sequence[object] = (),
) -> list[sqlite3.Row]:
    cursor = connection.execute(sql, tuple(parameters))
    try:
        return cast(list[sqlite3.Row], cursor.fetchall())
    finally:
        _close_cursor_preserving_primary(cursor)


def _fetch_one(
    connection: sqlite3.Connection,
    sql: str,
    parameters: Sequence[object] = (),
) -> sqlite3.Row:
    rows = _fetch_all(connection, sql, parameters)
    if len(rows) != 1:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return rows[0]


def installed_schema_descriptor(connection: sqlite3.Connection) -> dict[str, object]:
    """Describe the exact installed schema in the frozen object order."""

    raw_rows = _fetch_all(
        connection,
        """
        SELECT type, name, tbl_name, sql
        FROM sqlite_schema
        WHERE name NOT LIKE 'sqlite_%'
        """,
    )
    by_name = {cast(str, row["name"]): row for row in raw_rows}
    if tuple(name for name in SCHEMA_OBJECT_ORDER if name in by_name) != SCHEMA_OBJECT_ORDER:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if set(by_name) != set(SCHEMA_OBJECT_ORDER):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    objects: list[dict[str, object]] = []
    for name in SCHEMA_OBJECT_ORDER:
        row = by_name[name]
        object_type = cast(str, row["type"])
        table_name = cast(str, row["tbl_name"])
        raw_sql = row["sql"]
        if type(raw_sql) is not str:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        item: dict[str, object] = {
            "type": object_type,
            "name": name,
            "table": table_name,
            "normalized_sql": normalize_schema_sql(raw_sql),
        }
        if object_type == "table":
            quoted = _quote_identifier(name)
            columns = _fetch_all(connection, f"PRAGMA table_xinfo({quoted})")
            item["columns"] = [
                {
                    "cid": cast(int, column["cid"]),
                    "name": cast(str, column["name"]),
                    "declared_type": cast(str, column["type"]),
                    "not_null": bool(column["notnull"]),
                    "default_sql": cast(str | None, column["dflt_value"]),
                    "primary_key_position": cast(int, column["pk"]),
                    "hidden": cast(int, column["hidden"]),
                }
                for column in columns
            ]
            foreign_keys = _fetch_all(connection, f"PRAGMA foreign_key_list({quoted})")
            item["foreign_keys"] = [
                {
                    "id": cast(int, foreign_key["id"]),
                    "sequence": cast(int, foreign_key["seq"]),
                    "parent_table": cast(str, foreign_key["table"]),
                    "from_column": cast(str, foreign_key["from"]),
                    "to_column": cast(str, foreign_key["to"]),
                    "on_update": cast(str, foreign_key["on_update"]),
                    "on_delete": cast(str, foreign_key["on_delete"]),
                    "match": cast(str, foreign_key["match"]),
                }
                for foreign_key in foreign_keys
            ]
        elif object_type == "index":
            quoted = _quote_identifier(name)
            index_columns = _fetch_all(connection, f"PRAGMA index_xinfo({quoted})")
            item["columns"] = [
                {
                    "sequence": cast(int, column["seqno"]),
                    "column_id": cast(int, column["cid"]),
                    "name": cast(str | None, column["name"]),
                    "descending": bool(column["desc"]),
                    "collation": cast(str, column["coll"]),
                    "key": bool(column["key"]),
                }
                for column in index_columns
            ]
        objects.append(item)
    return {
        "descriptor_version": 1,
        "application_id": APPLICATION_ID,
        "user_version": USER_VERSION,
        "page_size": PAGE_SIZE,
        "objects": objects,
    }


def load_schema_descriptor() -> dict[str, object]:
    """Load only exact canonical descriptor fixture bytes."""

    try:
        raw = SCHEMA_DESCRIPTOR_PATH.read_bytes()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    document = raw[:-1]
    try:
        value = json.loads(document)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if type(value) is not dict:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    descriptor = cast(dict[str, object], value)
    if canonical_descriptor_bytes(descriptor) != document:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return descriptor


def load_schema_fingerprint() -> str:
    """Load and independently recompute the golden descriptor fingerprint."""

    try:
        raw = SCHEMA_FINGERPRINT_PATH.read_bytes()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        value = raw[:-1].decode("ascii")
    except UnicodeDecodeError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    _digest_bytes(value)
    if value != schema_fingerprint(load_schema_descriptor()):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return value


def _walk_without_aliases(path: Path, *, include_leaf: bool) -> None:
    target = path if include_leaf else path.parent
    if not target.is_absolute():
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    current = Path(target.anchor)
    parts = target.parts[1:]
    for part in parts:
        current = current / part
        try:
            details = current.lstat()
        except OSError:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if stat.S_ISLNK(details.st_mode) or not (
            stat.S_ISDIR(details.st_mode)
            if current != target or not include_leaf
            else stat.S_ISREG(details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)


def _resolve_existing(path: Path, *, code: HarnessFailureCode) -> Path:
    try:
        return path.resolve(strict=True)
    except (OSError, RuntimeError):
        raise HarnessFailure(code) from None


@contextmanager
def _pytest_root_scope(
    pytest_root: Path,
    *,
    node_id: str,
) -> Iterator[None]:
    """Bind one real pytest fixture object to one exact allowed TASK064 test node."""

    if (
        not isinstance(pytest_root, Path)
        or not pytest_root.is_absolute()
        or type(node_id) is not str
        or not any(
            node_id == module or node_id.startswith(f"{module}::")
            for module in _ALLOWED_TASK064_TEST_MODULES
        )
        or os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" (", maxsplit=1)[0] != node_id
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    _walk_without_aliases(pytest_root, include_leaf=False)
    try:
        details = pytest_root.lstat()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    resolved = _resolve_existing(
        pytest_root,
        code=HarnessFailureCode.INVALID_BOOTSTRAP_ROOT,
    )
    revocation_flag: mmap.mmap | None = None
    try:
        revocation_flag = mmap.mmap(
            -1,
            1,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_READ | mmap.PROT_WRITE,
        )
        revocation_flag[0] = 0
    except (OSError, ValueError):
        if revocation_flag is not None:
            with suppress(OSError, ValueError):
                revocation_flag.close()
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    identity = _ActivePytestRoot(
        path_object=pytest_root,
        resolved_path=resolved,
        device=details.st_dev,
        inode=details.st_ino,
        uid=details.st_uid,
        mode=stat.S_IMODE(details.st_mode),
        process_id=os.getpid(),
        node_id=node_id,
        nonce=secrets.token_bytes(32),
        revocation_flag=revocation_flag,
        evidence_ledger=_EvidenceLedger(
            nonce=secrets.token_bytes(32),
            observations={},
            operation_runs={},
            rejection_runs={},
        ),
    )
    key = id(pytest_root)
    if key in _ACTIVE_PYTEST_ROOTS:
        revocation_flag.close()
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    _REVOKED_PYTEST_ROOTS.pop(key, None)
    _ACTIVE_PYTEST_ROOTS[key] = identity
    try:
        yield
    finally:
        identity.evidence_ledger.closed = True
        identity.evidence_ledger.recording = False
        identity.evidence_ledger.observations.clear()
        identity.evidence_ledger.operation_runs.clear()
        identity.evidence_ledger.rejection_runs.clear()
        active_run = _ACTIVE_EVIDENCE_RUN.get()
        if active_run is not None and active_run._pytest_registration is identity:
            _ACTIVE_EVIDENCE_RUN.set(None)
        with suppress(IndexError, OSError, ValueError):
            identity.revocation_flag[0] = 1
            identity.revocation_flag.flush()
        if _ACTIVE_PYTEST_ROOTS.get(key) is identity:
            del _ACTIVE_PYTEST_ROOTS[key]
        _REVOKED_PYTEST_ROOTS[key] = identity
        with suppress(OSError, ValueError):
            identity.revocation_flag.close()


def _validate_bootstrap_root(pytest_root: Path) -> Path:
    if not isinstance(pytest_root, Path) or not pytest_root.is_absolute():
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    active = _ACTIVE_PYTEST_ROOTS.get(id(pytest_root))
    if (
        active is None
        or active.path_object is not pytest_root
        or active.process_id != os.getpid()
        or os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" (", maxsplit=1)[0] != active.node_id
        or type(active.nonce) is not bytes
        or len(active.nonce) != 32
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    _walk_without_aliases(pytest_root, include_leaf=False)
    try:
        details = pytest_root.lstat()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_dev != active.device
        or details.st_ino != active.inode
        or details.st_uid != active.uid
        or stat.S_IMODE(details.st_mode) != active.mode
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    resolved = _resolve_existing(
        pytest_root,
        code=HarnessFailureCode.INVALID_BOOTSTRAP_ROOT,
    )
    if resolved != active.resolved_path:
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    return resolved


def begin_generated_evidence_run(pytest_root: Path) -> _EvidenceRun:
    """Begin the only report-capable evidence run for this exact pytest scope."""

    _validate_bootstrap_root(pytest_root)
    registration = _ACTIVE_PYTEST_ROOTS[id(pytest_root)]
    ledger = registration.evidence_ledger
    if (
        ledger.closed
        or ledger.consumed
        or ledger.recording
        or ledger.run is not None
        or ledger.receipt is not None
        or ledger.observations
        or ledger.operation_runs
        or ledger.rejection_runs
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    run = _EvidenceRun(
        _constructor=_EVIDENCE_CAPABILITY_CONSTRUCTOR,
        _pytest_registration=registration,
        _nonce=secrets.token_bytes(32),
    )
    ledger.run = run
    ledger.recording = True
    _ACTIVE_EVIDENCE_RUN.set(run)
    return run


def _validated_evidence_run(run: _EvidenceRun) -> _EvidenceLedger:
    if (
        type(run) is not _EvidenceRun
        or run._constructor is not _EVIDENCE_CAPABILITY_CONSTRUCTOR
        or type(run._nonce) is not bytes
        or len(run._nonce) != 32
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    registration = run._pytest_registration
    _validate_bootstrap_root(registration.path_object)
    ledger = registration.evidence_ledger
    if (
        ledger.closed
        or ledger.consumed
        or ledger.run is not run
        or registration.process_id != os.getpid()
        or type(ledger.nonce) is not bytes
        or len(ledger.nonce) != 32
        or (ledger.recording and _ACTIVE_EVIDENCE_RUN.get() is not run)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return ledger


def _traceback_contains_harness_executor(
    error: BaseException,
    executor_names: tuple[str, ...],
) -> bool:
    expected_file = os.path.normcase(os.path.abspath(__file__))
    traceback = error.__traceback__
    while traceback is not None:
        frame = traceback.tb_frame
        if (
            os.path.normcase(os.path.abspath(frame.f_code.co_filename)) == expected_file
            and frame.f_code.co_name in executor_names
        ):
            return True
        traceback = traceback.tb_next
    return False


def _active_rejection_recording_ledger() -> _EvidenceLedger:
    """Validate outer receipt authority without masking an intentional root fault."""

    run = _ACTIVE_EVIDENCE_RUN.get()
    if (
        run is None
        or type(run) is not _EvidenceRun
        or run._constructor is not _EVIDENCE_CAPABILITY_CONSTRUCTOR
        or type(run._nonce) is not bytes
        or len(run._nonce) != 32
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    registration = run._pytest_registration
    ledger = registration.evidence_ledger
    if (
        ledger.run is not run
        or not ledger.recording
        or ledger.receipt is not None
        or ledger.closed
        or ledger.consumed
        or registration.process_id != os.getpid()
        or _ACTIVE_PYTEST_ROOTS.get(id(registration.path_object)) is not registration
        or os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" (", maxsplit=1)[0]
        != registration.node_id
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return ledger


def capture_harness_rejection(
    label: str,
    *,
    pytest_root: Path | None = None,
    token: StoreToken | None = None,
    stream_id: UUID | None = None,
    natural_key: bytes | None = None,
    limit: int | None = None,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
) -> tuple[str, HarnessFailureCode]:
    """Execute one fixed harness-owned rejection scenario and record its result."""

    _active_rejection_recording_ledger()
    state = _ACTIVE_REJECTION_COLLECTOR.get()
    if (
        state is None
        or state.run is not _ACTIVE_EVIDENCE_RUN.get()
        or len(state.observations) >= len(state.scenarios)
        or type(label) is not str
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    scenario = state.scenarios[len(state.observations)]
    if (
        scenario.kind != "harness"
        or scenario.label != label
        or _REGISTERED_REJECTION_SCENARIOS.get(label) is not scenario
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    executor_name = scenario.executor_names[0] if len(scenario.executor_names) == 1 else None
    if executor_name == "bootstrap_store":
        if (
            not isinstance(pytest_root, Path)
            or token is not None
            or stream_id is not None
            or natural_key is not None
            or limit is not None
            or expectation is not None
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    elif executor_name == "verify_store":
        if (
            type(token) is not StoreToken
            or pytest_root is not None
            or stream_id is not None
            or natural_key is not None
            or limit is not None
            or expectation is not None
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    elif executor_name == "audit_history":
        expected_limit = 101 if label == "audit_limit_overflow" else 100
        if (
            type(token) is not StoreToken
            or pytest_root is not None
            or type(stream_id) is not UUID
            or type(natural_key) is not bytes
            or limit != expected_limit
            or (
                label == "expectation_conflict_corrupt_precedence"
                and type(expectation) is not ContinuousPublicTradeStreamExpectationV1
            )
            or (label != "expectation_conflict_corrupt_precedence" and expectation is not None)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    else:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    outer_registration = state.run._pytest_registration
    target_registration = None if pytest_root is None else _ACTIVE_PYTEST_ROOTS.get(id(pytest_root))
    token_registration = None if token is None else _TOKEN_REGISTRY.get(token._nonce)
    if label == "relative_root" and pytest_root != Path("relative"):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "unregistered_root" and (
        pytest_root is None
        or not pytest_root.is_absolute()
        or target_registration is not None
        or pytest_root.parent != outer_registration.path_object
        or pytest_root.name != "unregistered-root"
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "reconstructed_root" and (
        pytest_root is None
        or pytest_root is outer_registration.path_object
        or pytest_root != outer_registration.path_object
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "sibling_root" and (
        pytest_root is None
        or pytest_root.parent != outer_registration.path_object.parent
        or pytest_root.name != f"{outer_registration.path_object.name}-sibling"
        or target_registration is not None
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "nested_root" and (
        pytest_root is None
        or pytest_root.parent != outer_registration.path_object
        or pytest_root.name != "nested-root"
        or target_registration is not None
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "symlink_root" and (pytest_root is None or not pytest_root.is_symlink()):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "forged_token" and (
        token is None or token._nonce != b"\x00" * 32 or token_registration is not None
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {
        "wrong_process_root",
        "wrong_node_root",
        "replaced_registered_root",
    } and (target_registration is None or target_registration is outer_registration):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {"wrong_process_token", "wrong_node_token"} and (
        token_registration is None or token_registration.pytest_registration is outer_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    revoked_target = None if pytest_root is None else _REVOKED_PYTEST_ROOTS.get(id(pytest_root))
    if label == "expired_root" and (
        target_registration is not None
        or revoked_target is None
        or revoked_target.path_object is not pytest_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "expired_token" and (
        token_registration is None
        or _ACTIVE_PYTEST_ROOTS.get(id(token_registration.pytest_registration.path_object))
        is token_registration.pytest_registration
        or _REVOKED_PYTEST_ROOTS.get(id(token_registration.pytest_registration.path_object))
        is not token_registration.pytest_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {
        "hardlink_database",
        "unexpected_entry",
        "readonly_database",
        "widened_root",
        "missing_database",
        "replaced_database",
        "allowed_name_symlink",
        "path_resolution_operation",
        "unsupported_generation",
        "malformed_generation",
        "short_page",
        "retained_history_canonical_bytes",
    } and (
        token_registration is None
        or token_registration.pytest_registration is not outer_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "hardlink_database" and token is not None:
        try:
            database_details = token._database_path.lstat()
            external_details = (token._pytest_root / "forbidden-hardlink.sqlite3").lstat()
            if (
                database_details.st_nlink != 2
                or external_details.st_nlink != 2
                or external_details.st_dev != database_details.st_dev
                or external_details.st_ino != database_details.st_ino
                or set(os.listdir(token._generation_root))
                - {
                    _DATABASE_BASENAME,
                    f"{_DATABASE_BASENAME}-wal",
                    f"{_DATABASE_BASENAME}-shm",
                }
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if (
        label == "unexpected_entry"
        and token is not None
        and not (token._generation_root / "unexpected-entry").is_file()
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "readonly_database" and token is not None:
        try:
            if stat.S_IMODE(token._database_path.lstat().st_mode) != 0o400:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if label == "widened_root":
        try:
            if (
                stat.S_IMODE(outer_registration.path_object.lstat().st_mode)
                == outer_registration.mode
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if label == "missing_database" and token is not None and token._database_path.exists():
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "replaced_database" and token is not None:
        try:
            details = token._database_path.lstat()
            if details.st_dev == token._device and details.st_ino == token._inode:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if (
        label == "allowed_name_symlink"
        and token is not None
        and not (token._generation_root / f"{_DATABASE_BASENAME}-shm").is_symlink()
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "short_page" and token is not None:
        try:
            if token._database_path.stat().st_size >= PAGE_SIZE:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if label in {"unsupported_generation", "malformed_generation"} and token is not None:
        descriptor = -1
        try:
            descriptor = os.open(
                token._database_path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            )
            header = os.pread(descriptor, 4, 60)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
        expected_version = 2 if label == "unsupported_generation" else 0
        if len(header) != 4 or struct.unpack(">I", header)[0] != expected_version:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "path_resolution_bootstrap" and pytest_root is not outer_registration.path_object:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if executor_name == "audit_history" and (
        token_registration is None
        or token_registration.pytest_registration is not outer_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "expectation_conflict_corrupt_precedence" and (
        expectation is None
        or expectation.identity.stream_id != stream_id
        or expectation.identity.source != "report-mismatching-source"
        or _natural_key_from_expectation(expectation) == natural_key
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {
        "retained_history_canonical_bytes",
        "two_candidate_corrupt_precedence",
        "expectation_conflict_corrupt_precedence",
    }:
        if token is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        predicate_connection, _ = _connect(token, writer=False)
        try:
            if label == "retained_history_canonical_bytes":
                predicate_row = _fetch_one(
                    predicate_connection,
                    """
                    SELECT
                        COUNT(*) AS candidate_count,
                        SUM(history.record_canonical_bytes = ?) AS malformed_count
                    FROM continuous_public_trade_stream AS stream
                    JOIN continuous_public_trade_history AS history
                      ON history.stream_row_id = stream.stream_row_id
                     AND history.successor_version = 1
                    """,
                    (b"{malformed-retained-creation",),
                )
                expected_candidates = 1
            else:
                predicate_row = _fetch_one(
                    predicate_connection,
                    """
                    SELECT
                        COUNT(*) AS candidate_count,
                        SUM(history.record_canonical_bytes = ?) AS malformed_count
                    FROM continuous_public_trade_stream AS stream
                    JOIN continuous_public_trade_history AS history
                      ON history.stream_row_id = stream.stream_row_id
                     AND history.successor_version = 1
                    WHERE stream.stream_uuid = ?
                       OR stream.natural_identity_key = ?
                    """,
                    (
                        b"{malformed-retained-creation",
                        cast(UUID, stream_id).bytes,
                        cast(bytes, natural_key),
                    ),
                )
                expected_candidates = 2 if label == "two_candidate_corrupt_precedence" else 1
            _verify_operation_authority(predicate_connection, token)
            if (
                predicate_row["candidate_count"] != expected_candidates
                or predicate_row["malformed_count"] != 1
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        finally:
            _close_preserving_primary(predicate_connection)

    original_getpid = os.getpid
    original_node = os.environ.get("PYTEST_CURRENT_TEST")
    original_resolve = Path.resolve

    def fail_resolve(_path: Path, *, strict: bool = False) -> Path:
        del strict
        raise FileNotFoundError("sanitized-report-path-probe")

    try:
        if label in {"wrong_process_root", "wrong_process_token"}:
            os.getpid = lambda: -1
        elif label in {"wrong_node_root", "wrong_node_token"}:
            os.environ["PYTEST_CURRENT_TEST"] = (
                f"{state.run._pytest_registration.node_id}::wrong (call)"
            )
        elif label in {"path_resolution_bootstrap", "path_resolution_operation"}:
            Path.resolve = fail_resolve  # type: ignore[method-assign,assignment]

        if executor_name == "bootstrap_store":
            bootstrap_store(cast(Path, pytest_root))
        elif executor_name == "verify_store":
            verify_store(cast(StoreToken, token))
        else:
            audit_history(
                cast(StoreToken, token),
                stream_id=cast(UUID, stream_id),
                natural_key=cast(bytes, natural_key),
                limit=cast(int, limit),
                expectation=expectation,
            )
    except HarnessFailure as error:
        if error.code is not scenario.code or not _traceback_contains_harness_executor(
            error,
            scenario.executor_names,
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        state.observations.append(
            _RejectionObservation(
                scenario=scenario,
                code=error.code,
                sqlite_errorcode=error.sqlite_errorcode,
                run=state.run,
                process_id=state.run._pytest_registration.process_id,
                ordinal=len(state.observations),
                target_nonce=None if token is None else token._nonce,
                call_digest=_evidence_payload_digest(
                    (
                        label,
                        None if pytest_root is None else str(pytest_root),
                        None if token is None else token._nonce,
                        stream_id,
                        natural_key,
                        limit,
                        expectation is not None,
                    )
                ),
            )
        )
        return label, error.code
    finally:
        os.getpid = original_getpid
        if original_node is None:
            os.environ.pop("PYTEST_CURRENT_TEST", None)
        else:
            os.environ["PYTEST_CURRENT_TEST"] = original_node
        Path.resolve = original_resolve  # type: ignore[method-assign]
    raise HarnessFailure(HarnessFailureCode.CORRUPT)


def capture_sqlite_rejection(
    label: str,
    connection: sqlite3.Connection,
    *,
    parameters: Sequence[object] = (),
) -> tuple[str, HarnessFailureCode]:
    """Execute one fixed SQL rejection scenario and record its exact result code."""

    _active_rejection_recording_ledger()
    state = _ACTIVE_REJECTION_COLLECTOR.get()
    if (
        state is None
        or state.run is not _ACTIVE_EVIDENCE_RUN.get()
        or len(state.observations) >= len(state.scenarios)
        or type(label) is not str
        or type(connection) is not _MeteredConnection
        or isinstance(parameters, (str, bytes, bytearray))
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    scenario = state.scenarios[len(state.observations)]
    if (
        scenario.kind != "sqlite"
        or scenario.label != label
        or _REGISTERED_REJECTION_SCENARIOS.get(label) is not scenario
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    binding = _CONNECTION_EVIDENCE_BINDINGS.get(id(connection))
    if (
        binding is None
        or not binding[1]
        or id(connection) not in _CONNECTION_PATH_SNAPSHOTS
        or _TOKEN_REGISTRY.get(binding[0]) is None
        or _TOKEN_REGISTRY[binding[0]].pytest_registration is not state.run._pytest_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    parameter_tuple = tuple(parameters)
    immutable_statements = {
        "metadata_update": "UPDATE stream_store_metadata SET page_size = 8192",
        "metadata_delete": "DELETE FROM stream_store_metadata",
        "history_update": (
            "UPDATE continuous_public_trade_history "
            "SET serialization_version = 1 WHERE successor_version = 1"
        ),
        "history_delete": "DELETE FROM continuous_public_trade_history",
        "stream_identity_update": (
            "UPDATE continuous_public_trade_stream SET stream_contract_version = 2"
        ),
        "stream_delete": "DELETE FROM continuous_public_trade_stream",
        "current_tail_jump": (
            "UPDATE continuous_public_trade_stream SET current_version = current_version + 2"
        ),
    }
    if label == "digest_byte_guard":
        statement = """
            INSERT INTO stream_tail_commit_guard (
                stream_row_id,
                successor_version,
                record_digest,
                unresolved_singleton_key
            ) VALUES (?, ?, ?, ?)
        """
        expected_parameter_count = 4
    elif label == "forbidden_schema_sql":
        statement = "DROP TRIGGER trg_history_no_delete"
        expected_parameter_count = 0
    elif label in {"transition_without_current_tail", "stream_without_creation_history"}:
        statement = "COMMIT"
        expected_parameter_count = 0
    elif label in {
        "orphan_creation_history",
        "wrong_predecessor_bytes",
        "wrong_predecessor_digest",
        "wrong_predecessor_root",
        "history_gap",
    }:
        statement = _INSERT_HISTORY_SQL
        expected_parameter_count = 15
    elif label in immutable_statements:
        statement = immutable_statements[label]
        expected_parameter_count = 0
    else:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if len(parameter_tuple) != expected_parameter_count:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "digest_byte_guard" and parameter_tuple != (
        99_001,
        2,
        b"sha256:" + (b"0" * 10) + b"\x00" + (b"f" * 53),
        0,
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {"transition_without_current_tail", "stream_without_creation_history"}:
        if not connection.in_transaction:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        stream_count = cast(
            int,
            _fetch_one(
                connection,
                "SELECT COUNT(*) FROM continuous_public_trade_stream",
            )[0],
        )
        history_count = cast(
            int,
            _fetch_one(
                connection,
                "SELECT COUNT(*) FROM continuous_public_trade_history",
            )[0],
        )
        guard_count = cast(
            int,
            _fetch_one(connection, "SELECT COUNT(*) FROM stream_tail_commit_guard")[0],
        )
        expected_counts = (1, 2, 1) if label == "transition_without_current_tail" else (1, 0, 0)
        if (stream_count, history_count, guard_count) != expected_counts:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "orphan_creation_history" and (
        parameter_tuple[0] != 99_902
        or parameter_tuple[1] != 1
        or parameter_tuple[2:5] != (b"creation", b"1.0", 1)
        or any(item is not None for item in parameter_tuple[9:14])
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label in {
        "wrong_predecessor_bytes",
        "wrong_predecessor_digest",
        "wrong_predecessor_root",
        "history_gap",
    }:
        retained = _fetch_one(
            connection,
            """
            SELECT
                current_version,
                current_record_canonical_bytes,
                current_record_digest,
                current_history_root
            FROM continuous_public_trade_stream
            WHERE stream_row_id = ?
            """,
            (parameter_tuple[0],),
        )
        current_version = retained["current_version"]
        retained_bytes = retained["current_record_canonical_bytes"]
        retained_digest = retained["current_record_digest"]
        retained_root = retained["current_history_root"]
        exact_common = (
            parameter_tuple[1] == 2
            and parameter_tuple[9] == current_version
            and parameter_tuple[11] == retained_root
            and parameter_tuple[12] == retained_bytes
            and parameter_tuple[13] == retained_digest
        )
        if label == "wrong_predecessor_bytes":
            valid = (
                parameter_tuple[1] == 2
                and parameter_tuple[9] == current_version
                and parameter_tuple[11] == retained_root
                and type(parameter_tuple[12]) is bytes
                and parameter_tuple[12].endswith(b" ")
                and parameter_tuple[12] != retained_bytes
                and parameter_tuple[13] == retained_digest
            )
        elif label == "wrong_predecessor_digest":
            valid = (
                parameter_tuple[1] == 2
                and parameter_tuple[9] == current_version
                and parameter_tuple[11] == retained_root
                and parameter_tuple[12] == retained_bytes
                and parameter_tuple[13] != retained_digest
            )
        elif label == "wrong_predecessor_root":
            valid = (
                parameter_tuple[1] == 2
                and parameter_tuple[9] == current_version
                and parameter_tuple[11] != retained_root
                and parameter_tuple[12] == retained_bytes
                and parameter_tuple[13] == retained_digest
            )
        else:
            valid = (
                parameter_tuple[1] == 3
                and parameter_tuple[9] == current_version + 1
                and parameter_tuple[11] == retained_root
                and parameter_tuple[12] == retained_bytes
                and parameter_tuple[13] == retained_digest
            )
        if not valid or (label != "history_gap" and exact_common):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        connection.execute(statement, parameter_tuple).close()
    except sqlite3.Error as error:
        sqlite_errorcode = getattr(error, "sqlite_errorcode", None)
        code = sqlite_result_failure_code(sqlite_errorcode)
        expected_sqlite_errorcode = {
            "digest_byte_guard": 275,  # SQLITE_CONSTRAINT_CHECK
            "forbidden_schema_sql": 23,  # SQLITE_AUTH
            "transition_without_current_tail": 787,  # SQLITE_CONSTRAINT_FOREIGNKEY
            "stream_without_creation_history": 787,
            "orphan_creation_history": 1811,  # SQLITE_CONSTRAINT_TRIGGER
            "metadata_update": 1811,
            "metadata_delete": 1811,
            "history_update": 1811,
            "history_delete": 1811,
            "stream_identity_update": 1811,
            "stream_delete": 1811,
            "current_tail_jump": 1811,
            "wrong_predecessor_bytes": 1811,
            "wrong_predecessor_digest": 1811,
            "wrong_predecessor_root": 1811,
            "history_gap": 1811,
        }[label]
        if (
            type(sqlite_errorcode) is not int
            or sqlite_errorcode != expected_sqlite_errorcode
            or code is not scenario.code
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        state.observations.append(
            _RejectionObservation(
                scenario=scenario,
                code=code,
                sqlite_errorcode=sqlite_errorcode,
                run=state.run,
                process_id=state.run._pytest_registration.process_id,
                ordinal=len(state.observations),
                target_nonce=binding[0],
                call_digest=_evidence_payload_digest(
                    (label, binding[0], statement, parameter_tuple)
                ),
            )
        )
        return label, code
    raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _generated_gate_payloads(
    evidence: GeneratedEvidenceAggregate,
) -> tuple[tuple[str, object], ...]:
    return (
        ("schema_identity", evidence.schema_identity),
        ("bootstrap_path_ownership", evidence.bootstrap_path_ownership),
        ("runtime_connection_controls", evidence.runtime_connection_controls),
        ("projection_roundtrip", evidence.projection_roundtrip),
        ("schema_constraints_corruption", evidence.schema_constraints_corruption),
        ("atomicity_classification", evidence.atomicity_classification),
        ("fresh_process_faults", evidence.fresh_process_faults),
        ("bounded_queries", evidence.bounded_queries),
        ("closed_error_mapping", evidence.closed_error_mapping),
        ("backup_restore", evidence.backup_restore),
        ("generation_copy", evidence.generation_copy),
        ("workload_thresholds", evidence.workload_thresholds),
    )


def _expected_gate_token_roles(
    evidence: GeneratedEvidenceAggregate,
) -> tuple[tuple[tuple[str, bytes], ...], ...]:
    report_token = evidence.bootstrap_path_ownership.token
    backup = evidence.backup_restore
    generation_copy = evidence.generation_copy
    report_role = (("report", report_token._nonce),)
    return (
        report_role,
        report_role,
        report_role,
        report_role,
        report_role,
        report_role,
        report_role,
        report_role,
        report_role,
        (
            ("source", backup.source_token._nonce),
            ("backup", backup.backup_token._nonce),
            ("restore", backup.restore_token._nonce),
        ),
        (
            ("source", generation_copy.source_token._nonce),
            ("destination", generation_copy.destination_token._nonce),
        ),
        report_role,
    )


def seal_generated_evidence_run(
    run: _EvidenceRun,
    *,
    evidence: GeneratedEvidenceAggregate,
) -> _EvidenceReceipt:
    """Seal execution-backed gate payloads into one consume-on-success receipt."""

    ledger = _validated_evidence_run(run)
    if (
        not ledger.recording
        or ledger.receipt is not None
        or type(evidence) is not GeneratedEvidenceAggregate
        or _ACTIVE_EVIDENCE_RUN.get() is not run
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    bootstrap = evidence.bootstrap_path_ownership
    if type(bootstrap) is not BootstrapPathEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        registered = _require_token(bootstrap.token)
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if registered.pytest_registration is not run._pytest_registration:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if (
        type(bootstrap.rejections) is not RejectionEvidence
        or type(evidence.schema_constraints_corruption) is not RejectionEvidence
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    gate_payloads = _generated_gate_payloads(evidence)
    if tuple(name for name, _ in gate_payloads) != GENERATED_EVIDENCE_GATES:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if set(ledger.observations) != set(range(len(GENERATED_EVIDENCE_GATES))):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    gate_receipts_list: list[_GateEvidenceReceipt] = []
    seen_payload_identities: set[int] = set()
    expected_token_roles = _expected_gate_token_roles(evidence)
    for ordinal, (name, payload) in enumerate(gate_payloads):
        observation = ledger.observations[ordinal]
        if (
            observation.value is not payload
            or observation.payload_digest != _evidence_payload_digest(payload)
            or observation.registration is not run._pytest_registration
            or observation.run is not run
            or observation.process_id != os.getpid()
            or observation.producer.gate != name
            or observation.producer.ordinal != ordinal
            or observation.producer.name == ""
            or id(payload) in seen_payload_identities
            or observation.token_roles != expected_token_roles[ordinal]
            or observation.private_receipt_digest != _private_gate_receipt_digest(run, name)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        seen_payload_identities.add(id(payload))
        gate_receipts_list.append(
            _GateEvidenceReceipt(
                _constructor=_EVIDENCE_CAPABILITY_CONSTRUCTOR,
                gate=name,
                payload=payload,
                payload_digest=observation.payload_digest,
            )
        )
    gate_receipts = tuple(gate_receipts_list)
    receipt = _EvidenceReceipt(
        _constructor=_EVIDENCE_CAPABILITY_CONSTRUCTOR,
        run=run,
        evidence=evidence,
        evidence_digest=_evidence_payload_digest(evidence),
        gates=gate_receipts,
    )
    ledger.receipt = receipt
    ledger.recording = False
    _ACTIVE_EVIDENCE_RUN.set(None)
    return receipt


def _validate_evidence_receipt(
    pytest_root: Path,
    receipt: _EvidenceReceipt,
    evidence: GeneratedEvidenceAggregate,
    *,
    require_exact_evidence: bool,
) -> _EvidenceLedger:
    _validate_bootstrap_root(pytest_root)
    if (
        type(receipt) is not _EvidenceReceipt
        or receipt._constructor is not _EVIDENCE_CAPABILITY_CONSTRUCTOR
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    ledger = _validated_evidence_run(receipt.run)
    registration = receipt.run._pytest_registration
    gate_payloads = _generated_gate_payloads(evidence)
    if (
        registration.path_object is not pytest_root
        or ledger.receipt is not receipt
        or ledger.recording
        or ledger.closed
        or ledger.consumed
        or len(receipt.gates) != len(GENERATED_EVIDENCE_GATES)
        or tuple(gate.gate for gate in receipt.gates) != GENERATED_EVIDENCE_GATES
        or any(
            type(gate) is not _GateEvidenceReceipt
            or gate._constructor is not _EVIDENCE_CAPABILITY_CONSTRUCTOR
            for gate in receipt.gates
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if require_exact_evidence:
        if receipt.evidence is not evidence or receipt.evidence_digest != _evidence_payload_digest(
            evidence
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        for receipt_gate, (name, payload) in zip(
            receipt.gates,
            gate_payloads,
            strict=True,
        ):
            if (
                receipt_gate.gate != name
                or receipt_gate.payload is not payload
                or receipt_gate.payload_digest != _evidence_payload_digest(payload)
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return ledger


def _validate_private_generation(path: Path) -> None:
    try:
        details = path.lstat()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != 0o700
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)


def _identity_for(
    path: Path,
    *,
    pytest_registration: _ActivePytestRoot,
) -> _RegisteredIdentity:
    try:
        details = path.stat(follow_symlinks=False)
        generation_details = path.parent.stat(follow_symlinks=False)
        pytest_root_details = path.parents[1].stat(follow_symlinks=False)
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if (
        not stat.S_ISREG(details.st_mode)
        or details.st_nlink != 1
        or stat.S_IMODE(details.st_mode) != 0o600
        or not stat.S_ISDIR(generation_details.st_mode)
        or generation_details.st_uid != os.getuid()
        or stat.S_IMODE(generation_details.st_mode) != 0o700
        or not stat.S_ISDIR(pytest_root_details.st_mode)
        or pytest_root_details.st_uid != os.getuid()
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return _RegisteredIdentity(
        pytest_root=path.parents[1],
        pytest_registration=pytest_registration,
        generation_root=path.parent,
        database_path=path,
        pytest_root_device=pytest_root_details.st_dev,
        pytest_root_inode=pytest_root_details.st_ino,
        pytest_root_uid=pytest_root_details.st_uid,
        pytest_root_mode=stat.S_IMODE(pytest_root_details.st_mode),
        generation_device=generation_details.st_dev,
        generation_inode=generation_details.st_ino,
        generation_uid=generation_details.st_uid,
        generation_mode=stat.S_IMODE(generation_details.st_mode),
        device=details.st_dev,
        inode=details.st_ino,
        uid=details.st_uid,
        mode=stat.S_IMODE(details.st_mode),
        link_count=details.st_nlink,
    )


def _open_owned_generation(identity: _RegisteredIdentity) -> tuple[int, int]:
    """Open the registered root and generation without following a replacement alias."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    root_descriptor = -1
    generation_descriptor = -1
    try:
        root_descriptor = os.open(identity.pytest_root, flags)
        root_details = os.fstat(root_descriptor)
        if (
            root_details.st_dev != identity.pytest_root_device
            or root_details.st_ino != identity.pytest_root_inode
            or root_details.st_uid != identity.pytest_root_uid
            or stat.S_IMODE(root_details.st_mode) != identity.pytest_root_mode
            or not stat.S_ISDIR(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        generation_descriptor = os.open(
            identity.generation_root.name,
            flags,
            dir_fd=root_descriptor,
        )
        generation_details = os.fstat(generation_descriptor)
        generation_entry = os.stat(
            identity.generation_root.name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if (
            generation_details.st_dev != identity.generation_device
            or generation_details.st_ino != identity.generation_inode
            or generation_details.st_uid != identity.generation_uid
            or stat.S_IMODE(generation_details.st_mode) != identity.generation_mode
            or not stat.S_ISDIR(generation_details.st_mode)
            or generation_entry.st_dev != generation_details.st_dev
            or generation_entry.st_ino != generation_details.st_ino
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        database_details = os.stat(
            _DATABASE_BASENAME,
            dir_fd=generation_descriptor,
            follow_symlinks=False,
        )
        if (
            database_details.st_dev != identity.device
            or database_details.st_ino != identity.inode
            or database_details.st_uid != identity.uid
            or database_details.st_nlink != identity.link_count
            or stat.S_IMODE(database_details.st_mode) != identity.mode
            or not stat.S_ISREG(database_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return root_descriptor, generation_descriptor
    except HarnessFailure:
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
        raise
    except (OSError, RuntimeError):
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _close_operation_path_snapshot(snapshot: _OperationPathSnapshot | None) -> None:
    if snapshot is None:
        return
    for item in snapshot.files:
        with suppress(OSError):
            os.close(item.descriptor)
    with suppress(OSError):
        os.close(snapshot.generation_descriptor)
    with suppress(OSError):
        os.close(snapshot.root_descriptor)


def _open_operation_path_snapshot(
    identity: _RegisteredIdentity,
) -> _OperationPathSnapshot:
    """Pin the owned generation and reject every observed alias before SQLite opens it."""

    root_descriptor = -1
    generation_descriptor = -1
    pinned: list[_PinnedFile] = []
    try:
        root_descriptor, generation_descriptor = _open_owned_generation(identity)
        names = tuple(sorted(os.listdir(generation_descriptor)))
        if (
            _DATABASE_BASENAME not in names
            or set(names) - _OWNED_DATABASE_FILENAMES
            or len(names) != len(set(names))
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        flags = (
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        for name in names:
            descriptor = os.open(name, flags, dir_fd=generation_descriptor)
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_dev != identity.device
                or details.st_uid != identity.uid
                or details.st_nlink != 1
                or stat.S_IMODE(details.st_mode) != 0o600
                or (
                    name == _DATABASE_BASENAME
                    and (details.st_ino != identity.inode or details.st_dev != identity.device)
                )
            ):
                os.close(descriptor)
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            pinned.append(
                _PinnedFile(
                    name=name,
                    descriptor=descriptor,
                    device=details.st_dev,
                    inode=details.st_ino,
                    uid=details.st_uid,
                    mode=stat.S_IMODE(details.st_mode),
                    link_count=details.st_nlink,
                )
            )
        return _OperationPathSnapshot(
            root_descriptor=root_descriptor,
            generation_descriptor=generation_descriptor,
            files=tuple(pinned),
        )
    except HarnessFailure:
        for item in pinned:
            with suppress(OSError):
                os.close(item.descriptor)
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
        raise
    except (OSError, RuntimeError):
        for item in pinned:
            with suppress(OSError):
                os.close(item.descriptor)
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _revalidate_operation_path_snapshot(
    identity: _RegisteredIdentity,
    snapshot: _OperationPathSnapshot,
) -> None:
    """Require stable pre-existing files and safe newly created SQLite sidecars."""

    try:
        generation = os.fstat(snapshot.generation_descriptor)
        if (
            generation.st_dev != identity.generation_device
            or generation.st_ino != identity.generation_inode
            or generation.st_uid != identity.generation_uid
            or stat.S_IMODE(generation.st_mode) != identity.generation_mode
            or not stat.S_ISDIR(generation.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        names = tuple(sorted(os.listdir(snapshot.generation_descriptor)))
        if (
            _DATABASE_BASENAME not in names
            or set(names) - _OWNED_DATABASE_FILENAMES
            or len(names) != len(set(names))
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        prior = {item.name: item for item in snapshot.files}
        for name in names:
            details = os.stat(
                name,
                dir_fd=snapshot.generation_descriptor,
                follow_symlinks=False,
            )
            expected = prior.get(name)
            if expected is not None:
                pinned_details = os.fstat(expected.descriptor)
                if (
                    not stat.S_ISREG(pinned_details.st_mode)
                    or pinned_details.st_dev != expected.device
                    or pinned_details.st_ino != expected.inode
                    or pinned_details.st_uid != expected.uid
                    or stat.S_IMODE(pinned_details.st_mode) != expected.mode
                    or pinned_details.st_nlink != expected.link_count
                    or details.st_dev != pinned_details.st_dev
                    or details.st_ino != pinned_details.st_ino
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_dev != identity.device
                or details.st_uid != identity.uid
                or details.st_nlink != 1
                or stat.S_IMODE(details.st_mode) != 0o600
                or (
                    name == _DATABASE_BASENAME
                    and (details.st_ino != identity.inode or details.st_dev != identity.device)
                )
                or (
                    expected is not None
                    and (
                        details.st_dev != expected.device
                        or details.st_ino != expected.inode
                        or details.st_uid != expected.uid
                        or stat.S_IMODE(details.st_mode) != expected.mode
                        or details.st_nlink != expected.link_count
                    )
                )
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    except HarnessFailure:
        raise
    except (OSError, RuntimeError):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _owned_file_size(
    identity: _RegisteredIdentity,
    name: str,
    *,
    required: bool,
    maximum: int,
) -> int:
    root_descriptor = -1
    generation_descriptor = -1
    file_descriptor = -1
    try:
        root_descriptor, generation_descriptor = _open_owned_generation(identity)
        flags = (
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        try:
            file_descriptor = os.open(name, flags, dir_fd=generation_descriptor)
        except FileNotFoundError:
            if not required:
                return 0
            raise
        details = os.fstat(file_descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_uid != identity.uid
            or details.st_nlink != 1
            or stat.S_IMODE(details.st_mode) != 0o600
            or not 0 <= details.st_size <= maximum
            or (
                name == _DATABASE_BASENAME
                and (details.st_dev != identity.device or details.st_ino != identity.inode)
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return details.st_size
    except HarnessFailure:
        raise
    except (OSError, RuntimeError):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    finally:
        if file_descriptor >= 0:
            with suppress(OSError):
                os.close(file_descriptor)
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)


def _require_token(token: StoreToken) -> _RegisteredIdentity:
    if type(token) is not StoreToken or type(token._nonce) is not bytes:
        raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
    registered = _TOKEN_REGISTRY.get(token._nonce)
    if registered is None:
        raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
    active_root = registered.pytest_registration
    current_process_id = os.getpid()
    try:
        registration_revoked = active_root.revocation_flag[0] != 0
    except (IndexError, OSError, ValueError):
        registration_revoked = True
    if (
        registration_revoked
        or _ACTIVE_PYTEST_ROOTS.get(id(active_root.path_object)) is not active_root
        or active_root.resolved_path != registered.pytest_root
        or active_root.device != registered.pytest_root_device
        or active_root.inode != registered.pytest_root_inode
        or active_root.uid != registered.pytest_root_uid
        or active_root.mode != registered.pytest_root_mode
        or os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" (", maxsplit=1)[0]
        != active_root.node_id
        or (current_process_id != active_root.process_id and os.getppid() != active_root.process_id)
        or token._pytest_root != registered.pytest_root
        or token._generation_root != registered.generation_root
        or token._database_path != registered.database_path
        or token._device != registered.device
        or token._inode != registered.inode
        or token._uid != registered.uid
        or token._mode != registered.mode
        or token._link_count != registered.link_count
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
    _validate_private_generation(registered.generation_root)
    _walk_without_aliases(registered.generation_root, include_leaf=False)
    _walk_without_aliases(registered.database_path, include_leaf=True)
    observed = _identity_for(
        registered.database_path,
        pytest_registration=registered.pytest_registration,
    )
    if observed != registered:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return registered


def _database_uri(path: Path) -> str:
    uri = path.as_uri()
    if "?" in uri:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return f"{uri}?mode=rw"


def _snapshot_database_path(snapshot: _OperationPathSnapshot) -> Path:
    if os.name != "posix" or not Path("/proc/self/fd").is_dir():
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return Path(f"/proc/self/fd/{snapshot.generation_descriptor}/{_DATABASE_BASENAME}")


class _MeteredCursor(sqlite3.Cursor):
    """Cursor that contributes to an explicit test-only measurement scope."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection)
        if isinstance(connection, _MeteredConnection):
            connection._task064_register_cursor(self)
        self._task064_measurement = _ACTIVE_CURSOR_MEASUREMENT.get()
        self._task064_is_open = True
        if self._task064_measurement is not None:
            self._task064_measurement.current += 1
            self._task064_measurement.maximum = max(
                self._task064_measurement.maximum,
                self._task064_measurement.current,
            )

    def close(self) -> None:
        connection = self.connection
        try:
            super().close()
        finally:
            if self._task064_is_open:
                self._task064_is_open = False
                if isinstance(connection, _MeteredConnection):
                    connection._task064_release_cursor(self)
                if self._task064_measurement is not None:
                    self._task064_measurement.current -= 1


class _MeteredConnection(sqlite3.Connection):
    """Connection whose convenience execute path always uses a metered cursor."""

    def _task064_register_cursor(self, cursor: _MeteredCursor) -> None:
        registry = getattr(self, "_task064_open_cursors", None)
        if registry is None:
            registry = set()
            self._task064_open_cursors = registry
        registry.add(cursor)

    def _task064_release_cursor(self, cursor: _MeteredCursor) -> None:
        registry = getattr(self, "_task064_open_cursors", None)
        if registry is not None:
            registry.discard(cursor)

    def close(self) -> None:
        _CONNECTION_EVIDENCE_BINDINGS.pop(id(self), None)
        snapshot = _CONNECTION_PATH_SNAPSHOTS.pop(id(self), None)
        try:
            registry = tuple(getattr(self, "_task064_open_cursors", ()))
            for cursor in registry:
                with suppress(BaseException):
                    cursor.close()
            super().close()
        finally:
            _close_operation_path_snapshot(snapshot)

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
        cursor = _MeteredCursor(self)
        cursor.row_factory = self.row_factory
        try:
            return cursor.execute(sql, parameters)
        except BaseException:
            with suppress(BaseException):
                cursor.close()
            raise


@contextmanager
def measure_open_cursors() -> Iterator[CursorMeasurement]:
    """Measure every Python DB-API cursor opened in the current context."""

    measurement = CursorMeasurement()
    token = _ACTIVE_CURSOR_MEASUREMENT.set(measurement)
    try:
        yield measurement
    finally:
        _ACTIVE_CURSOR_MEASUREMENT.reset(token)
        if measurement.current != 0 and sys.exception() is None:
            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)


_DBCONFIG: Final = (
    ("SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION", False, True),
    ("SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER", False, True),
    ("SQLITE_DBCONFIG_DQS_DDL", False, True),
    ("SQLITE_DBCONFIG_DQS_DML", False, True),
    ("SQLITE_DBCONFIG_TRUSTED_SCHEMA", False, True),
    ("SQLITE_DBCONFIG_WRITABLE_SCHEMA", False, True),
    ("SQLITE_DBCONFIG_LEGACY_ALTER_TABLE", False, True),
    ("SQLITE_DBCONFIG_LEGACY_FILE_FORMAT", False, True),
    ("SQLITE_DBCONFIG_RESET_DATABASE", False, True),
    ("SQLITE_DBCONFIG_ENABLE_VIEW", False, True),
    ("SQLITE_DBCONFIG_DEFENSIVE", True, True),
    ("SQLITE_DBCONFIG_ENABLE_FKEY", True, True),
    ("SQLITE_DBCONFIG_ENABLE_TRIGGER", True, True),
    ("SQLITE_DBCONFIG_ENABLE_QPSG", True, True),
    ("SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE", True, True),
)
_LIMITS: Final = (
    ("SQLITE_LIMIT_ATTACHED", 0),
    ("SQLITE_LIMIT_LENGTH", 1_048_576),
    ("SQLITE_LIMIT_SQL_LENGTH", 262_144),
    ("SQLITE_LIMIT_COLUMN", 128),
    ("SQLITE_LIMIT_EXPR_DEPTH", 128),
    ("SQLITE_LIMIT_COMPOUND_SELECT", 1),
    ("SQLITE_LIMIT_FUNCTION_ARG", 32),
    ("SQLITE_LIMIT_LIKE_PATTERN_LENGTH", 128),
    ("SQLITE_LIMIT_TRIGGER_DEPTH", 8),
    ("SQLITE_LIMIT_VARIABLE_NUMBER", 64),
    ("SQLITE_LIMIT_VDBE_OP", 100_000),
    ("SQLITE_LIMIT_WORKER_THREADS", 0),
)
ACCEPTED_DBCONFIG: Final = tuple((name, enabled) for name, enabled, _ in _DBCONFIG)
ACCEPTED_LIMITS: Final = _LIMITS
ACCEPTED_COMMON_PRAGMAS: Final = (
    ("busy_timeout", 0),
    ("foreign_keys", 1),
    ("trusted_schema", 0),
    ("read_uncommitted", 0),
    ("recursive_triggers", 0),
    ("ignore_check_constraints", 0),
    ("cell_size_check", 1),
    ("mmap_size", 0),
    ("temp_store", 2),
    ("locking_mode", "normal"),
    ("max_page_count", MAX_PAGE_COUNT),
    ("wal_autocheckpoint", WAL_AUTOCHECKPOINT_PAGES),
)
ACCEPTED_WRITER_PRAGMAS: Final = (
    *ACCEPTED_COMMON_PRAGMAS,
    ("synchronous", 2),
    ("query_only", 0),
)
ACCEPTED_READER_PRAGMAS: Final = (
    *ACCEPTED_COMMON_PRAGMAS,
    ("synchronous", 2),
    ("query_only", 1),
)
ACCEPTED_PRAGMA_PROFILES: Final = (
    ("reader", ACCEPTED_READER_PRAGMAS),
    ("writer", ACCEPTED_WRITER_PRAGMAS),
)
ACCEPTED_CONNECTION_PROFILES: Final = (
    ConnectionControlProfile(
        role="reader",
        dbconfig=ACCEPTED_DBCONFIG,
        defensive_available=True,
        defensive_enabled=True,
        limits=ACCEPTED_LIMITS,
        pragmas=ACCEPTED_READER_PRAGMAS,
    ),
    ConnectionControlProfile(
        role="writer",
        dbconfig=ACCEPTED_DBCONFIG,
        defensive_available=True,
        defensive_enabled=True,
        limits=ACCEPTED_LIMITS,
        pragmas=ACCEPTED_WRITER_PRAGMAS,
    ),
)


def _apply_connection_controls(
    connection: sqlite3.Connection,
) -> tuple[tuple[tuple[str, bool], ...], bool, bool]:
    defensive_available = False
    defensive_enabled = False
    observed_dbconfig: list[tuple[str, bool]] = []
    for name, enabled, required in _DBCONFIG:
        option = getattr(sqlite3, name, None)
        if option is None:
            if required:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            continue
        try:
            connection.setconfig(option, enabled)
            observed = connection.getconfig(option)
        except (AttributeError, sqlite3.Error):
            if required:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            continue
        if observed is not enabled:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        observed_dbconfig.append((name, observed))
        if name == "SQLITE_DBCONFIG_DEFENSIVE":
            defensive_available = True
            defensive_enabled = observed
    try:
        connection.enable_load_extension(False)
    except (AttributeError, sqlite3.Error):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    for name, limit in _LIMITS:
        category = getattr(sqlite3, name, None)
        if category is None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        connection.setlimit(category, limit)
        if connection.getlimit(category) != limit:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    exact_dbconfig = tuple(observed_dbconfig)
    if exact_dbconfig != ACCEPTED_DBCONFIG:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return exact_dbconfig, defensive_available, defensive_enabled


_DENIED_AUTHORIZER_ACTIONS: Final = frozenset(
    action
    for action in (
        getattr(sqlite3, "SQLITE_ATTACH", None),
        getattr(sqlite3, "SQLITE_DETACH", None),
        getattr(sqlite3, "SQLITE_ALTER_TABLE", None),
        getattr(sqlite3, "SQLITE_ANALYZE", None),
        getattr(sqlite3, "SQLITE_CREATE_INDEX", None),
        getattr(sqlite3, "SQLITE_CREATE_TABLE", None),
        getattr(sqlite3, "SQLITE_CREATE_TEMP_INDEX", None),
        getattr(sqlite3, "SQLITE_CREATE_TEMP_TABLE", None),
        getattr(sqlite3, "SQLITE_CREATE_TEMP_TRIGGER", None),
        getattr(sqlite3, "SQLITE_CREATE_TEMP_VIEW", None),
        getattr(sqlite3, "SQLITE_CREATE_TRIGGER", None),
        getattr(sqlite3, "SQLITE_CREATE_VTABLE", None),
        getattr(sqlite3, "SQLITE_CREATE_VIEW", None),
        getattr(sqlite3, "SQLITE_DROP_INDEX", None),
        getattr(sqlite3, "SQLITE_DROP_TABLE", None),
        getattr(sqlite3, "SQLITE_DROP_TEMP_INDEX", None),
        getattr(sqlite3, "SQLITE_DROP_TEMP_TABLE", None),
        getattr(sqlite3, "SQLITE_DROP_TEMP_TRIGGER", None),
        getattr(sqlite3, "SQLITE_DROP_TEMP_VIEW", None),
        getattr(sqlite3, "SQLITE_DROP_TRIGGER", None),
        getattr(sqlite3, "SQLITE_DROP_VTABLE", None),
        getattr(sqlite3, "SQLITE_DROP_VIEW", None),
        getattr(sqlite3, "SQLITE_REINDEX", None),
    )
    if type(action) is int
)


def _operation_authorizer(
    action: int,
    first: str | None,
    second: str | None,
    _database: str | None,
    trigger: str | None,
) -> int:
    if first == "stream_tail_commit_guard" and action in {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
    }:
        permitted = (
            action == sqlite3.SQLITE_INSERT and trigger == "trg_history_transition_pending"
        ) or (action == sqlite3.SQLITE_DELETE and trigger == "trg_stream_transition_finalize")
        return sqlite3.SQLITE_OK if permitted else sqlite3.SQLITE_DENY
    if action in _DENIED_AUTHORIZER_ACTIONS:
        return sqlite3.SQLITE_DENY
    if (
        action == sqlite3.SQLITE_PRAGMA
        and first
        in {
            "writable_schema",
            "trusted_schema",
            "foreign_keys",
            "journal_mode",
        }
        and second is not None
    ):
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_FUNCTION and second == "load_extension":
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _pragma_scalar(connection: sqlite3.Connection, pragma: str) -> object:
    row = _fetch_one(connection, f"PRAGMA {pragma}")
    return row[0]


def _connect(
    token: StoreToken,
    *,
    writer: bool,
    bootstrap: bool = False,
) -> tuple[sqlite3.Connection, RuntimeProfile]:
    registered = _require_token(token)
    snapshot = _open_operation_path_snapshot(registered)
    try:
        connection = sqlite3.connect(
            _database_uri(_snapshot_database_path(snapshot)),
            uri=True,
            autocommit=True,
            timeout=0.0,
            detect_types=0,
            check_same_thread=True,
            cached_statements=0,
            factory=_MeteredConnection,
        )
    except sqlite3.Error:
        _close_operation_path_snapshot(snapshot)
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except BaseException:
        _close_operation_path_snapshot(snapshot)
        raise
    _CONNECTION_PATH_SNAPSHOTS[id(connection)] = snapshot
    connection.row_factory = sqlite3.Row
    try:
        observed_dbconfig, defensive_available, defensive_enabled = _apply_connection_controls(
            connection
        )
        connection.execute("PRAGMA busy_timeout = 0").close()
        connection.execute("PRAGMA foreign_keys = ON").close()
        connection.execute("PRAGMA trusted_schema = OFF").close()
        connection.execute("PRAGMA read_uncommitted = OFF").close()
        connection.execute("PRAGMA recursive_triggers = OFF").close()
        connection.execute("PRAGMA ignore_check_constraints = OFF").close()
        connection.execute("PRAGMA cell_size_check = ON").close()
        connection.execute("PRAGMA mmap_size = 0").close()
        connection.execute("PRAGMA temp_store = MEMORY").close()
        connection.execute("PRAGMA cache_size = -8192").close()
        connection.execute("PRAGMA locking_mode = NORMAL").close()
        connection.execute(f"PRAGMA max_page_count = {MAX_PAGE_COUNT}").close()
        connection.execute(f"PRAGMA wal_autocheckpoint = {WAL_AUTOCHECKPOINT_PAGES}").close()
        connection.execute(
            "PRAGMA synchronous = FULL" if writer else "PRAGMA query_only = ON"
        ).close()
        source_id = cast(str, _fetch_one(connection, "SELECT sqlite_source_id()")[0])
        python_version = ".".join(str(part) for part in sys.version_info[:3])
        compile_options = tuple(
            sorted(cast(str, row[0]) for row in _fetch_all(connection, "PRAGMA compile_options"))
        )
        if (
            python_version != ACCEPTED_PYTHON_VERSION
            or sqlite3.sqlite_version != ACCEPTED_SQLITE_VERSION
            or sqlite3.threadsafety != ACCEPTED_THREADSAFETY
            or source_id != ACCEPTED_SQLITE_SOURCE_ID
            or compile_options != ACCEPTED_COMPILE_OPTIONS
            or "THREADSAFE=0" in compile_options
            or any(
                option in compile_options
                for option in ("OMIT_FOREIGN_KEY", "OMIT_TRIGGER", "OMIT_AUTHORIZATION")
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        database_list = _fetch_all(connection, "PRAGMA database_list")
        if len(database_list) != 1 or database_list[0]["name"] != "main":
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        observed_path = _resolve_existing(
            Path(cast(str, database_list[0]["file"])),
            code=HarnessFailureCode.UNAVAILABLE,
        )
        if observed_path != _resolve_existing(
            registered.database_path,
            code=HarnessFailureCode.UNAVAILABLE,
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if _require_token(token) != registered:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        observed_common_pragmas: tuple[tuple[str, str | int], ...] = (
            ("busy_timeout", cast(int, _pragma_scalar(connection, "busy_timeout"))),
            ("foreign_keys", cast(int, _pragma_scalar(connection, "foreign_keys"))),
            ("trusted_schema", cast(int, _pragma_scalar(connection, "trusted_schema"))),
            ("read_uncommitted", cast(int, _pragma_scalar(connection, "read_uncommitted"))),
            ("recursive_triggers", cast(int, _pragma_scalar(connection, "recursive_triggers"))),
            (
                "ignore_check_constraints",
                cast(int, _pragma_scalar(connection, "ignore_check_constraints")),
            ),
            ("cell_size_check", cast(int, _pragma_scalar(connection, "cell_size_check"))),
            ("mmap_size", cast(int, _pragma_scalar(connection, "mmap_size"))),
            ("temp_store", cast(int, _pragma_scalar(connection, "temp_store"))),
            ("locking_mode", cast(str, _pragma_scalar(connection, "locking_mode"))),
            ("max_page_count", cast(int, _pragma_scalar(connection, "max_page_count"))),
            (
                "wal_autocheckpoint",
                cast(int, _pragma_scalar(connection, "wal_autocheckpoint")),
            ),
        )
        observed_pragmas = (
            *observed_common_pragmas,
            ("synchronous", cast(int, _pragma_scalar(connection, "synchronous"))),
            ("query_only", cast(int, _pragma_scalar(connection, "query_only"))),
        )
        expected_pragmas = ACCEPTED_WRITER_PRAGMAS if writer else ACCEPTED_READER_PRAGMAS
        if observed_pragmas != expected_pragmas:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if not bootstrap:
            application_id = cast(int, _pragma_scalar(connection, "application_id"))
            user_version = cast(int, _pragma_scalar(connection, "user_version"))
            if application_id == APPLICATION_ID and 2 <= user_version <= (2**31) - 1:
                raise HarnessFailure(HarnessFailureCode.UNSUPPORTED_VERSION)
            if (
                application_id != APPLICATION_ID
                or user_version != USER_VERSION
                or cast(int, _pragma_scalar(connection, "page_size")) != PAGE_SIZE
                or cast(str, _pragma_scalar(connection, "journal_mode")).lower() != "wal"
                or cast(str, _pragma_scalar(connection, "encoding")) != "UTF-8"
                or cast(int, _pragma_scalar(connection, "auto_vacuum")) != 0
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        connection.set_authorizer(_operation_authorizer)
        _revalidate_operation_path_snapshot(registered, snapshot)
        profile = RuntimeProfile(
            role="writer" if writer else "reader",
            python_version=python_version,
            sqlite_version=sqlite3.sqlite_version,
            sqlite_source_id=source_id,
            threadsafety=sqlite3.threadsafety,
            compile_options=compile_options,
            dbconfig=observed_dbconfig,
            defensive_available=defensive_available,
            defensive_enabled=defensive_enabled,
            pragmas=observed_pragmas,
            limits=tuple(
                (name, connection.getlimit(cast(int, getattr(sqlite3, name))))
                for name, _ in _LIMITS
            ),
        )
        _CONNECTION_EVIDENCE_BINDINGS[id(connection)] = (token._nonce, writer)
        return connection, profile
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _close_best_effort(connection)
        raise failure from None
    except MemoryError:
        _close_best_effort(connection)
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except BaseException:
        _close_best_effort(connection)
        raise


def _connection_control_profile(profile: RuntimeProfile) -> ConnectionControlProfile:
    return ConnectionControlProfile(
        role=profile.role,
        dbconfig=profile.dbconfig,
        defensive_available=profile.defensive_available,
        defensive_enabled=profile.defensive_enabled,
        limits=profile.limits,
        pragmas=profile.pragmas,
    )


def _schema_statements() -> tuple[str, ...]:
    try:
        text = SCHEMA_PATH.read_text(encoding="utf-8")
    except OSError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if "\r" in text or "\x00" in text:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    statements: list[str] = []
    pending: list[str] = []
    for line in text.splitlines(keepends=True):
        pending.append(line)
        candidate = "".join(pending)
        if sqlite3.complete_statement(candidate):
            statement = candidate.strip()
            if statement:
                statements.append(statement)
            pending.clear()
    if "".join(pending).strip():
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return tuple(statements)


def _remove_unregistered_generation(
    generation_root: Path,
    database_path: Path,
) -> None:
    """Best-effort rollback for an internally named generation before token registration."""

    if (
        database_path.parent != generation_root
        or database_path.name != _DATABASE_BASENAME
        or not generation_root.name.startswith(_GENERATION_PREFIX)
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    permitted_names = {
        _DATABASE_BASENAME,
        f"{_DATABASE_BASENAME}-wal",
        f"{_DATABASE_BASENAME}-shm",
    }
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    root_descriptor = -1
    generation_descriptor = -1
    try:
        root_descriptor = os.open(generation_root.parent, flags)
        generation_descriptor = os.open(
            generation_root.name,
            flags,
            dir_fd=root_descriptor,
        )
        generation_details = os.fstat(generation_descriptor)
        generation_entry = os.stat(
            generation_root.name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        names = set(os.listdir(generation_descriptor))
        if (
            not stat.S_ISDIR(generation_details.st_mode)
            or generation_details.st_uid != os.getuid()
            or stat.S_IMODE(generation_details.st_mode) != 0o700
            or generation_entry.st_dev != generation_details.st_dev
            or generation_entry.st_ino != generation_details.st_ino
            or names - permitted_names
        ):
            return
        for name in names:
            details = os.stat(
                name,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
            if not (stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode)):
                return
        for name in sorted(names):
            os.unlink(name, dir_fd=generation_descriptor)
        os.rmdir(generation_root.name, dir_fd=root_descriptor)
    except (OSError, RuntimeError):
        return
    finally:
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)


def _remove_owned_files(token: StoreToken) -> None:
    registered = _TOKEN_REGISTRY.pop(token._nonce, None)
    if registered is None:
        return
    root_descriptor = -1
    generation_descriptor = -1
    permitted_names = {
        _DATABASE_BASENAME,
        f"{_DATABASE_BASENAME}-wal",
        f"{_DATABASE_BASENAME}-shm",
    }
    try:
        root_descriptor, generation_descriptor = _open_owned_generation(registered)
        names = set(os.listdir(generation_descriptor))
        if _DATABASE_BASENAME not in names or names - permitted_names:
            return
        for name in names:
            details = os.stat(
                name,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_uid != registered.uid
                or details.st_nlink != 1
                or stat.S_IMODE(details.st_mode) != 0o600
            ):
                return
        for name in sorted(names):
            os.unlink(name, dir_fd=generation_descriptor)
        os.rmdir(registered.generation_root.name, dir_fd=root_descriptor)
    except BaseException:
        return
    finally:
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)


@_operation_evidence_executor
def bootstrap_store(pytest_root: Path) -> StoreToken:
    """Create one empty, private, same-bootstrap-owned version-one generation."""

    root = _validate_bootstrap_root(pytest_root)
    active_root = _ACTIVE_PYTEST_ROOTS[id(pytest_root)]
    generation_name = _next_generation_name()
    generation_root = root / generation_name
    database_path = generation_root / _DATABASE_BASENAME
    generation_created = False
    root_descriptor = -1
    generation_descriptor = -1
    database_descriptor = -1
    try:
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        root_descriptor = os.open(root, directory_flags)
        root_details = os.fstat(root_descriptor)
        if (
            root_details.st_dev != active_root.device
            or root_details.st_ino != active_root.inode
            or root_details.st_uid != active_root.uid
            or stat.S_IMODE(root_details.st_mode) != active_root.mode
            or not stat.S_ISDIR(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        os.mkdir(generation_name, mode=0o700, dir_fd=root_descriptor)
        generation_created = True
        generation_descriptor = os.open(
            generation_name,
            directory_flags,
            dir_fd=root_descriptor,
        )
        os.fchmod(generation_descriptor, 0o700)
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        database_descriptor = os.open(
            _DATABASE_BASENAME,
            flags,
            0o600,
            dir_fd=generation_descriptor,
        )
        os.fchmod(database_descriptor, 0o600)
    except HarnessFailure:
        if generation_created:
            _remove_unregistered_generation(generation_root, database_path)
        raise
    except OSError:
        if generation_created:
            _remove_unregistered_generation(generation_root, database_path)
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    finally:
        if database_descriptor >= 0:
            with suppress(OSError):
                os.close(database_descriptor)
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
    try:
        registered = _identity_for(
            database_path,
            pytest_registration=active_root,
        )
        if (
            registered.pytest_root != root
            or registered.generation_root != generation_root
            or registered.uid != os.getuid()
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    except BaseException:
        _remove_unregistered_generation(generation_root, database_path)
        raise
    nonce = secrets.token_bytes(32)
    _TOKEN_REGISTRY[nonce] = registered
    token = StoreToken(
        _nonce=nonce,
        _pytest_root=registered.pytest_root,
        _generation_root=registered.generation_root,
        _database_path=registered.database_path,
        _device=registered.device,
        _inode=registered.inode,
        _uid=registered.uid,
        _mode=registered.mode,
        _link_count=registered.link_count,
    )
    try:
        connection, _ = _connect(token, writer=True, bootstrap=True)
        try:
            connection.set_authorizer(None)
            connection.execute(f"PRAGMA page_size = {PAGE_SIZE}").close()
            connection.execute("PRAGMA auto_vacuum = NONE").close()
            journal_mode = cast(
                str,
                _fetch_one(connection, "PRAGMA journal_mode = WAL")[0],
            )
            if journal_mode.lower() != "wal":
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            connection.execute("PRAGMA synchronous = FULL").close()
            connection.execute(f"PRAGMA application_id = {APPLICATION_ID}").close()
            connection.execute(f"PRAGMA user_version = {USER_VERSION}").close()
            connection.execute(f"PRAGMA max_page_count = {MAX_PAGE_COUNT}").close()
            connection.execute(f"PRAGMA wal_autocheckpoint = {WAL_AUTOCHECKPOINT_PAGES}").close()
            connection.execute("BEGIN IMMEDIATE").close()
            try:
                for statement in _schema_statements():
                    connection.execute(statement).close()
                live_descriptor = installed_schema_descriptor(connection)
                expected_descriptor = load_schema_descriptor()
                if live_descriptor != expected_descriptor:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                fingerprint = load_schema_fingerprint()
                connection.execute(
                    """
                    INSERT INTO stream_store_metadata (
                        singleton_key,
                        storage_marker,
                        physical_format_version,
                        schema_generation,
                        natural_identity_key_version,
                        page_size,
                        schema_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1,
                        STORAGE_MARKER,
                        USER_VERSION,
                        SCHEMA_GENERATION,
                        NATURAL_IDENTITY_KEY_VERSION,
                        PAGE_SIZE,
                        _digest_bytes(fingerprint),
                    ),
                ).close()
                _verify_operation_authority(connection, token)
                connection.execute("COMMIT").close()
            except BaseException:
                _rollback_best_effort(connection)
                raise
        finally:
            _close_preserving_primary(connection)
        verify_store(token)
        return token
    except BaseException:
        _remove_owned_files(token)
        raise


def provisional_schema_descriptor(pytest_root: Path) -> dict[str, object]:
    """Build the descriptor through the same owned bootstrap boundary for fixture generation."""

    token = bootstrap_store(pytest_root)
    try:
        connection, _ = _connect(token, writer=False)
        try:
            _verify_operation_snapshot(connection, token, writer=False)
            descriptor = installed_schema_descriptor(connection)
            _verify_operation_authority(connection, token)
            return descriptor
        finally:
            _close_preserving_primary(connection)
    finally:
        _remove_owned_files(token)


def _verify_schema_identity(connection: sqlite3.Connection) -> str:
    metadata = _fetch_one(
        connection,
        """
        SELECT
            singleton_key,
            storage_marker,
            physical_format_version,
            schema_generation,
            natural_identity_key_version,
            page_size,
            schema_fingerprint
        FROM stream_store_metadata
        """,
    )
    fingerprint = load_schema_fingerprint()
    if (
        tuple(metadata)
        != (
            1,
            STORAGE_MARKER,
            USER_VERSION,
            SCHEMA_GENERATION,
            NATURAL_IDENTITY_KEY_VERSION,
            PAGE_SIZE,
            _digest_bytes(fingerprint),
        )
        or installed_schema_descriptor(connection) != load_schema_descriptor()
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return fingerprint


def _verify_operation_authority(
    connection: sqlite3.Connection,
    token: StoreToken,
) -> _RegisteredIdentity:
    """Recheck active token authority and every pinned path identity."""

    registered = _require_token(token)
    snapshot = _CONNECTION_PATH_SNAPSHOTS.get(id(connection))
    if snapshot is None:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    _revalidate_operation_path_snapshot(registered, snapshot)
    return registered


def _operation_file_size(
    connection: sqlite3.Connection,
    token: StoreToken,
    name: str,
    *,
    required: bool,
    maximum: int,
) -> int:
    """Inspect a live SQLite-family file without opening or closing a lock-bearing fd."""

    if name not in _OWNED_DATABASE_FILENAMES:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    registered = _verify_operation_authority(connection, token)
    snapshot = _CONNECTION_PATH_SNAPSHOTS.get(id(connection))
    if snapshot is None:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    try:
        details = os.stat(
            name,
            dir_fd=snapshot.generation_descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        if not required:
            return 0
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if (
        not stat.S_ISREG(details.st_mode)
        or details.st_dev != registered.device
        or details.st_uid != registered.uid
        or details.st_nlink != 1
        or stat.S_IMODE(details.st_mode) != 0o600
        or not 0 <= details.st_size <= maximum
        or (
            name == _DATABASE_BASENAME
            and (details.st_ino != registered.inode or details.st_dev != registered.device)
        )
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    _verify_operation_authority(connection, token)
    return details.st_size


def _verify_operation_snapshot(
    connection: sqlite3.Connection,
    token: StoreToken,
    *,
    writer: bool,
) -> str:
    """Repeat every operation-critical identity/control check coherently."""

    registered = _verify_operation_authority(connection, token)
    database_list = _fetch_all(connection, "PRAGMA database_list")
    if (
        len(database_list) != 1
        or database_list[0]["name"] != "main"
        or _resolve_existing(
            Path(cast(str, database_list[0]["file"])),
            code=HarnessFailureCode.UNAVAILABLE,
        )
        != _resolve_existing(
            registered.database_path,
            code=HarnessFailureCode.UNAVAILABLE,
        )
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    source_id = cast(str, _fetch_one(connection, "SELECT sqlite_source_id()")[0])
    compile_options = tuple(
        sorted(cast(str, row[0]) for row in _fetch_all(connection, "PRAGMA compile_options"))
    )
    if (
        ".".join(str(part) for part in sys.version_info[:3]) != ACCEPTED_PYTHON_VERSION
        or sqlite3.sqlite_version != ACCEPTED_SQLITE_VERSION
        or source_id != ACCEPTED_SQLITE_SOURCE_ID
        or compile_options != ACCEPTED_COMPILE_OPTIONS
        or sqlite3.threadsafety != ACCEPTED_THREADSAFETY
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    for name, enabled, required in _DBCONFIG:
        option = getattr(sqlite3, name, None)
        if option is None:
            if required:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            continue
        if connection.getconfig(option) is not enabled:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    for name, expected in _LIMITS:
        category = getattr(sqlite3, name, None)
        if type(category) is not int or connection.getlimit(category) != expected:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    format_pragmas: tuple[tuple[str, object], ...] = (
        ("application_id", APPLICATION_ID),
        ("user_version", USER_VERSION),
        ("page_size", PAGE_SIZE),
        ("journal_mode", "wal"),
        ("encoding", "UTF-8"),
        ("auto_vacuum", 0),
    )
    control_pragmas: tuple[tuple[str, object], ...] = (
        ("busy_timeout", 0),
        ("foreign_keys", 1),
        ("trusted_schema", 0),
        ("read_uncommitted", 0),
        ("recursive_triggers", 0),
        ("ignore_check_constraints", 0),
        ("cell_size_check", 1),
        ("mmap_size", 0),
        ("temp_store", 2),
        ("locking_mode", "normal"),
        ("max_page_count", MAX_PAGE_COUNT),
        ("wal_autocheckpoint", WAL_AUTOCHECKPOINT_PAGES),
        ("synchronous", 2),
        ("query_only", 0 if writer else 1),
    )
    for pragma, expected_value in format_pragmas:
        observed = _pragma_scalar(connection, pragma)
        if pragma in {"journal_mode", "locking_mode"} and type(observed) is str:
            observed = observed.lower()
        if observed != expected_value:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for pragma, expected_value in control_pragmas:
        observed = _pragma_scalar(connection, pragma)
        if pragma == "locking_mode" and type(observed) is str:
            observed = observed.lower()
        if observed != expected_value:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if _fetch_all(
        connection,
        "SELECT 1 FROM stream_tail_commit_guard LIMIT 1",
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    fingerprint = _verify_schema_identity(connection)
    _verify_operation_authority(connection, token)
    return fingerprint


def _verify_integrity(connection: sqlite3.Connection) -> None:
    integrity = _fetch_all(connection, "PRAGMA integrity_check")
    if len(integrity) != 1 or integrity[0][0] != "ok":
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if _fetch_all(connection, "PRAGMA foreign_key_check"):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


@_operation_evidence_executor
def verify_store(token: StoreToken) -> VerificationSummary:
    """Fresh-open format, schema, integrity, FK, and full-history verification."""

    connection, profile = _connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        try:
            fingerprint = _verify_operation_snapshot(
                connection,
                token,
                writer=False,
            )
            _verify_integrity(connection)
            page_count = _require_exact_int(
                _pragma_scalar(connection, "page_count"),
                minimum=1,
                maximum=MAX_PAGE_COUNT,
            )
            maximum_materialized_rows = page_count * PAGE_SIZE
            stream_count = 0
            history_count = 0
            after_stream_row_id = 0
            while True:
                streams = _fetch_all(
                    connection,
                    """
                    SELECT *
                    FROM continuous_public_trade_stream
                    WHERE stream_row_id > ?
                    ORDER BY stream_row_id
                    LIMIT 100
                    """,
                    (after_stream_row_id,),
                )
                if not streams:
                    break
                if len(streams) > 100:
                    raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
                for stream in streams:
                    stream_row_id = _require_exact_int(
                        stream["stream_row_id"],
                        minimum=after_stream_row_id + 1,
                        maximum=MAX_CONTRACT_INTEGER,
                    )
                    after_stream_row_id = stream_row_id
                    stream_count += 1
                    if stream_count + history_count > maximum_materialized_rows:
                        raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
                    current_version = _require_exact_int(
                        stream["current_version"],
                        minimum=1,
                        maximum=MAX_CONTRACT_INTEGER,
                    )
                    next_version = 1
                    observed_history = 0
                    preceding_row: sqlite3.Row | None = None
                    current_entry: ContinuousPublicTradeStreamStoredHistoryEntryV1 | None = None
                    while next_version <= current_version:
                        page = _fetch_all(
                            connection,
                            """
                            SELECT *
                            FROM continuous_public_trade_history
                            INDEXED BY ux_cpt_history_stream_version
                            WHERE stream_row_id = ?
                              AND successor_version >= ?
                            ORDER BY successor_version
                            LIMIT 100
                            """,
                            (stream_row_id, next_version),
                        )
                        if (
                            not page
                            or len(page) > 100
                            or page[0]["successor_version"] != next_version
                        ):
                            raise HarnessFailure(HarnessFailureCode.CORRUPT)
                        entries = _validate_history_page_rows(
                            stream,
                            page,
                            preceding_row=preceding_row,
                        )
                        last_version = _require_exact_int(
                            page[-1]["successor_version"],
                            minimum=next_version,
                            maximum=MAX_CONTRACT_INTEGER,
                        )
                        if last_version > current_version:
                            raise HarnessFailure(HarnessFailureCode.CORRUPT)
                        if next_version == 1:
                            creation = entries[0]
                            if type(creation) is not ContinuousPublicTradeStreamStoredCreationV1:
                                raise HarnessFailure(HarnessFailureCode.CORRUPT)
                            _validate_stream_projection(stream, creation)
                        observed_history += len(page)
                        history_count += len(page)
                        if stream_count + history_count > maximum_materialized_rows:
                            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
                        preceding_row = page[-1]
                        current_entry = entries[-1]
                        next_version = last_version + 1
                        if last_version < current_version and len(page) < 100:
                            raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    if observed_history != current_version or current_entry is None:
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    if observed_history % 100 == 0 and _fetch_all(
                        connection,
                        """
                        SELECT successor_version
                        FROM continuous_public_trade_history
                        INDEXED BY ux_cpt_history_stream_version
                        WHERE stream_row_id = ?
                          AND successor_version >= ?
                        ORDER BY successor_version
                        LIMIT 100
                        """,
                        (stream_row_id, current_version + 1),
                    ):
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    if (
                        stream["current_version"] != current_entry.record.successor_version
                        or stream["current_record_canonical_bytes"] != current_entry.canonical_bytes
                        or _decode_digest(stream["current_record_digest"])
                        != current_entry.record_digest
                        or stream["current_envelope_canonical_bytes"]
                        != current_entry.successor_envelope.canonical_bytes
                        or _decode_digest(stream["current_envelope_digest"])
                        != current_entry.successor_envelope.envelope_digest
                        or _decode_digest(stream["current_history_root"])
                        != current_entry.history_root
                    ):
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if len(streams) < 100:
                    break
            freelist_count = _require_exact_int(
                _pragma_scalar(connection, "freelist_count"),
                minimum=0,
                maximum=page_count,
            )
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
        except BaseException:
            _rollback_best_effort(connection)
            raise
    finally:
        _close_preserving_primary(connection)
    writer_connection, writer_profile = _connect(token, writer=True)
    try:
        writer_connection.execute("BEGIN IMMEDIATE").close()
        _verify_operation_snapshot(
            writer_connection,
            token,
            writer=True,
        )
        _verify_operation_authority(writer_connection, token)
        writer_connection.execute("ROLLBACK").close()
    except BaseException:
        _rollback_best_effort(writer_connection)
        raise
    finally:
        _close_preserving_primary(writer_connection)
    connection_profiles = (
        _connection_control_profile(profile),
        _connection_control_profile(writer_profile),
    )
    if tuple(item.role for item in connection_profiles) != ("reader", "writer"):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    identity = _require_token(token)
    database_bytes = _owned_file_size(
        identity,
        _DATABASE_BASENAME,
        required=True,
        maximum=MAX_TEST_DATABASE_BYTES,
    )
    wal_bytes = _owned_file_size(
        identity,
        f"{_DATABASE_BASENAME}-wal",
        required=False,
        maximum=MAX_TEST_WAL_BYTES,
    )
    if _require_token(token) != identity:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return VerificationSummary(
        profile=profile,
        connection_profiles=connection_profiles,
        stream_count=stream_count,
        history_count=history_count,
        page_count=page_count,
        freelist_count=freelist_count,
        database_bytes=database_bytes,
        wal_bytes=wal_bytes,
        schema_fingerprint=fingerprint,
    )


def _stored_creation_without_scope(
    *,
    creation: ContinuousPublicTradeStreamCreationRecordV1,
    record_bytes: bytes,
    record_digest: str,
    envelope_bytes: bytes,
    envelope_digest: str,
    history_root: str,
) -> ContinuousPublicTradeStreamStoredCreationV1:
    """Construct a validated value with its deterministic scope supplied by the record."""

    from wealth.domain.continuous_public_trade_persistence import (
        ContinuousPublicTradeEvidenceKind,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    from wealth.ports.continuous_public_trade_stream_store import (
        ContinuousPublicTradeStreamStoredEnvelopeV1,
    )

    scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_CREATE_AUTHORITY,
        stream_id=creation.stream_id,
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
        stream_policy=creation.stream_policy,
    )
    return ContinuousPublicTradeStreamStoredCreationV1(
        record=creation,
        canonical_bytes=record_bytes,
        record_digest=record_digest,
        successor_envelope=ContinuousPublicTradeStreamStoredEnvelopeV1(
            envelope=decode_stream_envelope(envelope_bytes),
            canonical_bytes=envelope_bytes,
            envelope_digest=envelope_digest,
        ),
        history_root=history_root,
        create_authority_scope=scope,
    )


def _stored_transition_without_scopes(
    *,
    transition: ContinuousPublicTradeStreamTransitionRecordV1,
    record_bytes: bytes,
    record_digest: str,
    envelope_bytes: bytes,
    envelope_digest: str,
    history_root: str,
    predecessor_record_bytes: bytes,
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    """Construct a validated value with deterministic transition scopes."""

    from wealth.domain.continuous_public_trade_persistence import (
        ContinuousPublicTradeEvidenceKind,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    from wealth.ports.continuous_public_trade_stream_store import (
        ContinuousPublicTradeStreamStoredEnvelopeV1,
    )

    envelope = decode_stream_envelope(envelope_bytes)
    attachment = envelope.checkpoint.attachment
    payload = envelope.child_creation_payload
    is_attach = transition.transition_kind.value == "ATTACH"
    authority_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=transition.stream_id,
        transition_kind=transition.transition_kind,
        prior_version=transition.prior_version,
        prior_envelope_digest=transition.prior_envelope_digest,
        prior_history_root=transition.prior_history_root,
        successor_version=transition.successor_version,
        successor_envelope_digest=None if is_attach else envelope_digest,
        child_job_id=attachment.job_id if is_attach and attachment is not None else None,
        child_policy_fingerprint=(
            payload.child_checkpoint.policy_fingerprint
            if is_attach and payload is not None
            else None
        ),
        child_creation_fingerprint=None,
        reason_code=transition.reason_code,
        stream_policy=None,
    )
    completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None
    if transition.transition_kind.value == "CHILD_COMPLETED":
        if transition.prior_version == 1:
            predecessor_envelope_hex = decode_stream_creation_record(
                predecessor_record_bytes
            ).successor_envelope_hex
        else:
            predecessor_envelope_hex = decode_stream_transition_record(
                predecessor_record_bytes
            ).successor_envelope_hex
        predecessor_envelope = decode_stream_envelope(bytes.fromhex(predecessor_envelope_hex))
        predecessor_attachment = predecessor_envelope.checkpoint.attachment
        predecessor_payload = predecessor_envelope.child_creation_payload
        if predecessor_attachment is None or predecessor_payload is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        completion_scope = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
            stream_id=transition.stream_id,
            transition_kind=transition.transition_kind,
            prior_version=transition.prior_version,
            prior_envelope_digest=transition.prior_envelope_digest,
            prior_history_root=transition.prior_history_root,
            successor_version=transition.successor_version,
            successor_envelope_digest=envelope_digest,
            child_job_id=predecessor_attachment.job_id,
            child_policy_fingerprint=(predecessor_payload.child_checkpoint.policy_fingerprint),
            child_creation_fingerprint=predecessor_attachment.creation_fingerprint,
            reason_code=None,
            stream_policy=None,
        )
    return ContinuousPublicTradeStreamStoredTransitionV1(
        record=transition,
        canonical_bytes=record_bytes,
        record_digest=record_digest,
        successor_envelope=ContinuousPublicTradeStreamStoredEnvelopeV1(
            envelope=envelope,
            canonical_bytes=envelope_bytes,
            envelope_digest=envelope_digest,
        ),
        history_root=history_root,
        transition_authority_scope=authority_scope,
        child_completion_scope=completion_scope,
    )


def _exact_blob(value: object, *, minimum: int = 1, maximum: int) -> bytes:
    if type(value) is not bytes or not minimum <= len(value) <= maximum:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return value


def _entry_from_history_row_unchecked(
    row: sqlite3.Row,
    *,
    policy: ContinuousPublicTradePolicy,
    predecessor: sqlite3.Row | None = None,
) -> ContinuousPublicTradeStreamStoredHistoryEntryV1:
    """Decode and revalidate one retained history row without trusting projections."""

    _require_exact_int(row["stream_row_id"], minimum=1, maximum=MAX_CONTRACT_INTEGER)
    successor_version = _require_exact_int(
        row["successor_version"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    if row["record_model_version"] != _MODEL_VERSION or row["serialization_version"] != 1:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    record_bytes = _exact_blob(row["record_canonical_bytes"], maximum=65_536)
    envelope_bytes = _exact_blob(
        row["successor_envelope_canonical_bytes"],
        maximum=16_384,
    )
    envelope = decode_stream_envelope(envelope_bytes)
    envelope_digest = stream_envelope_digest(envelope)
    if (
        encode_stream_envelope(envelope) != envelope_bytes
        or _decode_digest(row["successor_envelope_digest"]) != envelope_digest
        or envelope.checkpoint.version != successor_version
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    if successor_version == 1:
        creation = decode_stream_creation_record(record_bytes)
        record_digest = stream_creation_digest(creation)
        history_root = initial_stream_history_root(creation)
        if (
            row["entry_kind"] != _ENTRY_CREATION
            or row["prior_version"] is not None
            or row["prior_envelope_digest"] is not None
            or row["prior_history_root"] is not None
            or row["predecessor_record_canonical_bytes"] is not None
            or row["predecessor_record_digest"] is not None
            or encode_stream_creation_record(creation) != record_bytes
            or creation.successor_version != 1
            or creation.successor_envelope_hex != envelope_bytes.hex()
            or creation.successor_envelope_digest != envelope_digest
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entry: ContinuousPublicTradeStreamStoredHistoryEntryV1 = _stored_creation_without_scope(
            creation=creation,
            record_bytes=record_bytes,
            record_digest=record_digest,
            envelope_bytes=envelope_bytes,
            envelope_digest=envelope_digest,
            history_root=history_root,
        )
    else:
        transition = decode_stream_transition_record(record_bytes)
        record_digest = stream_transition_digest(transition)
        history_root = next_stream_history_root(
            transition.prior_history_root,
            transition,
        )
        predecessor_bytes = _exact_blob(
            row["predecessor_record_canonical_bytes"],
            maximum=65_536,
        )
        if transition.prior_version == 1:
            predecessor_creation = decode_stream_creation_record(predecessor_bytes)
            predecessor_digest = stream_creation_digest(predecessor_creation)
            predecessor_envelope_hex = predecessor_creation.successor_envelope_hex
            predecessor_history_root = initial_stream_history_root(predecessor_creation)
            predecessor_recorded_at = predecessor_creation.recorded_at
        else:
            predecessor_transition = decode_stream_transition_record(predecessor_bytes)
            predecessor_digest = stream_transition_digest(predecessor_transition)
            predecessor_envelope_hex = predecessor_transition.successor_envelope_hex
            predecessor_history_root = next_stream_history_root(
                predecessor_transition.prior_history_root,
                predecessor_transition,
            )
            predecessor_recorded_at = predecessor_transition.recorded_at
        predecessor_envelope_bytes = bytes.fromhex(predecessor_envelope_hex)
        predecessor_envelope = decode_stream_envelope(predecessor_envelope_bytes)
        if (
            row["entry_kind"] != _ENTRY_TRANSITION
            or encode_stream_transition_record(transition) != record_bytes
            or transition.successor_version != successor_version
            or transition.prior_version != successor_version - 1
            or row["prior_version"] != transition.prior_version
            or _decode_digest(row["prior_envelope_digest"]) != transition.prior_envelope_digest
            or _decode_digest(row["prior_history_root"]) != transition.prior_history_root
            or _decode_digest(row["predecessor_record_digest"]) != predecessor_digest
            or stream_envelope_digest(predecessor_envelope) != transition.prior_envelope_digest
            or predecessor_history_root != transition.prior_history_root
            or transition.successor_envelope_hex != envelope_bytes.hex()
            or transition.successor_envelope_digest != envelope_digest
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if predecessor is not None and (
            predecessor["stream_row_id"] != row["stream_row_id"]
            or predecessor["successor_version"] != transition.prior_version
            or predecessor["record_canonical_bytes"] != predecessor_bytes
            or predecessor["record_digest"] != row["predecessor_record_digest"]
            or predecessor["successor_envelope_digest"] != row["prior_envelope_digest"]
            or predecessor["successor_history_root"] != row["prior_history_root"]
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entry = _stored_transition_without_scopes(
            transition=transition,
            record_bytes=record_bytes,
            record_digest=record_digest,
            envelope_bytes=envelope_bytes,
            envelope_digest=envelope_digest,
            history_root=history_root,
            predecessor_record_bytes=predecessor_bytes,
        )
        _validate_transition_against_prior(
            entry,
            prior_envelope=predecessor_envelope,
            prior_history_root=predecessor_history_root,
            prior_recorded_at=predecessor_recorded_at,
            policy=policy,
        )
    if (
        _decode_digest(row["record_digest"]) != record_digest
        or _decode_digest(row["successor_history_root"]) != history_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return entry


def _entry_from_history_row(
    row: sqlite3.Row,
    *,
    policy: ContinuousPublicTradePolicy,
    predecessor: sqlite3.Row | None = None,
) -> ContinuousPublicTradeStreamStoredHistoryEntryV1:
    try:
        return _entry_from_history_row_unchecked(
            row,
            policy=policy,
            predecessor=predecessor,
        )
    except HarnessFailure:
        raise
    except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None


def _validate_stream_projection(
    stream: sqlite3.Row,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> None:
    """Prove one stream row agrees with its authoritative creation bytes."""

    record = creation.record
    policy = record.stream_policy
    stream_uuid = stream["stream_uuid"]
    if type(stream_uuid) is not bytes or stream_uuid != record.stream_id.bytes:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    expected_natural_key = natural_identity_key(
        source=record.source,
        venue=record.venue,
        instrument=record.instrument,
        provider_symbol=record.provider_symbol,
        instrument_type=record.instrument_type.value,
        request_variant=record.request_variant,
    )
    exact_policy_integers = (
        _require_exact_int(policy.window_size_ms, minimum=1, maximum=MAX_CONTRACT_INTEGER),
        _require_exact_int(policy.settlement_lag_ms, minimum=0, maximum=MAX_CONTRACT_INTEGER),
        _require_exact_int(
            policy.max_catchup_span_ms,
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        ),
        _require_exact_int(
            policy.max_jobs_per_invocation,
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        ),
        _require_exact_int(
            policy.max_requests_per_job,
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        ),
        _require_exact_int(
            policy.max_records_per_job,
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        ),
    )
    if (
        stream["natural_identity_key"] != expected_natural_key
        or decode_natural_identity_key(stream["natural_identity_key"])
        != (
            record.source,
            record.venue,
            record.instrument,
            record.provider_symbol,
            record.instrument_type.value,
            record.request_variant,
        )
        or stream["stream_contract_version"] != 1
        or stream["creation_successor_version"] != 1
        or stream["creation_record_canonical_bytes"] != creation.canonical_bytes
        or _decode_digest(stream["creation_record_digest"]) != creation.record_digest
        or _decode_digest(stream["creation_history_root"]) != creation.history_root
        or stream["policy_schema_version"] != _MODEL_VERSION
        or (
            stream["policy_window_size_ms"],
            stream["policy_settlement_lag_ms"],
            stream["policy_max_catchup_span_ms"],
            stream["policy_max_jobs_per_invocation"],
            stream["policy_max_requests_per_job"],
            stream["policy_max_records_per_job"],
        )
        != exact_policy_integers
        or _decode_digest(stream["policy_fingerprint"]) != policy.policy_fingerprint
        or _require_exact_int(
            stream["stream_start_epoch_ms"],
            minimum=0,
            maximum=MAX_CONTRACT_INTEGER,
        )
        != record.stream_start_epoch_ms
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_bounded_current(
    stream: sqlite3.Row,
    history: Sequence[sqlite3.Row],
) -> tuple[
    ContinuousPublicTradeStreamStoredCreationV1,
    ContinuousPublicTradeStreamStoredHistoryEntryV1 | None,
    ContinuousPublicTradeStreamStoredHistoryEntryV1,
]:
    """Validate current state from at most creation, predecessor, and tail rows."""

    current_version = _require_exact_int(
        stream["current_version"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    by_version = {cast(int, row["successor_version"]): row for row in history}
    required_versions = {1, current_version}
    if current_version > 1:
        required_versions.add(current_version - 1)
    if set(by_version) != required_versions or len(history) != len(required_versions):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creation_witness = _creation_from_stream_row(stream)
    policy = _policy_from_creation(creation_witness)
    creation = _entry_from_history_row(by_version[1], policy=policy)
    if (
        type(creation) is not ContinuousPublicTradeStreamStoredCreationV1
        or creation != creation_witness
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    predecessor_row = by_version.get(current_version - 1) if current_version > 1 else None
    current = _entry_from_history_row(
        by_version[current_version],
        policy=policy,
        predecessor=predecessor_row,
    )
    predecessor = (
        None if predecessor_row is None else _entry_from_history_row(predecessor_row, policy=policy)
    )
    expected_stream_uuid = stream["stream_uuid"]
    if (
        creation.record.stream_id.bytes != expected_stream_uuid
        or current.record.stream_id.bytes != expected_stream_uuid
        or (predecessor is not None and predecessor.record.stream_id.bytes != expected_stream_uuid)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_stream_projection(stream, creation)
    if (
        stream["current_record_canonical_bytes"] != current.canonical_bytes
        or _decode_digest(stream["current_record_digest"]) != current.record_digest
        or stream["current_envelope_canonical_bytes"] != current.successor_envelope.canonical_bytes
        or _decode_digest(stream["current_envelope_digest"])
        != current.successor_envelope.envelope_digest
        or _decode_digest(stream["current_history_root"]) != current.history_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return creation, predecessor, current


def _natural_key_from_expectation(
    expectation: ContinuousPublicTradeStreamExpectationV1,
) -> bytes:
    identity = expectation.identity
    return natural_identity_key(
        source=identity.source,
        venue=identity.venue,
        instrument=identity.instrument,
        provider_symbol=identity.provider_symbol,
        instrument_type=identity.instrument_type.value,
        request_variant=identity.request_variant,
    )


def _expectation_matches_retained(
    expectation: ContinuousPublicTradeStreamExpectationV1,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    current_envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> bool:
    """Compare a boundary-revalidated expectation with one fully valid retained view."""

    identity = expectation.identity
    record = creation.record
    if (
        identity.stream_id != record.stream_id
        or identity.source != record.source
        or identity.venue != record.venue
        or identity.instrument != record.instrument
        or identity.provider_symbol != record.provider_symbol
        or identity.instrument_type is not record.instrument_type
        or identity.request_variant != record.request_variant
        or identity.policy_fingerprint != record.stream_policy.policy_fingerprint
        or identity.stream_start_epoch_ms != record.stream_start_epoch_ms
        or project_continuous_public_trade_policy(expectation.effective_stream_policy)
        != record.stream_policy
    ):
        return False
    try:
        validate_stream_load_bindings(
            record,
            current_envelope,
            effective_stream_policy=expectation.effective_stream_policy,
            effective_child_policy_fingerprint=(expectation.effective_child_policy_fingerprint),
        )
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        return False
    return True


_IDENTITY_SQL: Final = """
SELECT *
FROM continuous_public_trade_stream
WHERE stream_uuid = ? OR natural_identity_key = ?
ORDER BY stream_row_id
LIMIT 3
"""
_CURRENT_ROWS_SQL: Final = """
SELECT *
FROM continuous_public_trade_history
INDEXED BY ux_cpt_history_stream_version
WHERE stream_row_id = ?
  AND (
      successor_version = 1
      OR successor_version = ?
      OR successor_version = ?
  )
ORDER BY successor_version
LIMIT 3
"""
_INSERT_STREAM_SQL: Final = """
INSERT INTO continuous_public_trade_stream (
    stream_uuid,
    natural_identity_key,
    stream_contract_version,
    creation_successor_version,
    creation_record_canonical_bytes,
    creation_record_digest,
    creation_history_root,
    policy_schema_version,
    policy_window_size_ms,
    policy_settlement_lag_ms,
    policy_max_catchup_span_ms,
    policy_max_jobs_per_invocation,
    policy_max_requests_per_job,
    policy_max_records_per_job,
    policy_fingerprint,
    stream_start_epoch_ms,
    current_version,
    current_record_canonical_bytes,
    current_record_digest,
    current_envelope_canonical_bytes,
    current_envelope_digest,
    current_history_root
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""
_INSERT_HISTORY_SQL: Final = """
INSERT INTO continuous_public_trade_history (
    stream_row_id,
    successor_version,
    entry_kind,
    record_model_version,
    serialization_version,
    record_canonical_bytes,
    record_digest,
    successor_envelope_canonical_bytes,
    successor_envelope_digest,
    prior_version,
    prior_envelope_digest,
    prior_history_root,
    predecessor_record_canonical_bytes,
    predecessor_record_digest,
    successor_history_root
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_CAS_HISTORY_SQL: Final = """
SELECT *
FROM continuous_public_trade_history
INDEXED BY ux_cpt_history_stream_version
WHERE stream_row_id = ?
  AND (
      successor_version = ?
      OR successor_version = ?
  )
ORDER BY successor_version
LIMIT 2
"""
_UPDATE_CURRENT_SQL: Final = """
UPDATE continuous_public_trade_stream
SET
    current_version = ?,
    current_record_canonical_bytes = ?,
    current_record_digest = ?,
    current_envelope_canonical_bytes = ?,
    current_envelope_digest = ?,
    current_history_root = ?
WHERE stream_row_id = ?
  AND current_version = ?
  AND current_record_canonical_bytes = ?
  AND current_record_digest = ?
  AND current_envelope_digest = ?
  AND current_history_root = ?
"""


_CORRUPT_RESULT_CODES: Final = frozenset({11, 267, 523, 779, 26})
_OPERATIONAL_RESULT_CODES: Final = frozenset(
    {
        1,
        257,
        513,
        769,
        1025,
        1281,
        1537,
        3,
        5,
        261,
        517,
        773,
        6,
        262,
        518,
        8,
        264,
        520,
        776,
        1032,
        1288,
        1544,
        14,
        270,
        526,
        782,
        1038,
        1294,
        1550,
        19,
        275,
        531,
        787,
        1043,
        1299,
        1555,
        1811,
        2067,
        2323,
        2579,
        2835,
        3091,
    }
    | {10 + (ordinal * 256) for ordinal in range(37)}
)


def sqlite_result_failure_code(code: object) -> HarnessFailureCode:
    """Apply the frozen exact numeric result-code allowlists."""

    if type(code) is not int:
        return HarnessFailureCode.UNAVAILABLE
    if code in _CORRUPT_RESULT_CODES:
        return HarnessFailureCode.CORRUPT
    if code in _OPERATIONAL_RESULT_CODES:
        return HarnessFailureCode.UNAVAILABLE
    return HarnessFailureCode.UNAVAILABLE


def _sqlite_failure(error: sqlite3.Error) -> HarnessFailure:
    """Map numeric SQLite extended codes without parsing implementation messages."""

    raw_code = getattr(error, "sqlite_errorcode", None)
    exact_code = raw_code if type(raw_code) is int else None
    return HarnessFailure(
        sqlite_result_failure_code(exact_code),
        sqlite_errorcode=exact_code,
    )


def _rollback_best_effort(connection: sqlite3.Connection) -> None:
    """End a failed test transaction without replacing its sanitized primary outcome."""

    if connection.in_transaction:
        with suppress(BaseException):
            connection.execute("ROLLBACK").close()


def _close_best_effort(connection: sqlite3.Connection | None) -> None:
    if connection is None:
        return
    _CONNECTION_EVIDENCE_BINDINGS.pop(id(connection), None)
    snapshot = _CONNECTION_PATH_SNAPSHOTS.pop(id(connection), None)
    try:
        with suppress(BaseException):
            connection.close()
    finally:
        _close_operation_path_snapshot(snapshot)


def _close_checked(connection: sqlite3.Connection) -> None:
    _CONNECTION_EVIDENCE_BINDINGS.pop(id(connection), None)
    snapshot = _CONNECTION_PATH_SNAPSHOTS.pop(id(connection), None)
    try:
        connection.close()
    except sqlite3.Error as error:
        raise _sqlite_failure(error) from None
    except MemoryError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except Exception:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    finally:
        _close_operation_path_snapshot(snapshot)


def _close_preserving_primary(connection: sqlite3.Connection) -> None:
    if sys.exception() is None:
        _close_checked(connection)
    else:
        _close_best_effort(connection)


def _close_many_preserving_primary(
    *connections: sqlite3.Connection | None,
) -> None:
    if sys.exception() is not None:
        for connection in connections:
            _close_best_effort(connection)
        return
    first_failure: BaseException | None = None
    for connection in connections:
        if connection is None:
            continue
        try:
            _close_checked(connection)
        except BaseException as error:
            if first_failure is None:
                first_failure = error
    if first_failure is not None:
        raise first_failure


def _close_cursor_best_effort(cursor: sqlite3.Cursor | None) -> None:
    if cursor is not None:
        with suppress(BaseException):
            cursor.close()


def _close_cursor_preserving_primary(cursor: sqlite3.Cursor) -> None:
    if sys.exception() is None:
        try:
            cursor.close()
        except sqlite3.Error as error:
            raise _sqlite_failure(error) from None
        except MemoryError:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        except Exception:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    else:
        _close_cursor_best_effort(cursor)


def _validated_creation(
    value: ContinuousPublicTradeStreamStoredCreationV1,
    policy: ContinuousPublicTradePolicy,
) -> ContinuousPublicTradeStreamStoredCreationV1:
    try:
        exact = ContinuousPublicTradeStreamStoredCreationV1.revalidate_at_boundary(value)
        projected = project_continuous_public_trade_policy(policy)
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if exact.record.stream_policy != projected:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for integer, minimum in (
        (projected.window_size_ms, 1),
        (projected.settlement_lag_ms, 0),
        (projected.max_catchup_span_ms, 1),
        (projected.max_jobs_per_invocation, 1),
        (projected.max_requests_per_job, 1),
        (projected.max_records_per_job, 1),
        (exact.record.stream_start_epoch_ms, 0),
        (exact.record.successor_version, 1),
        (exact.record.serialization_version, 1),
    ):
        _require_exact_int(integer, minimum=minimum, maximum=MAX_CONTRACT_INTEGER)
    return exact


def _policy_from_creation(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> ContinuousPublicTradePolicy:
    try:
        exact = ContinuousPublicTradeStreamStoredCreationV1.revalidate_at_boundary(creation)
        return ContinuousPublicTradePolicy(**exact.record.stream_policy.model_dump())
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None


def _validate_transition_against_prior(
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    *,
    prior_envelope: ContinuousPublicTradeStreamEnvelopeV1,
    prior_history_root: str,
    prior_recorded_at: datetime,
    policy: ContinuousPublicTradePolicy,
) -> None:
    try:
        validate_stream_transition_link(
            prior_envelope,
            transition.record,
            policy=policy,
            prior_history_root=prior_history_root,
            prior_recorded_at=prior_recorded_at,
            transition_authority_scope=transition.transition_authority_scope,
            child_completion_scope=transition.child_completion_scope,
        )
    except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None


def _validated_transition(
    value: ContinuousPublicTradeStreamStoredTransitionV1,
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    try:
        exact = ContinuousPublicTradeStreamStoredTransitionV1.revalidate_at_boundary(value)
    except (AttributeError, TypeError, ValueError, OverflowError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    _require_exact_int(
        exact.record.prior_version,
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    _require_exact_int(
        exact.record.successor_version,
        minimum=2,
        maximum=MAX_CONTRACT_INTEGER,
    )
    _require_exact_int(
        exact.record.serialization_version,
        minimum=1,
        maximum=1,
    )
    return exact


def _identity_rows(
    connection: sqlite3.Connection,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> list[sqlite3.Row]:
    if type(stream_id) is not UUID:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decode_natural_identity_key(natural_key)
    rows = _fetch_all(connection, _IDENTITY_SQL, (stream_id.bytes, natural_key))
    if len(rows) > 2:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return rows


def _validate_identity_candidates(
    connection: sqlite3.Connection,
    streams: Sequence[sqlite3.Row],
) -> tuple[tuple[ContinuousPublicTradeStreamStoredCreationV1, ...], int]:
    if not streams or len(streams) > 2:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creations: list[ContinuousPublicTradeStreamStoredCreationV1] = []
    history_rows = 0
    for stream in streams:
        bounded_rows = _history_rows_for_current(connection, stream)
        creation, _, _ = _validate_bounded_current(stream, bounded_rows)
        history_rows += len(bounded_rows)
        creations.append(creation)
    return tuple(creations), history_rows


def _invoke_seam(
    hook: Callable[[str], None] | None,
    name: str,
) -> None:
    if hook is not None:
        hook(name)


@_operation_evidence_executor
def create_stream(
    token: StoreToken,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    policy: ContinuousPublicTradePolicy,
    *,
    seam_hook: Callable[[str], None] | None = None,
) -> MutationEvidence:
    """Atomically insert or classify one exact generated creation record."""

    exact = _validated_creation(creation, policy)
    record = exact.record
    natural_key = natural_identity_key(
        source=record.source,
        venue=record.venue,
        instrument=record.instrument,
        provider_symbol=record.provider_symbol,
        instrument_type=record.instrument_type.value,
        request_variant=record.request_variant,
    )
    statements = (
        "BEGIN IMMEDIATE",
        "identity candidates LIMIT 3",
        "current history LIMIT 3 per identity candidate",
        "INSERT stream",
        "INSERT creation history",
        "COMMIT",
    )
    _invoke_seam(seam_hook, "before_transaction")
    connection, _ = _connect(token, writer=True)
    rows_materialized = 0
    committed = False
    try:
        connection.execute("BEGIN IMMEDIATE").close()
        _invoke_seam(seam_hook, "after_lock")
        _verify_operation_snapshot(connection, token, writer=True)
        candidates = _identity_rows(
            connection,
            stream_id=record.stream_id,
            natural_key=natural_key,
        )
        rows_materialized += len(candidates)
        if candidates:
            retained, retained_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            rows_materialized += retained_rows
            if len(candidates) == 1 and (
                candidates[0]["stream_uuid"] == record.stream_id.bytes
                and candidates[0]["natural_identity_key"] == natural_key
                and retained[0].canonical_bytes == exact.canonical_bytes
                and retained[0].record_digest == exact.record_digest
                and retained[0].successor_envelope.canonical_bytes
                == exact.successor_envelope.canonical_bytes
                and retained[0].history_root == exact.history_root
            ):
                classification = StoreClassification.DUPLICATE
            else:
                classification = StoreClassification.CONFLICT
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                classification,
                statements,
                rows_materialized,
                committed,
            )

        projected = record.stream_policy
        cursor = connection.execute(
            _INSERT_STREAM_SQL,
            (
                record.stream_id.bytes,
                natural_key,
                1,
                1,
                exact.canonical_bytes,
                _digest_bytes(exact.record_digest),
                _digest_bytes(exact.history_root),
                projected.schema_version.encode("ascii"),
                projected.window_size_ms,
                projected.settlement_lag_ms,
                projected.max_catchup_span_ms,
                projected.max_jobs_per_invocation,
                projected.max_requests_per_job,
                projected.max_records_per_job,
                _digest_bytes(projected.policy_fingerprint),
                record.stream_start_epoch_ms,
                1,
                exact.canonical_bytes,
                _digest_bytes(exact.record_digest),
                exact.successor_envelope.canonical_bytes,
                _digest_bytes(exact.successor_envelope.envelope_digest),
                _digest_bytes(exact.history_root),
            ),
        )
        try:
            stream_row_id = _require_exact_int(
                cursor.lastrowid,
                minimum=1,
                maximum=MAX_CONTRACT_INTEGER,
            )
        finally:
            _close_cursor_preserving_primary(cursor)
        _invoke_seam(seam_hook, "between_stream_insert_and_creation_insert")
        connection.execute(
            _INSERT_HISTORY_SQL,
            (
                stream_row_id,
                1,
                _ENTRY_CREATION,
                _MODEL_VERSION,
                1,
                exact.canonical_bytes,
                _digest_bytes(exact.record_digest),
                exact.successor_envelope.canonical_bytes,
                _digest_bytes(exact.successor_envelope.envelope_digest),
                None,
                None,
                None,
                None,
                None,
                _digest_bytes(exact.history_root),
            ),
        ).close()
        _invoke_seam(seam_hook, "between_creation_insert_and_create_commit")
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        committed = True
        _invoke_seam(seam_hook, "after_commit_before_acknowledgement")
        return MutationEvidence(
            StoreClassification.INSERTED,
            statements,
            rows_materialized,
            committed,
        )
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _rollback_best_effort(connection)
        raise failure from None
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


def _history_rows_for_current(
    connection: sqlite3.Connection,
    stream: sqlite3.Row,
) -> list[sqlite3.Row]:
    stream_row_id = _require_exact_int(
        stream["stream_row_id"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    current_version = _require_exact_int(
        stream["current_version"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    return _fetch_all(
        connection,
        _CURRENT_ROWS_SQL,
        (stream_row_id, max(1, current_version - 1), current_version),
    )


@_operation_evidence_executor
def load_current(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
) -> CurrentSlice:
    """Load one exact identity with finite current and identity-conflict row budgets."""

    if type(stream_id) is not UUID:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decode_natural_identity_key(natural_key)
    exact_expectation = (
        None
        if expectation is None
        else ContinuousPublicTradeStreamExpectationV1.revalidate_at_boundary(expectation)
    )
    connection, _ = _connect(token, writer=False)
    statements = (
        "BEGIN",
        "identity candidates LIMIT 3",
        "current history LIMIT 3 per identity candidate",
        "COMMIT",
    )
    stream_rows = 0
    history_rows = 0
    decoded_rows = 0
    try:
        connection.execute("BEGIN").close()
        _verify_operation_snapshot(connection, token, writer=False)
        candidates = _identity_rows(
            connection,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        stream_rows = len(candidates)
        if not candidates:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return CurrentSlice(
                StoreClassification.NOT_FOUND,
                None,
                None,
                None,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        if len(candidates) == 2:
            _, history_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            decoded_rows = history_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return CurrentSlice(
                StoreClassification.IDENTITY_CONFLICT,
                None,
                None,
                None,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        stream = candidates[0]
        if (
            stream["stream_uuid"] != stream_id.bytes
            or stream["natural_identity_key"] != natural_key
        ):
            _, history_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            decoded_rows = history_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return CurrentSlice(
                StoreClassification.IDENTITY_CONFLICT,
                None,
                None,
                None,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        rows = _history_rows_for_current(connection, stream)
        history_rows = len(rows)
        creation, predecessor, current = _validate_bounded_current(stream, rows)
        decoded_rows = len(rows)
        if exact_expectation is not None and not _expectation_matches_retained(
            exact_expectation,
            creation,
            current.successor_envelope.envelope,
        ):
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return CurrentSlice(
                StoreClassification.IDENTITY_CONFLICT,
                None,
                None,
                None,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        return CurrentSlice(
            StoreClassification.FOUND,
            creation,
            predecessor,
            current,
            QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
        )
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _rollback_best_effort(connection)
        raise failure from None
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


@_operation_evidence_executor
def compare_and_swap_stream(
    token: StoreToken,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    *,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
    seam_hook: Callable[[str], None] | None = None,
    _test_max_page_count: int | None = None,
) -> MutationEvidence:
    """Atomically append one exact transition and advance its bound current row."""

    exact = _validated_transition(transition)
    exact_expectation = (
        None
        if expectation is None
        else ContinuousPublicTradeStreamExpectationV1.revalidate_at_boundary(expectation)
    )
    if _test_max_page_count is not None:
        _require_exact_int(
            _test_max_page_count,
            minimum=1,
            maximum=MAX_PAGE_COUNT,
        )
    record = exact.record
    statements = (
        "BEGIN IMMEDIATE",
        (
            "stream UUID lookup LIMIT 2"
            if exact_expectation is None
            else "stream identity lookup LIMIT 3"
        ),
        "history predecessor/successor LIMIT 2",
        "current history LIMIT 3",
        "INSERT transition history",
        "UPDATE current CAS",
        "COMMIT",
    )
    _invoke_seam(seam_hook, "before_transaction")
    connection, _ = _connect(token, writer=True)
    rows_materialized = 0
    committed = False
    try:
        connection.execute("BEGIN IMMEDIATE").close()
        _invoke_seam(seam_hook, "after_lock")
        _verify_operation_snapshot(connection, token, writer=True)
        if _test_max_page_count is not None:
            connection.execute(f"PRAGMA max_page_count = {_test_max_page_count}").close()
            if cast(int, _pragma_scalar(connection, "max_page_count")) != _test_max_page_count:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if exact_expectation is None:
            streams = _fetch_all(
                connection,
                """
                SELECT *
                FROM continuous_public_trade_stream
                INDEXED BY ux_cpt_stream_uuid
                WHERE stream_uuid = ?
                LIMIT 2
                """,
                (record.stream_id.bytes,),
            )
        else:
            streams = _identity_rows(
                connection,
                stream_id=record.stream_id,
                natural_key=_natural_key_from_expectation(exact_expectation),
            )
        rows_materialized += len(streams)
        if len(streams) != 1 or (
            exact_expectation is not None
            and (
                streams[0]["stream_uuid"] != record.stream_id.bytes
                or streams[0]["natural_identity_key"]
                != _natural_key_from_expectation(exact_expectation)
            )
        ):
            if exact_expectation is None and streams:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if streams:
                _, retained_rows = _validate_identity_candidates(
                    connection,
                    streams,
                )
                rows_materialized += retained_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                rows_materialized,
                committed,
            )
        stream = streams[0]
        stream_row_id = _require_exact_int(
            stream["stream_row_id"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        current_rows = _history_rows_for_current(connection, stream)
        rows_materialized += len(current_rows)
        creation, _, current = _validate_bounded_current(stream, current_rows)
        retained_policy = _policy_from_creation(creation)
        if exact_expectation is not None and not _expectation_matches_retained(
            exact_expectation,
            creation,
            current.successor_envelope.envelope,
        ):
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                rows_materialized,
                committed,
            )
        current_version = _require_exact_int(
            stream["current_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        history = _fetch_all(
            connection,
            _CAS_HISTORY_SQL,
            (
                stream_row_id,
                record.prior_version,
                record.successor_version,
            ),
        )
        rows_materialized += len(history)
        by_version = {cast(int, row["successor_version"]): row for row in history}
        successor_row = by_version.get(record.successor_version)
        predecessor_row = by_version.get(record.prior_version)
        if successor_row is not None:
            if predecessor_row is None:
                _verify_operation_authority(connection, token)
                connection.execute("COMMIT").close()
                committed = True
                return MutationEvidence(
                    StoreClassification.CORRUPT,
                    statements,
                    rows_materialized,
                    committed,
                )
            retained = _entry_from_history_row(
                successor_row,
                policy=retained_policy,
                predecessor=predecessor_row,
            )
            if current_version < record.successor_version:
                classification = StoreClassification.CORRUPT
            elif (
                retained.canonical_bytes == exact.canonical_bytes
                and retained.record_digest == exact.record_digest
                and retained.successor_envelope.canonical_bytes
                == exact.successor_envelope.canonical_bytes
                and retained.history_root == exact.history_root
            ):
                classification = StoreClassification.DUPLICATE
            else:
                classification = StoreClassification.CONFLICT
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                classification,
                statements,
                rows_materialized,
                committed,
            )
        if current_version >= record.successor_version:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CORRUPT,
                statements,
                rows_materialized,
                committed,
            )
        if predecessor_row is None:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CORRUPT,
                statements,
                rows_materialized,
                committed,
            )
        if (
            current_version != record.prior_version
            or current.canonical_bytes != predecessor_row["record_canonical_bytes"]
            or current.record_digest != _decode_digest(predecessor_row["record_digest"])
            or current.successor_envelope.envelope_digest != record.prior_envelope_digest
            or current.history_root != record.prior_history_root
        ):
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                rows_materialized,
                committed,
            )

        _validate_transition_against_prior(
            exact,
            prior_envelope=current.successor_envelope.envelope,
            prior_history_root=current.history_root,
            prior_recorded_at=current.record.recorded_at,
            policy=retained_policy,
        )
        connection.execute(
            _INSERT_HISTORY_SQL,
            (
                stream_row_id,
                record.successor_version,
                _ENTRY_TRANSITION,
                _MODEL_VERSION,
                1,
                exact.canonical_bytes,
                _digest_bytes(exact.record_digest),
                exact.successor_envelope.canonical_bytes,
                _digest_bytes(exact.successor_envelope.envelope_digest),
                record.prior_version,
                _digest_bytes(record.prior_envelope_digest),
                _digest_bytes(record.prior_history_root),
                current.canonical_bytes,
                _digest_bytes(current.record_digest),
                _digest_bytes(exact.history_root),
            ),
        ).close()
        _invoke_seam(seam_hook, "between_transition_insert_and_current_update")
        cursor = connection.execute(
            _UPDATE_CURRENT_SQL,
            (
                record.successor_version,
                exact.canonical_bytes,
                _digest_bytes(exact.record_digest),
                exact.successor_envelope.canonical_bytes,
                _digest_bytes(exact.successor_envelope.envelope_digest),
                _digest_bytes(exact.history_root),
                stream_row_id,
                record.prior_version,
                current.canonical_bytes,
                _digest_bytes(current.record_digest),
                _digest_bytes(current.successor_envelope.envelope_digest),
                _digest_bytes(current.history_root),
            ),
        )
        try:
            rowcount = cursor.rowcount
        finally:
            _close_cursor_preserving_primary(cursor)
        if rowcount == 0:
            _verify_operation_authority(connection, token)
            connection.execute("ROLLBACK").close()
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                rows_materialized,
                False,
            )
        if rowcount != 1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _invoke_seam(
            seam_hook,
            "between_current_update_and_compare_and_swap_commit",
        )
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        committed = True
        _invoke_seam(seam_hook, "after_commit_before_acknowledgement")
        return MutationEvidence(
            StoreClassification.UPDATED,
            statements,
            rows_materialized,
            committed,
        )
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _rollback_best_effort(connection)
        raise failure from None
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


_CREATE_KILL_SEAM_ORDER: Final = (
    "before_transaction",
    "after_lock",
    "between_stream_insert_and_creation_insert",
    "between_creation_insert_and_create_commit",
    "after_commit_before_acknowledgement",
)
_CAS_KILL_SEAM_ORDER: Final = (
    "before_transaction",
    "after_lock",
    "between_transition_insert_and_current_update",
    "between_current_update_and_compare_and_swap_commit",
    "after_commit_before_acknowledgement",
)
_CREATE_KILL_SEAMS: Final = frozenset(_CREATE_KILL_SEAM_ORDER)
_CAS_KILL_SEAMS: Final = frozenset(_CAS_KILL_SEAM_ORDER)


@_operation_evidence_executor
def sqlite_result_code_fault_evidence(
    token: StoreToken,
    *,
    seam: str,
) -> FaultEvidence:
    """Probe fixed READONLY or one-attempt BUSY mechanics in fresh processes."""

    if seam not in {"readonly", "busy"}:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _require_token(token)
    if not hasattr(os, "fork"):
        return FaultEvidence(
            seam=seam,
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="fork_unavailable",
        )
    before = verify_store(token)

    try:
        probe_ready_pipe, probe_control_pipe, result_pipe = _open_pipes(3)
    except OSError:
        return _unproven_process_fault(seam, reason="ipc_setup_failed")
    probe_ready_read, probe_ready_write = probe_ready_pipe
    probe_control_read, probe_control_write = probe_control_pipe
    result_read, result_write = result_pipe
    probe_descriptors = (
        probe_ready_read,
        probe_ready_write,
        probe_control_read,
        probe_control_write,
        result_read,
        result_write,
    )
    try:
        probe_process_id = os.fork()
    except OSError:
        _close_descriptors(probe_descriptors)
        return _unproven_process_fault(seam, reason="fork_spawn_failed")
    if probe_process_id == 0:
        os.close(probe_ready_read)
        os.close(probe_control_write)
        os.close(result_read)
        connection: sqlite3.Connection | None = None
        code = -1
        try:
            connection, _ = _connect(token, writer=True)
            _verify_operation_snapshot(connection, token, writer=True)
            os.write(probe_ready_write, b"R")
            if os.read(probe_control_read, 1) != b"C":
                os._exit(71)
            if seam == "readonly":
                connection.execute("PRAGMA query_only = ON").close()
            connection.execute("BEGIN IMMEDIATE").close()
            connection.execute(
                "UPDATE stream_store_metadata SET singleton_key = singleton_key WHERE 0"
            ).close()
        except sqlite3.Error as error:
            raw_code = getattr(error, "sqlite_errorcode", None)
            if type(raw_code) is int:
                code = raw_code
        except HarnessFailure as error:
            if type(error.sqlite_errorcode) is int:
                code = error.sqlite_errorcode
        except BaseException:
            code = -1
        finally:
            if connection is not None:
                _rollback_best_effort(connection)
                _close_best_effort(connection)
        with suppress(OSError):
            os.write(result_write, struct.pack(">i", code))
        os.close(result_write)
        os._exit(0 if code >= 0 else 70)

    os.close(probe_ready_write)
    os.close(probe_control_read)
    os.close(result_write)
    probe_selector = selectors.DefaultSelector()
    probe_selector.register(probe_ready_read, selectors.EVENT_READ)
    try:
        probe_ready = os.read(probe_ready_read, 1) if probe_selector.select(timeout=10.0) else b""
    finally:
        probe_selector.close()
        os.close(probe_ready_read)
    if probe_ready != b"R":
        with suppress(ProcessLookupError):
            os.kill(probe_process_id, signal.SIGKILL)
        with suppress(ChildProcessError):
            os.waitpid(probe_process_id, 0)
        os.close(probe_control_write)
        os.close(result_read)
        return FaultEvidence(
            seam=seam,
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="probe_open_failed",
        )

    holder_process_id: int | None = None
    holder_release_write: int | None = None
    holder_ok = True
    if seam == "busy":
        try:
            holder_ready_pipe, holder_release_pipe = _open_pipes(2)
        except OSError:
            _close_descriptors((probe_control_write, result_read))
            _terminate_and_reap_processes((probe_process_id,))
            return _unproven_process_fault(seam, reason="ipc_setup_failed")
        holder_ready_read, holder_ready_write = holder_ready_pipe
        holder_release_read, holder_release_write = holder_release_pipe
        holder_descriptors = (
            holder_ready_read,
            holder_ready_write,
            holder_release_read,
            holder_release_write,
        )
        try:
            holder_process_id = os.fork()
        except OSError:
            _close_descriptors(
                (
                    *holder_descriptors,
                    probe_control_write,
                    result_read,
                )
            )
            _terminate_and_reap_processes((probe_process_id,))
            return _unproven_process_fault(seam, reason="fork_spawn_failed")
        if holder_process_id == 0:
            os.close(holder_ready_read)
            os.close(holder_release_write)
            os.close(probe_control_write)
            os.close(result_read)
            holder_connection: sqlite3.Connection | None = None
            try:
                holder_connection, _ = _connect(token, writer=True)
                holder_connection.execute("BEGIN IMMEDIATE").close()
                os.write(holder_ready_write, b"R")
                if os.read(holder_release_read, 1) != b"C":
                    os._exit(71)
                _verify_operation_authority(holder_connection, token)
                holder_connection.execute("ROLLBACK").close()
                _close_checked(holder_connection)
                os._exit(0)
            except BaseException:
                _close_best_effort(holder_connection)
                os._exit(70)
        os.close(holder_ready_write)
        os.close(holder_release_read)
        selector = selectors.DefaultSelector()
        selector.register(holder_ready_read, selectors.EVENT_READ)
        try:
            ready = os.read(holder_ready_read, 1) if selector.select(timeout=10.0) else b""
        finally:
            selector.close()
            os.close(holder_ready_read)
        if ready != b"R":
            with suppress(ProcessLookupError):
                os.kill(holder_process_id, signal.SIGKILL)
            with suppress(ChildProcessError):
                os.waitpid(holder_process_id, 0)
            os.close(holder_release_write)
            with suppress(ProcessLookupError):
                os.kill(probe_process_id, signal.SIGKILL)
            with suppress(ChildProcessError):
                os.waitpid(probe_process_id, 0)
            os.close(probe_control_write)
            os.close(result_read)
            return FaultEvidence(
                seam=seam,
                disposition=EvidenceDisposition.UNPROVEN,
                sqlite_errorcode=None,
                observed_syscall=None,
                acknowledgement_bytes=0,
                reopened_state=ReopenedState.UNAVAILABLE,
                reason="busy_holder_failed",
            )
    with suppress(OSError):
        os.write(probe_control_write, b"C")
    os.close(probe_control_write)
    selector = selectors.DefaultSelector()
    selector.register(result_read, selectors.EVENT_READ)
    payload = b""
    try:
        events = selector.select(timeout=10.0)
        if events:
            payload = os.read(result_read, 4)
        if len(payload) != 4:
            with suppress(ProcessLookupError):
                os.kill(probe_process_id, signal.SIGKILL)
        _, probe_status = os.waitpid(probe_process_id, 0)
    finally:
        selector.close()
        os.close(result_read)
        if holder_process_id is not None and holder_release_write is not None:
            with suppress(OSError):
                os.write(holder_release_write, b"C")
            os.close(holder_release_write)
            try:
                _, holder_status = os.waitpid(holder_process_id, 0)
            except ChildProcessError:
                holder_ok = False
            else:
                holder_ok = os.WIFEXITED(holder_status) and os.WEXITSTATUS(holder_status) == 0
    if len(payload) != 4:
        return FaultEvidence(
            seam=seam,
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=len(payload),
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="result_code_probe_failed",
        )
    (sqlite_errorcode,) = struct.unpack(">i", payload)
    expected = sqlite3.SQLITE_READONLY if seam == "readonly" else sqlite3.SQLITE_BUSY
    try:
        after = verify_store(token)
        unchanged = (
            before.schema_fingerprint == after.schema_fingerprint
            and before.stream_count == after.stream_count
            and before.history_count == after.history_count
        )
    except HarnessFailure:
        unchanged = False
    passed = (
        os.WIFEXITED(probe_status)
        and os.WEXITSTATUS(probe_status) == 0
        and holder_ok
        and sqlite_errorcode == expected
        and sqlite_result_failure_code(sqlite_errorcode) is HarnessFailureCode.UNAVAILABLE
        and unchanged
    )
    return FaultEvidence(
        seam=seam,
        disposition=(EvidenceDisposition.PASS if passed else EvidenceDisposition.FAIL),
        sqlite_errorcode=sqlite_errorcode,
        observed_syscall=(
            "sqlite3-query-only" if seam == "readonly" else "forked-BEGIN-IMMEDIATE-contention"
        ),
        acknowledgement_bytes=len(payload),
        reopened_state=(ReopenedState.OLD if unchanged else ReopenedState.UNAVAILABLE),
        reason=None if passed else "result_code_or_state_mismatch",
    )


_WRITER_OUTCOME_PACKETS: Final = {
    StoreClassification.INSERTED: b"I",
    StoreClassification.UPDATED: b"U",
    StoreClassification.DUPLICATE: b"D",
    StoreClassification.CONFLICT: b"C",
    HarnessFailureCode.UNAVAILABLE: b"N",
    HarnessFailureCode.CORRUPT: b"R",
}
_WRITER_PACKET_OUTCOMES: Final = {
    packet: outcome for outcome, packet in _WRITER_OUTCOME_PACKETS.items()
}


@_operation_evidence_executor
def fresh_process_writer_contention_evidence(
    token: StoreToken,
    *,
    operation: str,
    natural_key: bytes,
    creation: ContinuousPublicTradeStreamStoredCreationV1 | None = None,
    policy: ContinuousPublicTradePolicy | None = None,
    transition: ContinuousPublicTradeStreamStoredTransitionV1 | None = None,
) -> WriterContentionEvidence:
    """Hold one real writer lock while a second fresh process makes one exact attempt."""

    decode_natural_identity_key(natural_key)
    if operation == "create":
        if creation is None or policy is None or transition is not None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        value: (
            ContinuousPublicTradeStreamStoredCreationV1
            | ContinuousPublicTradeStreamStoredTransitionV1
        ) = _validated_creation(creation, policy)
        expected_winner = StoreClassification.INSERTED
    elif operation == "compare_and_swap":
        if transition is None or creation is not None or policy is not None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        value = _validated_transition(transition)
        expected_winner = StoreClassification.UPDATED
    else:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    before = verify_store(token)
    if not hasattr(os, "fork"):
        return WriterContentionEvidence(
            operation=operation,
            winner_outcome=None,
            contender_outcome=None,
            contender_sqlite_errorcode=None,
            disposition=EvidenceDisposition.UNPROVEN,
            process_boundary="unavailable",
            stream_count=before.stream_count,
            history_count=before.history_count,
        )

    try:
        (
            winner_ready_pipe,
            winner_start_pipe,
            winner_locked_pipe,
            winner_release_pipe,
            winner_result_pipe,
            contender_ready_pipe,
            contender_start_pipe,
            contender_result_pipe,
        ) = _open_pipes(8)
    except OSError:
        return WriterContentionEvidence(
            operation=operation,
            winner_outcome=None,
            contender_outcome=None,
            contender_sqlite_errorcode=None,
            disposition=EvidenceDisposition.UNPROVEN,
            process_boundary="ipc_setup_failed",
            stream_count=before.stream_count,
            history_count=before.history_count,
        )
    winner_ready_read, winner_ready_write = winner_ready_pipe
    winner_start_read, winner_start_write = winner_start_pipe
    winner_locked_read, winner_locked_write = winner_locked_pipe
    winner_release_read, winner_release_write = winner_release_pipe
    winner_result_read, winner_result_write = winner_result_pipe
    contender_ready_read, contender_ready_write = contender_ready_pipe
    contender_start_read, contender_start_write = contender_start_pipe
    contender_result_read, contender_result_write = contender_result_pipe
    all_descriptors = (
        winner_ready_read,
        winner_ready_write,
        winner_start_read,
        winner_start_write,
        winner_locked_read,
        winner_locked_write,
        winner_release_read,
        winner_release_write,
        winner_result_read,
        winner_result_write,
        contender_ready_read,
        contender_ready_write,
        contender_start_read,
        contender_start_write,
        contender_result_read,
        contender_result_write,
    )

    def run_operation(
        seam_hook: Callable[[str], None] | None = None,
    ) -> StoreClassification:
        if operation == "create":
            return create_stream(
                token,
                cast(ContinuousPublicTradeStreamStoredCreationV1, value),
                cast(ContinuousPublicTradePolicy, policy),
                seam_hook=seam_hook,
            ).classification
        return compare_and_swap_stream(
            token,
            cast(ContinuousPublicTradeStreamStoredTransitionV1, value),
            seam_hook=seam_hook,
        ).classification

    def spawn_failed(
        process_ids: Sequence[int],
    ) -> WriterContentionEvidence:
        _close_descriptors(all_descriptors)
        _terminate_and_reap_processes(process_ids)
        return WriterContentionEvidence(
            operation=operation,
            winner_outcome=None,
            contender_outcome=None,
            contender_sqlite_errorcode=None,
            disposition=EvidenceDisposition.UNPROVEN,
            process_boundary="fork_spawn_failed",
            stream_count=before.stream_count,
            history_count=before.history_count,
        )

    try:
        winner_process_id = os.fork()
    except OSError:
        return spawn_failed(())
    if winner_process_id == 0:
        retained = {
            winner_ready_write,
            winner_start_read,
            winner_locked_write,
            winner_release_read,
            winner_result_write,
        }
        for descriptor in all_descriptors:
            if descriptor not in retained:
                with suppress(OSError):
                    os.close(descriptor)

        def hold_after_lock(seam: str) -> None:
            if seam == "after_lock":
                os.write(winner_locked_write, b"L")
                if os.read(winner_release_read, 1) != b"R":
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

        packet = b"E"
        try:
            os.write(winner_ready_write, b"R")
            if os.read(winner_start_read, 1) != b"S":
                os._exit(71)
            packet = _WRITER_OUTCOME_PACKETS.get(run_operation(hold_after_lock), b"E")
        except HarnessFailure as error:
            packet = _WRITER_OUTCOME_PACKETS.get(error.code, b"E")
        except BaseException:
            packet = b"E"
        with suppress(OSError):
            os.write(winner_result_write, packet)
        os._exit(0 if packet != b"E" else 70)

    try:
        contender_process_id = os.fork()
    except OSError:
        return spawn_failed((winner_process_id,))
    if contender_process_id == 0:
        retained = {contender_ready_write, contender_start_read, contender_result_write}
        for descriptor in all_descriptors:
            if descriptor not in retained:
                with suppress(OSError):
                    os.close(descriptor)
        packet = b"E" + struct.pack(">i", -1)
        contender_connection: sqlite3.Connection | None = None
        try:
            contender_connection, _ = _connect(token, writer=True)
            os.write(contender_ready_write, b"R")
            if os.read(contender_start_read, 1) != b"C":
                os._exit(71)
            contender_connection.execute("BEGIN IMMEDIATE").close()
            packet = b"E" + struct.pack(">i", -1)
        except sqlite3.Error as error:
            raw_code = getattr(error, "sqlite_errorcode", None)
            code = raw_code if type(raw_code) is int else -1
            packet = _WRITER_OUTCOME_PACKETS[HarnessFailureCode.UNAVAILABLE] + struct.pack(
                ">i", code
            )
        except HarnessFailure as error:
            code = error.sqlite_errorcode if type(error.sqlite_errorcode) is int else -1
            packet = _WRITER_OUTCOME_PACKETS.get(error.code, b"E") + struct.pack(">i", code)
        except BaseException:
            packet = b"E" + struct.pack(">i", -1)
        finally:
            if contender_connection is not None:
                _rollback_best_effort(contender_connection)
                _close_best_effort(contender_connection)
        with suppress(OSError):
            os.write(contender_result_write, packet)
        os._exit(0 if packet[:1] != b"E" else 70)

    for descriptor in (
        winner_ready_write,
        winner_start_read,
        winner_locked_write,
        winner_release_read,
        winner_result_write,
        contender_ready_write,
        contender_start_read,
        contender_result_write,
    ):
        os.close(descriptor)

    def read_packet(descriptor: int, size: int) -> bytes:
        selector = selectors.DefaultSelector()
        selector.register(descriptor, selectors.EVENT_READ)
        try:
            return os.read(descriptor, size) if selector.select(timeout=10.0) else b""
        finally:
            selector.close()

    live_processes = {winner_process_id, contender_process_id}
    protocol_ok = True
    winner_outcome: StoreClassification | HarnessFailureCode | None = None
    contender_outcome: StoreClassification | HarnessFailureCode | None = None
    contender_code: int | None = None
    parent_descriptors = (
        winner_ready_read,
        winner_start_write,
        winner_locked_read,
        winner_release_write,
        winner_result_read,
        contender_ready_read,
        contender_start_write,
        contender_result_read,
    )
    try:
        winner_ready = read_packet(winner_ready_read, 1)
        contender_ready = read_packet(contender_ready_read, 1)
        if winner_ready != b"R" or contender_ready != b"R":
            protocol_ok = False
        if protocol_ok:
            os.write(winner_start_write, b"S")
            winner_locked = read_packet(winner_locked_read, 1)
            if winner_locked != b"L":
                protocol_ok = False
        if protocol_ok:
            os.write(contender_start_write, b"C")
            contender_packet = read_packet(contender_result_read, 5)
            if len(contender_packet) != 5:
                protocol_ok = False
            else:
                contender_outcome = cast(
                    StoreClassification | HarnessFailureCode | None,
                    _WRITER_PACKET_OUTCOMES.get(contender_packet[:1]),
                )
                raw_code = struct.unpack(">i", contender_packet[1:])[0]
                contender_code = raw_code if raw_code >= 0 else None
            _, contender_status = os.waitpid(contender_process_id, 0)
            live_processes.remove(contender_process_id)
            protocol_ok = (
                protocol_ok
                and os.WIFEXITED(contender_status)
                and os.WEXITSTATUS(contender_status) == 0
            )
        if protocol_ok:
            os.write(winner_release_write, b"R")
            winner_packet = read_packet(winner_result_read, 1)
            winner_outcome = cast(
                StoreClassification | HarnessFailureCode | None,
                _WRITER_PACKET_OUTCOMES.get(winner_packet),
            )
            _, winner_status = os.waitpid(winner_process_id, 0)
            live_processes.remove(winner_process_id)
            protocol_ok = (
                os.WIFEXITED(winner_status)
                and os.WEXITSTATUS(winner_status) == 0
                and winner_outcome is not None
            )
    except (OSError, ChildProcessError, struct.error):
        protocol_ok = False
    finally:
        for descriptor in parent_descriptors:
            with suppress(OSError):
                os.close(descriptor)
        for process_id in live_processes:
            with suppress(ProcessLookupError):
                os.kill(process_id, signal.SIGKILL)
        for process_id in live_processes:
            with suppress(ChildProcessError):
                os.waitpid(process_id, 0)

    after = verify_store(token)
    current = load_current(
        token,
        stream_id=value.record.stream_id,
        natural_key=natural_key,
    )
    expected_stream_delta = 1 if operation == "create" else 0
    state_ok = (
        after.stream_count == before.stream_count + expected_stream_delta
        and after.history_count == before.history_count + 1
        and current.classification is StoreClassification.FOUND
        and current.current == value
    )
    disposition = (
        EvidenceDisposition.PASS
        if protocol_ok
        and winner_outcome is expected_winner
        and contender_outcome is HarnessFailureCode.UNAVAILABLE
        and contender_code == sqlite3.SQLITE_BUSY
        and state_ok
        else EvidenceDisposition.FAIL
    )
    return WriterContentionEvidence(
        operation=operation,
        winner_outcome=winner_outcome,
        contender_outcome=contender_outcome,
        contender_sqlite_errorcode=contender_code,
        disposition=disposition,
        process_boundary="two-forked-overlapping-writers",
        stream_count=after.stream_count,
        history_count=after.history_count,
    )


@_operation_evidence_executor
def fresh_process_two_writer_evidence(
    token: StoreToken,
    *,
    operation: str,
    creations: tuple[
        ContinuousPublicTradeStreamStoredCreationV1,
        ContinuousPublicTradeStreamStoredCreationV1,
    ]
    | None = None,
    policy: ContinuousPublicTradePolicy | None = None,
    transitions: tuple[
        ContinuousPublicTradeStreamStoredTransitionV1,
        ContinuousPublicTradeStreamStoredTransitionV1,
    ]
    | None = None,
) -> TwoWriterEvidence:
    """Classify deterministic post-commit duplicate/conflict in two fresh processes."""

    if operation == "create":
        if creations is None or policy is None or transitions is not None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_values: tuple[
            ContinuousPublicTradeStreamStoredCreationV1
            | ContinuousPublicTradeStreamStoredTransitionV1,
            ...,
        ] = tuple(_validated_creation(item, policy) for item in creations)
        expected_first = StoreClassification.INSERTED
    elif operation == "compare_and_swap":
        if transitions is None or policy is not None or creations is not None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_values = tuple(_validated_transition(item) for item in transitions)
        expected_first = StoreClassification.UPDATED
    else:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if len(exact_values) != 2:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    before = verify_store(token)
    if not hasattr(os, "fork"):
        return TwoWriterEvidence(
            operation=operation,
            outcomes=(),
            disposition=EvidenceDisposition.UNPROVEN,
            process_boundary="unavailable",
            stream_count=before.stream_count,
            history_count=before.history_count,
        )

    try:
        opened_pipes = _open_pipes(len(exact_values) * 3)
    except OSError:
        return TwoWriterEvidence(
            operation=operation,
            outcomes=(),
            disposition=EvidenceDisposition.UNPROVEN,
            process_boundary="ipc_setup_failed",
            stream_count=before.stream_count,
            history_count=before.history_count,
        )
    pipes = tuple(
        tuple(opened_pipes[offset : offset + 3]) for offset in range(0, len(opened_pipes), 3)
    )
    all_descriptors = tuple(
        descriptor for ready, control, result in pipes for descriptor in (*ready, *control, *result)
    )
    children: list[tuple[int, int, int, int]] = []
    for value, (ready, control, result) in zip(exact_values, pipes, strict=True):
        ready_read, ready_write = ready
        control_read, control_write = control
        result_read, result_write = result
        try:
            process_id = os.fork()
        except OSError:
            _close_descriptors(all_descriptors)
            _terminate_and_reap_processes(
                tuple(child_process_id for child_process_id, _, _, _ in children)
            )
            return TwoWriterEvidence(
                operation=operation,
                outcomes=(),
                disposition=EvidenceDisposition.UNPROVEN,
                process_boundary="fork_spawn_failed",
                stream_count=before.stream_count,
                history_count=before.history_count,
            )
        if process_id == 0:
            retained_descriptors = {ready_write, control_read, result_write}
            for descriptor in all_descriptors:
                if descriptor not in retained_descriptors:
                    with suppress(OSError):
                        os.close(descriptor)
            packet = b"E"
            try:
                os.write(ready_write, b"R")
                if os.read(control_read, 1) != b"C":
                    os._exit(71)
                if operation == "create":
                    outcome: StoreClassification | HarnessFailureCode = create_stream(
                        token,
                        cast(ContinuousPublicTradeStreamStoredCreationV1, value),
                        cast(ContinuousPublicTradePolicy, policy),
                    ).classification
                else:
                    outcome = compare_and_swap_stream(
                        token,
                        cast(ContinuousPublicTradeStreamStoredTransitionV1, value),
                    ).classification
                packet = _WRITER_OUTCOME_PACKETS.get(outcome, b"E")
            except HarnessFailure as error:
                packet = _WRITER_OUTCOME_PACKETS.get(error.code, b"E")
            except BaseException:
                packet = b"E"
            with suppress(OSError):
                os.write(result_write, packet)
            os._exit(0 if packet != b"E" else 70)
        children.append((process_id, ready_read, control_write, result_read))
    for _, (ready, control, result) in zip(exact_values, pipes, strict=True):
        _, ready_write = ready
        control_read, _ = control
        _, result_write = result
        os.close(ready_write)
        os.close(control_read)
        os.close(result_write)

    outcomes: list[StoreClassification | HarnessFailureCode] = []
    protocol_ok = True
    live_processes = {process_id for process_id, _, _, _ in children}
    try:
        for _, ready_read, _, _ in children:
            selector = selectors.DefaultSelector()
            selector.register(ready_read, selectors.EVENT_READ)
            try:
                readiness_packet = os.read(ready_read, 1) if selector.select(timeout=10.0) else b""
            finally:
                selector.close()
                os.close(ready_read)
            protocol_ok = protocol_ok and readiness_packet == b"R"
        for process_id, _, control_write, result_read in children:
            if protocol_ok:
                os.write(control_write, b"C")
            os.close(control_write)
            selector = selectors.DefaultSelector()
            selector.register(result_read, selectors.EVENT_READ)
            try:
                packet = os.read(result_read, 1) if selector.select(timeout=10.0) else b""
            finally:
                selector.close()
                os.close(result_read)
            _, status = os.waitpid(process_id, 0)
            live_processes.remove(process_id)
            decoded_outcome = cast(
                StoreClassification | HarnessFailureCode | None,
                _WRITER_PACKET_OUTCOMES.get(packet),
            )
            if not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 0 or decoded_outcome is None:
                protocol_ok = False
            else:
                outcomes.append(decoded_outcome)
    finally:
        for _, _, control_write, result_read in children:
            with suppress(OSError):
                os.close(control_write)
            with suppress(OSError):
                os.close(result_read)
        for process_id in live_processes:
            with suppress(ProcessLookupError):
                os.kill(process_id, signal.SIGKILL)
        for process_id in live_processes:
            with suppress(ChildProcessError):
                os.waitpid(process_id, 0)

    after = verify_store(token)
    expected_delta = 1
    state_ok = (
        after.history_count == before.history_count + expected_delta
        and after.stream_count == before.stream_count + (1 if operation == "create" else 0)
    )
    disposition = (
        EvidenceDisposition.PASS
        if protocol_ok
        and len(outcomes) == 2
        and outcomes[0] is expected_first
        and outcomes[1] in {StoreClassification.DUPLICATE, StoreClassification.CONFLICT}
        and state_ok
        else EvidenceDisposition.FAIL
    )
    return TwoWriterEvidence(
        operation=operation,
        outcomes=tuple(outcomes),
        disposition=disposition,
        process_boundary="two-forked-sequential-classifiers",
        stream_count=after.stream_count,
        history_count=after.history_count,
    )


@_operation_evidence_executor
def fresh_process_kill_evidence(
    token: StoreToken,
    *,
    seam: str,
    creation: ContinuousPublicTradeStreamStoredCreationV1 | None = None,
    policy: ContinuousPublicTradePolicy | None = None,
    transition: ContinuousPublicTradeStreamStoredTransitionV1 | None = None,
    natural_key: bytes,
) -> FaultEvidence:
    """Kill a forked writer at one exact ordinary transaction seam."""

    is_create = creation is not None
    if is_create:
        if policy is None or transition is not None or seam not in _CREATE_KILL_SEAMS:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_creation = _validated_creation(
            cast(ContinuousPublicTradeStreamStoredCreationV1, creation),
            policy,
        )
        stream_id = exact_creation.record.stream_id
        expected_new_bytes = exact_creation.canonical_bytes
        prior_version = 0
        prior_envelope_digest = ""
        prior_history_root = ""
    else:
        if transition is None or policy is not None or seam not in _CAS_KILL_SEAMS:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_transition = _validated_transition(transition)
        stream_id = exact_transition.record.stream_id
        expected_new_bytes = exact_transition.canonical_bytes
        prior_version = exact_transition.record.prior_version
        prior_envelope_digest = exact_transition.record.prior_envelope_digest
        prior_history_root = exact_transition.record.prior_history_root
    decode_natural_identity_key(natural_key)
    _require_token(token)
    if not hasattr(os, "fork"):
        return FaultEvidence(
            seam=seam,
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="fork_unavailable",
        )

    try:
        ((read_descriptor, write_descriptor),) = _open_pipes(1)
    except OSError:
        return _unproven_process_fault(seam, reason="ipc_setup_failed")
    try:
        process_id = os.fork()
    except OSError:
        _close_descriptors((read_descriptor, write_descriptor))
        return _unproven_process_fault(seam, reason="fork_spawn_failed")
    if process_id == 0:
        os.close(read_descriptor)

        def child_hook(observed_seam: str) -> None:
            if observed_seam == seam:
                os.write(write_descriptor, b"R")
                while True:
                    signal.pause()

        try:
            if is_create:
                create_stream(
                    token,
                    cast(
                        ContinuousPublicTradeStreamStoredCreationV1,
                        creation,
                    ),
                    cast(ContinuousPublicTradePolicy, policy),
                    seam_hook=child_hook,
                )
            else:
                compare_and_swap_stream(
                    token,
                    cast(
                        ContinuousPublicTradeStreamStoredTransitionV1,
                        transition,
                    ),
                    seam_hook=child_hook,
                )
            os.write(write_descriptor, b"A")
            os._exit(0)
        except BaseException:
            with suppress(OSError):
                os.write(write_descriptor, b"E")
            os._exit(70)

    os.close(write_descriptor)
    selector = selectors.DefaultSelector()
    selector.register(read_descriptor, selectors.EVENT_READ)
    observed = b""
    try:
        events = selector.select(timeout=10.0)
        if events:
            observed = os.read(read_descriptor, 1)
        with suppress(ProcessLookupError):
            os.kill(process_id, signal.SIGKILL)
        os.waitpid(process_id, 0)
    finally:
        selector.close()
        os.close(read_descriptor)
    if observed != b"R":
        return FaultEvidence(
            seam=seam,
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=1 if observed == b"A" else 0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="seam_handshake_failed",
        )

    try:
        verify_store(token)
        current = load_current(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        if is_create:
            if current.classification is StoreClassification.NOT_FOUND:
                reopened = ReopenedState.OLD
            elif (
                current.classification is StoreClassification.FOUND
                and current.current is not None
                and current.current.canonical_bytes == expected_new_bytes
            ):
                reopened = ReopenedState.NEW
            else:
                reopened = ReopenedState.UNAVAILABLE
        elif current.classification is StoreClassification.FOUND and current.current is not None:
            if current.current.canonical_bytes == expected_new_bytes:
                reopened = ReopenedState.NEW
            elif (
                current.current.record.successor_version == prior_version
                and current.current.successor_envelope.envelope_digest == prior_envelope_digest
                and current.current.history_root == prior_history_root
            ):
                reopened = ReopenedState.OLD
            else:
                reopened = ReopenedState.UNAVAILABLE
        else:
            reopened = ReopenedState.UNAVAILABLE
    except HarnessFailure:
        reopened = ReopenedState.UNAVAILABLE

    expected = (
        ReopenedState.NEW if seam == "after_commit_before_acknowledgement" else ReopenedState.OLD
    )
    duplicate_proven = True
    if reopened is ReopenedState.NEW:
        if is_create:
            duplicate_proven = (
                create_stream(
                    token,
                    cast(
                        ContinuousPublicTradeStreamStoredCreationV1,
                        creation,
                    ),
                    cast(ContinuousPublicTradePolicy, policy),
                ).classification
                is StoreClassification.DUPLICATE
            )
        else:
            duplicate_proven = (
                compare_and_swap_stream(
                    token,
                    cast(
                        ContinuousPublicTradeStreamStoredTransitionV1,
                        transition,
                    ),
                ).classification
                is StoreClassification.DUPLICATE
            )
    disposition = (
        EvidenceDisposition.PASS
        if reopened is expected and duplicate_proven
        else EvidenceDisposition.FAIL
    )
    return FaultEvidence(
        seam=seam,
        disposition=disposition,
        sqlite_errorcode=None,
        observed_syscall="fork/SIGKILL",
        acknowledgement_bytes=0,
        reopened_state=reopened,
        reason=None if disposition is EvidenceDisposition.PASS else "state_mismatch",
    )


@_operation_evidence_executor
def true_during_commit_evidence(
    token: StoreToken,
    *,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    natural_key: bytes,
) -> FaultEvidence:
    """Stop a child inside COMMIT and kill before its WAL frame page write."""

    exact = _validated_transition(transition)
    decode_natural_identity_key(natural_key)
    _require_token(token)
    if not hasattr(os, "fork") or not hasattr(os, "uname") or os.uname().machine != "x86_64":
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="ptrace_platform_unavailable",
        )

    try:
        ready_pipe, control_pipe = _open_pipes(2)
    except OSError:
        return _unproven_process_fault(
            "true_during_commit",
            reason="ipc_setup_failed",
        )
    ready_read, ready_write = ready_pipe
    control_read, control_write = control_pipe
    try:
        process_id = os.fork()
    except OSError:
        _close_descriptors((ready_read, ready_write, control_read, control_write))
        return _unproven_process_fault(
            "true_during_commit",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        os.close(ready_read)
        os.close(control_write)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

        def commit_gate(observed_seam: str) -> None:
            if observed_seam == "between_current_update_and_compare_and_swap_commit":
                os.write(ready_write, b"R")
                if os.read(control_read, 1) != b"C":
                    os._exit(71)

        try:
            compare_and_swap_stream(
                token,
                exact,
                seam_hook=commit_gate,
            )
            os.write(ready_write, b"A")
            os._exit(0)
        except BaseException:
            with suppress(OSError):
                os.write(ready_write, b"E")
            os._exit(70)

    os.close(ready_write)
    os.close(control_read)
    selector = selectors.DefaultSelector()
    selector.register(ready_read, selectors.EVENT_READ)
    handshake = b""
    try:
        events = selector.select(timeout=10.0)
        if events:
            handshake = os.read(ready_read, 1)
    finally:
        selector.close()
        os.close(ready_read)
    if handshake != b"R":
        with suppress(ProcessLookupError):
            os.kill(process_id, signal.SIGKILL)
        os.waitpid(process_id, 0)
        os.close(control_write)
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=1 if handshake == b"A" else 0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="commit_gate_handshake_failed",
        )

    library = ctypes.CDLL(None, use_errno=True)
    library.ptrace.restype = ctypes.c_long
    seized = _ptrace_raw(
        library,
        _PTRACE_SEIZE,
        process_id,
        ctypes.c_void_p(),
        ctypes.c_void_p(_PTRACE_O_TRACESYSGOOD),
    )
    interrupted = (
        seized == 0
        and _ptrace_raw(
            library,
            _PTRACE_INTERRUPT,
            process_id,
            ctypes.c_void_p(),
            ctypes.c_void_p(),
        )
        == 0
    )
    if not interrupted:
        with suppress(ProcessLookupError):
            os.kill(process_id, signal.SIGKILL)
        os.waitpid(process_id, 0)
        os.close(control_write)
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="ptrace_seize_failed",
        )
    _, initial_status = os.waitpid(process_id, 0)
    if not os.WIFSTOPPED(initial_status):
        with suppress(ProcessLookupError):
            os.kill(process_id, signal.SIGKILL)
        os.waitpid(process_id, 0)
        os.close(control_write)
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="ptrace_initial_stop_failed",
        )

    os.write(control_write, b"C")
    os.close(control_write)
    wal_path = f"{_require_token(token).database_path}-wal"
    pending_header_offset: int | None = None
    completed_header_offset: int | None = None
    page_write_stopped = False
    for _ in range(20_000):
        if (
            _ptrace_raw(
                library,
                _PTRACE_SYSCALL,
                process_id,
                ctypes.c_void_p(),
                ctypes.c_void_p(),
            )
            != 0
        ):
            break
        _, status = os.waitpid(process_id, 0)
        if os.WIFEXITED(status) or os.WIFSIGNALED(status):
            break
        if not os.WIFSTOPPED(status):
            continue
        if os.WSTOPSIG(status) != (signal.SIGTRAP | 0x80):
            continue
        information = _PtraceSyscallInfo()
        received = _ptrace_raw(
            library,
            _PTRACE_GET_SYSCALL_INFO,
            process_id,
            ctypes.c_void_p(ctypes.sizeof(information)),
            ctypes.byref(information),
        )
        if received <= 0 or information.architecture != _AUDIT_ARCH_X86_64:
            continue
        if information.operation == _PTRACE_SYSCALL_INFO_ENTRY:
            entry = information.payload.entry
            if entry.number != _X86_64_PWRITE64:
                continue
            file_descriptor = int(entry.arguments[0])
            byte_count = int(entry.arguments[2])
            offset = int(entry.arguments[3])
            try:
                target = os.readlink(f"/proc/{process_id}/fd/{file_descriptor}")
            except OSError:
                continue
            if target != wal_path:
                continue
            if byte_count == 24:
                pending_header_offset = offset
            elif (
                completed_header_offset is not None
                and byte_count == PAGE_SIZE
                and offset == completed_header_offset + 24
            ):
                page_write_stopped = True
                break
        elif (
            information.operation == _PTRACE_SYSCALL_INFO_EXIT and pending_header_offset is not None
        ):
            if information.payload.exit.return_value == 24:
                completed_header_offset = pending_header_offset
            pending_header_offset = None

    with suppress(ProcessLookupError):
        os.kill(process_id, signal.SIGKILL)
    with suppress(ChildProcessError):
        os.waitpid(process_id, 0)
    if not page_write_stopped:
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="wal_commit_write_not_observed",
        )

    try:
        verify_store(token)
        current = load_current(
            token,
            stream_id=exact.record.stream_id,
            natural_key=natural_key,
        )
        if (
            current.classification is StoreClassification.FOUND
            and current.current is not None
            and current.current.canonical_bytes == exact.canonical_bytes
        ):
            reopened = ReopenedState.NEW
            duplicate = (
                compare_and_swap_stream(token, exact).classification
                is StoreClassification.DUPLICATE
            )
        elif (
            current.classification is StoreClassification.FOUND
            and current.current is not None
            and current.current.record.successor_version == exact.record.prior_version
            and current.current.successor_envelope.envelope_digest
            == exact.record.prior_envelope_digest
            and current.current.history_root == exact.record.prior_history_root
        ):
            reopened = ReopenedState.OLD
            duplicate = True
        else:
            reopened = ReopenedState.UNAVAILABLE
            duplicate = False
    except HarnessFailure:
        reopened = ReopenedState.UNAVAILABLE
        duplicate = False
    disposition = (
        EvidenceDisposition.PASS
        if reopened in {ReopenedState.OLD, ReopenedState.NEW} and duplicate
        else EvidenceDisposition.FAIL
    )
    return FaultEvidence(
        seam="true_during_commit",
        disposition=disposition,
        sqlite_errorcode=None,
        observed_syscall="pwrite64(wal-frame-header)/pwrite64(wal-page-entry)",
        acknowledgement_bytes=0,
        reopened_state=reopened,
        reason=None if disposition is EvidenceDisposition.PASS else "reopen_failed",
    )


@_operation_evidence_executor
def ioerr_write_evidence(
    token: StoreToken,
    *,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    natural_key: bytes,
) -> FaultEvidence:
    """Force an exact child-only WAL write failure and prove old-state recovery."""

    exact = _validated_transition(transition)
    decode_natural_identity_key(natural_key)
    identity = _require_token(token)
    if not hasattr(os, "fork") or not hasattr(resource, "RLIMIT_FSIZE"):
        return FaultEvidence(
            seam="sqlite_ioerr_write",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="rlimit_fsize_unavailable",
        )

    try:
        ((read_descriptor, write_descriptor),) = _open_pipes(1)
    except OSError:
        return _unproven_process_fault(
            "sqlite_ioerr_write",
            reason="ipc_setup_failed",
        )
    try:
        process_id = os.fork()
    except OSError:
        _close_descriptors((read_descriptor, write_descriptor))
        return _unproven_process_fault(
            "sqlite_ioerr_write",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        os.close(read_descriptor)
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        efbig_proven = False

        def arm_file_limit(observed_seam: str) -> None:
            nonlocal efbig_proven
            if observed_seam != "between_current_update_and_compare_and_swap_commit":
                return
            wal_path = Path(f"{identity.database_path}-wal")
            wal_size = wal_path.stat().st_size
            resource.setrlimit(resource.RLIMIT_FSIZE, (wal_size, wal_size))
            file_descriptor = os.open(
                wal_path,
                os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                try:
                    os.pwrite(file_descriptor, b"X", wal_size)
                except OSError as error:
                    efbig_proven = error.errno == errno.EFBIG
            finally:
                os.close(file_descriptor)

        try:
            compare_and_swap_stream(
                token,
                exact,
                seam_hook=arm_file_limit,
            )
            os.write(write_descriptor, b"A")
            os._exit(0)
        except HarnessFailure as error:
            code = error.sqlite_errorcode
            packet = (
                b"E"
                + struct.pack(">i", code if type(code) is int else -1)
                + (b"\x01" if efbig_proven else b"\x00")
            )
            with suppress(OSError):
                os.write(write_descriptor, packet)
            os._exit(0)
        except BaseException:
            with suppress(OSError):
                os.write(write_descriptor, b"X")
            os._exit(70)

    os.close(write_descriptor)
    selector = selectors.DefaultSelector()
    selector.register(read_descriptor, selectors.EVENT_READ)
    packet = b""
    try:
        events = selector.select(timeout=10.0)
        if events:
            packet = os.read(read_descriptor, 6)
    finally:
        selector.close()
        os.close(read_descriptor)
    if len(packet) != 6 or packet[:1] != b"E":
        with suppress(ProcessLookupError):
            os.kill(process_id, signal.SIGKILL)
        with suppress(ChildProcessError):
            os.waitpid(process_id, 0)
        return FaultEvidence(
            seam="sqlite_ioerr_write",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=1 if packet == b"A" else 0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="ioerr_child_protocol_failed",
        )
    os.waitpid(process_id, 0)
    sqlite_errorcode = struct.unpack(">i", packet[1:5])[0]
    efbig_proven = packet[5:] == b"\x01"
    try:
        verify_store(token)
        current = load_current(
            token,
            stream_id=exact.record.stream_id,
            natural_key=natural_key,
        )
        old_state = (
            current.classification is StoreClassification.FOUND
            and current.current is not None
            and current.current.record.successor_version == exact.record.prior_version
            and current.current.successor_envelope.envelope_digest
            == exact.record.prior_envelope_digest
            and current.current.history_root == exact.record.prior_history_root
        )
    except HarnessFailure:
        old_state = False
    disposition = (
        EvidenceDisposition.PASS
        if sqlite_errorcode == 778 and efbig_proven and old_state
        else EvidenceDisposition.FAIL
    )
    return FaultEvidence(
        seam="sqlite_ioerr_write",
        disposition=disposition,
        sqlite_errorcode=sqlite_errorcode,
        observed_syscall="pwrite64/EFBIG",
        acknowledgement_bytes=0,
        reopened_state=(ReopenedState.OLD if old_state else ReopenedState.UNAVAILABLE),
        reason=None if disposition is EvidenceDisposition.PASS else "ioerr_mismatch",
    )


@_operation_evidence_executor
def max_page_count_evidence(
    token: StoreToken,
    *,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
    natural_key: bytes,
) -> FaultEvidence:
    """Use a fresh process and SQLite's explicit page ceiling as disk-full evidence."""

    exact = _validated_transition(transition)
    decode_natural_identity_key(natural_key)
    before = verify_store(token)
    if before.freelist_count != 0:
        return FaultEvidence(
            seam="max_page_count",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="freelist_not_empty",
        )
    if not hasattr(os, "fork"):
        return FaultEvidence(
            seam="max_page_count",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="fork_unavailable",
        )
    try:
        ((read_descriptor, write_descriptor),) = _open_pipes(1)
    except OSError:
        return _unproven_process_fault(
            "max_page_count",
            reason="ipc_setup_failed",
        )
    try:
        process_id = os.fork()
    except OSError:
        _close_descriptors((read_descriptor, write_descriptor))
        return _unproven_process_fault(
            "max_page_count",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        os.close(read_descriptor)
        code = -1
        try:
            compare_and_swap_stream(
                token,
                exact,
                _test_max_page_count=before.page_count,
            )
        except HarnessFailure as error:
            code = error.sqlite_errorcode if type(error.sqlite_errorcode) is int else -1
        except BaseException:
            code = -1
        with suppress(OSError):
            os.write(write_descriptor, struct.pack(">i", code))
        os.close(write_descriptor)
        os._exit(0 if code >= 0 else 70)

    os.close(write_descriptor)
    selector = selectors.DefaultSelector()
    selector.register(read_descriptor, selectors.EVENT_READ)
    payload = b""
    try:
        if selector.select(timeout=10.0):
            payload = os.read(read_descriptor, 4)
        if len(payload) != 4:
            with suppress(ProcessLookupError):
                os.kill(process_id, signal.SIGKILL)
        _, status = os.waitpid(process_id, 0)
    finally:
        selector.close()
        os.close(read_descriptor)
    child_ok = len(payload) == 4 and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    sqlite_errorcode = struct.unpack(">i", payload)[0] if len(payload) == 4 else None
    try:
        after = verify_store(token)
        current = load_current(
            token,
            stream_id=exact.record.stream_id,
            natural_key=natural_key,
        )
        old_state = (
            after.history_count == before.history_count
            and current.classification is StoreClassification.FOUND
            and current.current is not None
            and current.current.record.successor_version == exact.record.prior_version
            and current.current.successor_envelope.envelope_digest
            == exact.record.prior_envelope_digest
            and current.current.history_root == exact.record.prior_history_root
        )
    except HarnessFailure:
        old_state = False
    disposition = (
        EvidenceDisposition.PASS
        if child_ok and sqlite_errorcode == sqlite3.SQLITE_FULL and old_state
        else EvidenceDisposition.FAIL
    )
    return FaultEvidence(
        seam="max_page_count",
        disposition=disposition,
        sqlite_errorcode=sqlite_errorcode,
        observed_syscall="forked-sqlite-page-allocation",
        acknowledgement_bytes=0,
        reopened_state=(ReopenedState.OLD if old_state else ReopenedState.UNAVAILABLE),
        reason=None if disposition is EvidenceDisposition.PASS else "full_not_observed",
    )


@_operation_evidence_executor
def wal_concurrency_evidence(
    token: StoreToken,
    *,
    transitions: Sequence[ContinuousPublicTradeStreamStoredTransitionV1],
    natural_key: bytes,
) -> WalConcurrencyEvidence:
    """Exercise fresh reader, writer, and checkpointer processes with finite IPC."""

    if not 1 <= len(transitions) <= 16:
        raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
    exact_transitions = tuple(_validated_transition(item) for item in transitions)
    decode_natural_identity_key(natural_key)
    stream_id = exact_transitions[0].record.stream_id
    if any(
        item.record.stream_id != stream_id
        or (
            index > 0
            and item.record.prior_version != exact_transitions[index - 1].record.successor_version
        )
        for index, item in enumerate(exact_transitions)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if not hasattr(os, "fork"):
        return WalConcurrencyEvidence(
            disposition=EvidenceDisposition.UNPROVEN,
            initial_version=0,
            reader_snapshot_version=0,
            final_version=0,
            writes=0,
            checkpoint_samples=(),
            maximum_wal_bytes=0,
            process_boundary="unavailable",
        )
    if _CONNECTION_PATH_SNAPSHOTS:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    try:
        (
            reader_control_pipe,
            reader_result_pipe,
            writer_control_pipe,
            writer_result_pipe,
            checkpoint_control_pipe,
            checkpoint_result_pipe,
        ) = _open_pipes(6)
    except OSError:
        return WalConcurrencyEvidence(
            disposition=EvidenceDisposition.UNPROVEN,
            initial_version=0,
            reader_snapshot_version=0,
            final_version=0,
            writes=0,
            checkpoint_samples=(),
            maximum_wal_bytes=0,
            process_boundary="ipc_setup_failed",
        )
    reader_control_read, reader_control_write = reader_control_pipe
    reader_result_read, reader_result_write = reader_result_pipe
    writer_control_read, writer_control_write = writer_control_pipe
    writer_result_read, writer_result_write = writer_result_pipe
    checkpoint_control_read, checkpoint_control_write = checkpoint_control_pipe
    checkpoint_result_read, checkpoint_result_write = checkpoint_result_pipe
    all_descriptors = (
        reader_control_read,
        reader_control_write,
        reader_result_read,
        reader_result_write,
        writer_control_read,
        writer_control_write,
        writer_result_read,
        writer_result_write,
        checkpoint_control_read,
        checkpoint_control_write,
        checkpoint_result_read,
        checkpoint_result_write,
    )
    live_processes: set[int] = set()

    def spawn_failed() -> WalConcurrencyEvidence:
        _close_descriptors(all_descriptors)
        if not _terminate_and_reap_processes(tuple(live_processes)):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        live_processes.clear()
        return WalConcurrencyEvidence(
            disposition=EvidenceDisposition.UNPROVEN,
            initial_version=0,
            reader_snapshot_version=0,
            final_version=0,
            writes=0,
            checkpoint_samples=(),
            maximum_wal_bytes=0,
            process_boundary="fork_spawn_failed",
        )

    try:
        reader_process_id = os.fork()
    except OSError:
        return spawn_failed()
    if reader_process_id > 0:
        live_processes.add(reader_process_id)
    if reader_process_id == 0:
        for descriptor in all_descriptors:
            if descriptor not in {reader_control_read, reader_result_write}:
                with suppress(OSError):
                    os.close(descriptor)
        reader: sqlite3.Connection | None = None
        try:
            reader, _ = _connect(token, writer=False)
            reader.execute("BEGIN").close()
            _verify_operation_snapshot(reader, token, writer=False)
            initial_row = _fetch_one(
                reader,
                """
                SELECT current_version
                FROM continuous_public_trade_stream
                INDEXED BY ux_cpt_stream_uuid
                WHERE stream_uuid = ?
                """,
                (stream_id.bytes,),
            )
            initial_version = _require_exact_int(
                initial_row["current_version"],
                minimum=1,
                maximum=MAX_CONTRACT_INTEGER,
            )
            os.write(reader_result_write, b"R" + struct.pack(">q", initial_version))
            if os.read(reader_control_read, 1) != b"Q":
                os._exit(71)
            retained_reader_row = _fetch_one(
                reader,
                """
                SELECT current_version
                FROM continuous_public_trade_stream
                INDEXED BY ux_cpt_stream_uuid
                WHERE stream_uuid = ?
                """,
                (stream_id.bytes,),
            )
            retained_version = _require_exact_int(
                retained_reader_row["current_version"],
                minimum=1,
                maximum=MAX_CONTRACT_INTEGER,
            )
            _verify_operation_authority(reader, token)
            reader.execute("COMMIT").close()
            _close_checked(reader)
            reader = None
            os.write(reader_result_write, b"S" + struct.pack(">q", retained_version))
            os._exit(0)
        except BaseException:
            if reader is not None:
                _rollback_best_effort(reader)
                _close_best_effort(reader)
            with suppress(OSError):
                os.write(reader_result_write, b"E")
            os._exit(70)

    try:
        writer_process_id = os.fork()
    except OSError:
        return spawn_failed()
    if writer_process_id > 0:
        live_processes.add(writer_process_id)
    if writer_process_id == 0:
        for descriptor in all_descriptors:
            if descriptor not in {writer_control_read, writer_result_write}:
                with suppress(OSError):
                    os.close(descriptor)
        try:
            os.write(writer_result_write, b"R")
            for item in exact_transitions:
                if os.read(writer_control_read, 1) != b"W":
                    os._exit(71)
                outcome = compare_and_swap_stream(token, item).classification
                if outcome is not StoreClassification.UPDATED:
                    os._exit(72)
                os.write(writer_result_write, b"W")
            os._exit(0)
        except BaseException:
            with suppress(OSError):
                os.write(writer_result_write, b"E")
            os._exit(70)

    try:
        checkpoint_process_id = os.fork()
    except OSError:
        return spawn_failed()
    if checkpoint_process_id > 0:
        live_processes.add(checkpoint_process_id)
    if checkpoint_process_id == 0:
        for descriptor in all_descriptors:
            if descriptor not in {checkpoint_control_read, checkpoint_result_write}:
                with suppress(OSError):
                    os.close(descriptor)
        checkpointer: sqlite3.Connection | None = None
        try:
            checkpointer, _ = _connect(token, writer=True)
            os.write(checkpoint_result_write, b"R")
            for _ in exact_transitions:
                if os.read(checkpoint_control_read, 1) != b"P":
                    os._exit(71)
                _verify_operation_authority(checkpointer, token)
                checkpoint_row = tuple(_fetch_one(checkpointer, "PRAGMA wal_checkpoint(PASSIVE)"))
                _verify_operation_authority(checkpointer, token)
                if len(checkpoint_row) != 3 or any(
                    type(value) is not int for value in checkpoint_row
                ):
                    os._exit(72)
                wal_bytes = _operation_file_size(
                    checkpointer,
                    token,
                    f"{_DATABASE_BASENAME}-wal",
                    required=False,
                    maximum=MAX_TEST_WAL_BYTES,
                )
                packet = (*cast(tuple[int, int, int], checkpoint_row), wal_bytes)
                os.write(checkpoint_result_write, struct.pack(">qqqq", *packet))
            if os.read(checkpoint_control_read, 1) != b"T":
                os._exit(73)
            _verify_operation_authority(checkpointer, token)
            child_truncated = tuple(_fetch_one(checkpointer, "PRAGMA wal_checkpoint(TRUNCATE)"))
            _verify_operation_authority(checkpointer, token)
            if len(child_truncated) != 3 or any(
                type(value) is not int for value in child_truncated
            ):
                os._exit(74)
            _close_checked(checkpointer)
            checkpointer = None
            os.write(
                checkpoint_result_write,
                b"T" + struct.pack(">qqq", *cast(tuple[int, int, int], child_truncated)),
            )
            os._exit(0)
        except BaseException:
            _close_best_effort(checkpointer)
            with suppress(OSError):
                os.write(checkpoint_result_write, b"E")
            os._exit(70)

    _close_descriptors(
        (
            reader_control_read,
            reader_result_write,
            writer_control_read,
            writer_result_write,
            checkpoint_control_read,
            checkpoint_result_write,
        )
    )
    samples: list[tuple[int, int, int]] = []
    maximum_wal_bytes = 0
    initial_version = 0
    reader_snapshot_version = 0
    acknowledged_writes = 0
    truncated: tuple[int, int, int] = (-1, -1, -1)
    protocol_ok = True
    try:
        reader_ready = _read_process_packet("reader_ready", reader_result_read, 9)
        writer_ready = _read_process_packet("writer_ready", writer_result_read, 1)
        checkpoint_ready = _read_process_packet(
            "checkpoint_ready",
            checkpoint_result_read,
            1,
        )
        if (
            len(reader_ready) != 9
            or reader_ready[:1] != b"R"
            or writer_ready != b"R"
            or checkpoint_ready != b"R"
        ):
            protocol_ok = False
        else:
            initial_version = struct.unpack(">q", reader_ready[1:])[0]
        for _ in exact_transitions:
            if not protocol_ok:
                break
            os.write(writer_control_write, b"W")
            if _read_process_packet("writer_ack", writer_result_read, 1) != b"W":
                protocol_ok = False
                break
            acknowledged_writes += 1
            os.write(checkpoint_control_write, b"P")
            payload = _read_process_packet(
                "checkpoint_sample",
                checkpoint_result_read,
                32,
            )
            if len(payload) != 32:
                protocol_ok = False
                break
            busy, log, checkpointed, wal_bytes = struct.unpack(">qqqq", payload)
            if (
                busy not in {0, 1}
                or log < 0
                or checkpointed < 0
                or checkpointed > log
                or not 0 <= wal_bytes <= MAX_TEST_WAL_BYTES
            ):
                protocol_ok = False
                break
            samples.append((busy, log, checkpointed))
            maximum_wal_bytes = max(maximum_wal_bytes, wal_bytes)
        if protocol_ok:
            writer_status = _wait_for_owned_process(writer_process_id)
            if writer_status is not None:
                live_processes.discard(writer_process_id)
                protocol_ok = os.WIFEXITED(writer_status) and os.WEXITSTATUS(writer_status) == 0
            else:
                protocol_ok = False
        if protocol_ok:
            os.write(reader_control_write, b"Q")
            reader_result = _read_process_packet(
                "reader_snapshot",
                reader_result_read,
                9,
            )
            if len(reader_result) == 9 and reader_result[:1] == b"S":
                reader_snapshot_version = struct.unpack(">q", reader_result[1:])[0]
            else:
                protocol_ok = False
        if protocol_ok:
            reader_status = _wait_for_owned_process(reader_process_id)
            if reader_status is not None:
                live_processes.discard(reader_process_id)
                protocol_ok = os.WIFEXITED(reader_status) and os.WEXITSTATUS(reader_status) == 0
            else:
                protocol_ok = False
        if protocol_ok:
            os.write(checkpoint_control_write, b"T")
            truncate_result = _read_process_packet(
                "checkpoint_truncate",
                checkpoint_result_read,
                25,
            )
            if len(truncate_result) == 25 and truncate_result[:1] == b"T":
                truncated = cast(
                    tuple[int, int, int],
                    struct.unpack(">qqq", truncate_result[1:]),
                )
            else:
                protocol_ok = False
        if protocol_ok:
            checkpoint_status = _wait_for_owned_process(checkpoint_process_id)
            if checkpoint_status is not None:
                live_processes.discard(checkpoint_process_id)
                protocol_ok = (
                    os.WIFEXITED(checkpoint_status) and os.WEXITSTATUS(checkpoint_status) == 0
                )
            else:
                protocol_ok = False
    except (OSError, ChildProcessError, struct.error):
        protocol_ok = False
    finally:
        _close_descriptors(all_descriptors)
        cleanup_ok = _terminate_and_reap_processes(tuple(live_processes))
        live_processes.clear()
        if not cleanup_ok:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    final = load_current(
        token,
        stream_id=stream_id,
        natural_key=natural_key,
    )
    final_version = (
        final.current.record.successor_version
        if final.classification is StoreClassification.FOUND and final.current is not None
        else 0
    )
    verify_store(token)
    wal_growth_observed = bool(samples) and (
        samples[-1][1] > samples[0][1] if len(samples) > 1 else samples[0][1] > 0
    )
    disposition = (
        EvidenceDisposition.PASS
        if protocol_ok
        and initial_version == exact_transitions[0].record.prior_version
        and reader_snapshot_version == initial_version
        and final_version == exact_transitions[-1].record.successor_version
        and acknowledged_writes == len(exact_transitions)
        and len(samples) == len(exact_transitions)
        and wal_growth_observed
        and 0 < maximum_wal_bytes <= MAX_TEST_WAL_BYTES
        and truncated == (0, 0, 0)
        else EvidenceDisposition.FAIL
    )
    return WalConcurrencyEvidence(
        disposition=disposition,
        initial_version=initial_version,
        reader_snapshot_version=reader_snapshot_version,
        final_version=final_version,
        writes=acknowledged_writes,
        checkpoint_samples=tuple(samples),
        maximum_wal_bytes=maximum_wal_bytes,
        process_boundary="three-forked-role-children",
    )


def _creation_from_stream_row_unchecked(
    stream: sqlite3.Row,
) -> ContinuousPublicTradeStreamStoredCreationV1:
    record_bytes = _exact_blob(
        stream["creation_record_canonical_bytes"],
        maximum=65_536,
    )
    record = decode_stream_creation_record(record_bytes)
    record_digest = stream_creation_digest(record)
    envelope_bytes = bytes.fromhex(record.successor_envelope_hex)
    envelope = decode_stream_envelope(envelope_bytes)
    envelope_digest = stream_envelope_digest(envelope)
    history_root = initial_stream_history_root(record)
    if (
        encode_stream_creation_record(record) != record_bytes
        or _decode_digest(stream["creation_record_digest"]) != record_digest
        or _decode_digest(stream["creation_history_root"]) != history_root
        or record.successor_envelope_digest != envelope_digest
        or encode_stream_envelope(envelope) != envelope_bytes
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creation = _stored_creation_without_scope(
        creation=record,
        record_bytes=record_bytes,
        record_digest=record_digest,
        envelope_bytes=envelope_bytes,
        envelope_digest=envelope_digest,
        history_root=history_root,
    )
    _validate_stream_projection(stream, creation)
    return creation


def _creation_from_stream_row(
    stream: sqlite3.Row,
) -> ContinuousPublicTradeStreamStoredCreationV1:
    try:
        return _creation_from_stream_row_unchecked(stream)
    except HarnessFailure:
        raise
    except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None


def _validate_stream_witnesses_unchecked(
    stream: sqlite3.Row,
) -> tuple[
    ContinuousPublicTradeStreamStoredCreationV1,
    ContinuousPublicTradeStreamEnvelopeV1,
]:
    """Validate stream-row witnesses without an extra logical history query."""

    creation = _creation_from_stream_row(stream)
    current_version = _require_exact_int(
        stream["current_version"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    current_record_bytes = _exact_blob(
        stream["current_record_canonical_bytes"],
        maximum=65_536,
    )
    current_envelope_bytes = _exact_blob(
        stream["current_envelope_canonical_bytes"],
        maximum=16_384,
    )
    current_envelope = decode_stream_envelope(current_envelope_bytes)
    current_envelope_digest = stream_envelope_digest(current_envelope)
    if (
        encode_stream_envelope(current_envelope) != current_envelope_bytes
        or current_envelope.checkpoint.version != current_version
        or current_envelope.checkpoint.stream_id.bytes != stream["stream_uuid"]
        or _decode_digest(stream["current_envelope_digest"]) != current_envelope_digest
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if current_version == 1:
        if (
            current_record_bytes != creation.canonical_bytes
            or _decode_digest(stream["current_record_digest"]) != creation.record_digest
            or _decode_digest(stream["current_history_root"]) != creation.history_root
            or current_envelope_bytes != creation.successor_envelope.canonical_bytes
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return creation, current_envelope
    transition = decode_stream_transition_record(current_record_bytes)
    record_digest = stream_transition_digest(transition)
    history_root = next_stream_history_root(
        transition.prior_history_root,
        transition,
    )
    if (
        encode_stream_transition_record(transition) != current_record_bytes
        or transition.stream_id.bytes != stream["stream_uuid"]
        or transition.successor_version != current_version
        or transition.prior_version != current_version - 1
        or transition.successor_envelope_hex != current_envelope_bytes.hex()
        or transition.successor_envelope_digest != current_envelope_digest
        or _decode_digest(stream["current_record_digest"]) != record_digest
        or _decode_digest(stream["current_history_root"]) != history_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return creation, current_envelope


def _validate_stream_witnesses(
    stream: sqlite3.Row,
) -> tuple[
    ContinuousPublicTradeStreamStoredCreationV1,
    ContinuousPublicTradeStreamEnvelopeV1,
]:
    try:
        return _validate_stream_witnesses_unchecked(stream)
    except HarnessFailure:
        raise
    except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None


def _validate_history_page_rows(
    stream: sqlite3.Row,
    rows: Sequence[sqlite3.Row],
    *,
    preceding_row: sqlite3.Row | None = None,
) -> tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...]:
    policy = _policy_from_creation(_creation_from_stream_row(stream))
    entries: list[ContinuousPublicTradeStreamStoredHistoryEntryV1] = []
    prior_row = preceding_row
    expected_stream_row_id = stream["stream_row_id"]
    expected_stream_uuid = stream["stream_uuid"]
    previous_version = (
        None
        if preceding_row is None
        else _require_exact_int(
            preceding_row["successor_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
    )
    for row in rows:
        version = _require_exact_int(
            row["successor_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        if row["stream_row_id"] != expected_stream_row_id or (
            previous_version is not None and version != previous_version + 1
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entry = _entry_from_history_row(
            row,
            policy=policy,
            predecessor=prior_row,
        )
        if entry.record.stream_id.bytes != expected_stream_uuid:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entries.append(entry)
        prior_row = row
        previous_version = version
    return tuple(entries)


def _validate_tail_binding(
    stream: sqlite3.Row,
    entry: ContinuousPublicTradeStreamStoredHistoryEntryV1,
) -> None:
    if (
        entry.record.successor_version
        != _require_exact_int(
            stream["current_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        or entry.canonical_bytes
        != _exact_blob(stream["current_record_canonical_bytes"], maximum=65_536)
        or entry.record_digest != _decode_digest(stream["current_record_digest"])
        or entry.successor_envelope.canonical_bytes
        != _exact_blob(stream["current_envelope_canonical_bytes"], maximum=16_384)
        or entry.successor_envelope.envelope_digest
        != _decode_digest(stream["current_envelope_digest"])
        or entry.history_root != _decode_digest(stream["current_history_root"])
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def audit_bounds(
    *,
    current_version: int,
    limit: int,
    continuation_version: int | None,
) -> AuditBounds:
    """Compute an exact page bound without evaluating an overflowing sum."""

    current = _require_exact_int(
        current_version,
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    exact_limit = _require_exact_int(limit, minimum=1, maximum=100)
    if continuation_version is None:
        new_rows = min(current, exact_limit)
        return AuditBounds(
            low_version=1,
            high_version=new_rows,
            row_limit=new_rows,
            overlap_count=0,
        )
    continuation = _require_exact_int(
        continuation_version,
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    if continuation > current:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    remaining = current - continuation
    new_rows = min(remaining, exact_limit)
    return AuditBounds(
        low_version=continuation,
        high_version=continuation + new_rows,
        row_limit=1 + new_rows,
        overlap_count=1,
    )


@_operation_evidence_executor
def audit_history(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
    limit: int,
    continuation: tuple[int, str, str] | None = None,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
) -> AuditSlice:
    """Return a finite page with exactly the contractually required overlap."""

    if type(stream_id) is not UUID:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decode_natural_identity_key(natural_key)
    exact_limit = _require_exact_int(limit, minimum=1, maximum=100)
    exact_expectation = (
        None
        if expectation is None
        else ContinuousPublicTradeStreamExpectationV1.revalidate_at_boundary(expectation)
    )
    if continuation is not None:
        if type(continuation) is not tuple or len(continuation) != 3:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        through_version = _require_exact_int(
            continuation[0],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        through_envelope_digest = continuation[1]
        through_history_root = continuation[2]
        _digest_bytes(through_envelope_digest)
        _digest_bytes(through_history_root)
    else:
        through_version = 0
        through_envelope_digest = ""
        through_history_root = ""

    connection, _ = _connect(token, writer=False)
    statements = (
        "BEGIN",
        "identity candidates LIMIT 3",
        (
            "audit initial LIMIT requested"
            if continuation is None
            else "audit continuation LIMIT requested+1"
        ),
        "COMMIT",
    )
    conflict_statements = (
        "BEGIN",
        "identity candidates LIMIT 3",
        "audit identity current history LIMIT 3 per identity candidate",
        "COMMIT",
    )
    stream_rows = 0
    history_rows = 0
    decoded_rows = 0
    try:
        connection.execute("BEGIN").close()
        _verify_operation_snapshot(connection, token, writer=False)
        candidates = _identity_rows(
            connection,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        stream_rows = len(candidates)
        if not candidates:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.NOT_FOUND,
                (),
                0,
                0,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        if len(candidates) != 1:
            _, history_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            decoded_rows = history_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.IDENTITY_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(
                    conflict_statements,
                    stream_rows,
                    history_rows,
                    decoded_rows,
                ),
            )
        stream = candidates[0]
        creation, current_envelope = _validate_stream_witnesses(stream)
        if (
            stream["stream_uuid"] != stream_id.bytes
            or stream["natural_identity_key"] != natural_key
        ):
            _, history_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            decoded_rows = history_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.IDENTITY_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(
                    conflict_statements,
                    stream_rows,
                    history_rows,
                    decoded_rows,
                ),
            )
        if exact_expectation is not None and not _expectation_matches_retained(
            exact_expectation,
            creation,
            current_envelope,
        ):
            _, history_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            decoded_rows = history_rows
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.IDENTITY_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(
                    conflict_statements,
                    stream_rows,
                    history_rows,
                    decoded_rows,
                ),
            )
        stream_row_id = _require_exact_int(
            stream["stream_row_id"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        current_version = _require_exact_int(
            stream["current_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        try:
            bounds = audit_bounds(
                current_version=current_version,
                limit=exact_limit,
                continuation_version=(None if continuation is None else through_version),
            )
        except HarnessFailure:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.ANCHOR_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        if continuation is None:
            rows = _fetch_all(
                connection,
                """
                SELECT *
                FROM continuous_public_trade_history
                INDEXED BY ux_cpt_history_stream_version
                WHERE stream_row_id = ?
                  AND successor_version >= ?
                  AND successor_version <= ?
                ORDER BY successor_version
                LIMIT ?
                """,
                (
                    stream_row_id,
                    bounds.low_version,
                    bounds.high_version,
                    bounds.row_limit,
                ),
            )
            overlap_count = bounds.overlap_count
        else:
            rows = _fetch_all(
                connection,
                """
                SELECT *
                FROM continuous_public_trade_history
                INDEXED BY ux_cpt_history_stream_version
                WHERE stream_row_id = ?
                  AND successor_version >= ?
                  AND successor_version <= ?
                ORDER BY successor_version
                LIMIT ?
                """,
                (
                    stream_row_id,
                    bounds.low_version,
                    bounds.high_version,
                    bounds.row_limit,
                ),
            )
            overlap_count = bounds.overlap_count
        history_rows = len(rows)
        if not rows:
            if continuation is None:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.ANCHOR_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        if (
            history_rows != bounds.row_limit
            or bounds.row_limit != bounds.high_version - bounds.low_version + 1
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entries = _validate_history_page_rows(stream, rows)
        decoded_rows = len(entries)
        first = entries[0]
        if (
            first.record.successor_version != bounds.low_version
            or entries[-1].record.successor_version != bounds.high_version
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if continuation is None:
            if (
                first.record.successor_version != 1
                or first.canonical_bytes != creation.canonical_bytes
                or first.record_digest != creation.record_digest
                or first.successor_envelope.canonical_bytes
                != creation.successor_envelope.canonical_bytes
                or first.successor_envelope.envelope_digest
                != creation.successor_envelope.envelope_digest
                or first.history_root != creation.history_root
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        elif (
            first.record.successor_version != through_version
            or first.successor_envelope.envelope_digest != through_envelope_digest
            or first.history_root != through_history_root
        ):
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            return AuditSlice(
                StoreClassification.ANCHOR_CONFLICT,
                (),
                0,
                0,
                QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
            )
        if bounds.high_version == current_version:
            _validate_tail_binding(stream, entries[-1])
        new_count = len(entries) - overlap_count
        if continuation is not None and new_count == 0:
            classification = StoreClassification.AT_TAIL
        else:
            classification = StoreClassification.PAGE
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        return AuditSlice(
            classification,
            entries,
            overlap_count,
            new_count,
            QueryEvidence(statements, stream_rows, history_rows, decoded_rows),
        )
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _rollback_best_effort(connection)
        raise failure from None
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


@dataclass(frozen=True, slots=True)
class TestOnlySQLiteStreamStorePrototype:
    """TASK-062-shaped facade over one pytest-owned TASK-064 generation.

    This class exists only to prove the frozen port boundary.  It is not imported by production
    source, does not choose a path, and grants no runtime or retry authority.
    """

    token: StoreToken

    def create(
        self,
        command: ContinuousPublicTradeStreamCreateCommandV1,
        /,
    ) -> ContinuousPublicTradeStreamCreateResultV1:
        exact = ContinuousPublicTradeStreamCreateCommandV1.revalidate_at_boundary(command)
        stream_id = exact.expectation.identity.stream_id
        try:
            evidence = create_stream(
                self.token,
                exact.creation,
                exact.expectation.effective_stream_policy,
            )
        except MemoryError:
            return ContinuousPublicTradeStreamCreateUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCreateOutcome.UNAVAILABLE,
            )
        except HarnessFailure as error:
            if error.code is HarnessFailureCode.UNSUPPORTED_VERSION:
                return ContinuousPublicTradeStreamCreateRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamCreateOutcome.UNSUPPORTED_VERSION,
                )
            if error.code is HarnessFailureCode.CORRUPT:
                return ContinuousPublicTradeStreamCreateRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamCreateOutcome.CORRUPT,
                )
            return ContinuousPublicTradeStreamCreateUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCreateOutcome.UNAVAILABLE,
            )
        if evidence.classification in {
            StoreClassification.INSERTED,
            StoreClassification.DUPLICATE,
        }:
            receipt = ContinuousPublicTradeStreamCreateReceiptV1(creation=exact.creation)
            if evidence.classification is StoreClassification.INSERTED:
                return ContinuousPublicTradeStreamCreateAcceptedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamCreateOutcome.INSERTED,
                    receipt=receipt,
                )
            return ContinuousPublicTradeStreamCreateAcceptedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCreateOutcome.DUPLICATE,
                receipt=receipt,
            )
        return ContinuousPublicTradeStreamCreateRejectedResultV1(
            stream_id=stream_id,
            outcome=(
                ContinuousPublicTradeStreamCreateOutcome.CONFLICT
                if evidence.classification is StoreClassification.CONFLICT
                else ContinuousPublicTradeStreamCreateOutcome.CORRUPT
            ),
        )

    def load_current(
        self,
        query: ContinuousPublicTradeStreamLoadQueryV1,
        /,
    ) -> ContinuousPublicTradeStreamLoadResultV1:
        exact = ContinuousPublicTradeStreamLoadQueryV1.revalidate_at_boundary(query)
        expectation = exact.expectation
        stream_id = expectation.identity.stream_id
        try:
            result = load_current(
                self.token,
                stream_id=stream_id,
                natural_key=_natural_key_from_expectation(expectation),
                expectation=expectation,
            )
        except MemoryError:
            return ContinuousPublicTradeStreamLoadUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.UNAVAILABLE,
            )
        except HarnessFailure as error:
            if error.code is HarnessFailureCode.UNSUPPORTED_VERSION:
                return ContinuousPublicTradeStreamLoadRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamLoadOutcome.UNSUPPORTED_VERSION,
                )
            if error.code is HarnessFailureCode.CORRUPT:
                return ContinuousPublicTradeStreamLoadRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
                )
            return ContinuousPublicTradeStreamLoadUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.UNAVAILABLE,
            )
        if result.classification is StoreClassification.NOT_FOUND:
            return ContinuousPublicTradeStreamLoadNotFoundResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.NOT_FOUND,
            )
        if result.classification is StoreClassification.IDENTITY_CONFLICT:
            return ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.IDENTITY_CONFLICT,
            )
        if (
            result.classification is not StoreClassification.FOUND
            or result.creation is None
            or result.current is None
        ):
            return ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
            )
        try:
            view = ContinuousPublicTradeStreamCurrentViewV1(
                creation=result.creation,
                predecessor=result.predecessor,
                current=result.current,
            )
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            return ContinuousPublicTradeStreamLoadRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamLoadOutcome.CORRUPT,
            )
        return ContinuousPublicTradeStreamLoadFoundResultV1(
            stream_id=stream_id,
            outcome=ContinuousPublicTradeStreamLoadOutcome.FOUND,
            view=view,
        )

    def compare_and_swap(
        self,
        command: ContinuousPublicTradeStreamCompareAndSwapCommandV1,
        /,
    ) -> ContinuousPublicTradeStreamCompareAndSwapResultV1:
        exact = ContinuousPublicTradeStreamCompareAndSwapCommandV1.revalidate_at_boundary(command)
        stream_id = exact.expectation.identity.stream_id
        try:
            evidence = compare_and_swap_stream(
                self.token,
                exact.transition,
                expectation=exact.expectation,
            )
        except MemoryError:
            return ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCompareAndSwapOutcome.UNAVAILABLE,
            )
        except HarnessFailure as error:
            if error.code is HarnessFailureCode.UNSUPPORTED_VERSION:
                return ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                    stream_id=stream_id,
                    outcome=(ContinuousPublicTradeStreamCompareAndSwapOutcome.UNSUPPORTED_VERSION),
                )
            if error.code is HarnessFailureCode.CORRUPT:
                return ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT,
                )
            return ContinuousPublicTradeStreamCompareAndSwapUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCompareAndSwapOutcome.UNAVAILABLE,
            )
        if evidence.classification in {
            StoreClassification.UPDATED,
            StoreClassification.DUPLICATE,
        }:
            receipt = ContinuousPublicTradeStreamCompareAndSwapReceiptV1(
                transition=exact.transition
            )
            if evidence.classification is StoreClassification.UPDATED:
                return ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(
                    stream_id=stream_id,
                    outcome=(ContinuousPublicTradeStreamCompareAndSwapOutcome.UPDATED),
                    receipt=receipt,
                )
            return ContinuousPublicTradeStreamCompareAndSwapAcceptedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamCompareAndSwapOutcome.DUPLICATE,
                receipt=receipt,
            )
        return ContinuousPublicTradeStreamCompareAndSwapRejectedResultV1(
            stream_id=stream_id,
            outcome=(
                ContinuousPublicTradeStreamCompareAndSwapOutcome.CONFLICT
                if evidence.classification is StoreClassification.CONFLICT
                else ContinuousPublicTradeStreamCompareAndSwapOutcome.CORRUPT
            ),
        )

    def audit_page(
        self,
        query: ContinuousPublicTradeStreamAuditQueryV1,
        /,
    ) -> ContinuousPublicTradeStreamAuditResultV1:
        exact: ContinuousPublicTradeStreamAuditQueryV1
        if type(query) is ContinuousPublicTradeStreamAuditStartQueryV1:
            exact = ContinuousPublicTradeStreamAuditStartQueryV1.revalidate_at_boundary(query)
            continuation: tuple[int, str, str] | None = None
        elif type(query) is ContinuousPublicTradeStreamAuditContinuationQueryV1:
            exact = ContinuousPublicTradeStreamAuditContinuationQueryV1.revalidate_at_boundary(
                query
            )
            anchor = exact.continuation
            continuation = (
                anchor.through_version,
                anchor.through_envelope_digest,
                anchor.through_history_root,
            )
        else:
            ContinuousPublicTradeStreamAuditStartQueryV1.revalidate_at_boundary(query)
            raise AssertionError("unreachable audit query type")
        expectation = exact.expectation
        stream_id = expectation.identity.stream_id
        try:
            result = audit_history(
                self.token,
                stream_id=stream_id,
                natural_key=_natural_key_from_expectation(expectation),
                limit=exact.limit,
                continuation=continuation,
                expectation=expectation,
            )
        except MemoryError:
            return ContinuousPublicTradeStreamAuditUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.UNAVAILABLE,
            )
        except HarnessFailure as error:
            if error.code is HarnessFailureCode.UNSUPPORTED_VERSION:
                return ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamAuditOutcome.UNSUPPORTED_VERSION,
                )
            if error.code is HarnessFailureCode.CORRUPT:
                return ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
                )
            return ContinuousPublicTradeStreamAuditUnavailableResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.UNAVAILABLE,
            )
        if result.classification is StoreClassification.NOT_FOUND:
            return ContinuousPublicTradeStreamAuditNotFoundResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.NOT_FOUND,
            )
        if result.classification in {
            StoreClassification.IDENTITY_CONFLICT,
            StoreClassification.ANCHOR_CONFLICT,
        }:
            if result.classification is StoreClassification.IDENTITY_CONFLICT:
                return ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=stream_id,
                    outcome=(ContinuousPublicTradeStreamAuditOutcome.IDENTITY_CONFLICT),
                )
            return ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.ANCHOR_CONFLICT,
            )
        if result.classification is StoreClassification.AT_TAIL:
            if type(exact) is not ContinuousPublicTradeStreamAuditContinuationQueryV1:
                return ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
                )
            return ContinuousPublicTradeStreamAuditAtTailResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.AT_TAIL,
                continuation=exact.continuation,
            )
        if result.classification is not StoreClassification.PAGE or result.new_count < 1:
            return ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
            )
        if type(exact) is ContinuousPublicTradeStreamAuditStartQueryV1:
            predecessor = None
            records = result.entries
        else:
            if result.overlap_count != 1 or len(result.entries) < 2:
                return ContinuousPublicTradeStreamAuditRejectedResultV1(
                    stream_id=stream_id,
                    outcome=ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
                )
            predecessor = result.entries[0]
            records = result.entries[1:]
        final = records[-1]
        try:
            page = ContinuousPublicTradeStreamAuditPageV1(
                predecessor_overlap=predecessor,
                records=records,
                continuation=ContinuousPublicTradeStreamAuditContinuationV1(
                    stream_id=stream_id,
                    through_version=final.record.successor_version,
                    through_envelope_digest=(final.successor_envelope.envelope_digest),
                    through_history_root=final.history_root,
                ),
            )
            validate_continuous_public_trade_stream_audit_page(exact, page)
        except (ValueError, TypeError, AttributeError, OverflowError, RecursionError):
            return ContinuousPublicTradeStreamAuditRejectedResultV1(
                stream_id=stream_id,
                outcome=ContinuousPublicTradeStreamAuditOutcome.CORRUPT,
            )
        return ContinuousPublicTradeStreamAuditPageResultV1(
            stream_id=stream_id,
            outcome=ContinuousPublicTradeStreamAuditOutcome.PAGE,
            page=page,
        )


@_operation_evidence_executor
def query_plan_evidence(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> dict[str, tuple[str, ...]]:
    """Capture pinned planner evidence at the two contractual page limits."""

    if type(stream_id) is not UUID:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decode_natural_identity_key(natural_key)
    connection, _ = _connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        _verify_operation_snapshot(connection, token, writer=False)
        streams = _identity_rows(
            connection,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        if len(streams) != 1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        stream = streams[0]
        stream_row_id = _require_exact_int(
            stream["stream_row_id"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        current_version = _require_exact_int(
            stream["current_version"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )

        def details(sql: str, parameters: Sequence[object]) -> tuple[str, ...]:
            rows = _fetch_all(
                connection,
                f"EXPLAIN QUERY PLAN {sql}",
                parameters,
            )
            values = tuple(cast(str, row["detail"]) for row in rows)
            if not values or any(type(value) is not str for value in values):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            return values

        plans = {
            "identity": details(
                _IDENTITY_SQL,
                (stream_id.bytes, natural_key),
            ),
            "current": details(
                _CURRENT_ROWS_SQL,
                (
                    stream_row_id,
                    max(1, current_version - 1),
                    current_version,
                ),
            ),
        }
        audit_sql = """
        SELECT *
        FROM continuous_public_trade_history
        INDEXED BY ux_cpt_history_stream_version
        WHERE stream_row_id = ?
          AND successor_version >= ?
          AND successor_version <= ?
        ORDER BY successor_version
        LIMIT ?
        """
        for limit in (1, 100):
            plans[f"audit_initial_{limit}"] = details(
                audit_sql,
                (stream_row_id, 1, current_version, limit),
            )
            plans[f"audit_continuation_{limit}"] = details(
                audit_sql,
                (
                    stream_row_id,
                    1,
                    current_version,
                    limit + 1,
                ),
            )
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        return plans
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _rollback_best_effort(connection)
        raise failure from None
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


def measured_open_cursor_evidence(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> int:
    """Measure the Python DB-API cursor peak across the bounded read/verify surface."""

    with measure_open_cursors() as measurement:
        current = load_current(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        audit = audit_history(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
            limit=100,
        )
        verify_store(token)
    if (
        current.classification is not StoreClassification.FOUND
        or audit.classification is not StoreClassification.PAGE
        or not 1 <= measurement.maximum <= MAX_TEST_OPEN_CURSORS
    ):
        raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
    _require_token(token)
    return measurement.maximum


@_operation_evidence_executor
def measured_report_workload_evidence(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> WorkloadEvidence:
    """Execute and bind the exact finite latency, memory, and cursor workload."""

    latency_samples: list[int] = []
    for _ in range(WORKLOAD_RUNS):
        started = time.monotonic_ns()
        loaded = load_current(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        elapsed = time.monotonic_ns() - started
        if (
            loaded.classification is not StoreClassification.FOUND
            or not 0 <= elapsed <= MAX_OPERATION_LATENCY_NS
        ):
            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
        latency_samples.append(elapsed)

    tracemalloc.start()
    try:
        measured = load_current(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
        )
        _, peak_memory = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    maximum_open_cursors = measured_open_cursor_evidence(
        token,
        stream_id=stream_id,
        natural_key=natural_key,
    )
    if (
        measured.classification is not StoreClassification.FOUND
        or measured.query_evidence.history_rows != 3
        or not 0 <= peak_memory <= MAX_TEST_TRACED_MEMORY_BYTES
        or not 1 <= maximum_open_cursors <= MAX_TEST_OPEN_CURSORS
    ):
        raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
    _require_token(token)
    return WorkloadEvidence(
        query_evidence=measured.query_evidence,
        maximum_open_cursors=maximum_open_cursors,
        peak_traced_memory_bytes=peak_memory,
        latency_samples_ns=tuple(latency_samples),
    )


def _tail_manifest(
    token: StoreToken,
) -> tuple[tuple[str, int, str, str], ...]:
    connection, _ = _connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        _verify_operation_snapshot(connection, token, writer=False)
        rows = _fetch_all(
            connection,
            """
            SELECT
                stream_uuid,
                current_version,
                current_envelope_digest,
                current_history_root
            FROM continuous_public_trade_stream
            ORDER BY stream_uuid
            """,
        )
        result: list[tuple[str, int, str, str]] = []
        for row in rows:
            stream_uuid = row["stream_uuid"]
            if type(stream_uuid) is not bytes or len(stream_uuid) != 16:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            result.append(
                (
                    str(UUID(bytes=stream_uuid)),
                    _require_exact_int(
                        row["current_version"],
                        minimum=1,
                        maximum=MAX_CONTRACT_INTEGER,
                    ),
                    _decode_digest(row["current_envelope_digest"]),
                    _decode_digest(row["current_history_root"]),
                )
            )
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        return tuple(result)
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


def _closed_file_manifest(token: StoreToken) -> tuple[tuple[str, int, str], ...]:
    identity = _require_token(token)
    permitted_names = {
        _DATABASE_BASENAME,
        f"{_DATABASE_BASENAME}-wal",
        f"{_DATABASE_BASENAME}-shm",
    }
    root_descriptor = -1
    generation_descriptor = -1
    try:
        root_descriptor, generation_descriptor = _open_owned_generation(identity)
        names = tuple(sorted(os.listdir(generation_descriptor)))
        if (
            _DATABASE_BASENAME not in names
            or not names
            or set(names) - permitted_names
            or len(names) != len(set(names))
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        manifest: list[tuple[str, int, str]] = []
        file_flags = (
            os.O_RDONLY
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        for name in names:
            descriptor = os.open(
                name,
                file_flags,
                dir_fd=generation_descriptor,
            )
            try:
                before = os.fstat(descriptor)
                maximum = (
                    MAX_TEST_DATABASE_BYTES if name == _DATABASE_BASENAME else MAX_TEST_WAL_BYTES
                )
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != identity.uid
                    or before.st_nlink != 1
                    or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_size < 0
                    or before.st_size > maximum
                    or (
                        name == _DATABASE_BASENAME
                        and (before.st_dev != identity.device or before.st_ino != identity.inode)
                    )
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                digest = hashlib.sha256()
                total = 0
                while True:
                    block = os.read(descriptor, 65_536)
                    if not block:
                        break
                    total += len(block)
                    if total > maximum:
                        raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
                    digest.update(block)
                after = os.fstat(descriptor)
                stable = (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_uid",
                    "st_nlink",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
                if total != before.st_size or any(
                    getattr(before, field) != getattr(after, field) for field in stable
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                manifest.append((name, total, f"sha256:{digest.hexdigest()}"))
            finally:
                with suppress(OSError):
                    os.close(descriptor)
        if _require_token(token) != identity:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return tuple(manifest)
    except HarnessFailure:
        raise
    except (OSError, RuntimeError):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    finally:
        if generation_descriptor >= 0:
            with suppress(OSError):
                os.close(generation_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)


def _stream_identities(token: StoreToken) -> tuple[tuple[UUID, bytes], ...]:
    connection, _ = _connect(token, writer=False)
    identities: list[tuple[UUID, bytes]] = []
    try:
        connection.execute("BEGIN").close()
        _verify_operation_snapshot(connection, token, writer=False)
        after_row_id = 0
        while True:
            rows = _fetch_all(
                connection,
                """
                SELECT stream_row_id, stream_uuid, natural_identity_key
                FROM continuous_public_trade_stream
                WHERE stream_row_id > ?
                ORDER BY stream_row_id
                LIMIT 100
                """,
                (after_row_id,),
            )
            if not rows:
                break
            for row in rows:
                after_row_id = _require_exact_int(
                    row["stream_row_id"],
                    minimum=after_row_id + 1,
                    maximum=MAX_CONTRACT_INTEGER,
                )
                raw_uuid = row["stream_uuid"]
                raw_key = row["natural_identity_key"]
                if type(raw_uuid) is not bytes or len(raw_uuid) != 16:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                decode_natural_identity_key(raw_key)
                identities.append((UUID(bytes=raw_uuid), raw_key))
            if len(rows) < 100:
                break
        _verify_operation_authority(connection, token)
        connection.execute("COMMIT").close()
        return tuple(identities)
    except BaseException:
        _rollback_best_effort(connection)
        raise
    finally:
        _close_preserving_primary(connection)


def _validated_evidence_timestamp(value: str) -> str:
    if type(value) is not str:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except ValueError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != value:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return value


def _generation_evidence_id(token: StoreToken) -> str:
    identity = _require_token(token)
    payload = (
        b"wealth.continuous_public_trade.test_generation_evidence/v1\x00"
        + token._nonce
        + struct.pack(
            ">QQQ",
            identity.generation_device,
            identity.generation_inode,
            identity.generation_uid,
        )
        + identity.generation_root.name.encode("ascii")
    )
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


@_operation_evidence_executor
def online_backup(
    source: StoreToken,
    pytest_root: Path,
    *,
    evidence_recorded_at_utc: str,
    progress_hook: Callable[[], None] | None = None,
) -> tuple[StoreToken, BackupManifest]:
    """Run SQLite Online Backup into a fresh owned generation and verify it."""

    exact_evidence_time = _validated_evidence_timestamp(evidence_recorded_at_utc)
    source_before = verify_store(source)
    source_generation_id = _generation_evidence_id(source)
    destination: StoreToken | None = None
    source_connection: sqlite3.Connection | None = None
    destination_connection: sqlite3.Connection | None = None
    callback_count = 0
    hook_called = False
    snapshot_page_count: int | None = None

    def progress(status: int, remaining: int, total: int) -> None:
        nonlocal callback_count, hook_called, snapshot_page_count
        callback_count += 1
        if (
            type(status) is not int
            or type(remaining) is not int
            or type(total) is not int
            or remaining < 0
            or total < 0
            or remaining > total
            or total > MAX_PAGE_COUNT
            or callback_count > MAX_PAGE_COUNT + 2
        ):
            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
        if (
            progress_hook is not None
            and not hook_called
            and status != sqlite3.SQLITE_DONE
            and remaining > 0
        ):
            hook_called = True
            progress_hook()
        if status == sqlite3.SQLITE_DONE:
            if remaining != 0 or total < 1 or snapshot_page_count is not None:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            snapshot_page_count = total

    try:
        destination = bootstrap_store(pytest_root)
        source_connection, source_profile = _connect(source, writer=False)
        destination_connection, _ = _connect(destination, writer=True)
        source_connection.backup(
            destination_connection,
            pages=WAL_AUTOCHECKPOINT_PAGES,
            progress=progress,
            sleep=0.0,
        )
        _verify_operation_authority(source_connection, source)
        _verify_operation_authority(destination_connection, destination)
        if snapshot_page_count is None or (progress_hook is not None and not hook_called):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        checkpoint = tuple(
            _fetch_one(
                destination_connection,
                "PRAGMA wal_checkpoint(TRUNCATE)",
            )
        )
        if checkpoint != (0, 0, 0):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        _verify_operation_authority(source_connection, source)
        _verify_operation_authority(destination_connection, destination)
        _close_checked(destination_connection)
        destination_connection = None
        _close_checked(source_connection)
        source_connection = None

        destination_summary = verify_store(destination)
        source_after = verify_store(source)
        if (
            source_before.schema_fingerprint != destination_summary.schema_fingerprint
            or source_after.schema_fingerprint != destination_summary.schema_fingerprint
            or source_before.stream_count != destination_summary.stream_count
            or source_after.stream_count != destination_summary.stream_count
            or snapshot_page_count != destination_summary.page_count
            or not (
                source_before.history_count
                <= destination_summary.history_count
                <= source_after.history_count
            )
            or (progress_hook is None and source_before.history_count != source_after.history_count)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        destination_tails = _tail_manifest(destination)
        source_tail_by_id = {item[0]: item for item in _tail_manifest(source)}
        for stream_id, natural_key in _stream_identities(destination):
            destination_tail = next(
                (item for item in destination_tails if item[0] == str(stream_id)),
                None,
            )
            source_tail = source_tail_by_id.get(str(stream_id))
            if destination_tail is None or source_tail is None:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            source_entries = _complete_history(
                source,
                stream_id=stream_id,
                natural_key=natural_key,
            )
            destination_version = destination_tail[1]
            if (
                destination_version > len(source_entries)
                or source_entries[destination_version - 1].successor_envelope.envelope_digest
                != destination_tail[2]
                or source_entries[destination_version - 1].history_root != destination_tail[3]
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        files = _closed_file_manifest(destination)
        finalization_outcome = (
            "TRUNCATE_CHECKPOINT_CLOSED_STANDALONE_MAIN"
            if tuple(name for name, _, _ in files) == (_DATABASE_BASENAME,)
            else "TRUNCATE_CHECKPOINT_CLOSED_COMPLETE_FILE_SET"
        )
        _require_token(source)
        _require_token(destination)
        return destination, BackupManifest(
            source_generation_id=source_generation_id,
            destination_generation_id=_generation_evidence_id(destination),
            schema_fingerprint=destination_summary.schema_fingerprint,
            sqlite_source_id=source_profile.sqlite_source_id,
            page_size=PAGE_SIZE,
            source_page_count=snapshot_page_count,
            destination_page_count=destination_summary.page_count,
            checkpoint_outcome=checkpoint,
            finalization_outcome=finalization_outcome,
            evidence_recorded_at_utc=exact_evidence_time,
            source_streams=destination_summary.stream_count,
            source_history_rows=destination_summary.history_count,
            destination_streams=destination_summary.stream_count,
            destination_history_rows=destination_summary.history_count,
            files=files,
            per_stream_tails=destination_tails,
        )
    except sqlite3.Error as error:
        failure = _sqlite_failure(error)
        _close_best_effort(destination_connection)
        _close_best_effort(source_connection)
        if destination is not None:
            _remove_owned_files(destination)
        raise failure from None
    except BaseException:
        _close_best_effort(destination_connection)
        _close_best_effort(source_connection)
        if destination is not None:
            _remove_owned_files(destination)
        raise


@_operation_evidence_executor
def concurrent_write_backup_evidence(
    source: StoreToken,
    pytest_root: Path,
    *,
    transitions: tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...],
    evidence_recorded_at_utc: str,
) -> ConcurrentBackupEvidence:
    """Run one online backup while a bounded writer commits exact transitions."""

    if (
        type(transitions) is not tuple
        or len(transitions) != CONCURRENT_BACKUP_TRANSITIONS
        or any(
            type(transition) is not ContinuousPublicTradeStreamStoredTransitionV1
            for transition in transitions
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    source_before = verify_store(source)
    if not hasattr(os, "fork") or _CONNECTION_PATH_SNAPSHOTS:
        raise HarnessFailure(HarnessFailureCode.UNPROVEN)
    exact_transitions = tuple(_validated_transition(item) for item in transitions)
    cas_statements = (
        "BEGIN IMMEDIATE",
        "stream UUID lookup LIMIT 2",
        "history predecessor/successor LIMIT 2",
        "current history LIMIT 3",
        "INSERT transition history",
        "UPDATE current CAS",
        "COMMIT",
    )
    record_size = 2 + hashlib.sha256().digest_size
    result_packet_size = 1 + (CONCURRENT_BACKUP_TRANSITIONS * record_size)
    try:
        control_pipe, result_pipe = _open_pipes(2)
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNPROVEN) from None
    control_read, control_write = control_pipe
    result_read, result_write = result_pipe
    try:
        process_id = os.fork()
    except OSError:
        _close_descriptors((control_read, control_write, result_read, result_write))
        raise HarnessFailure(HarnessFailureCode.UNPROVEN) from None
    if process_id == 0:
        os.close(control_write)
        os.close(result_read)
        _ACTIVE_EVIDENCE_RUN.set(None)
        _ACTIVE_GATE_OPERATIONS.set(None)
        _ACTIVE_REJECTION_COLLECTOR.set(None)
        _ACTIVE_OPERATION_EXECUTOR_DEPTH.set(0)
        exit_code = 70
        try:
            _write_process_packet(result_write, b"R")
            if os.read(control_read, 1) != b"S":
                raise HarnessFailure(HarnessFailureCode.UNPROVEN)
            packet = bytearray(b"P")
            for transition in exact_transitions:
                mutation = compare_and_swap_stream(source, transition)
                if (
                    type(mutation) is not MutationEvidence
                    or mutation.classification is not StoreClassification.UPDATED
                    or mutation.statements != cas_statements
                    or type(mutation.rows_materialized) is not int
                    or not 0 <= mutation.rows_materialized <= 5
                    or mutation.committed is not True
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                packet.extend(struct.pack(">H", mutation.rows_materialized))
                packet.extend(bytes.fromhex(_evidence_payload_digest(mutation)))
            if len(packet) != result_packet_size:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _write_process_packet(result_write, bytes(packet))
            exit_code = 0
        except BaseException:
            with suppress(OSError):
                _write_process_packet(result_write, b"F")
        finally:
            _close_descriptors((control_read, result_write))
        os._exit(exit_code)

    try:
        os.close(control_read)
        control_read = -1
        os.close(result_write)
        result_write = -1
        ready = _read_process_packet("concurrent_backup_ready", result_read, 1)
        if ready != b"R":
            raise HarnessFailure(HarnessFailureCode.UNPROVEN)
    except BaseException as error:
        _close_descriptors(
            tuple(
                descriptor
                for descriptor in (
                    control_read,
                    control_write,
                    result_read,
                    result_write,
                )
                if descriptor >= 0
            )
        )
        if not _terminate_and_reap_processes((process_id,)):
            raise HarnessFailure(HarnessFailureCode.UNPROVEN) from error
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNPROVEN) from error

    progress_observations = 0
    writer_packet = b""

    def observe_progress() -> None:
        nonlocal control_write, progress_observations, writer_packet
        progress_observations += 1
        if progress_observations != 1 or control_write < 0:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _write_process_packet(control_write, b"S")
        os.close(control_write)
        control_write = -1
        writer_packet = _read_process_packet(
            "concurrent_backup_result",
            result_read,
            result_packet_size,
        )
        if len(writer_packet) != result_packet_size:
            raise HarnessFailure(HarnessFailureCode.UNPROVEN)

    backup_token: StoreToken | None = None
    try:
        backup_token, backup_manifest = online_backup(
            source,
            pytest_root,
            evidence_recorded_at_utc=evidence_recorded_at_utc,
            progress_hook=observe_progress,
        )
        status = _wait_for_owned_process(process_id)
        if (
            status is None
            or not os.WIFEXITED(status)
            or os.WEXITSTATUS(status) != 0
            or progress_observations != 1
            or not writer_packet.startswith(b"P")
        ):
            raise HarnessFailure(HarnessFailureCode.UNPROVEN)
        writer_mutations: list[MutationEvidence] = []
        offset = 1
        for _ in range(CONCURRENT_BACKUP_TRANSITIONS):
            rows_materialized = struct.unpack(
                ">H",
                writer_packet[offset : offset + 2],
            )[0]
            offset += 2
            observed_digest = writer_packet[offset : offset + hashlib.sha256().digest_size]
            offset += hashlib.sha256().digest_size
            mutation = MutationEvidence(
                classification=StoreClassification.UPDATED,
                statements=cas_statements,
                rows_materialized=rows_materialized,
                committed=True,
            )
            if not 0 <= rows_materialized <= 5 or observed_digest != bytes.fromhex(
                _evidence_payload_digest(mutation)
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            writer_mutations.append(mutation)
        if offset != len(writer_packet):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        source_after = verify_store(source)
        backup_summary = verify_store(backup_token)
        if (
            source_after.stream_count != source_before.stream_count
            or source_after.history_count != source_before.history_count + len(transitions)
            or source_after.page_count <= source_before.page_count
            or not (
                source_before.history_count
                <= backup_manifest.source_history_rows
                <= source_after.history_count
            )
            or not (
                source_before.page_count
                <= backup_manifest.source_page_count
                <= source_after.page_count
            )
            or backup_summary.history_count != backup_manifest.destination_history_rows
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return ConcurrentBackupEvidence(
            source_token=source,
            backup_token=backup_token,
            backup_manifest=backup_manifest,
            source_before=source_before,
            source_after=source_after,
            backup_summary=backup_summary,
            writer_mutations=tuple(writer_mutations),
            progress_observations=progress_observations,
            writer_process_boundary="one-forked-progress-triggered-writer",
        )
    except BaseException:
        if backup_token is not None:
            _remove_owned_files(backup_token)
        raise
    finally:
        _close_descriptors(
            tuple(
                descriptor
                for descriptor in (
                    control_read,
                    control_write,
                    result_read,
                    result_write,
                )
                if descriptor >= 0
            )
        )
        if not _terminate_and_reap_processes((process_id,)):
            raise HarnessFailure(HarnessFailureCode.UNPROVEN)


def _complete_history(
    token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> tuple[ContinuousPublicTradeStreamStoredHistoryEntryV1, ...]:
    page = audit_history(
        token,
        stream_id=stream_id,
        natural_key=natural_key,
        limit=100,
    )
    if page.classification is not StoreClassification.PAGE:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    entries = list(page.entries)
    while len(entries) >= 100:
        tail = entries[-1]
        continuation = (
            tail.record.successor_version,
            tail.successor_envelope.envelope_digest,
            tail.history_root,
        )
        next_page = audit_history(
            token,
            stream_id=stream_id,
            natural_key=natural_key,
            limit=100,
            continuation=continuation,
        )
        if next_page.classification is StoreClassification.AT_TAIL:
            break
        if (
            next_page.classification is not StoreClassification.PAGE
            or next_page.overlap_count != 1
            or not next_page.entries
            or next_page.entries[0].canonical_bytes != tail.canonical_bytes
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entries.extend(next_page.entries[1:])
        if next_page.new_count < 100:
            break
    _require_token(token)
    return tuple(entries)


@_operation_evidence_executor
def same_format_generation_copy(
    source: StoreToken,
    pytest_root: Path,
) -> StoreToken:
    """Copy exact validated values into a separate empty version-one generation."""

    source_before = verify_store(source)
    destination = bootstrap_store(pytest_root)
    try:
        identities = _stream_identities(source)
    except BaseException:
        _remove_owned_files(destination)
        raise

    try:
        for stream_id, natural_key in identities:
            source_entries = _complete_history(
                source,
                stream_id=stream_id,
                natural_key=natural_key,
            )
            creation = source_entries[0]
            if type(creation) is not ContinuousPublicTradeStreamStoredCreationV1:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            policy = ContinuousPublicTradePolicy(**creation.record.stream_policy.model_dump())
            if (
                create_stream(destination, creation, policy).classification
                is not StoreClassification.INSERTED
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            for entry in source_entries[1:]:
                if type(entry) is not ContinuousPublicTradeStreamStoredTransitionV1:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if (
                    compare_and_swap_stream(destination, entry).classification
                    is not StoreClassification.UPDATED
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
            destination_entries = _complete_history(
                destination,
                stream_id=stream_id,
                natural_key=natural_key,
            )
            if tuple(
                (
                    entry.canonical_bytes,
                    entry.record_digest,
                    entry.successor_envelope.canonical_bytes,
                    entry.successor_envelope.envelope_digest,
                    entry.history_root,
                )
                for entry in source_entries
            ) != tuple(
                (
                    entry.canonical_bytes,
                    entry.record_digest,
                    entry.successor_envelope.canonical_bytes,
                    entry.successor_envelope.envelope_digest,
                    entry.history_root,
                )
                for entry in destination_entries
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        destination_summary = verify_store(destination)
        source_after = verify_store(source)
        if (
            destination_summary.stream_count,
            destination_summary.history_count,
            _tail_manifest(destination),
        ) != (
            source_before.stream_count,
            source_before.history_count,
            _tail_manifest(source),
        ) or (
            source_after.stream_count,
            source_after.history_count,
        ) != (
            source_before.stream_count,
            source_before.history_count,
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _require_token(source)
        _require_token(destination)
        return destination
    except BaseException:
        _remove_owned_files(destination)
        raise


@_operation_evidence_executor
def closed_error_mapping_evidence() -> RejectionEvidence:
    """Return the exact frozen numeric result-code mapping observations."""

    codes = (*sorted(_CORRUPT_RESULT_CODES | _OPERATIONAL_RESULT_CODES), -1)
    return RejectionEvidence(tuple((str(code), sqlite_result_failure_code(code)) for code in codes))


_OPERATION_PRODUCERS_FROZEN = True


def _registered_rejection_scenario(
    label: str,
    *,
    kind: str,
    code: HarnessFailureCode,
    executor_names: tuple[str, ...] = (),
) -> _RejectionScenario:
    return _RejectionScenario(
        capability=object(),
        label=label,
        kind=kind,
        code=code,
        executor_names=executor_names,
    )


_BOOTSTRAP_REJECTION_CONTRACT: Final = (
    ("relative_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("unregistered_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("reconstructed_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("sibling_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("nested_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("symlink_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("forged_token", HarnessFailureCode.INVALID_TOKEN, "verify_store"),
    ("wrong_process_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("wrong_process_token", HarnessFailureCode.INVALID_TOKEN, "verify_store"),
    ("wrong_node_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("wrong_node_token", HarnessFailureCode.INVALID_TOKEN, "verify_store"),
    ("expired_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT, "bootstrap_store"),
    ("expired_token", HarnessFailureCode.INVALID_TOKEN, "verify_store"),
    (
        "replaced_registered_root",
        HarnessFailureCode.INVALID_BOOTSTRAP_ROOT,
        "bootstrap_store",
    ),
    ("hardlink_database", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("unexpected_entry", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("readonly_database", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("widened_root", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("missing_database", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("replaced_database", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    ("allowed_name_symlink", HarnessFailureCode.UNAVAILABLE, "verify_store"),
    (
        "path_resolution_bootstrap",
        HarnessFailureCode.INVALID_BOOTSTRAP_ROOT,
        "bootstrap_store",
    ),
    ("path_resolution_operation", HarnessFailureCode.UNAVAILABLE, "verify_store"),
)
_CORRUPTION_REJECTION_CONTRACT: Final = (
    ("digest_byte_guard", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("forbidden_schema_sql", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("transition_without_current_tail", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("stream_without_creation_history", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("orphan_creation_history", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("metadata_update", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("metadata_delete", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("history_update", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("history_delete", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("stream_identity_update", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("stream_delete", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("current_tail_jump", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("wrong_predecessor_bytes", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("wrong_predecessor_digest", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("wrong_predecessor_root", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("history_gap", HarnessFailureCode.UNAVAILABLE, "sqlite"),
    ("unsupported_generation", HarnessFailureCode.UNSUPPORTED_VERSION, "verify_store"),
    ("malformed_generation", HarnessFailureCode.CORRUPT, "verify_store"),
    ("short_page", HarnessFailureCode.CORRUPT, "verify_store"),
    ("audit_limit_overflow", HarnessFailureCode.CORRUPT, "audit_history"),
    ("retained_history_canonical_bytes", HarnessFailureCode.CORRUPT, "verify_store"),
    (
        "two_candidate_corrupt_precedence",
        HarnessFailureCode.CORRUPT,
        "audit_history",
    ),
    (
        "expectation_conflict_corrupt_precedence",
        HarnessFailureCode.CORRUPT,
        "audit_history",
    ),
)

_REGISTERED_REJECTION_SCENARIOS: Final = {
    label: _registered_rejection_scenario(
        label,
        kind=("sqlite" if executor == "sqlite" else "harness"),
        code=code,
        executor_names=(() if executor == "sqlite" else (executor,)),
    )
    for label, code, executor in (
        *_BOOTSTRAP_REJECTION_CONTRACT,
        *_CORRUPTION_REJECTION_CONTRACT,
    )
}


@contextmanager
def _fixed_gate_operation_scope(
    run: _EvidenceRun,
    *,
    gate: str,
    rejection_contract: Sequence[tuple[str, HarnessFailureCode, str]] = (),
) -> Iterator[None]:
    ledger = _validated_evidence_run(run)
    ordinal = GENERATED_EVIDENCE_GATES.index(gate)
    if (
        _ACTIVE_EVIDENCE_RUN.get() is not run
        or _ACTIVE_GATE_OPERATIONS.get() is not None
        or _ACTIVE_REJECTION_COLLECTOR.get() is not None
        or gate in ledger.operation_runs
        or gate in ledger.rejection_runs
        or ordinal in ledger.observations
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    operation_state = _GateOperationState(run=run, gate=gate, observations=[])
    operation_token = (
        _ACTIVE_GATE_OPERATIONS.set(operation_state)
        if gate in _GATE_OPERATION_PRODUCER_SEQUENCES
        else None
    )
    rejection_state: _RejectionCollectorState | None = None
    rejection_token = None
    if rejection_contract:
        rejection_state = _RejectionCollectorState(
            run=run,
            gate=gate,
            scenarios=tuple(
                _REGISTERED_REJECTION_SCENARIOS[label] for label, _, _ in rejection_contract
            ),
            observations=[],
        )
        rejection_token = _ACTIVE_REJECTION_COLLECTOR.set(rejection_state)
    completed = False
    try:
        yield
        if rejection_state is not None and len(rejection_state.observations) != len(
            rejection_state.scenarios
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        completed = True
    finally:
        if rejection_token is not None:
            _ACTIVE_REJECTION_COLLECTOR.reset(rejection_token)
        if operation_token is not None:
            _ACTIVE_GATE_OPERATIONS.reset(operation_token)
        if completed:
            expected_sequence = _GATE_OPERATION_PRODUCER_SEQUENCES.get(gate, ())
            if (
                tuple(observation.producer_name for observation in operation_state.observations)
                != expected_sequence
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            ledger.operation_runs[gate] = tuple(operation_state.observations)
            ledger.rejection_runs[gate] = (
                () if rejection_state is None else tuple(rejection_state.observations)
            )


@contextmanager
def bootstrap_path_operation_scope(run: _EvidenceRun) -> Iterator[None]:
    with _fixed_gate_operation_scope(
        run,
        gate="bootstrap_path_ownership",
        rejection_contract=_BOOTSTRAP_REJECTION_CONTRACT,
    ):
        yield


@contextmanager
def schema_corruption_operation_scope(run: _EvidenceRun) -> Iterator[None]:
    with _fixed_gate_operation_scope(
        run,
        gate="schema_constraints_corruption",
        rejection_contract=_CORRUPTION_REJECTION_CONTRACT,
    ):
        yield


@contextmanager
def fresh_process_operation_scope(run: _EvidenceRun) -> Iterator[None]:
    with _fixed_gate_operation_scope(run, gate="fresh_process_faults"):
        yield


@contextmanager
def atomicity_operation_scope(run: _EvidenceRun) -> Iterator[None]:
    with _fixed_gate_operation_scope(run, gate="atomicity_classification"):
        yield


@contextmanager
def bounded_query_operation_scope(run: _EvidenceRun) -> Iterator[None]:
    with _fixed_gate_operation_scope(run, gate="bounded_queries"):
        yield


def _report_token_role(
    arguments: tuple[object, ...],
    _keywords: Mapping[str, object],
    _result: object,
) -> tuple[tuple[str, StoreToken], ...]:
    if not arguments or type(arguments[0]) is not StoreToken:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (("report", arguments[0]),)


def _backup_token_roles(
    _arguments: tuple[object, ...],
    _keywords: Mapping[str, object],
    result: object,
) -> tuple[tuple[str, StoreToken], ...]:
    if type(result) is not BackupRestoreEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (
        ("source", result.source_token),
        ("backup", result.backup_token),
        ("restore", result.restore_token),
    )


def _generation_copy_token_roles(
    _arguments: tuple[object, ...],
    _keywords: Mapping[str, object],
    result: object,
) -> tuple[tuple[str, StoreToken], ...]:
    if type(result) is not GenerationCopyEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (
        ("source", result.source_token),
        ("destination", result.destination_token),
    )


def _validated_rejection_run(
    run: _EvidenceRun,
    *,
    gate: str,
    contract: Sequence[tuple[str, HarnessFailureCode, str]],
) -> tuple[_RejectionObservation, ...]:
    """Validate one exact private rejection receipt sequence."""

    observations = _validated_evidence_run(run).rejection_runs.get(gate)
    if observations is None or len(observations) != len(contract):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for ordinal, (observation, (label, code, executor)) in enumerate(
        zip(observations, contract, strict=True)
    ):
        try:
            call_digest = bytes.fromhex(observation.call_digest)
        except (TypeError, ValueError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        if (
            observation.run is not run
            or observation.process_id != run._pytest_registration.process_id
            or observation.ordinal != ordinal
            or observation.scenario is not _REGISTERED_REJECTION_SCENARIOS.get(label)
            or observation.scenario.label != label
            or observation.scenario.code is not code
            or observation.scenario.kind != ("sqlite" if executor == "sqlite" else "harness")
            or observation.code is not code
            or len(call_digest) != hashlib.sha256().digest_size
            or (
                executor == "sqlite"
                and (
                    type(observation.sqlite_errorcode) is not int
                    or observation.target_nonce is None
                )
            )
            or (executor == "bootstrap_store" and observation.target_nonce is not None)
            or (executor in {"verify_store", "audit_history"} and observation.target_nonce is None)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return observations


@_whole_gate_collector("schema_identity", token_roles=_report_token_role)
def collect_schema_identity_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> VerificationSummary:
    del run
    return verify_store(report_token)


@_whole_gate_collector("bootstrap_path_ownership", token_roles=_report_token_role)
def finalize_bootstrap_path_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> BootstrapPathEvidence:
    observations = _validated_rejection_run(
        run,
        gate="bootstrap_path_ownership",
        contract=_BOOTSTRAP_REJECTION_CONTRACT,
    )
    by_label = {observation.scenario.label: observation for observation in observations}
    scoped_nonce = by_label["wrong_process_token"].target_nonce
    if (
        scoped_nonce is None
        or by_label["wrong_node_token"].target_nonce != scoped_nonce
        or by_label["expired_token"].target_nonce != scoped_nonce
        or by_label["forged_token"].target_nonce != b"\x00" * 32
        or by_label["widened_root"].target_nonce != report_token._nonce
        or len(
            {
                cast(bytes, by_label[label].target_nonce)
                for label in (
                    "hardlink_database",
                    "unexpected_entry",
                    "readonly_database",
                    "missing_database",
                    "replaced_database",
                    "allowed_name_symlink",
                    "path_resolution_operation",
                )
            }
        )
        != 7
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return BootstrapPathEvidence(
        token=report_token,
        rejections=RejectionEvidence(
            tuple((observation.scenario.label, observation.code) for observation in observations)
        ),
    )


@_whole_gate_collector("runtime_connection_controls", token_roles=_report_token_role)
def collect_runtime_connection_controls_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
    summary: VerificationSummary,
) -> tuple[ConnectionControlProfile, ...]:
    ledger = _validated_evidence_run(run)
    schema_observation = ledger.observations.get(0)
    if (
        schema_observation is None
        or schema_observation.value is not summary
        or _require_token(report_token).pytest_registration is not run._pytest_registration
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return summary.connection_profiles


@_whole_gate_collector("projection_roundtrip", token_roles=_report_token_role)
def collect_projection_roundtrip_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> CurrentSlice:
    del run
    return load_current(
        report_token,
        stream_id=stream_id,
        natural_key=natural_key,
    )


@_whole_gate_collector("schema_constraints_corruption", token_roles=_report_token_role)
def finalize_schema_corruption_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> RejectionEvidence:
    del report_token
    observations = _validated_rejection_run(
        run,
        gate="schema_constraints_corruption",
        contract=_CORRUPTION_REJECTION_CONTRACT,
    )
    by_label = {observation.scenario.label: observation for observation in observations}
    if (
        by_label["stream_without_creation_history"].target_nonce
        != by_label["orphan_creation_history"].target_nonce
        or len(
            {
                by_label[label].target_nonce
                for label in (
                    "metadata_update",
                    "metadata_delete",
                    "history_update",
                    "history_delete",
                    "stream_identity_update",
                    "stream_delete",
                    "current_tail_jump",
                )
            }
        )
        != 1
        or len(
            {
                by_label[label].target_nonce
                for label in (
                    "wrong_predecessor_bytes",
                    "wrong_predecessor_digest",
                    "wrong_predecessor_root",
                    "history_gap",
                )
            }
        )
        != 1
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return RejectionEvidence(
        tuple((observation.scenario.label, observation.code) for observation in observations)
    )


@_whole_gate_collector("fresh_process_faults", token_roles=_report_token_role)
def finalize_fresh_process_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> FreshProcessEvidenceAggregate:
    observations = _validated_evidence_run(run).operation_runs.get("fresh_process_faults")
    if observations is None:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_nonce = report_token._nonce
    observed_store_nonces: set[bytes] = set()
    active_store_nonce: bytes | None = None
    for index, observation in enumerate(observations):
        if observation.sequence != index:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if observation.producer_name == "bootstrap_store":
            if (
                len(observation.token_nonces) != 1
                or observation.token_nonces[0] == report_nonce
                or observation.token_nonces[0] in observed_store_nonces
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            active_store_nonce = observation.token_nonces[0]
            observed_store_nonces.add(active_store_nonce)
        elif active_store_nonce is None or observation.token_nonces != (active_store_nonce,):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    evidence_observations = tuple(
        observation
        for observation in observations
        if observation.producer_name
        not in {
            "bootstrap_store",
            "create_stream",
        }
    )
    expected_sequence = (
        *(("fresh_process_kill_evidence", f"create:{seam}") for seam in _CREATE_KILL_SEAM_ORDER),
        *(
            ("fresh_process_kill_evidence", f"compare_and_swap:{seam}")
            for seam in _CAS_KILL_SEAM_ORDER
        ),
        ("sqlite_result_code_fault_evidence", "readonly"),
        ("sqlite_result_code_fault_evidence", "busy"),
        ("true_during_commit_evidence", "true_during_commit_evidence"),
        ("ioerr_write_evidence", "ioerr_write_evidence"),
        ("max_page_count_evidence", "max_page_count_evidence"),
        ("fresh_process_writer_contention_evidence", "create"),
        ("fresh_process_writer_contention_evidence", "compare_and_swap"),
        (
            "fresh_process_two_writer_evidence",
            "create:INSERTED,DUPLICATE",
        ),
        (
            "fresh_process_two_writer_evidence",
            "create:INSERTED,CONFLICT",
        ),
        (
            "fresh_process_two_writer_evidence",
            "compare_and_swap:UPDATED,DUPLICATE",
        ),
        (
            "fresh_process_two_writer_evidence",
            "compare_and_swap:UPDATED,CONFLICT",
        ),
        ("wal_concurrency_evidence", "wal_concurrency_evidence"),
    )
    if (
        tuple(
            (observation.producer_name, observation.operation_tag)
            for observation in evidence_observations
        )
        != expected_sequence
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    by_producer: dict[str, list[object]] = {}
    for observation in evidence_observations:
        if observation.payload_digest != _evidence_payload_digest(observation.result):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        by_producer.setdefault(observation.producer_name, []).append(observation.result)
    kill = by_producer.get("fresh_process_kill_evidence", [])
    result_codes = by_producer.get("sqlite_result_code_fault_evidence", [])
    during = by_producer.get("true_during_commit_evidence", [])
    ioerr = by_producer.get("ioerr_write_evidence", [])
    full = by_producer.get("max_page_count_evidence", [])
    contentions = by_producer.get("fresh_process_writer_contention_evidence", [])
    two_writers = by_producer.get("fresh_process_two_writer_evidence", [])
    wal = by_producer.get("wal_concurrency_evidence", [])
    if tuple(
        len(values)
        for values in (
            kill,
            result_codes,
            during,
            ioerr,
            full,
            contentions,
            two_writers,
            wal,
        )
    ) != (10, 2, 1, 1, 1, 2, 4, 1):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return FreshProcessEvidenceAggregate(
        create_faults=cast(tuple[FaultEvidence, ...], tuple(kill[:5])),
        compare_and_swap_faults=cast(tuple[FaultEvidence, ...], tuple(kill[5:])),
        result_code_faults=cast(tuple[FaultEvidence, ...], tuple(result_codes)),
        true_during_commit=cast(FaultEvidence, during[0]),
        ioerr_write=cast(FaultEvidence, ioerr[0]),
        max_page_count=cast(FaultEvidence, full[0]),
        writer_contentions=cast(
            tuple[WriterContentionEvidence, ...],
            tuple(contentions),
        ),
        two_writers=cast(tuple[TwoWriterEvidence, ...], tuple(two_writers)),
        wal_concurrency=cast(WalConcurrencyEvidence, wal[0]),
    )


@_whole_gate_collector("atomicity_classification", token_roles=_report_token_role)
def finalize_atomicity_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> AtomicityClassificationEvidence:
    ledger = _validated_evidence_run(run)
    fresh_observation = ledger.observations.get(
        GENERATED_EVIDENCE_GATES.index("fresh_process_faults")
    )
    operations = ledger.operation_runs.get("atomicity_classification")
    if (
        fresh_observation is None
        or type(fresh_observation.value) is not FreshProcessEvidenceAggregate
        or operations is None
        or len(operations) != len(_ATOMICITY_OPERATION_PRODUCER_SEQUENCE)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    atomicity_nonce = operations[0].token_nonces
    if (
        len(atomicity_nonce) != 1
        or atomicity_nonce[0] == report_token._nonce
        or any(
            operation.sequence != index or operation.token_nonces != atomicity_nonce
            for index, operation in enumerate(operations)
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    mutation_operations = tuple(
        operation for operation in operations if operation.producer_name != "bootstrap_store"
    )
    creates = tuple(
        cast(MutationEvidence, observation.result)
        for observation in mutation_operations
        if observation.producer_name == "create_stream"
    )
    swaps = tuple(
        cast(MutationEvidence, observation.result)
        for observation in mutation_operations
        if observation.producer_name == "compare_and_swap_stream"
    )
    if (
        tuple(
            (observation.producer_name, observation.operation_tag)
            for observation in mutation_operations
        )
        != (
            ("create_stream", "INSERTED"),
            ("create_stream", "DUPLICATE"),
            ("create_stream", "CONFLICT"),
            ("compare_and_swap_stream", "UPDATED"),
            ("compare_and_swap_stream", "DUPLICATE"),
            ("compare_and_swap_stream", "CONFLICT"),
        )
        or len(creates) != 3
        or len(swaps) != 3
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    fresh = fresh_observation.value
    return AtomicityClassificationEvidence(
        mutations=(*creates, *swaps),
        two_writers=fresh.two_writers,
        unknown_acknowledgements=(
            fresh.create_faults[-1],
            fresh.compare_and_swap_faults[-1],
        ),
    )


@_whole_gate_collector("bounded_queries", token_roles=_report_token_role)
def finalize_bounded_query_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> BoundedQueryEvidenceAggregate:
    ledger = _validated_evidence_run(run)
    projection_observation = ledger.observations.get(
        GENERATED_EVIDENCE_GATES.index("projection_roundtrip")
    )
    operations = ledger.operation_runs.get("bounded_queries")
    if (
        projection_observation is None
        or type(projection_observation.value) is not CurrentSlice
        or operations is None
        or len(operations) != len(_BOUNDED_OPERATION_PRODUCER_SEQUENCE)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_nonce = report_token._nonce
    conflict_nonce = operations[7].token_nonces
    maximum_nonce = operations[12].token_nonces
    if (
        any(operation.sequence != index for index, operation in enumerate(operations))
        or any(operation.token_nonces != (report_nonce,) for operation in operations[:7])
        or len(conflict_nonce) != 1
        or conflict_nonce[0] == report_nonce
        or any(operation.token_nonces != conflict_nonce for operation in operations[7:12])
        or len(maximum_nonce) != 1
        or maximum_nonce[0] in {report_nonce, conflict_nonce[0]}
        or any(operation.token_nonces != maximum_nonce for operation in operations[12:])
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    query_operations = tuple(
        operation
        for operation in operations
        if operation.producer_name
        in {
            "load_current",
            "audit_history",
            "query_plan_evidence",
        }
    )
    if tuple(
        (observation.producer_name, observation.operation_tag) for observation in query_operations
    ) != (
        ("load_current", "FOUND"),
        ("load_current", "NOT_FOUND"),
        ("audit_history", "PAGE:limit=1:continuation=False"),
        ("audit_history", "PAGE:limit=10:continuation=False"),
        ("audit_history", "PAGE:limit=1:continuation=True"),
        ("audit_history", "AT_TAIL:limit=100:continuation=True"),
        ("audit_history", "ANCHOR_CONFLICT:limit=1:continuation=True"),
        ("load_current", "IDENTITY_CONFLICT"),
        ("audit_history", "IDENTITY_CONFLICT:limit=100:continuation=False"),
        ("audit_history", "PAGE:limit=100:continuation=False"),
        ("audit_history", "PAGE:limit=100:continuation=True"),
        ("query_plan_evidence", "query_plan_evidence"),
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    loads = [
        cast(CurrentSlice, observation.result)
        for observation in query_operations
        if observation.producer_name == "load_current"
    ]
    audits = [
        cast(AuditSlice, observation.result)
        for observation in query_operations
        if observation.producer_name == "audit_history"
    ]
    plans = [
        cast(dict[str, tuple[str, ...]], observation.result)
        for observation in query_operations
        if observation.producer_name == "query_plan_evidence"
    ]
    if len(loads) != 3 or len(audits) != 8 or len(plans) != 1:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    projection = projection_observation.value
    if loads[0] != projection:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    (
        initial_one,
        _,
        continued_one,
        at_tail,
        anchor_conflict,
        audit_conflict,
        initial_hundred,
        continued_hundred,
    ) = audits
    plan_names = (
        "identity",
        "current",
        "audit_initial_1",
        "audit_continuation_1",
        "audit_initial_100",
        "audit_continuation_100",
    )
    return BoundedQueryEvidenceAggregate(
        queries=(
            ("current_found", projection.classification, projection.query_evidence),
            ("current_not_found", loads[1].classification, loads[1].query_evidence),
            (
                "current_identity_conflict",
                loads[2].classification,
                loads[2].query_evidence,
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
        plans=tuple((name, plans[0][name]) for name in plan_names),
    )


@_whole_gate_collector("closed_error_mapping", token_roles=_report_token_role)
def collect_closed_error_mapping_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
) -> RejectionEvidence:
    del run, report_token
    return closed_error_mapping_evidence()


@_whole_gate_collector("backup_restore", token_roles=_backup_token_roles)
def collect_backup_restore_evidence(
    run: _EvidenceRun,
    source_token: StoreToken,
    pytest_root: Path,
    *,
    transitions: tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...],
    backup_recorded_at_utc: str,
    restore_recorded_at_utc: str,
) -> BackupRestoreEvidence:
    del run
    concurrent: ConcurrentBackupEvidence | None = None
    restore_token: StoreToken | None = None
    try:
        concurrent = concurrent_write_backup_evidence(
            source_token,
            pytest_root,
            transitions=transitions,
            evidence_recorded_at_utc=backup_recorded_at_utc,
        )
        restore_token, restore_manifest = online_backup(
            concurrent.backup_token,
            pytest_root,
            evidence_recorded_at_utc=restore_recorded_at_utc,
        )
        restore_summary = verify_store(restore_token)
        if (
            concurrent.source_token is not source_token
            or concurrent.source_after != verify_store(source_token)
            or restore_manifest.source_generation_id
            != concurrent.backup_manifest.destination_generation_id
            or restore_summary.stream_count != concurrent.backup_summary.stream_count
            or restore_summary.history_count != concurrent.backup_summary.history_count
            or _tail_manifest(restore_token) != concurrent.backup_manifest.per_stream_tails
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return BackupRestoreEvidence(
            source_token=source_token,
            backup_token=concurrent.backup_token,
            restore_token=restore_token,
            backup_manifest=concurrent.backup_manifest,
            restore_manifest=restore_manifest,
            source_summary=concurrent.source_after,
            backup_summary=concurrent.backup_summary,
            restore_summary=restore_summary,
            concurrent_write=concurrent,
        )
    except BaseException:
        if restore_token is not None:
            _remove_owned_files(restore_token)
        if concurrent is not None:
            _remove_owned_files(concurrent.backup_token)
        raise


@_whole_gate_collector("generation_copy", token_roles=_generation_copy_token_roles)
def collect_generation_copy_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
    pytest_root: Path,
) -> GenerationCopyEvidence:
    del run
    destination = same_format_generation_copy(report_token, pytest_root)
    return GenerationCopyEvidence(
        source_token=report_token,
        destination_token=destination,
        source_generation_id=_generation_evidence_id(report_token),
        destination_generation_id=_generation_evidence_id(destination),
        source_summary=verify_store(report_token),
        destination_summary=verify_store(destination),
        source_tails=_tail_manifest(report_token),
        destination_tails=_tail_manifest(destination),
    )


@_whole_gate_collector("workload_thresholds", token_roles=_report_token_role)
def collect_workload_threshold_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
    *,
    stream_id: UUID,
    natural_key: bytes,
) -> WorkloadEvidence:
    del run
    return measured_report_workload_evidence(
        report_token,
        stream_id=stream_id,
        natural_key=natural_key,
    )


_GATE_COLLECTORS_FROZEN = True


def _validated_report_query_rows(evidence: QueryEvidence) -> int:
    if (
        type(evidence) is not QueryEvidence
        or type(evidence.statements) is not tuple
        or not evidence.statements
        or any(type(statement) is not str or not statement for statement in evidence.statements)
        or any(
            type(value) is not int or value < 0
            for value in (
                evidence.stream_rows,
                evidence.history_rows,
                evidence.decoded_rows,
            )
        )
        or evidence.decoded_rows != evidence.history_rows
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return evidence.stream_rows + evidence.history_rows


def _validate_rejection_evidence(
    evidence: RejectionEvidence,
    expected: tuple[tuple[str, HarnessFailureCode], ...],
) -> None:
    if (
        type(evidence) is not RejectionEvidence
        or type(evidence.checks) is not tuple
        or evidence.checks != expected
        or any(
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not HarnessFailureCode
            for item in evidence.checks
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_pass_fault(
    evidence: FaultEvidence,
    *,
    seam: str,
    sqlite_errorcode: int | None = None,
    reopened_state: ReopenedState | None = None,
) -> None:
    if (
        type(evidence) is not FaultEvidence
        or evidence.seam != seam
        or evidence.disposition is not EvidenceDisposition.PASS
        or evidence.reason is not None
        or evidence.sqlite_errorcode != sqlite_errorcode
        or (reopened_state is not None and evidence.reopened_state is not reopened_state)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_fresh_process_report_evidence(
    evidence: FreshProcessEvidenceAggregate,
) -> None:
    if (
        type(evidence) is not FreshProcessEvidenceAggregate
        or type(evidence.create_faults) is not tuple
        or type(evidence.compare_and_swap_faults) is not tuple
        or type(evidence.result_code_faults) is not tuple
        or tuple(item.seam for item in evidence.create_faults) != _CREATE_KILL_SEAM_ORDER
        or tuple(item.seam for item in evidence.compare_and_swap_faults) != _CAS_KILL_SEAM_ORDER
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for seam, fault in zip(
        _CREATE_KILL_SEAM_ORDER,
        evidence.create_faults,
        strict=True,
    ):
        _validate_pass_fault(
            fault,
            seam=seam,
            reopened_state=(
                ReopenedState.NEW
                if seam == "after_commit_before_acknowledgement"
                else ReopenedState.OLD
            ),
        )
        if fault.acknowledgement_bytes != 0 or fault.observed_syscall != "fork/SIGKILL":
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for seam, fault in zip(
        _CAS_KILL_SEAM_ORDER,
        evidence.compare_and_swap_faults,
        strict=True,
    ):
        _validate_pass_fault(
            fault,
            seam=seam,
            reopened_state=(
                ReopenedState.NEW
                if seam == "after_commit_before_acknowledgement"
                else ReopenedState.OLD
            ),
        )
        if fault.acknowledgement_bytes != 0 or fault.observed_syscall != "fork/SIGKILL":
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if len(evidence.result_code_faults) != 2:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for fault, seam, code in zip(
        evidence.result_code_faults,
        ("readonly", "busy"),
        (sqlite3.SQLITE_READONLY, sqlite3.SQLITE_BUSY),
        strict=True,
    ):
        _validate_pass_fault(
            fault,
            seam=seam,
            sqlite_errorcode=code,
            reopened_state=ReopenedState.OLD,
        )
        if fault.acknowledgement_bytes != 4:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_pass_fault(
        evidence.true_during_commit,
        seam="true_during_commit",
    )
    if (
        evidence.true_during_commit.reopened_state not in {ReopenedState.OLD, ReopenedState.NEW}
        or evidence.true_during_commit.acknowledgement_bytes != 0
        or evidence.true_during_commit.observed_syscall
        != "pwrite64(wal-frame-header)/pwrite64(wal-page-entry)"
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_pass_fault(
        evidence.ioerr_write,
        seam="sqlite_ioerr_write",
        sqlite_errorcode=778,
        reopened_state=ReopenedState.OLD,
    )
    _validate_pass_fault(
        evidence.max_page_count,
        seam="max_page_count",
        sqlite_errorcode=sqlite3.SQLITE_FULL,
        reopened_state=ReopenedState.OLD,
    )
    expected_contentions = (
        (
            "create",
            StoreClassification.INSERTED,
            (1, 1),
        ),
        (
            "compare_and_swap",
            StoreClassification.UPDATED,
            (1, 2),
        ),
    )
    if type(evidence.writer_contentions) is not tuple or len(evidence.writer_contentions) != len(
        expected_contentions
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for contention, (operation, winner, counts) in zip(
        evidence.writer_contentions,
        expected_contentions,
        strict=True,
    ):
        if (
            type(contention) is not WriterContentionEvidence
            or contention.operation != operation
            or contention.winner_outcome is not winner
            or contention.contender_outcome is not HarnessFailureCode.UNAVAILABLE
            or contention.contender_sqlite_errorcode != sqlite3.SQLITE_BUSY
            or contention.disposition is not EvidenceDisposition.PASS
            or contention.process_boundary != "two-forked-overlapping-writers"
            or (contention.stream_count, contention.history_count) != counts
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    expected_two_writer = (
        (
            "create",
            (
                StoreClassification.INSERTED,
                StoreClassification.DUPLICATE,
            ),
            (1, 1),
        ),
        (
            "create",
            (
                StoreClassification.INSERTED,
                StoreClassification.CONFLICT,
            ),
            (1, 1),
        ),
        (
            "compare_and_swap",
            (
                StoreClassification.UPDATED,
                StoreClassification.DUPLICATE,
            ),
            (1, 2),
        ),
        (
            "compare_and_swap",
            (
                StoreClassification.UPDATED,
                StoreClassification.CONFLICT,
            ),
            (1, 2),
        ),
    )
    if type(evidence.two_writers) is not tuple or len(evidence.two_writers) != len(
        expected_two_writer
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for two_writer, (operation, outcomes, counts) in zip(
        evidence.two_writers,
        expected_two_writer,
        strict=True,
    ):
        if (
            type(two_writer) is not TwoWriterEvidence
            or two_writer.operation != operation
            or two_writer.outcomes != outcomes
            or two_writer.disposition is not EvidenceDisposition.PASS
            or two_writer.process_boundary != "two-forked-sequential-classifiers"
            or (two_writer.stream_count, two_writer.history_count) != counts
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    wal = evidence.wal_concurrency
    if (
        type(wal) is not WalConcurrencyEvidence
        or wal.disposition is not EvidenceDisposition.PASS
        or wal.initial_version != 1
        or wal.reader_snapshot_version != 1
        or wal.final_version != 9
        or wal.writes != 8
        or not wal.checkpoint_samples
        or any(
            type(sample) is not tuple
            or len(sample) != 3
            or any(type(value) is not int or value < 0 for value in sample)
            or sample[2] > sample[1]
            for sample in wal.checkpoint_samples
        )
        or not 0 < wal.maximum_wal_bytes <= MAX_TEST_WAL_BYTES
        or wal.process_boundary != "three-forked-role-children"
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_bounded_query_report_evidence(
    evidence: BoundedQueryEvidenceAggregate,
    projection: CurrentSlice,
) -> None:
    expected_queries = (
        ("current_found", StoreClassification.FOUND, 1, 3, 3),
        ("current_not_found", StoreClassification.NOT_FOUND, 0, 0, 0),
        (
            "current_identity_conflict",
            StoreClassification.IDENTITY_CONFLICT,
            2,
            2,
            2,
        ),
        (
            "audit_identity_conflict",
            StoreClassification.IDENTITY_CONFLICT,
            2,
            2,
            2,
        ),
        ("audit_initial_1", StoreClassification.PAGE, 1, 1, 1),
        ("audit_continuation_1", StoreClassification.PAGE, 1, 2, 2),
        ("audit_initial_100", StoreClassification.PAGE, 1, 100, 100),
        ("audit_continuation_100", StoreClassification.PAGE, 1, 101, 101),
        ("audit_at_tail", StoreClassification.AT_TAIL, 1, 1, 1),
        (
            "audit_anchor_conflict",
            StoreClassification.ANCHOR_CONFLICT,
            1,
            2,
            2,
        ),
    )
    expected_plan_names = (
        "identity",
        "current",
        "audit_initial_1",
        "audit_continuation_1",
        "audit_initial_100",
        "audit_continuation_100",
    )
    if (
        type(evidence) is not BoundedQueryEvidenceAggregate
        or type(evidence.queries) is not tuple
        or len(evidence.queries) != len(expected_queries)
        or any(
            type(item) is not tuple
            or len(item) != 3
            or type(item[0]) is not str
            or type(item[1]) is not StoreClassification
            or type(item[2]) is not QueryEvidence
            for item in evidence.queries
        )
        or evidence.queries[0]
        != (
            "current_found",
            StoreClassification.FOUND,
            projection.query_evidence,
        )
        or type(evidence.plans) is not tuple
        or tuple(name for name, _ in evidence.plans) != expected_plan_names
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for observed, expected in zip(evidence.queries, expected_queries, strict=True):
        name, classification, query = observed
        expected_name, expected_classification, stream_rows, history_rows, decoded_rows = expected
        _validated_report_query_rows(query)
        if (
            name != expected_name
            or classification is not expected_classification
            or query.stream_rows != stream_rows
            or query.history_rows != history_rows
            or query.decoded_rows != decoded_rows
            or (
                name.startswith("audit_")
                and not any("audit" in statement for statement in query.statements)
            )
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for name, details in evidence.plans:
        if (
            type(name) is not str
            or type(details) is not tuple
            or not details
            or any(type(detail) is not str or not detail for detail in details)
            or any("SCAN continuous_public_trade_history" in detail for detail in details)
            or (
                name != "identity"
                and not any("ux_cpt_history_stream_version" in detail for detail in details)
            )
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    identity_plan = " ".join(dict(evidence.plans)["identity"])
    if (
        "ux_cpt_stream_uuid" not in identity_plan
        or "ux_cpt_stream_natural_key" not in identity_plan
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _derived_report_gates(
    root: Path,
    report: EvidenceReport,
) -> tuple[tuple[str, EvidenceDisposition, str | None], ...]:
    evidence = report.evidence
    if type(evidence) is not GeneratedEvidenceAggregate:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    bootstrap = evidence.bootstrap_path_ownership
    if type(bootstrap) is not BootstrapPathEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        registered = _require_token(bootstrap.token)
        observed_summary = verify_store(bootstrap.token)
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    summary = evidence.schema_identity
    profile = summary.profile
    if (
        registered.pytest_root != root
        or type(summary) is not VerificationSummary
        or observed_summary != summary
        or report.schema_fingerprint != summary.schema_fingerprint
        or report.python_version != profile.python_version
        or report.sqlite_version != profile.sqlite_version
        or report.sqlite_source_id != profile.sqlite_source_id
        or report.threadsafety != profile.threadsafety
        or report.compile_options != profile.compile_options
        or report.connection_profiles != summary.connection_profiles
        or evidence.runtime_connection_controls != summary.connection_profiles
        or report.stream_rows != summary.stream_count
        or report.history_rows != summary.history_count
        or report.database_bytes != summary.database_bytes
        or report.wal_bytes != summary.wal_bytes
        or report.page_count != summary.page_count
        or report.freelist_count != summary.freelist_count
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_rejection_evidence(
        bootstrap.rejections,
        (
            ("relative_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("unregistered_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("reconstructed_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("sibling_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("nested_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("symlink_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("forged_token", HarnessFailureCode.INVALID_TOKEN),
            ("wrong_process_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("wrong_process_token", HarnessFailureCode.INVALID_TOKEN),
            ("wrong_node_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("wrong_node_token", HarnessFailureCode.INVALID_TOKEN),
            ("expired_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("expired_token", HarnessFailureCode.INVALID_TOKEN),
            ("replaced_registered_root", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("hardlink_database", HarnessFailureCode.UNAVAILABLE),
            ("unexpected_entry", HarnessFailureCode.UNAVAILABLE),
            ("readonly_database", HarnessFailureCode.UNAVAILABLE),
            ("widened_root", HarnessFailureCode.UNAVAILABLE),
            ("missing_database", HarnessFailureCode.UNAVAILABLE),
            ("replaced_database", HarnessFailureCode.UNAVAILABLE),
            ("allowed_name_symlink", HarnessFailureCode.UNAVAILABLE),
            ("path_resolution_bootstrap", HarnessFailureCode.INVALID_BOOTSTRAP_ROOT),
            ("path_resolution_operation", HarnessFailureCode.UNAVAILABLE),
        ),
    )
    projection = evidence.projection_roundtrip
    if (
        type(projection) is not CurrentSlice
        or projection.classification is not StoreClassification.FOUND
        or projection.creation is None
        or projection.current is None
        or projection.current.record.stream_id != projection.creation.record.stream_id
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_report_query_rows(projection.query_evidence)
    record = projection.creation.record
    try:
        observed_projection = load_current(
            bootstrap.token,
            stream_id=record.stream_id,
            natural_key=natural_identity_key(
                source=record.source,
                venue=record.venue,
                instrument=record.instrument,
                provider_symbol=record.provider_symbol,
                instrument_type=record.instrument_type.value,
                request_variant=record.request_variant,
            ),
        )
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if observed_projection != projection:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_rejection_evidence(
        evidence.schema_constraints_corruption,
        (
            ("digest_byte_guard", HarnessFailureCode.UNAVAILABLE),
            ("forbidden_schema_sql", HarnessFailureCode.UNAVAILABLE),
            ("transition_without_current_tail", HarnessFailureCode.UNAVAILABLE),
            ("stream_without_creation_history", HarnessFailureCode.UNAVAILABLE),
            ("orphan_creation_history", HarnessFailureCode.UNAVAILABLE),
            ("metadata_update", HarnessFailureCode.UNAVAILABLE),
            ("metadata_delete", HarnessFailureCode.UNAVAILABLE),
            ("history_update", HarnessFailureCode.UNAVAILABLE),
            ("history_delete", HarnessFailureCode.UNAVAILABLE),
            ("stream_identity_update", HarnessFailureCode.UNAVAILABLE),
            ("stream_delete", HarnessFailureCode.UNAVAILABLE),
            ("current_tail_jump", HarnessFailureCode.UNAVAILABLE),
            ("wrong_predecessor_bytes", HarnessFailureCode.UNAVAILABLE),
            ("wrong_predecessor_digest", HarnessFailureCode.UNAVAILABLE),
            ("wrong_predecessor_root", HarnessFailureCode.UNAVAILABLE),
            ("history_gap", HarnessFailureCode.UNAVAILABLE),
            ("unsupported_generation", HarnessFailureCode.UNSUPPORTED_VERSION),
            ("malformed_generation", HarnessFailureCode.CORRUPT),
            ("short_page", HarnessFailureCode.CORRUPT),
            ("audit_limit_overflow", HarnessFailureCode.CORRUPT),
            ("retained_history_canonical_bytes", HarnessFailureCode.CORRUPT),
            ("two_candidate_corrupt_precedence", HarnessFailureCode.CORRUPT),
            (
                "expectation_conflict_corrupt_precedence",
                HarnessFailureCode.CORRUPT,
            ),
        ),
    )
    atomicity = evidence.atomicity_classification
    if type(atomicity) is not AtomicityClassificationEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    mutations = atomicity.mutations
    expected_mutation_classifications = (
        StoreClassification.INSERTED,
        StoreClassification.DUPLICATE,
        StoreClassification.CONFLICT,
        StoreClassification.UPDATED,
        StoreClassification.DUPLICATE,
        StoreClassification.CONFLICT,
    )
    if (
        type(mutations) is not tuple
        or len(mutations) != len(expected_mutation_classifications)
        or tuple(item.classification for item in mutations) != expected_mutation_classifications
        or any(
            type(item) is not MutationEvidence
            or item.committed is not True
            or type(item.rows_materialized) is not int
            or not 0 <= item.rows_materialized <= 5
            or type(item.statements) is not tuple
            or not item.statements
            or item.statements[0] != "BEGIN IMMEDIATE"
            or item.statements[-1] != "COMMIT"
            for item in mutations
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_fresh_process_report_evidence(evidence.fresh_process_faults)
    fresh_process = evidence.fresh_process_faults
    if (
        atomicity.two_writers != fresh_process.two_writers
        or atomicity.unknown_acknowledgements
        != (
            fresh_process.create_faults[-1],
            fresh_process.compare_and_swap_faults[-1],
        )
        or any(
            type(fault) is not FaultEvidence
            or fault.seam != "after_commit_before_acknowledgement"
            or fault.disposition is not EvidenceDisposition.PASS
            or fault.reopened_state is not ReopenedState.NEW
            or fault.acknowledgement_bytes != 0
            for fault in atomicity.unknown_acknowledgements
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_bounded_query_report_evidence(
        evidence.bounded_queries,
        projection,
    )
    _validate_rejection_evidence(
        evidence.closed_error_mapping,
        closed_error_mapping_evidence().checks,
    )
    backup = evidence.backup_restore
    if type(backup) is not BackupRestoreEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    concurrent = backup.concurrent_write
    if (
        type(concurrent) is not ConcurrentBackupEvidence
        or type(backup.backup_manifest) is not BackupManifest
        or type(backup.restore_manifest) is not BackupManifest
        or type(backup.source_summary) is not VerificationSummary
        or type(backup.backup_summary) is not VerificationSummary
        or type(backup.restore_summary) is not VerificationSummary
        or type(concurrent.source_before) is not VerificationSummary
        or type(concurrent.source_after) is not VerificationSummary
        or type(concurrent.backup_summary) is not VerificationSummary
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        backup_source_registration = _require_token(backup.source_token)
        _require_token(backup.backup_token)
        _require_token(backup.restore_token)
        observed_backup_source_summary = verify_store(backup.source_token)
        observed_backup_summary = verify_store(backup.backup_token)
        observed_restore_summary = verify_store(backup.restore_token)
        observed_backup_files = _closed_file_manifest(backup.backup_token)
        observed_restore_files = _closed_file_manifest(backup.restore_token)
        observed_backup_tails = _tail_manifest(backup.backup_token)
        observed_restore_tails = _tail_manifest(backup.restore_token)
        _validate_live_backup_manifest(
            backup.backup_manifest,
            source_token=backup.source_token,
            destination_token=backup.backup_token,
            source_summary=concurrent.source_before,
            destination_summary=backup.backup_summary,
            destination_files=observed_backup_files,
            destination_tails=observed_backup_tails,
            require_exact_source_snapshot=False,
        )
        _validate_live_backup_manifest(
            backup.restore_manifest,
            source_token=backup.backup_token,
            destination_token=backup.restore_token,
            source_summary=backup.backup_summary,
            destination_summary=backup.restore_summary,
            destination_files=observed_restore_files,
            destination_tails=observed_restore_tails,
            require_exact_source_snapshot=True,
        )
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if (
        backup.backup_token is not bootstrap.token
        or backup_source_registration.pytest_registration is not registered.pytest_registration
        or backup.backup_manifest != report.backup_manifest
        or concurrent.source_token is not backup.source_token
        or concurrent.backup_token is not backup.backup_token
        or concurrent.backup_manifest != backup.backup_manifest
        or concurrent.backup_summary != backup.backup_summary
        or backup.source_summary != observed_backup_source_summary
        or backup.source_summary != concurrent.source_after
        or backup.backup_summary != observed_backup_summary
        or backup.restore_summary != observed_restore_summary
        or backup.backup_summary != summary
        or backup.backup_manifest.destination_streams != backup.backup_summary.stream_count
        or backup.backup_manifest.destination_history_rows != backup.backup_summary.history_count
        or backup.backup_manifest.destination_page_count != backup.backup_summary.page_count
        or backup.restore_manifest.source_generation_id
        != backup.backup_manifest.destination_generation_id
        or backup.restore_manifest.source_streams != backup.backup_summary.stream_count
        or backup.restore_manifest.source_history_rows != backup.backup_summary.history_count
        or backup.restore_manifest.destination_streams != backup.restore_summary.stream_count
        or backup.restore_manifest.destination_history_rows != backup.restore_summary.history_count
        or backup.restore_manifest.source_page_count != backup.backup_summary.page_count
        or backup.restore_manifest.destination_page_count != backup.restore_summary.page_count
        or backup.source_summary.schema_fingerprint != summary.schema_fingerprint
        or backup.restore_summary.schema_fingerprint != summary.schema_fingerprint
        or backup.source_summary.profile != summary.profile
        or backup.restore_summary.profile != summary.profile
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        concurrent_source_registration = _require_token(concurrent.source_token)
        _require_token(concurrent.backup_token)
        observed_concurrent_source = verify_store(concurrent.source_token)
        observed_concurrent_backup = verify_store(concurrent.backup_token)
        observed_concurrent_files = _closed_file_manifest(concurrent.backup_token)
        observed_concurrent_tails = _tail_manifest(concurrent.backup_token)
        _validate_live_backup_manifest(
            concurrent.backup_manifest,
            source_token=concurrent.source_token,
            destination_token=concurrent.backup_token,
            source_summary=concurrent.source_before,
            destination_summary=concurrent.backup_summary,
            destination_files=observed_concurrent_files,
            destination_tails=observed_concurrent_tails,
            require_exact_source_snapshot=False,
        )
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if (
        concurrent_source_registration.pytest_registration is not registered.pytest_registration
        or concurrent.source_token is concurrent.backup_token
        or concurrent.source_after != observed_concurrent_source
        or concurrent.backup_summary != observed_concurrent_backup
        or concurrent.source_before.profile != summary.profile
        or concurrent.source_after.profile != summary.profile
        or concurrent.backup_summary.profile != summary.profile
        or concurrent.source_before.schema_fingerprint != summary.schema_fingerprint
        or concurrent.source_after.schema_fingerprint != summary.schema_fingerprint
        or concurrent.backup_summary.schema_fingerprint != summary.schema_fingerprint
        or concurrent.progress_observations != 1
        or type(concurrent.progress_observations) is not int
        or concurrent.writer_process_boundary != "one-forked-progress-triggered-writer"
        or type(concurrent.writer_mutations) is not tuple
        or len(concurrent.writer_mutations) != CONCURRENT_BACKUP_TRANSITIONS
        or any(
            type(mutation) is not MutationEvidence
            or mutation.classification is not StoreClassification.UPDATED
            or mutation.committed is not True
            or mutation.statements
            != (
                "BEGIN IMMEDIATE",
                "stream UUID lookup LIMIT 2",
                "history predecessor/successor LIMIT 2",
                "current history LIMIT 3",
                "INSERT transition history",
                "UPDATE current CAS",
                "COMMIT",
            )
            or type(mutation.rows_materialized) is not int
            or not 0 <= mutation.rows_materialized <= 5
            for mutation in concurrent.writer_mutations
        )
        or concurrent.source_before.stream_count != concurrent.source_after.stream_count
        or concurrent.source_before.stream_count != concurrent.backup_summary.stream_count
        or concurrent.source_after.history_count
        != concurrent.source_before.history_count + len(concurrent.writer_mutations)
        or concurrent.source_after.page_count <= concurrent.source_before.page_count
        or not (
            concurrent.source_before.history_count
            <= concurrent.backup_manifest.source_history_rows
            <= concurrent.source_after.history_count
        )
        or not (
            concurrent.source_before.page_count
            <= concurrent.backup_manifest.source_page_count
            <= concurrent.source_after.page_count
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    generation_copy = evidence.generation_copy
    if type(generation_copy) is not GenerationCopyEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        _require_token(generation_copy.source_token)
        _require_token(generation_copy.destination_token)
        observed_copy_source_summary = verify_store(generation_copy.source_token)
        observed_copy_destination_summary = verify_store(generation_copy.destination_token)
        observed_copy_source_id = _generation_evidence_id(generation_copy.source_token)
        observed_copy_destination_id = _generation_evidence_id(
            generation_copy.destination_token,
        )
        observed_copy_source_tails = _tail_manifest(generation_copy.source_token)
        observed_copy_destination_tails = _tail_manifest(
            generation_copy.destination_token,
        )
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if (
        generation_copy.source_token is not bootstrap.token
        or generation_copy.source_generation_id == generation_copy.destination_generation_id
        or generation_copy.source_generation_id != report.backup_manifest.destination_generation_id
        or generation_copy.source_tails != report.backup_manifest.per_stream_tails
        or generation_copy.source_generation_id != observed_copy_source_id
        or generation_copy.destination_generation_id != observed_copy_destination_id
        or generation_copy.source_summary != observed_copy_source_summary
        or generation_copy.destination_summary != observed_copy_destination_summary
        or generation_copy.source_tails != observed_copy_source_tails
        or generation_copy.destination_tails != observed_copy_destination_tails
        or generation_copy.source_summary != summary
        or type(generation_copy.destination_summary) is not VerificationSummary
        or generation_copy.destination_summary.profile != summary.profile
        or generation_copy.destination_summary.schema_fingerprint != summary.schema_fingerprint
        or (
            generation_copy.destination_summary.stream_count,
            generation_copy.destination_summary.history_count,
        )
        != (summary.stream_count, summary.history_count)
        or type(generation_copy.source_tails) is not tuple
        or not generation_copy.source_tails
        or generation_copy.source_tails != generation_copy.destination_tails
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _digest_bytes(generation_copy.source_generation_id)
    _digest_bytes(generation_copy.destination_generation_id)
    workload = evidence.workload_thresholds
    if (
        type(workload) is not WorkloadEvidence
        or workload.query_evidence != projection.query_evidence
        or report.query_evidence != workload.query_evidence
        or report.maximum_open_cursors != workload.maximum_open_cursors
        or report.peak_traced_memory_bytes != workload.peak_traced_memory_bytes
        or report.latency_samples_ns != workload.latency_samples_ns
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_report_query_rows(workload.query_evidence)
    return tuple(
        (name, EvidenceDisposition.PASS, None) for name in GENERATED_EVIDENCE_GATES
    ) + tuple(
        (
            name,
            EvidenceDisposition.NOT_APPLICABLE,
            TARGET_NOT_APPLICABLE_REASON,
        )
        for name in TARGET_NOT_APPLICABLE_GATES
    )


def _validate_backup_manifest_shape(manifest: BackupManifest) -> None:
    """Validate the complete closed-file manifest vocabulary and bounds."""

    if (
        type(manifest) is not BackupManifest
        or type(manifest.schema_fingerprint) is not str
        or type(manifest.sqlite_source_id) is not str
        or manifest.page_size != PAGE_SIZE
        or type(manifest.page_size) is not int
        or type(manifest.source_page_count) is not int
        or not 1 <= manifest.source_page_count <= MAX_PAGE_COUNT
        or type(manifest.destination_page_count) is not int
        or manifest.source_page_count != manifest.destination_page_count
        or type(manifest.checkpoint_outcome) is not tuple
        or manifest.checkpoint_outcome != (0, 0, 0)
        or any(type(value) is not int for value in manifest.checkpoint_outcome)
        or type(manifest.finalization_outcome) is not str
        or type(manifest.source_streams) is not int
        or manifest.source_streams < 0
        or type(manifest.source_history_rows) is not int
        or manifest.source_history_rows < 0
        or type(manifest.destination_streams) is not int
        or manifest.destination_streams < 0
        or type(manifest.destination_history_rows) is not int
        or manifest.destination_history_rows < 0
        or manifest.source_streams != manifest.destination_streams
        or manifest.source_history_rows != manifest.destination_history_rows
        or type(manifest.files) is not tuple
        or any(type(item) is not tuple or len(item) != 3 for item in manifest.files)
        or type(manifest.per_stream_tails) is not tuple
        or any(type(item) is not tuple or len(item) != 4 for item in manifest.per_stream_tails)
        or len(manifest.per_stream_tails) != manifest.destination_streams
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_evidence_timestamp(manifest.evidence_recorded_at_utc)
    for digest in (
        manifest.source_generation_id,
        manifest.destination_generation_id,
        manifest.schema_fingerprint,
    ):
        _digest_bytes(digest)
    if manifest.source_generation_id == manifest.destination_generation_id:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    names = tuple(item[0] for item in manifest.files)
    if (
        not manifest.files
        or names != tuple(sorted(names))
        or len(names) != len(set(names))
        or _DATABASE_BASENAME not in names
        or set(names) - _OWNED_DATABASE_FILENAMES
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    expected_finalization = (
        "TRUNCATE_CHECKPOINT_CLOSED_STANDALONE_MAIN"
        if names == (_DATABASE_BASENAME,)
        else "TRUNCATE_CHECKPOINT_CLOSED_COMPLETE_FILE_SET"
    )
    if manifest.finalization_outcome != expected_finalization:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    for name, size, digest in manifest.files:
        maximum = MAX_TEST_DATABASE_BYTES if name == _DATABASE_BASENAME else MAX_TEST_WAL_BYTES
        if (
            type(name) is not str
            or type(size) is not int
            or not 0 <= size <= maximum
            or (name == _DATABASE_BASENAME and size == 0)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _digest_bytes(digest)

    prior_stream_id = ""
    for stream_id, version, envelope_digest, history_root in manifest.per_stream_tails:
        if (
            type(stream_id) is not str
            or type(version) is not int
            or not 1 <= version <= MAX_CONTRACT_INTEGER
            or stream_id <= prior_stream_id
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            if str(UUID(stream_id)) != stream_id:
                raise ValueError
        except (ValueError, AttributeError, TypeError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        _digest_bytes(envelope_digest)
        _digest_bytes(history_root)
        prior_stream_id = stream_id


def _validate_report_backup_manifest(report: EvidenceReport) -> None:
    manifest = report.backup_manifest
    _validate_backup_manifest_shape(manifest)
    if (
        manifest.schema_fingerprint != report.schema_fingerprint
        or manifest.sqlite_source_id != report.sqlite_source_id
        or manifest.page_size != report.page_size
        or manifest.destination_page_count != report.page_count
        or manifest.evidence_recorded_at_utc != report.evidence_recorded_at_utc
        or manifest.destination_streams != report.stream_rows
        or manifest.destination_history_rows != report.history_rows
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_live_backup_manifest(
    manifest: BackupManifest,
    *,
    source_token: StoreToken,
    destination_token: StoreToken,
    source_summary: VerificationSummary,
    destination_summary: VerificationSummary,
    destination_files: tuple[tuple[str, int, str], ...],
    destination_tails: tuple[tuple[str, int, str, str], ...],
    require_exact_source_snapshot: bool,
) -> None:
    """Bind every manifest field to live source/destination identities and profiles."""

    _validate_backup_manifest_shape(manifest)
    if (
        type(source_token) is not StoreToken
        or type(destination_token) is not StoreToken
        or type(source_summary) is not VerificationSummary
        or type(destination_summary) is not VerificationSummary
        or type(destination_files) is not tuple
        or type(destination_tails) is not tuple
        or type(require_exact_source_snapshot) is not bool
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    source_registration = _require_token(source_token)
    destination_registration = _require_token(destination_token)
    if (
        source_registration.pytest_registration is not destination_registration.pytest_registration
        or manifest.source_generation_id != _generation_evidence_id(source_token)
        or manifest.destination_generation_id != _generation_evidence_id(destination_token)
        or manifest.schema_fingerprint != source_summary.schema_fingerprint
        or manifest.schema_fingerprint != destination_summary.schema_fingerprint
        or manifest.sqlite_source_id != source_summary.profile.sqlite_source_id
        or manifest.sqlite_source_id != destination_summary.profile.sqlite_source_id
        or source_summary.profile != destination_summary.profile
        or manifest.page_size != PAGE_SIZE
        or manifest.source_page_count != destination_summary.page_count
        or manifest.destination_page_count != destination_summary.page_count
        or manifest.source_streams != destination_summary.stream_count
        or manifest.source_history_rows != destination_summary.history_count
        or manifest.destination_streams != destination_summary.stream_count
        or manifest.destination_history_rows != destination_summary.history_count
        or manifest.files != destination_files
        or manifest.per_stream_tails != destination_tails
        or (
            require_exact_source_snapshot
            and (
                manifest.source_page_count != source_summary.page_count
                or manifest.source_streams != source_summary.stream_count
                or manifest.source_history_rows != source_summary.history_count
            )
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def write_evidence_report(
    pytest_root: Path,
    *,
    receipt: _EvidenceReceipt,
    report: EvidenceReport,
) -> Path:
    """Write canonical generated evidence beneath one validated pytest root only."""

    root = _validate_bootstrap_root(pytest_root)
    receipt_ledger = _validate_evidence_receipt(
        pytest_root,
        receipt,
        report.evidence,
        require_exact_evidence=False,
    )
    if (
        type(report) is not EvidenceReport
        or report.report_version != 1
        or type(report.report_version) is not int
        or report.task_id != TASK_ID
        or report.contract_generation != TASK_CONTRACT_GENERATION
        or type(report.contract_generation) is not int
        or report.contract_digest != TASK_CONTRACT_DIGEST
        or report.schema_fingerprint != load_schema_fingerprint()
        or report.application_id != APPLICATION_ID
        or type(report.application_id) is not int
        or report.user_version != USER_VERSION
        or type(report.user_version) is not int
        or report.schema_generation != SCHEMA_GENERATION
        or type(report.schema_generation) is not int
        or report.page_size != PAGE_SIZE
        or type(report.page_size) is not int
        or report.storage_marker != STORAGE_MARKER.decode("ascii")
        or report.python_version != ACCEPTED_PYTHON_VERSION
        or report.sqlite_version != ACCEPTED_SQLITE_VERSION
        or report.sqlite_source_id != ACCEPTED_SQLITE_SOURCE_ID
        or report.threadsafety != ACCEPTED_THREADSAFETY
        or type(report.threadsafety) is not int
        or report.compile_options != ACCEPTED_COMPILE_OPTIONS
        or report.connection_profiles != ACCEPTED_CONNECTION_PROFILES
        or report.environment_class != "generated-linux-pytest"
        or type(report.evidence_recorded_at_utc) is not str
        or report.workload_seed != WORKLOAD_SEED
        or type(report.workload_seed) is not int
        or report.workload_runs != WORKLOAD_RUNS
        or type(report.workload_runs) is not int
        or report.record_size_matrix != RECORD_SIZE_MATRIX
        or report.workload_matrix != WORKLOAD_MATRIX
        or report.maximum_operation_latency_ns != MAX_OPERATION_LATENCY_NS
        or type(report.maximum_operation_latency_ns) is not int
        or report.maximum_database_bytes != MAX_TEST_DATABASE_BYTES
        or type(report.maximum_database_bytes) is not int
        or report.maximum_wal_bytes != MAX_TEST_WAL_BYTES
        or type(report.maximum_wal_bytes) is not int
        or report.maximum_traced_memory_bytes != MAX_TEST_TRACED_MEMORY_BYTES
        or type(report.maximum_traced_memory_bytes) is not int
        or report.maximum_open_cursors_threshold != MAX_TEST_OPEN_CURSORS
        or type(report.maximum_open_cursors_threshold) is not int
        or report.maximum_page_count != MAX_PAGE_COUNT
        or type(report.maximum_page_count) is not int
        or report.wal_autocheckpoint_pages != WAL_AUTOCHECKPOINT_PAGES
        or type(report.wal_autocheckpoint_pages) is not int
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_evidence_timestamp(report.evidence_recorded_at_utc)
    _validate_report_backup_manifest(report)
    gates = _derived_report_gates(root, report)
    query_rows = _validated_report_query_rows(report.query_evidence)
    if (
        any(
            type(value) is not int or value < 0
            for value in (
                report.stream_rows,
                report.history_rows,
                report.database_bytes,
                report.wal_bytes,
                report.page_count,
                report.freelist_count,
                report.maximum_open_cursors,
                report.peak_traced_memory_bytes,
            )
        )
        or report.database_bytes > MAX_TEST_DATABASE_BYTES
        or report.wal_bytes > MAX_TEST_WAL_BYTES
        or not 1 <= report.page_count <= MAX_PAGE_COUNT
        or report.freelist_count > report.page_count
        or not 1 <= report.maximum_open_cursors <= MAX_TEST_OPEN_CURSORS
        or report.peak_traced_memory_bytes > MAX_TEST_TRACED_MEMORY_BYTES
        or len(report.latency_samples_ns) != WORKLOAD_RUNS
        or any(
            type(value) is not int or not 0 <= value <= MAX_OPERATION_LATENCY_NS
            for value in report.latency_samples_ns
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    document: dict[str, object] = {
        "report_version": report.report_version,
        "task": {
            "task_id": report.task_id,
            "contract_generation": report.contract_generation,
            "contract_digest": report.contract_digest,
        },
        "schema": {
            "schema_fingerprint": report.schema_fingerprint,
            "application_id": report.application_id,
            "user_version": report.user_version,
            "schema_generation": report.schema_generation,
            "page_size": report.page_size,
            "storage_marker": report.storage_marker,
        },
        "runtime": {
            "python_version": report.python_version,
            "sqlite_version": report.sqlite_version,
            "sqlite_source_id": report.sqlite_source_id,
            "threadsafety": report.threadsafety,
            "compile_options": list(report.compile_options),
            "connection_profiles": [
                {
                    "role": profile.role,
                    "dbconfig": [[name, value] for name, value in profile.dbconfig],
                    "defensive_available": profile.defensive_available,
                    "defensive_enabled": profile.defensive_enabled,
                    "limits": [[name, value] for name, value in profile.limits],
                    "pragmas": [[name, value] for name, value in profile.pragmas],
                }
                for profile in report.connection_profiles
            ],
        },
        "environment_class": report.environment_class,
        "evidence_recorded_at_utc": report.evidence_recorded_at_utc,
        "workload_contract": {
            "seed": report.workload_seed,
            "runs": report.workload_runs,
            "record_size_matrix": [list(row) for row in report.record_size_matrix],
            "workload_matrix": [list(row) for row in report.workload_matrix],
            "thresholds": {
                "maximum_operation_latency_ns": report.maximum_operation_latency_ns,
                "maximum_database_bytes": report.maximum_database_bytes,
                "maximum_wal_bytes": report.maximum_wal_bytes,
                "maximum_traced_memory_bytes": report.maximum_traced_memory_bytes,
                "maximum_open_cursors": report.maximum_open_cursors_threshold,
                "maximum_page_count": report.maximum_page_count,
                "wal_autocheckpoint_pages": report.wal_autocheckpoint_pages,
            },
        },
        "measurements": {
            "stream_rows": report.stream_rows,
            "history_rows": report.history_rows,
            "query_rows": query_rows,
            "database_bytes": report.database_bytes,
            "wal_bytes": report.wal_bytes,
            "page_count": report.page_count,
            "freelist_count": report.freelist_count,
            "maximum_open_cursors": report.maximum_open_cursors,
            "peak_traced_memory_bytes": report.peak_traced_memory_bytes,
            "latency_samples_ns": list(report.latency_samples_ns),
        },
        "backup_manifest": {
            "source_generation_id": report.backup_manifest.source_generation_id,
            "destination_generation_id": report.backup_manifest.destination_generation_id,
            "schema_fingerprint": report.backup_manifest.schema_fingerprint,
            "sqlite_source_id": report.backup_manifest.sqlite_source_id,
            "page_size": report.backup_manifest.page_size,
            "source_page_count": report.backup_manifest.source_page_count,
            "destination_page_count": report.backup_manifest.destination_page_count,
            "checkpoint_outcome": list(report.backup_manifest.checkpoint_outcome),
            "finalization_outcome": report.backup_manifest.finalization_outcome,
            "evidence_recorded_at_utc": (report.backup_manifest.evidence_recorded_at_utc),
            "source_streams": report.backup_manifest.source_streams,
            "source_history_rows": report.backup_manifest.source_history_rows,
            "destination_streams": report.backup_manifest.destination_streams,
            "destination_history_rows": report.backup_manifest.destination_history_rows,
            "files": [list(item) for item in report.backup_manifest.files],
            "per_stream_tails": [list(item) for item in report.backup_manifest.per_stream_tails],
        },
        "gates": [
            {
                "name": name,
                "disposition": disposition.value,
                "reason": reason,
            }
            for name, disposition, reason in gates
        ],
    }
    raw = canonical_descriptor_bytes(document) + b"\n"
    _validate_evidence_receipt(
        pytest_root,
        receipt,
        report.evidence,
        require_exact_evidence=True,
    )
    report_name = "task064-evidence.json"
    path = root / report_name
    stage_name = f".task064-evidence-{secrets.token_hex(16)}.tmp"
    flags = (
        os.O_CREAT
        | os.O_EXCL
        | os.O_WRONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    root_descriptor = -1
    stage_descriptor = -1
    stage_created = False
    published = False
    stage_details: os.stat_result | None = None
    try:
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        root_descriptor = os.open(root, directory_flags)
        active_root = _ACTIVE_PYTEST_ROOTS[id(pytest_root)]
        root_details = os.fstat(root_descriptor)
        if (
            root_details.st_dev != active_root.device
            or root_details.st_ino != active_root.inode
            or root_details.st_uid != active_root.uid
            or stat.S_IMODE(root_details.st_mode) != active_root.mode
            or not stat.S_ISDIR(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        stage_descriptor = os.open(stage_name, flags, 0o600, dir_fd=root_descriptor)
        stage_created = True
        os.fchmod(stage_descriptor, 0o600)
        view = memoryview(raw)
        while view:
            written = os.write(stage_descriptor, view)
            if written <= 0:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            view = view[written:]
        os.fsync(stage_descriptor)
        stage_details = os.fstat(stage_descriptor)
        if (
            not stat.S_ISREG(stage_details.st_mode)
            or stage_details.st_dev != root_details.st_dev
            or stage_details.st_uid != active_root.uid
            or stat.S_IMODE(stage_details.st_mode) != 0o600
            or stage_details.st_nlink != 1
            or stage_details.st_size != len(raw)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        os.close(stage_descriptor)
        stage_descriptor = -1

        final_ledger = _validate_evidence_receipt(
            pytest_root,
            receipt,
            report.evidence,
            require_exact_evidence=True,
        )
        if final_ledger is not receipt_ledger:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        final_root_details = os.fstat(root_descriptor)
        if (
            final_root_details.st_dev != root_details.st_dev
            or final_root_details.st_ino != root_details.st_ino
            or final_root_details.st_uid != root_details.st_uid
            or stat.S_IMODE(final_root_details.st_mode) != stat.S_IMODE(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        os.link(
            stage_name,
            report_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        published = True
        os.unlink(stage_name, dir_fd=root_descriptor)
        stage_created = False
        os.fsync(root_descriptor)
        final_details = os.stat(
            report_name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if (
            stage_details is None
            or final_details.st_dev != stage_details.st_dev
            or final_details.st_ino != stage_details.st_ino
            or final_details.st_uid != stage_details.st_uid
            or final_details.st_size != len(raw)
            or final_details.st_nlink != 1
            or not stat.S_ISREG(final_details.st_mode)
            or stat.S_IMODE(final_details.st_mode) != 0o600
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    except BaseException as error:
        cleanup_ok = True
        if stage_descriptor >= 0:
            try:
                os.close(stage_descriptor)
            except OSError:
                cleanup_ok = False
            stage_descriptor = -1
        if root_descriptor >= 0 and published and stage_details is not None:
            try:
                final_details = os.stat(
                    report_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                if (
                    final_details.st_dev != stage_details.st_dev
                    or final_details.st_ino != stage_details.st_ino
                ):
                    cleanup_ok = False
                else:
                    os.unlink(report_name, dir_fd=root_descriptor)
                    published = False
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0 and stage_created and stage_details is not None:
            try:
                pending_details = os.stat(
                    stage_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                if (
                    pending_details.st_dev != stage_details.st_dev
                    or pending_details.st_ino != stage_details.st_ino
                ):
                    cleanup_ok = False
                else:
                    os.unlink(stage_name, dir_fd=root_descriptor)
                    stage_created = False
            except OSError:
                cleanup_ok = False
        elif root_descriptor >= 0 and stage_created:
            try:
                os.unlink(stage_name, dir_fd=root_descriptor)
                stage_created = False
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0:
            try:
                os.fsync(root_descriptor)
            except OSError:
                cleanup_ok = False
        if not cleanup_ok:
            receipt_ledger.consumed = True
        if isinstance(error, HarnessFailure):
            raise
        if isinstance(error, (MemoryError, OSError)):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        raise
    finally:
        if stage_descriptor >= 0:
            with suppress(OSError):
                os.close(stage_descriptor)
        if root_descriptor >= 0:
            with suppress(OSError):
                os.close(root_descriptor)
    receipt_ledger.consumed = True
    return path
