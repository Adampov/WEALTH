"""Generated-data SQLite evidence harness for TASK-064.

This module is test support only.  It owns every database it opens, never accepts SQL or URI
options from a caller, and is intentionally not importable from production source.
"""

from __future__ import annotations

import atexit
import ctypes
import errno
import fcntl
import hashlib
import inspect
import json
import math
import mmap
import os
import resource
import secrets
import select
import selectors
import signal
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import threading
import time
import tracemalloc
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager, suppress
from contextvars import Context, ContextVar, Token
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import UTC, datetime
from enum import Enum, StrEnum
from functools import partial, wraps
from pathlib import Path
from threading import get_ident
from types import CodeType, MappingProxyType
from typing import Any, Final, Never, ParamSpec, TypeVar, cast
from uuid import UUID
from weakref import WeakSet

from wealth.domain.continuous_public_trade import (
    MAX_CONTRACT_INTEGER,
    ContinuousPublicTradePolicy,
    ContinuousPublicTradeTransitionKind,
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
TASK_CONTRACT_GENERATION: Final = 6
TASK_CONTRACT_DIGEST: Final = "ec89a1df740805cc9b43e6f2530e940c0bf9b66e8f25ed878d3207d091c4bcb8"
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


_RejectionScenarioSnapshot = tuple[
    str,
    str,
    HarnessFailureCode,
    tuple[str, ...],
    str | None,
]


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
class _MutationOperationBinding:
    """Exact mutation inputs retained by a closure-issued operation receipt."""

    stored_record: (
        ContinuousPublicTradeStreamStoredCreationV1 | ContinuousPublicTradeStreamStoredTransitionV1
    )
    policy_digest: str | None
    expectation_identity: tuple[object, ...] | None
    expectation_policy_digest: str | None
    expectation_child_policy_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class _QueryOperationBinding:
    """Exact bounded-query inputs retained by a closure-issued receipt."""

    stream_id: UUID
    natural_key: bytes
    limit: int | None
    continuation: tuple[int, str, str] | None
    expectation_identity: tuple[object, ...] | None
    expectation_policy_digest: str | None
    expectation_child_policy_fingerprint: str | None


@dataclass(frozen=True, slots=True)
class _ValidatedIdentityCandidate:
    """One bounded candidate retaining both creation and current witnesses."""

    creation: ContinuousPublicTradeStreamStoredCreationV1
    current: ContinuousPublicTradeStreamStoredHistoryEntryV1


@dataclass(frozen=True, slots=True)
class _SchemaFixtureSnapshot:
    """One fresh, operation-local view of the two committed schema fixtures."""

    descriptor: dict[str, object]
    fingerprint: str
    fingerprint_bytes: bytes


@dataclass(frozen=True, slots=True)
class _DecodedCanonicalHistoryRecord:
    """Canonical record evidence that carries no physical-row authority."""

    record: (
        ContinuousPublicTradeStreamCreationRecordV1 | ContinuousPublicTradeStreamTransitionRecordV1
    )
    canonical_bytes: bytes
    record_digest: str
    successor_envelope: ContinuousPublicTradeStreamEnvelopeV1
    successor_envelope_bytes: bytes
    successor_envelope_digest: str
    history_root: str


@dataclass(frozen=True, slots=True)
class _HistoryPredecessorLinkSeal:
    """Compact predecessor link evidence without retaining its decoded model."""

    stream_uuid: bytes
    successor_version: int
    entry_kind: bytes
    canonical_bytes: bytes
    record_digest: str
    successor_envelope_bytes: bytes
    successor_envelope_digest: str
    history_root: str
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class _ValidatedHistoryRowSnapshot:
    """One fully validated physical row plus its canonical decoded evidence."""

    row: sqlite3.Row
    decoded: _DecodedCanonicalHistoryRecord
    predecessor_link: _HistoryPredecessorLinkSeal | None
    entry: ContinuousPublicTradeStreamStoredHistoryEntryV1


_MAX_LOCAL_HISTORY_SNAPSHOTS: Final = 101
_HISTORY_ROW_COLUMNS: Final = (
    "history_row_id",
    "stream_row_id",
    "successor_version",
    "entry_kind",
    "record_model_version",
    "serialization_version",
    "record_canonical_bytes",
    "record_digest",
    "successor_envelope_canonical_bytes",
    "successor_envelope_digest",
    "prior_version",
    "prior_envelope_digest",
    "prior_history_root",
    "predecessor_record_canonical_bytes",
    "predecessor_record_digest",
    "successor_history_root",
)


@dataclass(slots=True)
class _HistorySnapshotCache:
    """Bounded operation-local issuer for decoded records and physical snapshots."""

    process_id: int
    decoded_records: dict[bytes, _DecodedCanonicalHistoryRecord]
    decoded_issued: dict[int, tuple[_DecodedCanonicalHistoryRecord, bytes]]
    validated_rows: dict[tuple[int, int], _ValidatedHistoryRowSnapshot]
    snapshot_issued: dict[int, tuple[object, ...]]
    valid: bool

    def _require_live(self) -> None:
        if type(self) is not _HistorySnapshotCache or self.valid is not True:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if (
            type(self.decoded_records) is not dict
            or type(self.decoded_issued) is not dict
            or type(self.validated_rows) is not dict
            or type(self.snapshot_issued) is not dict
        ):
            _invalidate_history_snapshot_cache_preserving_primary(self)
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if self.process_id != os.getpid():
            self.invalidate()
            raise HarnessFailure(HarnessFailureCode.CORRUPT)

    def decoded(self, canonical_bytes: bytes) -> _DecodedCanonicalHistoryRecord | None:
        self._require_live()
        decoded = self.decoded_records.get(canonical_bytes)
        if decoded is None:
            return None
        issued = self.decoded_issued.get(id(decoded))
        if (
            issued is None
            or issued[0] is not decoded
            or issued[1] != canonical_bytes
            or decoded.canonical_bytes != canonical_bytes
        ):
            self.invalidate()
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return decoded

    def issue_decoded(self, decoded: _DecodedCanonicalHistoryRecord) -> None:
        try:
            self._require_live()
            if type(decoded) is not _DecodedCanonicalHistoryRecord:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            canonical_bytes = decoded.canonical_bytes
            if canonical_bytes in self.decoded_records or id(decoded) in self.decoded_issued:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if len(self.decoded_records) >= _MAX_LOCAL_HISTORY_SNAPSHOTS:
                raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
            self.decoded_records[canonical_bytes] = decoded
            self.decoded_issued[id(decoded)] = (decoded, canonical_bytes)
        except BaseException:
            _invalidate_history_snapshot_cache_preserving_primary(self)
            raise

    def cached_row(self, key: tuple[int, int]) -> _ValidatedHistoryRowSnapshot | None:
        self._require_live()
        snapshot = self.validated_rows.get(key)
        if snapshot is not None:
            self.require_snapshot(snapshot)
        return snapshot

    def issue_snapshot(self, snapshot: _ValidatedHistoryRowSnapshot) -> None:
        try:
            self._require_live()
            _validate_history_snapshot_coherence(snapshot)
            key = (
                cast(int, snapshot.row["stream_row_id"]),
                cast(int, snapshot.row["successor_version"]),
            )
            row_keys, row_values = _history_row_binding(snapshot.row)
            if (
                key in self.validated_rows
                or id(snapshot) in self.snapshot_issued
                or self.decoded(snapshot.decoded.canonical_bytes) is not snapshot.decoded
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if len(self.validated_rows) >= _MAX_LOCAL_HISTORY_SNAPSHOTS:
                raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
            self.validated_rows[key] = snapshot
            self.snapshot_issued[id(snapshot)] = (
                snapshot,
                snapshot.row,
                snapshot.decoded,
                snapshot.predecessor_link,
                snapshot.entry,
                row_keys,
                row_values,
            )
        except BaseException:
            _invalidate_history_snapshot_cache_preserving_primary(self)
            raise

    def require_snapshot(self, snapshot: _ValidatedHistoryRowSnapshot) -> None:
        try:
            self._require_live()
            if type(snapshot) is not _ValidatedHistoryRowSnapshot:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            issued = self.snapshot_issued.get(id(snapshot))
            row_keys, row_values = _history_row_binding(snapshot.row)
            if (
                issued is None
                or issued[0] is not snapshot
                or issued[1] is not snapshot.row
                or issued[2] is not snapshot.decoded
                or issued[3] is not snapshot.predecessor_link
                or issued[4] is not snapshot.entry
                or issued[5] != row_keys
                or issued[6] != row_values
                or self.decoded(snapshot.decoded.canonical_bytes) is not snapshot.decoded
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _validate_history_snapshot_coherence(snapshot)
        except BaseException:
            _invalidate_history_snapshot_cache_preserving_primary(self)
            raise

    def retain_boundary(self, snapshot: _ValidatedHistoryRowSnapshot) -> None:
        try:
            self.require_snapshot(snapshot)
            decoded_record = self.decoded_issued[id(snapshot.decoded)]
            issued_snapshot = self.snapshot_issued[id(snapshot)]
            key = (
                cast(int, snapshot.row["stream_row_id"]),
                cast(int, snapshot.row["successor_version"]),
            )
            new_decoded = {snapshot.decoded.canonical_bytes: snapshot.decoded}
            new_decoded_issued = {id(snapshot.decoded): decoded_record}
            new_rows = {key: snapshot}
            new_snapshot_issued = {id(snapshot): issued_snapshot}
            self.decoded_records = new_decoded
            self.decoded_issued = new_decoded_issued
            self.validated_rows = new_rows
            self.snapshot_issued = new_snapshot_issued
            if (
                self.cardinalities() != (1, 1, 1, 1)
                or self.decoded_records.get(snapshot.decoded.canonical_bytes)
                is not snapshot.decoded
                or self.decoded_issued.get(id(snapshot.decoded)) is not decoded_record
                or self.validated_rows.get(key) is not snapshot
                or self.snapshot_issued.get(id(snapshot)) is not issued_snapshot
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
        except BaseException:
            _invalidate_history_snapshot_cache_preserving_primary(self)
            raise

    def cardinalities(self) -> tuple[int, int, int, int]:
        self._require_live()
        return (
            len(self.decoded_records),
            len(self.decoded_issued),
            len(self.validated_rows),
            len(self.snapshot_issued),
        )

    def invalidate(self) -> None:
        self.valid = False
        first_failure: BaseException | None = None
        decoded_records = self.decoded_records
        if type(decoded_records) is not dict:
            self.decoded_records = {}
        try:
            decoded_records.clear()
        except BaseException as error:
            first_failure = error
        decoded_issued = self.decoded_issued
        if type(decoded_issued) is not dict:
            self.decoded_issued = {}
        try:
            decoded_issued.clear()
        except BaseException as error:
            if first_failure is None:
                first_failure = error
        validated_rows = self.validated_rows
        if type(validated_rows) is not dict:
            self.validated_rows = {}
        try:
            validated_rows.clear()
        except BaseException as error:
            if first_failure is None:
                first_failure = error
        snapshot_issued = self.snapshot_issued
        if type(snapshot_issued) is not dict:
            self.snapshot_issued = {}
        try:
            snapshot_issued.clear()
        except BaseException as error:
            if first_failure is None:
                first_failure = error
        if first_failure is not None:
            raise first_failure


def _invalidate_history_snapshot_cache_preserving_primary(
    cache: _HistorySnapshotCache,
) -> None:
    try:
        cache.invalidate()
    except BaseException:
        return


def _new_history_snapshot_cache() -> _HistorySnapshotCache:
    return _HistorySnapshotCache(os.getpid(), {}, {}, {}, {}, True)


@dataclass(frozen=True, slots=True)
class _OperationObservation:
    """One closure-issued low-level executor result in a fixed gate scope."""

    producer_name: str
    operation_tag: str
    input_binding: _MutationOperationBinding | _QueryOperationBinding | None
    input_digest: str | None
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


@dataclass(slots=True)
class _ReportPublicationRootState:
    """Ledger-free root state for authenticated report-publication children."""

    nonce: bytes


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
    evidence_ledger: _EvidenceLedger | _ReportPublicationRootState


@dataclass(frozen=True, slots=True)
class _PytestRootCapability:
    """Opaque fixture result naming the exact roots pre-issued for one test."""

    roots: tuple[Path, ...]
    _nonce: bytes


@dataclass(frozen=True, slots=True)
class _PytestRootRegistrationPermit:
    """Opaque one-shot permit issued before a fixture creates its auxiliary roots."""

    _nonce: bytes


@dataclass(frozen=True, slots=True)
class _EvidenceRun:
    """Unforgeable object-identity capability for one active evidence execution."""

    _pytest_registration: _ActivePytestRoot
    _nonce: bytes


@dataclass(frozen=True, slots=True)
class _GateEvidenceReceipt:
    """One sealed generated-gate payload bound to its exact live object."""

    gate: str
    payload: object
    payload_digest: str


@dataclass(frozen=True, slots=True)
class _EvidenceReceipt:
    """Consume-on-success authority for one exact generated evidence aggregate."""

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
class _ConnectionImmutableRuntimeEvidence:
    """Connection-local immutable runtime observations, independent of public reports."""

    python_version: str
    sqlite_version: str
    threadsafety: int
    sqlite_source_id: str
    compile_options: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _LiveTransactionAuthorityView:
    """Fresh non-authoritative view returned by one exact closure-owned live check."""

    registered: _RegisteredIdentity
    database_list_path: str


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
    stream_rows: int
    history_rows: int
    committed: bool

    @property
    def rows_materialized(self) -> int:
        """Return the computed total without creating a forgeable third count."""

        return self.stream_rows + self.history_rows


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
    nonce: bytes


@dataclass(frozen=True, slots=True)
class _OperationPathAcquisition:
    nonce: bytes


_RegisteredIdentityFields = tuple[
    Path,
    _ActivePytestRoot,
    Path,
    Path,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
    int,
]
_TokenAuthorityView = tuple[
    str,
    str,
    str,
    int,
    int,
    int,
    int,
    int,
]


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
_GENERATION_COUNTER = 0


def _build_authority_nonce_issuer() -> Callable[[str], bytes]:
    """Capture one process-lifetime entropy source and reject every reused nonce."""

    entropy = secrets.token_bytes
    seen: set[bytes] = set()

    def issue(kind: str) -> bytes:
        if type(kind) is not str or not kind:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        try:
            nonce = entropy(32)
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if type(nonce) is not bytes or len(nonce) != 32 or nonce in seen:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        seen.add(nonce)
        return nonce

    return issue


_issue_authority_nonce = _build_authority_nonce_issuer()
del _build_authority_nonce_issuer


def _registered_identity_fields(identity: _RegisteredIdentity) -> _RegisteredIdentityFields:
    """Return the exact immutable authority-bearing fields of an identity view."""

    return (
        identity.pytest_root,
        identity.pytest_registration,
        identity.generation_root,
        identity.database_path,
        identity.pytest_root_device,
        identity.pytest_root_inode,
        identity.pytest_root_uid,
        identity.pytest_root_mode,
        identity.generation_device,
        identity.generation_inode,
        identity.generation_uid,
        identity.generation_mode,
        identity.device,
        identity.inode,
        identity.uid,
        identity.mode,
        identity.link_count,
    )


def _identity_from_fields(fields: _RegisteredIdentityFields) -> _RegisteredIdentity:
    """Return a fresh non-authoritative identity view from closure-owned fields."""

    return _RegisteredIdentity(*fields)


def _store_token_fields(token: StoreToken) -> tuple[object, ...]:
    """Include object identities so equal replacement fields cannot rebind a token."""

    return (
        id(token._nonce),
        token._nonce,
        id(token._pytest_root),
        str(token._pytest_root),
        id(token._generation_root),
        str(token._generation_root),
        id(token._database_path),
        str(token._database_path),
        token._device,
        token._inode,
        token._uid,
        token._mode,
        token._link_count,
    )


def _build_store_token_authority() -> tuple[
    Callable[
        [Callable[[Path, Callable[[_RegisteredIdentity], StoreToken]], StoreToken]],
        Callable[[Path], StoreToken],
    ],
    Callable[[StoreToken], _RegisteredIdentity | None],
    Callable[[bytes], _RegisteredIdentity | None],
    Callable[[StoreToken, _RegisteredIdentity], bool],
    Callable[[StoreToken], bool],
    Callable[[], Mapping[bytes, _TokenAuthorityView]],
]:
    """Keep token issuance and exact object identity outside mutable module mirrors."""

    nonce_issuer = _issue_authority_nonce
    issued: dict[int, tuple[StoreToken, tuple[object, ...], _RegisteredIdentityFields]] = {}
    by_nonce: dict[
        bytes,
        tuple[StoreToken, tuple[object, ...], _RegisteredIdentityFields],
    ] = {}
    retired: dict[int, tuple[StoreToken, tuple[object, ...]]] = {}

    def valid_record(
        token: StoreToken,
    ) -> tuple[StoreToken, tuple[object, ...], _RegisteredIdentityFields] | None:
        if type(token) is not StoreToken:
            return None
        record = issued.get(id(token))
        if (
            record is None
            or record[0] is not token
            or _store_token_fields(token) != record[1]
            or type(token._nonce) is not bytes
            or len(token._nonce) != 32
            or by_nonce.get(token._nonce) is not record
        ):
            return None
        return record

    def issuing_bootstrap(
        function: Callable[[Path, Callable[[_RegisteredIdentity], StoreToken]], StoreToken],
    ) -> Callable[[Path], StoreToken]:
        def wrapped(pytest_root: Path) -> StoreToken:
            def issue(registered: _RegisteredIdentity) -> StoreToken:
                registered_fields = _registered_identity_fields(registered)
                nonce = nonce_issuer("store-token")
                token = StoreToken(
                    _nonce=nonce,
                    _pytest_root=registered_fields[0],
                    _generation_root=registered_fields[2],
                    _database_path=registered_fields[3],
                    _device=registered_fields[12],
                    _inode=registered_fields[13],
                    _uid=registered_fields[14],
                    _mode=registered_fields[15],
                    _link_count=registered_fields[16],
                )
                token_fields = _store_token_fields(token)
                authority_record = (token, token_fields, registered_fields)
                if id(token) in issued or id(token) in retired or nonce in by_nonce:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                issued[id(token)] = authority_record
                by_nonce[nonce] = authority_record
                return token

            return function(pytest_root, issue)

        wrapped.__name__ = function.__name__
        wrapped.__qualname__ = function.__qualname__
        wrapped.__doc__ = function.__doc__
        wrapped.__annotations__ = {"pytest_root": Path, "return": StoreToken}
        return wrapped

    def lookup(token: StoreToken) -> _RegisteredIdentity | None:
        record = valid_record(token)
        return None if record is None else _identity_from_fields(record[2])

    def lookup_nonce(nonce: bytes) -> _RegisteredIdentity | None:
        if type(nonce) is not bytes or len(nonce) != 32:
            return None
        record = by_nonce.get(nonce)
        if record is None or valid_record(record[0]) is not record:
            return None
        return _identity_from_fields(record[2])

    def revoke(token: StoreToken, registered: _RegisteredIdentity) -> bool:
        record = valid_record(token)
        if record is None or _registered_identity_fields(registered) != record[2]:
            return False
        by_nonce.pop(token._nonce, None)
        issued.pop(id(token), None)
        retired[id(token)] = (token, record[1])
        return True

    def is_retired(token: StoreToken) -> bool:
        return bool(
            type(token) is StoreToken
            and type(token._nonce) is bytes
            and len(token._nonce) == 32
            and (record := retired.get(id(token))) is not None
            and record[0] is token
            and _store_token_fields(token) == record[1]
        )

    def snapshot() -> Mapping[bytes, _TokenAuthorityView]:
        return MappingProxyType(
            {
                nonce: (
                    "ACTIVE",
                    str(record[2][0]),
                    str(record[2][2]),
                    record[2][12],
                    record[2][13],
                    record[2][14],
                    record[2][15],
                    record[2][16],
                )
                for nonce, record in by_nonce.items()
                if valid_record(record[0]) is record
            }
        )

    return issuing_bootstrap, lookup, lookup_nonce, revoke, is_retired, snapshot


(
    _store_token_issuing_bootstrap,
    _lookup_store_token_authority,
    _lookup_store_identity_by_nonce,
    _revoke_store_token_authority,
    _is_retired_store_token,
    _token_authority_snapshot,
) = _build_store_token_authority()
del _build_store_token_authority

_Parameters = ParamSpec("_Parameters")
_Result = TypeVar("_Result")
_MAX_EVIDENCE_TRAVERSAL_DEPTH: Final = 64


@contextmanager
def _evidence_traversal_guard(
    value: object,
    *,
    depth: int,
    active_ids: set[int],
) -> Iterator[None]:
    """Reject cyclic or unbounded evidence shapes with one sanitized outcome."""

    identity = id(value)
    if depth > _MAX_EVIDENCE_TRAVERSAL_DEPTH or identity in active_ids:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    active_ids.add(identity)
    try:
        yield
    finally:
        active_ids.remove(identity)


def _normalized_evidence_payload(value: object) -> object:
    """Return a strict type-tagged value suitable for a private payload digest."""

    active_ids: set[int] = set()

    def normalize(item: object, depth: int) -> object:
        with _evidence_traversal_guard(
            item,
            depth=depth,
            active_ids=active_ids,
        ):
            if item is None:
                return ["none"]
            if isinstance(item, Enum):
                return [
                    "enum",
                    f"{type(item).__module__}.{type(item).__qualname__}",
                    item.value,
                ]
            if type(item) is bool:
                return ["bool", item]
            if type(item) is int:
                return ["int", item]
            if type(item) is float:
                if not math.isfinite(item):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                return ["float", item.hex()]
            if type(item) is str:
                return ["str", item]
            if type(item) is bytes:
                return ["bytes", item.hex()]
            if isinstance(item, UUID):
                return ["uuid", str(item)]
            if isinstance(item, Path):
                return ["path", str(item)]
            if isinstance(item, datetime):
                return ["datetime", item.isoformat()]
            if type(item) in (
                ContinuousPublicTradeStreamStoredCreationV1,
                ContinuousPublicTradeStreamStoredTransitionV1,
            ):
                try:
                    stored_value = cast(
                        ContinuousPublicTradeStreamStoredCreationV1
                        | ContinuousPublicTradeStreamStoredTransitionV1,
                        item,
                    )
                    dumped = stored_value.model_dump(mode="json")
                except (AttributeError, TypeError, ValueError):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
                return [
                    "pydantic",
                    f"{type(item).__module__}.{type(item).__qualname__}",
                    normalize(dumped, depth + 1),
                ]
            if type(item) is tuple:
                return [
                    "tuple",
                    [normalize(nested, depth + 1) for nested in item],
                ]
            if type(item) is list:
                return [
                    "list",
                    [normalize(nested, depth + 1) for nested in item],
                ]
            if type(item) is dict:
                normalized_items = [
                    (
                        normalize(key, depth + 1),
                        normalize(nested, depth + 1),
                    )
                    for key, nested in dict.items(item)
                ]
                normalized_items.sort(
                    key=lambda pair: json.dumps(
                        pair[0],
                        ensure_ascii=True,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                return ["mapping", normalized_items]
            if isinstance(item, Mapping):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if is_dataclass(item) and not isinstance(item, type):
                return [
                    "dataclass",
                    f"{type(item).__module__}.{type(item).__qualname__}",
                    [
                        [
                            field.name,
                            normalize(getattr(item, field.name), depth + 1),
                        ]
                        for field in fields(item)
                    ],
                ]
            raise HarnessFailure(HarnessFailureCode.CORRUPT)

    return normalize(value, 0)


def _evidence_payload_digest(value: object) -> str:
    try:
        raw = json.dumps(
            _normalized_evidence_payload(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (AttributeError, RecursionError, TypeError, ValueError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    return hashlib.sha256(raw).hexdigest()


def _collect_evidence_registrations(
    value: object,
    registrations: list[_ActivePytestRoot],
    token_nonces: list[bytes],
) -> None:
    active_ids: set[int] = set()

    def collect(item: object, depth: int) -> None:
        with _evidence_traversal_guard(
            item,
            depth=depth,
            active_ids=active_ids,
        ):
            if type(item) is StoreToken:
                registered = _lookup_store_token_authority(item)
                if registered is None:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if registered.pytest_registration not in registrations:
                    registrations.append(registered.pytest_registration)
                if item._nonce not in token_nonces:
                    token_nonces.append(item._nonce)
                return
            if isinstance(item, Path):
                registration = _lookup_active_pytest_root(item)
                if registration is not None and registration not in registrations:
                    registrations.append(registration)
                return
            if type(item) is tuple or type(item) is list:
                for nested in item:
                    collect(nested, depth + 1)
                return
            if type(item) is dict:
                for key, nested in dict.items(item):
                    collect(key, depth + 1)
                    collect(nested, depth + 1)
                return
            if isinstance(item, Mapping):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if is_dataclass(item) and not isinstance(item, type):
                for field in fields(item):
                    collect(getattr(item, field.name), depth + 1)

    collect(value, 0)


def _validate_collector_value_registration(
    value: object,
    registration: _ActivePytestRoot,
) -> None:
    """Fail before authority-bearing work when a value crosses pytest scopes."""

    active_ids: set[int] = set()

    def validate(item: object, depth: int) -> None:
        with _evidence_traversal_guard(
            item,
            depth=depth,
            active_ids=active_ids,
        ):
            if type(item) is StoreToken:
                registered = _lookup_store_token_authority(item)
                if (
                    registered is None
                    or registered.pytest_registration is not registration
                    or _require_token(item) != registered
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                return
            if isinstance(item, Path):
                if (
                    item is not registration.path_object
                    or _lookup_active_pytest_root(item) is not registration
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                _validate_bootstrap_root(item)
                return
            if type(item) is tuple or type(item) is list:
                for nested in item:
                    validate(nested, depth + 1)
                return
            if type(item) is dict:
                for key, nested in dict.items(item):
                    validate(key, depth + 1)
                    validate(nested, depth + 1)
                return
            if isinstance(item, Mapping):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if is_dataclass(item) and not isinstance(item, type):
                for field in fields(item):
                    validate(getattr(item, field.name), depth + 1)

    validate(value, 0)


def _validate_collector_value_authority(
    value: object,
    registration: _ActivePytestRoot,
) -> None:
    """Reject foreign registrations before any token or filesystem validation."""

    active_ids: set[int] = set()

    def validate(item: object, depth: int) -> None:
        with _evidence_traversal_guard(
            item,
            depth=depth,
            active_ids=active_ids,
        ):
            if type(item) is StoreToken:
                registered = _lookup_store_token_authority(item)
                if registered is None or registered.pytest_registration is not registration:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                return
            if isinstance(item, Path):
                if (
                    item is not registration.path_object
                    or _lookup_active_pytest_root(item) is not registration
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                return
            if type(item) is tuple or type(item) is list:
                for nested in item:
                    validate(nested, depth + 1)
                return
            if type(item) is dict:
                for key, nested in dict.items(item):
                    validate(key, depth + 1)
                    validate(nested, depth + 1)
                return
            if isinstance(item, Mapping):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if is_dataclass(item) and not isinstance(item, type):
                for field in fields(item):
                    validate(getattr(item, field.name), depth + 1)

    validate(value, 0)


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
    "compare_and_swap_stream",
    "compare_and_swap_stream",
    "create_stream",
    "compare_and_swap_stream",
    "create_stream",
    "compare_and_swap_stream",
    "compare_and_swap_stream",
    "create_stream",
    "compare_and_swap_stream",
)
_BOUNDED_OPERATION_PRODUCER_SEQUENCE: Final = (
    "load_current",
    "load_current",
    *(("audit_history",) * 5),
    "bootstrap_store",
    "create_stream",
    "compare_and_swap_stream",
    "compare_and_swap_stream",
    "create_stream",
    "compare_and_swap_stream",
    "compare_and_swap_stream",
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


def _policy_input_digest(policy: object) -> str:
    """Canonicalize only the exact frozen TASK061 policy input."""

    if type(policy) is not ContinuousPublicTradePolicy:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    try:
        payload = project_continuous_public_trade_policy(policy).model_dump(mode="json")
    except (AttributeError, TypeError, ValueError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    return _evidence_payload_digest(payload)


def _stored_creation_policy_digest(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> str:
    try:
        payload = creation.record.stream_policy.model_dump(mode="json")
    except (AttributeError, TypeError, ValueError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    return _evidence_payload_digest(payload)


def _stored_creation_identity_binding(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> tuple[object, ...]:
    record = creation.record
    return (
        1,
        record.stream_id,
        record.source,
        record.venue,
        record.instrument,
        record.provider_symbol,
        record.instrument_type.value,
        record.request_variant,
        record.stream_policy.policy_fingerprint,
        record.stream_start_epoch_ms,
    )


def _stored_creation_natural_key(
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> bytes:
    record = creation.record
    return natural_identity_key(
        source=record.source,
        venue=record.venue,
        instrument=record.instrument,
        provider_symbol=record.provider_symbol,
        instrument_type=record.instrument_type.value,
        request_variant=record.request_variant,
    )


def _transition_follows_binding(
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
) -> bool:
    return bool(
        transition.record.stream_id == prior.record.stream_id
        and transition.record.prior_version == prior.record.successor_version
        and transition.record.successor_version == prior.record.successor_version + 1
        and transition.record.prior_envelope_digest == prior.successor_envelope.envelope_digest
        and transition.record.prior_history_root == prior.history_root
    )


def _expectation_identity_binding(
    expectation: ContinuousPublicTradeStreamExpectationV1,
) -> tuple[object, ...]:
    identity = expectation.identity
    return (
        identity.stream_contract_version,
        identity.stream_id,
        identity.source,
        identity.venue,
        identity.instrument,
        identity.provider_symbol,
        identity.instrument_type.value,
        identity.request_variant,
        identity.policy_fingerprint,
        identity.stream_start_epoch_ms,
    )


def _operation_input_binding(
    producer_name: str,
    arguments: Mapping[str, object],
) -> _MutationOperationBinding | _QueryOperationBinding | None:
    """Bind exact mutation/query inputs without caller-supplied semantic labels."""

    if producer_name == "create_stream":
        creation = arguments.get("creation")
        policy = arguments.get("policy")
        if (
            type(creation) is not ContinuousPublicTradeStreamStoredCreationV1
            or type(policy) is not ContinuousPublicTradePolicy
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_creation = creation
        policy_digest = _policy_input_digest(policy)
        if policy_digest != _stored_creation_policy_digest(exact_creation):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return _MutationOperationBinding(
            stored_record=exact_creation,
            policy_digest=policy_digest,
            expectation_identity=None,
            expectation_policy_digest=None,
            expectation_child_policy_fingerprint=None,
        )
    if producer_name == "compare_and_swap_stream":
        transition = arguments.get("transition")
        expectation = arguments.get("expectation")
        if type(transition) is not ContinuousPublicTradeStreamStoredTransitionV1 or (
            expectation is not None
            and type(expectation) is not ContinuousPublicTradeStreamExpectationV1
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        exact_expectation = expectation
        return _MutationOperationBinding(
            stored_record=transition,
            policy_digest=None,
            expectation_identity=(
                None
                if exact_expectation is None
                else _expectation_identity_binding(exact_expectation)
            ),
            expectation_policy_digest=(
                None
                if exact_expectation is None
                else _policy_input_digest(exact_expectation.effective_stream_policy)
            ),
            expectation_child_policy_fingerprint=(
                None
                if exact_expectation is None
                else exact_expectation.effective_child_policy_fingerprint
            ),
        )
    if producer_name in {"load_current", "audit_history", "query_plan_evidence"}:
        stream_id = arguments.get("stream_id")
        natural_key = arguments.get("natural_key")
        limit = arguments.get("limit")
        continuation = arguments.get("continuation")
        expectation = arguments.get("expectation")
        if (
            type(stream_id) is not UUID
            or type(natural_key) is not bytes
            or (
                producer_name == "audit_history"
                and (type(limit) is not int or not 1 <= limit <= 100)
            )
            or (producer_name != "audit_history" and limit is not None)
            or (
                continuation is not None
                and (
                    type(continuation) is not tuple
                    or len(continuation) != 3
                    or type(continuation[0]) is not int
                    or type(continuation[1]) is not str
                    or type(continuation[2]) is not str
                )
            )
            or (producer_name != "audit_history" and continuation is not None)
            or (
                expectation is not None
                and type(expectation) is not ContinuousPublicTradeStreamExpectationV1
            )
            or (producer_name == "query_plan_evidence" and expectation is not None)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        decode_natural_identity_key(natural_key)
        if continuation is not None:
            _require_exact_int(
                continuation[0],
                minimum=1,
                maximum=MAX_CONTRACT_INTEGER,
            )
            _digest_bytes(continuation[1])
            _digest_bytes(continuation[2])
        exact_expectation = expectation
        return _QueryOperationBinding(
            stream_id=stream_id,
            natural_key=natural_key,
            limit=(cast(int, limit) if producer_name == "audit_history" else None),
            continuation=cast(tuple[int, str, str] | None, continuation),
            expectation_identity=(
                None
                if exact_expectation is None
                else _expectation_identity_binding(exact_expectation)
            ),
            expectation_policy_digest=(
                None
                if exact_expectation is None
                else _policy_input_digest(exact_expectation.effective_stream_policy)
            ),
            expectation_child_policy_fingerprint=(
                None
                if exact_expectation is None
                else exact_expectation.effective_child_policy_fingerprint
            ),
        )
    return None


def _operation_evidence_tag(
    producer_name: str,
    arguments: tuple[object, ...],
    keywords: Mapping[str, object],
    result: object,
    *,
    gate: str,
    sequence: int,
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
    if producer_name in {"create_stream", "compare_and_swap_stream"}:
        if type(result) is not MutationEvidence:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        semantic_case: str | None = None
        if (
            gate == "atomicity_classification"
            and sequence == 5
            and producer_name == "compare_and_swap_stream"
            and result.classification is StoreClassification.DUPLICATE
            and (result.stream_rows, result.history_rows) == (1, 5)
        ):
            semantic_case = "historical_duplicate_mature"
        elif (
            gate == "atomicity_classification"
            and sequence == 9
            and producer_name == "create_stream"
            and result.classification is StoreClassification.CONFLICT
            and (result.stream_rows, result.history_rows) == (2, 6)
        ):
            semantic_case = "create_two_candidate_conflict_mature"
        elif (
            gate == "atomicity_classification"
            and sequence == 10
            and producer_name == "compare_and_swap_stream"
            and result.classification is StoreClassification.CONFLICT
            and (result.stream_rows, result.history_rows) == (2, 6)
            and type(keywords.get("expectation")) is ContinuousPublicTradeStreamExpectationV1
        ):
            semantic_case = "cas_expectation_two_candidate_conflict_mature"
        prefix = (
            result.classification.value
            if semantic_case is None
            else f"{semantic_case}:{result.classification.value}"
        )
        return f"{prefix}:stream_rows={result.stream_rows}:history_rows={result.history_rows}"
    if producer_name == "load_current":
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


def _operation_evidence_executor_unsealed(  # noqa: UP047
    function: Callable[_Parameters, _Result],
    *,
    sequence_for_gate: Callable[[str], tuple[str, ...] | None],
    allowlist_for_gate: Callable[[str], frozenset[str] | None],
) -> Callable[_Parameters, _Result]:
    """Record a low-level result; the outer authority wrapper seals its identity."""

    if _OPERATION_PRODUCERS_FROZEN:
        raise RuntimeError("TASK064 operation producers are frozen")
    capability = object()
    producer_name = function.__name__
    function_signature = inspect.signature(function)
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
        expected_sequence = sequence_for_gate(state.gate)
        allowed = allowlist_for_gate(state.gate)
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
        try:
            bound = function_signature.bind(*args, **kwargs)
        except TypeError:
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        bound.apply_defaults()
        input_binding = _operation_input_binding(
            producer_name,
            bound.arguments,
        )
        input_digest = None if input_binding is None else _evidence_payload_digest(input_binding)
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
                    gate=state.gate,
                    sequence=len(state.observations),
                ),
                input_binding=input_binding,
                input_digest=input_digest,
                result=result,
                payload_digest=_evidence_payload_digest(result),
                sequence=len(state.observations),
                token_nonces=tuple(token_nonces),
            )
        )
        return result

    return wrapped


def _build_operation_evidence_authority() -> tuple[
    Callable[
        [Callable[_Parameters, _Result]],
        Callable[_Parameters, _Result],
    ],
    Callable[[_OperationObservation, _EvidenceRun, str, int], bool],
    Callable[[], None],
    Callable[[str], tuple[str, ...] | None],
]:
    """Keep operation minting authority out of module-visible mutable state."""

    issued: dict[tuple[int, str, int], dict[str, object]] = {}
    registration_open = True
    canonical_sequences = {
        gate: tuple(sequence) for gate, sequence in _GATE_OPERATION_PRODUCER_SEQUENCES.items()
    }
    canonical_allowlists = {
        gate: frozenset(allowlist) for gate, allowlist in _GATE_OPERATION_ALLOWLIST.items()
    }
    if set(canonical_sequences) != set(canonical_allowlists) or any(
        not sequence or set(sequence) - canonical_allowlists[gate]
        for gate, sequence in canonical_sequences.items()
    ):
        raise RuntimeError("invalid TASK064 operation authority contract")

    def sequence_for_gate(gate: str) -> tuple[str, ...] | None:
        return canonical_sequences.get(gate)

    def allowlist_for_gate(gate: str) -> frozenset[str] | None:
        return canonical_allowlists.get(gate)

    def decorate(
        function: Callable[_Parameters, _Result],
    ) -> Callable[_Parameters, _Result]:
        if not registration_open:
            raise RuntimeError("TASK064 operation producer registration is closed")
        capability = object()
        unsealed = _operation_evidence_executor_unsealed(
            function,
            sequence_for_gate=sequence_for_gate,
            allowlist_for_gate=allowlist_for_gate,
        )
        function_signature = inspect.signature(function)

        @wraps(unsealed)
        def authoritative(
            *args: _Parameters.args,
            **kwargs: _Parameters.kwargs,
        ) -> _Result:
            state = _ACTIVE_GATE_OPERATIONS.get()
            depth = _ACTIVE_OPERATION_EXECUTOR_DEPTH.get()
            records = state is not None and depth == 0
            if not records:
                return unsealed(*args, **kwargs)
            if state is None:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _validated_evidence_run(state.run)
            expected_sequence = sequence_for_gate(state.gate)
            sequence = len(state.observations)
            if (
                _ACTIVE_EVIDENCE_RUN.get() is not state.run
                or expected_sequence is None
                or sequence >= len(expected_sequence)
                or expected_sequence[sequence] != function.__name__
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            try:
                bound = function_signature.bind(*args, **kwargs)
            except TypeError:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            bound.apply_defaults()
            registrations: list[_ActivePytestRoot] = []
            token_nonces: list[bytes] = []
            for value in bound.arguments.values():
                _collect_evidence_registrations(
                    value,
                    registrations,
                    token_nonces,
                )
            if any(observed is not state.run._pytest_registration for observed in registrations):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            key = (id(state.run), state.gate, sequence)
            if key in issued:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            issued[key] = {
                "phase": "RESERVED",
                "run": state.run,
                "registration": state.run._pytest_registration,
                "capability": capability,
                "producer_name": function.__name__,
            }
            try:
                result = unsealed(*args, **kwargs)
            except BaseException:
                issued[key]["phase"] = "POISONED"
                raise
            if len(state.observations) != sequence + 1:
                issued[key]["phase"] = "POISONED"
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            observation = state.observations[sequence]
            try:
                payload_digest = _evidence_payload_digest(observation.result)
                input_digest = (
                    None
                    if observation.input_binding is None
                    else _evidence_payload_digest(observation.input_binding)
                )
            except HarnessFailure:
                issued[key]["phase"] = "POISONED"
                raise
            issued[key] = {
                "phase": "ISSUED",
                "run": state.run,
                "registration": state.run._pytest_registration,
                "capability": capability,
                "producer_name": function.__name__,
                "observation": observation,
                "input_binding": observation.input_binding,
                "result": observation.result,
                "operation_tag": observation.operation_tag,
                "input_digest": input_digest,
                "payload_digest": payload_digest,
                "token_nonces": observation.token_nonces,
            }
            return result

        return authoritative

    def validate(
        observation: _OperationObservation,
        run: _EvidenceRun,
        gate: str,
        sequence: int,
    ) -> bool:
        entry = issued.get((id(run), gate, sequence))
        if (
            entry is None
            or entry["phase"] != "ISSUED"
            or entry["run"] is not run
            or entry["registration"] is not run._pytest_registration
            or entry["observation"] is not observation
            or entry["producer_name"] != observation.producer_name
            or entry["input_binding"] is not observation.input_binding
            or entry["result"] is not observation.result
            or entry["operation_tag"] != observation.operation_tag
            or entry["token_nonces"] != observation.token_nonces
            or observation.sequence != sequence
        ):
            return False
        try:
            payload_digest = _evidence_payload_digest(observation.result)
            input_digest = (
                None
                if observation.input_binding is None
                else _evidence_payload_digest(observation.input_binding)
            )
        except HarnessFailure:
            return False
        return bool(
            entry["payload_digest"] == payload_digest == observation.payload_digest
            and entry["input_digest"] == input_digest == observation.input_digest
        )

    def freeze() -> None:
        nonlocal registration_open
        if not registration_open:
            raise RuntimeError("TASK064 operation producer registration is already closed")
        registration_open = False

    return decorate, validate, freeze, sequence_for_gate


(
    _operation_evidence_executor,
    _validate_issued_operation_observation,
    _freeze_operation_evidence_authority,
    _canonical_operation_sequence,
) = _build_operation_evidence_authority()


def _build_private_gate_receipt_authority(
    validate_operation: Callable[
        [_OperationObservation, _EvidenceRun, str, int],
        bool,
    ],
    validate_rejection: Callable[
        [_RejectionObservation, _EvidenceRun, str, int],
        bool,
    ],
) -> Callable[[_EvidenceRun, str], str | None]:
    """Capture exact low-level validators behind one non-replaceable digest closure."""

    def digest(run: _EvidenceRun, gate: str) -> str | None:
        ledger = _validated_evidence_run(run)
        operations = ledger.operation_runs.get(gate)
        rejections = ledger.rejection_runs.get(gate)
        if operations is None and rejections is None:
            return None
        if operations is not None and any(
            not validate_operation(
                observation,
                run,
                gate,
                sequence,
            )
            for sequence, observation in enumerate(operations)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if rejections is not None and any(
            not validate_rejection(
                observation,
                run,
                gate,
                sequence,
            )
            for sequence, observation in enumerate(rejections)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
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
                        observation.input_digest,
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

    return digest


def _whole_gate_collector_unsealed(
    gate: str,
    *,
    private_receipt_digest: Callable[[_EvidenceRun, str], str | None],
    token_roles: Callable[
        [Mapping[str, object], object],
        tuple[tuple[str, StoreToken], ...],
    ],
    preflight: (
        Callable[
            [_EvidenceRun, Mapping[str, object]],
            None,
        ]
        | None
    ) = None,
    rollback_outputs: (
        Callable[
            [_EvidenceRun, Mapping[str, object], object],
            None,
        ]
        | None
    ) = None,
) -> Callable[
    [Callable[_Parameters, _Result]],
    Callable[_Parameters, _Result],
]:
    """Create one gate recorder; the outer authority wrapper seals its identity."""

    if (
        _GATE_COLLECTORS_FROZEN
        or gate not in GENERATED_EVIDENCE_GATES
        or not callable(token_roles)
        or (preflight is not None and not callable(preflight))
        or (rollback_outputs is not None and not callable(rollback_outputs))
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
        function_signature = inspect.signature(function)
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
            try:
                bound = function_signature.bind(*args, **kwargs)
            except TypeError:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            bound.apply_defaults()
            run_value = bound.arguments.get("run")
            if type(run_value) is not _EvidenceRun:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            run = run_value
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
            invocation = {name: value for name, value in bound.arguments.items() if name != "run"}
            registration = run._pytest_registration
            for name, value in invocation.items():
                if name.endswith("_token") and type(value) is not StoreToken:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if name == "pytest_root" and not isinstance(value, Path):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                _validate_collector_value_authority(value, registration)
            for value in invocation.values():
                _validate_collector_value_registration(value, registration)
            if preflight is not None:
                preflight(run, invocation)
            result = function(*args, **kwargs)
            registrations: list[_ActivePytestRoot] = []
            token_nonces: list[bytes] = []
            try:
                _validate_collector_value_registration(result, registration)
                for value in (*invocation.values(), result):
                    _collect_evidence_registrations(
                        value,
                        registrations,
                        token_nonces,
                    )
                named_tokens = token_roles(invocation, result)
                if (
                    any(observed is not registration for observed in registrations)
                    or registration.process_id != os.getpid()
                    or _lookup_active_pytest_root(registration.path_object) is not registration
                    or type(named_tokens) is not tuple
                    or not named_tokens
                    or len(named_tokens) != len({name for name, _ in named_tokens})
                    or any(
                        type(name) is not str
                        or not name
                        or type(token) is not StoreToken
                        or _require_token(token).pytest_registration is not registration
                        for name, token in named_tokens
                    )
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                observation = _EvidenceObservation(
                    value=result,
                    payload_digest=_evidence_payload_digest(result),
                    registration=registration,
                    producer=registered,
                    run=run,
                    process_id=os.getpid(),
                    token_roles=tuple((name, token._nonce) for name, token in named_tokens),
                    private_receipt_digest=private_receipt_digest(
                        run,
                        registered.gate,
                    ),
                )
            except BaseException:
                if rollback_outputs is not None:
                    try:
                        rollback_outputs(run, invocation, result)
                    except BaseException:
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
                raise
            ledger.observations[registered.ordinal] = observation
            return result

        return wrapped

    return decorate


def _build_whole_gate_authority(
    private_receipt_digest: Callable[[_EvidenceRun, str], str | None],
) -> tuple[
    Callable[
        ...,
        Callable[
            [Callable[_Parameters, _Result]],
            Callable[_Parameters, _Result],
        ],
    ],
    Callable[[_EvidenceObservation, _EvidenceRun, int], bool],
    Callable[[], None],
]:
    """Keep whole-gate minting authority inside decorator closures."""

    issued: dict[tuple[int, int], dict[str, object]] = {}
    registration_open = True

    def collector(
        gate: str,
        *,
        token_roles: Callable[
            [Mapping[str, object], object],
            tuple[tuple[str, StoreToken], ...],
        ],
        preflight: (
            Callable[
                [_EvidenceRun, Mapping[str, object]],
                None,
            ]
            | None
        ) = None,
        rollback_outputs: (
            Callable[
                [_EvidenceRun, Mapping[str, object], object],
                None,
            ]
            | None
        ) = None,
    ) -> Callable[
        [Callable[_Parameters, _Result]],
        Callable[_Parameters, _Result],
    ]:
        if not registration_open:
            raise RuntimeError("TASK064 whole-gate collector registration is closed")
        unsealed_decorator = _whole_gate_collector_unsealed(
            gate,
            private_receipt_digest=private_receipt_digest,
            token_roles=token_roles,
            preflight=preflight,
            rollback_outputs=rollback_outputs,
        )

        def decorate(
            function: Callable[_Parameters, _Result],
        ) -> Callable[_Parameters, _Result]:
            capability = object()
            function_signature = inspect.signature(function)
            unsealed = unsealed_decorator(function)
            ordinal = GENERATED_EVIDENCE_GATES.index(gate)

            @wraps(unsealed)
            def authoritative(
                *args: _Parameters.args,
                **kwargs: _Parameters.kwargs,
            ) -> _Result:
                try:
                    bound = function_signature.bind(*args, **kwargs)
                except TypeError:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
                bound.apply_defaults()
                run = bound.arguments.get("run")
                if type(run) is not _EvidenceRun:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                ledger = _validated_evidence_run(run)
                if (
                    _ACTIVE_EVIDENCE_RUN.get() is not run
                    or not ledger.recording
                    or ledger.closed
                    or ledger.consumed
                    or ledger.receipt is not None
                    or ordinal in ledger.observations
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                invocation = {
                    name: value for name, value in bound.arguments.items() if name != "run"
                }
                registration = run._pytest_registration
                for name, value in invocation.items():
                    if name.endswith("_token") and type(value) is not StoreToken:
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    if name == "pytest_root" and not isinstance(value, Path):
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    _validate_collector_value_authority(value, registration)
                for value in invocation.values():
                    _validate_collector_value_registration(value, registration)
                if preflight is not None:
                    preflight(run, invocation)
                key = (id(run), ordinal)
                if key in issued:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                issued[key] = {
                    "phase": "RESERVED",
                    "run": run,
                    "registration": registration,
                    "capability": capability,
                    "gate": gate,
                    "ordinal": ordinal,
                    "producer_name": function.__name__,
                }
                try:
                    result = unsealed(*args, **kwargs)
                except BaseException:
                    if gate in {
                        "schema_identity",
                        "bootstrap_path_ownership",
                        "runtime_connection_controls",
                        "projection_roundtrip",
                        "schema_constraints_corruption",
                        "atomicity_classification",
                        "fresh_process_faults",
                        "bounded_queries",
                        "closed_error_mapping",
                        "workload_thresholds",
                    }:
                        del issued[key]
                    else:
                        issued[key]["phase"] = "POISONED"
                    raise
                ledger = _validated_evidence_run(run)
                observation = ledger.observations.get(ordinal)
                if observation is None or observation.value is not result:
                    issued[key]["phase"] = "POISONED"
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                try:
                    payload_digest = _evidence_payload_digest(result)
                    current_private_receipt_digest = private_receipt_digest(run, gate)
                except HarnessFailure:
                    issued[key]["phase"] = "POISONED"
                    raise
                issued[key] = {
                    **issued[key],
                    "phase": "ISSUED",
                    "observation": observation,
                    "result": result,
                    "payload_digest": payload_digest,
                    "process_id": observation.process_id,
                    "token_roles": observation.token_roles,
                    "private_receipt_digest": current_private_receipt_digest,
                    "producer": observation.producer,
                }
                return result

            return authoritative

        return decorate

    def validate(
        observation: _EvidenceObservation,
        run: _EvidenceRun,
        ordinal: int,
    ) -> bool:
        entry = issued.get((id(run), ordinal))
        if (
            entry is None
            or entry["phase"] != "ISSUED"
            or entry["run"] is not run
            or entry["registration"] is not run._pytest_registration
            or entry["observation"] is not observation
            or entry["result"] is not observation.value
            or entry["producer"] is not observation.producer
            or entry["gate"] != observation.producer.gate
            or entry["ordinal"] != ordinal
            or observation.producer.ordinal != ordinal
            or entry["producer_name"] != observation.producer.name
            or entry["process_id"] != observation.process_id
            or observation.process_id != os.getpid()
            or entry["token_roles"] != observation.token_roles
            or entry["private_receipt_digest"] != observation.private_receipt_digest
            or observation.registration is not run._pytest_registration
            or observation.run is not run
        ):
            return False
        try:
            payload_digest = _evidence_payload_digest(observation.value)
            current_private_receipt_digest = private_receipt_digest(
                run,
                observation.producer.gate,
            )
        except HarnessFailure:
            return False
        return bool(
            entry["payload_digest"] == payload_digest == observation.payload_digest
            and current_private_receipt_digest
            == entry["private_receipt_digest"]
            == observation.private_receipt_digest
        )

    def freeze() -> None:
        nonlocal registration_open
        if not registration_open:
            raise RuntimeError("TASK064 whole-gate collector registration is already closed")
        registration_open = False

    return collector, validate, freeze


def _close_descriptors(descriptors: Sequence[int]) -> bool:
    """Close each exact owned descriptor once and report any cleanup uncertainty."""

    cleanup_ok = True
    for descriptor in dict.fromkeys(descriptors):
        try:
            os.close(descriptor)
        except OSError:
            cleanup_ok = False
    return cleanup_ok


def _close_descriptors_checked(
    descriptors: Sequence[int],
    *,
    code: HarnessFailureCode = HarnessFailureCode.UNAVAILABLE,
) -> None:
    """Close exact parent-owned descriptors or fail closed."""

    if not _close_descriptors(descriptors):
        _mark_process_cleanup_uncertain()
        raise HarnessFailure(code)


def _open_pipes(count: int) -> tuple[tuple[int, int], ...]:
    pipes: list[tuple[int, int]] = []
    try:
        for _ in range(count):
            pipes.append(os.pipe())
    except OSError:
        if not _close_descriptors(tuple(descriptor for pipe in pipes for descriptor in pipe)):
            _mark_process_cleanup_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        raise
    return tuple(pipes)


def _wait_for_owned_process_event(
    process_id: int,
    *,
    timeout_seconds: float = 10.0,
    include_stopped: bool = False,
) -> int | None:
    """Boundedly observe one exact child exit or requested traced-stop event."""

    if type(process_id) is not int or process_id <= 0:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    deadline = time.monotonic() + timeout_seconds
    options = os.WNOHANG | (os.WUNTRACED if include_stopped else 0)
    while True:
        try:
            waited_process_id, status = os.waitpid(process_id, options)
        except InterruptedError:
            if time.monotonic() >= deadline:
                return None
            continue
        except (ChildProcessError, OSError):
            return None
        if waited_process_id == process_id:
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                return status
            if include_stopped and os.WIFSTOPPED(status):
                return status
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.01)


def _wait_for_owned_process(process_id: int, *, timeout_seconds: float = 10.0) -> int | None:
    """Boundedly reap one exact child without ever signaling a numeric PID."""

    return _wait_for_owned_process_event(
        process_id,
        timeout_seconds=timeout_seconds,
        include_stopped=False,
    )


def _terminate_and_reap_processes(process_ids: Sequence[int]) -> bool:
    """Kill and boundedly reap only PIDs still proven to be our unreaped children."""

    exact_process_ids = tuple(dict.fromkeys(process_ids))
    if any(type(process_id) is not int or process_id <= 0 for process_id in exact_process_ids):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    proven_live: set[int] = set()
    cleanup_ok = True
    ownership_deadline = time.monotonic() + 10.0
    for process_id in exact_process_ids:
        while True:
            try:
                waited_process_id, status = os.waitpid(process_id, os.WNOHANG)
            except InterruptedError:
                if time.monotonic() >= ownership_deadline:
                    cleanup_ok = False
                    break
                continue
            except ChildProcessError:
                break
            except OSError:
                cleanup_ok = False
                break
            if waited_process_id == 0:
                proven_live.add(process_id)
            elif waited_process_id == process_id:
                if not (os.WIFEXITED(status) or os.WIFSIGNALED(status)):
                    proven_live.add(process_id)
            else:
                cleanup_ok = False
            break
    remaining = set(proven_live)
    for process_id in tuple(proven_live):
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
                    waited_process_id, status = os.waitpid(process_id, os.WNOHANG)
                except InterruptedError:
                    if time.monotonic() >= deadline:
                        cleanup_ok = False
                        break
                    continue
                except ChildProcessError:
                    remaining.discard(process_id)
                except OSError:
                    cleanup_ok = False
                else:
                    if waited_process_id == process_id and (
                        os.WIFEXITED(status) or os.WIFSIGNALED(status)
                    ):
                        remaining.discard(process_id)
                break
        if remaining:
            time.sleep(0.01)
    return cleanup_ok and not remaining


def _build_process_cleanup_uncertainty() -> tuple[
    Callable[[], None],
    Callable[[], bool],
    Callable[[Callable[[], None]], None],
]:
    """Retain a redundant, fork-shared cleanup-uncertainty latch."""

    mmap_constructor = mmap.mmap
    mmap_flush = mmap.mmap.flush
    mmap_shared = mmap.MAP_SHARED
    mmap_protection = mmap.PROT_READ | mmap.PROT_WRITE
    pipe_constructor = os.pipe
    pipe_write = os.write
    pipe_close = os.close
    set_blocking = os.set_blocking
    set_inheritable = os.set_inheritable
    select_wait = select.select
    suppress_errors = suppress
    uncertain = False
    root_poison_marker: Callable[[], None] | None = None
    shared_poison = mmap_constructor(
        -1,
        1,
        flags=mmap_shared,
        prot=mmap_protection,
    )
    shared_poison[0] = 0
    mmap_flush(shared_poison)
    poison_read_descriptor, poison_write_descriptor = pipe_constructor()
    try:
        set_blocking(poison_read_descriptor, False)
        set_blocking(poison_write_descriptor, False)
        set_inheritable(poison_read_descriptor, False)
        set_inheritable(poison_write_descriptor, False)
    except BaseException:
        with suppress(OSError):
            pipe_close(poison_read_descriptor)
        with suppress(OSError):
            pipe_close(poison_write_descriptor)
        raise

    def mark() -> None:
        nonlocal uncertain
        uncertain = True
        try:
            shared_poison[0] = 1
            mmap_flush(shared_poison)
        except (IndexError, OSError, ValueError):
            pass
        with suppress_errors(BlockingIOError, OSError):
            pipe_write(poison_write_descriptor, b"\x01")
        marker = root_poison_marker
        if marker is not None:
            marker()

    def observed() -> bool:
        nonlocal uncertain
        if uncertain:
            return True
        try:
            mmap_poisoned = shared_poison[0] != 0
        except (IndexError, OSError, ValueError):
            mmap_poisoned = True
        try:
            readable, _, _ = select_wait(
                (poison_read_descriptor,),
                (),
                (),
                0.0,
            )
            pipe_poisoned = poison_read_descriptor in readable
        except (OSError, ValueError):
            pipe_poisoned = True
        if mmap_poisoned or pipe_poisoned:
            mark()
            return True
        return False

    def bind_root_poison(marker: Callable[[], None]) -> None:
        nonlocal root_poison_marker
        if root_poison_marker is not None or not callable(marker):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        root_poison_marker = marker

    return mark, observed, bind_root_poison


(
    _mark_process_cleanup_uncertain,
    _has_process_cleanup_uncertainty,
    _bind_process_cleanup_root_poison,
) = _build_process_cleanup_uncertainty()
del _build_process_cleanup_uncertainty


def _build_process_resource_finalizers(
    mark_cleanup_uncertain: Callable[[], None],
) -> tuple[
    Callable[..., None],
    Callable[..., int | None],
]:
    """Capture the irreversible cleanup marker behind both process finalizers."""

    def finalize(
        descriptors: Sequence[int],
        process_ids: Sequence[int],
        *,
        code: HarnessFailureCode = HarnessFailureCode.UNAVAILABLE,
    ) -> None:
        """Attempt descriptor and exact-child cleanup before reporting uncertainty."""

        try:
            descriptors_ok = _close_descriptors(descriptors)
        except BaseException:
            descriptors_ok = False
        try:
            processes_ok = _terminate_and_reap_processes(process_ids)
        except BaseException:
            processes_ok = False
        if not descriptors_ok or not processes_ok:
            mark_cleanup_uncertain()
            raise HarnessFailure(code)

    def wait_or_terminate(
        process_id: int,
        *,
        timeout_seconds: float = 10.0,
    ) -> int | None:
        """Boundedly wait for one child and guarantee a checked termination attempt."""

        status = _wait_for_owned_process(
            process_id,
            timeout_seconds=timeout_seconds,
        )
        if status is not None:
            return status
        if not _terminate_and_reap_processes((process_id,)):
            mark_cleanup_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return None

    return finalize, wait_or_terminate


(
    _finalize_process_resources,
    _wait_or_terminate_owned_process,
) = _build_process_resource_finalizers(_mark_process_cleanup_uncertain)
del _build_process_resource_finalizers


def _read_process_packet(stage: str, descriptor: int, size: int) -> bytes:
    """Read one exact finite child packet with a shared deadline."""

    if type(stage) is not str or not stage or type(descriptor) is not int or descriptor < 0:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _require_exact_int(size, minimum=1, maximum=1_048_576)
    timeout_seconds = (
        60.0 if stage in {"concurrent_backup_ready", "concurrent_backup_result"} else 10.0
    )
    try:
        os.set_blocking(descriptor, False)
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + timeout_seconds
    payload = bytearray()
    try:
        try:
            selector.register(descriptor, selectors.EVENT_READ)
        except (OSError, ValueError):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        while len(payload) < size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                ready = selector.select(timeout=remaining)
            except InterruptedError:
                continue
            except OSError:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            if not ready:
                continue
            try:
                chunk = os.read(descriptor, size - len(payload))
            except (InterruptedError, BlockingIOError):
                continue
            if not chunk:
                break
            payload.extend(chunk)
        return bytes(payload)
    finally:
        selector.close()


def _write_process_packet(stage: str, descriptor: int, payload: bytes) -> None:
    """Write one finite child packet with one deadline and complete-write semantics."""

    if (
        type(stage) is not str
        or not stage
        or type(descriptor) is not int
        or descriptor < 0
        or type(payload) is not bytes
        or not 1 <= len(payload) <= 1_048_576
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    timeout_seconds = (
        60.0 if stage in {"concurrent_backup_ready", "concurrent_backup_result"} else 10.0
    )
    try:
        os.set_blocking(descriptor, False)
    except OSError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    selector = selectors.DefaultSelector()
    deadline = time.monotonic() + timeout_seconds
    view = memoryview(payload)
    try:
        try:
            selector.register(descriptor, selectors.EVENT_WRITE)
        except (OSError, ValueError):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        while view:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            try:
                ready = selector.select(timeout=remaining)
            except InterruptedError:
                continue
            except OSError:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            if not ready:
                continue
            try:
                written = os.write(descriptor, view)
            except (InterruptedError, BlockingIOError):
                continue
            except OSError:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            if written <= 0 or written > len(view):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            view = view[written:]
    finally:
        selector.close()


def _build_fork_safe_connection_requirement(
    root_uncertain: Callable[[], bool],
    connection_unsafe: Callable[[], bool],
    cleanup_uncertain: Callable[[], bool],
) -> Callable[[], None]:
    """Capture the three irreversible fork-safety observers exactly once."""

    def require() -> None:
        if root_uncertain() or connection_unsafe() or cleanup_uncertain():
            raise HarnessFailure(HarnessFailureCode.UNPROVEN)

    return require


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

    return _schema_fingerprint_from_canonical_bytes(canonical_descriptor_bytes(descriptor))


def _schema_fingerprint_from_canonical_bytes(canonical_bytes: bytes) -> str:
    """Hash one already-canonical descriptor byte string exactly once."""

    if type(canonical_bytes) is not bytes:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    digest = hashlib.sha256(SCHEMA_DESCRIPTOR_DOMAIN + canonical_bytes).hexdigest()
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


def _load_canonical_schema_descriptor_fixture() -> tuple[dict[str, object], bytes]:
    try:
        raw = SCHEMA_DESCRIPTOR_PATH.read_bytes()
    except OSError:
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if not raw.endswith(b"\n") or raw.endswith(b"\n\n"):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    document = raw[:-1]
    descriptor = _parse_schema_descriptor_document(document)
    canonical_bytes = canonical_descriptor_bytes(descriptor)
    if canonical_bytes != document:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return descriptor, canonical_bytes


def _parse_schema_descriptor_document(document: bytes) -> dict[str, object]:
    try:
        value = json.loads(document)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    if type(value) is not dict:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return cast(dict[str, object], value)


def _load_schema_fingerprint_fixture() -> tuple[str, bytes]:
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
    return value, _digest_bytes(value)


def _load_schema_fixture_snapshot(
    *,
    expected_descriptor: Mapping[str, object] | None = None,
    fingerprint_first: bool = False,
) -> _SchemaFixtureSnapshot:
    if fingerprint_first:
        fingerprint, fingerprint_bytes = _load_schema_fingerprint_fixture()
    descriptor, canonical_bytes = _load_canonical_schema_descriptor_fixture()
    if expected_descriptor is not None and descriptor != expected_descriptor:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if not fingerprint_first:
        fingerprint, fingerprint_bytes = _load_schema_fingerprint_fixture()
    computed_fingerprint = _schema_fingerprint_from_canonical_bytes(canonical_bytes)
    if fingerprint != computed_fingerprint:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return _SchemaFixtureSnapshot(descriptor, fingerprint, fingerprint_bytes)


def load_schema_descriptor() -> dict[str, object]:
    """Load only one fresh exact canonical descriptor fixture."""

    descriptor, _ = _load_canonical_schema_descriptor_fixture()
    return descriptor


def load_schema_fingerprint() -> str:
    """Load one fresh, self-consistent view of both golden schema fixtures."""

    return _load_schema_fixture_snapshot(fingerprint_first=True).fingerprint


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


def _build_pytest_root_authority() -> tuple[
    Callable[[], None],
    Callable[..., _PytestRootRegistrationPermit],
    Callable[[_PytestRootRegistrationPermit], None],
    Callable[
        [_PytestRootRegistrationPermit, tuple[Path, ...]],
        AbstractContextManager[_PytestRootCapability],
    ],
    Callable[[Path], _ActivePytestRoot | None],
    Callable[[_ActivePytestRoot], bool],
    Callable[[Path], _ActivePytestRoot | None],
    Callable[[_PytestRootCapability, Path], None],
    Callable[[_ActivePytestRoot, bool], bool],
    Callable[[str], None],
    Callable[[str], bool],
    Callable[[], tuple[bool, bool]],
    Callable[[], bool],
    Callable[
        [
            Callable[[_EvidenceRun], bool],
            Callable[[_EvidenceRun], bool],
        ],
        None,
    ],
    Callable[[Callable[[_ActivePytestRoot], bool]], None],
    Callable[[], None],
]:
    """Pre-issue exact fixture roots from two sealed stdlib-only fixture call sites."""

    fixture_policy = (
        (
            "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
            "tests.unit.test_task_064_continuous_public_trade_stream_sqlite_schema",
            "_active_task064_pytest_root",
            "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
            "7136d6ff216b9b1d3bdf32030918302d8fa5a87dfd8555df68c81939bde2dd34",
            410,
            494,
            "_bind_task064_harness_module",
            "41d05f3670974dedbcdb1b12e85b39bb2f1b306eb13686ba016caa07c2eeeb06",
            34,
            "6f317f48e0fa6fbae521ae37479a58c82049228b6db2e8398077843e521328fc",
        ),
        (
            "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
            "tests.integration.test_task_064_continuous_public_trade_stream_sqlite_evidence",
            "_active_task064_pytest_root",
            "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
            "0a6b6e9109472cd92cc231ce31440744d8963390cba2e4688aea373ec21ac28d",
            430,
            514,
            "_bind_task064_harness_module",
            "e4bba9b8f370dc1c66fcf977d36438dc870ade6792cd281e1f4ff5ed0fbfc330",
            34,
            "af3a4f167e99474f886b30c1e2d7d83d624339c149333d231f58355d9c89536a",
        ),
    )
    real_getpid = os.getpid
    real_getppid = os.getppid
    real_getuid = os.getuid
    frame_getter = sys._getframe
    code_type = CodeType
    suppress_errors = suppress
    path_type = Path
    path_lstat = Path.lstat
    path_read_bytes = Path.read_bytes
    path_resolve = Path.resolve
    path_is_file = Path.is_file
    stat_is_link = stat.S_ISLNK
    stat_is_directory = stat.S_ISDIR
    stat_is_regular = stat.S_ISREG
    stat_mode = stat.S_IMODE
    digest_constructor = hashlib.sha256
    compile_code = compile
    type_of = type
    object_identity = id
    iter_values = iter
    length_of = len
    bytes_type = bytes
    bool_type = bool
    complex_type = complex
    dict_type = dict
    float_type = float
    frozenset_type = frozenset
    int_type = int
    list_type = list
    set_type = set
    str_type = str
    tuple_type = tuple
    bytes_join = bytes.join
    int_bit_length = int.bit_length
    int_to_bytes = int.to_bytes
    str_encode = str.encode
    binary_float_pack = struct.pack
    binary_float_pack_error = struct.error
    sort_values = sorted
    cast_value = cast
    recursion_error = RecursionError
    type_error = TypeError
    value_error = ValueError
    overflow_error = OverflowError
    implementation_name = sys.implementation.name
    implementation_cache_tag = sys.implementation.cache_tag
    accepted_python_version = ACCEPTED_PYTHON_VERSION
    runtime_python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    optimization_level = sys.flags.optimize
    mmap_constructor = mmap.mmap
    mmap_flush = mmap.mmap.flush
    mmap_close = mmap.mmap.close
    mmap_shared = mmap.MAP_SHARED
    mmap_protection = mmap.PROT_READ | mmap.PROT_WRITE
    pipe_constructor = os.pipe
    set_blocking = os.set_blocking
    set_inheritable = os.set_inheritable
    pipe_write = os.write
    pipe_close = os.close
    select_wait = select.select
    nonce_issuer = _issue_authority_nonce
    evidence_ledger_type = _EvidenceLedger
    publication_root_state_type = _ReportPublicationRootState
    active_evidence_run = _ACTIVE_EVIDENCE_RUN
    process_cleanup_uncertain = _has_process_cleanup_uncertainty
    active: dict[int, dict[str, object]] = {}
    revoked: dict[int, dict[str, object]] = {}
    all_roots: dict[int, dict[str, object]] = {}
    inode_records: dict[tuple[int, int], dict[str, object]] = {}
    sessions: dict[int, dict[str, object]] = {}
    capabilities: dict[int, dict[str, object]] = {}
    permits: dict[int, dict[str, object]] = {}
    revocation_uncertain = False
    fingerprint_cache: dict[tuple[int, str], tuple[CodeType, str]] = {}
    fingerprint_requests = 0
    fingerprint_computations = 0
    maximum_cached_fingerprints = 256
    root_fault_policy = frozenset(
        {
            "revocation_flag_write",
            "revocation_flag_flush",
            "revocation_after_flush",
            "revocation_close",
            "evidence_run_close",
            "permit_scope_construction",
            "permit_scope_entry",
            "permit_partial_activation",
            "permit_cancel",
        }
    )
    poison_delivery_fault_policy = frozenset(
        {
            "poison_mmap_write",
            "poison_mmap_flush",
            "poison_pipe_write",
        }
    )
    poison_probe_fault_policy = frozenset(
        {
            "poison_mmap_probe",
            "poison_pipe_probe",
        }
    )
    paired_fixture_cancel_faults = frozenset({"permit_scope_construction", "permit_cancel"})
    armed_faults: frozenset[str] = frozenset()
    fault_hits: frozenset[str] = frozenset()
    run_closer: Callable[[_EvidenceRun], bool] | None = None
    run_closed: Callable[[_EvidenceRun], bool] | None = None
    report_cache_teardown: Callable[[_ActivePytestRoot], bool] | None = None
    unit_module_globals_identity: int | None = None
    integration_module_globals_identity: int | None = None
    active_node_lifecycle: tuple[int, str] | None = None
    returned_node_tombstones: tuple[tuple[int, str], ...] = ()

    def code_fingerprint(code: CodeType, suffix: str) -> str:
        nonlocal fingerprint_requests
        nonlocal fingerprint_computations
        if (
            type_of(code) is not code_type
            or type_of(suffix) is not str_type
            or type_of(implementation_name) is not str_type
            or type_of(implementation_cache_tag) is not str_type
            or type_of(accepted_python_version) is not str_type
            or type_of(runtime_python_version) is not str_type
            or runtime_python_version != accepted_python_version
            or type_of(optimization_level) is not int_type
            or optimization_level not in {0, 1, 2}
            or fingerprint_requests >= 1_000_000
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        fingerprint_requests += 1
        cache_key = (object_identity(code), suffix)
        cached_fingerprint = fingerprint_cache.get(cache_key)
        if cached_fingerprint is not None:
            if cached_fingerprint[0] is not code:
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            return cached_fingerprint[1]
        fingerprint_computations += 1
        serializer_domain = b"TASK064-CODE-FINGERPRINT-TLV-V3"
        alias_domain = b"CONST-GRAPH-ALIAS-PARTITION-V3"
        maximum_depth = 64
        maximum_nodes = 65_536
        maximum_payload_bytes = 16 * 1024 * 1024
        active_ancestors = set_type()
        constant_snapshots: dict[int, tuple[CodeType, tuple[object, ...]]] = dict_type()
        frozenset_orders: dict[
            int,
            tuple[frozenset[object], tuple[object, ...]],
        ] = dict_type()
        nodes = 0
        materialized_bytes = 0

        def invalid_fingerprint() -> Never:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)

        def consume_node(depth: int) -> None:
            nonlocal nodes
            if type_of(depth) is not int_type or depth > maximum_depth:
                invalid_fingerprint()
            nodes += 1
            if nodes > maximum_nodes:
                invalid_fingerprint()

        def charge_bytes(size: int) -> None:
            nonlocal materialized_bytes
            if (
                type_of(size) is not int_type
                or size < 0
                or size > maximum_payload_bytes - materialized_bytes
            ):
                invalid_fingerprint()
            materialized_bytes += size

        def bounded_join(parts: Iterator[bytes]) -> bytes:
            fragments = list_type()
            payload_length = 0
            for fragment in parts:
                if type_of(fragment) is not bytes_type:
                    invalid_fingerprint()
                payload_length += length_of(fragment)
                if payload_length > maximum_payload_bytes:
                    invalid_fingerprint()
                fragments.append(fragment)
            charge_bytes(payload_length)
            return bytes_join(b"", fragments)

        def frame(tag: bytes, payload: bytes) -> bytes:
            if (
                type_of(tag) is not bytes_type
                or length_of(tag) != 2
                or type_of(payload) is not bytes_type
                or length_of(payload) > maximum_payload_bytes - 10
            ):
                invalid_fingerprint()
            charge_bytes(8)
            encoded_length = int_to_bytes(length_of(payload), 8, "big")
            charge_bytes(10 + length_of(payload))
            return bytes_join(b"", (tag, encoded_length, payload))

        def encode_text(value: str) -> bytes:
            if length_of(value) > maximum_payload_bytes // 4:
                invalid_fingerprint()
            encoded = str_encode(value, "utf-8", "surrogatepass")
            charge_bytes(length_of(encoded))
            return encoded

        def encode_integer(value: int) -> bytes:
            negative = value < 0
            magnitude = -value if negative else value
            width = (int_bit_length(magnitude) + 7) // 8 or 1
            if width > maximum_payload_bytes:
                invalid_fingerprint()
            charge_bytes(width)
            encoded_magnitude = int_to_bytes(magnitude, width, "big")
            charge_bytes(width + 1)
            return bytes_join(
                b"",
                (b"-" if negative else b"+", encoded_magnitude),
            )

        def encode_uint64(value: int) -> bytes:
            if type_of(value) is not int_type or value < 0 or value >= 1 << 64:
                invalid_fingerprint()
            charge_bytes(8)
            return int_to_bytes(value, 8, "big")

        def snapshot_constants(candidate: CodeType) -> tuple[object, ...]:
            identity = object_identity(candidate)
            existing = constant_snapshots.get(identity)
            if existing is not None:
                if existing[0] is not candidate:
                    invalid_fingerprint()
                return existing[1]
            constants = candidate.co_consts
            if type_of(constants) is not tuple_type:
                invalid_fingerprint()
            constant_snapshots[identity] = (candidate, constants)
            return constants

        def serialize_sequence(
            tag: bytes,
            values: tuple[object, ...] | frozenset[object],
            depth: int,
            *,
            ordered: bool,
        ) -> bytes:
            identity = object_identity(values)
            if identity in active_ancestors:
                invalid_fingerprint()
            active_ancestors.add(identity)
            try:
                if ordered:
                    serialized = list_type(serialize_constant(value, depth + 1) for value in values)
                else:
                    frozen_values = cast_value(frozenset[object], values)
                    cached_order = frozenset_orders.get(identity)
                    if cached_order is not None:
                        if cached_order[0] is not frozen_values:
                            invalid_fingerprint()
                        ordered_values = cached_order[1]
                        serialized = list_type(
                            serialize_constant(value, depth + 1) for value in ordered_values
                        )
                    else:
                        serialized_pairs = list_type(
                            (
                                serialize_constant(value, depth + 1),
                                value,
                            )
                            for value in frozen_values
                        )
                        serialized_pairs = list_type(
                            sort_values(
                                serialized_pairs,
                                key=lambda pair: pair[0],
                            )
                        )
                        previous: bytes | None = None
                        for serialized_value, _ in serialized_pairs:
                            if previous is not None and serialized_value == previous:
                                invalid_fingerprint()
                            previous = serialized_value
                        ordered_values = tuple_type(value for _, value in serialized_pairs)
                        frozenset_orders[identity] = (
                            frozen_values,
                            ordered_values,
                        )
                        serialized = list_type(value for value, _ in serialized_pairs)
                charge_bytes(8)
                encoded_length = int_to_bytes(length_of(serialized), 8, "big")
                body = bounded_join(
                    iter_values(
                        (
                            encoded_length,
                            *serialized,
                        )
                    )
                )
                return frame(tag, body)
            finally:
                active_ancestors.remove(identity)

        def serialize_code(candidate: CodeType, depth: int) -> bytes:
            consume_node(depth)
            identity = object_identity(candidate)
            if identity in active_ancestors:
                invalid_fingerprint()
            active_ancestors.add(identity)
            try:
                constants = snapshot_constants(candidate)
                fields = (
                    frame(b"sv", serializer_domain),
                    frame(b"im", encode_text(implementation_name)),
                    frame(b"ct", encode_text(implementation_cache_tag)),
                    frame(b"pv", encode_text(accepted_python_version)),
                    frame(
                        b"op",
                        serialize_constant(optimization_level, depth + 1),
                    ),
                    frame(b"fn", encode_text(suffix)),
                    frame(b"ln", encode_integer(1)),
                    frame(b"ac", serialize_constant(candidate.co_argcount, depth + 1)),
                    frame(
                        b"pa",
                        serialize_constant(candidate.co_posonlyargcount, depth + 1),
                    ),
                    frame(
                        b"ka",
                        serialize_constant(candidate.co_kwonlyargcount, depth + 1),
                    ),
                    frame(b"nl", serialize_constant(candidate.co_nlocals, depth + 1)),
                    frame(b"ss", serialize_constant(candidate.co_stacksize, depth + 1)),
                    frame(b"fg", serialize_constant(candidate.co_flags, depth + 1)),
                    frame(b"bc", serialize_constant(candidate.co_code, depth + 1)),
                    frame(b"cs", serialize_constant(constants, depth + 1)),
                    frame(b"ns", serialize_constant(candidate.co_names, depth + 1)),
                    frame(b"vn", serialize_constant(candidate.co_varnames, depth + 1)),
                    frame(b"nm", serialize_constant(candidate.co_name, depth + 1)),
                    frame(b"qn", serialize_constant(candidate.co_qualname, depth + 1)),
                    frame(b"lt", serialize_constant(candidate.co_linetable, depth + 1)),
                    frame(
                        b"et",
                        serialize_constant(candidate.co_exceptiontable, depth + 1),
                    ),
                    frame(b"fv", serialize_constant(candidate.co_freevars, depth + 1)),
                    frame(b"cv", serialize_constant(candidate.co_cellvars, depth + 1)),
                )
                return frame(b"co", bounded_join(iter_values(fields)))
            finally:
                active_ancestors.remove(identity)

        def serialize_constant(value: object, depth: int) -> bytes:
            value_type = type_of(value)
            if value_type is code_type:
                return serialize_code(cast_value(CodeType, value), depth)
            consume_node(depth)
            if value is None:
                return frame(b"no", b"")
            if value is Ellipsis:
                return frame(b"el", b"")
            if value_type is bool_type:
                return frame(b"bo", b"\x01" if value else b"\x00")
            if value_type is int_type:
                return frame(b"in", encode_integer(cast_value(int, value)))
            if value_type is float_type:
                charge_bytes(8)
                return frame(b"fl", binary_float_pack(">d", cast_value(float, value)))
            if value_type is complex_type:
                complex_value = cast_value(complex, value)
                charge_bytes(16)
                return frame(
                    b"cx",
                    binary_float_pack(">dd", complex_value.real, complex_value.imag),
                )
            if value_type is str_type:
                return frame(b"st", encode_text(cast_value(str, value)))
            if value_type is bytes_type:
                return frame(b"by", cast_value(bytes, value))
            if value_type is tuple_type:
                return serialize_sequence(
                    b"tu",
                    cast_value(tuple[object, ...], value),
                    depth,
                    ordered=True,
                )
            if value_type is frozenset_type:
                return serialize_sequence(
                    b"fs",
                    cast_value(frozenset[object], value),
                    depth,
                    ordered=False,
                )
            invalid_fingerprint()

        def serialize_alias_partition(root: CodeType) -> bytes:
            alias_active_ancestors = set_type()
            expanded_alias_containers = set_type()
            alias_objects: dict[int, object] = dict_type()
            alias_occurrences: dict[int, list[int]] = dict_type()
            occurrence_count = 0
            unique_count = 0

            def visit(value: object, depth: int) -> None:
                nonlocal occurrence_count
                nonlocal unique_count
                if type_of(depth) is not int_type or depth > maximum_depth:
                    invalid_fingerprint()
                value_type = type_of(value)
                if value is None or value is Ellipsis or value_type is bool_type:
                    return
                if value_type not in {
                    code_type,
                    tuple_type,
                    frozenset_type,
                    int_type,
                    float_type,
                    complex_type,
                    str_type,
                    bytes_type,
                }:
                    invalid_fingerprint()
                occurrence_count += 1
                if occurrence_count > maximum_nodes:
                    invalid_fingerprint()
                ordinal = occurrence_count - 1
                identity = object_identity(value)
                retained = alias_objects.get(identity)
                if retained is None:
                    alias_objects[identity] = value
                    alias_occurrences[identity] = list_type((ordinal,))
                    unique_count += 1
                    if unique_count > maximum_nodes:
                        invalid_fingerprint()
                else:
                    if retained is not value:
                        invalid_fingerprint()
                    alias_occurrences[identity].append(ordinal)

                if value_type not in {code_type, tuple_type, frozenset_type}:
                    return
                if identity in alias_active_ancestors:
                    invalid_fingerprint()
                if identity in expanded_alias_containers:
                    return
                expanded_alias_containers.add(identity)
                alias_active_ancestors.add(identity)
                try:
                    if value_type is code_type:
                        visit(
                            snapshot_constants(cast_value(CodeType, value)),
                            depth + 1,
                        )
                    elif value_type is tuple_type:
                        for item in cast_value(tuple[object, ...], value):
                            visit(item, depth + 1)
                    else:
                        frozen_value = cast_value(frozenset[object], value)
                        cached_order = frozenset_orders.get(identity)
                        if cached_order is None or cached_order[0] is not frozen_value:
                            invalid_fingerprint()
                        for item in cached_order[1]:
                            visit(item, depth + 1)
                finally:
                    alias_active_ancestors.remove(identity)

            visit(snapshot_constants(root), 0)
            if alias_active_ancestors or occurrence_count < unique_count:
                invalid_fingerprint()
            groups = list_type(
                occurrences
                for occurrences in alias_occurrences.values()
                if length_of(occurrences) > 1
            )
            groups = list_type(sort_values(groups, key=lambda group: group[0]))
            group_frames = list_type()
            for group in groups:
                encoded_ordinals = tuple_type(encode_uint64(ordinal) for ordinal in group)
                group_fields = (
                    frame(b"gc", encode_uint64(length_of(group))),
                    frame(
                        b"go",
                        bounded_join(iter_values(encoded_ordinals)),
                    ),
                )
                group_frames.append(
                    frame(
                        b"gp",
                        bounded_join(iter_values(group_fields)),
                    )
                )
            alias_fields = (
                frame(b"ad", alias_domain),
                frame(b"oc", encode_uint64(occurrence_count)),
                frame(b"uc", encode_uint64(unique_count)),
                frame(b"ag", encode_uint64(length_of(groups))),
                *group_frames,
            )
            return frame(b"al", bounded_join(iter_values(alias_fields)))

        try:
            value_payload = serialize_code(code, 0)
            if active_ancestors:
                invalid_fingerprint()
            alias_payload = serialize_alias_partition(code)
            payload_fields = (
                frame(b"vl", value_payload),
                alias_payload,
            )
            payload = frame(b"fp", bounded_join(iter_values(payload_fields)))
        except HarnessFailure:
            raise
        except (
            binary_float_pack_error,
            overflow_error,
            recursion_error,
            type_error,
            value_error,
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        fingerprint_digest = digest_constructor(payload).hexdigest()
        if len(fingerprint_cache) >= maximum_cached_fingerprints:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        fingerprint_cache[cache_key] = (code, fingerprint_digest)
        return fingerprint_digest

    try:
        harness_file = path_resolve(path_type(__file__), strict=True)
        checkout_root = harness_file.parents[2]
        allowed_fixture_paths = tuple(
            (
                policy[0],
                path_resolve(
                    checkout_root.joinpath(*suffix.split("/")),
                    strict=True,
                ),
            )
            for policy in fixture_policy
            for suffix in (policy[0],)
        )
    except (IndexError, OSError, RuntimeError, TypeError):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    if (
        harness_file.name != "continuous_public_trade_stream_sqlite_harness.py"
        or harness_file.parent.name != "support"
        or harness_file.parent.parent.name != "tests"
        or any(
            resolved != checkout_root.joinpath(*suffix.split("/"))
            or not path_is_file(resolved)
            or stat_is_link(path_lstat(resolved).st_mode)
            or not stat_is_regular(path_lstat(resolved).st_mode)
            or path_lstat(resolved).st_uid != real_getuid()
            for suffix, resolved in allowed_fixture_paths
        )
    ):
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    for policy, (_, resolved) in zip(
        fixture_policy,
        allowed_fixture_paths,
        strict=True,
    ):
        try:
            source_bytes = path_read_bytes(resolved)
            module_code = compile_code(
                source_bytes,
                str(resolved),
                "exec",
                flags=0,
                dont_inherit=True,
                optimize=0,
            )
            module_digest = digest_constructor(source_bytes).hexdigest()
        except (OSError, SyntaxError, ValueError):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        finally:
            with suppress_errors(UnboundLocalError):
                del module_code
        if module_digest != policy[10]:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    authority_poison = mmap_constructor(
        -1,
        1,
        flags=mmap_shared,
        prot=mmap_protection,
    )
    authority_poison[0] = 0
    mmap_flush(authority_poison)
    poison_read_descriptor, poison_write_descriptor = pipe_constructor()
    try:
        set_blocking(poison_read_descriptor, False)
        set_blocking(poison_write_descriptor, False)
        set_inheritable(poison_read_descriptor, False)
        set_inheritable(poison_write_descriptor, False)
    except BaseException:
        with suppress_errors(OSError):
            pipe_close(poison_read_descriptor)
        with suppress_errors(OSError):
            pipe_close(poison_write_descriptor)
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
    current_session: ContextVar[_PytestRootCapability | None] = ContextVar(
        "task064_pytest_root_session",
        default=None,
    )

    def consume_fault(name: str) -> bool:
        nonlocal armed_faults
        nonlocal fault_hits
        if name not in armed_faults:
            return False
        armed_faults = armed_faults - frozenset((name,))
        fault_hits = fault_hits | frozenset((name,))
        return True

    def arm_fault(name: str) -> None:
        nonlocal armed_faults
        if (
            type(name) is not str
            or name
            not in (root_fault_policy | poison_delivery_fault_policy | poison_probe_fault_policy)
            or name in armed_faults
            or uncertainty_latched()
            or len(armed_faults) >= 2
            or (
                name in root_fault_policy
                and any(
                    fault in root_fault_policy | poison_probe_fault_policy for fault in armed_faults
                )
                and (armed_faults | frozenset((name,))) != paired_fixture_cancel_faults
            )
            or (
                name in poison_delivery_fault_policy
                and any(
                    fault in poison_delivery_fault_policy | poison_probe_fault_policy
                    for fault in armed_faults
                )
            )
            or (name in poison_probe_fault_policy and armed_faults)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        armed_faults = armed_faults | frozenset((name,))

    def fault_hit(name: str) -> bool:
        if type(name) is not str or name not in (
            root_fault_policy | poison_delivery_fault_policy | poison_probe_fault_policy
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        return name in fault_hits

    def poison_channels() -> tuple[bool, bool]:
        try:
            mmap_poisoned = authority_poison[0] != 0
        except (IndexError, OSError, ValueError):
            mmap_poisoned = True
        try:
            readable, _, _ = select_wait(
                (poison_read_descriptor,),
                (),
                (),
                0.0,
            )
            pipe_poisoned = poison_read_descriptor in readable
        except (OSError, ValueError):
            pipe_poisoned = True
        return mmap_poisoned, pipe_poisoned

    def uncertainty_latched() -> bool:
        nonlocal revocation_uncertain
        if revocation_uncertain:
            return True
        mmap_poisoned = consume_fault("poison_mmap_probe")
        if not mmap_poisoned:
            try:
                mmap_poisoned = authority_poison[0] != 0
            except (IndexError, OSError, ValueError):
                mmap_poisoned = True
        pipe_poisoned = consume_fault("poison_pipe_probe")
        if not pipe_poisoned:
            try:
                readable, _, _ = select_wait(
                    (poison_read_descriptor,),
                    (),
                    (),
                    0.0,
                )
                pipe_poisoned = poison_read_descriptor in readable
            except (OSError, ValueError):
                pipe_poisoned = True
        if mmap_poisoned or pipe_poisoned:
            latch_uncertainty()
            return True
        return False

    def latch_uncertainty() -> None:
        nonlocal revocation_uncertain
        revocation_uncertain = True
        if not consume_fault("poison_mmap_write"):
            try:
                authority_poison[0] = 1
            except (IndexError, OSError, ValueError):
                pass
            else:
                if not consume_fault("poison_mmap_flush"):
                    with suppress_errors(OSError, ValueError):
                        mmap_flush(authority_poison)
        for record in tuple(active.values()):
            try:
                active_flag = cast(mmap.mmap, record["revocation_flag"])
                active_flag[0] = 1
                mmap_flush(active_flag)
            except (IndexError, OSError, ValueError):
                pass
        if not consume_fault("poison_pipe_write"):
            try:
                pipe_write(poison_write_descriptor, b"\x01")
            except BlockingIOError:
                pass
            except OSError:
                pass

    def bind_test_module() -> None:
        nonlocal unit_module_globals_identity
        nonlocal integration_module_globals_identity
        try:
            caller = frame_getter(1)
            code = caller.f_code
            call_offset = caller.f_lasti
            module_name = caller.f_globals.get("__name__")
            module_globals_identity = id(caller.f_globals)
        except (ValueError, AttributeError):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        finally:
            with suppress_errors(UnboundLocalError):
                del caller
        if (
            type(code) is not code_type
            or type(module_name) is not str
            or type(call_offset) is not int
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        for policy, (_, expected_path) in zip(
            fixture_policy,
            allowed_fixture_paths,
            strict=True,
        ):
            (
                suffix,
                expected_module,
                _,
                _,
                _,
                _,
                _,
                expected_binder,
                expected_binder_digest,
                expected_binder_offset,
                _,
            ) = policy
            if (
                code.co_filename == str(expected_path)
                and code.co_name == expected_binder
                and module_name == expected_module
                and call_offset == expected_binder_offset
                and code_fingerprint(code, suffix) == expected_binder_digest
            ):
                if suffix.startswith("tests/unit/"):
                    if unit_module_globals_identity is not None:
                        break
                    unit_module_globals_identity = module_globals_identity
                    return
                if integration_module_globals_identity is not None:
                    break
                integration_module_globals_identity = module_globals_identity
                return
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)

    def authenticate_fixture_call(
        *,
        phase: str,
    ) -> tuple[str, str, str]:
        try:
            caller = frame_getter(2)
            code = caller.f_code
            call_offset = caller.f_lasti
            module_name = caller.f_globals.get("__name__")
            module_globals_identity = id(caller.f_globals)
        except (ValueError, AttributeError):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        finally:
            with suppress_errors(UnboundLocalError):
                del caller
        if (
            type(code) is not code_type
            or type(module_name) is not str
            or type(call_offset) is not int
            or phase not in {"BEGIN", "ACTIVATE"}
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        for policy, (_, expected_path) in zip(
            fixture_policy,
            allowed_fixture_paths,
            strict=True,
        ):
            (
                suffix,
                expected_module,
                expected_fixture,
                node_prefix,
                expected_digest,
                begin_offset,
                activate_offset,
                _,
                _,
                _,
                _,
            ) = policy
            expected_offset = begin_offset if phase == "BEGIN" else activate_offset
            expected_globals_identity = (
                unit_module_globals_identity
                if suffix.startswith("tests/unit/")
                else integration_module_globals_identity
            )
            if (
                code.co_filename == str(expected_path)
                and code.co_name == expected_fixture
                and module_name == expected_module
                and expected_globals_identity is not None
                and module_globals_identity == expected_globals_identity
                and call_offset == expected_offset
                and code_fingerprint(code, suffix) == expected_digest
            ):
                return suffix, node_prefix, expected_digest
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)

    def observe_root(pytest_root: Path) -> tuple[object, ...]:
        if not isinstance(pytest_root, path_type) or not pytest_root.is_absolute():
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        target = pytest_root.parent
        current = path_type(target.anchor)
        try:
            for part in target.parts[1:]:
                current = current / part
                ancestor = path_lstat(current)
                if stat_is_link(ancestor.st_mode) or not stat_is_directory(ancestor.st_mode):
                    raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            details = path_lstat(pytest_root)
            resolved = path_resolve(pytest_root, strict=True)
        except HarnessFailure:
            raise
        except (OSError, RuntimeError):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        if (
            stat_is_link(details.st_mode)
            or not stat_is_directory(details.st_mode)
            or details.st_uid != real_getuid()
            or stat_mode(details.st_mode) != 0o700
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        return (
            id(pytest_root),
            str(pytest_root),
            str(resolved),
            details.st_dev,
            details.st_ino,
            details.st_uid,
            stat_mode(details.st_mode),
        )

    def identity_fields(identity: _ActivePytestRoot) -> tuple[object, ...]:
        ledger = identity.evidence_ledger
        if type(ledger) not in {evidence_ledger_type, publication_root_state_type}:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        return (
            id(identity.path_object),
            str(identity.path_object),
            id(identity.resolved_path),
            str(identity.resolved_path),
            identity.device,
            identity.inode,
            identity.uid,
            identity.mode,
            identity.process_id,
            identity.node_id,
            id(identity.nonce),
            identity.nonce,
            id(ledger),
            id(ledger.nonce),
            ledger.nonce,
        )

    def capability_fields(capability: _PytestRootCapability) -> tuple[object, ...]:
        return (
            tuple((id(root), str(root)) for root in capability.roots),
            id(capability._nonce),
            capability._nonce,
        )

    def cleanup_record_for_identity(identity: object) -> dict[str, object] | None:
        uncertainty_latched()
        if type(identity) is not _ActivePytestRoot:
            return None
        record = active.get(id(identity.path_object))
        inode_identity = (identity.device, identity.inode)
        if (
            record is None
            or record["identity"] is not identity
            or record["path"] is not identity.path_object
            or all_roots.get(id(identity.path_object)) is not record
            or inode_records.get(inode_identity) is not record
            or record["state"] != "ACTIVE"
            or identity_fields(identity) != record["identity_fields"]
            or real_getuid() != identity.uid
        ):
            return None
        try:
            if cast(mmap.mmap, record["revocation_flag"])[0] != 0 and not revocation_uncertain:
                return None
        except (IndexError, OSError, ValueError):
            return None
        return record

    def record_for_identity(identity: object) -> dict[str, object] | None:
        if uncertainty_latched():
            return None
        return cleanup_record_for_identity(identity)

    def validate_identity(identity: _ActivePytestRoot) -> bool:
        record = record_for_identity(identity)
        if record is None:
            return False
        try:
            return observe_root(identity.path_object) == record["root_observation"]
        except HarnessFailure:
            return False

    def lookup(pytest_root: object) -> _ActivePytestRoot | None:
        if uncertainty_latched() or not isinstance(pytest_root, path_type):
            return None
        record = active.get(id(pytest_root))
        if (
            record is None
            or record["path"] is not pytest_root
            or record_for_identity(record["identity"]) is not record
        ):
            return None
        return cast(_ActivePytestRoot, record["identity"])

    def lookup_revoked(pytest_root: object) -> _ActivePytestRoot | None:
        if not isinstance(pytest_root, path_type):
            return None
        record = revoked.get(id(pytest_root))
        if (
            record is None
            or record["path"] is not pytest_root
            or all_roots.get(id(pytest_root)) is not record
            or inode_records.get(cast(tuple[int, int], record["inode_identity"])) is not record
            or record["state"] not in {"REVOKED", "REVOCATION_UNCERTAIN"}
            or identity_fields(cast(_ActivePytestRoot, record["identity"]))
            != record["identity_fields"]
        ):
            return None
        return cast(_ActivePytestRoot, record["identity"])

    def revoke_identity(pytest_root: Path, identity: _ActivePytestRoot) -> None:
        record = cleanup_record_for_identity(identity)
        if record is None or record["path"] is not pytest_root:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        errors: list[BaseException] = []
        try:
            if report_cache_teardown is None or not report_cache_teardown(identity):
                raise RuntimeError("report validation cache teardown failed")
        except BaseException as error:
            errors.append(error)
            latch_uncertainty()
        record["state"] = "REVOKING"
        try:
            flag = cast(mmap.mmap, record["revocation_flag"])
            if consume_fault("revocation_flag_write"):
                raise OSError(errno.EIO, "injected revocation-flag write failure")
            flag[0] = 1
            if consume_fault("revocation_flag_flush"):
                raise OSError(errno.EIO, "injected revocation-flag flush failure")
            mmap_flush(flag)
            if consume_fault("revocation_after_flush"):
                raise OSError(errno.EIO, "injected post-flush revocation uncertainty")
        except BaseException as error:
            errors.append(error)
            latch_uncertainty()
            record["state"] = "REVOCATION_UNCERTAIN"
        else:
            record["state"] = "REVOKED"
        key = id(pytest_root)
        if active.get(key) is record:
            active.pop(key)
        revoked[key] = record

        ledger = record["ledger"]
        if type(ledger) is evidence_ledger_type:
            evidence_ledger = ledger
            issued_run = evidence_ledger.run
            if issued_run is not None:
                try:
                    if run_closer is None or run_closed is None:
                        raise RuntimeError("checked evidence-run close uncertainty")
                    close_result = (
                        False if consume_fault("evidence_run_close") else run_closer(issued_run)
                    )
                    if not close_result or not run_closed(issued_run):
                        raise RuntimeError("evidence-run authority did not reach CLOSED")
                except BaseException as error:
                    errors.append(error)
            try:
                evidence_ledger.closed = True
                evidence_ledger.recording = False
                evidence_ledger.observations.clear()
                evidence_ledger.operation_runs.clear()
                evidence_ledger.rejection_runs.clear()
            except BaseException as error:
                errors.append(error)
        elif type(ledger) is not publication_root_state_type:
            errors.append(RuntimeError("invalid pytest-root ledger state"))
        try:
            issued_active_run = active_evidence_run.get()
            if issued_active_run is not None and issued_active_run._pytest_registration is identity:
                active_evidence_run.set(None)
        except BaseException as error:
            errors.append(error)
        try:
            if consume_fault("revocation_close"):
                raise OSError(errno.EIO, "injected checked revocation close uncertainty")
            mmap_close(cast(mmap.mmap, record["revocation_flag"]))
        except BaseException as error:
            errors.append(error)
        if errors:
            latch_uncertainty()
            record["state"] = "REVOCATION_UNCERTAIN"
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None

    def bind_run_closer(
        closer: Callable[[_EvidenceRun], bool],
        closed_observer: Callable[[_EvidenceRun], bool],
    ) -> None:
        nonlocal run_closer
        nonlocal run_closed
        if (
            run_closer is not None
            or run_closed is not None
            or not callable(closer)
            or not callable(closed_observer)
            or closer is closed_observer
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        run_closer = closer
        run_closed = closed_observer

    def bind_report_cache_teardown(
        teardown: Callable[[_ActivePytestRoot], bool],
    ) -> None:
        nonlocal report_cache_teardown
        if report_cache_teardown is not None or not callable(teardown):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        report_cache_teardown = teardown

    def issue_identity(
        pytest_root: Path,
        *,
        node_id: str,
        owner_process_id: int,
        publication_only: bool,
    ) -> tuple[_ActivePytestRoot, dict[str, object]]:
        if uncertainty_latched():
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        observed = observe_root(pytest_root)
        if uncertainty_latched():
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        object_key = id(pytest_root)
        inode_identity = (cast(int, observed[3]), cast(int, observed[4]))
        if object_key in all_roots or inode_identity in inode_records:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        resolved = path_type(cast(str, observed[2]))
        revocation_flag: mmap.mmap | None = None
        try:
            revocation_flag = mmap_constructor(
                -1,
                1,
                flags=mmap_shared,
                prot=mmap_protection,
            )
            if uncertainty_latched():
                mmap_close(revocation_flag)
                revocation_flag = None
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            revocation_flag[0] = 0
            if uncertainty_latched():
                mmap_close(revocation_flag)
                revocation_flag = None
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            root_nonce = nonce_issuer("pytest-root")
            if type(publication_only) is not bool:
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            ledger_nonce = nonce_issuer(
                "report-publication-root" if publication_only else "evidence-ledger"
            )
            ledger: _EvidenceLedger | _ReportPublicationRootState = (
                publication_root_state_type(nonce=ledger_nonce)
                if publication_only
                else evidence_ledger_type(
                    nonce=ledger_nonce,
                    observations={},
                    operation_runs={},
                    rejection_runs={},
                )
            )
            identity = _ActivePytestRoot(
                path_object=pytest_root,
                resolved_path=resolved,
                device=cast(int, observed[3]),
                inode=cast(int, observed[4]),
                uid=cast(int, observed[5]),
                mode=cast(int, observed[6]),
                process_id=owner_process_id,
                node_id=node_id,
                nonce=root_nonce,
                evidence_ledger=ledger,
            )
            record: dict[str, object] = {
                "path": pytest_root,
                "identity": identity,
                "identity_fields": identity_fields(identity),
                "root_observation": observed,
                "ledger": ledger,
                "revocation_flag": revocation_flag,
                "owner_process_id": owner_process_id,
                "owner_uid": real_getuid(),
                "inode_identity": inode_identity,
                "state": "ACTIVE",
            }
            all_roots[object_key] = record
            inode_records[inode_identity] = record
            return identity, record
        except BaseException:
            if revocation_flag is not None:
                try:
                    mmap_close(revocation_flag)
                except (OSError, ValueError):
                    latch_uncertainty()
            raise

    def terminalize_node_lifecycle(node_key: tuple[int, str]) -> bool:
        nonlocal active_node_lifecycle
        nonlocal returned_node_tombstones
        if active_node_lifecycle != node_key:
            return False
        if node_key not in returned_node_tombstones:
            returned_node_tombstones = (*returned_node_tombstones, node_key)
        active_node_lifecycle = None
        return True

    def begin_registration(
        node_id: str,
        pytest_root: Path,
        publication_only: bool = False,
    ) -> _PytestRootRegistrationPermit:
        """Authorize one exact fixture call before it creates auxiliary roots."""

        nonlocal active_node_lifecycle
        nonlocal returned_node_tombstones
        if uncertainty_latched():
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        suffix, node_prefix, fixture_digest = authenticate_fixture_call(phase="BEGIN")
        owner_process_id = real_getpid()
        node_key = (owner_process_id, node_id)
        if node_key in returned_node_tombstones:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        if (
            type(node_id) is not str
            or not node_id
            or not (node_id == node_prefix or node_id.startswith(f"{node_prefix}::"))
            or not isinstance(pytest_root, path_type)
            or not pytest_root.is_absolute()
            or type(publication_only) is not bool
            or id(pytest_root) in all_roots
            or active_node_lifecycle is not None
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        active_node_lifecycle = node_key
        try:
            root_observation = observe_root(pytest_root)
            if uncertainty_latched():
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            permit = _PytestRootRegistrationPermit(
                _nonce=nonce_issuer("pytest-root-registration-permit"),
            )
            permits[id(permit)] = {
                "permit": permit,
                "permit_fields": (id(permit._nonce), permit._nonce),
                "module_suffix": suffix,
                "fixture_digest": fixture_digest,
                "node_id": node_id,
                "pytest_root": pytest_root,
                "path_type": type(pytest_root),
                "root_observation": root_observation,
                "owner_process_id": owner_process_id,
                "owner_uid": real_getuid(),
                "publication_only": publication_only,
                "state": "PERMIT_ISSUED",
            }
            return permit
        except BaseException:
            if not terminalize_node_lifecycle(node_key):
                latch_uncertainty()
            raise

    def cancel_registration(permit: _PytestRootRegistrationPermit) -> None:
        """Permanently cancel one exact unactivated or partially activated permit."""

        permit_record = permits.get(id(permit))
        node_key = (
            (
                cast(int, permit_record["owner_process_id"]),
                cast(str, permit_record["node_id"]),
            )
            if permit_record is not None
            else None
        )
        if (
            type(permit) is not _PytestRootRegistrationPermit
            or permit_record is None
            or permit_record["permit"] is not permit
            or permit_record["permit_fields"] != (id(permit._nonce), permit._nonce)
            or node_key is None
            or real_getpid() != permit_record["owner_process_id"]
            or real_getuid() != permit_record["owner_uid"]
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        if (
            permit_record["state"] in {"CANCELLED", "RETURNED"}
            and active_node_lifecycle is None
            and node_key in returned_node_tombstones
        ):
            return
        if (
            permit_record["state"] not in {"PERMIT_ISSUED", "READY", "ACTIVATING"}
            or active_node_lifecycle != node_key
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        permit_record["state"] = "CANCELLING"
        cancellation_errors: list[BaseException] = []
        try:
            if consume_fault("permit_cancel"):
                raise OSError(errno.EIO, "injected permit-cancellation uncertainty")
            for root, identity in reversed(
                cast(
                    list[tuple[Path, _ActivePytestRoot]],
                    permit_record.get("issued", []),
                )
            ):
                if active.get(id(root)) is not None:
                    try:
                        revoke_identity(root, identity)
                    except BaseException as error:
                        cancellation_errors.append(error)
        except BaseException as error:
            cancellation_errors.append(error)
        if not terminalize_node_lifecycle(node_key):
            cancellation_errors.append(HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT))
        permit_record["state"] = "CANCEL_UNCERTAIN" if cancellation_errors else "CANCELLED"
        if cancellation_errors:
            latch_uncertainty()
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None

    def scope(
        permit: _PytestRootRegistrationPermit,
        fixture_roots: tuple[Path, ...],
    ) -> AbstractContextManager[_PytestRootCapability]:
        """Validate all fixture-created roots and pre-issue them before test code."""

        if uncertainty_latched():
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None
        permit_record = permits.get(id(permit))
        expected_node_lifecycle = (
            (
                cast(int, permit_record["owner_process_id"]),
                cast(str, permit_record["node_id"]),
            )
            if permit_record is not None
            else None
        )
        if (
            type(permit) is not _PytestRootRegistrationPermit
            or permit_record is None
            or permit_record["permit"] is not permit
            or permit_record["permit_fields"] != (id(permit._nonce), permit._nonce)
            or permit_record["state"] != "PERMIT_ISSUED"
            or real_getpid() != permit_record["owner_process_id"]
            or real_getuid() != permit_record["owner_uid"]
            or type(fixture_roots) is not tuple
            or len(fixture_roots) != 4
            or active_node_lifecycle != expected_node_lifecycle
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        suffix, node_prefix, fixture_digest = authenticate_fixture_call(phase="ACTIVATE")
        pytest_root = cast(Path, permit_record["pytest_root"])
        roots = (pytest_root, *fixture_roots)
        if (
            suffix != permit_record["module_suffix"]
            or node_prefix != cast(str, permit_record["node_id"]).split("::", 1)[0]
            or fixture_digest != permit_record["fixture_digest"]
            or any(
                not isinstance(root, path_type)
                or type(root) is not permit_record["path_type"]
                or not root.is_absolute()
                for root in roots
            )
            or len({id(root) for root in roots}) != len(roots)
            or any(root.parent != pytest_root.parent for root in fixture_roots)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        if consume_fault("permit_scope_construction"):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        observations = tuple(observe_root(root) for root in roots)
        if (
            observations[0] != permit_record["root_observation"]
            or len({(item[3], item[4]) for item in observations}) != len(roots)
            or uncertainty_latched()
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        permit_record["roots"] = roots
        permit_record["root_observations"] = observations
        permit_record["state"] = "READY"

        @contextmanager
        def activate() -> Iterator[_PytestRootCapability]:
            if uncertainty_latched():
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            owner_process_id = real_getpid()
            session_key = id(permit)
            existing_session = sessions.get(session_key)
            if (
                current_session.get() is not None
                or existing_session is not None
                or permit_record["state"] != "READY"
                or owner_process_id != permit_record["owner_process_id"]
                or real_getuid() != permit_record["owner_uid"]
            ):
                raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
            permit_record["state"] = "ACTIVATING"
            issued: list[tuple[Path, _ActivePytestRoot]] = []
            permit_record["issued"] = issued
            capability: _PytestRootCapability | None = None
            capability_record: dict[str, object] | None = None
            session_token: Token[_PytestRootCapability | None] | None = None
            cleanup_errors: list[BaseException] = []
            activation_reached = False
            try:
                if consume_fault("permit_scope_entry"):
                    raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
                identities: set[tuple[int, int]] = set()
                for root in roots:
                    if uncertainty_latched():
                        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
                    identity, root_record = issue_identity(
                        root,
                        node_id=cast(str, permit_record["node_id"]),
                        owner_process_id=owner_process_id,
                        publication_only=cast(bool, permit_record["publication_only"]),
                    )
                    key = id(root)
                    inode_identity = (identity.device, identity.inode)
                    if (
                        key in active
                        or key in revoked
                        or all_roots.get(key) is not root_record
                        or inode_records.get(inode_identity) is not root_record
                        or inode_identity in identities
                    ):
                        try:
                            cast(mmap.mmap, root_record["revocation_flag"]).close()
                        except (OSError, ValueError):
                            cleanup_errors.append(
                                HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
                            )
                        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
                    identities.add(inode_identity)
                    active[key] = root_record
                    issued.append((root, identity))
                    if len(issued) == 1 and consume_fault("permit_partial_activation"):
                        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
                capability_nonce = nonce_issuer("pytest-root-capability")
                capability = _PytestRootCapability(
                    roots=roots,
                    _nonce=capability_nonce,
                )
                capability_record = {
                    "capability": capability,
                    "capability_fields": capability_fields(capability),
                    "roots": roots,
                    "permit": permit,
                    "module_suffix": permit_record["module_suffix"],
                    "fixture_digest": permit_record["fixture_digest"],
                    "node_id": permit_record["node_id"],
                    "owner_process_id": owner_process_id,
                    "owner_uid": permit_record["owner_uid"],
                    "state": "ACTIVE",
                }
                sessions[session_key] = capability_record
                capabilities[id(capability)] = capability_record
                permit_record["state"] = "ACTIVE"
                session_token = current_session.set(capability)
                activation_reached = True
                yield capability
            finally:
                if session_token is not None:
                    try:
                        current_session.reset(session_token)
                    except BaseException as error:
                        cleanup_errors.append(error)
                if capability is not None:
                    capability_record = capabilities.get(id(capability))
                    if (
                        capability_record is not None
                        and capability_record["capability"] is capability
                    ):
                        capability_record["state"] = (
                            "RETURNED" if activation_reached else "CANCELLED"
                        )
                permit_record["state"] = "RETURNED" if activation_reached else "CANCELLED"
                for root, identity in reversed(issued):
                    if active.get(id(root)) is not None:
                        try:
                            revoke_identity(root, identity)
                        except BaseException as error:
                            cleanup_errors.append(error)
                if expected_node_lifecycle is None or not terminalize_node_lifecycle(
                    expected_node_lifecycle
                ):
                    cleanup_errors.append(HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT))
                if cleanup_errors:
                    latch_uncertainty()
                    raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT) from None

        return activate()

    def revoke_fixture_root(
        capability: _PytestRootCapability,
        pytest_root: Path,
    ) -> None:
        capability_record = capabilities.get(id(capability))
        if (
            type(capability) is not _PytestRootCapability
            or capability_record is None
            or capability_record["capability"] is not capability
            or capability_record["state"] != "ACTIVE"
            or capability_fields(capability) != capability_record["capability_fields"]
            or current_session.get() is not capability
            or not isinstance(pytest_root, path_type)
            or not any(
                root is pytest_root
                for root in cast(tuple[Path, ...], capability_record["roots"])[1:]
            )
            or real_getpid() != capability_record["owner_process_id"]
            or real_getuid() != capability_record["owner_uid"]
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        record = active.get(id(pytest_root))
        identity = None if record is None else record.get("identity")
        if (
            type(identity) is not _ActivePytestRoot
            or cleanup_record_for_identity(identity) is not record
            or identity.path_object is not pytest_root
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        revoke_identity(pytest_root, identity)

    def session_owns(identity: _ActivePytestRoot, allow_direct_child: bool) -> bool:
        capability = current_session.get()
        capability_record = (
            None
            if type(capability) is not _PytestRootCapability
            else capabilities.get(id(capability))
        )
        current_process_id = real_getpid()
        return bool(
            not uncertainty_latched()
            and not process_cleanup_uncertain()
            and type(identity) is _ActivePytestRoot
            and type(allow_direct_child) is bool
            and type(capability) is _PytestRootCapability
            and capability_record is not None
            and capability_record["capability"] is capability
            and capability_record["state"] == "ACTIVE"
            and capability_fields(capability) == capability_record["capability_fields"]
            and real_getuid() == capability_record["owner_uid"]
            and identity.uid == capability_record["owner_uid"]
            and any(
                root is identity.path_object
                for root in cast(tuple[Path, ...], capability_record["roots"])
            )
            and lookup(identity.path_object) is identity
            and (
                current_process_id == capability_record["owner_process_id"]
                or (allow_direct_child and real_getppid() == capability_record["owner_process_id"])
            )
        )

    return (
        bind_test_module,
        begin_registration,
        cancel_registration,
        scope,
        lookup,
        validate_identity,
        lookup_revoked,
        revoke_fixture_root,
        session_owns,
        arm_fault,
        fault_hit,
        poison_channels,
        uncertainty_latched,
        bind_run_closer,
        bind_report_cache_teardown,
        latch_uncertainty,
    )


(
    _bind_task064_test_module,
    _begin_pytest_root_registration,
    _cancel_pytest_root_registration,
    _pytest_root_scope,
    _lookup_active_pytest_root,
    _validate_pytest_root_identity,
    _lookup_revoked_pytest_root,
    _revoke_fixture_root,
    _pytest_root_session_owns,
    _arm_pytest_root_authority_fault,
    _pytest_root_authority_fault_hit,
    _pytest_root_poison_channels,
    _pytest_root_authority_uncertain,
    _bind_pytest_root_run_closer,
    _bind_pytest_root_report_cache_teardown,
    _latch_pytest_root_authority_uncertainty,
) = _build_pytest_root_authority()
del _build_pytest_root_authority
_bind_process_cleanup_root_poison(_latch_pytest_root_authority_uncertainty)
del _bind_process_cleanup_root_poison


def _validate_bootstrap_root(pytest_root: Path) -> Path:
    if not isinstance(pytest_root, Path) or not pytest_root.is_absolute():
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    active = _lookup_active_pytest_root(pytest_root)
    if (
        active is None
        or active.path_object is not pytest_root
        or not _validate_pytest_root_identity(active)
        or not _pytest_root_session_owns(active, False)
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


_TASK064_CHILD_PROVENANCE_ENVIRONMENT = "WEALTH_TASK064_CHILD_PROVENANCE"
_TASK064_LEGACY_CHILD_MODE_ENVIRONMENTS = (
    "WEALTH_TASK064_EXEC_ISOLATION",
    "WEALTH_TASK064_POST_RETURN_FIXTURE_REPLAY",
    "WEALTH_TASK064_ROOT_LATCH_PROBE",
    "WEALTH_TASK064_SHARED_CLEANUP_PROBE",
    "WEALTH_TASK064_REPORT_ROOT_CLOSE_PROBE",
)


@dataclass(frozen=True, slots=True)
class _Task064ChildProvenance:
    envelope: str
    descriptor: int
    artifact_descriptor: int
    marker_path: Path
    marker_device: int
    marker_inode: int
    root_path: Path
    root_device: int
    root_inode: int
    nonce: str


@dataclass(frozen=True, slots=True)
class _Task064ChildProvenanceTicket:
    """Opaque, authority-registered proof that fixture-order authentication completed."""

    _authority_nonce: bytes


@dataclass(frozen=True, slots=True)
class _Task064ReportPublicationPermit:
    """Opaque child-local authority for one exact authenticated report publication."""

    _authority_nonce: bytes


@dataclass(frozen=True, slots=True)
class _Task064PublishedReportArtifactCapability:
    """Opaque parent authority for one receipt-backed published report artifact."""

    _authority_nonce: bytes


def _build_task064_child_provenance_authority() -> tuple[
    Callable[..., _Task064ChildProvenance],
    Callable[..., subprocess.Popen[bytes]],
    Callable[[_Task064ChildProvenance, str], bool],
    Callable[[str], _Task064ChildProvenanceTicket | None],
    Callable[[_Task064ChildProvenanceTicket | None, str, Path], None],
    Callable[[_Task064ChildProvenanceTicket | None, str], bool],
    Callable[[_Task064ChildProvenanceTicket | None, str], bool],
    Callable[[str, str, tuple[str, ...]], str | None],
    Callable[[_Task064ChildProvenanceTicket | None, str], None],
    Callable[[_Task064ChildProvenanceTicket | None, str], None],
    Callable[[_Task064ChildProvenance], bool],
    Callable[
        [
            Callable[[bytes], None],
            Callable[[], tuple[tuple[str, str], ...]],
            Callable[..., tuple[bytes, Mapping[str, object], Callable[..., object]]],
        ],
        None,
    ],
    Callable[[str, Path], tuple[bytes, _Task064ReportPublicationPermit]],
    Callable[[_Task064ReportPublicationPermit, Path, bytes], bool],
    Callable[
        [_Task064ReportPublicationPermit],
        tuple[Callable[[], bool], Callable[[], bool], Callable[[], bool]],
    ],
]:
    """Issue one-shot, inherited-FD provenance for internal pytest children."""

    envelope_environment = _TASK064_CHILD_PROVENANCE_ENVIRONMENT
    legacy_environments = _TASK064_LEGACY_CHILD_MODE_ENVIRONMENTS
    provenance_type = _Task064ChildProvenance
    ticket_type = _Task064ChildProvenanceTicket
    report_permit_type = _Task064ReportPublicationPermit
    published_artifact_capability_type = _Task064PublishedReportArtifactCapability
    failure_type = HarnessFailure
    invalid_code = HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    validate_root = _validate_bootstrap_root
    lookup_root = _lookup_active_pytest_root
    session_owns_root = _pytest_root_session_owns
    validate_root_identity = _validate_pytest_root_identity
    mark_authority_uncertain = _latch_pytest_root_authority_uncertainty
    path_type = Path
    type_of = type
    int_type = int
    str_type = str
    dict_type = dict
    list_type = list
    set_type = set
    tuple_type = tuple
    bytes_type = bytes
    bool_type = bool
    isinstance_value = isinstance
    base_exception_type = BaseException
    length_of = len
    all_values = all
    any_values = any
    minimum_value = min
    suppress_errors = suppress
    os_error = OSError
    type_error = TypeError
    value_error = ValueError
    unicode_error = UnicodeError
    file_not_found_error = FileNotFoundError
    process_lookup_error = ProcessLookupError
    environment = os.environ
    current_pid = os.getpid
    parent_pid = os.getppid
    current_thread_id = get_ident
    current_uid = os.getuid
    descriptor_is_inheritable = os.get_inheritable
    open_file = os.open
    close_file = os.close
    read_file = os.read
    write_file = os.write
    seek_file = os.lseek
    sync_file = os.fsync
    chmod_file = os.fchmod
    stat_file = os.fstat
    unlink_file = os.unlink
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    read_only = os.O_RDONLY
    write_only = os.O_WRONLY
    read_write = os.O_RDWR
    create_exclusive = os.O_CREAT | os.O_EXCL
    seek_start = os.SEEK_SET
    descriptor_flags = fcntl.fcntl
    get_file_status_flags = fcntl.F_GETFL
    access_mode_mask = os.O_ACCMODE
    monotonic_ns = time.monotonic_ns
    stat_is_regular = stat.S_ISREG
    stat_is_directory = stat.S_ISDIR
    stat_is_link = stat.S_ISLNK
    stat_mode = stat.S_IMODE
    canonical_dump = json.dumps
    canonical_load = json.loads
    digest_constructor = hashlib.sha256
    issue_nonce = secrets.token_hex
    issue_ticket_nonce = secrets.token_bytes
    object_identity = id
    task_contract_generation = TASK_CONTRACT_GENERATION
    task_contract_digest = TASK_CONTRACT_DIGEST
    schema_fingerprint_provider = load_schema_fingerprint
    context_var_type = ContextVar
    context_token_type = Token
    socket_constructor = socket.socket
    socket_family = socket.AF_UNIX
    socket_sequence_packet = socket.SOCK_SEQPACKET
    socket_level = socket.SOL_SOCKET
    socket_pass_credentials = socket.SO_PASSCRED
    socket_peer_credentials = socket.SO_PEERCRED
    socket_accepting = socket.SO_ACCEPTCONN
    socket_type_option = socket.SO_TYPE
    socket_credentials_message = socket.SCM_CREDENTIALS
    socket_timeout_error = socket.timeout
    credentials_size = struct.calcsize("3i")
    credentials_space = socket.CMSG_SPACE(credentials_size)
    unpack_credentials = struct.unpack
    lock_type = threading.Lock
    popen_constructor = subprocess.Popen
    subprocess_pipe = subprocess.PIPE
    timeout_expired_type = subprocess.TimeoutExpired
    kill_process_group = os.killpg
    terminate_signal = signal.SIGTERM
    kill_signal = signal.SIGKILL
    register_at_fork = os.register_at_fork
    register_at_exit = atexit.register
    python_executable = sys.executable
    repository_root = Path(__file__).resolve().parents[2]
    protocol_policy = (
        (
            "exec_isolation",
            "WEALTH_TASK064_EXEC_ISOLATION",
            (),
            True,
        ),
        (
            "post_return_fixture_replay",
            "WEALTH_TASK064_POST_RETURN_FIXTURE_REPLAY",
            (),
            True,
        ),
        (
            "root_latch",
            "WEALTH_TASK064_ROOT_LATCH_PROBE",
            (
                "revocation_flag_write",
                "revocation_flag_flush",
                "revocation_after_flush",
                "revocation_close",
                "evidence_run_close",
                "poison_mmap_write",
                "poison_mmap_flush",
                "poison_pipe_write",
                "poison_mmap_probe",
                "poison_pipe_probe",
            ),
            False,
        ),
        (
            "shared_cleanup",
            "WEALTH_TASK064_SHARED_CLEANUP_PROBE",
            ("run",),
            False,
        ),
        (
            "report_close",
            "WEALTH_TASK064_REPORT_ROOT_CLOSE_PROBE",
            (
                "readback_verified_root_close_ambiguity",
                "staging_close_ambiguity",
                "readback_close_ambiguity",
                "reentrant_root_revocation",
            ),
            False,
        ),
    )
    consumed_nonces: set[str] = set()
    ticket_records: dict[int, dict[str, object]] = {}
    provenance_records: dict[int, dict[str, object]] = {}
    launch_records: dict[int, dict[str, object]] = {}
    terminal_ticket_records: dict[
        int,
        tuple[_Task064ChildProvenanceTicket, int, str, str],
    ] = {}
    report_permit_records: dict[int, dict[str, object]] = {}
    terminal_report_permit_records: dict[
        int,
        tuple[_Task064ReportPublicationPermit, int, str],
    ] = {}
    active_body_ticket: ContextVar[_Task064ChildProvenanceTicket | None] = context_var_type(
        "task064_child_provenance_ticket",
        default=None,
    )
    maximum_packet_bytes = 8_192
    maximum_envelope_bytes = 2_048
    maximum_consumed_nonces = 1_024
    maximum_active_tickets = 64
    maximum_terminal_tickets = 64
    maximum_report_artifact_bytes = 4 * 1024 * 1024
    maximum_report_permits = 64
    maximum_report_child_lifetime_ns = 900_000_000_000
    maximum_parent_handshake_lifetime_ns = 30_000_000_000
    domain = "TASK064-CHILD-PROVENANCE-V1"
    artifact_metadata_keys = (
        "artifact_descriptor",
        "artifact_device",
        "artifact_inode",
        "artifact_uid",
        "artifact_mode",
        "artifact_nlink",
        "artifact_length",
        "artifact_sha256",
        "artifact_nonce",
        "artifact_deadline_ns",
        "contract_generation",
        "contract_digest",
        "schema_fingerprint",
        "source_fingerprints",
    )
    publication_metadata_keys = (
        "publication_nonce",
        "publication_run_digest",
        "publication_aggregate_digest",
        "publication_report_object_digest",
        "publication_root_path",
        "publication_root_device",
        "publication_root_inode",
        "publication_root_uid",
        "publication_root_mode",
        "publication_path",
        "publication_device",
        "publication_inode",
        "publication_uid",
        "publication_mode",
        "publication_nlink",
        "publication_process_id",
        "publication_thread_id",
        "publication_node_id",
        "publication_context_digest",
        "publication_expires_ns",
    )
    report_artifact_validator: Callable[[bytes], None] | None = None
    report_source_fingerprints: Callable[[], tuple[tuple[str, str], ...]] | None = None
    resolve_published_report_artifact: (
        Callable[..., tuple[bytes, Mapping[str, object], Callable[..., object]]] | None
    ) = None
    post_return_issuer_node_id = (
        "tests/integration/"
        "test_task_064_continuous_public_trade_stream_sqlite_evidence.py::"
        "test_exact_fixture_callable_replay_is_rejected_after_real_lifecycle_return"
    )

    def invalid() -> Never:
        raise failure_type(invalid_code)

    def policy_for(protocol: str) -> tuple[str, tuple[str, ...], bool]:
        for candidate, legacy_environment, fixed_modes, mode_is_node in protocol_policy:
            if protocol == candidate:
                return legacy_environment, fixed_modes, mode_is_node
        invalid()

    def canonical_json(value: object) -> str:
        try:
            encoded = canonical_dump(
                value,
                ensure_ascii=True,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (type_error, value_error):
            invalid()
        if type_of(encoded) is not str_type:
            invalid()
        return encoded

    def valid_hex(value: object, size: int) -> bool:
        if type_of(value) is not str_type:
            return False
        exact_value = str_type(value)
        return bool_type(
            length_of(exact_value) == size
            and all_values(character in "0123456789abcdef" for character in exact_value)
        )

    listener_owner_pid = current_pid()
    listener_address_prefix = b"\x00wealth-task064-g3-"
    # Threat boundary: kernel parent credentials plus the exact wrapper registry are
    # authoritative here. A malicious intermediary parent, or arbitrary same-process
    # closure/descriptor introspection, is outside the controlled-pytest threat model.

    def listener_address(process_id: int) -> bytes:
        if type_of(process_id) is not int_type or process_id <= 1:
            invalid()
        address = listener_address_prefix + str_type(process_id).encode("ascii")
        if length_of(address) > 96:
            invalid()
        return address

    authority_listener = socket_constructor(socket_family, socket_sequence_packet)
    authority_listener_address = listener_address(listener_owner_pid)
    try:
        authority_listener.setsockopt(socket_level, socket_pass_credentials, 1)
        authority_listener.bind(authority_listener_address)
        authority_listener.listen(maximum_active_tickets)
        authority_listener.settimeout(0.1)
        listener_descriptor = authority_listener.fileno()
        listener_details = stat_file(listener_descriptor)
        if (
            listener_descriptor <= 2
            or descriptor_is_inheritable(listener_descriptor)
            or not stat.S_ISSOCK(listener_details.st_mode)
            or listener_details.st_uid != current_uid()
            or authority_listener.getsockname() != authority_listener_address
            or authority_listener.getsockopt(socket_level, socket_accepting) != 1
            or authority_listener.getsockopt(socket_level, socket_type_option)
            != socket_sequence_packet
        ):
            invalid()
        listener_fields = (
            listener_descriptor,
            listener_details.st_dev,
            listener_details.st_ino,
            listener_details.st_uid,
            stat_mode(listener_details.st_mode),
        )
    except base_exception_type:
        with suppress_errors(base_exception_type):
            authority_listener.close()
        raise

    def close_in_forked_child() -> None:
        if current_pid() != listener_owner_pid:
            with suppress_errors(base_exception_type):
                authority_listener.close()

    def close_listener_at_exit() -> None:
        if current_pid() == listener_owner_pid:
            with suppress_errors(base_exception_type):
                authority_listener.close()

    register_at_fork(after_in_child=close_in_forked_child)
    register_at_exit(close_listener_at_exit)

    def listener_is_exact() -> bool:
        try:
            descriptor = authority_listener.fileno()
            details = stat_file(descriptor)
            return bool_type(
                current_pid() == listener_owner_pid
                and (
                    descriptor,
                    details.st_dev,
                    details.st_ino,
                    details.st_uid,
                    stat_mode(details.st_mode),
                )
                == listener_fields
                and not descriptor_is_inheritable(descriptor)
                and stat.S_ISSOCK(details.st_mode)
                and authority_listener.getsockname() == authority_listener_address
                and authority_listener.getsockopt(socket_level, socket_accepting) == 1
                and authority_listener.getsockopt(socket_level, socket_type_option)
                == socket_sequence_packet
            )
        except (os_error, value_error):
            return False

    def validate_protocol_values(
        protocol: str,
        target_node_id: str,
        mode: str,
    ) -> tuple[str, tuple[str, ...], bool]:
        if (
            type_of(protocol) is not str_type
            or type_of(target_node_id) is not str_type
            or type_of(mode) is not str_type
            or not target_node_id.startswith("tests/")
            or "::" not in target_node_id
            or "\x00" in target_node_id
            or "\x00" in mode
            or length_of(target_node_id) > 2_048
            or length_of(mode) > 2_048
        ):
            invalid()
        exact_protocol = str_type(protocol)
        exact_target_node_id = str_type(target_node_id)
        exact_mode = str_type(mode)
        legacy_environment, fixed_modes, mode_is_node = policy_for(exact_protocol)
        if (mode_is_node and exact_mode != exact_target_node_id) or (
            not mode_is_node and exact_mode not in fixed_modes
        ):
            invalid()
        return legacy_environment, fixed_modes, mode_is_node

    def scrub_child_environment() -> None:
        environment.pop(envelope_environment, None)
        for ambient_environment in legacy_environments:
            environment.pop(ambient_environment, None)

    def published_origin_matches(
        binding: Mapping[str, object],
        artifact: bytes,
    ) -> bool:
        if (
            not isinstance_value(binding, Mapping)
            or type_of(artifact) is not bytes_type
            or set_type(binding) != set_type(publication_metadata_keys)
        ):
            return False
        root_descriptor = -1
        descriptor = -1
        try:
            root_path = path_type(cast(str, binding["publication_root_path"]))
            publication_path = path_type(cast(str, binding["publication_path"]))
            if publication_path != root_path / "task064-evidence.json":
                return False
            root_descriptor = open_file(
                str_type(root_path),
                read_only | directory_only | no_follow | close_on_exec,
            )
            root_details = stat_file(root_descriptor)
            descriptor = open_file(
                "task064-evidence.json",
                read_only | no_follow | close_on_exec,
                dir_fd=root_descriptor,
            )
            details = stat_file(descriptor)
            path_details = publication_path.lstat()
            if (
                descriptor_flags(descriptor, get_file_status_flags) & access_mode_mask != read_only
                or not stat_is_directory(root_details.st_mode)
                or (
                    root_details.st_dev,
                    root_details.st_ino,
                    root_details.st_uid,
                    stat_mode(root_details.st_mode),
                )
                != (
                    binding["publication_root_device"],
                    binding["publication_root_inode"],
                    binding["publication_root_uid"],
                    binding["publication_root_mode"],
                )
                or stat_is_link(path_details.st_mode)
                or not stat_is_regular(details.st_mode)
                or not stat_is_regular(path_details.st_mode)
                or (
                    details.st_dev,
                    details.st_ino,
                    details.st_uid,
                    stat_mode(details.st_mode),
                    details.st_nlink,
                    details.st_size,
                )
                != (
                    binding["publication_device"],
                    binding["publication_inode"],
                    binding["publication_uid"],
                    binding["publication_mode"],
                    binding["publication_nlink"],
                    length_of(artifact),
                )
                or (
                    path_details.st_dev,
                    path_details.st_ino,
                    path_details.st_uid,
                    stat_mode(path_details.st_mode),
                    path_details.st_nlink,
                    path_details.st_size,
                )
                != (
                    details.st_dev,
                    details.st_ino,
                    details.st_uid,
                    stat_mode(details.st_mode),
                    details.st_nlink,
                    details.st_size,
                )
            ):
                return False
            seek_file(descriptor, 0, seek_start)
            observed = bytearray()
            while length_of(observed) <= length_of(artifact):
                fragment = read_file(
                    descriptor,
                    minimum_value(
                        65_536,
                        length_of(artifact) + 1 - length_of(observed),
                    ),
                )
                if type_of(fragment) is not bytes_type:
                    return False
                if not fragment:
                    break
                observed.extend(fragment)
            exact = bytes(observed)
            return bool_type(
                exact == artifact
                and length_of(exact) == length_of(artifact)
                and digest_constructor(exact).hexdigest()
                == digest_constructor(artifact).hexdigest()
            )
        except (os_error, type_error, value_error):
            return False
        finally:
            close_ok = True
            for pending_descriptor in (descriptor, root_descriptor):
                if pending_descriptor >= 0:
                    try:
                        close_file(pending_descriptor)
                    except os_error:
                        close_ok = False
            if not close_ok:
                mark_authority_uncertain()

    def close_authority_connection(connection: socket.socket) -> bool:
        try:
            connection.close()
        except os_error:
            mark_authority_uncertain()
            return False
        return True

    def send_parent_attestation_response(
        connection: socket.socket,
        request: Mapping[str, object],
        *,
        status: str,
    ) -> bool:
        try:
            connection.sendall(
                canonical_json(
                    {
                        "challenge": request["challenge"],
                        "child_pid": request["child_pid"],
                        "domain": f"{domain}/parent-attestation",
                        "mode": request["mode"],
                        "packet_sha256": request["packet_sha256"],
                        "parent_pid": listener_owner_pid,
                        "status": status,
                        "target_node_id": request["target_node_id"],
                    }
                ).encode("ascii")
            )
        except (os_error, type_error, value_error):
            return False
        return True

    def acknowledge_registered_child(
        record: dict[str, object],
        process: subprocess.Popen[bytes],
    ) -> bool:
        state_lock_value = record.get("state_lock")
        attester = record.get("attest")
        commit = record.get("commit")
        deadline = record.get("deadline_ns")
        if (
            current_pid() != listener_owner_pid
            or not listener_is_exact()
            or not isinstance_value(state_lock_value, lock_type)
            or not callable(attester)
            or not callable(commit)
            or type_of(deadline) is not int_type
            or type_of(process.pid) is not int_type
            or process.pid <= 1
        ):
            return False
        state_lock = cast(AbstractContextManager[None], state_lock_value)
        deadline_ns = cast(int, deadline)
        handshake_deadline_ns = minimum_value(
            deadline_ns,
            monotonic_ns() + maximum_parent_handshake_lifetime_ns,
        )
        attempts = 0
        while attempts < 64 and monotonic_ns() < handshake_deadline_ns:
            connection: socket.socket | None = None
            expected_peer = False
            try:
                if not listener_is_exact():
                    return False
                authority_listener.settimeout(
                    minimum_value(
                        0.1,
                        max(
                            0.001,
                            (handshake_deadline_ns - monotonic_ns()) / 1_000_000_000,
                        ),
                    )
                )
                try:
                    connection, _ = authority_listener.accept()
                except socket_timeout_error:
                    if process.poll() is not None:
                        return False
                    continue
                attempts += 1
                peer_raw = connection.getsockopt(
                    socket_level,
                    socket_peer_credentials,
                    credentials_size,
                )
                peer_credentials = cast(
                    tuple[int, int, int],
                    unpack_credentials("3i", peer_raw),
                )
                peer_pid, peer_uid, _ = peer_credentials
                expected_peer = peer_pid == process.pid and peer_uid == current_uid()
                if not expected_peer:
                    continue
                connection.setsockopt(socket_level, socket_pass_credentials, 1)
                connection.settimeout(
                    minimum_value(
                        1.0,
                        max(
                            0.001,
                            (handshake_deadline_ns - monotonic_ns()) / 1_000_000_000,
                        ),
                    )
                )
                connection_details = stat_file(connection.fileno())
                raw_request, ancillary, message_flags, _ = connection.recvmsg(
                    maximum_envelope_bytes + 1,
                    credentials_space,
                )
                message_credentials: tuple[int, int, int] | None = None
                for level, message_type, raw_credentials in ancillary:
                    if (
                        level == socket_level
                        and message_type == socket_credentials_message
                        and length_of(raw_credentials) >= credentials_size
                    ):
                        message_credentials = cast(
                            tuple[int, int, int],
                            unpack_credentials("3i", raw_credentials[:credentials_size]),
                        )
                        break
                try:
                    request_text = raw_request.decode("ascii")
                    request = canonical_load(request_text)
                except (unicode_error, value_error, type_error):
                    request = None
                    request_text = ""
                request_is_canonical = bool_type(
                    type_of(request) is dict_type
                    and set_type(request)
                    == {
                        "challenge",
                        "child_pid",
                        "domain",
                        "mode",
                        "packet_sha256",
                        "parent_pid",
                        "target_node_id",
                    }
                    and canonical_json(request) == request_text
                    and request["domain"] == f"{domain}/parent-attestation"
                    and valid_hex(request["challenge"], 64)
                    and valid_hex(request["packet_sha256"], 64)
                    and type_of(request["child_pid"]) is int_type
                    and request["child_pid"] == peer_pid
                    and request["parent_pid"] == listener_owner_pid
                    and type_of(request["mode"]) is str_type
                    and type_of(request["target_node_id"]) is str_type
                    and message_flags == 0
                    and message_credentials == peer_credentials
                    and peer_uid == current_uid()
                    and not descriptor_is_inheritable(connection.fileno())
                    and stat.S_ISSOCK(connection_details.st_mode)
                    and connection_details.st_uid == current_uid()
                    and connection.getsockname() == authority_listener_address
                    and connection.getsockopt(socket_level, socket_type_option)
                    == socket_sequence_packet
                )
                request_is_registered = bool_type(
                    request_is_canonical
                    and cast(Mapping[str, object], request)["packet_sha256"]
                    == record.get("packet_digest")
                    and cast(Mapping[str, object], request)["target_node_id"]
                    == record.get("target_node_id")
                    and cast(Mapping[str, object], request)["mode"] == record.get("mode")
                )
                with state_lock:
                    if (
                        not request_is_registered
                        or record.get("state") != "SPAWNED"
                        or record.get("child_process_id") != process.pid
                        or record.get("process") is not process
                        or record.get("parent_process_id") != listener_owner_pid
                        or record.get("owner_thread_id") != current_thread_id()
                        or cast(int, record["deadline_ns"]) <= monotonic_ns()
                        or process.poll() is not None
                        or not listener_is_exact()
                        or handshake_deadline_ns <= monotonic_ns()
                        or not cast(Callable[[str, int, int], bool], attester)(
                            cast(str, cast(Mapping[str, object], request)["packet_sha256"]),
                            process.pid,
                            handshake_deadline_ns,
                        )
                    ):
                        if request_is_canonical:
                            send_parent_attestation_response(
                                connection,
                                cast(Mapping[str, object], request),
                                status="REJECTED",
                            )
                        record["state"] = "REJECTED"
                        return False
                    if handshake_deadline_ns <= monotonic_ns():
                        send_parent_attestation_response(
                            connection,
                            cast(Mapping[str, object], request),
                            status="REJECTED",
                        )
                        record["state"] = "REJECTED"
                        return False
                    if not cast(Callable[[str, int, int], bool], commit)(
                        cast(str, cast(Mapping[str, object], request)["packet_sha256"]),
                        process.pid,
                        handshake_deadline_ns,
                    ):
                        send_parent_attestation_response(
                            connection,
                            cast(Mapping[str, object], request),
                            status="REJECTED",
                        )
                        record["transport_error"] = "ACK_COMMIT_FAILED"
                        mark_authority_uncertain()
                        return False
                    record["state"] = "ATTESTED"
                    if not send_parent_attestation_response(
                        connection,
                        cast(Mapping[str, object], request),
                        status="ATTESTED",
                    ):
                        record["transport_error"] = "ACK_SEND_FAILED"
                        mark_authority_uncertain()
                        return False
                    if not close_authority_connection(connection):
                        connection = None
                        record["transport_error"] = "ACK_CLOSE_FAILED"
                        return False
                    connection = None
                    return True
            except (os_error, socket_timeout_error, unicode_error, type_error, value_error):
                if expected_peer:
                    return False
            finally:
                if (
                    connection is not None
                    and not close_authority_connection(connection)
                    and expected_peer
                ):
                    record["transport_error"] = "CONNECTION_CLOSE_FAILED"
        return False

    def issue(
        pytest_root: Path,
        protocol: str,
        issuer_node_id: str,
        target_node_id: str,
        mode: str,
        *,
        report_artifact_capability: _Task064PublishedReportArtifactCapability | None = None,
        report_deadline_ns: int | None = None,
    ) -> _Task064ChildProvenance:
        if (
            current_pid() != listener_owner_pid
            or not listener_is_exact()
            or length_of(provenance_records) >= maximum_active_tickets
            or length_of(launch_records) >= maximum_active_tickets
        ):
            invalid()
        legacy_environment, _, _ = validate_protocol_values(
            protocol,
            target_node_id,
            mode,
        )
        report_protocol = protocol == "report_close"
        if report_protocol:
            if (
                report_artifact_validator is None
                or report_source_fingerprints is None
                or resolve_published_report_artifact is None
                or type_of(report_artifact_capability) is not published_artifact_capability_type
                or type_of(report_deadline_ns) is not int_type
            ):
                invalid()
            exact_report_deadline_ns = cast(int, report_deadline_ns)
            if (
                not 0
                < exact_report_deadline_ns - monotonic_ns()
                <= (maximum_report_child_lifetime_ns)
            ):
                invalid()
        else:
            if report_artifact_capability is not None or report_deadline_ns is not None:
                invalid()
        if environment.get(envelope_environment) is not None or any_values(
            environment.get(name) is not None for name in legacy_environments
        ):
            invalid()
        resolved_root = validate_root(pytest_root)
        active = lookup_root(pytest_root)
        if (
            active is None
            or active.path_object is not pytest_root
            or not session_owns_root(active, False)
            or type_of(active.node_id) is not str_type
            or type_of(issuer_node_id) is not str_type
            or active.node_id != issuer_node_id
            or (
                protocol == "post_return_fixture_replay"
                and issuer_node_id != post_return_issuer_node_id
            )
            or (protocol != "post_return_fixture_replay" and issuer_node_id != target_node_id)
        ):
            invalid()
        try:
            root_details = pytest_root.lstat()
        except os_error:
            invalid()
        if (
            stat_is_link(root_details.st_mode)
            or not stat_is_directory(root_details.st_mode)
            or root_details.st_uid != current_uid()
            or stat_mode(root_details.st_mode) != 0o700
        ):
            invalid()
        if report_protocol:
            assert resolve_published_report_artifact is not None
            (
                exact_report_artifact,
                publication_binding,
                raw_bind_packet_authority,
            ) = resolve_published_report_artifact(
                report_artifact_capability,
                pytest_root=pytest_root,
                issuer_node_id=issuer_node_id,
                target_node_id=target_node_id,
                mode=mode,
                deadline_ns=report_deadline_ns,
            )
            if (
                type_of(exact_report_artifact) is not bytes_type
                or not 0 < length_of(exact_report_artifact) <= maximum_report_artifact_bytes
                or not isinstance_value(publication_binding, Mapping)
                or set_type(publication_binding) != set_type(publication_metadata_keys)
                or not callable(raw_bind_packet_authority)
            ):
                invalid()
            bind_packet_authority = cast(
                Callable[
                    [str],
                    tuple[
                        Callable[[int], bool],
                        Callable[[str, int, int], bool],
                        Callable[[str, int, int], bool],
                        Callable[[], None],
                    ],
                ],
                raw_bind_packet_authority,
            )
            assert report_artifact_validator is not None
            assert report_source_fingerprints is not None
            report_artifact_validator(exact_report_artifact)
            source_fingerprints: tuple[tuple[str, str], ...] | None = report_source_fingerprints()
        else:
            exact_report_artifact = b""
            bind_packet_authority = None
            publication_binding = MappingProxyType(
                {name: None for name in publication_metadata_keys}
            )
            source_fingerprints = None
        nonce = issue_nonce(32)
        if not valid_hex(nonce, 64):
            invalid()
        marker_name = f".task064-child-provenance-{nonce}.json"
        marker_path = pytest_root / marker_name
        root_descriptor = -1
        marker_descriptor = -1
        artifact_write_descriptor = -1
        artifact_descriptor = -1
        cancel_authority: Callable[[], None] | None = None
        artifact_name: str | None = None
        artifact_unlinked = False
        issued = False
        provenance: _Task064ChildProvenance | None = None
        try:
            root_descriptor = open_file(
                str_type(resolved_root),
                read_only | directory_only | no_follow | close_on_exec,
            )
            opened_root = stat_file(root_descriptor)
            if (
                not stat_is_directory(opened_root.st_mode)
                or opened_root.st_dev != root_details.st_dev
                or opened_root.st_ino != root_details.st_ino
                or opened_root.st_uid != root_details.st_uid
                or stat_mode(opened_root.st_mode) != 0o700
            ):
                invalid()
            artifact_packet: dict[str, object]
            if report_protocol:
                artifact_nonce = issue_nonce(32)
                if not valid_hex(artifact_nonce, 64):
                    invalid()
                artifact_name = f".task064-report-artifact-{artifact_nonce}.json"
                artifact_write_descriptor = open_file(
                    artifact_name,
                    write_only | create_exclusive | no_follow | close_on_exec,
                    0o600,
                    dir_fd=root_descriptor,
                )
                chmod_file(artifact_write_descriptor, 0o600)
                artifact_offset = 0
                while artifact_offset < length_of(exact_report_artifact):
                    written = write_file(
                        artifact_write_descriptor,
                        exact_report_artifact[artifact_offset:],
                    )
                    if type_of(written) is not int_type or written <= 0:
                        invalid()
                    artifact_offset += written
                sync_file(artifact_write_descriptor)
                written_artifact_details = stat_file(artifact_write_descriptor)
                if (
                    not stat_is_regular(written_artifact_details.st_mode)
                    or written_artifact_details.st_dev != root_details.st_dev
                    or written_artifact_details.st_uid != current_uid()
                    or written_artifact_details.st_nlink != 1
                    or stat_mode(written_artifact_details.st_mode) != 0o600
                    or written_artifact_details.st_size != length_of(exact_report_artifact)
                ):
                    invalid()
                descriptor_to_close = artifact_write_descriptor
                artifact_write_descriptor = -1
                try:
                    close_file(descriptor_to_close)
                except os_error:
                    mark_authority_uncertain()
                    invalid()
                artifact_descriptor = open_file(
                    artifact_name,
                    read_only | no_follow | close_on_exec,
                    dir_fd=root_descriptor,
                )
                artifact_details = stat_file(artifact_descriptor)
                if (
                    descriptor_flags(artifact_descriptor, get_file_status_flags) & access_mode_mask
                    != read_only
                    or artifact_details.st_dev != written_artifact_details.st_dev
                    or artifact_details.st_ino != written_artifact_details.st_ino
                    or artifact_details.st_uid != written_artifact_details.st_uid
                    or artifact_details.st_mode != written_artifact_details.st_mode
                    or artifact_details.st_size != written_artifact_details.st_size
                    or artifact_details.st_nlink != 1
                ):
                    invalid()
                unlink_file(artifact_name, dir_fd=root_descriptor)
                artifact_unlinked = True
                artifact_details = stat_file(artifact_descriptor)
                if artifact_details.st_nlink != 0:
                    invalid()
                artifact_packet = {
                    "artifact_descriptor": artifact_descriptor,
                    "artifact_device": artifact_details.st_dev,
                    "artifact_inode": artifact_details.st_ino,
                    "artifact_uid": artifact_details.st_uid,
                    "artifact_mode": stat_mode(artifact_details.st_mode),
                    "artifact_nlink": artifact_details.st_nlink,
                    "artifact_length": length_of(exact_report_artifact),
                    "artifact_sha256": digest_constructor(exact_report_artifact).hexdigest(),
                    "artifact_nonce": artifact_nonce,
                    "artifact_deadline_ns": report_deadline_ns,
                    "contract_generation": task_contract_generation,
                    "contract_digest": task_contract_digest,
                    "schema_fingerprint": schema_fingerprint_provider(),
                    "source_fingerprints": (
                        [list_type(item) for item in source_fingerprints]
                        if source_fingerprints is not None
                        else None
                    ),
                    **publication_binding,
                }
            else:
                artifact_packet = {
                    "artifact_descriptor": None,
                    "artifact_device": None,
                    "artifact_inode": None,
                    "artifact_uid": None,
                    "artifact_mode": None,
                    "artifact_nlink": None,
                    "artifact_length": None,
                    "artifact_sha256": None,
                    "artifact_nonce": None,
                    "artifact_deadline_ns": None,
                    "contract_generation": None,
                    "contract_digest": None,
                    "schema_fingerprint": None,
                    "source_fingerprints": None,
                    **publication_binding,
                }
            marker_descriptor = open_file(
                marker_name,
                read_write | create_exclusive | no_follow | close_on_exec,
                0o600,
                dir_fd=root_descriptor,
            )
            chmod_file(marker_descriptor, 0o600)
            marker_details = stat_file(marker_descriptor)
            if (
                not stat_is_regular(marker_details.st_mode)
                or marker_details.st_uid != current_uid()
                or marker_details.st_nlink != 1
                or stat_mode(marker_details.st_mode) != 0o600
            ):
                invalid()
            packet = {
                "domain": domain,
                "protocol": protocol,
                "legacy_environment": legacy_environment,
                "parent_pid": current_pid(),
                "issuer_node_id": active.node_id,
                "target_node_id": target_node_id,
                "mode": mode,
                "nonce": nonce,
                "root_path": str_type(pytest_root),
                "root_device": root_details.st_dev,
                "root_inode": root_details.st_ino,
                "root_uid": root_details.st_uid,
                "root_mode": stat_mode(root_details.st_mode),
                "marker_name": marker_name,
                "marker_device": marker_details.st_dev,
                "marker_inode": marker_details.st_ino,
                "marker_uid": marker_details.st_uid,
                "marker_mode": stat_mode(marker_details.st_mode),
                **artifact_packet,
            }
            packet_text = canonical_json(packet)
            packet_bytes = packet_text.encode("ascii")
            if not 0 < length_of(packet_bytes) <= maximum_packet_bytes:
                invalid()
            offset = 0
            while offset < length_of(packet_bytes):
                written = write_file(marker_descriptor, packet_bytes[offset:])
                if type_of(written) is not int_type or written <= 0:
                    invalid()
                offset += written
            sync_file(marker_descriptor)
            seek_file(marker_descriptor, 0, seek_start)
            packet_digest = digest_constructor(packet_bytes).hexdigest()
            launch_record: dict[str, object] | None = None
            if report_protocol:
                if bind_packet_authority is None or not listener_is_exact():
                    invalid()
                (
                    arm_authority,
                    attest_authority,
                    commit_authority,
                    cancel_authority,
                ) = bind_packet_authority(packet_digest)
                state_lock = lock_type()
                launch_record = {
                    "arm": arm_authority,
                    "attest": attest_authority,
                    "commit": commit_authority,
                    "cancel": cancel_authority,
                    "state_lock": state_lock,
                    "deadline_ns": cast(int, report_deadline_ns),
                    "parent_process_id": current_pid(),
                    "owner_thread_id": current_thread_id(),
                    "target_node_id": target_node_id,
                    "mode": mode,
                    "packet_digest": packet_digest,
                    "child_process_id": None,
                    "state": "ISSUED",
                }
            envelope = canonical_json(
                {
                    "domain": domain,
                    "descriptor": marker_descriptor,
                    "marker_path": str_type(marker_path),
                    "packet_sha256": packet_digest,
                }
            )
            if length_of(envelope.encode("ascii")) > maximum_envelope_bytes:
                invalid()
            provenance = provenance_type(
                envelope=envelope,
                descriptor=marker_descriptor,
                artifact_descriptor=artifact_descriptor,
                marker_path=marker_path,
                marker_device=marker_details.st_dev,
                marker_inode=marker_details.st_ino,
                root_path=pytest_root,
                root_device=root_details.st_dev,
                root_inode=root_details.st_ino,
                nonce=nonce,
            )
            provenance_record = {
                "provenance": provenance,
                "provenance_fields": provenance_fields(provenance),
                "owner_process_id": current_pid(),
                "owner_thread_id": current_thread_id(),
                "protocol": protocol,
                "state": "ISSUED",
            }
            if object_identity(provenance) in provenance_records:
                invalid()
            provenance_records[object_identity(provenance)] = provenance_record
            if launch_record is not None:
                launch_record["provenance"] = provenance
                launch_record["provenance_fields"] = (
                    provenance.envelope,
                    provenance.descriptor,
                    provenance.artifact_descriptor,
                    object_identity(provenance.marker_path),
                    provenance.marker_device,
                    provenance.marker_inode,
                    object_identity(provenance.root_path),
                    provenance.root_device,
                    provenance.root_inode,
                    provenance.nonce,
                )
                if object_identity(provenance) in launch_records:
                    invalid()
                launch_records[object_identity(provenance)] = launch_record
            descriptor_to_close = root_descriptor
            root_descriptor = -1
            try:
                close_file(descriptor_to_close)
            except os_error:
                mark_authority_uncertain()
                invalid()
            issued = True
            return provenance
        except failure_type:
            raise
        except (os_error, unicode_error, value_error):
            invalid()
        finally:
            if not issued and provenance is not None:
                launch_records.pop(object_identity(provenance), None)
                provenance_records.pop(object_identity(provenance), None)
            if not issued and cancel_authority is not None:
                with suppress_errors(base_exception_type):
                    cancel_authority()
            if not issued and artifact_descriptor >= 0:
                try:
                    close_file(artifact_descriptor)
                except os_error:
                    mark_authority_uncertain()
            if artifact_write_descriptor >= 0:
                descriptor_to_close = artifact_write_descriptor
                artifact_write_descriptor = -1
                try:
                    close_file(descriptor_to_close)
                except os_error:
                    mark_authority_uncertain()
            if not artifact_unlinked and artifact_name is not None and root_descriptor >= 0:
                try:
                    unlink_file(artifact_name, dir_fd=root_descriptor)
                except file_not_found_error:
                    pass
                except os_error:
                    mark_authority_uncertain()
            if not issued and marker_descriptor >= 0:
                try:
                    close_file(marker_descriptor)
                except os_error:
                    mark_authority_uncertain()
                try:
                    marker_path.unlink()
                except file_not_found_error:
                    pass
                except os_error:
                    mark_authority_uncertain()
            if root_descriptor >= 0:
                try:
                    close_file(root_descriptor)
                except os_error:
                    mark_authority_uncertain()
                    if marker_descriptor >= 0:
                        with suppress_errors(os_error):
                            close_file(marker_descriptor)
                    with suppress_errors(os_error):
                        marker_path.unlink()
                    invalid()

    def provenance_fields(provenance: _Task064ChildProvenance) -> tuple[object, ...]:
        return (
            provenance.envelope,
            provenance.descriptor,
            provenance.artifact_descriptor,
            object_identity(provenance.marker_path),
            provenance.marker_device,
            provenance.marker_inode,
            object_identity(provenance.root_path),
            provenance.root_device,
            provenance.root_inode,
            provenance.nonce,
        )

    def exact_launch_record(
        provenance: _Task064ChildProvenance,
    ) -> dict[str, object] | None:
        if type_of(provenance) is not provenance_type:
            return None
        record = launch_records.get(object_identity(provenance))
        if (
            record is None
            or record.get("provenance") is not provenance
            or record.get("provenance_fields") != provenance_fields(provenance)
        ):
            return None
        return record

    def spawn_authenticated_child(
        provenance: _Task064ChildProvenance,
        *,
        pycache_prefix: Path,
    ) -> subprocess.Popen[bytes]:
        record = exact_launch_record(provenance)
        if (
            record is None
            or record.get("state") != "ISSUED"
            or type_of(record.get("deadline_ns")) is not int_type
            or cast(int, record["deadline_ns"]) <= monotonic_ns()
            or current_pid() != listener_owner_pid
            or record.get("parent_process_id") != listener_owner_pid
            or record.get("owner_thread_id") != current_thread_id()
            or not listener_is_exact()
            or not isinstance_value(pycache_prefix, path_type)
            or not pycache_prefix.is_absolute()
            or pycache_prefix.parent != provenance.root_path
            or not pycache_prefix.name.startswith("task064-")
            or "/" in pycache_prefix.name
            or "\\" in pycache_prefix.name
            or pycache_prefix.exists()
            or not reserved_environment_is_absent()
        ):
            invalid()
        state_lock_value = record.get("state_lock")
        arm = record.get("arm")
        cancel = record.get("cancel")
        if (
            not isinstance_value(state_lock_value, lock_type)
            or not callable(arm)
            or not callable(cancel)
        ):
            invalid()
        state_lock = cast(AbstractContextManager[None], state_lock_value)
        child_environment = {
            name: value
            for name, value in environment.items()
            if not name.startswith(("PYTHON", "PYTEST", "LD_"))
            and name not in legacy_environments
            and name != envelope_environment
        }
        child_environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
        child_environment["PYTHONDONTWRITEBYTECODE"] = "1"
        child_environment["PYTHONPYCACHEPREFIX"] = str_type(pycache_prefix)
        child_environment[envelope_environment] = provenance.envelope
        process: subprocess.Popen[bytes] | None = None
        cleanup_failed = False
        with state_lock:
            if (
                record.get("state") != "ISSUED"
                or cast(int, record["deadline_ns"]) <= monotonic_ns()
                or not listener_is_exact()
            ):
                invalid()
            record["state"] = "SPAWNING"
        try:
            process = popen_constructor(
                (
                    python_executable,
                    "-m",
                    "pytest",
                    "-q",
                    "-p",
                    "no:cacheprovider",
                    cast(str, record["target_node_id"]),
                ),
                cwd=repository_root,
                env=child_environment,
                pass_fds=(
                    provenance.descriptor,
                    provenance.artifact_descriptor,
                ),
                stdout=subprocess_pipe,
                stderr=subprocess_pipe,
                start_new_session=True,
            )
            if type_of(process.pid) is not int_type or process.pid <= 1:
                invalid()
            with state_lock:
                if (
                    record.get("state") != "SPAWNING"
                    or cast(int, record["deadline_ns"]) <= monotonic_ns()
                    or not listener_is_exact()
                ):
                    invalid()
                record["child_process_id"] = process.pid
                if not cast(Callable[[int], bool], arm)(process.pid):
                    invalid()
                if (
                    record.get("state") != "SPAWNING"
                    or cast(int, record["deadline_ns"]) <= monotonic_ns()
                    or not listener_is_exact()
                ):
                    invalid()
                record["process"] = process
                record["state"] = "SPAWNED"
            if not acknowledge_registered_child(record, process):
                invalid()
            return process
        except base_exception_type:
            committed_failure = False
            with state_lock:
                if record.get("state") == "ATTESTED":
                    committed_failure = True
                    record.setdefault("transport_error", "POST_COMMIT_LAUNCH_FAILURE")
                else:
                    record["state"] = "CANCELLED"
            with suppress_errors(base_exception_type):
                cast(Callable[[], None], cancel)()
            if process is not None:
                try:
                    try:
                        kill_process_group(process.pid, terminate_signal)
                    except process_lookup_error:
                        pass
                    except base_exception_type:
                        cleanup_failed = True
                    try:
                        process.wait(timeout=2.0)
                    except timeout_expired_type:
                        try:
                            kill_process_group(process.pid, kill_signal)
                        except process_lookup_error:
                            pass
                        except base_exception_type:
                            cleanup_failed = True
                        process.wait(timeout=10.0)
                    else:
                        try:
                            kill_process_group(process.pid, kill_signal)
                        except process_lookup_error:
                            pass
                        except base_exception_type:
                            cleanup_failed = True
                except base_exception_type:
                    cleanup_failed = True
                for stream in (process.stdout, process.stderr):
                    if stream is not None:
                        try:
                            stream.close()
                        except base_exception_type:
                            cleanup_failed = True
            if committed_failure or cleanup_failed:
                mark_authority_uncertain()
            raise

    def probe_parent_attestation_binding(
        provenance: _Task064ChildProvenance,
        observed_packet_digest: str,
    ) -> bool:
        record = exact_launch_record(provenance)
        if record is None:
            return False
        state_lock_value = record.get("state_lock")
        if not isinstance_value(state_lock_value, lock_type):
            return False
        state_lock = cast(AbstractContextManager[None], state_lock_value)
        with state_lock:
            return bool_type(
                record.get("state") == "ISSUED"
                and type_of(record.get("deadline_ns")) is int_type
                and cast(int, record["deadline_ns"]) > monotonic_ns()
                and current_pid() == listener_owner_pid
                and record.get("parent_process_id") == listener_owner_pid
                and record.get("owner_thread_id") == current_thread_id()
                and valid_hex(observed_packet_digest, 64)
                and observed_packet_digest == record.get("packet_digest")
                and listener_is_exact()
            )

    def authenticate_report_parent_listener(
        packet: object,
        envelope: Mapping[str, object],
        target_node_id: str,
    ) -> None:
        """Require the real parent to attest the exact packet before artifact access."""

        real_parent_pid = parent_pid()
        if type_of(packet) is not dict_type:
            invalid()
        exact_packet = cast(dict[str, object], packet)
        if (
            exact_packet.get("domain") != domain
            or exact_packet.get("protocol") != "report_close"
            or type_of(exact_packet.get("parent_pid")) is not int_type
            or exact_packet.get("parent_pid") != real_parent_pid
            or real_parent_pid <= 1
            or type_of(exact_packet.get("target_node_id")) is not str_type
            or exact_packet.get("target_node_id") != target_node_id
            or type_of(exact_packet.get("mode")) is not str_type
            or not valid_hex(envelope.get("packet_sha256"), 64)
        ):
            invalid()
        exact_packet_digest = cast(str, envelope["packet_sha256"])
        attestation_channel: socket.socket | None = None
        attestation_ok = False
        close_failed = False
        try:
            attestation_channel = socket_constructor(socket_family, socket_sequence_packet)
            attestation_channel.settimeout(maximum_parent_handshake_lifetime_ns / 1_000_000_000)
            channel_descriptor = attestation_channel.fileno()
            channel_details = stat_file(channel_descriptor)
            if (
                channel_descriptor <= 2
                or descriptor_is_inheritable(channel_descriptor)
                or not stat.S_ISSOCK(channel_details.st_mode)
                or channel_details.st_uid != current_uid()
                or attestation_channel.getsockopt(socket_level, socket_type_option)
                != socket_sequence_packet
            ):
                invalid()
            attestation_channel.connect(listener_address(real_parent_pid))
            peer_raw = attestation_channel.getsockopt(
                socket_level,
                socket_peer_credentials,
                credentials_size,
            )
            peer_pid, peer_uid, _ = unpack_credentials("3i", peer_raw)
            challenge = issue_nonce(32)
            if (
                peer_pid != real_parent_pid
                or peer_uid != current_uid()
                or not valid_hex(challenge, 64)
            ):
                invalid()
            request = canonical_json(
                {
                    "challenge": challenge,
                    "child_pid": current_pid(),
                    "domain": f"{domain}/parent-attestation",
                    "mode": exact_packet["mode"],
                    "packet_sha256": exact_packet_digest,
                    "parent_pid": real_parent_pid,
                    "target_node_id": target_node_id,
                }
            ).encode("ascii")
            attestation_channel.sendall(request)
            raw_response = attestation_channel.recv(maximum_envelope_bytes + 1)
            if not 0 < length_of(raw_response) <= maximum_envelope_bytes:
                invalid()
            response_text = raw_response.decode("ascii")
            response = canonical_load(response_text)
            if (
                type_of(response) is not dict_type
                or set_type(response)
                != {
                    "challenge",
                    "child_pid",
                    "domain",
                    "mode",
                    "packet_sha256",
                    "parent_pid",
                    "status",
                    "target_node_id",
                }
                or canonical_json(response) != response_text
                or response["domain"] != f"{domain}/parent-attestation"
                or response["challenge"] != challenge
                or response["packet_sha256"] != exact_packet_digest
                or response["child_pid"] != current_pid()
                or response["parent_pid"] != real_parent_pid
                or response["mode"] != exact_packet["mode"]
                or response["target_node_id"] != target_node_id
                or response["status"] != "ATTESTED"
                or attestation_channel.recv(1) != b""
            ):
                invalid()
            attestation_ok = True
        except failure_type:
            raise
        except (os_error, socket_timeout_error, unicode_error, type_error, value_error):
            invalid()
        finally:
            if attestation_channel is not None:
                try:
                    attestation_channel.close()
                except os_error:
                    close_failed = True
            if close_failed:
                mark_authority_uncertain()
        if not attestation_ok or close_failed:
            invalid()

    def authenticate_before_fixture(
        target_node_id: str,
    ) -> _Task064ChildProvenanceTicket | None:
        """Authenticate and consume any closed-policy packet before fixture authority starts."""

        if (
            type_of(target_node_id) is not str_type
            or not target_node_id.startswith("tests/")
            or "::" not in target_node_id
            or "\x00" in target_node_id
            or length_of(target_node_id) > 2_048
        ):
            invalid()
        raw_envelope = environment.get(envelope_environment)
        if raw_envelope is None:
            scrub_child_environment()
            return None
        if (
            type_of(raw_envelope) is not str_type
            or any_values(environment.get(name) is not None for name in legacy_environments)
            or active_body_ticket.get() is not None
            or length_of(ticket_records) >= maximum_active_tickets
        ):
            invalid()
        try:
            encoded_envelope = raw_envelope.encode("ascii")
            envelope = canonical_load(raw_envelope)
        except (type_error, unicode_error, value_error):
            invalid()
        if not 0 < length_of(encoded_envelope) <= maximum_envelope_bytes:
            invalid()
        if (
            type_of(envelope) is not dict_type
            or set_type(envelope) != {"domain", "descriptor", "marker_path", "packet_sha256"}
            or canonical_json(envelope) != raw_envelope
            or envelope["domain"] != domain
            or type_of(envelope["descriptor"]) is not int_type
            or envelope["descriptor"] <= 2
            or type_of(envelope["marker_path"]) is not str_type
            or not valid_hex(envelope["packet_sha256"], 64)
        ):
            invalid()
        descriptor = envelope["descriptor"]
        try:
            marker_details = stat_file(descriptor)
            seek_file(descriptor, 0, seek_start)
            fragments: list[bytes] = []
            packet_size = 0
            while True:
                fragment = read_file(
                    descriptor,
                    minimum_value(4_096, maximum_packet_bytes + 1 - packet_size),
                )
                if type_of(fragment) is not bytes_type:
                    invalid()
                if fragment == b"":
                    break
                fragments.append(fragment)
                packet_size += length_of(fragment)
                if packet_size > maximum_packet_bytes:
                    invalid()
            packet_bytes = b"".join(fragments)
            if (
                not packet_bytes
                or digest_constructor(packet_bytes).hexdigest() != envelope["packet_sha256"]
            ):
                invalid()
            packet_text = packet_bytes.decode("ascii")
            packet = canonical_load(packet_text)
        except failure_type:
            raise
        except (os_error, unicode_error, type_error, value_error):
            invalid()
        if (
            type_of(packet) is dict_type
            and cast(dict[str, object], packet).get("protocol") == "report_close"
        ):
            authenticate_report_parent_listener(packet, envelope, target_node_id)
        expected_packet_keys = {
            "domain",
            "protocol",
            "legacy_environment",
            "parent_pid",
            "issuer_node_id",
            "target_node_id",
            "mode",
            "nonce",
            "root_path",
            "root_device",
            "root_inode",
            "root_uid",
            "root_mode",
            "marker_name",
            "marker_device",
            "marker_inode",
            "marker_uid",
            "marker_mode",
            "artifact_descriptor",
            "artifact_device",
            "artifact_inode",
            "artifact_uid",
            "artifact_mode",
            "artifact_nlink",
            "artifact_length",
            "artifact_sha256",
            "artifact_nonce",
            "artifact_deadline_ns",
            "contract_generation",
            "contract_digest",
            "schema_fingerprint",
            "source_fingerprints",
            *publication_metadata_keys,
        }
        if (
            type_of(packet) is not dict_type
            or set_type(packet) != expected_packet_keys
            or canonical_json(packet) != packet_text
            or packet["domain"] != domain
        ):
            invalid()
        legacy_environment, _, _ = validate_protocol_values(
            packet["protocol"],
            packet["target_node_id"],
            packet["mode"],
        )
        protocol = str_type(packet["protocol"])
        expected_issuer_node_id = (
            post_return_issuer_node_id
            if protocol == "post_return_fixture_replay"
            else target_node_id
        )
        artifact_descriptor = -1
        artifact_bytes: bytes | None = None
        authenticated_source_fingerprints: tuple[tuple[str, str], ...] | None = None
        authenticated_publication_binding: Mapping[str, object] | None = None
        if protocol == "report_close":
            if (
                report_artifact_validator is None
                or report_source_fingerprints is None
                or type_of(packet["artifact_descriptor"]) is not int_type
                or packet["artifact_descriptor"] <= 2
                or packet["artifact_descriptor"] == descriptor
                or any_values(
                    type_of(packet[name]) is not int_type
                    for name in (
                        "artifact_device",
                        "artifact_inode",
                        "artifact_uid",
                        "artifact_mode",
                        "artifact_nlink",
                        "artifact_length",
                        "artifact_deadline_ns",
                        "contract_generation",
                    )
                )
                or packet["artifact_nlink"] != 0
                or not 0 < packet["artifact_length"] <= maximum_report_artifact_bytes
                or not 0
                < packet["artifact_deadline_ns"] - monotonic_ns()
                <= maximum_report_child_lifetime_ns
                or packet["contract_generation"] != task_contract_generation
                or packet["contract_digest"] != task_contract_digest
                or packet["schema_fingerprint"] != schema_fingerprint_provider()
                or not valid_hex(packet["artifact_sha256"], 64)
                or not valid_hex(packet["artifact_nonce"], 64)
                or any_values(
                    not valid_hex(packet[name], 64)
                    for name in (
                        "publication_nonce",
                        "publication_run_digest",
                        "publication_aggregate_digest",
                        "publication_report_object_digest",
                        "publication_context_digest",
                    )
                )
                or any_values(
                    type_of(packet[name]) is not int_type
                    for name in (
                        "publication_root_device",
                        "publication_root_inode",
                        "publication_root_uid",
                        "publication_root_mode",
                        "publication_device",
                        "publication_inode",
                        "publication_uid",
                        "publication_mode",
                        "publication_nlink",
                        "publication_process_id",
                        "publication_thread_id",
                        "publication_expires_ns",
                    )
                )
                or any_values(
                    type_of(packet[name]) is not str_type
                    for name in (
                        "publication_root_path",
                        "publication_path",
                        "publication_node_id",
                    )
                )
                or packet["publication_root_path"] != packet["root_path"]
                or packet["publication_path"]
                != str_type(path_type(packet["root_path"]) / "task064-evidence.json")
                or packet["publication_root_mode"] != 0o700
                or packet["publication_mode"] != 0o600
                or packet["publication_nlink"] != 1
                or packet["publication_process_id"] != packet["parent_pid"]
                or packet["publication_thread_id"] <= 0
                or packet["publication_node_id"] != packet["issuer_node_id"]
                or packet["publication_expires_ns"] < packet["artifact_deadline_ns"]
                or packet["publication_nonce"] in {packet["nonce"], packet["artifact_nonce"]}
                or packet["publication_nonce"] in consumed_nonces
                or type_of(packet["source_fingerprints"]) is not list_type
                or not packet["source_fingerprints"]
                or any_values(
                    type_of(item) is not list_type
                    or length_of(item) != 2
                    or type_of(item[0]) is not str_type
                    or not valid_hex(item[1], 64)
                    for item in packet["source_fingerprints"]
                )
            ):
                invalid()
            artifact_descriptor = cast(int, packet["artifact_descriptor"])
            authenticated_source_fingerprints = tuple_type(
                (str_type(item[0]), str_type(item[1])) for item in packet["source_fingerprints"]
            )
            if authenticated_source_fingerprints != report_source_fingerprints():
                invalid()
            try:
                artifact_details = stat_file(artifact_descriptor)
                if (
                    descriptor_flags(artifact_descriptor, get_file_status_flags) & access_mode_mask
                    != read_only
                    or not stat_is_regular(artifact_details.st_mode)
                    or artifact_details.st_dev != packet["artifact_device"]
                    or artifact_details.st_ino != packet["artifact_inode"]
                    or artifact_details.st_uid != packet["artifact_uid"]
                    or artifact_details.st_uid != current_uid()
                    or stat_mode(artifact_details.st_mode) != packet["artifact_mode"]
                    or packet["artifact_mode"] != 0o600
                    or artifact_details.st_nlink != packet["artifact_nlink"]
                    or artifact_details.st_size != packet["artifact_length"]
                ):
                    invalid()
                seek_file(artifact_descriptor, 0, seek_start)
                artifact_fragments: list[bytes] = []
                artifact_size = 0
                while artifact_size <= packet["artifact_length"]:
                    artifact_fragment = read_file(
                        artifact_descriptor,
                        minimum_value(
                            65_536,
                            packet["artifact_length"] + 1 - artifact_size,
                        ),
                    )
                    if type_of(artifact_fragment) is not bytes_type:
                        invalid()
                    if artifact_fragment == b"":
                        break
                    artifact_fragments.append(artifact_fragment)
                    artifact_size += length_of(artifact_fragment)
                artifact_bytes = b"".join(artifact_fragments)
            except failure_type:
                raise
            except (os_error, value_error):
                invalid()
            if (
                artifact_bytes is None
                or length_of(artifact_bytes) != packet["artifact_length"]
                or digest_constructor(artifact_bytes).hexdigest() != packet["artifact_sha256"]
            ):
                invalid()
            report_artifact_validator(artifact_bytes)
            authenticated_publication_binding = MappingProxyType(
                {name: packet[name] for name in publication_metadata_keys}
            )
            if not published_origin_matches(
                authenticated_publication_binding,
                artifact_bytes,
            ):
                invalid()
        elif any_values(
            packet[name] is not None
            for name in (*artifact_metadata_keys, *publication_metadata_keys)
        ):
            invalid()
        if (
            packet["target_node_id"] != target_node_id
            or packet["legacy_environment"] != legacy_environment
            or type_of(packet["parent_pid"]) is not int_type
            or packet["parent_pid"] <= 1
            or parent_pid() != packet["parent_pid"]
            or type_of(packet["issuer_node_id"]) is not str_type
            or packet["issuer_node_id"] != expected_issuer_node_id
            or (
                packet["protocol"] == "post_return_fixture_replay"
                and expected_issuer_node_id != post_return_issuer_node_id
            )
            or (
                packet["protocol"] != "post_return_fixture_replay"
                and expected_issuer_node_id != target_node_id
            )
            or not valid_hex(packet["nonce"], 64)
            or packet["nonce"] in consumed_nonces
            or (
                protocol == "report_close"
                and (
                    packet["artifact_nonce"] == packet["nonce"]
                    or packet["artifact_nonce"] in consumed_nonces
                )
            )
            or type_of(packet["root_path"]) is not str_type
            or type_of(packet["marker_name"]) is not str_type
            or packet["marker_name"] != f".task064-child-provenance-{packet['nonce']}.json"
            or envelope["marker_path"]
            != str_type(path_type(packet["root_path"]) / packet["marker_name"])
            or any_values(
                type_of(packet[name]) is not int_type
                for name in (
                    "root_device",
                    "root_inode",
                    "root_uid",
                    "root_mode",
                    "marker_device",
                    "marker_inode",
                    "marker_uid",
                    "marker_mode",
                )
            )
        ):
            invalid()
        root_path = path_type(packet["root_path"])
        marker_path = path_type(envelope["marker_path"])
        publication_path = (
            path_type(cast(str, packet["publication_path"])) if protocol == "report_close" else None
        )
        try:
            root_details = root_path.lstat()
            path_marker_details = marker_path.lstat()
            publication_details = publication_path.lstat() if publication_path is not None else None
        except os_error:
            invalid()
        if (
            stat_is_link(root_details.st_mode)
            or not stat_is_directory(root_details.st_mode)
            or root_details.st_dev != packet["root_device"]
            or root_details.st_ino != packet["root_inode"]
            or root_details.st_uid != packet["root_uid"]
            or packet["root_uid"] != current_uid()
            or stat_mode(root_details.st_mode) != packet["root_mode"]
            or packet["root_mode"] != 0o700
            or stat_is_link(path_marker_details.st_mode)
            or not stat_is_regular(path_marker_details.st_mode)
            or marker_details.st_dev != packet["marker_device"]
            or marker_details.st_ino != packet["marker_inode"]
            or marker_details.st_uid != packet["marker_uid"]
            or packet["marker_uid"] != current_uid()
            or stat_mode(marker_details.st_mode) != packet["marker_mode"]
            or packet["marker_mode"] != 0o600
            or marker_details.st_nlink != 1
            or path_marker_details.st_dev != marker_details.st_dev
            or path_marker_details.st_ino != marker_details.st_ino
            or (
                protocol == "report_close"
                and (
                    publication_details is None
                    or publication_path is None
                    or publication_path.parent != root_path
                    or stat_is_link(publication_details.st_mode)
                    or not stat_is_regular(publication_details.st_mode)
                    or publication_details.st_dev != packet["publication_device"]
                    or publication_details.st_ino != packet["publication_inode"]
                    or publication_details.st_uid != packet["publication_uid"]
                    or publication_details.st_uid != current_uid()
                    or stat_mode(publication_details.st_mode) != packet["publication_mode"]
                    or publication_details.st_nlink != packet["publication_nlink"]
                    or publication_details.st_size != packet["artifact_length"]
                    or root_details.st_dev != packet["publication_root_device"]
                    or root_details.st_ino != packet["publication_root_inode"]
                    or root_details.st_uid != packet["publication_root_uid"]
                    or stat_mode(root_details.st_mode) != packet["publication_root_mode"]
                )
            )
        ):
            invalid()
        if length_of(consumed_nonces) >= maximum_consumed_nonces:
            invalid()
        root_descriptor = -1
        consumption_irreversible = False
        consumption_failed = False
        root_close_failed = False
        try:
            root_descriptor = open_file(
                str_type(root_path),
                read_only | directory_only | no_follow | close_on_exec,
            )
            opened_root = stat_file(root_descriptor)
            if (
                opened_root.st_dev != root_details.st_dev
                or opened_root.st_ino != root_details.st_ino
                or opened_root.st_uid != root_details.st_uid
                or stat_mode(opened_root.st_mode) != 0o700
            ):
                invalid()
            unlink_file(packet["marker_name"], dir_fd=root_descriptor)
            consumption_irreversible = True
            if artifact_descriptor >= 0:
                descriptor_to_close = artifact_descriptor
                artifact_descriptor = -1
                close_file(descriptor_to_close)
            close_file(descriptor)
        except failure_type:
            consumption_failed = True
        except os_error:
            consumption_failed = True
        finally:
            if root_descriptor >= 0:
                try:
                    close_file(root_descriptor)
                except os_error:
                    root_close_failed = True
        if consumption_irreversible:
            consumed_nonces.add(packet["nonce"])
            if protocol == "report_close":
                consumed_nonces.add(cast(str, packet["artifact_nonce"]))
                consumed_nonces.add(cast(str, packet["publication_nonce"]))
            scrub_child_environment()
        if consumption_failed or root_close_failed:
            if consumption_irreversible or root_close_failed:
                mark_authority_uncertain()
            invalid()
        if not consumption_irreversible:
            invalid()
        ticket: _Task064ChildProvenanceTicket | None = None
        ticket_key = -1
        try:
            ticket = ticket_type(_authority_nonce=issue_ticket_nonce(32))
            if (
                type_of(ticket._authority_nonce) is not bytes_type
                or length_of(ticket._authority_nonce) != 32
            ):
                invalid()
            ticket_key = object_identity(ticket)
            ticket_records[ticket_key] = {
                "ticket": ticket,
                "ticket_fields": (
                    object_identity(ticket._authority_nonce),
                    ticket._authority_nonce,
                ),
                "owner_process_id": current_pid(),
                "node_id": target_node_id,
                "protocol": protocol,
                "issuer_node_id": expected_issuer_node_id,
                "mode": str_type(packet["mode"]),
                "parent_process_id": packet["parent_pid"],
                "provenance_nonce": packet["nonce"],
                "report_artifact": artifact_bytes,
                "report_artifact_digest": packet["artifact_sha256"],
                "report_artifact_length": packet["artifact_length"],
                "report_artifact_nonce": packet["artifact_nonce"],
                "report_deadline_ns": packet["artifact_deadline_ns"],
                "report_contract_generation": packet["contract_generation"],
                "report_contract_digest": packet["contract_digest"],
                "report_schema_fingerprint": packet["schema_fingerprint"],
                "report_source_fingerprints": authenticated_source_fingerprints,
                "report_publication_binding": authenticated_publication_binding,
                "report_artifact_state": ("AUTHENTICATED" if protocol == "report_close" else None),
                "provenance_root": root_path,
                "provenance_root_fields": (
                    root_details.st_dev,
                    root_details.st_ino,
                    root_details.st_uid,
                    stat_mode(root_details.st_mode),
                ),
                "state": "AUTHENTICATED",
            }
        except base_exception_type:
            if ticket_key >= 0:
                ticket_records.pop(ticket_key, None)
            mark_authority_uncertain()
            invalid()
        return ticket

    def reserved_environment_is_absent() -> bool:
        return bool_type(
            environment.get(envelope_environment) is None
            and not any_values(environment.get(name) is not None for name in legacy_environments)
        )

    def exact_ticket_record(
        ticket: _Task064ChildProvenanceTicket,
        target_node_id: str,
    ) -> dict[str, object] | None:
        record = ticket_records.get(object_identity(ticket))
        if (
            type_of(ticket) is not ticket_type
            or type_of(target_node_id) is not str_type
            or record is None
            or record["ticket"] is not ticket
            or record["ticket_fields"]
            != (object_identity(ticket._authority_nonce), ticket._authority_nonce)
            or record["owner_process_id"] != current_pid()
            or record["node_id"] != target_node_id
        ):
            return None
        return record

    def roots_are_exact(record: dict[str, object], *, require_active: bool) -> bool:
        provenance_root = record.get("provenance_root")
        fixture_root = record.get("fixture_root")
        if not isinstance(provenance_root, path_type) or not isinstance(
            fixture_root,
            path_type,
        ):
            return False
        try:
            provenance_details = provenance_root.lstat()
            fixture_details = fixture_root.lstat()
        except os_error:
            return False
        if (
            stat_is_link(provenance_details.st_mode)
            or not stat_is_directory(provenance_details.st_mode)
            or (
                provenance_details.st_dev,
                provenance_details.st_ino,
                provenance_details.st_uid,
                stat_mode(provenance_details.st_mode),
            )
            != record.get("provenance_root_fields")
            or stat_is_link(fixture_details.st_mode)
            or not stat_is_directory(fixture_details.st_mode)
            or (
                object_identity(fixture_root),
                fixture_details.st_dev,
                fixture_details.st_ino,
                fixture_details.st_uid,
                stat_mode(fixture_details.st_mode),
            )
            != record.get("fixture_root_fields")
            or fixture_details.st_uid != current_uid()
        ):
            return False
        if not require_active:
            return True
        active_root = lookup_root(fixture_root)
        return bool_type(
            active_root is not None
            and active_root.path_object is fixture_root
            and active_root.node_id == record["node_id"]
            and validate_root_identity(active_root)
            and session_owns_root(active_root, False)
        )

    def activate_fixture_ticket(
        ticket: _Task064ChildProvenanceTicket | None,
        target_node_id: str,
        fixture_root: Path,
    ) -> None:
        """Bind an authenticated ticket to the exact originating fixture context."""

        if ticket is None:
            return
        record = exact_ticket_record(ticket, target_node_id)
        if (
            record is None
            or record["state"] != "AUTHENTICATED"
            or not isinstance_value(fixture_root, path_type)
            or not fixture_root.is_absolute()
            or active_body_ticket.get() is not None
            or not reserved_environment_is_absent()
        ):
            invalid()
        try:
            fixture_details = fixture_root.lstat()
        except os_error:
            invalid()
        if (
            stat_is_link(fixture_details.st_mode)
            or not stat_is_directory(fixture_details.st_mode)
            or fixture_details.st_uid != current_uid()
        ):
            invalid()
        context_token = active_body_ticket.set(ticket)
        if type_of(context_token) is not context_token_type:
            mark_authority_uncertain()
            invalid()
        record["fixture_root"] = fixture_root
        record["fixture_root_fields"] = (
            object_identity(fixture_root),
            fixture_details.st_dev,
            fixture_details.st_ino,
            fixture_details.st_uid,
            stat_mode(fixture_details.st_mode),
        )
        record["context_token"] = context_token
        record["state"] = "ACTIVATED"

    def transition_to_claimed(
        record: dict[str, object],
        ticket: _Task064ChildProvenanceTicket,
    ) -> None:
        context_token = record.get("context_token")
        if (
            type_of(context_token) is not context_token_type
            or active_body_ticket.get() is not ticket
        ):
            invalid()
        try:
            active_body_ticket.reset(context_token)  # type: ignore[arg-type]
        except base_exception_type:
            invalid()
        try:
            finalize_token = active_body_ticket.set(ticket)
        except base_exception_type:
            mark_authority_uncertain()
            invalid()
        if type_of(finalize_token) is not context_token_type:
            mark_authority_uncertain()
            invalid()
        record["context_token"] = finalize_token
        record["state"] = "CLAIMED"

    def ticket_context_is_exact(
        record: dict[str, object],
        ticket: _Task064ChildProvenanceTicket,
    ) -> bool:
        context_token = record.get("context_token")
        if (
            type_of(context_token) is not context_token_type
            or active_body_ticket.get() is not ticket
        ):
            return False
        try:
            active_body_ticket.reset(
                cast(Token[_Task064ChildProvenanceTicket | None], context_token)
            )
            rotated_token = active_body_ticket.set(ticket)
        except base_exception_type:
            return False
        if type_of(rotated_token) is not context_token_type:
            return False
        record["context_token"] = rotated_token
        return True

    def claim_post_return(
        ticket: _Task064ChildProvenanceTicket | None,
        target_node_id: str,
    ) -> bool:
        """Claim a post-return packet locally without exposing its mode to test code."""

        if ticket is None:
            return False
        record = exact_ticket_record(ticket, target_node_id)
        if (
            record is None
            or record["state"] != "ACTIVATED"
            or not reserved_environment_is_absent()
            or not roots_are_exact(record, require_active=False)
        ):
            invalid()
        if record["protocol"] != "post_return_fixture_replay":
            if active_body_ticket.get() is not ticket:
                invalid()
            return False
        if (
            record["issuer_node_id"] != post_return_issuer_node_id
            or record["mode"] != target_node_id
        ):
            invalid()
        transition_to_claimed(record, ticket)
        return True

    def ticket_is_report_close(
        ticket: _Task064ChildProvenanceTicket | None,
        target_node_id: str,
    ) -> bool:
        if ticket is None:
            return False
        record = exact_ticket_record(ticket, target_node_id)
        if (
            record is None
            or record["state"] != "ACTIVATED"
            or active_body_ticket.get() is not ticket
            or not reserved_environment_is_absent()
            or not roots_are_exact(record, require_active=False)
        ):
            invalid()
        return bool_type(
            record["protocol"] == "report_close"
            and record["issuer_node_id"] == target_node_id
            and record["report_artifact_state"] == "AUTHENTICATED"
        )

    def claim_body_dispatch(
        protocol: str,
        target_node_id: str,
        allowed_modes: tuple[str, ...],
    ) -> str | None:
        """Claim one authenticated body ticket without consulting process environment."""

        if (
            type_of(protocol) is not str_type
            or type_of(target_node_id) is not str_type
            or type_of(allowed_modes) is not tuple_type
            or any_values(type_of(item) is not str_type for item in allowed_modes)
        ):
            invalid()
        _, fixed_modes, mode_is_node = policy_for(protocol)
        exact_modes = (target_node_id,) if mode_is_node else fixed_modes
        if allowed_modes != exact_modes:
            invalid()
        ticket = active_body_ticket.get()
        if ticket is None:
            if environment.get(envelope_environment) is not None:
                invalid()
            if any_values(environment.get(name) is not None for name in legacy_environments):
                scrub_child_environment()
            if any_values(
                record["owner_process_id"] == current_pid() and record["node_id"] == target_node_id
                for record in ticket_records.values()
            ):
                invalid()
            return None
        if not reserved_environment_is_absent():
            scrub_child_environment()
            invalid()
        record = ticket_records.get(object_identity(ticket))
        if (
            type_of(ticket) is not ticket_type
            or record is None
            or record["ticket"] is not ticket
            or record["ticket_fields"]
            != (object_identity(ticket._authority_nonce), ticket._authority_nonce)
            or record["owner_process_id"] != current_pid()
            or record["node_id"] != target_node_id
            or record["protocol"] != protocol
            or record["mode"] not in exact_modes
            or record["issuer_node_id"] != target_node_id
            or record["state"] != "ACTIVATED"
            or not roots_are_exact(record, require_active=True)
        ):
            invalid()
        transition_to_claimed(record, ticket)
        return str_type(record["mode"])

    def bind_report_artifact_validation(
        validator: Callable[[bytes], None],
        fingerprint_provider: Callable[[], tuple[tuple[str, str], ...]],
        capability_resolver: Callable[
            ...,
            tuple[bytes, Mapping[str, object], Callable[..., object]],
        ],
    ) -> None:
        nonlocal report_artifact_validator
        nonlocal report_source_fingerprints
        nonlocal resolve_published_report_artifact

        if (
            report_artifact_validator is not None
            or report_source_fingerprints is not None
            or resolve_published_report_artifact is not None
            or not callable(validator)
            or not callable(fingerprint_provider)
            or not callable(capability_resolver)
        ):
            invalid()
        report_artifact_validator = validator
        report_source_fingerprints = fingerprint_provider
        resolve_published_report_artifact = capability_resolver

    def exact_report_permit_record(
        permit: _Task064ReportPublicationPermit,
    ) -> dict[str, object] | None:
        if type_of(permit) is not report_permit_type:
            return None
        record = report_permit_records.get(object_identity(permit))
        if (
            record is None
            or record["permit"] is not permit
            or record["permit_fields"]
            != (object_identity(permit._authority_nonce), permit._authority_nonce)
            or record["child_process_id"] != current_pid()
            or record["child_thread_id"] != current_thread_id()
        ):
            return None
        return record

    def terminalize_report_permit_record(
        record: dict[str, object],
        terminal_state: str,
    ) -> bool:
        permit = record.get("permit")
        if type_of(permit) is not report_permit_type:
            mark_authority_uncertain()
            return False
        exact_permit = cast(_Task064ReportPublicationPermit, permit)
        permit_key = object_identity(exact_permit)
        if (
            report_permit_records.get(permit_key) is not record
            or record.get("state") not in {"ACTIVE", "PREPARED"}
            or terminal_state not in {"CONSUMED", "TERMINALIZED"}
            or length_of(terminal_report_permit_records) >= maximum_report_permits
        ):
            mark_authority_uncertain()
            return False
        ticket = record.get("ticket")
        ticket_record = (
            ticket_records.get(object_identity(ticket)) if type_of(ticket) is ticket_type else None
        )
        ticket_is_exact = bool_type(
            type_of(ticket) is ticket_type
            and ticket_record is not None
            and ticket_record.get("ticket") is ticket
            and ticket_record.get("state") == "CLAIMED"
            and ticket_record.get("report_artifact_state") == "PERMIT_ISSUED"
            and active_body_ticket.get() is ticket
        )
        record["state"] = terminal_state
        if ticket_is_exact:
            cast(dict[str, object], ticket_record)["report_artifact_state"] = terminal_state
        else:
            mark_authority_uncertain()
        terminal_report_permit_records[permit_key] = (
            exact_permit,
            current_pid(),
            terminal_state,
        )
        del report_permit_records[permit_key]
        return ticket_is_exact

    def report_permit_is_valid(
        permit: _Task064ReportPublicationPermit,
        publication_root: Path,
        artifact: bytes,
        *,
        terminalize_invalid: bool = True,
    ) -> bool:
        if type_of(terminalize_invalid) is not bool_type:
            return False
        record = exact_report_permit_record(permit)
        if record is None:
            return False
        ticket = record.get("ticket")
        ticket_record = (
            ticket_records.get(object_identity(ticket)) if type_of(ticket) is ticket_type else None
        )
        if (
            type_of(ticket) is ticket_type
            and ticket_record is not None
            and ticket_record.get("ticket") is ticket
            and not ticket_context_is_exact(
                ticket_record,
                cast(_Task064ChildProvenanceTicket, ticket),
            )
        ):
            return False
        try:
            source_fingerprints = (
                report_source_fingerprints() if report_source_fingerprints is not None else None
            )
            schema_fingerprint = schema_fingerprint_provider()
            deadline_ns = record.get("deadline_ns")
            publication_binding = record.get("publication_binding")
            bindings_are_exact = bool_type(
                record.get("state") in {"ACTIVE", "PREPARED"}
                and reserved_environment_is_absent()
                and isinstance_value(publication_root, path_type)
                and publication_root is record.get("publication_root")
                and type_of(artifact) is bytes_type
                and artifact is record.get("report_artifact")
                and type_of(deadline_ns) is int_type
                and 0 < cast(int, deadline_ns) - monotonic_ns() <= maximum_report_child_lifetime_ns
                and report_artifact_validator is not None
                and report_source_fingerprints is not None
                and record.get("source_fingerprints") == source_fingerprints
                and record.get("contract_generation") == task_contract_generation
                and record.get("contract_digest") == task_contract_digest
                and record.get("schema_fingerprint") == schema_fingerprint
                and record.get("report_digest") == digest_constructor(artifact).hexdigest()
                and record.get("report_length") == length_of(artifact)
                and isinstance_value(publication_binding, Mapping)
                and set_type(cast(Mapping[str, object], publication_binding))
                == set_type(publication_metadata_keys)
                and cast(Mapping[str, object], publication_binding).get("publication_process_id")
                == parent_pid()
                and cast(Mapping[str, object], publication_binding).get("publication_node_id")
                == record.get("issuer_node_id")
                and cast(
                    int,
                    cast(Mapping[str, object], publication_binding).get("publication_expires_ns"),
                )
                >= cast(int, deadline_ns)
                and type_of(ticket) is ticket_type
                and ticket_record is not None
                and ticket_record.get("ticket") is ticket
                and ticket_record.get("ticket_fields")
                == (
                    object_identity(cast(_Task064ChildProvenanceTicket, ticket)._authority_nonce),
                    cast(_Task064ChildProvenanceTicket, ticket)._authority_nonce,
                )
                and ticket_record.get("owner_process_id") == current_pid()
                and ticket_record.get("node_id") == record.get("target_node_id")
                and ticket_record.get("protocol") == record.get("protocol")
                and record.get("protocol") == "report_close"
                and ticket_record.get("mode") == record.get("mode")
                and ticket_record.get("issuer_node_id") == record.get("issuer_node_id")
                and record.get("issuer_node_id") == record.get("target_node_id")
                and ticket_record.get("parent_process_id") == record.get("parent_process_id")
                and record.get("parent_process_id") == parent_pid()
                and record.get("child_process_id") == current_pid()
                and record.get("child_thread_id") == current_thread_id()
                and ticket_record.get("provenance_nonce") == record.get("provenance_nonce")
                and ticket_record.get("report_artifact_nonce") == record.get("artifact_nonce")
                and ticket_record.get("report_artifact") is artifact
                and ticket_record.get("report_artifact_digest") == record.get("report_digest")
                and ticket_record.get("report_artifact_length") == record.get("report_length")
                and ticket_record.get("report_deadline_ns") == deadline_ns
                and ticket_record.get("report_contract_generation")
                == record.get("contract_generation")
                and ticket_record.get("report_contract_digest") == record.get("contract_digest")
                and ticket_record.get("report_schema_fingerprint")
                == record.get("schema_fingerprint")
                and ticket_record.get("report_source_fingerprints")
                == record.get("source_fingerprints")
                and ticket_record.get("report_publication_binding") is publication_binding
                and ticket_record.get("report_artifact_state") == "PERMIT_ISSUED"
                and ticket_record.get("state") == "CLAIMED"
                and active_body_ticket.get() is ticket
                and roots_are_exact(ticket_record, require_active=True)
            )
        except (failure_type, os_error, value_error, type_error):
            bindings_are_exact = False
        if not bindings_are_exact:
            if terminalize_invalid:
                terminalize_report_permit_record(record, "TERMINALIZED")
            return False
        try:
            root_details = publication_root.lstat()
            exact_publication_binding = cast(Mapping[str, object], publication_binding)
            origin_root = path_type(cast(str, exact_publication_binding["publication_root_path"]))
            origin_path = path_type(cast(str, exact_publication_binding["publication_path"]))
            origin_root_details = origin_root.lstat()
            origin_details = origin_path.lstat()
        except os_error:
            if terminalize_invalid:
                terminalize_report_permit_record(record, "TERMINALIZED")
            return False
        active_root = lookup_root(publication_root)
        if (
            active_root is None
            or active_root.path_object is not publication_root
            or active_root.node_id != record["target_node_id"]
            or not validate_root_identity(active_root)
            or not session_owns_root(active_root, False)
            or (
                object_identity(publication_root),
                root_details.st_dev,
                root_details.st_ino,
                root_details.st_uid,
                stat_mode(root_details.st_mode),
            )
            != record["publication_root_fields"]
            or origin_path.parent != origin_root
            or stat_is_link(origin_root_details.st_mode)
            or not stat_is_directory(origin_root_details.st_mode)
            or origin_root_details.st_dev != exact_publication_binding["publication_root_device"]
            or origin_root_details.st_ino != exact_publication_binding["publication_root_inode"]
            or origin_root_details.st_uid != exact_publication_binding["publication_root_uid"]
            or stat_mode(origin_root_details.st_mode)
            != exact_publication_binding["publication_root_mode"]
            or stat_is_link(origin_details.st_mode)
            or not stat_is_regular(origin_details.st_mode)
            or origin_details.st_dev != exact_publication_binding["publication_device"]
            or origin_details.st_ino != exact_publication_binding["publication_inode"]
            or origin_details.st_uid != exact_publication_binding["publication_uid"]
            or stat_mode(origin_details.st_mode) != exact_publication_binding["publication_mode"]
            or origin_details.st_nlink != exact_publication_binding["publication_nlink"]
            or origin_details.st_size != record["report_length"]
        ):
            if terminalize_invalid:
                terminalize_report_permit_record(record, "TERMINALIZED")
            return False
        try:
            assert report_artifact_validator is not None
            report_artifact_validator(artifact)
        except failure_type:
            if terminalize_invalid:
                terminalize_report_permit_record(record, "TERMINALIZED")
            return False
        if not published_origin_matches(exact_publication_binding, artifact):
            if terminalize_invalid:
                terminalize_report_permit_record(record, "TERMINALIZED")
            return False
        return True

    def claim_report_publication(
        target_node_id: str,
        publication_root: Path,
    ) -> tuple[bytes, _Task064ReportPublicationPermit]:
        ticket = active_body_ticket.get()
        if type_of(ticket) is not ticket_type:
            invalid()
        exact_ticket = cast(_Task064ChildProvenanceTicket, ticket)
        record = exact_ticket_record(exact_ticket, target_node_id)
        if (
            record is None
            or record["state"] != "CLAIMED"
            or record["protocol"] != "report_close"
            or record["report_artifact_state"] != "AUTHENTICATED"
            or not roots_are_exact(record, require_active=True)
            or not reserved_environment_is_absent()
            or report_artifact_validator is None
            or report_source_fingerprints is None
            or length_of(report_permit_records) >= maximum_report_permits
            or length_of(terminal_report_permit_records) >= maximum_report_permits
        ):
            invalid()
        raw_value = record["report_artifact"]
        deadline_value = record["report_deadline_ns"]
        publication_binding_value = record["report_publication_binding"]
        if (
            type_of(raw_value) is not bytes_type
            or type_of(deadline_value) is not int_type
            or not isinstance_value(publication_binding_value, Mapping)
            or set_type(cast(Mapping[str, object], publication_binding_value))
            != set_type(publication_metadata_keys)
        ):
            invalid()
        raw = cast(bytes, raw_value)
        exact_deadline_ns = cast(int, deadline_value)
        if (
            not 0 < exact_deadline_ns - monotonic_ns() <= maximum_report_child_lifetime_ns
            or record["report_source_fingerprints"] != report_source_fingerprints()
            or record["report_contract_generation"] != task_contract_generation
            or record["report_contract_digest"] != task_contract_digest
            or record["report_schema_fingerprint"] != schema_fingerprint_provider()
            or record["report_artifact_digest"] != digest_constructor(raw).hexdigest()
            or record["report_artifact_length"] != length_of(raw)
            or cast(Mapping[str, object], publication_binding_value)["publication_process_id"]
            != parent_pid()
            or cast(Mapping[str, object], publication_binding_value)["publication_node_id"]
            != target_node_id
            or cast(
                int,
                cast(Mapping[str, object], publication_binding_value)["publication_expires_ns"],
            )
            < exact_deadline_ns
        ):
            invalid()
        report_artifact_validator(raw)
        resolved_root = validate_root(publication_root)
        active_root = lookup_root(publication_root)
        if (
            active_root is None
            or active_root.path_object is not publication_root
            or active_root.node_id != target_node_id
            or not validate_root_identity(active_root)
            or not session_owns_root(active_root, False)
        ):
            invalid()
        try:
            root_details = resolved_root.lstat()
        except os_error:
            invalid()
        permit = report_permit_type(_authority_nonce=issue_ticket_nonce(32))
        if (
            type_of(permit._authority_nonce) is not bytes_type
            or length_of(permit._authority_nonce) != 32
            or object_identity(permit) in report_permit_records
        ):
            invalid()
        permit_record: dict[str, object] = {
            "permit": permit,
            "permit_fields": (
                object_identity(permit._authority_nonce),
                permit._authority_nonce,
            ),
            "ticket": ticket,
            "protocol": record["protocol"],
            "mode": record["mode"],
            "issuer_node_id": record["issuer_node_id"],
            "target_node_id": record["node_id"],
            "parent_process_id": record["parent_process_id"],
            "child_process_id": current_pid(),
            "child_thread_id": current_thread_id(),
            "provenance_nonce": record["provenance_nonce"],
            "artifact_nonce": record["report_artifact_nonce"],
            "publication_root": publication_root,
            "publication_root_fields": (
                object_identity(publication_root),
                root_details.st_dev,
                root_details.st_ino,
                root_details.st_uid,
                stat_mode(root_details.st_mode),
            ),
            "source_fingerprints": record["report_source_fingerprints"],
            "contract_generation": record["report_contract_generation"],
            "contract_digest": record["report_contract_digest"],
            "schema_fingerprint": record["report_schema_fingerprint"],
            "publication_binding": record["report_publication_binding"],
            "report_artifact": raw,
            "report_digest": record["report_artifact_digest"],
            "report_length": record["report_artifact_length"],
            "deadline_ns": record["report_deadline_ns"],
            "state": "ACTIVE",
        }
        report_permit_records[object_identity(permit)] = permit_record
        record["report_artifact_state"] = "PERMIT_ISSUED"
        if not report_permit_is_valid(permit, publication_root, raw):
            invalid()
        return raw, permit

    def prepare_report_permit_consumption(
        permit: _Task064ReportPublicationPermit,
    ) -> tuple[Callable[[], bool], Callable[[], bool], Callable[[], bool]]:
        record = exact_report_permit_record(permit)
        if (
            record is None
            or record["state"] != "ACTIVE"
            or not report_permit_is_valid(
                permit,
                cast(Path, record["publication_root"]),
                cast(bytes, record["report_artifact"]),
            )
        ):
            invalid()
        preparation_nonce = issue_ticket_nonce(32)
        if type_of(preparation_nonce) is not bytes_type or length_of(preparation_nonce) != 32:
            terminalize_report_permit_record(record, "TERMINALIZED")
            invalid()
        record["state"] = "PREPARED"
        record["preparation_fields"] = (
            object_identity(preparation_nonce),
            preparation_nonce,
        )
        prepared_state = "READY"
        permit_key = object_identity(permit)

        def terminalize(state: str) -> bool:
            nonlocal prepared_state
            if (
                prepared_state != "READY"
                or exact_report_permit_record(permit) is not record
                or record.get("state") != "PREPARED"
                or record.get("preparation_fields")
                != (object_identity(preparation_nonce), preparation_nonce)
            ):
                return False
            if not terminalize_report_permit_record(record, state):
                return False
            prepared_state = state
            return True

        def commit() -> bool:
            if not report_permit_is_valid(
                permit,
                cast(Path, record["publication_root"]),
                cast(bytes, record["report_artifact"]),
            ):
                return False
            return terminalize("CONSUMED")

        def terminate() -> bool:
            return terminalize("TERMINALIZED")

        def is_terminal() -> bool:
            terminal = terminal_report_permit_records.get(permit_key)
            return bool_type(
                terminal is not None
                and terminal[0] is permit
                and terminal[1] == current_pid()
                and terminal[2] in {"CONSUMED", "TERMINALIZED"}
                and permit_key not in report_permit_records
            )

        return commit, terminate, is_terminal

    def terminalize_fixture_ticket(
        ticket: _Task064ChildProvenanceTicket,
        target_node_id: str,
        *,
        require_claimed: bool,
        terminal_state: str,
    ) -> None:
        ticket_key = object_identity(ticket)
        record = exact_ticket_record(ticket, target_node_id)
        if (
            record is None
            or (require_claimed and record["state"] != "CLAIMED")
            or record["state"] not in {"AUTHENTICATED", "ACTIVATED", "CLAIMED"}
            or terminal_state not in {"RETURNED", "CANCELLED", "UNCLAIMED"}
            or length_of(terminal_ticket_records) >= maximum_terminal_tickets
        ):
            invalid()
        if record["state"] in {"ACTIVATED", "CLAIMED"}:
            context_token = record.get("context_token")
            if (
                type_of(context_token) is not context_token_type
                or active_body_ticket.get() is not ticket
            ):
                mark_authority_uncertain()
                invalid()
            try:
                active_body_ticket.reset(context_token)  # type: ignore[arg-type]
            except base_exception_type:
                mark_authority_uncertain()
                invalid()
        terminal_ticket_records[ticket_key] = (
            ticket,
            current_pid(),
            target_node_id,
            terminal_state,
        )
        del ticket_records[ticket_key]

    def finish_fixture_ticket(
        ticket: _Task064ChildProvenanceTicket | None,
        target_node_id: str,
    ) -> None:
        """Require one exact claim and erase every fixture-local ticket reference."""

        if ticket is None:
            return
        record = exact_ticket_record(ticket, target_node_id)
        if record is None:
            invalid()
        if record["state"] != "CLAIMED":
            terminalize_fixture_ticket(
                ticket,
                target_node_id,
                require_claimed=False,
                terminal_state="UNCLAIMED",
            )
            invalid()
        if record["protocol"] == "report_close" and record.get("report_artifact_state") not in {
            "CONSUMED",
            "TERMINALIZED",
        }:
            for permit_record in tuple_type(report_permit_records.values()):
                if permit_record.get("ticket") is ticket:
                    terminalize_report_permit_record(
                        permit_record,
                        "TERMINALIZED",
                    )
            terminalize_fixture_ticket(
                ticket,
                target_node_id,
                require_claimed=True,
                terminal_state="UNCLAIMED",
            )
            invalid()
        terminalize_fixture_ticket(
            ticket,
            target_node_id,
            require_claimed=True,
            terminal_state="RETURNED",
        )

    def cancel_fixture_ticket(
        ticket: _Task064ChildProvenanceTicket | None,
        target_node_id: str,
    ) -> None:
        """Idempotently erase an exact ticket after any fixture failure."""

        if ticket is None:
            return
        ticket_key = object_identity(ticket)
        terminal_record = terminal_ticket_records.get(ticket_key)
        if terminal_record is not None:
            if terminal_record[:3] != (ticket, current_pid(), target_node_id):
                invalid()
            return
        for permit_record in tuple_type(report_permit_records.values()):
            if permit_record.get("ticket") is ticket:
                terminalize_report_permit_record(
                    permit_record,
                    "TERMINALIZED",
                )
        terminalize_fixture_ticket(
            ticket,
            target_node_id,
            require_claimed=False,
            terminal_state="CANCELLED",
        )

    def close(provenance: _Task064ChildProvenance) -> bool:
        if (
            type_of(provenance) is not provenance_type
            or current_pid() != listener_owner_pid
            or not listener_is_exact()
        ):
            invalid()
        provenance_record = provenance_records.get(object_identity(provenance))
        if (
            provenance_record is None
            or provenance_record.get("provenance") is not provenance
            or provenance_record.get("provenance_fields") != provenance_fields(provenance)
            or provenance_record.get("owner_process_id") != current_pid()
            or provenance_record.get("owner_thread_id") != current_thread_id()
            or provenance_record.get("state") != "ISSUED"
        ):
            invalid()
        close_ok = True
        launch_record = exact_launch_record(provenance)
        launch_state_lock: AbstractContextManager[None] | None = None
        if launch_record is not None:
            cancel = launch_record.get("cancel")
            state_lock_value = launch_record.get("state_lock")
            if (
                not isinstance_value(state_lock_value, lock_type)
                or not callable(cancel)
                or launch_record.get("parent_process_id") != listener_owner_pid
                or launch_record.get("owner_thread_id") != current_thread_id()
            ):
                invalid()
            launch_state_lock = cast(AbstractContextManager[None], state_lock_value)
            try:
                with launch_state_lock:
                    terminal_state = launch_record.get("state")
                    if (
                        terminal_state not in {"ISSUED", "ATTESTED"}
                        or (
                            launch_record.get("child_process_id") is not None
                            and terminal_state != "ATTESTED"
                        )
                        or launch_record.get("transport_error") is not None
                        or not listener_is_exact()
                    ):
                        close_ok = False
                cast(Callable[[], None], cancel)()
            except base_exception_type:
                close_ok = False
        if provenance.artifact_descriptor >= 0:
            try:
                close_file(provenance.artifact_descriptor)
            except os_error:
                close_ok = False
        try:
            close_file(provenance.descriptor)
        except os_error:
            close_ok = False
        consumed = False
        try:
            marker_details = provenance.marker_path.lstat()
        except file_not_found_error:
            consumed = True
        except os_error:
            close_ok = False
        else:
            if (
                stat_is_link(marker_details.st_mode)
                or not stat_is_regular(marker_details.st_mode)
                or marker_details.st_dev != provenance.marker_device
                or marker_details.st_ino != provenance.marker_inode
                or marker_details.st_uid != current_uid()
                or stat_mode(marker_details.st_mode) != 0o600
            ):
                close_ok = False
            else:
                try:
                    unlink_file(provenance.marker_path)
                except os_error:
                    close_ok = False
        if not close_ok:
            if launch_record is not None and launch_state_lock is not None:
                with launch_state_lock:
                    launch_record["state"] = "CLOSE_UNCERTAIN"
            provenance_record["state"] = "CLOSE_UNCERTAIN"
            mark_authority_uncertain()
            invalid()
        if launch_record is not None and launch_state_lock is not None:
            with launch_state_lock:
                launch_record["state"] = "CLOSED"
        provenance_record["state"] = "CLOSED"
        provenance_records.pop(object_identity(provenance), None)
        if launch_record is not None:
            launch_records.pop(object_identity(provenance), None)
        return consumed

    return (
        issue,
        spawn_authenticated_child,
        probe_parent_attestation_binding,
        authenticate_before_fixture,
        activate_fixture_ticket,
        claim_post_return,
        ticket_is_report_close,
        claim_body_dispatch,
        finish_fixture_ticket,
        cancel_fixture_ticket,
        close,
        bind_report_artifact_validation,
        claim_report_publication,
        report_permit_is_valid,
        prepare_report_permit_consumption,
    )


(
    _issue_task064_child_provenance,
    _spawn_task064_authenticated_child,
    _probe_task064_parent_attestation_binding,
    _authenticate_task064_child_provenance,
    _activate_task064_child_provenance,
    _claim_task064_post_return_child_provenance,
    _task064_child_ticket_is_report_close,
    _claim_task064_child_dispatch_provenance,
    _finish_task064_child_provenance,
    _cancel_task064_child_provenance,
    _close_task064_child_provenance,
    _bind_task064_report_artifact_validation,
    _claim_task064_report_close_publication,
    _validate_task064_report_publication_permit,
    _prepare_task064_report_publication_permit_consumption,
) = _build_task064_child_provenance_authority()
del _build_task064_child_provenance_authority


def _build_evidence_run_authority() -> tuple[
    Callable[[Path], _EvidenceRun],
    Callable[[_EvidenceRun, tuple[str, ...]], bool],
    Callable[[_EvidenceRun], Callable[[], bool]],
    Callable[[_EvidenceRun], Callable[[], bool]],
    Callable[[_EvidenceRun], bool],
    Callable[[_EvidenceRun], bool],
    Callable[[str], None],
]:
    """Own irreversible run issuance and lifecycle outside mutable ledger mirrors."""

    nonce_issuer = _issue_authority_nonce
    root_lookup = _lookup_active_pytest_root
    root_session_owns = _pytest_root_session_owns
    mark_authority_uncertain = _latch_pytest_root_authority_uncertainty
    active_run_context = _ACTIVE_EVIDENCE_RUN
    evidence_ledger_type = _EvidenceLedger
    records: dict[int, dict[str, object]] = {}
    begin_stage_faults = frozenset(
        {
            "after_record_reservation",
            "after_ledger_run",
            "after_ledger_recording",
            "after_context_set",
        }
    )
    begin_rollback_faults = frozenset(
        {
            "rollback_context_reset",
            "rollback_ledger_restore",
        }
    )
    armed_begin_faults: frozenset[str] = frozenset()

    def arm_begin_fault(name: str) -> None:
        nonlocal armed_begin_faults
        if (
            type(name) is not str
            or name not in begin_stage_faults | begin_rollback_faults
            or name in armed_begin_faults
            or len(armed_begin_faults) >= 2
            or (
                name in begin_stage_faults
                and any(fault in begin_stage_faults for fault in armed_begin_faults)
            )
            or (
                name in begin_rollback_faults
                and any(fault in begin_rollback_faults for fault in armed_begin_faults)
            )
            or (name == "rollback_context_reset" and "after_context_set" not in armed_begin_faults)
            or (
                name == "rollback_ledger_restore"
                and not (
                    armed_begin_faults
                    & frozenset(
                        {
                            "after_ledger_run",
                            "after_ledger_recording",
                            "after_context_set",
                        }
                    )
                )
            )
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        armed_begin_faults = armed_begin_faults | frozenset((name,))

    def consume_begin_fault(name: str) -> bool:
        nonlocal armed_begin_faults
        if name not in armed_begin_faults:
            return False
        armed_begin_faults = armed_begin_faults - frozenset((name,))
        return True

    def run_fields(run: _EvidenceRun) -> tuple[object, ...]:
        registration = run._pytest_registration
        ledger = registration.evidence_ledger
        return (
            id(registration),
            id(run._nonce),
            run._nonce,
            id(ledger),
            id(ledger.nonce),
            ledger.nonce,
        )

    def record_matches(run: _EvidenceRun, phases: tuple[str, ...]) -> bool:
        if (
            type(run) is not _EvidenceRun
            or type(phases) is not tuple
            or type(run._pytest_registration.evidence_ledger) is not evidence_ledger_type
        ):
            return False
        record = records.get(id(run))
        return not (
            record is None
            or record["run"] is not run
            or record["run_fields"] != run_fields(run)
            or type(run._nonce) is not bytes
            or len(run._nonce) != 32
            or record["phase"] not in phases
            or type(record["version"]) is not int
        )

    def validate(run: _EvidenceRun, phases: tuple[str, ...]) -> bool:
        return bool(
            record_matches(run, phases)
            and root_lookup(run._pytest_registration.path_object) is run._pytest_registration
            and root_session_owns(run._pytest_registration, False)
        )

    def begin(pytest_root: Path) -> _EvidenceRun:
        _validate_bootstrap_root(pytest_root)
        registration = _lookup_active_pytest_root(pytest_root)
        if registration is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        ledger = registration.evidence_ledger
        if (
            type(ledger) is not evidence_ledger_type
            or ledger.closed
            or ledger.consumed
            or ledger.recording
            or ledger.run is not None
            or ledger.receipt is not None
            or ledger.observations
            or ledger.operation_runs
            or ledger.rejection_runs
            or active_run_context.get() is not None
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        run = _EvidenceRun(
            _pytest_registration=registration,
            _nonce=nonce_issuer("evidence-run"),
        )
        if id(run) in records:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        record: dict[str, object] = {
            "run": run,
            "registration": registration,
            "ledger": ledger,
            "run_fields": run_fields(run),
            "phase": "BEGINNING",
            "version": 0,
        }
        context_token: Token[_EvidenceRun | None] | None = None
        try:
            records[id(run)] = record
            if consume_begin_fault("after_record_reservation"):
                raise RuntimeError("injected evidence-run reservation failure")
            ledger.run = run
            if consume_begin_fault("after_ledger_run"):
                raise RuntimeError("injected evidence-run ledger binding failure")
            ledger.recording = True
            if consume_begin_fault("after_ledger_recording"):
                raise RuntimeError("injected evidence-run recording failure")
            context_token = active_run_context.set(run)
            if consume_begin_fault("after_context_set"):
                raise RuntimeError("injected evidence-run context failure")
            if (
                records.get(id(run)) is not record
                or record["phase"] != "BEGINNING"
                or record["version"] != 0
                or record["run_fields"] != run_fields(run)
                or ledger.run is not run
                or not ledger.recording
                or ledger.receipt is not None
                or ledger.closed
                or ledger.consumed
                or ledger.observations
                or ledger.operation_runs
                or ledger.rejection_runs
                or active_run_context.get() is not run
            ):
                raise RuntimeError("evidence-run begin postcondition failed")
            record["phase"] = "RECORDING"
            record["version"] = 1
            if not validate(run, ("RECORDING",)):
                raise RuntimeError("evidence-run begin transition failed")
            return run
        except BaseException:
            rollback_ok = True
            if context_token is not None:
                if consume_begin_fault("rollback_context_reset"):
                    rollback_ok = False
                else:
                    try:
                        active_run_context.reset(context_token)
                    except BaseException:
                        rollback_ok = False
            if consume_begin_fault("rollback_ledger_restore"):
                rollback_ok = False
            else:
                try:
                    ledger.run = None
                    ledger.recording = False
                    ledger.receipt = None
                except BaseException:
                    rollback_ok = False
            try:
                record["phase"] = "ABORTED"
                record["version"] = cast(int, record["version"]) + 1
            except BaseException:
                rollback_ok = False
            if not (
                records.get(id(run)) is record
                and record.get("phase") == "ABORTED"
                and active_run_context.get() is None
                and ledger.run is None
                and not ledger.recording
                and ledger.receipt is None
                and not ledger.closed
                and not ledger.consumed
                and not ledger.observations
                and not ledger.operation_runs
                and not ledger.rejection_runs
            ):
                rollback_ok = False
            if not rollback_ok:
                mark_authority_uncertain()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None

    def prepare_seal(run: _EvidenceRun) -> Callable[[], bool]:
        if not validate(run, ("RECORDING",)):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        record = records[id(run)]
        expected_version = cast(int, record["version"])
        prepared_state = "READY"

        def transition() -> bool:
            nonlocal prepared_state
            if (
                prepared_state != "READY"
                or records.get(id(run)) is not record
                or not validate(run, ("RECORDING",))
                or record["version"] != expected_version
            ):
                return False
            prepared_state = "COMMITTING"
            record["phase"] = "SEALED"
            record["version"] = expected_version + 1
            prepared_state = "DONE"
            return record_matches(run, ("SEALED",))

        return transition

    def prepare_consumption(run: _EvidenceRun) -> Callable[[], bool]:
        if not validate(run, ("SEALED",)):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        record = records[id(run)]
        expected_version = cast(int, record["version"])
        prepared_state = "READY"

        def transition() -> bool:
            nonlocal prepared_state
            if (
                prepared_state != "READY"
                or records.get(id(run)) is not record
                or not record_matches(run, ("SEALED",))
                or record["version"] != expected_version
            ):
                return False
            prepared_state = "COMMITTING"
            record["phase"] = "CONSUMED"
            record["version"] = expected_version + 1
            prepared_state = "DONE"
            return record_matches(run, ("CONSUMED",))

        return transition

    def close(run: _EvidenceRun) -> bool:
        if not record_matches(run, ("RECORDING", "SEALED", "CONSUMED", "ABORTED")):
            return False
        record = records[id(run)]
        version = cast(int, record["version"])
        record["phase"] = "CLOSED"
        record["version"] = version + 1
        return record_matches(run, ("CLOSED",))

    def is_closed(run: _EvidenceRun) -> bool:
        return record_matches(run, ("CLOSED",))

    return (
        begin,
        validate,
        prepare_seal,
        prepare_consumption,
        close,
        is_closed,
        arm_begin_fault,
    )


(
    begin_generated_evidence_run,
    _validate_issued_evidence_run,
    _prepare_issued_evidence_run_seal,
    _prepare_issued_evidence_run_consumption,
    _close_issued_evidence_run,
    _is_issued_evidence_run_closed,
    _arm_evidence_run_begin_fault,
) = _build_evidence_run_authority()
del _build_evidence_run_authority
_bind_pytest_root_run_closer(
    _close_issued_evidence_run,
    _is_issued_evidence_run_closed,
)
del _bind_pytest_root_run_closer
del _close_issued_evidence_run
del _is_issued_evidence_run_closed


def _validated_evidence_run(run: _EvidenceRun) -> _EvidenceLedger:
    if type(run) is not _EvidenceRun or not _validate_issued_evidence_run(
        run, ("RECORDING", "SEALED")
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    registration = run._pytest_registration
    _validate_bootstrap_root(registration.path_object)
    ledger = registration.evidence_ledger
    if type(ledger) is not _EvidenceLedger:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    exact_ledger = ledger
    if (
        exact_ledger.closed
        or exact_ledger.consumed
        or exact_ledger.run is not run
        or registration.process_id != os.getpid()
        or type(exact_ledger.nonce) is not bytes
        or len(exact_ledger.nonce) != 32
        or (exact_ledger.recording and _ACTIVE_EVIDENCE_RUN.get() is not run)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return exact_ledger


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


def _active_rejection_recording_ledger(
    *,
    allow_intentional_root_fault: bool = False,
) -> _EvidenceLedger:
    """Validate outer receipt authority without masking an intentional root fault."""

    run = _ACTIVE_EVIDENCE_RUN.get()
    if (
        run is None
        or type(run) is not _EvidenceRun
        or not _validate_issued_evidence_run(run, ("RECORDING",))
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    registration = run._pytest_registration
    ledger = registration.evidence_ledger
    if type(ledger) is not _EvidenceLedger:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    exact_ledger = ledger
    if (
        exact_ledger.run is not run
        or not exact_ledger.recording
        or exact_ledger.receipt is not None
        or exact_ledger.closed
        or exact_ledger.consumed
        or registration.process_id != os.getpid()
        or _lookup_active_pytest_root(registration.path_object) is not registration
        or not _pytest_root_session_owns(registration, False)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if not allow_intentional_root_fault:
        _validate_bootstrap_root(registration.path_object)
    return exact_ledger


def _capture_harness_rejection_unsealed(
    label: str,
    *,
    scenario_snapshot: Callable[[_RejectionScenario], _RejectionScenarioSnapshot],
    pytest_root: Path | None = None,
    token: StoreToken | None = None,
    stream_id: UUID | None = None,
    natural_key: bytes | None = None,
    limit: int | None = None,
    expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
) -> tuple[str, HarnessFailureCode]:
    """Execute one fixed harness-owned rejection scenario and record its result."""

    _active_rejection_recording_ledger(
        allow_intentional_root_fault=label == "widened_root",
    )
    state = _ACTIVE_REJECTION_COLLECTOR.get()
    if (
        state is None
        or state.run is not _ACTIVE_EVIDENCE_RUN.get()
        or len(state.observations) >= len(state.scenarios)
        or type(label) is not str
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    scenario = state.scenarios[len(state.observations)]
    (
        scenario_label,
        scenario_kind,
        scenario_code,
        scenario_executors,
        _,
    ) = scenario_snapshot(scenario)
    if scenario_kind != "harness" or scenario_label != label:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    executor_name = scenario_executors[0] if len(scenario_executors) == 1 else None
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
    target_registration = None if pytest_root is None else _lookup_active_pytest_root(pytest_root)
    token_registration = None if token is None else _lookup_store_token_authority(token)
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
    revoked_target = None if pytest_root is None else _lookup_revoked_pytest_root(pytest_root)
    if label == "expired_root" and (
        target_registration is not None
        or revoked_target is None
        or revoked_target.path_object is not pytest_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if label == "expired_token" and (
        token_registration is None
        or _lookup_active_pytest_root(token_registration.pytest_registration.path_object)
        is token_registration.pytest_registration
        or _lookup_revoked_pytest_root(token_registration.pytest_registration.path_object)
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

    original_resolve = Path.resolve

    def fail_resolve(_path: Path, *, strict: bool = False) -> Path:
        del strict
        raise FileNotFoundError("sanitized-report-path-probe")

    def execute_scenario() -> None:
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

    try:
        if label in {"wrong_process_root", "wrong_process_token"}:
            wrong_process_registration = (
                cast(_ActivePytestRoot, target_registration)
                if label == "wrong_process_root"
                else cast(_RegisteredIdentity, token_registration).pytest_registration
            )
            original_process_id = wrong_process_registration.process_id
            object.__setattr__(
                wrong_process_registration,
                "process_id",
                -1,
            )
            try:
                execute_scenario()
            finally:
                object.__setattr__(
                    wrong_process_registration,
                    "process_id",
                    original_process_id,
                )
        elif label in {"path_resolution_bootstrap", "path_resolution_operation"}:
            Path.resolve = fail_resolve  # type: ignore[method-assign,assignment]
            execute_scenario()
        elif label in {"wrong_node_root", "wrong_node_token"}:
            Context().run(execute_scenario)
        else:
            execute_scenario()
    except HarnessFailure as error:
        if error.code is not scenario_code or not _traceback_contains_harness_executor(
            error,
            scenario_executors,
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
        Path.resolve = original_resolve  # type: ignore[method-assign]
    raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _capture_sqlite_rejection_unsealed(
    label: str,
    connection: sqlite3.Connection,
    *,
    scenario_snapshot: Callable[[_RejectionScenario], _RejectionScenarioSnapshot],
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
    (
        scenario_label,
        scenario_kind,
        scenario_code,
        scenario_executors,
        _,
    ) = scenario_snapshot(scenario)
    if scenario_kind != "sqlite" or scenario_label != label or scenario_executors:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    binding = _connection_authority_binding(connection)
    token_registration = None if binding is None else _lookup_store_identity_by_nonce(binding[0])
    if (
        binding is None
        or not binding[1]
        or not _connection_has_path_snapshot(connection)
        or token_registration is None
        or token_registration.pytest_registration is not state.run._pytest_registration
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
            or code is not scenario_code
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


def _build_rejection_evidence_authority(
    scenario_snapshot: Callable[[_RejectionScenario], _RejectionScenarioSnapshot],
) -> tuple[
    Callable[..., tuple[str, HarnessFailureCode]],
    Callable[..., tuple[str, HarnessFailureCode]],
    Callable[[_RejectionObservation, _EvidenceRun, str, int], bool],
]:
    """Issue rejection receipts only after the fixed executor path completes."""

    issued: dict[tuple[int, str, int], dict[str, object]] = {}
    harness_capability = object()
    sqlite_capability = object()

    def reserve(
        capability: object,
        state: _RejectionCollectorState,
        label: str,
    ) -> tuple[tuple[int, str, int], int]:
        ledger = _active_rejection_recording_ledger(
            allow_intentional_root_fault=label == "widened_root",
        )
        prior_count = len(state.observations)
        if (
            _ACTIVE_EVIDENCE_RUN.get() is not state.run
            or ledger is not state.run._pytest_registration.evidence_ledger
            or prior_count >= len(state.scenarios)
            or state.scenarios[prior_count].label != label
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        scenario_snapshot(state.scenarios[prior_count])
        key = (id(state.run), state.gate, prior_count)
        if key in issued:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        issued[key] = {
            "phase": "RESERVED",
            "run": state.run,
            "registration": state.run._pytest_registration,
            "capability": capability,
            "label": label,
        }
        return key, prior_count

    def record_latest(
        key: tuple[int, str, int],
        state: _RejectionCollectorState,
        prior_count: int,
    ) -> None:
        if len(state.observations) != prior_count + 1:
            issued[key]["phase"] = "POISONED"
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        observation = state.observations[prior_count]
        immutable_scenario_snapshot = scenario_snapshot(observation.scenario)
        issued[key] = {
            **issued[key],
            "phase": "ISSUED",
            "observation": observation,
            "scenario": observation.scenario,
            "code": observation.code,
            "sqlite_errorcode": observation.sqlite_errorcode,
            "process_id": observation.process_id,
            "ordinal": observation.ordinal,
            "target_nonce": observation.target_nonce,
            "call_digest": observation.call_digest,
            "scenario_snapshot": immutable_scenario_snapshot,
        }

    def capture_harness(
        label: str,
        *,
        pytest_root: Path | None = None,
        token: StoreToken | None = None,
        stream_id: UUID | None = None,
        natural_key: bytes | None = None,
        limit: int | None = None,
        expectation: ContinuousPublicTradeStreamExpectationV1 | None = None,
    ) -> tuple[str, HarnessFailureCode]:
        state = _ACTIVE_REJECTION_COLLECTOR.get()
        if state is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        key, prior_count = reserve(harness_capability, state, label)
        try:
            result = _capture_harness_rejection_unsealed(
                label,
                scenario_snapshot=scenario_snapshot,
                pytest_root=pytest_root,
                token=token,
                stream_id=stream_id,
                natural_key=natural_key,
                limit=limit,
                expectation=expectation,
            )
        except BaseException:
            issued[key]["phase"] = "POISONED"
            raise
        record_latest(key, state, prior_count)
        return result

    def capture_sqlite(
        label: str,
        connection: sqlite3.Connection,
        *,
        parameters: Sequence[object] = (),
    ) -> tuple[str, HarnessFailureCode]:
        state = _ACTIVE_REJECTION_COLLECTOR.get()
        if state is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        key, prior_count = reserve(sqlite_capability, state, label)
        try:
            result = _capture_sqlite_rejection_unsealed(
                label,
                connection,
                scenario_snapshot=scenario_snapshot,
                parameters=parameters,
            )
        except BaseException:
            issued[key]["phase"] = "POISONED"
            raise
        record_latest(key, state, prior_count)
        return result

    def validate(
        observation: _RejectionObservation,
        run: _EvidenceRun,
        gate: str,
        sequence: int,
    ) -> bool:
        entry = issued.get((id(run), gate, sequence))
        try:
            current_scenario_snapshot = scenario_snapshot(observation.scenario)
        except HarnessFailure:
            return False
        return bool(
            entry is not None
            and entry["phase"] == "ISSUED"
            and entry["run"] is run
            and entry["registration"] is run._pytest_registration
            and entry["observation"] is observation
            and entry["scenario"] is observation.scenario
            and entry["scenario_snapshot"] == current_scenario_snapshot
            and entry["label"] == current_scenario_snapshot[0]
            and entry["code"] is observation.code
            and entry["sqlite_errorcode"] == observation.sqlite_errorcode
            and entry["process_id"] == observation.process_id == os.getpid()
            and entry["ordinal"] == observation.ordinal == sequence
            and entry["target_nonce"] == observation.target_nonce
            and entry["call_digest"] == observation.call_digest
            and type(observation.call_digest) is str
            and len(observation.call_digest) == 64
            and all(character in "0123456789abcdef" for character in observation.call_digest)
        )

    return capture_harness, capture_sqlite, validate


def _generated_gate_payloads(
    evidence: GeneratedEvidenceAggregate,
) -> tuple[tuple[str, object], ...]:
    if type(evidence) is not GeneratedEvidenceAggregate:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
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
    if (
        type(evidence) is not GeneratedEvidenceAggregate
        or type(evidence.bootstrap_path_ownership) is not BootstrapPathEvidence
        or type(evidence.backup_restore) is not BackupRestoreEvidence
        or type(evidence.generation_copy) is not GenerationCopyEvidence
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_token = evidence.bootstrap_path_ownership.token
    backup = evidence.backup_restore
    generation_copy = evidence.generation_copy
    if (
        type(report_token) is not StoreToken
        or type(backup.source_token) is not StoreToken
        or type(backup.backup_token) is not StoreToken
        or type(backup.restore_token) is not StoreToken
        or type(generation_copy.source_token) is not StoreToken
        or type(generation_copy.destination_token) is not StoreToken
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
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


def _seal_generated_evidence_run_unsealed(
    run: _EvidenceRun,
    *,
    evidence: GeneratedEvidenceAggregate,
    private_receipt_digest: Callable[[_EvidenceRun, str], str | None],
    validate_gate_observation: Callable[[_EvidenceObservation, _EvidenceRun, int], bool],
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
            or not validate_gate_observation(observation, run, ordinal)
            or id(payload) in seen_payload_identities
            or observation.token_roles != expected_token_roles[ordinal]
            or observation.private_receipt_digest != private_receipt_digest(run, name)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        seen_payload_identities.add(id(payload))
        gate_receipts_list.append(
            _GateEvidenceReceipt(
                gate=name,
                payload=payload,
                payload_digest=observation.payload_digest,
            )
        )
    gate_receipts = tuple(gate_receipts_list)
    receipt = _EvidenceReceipt(
        run=run,
        evidence=evidence,
        evidence_digest=_evidence_payload_digest(evidence),
        gates=gate_receipts,
    )
    return receipt


def _build_evidence_receipt_authority(
    private_receipt_digest: Callable[[_EvidenceRun, str], str | None],
    validate_gate_observation: Callable[[_EvidenceObservation, _EvidenceRun, int], bool],
) -> tuple[
    Callable[..., _EvidenceReceipt],
    Callable[[_EvidenceReceipt, GeneratedEvidenceAggregate, bool], bool],
    Callable[
        [_EvidenceReceipt],
        tuple[
            Callable[[], bool],
            Callable[[], bool],
            Callable[[], bool],
        ],
    ],
    Callable[[], None],
]:
    """Issue and consume exact receipts under an irreversible closure-held lifecycle."""

    issued: dict[int, dict[str, object]] = {}
    seal_implementation = _seal_generated_evidence_run_unsealed
    prepare_run_consumption = _prepare_issued_evidence_run_consumption
    prepare_run_seal = _prepare_issued_evidence_run_seal
    validate_run = _validate_issued_evidence_run
    active_run_context = _ACTIVE_EVIDENCE_RUN
    root_lookup = _lookup_active_pytest_root
    root_session_owns = _pytest_root_session_owns
    process_cleanup_uncertain = _has_process_cleanup_uncertainty
    mark_cleanup_uncertain = _mark_process_cleanup_uncertain
    seal_transition_fault = False

    def arm_seal_transition_fault() -> None:
        nonlocal seal_transition_fault
        if seal_transition_fault:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        seal_transition_fault = True

    def seal(
        run: _EvidenceRun,
        *,
        evidence: GeneratedEvidenceAggregate,
    ) -> _EvidenceReceipt:
        nonlocal seal_transition_fault
        if (
            type(evidence) is not GeneratedEvidenceAggregate
            or not validate_run(run, ("RECORDING",))
            or id(run) in issued
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        ledger = _validated_evidence_run(run)
        if set(ledger.observations) != set(range(len(GENERATED_EVIDENCE_GATES))):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        observations = tuple(
            ledger.observations[ordinal] for ordinal in range(len(GENERATED_EVIDENCE_GATES))
        )
        if any(
            not validate_gate_observation(observation, run, ordinal)
            for ordinal, observation in enumerate(observations)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        receipt = seal_implementation(
            run,
            evidence=evidence,
            private_receipt_digest=private_receipt_digest,
            validate_gate_observation=validate_gate_observation,
        )
        gates = receipt.gates
        gate_snapshots = tuple(
            (
                gate,
                gate.gate,
                gate.payload,
                _evidence_payload_digest(gate.payload),
            )
            for gate in gates
        )
        second_evidence_digest = _evidence_payload_digest(evidence)
        if (
            any(
                snapshot[3] != gate.payload_digest
                for gate, snapshot in zip(gates, gate_snapshots, strict=True)
            )
            or second_evidence_digest != receipt.evidence_digest
            or ledger.receipt is not None
            or not ledger.recording
            or active_run_context.get() is not run
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        run_seal_transition = prepare_run_seal(run)
        prepared_record: dict[str, object] = {
            "phase": "SEALED",
            "version": 0,
            "run": run,
            "registration": run._pytest_registration,
            "ledger": ledger,
            "receipt": receipt,
            "evidence": evidence,
            "evidence_digest": second_evidence_digest,
            "gates": gates,
            "gate_snapshots": gate_snapshots,
            "observations": observations,
        }
        issued[id(run)] = prepared_record
        context_token: Token[_EvidenceRun | None] | None = None
        try:
            if seal_transition_fault:
                seal_transition_fault = False
                raise RuntimeError("injected active evidence-run transition failure")
            context_token = active_run_context.set(None)
            ledger.receipt = receipt
            ledger.recording = False
            if not run_seal_transition():
                raise RuntimeError("evidence-run seal transition lost authority")
        except BaseException:
            if issued.get(id(run)) is prepared_record:
                issued.pop(id(run))
            retryable = bool(
                context_token is None
                or (validate_run(run, ("RECORDING",)) and not ledger.closed and not ledger.consumed)
            )
            if retryable:
                ledger.receipt = None
                ledger.recording = True
                if context_token is not None:
                    try:
                        active_run_context.reset(context_token)
                    except BaseException:
                        retryable = False
            if not retryable:
                mark_cleanup_uncertain()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        return receipt

    def validate(
        receipt: _EvidenceReceipt,
        evidence: GeneratedEvidenceAggregate,
        require_exact_evidence: bool,
    ) -> bool:
        if (
            type(receipt) is not _EvidenceReceipt
            or type(evidence) is not GeneratedEvidenceAggregate
            or type(require_exact_evidence) is not bool
            or (require_exact_evidence and receipt.evidence is not evidence)
        ):
            return False
        record = issued.get(id(receipt.run))
        registration = receipt.run._pytest_registration
        ledger = registration.evidence_ledger
        if (
            type(ledger) is not _EvidenceLedger
            or record is None
            or record["phase"] != "SEALED"
            or record["run"] is not receipt.run
            or record["registration"] is not registration
            or record["ledger"] is not ledger
            or record["receipt"] is not receipt
            or record["gates"] is not receipt.gates
            or record["evidence"] is not receipt.evidence
            or not validate_run(receipt.run, ("SEALED",))
            or ledger.receipt is not receipt
            or ledger.recording
            or ledger.closed
            or ledger.consumed
        ):
            return False
        try:
            evidence_digest = _evidence_payload_digest(receipt.evidence)
        except HarnessFailure:
            return False
        if (
            record["evidence_digest"] != evidence_digest
            or receipt.evidence_digest != evidence_digest
            or len(receipt.gates) != len(GENERATED_EVIDENCE_GATES)
            or len(cast(tuple[object, ...], record["gate_snapshots"])) != len(receipt.gates)
        ):
            return False
        # The fresh aggregate digest above traverses all twelve ordered payload objects. The
        # loop retains their exact receipt/snapshot identities and stored digests; the sealed
        # observation validator supplies the one remaining live per-gate proof.
        for ordinal, (gate, snapshot) in enumerate(
            zip(
                receipt.gates,
                cast(tuple[tuple[object, str, object, str], ...], record["gate_snapshots"]),
                strict=True,
            )
        ):
            exact_gate, gate_name, payload, payload_digest = snapshot
            observation = cast(tuple[_EvidenceObservation, ...], record["observations"])[ordinal]
            if (
                type(gate) is not _GateEvidenceReceipt
                or gate is not exact_gate
                or gate.gate != gate_name
                or gate.gate != GENERATED_EVIDENCE_GATES[ordinal]
                or gate.payload is not payload
                or gate.payload_digest != payload_digest
                or not validate_gate_observation(observation, receipt.run, ordinal)
                or ledger.observations.get(ordinal) is not observation
            ):
                return False
        return True

    def prepare_consumption(
        receipt: _EvidenceReceipt,
    ) -> tuple[
        Callable[[], bool],
        Callable[[], bool],
        Callable[[], bool],
    ]:
        if type(receipt) is not _EvidenceReceipt:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        record = issued.get(id(receipt.run))
        if (
            record is None
            or record["phase"] != "SEALED"
            or record["run"] is not receipt.run
            or record["registration"] is not receipt.run._pytest_registration
            or record["receipt"] is not receipt
            or record["version"] != 0
            or not validate_run(receipt.run, ("SEALED",))
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        ledger = cast(_EvidenceLedger, record["ledger"])
        if (
            ledger is not receipt.run._pytest_registration.evidence_ledger
            or ledger.receipt is not receipt
            or ledger.recording
            or ledger.closed
            or ledger.consumed
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        expected_version = record["version"]
        run_transition = prepare_run_consumption(receipt.run)
        prepared_state = "READY"

        def terminal_state() -> bool:
            return bool(
                issued.get(id(receipt.run)) is record
                and record["phase"] == "CONSUMED"
                and record["version"] == expected_version + 1
                and record["receipt"] is receipt
                and ledger.receipt is receipt
                and not ledger.recording
                and not ledger.closed
                and ledger.consumed
            )

        def transition(require_active_root: bool) -> bool:
            nonlocal prepared_state
            if (
                prepared_state != "READY"
                or issued.get(id(receipt.run)) is not record
                or record["phase"] != "SEALED"
                or record["version"] != expected_version
                or record["receipt"] is not receipt
                or ledger.recording
                or ledger.closed
                or ledger.consumed
                or (
                    require_active_root
                    and (
                        process_cleanup_uncertain()
                        or root_lookup(receipt.run._pytest_registration.path_object)
                        is not receipt.run._pytest_registration
                        or not root_session_owns(receipt.run._pytest_registration, False)
                    )
                )
            ):
                return False
            prepared_state = "COMMITTING"
            if not run_transition():
                prepared_state = "FAILED"
                return False
            record["phase"] = "CONSUMED"
            record["version"] = expected_version + 1
            ledger.receipt = receipt
            ledger.recording = False
            ledger.consumed = True
            prepared_state = "DONE"
            return terminal_state()

        def commit() -> bool:
            return transition(True)

        def terminalize() -> bool:
            return transition(False)

        return commit, terminalize, terminal_state

    return seal, validate, prepare_consumption, arm_seal_transition_fault


def _validate_evidence_receipt_unbound(
    validate_issued_receipt: Callable[
        [_EvidenceReceipt, GeneratedEvidenceAggregate, bool],
        bool,
    ],
    pytest_root: Path,
    receipt: _EvidenceReceipt,
    evidence: GeneratedEvidenceAggregate,
    require_exact_evidence: bool,
) -> _EvidenceLedger:
    _validate_bootstrap_root(pytest_root)
    if (
        type(receipt) is not _EvidenceReceipt
        or type(evidence) is not GeneratedEvidenceAggregate
        or not validate_issued_receipt(
            receipt,
            evidence,
            require_exact_evidence,
        )
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
        or any(type(gate) is not _GateEvidenceReceipt for gate in receipt.gates)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if require_exact_evidence:
        if receipt.evidence is not evidence:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        # The issued validator has just recomputed the complete aggregate digest. Keep the
        # independent ordered name/object mapping check without duplicating content hashing.
        for receipt_gate, (name, payload) in zip(
            receipt.gates,
            gate_payloads,
            strict=True,
        ):
            if receipt_gate.gate != name or receipt_gate.payload is not payload:
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


def _open_owned_generation(
    identity: _RegisteredIdentity,
    *,
    acquisition: _OperationPathAcquisition,
) -> tuple[int, int]:
    """Open the registered root and generation without following a replacement alias."""

    try:
        root_descriptor = _open_and_adopt_operation_path_descriptor(
            acquisition,
            "root",
            None,
            False,
        )
        root_details = os.fstat(root_descriptor)
        if (
            root_details.st_dev != identity.pytest_root_device
            or root_details.st_ino != identity.pytest_root_inode
            or root_details.st_uid != identity.pytest_root_uid
            or stat.S_IMODE(root_details.st_mode) != identity.pytest_root_mode
            or not stat.S_ISDIR(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        generation_descriptor = _open_and_adopt_operation_path_descriptor(
            acquisition,
            "generation",
            None,
            False,
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
    except BaseException as error:
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _build_connection_authority() -> tuple[
    Callable[[_RegisteredIdentity], _OperationPathAcquisition],
    Callable[[_OperationPathAcquisition, str, str | None, bool], int],
    Callable[
        [_OperationPathAcquisition, Sequence[_PinnedFile]],
        _OperationPathSnapshot,
    ],
    Callable[[_OperationPathAcquisition], None],
    Callable[[_OperationPathSnapshot], None],
    Callable[[sqlite3.Connection, _OperationPathSnapshot, bytes, bool], None],
    Callable[[sqlite3.Connection, _OperationPathSnapshot, bytes, bool], None],
    Callable[[sqlite3.Connection, StoreToken], None],
    Callable[[sqlite3.Connection], None],
    Callable[[sqlite3.Connection], _ConnectionImmutableRuntimeEvidence],
    Callable[[sqlite3.Connection, bytes, bool], _ConnectionImmutableRuntimeEvidence],
    Callable[[sqlite3.Connection, bool], bool],
    Callable[[sqlite3.Connection, StoreToken, bool], _LiveTransactionAuthorityView],
    Callable[[sqlite3.Connection], None],
    Callable[[_OperationPathSnapshot], Path],
    Callable[[_RegisteredIdentity, _OperationPathSnapshot], None],
    Callable[[_RegisteredIdentity, sqlite3.Connection], None],
    Callable[[_RegisteredIdentity, sqlite3.Connection], None],
    Callable[[_RegisteredIdentity, sqlite3.Connection, str, bool, int], int],
    Callable[[sqlite3.Connection], bool],
    Callable[[sqlite3.Connection], tuple[bytes, bool] | None],
    Callable[[], bool],
    Callable[[sqlite3.Connection], str | None],
    Callable[[str], None],
]:
    """Track exact live/close-uncertain SQLite and descriptor authority in a closure."""

    nonce_issuer = _issue_authority_nonce
    real_open = os.open
    real_close = os.close
    real_fstat = os.fstat
    real_stat = os.stat
    real_listdir = os.listdir
    real_getpid = os.getpid
    real_path_isabs = os.path.isabs
    real_path_normpath = os.path.normpath
    real_path_split = os.path.split
    is_directory = stat.S_ISDIR
    is_regular = stat.S_ISREG
    exact_mode = stat.S_IMODE
    sqlite_close = sqlite3.Connection.close
    sqlite_connection_type = sqlite3.Connection
    token_type = StoreToken
    token_fields = _store_token_fields
    token_lookup = _lookup_store_token_authority
    registered_fields_for = _registered_identity_fields
    identity_from_fields = _identity_from_fields
    root_lookup = _lookup_active_pytest_root
    root_session_owns = _pytest_root_session_owns
    root_authority_uncertain = _pytest_root_authority_uncertain
    process_cleanup_uncertain = _has_process_cleanup_uncertainty
    runtime_evidence_type = _ConnectionImmutableRuntimeEvidence
    live_authority_view_type = _LiveTransactionAuthorityView
    runtime_sys = sys
    runtime_sqlite = sqlite3
    runtime_fetch_one = _fetch_one
    runtime_fetch_all = _fetch_all
    accepted_runtime_fields = (
        ACCEPTED_PYTHON_VERSION,
        ACCEPTED_SQLITE_VERSION,
        ACCEPTED_THREADSAFETY,
        ACCEPTED_SQLITE_SOURCE_ID,
        ACCEPTED_COMPILE_OPTIONS,
    )
    mark_cleanup_uncertain = _mark_process_cleanup_uncertain
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    pthread_sigmask = getattr(signal, "pthread_sigmask", None)
    sig_block = getattr(signal, "SIG_BLOCK", None)
    sig_setmask = getattr(signal, "SIG_SETMASK", None)
    blockable_signals = frozenset(
        observed
        for observed in signal.valid_signals()
        if observed not in {signal.SIGKILL, signal.SIGSTOP}
    )
    owned_names = frozenset(_OWNED_DATABASE_FILENAMES)
    optional_names = owned_names - {_DATABASE_BASENAME}
    acquisition_records: dict[int, dict[str, object]] = {}
    snapshot_records: dict[int, dict[str, object]] = {}
    optional_seal_states: dict[
        int,
        tuple[
            _OperationPathSnapshot,
            tuple[tuple[str, tuple[int, int, int, int, int]], ...],
        ],
    ] = {}
    connection_records: dict[int, dict[str, object]] = {}
    closed_connections: WeakSet[sqlite3.Connection] = WeakSet()
    fork_unsafe_latched = False
    armed_fault: str | None = None
    allowed_faults = frozenset(
        {
            "after_root_open",
            "after_generation_open",
            "after_file_open",
            "file_close",
            "generation_close",
            "root_close",
            "sqlite_close",
        }
    )

    def consume_fault(name: str) -> bool:
        nonlocal armed_fault
        if armed_fault != name:
            return False
        armed_fault = None
        return True

    def arm_fault(name: str) -> None:
        nonlocal armed_fault
        if type(name) is not str or name not in allowed_faults or armed_fault is not None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        armed_fault = name

    def acquisition_fields(acquisition: _OperationPathAcquisition) -> tuple[object, ...]:
        return (id(acquisition.nonce), acquisition.nonce)

    def snapshot_fields(snapshot: _OperationPathSnapshot) -> tuple[object, ...]:
        return (id(snapshot.nonce), snapshot.nonce)

    def valid_acquisition(
        acquisition: object,
        states: tuple[str, ...],
    ) -> dict[str, object] | None:
        if type(acquisition) is not _OperationPathAcquisition:
            return None
        record = acquisition_records.get(id(acquisition))
        if (
            record is None
            or record["acquisition"] is not acquisition
            or record["acquisition_fields"] != acquisition_fields(acquisition)
            or record["state"] not in states
        ):
            return None
        return record

    def valid_snapshot(
        snapshot: object,
        states: tuple[str, ...],
    ) -> dict[str, object] | None:
        if type(snapshot) is not _OperationPathSnapshot:
            return None
        record = snapshot_records.get(id(snapshot))
        if (
            record is None
            or record["snapshot"] is not snapshot
            or record["snapshot_fields"] != snapshot_fields(snapshot)
            or record["state"] not in states
        ):
            return None
        return record

    def latch_uncertain() -> None:
        nonlocal fork_unsafe_latched
        fork_unsafe_latched = True
        mark_cleanup_uncertain()

    def begin_snapshot_acquisition(
        identity: _RegisteredIdentity,
    ) -> _OperationPathAcquisition:
        nonce = nonce_issuer("operation-path-acquisition")
        acquisition = _OperationPathAcquisition(nonce=nonce)
        acquisition_records[id(acquisition)] = {
            "acquisition": acquisition,
            "acquisition_fields": acquisition_fields(acquisition),
            "identity_fields": _registered_identity_fields(identity),
            "state": "ACQUIRING",
            "descriptors": [],
            "roles": [],
            "close_attempted": set(),
        }
        return acquisition

    def open_and_adopt_snapshot_descriptor(
        acquisition: _OperationPathAcquisition,
        role: str,
        name: str | None = None,
        missing_ok: bool = False,
    ) -> int:
        nonlocal fork_unsafe_latched
        record = valid_acquisition(acquisition, ("ACQUIRING",))
        descriptors = [] if record is None else cast(list[int], record["descriptors"])
        roles = [] if record is None else cast(list[tuple[str, str | None]], record["roles"])
        if (
            record is None
            or type(role) is not str
            or type(missing_ok) is not bool
            or role not in {"root", "generation", "file"}
            or (role != "file" and (name is not None or missing_ok))
            or (
                role == "file"
                and (
                    type(name) is not str
                    or name not in owned_names
                    or any(observed_name == name for _, observed_name in roles)
                )
            )
            or (role == "root" and descriptors)
            or (role == "generation" and len(descriptors) != 1)
            or (role == "file" and len(descriptors) < 2)
        ):
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        identity_fields = cast(_RegisteredIdentityFields, record["identity_fields"])
        if role == "root":
            path: str | Path = identity_fields[0]
            flags = directory_flags
            directory_descriptor = None
        elif role == "generation":
            path = identity_fields[2].name
            flags = directory_flags
            directory_descriptor = descriptors[0]
        else:
            path = cast(str, name)
            flags = file_flags
            directory_descriptor = descriptors[1]
        old_signal_mask: object | None = None
        descriptor = -1
        record["state"] = "OPENING"
        try:
            if pthread_sigmask is not None and sig_block is not None and sig_setmask is not None:
                old_signal_mask = pthread_sigmask(sig_block, blockable_signals)
            if directory_descriptor is None:
                descriptor = real_open(path, flags)
            else:
                descriptor = real_open(path, flags, dir_fd=directory_descriptor)
            if type(descriptor) is not int or descriptor < 0 or descriptor in descriptors:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            descriptors.append(descriptor)
            roles.append((role, name))
            if consume_fault(f"after_{role}_open"):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            record["state"] = "ACQUIRING"
        except FileNotFoundError:
            if descriptor < 0:
                record["state"] = "ACQUIRING"
                if missing_ok:
                    return -1
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            record["state"] = "OPEN_UNCERTAIN"
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        except OSError:
            if descriptor < 0:
                record["state"] = "ACQUIRING"
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            record["state"] = "OPEN_UNCERTAIN"
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        except BaseException as error:
            if descriptor >= 0 and descriptor not in descriptors:
                descriptors.append(descriptor)
                roles.append((role, name))
            record["state"] = "OPEN_UNCERTAIN"
            latch_uncertain()
            if isinstance(error, HarnessFailure):
                raise
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        finally:
            if old_signal_mask is not None:
                try:
                    cast(Callable[[object, object], object], pthread_sigmask)(
                        cast(object, sig_setmask),
                        old_signal_mask,
                    )
                except BaseException:
                    record["state"] = "CLOSE_UNCERTAIN"
                    latch_uncertain()
        if record["state"] != "ACQUIRING":
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return descriptor

    def complete_snapshot_acquisition(
        acquisition: _OperationPathAcquisition,
        files: Sequence[_PinnedFile],
    ) -> _OperationPathSnapshot:
        nonlocal fork_unsafe_latched
        acquisition_record = valid_acquisition(acquisition, ("ACQUIRING",))
        descriptors = (
            ()
            if acquisition_record is None
            else tuple(cast(list[int], acquisition_record["descriptors"]))
        )
        exact_files = tuple(files)
        if (
            acquisition_record is None
            or type(files) not in {tuple, list}
            or len(descriptors) < 2
            or descriptors[2:] != tuple(item.descriptor for item in exact_files)
            or any(type(item) is not _PinnedFile for item in exact_files)
            or len(set(descriptors)) != len(descriptors)
        ):
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        pinned_fields = tuple(
            (
                item.name,
                item.descriptor,
                item.device,
                item.inode,
                item.uid,
                item.mode,
                item.link_count,
            )
            for item in exact_files
        )
        snapshot = _OperationPathSnapshot(
            nonce=nonce_issuer("operation-path-snapshot"),
        )
        optional_file_seals = tuple(
            sorted(
                (
                    item[0],
                    (item[2], item[3], item[4], item[5], item[6]),
                )
                for item in pinned_fields
                if item[0] in optional_names
            )
        )
        optional_seal_state = (snapshot, optional_file_seals)
        if id(snapshot) in optional_seal_states:
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        optional_seal_states[id(snapshot)] = optional_seal_state
        snapshot_records[id(snapshot)] = {
            "snapshot": snapshot,
            "snapshot_fields": snapshot_fields(snapshot),
            "identity_fields": acquisition_record["identity_fields"],
            "descriptors": descriptors,
            "roles": tuple(cast(list[tuple[str, str | None]], acquisition_record["roles"])),
            "pinned_fields": pinned_fields,
            "optional_seal_state": optional_seal_state,
            "close_attempted": set(),
            "acquisition_record": acquisition_record,
            "state": "LIVE",
        }
        acquisition_record["state"] = "TRANSFERRED"
        return snapshot

    def fail_snapshot_acquisition(
        acquisition: _OperationPathAcquisition,
    ) -> None:
        record = valid_acquisition(
            acquisition,
            ("ACQUIRING", "OPENING", "OPEN_UNCERTAIN", "CLOSE_UNCERTAIN"),
        )
        if record is None:
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        already_uncertain = record["state"] in {"OPEN_UNCERTAIN", "CLOSE_UNCERTAIN"}
        record["state"] = "CLOSING"
        close_attempted = cast(set[int], record["close_attempted"])
        close_failed = False
        descriptors = cast(list[int], record["descriptors"])
        roles = cast(list[tuple[str, str | None]], record["roles"])
        for descriptor, (role, _) in reversed(tuple(zip(descriptors, roles, strict=True))):
            if descriptor in close_attempted:
                continue
            close_attempted.add(descriptor)
            try:
                real_close(descriptor)
                if consume_fault(f"{role}_close"):
                    raise OSError(errno.EIO, "injected checked close uncertainty")
            except BaseException:
                close_failed = True
        if close_failed or already_uncertain:
            record["state"] = "CLOSE_UNCERTAIN"
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        record["state"] = "CLOSED"

    def close_snapshot(snapshot: _OperationPathSnapshot) -> None:
        record = valid_snapshot(snapshot, ("LIVE", "CLOSED", "CLOSE_UNCERTAIN"))
        if record is None:
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        state = record["state"]
        if state == "CLOSED":
            return
        if state != "LIVE":
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        optional_seal_state = record.get("optional_seal_state")
        optional_seal_state_is_exact = bool(
            type(optional_seal_state) is tuple
            and len(optional_seal_state) == 2
            and optional_seal_state[0] is snapshot
            and optional_seal_states.get(id(snapshot)) is optional_seal_state
        )
        record["state"] = "CLOSING"
        close_attempted = cast(set[int], record["close_attempted"])
        close_failed = False
        descriptors = cast(tuple[int, ...], record["descriptors"])
        roles = cast(tuple[tuple[str, str | None], ...], record["roles"])
        for descriptor, (role, _) in reversed(tuple(zip(descriptors, roles, strict=True))):
            if descriptor in close_attempted:
                continue
            close_attempted.add(descriptor)
            try:
                real_close(descriptor)
                if consume_fault(f"{role}_close"):
                    raise OSError(errno.EIO, "injected checked close uncertainty")
            except BaseException:
                close_failed = True
        if close_failed or not optional_seal_state_is_exact:
            record["state"] = "CLOSE_UNCERTAIN"
            cast(dict[str, object], record["acquisition_record"])["state"] = "CLOSE_UNCERTAIN"
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if optional_seal_states.pop(id(snapshot), None) is not optional_seal_state:
            record["state"] = "CLOSE_UNCERTAIN"
            cast(dict[str, object], record["acquisition_record"])["state"] = "CLOSE_UNCERTAIN"
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        record["state"] = "CLOSED"
        cast(dict[str, object], record["acquisition_record"])["state"] = "CLOSED"

    def register_connection(
        connection: sqlite3.Connection,
        snapshot: _OperationPathSnapshot,
        nonce: bytes,
        writer: bool,
    ) -> None:
        nonlocal fork_unsafe_latched
        snapshot_record = valid_snapshot(snapshot, ("LIVE",))
        creator_pid = real_getpid()
        if (
            not isinstance(connection, sqlite_connection_type)
            or snapshot_record is None
            or type(nonce) is not bytes
            or len(nonce) != 32
            or type(writer) is not bool
            or type(creator_pid) is not int
            or creator_pid <= 0
            or id(connection) in connection_records
            or connection in closed_connections
        ):
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        connection_records[id(connection)] = {
            "connection": connection,
            "snapshot": snapshot,
            "snapshot_record": snapshot_record,
            "nonce": nonce,
            "nonce_identity": id(nonce),
            "writer": writer,
            "creator_pid": creator_pid,
            "token_state": "UNBOUND",
            "token": None,
            "token_fields": None,
            "identity_fields": None,
            "alias_paths": None,
            "database_list_path": None,
            "authority_binding": None,
            "runtime_state": "UNBOUND",
            "runtime_evidence": None,
            "runtime_evidence_fields": None,
            "runtime_evidence_binding": None,
            "runtime_seal": None,
            "state": "LIVE",
        }

    def build_alias_paths(database_path: Path) -> tuple[str, ...]:
        path = str(database_path)
        if not real_path_isabs(path) or real_path_normpath(path) != path:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        reversed_paths: list[str] = []
        current = path
        while True:
            reversed_paths.append(current)
            parent, _ = real_path_split(current)
            if parent == current:
                break
            if not parent or len(reversed_paths) > 256:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            current = parent
        paths = tuple(reversed(reversed_paths))
        if not paths or paths[-1] != path:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return paths

    def authority_binding_fields(
        connection: sqlite3.Connection,
        record: dict[str, object],
    ) -> tuple[object, ...]:
        return (
            id(connection),
            connection,
            id(record["snapshot"]),
            record["snapshot"],
            id(record["snapshot_record"]),
            record["snapshot_record"],
            id(record["token"]),
            record["token"],
            id(record["token_fields"]),
            record["token_fields"],
            id(record["identity_fields"]),
            record["identity_fields"],
            id(record["alias_paths"]),
            record["alias_paths"],
            id(record["database_list_path"]),
            record["database_list_path"],
            record["nonce_identity"],
            record["nonce"],
            record["writer"],
            record["creator_pid"],
        )

    def bind_connection_token(
        connection: sqlite3.Connection,
        token: StoreToken,
    ) -> None:
        record = connection_records.get(id(connection))
        snapshot_record = None if record is None else record.get("snapshot_record")
        registered = token_lookup(token)
        registered_fields = None if registered is None else registered_fields_for(registered)
        current_token_fields = None if registered is None else token_fields(token)
        descriptors = (
            ()
            if type(snapshot_record) is not dict
            else cast(tuple[int, ...], snapshot_record.get("descriptors", ()))
        )
        if (
            record is None
            or record.get("connection") is not connection
            or record.get("state") != "LIVE"
            or record.get("creator_pid") != real_getpid()
            or record.get("token_state") != "UNBOUND"
            or any(
                record.get(name) is not None
                for name in (
                    "token",
                    "token_fields",
                    "identity_fields",
                    "alias_paths",
                    "database_list_path",
                    "authority_binding",
                )
            )
            or type(token) is not token_type
            or registered is None
            or registered_fields is None
            or current_token_fields is None
            or type(token._nonce) is not bytes
            or token._nonce is not record.get("nonce")
            or id(token._nonce) != record.get("nonce_identity")
            or type(snapshot_record) is not dict
            or valid_snapshot(record.get("snapshot"), ("LIVE",)) is not snapshot_record
            or registered_fields != snapshot_record.get("identity_fields")
            or len(descriptors) < 2
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        alias_paths = build_alias_paths(registered.database_path)
        if alias_paths[-3:] != (
            str(registered.pytest_root),
            str(registered.generation_root),
            str(registered.database_path),
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        database_list_path = str(registered.database_path)
        record["token"] = token
        record["token_fields"] = current_token_fields
        record["identity_fields"] = snapshot_record["identity_fields"]
        record["alias_paths"] = alias_paths
        record["database_list_path"] = database_list_path
        record["authority_binding"] = authority_binding_fields(connection, record)
        record["token_state"] = "BOUND"

    def has_exact_authority_binding(
        connection: sqlite3.Connection,
        record: dict[str, object],
    ) -> bool:
        binding = record.get("authority_binding")
        if (
            record.get("token_state") != "BOUND"
            or type(record.get("token")) is not token_type
            or type(record.get("token_fields")) is not tuple
            or type(record.get("identity_fields")) is not tuple
            or type(record.get("alias_paths")) is not tuple
            or type(record.get("database_list_path")) is not str
            or type(binding) is not tuple
            or len(binding) != 20
        ):
            return False
        expected = authority_binding_fields(connection, record)
        return bool(
            binding == expected
            and binding[1] is connection
            and binding[3] is record["snapshot"]
            and binding[5] is record["snapshot_record"]
            and binding[7] is record["token"]
            and binding[9] is record["token_fields"]
            and binding[11] is record["identity_fields"]
            and binding[13] is record["alias_paths"]
            and binding[15] is record["database_list_path"]
            and binding[17] is record["nonce"]
        )

    def discard_failed_connection_open(
        connection: sqlite3.Connection,
        snapshot: _OperationPathSnapshot,
        nonce: bytes,
        writer: bool,
    ) -> None:
        nonlocal fork_unsafe_latched
        record = connection_records.get(id(connection))
        if record is not None:
            if (
                record.get("connection") is not connection
                or record.get("snapshot") is not snapshot
                or valid_snapshot(snapshot, ("LIVE",)) is not record.get("snapshot_record")
                or record.get("nonce") is not nonce
                or record.get("nonce_identity") != id(nonce)
                or record.get("writer") is not writer
                or record.get("creator_pid") != real_getpid()
                or record.get("state") != "LIVE"
            ):
                fork_unsafe_latched = True
                latch_uncertain()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            close_connection(connection)
            return
        snapshot_record = valid_snapshot(snapshot, ("LIVE",))
        creator_pid = real_getpid()
        if (
            not isinstance(connection, sqlite_connection_type)
            or snapshot_record is None
            or type(nonce) is not bytes
            or len(nonce) != 32
            or type(writer) is not bool
            or type(creator_pid) is not int
            or creator_pid <= 0
            or connection in closed_connections
        ):
            fork_unsafe_latched = True
            latch_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        record = {
            "connection": connection,
            "snapshot": snapshot,
            "snapshot_record": snapshot_record,
            "nonce": nonce,
            "nonce_identity": id(nonce),
            "writer": writer,
            "creator_pid": creator_pid,
            "token_state": "UNBOUND",
            "token": None,
            "token_fields": None,
            "identity_fields": None,
            "alias_paths": None,
            "database_list_path": None,
            "authority_binding": None,
            "runtime_state": "UNBOUND",
            "runtime_evidence": None,
            "runtime_evidence_fields": None,
            "runtime_evidence_binding": None,
            "runtime_seal": None,
            "state": "DISCARDING",
        }
        connection_records[id(connection)] = record
        sqlite_closed = True
        try:
            sqlite_close(connection)
            if consume_fault("sqlite_close"):
                raise sqlite3.OperationalError("injected checked sqlite close uncertainty")
        except BaseException:
            sqlite_closed = False
        snapshot_closed = True
        try:
            close_snapshot(snapshot)
        except HarnessFailure:
            snapshot_closed = False
        if sqlite_closed and snapshot_closed:
            record["state"] = "CLOSED"
            closed_connections.add(connection)
            del connection_records[id(connection)]
            return
        record["state"] = "CLOSE_UNCERTAIN"
        latch_uncertain()
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    def exact_runtime_record(
        connection: sqlite3.Connection,
        runtime_states: tuple[str, ...],
    ) -> dict[str, object]:
        record = connection_records.get(id(connection))
        snapshot = None if record is None else record.get("snapshot")
        if (
            record is None
            or record.get("connection") is not connection
            or record.get("state") != "LIVE"
            or type(record.get("creator_pid")) is not int
            or record["creator_pid"] != real_getpid()
            or type(record.get("nonce")) is not bytes
            or len(cast(bytes, record["nonce"])) != 32
            or record.get("nonce_identity") != id(record["nonce"])
            or type(record.get("writer")) is not bool
            or not has_exact_authority_binding(connection, record)
            or record.get("runtime_state") not in runtime_states
            or valid_snapshot(snapshot, ("LIVE",)) is not record.get("snapshot_record")
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return record

    def runtime_evidence_fields(
        evidence: _ConnectionImmutableRuntimeEvidence,
    ) -> tuple[str, str, int, str, tuple[str, ...]]:
        return (
            evidence.python_version,
            evidence.sqlite_version,
            evidence.threadsafety,
            evidence.sqlite_source_id,
            evidence.compile_options,
        )

    def capture_connection_runtime(connection: sqlite3.Connection) -> None:
        record = exact_runtime_record(connection, ("UNBOUND",))
        if any(
            record[name] is not None
            for name in (
                "runtime_evidence",
                "runtime_evidence_fields",
                "runtime_evidence_binding",
                "runtime_seal",
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        record["runtime_state"] = "CAPTURED"
        source_id = cast(
            str,
            runtime_fetch_one(connection, "SELECT sqlite_source_id()")[0],
        )
        python_version = ".".join(str(part) for part in runtime_sys.version_info[:3])
        compile_options = tuple(
            sorted(
                cast(str, row[0]) for row in runtime_fetch_all(connection, "PRAGMA compile_options")
            )
        )
        evidence = runtime_evidence_type(
            python_version=python_version,
            sqlite_version=runtime_sqlite.sqlite_version,
            threadsafety=runtime_sqlite.threadsafety,
            sqlite_source_id=source_id,
            compile_options=compile_options,
        )
        evidence_fields = runtime_evidence_fields(evidence)
        binding = (
            id(connection),
            connection,
            id(record["snapshot"]),
            record["snapshot"],
            record["nonce_identity"],
            record["nonce"],
            record["writer"],
            record["creator_pid"],
            id(evidence),
            evidence,
            evidence_fields,
        )
        record["runtime_evidence"] = evidence
        record["runtime_evidence_fields"] = evidence_fields
        record["runtime_evidence_binding"] = binding
        if (
            evidence_fields != accepted_runtime_fields
            or "THREADSAFE=0" in compile_options
            or any(
                option in compile_options
                for option in ("OMIT_FOREIGN_KEY", "OMIT_TRIGGER", "OMIT_AUTHORIZATION")
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    def exact_captured_runtime(
        connection: sqlite3.Connection,
        record: dict[str, object],
    ) -> _ConnectionImmutableRuntimeEvidence:
        evidence = record.get("runtime_evidence")
        fields = record.get("runtime_evidence_fields")
        binding = record.get("runtime_evidence_binding")
        expected_binding = (
            id(connection),
            connection,
            id(record["snapshot"]),
            record["snapshot"],
            record["nonce_identity"],
            record["nonce"],
            record["writer"],
            record["creator_pid"],
            id(evidence),
            evidence,
            fields,
        )
        if (
            type(evidence) is not runtime_evidence_type
            or type(fields) is not tuple
            or len(fields) != 5
            or runtime_evidence_fields(evidence) != fields
            or fields != accepted_runtime_fields
            or type(binding) is not tuple
            or binding != expected_binding
            or binding[1] is not connection
            or binding[3] is not record["snapshot"]
            or binding[5] is not record["nonce"]
            or binding[9] is not evidence
            or binding[10] is not fields
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return evidence

    def seal_connection_runtime(
        connection: sqlite3.Connection,
    ) -> _ConnectionImmutableRuntimeEvidence:
        record = exact_runtime_record(connection, ("CAPTURED",))
        evidence = exact_captured_runtime(connection, record)
        if record["runtime_seal"] is not None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        seal = (
            id(connection),
            connection,
            id(evidence),
            evidence,
            record["runtime_evidence_binding"],
        )
        record["runtime_seal"] = seal
        record["runtime_state"] = "SEALED"
        return runtime_evidence_type(*runtime_evidence_fields(evidence))

    def consume_connection_runtime(
        connection: sqlite3.Connection,
        nonce: bytes,
        writer: bool,
    ) -> _ConnectionImmutableRuntimeEvidence:
        record = exact_runtime_record(connection, ("SEALED",))
        evidence = exact_captured_runtime(connection, record)
        seal = record.get("runtime_seal")
        expected_seal = (
            id(connection),
            connection,
            id(evidence),
            evidence,
            record["runtime_evidence_binding"],
        )
        if (
            type(nonce) is not bytes
            or len(nonce) != 32
            or nonce is not record["nonce"]
            or type(writer) is not bool
            or writer is not record["writer"]
            or type(seal) is not tuple
            or seal != expected_seal
            or seal[1] is not connection
            or seal[3] is not evidence
            or seal[4] is not record["runtime_evidence_binding"]
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return runtime_evidence_type(*runtime_evidence_fields(evidence))

    def require_connection_transaction_state(
        connection: sqlite3.Connection,
        writer: bool,
    ) -> bool:
        record = exact_runtime_record(connection, ("SEALED",))
        if type(writer) is not bool or writer is not record["writer"]:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return connection.in_transaction is True

    def require_live_transaction_authority(
        connection: sqlite3.Connection,
        token: StoreToken,
        writer: bool,
    ) -> _LiveTransactionAuthorityView:
        record = connection_records.get(id(connection))
        snapshot_record = None if record is None else record.get("snapshot_record")
        if (
            record is None
            or record.get("connection") is not connection
            or record.get("state") != "LIVE"
            or record.get("creator_pid") != real_getpid()
            or type(writer) is not bool
            or writer is not record.get("writer")
            or connection.in_transaction is not True
            or type(snapshot_record) is not dict
            or valid_snapshot(record.get("snapshot"), ("LIVE",)) is not snapshot_record
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if (
            process_cleanup_uncertain()
            or root_authority_uncertain()
            or type(token) is not token_type
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
        registered = token_lookup(token)
        if registered is None:
            raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
        active_root = registered.pytest_registration
        registered_fields = registered_fields_for(registered)
        if (
            root_lookup(active_root.path_object) is not active_root
            or active_root.resolved_path != registered.pytest_root
            or active_root.device != registered.pytest_root_device
            or active_root.inode != registered.pytest_root_inode
            or active_root.uid != registered.pytest_root_uid
            or active_root.mode != registered.pytest_root_mode
            or not root_session_owns(active_root, True)
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
        if (
            record["token"] is not token
            or token_fields(token) != record["token_fields"]
            or token._nonce is not record["nonce"]
            or id(token._nonce) != record["nonce_identity"]
            or registered_fields != record["identity_fields"]
            or registered_fields != snapshot_record.get("identity_fields")
            or not has_exact_authority_binding(connection, record)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        exact_runtime_record(connection, ("SEALED",))
        consume_connection_runtime(connection, token._nonce, writer)
        return live_authority_view_type(
            registered=identity_from_fields(record["identity_fields"]),
            database_list_path=cast(str, record["database_list_path"]),
        )

    def close_connection(
        connection: sqlite3.Connection,
    ) -> None:
        nonlocal fork_unsafe_latched
        record = connection_records.get(id(connection))
        if record is None:
            if connection in closed_connections:
                return
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if record["connection"] is not connection:
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        state = record["state"]
        if state == "CLOSED":
            return
        if state != "LIVE":
            fork_unsafe_latched = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        sqlite_closed = True
        try:
            sqlite_close(connection)
            if consume_fault("sqlite_close"):
                raise sqlite3.OperationalError("injected checked sqlite close uncertainty")
        except BaseException:
            sqlite_closed = False
        snapshot_closed = True
        try:
            close_snapshot(cast(_OperationPathSnapshot, record["snapshot"]))
        except HarnessFailure:
            snapshot_closed = False
        if sqlite_closed and snapshot_closed:
            record["state"] = "CLOSED"
            closed_connections.add(connection)
            del connection_records[id(connection)]
            return
        record["state"] = "CLOSE_UNCERTAIN"
        latch_uncertain()
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    def snapshot_database_path(snapshot: _OperationPathSnapshot) -> Path:
        record = valid_snapshot(snapshot, ("LIVE",))
        if record is None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        descriptors = cast(tuple[int, ...], record["descriptors"])
        if len(descriptors) < 2 or not Path("/proc/self/fd").is_dir():
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return Path(f"/proc/self/fd/{descriptors[1]}/{_DATABASE_BASENAME}")

    def revalidate_snapshot(
        identity: _RegisteredIdentity,
        snapshot: _OperationPathSnapshot,
    ) -> None:
        record = valid_snapshot(snapshot, ("LIVE",))
        exact_identity_fields = registered_fields_for(identity)
        acquisition_record = None if record is None else record.get("acquisition_record")
        if (
            record is None
            or exact_identity_fields != record["identity_fields"]
            or type(acquisition_record) is not dict
            or acquisition_record.get("state") != "TRANSFERRED"
            or acquisition_record.get("identity_fields") is not record["identity_fields"]
            or tuple(cast(list[int], acquisition_record.get("descriptors", [])))
            != record["descriptors"]
            or tuple(
                cast(
                    list[tuple[str, str | None]],
                    acquisition_record.get("roles", []),
                )
            )
            != record["roles"]
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        descriptors = cast(tuple[int, ...], record["descriptors"])
        roles = cast(tuple[tuple[str, str | None], ...], record["roles"])
        pinned_fields = cast(
            tuple[tuple[str, int, int, int, int, int, int], ...],
            record["pinned_fields"],
        )
        optional_seal_state = record.get("optional_seal_state")
        registered_optional_seal_state = optional_seal_states.get(id(snapshot))
        if (
            len(descriptors) < 2
            or len(roles) != len(descriptors)
            or roles[:2] != (("root", None), ("generation", None))
            or roles[2:] != tuple(("file", item[0]) for item in pinned_fields)
            or type(optional_seal_state) is not tuple
            or len(optional_seal_state) != 2
            or optional_seal_state[0] is not snapshot
            or registered_optional_seal_state is not optional_seal_state
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        exact_optional_seal_state = cast(
            tuple[
                _OperationPathSnapshot,
                tuple[tuple[str, tuple[int, int, int, int, int]], ...],
            ],
            optional_seal_state,
        )
        if type(exact_optional_seal_state[1]) is not tuple:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        optional_file_seal_items = exact_optional_seal_state[1]
        optional_file_seals = dict(optional_file_seal_items)
        if (
            tuple(sorted(optional_file_seal_items)) != optional_file_seal_items
            or len(optional_file_seals) != len(optional_file_seal_items)
            or set(optional_file_seals) - optional_names
            or any(
                type(name) is not str
                or type(seal) is not tuple
                or len(seal) != 5
                or any(type(value) is not int for value in seal)
                for name, seal in optional_file_seal_items
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        try:
            root = real_fstat(descriptors[0])
            generation = real_fstat(descriptors[1])
            if (
                root.st_dev != exact_identity_fields[4]
                or root.st_ino != exact_identity_fields[5]
                or root.st_uid != exact_identity_fields[6]
                or exact_mode(root.st_mode) != exact_identity_fields[7]
                or not is_directory(root.st_mode)
                or generation.st_dev != exact_identity_fields[8]
                or generation.st_ino != exact_identity_fields[9]
                or generation.st_uid != exact_identity_fields[10]
                or exact_mode(generation.st_mode) != exact_identity_fields[11]
                or not is_directory(generation.st_mode)
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            root_path = real_stat(exact_identity_fields[0], follow_symlinks=False)
            generation_name = real_path_split(str(exact_identity_fields[2]))[1]
            generation_entry = real_stat(
                generation_name,
                dir_fd=descriptors[0],
                follow_symlinks=False,
            )
            if (
                not generation_name
                or root_path.st_dev != root.st_dev
                or root_path.st_ino != root.st_ino
                or root_path.st_uid != root.st_uid
                or exact_mode(root_path.st_mode) != exact_mode(root.st_mode)
                or not is_directory(root_path.st_mode)
                or generation_entry.st_dev != generation.st_dev
                or generation_entry.st_ino != generation.st_ino
                or generation_entry.st_uid != generation.st_uid
                or exact_mode(generation_entry.st_mode) != exact_mode(generation.st_mode)
                or not is_directory(generation_entry.st_mode)
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            observed_names = tuple(real_listdir(descriptors[1]))
            if any(type(name) is not str for name in observed_names):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            names = tuple(sorted(observed_names))
            if (
                _DATABASE_BASENAME not in names
                or set(names) - owned_names
                or len(names) != len(set(names))
                or set(optional_file_seals) - set(names)
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            prior = {item[0]: item for item in pinned_fields}
            if (
                len(prior) != len(pinned_fields)
                or _DATABASE_BASENAME not in prior
                or set(prior) - owned_names
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            pending_optional_seals: dict[str, tuple[int, int, int, int, int]] = {}
            for name in names:
                details = real_stat(
                    name,
                    dir_fd=descriptors[1],
                    follow_symlinks=False,
                )
                expected = prior.get(name)
                if expected is not None:
                    pinned_details = real_fstat(expected[1])
                    if (
                        not is_regular(pinned_details.st_mode)
                        or pinned_details.st_dev != expected[2]
                        or pinned_details.st_ino != expected[3]
                        or pinned_details.st_uid != expected[4]
                        or exact_mode(pinned_details.st_mode) != expected[5]
                        or pinned_details.st_nlink != expected[6]
                        or details.st_dev != pinned_details.st_dev
                        or details.st_ino != pinned_details.st_ino
                    ):
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if (
                    not is_regular(details.st_mode)
                    or details.st_dev != exact_identity_fields[12]
                    or details.st_uid != exact_identity_fields[14]
                    or details.st_nlink != 1
                    or exact_mode(details.st_mode) != 0o600
                    or (
                        name == _DATABASE_BASENAME
                        and (
                            details.st_ino != exact_identity_fields[13]
                            or details.st_dev != exact_identity_fields[12]
                        )
                    )
                    or (
                        expected is not None
                        and (
                            details.st_dev != expected[2]
                            or details.st_ino != expected[3]
                            or details.st_uid != expected[4]
                            or exact_mode(details.st_mode) != expected[5]
                            or details.st_nlink != expected[6]
                        )
                    )
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if name in optional_names:
                    observed_seal = (
                        details.st_dev,
                        details.st_ino,
                        details.st_uid,
                        exact_mode(details.st_mode),
                        details.st_nlink,
                    )
                    sealed = optional_file_seals.get(name)
                    if sealed is None:
                        pending_optional_seals[name] = observed_seal
                    elif sealed != observed_seal:
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if pending_optional_seals:
                second_names = tuple(real_listdir(descriptors[1]))
                if (
                    any(type(name) is not str for name in second_names)
                    or tuple(sorted(second_names)) != names
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                for name, candidate in pending_optional_seals.items():
                    second_details = real_stat(
                        name,
                        dir_fd=descriptors[1],
                        follow_symlinks=False,
                    )
                    if (
                        not is_regular(second_details.st_mode)
                        or (
                            second_details.st_dev,
                            second_details.st_ino,
                            second_details.st_uid,
                            exact_mode(second_details.st_mode),
                            second_details.st_nlink,
                        )
                        != candidate
                    ):
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if (
                    optional_seal_states.get(id(snapshot)) is not optional_seal_state
                    or record.get("optional_seal_state") is not optional_seal_state
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                next_optional_file_seals = tuple(
                    sorted((*optional_file_seals.items(), *pending_optional_seals.items()))
                )
                next_optional_seal_state = (snapshot, next_optional_file_seals)
                optional_seal_states[id(snapshot)] = next_optional_seal_state
                record["optional_seal_state"] = next_optional_seal_state
        except HarnessFailure:
            raise
        except (OSError, RuntimeError, IndexError, KeyError, TypeError):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None

    def revalidate_connection(
        identity: _RegisteredIdentity,
        connection: sqlite3.Connection,
    ) -> None:
        record = connection_records.get(id(connection))
        if (
            record is None
            or record["connection"] is not connection
            or record["state"] != "LIVE"
            or record.get("creator_pid") != real_getpid()
            or not has_exact_authority_binding(connection, record)
            or registered_fields_for(identity) != record.get("identity_fields")
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        revalidate_snapshot(identity, cast(_OperationPathSnapshot, record["snapshot"]))

    def revalidate_connection_alias_path(
        identity: _RegisteredIdentity,
        connection: sqlite3.Connection,
    ) -> None:
        record = connection_records.get(id(connection))
        exact_identity_fields = registered_fields_for(identity)
        paths = None if record is None else record.get("alias_paths")
        if (
            record is None
            or record.get("connection") is not connection
            or record.get("state") != "LIVE"
            or record.get("creator_pid") != real_getpid()
            or not has_exact_authority_binding(connection, record)
            or exact_identity_fields != record.get("identity_fields")
            or type(paths) is not tuple
            or len(paths) < 3
            or any(type(path) is not str for path in paths)
            or paths[-3:]
            != (
                str(exact_identity_fields[0]),
                str(exact_identity_fields[2]),
                str(exact_identity_fields[3]),
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        try:
            for index, path in enumerate(cast(tuple[str, ...], paths)):
                details = real_stat(path, follow_symlinks=False)
                if index < len(paths) - 1:
                    if not is_directory(details.st_mode):
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                elif (
                    not is_regular(details.st_mode)
                    or details.st_dev != exact_identity_fields[12]
                    or details.st_ino != exact_identity_fields[13]
                    or details.st_uid != exact_identity_fields[14]
                    or exact_mode(details.st_mode) != exact_identity_fields[15]
                    or details.st_nlink != exact_identity_fields[16]
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if index == len(paths) - 3 and (
                    details.st_dev != exact_identity_fields[4]
                    or details.st_ino != exact_identity_fields[5]
                    or details.st_uid != exact_identity_fields[6]
                    or exact_mode(details.st_mode) != exact_identity_fields[7]
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if index == len(paths) - 2 and (
                    details.st_dev != exact_identity_fields[8]
                    or details.st_ino != exact_identity_fields[9]
                    or details.st_uid != exact_identity_fields[10]
                    or exact_mode(details.st_mode) != exact_identity_fields[11]
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        except HarnessFailure:
            raise
        except (OSError, RuntimeError, IndexError, TypeError):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None

    def connection_file_size(
        identity: _RegisteredIdentity,
        connection: sqlite3.Connection,
        name: str,
        required: bool,
        maximum: int,
    ) -> int:
        record = connection_records.get(id(connection))
        if (
            record is None
            or record["connection"] is not connection
            or record["state"] != "LIVE"
            or name not in owned_names
            or type(required) is not bool
            or type(maximum) is not int
            or maximum < 0
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        snapshot = cast(_OperationPathSnapshot, record["snapshot"])
        revalidate_snapshot(identity, snapshot)
        snapshot_record = valid_snapshot(snapshot, ("LIVE",))
        if snapshot_record is None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        generation_descriptor = cast(tuple[int, ...], snapshot_record["descriptors"])[1]
        try:
            details = real_stat(
                name,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            if not required:
                revalidate_snapshot(identity, snapshot)
                return 0
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        except OSError:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_dev != identity.device
            or details.st_uid != identity.uid
            or details.st_nlink != 1
            or stat.S_IMODE(details.st_mode) != 0o600
            or not 0 <= details.st_size <= maximum
            or (
                name == _DATABASE_BASENAME
                and (details.st_ino != identity.inode or details.st_dev != identity.device)
            )
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        revalidate_snapshot(identity, snapshot)
        return details.st_size

    def connection_has_snapshot(connection: sqlite3.Connection) -> bool:
        record = connection_records.get(id(connection))
        return bool(
            record is not None
            and record["connection"] is connection
            and record["state"] == "LIVE"
            and valid_snapshot(record["snapshot"], ("LIVE",)) is record["snapshot_record"]
        )

    def connection_binding(connection: sqlite3.Connection) -> tuple[bytes, bool] | None:
        record = connection_records.get(id(connection))
        if record is None or record["connection"] is not connection or record["state"] != "LIVE":
            return None
        return cast(bytes, record["nonce"]), cast(bool, record["writer"])

    def fork_unsafe() -> bool:
        return bool(
            fork_unsafe_latched
            or any(
                record["state"] not in {"CLOSED", "TRANSFERRED"}
                for record in acquisition_records.values()
            )
            or any(record["state"] != "CLOSED" for record in snapshot_records.values())
            or any(record["state"] != "CLOSED" for record in connection_records.values())
        )

    def connection_state(connection: sqlite3.Connection) -> str | None:
        record = connection_records.get(id(connection))
        if record is None:
            return "CLOSED" if connection in closed_connections else None
        if record["connection"] is not connection:
            return None
        return cast(str, record["state"])

    return (
        begin_snapshot_acquisition,
        open_and_adopt_snapshot_descriptor,
        complete_snapshot_acquisition,
        fail_snapshot_acquisition,
        close_snapshot,
        register_connection,
        discard_failed_connection_open,
        bind_connection_token,
        capture_connection_runtime,
        seal_connection_runtime,
        consume_connection_runtime,
        require_connection_transaction_state,
        require_live_transaction_authority,
        close_connection,
        snapshot_database_path,
        revalidate_snapshot,
        revalidate_connection,
        revalidate_connection_alias_path,
        connection_file_size,
        connection_has_snapshot,
        connection_binding,
        fork_unsafe,
        connection_state,
        arm_fault,
    )


(
    _begin_operation_path_acquisition,
    _open_and_adopt_operation_path_descriptor,
    _complete_operation_path_acquisition,
    _fail_operation_path_acquisition,
    _close_operation_path_snapshot,
    _register_live_connection,
    _discard_failed_connection_open,
    _bind_live_connection_token,
    _capture_connection_immutable_runtime,
    _seal_connection_immutable_runtime,
    _consume_connection_immutable_runtime,
    _require_connection_transaction_state,
    _require_live_transaction_authority,
    _close_live_connection,
    _snapshot_database_path,
    _revalidate_operation_path_snapshot,
    _revalidate_connection_path,
    _revalidate_connection_alias_free_path,
    _connection_operation_file_size,
    _connection_has_path_snapshot,
    _connection_authority_binding,
    _has_fork_unsafe_connection_authority,
    _connection_authority_state,
    _arm_connection_authority_fault,
) = _build_connection_authority()
del _build_connection_authority


_require_fork_safe_connection_state = _build_fork_safe_connection_requirement(
    _pytest_root_authority_uncertain,
    _has_fork_unsafe_connection_authority,
    _has_process_cleanup_uncertainty,
)
del _build_fork_safe_connection_requirement


def _capture_fork_guard(  # noqa: UP047
    function: Callable[_Parameters, _Result],
) -> Callable[_Parameters, _Result]:
    """Put an immutable fork guard outside evidence-executor fast paths."""

    require_fork_safe = _require_fork_safe_connection_state

    def guarded(
        *args: _Parameters.args,
        **kwargs: _Parameters.kwargs,
    ) -> _Result:
        require_fork_safe()
        return function(*args, **kwargs)

    guarded.__name__ = function.__name__
    guarded.__qualname__ = function.__qualname__
    guarded.__doc__ = function.__doc__
    guarded.__module__ = function.__module__
    guarded.__annotations__ = dict(function.__annotations__)
    guarded.__signature__ = inspect.signature(function)  # type: ignore[attr-defined]
    return guarded


def _open_operation_path_snapshot(
    identity: _RegisteredIdentity,
) -> _OperationPathSnapshot:
    """Pin the owned generation and reject every observed alias before SQLite opens it."""

    acquisition = _begin_operation_path_acquisition(identity)
    pinned: list[_PinnedFile] = []
    try:
        _, generation_descriptor = _open_owned_generation(
            identity,
            acquisition=acquisition,
        )
        names = tuple(sorted(os.listdir(generation_descriptor)))
        if (
            _DATABASE_BASENAME not in names
            or set(names) - _OWNED_DATABASE_FILENAMES
            or len(names) != len(set(names))
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        for name in names:
            descriptor = _open_and_adopt_operation_path_descriptor(
                acquisition,
                "file",
                name,
                False,
            )
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
        return _complete_operation_path_acquisition(
            acquisition,
            tuple(pinned),
        )
    except BaseException as error:
        try:
            _fail_operation_path_acquisition(acquisition)
        except HarnessFailure:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _owned_file_size(
    identity: _RegisteredIdentity,
    name: str,
    *,
    required: bool,
    maximum: int,
) -> int:
    acquisition = _begin_operation_path_acquisition(identity)
    snapshot: _OperationPathSnapshot | None = None
    result: int | None = None
    try:
        _, _ = _open_owned_generation(
            identity,
            acquisition=acquisition,
        )
        file_descriptor = _open_and_adopt_operation_path_descriptor(
            acquisition,
            "file",
            name,
            not required,
        )
        pinned: tuple[_PinnedFile, ...] = ()
        if file_descriptor < 0:
            result = 0
        else:
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
            result = details.st_size
            pinned = (
                _PinnedFile(
                    name=name,
                    descriptor=file_descriptor,
                    device=details.st_dev,
                    inode=details.st_ino,
                    uid=details.st_uid,
                    mode=stat.S_IMODE(details.st_mode),
                    link_count=details.st_nlink,
                ),
            )
        snapshot = _complete_operation_path_acquisition(acquisition, pinned)
    except BaseException as error:
        try:
            _fail_operation_path_acquisition(acquisition)
        except HarnessFailure:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    try:
        _close_operation_path_snapshot(snapshot)
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if result is None:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return result


def _require_token(token: StoreToken) -> _RegisteredIdentity:
    if type(token) is not StoreToken or type(token._nonce) is not bytes or len(token._nonce) != 32:
        raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
    registered = _lookup_store_token_authority(token)
    if registered is None:
        raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
    active_root = registered.pytest_registration
    if (
        _lookup_active_pytest_root(active_root.path_object) is not active_root
        or active_root.resolved_path != registered.pytest_root
        or active_root.device != registered.pytest_root_device
        or active_root.inode != registered.pytest_root_inode
        or active_root.uid != registered.pytest_root_uid
        or active_root.mode != registered.pytest_root_mode
        or not _pytest_root_session_owns(active_root, True)
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


def _build_protected_token_requirement(
    implementation: Callable[[StoreToken], _RegisteredIdentity],
    cleanup_uncertain: Callable[[], bool],
) -> Callable[[StoreToken], _RegisteredIdentity]:
    """Reject shared cleanup poison before any token-backed path observation."""

    def require(token: StoreToken) -> _RegisteredIdentity:
        if cleanup_uncertain():
            raise HarnessFailure(HarnessFailureCode.INVALID_TOKEN)
        return implementation(token)

    return require


_require_token = _build_protected_token_requirement(  # type: ignore[assignment]
    _require_token,
    _has_process_cleanup_uncertainty,
)
del _build_protected_token_requirement


def _require_direct_token_root_pair(
    token: StoreToken,
    pytest_root: Path,
) -> _RegisteredIdentity:
    """Reject cross-scope direct API calls before any source or destination access."""

    if (
        type(token) is not StoreToken
        or type(token._nonce) is not bytes
        or len(token._nonce) != 32
        or not isinstance(pytest_root, Path)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    registered = _lookup_store_token_authority(token)
    if registered is None or not _cleanup_token_matches_identity(token, registered):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    active = registered.pytest_registration
    if (
        pytest_root is not active.path_object
        or _lookup_active_pytest_root(pytest_root) is not active
        or not _pytest_root_session_owns(active, False)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validate_bootstrap_root(pytest_root)
    if _require_token(token) != registered:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return registered


def _database_uri(path: Path) -> str:
    uri = path.as_uri()
    if "?" in uri:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    return f"{uri}?mode=rw"


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
        registry = tuple(getattr(self, "_task064_open_cursors", ()))
        for cursor in registry:
            with suppress(BaseException):
                cursor.close()
        _close_live_connection(self)

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
    ("cache_size", -8192),
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
    try:
        _register_live_connection(connection, snapshot, token._nonce, writer)
    except BaseException as error:
        try:
            _discard_failed_connection_open(connection, snapshot, token._nonce, writer)
        except HarnessFailure:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, MemoryError):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        raise
    try:
        _bind_live_connection_token(connection, token)
        connection.row_factory = sqlite3.Row
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
        _capture_connection_immutable_runtime(connection)
        database_list = _fetch_all(connection, "PRAGMA database_list")
        if len(database_list) != 1 or database_list[0]["name"] != "main":
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        observed_path = _resolve_existing(
            Path(database_list[0]["file"]),
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
            ("cache_size", cast(int, _pragma_scalar(connection, "cache_size"))),
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
        runtime_evidence = _seal_connection_immutable_runtime(connection)
        profile = RuntimeProfile(
            role="writer" if writer else "reader",
            python_version=runtime_evidence.python_version,
            sqlite_version=runtime_evidence.sqlite_version,
            sqlite_source_id=runtime_evidence.sqlite_source_id,
            threadsafety=runtime_evidence.threadsafety,
            compile_options=runtime_evidence.compile_options,
            dbconfig=observed_dbconfig,
            defensive_available=defensive_available,
            defensive_enabled=defensive_enabled,
            pragmas=observed_pragmas,
            limits=tuple(
                (name, connection.getlimit(cast(int, getattr(sqlite3, name))))
                for name, _ in _LIMITS
            ),
        )
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
    """Remove an internally named generation with positively proven descriptor closure."""

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
    completed = False
    primary: BaseException | None = None
    close_ok = True
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
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        for name in names:
            details = os.stat(
                name,
                dir_fd=generation_descriptor,
                follow_symlinks=False,
            )
            if not (stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode)):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        for name in sorted(names):
            os.unlink(name, dir_fd=generation_descriptor)
        os.rmdir(generation_root.name, dir_fd=root_descriptor)
        completed = True
    except BaseException as error:
        primary = error
    finally:
        if generation_descriptor >= 0:
            descriptor = generation_descriptor
            generation_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                close_ok = False
        if root_descriptor >= 0:
            descriptor = root_descriptor
            root_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                close_ok = False
    if not close_ok:
        _mark_process_cleanup_uncertain()
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if primary is not None or not completed:
        if isinstance(primary, HarnessFailure):
            raise primary
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _cleanup_token_matches_identity(
    token: StoreToken,
    registered: _RegisteredIdentity,
) -> bool:
    """Check the exact registry-bound token without requiring a live pytest scope."""

    return bool(
        type(token) is StoreToken
        and type(token._nonce) is bytes
        and len(token._nonce) == 32
        and token._pytest_root == registered.pytest_root
        and token._generation_root == registered.generation_root
        and token._database_path == registered.database_path
        and token._device == registered.device
        and token._inode == registered.inode
        and token._uid == registered.uid
        and token._mode == registered.mode
        and token._link_count == registered.link_count
    )


def _open_owned_generation_for_cleanup(
    registered: _RegisteredIdentity,
) -> tuple[int, int]:
    """Open an exact registered generation even after a partial prior cleanup."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    root_descriptor = -1
    generation_descriptor = -1
    try:
        root_descriptor = os.open(registered.pytest_root, flags)
        root_details = os.fstat(root_descriptor)
        if (
            root_details.st_dev != registered.pytest_root_device
            or root_details.st_ino != registered.pytest_root_inode
            or root_details.st_uid != registered.pytest_root_uid
            or stat.S_IMODE(root_details.st_mode) != registered.pytest_root_mode
            or not stat.S_ISDIR(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        try:
            generation_descriptor = os.open(
                registered.generation_root.name,
                flags,
                dir_fd=root_descriptor,
            )
        except FileNotFoundError:
            for name in os.listdir(root_descriptor):
                details = os.stat(
                    name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                if (
                    details.st_dev == registered.generation_device
                    and details.st_ino == registered.generation_inode
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            return root_descriptor, -1
        generation_details = os.fstat(generation_descriptor)
        generation_entry = os.stat(
            registered.generation_root.name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        if (
            generation_details.st_dev != registered.generation_device
            or generation_details.st_ino != registered.generation_inode
            or generation_details.st_uid != registered.generation_uid
            or stat.S_IMODE(generation_details.st_mode) != registered.generation_mode
            or not stat.S_ISDIR(generation_details.st_mode)
            or generation_entry.st_dev != generation_details.st_dev
            or generation_entry.st_ino != generation_details.st_ino
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        return root_descriptor, generation_descriptor
    except BaseException as error:
        cleanup_ok = _close_descriptors(
            tuple(
                descriptor
                for descriptor in (generation_descriptor, root_descriptor)
                if descriptor >= 0
            )
        )
        if not cleanup_ok:
            _mark_process_cleanup_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from error
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


def _remove_owned_files(token: StoreToken) -> None:
    """Totally remove one owned generation or retain observable retry authority."""

    if type(token) is not StoreToken or type(token._nonce) is not bytes or len(token._nonce) != 32:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    registered = _lookup_store_token_authority(token)
    if registered is None:
        if _is_retired_store_token(token):
            return
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if not _cleanup_token_matches_identity(token, registered):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    root_descriptor = -1
    generation_descriptor = -1
    completed = False
    primary: BaseException | None = None
    close_ok = True
    try:
        root_descriptor, generation_descriptor = _open_owned_generation_for_cleanup(registered)
        if generation_descriptor < 0:
            completed = True
        else:
            names = set(os.listdir(generation_descriptor))
            if names - _OWNED_DATABASE_FILENAMES:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            for name in names:
                details = os.stat(
                    name,
                    dir_fd=generation_descriptor,
                    follow_symlinks=False,
                )
                if (
                    not stat.S_ISREG(details.st_mode)
                    or details.st_dev != registered.device
                    or details.st_uid != registered.uid
                    or details.st_nlink != 1
                    or stat.S_IMODE(details.st_mode) != 0o600
                    or (name == _DATABASE_BASENAME and details.st_ino != registered.inode)
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            removal_order = (
                *sorted(names - {_DATABASE_BASENAME}),
                *((_DATABASE_BASENAME,) if _DATABASE_BASENAME in names else ()),
            )
            for name in removal_order:
                os.unlink(name, dir_fd=generation_descriptor)
            descriptor = generation_descriptor
            generation_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                close_ok = False
            if close_ok:
                os.rmdir(
                    registered.generation_root.name,
                    dir_fd=root_descriptor,
                )
                completed = True
    except BaseException as error:
        primary = error
    finally:
        if generation_descriptor >= 0:
            descriptor = generation_descriptor
            generation_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                close_ok = False
        if root_descriptor >= 0:
            descriptor = root_descriptor
            root_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                close_ok = False
    if not close_ok:
        _mark_process_cleanup_uncertain()
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    if primary is not None or not completed:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from primary
    if not _revoke_store_token_authority(token, registered):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)


@_operation_evidence_executor
@_store_token_issuing_bootstrap
def bootstrap_store(
    pytest_root: Path,
    issue_token: Callable[[_RegisteredIdentity], StoreToken],
) -> StoreToken:
    """Create one empty, private, same-bootstrap-owned version-one generation."""

    root = _validate_bootstrap_root(pytest_root)
    active_root = _lookup_active_pytest_root(pytest_root)
    if active_root is None:
        raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
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
    try:
        token = issue_token(registered)
    except BaseException:
        _remove_unregistered_generation(generation_root, database_path)
        raise
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
                fixture_snapshot = _load_schema_fixture_snapshot(
                    expected_descriptor=live_descriptor
                )
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
                        fixture_snapshot.fingerprint_bytes,
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


del _store_token_issuing_bootstrap


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
    fixture_snapshot = _load_schema_fixture_snapshot(fingerprint_first=True)
    fingerprint = fixture_snapshot.fingerprint
    if (
        tuple(metadata)
        != (
            1,
            STORAGE_MARKER,
            USER_VERSION,
            SCHEMA_GENERATION,
            NATURAL_IDENTITY_KEY_VERSION,
            PAGE_SIZE,
            fixture_snapshot.fingerprint_bytes,
        )
        or installed_schema_descriptor(connection) != fixture_snapshot.descriptor
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return fingerprint


def _verify_operation_authority(
    connection: sqlite3.Connection,
    token: StoreToken,
) -> _RegisteredIdentity:
    """Recheck active token authority and every pinned path identity."""

    registered = _require_token(token)
    _revalidate_connection_path(registered, connection)
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
    return _connection_operation_file_size(
        registered,
        connection,
        name,
        required,
        maximum,
    )


def _verify_operation_snapshot(
    connection: sqlite3.Connection,
    token: StoreToken,
    *,
    writer: bool,
) -> str:
    """Repeat every operation-critical identity/control check coherently."""

    transactional = _require_connection_transaction_state(connection, writer)
    if transactional:
        live_authority = _require_live_transaction_authority(
            connection,
            token,
            writer,
        )
        registered = live_authority.registered
        _revalidate_connection_path(registered, connection)
    else:
        live_authority = None
        registered = _verify_operation_authority(connection, token)
    database_list = _fetch_all(connection, "PRAGMA database_list")
    if (
        len(database_list) != 1
        or database_list[0]["name"] != "main"
        or type(database_list[0]["file"]) is not str
        or not database_list[0]["file"]
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if transactional:
        if live_authority is None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if database_list[0]["file"] != live_authority.database_list_path:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        runtime_evidence = _consume_connection_immutable_runtime(
            connection,
            token._nonce,
            writer,
        )
        python_version = runtime_evidence.python_version
        sqlite_version = runtime_evidence.sqlite_version
        threadsafety = runtime_evidence.threadsafety
        source_id = runtime_evidence.sqlite_source_id
        compile_options = runtime_evidence.compile_options
    else:
        if _resolve_existing(
            Path(database_list[0]["file"]),
            code=HarnessFailureCode.UNAVAILABLE,
        ) != _resolve_existing(
            registered.database_path,
            code=HarnessFailureCode.UNAVAILABLE,
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        source_id = cast(str, _fetch_one(connection, "SELECT sqlite_source_id()")[0])
        compile_options = tuple(
            sorted(cast(str, row[0]) for row in _fetch_all(connection, "PRAGMA compile_options"))
        )
        python_version = ".".join(str(part) for part in sys.version_info[:3])
        sqlite_version = sqlite3.sqlite_version
        threadsafety = sqlite3.threadsafety
    if (
        python_version != ACCEPTED_PYTHON_VERSION
        or sqlite_version != ACCEPTED_SQLITE_VERSION
        or source_id != ACCEPTED_SQLITE_SOURCE_ID
        or compile_options != ACCEPTED_COMPILE_OPTIONS
        or threadsafety != ACCEPTED_THREADSAFETY
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
        ("cache_size", -8192),
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
    if transactional:
        final_authority = _require_live_transaction_authority(
            connection,
            token,
            writer,
        )
        if final_authority.registered != registered:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        _revalidate_connection_path(final_authority.registered, connection)
        _revalidate_connection_alias_free_path(final_authority.registered, connection)
    else:
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
    history_cache: _HistorySnapshotCache | None = None
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
                    creation_witness = _creation_from_stream_row(stream)
                    policy = _policy_from_creation(creation_witness)
                    history_cache = _new_history_snapshot_cache()
                    next_version = 1
                    observed_history = 0
                    preceding_snapshot: _ValidatedHistoryRowSnapshot | None = None
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
                        snapshots = _validate_history_page_rows(
                            stream,
                            page,
                            policy=policy,
                            preceding_snapshot=preceding_snapshot,
                            cache=history_cache,
                        )
                        last_version = _require_exact_int(
                            page[-1]["successor_version"],
                            minimum=next_version,
                            maximum=MAX_CONTRACT_INTEGER,
                        )
                        if last_version > current_version:
                            raise HarnessFailure(HarnessFailureCode.CORRUPT)
                        if next_version == 1:
                            creation = snapshots[0].entry
                            if (
                                type(creation) is not ContinuousPublicTradeStreamStoredCreationV1
                                or creation != creation_witness
                            ):
                                raise HarnessFailure(HarnessFailureCode.CORRUPT)
                        observed_history += len(page)
                        history_count += len(page)
                        if stream_count + history_count > maximum_materialized_rows:
                            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
                        preceding_snapshot = snapshots[-1]
                        current_entry = preceding_snapshot.entry
                        del snapshots
                        history_cache.retain_boundary(preceding_snapshot)
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
                    history_cache.invalidate()
                    history_cache = None
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
            if history_cache is not None:
                _invalidate_history_snapshot_cache_preserving_primary(history_cache)
            _rollback_best_effort(connection)
            raise
    finally:
        if history_cache is not None:
            _invalidate_history_snapshot_cache_preserving_primary(history_cache)
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
    envelope: ContinuousPublicTradeStreamEnvelopeV1,
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
            envelope=envelope,
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
    envelope: ContinuousPublicTradeStreamEnvelopeV1,
    envelope_bytes: bytes,
    envelope_digest: str,
    history_root: str,
    predecessor: _DecodedCanonicalHistoryRecord,
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    """Construct a validated value with deterministic transition scopes."""

    from wealth.domain.continuous_public_trade_persistence import (
        ContinuousPublicTradeEvidenceKind,
        ContinuousPublicTradeEvidenceScopeV1,
    )
    from wealth.ports.continuous_public_trade_stream_store import (
        ContinuousPublicTradeStreamStoredEnvelopeV1,
    )

    attachment = envelope.checkpoint.attachment
    payload = envelope.child_creation_payload
    is_attach = transition.transition_kind is ContinuousPublicTradeTransitionKind.ATTACH
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
    if transition.transition_kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        predecessor_envelope = predecessor.successor_envelope
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


def _decode_canonical_history_record(
    record_bytes: bytes,
    *,
    successor_version: int,
    cache: _HistorySnapshotCache,
    retain: bool = True,
) -> _DecodedCanonicalHistoryRecord:
    """Decode one canonical record once without granting it physical-row authority."""

    cached = cache.decoded(record_bytes)
    if cached is not None:
        if cached.record.successor_version != successor_version:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return cached
    record: (
        ContinuousPublicTradeStreamCreationRecordV1 | ContinuousPublicTradeStreamTransitionRecordV1
    )
    if successor_version == 1:
        creation_record = decode_stream_creation_record(record_bytes)
        record = creation_record
        record_digest = stream_creation_digest(creation_record)
        history_root = initial_stream_history_root(creation_record)
        if (
            encode_stream_creation_record(creation_record) != record_bytes
            or creation_record.successor_version != successor_version
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    else:
        transition_record = decode_stream_transition_record(record_bytes)
        record = transition_record
        record_digest = stream_transition_digest(transition_record)
        history_root = next_stream_history_root(
            transition_record.prior_history_root,
            transition_record,
        )
        if (
            encode_stream_transition_record(transition_record) != record_bytes
            or transition_record.successor_version != successor_version
            or transition_record.prior_version != successor_version - 1
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
    envelope_bytes = _exact_blob(bytes.fromhex(record.successor_envelope_hex), maximum=16_384)
    envelope = decode_stream_envelope(envelope_bytes)
    envelope_digest = stream_envelope_digest(envelope)
    if (
        encode_stream_envelope(envelope) != envelope_bytes
        or record.successor_envelope_digest != envelope_digest
        or envelope.checkpoint.version != successor_version
        or envelope.checkpoint.stream_id != record.stream_id
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    decoded = _DecodedCanonicalHistoryRecord(
        record=record,
        canonical_bytes=record_bytes,
        record_digest=record_digest,
        successor_envelope=envelope,
        successor_envelope_bytes=envelope_bytes,
        successor_envelope_digest=envelope_digest,
        history_root=history_root,
    )
    if retain:
        cache.issue_decoded(decoded)
    return decoded


def _predecessor_link_seal(
    decoded: _DecodedCanonicalHistoryRecord,
) -> _HistoryPredecessorLinkSeal:
    version = decoded.record.successor_version
    return _HistoryPredecessorLinkSeal(
        stream_uuid=decoded.record.stream_id.bytes,
        successor_version=version,
        entry_kind=_ENTRY_CREATION if version == 1 else _ENTRY_TRANSITION,
        canonical_bytes=decoded.canonical_bytes,
        record_digest=decoded.record_digest,
        successor_envelope_bytes=decoded.successor_envelope_bytes,
        successor_envelope_digest=decoded.successor_envelope_digest,
        history_root=decoded.history_root,
        recorded_at=decoded.record.recorded_at,
    )


def _history_row_binding(row: sqlite3.Row) -> tuple[tuple[str, ...], tuple[object, ...]]:
    keys = tuple(row.keys())
    if keys != _HISTORY_ROW_COLUMNS:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return keys, tuple(row[key] for key in keys)


def _same_physical_history_row(row: sqlite3.Row, snapshot: _ValidatedHistoryRowSnapshot) -> bool:
    return _history_row_binding(row) == _history_row_binding(snapshot.row)


def _validate_history_snapshot_coherence(snapshot: _ValidatedHistoryRowSnapshot) -> None:
    if type(snapshot) is not _ValidatedHistoryRowSnapshot:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    row = snapshot.row
    decoded = snapshot.decoded
    entry = snapshot.entry
    successor_version = _require_exact_int(
        row["successor_version"], minimum=1, maximum=MAX_CONTRACT_INTEGER
    )
    _require_exact_int(row["history_row_id"], minimum=1, maximum=MAX_CONTRACT_INTEGER)
    _require_exact_int(row["stream_row_id"], minimum=1, maximum=MAX_CONTRACT_INTEGER)
    if (
        type(decoded) is not _DecodedCanonicalHistoryRecord
        or decoded.record.successor_version != successor_version
        or row["record_model_version"] != _MODEL_VERSION
        or row["serialization_version"] != 1
        or row["record_canonical_bytes"] != decoded.canonical_bytes
        or row["record_digest"] != _digest_bytes(decoded.record_digest)
        or row["successor_envelope_canonical_bytes"] != decoded.successor_envelope_bytes
        or row["successor_envelope_digest"] != _digest_bytes(decoded.successor_envelope_digest)
        or row["successor_history_root"] != _digest_bytes(decoded.history_root)
        or entry.record != decoded.record
        or entry.canonical_bytes != decoded.canonical_bytes
        or entry.record_digest != decoded.record_digest
        or entry.successor_envelope.envelope != decoded.successor_envelope
        or entry.successor_envelope.canonical_bytes != decoded.successor_envelope_bytes
        or entry.successor_envelope.envelope_digest != decoded.successor_envelope_digest
        or entry.history_root != decoded.history_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if successor_version == 1:
        if (
            type(decoded.record) is not ContinuousPublicTradeStreamCreationRecordV1
            or type(entry) is not ContinuousPublicTradeStreamStoredCreationV1
            or snapshot.predecessor_link is not None
            or row["entry_kind"] != _ENTRY_CREATION
            or row["prior_version"] is not None
            or row["prior_envelope_digest"] is not None
            or row["prior_history_root"] is not None
            or row["predecessor_record_canonical_bytes"] is not None
            or row["predecessor_record_digest"] is not None
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return
    transition = decoded.record
    predecessor = snapshot.predecessor_link
    if (
        type(transition) is not ContinuousPublicTradeStreamTransitionRecordV1
        or type(entry) is not ContinuousPublicTradeStreamStoredTransitionV1
        or type(predecessor) is not _HistoryPredecessorLinkSeal
        or predecessor.stream_uuid != transition.stream_id.bytes
        or predecessor.successor_version != transition.prior_version
        or predecessor.entry_kind
        != (_ENTRY_CREATION if transition.prior_version == 1 else _ENTRY_TRANSITION)
        or row["entry_kind"] != _ENTRY_TRANSITION
        or row["prior_version"] != transition.prior_version
        or row["prior_envelope_digest"] != _digest_bytes(transition.prior_envelope_digest)
        or row["prior_history_root"] != _digest_bytes(transition.prior_history_root)
        or row["predecessor_record_canonical_bytes"] != predecessor.canonical_bytes
        or row["predecessor_record_digest"] != _digest_bytes(predecessor.record_digest)
        or predecessor.successor_envelope_digest != transition.prior_envelope_digest
        or predecessor.history_root != transition.prior_history_root
        or predecessor.recorded_at > transition.recorded_at
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _validate_predecessor_row_binding(
    row: sqlite3.Row,
    predecessor: _ValidatedHistoryRowSnapshot,
    transition: ContinuousPublicTradeStreamTransitionRecordV1,
    *,
    cache: _HistorySnapshotCache,
) -> None:
    cache.require_snapshot(predecessor)
    expected_kind = _ENTRY_CREATION if transition.prior_version == 1 else _ENTRY_TRANSITION
    prior = predecessor.row
    if (
        prior["stream_row_id"] != row["stream_row_id"]
        or prior["successor_version"] != transition.prior_version
        or prior["entry_kind"] != expected_kind
        or prior["record_model_version"] != _MODEL_VERSION
        or prior["serialization_version"] != 1
        or prior["record_canonical_bytes"] != row["predecessor_record_canonical_bytes"]
        or prior["record_digest"] != row["predecessor_record_digest"]
        or prior["successor_envelope_digest"] != row["prior_envelope_digest"]
        or prior["successor_history_root"] != row["prior_history_root"]
        or predecessor.decoded.canonical_bytes != row["predecessor_record_canonical_bytes"]
        or predecessor.decoded.record.successor_version != transition.prior_version
        or predecessor.decoded.record.stream_id != transition.stream_id
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


def _history_snapshot_from_row_unchecked(
    row: sqlite3.Row,
    *,
    policy: ContinuousPublicTradePolicy,
    predecessor: _ValidatedHistoryRowSnapshot | None = None,
    cache: _HistorySnapshotCache,
    retain_embedded_predecessor: bool = True,
) -> _ValidatedHistoryRowSnapshot:
    """Decode and revalidate one retained physical row exactly once per local scope."""

    stream_row_id = _require_exact_int(
        row["stream_row_id"], minimum=1, maximum=MAX_CONTRACT_INTEGER
    )
    successor_version = _require_exact_int(
        row["successor_version"],
        minimum=1,
        maximum=MAX_CONTRACT_INTEGER,
    )
    row_key = (stream_row_id, successor_version)
    cached_row = cache.cached_row(row_key)
    if cached_row is not None:
        if not _same_physical_history_row(row, cached_row):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if predecessor is not None:
            cached_record = cached_row.decoded.record
            if type(cached_record) is not ContinuousPublicTradeStreamTransitionRecordV1:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _validate_predecessor_row_binding(
                row,
                predecessor,
                cached_record,
                cache=cache,
            )
        return cached_row
    if row["record_model_version"] != _MODEL_VERSION or row["serialization_version"] != 1:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    record_bytes = _exact_blob(row["record_canonical_bytes"], maximum=65_536)
    decoded = _decode_canonical_history_record(
        record_bytes,
        successor_version=successor_version,
        cache=cache,
    )
    envelope_bytes = _exact_blob(
        row["successor_envelope_canonical_bytes"],
        maximum=16_384,
    )
    if (
        envelope_bytes != decoded.successor_envelope_bytes
        or _decode_digest(row["record_digest"]) != decoded.record_digest
        or _decode_digest(row["successor_envelope_digest"]) != decoded.successor_envelope_digest
        or _decode_digest(row["successor_history_root"]) != decoded.history_root
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    predecessor_decoded: _DecodedCanonicalHistoryRecord | None = None
    if successor_version == 1:
        creation = decoded.record
        if type(creation) is not ContinuousPublicTradeStreamCreationRecordV1 or (
            row["entry_kind"] != _ENTRY_CREATION
            or row["prior_version"] is not None
            or row["prior_envelope_digest"] is not None
            or row["prior_history_root"] is not None
            or row["predecessor_record_canonical_bytes"] is not None
            or row["predecessor_record_digest"] is not None
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entry: ContinuousPublicTradeStreamStoredHistoryEntryV1 = _stored_creation_without_scope(
            creation=creation,
            record_bytes=record_bytes,
            record_digest=decoded.record_digest,
            envelope=decoded.successor_envelope,
            envelope_bytes=envelope_bytes,
            envelope_digest=decoded.successor_envelope_digest,
            history_root=decoded.history_root,
        )
    else:
        transition = decoded.record
        if type(transition) is not ContinuousPublicTradeStreamTransitionRecordV1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        predecessor_bytes = _exact_blob(
            row["predecessor_record_canonical_bytes"],
            maximum=65_536,
        )
        if predecessor is None:
            predecessor_decoded = _decode_canonical_history_record(
                predecessor_bytes,
                successor_version=transition.prior_version,
                cache=cache,
                retain=retain_embedded_predecessor,
            )
        else:
            _validate_predecessor_row_binding(
                row,
                predecessor,
                transition,
                cache=cache,
            )
            predecessor_decoded = predecessor.decoded
        if (
            row["entry_kind"] != _ENTRY_TRANSITION
            or row["prior_version"] != transition.prior_version
            or _decode_digest(row["prior_envelope_digest"]) != transition.prior_envelope_digest
            or _decode_digest(row["prior_history_root"]) != transition.prior_history_root
            or _decode_digest(row["predecessor_record_digest"]) != predecessor_decoded.record_digest
            or predecessor_bytes != predecessor_decoded.canonical_bytes
            or predecessor_decoded.successor_envelope_digest != transition.prior_envelope_digest
            or predecessor_decoded.history_root != transition.prior_history_root
            or predecessor_decoded.record.stream_id != transition.stream_id
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        entry = _stored_transition_without_scopes(
            transition=transition,
            record_bytes=record_bytes,
            record_digest=decoded.record_digest,
            envelope=decoded.successor_envelope,
            envelope_bytes=envelope_bytes,
            envelope_digest=decoded.successor_envelope_digest,
            history_root=decoded.history_root,
            predecessor=predecessor_decoded,
        )
        _validate_transition_against_prior(
            entry,
            prior_envelope=predecessor_decoded.successor_envelope,
            prior_history_root=predecessor_decoded.history_root,
            prior_recorded_at=predecessor_decoded.record.recorded_at,
            policy=policy,
        )
    snapshot = _ValidatedHistoryRowSnapshot(
        row=row,
        decoded=decoded,
        predecessor_link=(
            None
            if successor_version == 1
            else _predecessor_link_seal(cast(_DecodedCanonicalHistoryRecord, predecessor_decoded))
        ),
        entry=entry,
    )
    cache.issue_snapshot(snapshot)
    return snapshot


def _history_snapshot_from_row(
    row: sqlite3.Row,
    *,
    policy: ContinuousPublicTradePolicy,
    predecessor: _ValidatedHistoryRowSnapshot | None = None,
    cache: _HistorySnapshotCache | None = None,
    retain_embedded_predecessor: bool = True,
) -> _ValidatedHistoryRowSnapshot:
    local_cache = _new_history_snapshot_cache() if cache is None else cache
    owns_cache = cache is None
    try:
        result = _history_snapshot_from_row_unchecked(
            row,
            policy=policy,
            predecessor=predecessor,
            cache=local_cache,
            retain_embedded_predecessor=retain_embedded_predecessor,
        )
    except HarnessFailure:
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise
    except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    except BaseException:
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise
    if owns_cache:
        local_cache.invalidate()
    return result


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


def _validate_bounded_current_unchecked(
    stream: sqlite3.Row,
    history: Sequence[sqlite3.Row],
    *,
    cache: _HistorySnapshotCache,
) -> tuple[
    _ValidatedHistoryRowSnapshot,
    _ValidatedHistoryRowSnapshot | None,
    _ValidatedHistoryRowSnapshot,
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
    creation_snapshot = _history_snapshot_from_row(
        by_version[1],
        policy=policy,
        cache=cache,
    )
    creation = creation_snapshot.entry
    if (
        type(creation) is not ContinuousPublicTradeStreamStoredCreationV1
        or creation != creation_witness
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if current_version == 1:
        predecessor_snapshot = None
        current_snapshot = creation_snapshot
    else:
        predecessor_version = current_version - 1
        if predecessor_version == 1:
            predecessor_snapshot = creation_snapshot
        else:
            predecessor_snapshot = _history_snapshot_from_row(
                by_version[predecessor_version],
                policy=policy,
                predecessor=creation_snapshot if predecessor_version == 2 else None,
                cache=cache,
            )
        current_snapshot = _history_snapshot_from_row(
            by_version[current_version],
            policy=policy,
            predecessor=predecessor_snapshot,
            cache=cache,
        )
    predecessor = None if predecessor_snapshot is None else predecessor_snapshot.entry
    current = current_snapshot.entry
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
    return creation_snapshot, predecessor_snapshot, current_snapshot


def _validate_bounded_current(
    stream: sqlite3.Row,
    history: Sequence[sqlite3.Row],
    *,
    cache: _HistorySnapshotCache | None = None,
) -> tuple[
    _ValidatedHistoryRowSnapshot,
    _ValidatedHistoryRowSnapshot | None,
    _ValidatedHistoryRowSnapshot,
]:
    local_cache = _new_history_snapshot_cache() if cache is None else cache
    owns_cache = cache is None
    try:
        result = _validate_bounded_current_unchecked(
            stream,
            history,
            cache=local_cache,
        )
    except BaseException:
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise
    if owns_cache:
        local_cache.invalidate()
    return result


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


def _expectation_matches_creation(
    expectation: ContinuousPublicTradeStreamExpectationV1,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
) -> bool:
    """Compare immutable identity and the complete effective stream policy."""

    identity = expectation.identity
    record = creation.record
    return not (
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
    )


def _expectation_matches_retained(
    expectation: ContinuousPublicTradeStreamExpectationV1,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    current_envelope: ContinuousPublicTradeStreamEnvelopeV1,
) -> bool:
    """Compare a boundary-revalidated expectation with one fully valid retained view."""

    if not _expectation_matches_creation(expectation, creation):
        return False
    record = creation.record
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


def _cas_expectation_matches_transition(
    expectation: ContinuousPublicTradeStreamExpectationV1,
    creation: ContinuousPublicTradeStreamStoredCreationV1,
    transition: ContinuousPublicTradeStreamStoredTransitionV1,
) -> bool:
    """Bind CAS expectation child state to the requested historical successor."""

    if not _expectation_matches_creation(expectation, creation):
        return False
    payload = transition.successor_envelope.envelope.child_creation_payload
    completion_scope = transition.child_completion_scope
    expected_child_policy_fingerprint = (
        payload.child_checkpoint.policy_fingerprint
        if payload is not None
        else (completion_scope.child_policy_fingerprint if completion_scope is not None else None)
    )
    return bool(expectation.effective_child_policy_fingerprint == expected_child_policy_fingerprint)


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
    with suppress(BaseException):
        connection.close()


def _close_checked(connection: sqlite3.Connection) -> None:
    try:
        connection.close()
    except sqlite3.Error as error:
        raise _sqlite_failure(error) from None
    except MemoryError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except Exception:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None


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


def _validated_identity_candidate_views(
    connection: sqlite3.Connection,
    streams: Sequence[sqlite3.Row],
) -> tuple[tuple[_ValidatedIdentityCandidate, ...], int]:
    if not streams or len(streams) > 2:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    candidates: list[_ValidatedIdentityCandidate] = []
    history_rows = 0
    for stream in streams:
        bounded_rows = _history_rows_for_current(connection, stream)
        creation_snapshot, _, current_snapshot = _validate_bounded_current(stream, bounded_rows)
        creation = creation_snapshot.entry
        current = current_snapshot.entry
        if type(creation) is not ContinuousPublicTradeStreamStoredCreationV1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        history_rows += len(bounded_rows)
        candidates.append(
            _ValidatedIdentityCandidate(
                creation=creation,
                current=current,
            )
        )
    return tuple(candidates), history_rows


def _validate_identity_candidates(
    connection: sqlite3.Connection,
    streams: Sequence[sqlite3.Row],
) -> tuple[tuple[ContinuousPublicTradeStreamStoredCreationV1, ...], int]:
    candidates, history_rows = _validated_identity_candidate_views(
        connection,
        streams,
    )
    return tuple(candidate.creation for candidate in candidates), history_rows


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
    stream_rows = 0
    history_rows = 0
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
        stream_rows += len(candidates)
        if candidates:
            retained, retained_rows = _validate_identity_candidates(
                connection,
                candidates,
            )
            history_rows += retained_rows
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
                stream_rows,
                history_rows,
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
            stream_rows,
            history_rows,
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
        creation_snapshot, predecessor_snapshot, current_snapshot = _validate_bounded_current(
            stream, rows
        )
        creation = creation_snapshot.entry
        predecessor = None if predecessor_snapshot is None else predecessor_snapshot.entry
        current = current_snapshot.entry
        if type(creation) is not ContinuousPublicTradeStreamStoredCreationV1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
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
    stream_rows = 0
    history_rows = 0
    committed = False
    history_cache: _HistorySnapshotCache | None = None
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
        stream_rows += len(streams)
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
                candidate_views, retained_rows = _validated_identity_candidate_views(
                    connection,
                    streams,
                )
                history_rows += retained_rows
                if (
                    len(candidate_views) == 1
                    and candidate_views[0].creation.record.stream_id == record.stream_id
                ):
                    target = candidate_views[0]
                    _validate_transition_against_prior(
                        exact,
                        prior_envelope=target.current.successor_envelope.envelope,
                        prior_history_root=target.current.history_root,
                        prior_recorded_at=target.current.record.recorded_at,
                        policy=_policy_from_creation(target.creation),
                    )
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                stream_rows,
                history_rows,
                committed,
            )
        stream = streams[0]
        stream_row_id = _require_exact_int(
            stream["stream_row_id"],
            minimum=1,
            maximum=MAX_CONTRACT_INTEGER,
        )
        current_rows = _history_rows_for_current(connection, stream)
        history_rows += len(current_rows)
        history_cache = _new_history_snapshot_cache()
        creation_snapshot, _, current_snapshot = _validate_bounded_current(
            stream,
            current_rows,
            cache=history_cache,
        )
        creation = creation_snapshot.entry
        current = current_snapshot.entry
        if type(creation) is not ContinuousPublicTradeStreamStoredCreationV1:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        retained_policy = _policy_from_creation(creation)
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
        history_rows += len(history)
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
                    stream_rows,
                    history_rows,
                    committed,
                )
            requested_predecessor_snapshot = _history_snapshot_from_row(
                predecessor_row,
                policy=retained_policy,
                cache=history_cache,
            )
            retained_snapshot = _history_snapshot_from_row(
                successor_row,
                policy=retained_policy,
                predecessor=requested_predecessor_snapshot,
                cache=history_cache,
            )
            retained = retained_snapshot.entry
            requested_predecessor = requested_predecessor_snapshot.entry
            _validate_transition_against_prior(
                exact,
                prior_envelope=requested_predecessor.successor_envelope.envelope,
                prior_history_root=requested_predecessor.history_root,
                prior_recorded_at=requested_predecessor.record.recorded_at,
                policy=retained_policy,
            )
            if current_version < record.successor_version:
                classification = StoreClassification.CORRUPT
            elif (
                retained.canonical_bytes == exact.canonical_bytes
                and retained.record_digest == exact.record_digest
                and retained.successor_envelope.canonical_bytes
                == exact.successor_envelope.canonical_bytes
                and retained.history_root == exact.history_root
                and (
                    exact_expectation is None
                    or _cas_expectation_matches_transition(
                        exact_expectation,
                        creation,
                        exact,
                    )
                )
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
                stream_rows,
                history_rows,
                committed,
            )
        if current_version >= record.successor_version:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CORRUPT,
                statements,
                stream_rows,
                history_rows,
                committed,
            )
        if predecessor_row is None:
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CORRUPT,
                statements,
                stream_rows,
                history_rows,
                committed,
            )
        requested_predecessor_snapshot = _history_snapshot_from_row(
            predecessor_row,
            policy=retained_policy,
            cache=history_cache,
        )
        requested_predecessor = requested_predecessor_snapshot.entry
        _validate_transition_against_prior(
            exact,
            prior_envelope=requested_predecessor.successor_envelope.envelope,
            prior_history_root=requested_predecessor.history_root,
            prior_recorded_at=requested_predecessor.record.recorded_at,
            policy=retained_policy,
        )
        if exact_expectation is not None and not _cas_expectation_matches_transition(
            exact_expectation,
            creation,
            exact,
        ):
            _verify_operation_authority(connection, token)
            connection.execute("COMMIT").close()
            committed = True
            return MutationEvidence(
                StoreClassification.CONFLICT,
                statements,
                stream_rows,
                history_rows,
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
                stream_rows,
                history_rows,
                committed,
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
                stream_rows,
                history_rows,
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
            stream_rows,
            history_rows,
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
        if history_cache is not None:
            _invalidate_history_snapshot_cache_preserving_primary(history_cache)
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


@_capture_fork_guard
@_operation_evidence_executor
def sqlite_result_code_fault_evidence(
    token: StoreToken,
    *,
    seam: str,
) -> FaultEvidence:
    """Probe fixed READONLY or one-attempt BUSY mechanics in fresh processes."""

    if seam not in {"readonly", "busy"}:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _require_fork_safe_connection_state()
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
        _close_descriptors_checked(probe_descriptors)
        return _unproven_process_fault(seam, reason="fork_spawn_failed")
    if probe_process_id == 0:
        connection: sqlite3.Connection | None = None
        code = -1
        exit_code = 70
        try:
            os.close(probe_ready_read)
            os.close(probe_control_write)
            os.close(result_read)
            connection, _ = _connect(token, writer=True)
            _verify_operation_snapshot(connection, token, writer=True)
            _write_process_packet("result_code_probe_ready", probe_ready_write, b"R")
            if _read_process_packet("result_code_probe_control", probe_control_read, 1) != b"C":
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
            try:
                _write_process_packet(
                    "result_code_probe_result", result_write, struct.pack(">i", code)
                )
                os.close(result_write)
                exit_code = 0 if code >= 0 else 70
            except BaseException:
                exit_code = 70
            os._exit(exit_code)

    owned_descriptors = set(probe_descriptors)
    live_processes = {probe_process_id}

    def close_owned(*descriptors: int) -> None:
        for descriptor in descriptors:
            if descriptor not in owned_descriptors:
                continue
            _close_descriptors_checked((descriptor,))
            owned_descriptors.remove(descriptor)

    holder_process_id: int | None = None
    holder_release_write: int | None = None
    holder_ok = True
    payload = b""
    probe_status: int | None = None
    try:
        close_owned(probe_ready_write, probe_control_read, result_write)
        probe_ready = _read_process_packet("result_code_probe_ready", probe_ready_read, 1)
        close_owned(probe_ready_read)
        if probe_ready != b"R":
            return FaultEvidence(
                seam=seam,
                disposition=EvidenceDisposition.UNPROVEN,
                sqlite_errorcode=None,
                observed_syscall=None,
                acknowledgement_bytes=0,
                reopened_state=ReopenedState.UNAVAILABLE,
                reason="probe_open_failed",
            )

        if seam == "busy":
            try:
                holder_ready_pipe, holder_release_pipe = _open_pipes(2)
            except OSError:
                return _unproven_process_fault(seam, reason="ipc_setup_failed")
            holder_ready_read, holder_ready_write = holder_ready_pipe
            holder_release_read, holder_release_write = holder_release_pipe
            holder_descriptors = (
                holder_ready_read,
                holder_ready_write,
                holder_release_read,
                holder_release_write,
            )
            owned_descriptors.update(holder_descriptors)
            try:
                holder_process_id = os.fork()
            except OSError:
                return _unproven_process_fault(seam, reason="fork_spawn_failed")
            if holder_process_id == 0:
                holder_connection: sqlite3.Connection | None = None
                exit_code = 70
                try:
                    os.close(holder_ready_read)
                    os.close(holder_release_write)
                    os.close(probe_control_write)
                    os.close(result_read)
                    holder_connection, _ = _connect(token, writer=True)
                    holder_connection.execute("BEGIN IMMEDIATE").close()
                    _write_process_packet("busy_holder_ready", holder_ready_write, b"R")
                    if _read_process_packet("busy_holder_release", holder_release_read, 1) != b"C":
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                    _verify_operation_authority(holder_connection, token)
                    holder_connection.execute("ROLLBACK").close()
                    _close_checked(holder_connection)
                    holder_connection = None
                    exit_code = 0
                except BaseException:
                    _close_best_effort(holder_connection)
                os._exit(exit_code)
            live_processes.add(holder_process_id)
            close_owned(holder_ready_write, holder_release_read)
            ready = _read_process_packet("busy_holder_ready", holder_ready_read, 1)
            close_owned(holder_ready_read)
            if ready != b"R":
                return FaultEvidence(
                    seam=seam,
                    disposition=EvidenceDisposition.UNPROVEN,
                    sqlite_errorcode=None,
                    observed_syscall=None,
                    acknowledgement_bytes=0,
                    reopened_state=ReopenedState.UNAVAILABLE,
                    reason="busy_holder_failed",
                )

        _write_process_packet("result_code_probe_control", probe_control_write, b"C")
        close_owned(probe_control_write)
        payload = _read_process_packet("result_code_probe_result", result_read, 4)
        probe_status = _wait_or_terminate_owned_process(probe_process_id)
        live_processes.discard(probe_process_id)
        close_owned(result_read)
        if holder_process_id is not None and holder_release_write is not None:
            _write_process_packet("busy_holder_release", holder_release_write, b"C")
            close_owned(holder_release_write)
            holder_status = _wait_or_terminate_owned_process(holder_process_id)
            live_processes.discard(holder_process_id)
            holder_ok = (
                holder_status is not None
                and os.WIFEXITED(holder_status)
                and os.WEXITSTATUS(holder_status) == 0
            )
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
            probe_status is not None
            and os.WIFEXITED(probe_status)
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
    except (ChildProcessError, OSError):
        return _unproven_process_fault(seam, reason="process_protocol_failed")
    finally:
        _finalize_process_resources(
            tuple(owned_descriptors),
            tuple(live_processes),
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


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
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
        _finalize_process_resources(all_descriptors, process_ids)
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
                _write_process_packet("writer_winner_locked", winner_locked_write, b"L")
                if _read_process_packet("writer_winner_release", winner_release_read, 1) != b"R":
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

        packet = b"E"
        try:
            _write_process_packet("writer_winner_ready", winner_ready_write, b"R")
            if _read_process_packet("writer_winner_start", winner_start_read, 1) != b"S":
                os._exit(71)
            packet = _WRITER_OUTCOME_PACKETS.get(run_operation(hold_after_lock), b"E")
        except HarnessFailure as error:
            packet = _WRITER_OUTCOME_PACKETS.get(error.code, b"E")
        except BaseException:
            packet = b"E"
        with suppress(OSError):
            _write_process_packet("writer_winner_result", winner_result_write, packet)
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
            _write_process_packet("writer_contender_ready", contender_ready_write, b"R")
            if _read_process_packet("writer_contender_start", contender_start_read, 1) != b"C":
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
            _write_process_packet("writer_contender_result", contender_result_write, packet)
        os._exit(0 if packet[:1] != b"E" else 70)

    child_descriptors = (
        winner_ready_write,
        winner_start_read,
        winner_locked_write,
        winner_release_read,
        winner_result_write,
        contender_ready_write,
        contender_start_read,
        contender_result_write,
    )

    def read_packet(descriptor: int, size: int) -> bytes:
        return _read_process_packet("writer_contention", descriptor, size)

    live_processes = {winner_process_id, contender_process_id}
    protocol_ok = True
    winner_outcome: StoreClassification | HarnessFailureCode | None = None
    contender_outcome: StoreClassification | HarnessFailureCode | None = None
    contender_code: int | None = None
    owned_descriptors = set(all_descriptors)
    try:
        _close_descriptors_checked(child_descriptors)
        owned_descriptors.difference_update(child_descriptors)
        winner_ready = read_packet(winner_ready_read, 1)
        contender_ready = read_packet(contender_ready_read, 1)
        if winner_ready != b"R" or contender_ready != b"R":
            protocol_ok = False
        if protocol_ok:
            _write_process_packet("writer_winner_start", winner_start_write, b"S")
            winner_locked = read_packet(winner_locked_read, 1)
            if winner_locked != b"L":
                protocol_ok = False
        if protocol_ok:
            _write_process_packet("writer_contender_start", contender_start_write, b"C")
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
            contender_status = _wait_or_terminate_owned_process(contender_process_id)
            live_processes.remove(contender_process_id)
            protocol_ok = (
                protocol_ok
                and contender_status is not None
                and os.WIFEXITED(contender_status)
                and os.WEXITSTATUS(contender_status) == 0
            )
        if protocol_ok:
            _write_process_packet("writer_winner_release", winner_release_write, b"R")
            winner_packet = read_packet(winner_result_read, 1)
            winner_outcome = cast(
                StoreClassification | HarnessFailureCode | None,
                _WRITER_PACKET_OUTCOMES.get(winner_packet),
            )
            winner_status = _wait_or_terminate_owned_process(winner_process_id)
            live_processes.remove(winner_process_id)
            protocol_ok = (
                winner_status is not None
                and os.WIFEXITED(winner_status)
                and os.WEXITSTATUS(winner_status) == 0
                and winner_outcome is not None
            )
    except (OSError, ChildProcessError, struct.error):
        protocol_ok = False
    finally:
        _finalize_process_resources(tuple(owned_descriptors), tuple(live_processes))

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


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
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
            _finalize_process_resources(
                all_descriptors,
                tuple(child_process_id for child_process_id, _, _, _ in children),
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
                _write_process_packet("two_writer_ready", ready_write, b"R")
                if _read_process_packet("two_writer_control", control_read, 1) != b"C":
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
                _write_process_packet("two_writer_result", result_write, packet)
            os._exit(0 if packet != b"E" else 70)
        children.append((process_id, ready_read, control_write, result_read))

    outcomes: list[StoreClassification | HarnessFailureCode] = []
    protocol_ok = True
    live_processes = {process_id for process_id, _, _, _ in children}
    child_descriptors = tuple(
        descriptor
        for _, (ready, control, result) in zip(exact_values, pipes, strict=True)
        for descriptor in (ready[1], control[0], result[1])
    )
    owned_descriptors = set(all_descriptors)
    try:
        _close_descriptors_checked(child_descriptors)
        owned_descriptors.difference_update(child_descriptors)
        for _, ready_read, _, _ in children:
            readiness_packet = _read_process_packet("two_writer_ready", ready_read, 1)
            protocol_ok = protocol_ok and readiness_packet == b"R"
        for process_id, _, control_write, result_read in children:
            if protocol_ok:
                _write_process_packet("two_writer_control", control_write, b"C")
            packet = _read_process_packet("two_writer_result", result_read, 1)
            status = _wait_or_terminate_owned_process(process_id)
            live_processes.remove(process_id)
            decoded_outcome = cast(
                StoreClassification | HarnessFailureCode | None,
                _WRITER_PACKET_OUTCOMES.get(packet),
            )
            if (
                status is None
                or not os.WIFEXITED(status)
                or os.WEXITSTATUS(status) != 0
                or decoded_outcome is None
            ):
                protocol_ok = False
            else:
                outcomes.append(decoded_outcome)
    finally:
        _finalize_process_resources(tuple(owned_descriptors), tuple(live_processes))

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


@_capture_fork_guard
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

    _require_fork_safe_connection_state()
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
        _close_descriptors_checked((read_descriptor, write_descriptor))
        return _unproven_process_fault(seam, reason="fork_spawn_failed")
    if process_id == 0:
        exit_code = 70
        try:
            os.close(read_descriptor)

            def child_hook(observed_seam: str) -> None:
                if observed_seam == seam:
                    _write_process_packet("kill_seam_ready", write_descriptor, b"R")
                    while True:
                        signal.pause()

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
            _write_process_packet("kill_seam_result", write_descriptor, b"A")
            exit_code = 0
        except BaseException:
            with suppress(OSError):
                _write_process_packet("kill_seam_result", write_descriptor, b"E")
        finally:
            with suppress(OSError):
                os.close(write_descriptor)
            os._exit(exit_code)

    observed = b""
    owned_descriptors = {read_descriptor, write_descriptor}
    try:
        _close_descriptors_checked((write_descriptor,))
        owned_descriptors.remove(write_descriptor)
        observed = _read_process_packet("kill_seam_ready", read_descriptor, 1)
    finally:
        _finalize_process_resources(tuple(owned_descriptors), (process_id,))
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


def _trace_commit_page_write(
    process_id: int,
    *,
    ready_read: int,
    ready_write: int,
    control_read: int,
    control_write: int,
    wal_path: str,
) -> tuple[bool, int, str | None]:
    """Boundedly trace one exact child until the WAL frame page write is stopped."""

    descriptors = (ready_read, ready_write, control_read, control_write)
    handshake = b""
    try:
        handshake = _read_process_packet("commit_trace_ready", ready_read, 1)
        if handshake != b"R":
            return False, 1 if handshake == b"A" else 0, "commit_gate_handshake_failed"

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
            return False, 0, "ptrace_seize_failed"
        initial_status = _wait_for_owned_process_event(
            process_id,
            timeout_seconds=10.0,
            include_stopped=True,
        )
        if initial_status is None or not os.WIFSTOPPED(initial_status):
            return False, 0, "ptrace_initial_stop_failed"

        _write_process_packet("commit_trace_control", control_write, b"C")
        pending_header_offset: int | None = None
        completed_header_offset: int | None = None
        deadline = time.monotonic() + 10.0
        for _ in range(20_000):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
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
            status = _wait_for_owned_process_event(
                process_id,
                timeout_seconds=remaining,
                include_stopped=True,
            )
            if status is None or os.WIFEXITED(status) or os.WIFSIGNALED(status):
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
                    return True, 0, None
            elif (
                information.operation == _PTRACE_SYSCALL_INFO_EXIT
                and pending_header_offset is not None
            ):
                if information.payload.exit.return_value == 24:
                    completed_header_offset = pending_header_offset
                pending_header_offset = None
        return False, 0, "wal_commit_write_not_observed"
    except (ChildProcessError, OSError):
        return False, 0, "ptrace_protocol_failed"
    finally:
        _finalize_process_resources(descriptors, (process_id,))


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
    identity = _require_token(token)
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
        _close_descriptors_checked((ready_read, ready_write, control_read, control_write))
        return _unproven_process_fault(
            "true_during_commit",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        exit_code = 70
        try:
            os.close(ready_read)
            os.close(control_write)
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

            def commit_gate(observed_seam: str) -> None:
                if observed_seam == "between_current_update_and_compare_and_swap_commit":
                    _write_process_packet("true_commit_ready", ready_write, b"R")
                    if _read_process_packet("true_commit_control", control_read, 1) != b"C":
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

            compare_and_swap_stream(
                token,
                exact,
                seam_hook=commit_gate,
            )
            _write_process_packet("true_commit_result", ready_write, b"A")
            exit_code = 0
        except BaseException:
            with suppress(OSError):
                _write_process_packet("true_commit_result", ready_write, b"E")
        finally:
            _close_descriptors((ready_write, control_read))
            os._exit(exit_code)

    wal_path = f"{identity.database_path}-wal"
    page_write_stopped, acknowledgement_bytes, trace_reason = _trace_commit_page_write(
        process_id,
        ready_read=ready_read,
        ready_write=ready_write,
        control_read=control_read,
        control_write=control_write,
        wal_path=wal_path,
    )
    if not page_write_stopped:
        return FaultEvidence(
            seam="true_during_commit",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=acknowledgement_bytes,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason=trace_reason,
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


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
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
        _close_descriptors_checked((read_descriptor, write_descriptor))
        return _unproven_process_fault(
            "sqlite_ioerr_write",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        efbig_proven = False
        exit_code = 70
        try:
            os.close(read_descriptor)
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)

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

            compare_and_swap_stream(
                token,
                exact,
                seam_hook=arm_file_limit,
            )
            _write_process_packet("ioerr_result", write_descriptor, b"A")
            exit_code = 0
        except HarnessFailure as error:
            code = error.sqlite_errorcode
            packet = (
                b"E"
                + struct.pack(">i", code if type(code) is int else -1)
                + (b"\x01" if efbig_proven else b"\x00")
            )
            try:
                _write_process_packet("ioerr_result", write_descriptor, packet)
                exit_code = 0
            except OSError:
                exit_code = 70
        except BaseException:
            with suppress(OSError):
                _write_process_packet("ioerr_result", write_descriptor, b"X")
        finally:
            with suppress(OSError):
                os.close(write_descriptor)
            os._exit(exit_code)

    packet = b""
    status: int | None = None
    live_processes = {process_id}
    owned_descriptors = {read_descriptor, write_descriptor}
    try:
        _close_descriptors_checked((write_descriptor,))
        owned_descriptors.remove(write_descriptor)
        packet = _read_process_packet("ioerr_result", read_descriptor, 6)
        status = _wait_or_terminate_owned_process(process_id)
        live_processes.remove(process_id)
    finally:
        _finalize_process_resources(tuple(owned_descriptors), tuple(live_processes))
    if (
        len(packet) != 6
        or packet[:1] != b"E"
        or status is None
        or not os.WIFEXITED(status)
        or os.WEXITSTATUS(status) != 0
    ):
        return FaultEvidence(
            seam="sqlite_ioerr_write",
            disposition=EvidenceDisposition.UNPROVEN,
            sqlite_errorcode=None,
            observed_syscall=None,
            acknowledgement_bytes=1 if packet == b"A" else 0,
            reopened_state=ReopenedState.UNAVAILABLE,
            reason="ioerr_child_protocol_failed",
        )
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


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
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
        _close_descriptors_checked((read_descriptor, write_descriptor))
        return _unproven_process_fault(
            "max_page_count",
            reason="fork_spawn_failed",
        )
    if process_id == 0:
        code = -1
        exit_code = 70
        try:
            os.close(read_descriptor)
            compare_and_swap_stream(
                token,
                exact,
                _test_max_page_count=before.page_count,
            )
        except HarnessFailure as error:
            code = error.sqlite_errorcode if type(error.sqlite_errorcode) is int else -1
        except BaseException:
            code = -1
        finally:
            try:
                _write_process_packet(
                    "max_page_count_result",
                    write_descriptor,
                    struct.pack(">i", code),
                )
                os.close(write_descriptor)
                exit_code = 0 if code >= 0 else 70
            except BaseException:
                exit_code = 70
            os._exit(exit_code)

    payload = b""
    status: int | None = None
    live_processes = {process_id}
    owned_descriptors = {read_descriptor, write_descriptor}
    try:
        _close_descriptors_checked((write_descriptor,))
        owned_descriptors.remove(write_descriptor)
        payload = _read_process_packet("max_page_count_result", read_descriptor, 4)
        status = _wait_or_terminate_owned_process(process_id)
        live_processes.remove(process_id)
    finally:
        _finalize_process_resources(tuple(owned_descriptors), tuple(live_processes))
    child_ok = (
        len(payload) == 4
        and status is not None
        and os.WIFEXITED(status)
        and os.WEXITSTATUS(status) == 0
    )
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


@_capture_fork_guard
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
    _require_fork_safe_connection_state()
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
        _finalize_process_resources(all_descriptors, tuple(live_processes))
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
            _write_process_packet(
                "reader_ready",
                reader_result_write,
                b"R" + struct.pack(">q", initial_version),
            )
            if _read_process_packet("reader_control", reader_control_read, 1) != b"Q":
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
            _write_process_packet(
                "reader_snapshot",
                reader_result_write,
                b"S" + struct.pack(">q", retained_version),
            )
            os._exit(0)
        except BaseException:
            if reader is not None:
                _rollback_best_effort(reader)
                _close_best_effort(reader)
            with suppress(OSError):
                _write_process_packet("reader_result", reader_result_write, b"E")
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
            _write_process_packet("writer_ready", writer_result_write, b"R")
            for item in exact_transitions:
                if _read_process_packet("writer_control", writer_control_read, 1) != b"W":
                    os._exit(71)
                outcome = compare_and_swap_stream(token, item).classification
                if outcome is not StoreClassification.UPDATED:
                    os._exit(72)
                _write_process_packet("writer_ack", writer_result_write, b"W")
            os._exit(0)
        except BaseException:
            with suppress(OSError):
                _write_process_packet("writer_result", writer_result_write, b"E")
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
            _write_process_packet("checkpoint_ready", checkpoint_result_write, b"R")
            for _ in exact_transitions:
                if _read_process_packet("checkpoint_control", checkpoint_control_read, 1) != b"P":
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
                _write_process_packet(
                    "checkpoint_sample",
                    checkpoint_result_write,
                    struct.pack(">qqqq", *packet),
                )
            if _read_process_packet("checkpoint_control", checkpoint_control_read, 1) != b"T":
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
            _write_process_packet(
                "checkpoint_truncate",
                checkpoint_result_write,
                b"T" + struct.pack(">qqq", *cast(tuple[int, int, int], child_truncated)),
            )
            os._exit(0)
        except BaseException:
            _close_best_effort(checkpointer)
            with suppress(OSError):
                _write_process_packet("checkpoint_result", checkpoint_result_write, b"E")
            os._exit(70)

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
            _write_process_packet("writer_control", writer_control_write, b"W")
            if _read_process_packet("writer_ack", writer_result_read, 1) != b"W":
                protocol_ok = False
                break
            acknowledged_writes += 1
            _write_process_packet("checkpoint_control", checkpoint_control_write, b"P")
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
            _write_process_packet("reader_control", reader_control_write, b"Q")
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
            _write_process_packet("checkpoint_control", checkpoint_control_write, b"T")
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
        _finalize_process_resources(all_descriptors, tuple(live_processes))
        live_processes.clear()

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
        envelope=envelope,
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
    policy: ContinuousPublicTradePolicy,
    preceding_snapshot: _ValidatedHistoryRowSnapshot | None = None,
    cache: _HistorySnapshotCache | None = None,
) -> tuple[_ValidatedHistoryRowSnapshot, ...]:
    local_cache = _new_history_snapshot_cache() if cache is None else cache
    owns_cache = cache is None
    try:
        if len(rows) + int(preceding_snapshot is not None) > _MAX_LOCAL_HISTORY_SNAPSHOTS:
            raise HarnessFailure(HarnessFailureCode.BOUNDS_EXCEEDED)
        if preceding_snapshot is not None:
            local_cache.require_snapshot(preceding_snapshot)
        snapshots: list[_ValidatedHistoryRowSnapshot] = []
        prior_snapshot = preceding_snapshot
        expected_stream_row_id = stream["stream_row_id"]
        expected_stream_uuid = stream["stream_uuid"]
        previous_version = (
            None
            if preceding_snapshot is None
            else _require_exact_int(
                preceding_snapshot.row["successor_version"],
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
            snapshot = _history_snapshot_from_row(
                row,
                policy=policy,
                predecessor=prior_snapshot,
                cache=local_cache,
                retain_embedded_predecessor=False,
            )
            entry = snapshot.entry
            if entry.record.stream_id.bytes != expected_stream_uuid:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            snapshots.append(snapshot)
            prior_snapshot = snapshot
            previous_version = version
        result = tuple(snapshots)
    except HarnessFailure:
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise
    except (AttributeError, TypeError, ValueError, OverflowError, UnicodeError):
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    except BaseException:
        _invalidate_history_snapshot_cache_preserving_primary(local_cache)
        raise
    if owns_cache:
        local_cache.invalidate()
    return result


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
        snapshots = _validate_history_page_rows(
            stream,
            rows,
            policy=_policy_from_creation(creation),
        )
        entries = tuple(snapshot.entry for snapshot in snapshots)
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
    acquisition = _begin_operation_path_acquisition(identity)
    snapshot: _OperationPathSnapshot | None = None
    manifest: list[tuple[str, int, str]] = []
    pinned: list[_PinnedFile] = []
    try:
        _, generation_descriptor = _open_owned_generation(
            identity,
            acquisition=acquisition,
        )
        names = tuple(sorted(os.listdir(generation_descriptor)))
        if (
            _DATABASE_BASENAME not in names
            or not names
            or set(names) - permitted_names
            or len(names) != len(set(names))
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        for name in names:
            descriptor = _open_and_adopt_operation_path_descriptor(
                acquisition,
                "file",
                name,
                False,
            )
            before = os.fstat(descriptor)
            maximum = MAX_TEST_DATABASE_BYTES if name == _DATABASE_BASENAME else MAX_TEST_WAL_BYTES
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
            pinned.append(
                _PinnedFile(
                    name=name,
                    descriptor=descriptor,
                    device=before.st_dev,
                    inode=before.st_ino,
                    uid=before.st_uid,
                    mode=stat.S_IMODE(before.st_mode),
                    link_count=before.st_nlink,
                )
            )
            manifest.append((name, total, f"sha256:{digest.hexdigest()}"))
        if tuple(sorted(os.listdir(generation_descriptor))) != names:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if _require_token(token) != identity:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        snapshot = _complete_operation_path_acquisition(
            acquisition,
            tuple(pinned),
        )
    except BaseException as error:
        try:
            _fail_operation_path_acquisition(acquisition)
        except HarnessFailure:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, HarnessFailure):
            raise
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    try:
        _close_operation_path_snapshot(snapshot)
    except HarnessFailure:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    return tuple(manifest)


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

    _require_direct_token_root_pair(source, pytest_root)
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


def _validated_applicable_transition_chain(
    source: StoreToken,
    transitions: object,
) -> tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...]:
    """Read-only validate the complete concurrent-write batch before any fork."""

    if (
        type(transitions) is not tuple
        or len(transitions) != CONCURRENT_BACKUP_TRANSITIONS
        or any(
            type(transition) is not ContinuousPublicTradeStreamStoredTransitionV1
            for transition in transitions
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    exact = tuple(
        _validated_transition(cast(ContinuousPublicTradeStreamStoredTransitionV1, transition))
        for transition in transitions
    )
    identities = tuple(
        (stream_id, natural_key)
        for stream_id, natural_key in _stream_identities(source)
        if stream_id == exact[0].record.stream_id
    )
    if len(identities) != 1:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    stream_id, natural_key = identities[0]
    current = load_current(
        source,
        stream_id=stream_id,
        natural_key=natural_key,
    )
    if (
        current.classification is not StoreClassification.FOUND
        or current.creation is None
        or current.current is None
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    policy = _policy_from_creation(current.creation)
    prior = current.current
    for transition in exact:
        if transition.record.stream_id != stream_id:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _validate_transition_against_prior(
            transition,
            prior_envelope=prior.successor_envelope.envelope,
            prior_history_root=prior.history_root,
            prior_recorded_at=prior.record.recorded_at,
            policy=policy,
        )
        prior = transition
    return exact


@_capture_fork_guard
@_operation_evidence_executor
def concurrent_write_backup_evidence(
    source: StoreToken,
    pytest_root: Path,
    *,
    transitions: tuple[ContinuousPublicTradeStreamStoredTransitionV1, ...],
    evidence_recorded_at_utc: str,
) -> ConcurrentBackupEvidence:
    """Run one online backup while a bounded writer commits exact transitions."""

    _require_fork_safe_connection_state()
    _require_direct_token_root_pair(source, pytest_root)
    exact_transitions = _validated_applicable_transition_chain(source, transitions)
    exact_evidence_time = _validated_evidence_timestamp(evidence_recorded_at_utc)
    source_before = verify_store(source)
    if not hasattr(os, "fork"):
        raise HarnessFailure(HarnessFailureCode.UNPROVEN)
    cas_statements = (
        "BEGIN IMMEDIATE",
        "stream UUID lookup LIMIT 2",
        "history predecessor/successor LIMIT 2",
        "current history LIMIT 3",
        "INSERT transition history",
        "UPDATE current CAS",
        "COMMIT",
    )
    record_size = 4 + hashlib.sha256().digest_size
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
        _close_descriptors_checked((control_read, control_write, result_read, result_write))
        raise HarnessFailure(HarnessFailureCode.UNPROVEN) from None
    if process_id == 0:
        exit_code = 70
        try:
            os.close(control_write)
            os.close(result_read)
            _ACTIVE_EVIDENCE_RUN.set(None)
            _ACTIVE_GATE_OPERATIONS.set(None)
            _ACTIVE_REJECTION_COLLECTOR.set(None)
            _ACTIVE_OPERATION_EXECUTOR_DEPTH.set(0)
            _write_process_packet("concurrent_backup_ready", result_write, b"R")
            if _read_process_packet("concurrent_backup_control", control_read, 1) != b"S":
                raise HarnessFailure(HarnessFailureCode.UNPROVEN)
            packet = bytearray(b"P")
            for transition in exact_transitions:
                mutation = compare_and_swap_stream(source, transition)
                if (
                    type(mutation) is not MutationEvidence
                    or mutation.classification is not StoreClassification.UPDATED
                    or mutation.statements != cas_statements
                    or type(mutation.stream_rows) is not int
                    or not 0 <= mutation.stream_rows <= 2
                    or type(mutation.history_rows) is not int
                    or not 0 <= mutation.history_rows <= 6
                    or mutation.committed is not True
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                packet.extend(
                    struct.pack(
                        ">HH",
                        mutation.stream_rows,
                        mutation.history_rows,
                    )
                )
                packet.extend(bytes.fromhex(_evidence_payload_digest(mutation)))
            if len(packet) != result_packet_size:
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            _write_process_packet(
                "concurrent_backup_result",
                result_write,
                bytes(packet),
            )
            exit_code = 0
        except BaseException:
            with suppress(OSError):
                _write_process_packet("concurrent_backup_failure", result_write, b"F")
        finally:
            if not _close_descriptors((control_read, result_write)):
                exit_code = 70
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
        _finalize_process_resources(
            tuple(
                descriptor
                for descriptor in (
                    control_read,
                    control_write,
                    result_read,
                    result_write,
                )
                if descriptor >= 0
            ),
            (process_id,),
            code=HarnessFailureCode.UNPROVEN,
        )
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
        _write_process_packet("concurrent_backup_control", control_write, b"S")
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
            evidence_recorded_at_utc=exact_evidence_time,
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
            stream_rows, history_rows = struct.unpack(
                ">HH",
                writer_packet[offset : offset + 4],
            )
            offset += 4
            observed_digest = writer_packet[offset : offset + hashlib.sha256().digest_size]
            offset += hashlib.sha256().digest_size
            mutation = MutationEvidence(
                classification=StoreClassification.UPDATED,
                statements=cas_statements,
                stream_rows=stream_rows,
                history_rows=history_rows,
                committed=True,
            )
            if (
                not 0 <= stream_rows <= 2
                or not 0 <= history_rows <= 6
                or observed_digest != bytes.fromhex(_evidence_payload_digest(mutation))
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
        _finalize_process_resources(
            tuple(
                descriptor
                for descriptor in (
                    control_read,
                    control_write,
                    result_read,
                    result_write,
                )
                if descriptor >= 0
            ),
            (process_id,),
            code=HarnessFailureCode.UNPROVEN,
        )


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

    _require_direct_token_root_pair(source, pytest_root)
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


_freeze_operation_evidence_authority()
for _closed_operation_authority_name in (
    "_operation_evidence_executor",
    "_freeze_operation_evidence_authority",
    "_operation_evidence_executor_unsealed",
    "_build_operation_evidence_authority",
    "_capture_fork_guard",
    "_OPERATION_PRODUCERS_FROZEN",
    "_GATE_OPERATION_ALLOWLIST",
    "_FRESH_OPERATION_PRODUCER_SEQUENCE",
    "_ATOMICITY_OPERATION_PRODUCER_SEQUENCE",
    "_BOUNDED_OPERATION_PRODUCER_SEQUENCE",
    "_GATE_OPERATION_PRODUCER_SEQUENCES",
):
    globals().pop(_closed_operation_authority_name, None)
globals().pop("_closed_operation_authority_name", None)


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


def _build_rejection_scenario_authority() -> tuple[
    Callable[[str], _RejectionScenario],
    Callable[[_RejectionScenario, str, HarnessFailureCode, str], bool],
    Callable[[_RejectionScenario], _RejectionScenarioSnapshot],
]:
    """Own the immutable label/code/executor scenario capabilities in one closure."""

    scenarios: dict[str, _RejectionScenario] = {}
    snapshots: dict[int, tuple[_RejectionScenario, _RejectionScenarioSnapshot]] = {}
    for label, code, executor in (
        *_BOOTSTRAP_REJECTION_CONTRACT,
        *_CORRUPTION_REJECTION_CONTRACT,
    ):
        if label in scenarios:
            raise RuntimeError("duplicate TASK064 rejection scenario")
        scenario = _RejectionScenario(
            capability=object(),
            label=label,
            kind=("sqlite" if executor == "sqlite" else "harness"),
            code=code,
            executor_names=(() if executor == "sqlite" else (executor,)),
        )
        scenarios[label] = scenario
        snapshots[id(scenario)] = (
            scenario,
            (
                scenario.label,
                scenario.kind,
                scenario.code,
                scenario.executor_names,
                scenario.normalized_sql,
            ),
        )

    def snapshot_for_scenario(
        scenario: _RejectionScenario,
    ) -> _RejectionScenarioSnapshot:
        record = snapshots.get(id(scenario))
        if (
            record is None
            or record[0] is not scenario
            or (
                scenario.label,
                scenario.kind,
                scenario.code,
                scenario.executor_names,
                scenario.normalized_sql,
            )
            != record[1]
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return record[1]

    def scenario_for_label(label: str) -> _RejectionScenario:
        if type(label) is not str:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        scenario = scenarios.get(label)
        if scenario is None:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        snapshot_for_scenario(scenario)
        return scenario

    def validate(
        scenario: _RejectionScenario,
        label: str,
        code: HarnessFailureCode,
        executor: str,
    ) -> bool:
        expected = scenarios.get(label)
        expected_kind = "sqlite" if executor == "sqlite" else "harness"
        expected_executors = () if executor == "sqlite" else (executor,)
        try:
            current_snapshot = snapshot_for_scenario(scenario)
        except HarnessFailure:
            return False
        return bool(
            expected is scenario
            and current_snapshot == (label, expected_kind, code, expected_executors, None)
        )

    return scenario_for_label, validate, snapshot_for_scenario


(
    _canonical_rejection_scenario,
    _validate_canonical_rejection_scenario,
    _canonical_rejection_scenario_snapshot,
) = _build_rejection_scenario_authority()

(
    capture_harness_rejection,
    capture_sqlite_rejection,
    _validate_issued_rejection_observation,
) = _build_rejection_evidence_authority(_canonical_rejection_scenario_snapshot)
_private_gate_receipt_digest = _build_private_gate_receipt_authority(
    _validate_issued_operation_observation,
    _validate_issued_rejection_observation,
)
(
    _whole_gate_collector,
    _validate_issued_gate_observation,
    _freeze_whole_gate_authority,
) = _build_whole_gate_authority(_private_gate_receipt_digest)
(
    seal_generated_evidence_run,
    _validate_issued_evidence_receipt,
    _prepare_issued_evidence_receipt_consumption,
    _arm_evidence_seal_transition_fault,
) = _build_evidence_receipt_authority(
    _private_gate_receipt_digest,
    _validate_issued_gate_observation,
)


@contextmanager
def _fixed_gate_operation_scope_unbound(
    validate_operation_observation: Callable[
        [_OperationObservation, _EvidenceRun, str, int],
        bool,
    ],
    validate_rejection_observation: Callable[
        [_RejectionObservation, _EvidenceRun, str, int],
        bool,
    ],
    sequence_for_gate: Callable[[str], tuple[str, ...] | None],
    scenario_for_label: Callable[[str], _RejectionScenario],
    validate_scenario: Callable[
        [_RejectionScenario, str, HarnessFailureCode, str],
        bool,
    ],
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
    expected_operation_sequence = sequence_for_gate(gate)
    operation_state = _GateOperationState(run=run, gate=gate, observations=[])
    operation_token = (
        _ACTIVE_GATE_OPERATIONS.set(operation_state)
        if expected_operation_sequence is not None
        else None
    )
    rejection_state: _RejectionCollectorState | None = None
    rejection_token = None
    if rejection_contract:
        rejection_scenarios = tuple(scenario_for_label(label) for label, _, _ in rejection_contract)
        if any(
            not validate_scenario(
                scenario,
                label,
                code,
                executor,
            )
            for scenario, (label, code, executor) in zip(
                rejection_scenarios,
                rejection_contract,
                strict=True,
            )
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        rejection_state = _RejectionCollectorState(
            run=run,
            gate=gate,
            scenarios=rejection_scenarios,
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
            expected_sequence = (
                () if expected_operation_sequence is None else expected_operation_sequence
            )
            if (
                tuple(observation.producer_name for observation in operation_state.observations)
                != expected_sequence
                or any(
                    not validate_operation_observation(
                        observation,
                        run,
                        gate,
                        sequence,
                    )
                    for sequence, observation in enumerate(operation_state.observations)
                )
                or (
                    rejection_state is not None
                    and any(
                        not validate_rejection_observation(
                            observation,
                            run,
                            gate,
                            sequence,
                        )
                        for sequence, observation in enumerate(rejection_state.observations)
                    )
                )
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            ledger.operation_runs[gate] = tuple(operation_state.observations)
            ledger.rejection_runs[gate] = (
                () if rejection_state is None else tuple(rejection_state.observations)
            )


_fixed_gate_operation_scope = partial(
    _fixed_gate_operation_scope_unbound,
    _validate_issued_operation_observation,
    _validate_issued_rejection_observation,
    _canonical_operation_sequence,
    _canonical_rejection_scenario,
    _validate_canonical_rejection_scenario,
)
for _closed_receipt_authority_name in (
    "_fixed_gate_operation_scope_unbound",
    "_validate_issued_operation_observation",
    "_validate_issued_rejection_observation",
    "_canonical_operation_sequence",
    "_canonical_rejection_scenario",
    "_validate_canonical_rejection_scenario",
    "_canonical_rejection_scenario_snapshot",
    "_build_rejection_scenario_authority",
    "_build_rejection_evidence_authority",
    "_build_private_gate_receipt_authority",
    "_private_gate_receipt_digest",
    "_prepare_issued_evidence_run_consumption",
    "_prepare_issued_evidence_run_seal",
    "_seal_generated_evidence_run_unsealed",
    "_build_evidence_receipt_authority",
):
    globals().pop(_closed_receipt_authority_name, None)
globals().pop("_closed_receipt_authority_name", None)


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
    arguments: Mapping[str, object],
    _result: object,
) -> tuple[tuple[str, StoreToken], ...]:
    report_token = arguments.get("report_token")
    if type(report_token) is not StoreToken:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (("report", report_token),)


def _backup_token_roles(
    _arguments: Mapping[str, object],
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
    _arguments: Mapping[str, object],
    result: object,
) -> tuple[tuple[str, StoreToken], ...]:
    if type(result) is not GenerationCopyEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (
        ("source", result.source_token),
        ("destination", result.destination_token),
    )


def _preflight_backup_restore_collector(
    run: _EvidenceRun,
    arguments: Mapping[str, object],
) -> None:
    """Validate every result-independent backup input before source mutation."""

    if (
        set(arguments)
        != {
            "source_token",
            "pytest_root",
            "transitions",
            "backup_recorded_at_utc",
            "restore_recorded_at_utc",
        }
        or type(arguments["source_token"]) is not StoreToken
        or not isinstance(arguments["pytest_root"], Path)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    source = arguments["source_token"]
    pytest_root = arguments["pytest_root"]
    if (
        _require_token(source).pytest_registration is not run._pytest_registration
        or pytest_root is not run._pytest_registration.path_object
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_evidence_timestamp(cast(str, arguments["backup_recorded_at_utc"]))
    _validated_evidence_timestamp(cast(str, arguments["restore_recorded_at_utc"]))
    _validated_applicable_transition_chain(source, arguments["transitions"])


def _preflight_generation_copy_collector(
    run: _EvidenceRun,
    arguments: Mapping[str, object],
) -> None:
    """Require the canonical report/backup token before creating a copy output."""

    if (
        set(arguments) != {"report_token", "pytest_root"}
        or type(arguments["report_token"]) is not StoreToken
        or not isinstance(arguments["pytest_root"], Path)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_token = arguments["report_token"]
    pytest_root = arguments["pytest_root"]
    ledger = _validated_evidence_run(run)
    bootstrap_ordinal = GENERATED_EVIDENCE_GATES.index("bootstrap_path_ownership")
    backup_ordinal = GENERATED_EVIDENCE_GATES.index("backup_restore")
    bootstrap_observation = ledger.observations.get(bootstrap_ordinal)
    backup_observation = ledger.observations.get(backup_ordinal)
    if (
        bootstrap_observation is None
        or backup_observation is None
        or not _validate_issued_gate_observation(
            bootstrap_observation,
            run,
            bootstrap_ordinal,
        )
        or not _validate_issued_gate_observation(
            backup_observation,
            run,
            backup_ordinal,
        )
        or type(bootstrap_observation.value) is not BootstrapPathEvidence
        or type(backup_observation.value) is not BackupRestoreEvidence
        or bootstrap_observation.value.token is not report_token
        or backup_observation.value.backup_token is not report_token
        or pytest_root is not run._pytest_registration.path_object
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _require_direct_token_root_pair(report_token, pytest_root)


def _remove_new_collector_token(
    run: _EvidenceRun,
    token: object,
    *,
    retained_nonces: frozenset[bytes],
) -> None:
    """Remove only one live output generation owned by this invocation."""

    if (
        type(token) is not StoreToken
        or type(token._nonce) is not bytes
        or len(token._nonce) != 32
        or token._nonce in retained_nonces
    ):
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    registered = _lookup_store_token_authority(token)
    if registered is None or registered.pytest_registration is not run._pytest_registration:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    _remove_owned_files(token)


def _rollback_backup_restore_outputs(
    run: _EvidenceRun,
    arguments: Mapping[str, object],
    result: object,
) -> None:
    source_token = arguments.get("source_token")
    if type(source_token) is not StoreToken:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if type(result) is not BackupRestoreEvidence:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    retained = frozenset({source_token._nonce})
    cleanup_ok = True
    for token in (result.restore_token, result.backup_token):
        try:
            _remove_new_collector_token(
                run,
                token,
                retained_nonces=retained,
            )
        except BaseException:
            cleanup_ok = False
    if not cleanup_ok:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)


def _rollback_generation_copy_outputs(
    run: _EvidenceRun,
    arguments: Mapping[str, object],
    result: object,
) -> None:
    report_token = arguments.get("report_token")
    if type(report_token) is not StoreToken:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if type(result) is not GenerationCopyEvidence:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    _remove_new_collector_token(
        run,
        result.destination_token,
        retained_nonces=frozenset({report_token._nonce}),
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
            or observation.scenario.label != label
            or observation.scenario.code is not code
            or observation.scenario.kind != ("sqlite" if executor == "sqlite" else "harness")
            or observation.scenario.executor_names != (() if executor == "sqlite" else (executor,))
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
    expected_operation_sequence = (
        "bootstrap_store",
        "create_stream",
        "compare_and_swap_stream",
        "compare_and_swap_stream",
        "create_stream",
        "compare_and_swap_stream",
        "create_stream",
        "compare_and_swap_stream",
        "compare_and_swap_stream",
        "create_stream",
        "compare_and_swap_stream",
    )
    fresh_observation = ledger.observations.get(
        GENERATED_EVIDENCE_GATES.index("fresh_process_faults")
    )
    operations = ledger.operation_runs.get("atomicity_classification")
    if (
        fresh_observation is None
        or type(fresh_observation.value) is not FreshProcessEvidenceAggregate
        or operations is None
        or len(operations) != len(expected_operation_sequence)
        or tuple(operation.producer_name for operation in operations) != expected_operation_sequence
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
    expected_mutations = (
        ("create_stream", StoreClassification.INSERTED, 0, 0, None),
        ("compare_and_swap_stream", StoreClassification.UPDATED, 1, 2, None),
        ("compare_and_swap_stream", StoreClassification.UPDATED, 1, 3, None),
        ("create_stream", StoreClassification.DUPLICATE, 1, 3, None),
        (
            "compare_and_swap_stream",
            StoreClassification.DUPLICATE,
            1,
            5,
            "historical_duplicate_mature",
        ),
        ("create_stream", StoreClassification.INSERTED, 0, 0, None),
        ("compare_and_swap_stream", StoreClassification.UPDATED, 1, 2, None),
        ("compare_and_swap_stream", StoreClassification.UPDATED, 1, 3, None),
        (
            "create_stream",
            StoreClassification.CONFLICT,
            2,
            6,
            "create_two_candidate_conflict_mature",
        ),
        (
            "compare_and_swap_stream",
            StoreClassification.CONFLICT,
            2,
            6,
            "cas_expectation_two_candidate_conflict_mature",
        ),
    )
    if len(mutation_operations) != len(expected_mutations):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    input_bindings: list[_MutationOperationBinding] = []
    for observation, expected in zip(
        mutation_operations,
        expected_mutations,
        strict=True,
    ):
        producer_name, classification, stream_rows, history_rows, semantic_case = expected
        mutation = observation.result
        input_binding = observation.input_binding
        tag_prefix = (
            classification.value
            if semantic_case is None
            else f"{semantic_case}:{classification.value}"
        )
        if (
            observation.producer_name != producer_name
            or observation.operation_tag
            != (f"{tag_prefix}:stream_rows={stream_rows}:history_rows={history_rows}")
            or type(mutation) is not MutationEvidence
            or mutation.classification is not classification
            or mutation.stream_rows != stream_rows
            or mutation.history_rows != history_rows
            or mutation.committed is not True
            or type(input_binding) is not _MutationOperationBinding
            or observation.input_digest != _evidence_payload_digest(input_binding)
            or observation.payload_digest != _evidence_payload_digest(mutation)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        input_bindings.append(input_binding)
    (
        create_a_binding,
        transition_a_v2_binding,
        transition_a_v3_binding,
        replay_create_a_binding,
        replay_transition_a_v3_binding,
        create_b_binding,
        transition_b_v2_binding,
        transition_b_v3_binding,
        mixed_create_binding,
        mixed_transition_v2_conflict_binding,
    ) = input_bindings
    creation_bindings = (
        create_a_binding,
        replay_create_a_binding,
        create_b_binding,
        mixed_create_binding,
    )
    transition_bindings = (
        transition_a_v2_binding,
        transition_a_v3_binding,
        replay_transition_a_v3_binding,
        transition_b_v2_binding,
        transition_b_v3_binding,
        mixed_transition_v2_conflict_binding,
    )
    if any(
        type(binding.stored_record) is not ContinuousPublicTradeStreamStoredCreationV1
        or binding.policy_digest is None
        or binding.expectation_identity is not None
        or binding.expectation_policy_digest is not None
        or binding.expectation_child_policy_fingerprint is not None
        for binding in creation_bindings
    ) or any(
        type(binding.stored_record) is not ContinuousPublicTradeStreamStoredTransitionV1
        or binding.policy_digest is not None
        for binding in transition_bindings
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creation_a = cast(
        ContinuousPublicTradeStreamStoredCreationV1,
        create_a_binding.stored_record,
    )
    transition_a_v2 = cast(
        ContinuousPublicTradeStreamStoredTransitionV1,
        transition_a_v2_binding.stored_record,
    )
    transition_a_v3 = cast(
        ContinuousPublicTradeStreamStoredTransitionV1,
        transition_a_v3_binding.stored_record,
    )
    creation_b = cast(
        ContinuousPublicTradeStreamStoredCreationV1,
        create_b_binding.stored_record,
    )
    transition_b_v2 = cast(
        ContinuousPublicTradeStreamStoredTransitionV1,
        transition_b_v2_binding.stored_record,
    )
    transition_b_v3 = cast(
        ContinuousPublicTradeStreamStoredTransitionV1,
        transition_b_v3_binding.stored_record,
    )
    mixed_creation = cast(
        ContinuousPublicTradeStreamStoredCreationV1,
        mixed_create_binding.stored_record,
    )
    mixed_transition_v2_conflict = cast(
        ContinuousPublicTradeStreamStoredTransitionV1,
        mixed_transition_v2_conflict_binding.stored_record,
    )
    if (
        create_a_binding.policy_digest != _stored_creation_policy_digest(creation_a)
        or create_b_binding.policy_digest != _stored_creation_policy_digest(creation_b)
        or mixed_create_binding.policy_digest != _stored_creation_policy_digest(mixed_creation)
        or replay_create_a_binding != create_a_binding
        or replay_transition_a_v3_binding != transition_a_v3_binding
        or not _transition_follows_binding(creation_a, transition_a_v2)
        or not _transition_follows_binding(transition_a_v2, transition_a_v3)
        or not _transition_follows_binding(creation_b, transition_b_v2)
        or not _transition_follows_binding(transition_b_v2, transition_b_v3)
        or not _transition_follows_binding(
            mixed_creation,
            mixed_transition_v2_conflict,
        )
        or creation_a.record.stream_id == creation_b.record.stream_id
        or _stored_creation_natural_key(creation_a) == _stored_creation_natural_key(creation_b)
        or mixed_creation.record.stream_id != creation_a.record.stream_id
        or _stored_creation_natural_key(mixed_creation) != _stored_creation_natural_key(creation_b)
        or mixed_creation.record_digest in {creation_a.record_digest, creation_b.record_digest}
        or any(
            binding.expectation_identity is not None
            or binding.expectation_policy_digest is not None
            or binding.expectation_child_policy_fingerprint is not None
            for binding in transition_bindings[:-1]
        )
        or mixed_transition_v2_conflict_binding.expectation_identity
        != _stored_creation_identity_binding(mixed_creation)
        or mixed_transition_v2_conflict_binding.expectation_policy_digest
        != _stored_creation_policy_digest(mixed_creation)
        or mixed_transition_v2_conflict_binding.expectation_child_policy_fingerprint is not None
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creation_a_policy = _policy_from_creation(creation_a)
    creation_b_policy = _policy_from_creation(creation_b)
    mixed_creation_policy = _policy_from_creation(mixed_creation)
    for prior, transition, policy in (
        (creation_a, transition_a_v2, creation_a_policy),
        (transition_a_v2, transition_a_v3, creation_a_policy),
        (creation_b, transition_b_v2, creation_b_policy),
        (transition_b_v2, transition_b_v3, creation_b_policy),
        (mixed_creation, mixed_transition_v2_conflict, mixed_creation_policy),
    ):
        _validate_transition_against_prior(
            transition,
            prior_envelope=prior.successor_envelope.envelope,
            prior_history_root=prior.history_root,
            prior_recorded_at=prior.record.recorded_at,
            policy=policy,
        )
    public_mutations = tuple(
        cast(MutationEvidence, operations[index].result) for index in (1, 4, 9, 2, 5, 10)
    )
    fresh = fresh_observation.value
    return AtomicityClassificationEvidence(
        mutations=public_mutations,
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
    expected_operation_sequence = (
        "load_current",
        "load_current",
        *(("audit_history",) * 5),
        "bootstrap_store",
        "create_stream",
        "compare_and_swap_stream",
        "compare_and_swap_stream",
        "create_stream",
        "compare_and_swap_stream",
        "compare_and_swap_stream",
        "load_current",
        "audit_history",
        "bootstrap_store",
        "create_stream",
        *(("compare_and_swap_stream",) * 102),
        "audit_history",
        "audit_history",
        "query_plan_evidence",
    )
    projection_observation = ledger.observations.get(
        GENERATED_EVIDENCE_GATES.index("projection_roundtrip")
    )
    operations = ledger.operation_runs.get("bounded_queries")
    if (
        projection_observation is None
        or type(projection_observation.value) is not CurrentSlice
        or operations is None
        or len(operations) != len(expected_operation_sequence)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_nonce = report_token._nonce
    conflict_nonce = operations[7].token_nonces
    maximum_nonce = operations[16].token_nonces
    if (
        any(operation.sequence != index for index, operation in enumerate(operations))
        or tuple(operation.producer_name for operation in operations) != expected_operation_sequence
        or any(
            operation.payload_digest != _evidence_payload_digest(operation.result)
            for operation in operations
        )
        or any(operation.token_nonces != (report_nonce,) for operation in operations[:7])
        or len(conflict_nonce) != 1
        or conflict_nonce[0] == report_nonce
        or any(operation.token_nonces != conflict_nonce for operation in operations[7:16])
        or len(maximum_nonce) != 1
        or maximum_nonce[0] in {report_nonce, conflict_nonce[0]}
        or any(operation.token_nonces != maximum_nonce for operation in operations[16:])
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    projection = projection_observation.value
    if projection.creation is None or projection.current is None:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_creation = projection.creation
    report_current = projection.current
    report_natural_key = _stored_creation_natural_key(report_creation)

    def query_binding(index: int) -> _QueryOperationBinding:
        binding = operations[index].input_binding
        if type(binding) is not _QueryOperationBinding or operations[
            index
        ].input_digest != _evidence_payload_digest(binding):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return binding

    def mutation_binding(index: int) -> _MutationOperationBinding:
        binding = operations[index].input_binding
        if type(binding) is not _MutationOperationBinding or operations[
            index
        ].input_digest != _evidence_payload_digest(binding):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return binding

    report_query_bindings = tuple(query_binding(index) for index in range(7))
    (
        report_found_binding,
        report_missing_binding,
        report_initial_one_binding,
        report_initial_ten_binding,
        report_continued_one_binding,
        report_at_tail_binding,
        report_anchor_conflict_binding,
    ) = report_query_bindings
    creation_anchor = (
        report_creation.record.successor_version,
        report_creation.successor_envelope.envelope_digest,
        report_creation.history_root,
    )
    tail_anchor = (
        report_current.record.successor_version,
        report_current.successor_envelope.envelope_digest,
        report_current.history_root,
    )
    if (
        report_found_binding.stream_id != report_creation.record.stream_id
        or report_found_binding.natural_key != report_natural_key
        or report_missing_binding.stream_id == report_creation.record.stream_id
        or report_missing_binding.natural_key == report_natural_key
        or report_missing_binding.expectation_identity is not None
        or report_missing_binding.expectation_policy_digest is not None
        or report_missing_binding.expectation_child_policy_fingerprint is not None
        or any(
            binding.stream_id != report_creation.record.stream_id
            or binding.natural_key != report_natural_key
            or binding.expectation_identity is not None
            or binding.expectation_policy_digest is not None
            or binding.expectation_child_policy_fingerprint is not None
            for binding in (
                report_found_binding,
                report_initial_one_binding,
                report_initial_ten_binding,
                report_continued_one_binding,
                report_at_tail_binding,
                report_anchor_conflict_binding,
            )
        )
        or report_found_binding.limit is not None
        or report_found_binding.continuation is not None
        or report_missing_binding.limit is not None
        or report_missing_binding.continuation is not None
        or (report_initial_one_binding.limit, report_initial_one_binding.continuation) != (1, None)
        or (report_initial_ten_binding.limit, report_initial_ten_binding.continuation) != (10, None)
        or (
            report_continued_one_binding.limit,
            report_continued_one_binding.continuation,
        )
        != (1, creation_anchor)
        or (report_at_tail_binding.limit, report_at_tail_binding.continuation) != (100, tail_anchor)
        or report_anchor_conflict_binding.limit != 1
        or report_anchor_conflict_binding.continuation is None
        or report_anchor_conflict_binding.continuation[0] != creation_anchor[0]
        or report_anchor_conflict_binding.continuation[1] == creation_anchor[1]
        or report_anchor_conflict_binding.continuation[2] != creation_anchor[2]
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    conflict_create_a = mutation_binding(8)
    conflict_a_v2 = mutation_binding(9)
    conflict_a_v3 = mutation_binding(10)
    conflict_create_b = mutation_binding(11)
    conflict_b_v2 = mutation_binding(12)
    conflict_b_v3 = mutation_binding(13)
    conflict_creation_a = conflict_create_a.stored_record
    conflict_transition_a_v2 = conflict_a_v2.stored_record
    conflict_transition_a_v3 = conflict_a_v3.stored_record
    conflict_creation_b = conflict_create_b.stored_record
    conflict_transition_b_v2 = conflict_b_v2.stored_record
    conflict_transition_b_v3 = conflict_b_v3.stored_record
    if (
        type(conflict_creation_a) is not ContinuousPublicTradeStreamStoredCreationV1
        or type(conflict_creation_b) is not ContinuousPublicTradeStreamStoredCreationV1
        or type(conflict_transition_a_v2) is not ContinuousPublicTradeStreamStoredTransitionV1
        or type(conflict_transition_a_v3) is not ContinuousPublicTradeStreamStoredTransitionV1
        or type(conflict_transition_b_v2) is not ContinuousPublicTradeStreamStoredTransitionV1
        or type(conflict_transition_b_v3) is not ContinuousPublicTradeStreamStoredTransitionV1
        or conflict_create_a.policy_digest != _stored_creation_policy_digest(conflict_creation_a)
        or conflict_create_b.policy_digest != _stored_creation_policy_digest(conflict_creation_b)
        or not _transition_follows_binding(
            conflict_creation_a,
            conflict_transition_a_v2,
        )
        or not _transition_follows_binding(
            conflict_transition_a_v2,
            conflict_transition_a_v3,
        )
        or not _transition_follows_binding(
            conflict_creation_b,
            conflict_transition_b_v2,
        )
        or not _transition_follows_binding(
            conflict_transition_b_v2,
            conflict_transition_b_v3,
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    conflict_a_policy = _policy_from_creation(conflict_creation_a)
    conflict_b_policy = _policy_from_creation(conflict_creation_b)
    _validate_transition_against_prior(
        conflict_transition_a_v2,
        prior_envelope=conflict_creation_a.successor_envelope.envelope,
        prior_history_root=conflict_creation_a.history_root,
        prior_recorded_at=conflict_creation_a.record.recorded_at,
        policy=conflict_a_policy,
    )
    _validate_transition_against_prior(
        conflict_transition_a_v3,
        prior_envelope=conflict_transition_a_v2.successor_envelope.envelope,
        prior_history_root=conflict_transition_a_v2.history_root,
        prior_recorded_at=conflict_transition_a_v2.record.recorded_at,
        policy=conflict_a_policy,
    )
    _validate_transition_against_prior(
        conflict_transition_b_v2,
        prior_envelope=conflict_creation_b.successor_envelope.envelope,
        prior_history_root=conflict_creation_b.history_root,
        prior_recorded_at=conflict_creation_b.record.recorded_at,
        policy=conflict_b_policy,
    )
    _validate_transition_against_prior(
        conflict_transition_b_v3,
        prior_envelope=conflict_transition_b_v2.successor_envelope.envelope,
        prior_history_root=conflict_transition_b_v2.history_root,
        prior_recorded_at=conflict_transition_b_v2.record.recorded_at,
        policy=conflict_b_policy,
    )
    conflict_current_binding = query_binding(14)
    conflict_audit_binding = query_binding(15)
    conflict_key_b = _stored_creation_natural_key(conflict_creation_b)
    if (
        conflict_creation_a.record.stream_id == conflict_creation_b.record.stream_id
        or _stored_creation_natural_key(conflict_creation_a) == conflict_key_b
        or any(
            binding.stream_id != conflict_creation_a.record.stream_id
            or binding.natural_key != conflict_key_b
            or binding.expectation_identity is not None
            or binding.expectation_policy_digest is not None
            or binding.expectation_child_policy_fingerprint is not None
            for binding in (conflict_current_binding, conflict_audit_binding)
        )
        or conflict_current_binding.limit is not None
        or conflict_current_binding.continuation is not None
        or (conflict_audit_binding.limit, conflict_audit_binding.continuation) != (100, None)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)

    maximum_create = mutation_binding(17)
    maximum_creation = maximum_create.stored_record
    if (
        type(maximum_creation) is not ContinuousPublicTradeStreamStoredCreationV1
        or maximum_create.policy_digest != _stored_creation_policy_digest(maximum_creation)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    maximum_policy = _policy_from_creation(maximum_creation)
    maximum_prior: ContinuousPublicTradeStreamStoredHistoryEntryV1 = maximum_creation
    for index in range(18, 120):
        binding = mutation_binding(index)
        transition = binding.stored_record
        if type(
            transition
        ) is not ContinuousPublicTradeStreamStoredTransitionV1 or not _transition_follows_binding(
            maximum_prior, transition
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        _validate_transition_against_prior(
            transition,
            prior_envelope=maximum_prior.successor_envelope.envelope,
            prior_history_root=maximum_prior.history_root,
            prior_recorded_at=maximum_prior.record.recorded_at,
            policy=maximum_policy,
        )
        maximum_prior = transition
    maximum_initial_binding = query_binding(120)
    maximum_continued_binding = query_binding(121)
    maximum_plan_binding = query_binding(122)
    maximum_natural_key = _stored_creation_natural_key(maximum_creation)
    maximum_creation_anchor = (
        maximum_creation.record.successor_version,
        maximum_creation.successor_envelope.envelope_digest,
        maximum_creation.history_root,
    )
    if (
        any(
            binding.stream_id != maximum_creation.record.stream_id
            or binding.natural_key != maximum_natural_key
            or binding.expectation_identity is not None
            or binding.expectation_policy_digest is not None
            or binding.expectation_child_policy_fingerprint is not None
            for binding in (
                maximum_initial_binding,
                maximum_continued_binding,
                maximum_plan_binding,
            )
        )
        or (maximum_initial_binding.limit, maximum_initial_binding.continuation) != (100, None)
        or (
            maximum_continued_binding.limit,
            maximum_continued_binding.continuation,
        )
        != (100, maximum_creation_anchor)
        or maximum_plan_binding.limit is not None
        or maximum_plan_binding.continuation is not None
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
    if (
        loads[2].query_evidence.stream_rows,
        loads[2].query_evidence.history_rows,
        loads[2].query_evidence.decoded_rows,
    ) != (2, 6, 6) or (
        audit_conflict.query_evidence.stream_rows,
        audit_conflict.query_evidence.history_rows,
        audit_conflict.query_evidence.decoded_rows,
    ) != (2, 6, 6):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
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


@_whole_gate_collector(
    "backup_restore",
    token_roles=_backup_token_roles,
    preflight=_preflight_backup_restore_collector,
    rollback_outputs=_rollback_backup_restore_outputs,
)
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
    exact_backup_time = _validated_evidence_timestamp(backup_recorded_at_utc)
    exact_restore_time = _validated_evidence_timestamp(restore_recorded_at_utc)
    exact_transitions = _validated_applicable_transition_chain(
        source_token,
        transitions,
    )
    concurrent: ConcurrentBackupEvidence | None = None
    restore_token: StoreToken | None = None
    try:
        concurrent = concurrent_write_backup_evidence(
            source_token,
            pytest_root,
            transitions=exact_transitions,
            evidence_recorded_at_utc=exact_backup_time,
        )
        restore_token, restore_manifest = online_backup(
            concurrent.backup_token,
            pytest_root,
            evidence_recorded_at_utc=exact_restore_time,
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
    except BaseException as error:
        cleanup_ok = True
        for token in (
            restore_token,
            None if concurrent is None else concurrent.backup_token,
        ):
            if token is None:
                continue
            try:
                _remove_owned_files(token)
            except BaseException:
                cleanup_ok = False
        if not cleanup_ok:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from error
        raise


@_whole_gate_collector(
    "generation_copy",
    token_roles=_generation_copy_token_roles,
    preflight=_preflight_generation_copy_collector,
    rollback_outputs=_rollback_generation_copy_outputs,
)
def collect_generation_copy_evidence(
    run: _EvidenceRun,
    report_token: StoreToken,
    pytest_root: Path,
) -> GenerationCopyEvidence:
    del run
    destination: StoreToken | None = None
    try:
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
    except BaseException as error:
        if destination is not None:
            try:
                _remove_owned_files(destination)
            except BaseException:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from error
        raise


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


_freeze_whole_gate_authority()
for _closed_gate_authority_name in (
    "_whole_gate_collector",
    "_freeze_whole_gate_authority",
    "_whole_gate_collector_unsealed",
    "_build_whole_gate_authority",
    "_GATE_COLLECTORS_FROZEN",
):
    globals().pop(_closed_gate_authority_name, None)
globals().pop("_closed_gate_authority_name", None)


def _build_task064_report_artifact_validation() -> tuple[
    Callable[[bytes], None],
    Callable[[], tuple[tuple[str, str], ...]],
]:
    """Close exact sanitized-report validation and source binding over immutable primitives."""

    canonical_load = json.loads
    canonical_dump = json.dumps
    digest_constructor = hashlib.sha256
    path_type = Path
    path_resolve = Path.resolve
    path_joinpath = Path.joinpath
    path_lstat = Path.lstat
    path_read_bytes = Path.read_bytes
    current_uid = os.getuid
    stat_is_link = stat.S_ISLNK
    stat_is_regular = stat.S_ISREG
    stat_mode = stat.S_IMODE
    type_of = type
    dict_type = dict
    list_type = list
    str_type = str
    int_type = int
    bytes_type = bytes
    length_of = len
    tuple_type = tuple
    set_type = set
    failure_type = HarnessFailure
    corrupt_code = HarnessFailureCode.CORRUPT
    validate_timestamp = _validated_evidence_timestamp
    uuid_type = UUID
    maximum_artifact_bytes = 4 * 1024 * 1024
    maximum_database_bytes = MAX_TEST_DATABASE_BYTES
    maximum_wal_bytes = MAX_TEST_WAL_BYTES
    maximum_page_count = MAX_PAGE_COUNT
    maximum_contract_integer = MAX_CONTRACT_INTEGER
    database_basename = _DATABASE_BASENAME
    owned_database_filenames = _OWNED_DATABASE_FILENAMES
    task_id = TASK_ID
    task_contract_generation = TASK_CONTRACT_GENERATION
    task_contract_digest = TASK_CONTRACT_DIGEST
    application_id = APPLICATION_ID
    user_version = USER_VERSION
    schema_generation = SCHEMA_GENERATION
    page_size = PAGE_SIZE
    storage_marker = STORAGE_MARKER.decode("ascii")
    accepted_python_version = ACCEPTED_PYTHON_VERSION
    accepted_sqlite_version = ACCEPTED_SQLITE_VERSION
    accepted_sqlite_source_id = ACCEPTED_SQLITE_SOURCE_ID
    accepted_threadsafety = ACCEPTED_THREADSAFETY
    accepted_compile_options = list(ACCEPTED_COMPILE_OPTIONS)
    workload_seed = WORKLOAD_SEED
    workload_runs = WORKLOAD_RUNS
    record_size_matrix = [list(item) for item in RECORD_SIZE_MATRIX]
    workload_matrix = [list(item) for item in WORKLOAD_MATRIX]
    maximum_operation_latency_ns = MAX_OPERATION_LATENCY_NS
    maximum_traced_memory_bytes = MAX_TEST_TRACED_MEMORY_BYTES
    maximum_open_cursors = MAX_TEST_OPEN_CURSORS
    wal_autocheckpoint_pages = WAL_AUTOCHECKPOINT_PAGES
    expected_schema_fingerprint = load_schema_fingerprint()
    checkout_root = path_resolve(path_type(__file__), strict=True).parents[2]
    source_suffixes = (
        "tests/support/continuous_public_trade_stream_sqlite_harness.py",
        "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
        "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
        "docs/decisions/0032-continuous-public-trade-stream-sqlite-schema-evidence-harness.md",
    )
    source_snapshots: tuple[tuple[str, Path, tuple[int, int, int, int, int], str], ...] = tuple()
    mutable_source_snapshots: list[tuple[str, Path, tuple[int, int, int, int, int], str]] = []
    for source_suffix in source_suffixes:
        source_path = path_joinpath(checkout_root, *source_suffix.split("/"))
        try:
            resolved_source = path_resolve(source_path, strict=True)
            source_details = path_lstat(source_path)
            source_raw = path_read_bytes(source_path)
        except (OSError, RuntimeError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        if (
            resolved_source != source_path
            or stat_is_link(source_details.st_mode)
            or not stat_is_regular(source_details.st_mode)
            or source_details.st_uid != current_uid()
            or source_details.st_size != length_of(source_raw)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        mutable_source_snapshots.append(
            (
                source_suffix,
                source_path,
                (
                    source_details.st_dev,
                    source_details.st_ino,
                    source_details.st_uid,
                    stat_mode(source_details.st_mode),
                    source_details.st_size,
                ),
                digest_constructor(source_raw).hexdigest(),
            )
        )
    source_snapshots = tuple(mutable_source_snapshots)
    del mutable_source_snapshots
    expected_runtime_profiles = [
        {
            "role": profile.role,
            "dbconfig": [list(item) for item in profile.dbconfig],
            "defensive_available": profile.defensive_available,
            "defensive_enabled": profile.defensive_enabled,
            "limits": [list(item) for item in profile.limits],
            "pragmas": [list(item) for item in profile.pragmas],
        }
        for profile in ACCEPTED_CONNECTION_PROFILES
    ]
    expected_gates = [
        *[
            {
                "name": name,
                "disposition": EvidenceDisposition.PASS.value,
                "reason": None,
            }
            for name in GENERATED_EVIDENCE_GATES
        ],
        *[
            {
                "name": name,
                "disposition": EvidenceDisposition.NOT_APPLICABLE.value,
                "reason": TARGET_NOT_APPLICABLE_REASON,
            }
            for name in TARGET_NOT_APPLICABLE_GATES
        ],
    ]

    def invalid() -> Never:
        raise failure_type(corrupt_code)

    def exact_keys(value: object, keys: tuple[str, ...]) -> dict[str, object]:
        if type_of(value) is not dict_type:
            invalid()
        exact_mapping = cast(dict[str, object], value)
        if tuple_type(exact_mapping) != keys:
            invalid()
        return exact_mapping

    def valid_prefixed_digest(value: object) -> bool:
        if type_of(value) is not str_type:
            return False
        exact_value = cast(str, value)
        return bool(
            length_of(exact_value) == 71
            and exact_value.startswith("sha256:")
            and all(character in "0123456789abcdef" for character in exact_value[7:])
        )

    def exact_value(value: object, expected: object) -> bool:
        if type_of(value) is not type_of(expected):
            return False
        if type_of(expected) is dict_type:
            exact_mapping = cast(dict[str, object], value)
            expected_mapping = cast(dict[str, object], expected)
            return bool(
                tuple_type(exact_mapping) == tuple_type(expected_mapping)
                and all(
                    exact_value(exact_mapping[key], expected_mapping[key])
                    for key in expected_mapping
                )
            )
        if type_of(expected) is list_type:
            exact_list = cast(list[object], value)
            expected_list = cast(list[object], expected)
            return bool(
                length_of(exact_list) == length_of(expected_list)
                and all(
                    exact_value(item, expected_item)
                    for item, expected_item in zip(exact_list, expected_list, strict=True)
                )
            )
        return bool(value == expected)

    def exact_int(value: object, *, minimum: int, maximum: int) -> int:
        if type_of(value) is not int_type or not minimum <= cast(int, value) <= maximum:
            invalid()
        return cast(int, value)

    def source_fingerprints() -> tuple[tuple[str, str], ...]:
        result: list[tuple[str, str]] = []
        for suffix, path, expected_identity, expected_digest in source_snapshots:
            try:
                resolved = path_resolve(path, strict=True)
                details = path_lstat(path)
                raw = path_read_bytes(path)
            except (OSError, RuntimeError):
                invalid()
            if (
                resolved != path
                or stat_is_link(details.st_mode)
                or not stat_is_regular(details.st_mode)
                or details.st_uid != current_uid()
                or (
                    details.st_dev,
                    details.st_ino,
                    details.st_uid,
                    stat_mode(details.st_mode),
                    details.st_size,
                )
                != expected_identity
                or length_of(raw) != details.st_size
                or digest_constructor(raw).hexdigest() != expected_digest
            ):
                invalid()
            result.append((suffix, expected_digest))
        return tuple_type(result)

    def validate(raw: bytes) -> None:
        if (
            type_of(raw) is not bytes_type
            or not 0 < length_of(raw) <= maximum_artifact_bytes
            or not raw.endswith(b"\n")
            or raw.endswith(b"\n\n")
        ):
            invalid()
        try:
            text = raw.decode("ascii")
            document = canonical_load(text)
            canonical = (
                canonical_dump(
                    document,
                    allow_nan=False,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("ascii")
                + b"\n"
            )
        except (UnicodeError, TypeError, ValueError, RecursionError):
            invalid()
        if canonical != raw:
            invalid()
        report = exact_keys(
            document,
            (
                "report_version",
                "task",
                "schema",
                "runtime",
                "environment_class",
                "evidence_recorded_at_utc",
                "workload_contract",
                "measurements",
                "backup_manifest",
                "gates",
            ),
        )
        task = exact_keys(
            report["task"],
            ("task_id", "contract_generation", "contract_digest"),
        )
        schema = exact_keys(
            report["schema"],
            (
                "schema_fingerprint",
                "application_id",
                "user_version",
                "schema_generation",
                "page_size",
                "storage_marker",
            ),
        )
        runtime = exact_keys(
            report["runtime"],
            (
                "python_version",
                "sqlite_version",
                "sqlite_source_id",
                "threadsafety",
                "compile_options",
                "connection_profiles",
            ),
        )
        workload = exact_keys(
            report["workload_contract"],
            ("seed", "runs", "record_size_matrix", "workload_matrix", "thresholds"),
        )
        thresholds = exact_keys(
            workload["thresholds"],
            (
                "maximum_operation_latency_ns",
                "maximum_database_bytes",
                "maximum_wal_bytes",
                "maximum_traced_memory_bytes",
                "maximum_open_cursors",
                "maximum_page_count",
                "wal_autocheckpoint_pages",
            ),
        )
        measurements = exact_keys(
            report["measurements"],
            (
                "stream_rows",
                "history_rows",
                "query_rows",
                "database_bytes",
                "wal_bytes",
                "page_count",
                "freelist_count",
                "maximum_open_cursors",
                "peak_traced_memory_bytes",
                "latency_samples_ns",
            ),
        )
        manifest = exact_keys(
            report["backup_manifest"],
            (
                "source_generation_id",
                "destination_generation_id",
                "schema_fingerprint",
                "sqlite_source_id",
                "page_size",
                "source_page_count",
                "destination_page_count",
                "checkpoint_outcome",
                "finalization_outcome",
                "evidence_recorded_at_utc",
                "source_streams",
                "source_history_rows",
                "destination_streams",
                "destination_history_rows",
                "files",
                "per_stream_tails",
            ),
        )
        if (
            report["report_version"] != 1
            or type_of(report["report_version"]) is not int_type
            or not exact_value(
                task,
                {
                    "task_id": task_id,
                    "contract_generation": task_contract_generation,
                    "contract_digest": task_contract_digest,
                },
            )
            or not exact_value(
                schema,
                {
                    "schema_fingerprint": expected_schema_fingerprint,
                    "application_id": application_id,
                    "user_version": user_version,
                    "schema_generation": schema_generation,
                    "page_size": page_size,
                    "storage_marker": storage_marker,
                },
            )
            or not exact_value(
                runtime,
                {
                    "python_version": accepted_python_version,
                    "sqlite_version": accepted_sqlite_version,
                    "sqlite_source_id": accepted_sqlite_source_id,
                    "threadsafety": accepted_threadsafety,
                    "compile_options": accepted_compile_options,
                    "connection_profiles": expected_runtime_profiles,
                },
            )
            or report["environment_class"] != "generated-linux-pytest"
            or type_of(report["evidence_recorded_at_utc"]) is not str_type
            or not exact_value(
                workload,
                {
                    "seed": workload_seed,
                    "runs": workload_runs,
                    "record_size_matrix": record_size_matrix,
                    "workload_matrix": workload_matrix,
                    "thresholds": {
                        "maximum_operation_latency_ns": maximum_operation_latency_ns,
                        "maximum_database_bytes": maximum_database_bytes,
                        "maximum_wal_bytes": maximum_wal_bytes,
                        "maximum_traced_memory_bytes": maximum_traced_memory_bytes,
                        "maximum_open_cursors": maximum_open_cursors,
                        "maximum_page_count": maximum_page_count,
                        "wal_autocheckpoint_pages": wal_autocheckpoint_pages,
                    },
                },
            )
            or thresholds is not workload["thresholds"]
            or not exact_value(report["gates"], expected_gates)
        ):
            invalid()
        validate_timestamp(cast(str, report["evidence_recorded_at_utc"]))
        stream_rows = exact_int(measurements["stream_rows"], minimum=0, maximum=2**63 - 1)
        history_rows = exact_int(measurements["history_rows"], minimum=0, maximum=2**63 - 1)
        exact_int(measurements["query_rows"], minimum=0, maximum=2**63 - 1)
        database_bytes = exact_int(
            measurements["database_bytes"], minimum=0, maximum=maximum_database_bytes
        )
        wal_bytes = exact_int(measurements["wal_bytes"], minimum=0, maximum=maximum_wal_bytes)
        page_count = exact_int(measurements["page_count"], minimum=1, maximum=maximum_page_count)
        freelist_count = exact_int(measurements["freelist_count"], minimum=0, maximum=page_count)
        del database_bytes, wal_bytes, freelist_count
        exact_int(
            measurements["maximum_open_cursors"],
            minimum=1,
            maximum=maximum_open_cursors,
        )
        exact_int(
            measurements["peak_traced_memory_bytes"],
            minimum=0,
            maximum=maximum_traced_memory_bytes,
        )
        latency_samples = measurements["latency_samples_ns"]
        if type_of(latency_samples) is not list_type:
            invalid()
        exact_latency_samples = cast(list[object], latency_samples)
        if length_of(exact_latency_samples) != workload_runs:
            invalid()
        for latency_sample in exact_latency_samples:
            exact_int(
                latency_sample,
                minimum=0,
                maximum=maximum_operation_latency_ns,
            )
        source_generation_id = manifest["source_generation_id"]
        destination_generation_id = manifest["destination_generation_id"]
        source_page_count = exact_int(
            manifest["source_page_count"], minimum=1, maximum=maximum_page_count
        )
        destination_page_count = exact_int(
            manifest["destination_page_count"], minimum=1, maximum=maximum_page_count
        )
        source_streams = exact_int(manifest["source_streams"], minimum=0, maximum=2**63 - 1)
        source_history_rows = exact_int(
            manifest["source_history_rows"], minimum=0, maximum=2**63 - 1
        )
        destination_streams = exact_int(
            manifest["destination_streams"], minimum=0, maximum=2**63 - 1
        )
        destination_history_rows = exact_int(
            manifest["destination_history_rows"], minimum=0, maximum=2**63 - 1
        )
        if (
            not valid_prefixed_digest(source_generation_id)
            or not valid_prefixed_digest(destination_generation_id)
            or source_generation_id == destination_generation_id
            or manifest["schema_fingerprint"] != schema["schema_fingerprint"]
            or not valid_prefixed_digest(manifest["schema_fingerprint"])
            or manifest["sqlite_source_id"] != runtime["sqlite_source_id"]
            or manifest["page_size"] != page_size
            or type_of(manifest["page_size"]) is not int_type
            or source_page_count != destination_page_count
            or destination_page_count != page_count
            or not exact_value(manifest["checkpoint_outcome"], [0, 0, 0])
            or manifest["evidence_recorded_at_utc"] != report["evidence_recorded_at_utc"]
            or type_of(manifest["finalization_outcome"]) is not str_type
            or source_streams != destination_streams
            or source_history_rows != destination_history_rows
            or destination_streams != stream_rows
            or destination_history_rows != history_rows
            or type_of(manifest["files"]) is not list_type
            or not manifest["files"]
            or type_of(manifest["per_stream_tails"]) is not list_type
            or length_of(cast(list[object], manifest["per_stream_tails"])) != destination_streams
        ):
            invalid()
        files = cast(list[object], manifest["files"])
        file_names: list[str] = []
        for item in files:
            if type_of(item) is not list_type:
                invalid()
            exact_item = cast(list[object], item)
            if length_of(exact_item) != 3:
                invalid()
            name = exact_item[0]
            size = exact_item[1]
            digest = exact_item[2]
            if (
                type_of(name) is not str_type
                or name not in owned_database_filenames
                or type_of(size) is not int_type
                or not 0
                <= cast(int, size)
                <= (maximum_database_bytes if name == database_basename else maximum_wal_bytes)
                or (name == database_basename and size == 0)
                or not valid_prefixed_digest(digest)
            ):
                invalid()
            file_names.append(name)
        if (
            tuple_type(file_names) != tuple_type(sorted(file_names))
            or length_of(set_type(file_names)) != length_of(file_names)
            or database_basename not in file_names
        ):
            invalid()
        expected_finalization = (
            "TRUNCATE_CHECKPOINT_CLOSED_STANDALONE_MAIN"
            if tuple_type(file_names) == (database_basename,)
            else "TRUNCATE_CHECKPOINT_CLOSED_COMPLETE_FILE_SET"
        )
        if manifest["finalization_outcome"] != expected_finalization:
            invalid()
        prior_stream_id = ""
        for item in cast(list[object], manifest["per_stream_tails"]):
            if type_of(item) is not list_type:
                invalid()
            exact_item = cast(list[object], item)
            if length_of(exact_item) != 4:
                invalid()
            stream_id = exact_item[0]
            version = exact_item[1]
            if (
                type_of(stream_id) is not str_type
                or type_of(version) is not int_type
                or not 1 <= cast(int, version) <= maximum_contract_integer
                or cast(str, stream_id) <= prior_stream_id
                or not valid_prefixed_digest(exact_item[2])
                or not valid_prefixed_digest(exact_item[3])
            ):
                invalid()
            try:
                if str(uuid_type(cast(str, stream_id))) != stream_id:
                    invalid()
            except (ValueError, TypeError, AttributeError):
                invalid()
            prior_stream_id = stream_id

    return validate, source_fingerprints


def _build_task064_published_report_artifact_authority(
    validate_artifact: Callable[[bytes], None],
    source_fingerprints: Callable[[], tuple[tuple[str, str], ...]],
) -> tuple[
    Callable[..., None],
    Callable[[Path], _Task064PublishedReportArtifactCapability],
    Callable[
        ...,
        tuple[
            bytes,
            Mapping[str, object],
            Callable[
                [str],
                tuple[
                    Callable[[int], bool],
                    Callable[[str, int, int], bool],
                    Callable[[str, int, int], bool],
                    Callable[[], None],
                ],
            ],
        ],
    ],
]:
    """Mint report-close authority only from a consumed normal publication."""

    capability_type = _Task064PublishedReportArtifactCapability
    receipt_type = _EvidenceReceipt
    report_type = EvidenceReport
    ledger_type = _EvidenceLedger
    path_type = Path
    mapping_proxy_type = MappingProxyType
    type_of = type
    isinstance_value = isinstance
    object_identity = id
    length_of = len
    current_pid = os.getpid
    current_thread_id = get_ident
    current_uid = os.getuid
    monotonic_ns = time.monotonic_ns
    issue_nonce = secrets.token_bytes
    issue_hex_nonce = secrets.token_hex
    digest_constructor = hashlib.sha256
    payload_digest = _evidence_payload_digest
    root_lookup = _lookup_active_pytest_root
    root_session_owns = _pytest_root_session_owns
    mark_authority_uncertain = _latch_pytest_root_authority_uncertainty
    schema_fingerprint_provider = load_schema_fingerprint
    open_file = os.open
    close_file = os.close
    read_file = os.read
    stat_file = os.fstat
    path_lstat = Path.lstat
    descriptor_flags = fcntl.fcntl
    get_file_status_flags = fcntl.F_GETFL
    access_mode_mask = os.O_ACCMODE
    read_only = os.O_RDONLY
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    close_on_exec = getattr(os, "O_CLOEXEC", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    stat_is_link = stat.S_ISLNK
    stat_is_regular = stat.S_ISREG
    stat_is_directory = stat.S_ISDIR
    stat_mode = stat.S_IMODE
    failure_type = HarnessFailure
    corrupt_code = HarnessFailureCode.CORRUPT
    invalid_root_code = HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    task_contract_generation = TASK_CONTRACT_GENERATION
    task_contract_digest = TASK_CONTRACT_DIGEST
    maximum_lifetime_ns = 900_000_000_000
    maximum_records = 64
    report_name = "task064-evidence.json"
    report_close_modes = frozenset(
        {
            "readback_verified_root_close_ambiguity",
            "staging_close_ambiguity",
            "readback_close_ambiguity",
            "reentrant_root_revocation",
        }
    )
    context_binding: ContextVar[bytes | None] = ContextVar(
        "task064_published_report_artifact_context",
        default=None,
    )
    context_token_type = Token
    base_exception_type = BaseException
    lock_type = threading.Lock
    records: dict[int, dict[str, object]] = {}
    paths: dict[int, dict[str, object]] = {}

    def corrupt() -> Never:
        raise failure_type(corrupt_code)

    def invalid() -> Never:
        raise failure_type(invalid_root_code)

    def exact_record(
        capability: _Task064PublishedReportArtifactCapability,
    ) -> dict[str, object] | None:
        if type_of(capability) is not capability_type:
            return None
        record = records.get(object_identity(capability))
        if (
            record is None
            or record.get("capability") is not capability
            or record.get("capability_fields")
            != (
                object_identity(capability._authority_nonce),
                capability._authority_nonce,
            )
        ):
            return None
        return record

    def read_exact_published_artifact(record: Mapping[str, object]) -> bytes:
        root = record.get("root")
        path = record.get("path")
        raw = record.get("raw")
        if (
            not isinstance_value(root, path_type)
            or not isinstance_value(path, path_type)
            or type_of(raw) is not bytes
            or cast(Path, path).parent != cast(Path, root)
            or cast(Path, path).name != report_name
        ):
            invalid()
        root_descriptor = -1
        descriptor = -1
        try:
            root_descriptor = open_file(
                cast(Path, root),
                read_only | directory_only | no_follow | close_on_exec,
            )
            root_details = stat_file(root_descriptor)
            descriptor = open_file(
                report_name,
                read_only | no_follow | close_on_exec,
                dir_fd=root_descriptor,
            )
            details = stat_file(descriptor)
            path_details = path_lstat(cast(Path, path))
            if (
                descriptor_flags(descriptor, get_file_status_flags) & access_mode_mask != read_only
                or not stat_is_directory(root_details.st_mode)
                or (
                    root_details.st_dev,
                    root_details.st_ino,
                    root_details.st_uid,
                    stat_mode(root_details.st_mode),
                )
                != record.get("root_fields")
                or stat_is_link(path_details.st_mode)
                or not stat_is_regular(details.st_mode)
                or not stat_is_regular(path_details.st_mode)
                or (
                    details.st_dev,
                    details.st_ino,
                    details.st_uid,
                    stat_mode(details.st_mode),
                    details.st_nlink,
                    details.st_size,
                )
                != record.get("path_fields")
                or (
                    path_details.st_dev,
                    path_details.st_ino,
                    path_details.st_uid,
                    stat_mode(path_details.st_mode),
                    path_details.st_nlink,
                    path_details.st_size,
                )
                != record.get("path_fields")
            ):
                invalid()
            observed = bytearray()
            expected_length = length_of(cast(bytes, raw))
            while length_of(observed) <= expected_length:
                fragment = read_file(
                    descriptor,
                    min(65_536, expected_length + 1 - length_of(observed)),
                )
                if type_of(fragment) is not bytes:
                    invalid()
                if not fragment:
                    break
                observed.extend(fragment)
            exact = bytes(observed)
            if (
                exact is raw
                or length_of(exact) != expected_length
                or exact != raw
                or digest_constructor(exact).hexdigest() != record.get("report_digest")
            ):
                invalid()
            return exact
        except failure_type:
            raise
        except (OSError, TypeError, ValueError):
            invalid()
        finally:
            close_failed = False
            for pending_descriptor in (descriptor, root_descriptor):
                if pending_descriptor >= 0:
                    try:
                        close_file(pending_descriptor)
                    except OSError:
                        close_failed = True
            if close_failed:
                mark_authority_uncertain()
                invalid()

    def evidence_run_digest(run: _EvidenceRun, root: _ActivePytestRoot) -> str:
        hasher = digest_constructor()
        hasher.update(b"TASK064-PUBLISHED-REPORT-RUN-V1\x00")
        hasher.update(run._nonce)
        hasher.update(root.nonce)
        hasher.update(str(object_identity(run)).encode("ascii"))
        return hasher.hexdigest()

    def register(
        pytest_root: Path,
        *,
        receipt: _EvidenceReceipt,
        report: EvidenceReport,
        ledger: _EvidenceLedger,
        path: Path,
        raw: bytes,
    ) -> None:
        if (
            not isinstance_value(pytest_root, path_type)
            or not isinstance_value(path, path_type)
            or type_of(receipt) is not receipt_type
            or type_of(report) is not report_type
            or type_of(ledger) is not ledger_type
            or type_of(raw) is not bytes
            or not raw
            or length_of(records) >= maximum_records
            or length_of(paths) >= maximum_records
        ):
            corrupt()
        active_root = root_lookup(pytest_root)
        if (
            active_root is None
            or active_root.path_object is not pytest_root
            or active_root.process_id != current_pid()
            or type_of(active_root.evidence_ledger) is not ledger_type
            or active_root.evidence_ledger is not ledger
            or not root_session_owns(active_root, False)
            or receipt.run._pytest_registration is not active_root
            or receipt.evidence is not report.evidence
            or receipt.evidence_digest != payload_digest(report.evidence)
            or ledger.run is not receipt.run
            or ledger.receipt is not receipt
            or ledger.recording
            or ledger.closed
            or not ledger.consumed
            or report.contract_generation != task_contract_generation
            or report.contract_digest != task_contract_digest
            or report.schema_fingerprint != schema_fingerprint_provider()
            or path != pytest_root / report_name
            or paths.get(object_identity(path)) is not None
        ):
            corrupt()
        validate_artifact(raw)
        exact_sources = source_fingerprints()
        try:
            root_details = path_lstat(pytest_root)
            path_details = path_lstat(path)
        except OSError:
            corrupt()
        if (
            stat_is_link(root_details.st_mode)
            or not stat_is_directory(root_details.st_mode)
            or root_details.st_uid != current_uid()
            or stat_mode(root_details.st_mode) != 0o700
            or stat_is_link(path_details.st_mode)
            or not stat_is_regular(path_details.st_mode)
            or path_details.st_dev != root_details.st_dev
            or path_details.st_uid != current_uid()
            or stat_mode(path_details.st_mode) != 0o600
            or path_details.st_nlink != 1
            or path_details.st_size != length_of(raw)
        ):
            corrupt()
        capability = capability_type(_authority_nonce=issue_nonce(32))
        context_nonce = issue_nonce(32)
        publication_nonce = issue_hex_nonce(32)
        if (
            type_of(capability._authority_nonce) is not bytes
            or length_of(capability._authority_nonce) != 32
            or type_of(context_nonce) is not bytes
            or length_of(context_nonce) != 32
            or type_of(publication_nonce) is not str
            or length_of(publication_nonce) != 64
            or object_identity(capability) in records
        ):
            corrupt()
        report_digest = digest_constructor(raw).hexdigest()
        expires_ns = monotonic_ns() + maximum_lifetime_ns
        publication_binding: Mapping[str, object] = mapping_proxy_type(
            {
                "publication_nonce": publication_nonce,
                "publication_run_digest": evidence_run_digest(receipt.run, active_root),
                "publication_aggregate_digest": receipt.evidence_digest,
                "publication_report_object_digest": payload_digest(report),
                "publication_root_path": str(pytest_root),
                "publication_root_device": root_details.st_dev,
                "publication_root_inode": root_details.st_ino,
                "publication_root_uid": root_details.st_uid,
                "publication_root_mode": stat_mode(root_details.st_mode),
                "publication_path": str(path),
                "publication_device": path_details.st_dev,
                "publication_inode": path_details.st_ino,
                "publication_uid": path_details.st_uid,
                "publication_mode": stat_mode(path_details.st_mode),
                "publication_nlink": path_details.st_nlink,
                "publication_process_id": current_pid(),
                "publication_thread_id": current_thread_id(),
                "publication_node_id": active_root.node_id,
                "publication_context_digest": digest_constructor(context_nonce).hexdigest(),
                "publication_expires_ns": expires_ns,
            }
        )
        try:
            context_token = context_binding.set(context_nonce)
        except base_exception_type:
            corrupt()
        if type_of(context_token) is not context_token_type:
            corrupt()
        record: dict[str, object] = {
            "capability": capability,
            "capability_fields": (
                object_identity(capability._authority_nonce),
                capability._authority_nonce,
            ),
            "context_fields": (object_identity(context_nonce), context_nonce),
            "context_token": context_token,
            "owner_process_id": current_pid(),
            "owner_thread_id": current_thread_id(),
            "node_id": active_root.node_id,
            "root": pytest_root,
            "root_identity": active_root,
            "root_fields": (
                root_details.st_dev,
                root_details.st_ino,
                root_details.st_uid,
                stat_mode(root_details.st_mode),
            ),
            "path": path,
            "path_fields": (
                path_details.st_dev,
                path_details.st_ino,
                path_details.st_uid,
                stat_mode(path_details.st_mode),
                path_details.st_nlink,
                path_details.st_size,
            ),
            "run": receipt.run,
            "receipt": receipt,
            "ledger": ledger,
            "aggregate": report.evidence,
            "report": report,
            "raw": raw,
            "raw_fields": (object_identity(raw), raw),
            "report_digest": report_digest,
            "source_fingerprints": exact_sources,
            "contract_generation": task_contract_generation,
            "contract_digest": task_contract_digest,
            "schema_fingerprint": report.schema_fingerprint,
            "publication_binding": publication_binding,
            "expires_ns": expires_ns,
            "issued_modes": set(),
            "state": "PUBLISHED",
        }
        records[object_identity(capability)] = record
        paths[object_identity(path)] = record

    def context_is_exact(record: dict[str, object]) -> bool:
        context_fields = record.get("context_fields")
        context_token = record.get("context_token")
        if (
            type_of(context_fields) is not tuple
            or length_of(cast(tuple[object, ...], context_fields)) != 2
            or type_of(context_token) is not context_token_type
            or context_binding.get() is not cast(tuple[object, ...], context_fields)[1]
        ):
            return False
        try:
            context_binding.reset(cast(Token[bytes | None], context_token))
            rotated_token = context_binding.set(
                cast(bytes, cast(tuple[object, ...], context_fields)[1])
            )
        except base_exception_type:
            return False
        if type_of(rotated_token) is not context_token_type:
            return False
        record["context_token"] = rotated_token
        return True

    def record_is_current(record: dict[str, object]) -> bool:
        root = record.get("root")
        root_identity = record.get("root_identity")
        run = record.get("run")
        receipt = record.get("receipt")
        ledger = record.get("ledger")
        aggregate = record.get("aggregate")
        report = record.get("report")
        raw = record.get("raw")
        raw_fields = record.get("raw_fields")
        publication_binding = record.get("publication_binding")
        return bool(
            record.get("owner_process_id") == current_pid()
            and record.get("owner_thread_id") == current_thread_id()
            and context_is_exact(record)
            and record.get("source_fingerprints") == source_fingerprints()
            and record.get("contract_generation") == task_contract_generation
            and record.get("contract_digest") == task_contract_digest
            and record.get("schema_fingerprint") == schema_fingerprint_provider()
            and type_of(record.get("expires_ns")) is int
            and 0 < cast(int, record["expires_ns"]) - monotonic_ns() <= maximum_lifetime_ns
            and isinstance_value(root, path_type)
            and type_of(root_identity) is _ActivePytestRoot
            and root_lookup(cast(Path, root)) is root_identity
            and cast(_ActivePytestRoot, root_identity).path_object is root
            and root_session_owns(cast(_ActivePytestRoot, root_identity), False)
            and type_of(run) is _EvidenceRun
            and type_of(receipt) is receipt_type
            and type_of(ledger) is ledger_type
            and type_of(report) is report_type
            and type_of(raw) is bytes
            and type_of(raw_fields) is tuple
            and raw_fields == (object_identity(raw), raw)
            and cast(_EvidenceReceipt, receipt).run is run
            and cast(_EvidenceReceipt, receipt).evidence is aggregate
            and cast(_EvidenceReceipt, receipt).evidence_digest == payload_digest(aggregate)
            and cast(_EvidenceLedger, ledger).run is run
            and cast(_EvidenceLedger, ledger).receipt is receipt
            and not cast(_EvidenceLedger, ledger).recording
            and not cast(_EvidenceLedger, ledger).closed
            and cast(_EvidenceLedger, ledger).consumed
            and cast(EvidenceReport, report).evidence is aggregate
            and isinstance_value(publication_binding, Mapping)
            and cast(Mapping[str, object], publication_binding).get("publication_run_digest")
            == evidence_run_digest(
                run,
                cast(_ActivePytestRoot, root_identity),
            )
            and cast(Mapping[str, object], publication_binding).get("publication_aggregate_digest")
            == payload_digest(aggregate)
            and cast(Mapping[str, object], publication_binding).get(
                "publication_report_object_digest"
            )
            == payload_digest(report)
            and record.get("report_digest") == digest_constructor(cast(bytes, raw)).hexdigest()
        )

    def claim(path: Path) -> _Task064PublishedReportArtifactCapability:
        if not isinstance_value(path, path_type):
            invalid()
        record = paths.get(object_identity(path))
        capability = None if record is None else record.get("capability")
        if (
            record is None
            or record.get("path") is not path
            or record.get("state") != "PUBLISHED"
            or type_of(capability) is not capability_type
            or exact_record(cast(_Task064PublishedReportArtifactCapability, capability))
            is not record
            or not record_is_current(record)
        ):
            invalid()
        exact = read_exact_published_artifact(record)
        if exact != record.get("raw"):
            invalid()
        record["state"] = "CLAIMED"
        return cast(_Task064PublishedReportArtifactCapability, capability)

    def resolve(
        capability: _Task064PublishedReportArtifactCapability,
        *,
        pytest_root: Path,
        issuer_node_id: str,
        target_node_id: str,
        mode: str,
        deadline_ns: int,
    ) -> tuple[
        bytes,
        Mapping[str, object],
        Callable[
            [str],
            tuple[
                Callable[[int], bool],
                Callable[[str, int, int], bool],
                Callable[[str, int, int], bool],
                Callable[[], None],
            ],
        ],
    ]:
        record = exact_record(capability)
        if (
            record is None
            or record.get("state") not in {"CLAIMED", "FANOUTING"}
            or not record_is_current(record)
            or not isinstance_value(pytest_root, path_type)
            or record.get("root") is not pytest_root
            or type_of(issuer_node_id) is not str
            or type_of(target_node_id) is not str
            or issuer_node_id != target_node_id
            or issuer_node_id != record.get("node_id")
            or type_of(mode) is not str
            or mode not in report_close_modes
            or type_of(deadline_ns) is not int
            or not 0 < deadline_ns - monotonic_ns() <= maximum_lifetime_ns
            or deadline_ns > cast(int, record.get("expires_ns"))
        ):
            invalid()
        active_root = root_lookup(pytest_root)
        issued_modes = record.get("issued_modes")
        if (
            active_root is None
            or active_root is not record.get("root_identity")
            or active_root.path_object is not pytest_root
            or not root_session_owns(active_root, False)
            or type_of(issued_modes) is not set
            or mode in cast(set[object], issued_modes)
            or length_of(cast(set[object], issued_modes)) >= length_of(report_close_modes)
        ):
            invalid()
        exact = read_exact_published_artifact(record)
        raw = record.get("raw")
        binding = record.get("publication_binding")
        if (
            type_of(raw) is not bytes
            or exact != raw
            or digest_constructor(exact).hexdigest() != record.get("report_digest")
            or not isinstance_value(binding, Mapping)
        ):
            invalid()
        cast(set[object], issued_modes).add(mode)
        record["state"] = (
            "FANOUT_COMPLETE"
            if length_of(cast(set[object], issued_modes)) == length_of(report_close_modes)
            else "FANOUTING"
        )

        session_lock = lock_type()
        session_state = "UNBOUND"
        session_packet_digest: str | None = None
        session_child_pid: int | None = None

        def bind_packet_digest(
            packet_digest: str,
        ) -> tuple[
            Callable[[int], bool],
            Callable[[str, int, int], bool],
            Callable[[str, int, int], bool],
            Callable[[], None],
        ]:
            nonlocal session_state
            nonlocal session_packet_digest
            if (
                session_state != "UNBOUND"
                or type_of(packet_digest) is not str
                or length_of(packet_digest) != 64
                or any(character not in "0123456789abcdef" for character in packet_digest)
                or exact_record(capability) is not record
                or not record_is_current(record)
                or mode not in cast(set[object], record.get("issued_modes"))
            ):
                invalid()
            session_packet_digest = packet_digest
            session_state = "BOUND"

            def arm(child_pid: int) -> bool:
                nonlocal session_state
                nonlocal session_child_pid
                with session_lock:
                    if (
                        session_state != "BOUND"
                        or type_of(child_pid) is not int
                        or child_pid <= 1
                        or session_packet_digest != packet_digest
                        or exact_record(capability) is not record
                        or not record_is_current(record)
                        or mode not in cast(set[object], record.get("issued_modes"))
                        or deadline_ns <= monotonic_ns()
                    ):
                        session_state = "CANCELLED"
                        return False
                    session_child_pid = child_pid
                    session_state = "ARMED"
                    return True

            def attest(
                observed_packet_digest: str,
                child_pid: int,
                handshake_deadline_ns: int,
            ) -> bool:
                nonlocal session_state
                with session_lock:
                    if (
                        session_state != "ARMED"
                        or observed_packet_digest != session_packet_digest
                        or child_pid != session_child_pid
                        or exact_record(capability) is not record
                        or record.get("state") not in {"FANOUTING", "FANOUT_COMPLETE"}
                        or not record_is_current(record)
                        or mode not in cast(set[object], record.get("issued_modes"))
                        or deadline_ns <= monotonic_ns()
                        or type_of(handshake_deadline_ns) is not int
                        or not monotonic_ns() < handshake_deadline_ns <= deadline_ns
                    ):
                        session_state = "CANCELLED"
                        return False
                    try:
                        exact_at_ack = read_exact_published_artifact(record)
                    except failure_type:
                        session_state = "CANCELLED"
                        return False
                    if (
                        exact_at_ack != record.get("raw")
                        or digest_constructor(exact_at_ack).hexdigest()
                        != record.get("report_digest")
                        or handshake_deadline_ns <= monotonic_ns()
                    ):
                        session_state = "CANCELLED"
                        return False
                    session_state = "ACK_READY"
                    return True

            def commit(
                observed_packet_digest: str,
                child_pid: int,
                handshake_deadline_ns: int,
            ) -> bool:
                nonlocal session_state
                with session_lock:
                    if (
                        session_state != "ACK_READY"
                        or observed_packet_digest != session_packet_digest
                        or child_pid != session_child_pid
                        or exact_record(capability) is not record
                        or record.get("state") not in {"FANOUTING", "FANOUT_COMPLETE"}
                        or not record_is_current(record)
                        or mode not in cast(set[object], record.get("issued_modes"))
                        or deadline_ns <= monotonic_ns()
                        or type_of(handshake_deadline_ns) is not int
                        or not monotonic_ns() < handshake_deadline_ns <= deadline_ns
                    ):
                        session_state = "CANCELLED"
                        return False
                    try:
                        exact_at_commit = read_exact_published_artifact(record)
                    except failure_type:
                        session_state = "CANCELLED"
                        return False
                    if (
                        exact_at_commit != record.get("raw")
                        or digest_constructor(exact_at_commit).hexdigest()
                        != record.get("report_digest")
                        or handshake_deadline_ns <= monotonic_ns()
                    ):
                        session_state = "CANCELLED"
                        return False
                    session_state = "ATTESTED"
                    return True

            def cancel() -> None:
                nonlocal session_state
                with session_lock:
                    if session_state != "ATTESTED":
                        session_state = "CANCELLED"

            return arm, attest, commit, cancel

        return raw, cast(Mapping[str, object], binding), bind_packet_digest

    return register, claim, resolve


(
    _task064_report_artifact_validator,
    _task064_report_source_fingerprints,
) = _build_task064_report_artifact_validation()
(
    _register_task064_published_report_artifact,
    _claim_task064_published_report_artifact,
    _resolve_task064_published_report_artifact,
) = _build_task064_published_report_artifact_authority(
    _task064_report_artifact_validator,
    _task064_report_source_fingerprints,
)
_bind_task064_report_artifact_validation(
    _task064_report_artifact_validator,
    _task064_report_source_fingerprints,
    _resolve_task064_published_report_artifact,
)
del _build_task064_report_artifact_validation
del _build_task064_published_report_artifact_authority
del _bind_task064_report_artifact_validation
del _task064_report_artifact_validator
del _resolve_task064_published_report_artifact


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
        or len(evidence.create_faults) != len(_CREATE_KILL_SEAM_ORDER)
        or len(evidence.compare_and_swap_faults) != len(_CAS_KILL_SEAM_ORDER)
        or len(evidence.result_code_faults) != 2
        or any(type(item) is not FaultEvidence for item in evidence.create_faults)
        or any(type(item) is not FaultEvidence for item in evidence.compare_and_swap_faults)
        or any(type(item) is not FaultEvidence for item in evidence.result_code_faults)
        or type(evidence.true_during_commit) is not FaultEvidence
        or type(evidence.ioerr_write) is not FaultEvidence
        or type(evidence.max_page_count) is not FaultEvidence
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
        or type(wal.checkpoint_samples) is not tuple
        or not wal.checkpoint_samples
        or any(
            type(sample) is not tuple
            or len(sample) != 3
            or any(type(value) is not int or value < 0 for value in sample)
            or sample[2] > sample[1]
            for sample in wal.checkpoint_samples
        )
        or type(wal.maximum_wal_bytes) is not int
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
            6,
            6,
        ),
        (
            "audit_identity_conflict",
            StoreClassification.IDENTITY_CONFLICT,
            2,
            6,
            6,
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
        or any(
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not tuple
            for item in evidence.plans
        )
        or tuple(item[0] for item in evidence.plans) != expected_plan_names
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


def _validate_verification_summary_shape(summary: VerificationSummary) -> None:
    """Reject malformed nested summary values before any dereference or range check."""

    if (
        type(summary) is not VerificationSummary
        or type(summary.profile) is not RuntimeProfile
        or type(summary.connection_profiles) is not tuple
        or any(
            type(profile) is not ConnectionControlProfile for profile in summary.connection_profiles
        )
        or any(
            type(value) is not int or value < 0
            for value in (
                summary.stream_count,
                summary.history_count,
                summary.freelist_count,
                summary.database_bytes,
                summary.wal_bytes,
            )
        )
        or type(summary.page_count) is not int
        or not 1 <= summary.page_count <= MAX_PAGE_COUNT
        or summary.freelist_count > summary.page_count
        or type(summary.schema_fingerprint) is not str
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)


_TASK064_REPORT_VALIDATION_GENERATION: Final = 6
_TASK064_REPORT_VALIDATION_CONTRACT_SHA256: Final = (
    "ec89a1df740805cc9b43e6f2530e940c0bf9b66e8f25ed878d3207d091c4bcb8"
)
_TASK064_REPORT_VALIDATION_LIFETIME_NS: Final = 120_000_000_000
_TASK064_REPORT_ROLE_NAMES: Final = (
    "bootstrap",
    "backup_source",
    "backup",
    "restore",
    "concurrent_source",
    "concurrent_backup",
    "generation_source",
    "generation_destination",
)
_TASK064_REPORT_ROLE_ORDINALS: Final = (0, 1, 0, 2, 1, 0, 0, 3)
_TASK064_REPORT_FILE_NAMES: Final = (
    _DATABASE_BASENAME,
    f"{_DATABASE_BASENAME}-wal",
    f"{_DATABASE_BASENAME}-shm",
)


@dataclass(frozen=True, slots=True)
class _ReportFileObservation:
    name: str
    present: bool
    device: int | None
    inode: int | None
    uid: int | None
    mode: int | None
    link_count: int | None
    size: int | None
    mtime_ns: int | None
    ctime_ns: int | None
    sha256: str | None


@dataclass(frozen=True, slots=True)
class _ReportStoreFileSnapshot:
    token: StoreToken
    registration: _RegisteredIdentity
    pytest_root: Path
    generation_root: Path
    database_path: Path
    generation_id: str
    files: tuple[_ReportFileObservation, ...]


@dataclass(frozen=True, slots=True)
class _ReportFileSnapshot:
    roles: tuple[tuple[str, int], ...]
    stores: tuple[_ReportStoreFileSnapshot, ...]


@dataclass(frozen=True, slots=True)
class _ReportLiveEpoch:
    snapshot: _ReportFileSnapshot
    observations: tuple[
        tuple[
            int,
            VerificationSummary,
            tuple[tuple[str, int, str, str], ...],
            CurrentSlice | None,
        ],
        ...,
    ]
    raw: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class _ReportValidationEntry:
    pytest_root: Path
    registration: _ActivePytestRoot
    run: _EvidenceRun
    ledger: _EvidenceLedger
    receipt: _EvidenceReceipt
    evidence: GeneratedEvidenceAggregate
    report: EvidenceReport
    process_id: int
    thread_id: int
    node_id: str
    context_run: _EvidenceRun | None
    contract_generation: int
    contract_sha256: str
    schema_fingerprint: str
    sqlite_source_id: str
    source_fingerprints: tuple[tuple[str, str], ...]
    evidence_digest: str
    report_core_sha256: str
    live_validation_sha256: str
    raw: bytes
    raw_length: int
    raw_sha256: str
    epoch: _ReportLiveEpoch
    issued_ns: int
    expires_ns: int


@dataclass(frozen=True, slots=True)
class _PreparedEvidenceReport:
    ledger: _EvidenceLedger
    schema_fingerprint: str
    gates: tuple[tuple[str, EvidenceDisposition, str | None], ...]
    live_observations: tuple[
        tuple[
            int,
            VerificationSummary,
            tuple[tuple[str, int, str, str], ...],
            CurrentSlice | None,
        ],
        ...,
    ]
    raw: bytes


def _valid_task064_report_file_observation(
    observation: _ReportFileObservation,
    expected_name: str,
) -> bool:
    if (
        type(observation) is not _ReportFileObservation
        or expected_name not in _TASK064_REPORT_FILE_NAMES
        or observation.name != expected_name
        or type(observation.present) is not bool
    ):
        return False
    nullable_values = (
        observation.device,
        observation.inode,
        observation.uid,
        observation.mode,
        observation.link_count,
        observation.size,
        observation.mtime_ns,
        observation.ctime_ns,
        observation.sha256,
    )
    if not observation.present:
        return all(value is None for value in nullable_values)
    integer_values = nullable_values[:-1]
    maximum = MAX_TEST_DATABASE_BYTES if expected_name == _DATABASE_BASENAME else MAX_TEST_WAL_BYTES
    return bool(
        all(type(value) is int and value >= 0 for value in integer_values)
        and observation.mode == 0o600
        and observation.link_count == 1
        and type(observation.size) is int
        and 0 <= observation.size <= maximum
        and (expected_name != _DATABASE_BASENAME or observation.size > 0)
        and type(observation.sha256) is str
        and len(observation.sha256) == 71
        and observation.sha256.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in observation.sha256[7:])
    )


def _task064_report_role_tokens(
    evidence: GeneratedEvidenceAggregate,
) -> tuple[tuple[tuple[str, int], ...], tuple[StoreToken, ...]]:
    if type(evidence) is not GeneratedEvidenceAggregate:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    bootstrap = evidence.bootstrap_path_ownership
    backup = evidence.backup_restore
    generation_copy = evidence.generation_copy
    if (
        type(bootstrap) is not BootstrapPathEvidence
        or type(backup) is not BackupRestoreEvidence
        or type(backup.concurrent_write) is not ConcurrentBackupEvidence
        or type(generation_copy) is not GenerationCopyEvidence
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    role_tokens = (
        bootstrap.token,
        backup.source_token,
        backup.backup_token,
        backup.restore_token,
        backup.concurrent_write.source_token,
        backup.concurrent_write.backup_token,
        generation_copy.source_token,
        generation_copy.destination_token,
    )
    if any(type(token) is not StoreToken for token in role_tokens):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    unique: list[StoreToken] = []
    ordinals: list[int] = []
    for token in role_tokens:
        ordinal = next(
            (index for index, candidate in enumerate(unique) if candidate is token),
            None,
        )
        if ordinal is None:
            ordinal = len(unique)
            unique.append(token)
        ordinals.append(ordinal)
    if tuple(ordinals) != _TASK064_REPORT_ROLE_ORDINALS or len(unique) != 4:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    return (
        tuple(zip(_TASK064_REPORT_ROLE_NAMES, ordinals, strict=True)),
        tuple(unique),
    )


def _capture_task064_report_file_snapshot(
    evidence: GeneratedEvidenceAggregate,
) -> _ReportFileSnapshot:
    roles, tokens = _task064_report_role_tokens(evidence)
    stores: list[_ReportStoreFileSnapshot] = []
    for token in tokens:
        try:
            identity = _require_token(token)
        except HarnessFailure as error:
            if error.code in {
                HarnessFailureCode.CORRUPT,
                HarnessFailureCode.INVALID_TOKEN,
            }:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        try:
            acquisition = _begin_operation_path_acquisition(identity)
        except HarnessFailure as error:
            if error.code in {
                HarnessFailureCode.CORRUPT,
                HarnessFailureCode.INVALID_TOKEN,
            }:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        snapshot: _OperationPathSnapshot | None = None
        pinned: list[_PinnedFile] = []
        try:
            _, generation_descriptor = _open_owned_generation(
                identity,
                acquisition=acquisition,
            )
            names = tuple(sorted(os.listdir(generation_descriptor)))
            if (
                _DATABASE_BASENAME not in names
                or set(names) - set(_TASK064_REPORT_FILE_NAMES)
                or len(names) != len(set(names))
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            observations: list[_ReportFileObservation] = []
            for name in _TASK064_REPORT_FILE_NAMES:
                if name not in names:
                    observations.append(
                        _ReportFileObservation(
                            name=name,
                            present=False,
                            device=None,
                            inode=None,
                            uid=None,
                            mode=None,
                            link_count=None,
                            size=None,
                            mtime_ns=None,
                            ctime_ns=None,
                            sha256=None,
                        )
                    )
                    continue
                descriptor = _open_and_adopt_operation_path_descriptor(
                    acquisition,
                    "file",
                    name,
                    False,
                )
                before = os.fstat(descriptor)
                path_before = os.stat(
                    name,
                    dir_fd=generation_descriptor,
                    follow_symlinks=False,
                )
                maximum = (
                    MAX_TEST_DATABASE_BYTES if name == _DATABASE_BASENAME else MAX_TEST_WAL_BYTES
                )
                stable_fields = (
                    "st_dev",
                    "st_ino",
                    "st_uid",
                    "st_mode",
                    "st_nlink",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
                if (
                    not stat.S_ISREG(before.st_mode)
                    or before.st_uid != identity.uid
                    or stat.S_IMODE(before.st_mode) != 0o600
                    or before.st_nlink != 1
                    or type(before.st_size) is not int
                    or not 0 <= before.st_size <= maximum
                    or (
                        name == _DATABASE_BASENAME
                        and (
                            before.st_size == 0
                            or before.st_dev != identity.device
                            or before.st_ino != identity.inode
                        )
                    )
                    or any(
                        getattr(before, field) != getattr(path_before, field)
                        for field in stable_fields
                    )
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                digest = hashlib.sha256()
                total = 0
                while total < before.st_size:
                    block = os.read(
                        descriptor,
                        min(65_536, before.st_size - total),
                    )
                    if not block:
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                    total += len(block)
                    if total > maximum:
                        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                    digest.update(block)
                if os.read(descriptor, 1) != b"":
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                after = os.fstat(descriptor)
                path_after = os.stat(
                    name,
                    dir_fd=generation_descriptor,
                    follow_symlinks=False,
                )
                if total != before.st_size or any(
                    getattr(before, field) != getattr(after, field)
                    or getattr(before, field) != getattr(path_after, field)
                    for field in stable_fields
                ):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                pinned.append(
                    _PinnedFile(
                        name=name,
                        descriptor=descriptor,
                        device=before.st_dev,
                        inode=before.st_ino,
                        uid=before.st_uid,
                        mode=stat.S_IMODE(before.st_mode),
                        link_count=before.st_nlink,
                    )
                )
                observations.append(
                    _ReportFileObservation(
                        name=name,
                        present=True,
                        device=before.st_dev,
                        inode=before.st_ino,
                        uid=before.st_uid,
                        mode=stat.S_IMODE(before.st_mode),
                        link_count=before.st_nlink,
                        size=before.st_size,
                        mtime_ns=before.st_mtime_ns,
                        ctime_ns=before.st_ctime_ns,
                        sha256=f"sha256:{digest.hexdigest()}",
                    )
                )
            if tuple(sorted(os.listdir(generation_descriptor))) != names:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            confirmed_identity = _require_token(token)
            if (
                confirmed_identity != identity
                or confirmed_identity.pytest_registration is not identity.pytest_registration
                or confirmed_identity.pytest_root is not identity.pytest_root
                or confirmed_identity.generation_root is not identity.generation_root
                or confirmed_identity.database_path is not identity.database_path
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            snapshot = _complete_operation_path_acquisition(
                acquisition,
                tuple(pinned),
            )
        except BaseException as error:
            try:
                _fail_operation_path_acquisition(acquisition)
            except HarnessFailure:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            if isinstance(error, HarnessFailure):
                if error.code in {
                    HarnessFailureCode.CORRUPT,
                    HarnessFailureCode.INVALID_TOKEN,
                }:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            if isinstance(error, (MemoryError, OSError)):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
            raise
        try:
            _close_operation_path_snapshot(snapshot)
        except HarnessFailure:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        try:
            generation_id = _generation_evidence_id(token)
        except HarnessFailure as error:
            if error.code in {
                HarnessFailureCode.CORRUPT,
                HarnessFailureCode.INVALID_TOKEN,
            }:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        stores.append(
            _ReportStoreFileSnapshot(
                token=token,
                registration=identity,
                pytest_root=identity.pytest_root,
                generation_root=identity.generation_root,
                database_path=identity.database_path,
                generation_id=generation_id,
                files=tuple(observations),
            )
        )
    return _ReportFileSnapshot(roles=roles, stores=tuple(stores))


def _same_task064_report_file_snapshot(
    expected: _ReportFileSnapshot,
    observed: _ReportFileSnapshot,
    _valid_file: Callable[[_ReportFileObservation, str], bool] = (
        _valid_task064_report_file_observation
    ),
) -> bool:
    return bool(
        type(expected) is _ReportFileSnapshot
        and type(observed) is _ReportFileSnapshot
        and expected.roles == observed.roles
        and len(expected.stores) == len(observed.stores) == 4
        and all(
            left.token is right.token
            and left.registration.pytest_registration is right.registration.pytest_registration
            and left.registration.pytest_root is left.pytest_root
            and right.registration.pytest_root is right.pytest_root
            and left.registration.generation_root is left.generation_root
            and right.registration.generation_root is right.generation_root
            and left.registration.database_path is left.database_path
            and right.registration.database_path is right.database_path
            and left.pytest_root is right.pytest_root
            and left.generation_root is right.generation_root
            and left.database_path is right.database_path
            and left.generation_id == right.generation_id
            and left.files == right.files
            and len(left.files) == len(right.files) == 3
            and all(
                _valid_file(left_file, expected_name) and _valid_file(right_file, expected_name)
                for left_file, right_file, expected_name in zip(
                    left.files,
                    right.files,
                    _TASK064_REPORT_FILE_NAMES,
                    strict=True,
                )
            )
            for left, right in zip(expected.stores, observed.stores, strict=True)
        )
    )


def _same_task064_report_validation_boundary(
    before: _ReportFileSnapshot,
    after: _ReportFileSnapshot,
    _valid_file: Callable[[_ReportFileObservation, str], bool] = (
        _valid_task064_report_file_observation
    ),
) -> bool:
    """Allow only live SQLite SHM timestamp volatility across validation."""

    bindings_match = bool(
        type(before) is _ReportFileSnapshot
        and type(after) is _ReportFileSnapshot
        and before.roles == after.roles
        and len(before.stores) == len(after.stores) == 4
        and all(
            left.token is right.token
            and left.registration.pytest_registration is right.registration.pytest_registration
            and left.registration.pytest_root is left.pytest_root
            and right.registration.pytest_root is right.pytest_root
            and left.registration.generation_root is left.generation_root
            and right.registration.generation_root is right.generation_root
            and left.registration.database_path is left.database_path
            and right.registration.database_path is right.database_path
            and left.pytest_root is right.pytest_root
            and left.generation_root is right.generation_root
            and left.database_path is right.database_path
            and left.generation_id == right.generation_id
            for left, right in zip(before.stores, after.stores, strict=True)
        )
    )
    if not bindings_match:
        return False
    for left_store, right_store in zip(before.stores, after.stores, strict=True):
        if (
            len(left_store.files) != 3
            or len(right_store.files) != 3
            or tuple(item.name for item in left_store.files) != _TASK064_REPORT_FILE_NAMES
            or tuple(item.name for item in right_store.files) != _TASK064_REPORT_FILE_NAMES
        ):
            return False
        for ordinal, (left, right) in enumerate(
            zip(left_store.files, right_store.files, strict=True)
        ):
            if (
                type(left) is not _ReportFileObservation
                or type(right) is not _ReportFileObservation
                or not _valid_file(left, _TASK064_REPORT_FILE_NAMES[ordinal])
                or not _valid_file(right, _TASK064_REPORT_FILE_NAMES[ordinal])
            ):
                return False
            if ordinal < 2:
                if left != right:
                    return False
                continue
            if not left.present or not right.present:
                if left != right:
                    return False
                continue
            if (
                left.name != right.name
                or left.present is not True
                or right.present is not True
                or left.device != right.device
                or left.inode != right.inode
                or left.uid != right.uid
                or left.mode != right.mode
                or left.link_count != right.link_count
                or left.size != right.size
                or left.sha256 != right.sha256
            ):
                return False
    return True


def _same_task064_report_snapshot_bindings(
    expected: _ReportFileSnapshot,
    observed: _ReportFileSnapshot,
) -> bool:
    return bool(
        type(expected) is _ReportFileSnapshot
        and type(observed) is _ReportFileSnapshot
        and expected.roles == observed.roles
        and len(expected.stores) == len(observed.stores) == 4
        and all(
            left.token is right.token
            and left.registration.pytest_registration is right.registration.pytest_registration
            and left.registration.pytest_root is left.pytest_root
            and right.registration.pytest_root is right.pytest_root
            and left.registration.generation_root is left.generation_root
            and right.registration.generation_root is right.generation_root
            and left.registration.database_path is left.database_path
            and right.registration.database_path is right.database_path
            and left.pytest_root is right.pytest_root
            and left.generation_root is right.generation_root
            and left.database_path is right.database_path
            and left.generation_id == right.generation_id
            for left, right in zip(expected.stores, observed.stores, strict=True)
        )
    )


def _build_task064_report_live_epoch(
    snapshot: _ReportFileSnapshot,
    observations: tuple[
        tuple[
            int,
            VerificationSummary,
            tuple[tuple[str, int, str, str], ...],
            CurrentSlice | None,
        ],
        ...,
    ],
    _valid_file: Callable[[_ReportFileObservation, str], bool] = (
        _valid_task064_report_file_observation
    ),
) -> _ReportLiveEpoch:
    if (
        type(snapshot) is not _ReportFileSnapshot
        or type(observations) is not tuple
        or len(snapshot.stores) != 4
        or snapshot.roles
        != tuple(zip(_TASK064_REPORT_ROLE_NAMES, _TASK064_REPORT_ROLE_ORDINALS, strict=True))
        or len(observations) != 4
        or tuple(item[0] for item in observations) != (0, 1, 2, 3)
        or observations[0][3] is None
        or any(item[3] is not None for item in observations[1:])
        or any(
            len(store.files) != 3
            or any(
                not _valid_file(file_observation, expected_name)
                for file_observation, expected_name in zip(
                    store.files,
                    _TASK064_REPORT_FILE_NAMES,
                    strict=True,
                )
            )
            for store in snapshot.stores
        )
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    entries: list[dict[str, object]] = []
    for ordinal, (store, observation) in enumerate(zip(snapshot.stores, observations, strict=True)):
        observed_ordinal, summary, tails, _ = observation
        if observed_ordinal != ordinal:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        registration = store.registration
        root_registration = registration.pytest_registration
        entries.append(
            {
                "ordinal": ordinal,
                "generation_id": store.generation_id,
                "registration": {
                    "pytest_root": [
                        registration.pytest_root_device,
                        registration.pytest_root_inode,
                        registration.pytest_root_uid,
                        registration.pytest_root_mode,
                        root_registration.process_id,
                        root_registration.node_id,
                    ],
                    "generation": [
                        registration.generation_device,
                        registration.generation_inode,
                        registration.generation_uid,
                        registration.generation_mode,
                    ],
                    "database": [
                        registration.device,
                        registration.inode,
                        registration.uid,
                        registration.mode,
                        registration.link_count,
                    ],
                },
                "summary_sha256": _evidence_payload_digest(summary),
                "tails_sha256": _evidence_payload_digest(tails),
                "files": [
                    {
                        "name": file.name,
                        "present": file.present,
                        "device": file.device,
                        "inode": file.inode,
                        "uid": file.uid,
                        "mode": file.mode,
                        "link_count": file.link_count,
                        "size": file.size,
                        "mtime_ns": file.mtime_ns,
                        "ctime_ns": file.ctime_ns,
                        "sha256": file.sha256,
                    }
                    for file in store.files
                ],
            }
        )
    payload = {
        "domain": "TASK064-REPORT-LIVE-EPOCH-V2",
        "roles": [list(role) for role in snapshot.roles],
        "entries": entries,
    }
    try:
        raw = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except MemoryError:
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
    except (TypeError, ValueError, UnicodeError):
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
    return _ReportLiveEpoch(
        snapshot=snapshot,
        observations=observations,
        raw=raw,
        sha256=hashlib.sha256(raw).hexdigest(),
    )


def _report_core_digest(report: EvidenceReport, evidence_digest: str) -> str:
    if type(report) is not EvidenceReport or type(evidence_digest) is not str:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    report_fields = fields(EvidenceReport)
    if len(report_fields) != 41 or report_fields[-1].name != "evidence":
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    values = tuple(getattr(report, field.name) for field in report_fields[:-1])
    return _evidence_payload_digest(("TASK064-REPORT-CORE-V1", values, evidence_digest))


def _derived_report_gates(
    root: Path,
    report: EvidenceReport,
    *,
    validate_live: bool = True,
    preserve_live_unavailable: bool = False,
    live_observations: list[
        tuple[
            int,
            VerificationSummary,
            tuple[tuple[str, int, str, str], ...],
            CurrentSlice | None,
        ]
    ]
    | None = None,
) -> tuple[tuple[str, EvidenceDisposition, str | None], ...]:
    if type(report) is not EvidenceReport:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    evidence = report.evidence
    if type(evidence) is not GeneratedEvidenceAggregate:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    bootstrap = evidence.bootstrap_path_ownership
    if type(bootstrap) is not BootstrapPathEvidence:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if (
        type(validate_live) is not bool
        or type(preserve_live_unavailable) is not bool
        or (not validate_live and live_observations is not None)
        or (preserve_live_unavailable and not validate_live)
        or (preserve_live_unavailable and live_observations is None)
        or (not preserve_live_unavailable and live_observations is not None)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    observed_live_summaries: list[tuple[StoreToken, VerificationSummary]] = []
    observed_live_tails: list[tuple[StoreToken, tuple[tuple[str, int, str, str], ...]]] = []

    def remap_live_failure(error: HarnessFailure) -> Never:
        if preserve_live_unavailable and error.code is HarnessFailureCode.UNAVAILABLE:
            raise error
        raise HarnessFailure(HarnessFailureCode.CORRUPT) from None

    def verify_live_store_once(token: StoreToken) -> VerificationSummary:
        if not preserve_live_unavailable:
            return verify_store(token)
        for observed_token, observed_value in observed_live_summaries:
            if token is observed_token:
                return observed_value
        observed_value = verify_store(token)
        observed_live_summaries.append((token, observed_value))
        return observed_value

    def live_tails_once(
        token: StoreToken,
    ) -> tuple[tuple[str, int, str, str], ...]:
        if not preserve_live_unavailable:
            return _tail_manifest(token)
        for observed_token, observed_value in observed_live_tails:
            if token is observed_token:
                return observed_value
        observed_value = _tail_manifest(token)
        observed_live_tails.append((token, observed_value))
        return observed_value

    registered: _RegisteredIdentity | None = None
    observed_summary: VerificationSummary | None = None
    if validate_live:
        try:
            registered = _require_token(bootstrap.token)
            observed_summary = verify_live_store_once(bootstrap.token)
        except HarnessFailure as error:
            remap_live_failure(error)
    summary = evidence.schema_identity
    _validate_verification_summary_shape(summary)
    profile = summary.profile
    if (
        (
            validate_live
            and (
                registered is None or registered.pytest_root != root or observed_summary != summary
            )
        )
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
        or type(projection.creation) is not ContinuousPublicTradeStreamStoredCreationV1
        or projection.current is None
        or type(projection.current)
        not in {
            ContinuousPublicTradeStreamStoredCreationV1,
            ContinuousPublicTradeStreamStoredTransitionV1,
        }
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    creation = projection.creation
    current = projection.current
    if current.record.stream_id != creation.record.stream_id:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    _validated_report_query_rows(projection.query_evidence)
    observed_projection: CurrentSlice | None = None
    if validate_live:
        record = creation.record
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
        except HarnessFailure as error:
            remap_live_failure(error)
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
    expected_mutations = (
        (StoreClassification.INSERTED, 0, 0),
        (StoreClassification.DUPLICATE, 1, 3),
        (StoreClassification.CONFLICT, 2, 6),
        (StoreClassification.UPDATED, 1, 2),
        (StoreClassification.DUPLICATE, 1, 5),
        (StoreClassification.CONFLICT, 2, 6),
    )
    if (
        type(mutations) is not tuple
        or len(mutations) != len(expected_mutations)
        or any(type(item) is not MutationEvidence for item in mutations)
        or tuple((item.classification, item.stream_rows, item.history_rows) for item in mutations)
        != expected_mutations
        or any(
            item.committed is not True
            or type(item.stream_rows) is not int
            or type(item.history_rows) is not int
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
    for candidate_summary in (
        backup.source_summary,
        backup.backup_summary,
        backup.restore_summary,
        concurrent.source_before,
        concurrent.source_after,
        concurrent.backup_summary,
    ):
        _validate_verification_summary_shape(candidate_summary)
    backup_source_registration: _RegisteredIdentity | None = None
    observed_backup_source_summary: VerificationSummary | None = None
    observed_backup_summary: VerificationSummary | None = None
    observed_restore_summary: VerificationSummary | None = None
    if validate_live:
        try:
            backup_source_registration = _require_token(backup.source_token)
            _require_token(backup.backup_token)
            _require_token(backup.restore_token)
            observed_backup_source_summary = verify_live_store_once(backup.source_token)
            observed_backup_summary = verify_live_store_once(backup.backup_token)
            observed_restore_summary = verify_live_store_once(backup.restore_token)
            observed_backup_files = _closed_file_manifest(backup.backup_token)
            observed_restore_files = _closed_file_manifest(backup.restore_token)
            observed_backup_tails = live_tails_once(backup.backup_token)
            observed_restore_tails = live_tails_once(backup.restore_token)
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
        except HarnessFailure as error:
            remap_live_failure(error)
    if (
        backup.backup_token is not bootstrap.token
        or (
            validate_live
            and (
                backup_source_registration is None
                or registered is None
                or backup_source_registration.pytest_registration
                is not registered.pytest_registration
                or backup.source_summary != observed_backup_source_summary
                or backup.backup_summary != observed_backup_summary
                or backup.restore_summary != observed_restore_summary
            )
        )
        or backup.backup_manifest != report.backup_manifest
        or concurrent.source_token is not backup.source_token
        or concurrent.backup_token is not backup.backup_token
        or concurrent.backup_manifest != backup.backup_manifest
        or concurrent.backup_summary != backup.backup_summary
        or backup.source_summary != concurrent.source_after
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
    concurrent_source_registration: _RegisteredIdentity | None = None
    observed_concurrent_source: VerificationSummary | None = None
    observed_concurrent_backup: VerificationSummary | None = None
    if validate_live:
        try:
            concurrent_source_registration = _require_token(concurrent.source_token)
            _require_token(concurrent.backup_token)
            observed_concurrent_source = verify_live_store_once(concurrent.source_token)
            observed_concurrent_backup = verify_live_store_once(concurrent.backup_token)
            observed_concurrent_files = _closed_file_manifest(concurrent.backup_token)
            observed_concurrent_tails = live_tails_once(concurrent.backup_token)
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
        except HarnessFailure as error:
            remap_live_failure(error)
    if (
        (
            validate_live
            and (
                concurrent_source_registration is None
                or registered is None
                or concurrent_source_registration.pytest_registration
                is not registered.pytest_registration
                or concurrent.source_after != observed_concurrent_source
                or concurrent.backup_summary != observed_concurrent_backup
            )
        )
        or concurrent.source_token is concurrent.backup_token
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
            or type(mutation.stream_rows) is not int
            or mutation.stream_rows != 1
            or type(mutation.history_rows) is not int
            or not 2 <= mutation.history_rows <= 4
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
    _validate_verification_summary_shape(generation_copy.source_summary)
    _validate_verification_summary_shape(generation_copy.destination_summary)
    observed_copy_source_summary: VerificationSummary | None = None
    observed_copy_destination_summary: VerificationSummary | None = None
    observed_copy_source_id: str | None = None
    observed_copy_destination_id: str | None = None
    observed_copy_source_tails: tuple[tuple[str, int, str, str], ...] | None = None
    observed_copy_destination_tails: tuple[tuple[str, int, str, str], ...] | None = None
    if validate_live:
        try:
            _require_token(generation_copy.source_token)
            _require_token(generation_copy.destination_token)
            observed_copy_source_summary = verify_live_store_once(generation_copy.source_token)
            observed_copy_destination_summary = verify_live_store_once(
                generation_copy.destination_token
            )
            observed_copy_source_id = _generation_evidence_id(generation_copy.source_token)
            observed_copy_destination_id = _generation_evidence_id(
                generation_copy.destination_token,
            )
            observed_copy_source_tails = live_tails_once(generation_copy.source_token)
            observed_copy_destination_tails = live_tails_once(
                generation_copy.destination_token,
            )
        except HarnessFailure as error:
            remap_live_failure(error)
    if (
        generation_copy.source_token is not bootstrap.token
        or generation_copy.source_generation_id == generation_copy.destination_generation_id
        or generation_copy.source_generation_id != report.backup_manifest.destination_generation_id
        or generation_copy.source_tails != report.backup_manifest.per_stream_tails
        or (
            validate_live
            and (
                generation_copy.source_generation_id != observed_copy_source_id
                or generation_copy.destination_generation_id != observed_copy_destination_id
                or generation_copy.source_summary != observed_copy_source_summary
                or generation_copy.destination_summary != observed_copy_destination_summary
                or generation_copy.source_tails != observed_copy_source_tails
                or generation_copy.destination_tails != observed_copy_destination_tails
            )
        )
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
    if validate_live and preserve_live_unavailable:
        _, ordinal_tokens = _task064_report_role_tokens(evidence)
        try:
            exact_observations = tuple(
                (
                    ordinal,
                    verify_live_store_once(token),
                    live_tails_once(token),
                    observed_projection if ordinal == 0 else None,
                )
                for ordinal, token in enumerate(ordinal_tokens)
            )
        except HarnessFailure as error:
            remap_live_failure(error)
        if (
            len(observed_live_summaries) != 4
            or len(observed_live_tails) != 4
            or observed_projection is None
            or (live_observations is not None and live_observations)
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if live_observations is not None:
            live_observations.extend(exact_observations)
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
        or any(
            type(item) is not tuple
            or len(item) != 3
            or type(item[0]) is not str
            or type(item[1]) is not int
            or type(item[2]) is not str
            for item in manifest.files
        )
        or type(manifest.per_stream_tails) is not tuple
        or any(
            type(item) is not tuple
            or len(item) != 4
            or type(item[0]) is not str
            or type(item[1]) is not int
            or type(item[2]) is not str
            or type(item[3]) is not str
            for item in manifest.per_stream_tails
        )
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


def _publish_evidence_report_bytes_unbound(
    mark_cleanup_uncertain: Callable[[], None],
    cleanup_uncertain_observed: Callable[[], bool],
    root_lookup: Callable[[Path], _ActivePytestRoot | None],
    pytest_root: Path,
    raw: bytes,
    *,
    validate_before_link: Callable[[], bool],
    validate_live_source_before_link: Callable[[], bool],
    finalize_live_source: Callable[[bool], None],
    prepare_consumption: Callable[
        [],
        tuple[Callable[[], bool], Callable[[], bool], Callable[[], bool]],
    ],
) -> Path:
    """Publish one exact canonical report through the single closed state machine."""

    if cleanup_uncertain_observed():
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if type(raw) is not bytes or not raw:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    root = _validate_bootstrap_root(pytest_root)
    if not validate_before_link():
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    commit_consumption, terminalize_consumption, consumption_terminal = prepare_consumption()
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
    readback_descriptor = -1
    stage_created = False
    publication_attempted = False
    published = False
    publication_readback_started = False
    publication_readback_verified = False
    cleanup_uncertain = False
    stage_details: os.stat_result | None = None
    foreign_final_before: tuple[tuple[int, int, int, int, int, int, int, int], bytes] | None = None

    def snapshot_foreign_final(
        directory_descriptor: int,
    ) -> tuple[tuple[int, int, int, int, int, int, int, int], bytes] | None:
        descriptor = -1
        try:
            try:
                path_before = os.stat(
                    report_name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return None
            if (
                not stat.S_ISREG(path_before.st_mode)
                or path_before.st_uid != os.getuid()
                or path_before.st_nlink != 1
                or type(path_before.st_size) is not int
                or not 0 <= path_before.st_size <= 4 * 1024 * 1024
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            descriptor = os.open(
                report_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_descriptor,
            )
            before = os.fstat(descriptor)
            stable_fields = (
                "st_dev",
                "st_ino",
                "st_uid",
                "st_mode",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
            if any(
                getattr(before, field) != getattr(path_before, field) for field in stable_fields
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            observed = bytearray()
            while len(observed) < before.st_size:
                try:
                    chunk = os.read(descriptor, before.st_size - len(observed))
                except InterruptedError:
                    continue
                if not chunk:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                observed.extend(chunk)
            while True:
                try:
                    trailing = os.read(descriptor, 1)
                    break
                except InterruptedError:
                    continue
            after = os.fstat(descriptor)
            path_after = os.stat(
                report_name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if trailing != b"" or any(
                getattr(before, field) != getattr(after, field)
                or getattr(before, field) != getattr(path_after, field)
                for field in stable_fields
            ):
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            identity = (
                before.st_dev,
                before.st_ino,
                before.st_uid,
                before.st_mode,
                before.st_nlink,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            exact_descriptor = descriptor
            descriptor = -1
            os.close(exact_descriptor)
            return identity, bytes(observed)
        except BaseException as error:
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from error
            raise

    try:
        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0)
        )
        root_descriptor = os.open(root, directory_flags)
        active_root = root_lookup(pytest_root)
        if active_root is None:
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
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
        descriptor = stage_descriptor
        stage_descriptor = -1
        try:
            os.close(descriptor)
        except OSError:
            cleanup_uncertain = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None

        final_root_details = os.fstat(root_descriptor)
        if (
            final_root_details.st_dev != root_details.st_dev
            or final_root_details.st_ino != root_details.st_ino
            or final_root_details.st_uid != root_details.st_uid
            or stat.S_IMODE(final_root_details.st_mode) != stat.S_IMODE(root_details.st_mode)
        ):
            raise HarnessFailure(HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        foreign_final_before = snapshot_foreign_final(root_descriptor)
        if not validate_before_link():
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if not validate_live_source_before_link():
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        publication_attempted = True
        os.link(
            stage_name,
            report_name,
            src_dir_fd=root_descriptor,
            dst_dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        published = True
        if foreign_final_before is not None:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        os.unlink(stage_name, dir_fd=root_descriptor)
        stage_created = False
        os.fsync(root_descriptor)
        publication_readback_started = True
        final_path_details = os.stat(
            report_name,
            dir_fd=root_descriptor,
            follow_symlinks=False,
        )
        readback_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        readback_descriptor = os.open(
            report_name,
            readback_flags,
            dir_fd=root_descriptor,
        )
        final_details = os.fstat(readback_descriptor)
        if (
            stage_details is None
            or final_details.st_dev != stage_details.st_dev
            or final_details.st_ino != stage_details.st_ino
            or final_details.st_uid != stage_details.st_uid
            or final_details.st_size != len(raw)
            or final_details.st_nlink != 1
            or not stat.S_ISREG(final_details.st_mode)
            or stat.S_IMODE(final_details.st_mode) != 0o600
            or final_path_details.st_dev != final_details.st_dev
            or final_path_details.st_ino != final_details.st_ino
            or final_path_details.st_uid != final_details.st_uid
            or final_path_details.st_mode != final_details.st_mode
            or final_path_details.st_nlink != final_details.st_nlink
            or final_path_details.st_size != final_details.st_size
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        observed = bytearray()
        while len(observed) < len(raw):
            try:
                chunk = os.read(readback_descriptor, len(raw) - len(observed))
            except InterruptedError:
                continue
            if not chunk:
                break
            observed.extend(chunk)
        while True:
            try:
                trailing = os.read(readback_descriptor, 1)
                break
            except InterruptedError:
                continue
        if bytes(observed) != raw or trailing != b"":
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        publication_readback_verified = True
        descriptor = readback_descriptor
        readback_descriptor = -1
        try:
            os.close(descriptor)
        except OSError:
            cleanup_uncertain = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        descriptor = root_descriptor
        root_descriptor = -1
        try:
            os.close(descriptor)
        except OSError:
            cleanup_uncertain = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if not commit_consumption() or not consumption_terminal():
            cleanup_uncertain = True
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        finalize_live_source(False)
        return path
    except BaseException as error:
        cleanup_ok = not cleanup_uncertain
        collision_unchanged = False
        owned_final_removed = False
        final_name_proven = False
        if readback_descriptor >= 0:
            descriptor = readback_descriptor
            readback_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                cleanup_ok = False
        if stage_descriptor >= 0:
            descriptor = stage_descriptor
            stage_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0 and publication_attempted and not publication_readback_verified:
            try:
                final_details = os.stat(
                    report_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                same_staged_inode = stage_details is not None and (
                    final_details.st_dev == stage_details.st_dev
                    and final_details.st_ino == stage_details.st_ino
                )
                if same_staged_inode:
                    os.unlink(report_name, dir_fd=root_descriptor)
                    published = False
                    owned_final_removed = True
                elif isinstance(error, FileExistsError) and not published:
                    try:
                        observed_foreign = snapshot_foreign_final(root_descriptor)
                    except BaseException:
                        observed_foreign = None
                    collision_unchanged = bool(
                        foreign_final_before is not None
                        and observed_foreign == foreign_final_before
                        and stage_details is not None
                        and observed_foreign[0][0:2] != (stage_details.st_dev, stage_details.st_ino)
                    )
                    if not collision_unchanged:
                        cleanup_ok = False
                else:
                    cleanup_ok = False
            except FileNotFoundError:
                published = False
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0 and stage_created:
            try:
                pending_details = os.stat(
                    stage_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
                if (
                    (
                        stage_details is not None
                        and (
                            pending_details.st_dev != stage_details.st_dev
                            or pending_details.st_ino != stage_details.st_ino
                        )
                    )
                    or not stat.S_ISREG(pending_details.st_mode)
                    or pending_details.st_uid != os.getuid()
                    or stat.S_IMODE(pending_details.st_mode) != 0o600
                    or pending_details.st_nlink != 1
                ):
                    cleanup_ok = False
                else:
                    os.unlink(stage_name, dir_fd=root_descriptor)
                    stage_created = False
            except FileNotFoundError:
                stage_created = False
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0 and not publication_readback_verified:
            try:
                os.fsync(root_descriptor)
                try:
                    os.stat(
                        stage_name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    cleanup_ok = False
                if collision_unchanged:
                    try:
                        final_after_sync = snapshot_foreign_final(root_descriptor)
                    except BaseException:
                        final_after_sync = None
                    final_name_proven = bool(
                        foreign_final_before is not None
                        and final_after_sync == foreign_final_before
                    )
                    if not final_name_proven:
                        cleanup_ok = False
                else:
                    try:
                        os.stat(
                            report_name,
                            dir_fd=root_descriptor,
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        final_name_proven = True
                    else:
                        cleanup_ok = False
            except OSError:
                cleanup_ok = False
        if root_descriptor >= 0:
            descriptor = root_descriptor
            root_descriptor = -1
            try:
                os.close(descriptor)
            except OSError:
                cleanup_ok = False
        elif stage_created or (
            publication_attempted
            and not publication_readback_verified
            and not isinstance(error, FileExistsError)
        ):
            cleanup_ok = False
        if not cleanup_ok:
            mark_cleanup_uncertain()
        retryable = bool(
            cleanup_ok
            and final_name_proven
            and not publication_readback_started
            and not publication_readback_verified
            and (
                not publication_attempted
                or collision_unchanged
                or (
                    publication_attempted
                    and not isinstance(error, FileExistsError)
                    and foreign_final_before is None
                    and (not published or owned_final_removed)
                )
            )
        )
        finalize_live_source(retryable)
        if publication_readback_verified or not cleanup_ok:
            terminalized = terminalize_consumption()
            if not terminalized and not consumption_terminal():
                mark_cleanup_uncertain()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, HarnessFailure):
            raise
        if isinstance(error, (MemoryError, OSError)):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        raise


def _write_evidence_report_unbound(
    validate_receipt: Callable[
        [Path, _EvidenceReceipt, GeneratedEvidenceAggregate, bool],
        _EvidenceLedger,
    ],
    prepare_receipt_consumption: Callable[
        [_EvidenceReceipt],
        tuple[
            Callable[[], bool],
            Callable[[], bool],
            Callable[[], bool],
        ],
    ],
    publish_bytes: Callable[..., Path],
    mark_cleanup_uncertain: Callable[[], None],
    cleanup_uncertain_observed: Callable[[], bool],
    root_lookup: Callable[[Path], _ActivePytestRoot | None],
    serialize_report: Callable[[Mapping[str, object]], bytes],
    schema_fingerprint_provider: Callable[[], str],
    register_published_artifact: Callable[..., None],
    pytest_root: Path,
    *,
    receipt: _EvidenceReceipt,
    report: EvidenceReport,
    validate_live: bool,
    preserve_live_unavailable: bool,
    publish: bool,
    validate_prepared_report: Callable[[_EvidenceLedger, bytes], bool],
    validate_live_source_before_link: Callable[[], bool],
    finalize_live_source: Callable[[bool], None],
) -> Path | _PreparedEvidenceReport:
    """Write canonical generated evidence beneath one validated pytest root only."""

    if (
        type(validate_live) is not bool
        or type(preserve_live_unavailable) is not bool
        or type(publish) is not bool
        or (preserve_live_unavailable and not validate_live)
    ):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if cleanup_uncertain_observed():
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
    if type(report) is not EvidenceReport:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    root = _validate_bootstrap_root(pytest_root)
    receipt_ledger = validate_receipt(
        pytest_root,
        receipt,
        report.evidence,
        False,
    )
    observed_schema_fingerprint = schema_fingerprint_provider()
    if (
        type(report) is not EvidenceReport
        or report.report_version != 1
        or type(report.report_version) is not int
        or report.task_id != TASK_ID
        or report.contract_generation != TASK_CONTRACT_GENERATION
        or type(report.contract_generation) is not int
        or report.contract_digest != TASK_CONTRACT_DIGEST
        or report.schema_fingerprint != observed_schema_fingerprint
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
    observed_live: list[
        tuple[
            int,
            VerificationSummary,
            tuple[tuple[str, int, str, str], ...],
            CurrentSlice | None,
        ]
    ] = []
    gates = _derived_report_gates(
        root,
        report,
        validate_live=validate_live,
        preserve_live_unavailable=preserve_live_unavailable,
        live_observations=observed_live if preserve_live_unavailable else None,
    )
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
        or type(report.latency_samples_ns) is not tuple
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
    raw = serialize_report(document)
    exact_receipt_ledger = validate_receipt(
        pytest_root,
        receipt,
        report.evidence,
        True,
    )
    if exact_receipt_ledger is not receipt_ledger:
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if not validate_prepared_report(receipt_ledger, raw):
        raise HarnessFailure(HarnessFailureCode.CORRUPT)
    if not publish:
        return _PreparedEvidenceReport(
            ledger=receipt_ledger,
            schema_fingerprint=observed_schema_fingerprint,
            gates=gates,
            live_observations=tuple(observed_live),
            raw=raw,
        )

    def validate_before_link() -> bool:
        return (
            validate_receipt(
                pytest_root,
                receipt,
                report.evidence,
                True,
            )
            is receipt_ledger
        )

    def prepare_consumption() -> tuple[
        Callable[[], bool],
        Callable[[], bool],
        Callable[[], bool],
    ]:
        return prepare_receipt_consumption(receipt)

    published_path = publish_bytes(
        mark_cleanup_uncertain,
        cleanup_uncertain_observed,
        root_lookup,
        pytest_root,
        raw,
        validate_before_link=validate_before_link,
        validate_live_source_before_link=validate_live_source_before_link,
        finalize_live_source=finalize_live_source,
        prepare_consumption=prepare_consumption,
    )
    try:
        register_published_artifact(
            pytest_root,
            receipt=receipt,
            report=report,
            ledger=receipt_ledger,
            path=published_path,
            raw=raw,
        )
    except BaseException:
        mark_cleanup_uncertain()
        raise
    return published_path


def _build_evidence_report_writer(
    validate_issued_receipt: Callable[
        [_EvidenceReceipt, GeneratedEvidenceAggregate, bool],
        bool,
    ],
    prepare_receipt_consumption: Callable[
        [_EvidenceReceipt],
        tuple[
            Callable[[], bool],
            Callable[[], bool],
            Callable[[], bool],
        ],
    ],
    validate_report_permit: Callable[[_Task064ReportPublicationPermit, Path, bytes], bool],
    prepare_report_permit_consumption: Callable[
        [_Task064ReportPublicationPermit],
        tuple[Callable[[], bool], Callable[[], bool], Callable[[], bool]],
    ],
    register_published_artifact: Callable[..., None],
    report_source_fingerprints: Callable[[], tuple[tuple[str, str], ...]],
) -> tuple[
    Callable[..., Path],
    Callable[..., Path],
    Callable[..., None],
    Callable[[], tuple[str, bool, int | None, int | None]],
    Callable[[], None],
    Callable[[Path], bool],
    Callable[[_ActivePytestRoot], bool],
]:
    """Capture receipt validation and consumption behind the public writer."""

    validation_implementation = _validate_evidence_receipt_unbound
    writer_implementation = _write_evidence_report_unbound
    publisher_implementation = _publish_evidence_report_bytes_unbound
    capture_file_snapshot = _capture_task064_report_file_snapshot
    same_file_snapshot = _same_task064_report_file_snapshot
    same_validation_boundary = _same_task064_report_validation_boundary
    same_snapshot_bindings = _same_task064_report_snapshot_bindings
    build_live_epoch = _build_task064_report_live_epoch
    report_core_digest = _report_core_digest
    mark_cleanup_uncertain = _mark_process_cleanup_uncertain
    cleanup_uncertain_observed = _has_process_cleanup_uncertainty
    root_lookup = _lookup_active_pytest_root
    root_identity_is_valid = _validate_pytest_root_identity
    root_session_owns = _pytest_root_session_owns
    generation3_schema_fingerprint_provider = load_schema_fingerprint
    json_serializer = json.dumps
    digest_constructor = hashlib.sha256
    evidence_digest = _evidence_payload_digest
    schema_descriptor_path = SCHEMA_DESCRIPTOR_PATH
    schema_fingerprint_path = SCHEMA_FINGERPRINT_PATH
    path_read_bytes = Path.read_bytes
    schema_document_parser = _parse_schema_descriptor_document
    descriptor_serializer = canonical_descriptor_bytes
    schema_fingerprint_from_descriptor = _schema_fingerprint_from_canonical_bytes
    active_evidence_run = _ACTIVE_EVIDENCE_RUN
    real_getpid = os.getpid
    real_get_ident = get_ident
    real_monotonic_ns = time.monotonic_ns
    register_at_fork = getattr(os, "register_at_fork", None)
    lock_constructor = threading.Lock
    replace_value = replace
    maximum_contract_integer = MAX_CONTRACT_INTEGER
    validation_lifetime_ns = _TASK064_REPORT_VALIDATION_LIFETIME_NS
    cache_lock = lock_constructor()
    entry_gate = lock_constructor()
    cache_lock_owner: tuple[int, int] | None = None
    cache_state = "EMPTY"
    cached_entry: _ReportValidationEntry | None = None
    cached_owner: (
        tuple[
            Path,
            _ActivePytestRoot,
            _EvidenceRun,
            _EvidenceLedger,
            _EvidenceReceipt,
            GeneratedEvidenceAggregate,
            EvidenceReport,
            int,
            int,
            _EvidenceRun | None,
        ]
        | None
    ) = None
    attempt_serial = 0
    active_attempt: int | None = None
    active_registration: _ActivePytestRoot | None = None
    invalidation_generation = 0
    last_clock_ns: int | None = None
    source_fingerprints = report_source_fingerprints()

    def clear_cache() -> None:
        nonlocal cache_state
        nonlocal cached_entry
        nonlocal cached_owner
        nonlocal active_attempt
        nonlocal active_registration
        cache_state = "EMPTY"
        cached_entry = None
        cached_owner = None
        active_attempt = None
        active_registration = None

    def invalidate_active_cache_attempt() -> None:
        nonlocal invalidation_generation
        invalidation_generation += 1
        clear_cache()

    def clock_value(prior: int | None = None) -> int:
        nonlocal last_clock_ns
        try:
            observed = real_monotonic_ns()
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if (
            type(observed) is not int
            or observed < 0
            or (prior is not None and observed < prior)
            or (last_clock_ns is not None and observed < last_clock_ns)
        ):
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        last_clock_ns = observed
        return observed

    @contextmanager
    def cache_entry_scope() -> Iterator[tuple[int, str, _ReportValidationEntry | None]]:
        nonlocal cache_lock_owner
        starting_invalidation_generation = 0
        starting_cache_state = "EMPTY"
        starting_cached_entry: _ReportValidationEntry | None = None
        caller = (real_getpid(), real_get_ident())
        entry_gate.acquire()
        if cache_lock_owner == caller:
            try:
                mark_cleanup_uncertain()
                invalidate_active_cache_attempt()
            finally:
                entry_gate.release()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        acquired = cache_lock.acquire(blocking=False)
        if not acquired and cache_state in {"VALIDATING", "PUBLISHING"}:
            try:
                mark_cleanup_uncertain()
                invalidate_active_cache_attempt()
            finally:
                entry_gate.release()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if acquired:
            cache_lock_owner = caller
        entry_gate.release()
        if not acquired:
            cache_lock.acquire()
            entry_gate.acquire()
            if cache_lock_owner is not None:
                mark_cleanup_uncertain()
                invalidate_active_cache_attempt()
            cache_lock_owner = caller
            entry_gate.release()
        starting_invalidation_generation = invalidation_generation
        starting_cache_state = cache_state
        starting_cached_entry = cached_entry
        try:
            yield (
                starting_invalidation_generation,
                starting_cache_state,
                starting_cached_entry,
            )
        finally:
            entry_gate.acquire()
            invalidated = invalidation_generation != starting_invalidation_generation
            if invalidated:
                clear_cache()
            cache_lock_owner = None
            cache_lock.release()
            entry_gate.release()
            if invalidated:
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None

    def call_selects_entry_owner(
        entry: _ReportValidationEntry,
        pytest_root: Path,
        receipt: _EvidenceReceipt,
        report: EvidenceReport,
    ) -> bool:
        return bool(
            type(entry) is _ReportValidationEntry
            and type(receipt) is _EvidenceReceipt
            and type(report) is EvidenceReport
            and entry.pytest_root is pytest_root
            and entry.receipt is receipt
            and entry.report is report
        )

    def call_context_matches_entry(entry: _ReportValidationEntry) -> bool:
        return bool(
            type(entry) is _ReportValidationEntry
            and entry.process_id == real_getpid()
            and entry.thread_id == real_get_ident()
            and entry.context_run is None
            and active_evidence_run.get() is entry.context_run
            and root_session_owns(entry.registration, False)
        )

    def cached_schema_fingerprint() -> str:
        try:
            fingerprint_raw = path_read_bytes(schema_fingerprint_path)
            descriptor_raw = path_read_bytes(schema_descriptor_path)
        except OSError:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if (
            not fingerprint_raw.endswith(b"\n")
            or fingerprint_raw.endswith(b"\n\n")
            or not descriptor_raw.endswith(b"\n")
            or descriptor_raw.endswith(b"\n\n")
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            fingerprint = fingerprint_raw[:-1].decode("ascii")
            descriptor = schema_document_parser(descriptor_raw[:-1])
            canonical_descriptor = descriptor_serializer(descriptor)
            computed_fingerprint = schema_fingerprint_from_descriptor(
                canonical_descriptor,
            )
        except HarnessFailure:
            raise
        except (UnicodeError, TypeError, ValueError, AttributeError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if canonical_descriptor != descriptor_raw[:-1] or fingerprint != computed_fingerprint:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        return fingerprint

    def cached_owner_matches_entry(entry: _ReportValidationEntry) -> bool:
        owner = cached_owner
        return bool(
            type(owner) is tuple
            and len(owner) == 10
            and owner[0] is entry.pytest_root
            and owner[1] is entry.registration
            and owner[2] is entry.run
            and owner[3] is entry.ledger
            and owner[4] is entry.receipt
            and owner[5] is entry.evidence
            and owner[6] is entry.report
            and owner[9] is entry.context_run
        )

    def validate_entry_bindings(entry: _ReportValidationEntry) -> None:
        if type(entry) is not _ReportValidationEntry:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        owner = cached_owner
        if (
            owner is None
            or type(owner) is not tuple
            or len(owner) != 10
            or owner[0] is not entry.pytest_root
            or owner[1] is not entry.registration
            or owner[2] is not entry.run
            or owner[3] is not entry.ledger
            or owner[4] is not entry.receipt
            or owner[5] is not entry.evidence
            or owner[6] is not entry.report
            or owner[9] is not entry.context_run
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        original_issued_ns = owner[7]
        original_expires_ns = owner[8]
        if (
            type(original_issued_ns) is not int
            or type(original_expires_ns) is not int
            or original_issued_ns < 0
            or original_issued_ns > maximum_contract_integer - validation_lifetime_ns
            or original_expires_ns != original_issued_ns + validation_lifetime_ns
            or type(entry.issued_ns) is not int
            or type(entry.expires_ns) is not int
            or entry.issued_ns != original_issued_ns
            or not original_issued_ns <= entry.expires_ns <= original_expires_ns
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            root_registration = root_lookup(entry.pytest_root)
            root_identity_live = root_identity_is_valid(entry.registration)
            root_session_live = root_session_owns(entry.registration, False)
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if root_registration is None or not root_identity_live or not root_session_live:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        if root_registration is not entry.registration:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if not (
            entry.contract_generation == _TASK064_REPORT_VALIDATION_GENERATION
            and entry.contract_sha256 == _TASK064_REPORT_VALIDATION_CONTRACT_SHA256
            and entry.process_id == real_getpid()
            and entry.thread_id == real_get_ident()
            and entry.registration.path_object is entry.pytest_root
            and entry.registration.node_id == entry.node_id
            and entry.run is entry.receipt.run
            and entry.run._pytest_registration is entry.registration
            and entry.ledger is entry.registration.evidence_ledger
            and entry.ledger.run is entry.run
            and entry.ledger.receipt is entry.receipt
            and not entry.ledger.recording
            and not entry.ledger.closed
            and not entry.ledger.consumed
            and entry.receipt.evidence is entry.evidence
            and entry.receipt.evidence_digest == entry.evidence_digest
            and entry.report.evidence is entry.evidence
            and active_evidence_run.get() is entry.context_run
            and entry.context_run is None
            and entry.sqlite_source_id == ACCEPTED_SQLITE_SOURCE_ID
            and entry.schema_fingerprint == entry.report.schema_fingerprint
            and type(entry.raw) is bytes
            and entry.raw_length == len(entry.raw)
            and entry.raw_length > 0
            and type(entry.epoch) is _ReportLiveEpoch
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            current_schema_fingerprint = cached_schema_fingerprint()
        except HarnessFailure:
            raise
        if entry.schema_fingerprint != current_schema_fingerprint:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        try:
            current_source_fingerprints = report_source_fingerprints()
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if entry.source_fingerprints != source_fingerprints:
            raise HarnessFailure(HarnessFailureCode.CORRUPT)
        if current_source_fingerprints != source_fingerprints:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        try:
            current_report_core = report_core_digest(
                entry.report,
                entry.evidence_digest,
            )
            current_evidence_digest = evidence_digest(entry.evidence)
            current_raw_sha256 = digest_constructor(entry.raw).hexdigest()
            current_epoch_sha256 = digest_constructor(entry.epoch.raw).hexdigest()
            current_live_validation_sha256 = evidence_digest(
                (
                    "TASK064-REPORT-LIVE-VALIDATION-V1",
                    entry.epoch.observations,
                )
            )
            rebuilt_epoch = build_live_epoch(
                entry.epoch.snapshot,
                entry.epoch.observations,
            )
        except HarnessFailure:
            raise
        except (TypeError, ValueError, AttributeError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        except BaseException:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if (
            current_report_core != entry.report_core_sha256
            or current_evidence_digest != entry.evidence_digest
            or current_raw_sha256 != entry.raw_sha256
            or current_epoch_sha256 != entry.epoch.sha256
            or current_live_validation_sha256 != entry.live_validation_sha256
            or rebuilt_epoch.raw != entry.epoch.raw
        ):
            raise HarnessFailure(HarnessFailureCode.CORRUPT)

    def entry_bindings_are_live(entry: _ReportValidationEntry) -> bool:
        try:
            validate_entry_bindings(entry)
        except BaseException:
            return False
        return True

    def fail_reentrant_entry() -> Never:
        mark_cleanup_uncertain()
        invalidate_active_cache_attempt()
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    def raise_sanitized(error: BaseException) -> Never:
        if isinstance(error, HarnessFailure):
            if error.code in {
                HarnessFailureCode.CORRUPT,
                HarnessFailureCode.INVALID_TOKEN,
                HarnessFailureCode.UNSUPPORTED_VERSION,
                HarnessFailureCode.BOUNDS_EXCEEDED,
                HarnessFailureCode.UNPROVEN,
            }:
                raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        if isinstance(error, (TypeError, ValueError, AttributeError)):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None
        raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None

    def serialize_report(document: Mapping[str, object]) -> bytes:
        try:
            return (
                json_serializer(
                    document,
                    allow_nan=False,
                    ensure_ascii=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                + b"\n"
            )
        except MemoryError:
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
        except (TypeError, ValueError):
            raise HarnessFailure(HarnessFailureCode.CORRUPT) from None

    def validate_receipt(
        pytest_root: Path,
        receipt: _EvidenceReceipt,
        evidence: GeneratedEvidenceAggregate,
        require_exact_evidence: bool,
    ) -> _EvidenceLedger:
        return validation_implementation(
            validate_issued_receipt,
            pytest_root,
            receipt,
            evidence,
            require_exact_evidence,
        )

    def writer(
        pytest_root: Path,
        *,
        receipt: _EvidenceReceipt,
        report: EvidenceReport,
    ) -> Path:
        nonlocal cache_state
        nonlocal active_attempt
        nonlocal active_registration
        nonlocal attempt_serial
        with cache_entry_scope() as (
            scope_invalidation_generation,
            state_at_entry,
            entry_at_entry,
        ):
            if invalidation_generation != scope_invalidation_generation:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if cache_state != state_at_entry or cached_entry is not entry_at_entry:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if state_at_entry in {"VALIDATING", "PUBLISHING"}:
                fail_reentrant_entry()

            def accept_prepared(
                _ledger: _EvidenceLedger,
                _raw: bytes,
            ) -> bool:
                return True

            def accept_live_source() -> bool:
                return True

            def finish_unprimed(_retryable: bool) -> None:
                return None

            if state_at_entry == "EMPTY":
                if (
                    entry_at_entry is not None
                    or cached_owner is not None
                    or active_registration is not None
                ):
                    mark_cleanup_uncertain()
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                result = writer_implementation(
                    validate_receipt,
                    prepare_receipt_consumption,
                    publisher_implementation,
                    mark_cleanup_uncertain,
                    cleanup_uncertain_observed,
                    root_lookup,
                    serialize_report,
                    generation3_schema_fingerprint_provider,
                    register_published_artifact,
                    pytest_root,
                    receipt=receipt,
                    report=report,
                    validate_live=True,
                    preserve_live_unavailable=False,
                    publish=True,
                    validate_prepared_report=accept_prepared,
                    validate_live_source_before_link=accept_live_source,
                    finalize_live_source=finish_unprimed,
                )
                if type(result) is not type(pytest_root):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                return result
            if state_at_entry != "READY" or type(entry_at_entry) is not _ReportValidationEntry:
                mark_cleanup_uncertain()
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            entry = entry_at_entry
            if not call_selects_entry_owner(entry, pytest_root, receipt, report):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if not cached_owner_matches_entry(entry):
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if not call_context_matches_entry(entry):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            try:
                now = clock_value(entry.issued_ns)
            except HarnessFailure:
                clear_cache()
                raise
            if now >= entry.expires_ns:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            try:
                validate_entry_bindings(entry)
            except BaseException as error:
                clear_cache()
                raise_sanitized(error)
            attempt_serial += 1
            attempt = attempt_serial
            active_attempt = attempt
            active_registration = entry.registration
            cache_state = "PUBLISHING"
            if invalidation_generation != scope_invalidation_generation:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            try:
                writer_snapshot = capture_file_snapshot(entry.evidence)
                writer_epoch = build_live_epoch(
                    writer_snapshot,
                    entry.epoch.observations,
                )
            except BaseException as error:
                clear_cache()
                raise_sanitized(error)
            if cache_state != "PUBLISHING" or active_attempt != attempt:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if not same_snapshot_bindings(entry.epoch.snapshot, writer_snapshot):
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if not same_file_snapshot(entry.epoch.snapshot, writer_snapshot):
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if writer_epoch.raw != entry.epoch.raw:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.CORRUPT)

            def validate_cached_prepared(
                ledger: _EvidenceLedger,
                raw: bytes,
            ) -> bool:
                return bool(
                    cache_state == "PUBLISHING"
                    and active_attempt == attempt
                    and cached_entry is entry
                    and ledger is entry.ledger
                    and raw == entry.raw
                    and len(raw) == entry.raw_length
                    and digest_constructor(raw).hexdigest() == entry.raw_sha256
                )

            def validate_cached_live_source() -> bool:
                if (
                    cache_state != "PUBLISHING"
                    or active_attempt != attempt
                    or cached_entry is not entry
                ):
                    clear_cache()
                    return False
                try:
                    validate_entry_bindings(entry)
                except BaseException as error:
                    clear_cache()
                    raise_sanitized(error)
                try:
                    current_snapshot = capture_file_snapshot(entry.evidence)
                    current_epoch = build_live_epoch(
                        current_snapshot,
                        entry.epoch.observations,
                    )
                    current_clock = clock_value(entry.issued_ns)
                except BaseException as error:
                    clear_cache()
                    raise_sanitized(error)
                if current_clock >= entry.expires_ns:
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if not same_snapshot_bindings(entry.epoch.snapshot, current_snapshot):
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if not same_file_snapshot(entry.epoch.snapshot, current_snapshot):
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if current_epoch.raw != entry.epoch.raw:
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                return True

            publication_finalized = False

            def finalize_cached_live_source(retryable: bool) -> None:
                nonlocal publication_finalized
                nonlocal cache_state
                nonlocal active_attempt
                if publication_finalized:
                    return
                publication_finalized = True
                if (
                    not retryable
                    or cleanup_uncertain_observed()
                    or cache_state != "PUBLISHING"
                    or active_attempt != attempt
                    or cached_entry is not entry
                    or not entry_bindings_are_live(entry)
                ):
                    clear_cache()
                    return
                try:
                    current_clock = clock_value(entry.issued_ns)
                    current_snapshot = capture_file_snapshot(entry.evidence)
                    current_epoch = build_live_epoch(
                        current_snapshot,
                        entry.epoch.observations,
                    )
                except BaseException:
                    clear_cache()
                    return
                if (
                    current_clock >= entry.expires_ns
                    or not same_file_snapshot(entry.epoch.snapshot, current_snapshot)
                    or current_epoch.raw != entry.epoch.raw
                ):
                    clear_cache()
                    return
                cache_state = "READY"
                active_attempt = None

            try:
                result = writer_implementation(
                    validate_receipt,
                    prepare_receipt_consumption,
                    publisher_implementation,
                    mark_cleanup_uncertain,
                    cleanup_uncertain_observed,
                    root_lookup,
                    serialize_report,
                    cached_schema_fingerprint,
                    register_published_artifact,
                    pytest_root,
                    receipt=receipt,
                    report=report,
                    validate_live=False,
                    preserve_live_unavailable=False,
                    publish=True,
                    validate_prepared_report=validate_cached_prepared,
                    validate_live_source_before_link=validate_cached_live_source,
                    finalize_live_source=finalize_cached_live_source,
                )
            except BaseException as error:
                if not publication_finalized:
                    clear_cache()
                if cache_state == "PUBLISHING" or active_attempt == attempt:
                    clear_cache()
                if invalidation_generation != scope_invalidation_generation:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
                raise_sanitized(error)
            if not publication_finalized:
                mark_cleanup_uncertain()
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if type(result) is not type(pytest_root) or cache_state != "EMPTY":
                mark_cleanup_uncertain()
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            return result

    def prime(
        pytest_root: Path,
        *,
        receipt: _EvidenceReceipt,
        report: EvidenceReport,
    ) -> None:
        nonlocal cache_state
        nonlocal cached_entry
        nonlocal cached_owner
        nonlocal attempt_serial
        nonlocal active_attempt
        nonlocal active_registration
        with cache_entry_scope() as (
            scope_invalidation_generation,
            state_at_entry,
            entry_at_entry,
        ):
            if invalidation_generation != scope_invalidation_generation:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if cache_state != state_at_entry or cached_entry is not entry_at_entry:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            if state_at_entry in {"VALIDATING", "PUBLISHING"}:
                fail_reentrant_entry()
            if state_at_entry == "READY":
                entry = entry_at_entry
                if type(entry) is _ReportValidationEntry and call_selects_entry_owner(
                    entry,
                    pytest_root,
                    receipt,
                    report,
                ):
                    if not cached_owner_matches_entry(entry):
                        clear_cache()
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    if not call_context_matches_entry(entry):
                        raise HarnessFailure(HarnessFailureCode.CORRUPT)
                    try:
                        now = clock_value(entry.issued_ns)
                    except HarnessFailure:
                        clear_cache()
                        raise
                    if now >= entry.expires_ns:
                        clear_cache()
                    else:
                        try:
                            validate_entry_bindings(entry)
                        except BaseException as error:
                            clear_cache()
                            raise_sanitized(error)
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            if state_at_entry != "EMPTY" or entry_at_entry is not None:
                mark_cleanup_uncertain()
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            attempt_serial += 1
            attempt = attempt_serial
            active_attempt = attempt
            cache_state = "VALIDATING"
            if invalidation_generation != scope_invalidation_generation:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

            def accept_prepared(
                _ledger: _EvidenceLedger,
                _raw: bytes,
            ) -> bool:
                return cache_state == "VALIDATING" and active_attempt == attempt

            def unused_live_source() -> bool:
                return False

            def unused_finalizer(_retryable: bool) -> None:
                return None

            try:
                if cleanup_uncertain_observed():
                    clear_cache()
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                registration = root_lookup(pytest_root)
                if registration is None or registration.path_object is not pytest_root:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                active_registration = registration
                context_run = active_evidence_run.get()
                if context_run is not None:
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                before = capture_file_snapshot(report.evidence)
                prepared = writer_implementation(
                    validate_receipt,
                    prepare_receipt_consumption,
                    publisher_implementation,
                    mark_cleanup_uncertain,
                    cleanup_uncertain_observed,
                    root_lookup,
                    serialize_report,
                    cached_schema_fingerprint,
                    register_published_artifact,
                    pytest_root,
                    receipt=receipt,
                    report=report,
                    validate_live=True,
                    preserve_live_unavailable=True,
                    publish=False,
                    validate_prepared_report=accept_prepared,
                    validate_live_source_before_link=unused_live_source,
                    finalize_live_source=unused_finalizer,
                )
                if type(prepared) is not _PreparedEvidenceReport:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                after = capture_file_snapshot(report.evidence)
                if cache_state != "VALIDATING" or active_attempt != attempt:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                if not same_snapshot_bindings(before, after):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                if not same_validation_boundary(before, after):
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                observations = prepared.live_observations
                epoch = build_live_epoch(after, observations)
                live_validation_sha256 = evidence_digest(
                    (
                        "TASK064-REPORT-LIVE-VALIDATION-V1",
                        observations,
                    )
                )
                if (
                    receipt.run._pytest_registration is not registration
                    or active_evidence_run.get() is not context_run
                    or prepared.ledger is not registration.evidence_ledger
                ):
                    raise HarnessFailure(HarnessFailureCode.CORRUPT)
                issued_ns = clock_value()
                if issued_ns > maximum_contract_integer - validation_lifetime_ns:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                expires_ns = issued_ns + validation_lifetime_ns
                raw = prepared.raw
                entry = _ReportValidationEntry(
                    pytest_root=pytest_root,
                    registration=registration,
                    run=receipt.run,
                    ledger=prepared.ledger,
                    receipt=receipt,
                    evidence=receipt.evidence,
                    report=report,
                    process_id=real_getpid(),
                    thread_id=real_get_ident(),
                    node_id=registration.node_id,
                    context_run=context_run,
                    contract_generation=_TASK064_REPORT_VALIDATION_GENERATION,
                    contract_sha256=_TASK064_REPORT_VALIDATION_CONTRACT_SHA256,
                    schema_fingerprint=prepared.schema_fingerprint,
                    sqlite_source_id=ACCEPTED_SQLITE_SOURCE_ID,
                    source_fingerprints=source_fingerprints,
                    evidence_digest=receipt.evidence_digest,
                    report_core_sha256=report_core_digest(
                        report,
                        receipt.evidence_digest,
                    ),
                    live_validation_sha256=live_validation_sha256,
                    raw=raw,
                    raw_length=len(raw),
                    raw_sha256=digest_constructor(raw).hexdigest(),
                    epoch=epoch,
                    issued_ns=issued_ns,
                    expires_ns=expires_ns,
                )
                if cache_state != "VALIDATING" or active_attempt != attempt:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
                cached_owner = (
                    pytest_root,
                    registration,
                    receipt.run,
                    prepared.ledger,
                    receipt,
                    receipt.evidence,
                    report,
                    issued_ns,
                    expires_ns,
                    context_run,
                )
                validate_entry_bindings(entry)
                cached_entry = entry
                cache_state = "READY"
                active_attempt = None
            except BaseException as error:
                if active_attempt == attempt or cache_state == "VALIDATING":
                    clear_cache()
                if invalidation_generation != scope_invalidation_generation:
                    raise HarnessFailure(HarnessFailureCode.UNAVAILABLE) from None
                raise_sanitized(error)
            return None

    def observe() -> tuple[str, bool, int | None, int | None]:
        caller = (real_getpid(), real_get_ident())
        entry_gate.acquire()
        if cache_lock_owner == caller:
            try:
                mark_cleanup_uncertain()
                invalidate_active_cache_attempt()
            finally:
                entry_gate.release()
            raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
        entry_gate.release()
        with cache_lock:
            entry = cached_entry
            return (
                cache_state,
                type(entry) is _ReportValidationEntry,
                entry.issued_ns if type(entry) is _ReportValidationEntry else None,
                entry.expires_ns if type(entry) is _ReportValidationEntry else None,
            )

    def force_expiry() -> None:
        nonlocal cached_entry
        with cache_entry_scope() as (
            scope_invalidation_generation,
            state_at_entry,
            entry_at_entry,
        ):
            if (
                invalidation_generation != scope_invalidation_generation
                or cache_state != state_at_entry
                or cached_entry is not entry_at_entry
            ):
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)
            entry = entry_at_entry
            if (
                state_at_entry != "READY"
                or type(entry) is not _ReportValidationEntry
                or entry.process_id != real_getpid()
                or entry.thread_id != real_get_ident()
                or active_evidence_run.get() is not entry.context_run
            ):
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            try:
                validate_entry_bindings(entry)
            except BaseException as error:
                clear_cache()
                raise_sanitized(error)
            try:
                observed = clock_value(entry.issued_ns)
            except HarnessFailure:
                clear_cache()
                raise
            if observed >= entry.expires_ns:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.CORRUPT)
            cached_entry = replace_value(entry, expires_ns=observed)
            if invalidation_generation != scope_invalidation_generation:
                clear_cache()
                raise HarnessFailure(HarnessFailureCode.UNAVAILABLE)

    def root_cache_teardown(identity: _ActivePytestRoot) -> bool:
        nonlocal invalidation_generation
        entry_gate.acquire()
        acquired = cache_lock.acquire(blocking=False)
        try:
            entry = cached_entry
            owner = (
                entry.registration if type(entry) is _ReportValidationEntry else active_registration
            )
            if cache_state == "EMPTY" and entry is None and active_registration is None:
                return True
            if (
                not acquired
                and owner is None
                and cache_state in {"VALIDATING", "PUBLISHING"}
                and active_attempt is not None
            ):
                invalidation_generation += 1
                clear_cache()
                return True
            if owner is not identity:
                return True
            if not acquired:
                invalidation_generation += 1
            clear_cache()
            return bool(
                cache_state == "EMPTY" and cached_entry is None and active_registration is None
            )
        finally:
            if acquired:
                cache_lock.release()
            entry_gate.release()

    def fixture_cache_teardown(pytest_root: Path) -> bool:
        identity = root_lookup(pytest_root)
        if identity is None or identity.path_object is not pytest_root:
            return False
        return root_cache_teardown(identity)

    def clear_after_fork() -> None:
        nonlocal cache_lock
        nonlocal cache_lock_owner
        nonlocal entry_gate
        nonlocal invalidation_generation
        nonlocal last_clock_ns
        cache_lock = lock_constructor()
        entry_gate = lock_constructor()
        cache_lock_owner = None
        invalidation_generation = 0
        last_clock_ns = None
        clear_cache()

    if register_at_fork is not None:
        register_at_fork(after_in_child=clear_after_fork)

    def report_close_probe_writer(
        pytest_root: Path,
        *,
        permit: _Task064ReportPublicationPermit,
        artifact: bytes,
    ) -> Path:
        def validate_before_link() -> bool:
            return validate_report_permit(permit, pytest_root, artifact)

        def prepare_consumption() -> tuple[
            Callable[[], bool],
            Callable[[], bool],
            Callable[[], bool],
        ]:
            return prepare_report_permit_consumption(permit)

        def validate_live_source_before_link() -> bool:
            return True

        def finalize_live_source(_retryable: bool) -> None:
            return None

        return publisher_implementation(
            mark_cleanup_uncertain,
            cleanup_uncertain_observed,
            root_lookup,
            pytest_root,
            artifact,
            validate_before_link=validate_before_link,
            validate_live_source_before_link=validate_live_source_before_link,
            finalize_live_source=finalize_live_source,
            prepare_consumption=prepare_consumption,
        )

    return (
        writer,
        report_close_probe_writer,
        prime,
        observe,
        force_expiry,
        fixture_cache_teardown,
        root_cache_teardown,
    )


(
    write_evidence_report,
    _publish_task064_report_close_probe,
    prime_evidence_report_validation,
    _observe_task064_report_validation_for_test,
    _force_task064_report_validation_expiry_for_test,
    _teardown_task064_report_validation_at_fixture_exit,
    _task064_report_validation_root_teardown,
) = _build_evidence_report_writer(
    _validate_issued_evidence_receipt,
    _prepare_issued_evidence_receipt_consumption,
    _validate_task064_report_publication_permit,
    _prepare_task064_report_publication_permit_consumption,
    _register_task064_published_report_artifact,
    _task064_report_source_fingerprints,
)
_bind_pytest_root_report_cache_teardown(
    _task064_report_validation_root_teardown,
)
del _bind_pytest_root_report_cache_teardown
del _task064_report_validation_root_teardown
for _closed_report_writer_authority_name in (
    "_build_evidence_report_writer",
    "_valid_task064_report_file_observation",
    "_validate_evidence_receipt_unbound",
    "_write_evidence_report_unbound",
    "_publish_evidence_report_bytes_unbound",
    "_capture_task064_report_file_snapshot",
    "_same_task064_report_file_snapshot",
    "_same_task064_report_validation_boundary",
    "_same_task064_report_snapshot_bindings",
    "_build_task064_report_live_epoch",
    "_report_core_digest",
    "_validate_issued_evidence_receipt",
    "_prepare_issued_evidence_receipt_consumption",
    "_validate_task064_report_publication_permit",
    "_prepare_task064_report_publication_permit_consumption",
    "_register_task064_published_report_artifact",
    "_task064_report_source_fingerprints",
):
    globals().pop(_closed_report_writer_authority_name, None)
globals().pop("_closed_report_writer_authority_name", None)
