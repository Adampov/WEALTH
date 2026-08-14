"""Shared deterministic fixtures and the inert TASK-064 CI observer."""

from __future__ import annotations

import argparse
import errno
import fcntl
import importlib.metadata
import json
import math
import mmap
import os
import signal
import stat
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path, PurePath
from types import FunctionType
from typing import Protocol, cast

import _pytest.pathlib as pytest_pathlib
import pytest

_CONTRACT_GENERATION = 6
_CONTRACT_SHA256 = "ec89a1df740805cc9b43e6f2530e940c0bf9b66e8f25ed878d3207d091c4bcb8"
_CI_PHASES = ("collect", "execute")
_CI_SHARDS = (
    "report",
    "remainder-0",
    "remainder-1",
    "remainder-2",
    "remainder-3",
    "report-proof-unprimed-success",
    "report-proof-expired-entry",
    "report-proof-ready-teardown",
)
_REPORT_PROOF_MODES = (
    "primed-full",
    "unprimed-success",
    "expired-entry",
    "ready-teardown",
)
_EXPECTED_ADDOPTS = (
    "--strict-config",
    "--strict-markers",
    "--import-mode=importlib",
)
_EXPECTED_EXTERNAL_PLUGINS = [
    [
        "hypothesis",
        "6.157.1",
        "pytest11",
        "hypothesispytest",
        "_hypothesis_pytestplugin",
    ]
]
_HEX_DIGITS = frozenset("0123456789abcdef")
_COLLECTION_PACKET_LIMIT = 400_000
_EXECUTION_PACKET_LIMIT = 1_500_000
_PROOF_PACKET_LIMIT = 4_096
_IO_CHUNK = 65_536
_REPORT_NODE = (
    "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py::"
    "test_finite_typical_workload_measurements_and_sanitized_report"
)
_FORBIDDEN_ACTIVE_ENV = (
    "PYTEST_ADDOPTS",
    "PYTEST_PLUGINS",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
    "PYTHONPATH",
    "PYTHONHOME",
)
_RUNTIME_UID = os.getuid()
_RUNTIME_EUID = os.geteuid()
_RUNTIME_GID = os.getgid()
_RUNTIME_EGID = os.getegid()
_FD_CLOEXEC = fcntl.FD_CLOEXEC
_ACCESS_MODE_MASK = os.O_ACCMODE
_READ_ONLY = os.O_RDONLY
_WRITE_ONLY = os.O_WRONLY
_READ_WRITE = os.O_RDWR


class _ObserverFailure(RuntimeError):
    """Represent a closed TASK-064 observer failure."""


class _ScandirStream(Protocol):
    def __iter__(self) -> Iterator[os.DirEntry[str]]: ...

    def __next__(self) -> os.DirEntry[str]: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class _DescriptorSnapshot:
    device: int
    inode: int
    uid: int
    mode: int
    link_count: int
    size: int
    mtime_ns: int
    ctime_ns: int


@dataclass
class _ObservationHandle:
    descriptor: int
    _initial: _DescriptorSnapshot | None
    label: str
    terminal: bool = False

    @property
    def initial(self) -> _DescriptorSnapshot:
        if self._initial is None:
            raise _ObserverFailure(f"{self.label} initial identity is unavailable")
        return self._initial

    def detach(self) -> int:
        if self.terminal:
            raise _ObserverFailure(f"{self.label} owner is already terminal")
        descriptor = self.descriptor
        self.descriptor = -1
        self.terminal = True
        return descriptor


@dataclass
class _MmapOwner:
    mapping: mmap.mmap | None
    terminal: bool = False

    def require(self) -> mmap.mmap:
        if self.terminal or self.mapping is None:
            raise _ObserverFailure("fork-poison owner is terminal")
        return self.mapping

    def detach(self) -> mmap.mmap:
        mapping = self.require()
        self.mapping = None
        self.terminal = True
        return mapping


@dataclass(frozen=True)
class _Task064RawInvocation:
    phase: str
    nonce: str
    observation_fd: int
    shard_id: str | None
    proof_mode: str | None
    proof_nonce: str | None
    proof_observation_fd: int | None
    selector_nodes: tuple[str, ...]
    basetemp: Path


@dataclass(frozen=True)
class _Task064ProofRecord:
    publication_status: str
    output_bytes: int | None
    output_sha256: str | None
    cache_state_at_assertion: str
    receipt_consumed: bool
    final_file_present: bool
    stage_residue_count: int
    teardown: Callable[[], None]
    observe: Callable[[], tuple[str, bool, int | None, int | None]]


class Task064ReportProofRecorder(Protocol):
    """Private callable supplied only to the frozen report node."""

    def __call__(
        self,
        *,
        publication_status: str,
        output_bytes: int | None,
        output_sha256: str | None,
        cache_state_at_assertion: str,
        receipt_consumed: bool,
        final_file_present: bool,
        stage_residue_count: int,
        teardown: Callable[[], None],
        observe: Callable[[], tuple[str, bool, int | None, int | None]],
    ) -> None: ...


def _ascii_text(value: object, *, label: str) -> str:
    if type(value) is not str or "\x00" in value or "\r" in value or "\n" in value:
        raise _ObserverFailure(f"invalid {label}")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as error:
        raise _ObserverFailure(f"non-ASCII {label}") from error
    return value


def _hex_64(value: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in _HEX_DIGITS for character in value)
    ):
        raise argparse.ArgumentTypeError("value must be 64 lowercase hexadecimal digits")
    return value


def _canonical_descriptor(value: object, *, label: str) -> int:
    text = _ascii_text(value, label=label)
    if not text.isdecimal() or text.startswith("0"):
        raise _ObserverFailure(f"noncanonical {label}")
    descriptor = int(text, 10)
    if type(descriptor) is not int or descriptor <= 2 or str(descriptor) != text:
        raise _ObserverFailure(f"invalid {label}")
    return descriptor


def _exact_integer(value: object, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise _ObserverFailure(f"invalid {label}")
    return value


def _snapshot(descriptor: int) -> _DescriptorSnapshot:
    status = os.fstat(descriptor)
    values = (
        status.st_dev,
        status.st_ino,
        status.st_uid,
        status.st_mode,
        status.st_nlink,
        status.st_size,
        status.st_mtime_ns,
        status.st_ctime_ns,
    )
    if any(type(value) is not int or value < 0 for value in values):
        raise _ObserverFailure("invalid descriptor metadata")
    return _DescriptorSnapshot(*values)


def _validate_adopted_observation(handle: _ObservationHandle) -> _ObservationHandle:
    descriptor = handle.descriptor
    label = handle.label
    primary: BaseException | None = None
    try:
        access_flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        if type(access_flags) is not int or access_flags & os.O_ACCMODE != os.O_RDWR:
            raise _ObserverFailure(f"{label} descriptor is not read-write")
        prior_flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        if type(prior_flags) is not int or prior_flags < 0:
            raise _ObserverFailure(f"invalid {label} descriptor flags")
        expected_flags = prior_flags | fcntl.FD_CLOEXEC
        result = fcntl.fcntl(descriptor, fcntl.F_SETFD, expected_flags)
        if type(result) is not int or result != 0:
            raise _ObserverFailure(f"failed to restore {label} CLOEXEC")
        readback_flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        if type(readback_flags) is not int or readback_flags != expected_flags:
            raise _ObserverFailure(f"{label} CLOEXEC readback differs")
        initial = _snapshot(descriptor)
        if (
            not stat.S_ISREG(initial.mode)
            or initial.uid != os.getuid()
            or stat.S_IMODE(initial.mode) != 0o600
            or initial.link_count != 0
            or initial.size != 0
        ):
            raise _ObserverFailure(f"invalid anonymous {label} object")
        handle._initial = initial
    except BaseException as error:
        primary = error
    if primary is not None:
        try:
            _close_once(handle)
        except BaseException as close_error:
            primary.add_note(f"{label} validation close uncertainty: {close_error!r}")
        raise primary
    return handle


def _validate_initial_observation(descriptor: int, *, label: str) -> _ObservationHandle:
    if type(descriptor) is not int or descriptor <= 2:
        raise _ObserverFailure(f"invalid {label} descriptor")
    return _validate_adopted_observation(
        _ObservationHandle(descriptor=descriptor, _initial=None, label=label)
    )


def _canonical_json_bytes(packet: dict[str, object], *, limit: int) -> bytes:
    def validate(value: object) -> None:
        if isinstance(value, float) and not math.isfinite(value):
            raise _ObserverFailure("non-finite observation value")
        if isinstance(value, dict):
            for key, child in value.items():
                _ascii_text(key, label="observation key")
                validate(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                validate(child)
        elif isinstance(value, str):
            _ascii_text(value, label="observation value")

    validate(packet)
    encoded = json.dumps(
        packet,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > limit:
        raise _ObserverFailure("observation exceeds its byte bound")
    return encoded


def _close_once(handle: _ObservationHandle) -> None:
    descriptor = handle.detach()
    if descriptor <= 2:
        raise _ObserverFailure(f"{handle.label} descriptor is not owned")
    try:
        os.close(descriptor)
    except BaseException as error:
        raise _ObserverFailure(f"uncertain {handle.label} close") from error


def _write_packet(handle: _ObservationHandle, payload: bytes, *, limit: int) -> None:
    primary: BaseException | None = None
    try:
        if type(payload) is not bytes or len(payload) > limit:
            raise _ObserverFailure("invalid observation payload")
        descriptor = handle.descriptor
        if descriptor <= 2 or _snapshot(descriptor) != handle.initial:
            raise _ObserverFailure(f"{handle.label} initial identity changed")
        offset = 0
        while offset < len(payload):
            remaining = len(payload) - offset
            chunk = payload[offset : offset + min(_IO_CHUNK, remaining)]
            written = os.pwrite(descriptor, chunk, offset)
            if type(written) is not int or written <= 0 or written > len(chunk):
                raise _ObserverFailure("invalid positional write progress")
            offset += written
        if offset != len(payload):
            raise _ObserverFailure("observation write length differs")
        os.fsync(descriptor)
        written_snapshot = _snapshot(descriptor)
        if (
            written_snapshot.device != handle.initial.device
            or written_snapshot.inode != handle.initial.inode
            or written_snapshot.uid != handle.initial.uid
            or written_snapshot.mode != handle.initial.mode
            or written_snapshot.link_count != handle.initial.link_count
            or written_snapshot.size != len(payload)
        ):
            raise _ObserverFailure("observation post-write identity differs")
        reproduced = bytearray()
        read_offset = 0
        while read_offset < len(payload):
            requested = min(_IO_CHUNK, len(payload) - read_offset)
            chunk = os.pread(descriptor, requested, read_offset)
            if type(chunk) is not bytes or not chunk or len(chunk) > requested:
                raise _ObserverFailure("invalid positional read progress")
            reproduced.extend(chunk)
            read_offset += len(chunk)
        trailing = os.pread(descriptor, 1, len(payload))
        if type(trailing) is not bytes or trailing != b"":
            raise _ObserverFailure("observation trailing EOF differs")
        if bytes(reproduced) != payload:
            raise _ObserverFailure("observation readback differs")
        if _snapshot(descriptor) != written_snapshot:
            raise _ObserverFailure("observation metadata changed during readback")
    except BaseException as error:
        primary = error
    try:
        _close_once(handle)
    except BaseException as close_error:
        if primary is not None:
            primary.add_note(f"{handle.label} close uncertainty: {close_error!r}")
        else:
            primary = close_error
    if primary is not None:
        raise primary


@dataclass(frozen=True, slots=True)
class _ExactDescriptorStat:
    device: int
    inode: int
    rdev: int
    mode: int
    uid: int
    gid: int
    link_count: int
    size: int


@dataclass(frozen=True, slots=True)
class _ExactDescriptorRecord:
    descriptor: int
    target: str
    descriptor_flags: int
    status_flags: int
    status: _ExactDescriptorStat


@dataclass(frozen=True, slots=True)
class _DescriptorProbeOutcome:
    descriptor: int
    state: str
    record: _ExactDescriptorRecord | None


@dataclass(frozen=True, slots=True)
class _ExactCaptureRoot:
    path: str
    status: _ExactDescriptorStat


@dataclass(frozen=True, slots=True)
class _ExactRuntimeDescriptorTopology:
    capture_root: _ExactCaptureRoot
    named_null: _ExactDescriptorStat
    named_urandom: _ExactDescriptorStat
    standards: tuple[
        _ExactDescriptorRecord,
        _ExactDescriptorRecord,
        _ExactDescriptorRecord,
    ]
    observations: tuple[_ExactDescriptorRecord, ...]
    runtime: tuple[_ExactDescriptorRecord, ...]
    scan_transients: tuple[int, ...]


def _exact_descriptor_stat(status: os.stat_result, *, label: str) -> _ExactDescriptorStat:
    if type(label) is not str or label == "":
        raise _ObserverFailure("descriptor metadata is not exact")
    values = (
        status.st_dev,
        status.st_ino,
        status.st_rdev,
        status.st_mode,
        status.st_uid,
        status.st_gid,
        status.st_nlink,
        status.st_size,
    )
    if any(type(value) is not int or value < 0 for value in values):
        raise _ObserverFailure("descriptor metadata is not exact")
    return _ExactDescriptorStat(*values)


def _probe_descriptor_once(
    descriptor: int,
    *,
    label: str,
    ebadf_is_transient: bool = False,
) -> _DescriptorProbeOutcome:
    if (
        type(descriptor) is not int
        or descriptor < 0
        or type(label) is not str
        or label == ""
        or type(ebadf_is_transient) is not bool
    ):
        raise _ObserverFailure("descriptor metadata is not exact")
    try:
        before_raw = os.fstat(descriptor)
    except OSError as error:
        if ebadf_is_transient and error.errno == errno.EBADF:
            return _DescriptorProbeOutcome(descriptor, "EBADF", None)
        message = (
            "descriptor scan transient probe is uncertain"
            if ebadf_is_transient
            else "runtime descriptor probe is uncertain"
        )
        raise _ObserverFailure(message) from error
    except BaseException as error:
        raise _ObserverFailure("runtime descriptor probe is uncertain") from error
    before = _exact_descriptor_stat(before_raw, label=f"{label} before")
    try:
        target = os.readlink(f"/proc/self/fd/{descriptor}")
        descriptor_flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
        status_flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
        after = _exact_descriptor_stat(os.fstat(descriptor), label=f"{label} after")
    except BaseException as error:
        if isinstance(error, _ObserverFailure):
            raise
        raise _ObserverFailure("runtime descriptor probe is uncertain") from error
    if (
        type(target) is not str
        or "\x00" in target
        or "\r" in target
        or "\n" in target
        or type(descriptor_flags) is not int
        or descriptor_flags < 0
        or type(status_flags) is not int
        or status_flags < 0
    ):
        raise _ObserverFailure("descriptor metadata is not exact")
    if before != after:
        raise _ObserverFailure("runtime descriptor probe is uncertain")
    return _DescriptorProbeOutcome(
        descriptor,
        "LIVE",
        _ExactDescriptorRecord(
            descriptor,
            target,
            descriptor_flags,
            status_flags,
            before,
        ),
    )


def _probe_capture_root_once(capture_root: Path) -> _ExactCaptureRoot:
    if type(capture_root) is not type(Path()):
        raise _ObserverFailure("pytest capture root is not canonical")
    path = os.fspath(capture_root)
    if (
        type(path) is not str
        or "\x00" in path
        or "\r" in path
        or "\n" in path
        or not capture_root.is_absolute()
    ):
        raise _ObserverFailure("pytest capture root is not canonical")
    try:
        resolved = capture_root.resolve(strict=True)
        raw_status = os.stat(capture_root, follow_symlinks=False)
    except BaseException as error:
        raise _ObserverFailure("pytest capture root probe is uncertain") from error
    if resolved != capture_root:
        raise _ObserverFailure("pytest capture root is not canonical")
    root_status = _exact_descriptor_stat(raw_status, label="pytest capture root")
    if (
        not stat.S_ISDIR(root_status.mode)
        or root_status.uid not in {_RUNTIME_UID, _RUNTIME_EUID}
        or root_status.gid not in {_RUNTIME_GID, _RUNTIME_EGID}
        or _RUNTIME_UID != _RUNTIME_EUID
        or _RUNTIME_GID != _RUNTIME_EGID
        or stat.S_IMODE(root_status.mode) != 0o700
    ):
        raise _ObserverFailure("pytest capture root identity differs")
    return _ExactCaptureRoot(path, root_status)


def _probe_named_stat_once(path: str, *, label: str) -> _ExactDescriptorStat:
    try:
        raw_status = os.stat(path, follow_symlinks=False)
    except BaseException as error:
        raise _ObserverFailure("runtime descriptor probe is uncertain") from error
    return _exact_descriptor_stat(raw_status, label=label)


def _require_exact_stat_record(value: object) -> _ExactDescriptorStat:
    if type(value) is not _ExactDescriptorStat:
        raise _ObserverFailure("descriptor metadata is not exact")
    values = (
        value.device,
        value.inode,
        value.rdev,
        value.mode,
        value.uid,
        value.gid,
        value.link_count,
        value.size,
    )
    if any(type(item) is not int or item < 0 for item in values):
        raise _ObserverFailure("descriptor metadata is not exact")
    return value


def _require_exact_descriptor_record(value: object) -> _ExactDescriptorRecord:
    if type(value) is not _ExactDescriptorRecord:
        raise _ObserverFailure("descriptor metadata is not exact")
    if (
        type(value.descriptor) is not int
        or value.descriptor < 0
        or type(value.target) is not str
        or "\x00" in value.target
        or "\r" in value.target
        or "\n" in value.target
        or type(value.descriptor_flags) is not int
        or value.descriptor_flags < 0
        or type(value.status_flags) is not int
        or value.status_flags < 0
    ):
        raise _ObserverFailure("descriptor metadata is not exact")
    _require_exact_stat_record(value.status)
    return value


def _classify_exact_runtime_descriptor_topology(
    topology: _ExactRuntimeDescriptorTopology,
) -> tuple[str, ...]:
    """Classify frozen topology facts without descriptor or filesystem authority."""

    if type(topology) is not _ExactRuntimeDescriptorTopology:
        raise _ObserverFailure("descriptor metadata is not exact")
    root = topology.capture_root
    if (
        type(root) is not _ExactCaptureRoot
        or type(root.path) is not str
        or "\x00" in root.path
        or "\r" in root.path
        or "\n" in root.path
        or not Path(root.path).is_absolute()
    ):
        raise _ObserverFailure("pytest capture root is not canonical")
    root_status = _require_exact_stat_record(root.status)
    named_null = _require_exact_stat_record(topology.named_null)
    named_urandom = _require_exact_stat_record(topology.named_urandom)
    if (
        not stat.S_ISDIR(root_status.mode)
        or root_status.uid != _RUNTIME_UID
        or root_status.gid != _RUNTIME_GID
        or stat.S_IMODE(root_status.mode) != 0o700
    ):
        raise _ObserverFailure("pytest capture root identity differs")
    if (
        not stat.S_ISCHR(named_null.mode)
        or not stat.S_ISCHR(named_urandom.mode)
        or named_null == named_urandom
    ):
        raise _ObserverFailure("pytest capture standard identity differs")

    if type(topology.standards) is not tuple or len(topology.standards) != 3:
        raise _ObserverFailure("required inherited descriptor is absent")
    standards = tuple(_require_exact_descriptor_record(value) for value in topology.standards)
    if tuple(value.descriptor for value in standards) != (0, 1, 2):
        raise _ObserverFailure("required inherited descriptor is absent")
    stdin, stdout, stderr = standards
    if (
        tuple(value.descriptor_flags for value in standards) != (0, 0, 0)
        or stdin.target != "/dev/null"
        or stdin.status != named_null
        or stdin.status_flags & _ACCESS_MODE_MASK != _READ_ONLY
        or stdout.status_flags != stderr.status_flags
        or stdout.status_flags & _ACCESS_MODE_MASK != _READ_WRITE
        or stdout.status == stderr.status
    ):
        raise _ObserverFailure("pytest capture standard identity differs")
    for value in (stdout, stderr):
        status = value.status
        if (
            not stat.S_ISREG(status.mode)
            or status.device != root_status.device
            or status.rdev != 0
            or status.uid != _RUNTIME_UID
            or status.gid != root_status.gid
            or stat.S_IMODE(status.mode) != 0o600
            or status.link_count != 0
            or status.size != 0
            or value.target != f"{root.path}/#{status.inode} (deleted)"
        ):
            raise _ObserverFailure("pytest capture standard identity differs")

    if type(topology.scan_transients) is not tuple or any(
        type(value) is not int or value <= 2 for value in topology.scan_transients
    ):
        raise _ObserverFailure("descriptor metadata is not exact")
    if len(topology.scan_transients) != 1:
        raise _ObserverFailure("descriptor scan transient cardinality differs")

    if type(topology.observations) is not tuple:
        raise _ObserverFailure("descriptor metadata is not exact")
    observations = tuple(_require_exact_descriptor_record(value) for value in topology.observations)
    if len(observations) not in {1, 2}:
        raise _ObserverFailure("required inherited descriptor is absent")
    observation_numbers = tuple(value.descriptor for value in observations)
    observation_statuses = tuple(value.status for value in observations)
    if (
        any(value <= 2 for value in observation_numbers)
        or tuple(sorted(observation_numbers)) != observation_numbers
        or len(set(observation_numbers)) != len(observation_numbers)
        or len(set(observation_statuses)) != len(observation_statuses)
        or any(
            value.descriptor_flags != _FD_CLOEXEC
            or value.status_flags & _ACCESS_MODE_MASK != _READ_WRITE
            or not stat.S_ISREG(value.status.mode)
            or value.status.rdev != 0
            or value.status.uid != _RUNTIME_UID
            or stat.S_IMODE(value.status.mode) != 0o600
            or value.status.link_count != 0
            or value.status.size != 0
            for value in observations
        )
    ):
        raise _ObserverFailure("observation descriptor identity differs")

    if type(topology.runtime) is not tuple:
        raise _ObserverFailure("descriptor metadata is not exact")
    runtime = tuple(_require_exact_descriptor_record(value) for value in topology.runtime)
    if len(runtime) > 7:
        raise _ObserverFailure("unexpected inherited descriptor")
    if len(runtime) < 7:
        raise _ObserverFailure("runtime descriptor cardinality differs")
    runtime_numbers = tuple(value.descriptor for value in runtime)
    if (
        any(value <= 2 for value in runtime_numbers)
        or tuple(sorted(runtime_numbers)) != runtime_numbers
        or len(set(runtime_numbers)) != len(runtime_numbers)
    ):
        raise _ObserverFailure("runtime descriptor numbers differ")
    if set(runtime_numbers).intersection(observation_numbers) or set(
        value.status for value in runtime
    ).intersection(observation_statuses):
        raise _ObserverFailure("observation descriptor identity differs")
    allowed_status_flags = {stdin.status_flags, stdout.status_flags, _WRITE_ONLY}
    if any(
        value.descriptor_flags != _FD_CLOEXEC or value.status_flags not in allowed_status_flags
        for value in runtime
    ):
        raise _ObserverFailure("runtime descriptor flags differ")

    urandom = tuple(
        value
        for value in runtime
        if value.target == "/dev/urandom" or value.status == named_urandom
    )
    if len(urandom) != 1:
        raise _ObserverFailure("runtime urandom cardinality differs")
    urandom_record = urandom[0]
    if (
        urandom_record.target != "/dev/urandom"
        or urandom_record.status != named_urandom
        or not stat.S_ISCHR(urandom_record.status.mode)
        or urandom_record.status_flags != stdin.status_flags
        or urandom_record.status_flags & _ACCESS_MODE_MASK != _READ_ONLY
    ):
        raise _ObserverFailure("runtime urandom identity differs")

    classifications: list[str] = []
    pipe_statuses: list[_ExactDescriptorStat] = []
    for value in runtime:
        if value is urandom_record:
            continue
        if value.status == stdin.status:
            if value.target != stdin.target or value.status_flags != stdin.status_flags:
                raise _ObserverFailure("pytest capture topology order differs")
            classifications.append("stdin")
        elif value.status == stdout.status:
            if value.target != stdout.target or value.status_flags != stdout.status_flags:
                raise _ObserverFailure("pytest capture alias identity differs")
            classifications.append("stdout")
        elif value.status == stderr.status:
            if value.target != stderr.target or value.status_flags != stderr.status_flags:
                raise _ObserverFailure("pytest capture alias identity differs")
            classifications.append("stderr")
        elif stat.S_ISFIFO(value.status.mode):
            pipe_status = value.status
            if (
                stat.S_IMODE(pipe_status.mode) != 0o600
                or pipe_status.uid != _RUNTIME_UID
                or pipe_status.gid != _RUNTIME_GID
                or pipe_status.link_count != 1
                or pipe_status.size != 0
                or pipe_status.rdev != 0
                or value.status_flags != _WRITE_ONLY
                or value.target != f"pipe:[{pipe_status.inode}]"
            ):
                raise _ObserverFailure("pytest capture pipe identity differs")
            pipe_statuses.append(pipe_status)
            classifications.append("pipe")
        else:
            raise _ObserverFailure("pytest capture topology order differs")
    if classifications.count("stdout") != 1 or classifications.count("stderr") != 1:
        raise _ObserverFailure("pytest capture alias identity differs")
    if len(pipe_statuses) != 2 or len(set(pipe_statuses)) != 2:
        raise _ObserverFailure("pytest capture pipe identity differs")
    expected = ("stdin", "stdin", "pipe", "stdout", "pipe", "stderr")
    exact = tuple(classifications)
    if exact != expected:
        if (
            exact[:3] == expected[:3]
            and exact[4] == expected[4]
            and exact[3:] == ("stderr", "pipe", "stdout")
        ):
            raise _ObserverFailure("pytest capture alias identity differs")
        raise _ObserverFailure("pytest capture topology order differs")
    return exact


def _observation_matches_initial(
    record: _ExactDescriptorRecord,
    initial: _DescriptorSnapshot,
) -> bool:
    return (
        record.status.device == initial.device
        and record.status.inode == initial.inode
        and record.status.uid == initial.uid
        and record.status.mode == initial.mode
        and record.status.link_count == initial.link_count
        and record.status.size == initial.size
    )


def _collect_exact_runtime_descriptor_topology(
    handles: list[_ObservationHandle],
    *,
    capture_root: Path,
) -> _ExactRuntimeDescriptorTopology:
    root = _probe_capture_root_once(capture_root)
    if type(handles) is not list or any(
        type(handle) is not _ObservationHandle for handle in handles
    ):
        raise _ObserverFailure("descriptor metadata is not exact")
    handle_descriptors = tuple(handle.descriptor for handle in handles)
    if (
        len(handles) not in {1, 2}
        or any(type(value) is not int or value <= 2 for value in handle_descriptors)
        or len(set(handle_descriptors)) != len(handle_descriptors)
    ):
        raise _ObserverFailure("observation descriptor identity differs")
    expected = {0, 1, 2, *handle_descriptors}
    raw_directory: object = None
    iterator: _ScandirStream | None = None
    primary: BaseException | None = None
    topology: _ExactRuntimeDescriptorTopology | None = None
    observed: set[int] = set()
    try:
        raw_directory = os.open(
            "/proc/self/fd",
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
        if type(raw_directory) is not int or raw_directory <= 2 or raw_directory in expected:
            raise _ObserverFailure("invalid inherited-descriptor directory handle")
        iterator = os.scandir(raw_directory)
        for yielded, entry in enumerate(iterator, start=1):
            if yielded > 64:
                raise _ObserverFailure("inherited descriptor count exceeds its cap")
            name = entry.name
            if (
                type(name) is not str
                or not name.isdecimal()
                or (name != "0" and name.startswith("0"))
            ):
                raise _ObserverFailure("noncanonical inherited descriptor name")
            descriptor = int(name, 10)
            if str(descriptor) != name or descriptor in observed:
                raise _ObserverFailure("duplicate inherited descriptor name")
            observed.add(descriptor)
    except BaseException as error:
        primary = error
    if iterator is not None:
        try:
            iterator.close()
        except BaseException as close_error:
            if primary is not None:
                primary.add_note(f"descriptor iterator close uncertainty: {close_error!r}")
            else:
                primary = _ObserverFailure("descriptor iterator close uncertainty")
    if primary is None:
        directory_fd = cast(int, raw_directory)
        expected_with_directory = {*expected, directory_fd}
        if not expected_with_directory.issubset(observed):
            primary = _ObserverFailure("required inherited descriptor is absent")
        else:
            outcomes: dict[int, _DescriptorProbeOutcome] = {}
            try:
                for descriptor in sorted(observed):
                    outcomes[descriptor] = _probe_descriptor_once(
                        descriptor,
                        label="inherited descriptor",
                        ebadf_is_transient=descriptor not in expected_with_directory,
                    )
                if any(
                    outcomes[descriptor].state != "LIVE" for descriptor in expected_with_directory
                ):
                    raise _ObserverFailure("required inherited descriptor is absent")
                transients = tuple(
                    descriptor
                    for descriptor in sorted(observed - expected_with_directory)
                    if outcomes[descriptor].state == "EBADF"
                )
                live_runtime = tuple(
                    cast(_ExactDescriptorRecord, outcomes[descriptor].record)
                    for descriptor in sorted(observed - expected_with_directory)
                    if outcomes[descriptor].state == "LIVE"
                )
                standards = (
                    cast(_ExactDescriptorRecord, outcomes[0].record),
                    cast(_ExactDescriptorRecord, outcomes[1].record),
                    cast(_ExactDescriptorRecord, outcomes[2].record),
                )
                observations = tuple(
                    cast(_ExactDescriptorRecord, outcomes[descriptor].record)
                    for descriptor in handle_descriptors
                )
                if any(
                    not _observation_matches_initial(record, handle.initial)
                    for record, handle in zip(observations, handles, strict=True)
                ):
                    raise _ObserverFailure("observation descriptor identity differs")
                topology = _ExactRuntimeDescriptorTopology(
                    capture_root=root,
                    named_null=_probe_named_stat_once("/dev/null", label="named null"),
                    named_urandom=_probe_named_stat_once(
                        "/dev/urandom",
                        label="named urandom",
                    ),
                    standards=standards,
                    observations=observations,
                    runtime=live_runtime,
                    scan_transients=transients,
                )
            except BaseException as error:
                primary = error
    provisionally_owned = (
        type(raw_directory) is int and raw_directory > 2 and raw_directory not in expected
    )
    if provisionally_owned:
        try:
            os.close(cast(int, raw_directory))
        except BaseException as close_error:
            if primary is not None:
                primary.add_note(f"descriptor directory close uncertainty: {close_error!r}")
            else:
                primary = _ObserverFailure("descriptor directory close uncertainty")
    if primary is not None:
        raise primary
    if topology is None:
        raise _ObserverFailure("runtime descriptor probe is uncertain")
    return topology


def _require_exact_inherited_descriptors(
    handles: list[_ObservationHandle],
    *,
    capture_root: Path,
) -> None:
    topology = _collect_exact_runtime_descriptor_topology(
        handles,
        capture_root=capture_root,
    )
    _classify_exact_runtime_descriptor_topology(topology)


def _raw_option_values(name: str) -> list[str]:
    values: list[str] = []
    arguments = sys.argv[1:]
    for index, argument in enumerate(arguments):
        if argument == name:
            if index + 1 >= len(arguments) or arguments[index + 1].startswith("--"):
                raise pytest.UsageError(f"{name} is missing its value")
            values.append(arguments[index + 1])
        elif argument.startswith(name + "="):
            values.append(argument[len(name) + 1 :])
    if len(values) > 1:
        raise pytest.UsageError(f"duplicate {name} is forbidden")
    return values


def _private_temp_root() -> Path:
    roots: list[Path] = []
    for name in ("TMPDIR", "TEMP", "TMP"):
        raw = os.environ.get(name)
        if raw is None:
            raise _ObserverFailure(f"missing {name}")
        supplied = Path(_ascii_text(raw, label=name))
        if not supplied.is_absolute():
            raise _ObserverFailure(f"{name} is not absolute")
        resolved = supplied.resolve(strict=True)
        if supplied != resolved:
            raise _ObserverFailure(f"{name} is not canonical")
        status = resolved.stat()
        if (
            not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.getuid()
            or stat.S_IMODE(status.st_mode) != 0o700
        ):
            raise _ObserverFailure(f"{name} is not a private directory")
        roots.append(resolved)
    if len(set(roots)) != 1:
        raise _ObserverFailure("child temp roots differ")
    return roots[0]


def _validate_raw_task064_options() -> _Task064RawInvocation | None:
    names = (
        "--task064-ci-phase",
        "--task064-ci-nonce",
        "--task064-ci-observation-fd",
        "--task064-ci-shard-id",
        "--task064-report-proof-mode",
        "--task064-report-proof-nonce",
        "--task064-report-proof-observation-fd",
    )
    values = {name: _raw_option_values(name) for name in names}
    core_present = tuple(bool(values[name]) for name in names[:3])
    shard_present = bool(values[names[3]])
    proof_present = tuple(bool(values[name]) for name in names[4:])
    if not any(core_present) and not shard_present and not any(proof_present):
        return None
    if sys.flags.optimize != 0:
        raise pytest.UsageError("optimized Python is forbidden for TASK-064 observation")
    for name in _FORBIDDEN_ACTIVE_ENV:
        if name in os.environ:
            raise pytest.UsageError(f"inherited {name} is forbidden")
    if not all(core_present):
        raise pytest.UsageError("TASK-064 CI observer options must be supplied together")
    if any(proof_present) and not all(proof_present):
        raise pytest.UsageError("TASK-064 proof options must be supplied together")
    try:
        phase = _ascii_text(values[names[0]][0], label="CI phase")
        nonce = _hex_64(values[names[1]][0])
        observation_fd = _canonical_descriptor(values[names[2]][0], label="CI observation fd")
    except (IndexError, _ObserverFailure, argparse.ArgumentTypeError) as error:
        raise pytest.UsageError("invalid TASK-064 CI options") from error
    if phase not in _CI_PHASES:
        raise pytest.UsageError("invalid TASK-064 CI phase")
    shard_id = values[names[3]][0] if shard_present else None
    if phase == "collect" and shard_id is not None:
        raise pytest.UsageError("collection observation forbids a shard ID")
    if phase == "execute" and shard_id not in _CI_SHARDS:
        raise pytest.UsageError("execution observation requires an exact shard ID")

    proof_mode: str | None = None
    proof_nonce: str | None = None
    proof_fd: int | None = None
    if any(proof_present):
        try:
            proof_mode = _ascii_text(values[names[4]][0], label="proof mode")
            proof_nonce = _hex_64(values[names[5]][0])
            proof_fd = _canonical_descriptor(values[names[6]][0], label="proof observation fd")
        except (IndexError, _ObserverFailure, argparse.ArgumentTypeError) as error:
            raise pytest.UsageError("invalid TASK-064 proof options") from error
        if proof_mode not in _REPORT_PROOF_MODES or proof_nonce == nonce:
            raise pytest.UsageError("invalid TASK-064 proof identity")
        if proof_fd == observation_fd:
            raise pytest.UsageError("proof and CI observation descriptors must differ")
        expected_shard = "report" if proof_mode == "primed-full" else f"report-proof-{proof_mode}"
        if phase != "execute" or shard_id != expected_shard:
            raise pytest.UsageError("proof mode and observer shard are inconsistent")
    elif shard_id == "report" or (
        isinstance(shard_id, str) and shard_id.startswith("report-proof-")
    ):
        raise pytest.UsageError("report execution requires exact proof options")

    private_root = _private_temp_root()
    arguments = sys.argv[1:]
    fixed_lead = ["-q", "-p", "no:cacheprovider"]
    if phase == "collect":
        fixed_lead.insert(0, "--collect-only")
    basetemp_index = len(fixed_lead)
    if len(arguments) <= basetemp_index or not arguments[basetemp_index].startswith("--basetemp="):
        raise pytest.UsageError("TASK-064 basetemp argument is missing or reordered")
    raw_basetemp = arguments[basetemp_index][len("--basetemp=") :]
    try:
        basetemp = Path(_ascii_text(raw_basetemp, label="basetemp"))
    except _ObserverFailure as error:
        raise pytest.UsageError("invalid TASK-064 basetemp") from error
    if (
        not basetemp.is_absolute()
        or basetemp.parent != private_root
        or basetemp.name in {"", ".", ".."}
        or basetemp.exists()
    ):
        raise pytest.UsageError("TASK-064 basetemp is outside the private root")
    expected_arguments = [
        *fixed_lead,
        f"--basetemp={basetemp}",
        f"--task064-ci-phase={phase}",
        f"--task064-ci-nonce={nonce}",
        f"--task064-ci-observation-fd={observation_fd}",
    ]
    if shard_id is not None:
        expected_arguments.append(f"--task064-ci-shard-id={shard_id}")
    if proof_mode is not None:
        if proof_nonce is None or proof_fd is None:
            raise pytest.UsageError("incomplete TASK-064 proof invocation")
        expected_arguments.extend(
            [
                f"--task064-report-proof-mode={proof_mode}",
                f"--task064-report-proof-nonce={proof_nonce}",
                f"--task064-report-proof-observation-fd={proof_fd}",
            ]
        )
    fixed_count = len(expected_arguments)
    if arguments[:fixed_count] != expected_arguments:
        raise pytest.UsageError("TASK-064 pytest argv prefix or option order differs")
    selector_nodes = tuple(arguments[fixed_count:])
    if phase == "collect" and selector_nodes:
        raise pytest.UsageError("collection invocation has a selector tail")
    if phase == "execute" and not selector_nodes:
        raise pytest.UsageError("execution invocation lacks a selector tail")
    for node_id in selector_nodes:
        try:
            node_id = _ascii_text(node_id, label="selector node")
        except _ObserverFailure as error:
            raise pytest.UsageError("invalid TASK-064 selector node") from error
        if not node_id or node_id.startswith("-") or len(node_id.encode("ascii")) > 308:
            raise pytest.UsageError("invalid TASK-064 selector node")
    if len(selector_nodes) != len(set(selector_nodes)):
        raise pytest.UsageError("duplicate TASK-064 selector node")
    if proof_mode is not None and selector_nodes != (_REPORT_NODE,):
        raise pytest.UsageError("report proof selector differs")

    pytest_file = pytest.__file__
    if type(pytest_file) is not str:
        raise pytest.UsageError("pytest module identity is unavailable")
    expected_argv_zero = str(Path(pytest_file).resolve(strict=True).with_name("__main__.py"))
    if sys.argv != [expected_argv_zero, *expected_arguments, *selector_nodes]:
        raise pytest.UsageError("TASK-064 sys.argv identity differs")
    expected_orig_argv = [sys.executable, "-m", "pytest", *expected_arguments, *selector_nodes]
    if getattr(sys, "orig_argv", None) != expected_orig_argv:
        raise pytest.UsageError("TASK-064 sys.orig_argv identity differs")
    return _Task064RawInvocation(
        phase=phase,
        nonce=nonce,
        observation_fd=observation_fd,
        shard_id=shard_id,
        proof_mode=proof_mode,
        proof_nonce=proof_nonce,
        proof_observation_fd=proof_fd,
        selector_nodes=selector_nodes,
        basetemp=basetemp,
    )


_TASK064_FORCE_SYMLINK_NAME = "_force_symlink"
_TASK064_FORCE_SYMLINK_LIMIT = 50_000
_TASK064_ORIGINAL_FORCE_SYMLINK: object = vars(pytest_pathlib).get(_TASK064_FORCE_SYMLINK_NAME)
_TASK064_ORIGINAL_FORCE_SYMLINK_CODE: object = getattr(
    _TASK064_ORIGINAL_FORCE_SYMLINK,
    "__code__",
    None,
)
_TASK064_MAKE_NUMBERED_DIR: object = vars(pytest_pathlib).get("make_numbered_dir")
_TASK064_MAKE_NUMBERED_DIR_CODE: object = getattr(
    _TASK064_MAKE_NUMBERED_DIR,
    "__code__",
    None,
)
_TASK064_FORCE_SYMLINK_INSTALLED = False
_TASK064_FORCE_SYMLINK_CALLS = 0
_TASK064_FORCE_SYMLINK_INVALID = False
_TASK064_FORCE_SYMLINK_OWNER_PID = -1
_TASK064_FORCE_SYMLINK_PRIVATE_ROOT: Path | None = None
_TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY: tuple[int, int, int, int] | None = None
_TASK064_FORCE_SYMLINK_CONFIG: pytest.Config | None = None


def _task064_nested_pytest_private_root() -> Path | None:
    if (
        _RAW_INVOCATION is not None
        or os.environ.get("PYTEST_DISABLE_PLUGIN_AUTOLOAD") != "1"
        or os.environ.get("PYTHONDONTWRITEBYTECODE") != "1"
    ):
        return None
    try:
        private_root = _private_temp_root()
    except _ObserverFailure:
        return None
    if os.environ.get("HYPOTHESIS_STORAGE_DIRECTORY") != os.fspath(
        private_root / "hypothesis"
    ) or os.environ.get("COVERAGE_FILE") != os.fspath(private_root / "coverage" / ".coverage"):
        return None
    pycache_raw = os.environ.get("PYTHONPYCACHEPREFIX")
    if type(pycache_raw) is not str:
        return None
    pycache = Path(pycache_raw)
    try:
        pycache_relative = pycache.relative_to(private_root)
    except ValueError:
        return None
    if (
        not pycache.is_absolute()
        or len(pycache_relative.parts) < 2
        or not pycache.name.startswith("task064-")
        or any(part in {"", ".", ".."} for part in pycache_relative.parts)
    ):
        return None
    for directory in (private_root / "hypothesis", private_root / "coverage"):
        try:
            details = directory.lstat()
        except OSError:
            return None
        if (
            not stat.S_ISDIR(details.st_mode)
            or details.st_uid != _RUNTIME_UID
            or stat.S_IMODE(details.st_mode) != 0o700
        ):
            return None
    return private_root


def _task064_pytest_alias_root_parts(root: Path) -> tuple[str, ...] | None:
    private_root = _TASK064_FORCE_SYMLINK_PRIVATE_ROOT
    raw = _RAW_INVOCATION
    if private_root is None or type(root) is not type(private_root) or not root.is_absolute():
        return None
    try:
        relative = root.relative_to(private_root)
    except ValueError:
        return None
    parts = relative.parts
    if raw is not None and root == raw.basetemp:
        return parts
    if len(parts) == 1:
        outer = parts[0]
        if outer.startswith("pytest-of-") and len(outer) > len("pytest-of-"):
            return parts
        return None
    if len(parts) == 2:
        outer, session = parts
        suffix = session.removeprefix("pytest-")
        if (
            outer.startswith("pytest-of-")
            and len(outer) > len("pytest-of-")
            and suffix.isdecimal()
            and str(int(suffix, 10)) == suffix
        ):
            return parts
    return None


def _task064_open_pytest_alias_root(root: Path) -> int:
    parts = _task064_pytest_alias_root_parts(root)
    private_root = _TASK064_FORCE_SYMLINK_PRIVATE_ROOT
    expected_identity = _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY
    if parts is None or private_root is None or expected_identity is None:
        raise _ObserverFailure("TASK-064 pytest current-alias root differs")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = -1
    try:
        descriptor = os.open(private_root, flags)
        details = os.fstat(descriptor)
        if (
            details.st_dev,
            details.st_ino,
            details.st_uid,
            stat.S_IMODE(details.st_mode),
        ) != expected_identity or not stat.S_ISDIR(details.st_mode):
            raise _ObserverFailure("TASK-064 pytest private root identity differs")
        for part in parts:
            next_descriptor = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        raise


def _task064_suppress_pytest_current_symlink(
    root: Path,
    target: str | PurePath,
    link_to: str | Path,
) -> None:
    global _TASK064_FORCE_SYMLINK_CALLS, _TASK064_FORCE_SYMLINK_INVALID
    _TASK064_FORCE_SYMLINK_CALLS += 1
    valid = False
    root_descriptor = -1
    if (
        os.getpid() == _TASK064_FORCE_SYMLINK_OWNER_PID
        and 0 < _TASK064_FORCE_SYMLINK_CALLS <= _TASK064_FORCE_SYMLINK_LIMIT
        and type(root) is type(_TASK064_FORCE_SYMLINK_PRIVATE_ROOT)
        and type(target) is str
        and target.endswith("current")
        and target not in {"current", ".", ".."}
        and target.isascii()
        and "\x00" not in target
        and "/" not in target
        and "\\" not in target
        and isinstance(link_to, Path)
        and type(link_to) is type(root)
        and link_to.parent == root
    ):
        prefix = target[: -len("current")]
        suffix = link_to.name[len(prefix) :] if link_to.name.startswith(prefix) else ""
        try:
            root_descriptor = _task064_open_pytest_alias_root(root)
            try:
                os.stat(target, dir_fd=root_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                alias_absent = True
            except OSError:
                alias_absent = False
            else:
                alias_absent = False
            target_status = os.stat(
                link_to.name,
                dir_fd=root_descriptor,
                follow_symlinks=False,
            )
        except (OSError, _ObserverFailure):
            valid = False
        else:
            valid = (
                alias_absent
                and prefix != ""
                and suffix.isdecimal()
                and str(int(suffix, 10)) == suffix
                and stat.S_ISDIR(target_status.st_mode)
                and target_status.st_uid == _RUNTIME_UID
                and stat.S_IMODE(target_status.st_mode) == 0o700
            )
        finally:
            if root_descriptor >= 0:
                try:
                    os.close(root_descriptor)
                except OSError:
                    valid = False
    if not valid:
        _TASK064_FORCE_SYMLINK_INVALID = True
        raise _ObserverFailure("TASK-064 pytest current-alias request differs")


_TASK064_FORCE_SYMLINK_CODE = _task064_suppress_pytest_current_symlink.__code__


def _task064_validate_pytest_current_symlink_suppression() -> None:
    current = vars(pytest_pathlib).get(_TASK064_FORCE_SYMLINK_NAME)
    original = _TASK064_ORIGINAL_FORCE_SYMLINK
    make_numbered_dir = _TASK064_MAKE_NUMBERED_DIR
    private_root = _TASK064_FORCE_SYMLINK_PRIVATE_ROOT
    config = _TASK064_FORCE_SYMLINK_CONFIG
    runtime = _RUNTIME
    if (
        not _TASK064_FORCE_SYMLINK_INSTALLED
        or _TASK064_FORCE_SYMLINK_INVALID
        or os.getpid() != _TASK064_FORCE_SYMLINK_OWNER_PID
        or type(_TASK064_FORCE_SYMLINK_CALLS) is not int
        or not 0 <= _TASK064_FORCE_SYMLINK_CALLS <= _TASK064_FORCE_SYMLINK_LIMIT
        or config is None
        or private_root is None
        or sys.modules.get("_pytest.pathlib") is not pytest_pathlib
        or pytest.__version__ != "9.1.1"
        or type(original) is not FunctionType
        or original.__code__ is not _TASK064_ORIGINAL_FORCE_SYMLINK_CODE
        or type(make_numbered_dir) is not FunctionType
        or make_numbered_dir.__code__ is not _TASK064_MAKE_NUMBERED_DIR_CODE
        or make_numbered_dir.__globals__.get(_TASK064_FORCE_SYMLINK_NAME) is not current
        or type(current) is not FunctionType
        or current is not _task064_suppress_pytest_current_symlink
        or current.__code__ is not _TASK064_FORCE_SYMLINK_CODE
        or (_RAW_INVOCATION is not None and (runtime is None or not runtime._is_owner()))
    ):
        raise _ObserverFailure("TASK-064 pytest current-alias suppression differs")
    try:
        observed_private_root = _private_temp_root()
    except _ObserverFailure as error:
        raise _ObserverFailure("TASK-064 pytest private root disappeared") from error
    if observed_private_root != private_root:
        raise _ObserverFailure("TASK-064 pytest private root changed")


def _task064_install_pytest_current_symlink_suppression(
    config: pytest.Config,
    private_root: Path,
) -> None:
    global _TASK064_FORCE_SYMLINK_CALLS
    global _TASK064_FORCE_SYMLINK_CONFIG
    global _TASK064_FORCE_SYMLINK_INVALID
    global _TASK064_FORCE_SYMLINK_INSTALLED
    global _TASK064_FORCE_SYMLINK_OWNER_PID
    global _TASK064_FORCE_SYMLINK_PRIVATE_ROOT
    global _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY
    original = _TASK064_ORIGINAL_FORCE_SYMLINK
    make_numbered_dir = _TASK064_MAKE_NUMBERED_DIR
    try:
        private_details = private_root.lstat()
    except OSError as error:
        raise _ObserverFailure("TASK-064 pytest private root is unavailable") from error
    if (
        _TASK064_FORCE_SYMLINK_INSTALLED
        or _TASK064_FORCE_SYMLINK_CALLS != 0
        or _TASK064_FORCE_SYMLINK_INVALID
        or _TASK064_FORCE_SYMLINK_CONFIG is not None
        or _TASK064_FORCE_SYMLINK_PRIVATE_ROOT is not None
        or sys.modules.get("_pytest.pathlib") is not pytest_pathlib
        or pytest.__version__ != "9.1.1"
        or type(original) is not FunctionType
        or original.__module__ != "_pytest.pathlib"
        or original.__name__ != "_force_symlink"
        or original.__qualname__ != "_force_symlink"
        or original.__code__ is not _TASK064_ORIGINAL_FORCE_SYMLINK_CODE
        or original.__code__.co_argcount != 3
        or original.__code__.co_posonlyargcount != 0
        or original.__code__.co_kwonlyargcount != 0
        or original.__code__.co_varnames[:3] != ("root", "target", "link_to")
        or type(make_numbered_dir) is not FunctionType
        or make_numbered_dir.__code__ is not _TASK064_MAKE_NUMBERED_DIR_CODE
        or make_numbered_dir.__globals__.get(_TASK064_FORCE_SYMLINK_NAME) is not original
        or vars(pytest_pathlib).get(_TASK064_FORCE_SYMLINK_NAME) is not original
        or not stat.S_ISDIR(private_details.st_mode)
        or private_details.st_uid != _RUNTIME_UID
        or stat.S_IMODE(private_details.st_mode) != 0o700
    ):
        raise _ObserverFailure("TASK-064 pytest current-alias source differs")
    _TASK064_FORCE_SYMLINK_CONFIG = config
    _TASK064_FORCE_SYMLINK_OWNER_PID = os.getpid()
    _TASK064_FORCE_SYMLINK_PRIVATE_ROOT = private_root
    _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY = (
        private_details.st_dev,
        private_details.st_ino,
        private_details.st_uid,
        stat.S_IMODE(private_details.st_mode),
    )
    try:
        vars(pytest_pathlib)[_TASK064_FORCE_SYMLINK_NAME] = _task064_suppress_pytest_current_symlink
        _TASK064_FORCE_SYMLINK_INSTALLED = True
        _task064_validate_pytest_current_symlink_suppression()
    except BaseException:
        vars(pytest_pathlib)[_TASK064_FORCE_SYMLINK_NAME] = original
        _TASK064_FORCE_SYMLINK_INSTALLED = False
        _TASK064_FORCE_SYMLINK_CALLS = 0
        _TASK064_FORCE_SYMLINK_CONFIG = None
        _TASK064_FORCE_SYMLINK_INVALID = False
        _TASK064_FORCE_SYMLINK_OWNER_PID = -1
        _TASK064_FORCE_SYMLINK_PRIVATE_ROOT = None
        _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY = None
        raise


def _task064_restore_pytest_current_symlink_suppression(config: pytest.Config) -> None:
    global _TASK064_FORCE_SYMLINK_CALLS
    global _TASK064_FORCE_SYMLINK_CONFIG
    global _TASK064_FORCE_SYMLINK_INVALID
    global _TASK064_FORCE_SYMLINK_INSTALLED
    global _TASK064_FORCE_SYMLINK_OWNER_PID
    global _TASK064_FORCE_SYMLINK_PRIVATE_ROOT
    global _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY
    if not _TASK064_FORCE_SYMLINK_INSTALLED:
        return
    if config is not _TASK064_FORCE_SYMLINK_CONFIG:
        raise _ObserverFailure("TASK-064 pytest current-alias config differs")
    primary: BaseException | None = None
    try:
        _task064_validate_pytest_current_symlink_suppression()
    except BaseException as error:
        primary = error
    vars(pytest_pathlib)[_TASK064_FORCE_SYMLINK_NAME] = _TASK064_ORIGINAL_FORCE_SYMLINK
    _TASK064_FORCE_SYMLINK_INSTALLED = False
    _TASK064_FORCE_SYMLINK_CONFIG = None
    _TASK064_FORCE_SYMLINK_OWNER_PID = -1
    _TASK064_FORCE_SYMLINK_PRIVATE_ROOT = None
    _TASK064_FORCE_SYMLINK_PRIVATE_ROOT_IDENTITY = None
    _TASK064_FORCE_SYMLINK_CALLS = 0
    _TASK064_FORCE_SYMLINK_INVALID = False
    if vars(pytest_pathlib).get(_TASK064_FORCE_SYMLINK_NAME) is not _TASK064_ORIGINAL_FORCE_SYMLINK:
        restore_error = _ObserverFailure("TASK-064 pytest current-alias restore differs")
        if primary is not None:
            restore_error.add_note(f"suppression validation failed: {type(primary).__name__}")
        raise restore_error
    if primary is not None:
        raise primary


class _Task064ObserverRuntime:
    def __init__(self, raw: _Task064RawInvocation) -> None:
        self.owner_pid = os.getpid()
        self._captured_exit = os._exit
        self._captured_pidfd_signal = signal.pidfd_send_signal
        self._captured_sigkill = signal.SIGKILL
        self._ci: _ObservationHandle | None = None
        self.proof: _ObservationHandle | None = None
        self._poison: _MmapOwner | None = None
        self.self_pidfd = -1
        self._inherited_pidfd_for_fork = -1
        self.cleanup_uncertain = False
        self.proof_written = False
        self.ci_written = False
        sensitive_fds: set[int] = {0, 1, 2}
        provisional_mapping: mmap.mmap | None = None
        primary: BaseException | None = None
        try:
            if type(raw.observation_fd) is not int or raw.observation_fd <= 2:
                raise _ObserverFailure("invalid CI observation descriptor")
            self._ci = _ObservationHandle(
                descriptor=raw.observation_fd,
                _initial=None,
                label="CI observation",
            )
            _validate_adopted_observation(self._ci)
            if raw.proof_observation_fd is not None:
                if raw.proof_observation_fd == self.ci.descriptor:
                    raise _ObserverFailure("observation descriptor numbers are not distinct")
                if type(raw.proof_observation_fd) is not int or raw.proof_observation_fd <= 2:
                    raise _ObserverFailure("invalid proof observation descriptor")
                self.proof = _ObservationHandle(
                    descriptor=raw.proof_observation_fd,
                    _initial=None,
                    label="proof observation",
                )
                _validate_adopted_observation(self.proof)
            handles = [self.ci, *(tuple() if self.proof is None else (self.proof,))]
            handle_numbers = [item.descriptor for item in handles]
            if len(set(handle_numbers)) != len(handle_numbers):
                raise _ObserverFailure("observation descriptor numbers are not distinct")
            identities = {(item.initial.device, item.initial.inode) for item in handles}
            if len(identities) != len(handles):
                raise _ObserverFailure("observation object identities are not distinct")
            standard_identities = {
                (snapshot.device, snapshot.inode, snapshot.mode)
                for snapshot in (_snapshot(fd) for fd in (0, 1, 2))
            }
            if any(
                (item.initial.device, item.initial.inode, item.initial.mode) in standard_identities
                for item in handles
            ):
                raise _ObserverFailure("observation identity aliases a standard descriptor")
            _require_exact_inherited_descriptors(
                handles,
                capture_root=raw.basetemp.parent,
            )
            provisional_mapping = mmap.mmap(
                -1,
                1,
                flags=mmap.MAP_SHARED,
                prot=mmap.PROT_READ | mmap.PROT_WRITE,
            )
            self._poison = _MmapOwner(provisional_mapping)
            provisional_mapping = None
            poison_mapping = self.poison.require()
            poison_mapping[0] = 0
            poison_value = poison_mapping[0]
            if type(poison_value) is not int or poison_value != 0:
                raise _ObserverFailure("fork-poison initialization failed")
            sensitive_fds = {0, 1, 2, *handle_numbers}
            other_identity_triples = {
                (snapshot.device, snapshot.inode, snapshot.mode)
                for snapshot in (_snapshot(fd) for fd in sensitive_fds)
            }
            self.self_pidfd = os.pidfd_open(self.owner_pid, 0)
            if (
                type(self.self_pidfd) is not int
                or self.self_pidfd <= 2
                or self.self_pidfd in sensitive_fds
            ):
                raise _ObserverFailure("self pidfd number aliases another handle")
            self_identity = _snapshot(self.self_pidfd)
            if (
                self_identity.device,
                self_identity.inode,
                self_identity.mode,
            ) in other_identity_triples:
                raise _ObserverFailure("self pidfd identity aliases another handle")
            prior = fcntl.fcntl(self.self_pidfd, fcntl.F_GETFD)
            if type(prior) is not int or prior < 0:
                raise _ObserverFailure("invalid self-pidfd flags")
            expected = prior | fcntl.FD_CLOEXEC
            result = fcntl.fcntl(self.self_pidfd, fcntl.F_SETFD, expected)
            if type(result) is not int or result != 0:
                raise _ObserverFailure("failed to restore self-pidfd CLOEXEC")
            readback = fcntl.fcntl(self.self_pidfd, fcntl.F_GETFD)
            if type(readback) is not int or readback != expected:
                raise _ObserverFailure("self-pidfd CLOEXEC readback differs")
            if self._captured_pidfd_signal(self.self_pidfd, 0, None, 0) is not None:
                raise _ObserverFailure("self-pidfd zero signal differs")
            os.register_at_fork(after_in_child=self._after_fork_child)
        except BaseException as error:
            primary = error
        if primary is not None:
            self.cleanup_uncertain = True
            failures: list[str] = []
            pidfd = self.self_pidfd
            self.self_pidfd = -1
            pidfd_is_provisionally_owned = (
                type(pidfd) is int and pidfd > 2 and pidfd not in sensitive_fds
            )
            if pidfd_is_provisionally_owned:
                try:
                    os.close(pidfd)
                except BaseException:
                    failures.append("self pidfd")
            if self._poison is not None and not self._poison.terminal:
                try:
                    poison = self._poison.detach()
                    poison.close()
                except BaseException:
                    failures.append("fork poison")
            elif provisional_mapping is not None:
                try:
                    provisional_mapping.close()
                except BaseException:
                    failures.append("provisional fork poison")
            for handle in (self.proof, self._ci):
                if handle is not None and not handle.terminal:
                    try:
                        _close_once(handle)
                    except BaseException:
                        failures.append(handle.label)
            if failures:
                primary.add_note(f"observer constructor cleanup uncertainties: {failures!r}")
            raise primary

    @property
    def ci(self) -> _ObservationHandle:
        if self._ci is None:
            raise _ObserverFailure("CI observation owner is absent")
        return self._ci

    @property
    def poison(self) -> _MmapOwner:
        if self._poison is None:
            raise _ObserverFailure("fork-poison owner is absent")
        return self._poison

    def _is_owner(self) -> bool:
        return os.getpid() == self.owner_pid and _RUNTIME is self

    def _after_fork_child(self) -> None:
        try:
            self._after_fork_child_impl()
        except BaseException:
            self._after_fork_child_fatal()

    def _after_fork_child_impl(self) -> None:
        inherited_pidfd = self.self_pidfd
        self._inherited_pidfd_for_fork = inherited_pidfd
        self.self_pidfd = -1
        handles = (self._ci, self.proof)
        detached: list[int | None] = [None, None]
        failed = False
        try:
            for index, handle in enumerate(handles):
                if handle is None:
                    continue
                if handle.terminal:
                    if handle.descriptor != -1:
                        failed = True
                    continue
                descriptor = handle.descriptor
                if type(descriptor) is not int or descriptor <= 2:
                    failed = True
                else:
                    detached[index] = descriptor
                handle.descriptor = -1
                handle.terminal = True
        except BaseException:
            failed = True
        for index, handle in enumerate(handles):
            if handle is None:
                continue
            try:
                if not handle.terminal:
                    descriptor = handle.descriptor
                    if detached[index] is None:
                        if type(descriptor) is int and descriptor > 2:
                            detached[index] = descriptor
                        else:
                            failed = True
                    elif descriptor != detached[index]:
                        failed = True
                    handle.descriptor = -1
                    handle.terminal = True
                if handle.descriptor != -1:
                    handle.descriptor = -1
                    failed = True
            except BaseException:
                failed = True
        attempted: set[int] = set()
        for detached_descriptor in detached:
            if detached_descriptor is None:
                continue
            if detached_descriptor in attempted:
                failed = True
                continue
            attempted.add(detached_descriptor)
            try:
                os.close(detached_descriptor)
            except BaseException:
                failed = True
        for handle in handles:
            if handle is not None and (not handle.terminal or handle.descriptor != -1):
                failed = True
        if not failed:
            self._inherited_pidfd_for_fork = -1
            return
        self._after_fork_child_fatal()

    def _after_fork_child_fatal(self) -> None:
        poisoned = False
        try:
            poison_mapping = self.poison.require()
            poison_mapping[0] = 1
            poison_value = poison_mapping[0]
            poisoned = type(poison_value) is int and poison_value == 1
        except BaseException:
            poisoned = False
        if poisoned:
            self._captured_exit(191)
        inherited_pidfd = self._inherited_pidfd_for_fork
        if type(inherited_pidfd) is not int or inherited_pidfd <= 2:
            inherited_pidfd = self.self_pidfd
        try:
            result = self._captured_pidfd_signal(
                inherited_pidfd,
                self._captured_sigkill,
                None,
                0,
            )
            if result is not None:
                self._captured_exit(191)
        except BaseException:
            self._captured_exit(191)
        self._captured_exit(191)

    def _require_owner(self) -> None:
        if not self._is_owner():
            raise _ObserverFailure("observation writer is not the exec owner")

    def latch_uncertainty(self) -> None:
        self.cleanup_uncertain = True

    def _close_anchors(self, *, require_live: bool) -> list[str]:
        failures: list[str] = []
        pidfd = self.self_pidfd
        self.self_pidfd = -1
        if pidfd == -1:
            if require_live:
                failures.append("self pidfd ownership")
        elif type(pidfd) is not int or pidfd <= 2:
            failures.append("self pidfd ownership")
        else:
            try:
                os.close(pidfd)
            except BaseException:
                failures.append("self pidfd close")
        poison = self._poison
        if poison is None:
            failures.append("fork poison ownership")
        elif poison.terminal:
            if require_live:
                failures.append("fork poison ownership")
        else:
            try:
                mapping = poison.detach()
                mapping.close()
            except BaseException:
                failures.append("fork poison close")
        return failures

    def _abort_all(self, primary: BaseException) -> None:
        self.cleanup_uncertain = True
        failures = self._close_anchors(require_live=False)
        for handle in (self.proof, self._ci):
            if handle is not None and not handle.terminal:
                try:
                    _close_once(handle)
                except BaseException:
                    failures.append(handle.label)
        if failures:
            primary.add_note(f"observer terminal cleanup uncertainties: {failures!r}")
        raise primary

    def write_proof(self, packet: dict[str, object]) -> None:
        self._require_owner()
        if self.proof is None or self.proof_written or self.cleanup_uncertain:
            raise _ObserverFailure("proof observation is not writable")
        poison_value = self.poison.require()[0]
        if type(poison_value) is not int or poison_value != 0:
            raise _ObserverFailure("fork poison blocks proof observation")
        payload = _canonical_json_bytes(packet, limit=_PROOF_PACKET_LIMIT)
        _write_packet(self.proof, payload, limit=_PROOF_PACKET_LIMIT)
        self.proof_written = True

    def finish_ci(self, packet: dict[str, object]) -> None:
        primary: BaseException | None = None
        payload: bytes | None = None
        payload_limit = 0
        try:
            self._require_owner()
            if self.ci_written or self.cleanup_uncertain:
                raise _ObserverFailure("CI observation is not writable")
            poison_value = self.poison.require()[0]
            if type(poison_value) is not int or poison_value != 0:
                self._captured_exit(191)
            if self.proof is not None and not self.proof_written:
                failure_tail = _TASK064_REPORT_CALL_FAILURE_TAIL
                if failure_tail is None:
                    raise _ObserverFailure("required proof observation is missing")
                raise _ObserverFailure(
                    "required proof observation is missing; "
                    f"report_call_failure_tail={failure_tail!r}"
                )
            raw = _RAW_INVOCATION
            if raw is None:
                raise _ObserverFailure("raw invocation disappeared before publication")
            payload_limit = (
                _COLLECTION_PACKET_LIMIT if raw.phase == "collect" else _EXECUTION_PACKET_LIMIT
            )
            payload = _canonical_json_bytes(packet, limit=payload_limit)
        except BaseException as error:
            primary = error
        if primary is not None:
            self._abort_all(primary)
            return
        if payload is None or payload_limit <= 0:
            self._abort_all(_ObserverFailure("CI payload was not constructed"))
            return
        anchor_failures = self._close_anchors(require_live=True)
        if anchor_failures:
            failure = _ObserverFailure("terminal observer anchor close uncertainty")
            failure.add_note(f"anchor failures: {anchor_failures!r}")
            self._abort_all(failure)
            return
        try:
            _write_packet(self.ci, payload, limit=payload_limit)
        except BaseException:
            self.cleanup_uncertain = True
            raise
        self.ci_written = True


_RAW_INVOCATION: _Task064RawInvocation | None = None
_RUNTIME: _Task064ObserverRuntime | None = None


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register inert CI observation and report-proof options."""

    global _RAW_INVOCATION, _RUNTIME
    if _RAW_INVOCATION is not None or _RUNTIME is not None:
        raise pytest.UsageError("TASK-064 observer initialized twice")
    raw_invocation = _validate_raw_task064_options()
    group = parser.getgroup("task064-ci")
    group.addoption(
        "--task064-ci-phase",
        action="store",
        choices=_CI_PHASES,
        default=None,
        dest="task064_ci_phase",
    )
    group.addoption(
        "--task064-ci-nonce",
        action="store",
        type=_hex_64,
        default=None,
        dest="task064_ci_nonce",
    )
    group.addoption(
        "--task064-ci-observation-fd",
        action="store",
        default=None,
        dest="task064_ci_observation_fd",
    )
    group.addoption(
        "--task064-ci-shard-id",
        action="store",
        choices=_CI_SHARDS,
        default=None,
        dest="task064_ci_shard_id",
    )
    group.addoption(
        "--task064-report-proof-mode",
        action="store",
        choices=_REPORT_PROOF_MODES,
        default=None,
        dest="task064_report_proof_mode",
    )
    group.addoption(
        "--task064-report-proof-nonce",
        action="store",
        type=_hex_64,
        default=None,
        dest="task064_report_proof_nonce",
    )
    group.addoption(
        "--task064-report-proof-observation-fd",
        action="store",
        default=None,
        dest="task064_report_proof_observation_fd",
    )
    _RAW_INVOCATION = raw_invocation
    if raw_invocation is not None:
        try:
            _RUNTIME = _Task064ObserverRuntime(raw_invocation)
        except BaseException as error:
            raise pytest.UsageError("TASK-064 observer initialization failed") from error


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    if _RAW_INVOCATION is not None:
        return
    private_root = _task064_nested_pytest_private_root()
    if private_root is None:
        return
    try:
        _task064_install_pytest_current_symlink_suppression(config, private_root)
    except _ObserverFailure as error:
        raise pytest.UsageError("TASK-064 pytest current-alias suppression failed") from error


def _external_plugins(config: pytest.Config) -> list[list[str]]:
    records: set[tuple[str, str, str, str, str]] = set()
    for distribution in importlib.metadata.distributions():
        name = distribution.metadata.get("Name")
        if name is None:
            continue
        normalized = name.lower().replace("_", "-")
        for entry_point in distribution.entry_points:
            if entry_point.group != "pytest11":
                continue
            if config.pluginmanager.get_plugin(entry_point.name) is None:
                continue
            records.add(
                (
                    normalized,
                    distribution.version,
                    entry_point.group,
                    entry_point.name,
                    entry_point.value,
                )
            )
    return [list(record) for record in sorted(records)]


def _worker_indicator_count(config: pytest.Config) -> int:
    count = int(hasattr(config, "workerinput"))
    for name in (
        "PYTEST_XDIST_WORKER",
        "PYTEST_XDIST_WORKER_COUNT",
        "PYTEST_XDIST_TESTRUNUID",
    ):
        count += int(name in os.environ)
    plugins: set[int] = set()
    for name, plugin in config.pluginmanager.list_name_plugin():
        module_name = getattr(plugin, "__name__", "")
        identity = f"{name} {module_name}".lower()
        if "xdist" in identity or "execnet" in identity:
            plugins.add(id(plugin))
    count += len(plugins)
    count += sum(
        1
        for module_name in sys.modules
        if module_name == "xdist"
        or module_name.startswith("xdist.")
        or module_name == "execnet"
        or module_name.startswith("execnet.")
    )
    return count


@dataclass
class _Task064Observer:
    phase: str
    nonce: str
    shard_id: str | None
    selector_nodes: tuple[str, ...]
    assigned_nodes: list[str] = field(default_factory=list)
    collected_nodes: list[str] = field(default_factory=list)
    started_nodes: list[str] = field(default_factory=list)
    finished_nodes: list[str] = field(default_factory=list)
    reports: list[list[object]] = field(default_factory=list)
    unknown_report_count: int = 0

    def collection_finished(self, session: pytest.Session) -> None:
        for name in _FORBIDDEN_ACTIVE_ENV:
            if name in os.environ:
                raise pytest.UsageError(f"inherited {name} is forbidden")
        configured_addopts = tuple(cast(list[str], session.config.getini("addopts")))
        if configured_addopts != _EXPECTED_ADDOPTS:
            raise pytest.UsageError("configured addopts do not match TASK-064")
        if self.phase == "execute":
            configured = tuple(
                _ascii_text(value, label="assigned node ID") for value in session.config.args
            )
            if configured != self.selector_nodes:
                raise pytest.UsageError("parsed selector tail differs")
            self.assigned_nodes = list(self.selector_nodes)
        self.collected_nodes = [
            _ascii_text(item.nodeid, label="collected node ID") for item in session.items
        ]

    def log_start(self, node_id: str) -> None:
        self.started_nodes.append(_ascii_text(node_id, label="started node ID"))

    def log_report(self, report: pytest.TestReport) -> None:
        node_id = _ascii_text(report.nodeid, label="reported node ID")
        if report.when not in {"setup", "call", "teardown"} or report.outcome not in {
            "passed",
            "failed",
            "skipped",
        }:
            self.unknown_report_count += 1
            return
        self.reports.append([node_id, report.when, report.outcome, hasattr(report, "wasxfail")])

    def log_finish(self, node_id: str) -> None:
        self.finished_nodes.append(_ascii_text(node_id, label="finished node ID"))


_OBSERVER: _Task064Observer | None = None
_COLLECT_FAILED_COUNT = 0
_COLLECT_SKIPPED_COUNT = 0
_DESELECTED_NODES: list[str] = []
_INTERRUPTED_COUNT = 0
_INTERNAL_ERROR_COUNT = 0
_TASK064_REPORT_CALL_FAILURE_TAIL: str | None = None


def _observer_from_config(config: pytest.Config) -> _Task064Observer | None:
    global _OBSERVER
    if _RAW_INVOCATION is None:
        return None
    runtime = _RUNTIME
    if runtime is None or not runtime._is_owner():
        raise pytest.UsageError("TASK-064 observer runtime owner differs")
    if _OBSERVER is not None:
        return _OBSERVER
    raw = _RAW_INVOCATION
    try:
        parsed_fd = _canonical_descriptor(
            config.getoption("task064_ci_observation_fd", default=None),
            label="parsed CI observation fd",
        )
    except _ObserverFailure as error:
        raise pytest.UsageError("invalid parsed CI observation fd") from error
    parsed_proof_raw = config.getoption("task064_report_proof_observation_fd", default=None)
    parsed_proof_fd = (
        None
        if parsed_proof_raw is None
        else _canonical_descriptor(parsed_proof_raw, label="parsed proof observation fd")
    )
    if (
        config.getoption("task064_ci_phase", default=None) != raw.phase
        or config.getoption("task064_ci_nonce", default=None) != raw.nonce
        or parsed_fd != raw.observation_fd
        or config.getoption("task064_ci_shard_id", default=None) != raw.shard_id
        or config.getoption("task064_report_proof_mode", default=None) != raw.proof_mode
        or config.getoption("task064_report_proof_nonce", default=None) != raw.proof_nonce
        or parsed_proof_fd != raw.proof_observation_fd
    ):
        raise pytest.UsageError("parsed TASK-064 options differ from authenticated argv")
    if not _TASK064_FORCE_SYMLINK_INSTALLED:
        try:
            _task064_install_pytest_current_symlink_suppression(
                config,
                _private_temp_root(),
            )
        except _ObserverFailure as error:
            raise pytest.UsageError("TASK-064 pytest current-alias suppression failed") from error
    elif config is not _TASK064_FORCE_SYMLINK_CONFIG:
        raise pytest.UsageError("TASK-064 pytest current-alias config differs")
    else:
        try:
            _task064_validate_pytest_current_symlink_suppression()
        except _ObserverFailure as error:
            raise pytest.UsageError("TASK-064 pytest current-alias suppression differs") from error
    _OBSERVER = _Task064Observer(
        phase=raw.phase,
        nonce=raw.nonce,
        shard_id=raw.shard_id,
        selector_nodes=raw.selector_nodes,
    )
    return _OBSERVER


def pytest_collectreport(report: pytest.CollectReport) -> None:
    if _RAW_INVOCATION is None:
        return
    global _COLLECT_FAILED_COUNT, _COLLECT_SKIPPED_COUNT
    _COLLECT_FAILED_COUNT += int(report.failed)
    _COLLECT_SKIPPED_COUNT += int(report.skipped)


def pytest_deselected(items: list[pytest.Item]) -> None:
    if _RAW_INVOCATION is None:
        return
    _DESELECTED_NODES.extend(_ascii_text(item.nodeid, label="deselected node") for item in items)


def pytest_collection_finish(session: pytest.Session) -> None:
    observer = _observer_from_config(session.config)
    if observer is not None:
        observer.collection_finished(session)


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    del location
    if _OBSERVER is not None:
        _OBSERVER.log_start(nodeid)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    global _TASK064_REPORT_CALL_FAILURE_TAIL
    if _OBSERVER is not None:
        _OBSERVER.log_report(report)
    raw = _RAW_INVOCATION
    if (
        _TASK064_REPORT_CALL_FAILURE_TAIL is None
        and raw is not None
        and raw.proof_mode is not None
        and report.nodeid == _REPORT_NODE
        and report.when == "call"
        and report.failed
    ):
        try:
            diagnostic = report.longreprtext.encode("ascii", errors="backslashreplace")
        except BaseException:
            diagnostic = b""
        _TASK064_REPORT_CALL_FAILURE_TAIL = diagnostic[-768:].decode("ascii")


def pytest_runtest_logfinish(nodeid: str, location: tuple[str, int | None, str]) -> None:
    del location
    if _OBSERVER is not None:
        _OBSERVER.log_finish(nodeid)


def pytest_keyboard_interrupt(excinfo: pytest.ExceptionInfo[BaseException]) -> None:
    del excinfo
    if _RAW_INVOCATION is None:
        return
    global _INTERRUPTED_COUNT
    _INTERRUPTED_COUNT += 1


def pytest_internalerror(
    excrepr: object,
    excinfo: pytest.ExceptionInfo[BaseException],
) -> None:
    del excrepr, excinfo
    if _RAW_INVOCATION is None:
        return
    global _INTERNAL_ERROR_COUNT
    _INTERNAL_ERROR_COUNT += 1


def _validate_proof_record(mode: str, record: _Task064ProofRecord) -> None:
    if type(record.publication_status) is not str or record.publication_status not in {
        "PUBLISHED",
        "NONE",
    }:
        raise _ObserverFailure("invalid proof publication status")
    if type(record.cache_state_at_assertion) is not str:
        raise _ObserverFailure("invalid proof cache state")
    _exact_integer(record.stage_residue_count, label="proof stage residue")
    if type(record.receipt_consumed) is not bool or type(record.final_file_present) is not bool:
        raise _ObserverFailure("invalid proof boolean")
    if mode in {"primed-full", "unprimed-success"}:
        if (
            record.publication_status != "PUBLISHED"
            or type(record.output_bytes) is not int
            or record.output_bytes <= 0
            or type(record.output_sha256) is not str
            or len(record.output_sha256) != 64
            or any(character not in _HEX_DIGITS for character in record.output_sha256)
            or record.cache_state_at_assertion != "EMPTY"
            or not record.receipt_consumed
            or not record.final_file_present
            or record.stage_residue_count != 0
        ):
            raise _ObserverFailure("successful proof facts differ")
    else:
        expected_state = "READY" if mode == "ready-teardown" else "EMPTY"
        if (
            record.publication_status != "NONE"
            or record.output_bytes is not None
            or record.output_sha256 is not None
            or record.cache_state_at_assertion != expected_state
            or record.receipt_consumed
            or record.final_file_present
            or record.stage_residue_count != 0
        ):
            raise _ObserverFailure("nonpublication proof facts differ")


@pytest.fixture
def task064_report_proof_recorder(
    request: pytest.FixtureRequest,
    _active_task064_pytest_root: object,
) -> Iterator[Task064ReportProofRecorder]:
    """Finalize the exact report proof before the authenticated root tears down."""

    del _active_task064_pytest_root
    pending: _Task064ProofRecord | None = None

    def record(
        *,
        publication_status: str,
        output_bytes: int | None,
        output_sha256: str | None,
        cache_state_at_assertion: str,
        receipt_consumed: bool,
        final_file_present: bool,
        stage_residue_count: int,
        teardown: Callable[[], None],
        observe: Callable[[], tuple[str, bool, int | None, int | None]],
    ) -> None:
        nonlocal pending
        raw = _RAW_INVOCATION
        if (
            pending is not None
            or raw is None
            or raw.proof_mode is None
            or request.node.nodeid != _REPORT_NODE
            or raw.selector_nodes != (_REPORT_NODE,)
            or not callable(teardown)
            or not callable(observe)
        ):
            raise _ObserverFailure("invalid report-proof registration")
        pending = _Task064ProofRecord(
            publication_status=publication_status,
            output_bytes=output_bytes,
            output_sha256=output_sha256,
            cache_state_at_assertion=cache_state_at_assertion,
            receipt_consumed=receipt_consumed,
            final_file_present=final_file_present,
            stage_residue_count=stage_residue_count,
            teardown=teardown,
            observe=observe,
        )

    yield record
    raw = _RAW_INVOCATION
    if raw is None:
        if pending is not None:
            raise _ObserverFailure("inert report proof retained state")
        return
    if request.node.nodeid != _REPORT_NODE or raw.proof_mode is None:
        raise _ObserverFailure("proof recorder used outside the exact report invocation")
    runtime = _RUNTIME
    if runtime is None or pending is None:
        raise _ObserverFailure("required report proof is missing")
    captured = pending
    pending = None
    try:
        _validate_proof_record(raw.proof_mode, captured)
        captured.teardown()
        observed = captured.observe()
        if (
            type(observed) is not tuple
            or len(observed) != 4
            or type(observed[0]) is not str
            or observed[0] != "EMPTY"
            or observed[1] is not False
            or type(observed[1]) is not bool
            or observed[2] is not None
            or observed[3] is not None
        ):
            raise _ObserverFailure("report cache was not empty after teardown")
        packet: dict[str, object] = {
            "domain": "TASK064-REPORT-PROOF-OBSERVATION-V1",
            "nonce": raw.proof_nonce,
            "observer_pid": os.getpid(),
            "observer_parent_pid": os.getppid(),
            "mode": raw.proof_mode,
            "node_id": _REPORT_NODE,
            "contract_generation": _CONTRACT_GENERATION,
            "contract_sha256": _CONTRACT_SHA256,
            "publication_status": captured.publication_status,
            "output_bytes": captured.output_bytes,
            "output_sha256": captured.output_sha256,
            "cache_state_at_assertion": captured.cache_state_at_assertion,
            "cache_state_after_teardown": "EMPTY",
            "cache_teardown_completed": True,
            "receipt_consumed": captured.receipt_consumed,
            "final_file_present": captured.final_file_present,
            "stage_residue_count": captured.stage_residue_count,
            "status": "PASS",
        }
        runtime.write_proof(packet)
    except BaseException:
        runtime.latch_uncertainty()
        raise


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int | pytest.ExitCode) -> None:
    if _RAW_INVOCATION is None:
        if _TASK064_FORCE_SYMLINK_INSTALLED and session.config is _TASK064_FORCE_SYMLINK_CONFIG:
            _task064_validate_pytest_current_symlink_suppression()
        return
    runtime = _RUNTIME
    if runtime is None:
        raise pytest.UsageError("TASK-064 observer runtime is absent")
    try:
        observer = _observer_from_config(session.config)
        if observer is None:
            raise _ObserverFailure("active TASK-064 observer is absent")
        _task064_validate_pytest_current_symlink_suppression()
        if observer.phase == "execute" and _TASK064_FORCE_SYMLINK_CALLS == 0:
            raise _ObserverFailure("TASK-064 pytest current-alias suppression was unused")
        plugins = _external_plugins(session.config)
        worker_count = _worker_indicator_count(session.config)
        surviving_threads = sum(
            1 for thread in threading.enumerate() if thread is not threading.main_thread()
        )
        common: dict[str, object] = {
            "domain": (
                "TASK064-CI-COLLECTION-OBSERVATION-V1"
                if observer.phase == "collect"
                else "TASK064-CI-EXECUTION-OBSERVATION-V1"
            ),
            "nonce": observer.nonce,
            "observer_pid": os.getpid(),
            "observer_parent_pid": os.getppid(),
        }
        if observer.phase == "collect":
            packet: dict[str, object] = {
                **common,
                "pytest_exitstatus": int(exitstatus),
                "collect_failed_count": _COLLECT_FAILED_COUNT,
                "collect_skipped_count": _COLLECT_SKIPPED_COUNT,
                "deselected_nodes": _DESELECTED_NODES,
                "interrupted_count": _INTERRUPTED_COUNT,
                "internal_error_count": _INTERNAL_ERROR_COUNT,
                "external_plugins": plugins,
                "worker_indicator_count": worker_count,
                "surviving_non_main_thread_count": surviving_threads,
                "nodes": observer.collected_nodes,
            }
        else:
            packet = {
                **common,
                "shard_id": observer.shard_id,
                "pytest_exitstatus": int(exitstatus),
                "collect_failed_count": _COLLECT_FAILED_COUNT,
                "collect_skipped_count": _COLLECT_SKIPPED_COUNT,
                "deselected_nodes": _DESELECTED_NODES,
                "interrupted_count": _INTERRUPTED_COUNT,
                "internal_error_count": _INTERNAL_ERROR_COUNT,
                "external_plugins": plugins,
                "worker_indicator_count": worker_count,
                "surviving_non_main_thread_count": surviving_threads,
                "unknown_report_count": observer.unknown_report_count,
                "assigned_nodes": observer.assigned_nodes,
                "collected_nodes": observer.collected_nodes,
                "started_nodes": observer.started_nodes,
                "finished_nodes": observer.finished_nodes,
                "reports": observer.reports,
            }
        if plugins != _EXPECTED_EXTERNAL_PLUGINS or worker_count != 0 or surviving_threads != 0:
            raise _ObserverFailure("TASK-064 terminal observer invariant failed")
        runtime.finish_ci(packet)
    except BaseException as error:
        runtime._abort_all(error)


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config: pytest.Config) -> None:
    if not _TASK064_FORCE_SYMLINK_INSTALLED:
        return
    try:
        _task064_restore_pytest_current_symlink_suppression(config)
    except BaseException as error:
        raise pytest.UsageError("TASK-064 pytest current-alias restore failed") from error


class _NamedFoldZeroTimezone(tzinfo):
    """Expose a zero offset without being Python's fixed UTC singleton."""

    def utcoffset(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def dst(self, value: datetime | None) -> timedelta:
        del value
        return timedelta(0)

    def tzname(self, value: datetime | None) -> str:
        del value
        return "named-fold-zero"


_CLOCK_BASE = datetime(2026, 7, 25, 12, 0)
_INVALID_CLOCK_VALUES = (
    pytest.param(_CLOCK_BASE, id="naive"),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=timezone(timedelta(hours=2))),
        id="positive-offset",
    ),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=timezone(timedelta(hours=-5))),
        id="negative-offset",
    ),
    pytest.param(
        _CLOCK_BASE.replace(tzinfo=_NamedFoldZeroTimezone(), fold=1),
        id="named-fold-zero-offset",
    ),
)


@pytest.fixture(params=_INVALID_CLOCK_VALUES)
def invalid_clock_value(request: pytest.FixtureRequest) -> datetime:
    """Return each clock representation forbidden by the fixed-UTC contract."""

    return cast(datetime, request.param)
