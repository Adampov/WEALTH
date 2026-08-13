"""Generated-data integration evidence for the TASK-064 SQLite harness."""

from __future__ import annotations

import ast
import contextlib
import contextvars
import dis
import errno
import fcntl
import hashlib
import importlib.util
import inspect
import json
import marshal
import os
import secrets
import selectors
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import CodeType, FunctionType
from typing import TYPE_CHECKING, Any, Never, cast
from uuid import UUID

import pytest
from _pytest.assertion.rewrite import _rewrite_test

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


def _bind_task064_harness_module() -> None:
    harness._bind_task064_test_module()


_bind_task064_harness_module()
del _bind_task064_harness_module


RECORDED_AT = datetime(2026, 7, 29, 0, 0, tzinfo=UTC)
_FIXTURE_REQUEST_TYPE = pytest.FixtureRequest
_TEMP_PATH_FACTORY_TYPE = pytest.TempPathFactory
_TEMP_PATH_MKTEMP = pytest.TempPathFactory.mktemp
type _Task064FixtureCallable = Callable[
    [pytest.FixtureRequest, Path, pytest.TempPathFactory],
    Iterator[harness._PytestRootCapability],
]


def _fresh_subprocess_pycache_environment(
    pycache_root: Path,
    *,
    label: str,
) -> tuple[dict[str, str], Path]:
    """Exclude every pre-existing pytest-rewrite cache from child authority code."""

    if (
        not isinstance(pycache_root, Path)
        or not pycache_root.is_absolute()
        or type(label) is not str
        or not label.startswith("task064-")
        or "/" in label
        or "\\" in label
    ):
        raise AssertionError("invalid TASK064 subprocess pycache isolation")
    pycache_prefix = pycache_root / label
    if pycache_prefix.exists():
        raise AssertionError("TASK064 subprocess pycache prefix already exists")
    environment = os.environ.copy()
    environment.pop(harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT, None)
    for reserved_name in harness._TASK064_LEGACY_CHILD_MODE_ENVIRONMENTS:
        environment.pop(reserved_name, None)
    environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONPYCACHEPREFIX"] = str(pycache_prefix)
    assert Path(environment["PYTHONPYCACHEPREFIX"]) == pycache_prefix
    return environment, pycache_prefix


def _assert_task064_child_fixture_fingerprint_budget() -> None:
    begin_closure = inspect.getclosurevars(harness._begin_pytest_root_registration)
    authenticate_fixture_call = cast(
        FunctionType,
        begin_closure.nonlocals["authenticate_fixture_call"],
    )
    authenticate_closure = inspect.getclosurevars(authenticate_fixture_call)
    fingerprint = cast(
        Callable[[CodeType, str], str],
        authenticate_closure.nonlocals["code_fingerprint"],
    )
    metrics = inspect.getclosurevars(fingerprint).nonlocals
    assert metrics["fingerprint_requests"] == 3
    assert metrics["fingerprint_computations"] == 2
    assert len(cast(dict[object, object], metrics["fingerprint_cache"])) == 2


def _task064_child_dispatch_plan(
    protocol: str,
    node_id: str,
    modes: tuple[str, ...],
) -> tuple[str | None, tuple[str, ...]]:
    child_mode = harness._claim_task064_child_dispatch_provenance(
        protocol,
        node_id,
        modes,
    )
    if child_mode is not None:
        _assert_task064_child_fixture_fingerprint_budget()
    return child_mode, (() if child_mode is not None else modes)


def _run_task064_pytest_child(
    pytest_root: Path,
    *,
    protocol: str,
    issuer_node_id: str,
    target_node_id: str,
    mode: str,
    pycache_label: str,
    timeout_seconds: int,
) -> tuple[subprocess.CompletedProcess[str], float]:
    environment, pycache_prefix = _fresh_subprocess_pycache_environment(
        pytest_root,
        label=pycache_label,
    )
    if protocol == "report_close":
        raise AssertionError("report-close children require the authenticated authority launcher")
    provenance = harness._issue_task064_child_provenance(
        pytest_root,
        protocol,
        issuer_node_id,
        target_node_id,
        mode,
    )
    environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT] = provenance.envelope
    started = time.monotonic()
    try:
        completed = subprocess.run(
            (
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                target_node_id,
            ),
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            pass_fds=tuple(
                descriptor
                for descriptor in (
                    provenance.descriptor,
                    provenance.artifact_descriptor,
                )
                if descriptor >= 0
            ),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    finally:
        provenance_consumed = harness._close_task064_child_provenance(provenance)
    elapsed_seconds = time.monotonic() - started
    assert provenance_consumed, (
        f"TASK064 child did not consume one-shot provenance for {protocol!r}/{mode!r}"
    )
    assert not pycache_prefix.exists()
    return completed, elapsed_seconds


def _run_task064_exec_isolated(
    request: pytest.FixtureRequest,
    pycache_root: Path,
) -> bool:
    """Run one poison-producing parameter in a fresh interpreter exactly once."""

    isolated_node_id, dispatch_modes = _task064_child_dispatch_plan(
        "exec_isolation",
        request.node.nodeid,
        (request.node.nodeid,),
    )
    if isolated_node_id is not None:
        assert isolated_node_id == request.node.nodeid
        assert dispatch_modes == ()
        return False
    assert dispatch_modes == (request.node.nodeid,)
    completed, _ = _run_task064_pytest_child(
        pycache_root,
        protocol="exec_isolation",
        issuer_node_id=request.node.nodeid,
        target_node_id=request.node.nodeid,
        mode=request.node.nodeid,
        pycache_label="task064-exec-isolation-pycache",
        timeout_seconds=180,
    )
    assert completed.returncode == 0, (
        f"isolated TASK064 probe failed for {request.node.nodeid!r}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return True


class _HostileMapping(dict[object, object]):
    """Mapping subclass whose polymorphic iteration must never be invoked."""

    items_calls: int

    def __init__(self) -> None:
        super().__init__()
        self.items_calls = 0

    def items(self) -> Any:
        self.items_calls += 1
        raise AssertionError("hostile Mapping.items() must not be called")


def _build_active_task064_pytest_root_fixture() -> Callable[..., object]:
    module_globals = globals()
    module_name = __name__
    module_file = __file__
    pytest_module = pytest
    harness_module = harness
    inspect_module = inspect
    os_module = os
    contextvars_module = contextvars
    fixture_request_type = _FIXTURE_REQUEST_TYPE
    temp_path_factory_type = _TEMP_PATH_FACTORY_TYPE
    temp_path_mktemp = _TEMP_PATH_MKTEMP
    path_type = Path
    function_type = FunctionType
    pytest_fixture = pytest.fixture
    unwrap_fixture = inspect.unwrap
    environment = os.environ
    context_type = contextvars.Context
    begin_registration = harness._begin_pytest_root_registration
    root_scope = harness._pytest_root_scope
    cancel_registration = harness._cancel_pytest_root_registration
    authenticate_child_provenance = harness._authenticate_task064_child_provenance
    activate_child_provenance = harness._activate_task064_child_provenance
    claim_post_return_child_provenance = harness._claim_task064_post_return_child_provenance
    child_ticket_is_report_close = harness._task064_child_ticket_is_report_close
    claim_child_dispatch_provenance = harness._claim_task064_child_dispatch_provenance
    finish_child_provenance = harness._finish_task064_child_provenance
    cancel_child_provenance = harness._cancel_task064_child_provenance
    harness_failure_type = harness.HarnessFailure
    harness_failure_code_type = harness.HarnessFailureCode
    invalid_bootstrap_code = harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    dict_get = dict.get
    getattr_value = getattr
    isinstance_value = isinstance
    type_of = type
    bool_type = bool
    str_type = str
    runtime_error_type = RuntimeError
    assertion_error_type = AssertionError
    base_exception_type = BaseException
    next_value = next
    raw_fixture: _Task064FixtureCallable | None = None
    exported_fixture: Callable[..., object] | None = None

    def invoke_fixture_callable(
        fixture: _Task064FixtureCallable,
        fixture_request: pytest.FixtureRequest,
        fixture_root: Path,
        fixture_factory: pytest.TempPathFactory,
    ) -> Iterator[harness._PytestRootCapability]:
        return fixture(fixture_request, fixture_root, fixture_factory)

    def bindings_are_exact() -> bool:
        return bool_type(
            raw_fixture is not None
            and exported_fixture is not None
            and dict_get(module_globals, "__name__") == module_name
            and dict_get(module_globals, "__file__") == module_file
            and dict_get(module_globals, "pytest") is pytest_module
            and dict_get(module_globals, "harness") is harness_module
            and dict_get(module_globals, "inspect") is inspect_module
            and dict_get(module_globals, "os") is os_module
            and dict_get(module_globals, "contextvars") is contextvars_module
            and dict_get(module_globals, "Path") is path_type
            and dict_get(module_globals, "FunctionType") is function_type
            and dict_get(module_globals, "_FIXTURE_REQUEST_TYPE") is fixture_request_type
            and dict_get(module_globals, "_TEMP_PATH_FACTORY_TYPE") is temp_path_factory_type
            and dict_get(module_globals, "_TEMP_PATH_MKTEMP") is temp_path_mktemp
            and dict_get(module_globals, "_active_task064_pytest_root") is exported_fixture
            and getattr_value(pytest_module, "FixtureRequest", None) is fixture_request_type
            and getattr_value(pytest_module, "TempPathFactory", None) is temp_path_factory_type
            and getattr_value(temp_path_factory_type, "mktemp", None) is temp_path_mktemp
            and getattr_value(pytest_module, "fixture", None) is pytest_fixture
            and getattr_value(inspect_module, "unwrap", None) is unwrap_fixture
            and getattr_value(os_module, "environ", None) is environment
            and getattr_value(contextvars_module, "Context", None) is context_type
            and getattr_value(harness_module, "_begin_pytest_root_registration", None)
            is begin_registration
            and getattr_value(harness_module, "_pytest_root_scope", None) is root_scope
            and getattr_value(harness_module, "_cancel_pytest_root_registration", None)
            is cancel_registration
            and getattr_value(
                harness_module,
                "_authenticate_task064_child_provenance",
                None,
            )
            is authenticate_child_provenance
            and getattr_value(harness_module, "_activate_task064_child_provenance", None)
            is activate_child_provenance
            and getattr_value(
                harness_module,
                "_claim_task064_post_return_child_provenance",
                None,
            )
            is claim_post_return_child_provenance
            and getattr_value(
                harness_module,
                "_task064_child_ticket_is_report_close",
                None,
            )
            is child_ticket_is_report_close
            and getattr_value(
                harness_module,
                "_claim_task064_child_dispatch_provenance",
                None,
            )
            is claim_child_dispatch_provenance
            and getattr_value(harness_module, "_finish_task064_child_provenance", None)
            is finish_child_provenance
            and getattr_value(harness_module, "_cancel_task064_child_provenance", None)
            is cancel_child_provenance
            and getattr_value(harness_module, "HarnessFailure", None) is harness_failure_type
            and getattr_value(harness_module, "HarnessFailureCode", None)
            is harness_failure_code_type
            and getattr_value(harness_failure_code_type, "INVALID_BOOTSTRAP_ROOT", None)
            is invalid_bootstrap_code
            and unwrap_fixture(exported_fixture) is raw_fixture
        )

    def _active_task064_pytest_root(
        request: pytest.FixtureRequest,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
    ) -> Iterator[harness._PytestRootCapability]:
        sealed_raw_fixture = raw_fixture
        sealed_exported_fixture = exported_fixture
        if (
            not bindings_are_exact()
            or sealed_raw_fixture is None
            or sealed_exported_fixture is None
        ):
            raise runtime_error_type("invalid TASK064 pytest fixture dependency seal")
        if (
            not isinstance_value(request, fixture_request_type)
            or not isinstance_value(tmp_path_factory, temp_path_factory_type)
            or not isinstance_value(tmp_path, path_type)
            or type_of(request.node.nodeid) is not str_type
        ):
            raise runtime_error_type("invalid TASK064 pytest fixture authority")
        node_id = request.node.nodeid
        ticket = authenticate_child_provenance(node_id)
        permit = None
        try:
            activate_child_provenance(ticket, node_id, tmp_path)
            publication_only = child_ticket_is_report_close(ticket, node_id)
            replay_mode = claim_post_return_child_provenance(ticket, node_id)
            replay_fixture = unwrap_fixture(sealed_exported_fixture)
            if (
                type_of(replay_fixture) is not function_type
                or replay_fixture is not sealed_raw_fixture
            ):
                raise runtime_error_type("invalid TASK064 pytest fixture callable")
            permit = begin_registration(
                node_id,
                tmp_path,
                publication_only,
            )
            fixture_roots = (
                temp_path_mktemp(tmp_path_factory, "task064-secondary-0"),
                temp_path_mktemp(tmp_path_factory, "task064-secondary-1"),
                temp_path_mktemp(tmp_path_factory, "task064-secondary-2"),
                temp_path_mktemp(tmp_path_factory, "task064-secondary-3"),
            )
            fixture_scope = root_scope(permit, fixture_roots)
            failures_before_yield = request.session.testsfailed
            with fixture_scope as capability:
                yield capability
            if ticket is not None and request.session.testsfailed != failures_before_yield:
                raise runtime_error_type("TASK064 authenticated child body failed")
            if replay_mode:
                replay_root = temp_path_mktemp(
                    tmp_path_factory,
                    "task064-returned-replay",
                )
                replay_parent = replay_root.parent
                replay_inventory = tuple(replay_parent.iterdir())
                replay_calls = 0

                def invoke_exact_fixture() -> Iterator[harness._PytestRootCapability]:
                    nonlocal replay_calls
                    replay_calls += 1
                    return invoke_fixture_callable(
                        sealed_raw_fixture,
                        request,
                        replay_root,
                        tmp_path_factory,
                    )

                replay_generator: Iterator[harness._PytestRootCapability] | None = None
                replay_context = context_type()
                try:
                    replay_generator = replay_context.run(invoke_exact_fixture)
                    try:
                        replay_context.run(next_value, replay_generator)
                    except harness_failure_type as error:
                        if error.code is not invalid_bootstrap_code:
                            raise
                    else:
                        raise assertion_error_type(
                            "returned fixture lifecycle replay minted authority"
                        )
                finally:
                    if replay_generator is not None:
                        replay_generator.close()  # type: ignore[attr-defined]
                assert replay_calls == 1
                assert tuple(replay_parent.iterdir()) == replay_inventory
                assert tuple(replay_root.iterdir()) == ()
            finish_child_provenance(ticket, node_id)
        except base_exception_type as fixture_error:
            cleanup_errors: list[BaseException] = []
            if permit is not None:
                try:
                    cancel_registration(permit)
                except base_exception_type as cleanup_error:
                    cleanup_errors.append(cleanup_error)
            try:
                cancel_child_provenance(ticket, node_id)
            except base_exception_type as cleanup_error:
                cleanup_errors.append(cleanup_error)
            if cleanup_errors:
                raise harness_failure_type(invalid_bootstrap_code) from fixture_error
            raise

    raw_fixture = _active_task064_pytest_root
    exported_fixture = pytest_fixture(autouse=True)(raw_fixture)
    if unwrap_fixture(exported_fixture) is not raw_fixture:
        raise runtime_error_type("invalid TASK064 pytest fixture decoration")
    return exported_fixture


_active_task064_pytest_root = _build_active_task064_pytest_root_fixture()
del _build_active_task064_pytest_root_fixture


def _assert_task064_report_provenance_packet_boundaries(
    provenance: harness._Task064ChildProvenance,
    *,
    target_node_id: str,
    sibling_artifact_descriptor: int,
    pycache_prefix: Path,
) -> None:
    original_packet_raw = os.pread(provenance.descriptor, 8_192, 0)
    original_packet = cast(dict[str, object], json.loads(original_packet_raw))
    assert not any(key.startswith("authority_") for key in original_packet)
    original_artifact = os.pread(
        provenance.artifact_descriptor,
        cast(int, original_packet["artifact_length"]) + 1,
        0,
    )
    assert len(original_artifact) == original_packet["artifact_length"]
    marker_identity = provenance.marker_path.lstat()
    artifact_identity = os.fstat(provenance.artifact_descriptor)
    assert harness._probe_task064_parent_attestation_binding(
        provenance,
        hashlib.sha256(original_packet_raw).hexdigest(),
    )

    def canonical_packet(packet: dict[str, object]) -> bytes:
        return json.dumps(
            packet,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")

    def rewrite_marker(raw: bytes) -> None:
        os.ftruncate(provenance.descriptor, 0)
        assert os.pwrite(provenance.descriptor, raw, 0) == len(raw)
        os.fsync(provenance.descriptor)

    def reject(packet: dict[str, object]) -> None:
        hostile_raw = canonical_packet(packet)
        hostile_digest = hashlib.sha256(hostile_raw).hexdigest()
        assert not harness._probe_task064_parent_attestation_binding(
            provenance,
            hostile_digest,
        )
        rewrite_marker(hostile_raw)
        envelope = cast(dict[str, object], json.loads(provenance.envelope))
        envelope["packet_sha256"] = hostile_digest
        hostile_envelope = json.dumps(
            envelope,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        # This probe is deliberately non-authorizing: it proves the closed broker
        # remains bound to the original packet digest without arming or spawning.
        assert json.loads(hostile_envelope)["packet_sha256"] == hostile_digest
        assert provenance.marker_path.exists()
        assert provenance.marker_path.lstat().st_ino == marker_identity.st_ino

    scalar_mutations: tuple[tuple[str, object], ...] = (
        ("artifact_descriptor", provenance.descriptor),
        ("artifact_descriptor", sibling_artifact_descriptor),
        ("artifact_device", cast(int, original_packet["artifact_device"]) + 1),
        ("artifact_inode", cast(int, original_packet["artifact_inode"]) + 1),
        ("artifact_uid", cast(int, original_packet["artifact_uid"]) + 1),
        ("artifact_mode", 0o400),
        ("artifact_nlink", 1),
        ("artifact_length", original_packet["artifact_length"] + 1),
        ("artifact_sha256", "0" * 64),
        ("artifact_nonce", original_packet["nonce"]),
        ("artifact_deadline_ns", time.monotonic_ns() - 1),
        ("artifact_deadline_ns", time.monotonic_ns() + 901_000_000_000),
        ("contract_generation", cast(int, original_packet["contract_generation"]) + 1),
        ("contract_digest", "0" * 64),
        ("schema_fingerprint", "sha256:" + "0" * 64),
        ("parent_pid", os.getpid() + 1),
        ("issuer_node_id", f"{target_node_id}::wrong-issuer"),
        ("target_node_id", f"{target_node_id}::wrong-target"),
        ("mode", "task064-wrong-mode"),
        ("publication_nonce", original_packet["nonce"]),
        (
            "publication_run_digest",
            "0" * 64,
        ),
        ("publication_root_inode", cast(int, original_packet["publication_root_inode"]) + 1),
        ("publication_inode", cast(int, original_packet["publication_inode"]) + 1),
        ("publication_nlink", 0),
        ("publication_process_id", os.getpid() + 1),
        ("publication_thread_id", 0),
        ("publication_node_id", f"{target_node_id}::wrong-publication-node"),
        ("publication_context_digest", "0" * 64),
        ("publication_expires_ns", time.monotonic_ns() - 1),
    )
    try:
        for key, hostile_value in scalar_mutations:
            packet = cast(dict[str, object], json.loads(original_packet_raw))
            packet[key] = hostile_value
            reject(packet)
        packet = cast(dict[str, object], json.loads(original_packet_raw))
        source_fingerprints = cast(list[list[str]], packet["source_fingerprints"])
        source_fingerprints[0][1] = "0" * 64
        reject(packet)

        read_write_alias = os.open(
            f"/proc/self/fd/{provenance.artifact_descriptor}",
            os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            packet = cast(dict[str, object], json.loads(original_packet_raw))
            packet["artifact_descriptor"] = read_write_alias
            reject(packet)
        finally:
            os.close(read_write_alias)

        mutation_descriptor = os.open(
            f"/proc/self/fd/{provenance.artifact_descriptor}",
            os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            same_length = original_artifact.replace(
                b'"report_version":1',
                b'"report_version":2',
                1,
            )
            assert len(same_length) == len(original_artifact)
            for hostile_artifact in (
                original_artifact[:-1],
                original_artifact + b"\n",
                same_length,
                b" " + original_artifact[1:],
            ):
                os.ftruncate(mutation_descriptor, len(hostile_artifact))
                assert os.pwrite(mutation_descriptor, hostile_artifact, 0) == len(hostile_artifact)
                os.fsync(mutation_descriptor)
                packet = cast(dict[str, object], json.loads(original_packet_raw))
                packet["artifact_length"] = len(hostile_artifact)
                packet["artifact_sha256"] = hashlib.sha256(hostile_artifact).hexdigest()
                reject(packet)
        finally:
            os.ftruncate(mutation_descriptor, len(original_artifact))
            assert os.pwrite(mutation_descriptor, original_artifact, 0) == len(original_artifact)
            os.fsync(mutation_descriptor)
            os.close(mutation_descriptor)
    finally:
        rewrite_marker(original_packet_raw)
    assert harness._probe_task064_parent_attestation_binding(
        provenance,
        hashlib.sha256(original_packet_raw).hexdigest(),
    )
    assert os.pread(provenance.descriptor, 8_192, 0) == original_packet_raw
    assert (
        os.pread(provenance.artifact_descriptor, len(original_artifact) + 1, 0) == original_artifact
    )
    assert os.fstat(provenance.artifact_descriptor).st_ino == artifact_identity.st_ino

    copied_provenance = replace(provenance)
    with pytest.raises(harness.HarnessFailure) as copied_launch:
        harness._spawn_task064_authenticated_child(
            copied_provenance,
            pycache_prefix=pycache_prefix,
        )
    assert copied_launch.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    with pytest.raises(harness.HarnessFailure) as copied_close:
        harness._close_task064_child_provenance(copied_provenance)
    assert copied_close.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    assert provenance.marker_path.lstat().st_ino == marker_identity.st_ino
    competing_listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        with pytest.raises(OSError) as occupied:
            competing_listener.bind(b"\x00wealth-task064-g3-" + str(os.getpid()).encode("ascii"))
        assert occupied.value.errno == errno.EADDRINUSE
    finally:
        competing_listener.close()
    assert harness._probe_task064_parent_attestation_binding(
        provenance,
        hashlib.sha256(original_packet_raw).hexdigest(),
    )


_TASK064_PARENT_HANDSHAKE_TIMEOUT_SECONDS = 30.0


def _launch_task064_wrapper_bypass_probe(
    pytest_root: Path,
    *,
    provenance: harness._Task064ChildProvenance,
    target_node_id: str,
    pycache_prefix: Path,
    extra_pass_fds: tuple[int, ...] = (),
) -> tuple[subprocess.Popen[bytes], int, Path, bytes]:
    """Queue one exact-provenance direct child before the registered wrapper child."""

    observer_root = pytest_root / f"task064-wrapper-bypass-observer-{secrets.token_hex(8)}"
    observer_path = observer_root / "sitecustomize.py"
    observer_source = textwrap.dedent(
        """
            import os
            import socket
            from pathlib import Path

            _probe_fd = int(os.environ["WEALTH_TASK064_BYPASS_PROBE_FD"])
            _artifact_fd = int(os.environ["WEALTH_TASK064_BYPASS_ARTIFACT_FD"])
            _watched_paths = {
                os.environ["WEALTH_TASK064_BYPASS_ROOT_PATH"],
                os.environ["WEALTH_TASK064_BYPASS_MARKER_PATH"],
                os.environ["WEALTH_TASK064_BYPASS_PUBLICATION_PATH"],
            }
            _expected_address = b"\\x00wealth-task064-g3-" + str(os.getppid()).encode("ascii")
            _attested = False

            def _emit(marker):
                try:
                    os.write(_probe_fd, marker)
                except OSError:
                    pass

            _original_socket = socket.socket

            class _ObservedSocket(_original_socket):
                def connect(self, address):
                    result = super().connect(address)
                    if address == _expected_address:
                        _emit(b"C")
                    return result

                def recv(self, *args, **kwargs):
                    global _attested
                    data = super().recv(*args, **kwargs)
                    if b'"status":"ATTESTED"' in data:
                        _attested = True
                    return data

            socket.socket = _ObservedSocket

            _original_fstat = os.fstat
            def _observed_fstat(descriptor):
                if not _attested and descriptor == _artifact_fd:
                    _emit(b"F")
                return _original_fstat(descriptor)
            os.fstat = _observed_fstat

            _original_read = os.read
            def _observed_read(descriptor, size):
                if not _attested and descriptor == _artifact_fd:
                    _emit(b"R")
                return _original_read(descriptor, size)
            os.read = _observed_read

            _original_open = os.open
            def _observed_open(path, *args, **kwargs):
                exact_path = os.fsdecode(os.fspath(path))
                if not _attested and (
                    exact_path in _watched_paths
                    or exact_path == "task064-evidence.json"
                ):
                    _emit(b"O")
                return _original_open(path, *args, **kwargs)
            os.open = _observed_open

            _original_lstat = Path.lstat
            def _observed_lstat(self, *args, **kwargs):
                if not _attested and os.fspath(self) in _watched_paths:
                    _emit(b"L")
                return _original_lstat(self, *args, **kwargs)
            Path.lstat = _observed_lstat
            """
    )
    probe_read_descriptor = -1
    probe_write_descriptor = -1
    process: subprocess.Popen[bytes] | None = None
    selector: selectors.BaseSelector | None = None
    try:
        observer_root.mkdir(mode=0o700)
        observer_path.write_text(observer_source, encoding="utf-8")
        probe_read_descriptor, probe_write_descriptor = os.pipe2(os.O_CLOEXEC)
        child_environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith(("PYTHON", "PYTEST", "LD_"))
            and name
            not in {
                "WEALTH_TASK064_EXEC_ISOLATION",
                "WEALTH_TASK064_POST_RETURN_FIXTURE_REPLAY",
                "WEALTH_TASK064_ROOT_LATCH_PROBE",
                "WEALTH_TASK064_SHARED_CLEANUP_PROBE",
                "WEALTH_TASK064_REPORT_ROOT_CLOSE_PROBE",
                harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT,
            }
        }
        child_environment.update(
            {
                "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPYCACHEPREFIX": str(pycache_prefix),
                "PYTHONPATH": str(observer_root),
                harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT: provenance.envelope,
                "WEALTH_TASK064_BYPASS_PROBE_FD": str(probe_write_descriptor),
                "WEALTH_TASK064_BYPASS_ARTIFACT_FD": str(provenance.artifact_descriptor),
                "WEALTH_TASK064_BYPASS_ROOT_PATH": str(provenance.root_path),
                "WEALTH_TASK064_BYPASS_MARKER_PATH": str(provenance.marker_path),
                "WEALTH_TASK064_BYPASS_PUBLICATION_PATH": str(
                    provenance.root_path / "task064-evidence.json"
                ),
            }
        )
        process = subprocess.Popen(
            (
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-p",
                "no:cacheprovider",
                target_node_id,
            ),
            cwd=Path(__file__).resolve().parents[2],
            env=child_environment,
            pass_fds=(
                provenance.descriptor,
                provenance.artifact_descriptor,
                probe_write_descriptor,
                *extra_pass_fds,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        owned_write_descriptor = probe_write_descriptor
        probe_write_descriptor = -1
        os.close(owned_write_descriptor)
        selector = selectors.DefaultSelector()
        initial_observation = b""
        selector.register(probe_read_descriptor, selectors.EVENT_READ)
        handshake_deadline = time.monotonic() + _TASK064_PARENT_HANDSHAKE_TIMEOUT_SECONDS
        while True:
            return_code = process.poll()
            if return_code is not None:
                raise AssertionError(
                    "TASK064 wrapper-bypass child exited "
                    f"with status {return_code} before queuing its attestation"
                )
            remaining = handshake_deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("TASK064 wrapper-bypass child did not queue its attestation")
            if selector.select(timeout=min(0.1, remaining)):
                break
        initial_observation = os.read(probe_read_descriptor, 64)
        if initial_observation != b"C" or process.poll() is not None:
            raise AssertionError(
                "TASK064 wrapper-bypass child accessed authority data or exited before rejection"
            )
        return process, probe_read_descriptor, observer_root, initial_observation
    except BaseException:
        if process is not None:
            process_is_live = True
            with contextlib.suppress(BaseException):
                process_is_live = process.poll() is None
            if process_is_live:
                with contextlib.suppress(BaseException):
                    os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(BaseException):
                process.wait(timeout=10.0)
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    with contextlib.suppress(BaseException):
                        stream.close()
        if probe_read_descriptor >= 0:
            owned_read_descriptor = probe_read_descriptor
            probe_read_descriptor = -1
            with contextlib.suppress(BaseException):
                os.close(owned_read_descriptor)
        with contextlib.suppress(BaseException):
            observer_path.unlink()
        with contextlib.suppress(BaseException):
            observer_root.rmdir()
        raise
    finally:
        if selector is not None:
            owned_selector = selector
            selector = None
            with contextlib.suppress(BaseException):
                owned_selector.close()
        if probe_write_descriptor >= 0:
            owned_write_descriptor = probe_write_descriptor
            probe_write_descriptor = -1
            with contextlib.suppress(BaseException):
                os.close(owned_write_descriptor)


def _finish_task064_wrapper_bypass_probe(
    probe: tuple[subprocess.Popen[bytes], int, Path, bytes],
) -> None:
    process, probe_read_descriptor, observer_root, initial_observation = probe
    observer_path = observer_root / "sitecustomize.py"
    stdout = b""
    stderr = b""
    observation = bytearray(initial_observation)
    try:
        try:
            stdout, stderr = process.communicate(timeout=10.0)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate(timeout=10.0)
            raise AssertionError(
                "TASK064 wrapper-bypass child was not boundedly rejected"
            ) from None
        while True:
            fragment = os.read(probe_read_descriptor, 64)
            if not fragment:
                break
            observation.extend(fragment)
        assert process.returncode is not None and process.returncode != 0
        assert process.poll() == process.returncode
        assert bytes(observation) == b"C"
        assert len(stdout) <= 1_048_576
        assert len(stderr) <= 1_048_576
    finally:
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=10.0)
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        os.close(probe_read_descriptor)
        with contextlib.suppress(OSError):
            observer_path.unlink()
        with contextlib.suppress(OSError):
            observer_root.rmdir()
        assert not observer_path.exists()
        assert not observer_root.exists()


def _launch_task064_synthetic_fake_ack_probe(
    pytest_root: Path,
    *,
    target_node_id: str,
    mode: str,
    pycache_prefix: Path,
) -> tuple[
    tuple[subprocess.Popen[bytes], int, Path, bytes],
    harness._Task064ChildProvenance,
    socket.socket,
    threading.Thread,
    list[bool],
]:
    """Queue synthetic files plus a usable packet-supplied same-parent ACK channel."""

    nonce = secrets.token_hex(32)
    marker_path = pytest_root / f".task064-child-provenance-{nonce}.json"
    artifact_path = pytest_root / f".task064-synthetic-artifact-{nonce}.json"
    marker_descriptor = -1
    artifact_descriptor = -1
    fake_parent: socket.socket | None = None
    fake_child: socket.socket | None = None
    fake_ack_thread: threading.Thread | None = None
    fake_ack_thread_started = False
    fake_ack_used: list[bool] = []
    probe: tuple[subprocess.Popen[bytes], int, Path, bytes] | None = None
    try:
        artifact_write_descriptor = os.open(
            artifact_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        try:
            synthetic_artifact = b'{"synthetic":"not-authority"}\n'
            assert os.write(artifact_write_descriptor, synthetic_artifact) == len(
                synthetic_artifact
            )
            os.fsync(artifact_write_descriptor)
        finally:
            os.close(artifact_write_descriptor)
        artifact_descriptor = os.open(artifact_path, os.O_RDONLY | os.O_CLOEXEC)
        artifact_path.unlink()
        assert os.fstat(artifact_descriptor).st_nlink == 0

        fake_parent, fake_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        fake_child_details = os.fstat(fake_child.fileno())
        packet = {
            "authority_descriptor": fake_child.fileno(),
            "authority_device": fake_child_details.st_dev,
            "authority_inode": fake_child_details.st_ino,
            "authority_mode": stat.S_IMODE(fake_child_details.st_mode),
            "authority_uid": fake_child_details.st_uid,
            "domain": "TASK064-CHILD-PROVENANCE-V1",
            "mode": mode,
            "parent_pid": os.getpid(),
            "protocol": "report_close",
            "target_node_id": target_node_id,
        }
        packet_raw = json.dumps(
            packet,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        packet_digest = hashlib.sha256(packet_raw).hexdigest()
        marker_descriptor = os.open(
            marker_path,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        assert os.write(marker_descriptor, packet_raw) == len(packet_raw)
        os.fsync(marker_descriptor)
        os.lseek(marker_descriptor, 0, os.SEEK_SET)
        marker_details = os.fstat(marker_descriptor)
        root_details = pytest_root.lstat()
        envelope = json.dumps(
            {
                "descriptor": marker_descriptor,
                "domain": "TASK064-CHILD-PROVENANCE-V1",
                "marker_path": str(marker_path),
                "packet_sha256": packet_digest,
            },
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        synthetic_provenance = harness._Task064ChildProvenance(
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

        def serve_fake_ack() -> None:
            assert fake_parent is not None
            fake_parent.settimeout(10.0)
            try:
                raw_request = fake_parent.recv(2_049)
                if not raw_request:
                    fake_ack_used.append(False)
                    return
                request = cast(dict[str, object], json.loads(raw_request))
                fake_ack_used.append(True)
                fake_parent.sendall(
                    json.dumps(
                        {
                            "challenge": request["challenge"],
                            "child_pid": request["child_pid"],
                            "domain": "TASK064-CHILD-PROVENANCE-V1/parent-attestation",
                            "mode": request["mode"],
                            "packet_sha256": request["packet_sha256"],
                            "parent_pid": os.getpid(),
                            "status": "ATTESTED",
                            "target_node_id": request["target_node_id"],
                        },
                        ensure_ascii=True,
                        allow_nan=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("ascii")
                )
                fake_parent.shutdown(socket.SHUT_WR)
            except (OSError, TimeoutError, ValueError, KeyError):
                fake_ack_used.append(False)

        fake_ack_thread = threading.Thread(
            target=serve_fake_ack,
            name="task064-synthetic-fake-parent-ack",
            daemon=True,
        )
        fake_ack_thread.start()
        fake_ack_thread_started = True
        probe = _launch_task064_wrapper_bypass_probe(
            pytest_root,
            provenance=synthetic_provenance,
            target_node_id=target_node_id,
            pycache_prefix=pycache_prefix,
            extra_pass_fds=(fake_child.fileno(),),
        )
        fake_child.close()
        fake_child = None
        return probe, synthetic_provenance, fake_parent, fake_ack_thread, fake_ack_used
    except BaseException:
        if probe is not None:
            with contextlib.suppress(BaseException):
                _finish_task064_wrapper_bypass_probe(probe)
        for channel in (fake_child, fake_parent):
            if channel is not None:
                with contextlib.suppress(OSError):
                    channel.close()
        if fake_ack_thread is not None and fake_ack_thread_started:
            fake_ack_thread.join(timeout=2.0)
        for descriptor in (artifact_descriptor, marker_descriptor):
            if descriptor >= 0:
                with contextlib.suppress(OSError):
                    os.close(descriptor)
        with contextlib.suppress(OSError):
            marker_path.unlink()
        with contextlib.suppress(OSError):
            artifact_path.unlink()
        raise


def _finish_task064_synthetic_fake_ack_probe(
    probe: tuple[
        tuple[subprocess.Popen[bytes], int, Path, bytes],
        harness._Task064ChildProvenance,
        socket.socket,
        threading.Thread,
        list[bool],
    ],
) -> None:
    child_probe, provenance, fake_parent, fake_ack_thread, fake_ack_used = probe
    try:
        _finish_task064_wrapper_bypass_probe(child_probe)
    finally:
        fake_parent.close()
        fake_ack_thread.join(timeout=2.0)
        for descriptor in (provenance.artifact_descriptor, provenance.descriptor):
            with contextlib.suppress(OSError):
                os.close(descriptor)
        with contextlib.suppress(OSError):
            provenance.marker_path.unlink()
    assert not fake_ack_thread.is_alive()
    assert fake_ack_used == [False]
    assert not provenance.marker_path.exists()


def _assert_task064_listener_closed_in_raw_fork() -> None:
    """Prove the registered at-fork hook removes the parent's listener from a raw child."""

    result_read_descriptor, result_write_descriptor = os.pipe2(os.O_CLOEXEC)
    process_id = os.fork()
    if process_id == 0:
        os.close(result_read_descriptor)
        inherited_listener = False
        parent_address = b"\x00wealth-task064-g3-" + str(os.getppid()).encode("ascii")
        try:
            for descriptor_path in Path("/proc/self/fd").iterdir():
                try:
                    descriptor = int(descriptor_path.name)
                except ValueError:
                    continue
                if descriptor == result_write_descriptor:
                    continue
                duplicate = -1
                candidate: socket.socket | None = None
                try:
                    duplicate = os.dup(descriptor)
                    candidate = socket.socket(fileno=duplicate)
                    duplicate = -1
                    if candidate.getsockname() == parent_address:
                        inherited_listener = True
                        break
                except OSError:
                    pass
                finally:
                    if candidate is not None:
                        candidate.close()
                    elif duplicate >= 0:
                        os.close(duplicate)
            os.write(result_write_descriptor, b"1" if inherited_listener else b"0")
        finally:
            os.close(result_write_descriptor)
        os._exit(0)
    os.close(result_write_descriptor)
    selector = selectors.DefaultSelector()
    try:
        selector.register(result_read_descriptor, selectors.EVENT_READ)
        if not selector.select(timeout=5.0):
            with contextlib.suppress(ProcessLookupError):
                os.kill(process_id, signal.SIGKILL)
            os.waitpid(process_id, 0)
            raise TimeoutError("TASK064 raw-fork listener probe exceeded its deadline")
        result = bytearray()
        while True:
            fragment = os.read(result_read_descriptor, 8)
            if not fragment:
                break
            result.extend(fragment)
        waited_process_id, wait_status = os.waitpid(process_id, 0)
        assert waited_process_id == process_id
        assert os.waitstatus_to_exitcode(wait_status) == 0
        assert bytes(result) == b"0"
    finally:
        selector.close()
        os.close(result_read_descriptor)


def _run_task064_pytest_children_concurrently(
    pytest_root: Path,
    *,
    protocol: str,
    issuer_node_id: str,
    target_node_id: str,
    modes: tuple[str, ...],
    pycache_label: str,
    timeout_seconds: float,
    report_artifact_capability: (harness._Task064PublishedReportArtifactCapability | None) = None,
) -> dict[str, tuple[subprocess.CompletedProcess[str], float]]:
    """Run one authenticated child per mode beneath one shared deadline."""

    if (
        type(protocol) is not str
        or type(issuer_node_id) is not str
        or type(target_node_id) is not str
        or type(modes) is not tuple
        or not modes
        or any(type(mode) is not str or not mode for mode in modes)
        or len(set(modes)) != len(modes)
        or type(pycache_label) is not str
        or not pycache_label
        or type(timeout_seconds) not in {int, float}
        or timeout_seconds <= 0
        or (protocol == "report_close")
        != (type(report_artifact_capability) is harness._Task064PublishedReportArtifactCapability)
    ):
        raise AssertionError("invalid TASK064 concurrent child request")

    prepared: dict[
        str,
        tuple[
            dict[str, str],
            Path,
            harness._Task064ChildProvenance,
        ],
    ] = {}
    processes: dict[str, subprocess.Popen[bytes]] = {}
    process_starts: dict[str, float] = {}
    drain_threads: dict[str, threading.Thread] = {}
    started_drain_modes: set[str] = set()
    drained: dict[str, tuple[str, str, int, float]] = {}
    drain_errors: dict[str, BaseException] = {}
    provenance_consumed: dict[str, bool] = {}
    cleanup_errors: list[BaseException] = []
    condition = threading.Condition()
    maximum_output_stream_bytes = 1_048_576
    maximum_output_aggregate_bytes = 4_194_304
    aggregate_output_bytes = 0
    batch_started = time.monotonic()
    shared_deadline = batch_started + timeout_seconds
    shared_deadline_ns = time.monotonic_ns() + int(timeout_seconds * 1_000_000_000)
    orchestration_error: BaseException | None = None
    wrapper_bypass_probe: tuple[subprocess.Popen[bytes], int, Path, bytes] | None = None
    synthetic_fake_ack_probe: (
        tuple[
            tuple[subprocess.Popen[bytes], int, Path, bytes],
            harness._Task064ChildProvenance,
            socket.socket,
            threading.Thread,
            list[bool],
        ]
        | None
    ) = None

    def drain_child(mode: str, process: subprocess.Popen[bytes]) -> None:
        nonlocal aggregate_output_bytes

        stdout_buffer = bytearray()
        stderr_buffer = bytearray()
        selector = selectors.DefaultSelector()
        try:
            if process.stdout is None or process.stderr is None:
                raise AssertionError("TASK064 child pipes were not captured")
            selector.register(process.stdout, selectors.EVENT_READ, ("stdout", stdout_buffer))
            selector.register(process.stderr, selectors.EVENT_READ, ("stderr", stderr_buffer))
            while selector.get_map():
                for key, _ in selector.select():
                    stream_name, stream_buffer = cast(tuple[str, bytearray], key.data)
                    fragment = os.read(key.fd, 65_536)
                    if fragment == b"":
                        selector.unregister(key.fileobj)
                        continue
                    if len(stream_buffer) + len(fragment) > maximum_output_stream_bytes:
                        raise AssertionError(
                            f"TASK064 child {mode!r} exceeded its {stream_name} byte ceiling"
                        )
                    with condition:
                        if aggregate_output_bytes + len(fragment) > maximum_output_aggregate_bytes:
                            raise AssertionError(
                                "TASK064 concurrent children exceeded their aggregate "
                                "output byte ceiling"
                            )
                        aggregate_output_bytes += len(fragment)
                    stream_buffer.extend(fragment)
            returncode = process.wait()
            if type(returncode) is not int:
                raise AssertionError("invalid TASK064 child completion")
            stdout = bytes(stdout_buffer).decode("utf-8", errors="replace")
            stderr = bytes(stderr_buffer).decode("utf-8", errors="replace")
            completed_at = time.monotonic()
        except BaseException as error:
            with condition:
                drain_errors[mode] = error
                condition.notify_all()
        else:
            with condition:
                drained[mode] = (stdout, stderr, returncode, completed_at)
                condition.notify_all()
        finally:
            stream_cleanup_errors: list[BaseException] = []
            try:
                selector.close()
            except BaseException as error:
                stream_cleanup_errors.append(error)
            for stream in (process.stdout, process.stderr):
                if stream is None:
                    continue
                try:
                    stream.close()
                except BaseException as error:
                    stream_cleanup_errors.append(error)
            if stream_cleanup_errors:
                with condition:
                    cleanup_errors.extend(stream_cleanup_errors)
                    condition.notify_all()

    def record_cleanup_error(mode: str, operation: str, error: BaseException) -> None:
        cleanup_errors.append(
            RuntimeError(f"TASK064 concurrent child {operation} failed for {mode!r}: {error!r}")
        )

    def process_is_live(mode: str, process: subprocess.Popen[bytes]) -> bool:
        try:
            return process.poll() is None
        except BaseException as error:
            record_cleanup_error(mode, "poll", error)
            return True

    def signal_live_children(signal_number: signal.Signals) -> None:
        for mode, process in processes.items():
            if not process_is_live(mode, process):
                continue
            try:
                os.killpg(process.pid, signal_number)
            except ProcessLookupError:
                continue
            except BaseException as error:
                record_cleanup_error(mode, f"killpg-{signal_number.name}", error)
                try:
                    if signal_number is signal.SIGTERM:
                        process.terminate()
                    else:
                        process.kill()
                except BaseException as fallback_error:
                    record_cleanup_error(
                        mode,
                        f"fallback-{signal_number.name}",
                        fallback_error,
                    )

    def wait_for_drains(deadline: float) -> None:
        with condition:
            while any(
                mode not in drained and mode not in drain_errors for mode in started_drain_modes
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return
                condition.wait(timeout=remaining)

    def close_unstarted_child_pipes() -> None:
        for mode, process in processes.items():
            if mode in started_drain_modes:
                continue
            for stream_name, stream in (
                ("stdout", process.stdout),
                ("stderr", process.stderr),
            ):
                if stream is None:
                    record_cleanup_error(
                        mode,
                        f"close-unstarted-{stream_name}",
                        AssertionError("TASK064 child pipe was not captured"),
                    )
                    continue
                try:
                    stream.close()
                except BaseException as error:
                    record_cleanup_error(mode, f"close-unstarted-{stream_name}", error)

    def wait_for_unstarted_children(deadline: float, *, final: bool) -> None:
        for mode, process in processes.items():
            if mode in started_drain_modes:
                continue
            remaining = max(0.0, deadline - time.monotonic())
            try:
                returncode = process.wait(timeout=remaining)
            except (subprocess.TimeoutExpired, TimeoutError) as error:
                if final:
                    record_cleanup_error(mode, "wait-unstarted-timeout", error)
            except BaseException as error:
                record_cleanup_error(mode, "wait-unstarted", error)
            else:
                if type(returncode) is not int:
                    record_cleanup_error(
                        mode,
                        "wait-unstarted",
                        AssertionError("invalid TASK064 child completion"),
                    )

    try:
        for index, mode in enumerate(modes):
            environment, pycache_prefix = _fresh_subprocess_pycache_environment(
                pytest_root,
                label=f"{pycache_label}-{index:02d}-{mode.replace('_', '-')}",
            )
            provenance = (
                harness._issue_task064_child_provenance(
                    pytest_root,
                    protocol,
                    issuer_node_id,
                    target_node_id,
                    mode,
                    report_artifact_capability=report_artifact_capability,
                    report_deadline_ns=shared_deadline_ns,
                )
                if protocol == "report_close"
                else harness._issue_task064_child_provenance(
                    pytest_root,
                    protocol,
                    issuer_node_id,
                    target_node_id,
                    mode,
                )
            )
            if protocol != "report_close":
                environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT] = provenance.envelope
            prepared[mode] = (environment, pycache_prefix, provenance)
        if (
            len(prepared) != len(modes)
            or len({id(environment) for environment, _, _ in prepared.values()}) != len(modes)
            or len({prefix for _, prefix, _ in prepared.values()}) != len(modes)
            or len({item.descriptor for _, _, item in prepared.values()}) != len(modes)
            or (
                protocol == "report_close"
                and len({item.artifact_descriptor for _, _, item in prepared.values()})
                != len(modes)
            )
            or len({item.envelope for _, _, item in prepared.values()}) != len(modes)
            or len({item.marker_path for _, _, item in prepared.values()}) != len(modes)
        ):
            raise AssertionError("TASK064 child preparation did not remain isolated")
        if protocol == "report_close" and all(
            type(item) is harness._Task064ChildProvenance for _, _, item in prepared.values()
        ):
            exact_provenances = tuple(item for _, _, item in prepared.values())
            _assert_task064_report_provenance_packet_boundaries(
                exact_provenances[0],
                target_node_id=target_node_id,
                sibling_artifact_descriptor=exact_provenances[1].artifact_descriptor,
                pycache_prefix=prepared[modes[0]][1],
            )
            artifact_identities: set[tuple[int, int]] = set()
            for _, _, provenance in prepared.values():
                marker_details = os.fstat(provenance.descriptor)
                artifact_details = os.fstat(provenance.artifact_descriptor)
                assert stat.S_ISREG(marker_details.st_mode)
                assert stat.S_IMODE(marker_details.st_mode) == 0o600
                assert marker_details.st_nlink == 1
                assert stat.S_ISREG(artifact_details.st_mode)
                assert stat.S_IMODE(artifact_details.st_mode) == 0o600
                assert artifact_details.st_nlink == 0
                assert artifact_details.st_size > 0
                assert (marker_details.st_dev, marker_details.st_ino) != (
                    artifact_details.st_dev,
                    artifact_details.st_ino,
                )
                assert (
                    fcntl.fcntl(provenance.artifact_descriptor, fcntl.F_GETFL) & os.O_ACCMODE
                    == os.O_RDONLY
                )
                assert os.lseek(provenance.artifact_descriptor, 0, os.SEEK_CUR) == 0
                os.lseek(
                    provenance.artifact_descriptor,
                    min(17, artifact_details.st_size),
                    os.SEEK_SET,
                )
                artifact_identities.add((artifact_details.st_dev, artifact_details.st_ino))
            assert len(artifact_identities) == len(modes)
            if pycache_label == "task064-report-close-pycache" and target_node_id.endswith(
                "::test_finite_typical_workload_measurements_and_sanitized_report"
            ):
                _assert_task064_listener_closed_in_raw_fork()
                wrapper_bypass_probe = _launch_task064_wrapper_bypass_probe(
                    pytest_root,
                    provenance=exact_provenances[0],
                    target_node_id=target_node_id,
                    pycache_prefix=prepared[modes[0]][1],
                )
                synthetic_fake_ack_probe = _launch_task064_synthetic_fake_ack_probe(
                    pytest_root,
                    target_node_id=target_node_id,
                    mode=modes[0],
                    pycache_prefix=prepared[modes[0]][1],
                )

        for mode in modes:
            environment, pycache_prefix, provenance = prepared[mode]
            process_starts[mode] = time.monotonic()
            if (
                protocol == "report_close"
                and mode == modes[0]
                and type(provenance) is harness._Task064ChildProvenance
            ):
                module_popen = subprocess.Popen
                hostile_popen_calls = 0

                def hostile_module_popen(*_args: object, **_kwargs: object) -> Never:
                    nonlocal hostile_popen_calls
                    hostile_popen_calls += 1
                    raise AssertionError("TASK064 authenticated launcher used mutable module Popen")

                mutable_subprocess = cast(Any, subprocess)
                mutable_subprocess.Popen = hostile_module_popen
                try:
                    processes[mode] = harness._spawn_task064_authenticated_child(
                        provenance,
                        pycache_prefix=pycache_prefix,
                    )
                finally:
                    mutable_subprocess.Popen = module_popen
                assert hostile_popen_calls == 0
                if wrapper_bypass_probe is not None:
                    pending_bypass_probe = wrapper_bypass_probe
                    wrapper_bypass_probe = None
                    _finish_task064_wrapper_bypass_probe(pending_bypass_probe)
                if synthetic_fake_ack_probe is not None:
                    pending_synthetic_probe = synthetic_fake_ack_probe
                    synthetic_fake_ack_probe = None
                    _finish_task064_synthetic_fake_ack_probe(pending_synthetic_probe)
            else:
                processes[mode] = (
                    harness._spawn_task064_authenticated_child(
                        provenance,
                        pycache_prefix=pycache_prefix,
                    )
                    if protocol == "report_close"
                    else subprocess.Popen(
                        (
                            sys.executable,
                            "-m",
                            "pytest",
                            "-q",
                            "-p",
                            "no:cacheprovider",
                            target_node_id,
                        ),
                        cwd=Path(__file__).resolve().parents[2],
                        env=environment,
                        pass_fds=tuple(
                            descriptor
                            for descriptor in (
                                provenance.descriptor,
                                provenance.artifact_descriptor,
                            )
                            if descriptor >= 0
                        ),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        start_new_session=True,
                    )
                )
    except BaseException as error:
        orchestration_error = error
    finally:
        if wrapper_bypass_probe is not None:
            pending_bypass_probe = wrapper_bypass_probe
            wrapper_bypass_probe = None
            try:
                _finish_task064_wrapper_bypass_probe(pending_bypass_probe)
            except BaseException as error:
                cleanup_errors.append(error)
        if synthetic_fake_ack_probe is not None:
            pending_synthetic_probe = synthetic_fake_ack_probe
            synthetic_fake_ack_probe = None
            try:
                _finish_task064_synthetic_fake_ack_probe(pending_synthetic_probe)
            except BaseException as error:
                cleanup_errors.append(error)
        for mode, process in processes.items():
            try:
                thread = threading.Thread(
                    target=drain_child,
                    args=(mode, process),
                    name=f"task064-child-drain-{mode}",
                    daemon=True,
                )
                thread.start()
            except BaseException as error:
                if orchestration_error is None:
                    orchestration_error = error
                with condition:
                    drain_errors[mode] = error
                    condition.notify_all()
                continue
            drain_threads[mode] = thread
            started_drain_modes.add(mode)

        if orchestration_error is None:
            with condition:
                while len(drained) + len(drain_errors) < len(processes):
                    if drain_errors or any(
                        returncode != 0 for _, _, returncode, _ in drained.values()
                    ):
                        orchestration_error = AssertionError(
                            "TASK064 child failed before its siblings completed"
                        )
                        break
                    remaining = shared_deadline - time.monotonic()
                    if remaining <= 0:
                        orchestration_error = TimeoutError(
                            "TASK064 concurrent children exceeded their shared deadline"
                        )
                        break
                    condition.wait(timeout=remaining)
            if orchestration_error is None and (
                len(drained) != len(processes)
                or drain_errors
                or any(returncode != 0 for _, _, returncode, _ in drained.values())
            ):
                orchestration_error = AssertionError(
                    "TASK064 concurrent children did not all complete successfully"
                )

        if orchestration_error is not None:
            signal_live_children(signal.SIGTERM)
            close_unstarted_child_pipes()
            term_deadline = time.monotonic() + 2.0
            wait_for_drains(term_deadline)
            wait_for_unstarted_children(term_deadline, final=False)
            signal_live_children(signal.SIGKILL)
        kill_deadline = time.monotonic() + 10.0
        wait_for_drains(kill_deadline)
        wait_for_unstarted_children(kill_deadline, final=True)
        join_deadline = time.monotonic() + 2.0
        for mode, thread in drain_threads.items():
            try:
                thread.join(timeout=max(0.0, join_deadline - time.monotonic()))
            except BaseException as error:
                record_cleanup_error(mode, "join-started-drain", error)
                continue
            try:
                if thread.is_alive():
                    cleanup_errors.append(
                        AssertionError(f"TASK064 child drain remained live for {mode!r}")
                    )
            except BaseException as error:
                record_cleanup_error(mode, "query-started-drain", error)
        for mode, process in processes.items():
            if process_is_live(mode, process):
                cleanup_errors.append(
                    AssertionError(f"TASK064 child process remained live for {mode!r}")
                )
        if any(mode not in drained and mode not in drain_errors for mode in started_drain_modes):
            cleanup_errors.append(
                AssertionError("TASK064 concurrent child cleanup did not reap every process")
            )

        for mode, (_, pycache_prefix, provenance) in prepared.items():
            try:
                provenance_consumed[mode] = harness._close_task064_child_provenance(provenance)
            except BaseException as error:
                cleanup_errors.append(error)
                provenance_consumed[mode] = False
            try:
                marker_exists = provenance.marker_path.exists()
            except BaseException as error:
                record_cleanup_error(mode, "marker-residue-check", error)
            else:
                if marker_exists:
                    cleanup_errors.append(
                        AssertionError(f"TASK064 child marker residue for {mode!r}")
                    )
            try:
                pycache_exists = pycache_prefix.exists()
            except BaseException as error:
                record_cleanup_error(mode, "pycache-residue-check", error)
            else:
                if pycache_exists:
                    cleanup_errors.append(
                        AssertionError(f"TASK064 child pycache residue for {mode!r}")
                    )

    completed: dict[str, tuple[subprocess.CompletedProcess[str], float]] = {}
    for mode, (stdout, stderr, returncode, completed_at) in drained.items():
        process = processes[mode]
        completed[mode] = (
            subprocess.CompletedProcess(
                args=process.args,
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
            ),
            completed_at - process_starts[mode],
        )
    failed_modes = {
        mode: result.returncode for mode, (result, _) in completed.items() if result.returncode != 0
    }
    if (
        orchestration_error is not None
        or drain_errors
        or cleanup_errors
        or set(completed) != set(modes)
        or failed_modes
        or provenance_consumed != dict.fromkeys(modes, True)
    ):
        diagnostics = "\n".join(
            (f"{mode}: rc={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
            for mode, (result, _) in completed.items()
        )
        raise AssertionError(
            "TASK064 concurrent child orchestration failed: "
            f"orchestration={orchestration_error!r}, "
            f"drain={drain_errors!r}, cleanup={cleanup_errors!r}, "
            f"consumed={provenance_consumed!r}, failed={failed_modes!r}\n"
            f"{diagnostics}"
        )
    return {mode: completed[mode] for mode in modes}


@pytest.mark.parametrize(
    ("protocol", "legacy_environment", "ambient_mode", "dispatch_modes"),
    (
        (
            "exec_isolation",
            "WEALTH_TASK064_EXEC_ISOLATION",
            "<node>",
            ("<node>",),
        ),
        (
            "post_return_fixture_replay",
            "WEALTH_TASK064_POST_RETURN_FIXTURE_REPLAY",
            "<node>",
            ("<node>",),
        ),
        (
            "root_latch",
            "WEALTH_TASK064_ROOT_LATCH_PROBE",
            "revocation_flag_write",
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
        ),
        (
            "shared_cleanup",
            "WEALTH_TASK064_SHARED_CLEANUP_PROBE",
            "run",
            ("run",),
        ),
        (
            "report_close",
            "WEALTH_TASK064_REPORT_ROOT_CLOSE_PROBE",
            "staging_close_ambiguity",
            (
                "readback_verified_root_close_ambiguity",
                "staging_close_ambiguity",
                "readback_close_ambiguity",
                "reentrant_root_revocation",
            ),
        ),
    ),
)
def test_bare_legacy_child_modes_are_scrubbed_without_shortening_parent_matrix(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    protocol: str,
    legacy_environment: str,
    ambient_mode: str,
    dispatch_modes: tuple[str, ...],
) -> None:
    node_id = request.node.nodeid
    resolved_ambient_mode = node_id if ambient_mode == "<node>" else ambient_mode
    resolved_dispatch_modes = tuple(
        node_id if mode == "<node>" else mode for mode in dispatch_modes
    )
    monkeypatch.setenv(legacy_environment, resolved_ambient_mode)

    child_mode, remaining_modes = _task064_child_dispatch_plan(
        protocol,
        node_id,
        resolved_dispatch_modes,
    )

    assert child_mode is None
    assert remaining_modes == resolved_dispatch_modes
    assert harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT not in os.environ
    assert all(name not in os.environ for name in harness._TASK064_LEGACY_CHILD_MODE_ENVIRONMENTS)


@pytest.mark.parametrize("raw_envelope", (b"\xff", "\N{LATIN SMALL LETTER E WITH ACUTE}".encode()))
def test_non_ascii_child_provenance_envelopes_fail_closed_as_harness_failures(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    raw_envelope: bytes,
) -> None:
    environment_bytes = os.environb
    envelope_name = harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT.encode("ascii")
    monkeypatch.setitem(environment_bytes, envelope_name, raw_envelope)

    with pytest.raises(harness.HarnessFailure) as rejected:
        harness._authenticate_task064_child_provenance(request.node.nodeid)

    assert rejected.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT


@pytest.mark.parametrize(
    ("protocol", "mode", "allowed_modes"),
    (
        ("exec_isolation", "<node>", ("<node>",)),
        (
            "root_latch",
            "revocation_flag_write",
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
        ),
        ("shared_cleanup", "run", ("run",)),
    ),
    ids=("exec-isolation", "root-latch", "shared-cleanup"),
)
def test_child_provenance_binds_issuer_target_mode_and_is_consumed_once(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    protocol: str,
    mode: str,
    allowed_modes: tuple[str, ...],
) -> None:
    node_id = request.node.nodeid
    resolved_mode = node_id if mode == "<node>" else mode
    resolved_allowed_modes = tuple(
        node_id if candidate == "<node>" else candidate for candidate in allowed_modes
    )
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_child_dispatch_provenance
    ).nonlocals
    active_ticket_context = cast(
        contextvars.ContextVar[object | None],
        claim_closure["active_body_ticket"],
    )
    ticket_records = cast(dict[int, dict[str, object]], claim_closure["ticket_records"])
    active_ticket = active_ticket_context.get()
    if active_ticket is not None and protocol == "exec_isolation":
        active_record = ticket_records[id(active_ticket)]

        def assert_still_activated() -> None:
            assert active_ticket_context.get() is active_ticket
            assert ticket_records[id(active_ticket)] is active_record
            assert active_record["state"] == "ACTIVATED"

        for reserved_name, reserved_value in (
            ("WEALTH_TASK064_EXEC_ISOLATION", node_id),
            (harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT, "{}"),
        ):
            os.environ[reserved_name] = reserved_value
            with pytest.raises(harness.HarnessFailure) as rejected_late_environment:
                harness._claim_task064_child_dispatch_provenance(
                    protocol,
                    node_id,
                    resolved_allowed_modes,
                )
            assert rejected_late_environment.value.code is (
                harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
            )
            assert all(
                name not in os.environ
                for name in (
                    harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT,
                    *harness._TASK064_LEGACY_CHILD_MODE_ENVIRONMENTS,
                )
            )
            assert_still_activated()

        wrong_node_id = f"{node_id}::wrong-node"
        for wrong_protocol, wrong_node, wrong_modes in (
            ("shared_cleanup", node_id, ("run",)),
            (protocol, wrong_node_id, (wrong_node_id,)),
            (protocol, node_id, (wrong_node_id,)),
        ):
            with pytest.raises(harness.HarnessFailure) as rejected_binding:
                harness._claim_task064_child_dispatch_provenance(
                    wrong_protocol,
                    wrong_node,
                    wrong_modes,
                )
            assert rejected_binding.value.code is (
                harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
            )
            assert_still_activated()

        for isolated_context in (contextvars.Context(), contextvars.copy_context()):
            with pytest.raises(harness.HarnessFailure) as rejected_context:
                isolated_context.run(
                    harness._claim_task064_child_dispatch_provenance,
                    protocol,
                    node_id,
                    resolved_allowed_modes,
                )
            assert rejected_context.value.code is (
                harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
            )
            assert_still_activated()

        thread_errors: list[harness.HarnessFailure] = []

        def claim_in_thread() -> None:
            try:
                harness._claim_task064_child_dispatch_provenance(
                    protocol,
                    node_id,
                    resolved_allowed_modes,
                )
            except harness.HarnessFailure as error:
                thread_errors.append(error)

        claim_thread = threading.Thread(target=claim_in_thread)
        claim_thread.start()
        claim_thread.join(timeout=10)
        assert not claim_thread.is_alive()
        assert len(thread_errors) == 1
        assert thread_errors[0].code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        assert_still_activated()

        result_read, result_write = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(result_read)
            child_result = b"F"
            try:
                try:
                    harness._claim_task064_child_dispatch_provenance(
                        protocol,
                        node_id,
                        resolved_allowed_modes,
                    )
                except harness.HarnessFailure as error:
                    if error.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT:
                        child_result = b"P"
            finally:
                with contextlib.suppress(OSError):
                    os.write(result_write, child_result)
                with contextlib.suppress(OSError):
                    os.close(result_write)
                os._exit(0)
        os.close(result_write)
        os.set_blocking(result_read, False)
        result_selector = selectors.DefaultSelector()
        result_selector.register(result_read, selectors.EVENT_READ)
        child_reaped = False
        try:
            ready = result_selector.select(timeout=10)
            assert ready
            assert os.read(result_read, 1) == b"P"
            reap_deadline = time.monotonic() + 10
            while True:
                waited_pid, child_status = os.waitpid(child_pid, os.WNOHANG)
                if waited_pid == child_pid:
                    child_reaped = True
                    break
                assert time.monotonic() < reap_deadline
                time.sleep(0.01)
        finally:
            result_selector.close()
            os.close(result_read)
            if not child_reaped:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(child_pid, signal.SIGKILL)
                with contextlib.suppress(ChildProcessError):
                    os.waitpid(child_pid, 0)
        assert os.WIFEXITED(child_status)
        assert os.WEXITSTATUS(child_status) == 0
        assert_still_activated()

    child_mode, remaining_modes = _task064_child_dispatch_plan(
        protocol,
        node_id,
        resolved_allowed_modes,
    )
    if child_mode is not None:
        assert child_mode == resolved_mode
        assert remaining_modes == ()
        with pytest.raises(harness.HarnessFailure) as replayed_claim:
            _task064_child_dispatch_plan(protocol, node_id, resolved_allowed_modes)
        assert replayed_claim.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        return
    assert remaining_modes == resolved_allowed_modes
    inventory_before_invalid_mode = tuple(tmp_path.iterdir())
    with pytest.raises(harness.HarnessFailure) as invalid_mode:
        harness._issue_task064_child_provenance(
            tmp_path,
            protocol,
            node_id,
            node_id,
            "task064-wrong-mode",
        )
    assert invalid_mode.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    assert tuple(tmp_path.iterdir()) == inventory_before_invalid_mode
    completed, _ = _run_task064_pytest_child(
        tmp_path,
        protocol=protocol,
        issuer_node_id=node_id,
        target_node_id=node_id,
        mode=resolved_mode,
        pycache_label=f"task064-child-provenance-{protocol}-pycache",
        timeout_seconds=180,
    )
    assert completed.returncode == 0, (
        f"TASK064 fixture-order provenance probe failed for {protocol!r}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def test_post_return_ticket_is_claimed_locally_once(
    request: pytest.FixtureRequest,
) -> None:
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_post_return_child_provenance
    ).nonlocals
    active_ticket_context = cast(
        contextvars.ContextVar[object | None],
        claim_closure["active_body_ticket"],
    )
    active_ticket = active_ticket_context.get()
    exact_ticket_record = cast(
        FunctionType,
        claim_closure["exact_ticket_record"],
    )
    exact_record_closure = inspect.getclosurevars(exact_ticket_record).nonlocals
    ticket_records = cast(
        dict[int, dict[str, object]],
        exact_record_closure["ticket_records"],
    )
    matching_records = tuple(
        (ticket_key, record)
        for ticket_key, record in ticket_records.items()
        if record["owner_process_id"] == os.getpid() and record["node_id"] == request.node.nodeid
    )
    if active_ticket is None:
        assert not matching_records
        return
    assert type(active_ticket) is harness._Task064ChildProvenanceTicket
    assert len(matching_records) == 1
    ticket_key, record = matching_records[0]
    assert ticket_key == id(active_ticket)
    assert ticket_records[id(active_ticket)] is record
    assert record["ticket"] is active_ticket
    assert exact_ticket_record(active_ticket, request.node.nodeid) is record
    assert record["owner_process_id"] == os.getpid()
    assert record["node_id"] == request.node.nodeid
    assert record["protocol"] == "post_return_fixture_replay"
    assert record["mode"] == request.node.nodeid
    assert record["state"] == "CLAIMED"
    ticket = active_ticket
    for invocation_context in (None, contextvars.Context(), contextvars.copy_context()):
        with pytest.raises(harness.HarnessFailure) as replayed_claim:
            if invocation_context is None:
                harness._claim_task064_post_return_child_provenance(
                    ticket,
                    request.node.nodeid,
                )
            else:
                invocation_context.run(
                    harness._claim_task064_post_return_child_provenance,
                    ticket,
                    request.node.nodeid,
                )
        assert replayed_claim.value.code is (harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        assert record["state"] == "CLAIMED"


def test_unclaimed_authenticated_child_ticket_is_terminalized_at_teardown(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_child_dispatch_provenance
    ).nonlocals
    active_ticket_context = cast(
        contextvars.ContextVar[object | None],
        claim_closure["active_body_ticket"],
    )
    active_ticket = active_ticket_context.get()
    if active_ticket is not None:
        ticket_records = cast(dict[int, dict[str, object]], claim_closure["ticket_records"])
        record = ticket_records[id(active_ticket)]
        assert record["node_id"] == request.node.nodeid
        assert record["protocol"] == "exec_isolation"
        assert record["state"] == "ACTIVATED"

        class UnclaimedTerminalProbe:
            checked = False

            @pytest.hookimpl(trylast=True)
            def pytest_sessionfinish(
                self,
                session: pytest.Session,
                exitstatus: int,
            ) -> None:
                del session, exitstatus
                cancel_closure = inspect.getclosurevars(
                    harness._cancel_task064_child_provenance
                ).nonlocals
                terminalize_closure = inspect.getclosurevars(
                    cancel_closure["terminalize_fixture_ticket"]
                ).nonlocals
                remaining = cast(
                    dict[int, dict[str, object]],
                    terminalize_closure["ticket_records"],
                )
                terminal = cast(
                    dict[int, tuple[object, int, str, str]],
                    terminalize_closure["terminal_ticket_records"],
                )
                assert remaining == {}
                assert terminalize_closure["active_body_ticket"].get() is None
                assert len(terminal) == 1
                terminal_ticket, owner_pid, node_id, state = next(iter(terminal.values()))
                assert terminal_ticket is active_ticket
                assert owner_pid == os.getpid()
                assert node_id == request.node.nodeid
                assert state == "UNCLAIMED"
                self.checked = True
                print("TASK064_UNCLAIMED_TICKET_TERMINALIZED")

        probe = UnclaimedTerminalProbe()
        request.config.pluginmanager.register(
            probe,
            name="task064-unclaimed-ticket-terminal-probe",
        )
        return

    completed, _ = _run_task064_pytest_child(
        tmp_path,
        protocol="exec_isolation",
        issuer_node_id=request.node.nodeid,
        target_node_id=request.node.nodeid,
        mode=request.node.nodeid,
        pycache_label="task064-unclaimed-ticket-pycache",
        timeout_seconds=180,
    )
    assert completed.returncode != 0
    assert "TASK064_UNCLAIMED_TICKET_TERMINALIZED" in completed.stdout


def test_authenticated_ticket_cleanup_is_total_across_fixture_failures(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    failure_environment = "WEALTH_TASK064_TICKET_CLEANUP_TEST"
    failure_mode = os.environ.get(failure_environment)
    if failure_mode is not None:
        assert failure_mode in {"body-failure", "teardown-failure"}
        child_mode, remaining_modes = _task064_child_dispatch_plan(
            "exec_isolation",
            request.node.nodeid,
            (request.node.nodeid,),
        )
        assert child_mode == request.node.nodeid
        assert remaining_modes == ()
        if failure_mode == "body-failure":
            raise RuntimeError("injected TASK064 authenticated body failure")
        return

    repository_root = Path(__file__).resolve().parents[2]
    failure_modes = (
        "begin",
        "mktemp-0",
        "mktemp-1",
        "mktemp-2",
        "mktemp-3",
        "scope-construction",
        "scope-entry",
        "body-failure",
        "teardown-failure",
    )
    for mode in failure_modes:
        environment, pycache_prefix = _fresh_subprocess_pycache_environment(
            tmp_path,
            label=f"task064-ticket-cleanup-{mode}-pycache",
        )
        provenance = harness._issue_task064_child_provenance(
            tmp_path,
            "exec_isolation",
            request.node.nodeid,
            request.node.nodeid,
            request.node.nodeid,
        )
        environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT] = provenance.envelope
        environment[failure_environment] = mode
        program = textwrap.dedent(
            f"""
            import inspect
            import os
            import pytest
            import _pytest.tmpdir as pytest_tmpdir
            import tests.support.continuous_public_trade_stream_sqlite_harness as harness

            target_node_id = {request.node.nodeid!r}
            failure_mode = {mode!r}

            class CleanupProbe:
                def __init__(self):
                    self.checked = False
                    self.real_make_numbered_dir = pytest_tmpdir.make_numbered_dir
                    self.make_numbered_calls = 0

                @pytest.hookimpl(tryfirst=True)
                def pytest_configure(self, config):
                    del config
                    if failure_mode == "begin":
                        harness._arm_pytest_root_authority_fault("poison_mmap_probe")
                    elif failure_mode.startswith("mktemp-"):
                        target_prefix = (
                            "task064-secondary-"
                            + failure_mode.rsplit("-", 1)[1]
                        )

                        def fail_exact_mktemp(*args, **kwargs):
                            self.make_numbered_calls += 1
                            prefix = kwargs.get(
                                "prefix",
                                args[1] if len(args) > 1 else None,
                            )
                            if isinstance(prefix, str) and prefix.startswith(target_prefix):
                                raise OSError("injected TASK064 mktemp failure")
                            return self.real_make_numbered_dir(*args, **kwargs)

                        pytest_tmpdir.make_numbered_dir = fail_exact_mktemp
                    elif failure_mode == "scope-construction":
                        harness._arm_pytest_root_authority_fault(
                            "permit_scope_construction"
                        )
                    elif failure_mode == "scope-entry":
                        harness._arm_pytest_root_authority_fault("permit_scope_entry")
                    elif failure_mode == "teardown-failure":
                        harness._arm_pytest_root_authority_fault("revocation_flag_write")

                @pytest.hookimpl(trylast=True)
                def pytest_sessionfinish(self, session, exitstatus):
                    del session, exitstatus
                    pytest_tmpdir.make_numbered_dir = self.real_make_numbered_dir
                    cancel_closure = inspect.getclosurevars(
                        harness._cancel_task064_child_provenance
                    ).nonlocals
                    terminalize_closure = inspect.getclosurevars(
                        cancel_closure["terminalize_fixture_ticket"]
                    ).nonlocals
                    assert terminalize_closure["ticket_records"] == {{}}
                    assert terminalize_closure["active_body_ticket"].get() is None
                    terminal = terminalize_closure["terminal_ticket_records"]
                    assert len(terminal) == 1
                    ticket, owner_pid, node_id, state = next(iter(terminal.values()))
                    assert ticket is not None
                    assert owner_pid == os.getpid()
                    assert node_id == target_node_id
                    assert state == "CANCELLED"
                    root_closure = inspect.getclosurevars(
                        harness._cancel_pytest_root_registration
                    ).nonlocals
                    assert root_closure["active_node_lifecycle"] is None
                    self.checked = True
                    print(
                        "TASK064_TICKET_CLEANUP_OK:"
                        + failure_mode
                        + ":"
                        + state
                    )

            probe = CleanupProbe()
            exit_code = pytest.main(
                ["-q", "-s", "-p", "no:cacheprovider", target_node_id],
                plugins=[probe],
            )
            assert exit_code != pytest.ExitCode.OK
            assert probe.checked
            """
        )
        try:
            completed = subprocess.run(
                (sys.executable, "-c", program),
                cwd=repository_root,
                env=environment,
                pass_fds=(provenance.descriptor,),
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
        finally:
            packet_consumed = harness._close_task064_child_provenance(provenance)
        assert packet_consumed
        assert not pycache_prefix.exists()
        assert completed.returncode == 0, (
            f"authenticated ticket cleanup probe failed for {mode!r}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
        assert f"TASK064_TICKET_CLEANUP_OK:{mode}:CANCELLED" in completed.stdout


@pytest.mark.parametrize("output_limit_failure", (False, True))
def test_concurrent_report_close_children_preissue_drain_and_cleanup_causally(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output_limit_failure: bool,
) -> None:
    modes = (
        "readback_verified_root_close_ambiguity",
        "staging_close_ambiguity",
        "readback_close_ambiguity",
        "reentrant_root_revocation",
    )
    fake_capability = harness._Task064PublishedReportArtifactCapability(_authority_nonce=b"f" * 32)
    main_thread = threading.get_ident()
    issued: list[str] = []
    closed: list[str] = []
    launched: list[str] = []
    signaled: list[tuple[str, int]] = []
    prefixes: list[Path] = []
    processes_by_pid: dict[int, FakeProcess] = {}
    overflow_descriptors: set[int] = set()
    overflow_emitted: set[int] = set()

    class FakeProvenance:
        def __init__(self, mode: str, descriptor: int) -> None:
            self.mode = mode
            self.envelope = f"task064-envelope:{mode}"
            self.descriptor = descriptor
            self.artifact_descriptor = descriptor + 100
            self.marker_path = tmp_path / f".task064-fake-marker-{mode}.json"

    class FakeProcess:
        def __init__(
            self,
            args: Sequence[str],
            **kwargs: object,
        ) -> None:
            environment = cast(dict[str, str], kwargs["env"])
            envelope = environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT]
            mode = envelope.removeprefix("task064-envelope:")
            assert issued == list(modes)
            assert mode in modes
            assert mode not in launched
            assert kwargs["pass_fds"] == (
                500 + modes.index(mode),
                600 + modes.index(mode),
            )
            assert kwargs["start_new_session"] is True
            assert kwargs["stdout"] is subprocess.PIPE
            assert kwargs["stderr"] is subprocess.PIPE
            self.mode = mode
            self.args = args
            self.pid = 10_000 + modes.index(mode)
            self.returncode: int | None = None
            stdout_read, self._stdout_write = os.pipe()
            stderr_read, self._stderr_write = os.pipe()
            self.stdout = os.fdopen(stdout_read, "rb", buffering=0)
            self.stderr = os.fdopen(stderr_read, "rb", buffering=0)
            self._finished = threading.Event()
            processes_by_pid[self.pid] = self
            launched.append(mode)
            if output_limit_failure:
                if mode == modes[0]:
                    os.write(self._stdout_write, b"x")
                    overflow_descriptors.add(self.stdout.fileno())
            else:
                os.write(self._stdout_write, f"stdout:{mode}".encode())
                os.write(self._stderr_write, f"stderr:{mode}".encode())
                self._finish(0)

        def _finish(self, returncode: int) -> None:
            if self.returncode is not None:
                return
            self.returncode = returncode
            for attribute in ("_stdout_write", "_stderr_write"):
                descriptor = cast(int, getattr(self, attribute))
                if descriptor >= 0:
                    os.close(descriptor)
                    setattr(self, attribute, -1)
            self._finished.set()

        def poll(self) -> int | None:
            return self.returncode

        def wait(self, timeout: float | None = None) -> int:
            if not self._finished.wait(timeout=timeout):
                raise TimeoutError("fake TASK064 child was not terminated")
            assert self.returncode is not None
            return self.returncode

        def terminate(self) -> None:
            self._finish(-int(signal.SIGTERM))

        def kill(self) -> None:
            self._finish(-int(signal.SIGKILL))

    def fake_environment(pycache_root: Path, *, label: str) -> tuple[dict[str, str], Path]:
        prefix = pycache_root / label
        prefixes.append(prefix)
        return {}, prefix

    def fake_issue(
        _pytest_root: Path,
        protocol: str,
        issuer_node_id: str,
        target_node_id: str,
        mode: str,
        *,
        report_artifact_capability: harness._Task064PublishedReportArtifactCapability,
        report_deadline_ns: int,
    ) -> FakeProvenance:
        assert threading.get_ident() == main_thread
        assert protocol == "report_close"
        assert issuer_node_id == target_node_id == "tests/fake.py::test_report"
        assert report_artifact_capability is fake_capability
        assert report_deadline_ns > time.monotonic_ns()
        issued.append(mode)
        return FakeProvenance(mode, 500 + modes.index(mode))

    def fake_close(provenance: FakeProvenance) -> bool:
        assert threading.get_ident() == main_thread
        closed.append(provenance.mode)
        return not output_limit_failure

    def fake_spawn(
        provenance: FakeProvenance,
        *,
        pycache_prefix: Path,
    ) -> FakeProcess:
        assert pycache_prefix in prefixes
        return FakeProcess(
            ("task064-fake-child",),
            env={harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT: provenance.envelope},
            pass_fds=(provenance.descriptor, provenance.artifact_descriptor),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

    def fake_killpg(process_id: int, signal_number: int) -> None:
        process = processes_by_pid[process_id]
        signaled.append((process.mode, signal_number))
        process._finish(-signal_number)

    real_read = os.read

    def bounded_fake_read(descriptor: int, size: int) -> bytes:
        if descriptor in overflow_descriptors and descriptor not in overflow_emitted:
            overflow_emitted.add(descriptor)
            return b"x" * 1_048_577
        return real_read(descriptor, size)

    monkeypatch.setattr(
        sys.modules[__name__],
        "_fresh_subprocess_pycache_environment",
        fake_environment,
    )
    monkeypatch.setattr(harness, "_issue_task064_child_provenance", fake_issue)
    monkeypatch.setattr(harness, "_spawn_task064_authenticated_child", fake_spawn)
    monkeypatch.setattr(harness, "_close_task064_child_provenance", fake_close)
    monkeypatch.setattr(os, "killpg", fake_killpg)
    monkeypatch.setattr(os, "read", bounded_fake_read)

    def invoke() -> dict[str, tuple[subprocess.CompletedProcess[str], float]]:
        return _run_task064_pytest_children_concurrently(
            tmp_path,
            protocol="report_close",
            issuer_node_id="tests/fake.py::test_report",
            target_node_id="tests/fake.py::test_report",
            modes=modes,
            pycache_label="task064-fake-concurrent-pycache",
            timeout_seconds=2.0,
            report_artifact_capability=fake_capability,
        )

    if output_limit_failure:
        with pytest.raises(AssertionError) as rejected:
            invoke()
        assert "byte ceiling" in str(rejected.value)
        assert {mode for mode, _ in signaled} == set(modes)
        assert all(signal_number == signal.SIGTERM for _, signal_number in signaled)
        assert overflow_emitted
    else:
        results = invoke()
        assert tuple(results) == modes
        assert all(
            result.returncode == 0
            and result.stdout == f"stdout:{mode}"
            and result.stderr == f"stderr:{mode}"
            for mode, (result, _) in results.items()
        )
        assert signaled == []
    assert issued == launched == closed == list(modes)
    assert len(set(prefixes)) == len(modes)
    assert all(process.poll() is not None for process in processes_by_pid.values())


@pytest.mark.parametrize(
    "thread_failure",
    ("constructor", "partial-start"),
    ids=("constructor-failure", "partial-start-failure"),
)
def test_concurrent_child_drain_start_failures_cannot_bypass_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    thread_failure: str,
) -> None:
    modes = (
        "readback_verified_root_close_ambiguity",
        "staging_close_ambiguity",
        "readback_close_ambiguity",
        "reentrant_root_revocation",
    )
    fake_capability = harness._Task064PublishedReportArtifactCapability(_authority_nonce=b"g" * 32)
    failing_mode = modes[0] if thread_failure == "constructor" else modes[1]
    failure_label = f"task064-{thread_failure}-failure"
    main_thread = threading.get_ident()
    real_thread_type = threading.Thread
    issued: list[str] = []
    closed: list[str] = []
    launched: list[str] = []
    signaled: list[tuple[str, int]] = []
    constructed_threads: list[str] = []
    start_attempts: list[str] = []
    started_threads: list[str] = []
    joined_threads: list[str] = []
    queried_threads: list[str] = []
    prefixes: dict[str, Path] = {}
    provenances: dict[str, FakeProvenance] = {}
    processes_by_pid: dict[int, FakeProcess] = {}
    wait_threads: dict[str, list[int]] = {mode: [] for mode in modes}
    wait_timeouts: dict[str, list[float | None]] = {mode: [] for mode in modes}

    class FakeProvenance:
        def __init__(self, mode: str, descriptor: int) -> None:
            self.mode = mode
            self.envelope = f"task064-thread-failure-envelope:{mode}"
            self.descriptor = descriptor
            self.artifact_descriptor = descriptor + 100
            self.marker_path = tmp_path / f".task064-thread-failure-{mode}.json"
            self.marker_path.write_text("marker", encoding="utf-8")

    class FakeProcess:
        def __init__(
            self,
            args: Sequence[str],
            **kwargs: object,
        ) -> None:
            environment = cast(dict[str, str], kwargs["env"])
            envelope = environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT]
            mode = envelope.removeprefix("task064-thread-failure-envelope:")
            assert issued == list(modes)
            assert mode in modes
            assert mode not in launched
            assert kwargs["pass_fds"] == (
                700 + modes.index(mode),
                800 + modes.index(mode),
            )
            assert kwargs["start_new_session"] is True
            assert kwargs["stdout"] is subprocess.PIPE
            assert kwargs["stderr"] is subprocess.PIPE
            self.mode = mode
            self.args = args
            self.pid = 20_000 + modes.index(mode)
            self.returncode: int | None = None
            stdout_read, self._stdout_write = os.pipe()
            stderr_read, self._stderr_write = os.pipe()
            self.stdout = os.fdopen(stdout_read, "rb", buffering=0)
            self.stderr = os.fdopen(stderr_read, "rb", buffering=0)
            self._finished = threading.Event()
            os.write(self._stdout_write, f"stdout:{mode}".encode())
            os.write(self._stderr_write, f"stderr:{mode}".encode())
            processes_by_pid[self.pid] = self
            launched.append(mode)

        def _finish(self, returncode: int) -> None:
            if self.returncode is not None:
                return
            self.returncode = returncode
            for attribute in ("_stdout_write", "_stderr_write"):
                descriptor = cast(int, getattr(self, attribute))
                if descriptor >= 0:
                    os.close(descriptor)
                    setattr(self, attribute, -1)
            if self.mode != failing_mode:
                prefixes[self.mode].rmdir()
            self._finished.set()

        def poll(self) -> int | None:
            return self.returncode

        def wait(self, timeout: float | None = None) -> int:
            wait_threads[self.mode].append(threading.get_ident())
            wait_timeouts[self.mode].append(timeout)
            if not self._finished.wait(timeout=timeout):
                raise TimeoutError("fake TASK064 child was not terminated")
            assert self.returncode is not None
            return self.returncode

        def terminate(self) -> None:
            self._finish(-int(signal.SIGTERM))

        def kill(self) -> None:
            self._finish(-int(signal.SIGKILL))

    class TrackingThread:
        def __init__(
            self,
            mode: str,
            *,
            target: Callable[..., object],
            arguments: tuple[object, ...],
            name: str,
            daemon: bool,
        ) -> None:
            self.mode = mode
            self._thread = real_thread_type(
                target=target,
                args=arguments,
                name=name,
                daemon=daemon,
            )

        def start(self) -> None:
            start_attempts.append(self.mode)
            if thread_failure == "partial-start" and self.mode == failing_mode:
                raise RuntimeError(failure_label)
            self._thread.start()
            started_threads.append(self.mode)

        def join(self, timeout: float | None = None) -> None:
            joined_threads.append(self.mode)
            self._thread.join(timeout=timeout)

        def is_alive(self) -> bool:
            queried_threads.append(self.mode)
            return self._thread.is_alive()

    def fake_environment(pycache_root: Path, *, label: str) -> tuple[dict[str, str], Path]:
        mode = modes[len(prefixes)]
        prefix = pycache_root / label
        prefix.mkdir()
        prefixes[mode] = prefix
        return {}, prefix

    def fake_issue(
        _pytest_root: Path,
        protocol: str,
        issuer_node_id: str,
        target_node_id: str,
        mode: str,
        *,
        report_artifact_capability: harness._Task064PublishedReportArtifactCapability,
        report_deadline_ns: int,
    ) -> FakeProvenance:
        assert threading.get_ident() == main_thread
        assert protocol == "report_close"
        assert issuer_node_id == target_node_id == "tests/fake.py::test_report"
        assert report_artifact_capability is fake_capability
        assert report_deadline_ns > time.monotonic_ns()
        issued.append(mode)
        provenance = FakeProvenance(mode, 700 + modes.index(mode))
        provenances[mode] = provenance
        return provenance

    def fake_close(provenance: FakeProvenance) -> bool:
        assert threading.get_ident() == main_thread
        closed.append(provenance.mode)
        if provenance.mode != failing_mode:
            provenance.marker_path.unlink()
        return True

    def fake_spawn(
        provenance: FakeProvenance,
        *,
        pycache_prefix: Path,
    ) -> FakeProcess:
        assert prefixes[provenance.mode] == pycache_prefix
        return FakeProcess(
            ("task064-fake-child",),
            env={harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT: provenance.envelope},
            pass_fds=(provenance.descriptor, provenance.artifact_descriptor),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

    def fake_killpg(process_id: int, signal_number: int) -> None:
        process = processes_by_pid[process_id]
        signaled.append((process.mode, signal_number))
        process._finish(-signal_number)

    def fake_thread(*_args: object, **kwargs: object) -> TrackingThread:
        target = cast(Callable[..., object], kwargs["target"])
        arguments = cast(tuple[object, ...], kwargs["args"])
        mode = cast(str, arguments[0])
        constructed_threads.append(mode)
        if thread_failure == "constructor" and mode == failing_mode:
            raise RuntimeError(failure_label)
        return TrackingThread(
            mode,
            target=target,
            arguments=arguments,
            name=cast(str, kwargs["name"]),
            daemon=cast(bool, kwargs["daemon"]),
        )

    monkeypatch.setattr(
        sys.modules[__name__],
        "_fresh_subprocess_pycache_environment",
        fake_environment,
    )
    monkeypatch.setattr(harness, "_issue_task064_child_provenance", fake_issue)
    monkeypatch.setattr(harness, "_spawn_task064_authenticated_child", fake_spawn)
    monkeypatch.setattr(harness, "_close_task064_child_provenance", fake_close)
    monkeypatch.setattr(os, "killpg", fake_killpg)
    monkeypatch.setattr(threading, "Thread", fake_thread)

    with pytest.raises(AssertionError) as rejected:
        _run_task064_pytest_children_concurrently(
            tmp_path,
            protocol="report_close",
            issuer_node_id="tests/fake.py::test_report",
            target_node_id="tests/fake.py::test_report",
            modes=modes,
            pycache_label="task064-thread-failure-pycache",
            timeout_seconds=2.0,
            report_artifact_capability=fake_capability,
        )

    diagnostics = str(rejected.value)
    expected_started_modes = [mode for mode in modes if mode != failing_mode]
    assert failure_label in diagnostics
    assert f"TASK064 child marker residue for {failing_mode!r}" in diagnostics
    assert f"TASK064 child pycache residue for {failing_mode!r}" in diagnostics
    assert issued == launched == closed == list(modes)
    assert {mode for mode, _ in signaled} == set(modes)
    assert all(signal_number == signal.SIGTERM for _, signal_number in signaled)
    assert started_threads == joined_threads == queried_threads == expected_started_modes
    assert failing_mode not in joined_threads
    assert failing_mode not in queried_threads
    if thread_failure == "constructor":
        assert failing_mode not in start_attempts
    else:
        assert start_attempts == list(modes)
    assert wait_threads[failing_mode]
    assert set(wait_threads[failing_mode]) == {main_thread}
    assert all(
        thread_id != main_thread
        for mode in expected_started_modes
        for thread_id in wait_threads[mode]
    )
    assert all(timeout is None or 0.0 <= timeout <= 10.0 for timeout in wait_timeouts[failing_mode])
    assert all(process.poll() is not None for process in processes_by_pid.values())
    assert all(
        process.stdout.closed and process.stderr.closed for process in processes_by_pid.values()
    )
    assert all(
        not provenance.marker_path.exists()
        for mode, provenance in provenances.items()
        if mode != failing_mode
    )
    assert all(not prefix.exists() for mode, prefix in prefixes.items() if mode != failing_mode)


def _legacy_report_close_orchestration_shape() -> None:
    helper_tree = ast.parse(
        textwrap.dedent(inspect.getsource(_run_task064_pytest_children_concurrently))
    )
    helper = cast(ast.FunctionDef, helper_tree.body[0])
    helper_calls = tuple(node for node in ast.walk(helper) if isinstance(node, ast.Call))
    issue_calls = tuple(
        call
        for call in helper_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "_issue_task064_child_provenance"
    )
    launch_calls = tuple(
        call
        for call in helper_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "Popen"
    )
    thread_calls = tuple(
        call
        for call in helper_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "Thread"
    )
    assert len(issue_calls) == len(launch_calls) == len(thread_calls) == 1
    assert issue_calls[0].lineno < launch_calls[0].lineno < thread_calls[0].lineno
    drain_child = next(
        node
        for node in helper.body
        if isinstance(node, ast.FunctionDef) and node.name == "drain_child"
    )
    assert not {
        node.attr
        for node in ast.walk(drain_child)
        if isinstance(node, ast.Attribute)
        and node.attr
        in {
            "_issue_task064_child_provenance",
            "_close_task064_child_provenance",
            "_lookup_active_pytest_root",
            "_pytest_root_session_owns",
        }
    }
    literal_assignments = {
        target.id: node.value.value
        for node in helper.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance((target := node.targets[0]), ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    assert literal_assignments["maximum_output_stream_bytes"] == 1_048_576
    assert literal_assignments["maximum_output_aggregate_bytes"] == 4_194_304
    assert (
        sum(
            isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id == "os"
            and call.func.attr == "read"
            for call in helper_calls
        )
        == 1
    )

    report_tree = ast.parse(
        textwrap.dedent(
            inspect.getsource(test_finite_typical_workload_measurements_and_sanitized_report)
        )
    )
    report = cast(ast.FunctionDef, report_tree.body[0])
    report_calls = tuple(node for node in ast.walk(report) if isinstance(node, ast.Call))
    assert (
        sum(
            isinstance(call.func, ast.Name)
            and call.func.id == "_run_task064_pytest_children_concurrently"
            for call in report_calls
        )
        == 1
    )
    assert not any(
        isinstance(call.func, ast.Name) and call.func.id == "_run_task064_pytest_child"
        for call in report_calls
    )

    normal_guards = tuple(
        node
        for node in ast.walk(report)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "close_probe_mode"
        and len(node.test.ops) == 1
        and isinstance(node.test.ops[0], ast.Is)
        and len(node.test.comparators) == 1
        and isinstance(node.test.comparators[0], ast.Constant)
        and node.test.comparators[0].value is None
    )
    normal_only_nodes = {
        id(node)
        for guard in normal_guards
        for statement in guard.body
        for node in ast.walk(statement)
    }
    negative_names = {
        "historical_binding_substitution",
        "semantic_substitution",
        "missing_expectation_substitution",
        "same_shape_query_splice",
        "coherent_query_splice",
        "shallow_conflicts",
        "mismatch_second_aggregate_digest",
        "second_pass_mismatch",
    }
    for negative_name in negative_names:
        occurrences = tuple(
            node
            for node in ast.walk(report)
            if (
                (isinstance(node, ast.Name) and node.id == negative_name)
                or (isinstance(node, ast.FunctionDef) and node.name == negative_name)
            )
        )
        assert occurrences
        assert all(id(node) in normal_only_nodes for node in occurrences)
    arm_calls = tuple(
        call
        for call in report_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "_arm_evidence_seal_transition_fault"
    )
    assert len(arm_calls) == 1 and id(arm_calls[0]) in normal_only_nodes

    positive_calls = {
        "collect_backup_restore_evidence",
        "collect_schema_identity_evidence",
        "finalize_bootstrap_path_evidence",
        "collect_runtime_connection_controls_evidence",
        "collect_projection_roundtrip_evidence",
        "finalize_schema_corruption_evidence",
        "finalize_fresh_process_evidence",
        "finalize_atomicity_evidence",
        "finalize_bounded_query_evidence",
        "collect_closed_error_mapping_evidence",
        "collect_generation_copy_evidence",
        "collect_workload_threshold_evidence",
    }
    unconditional_positive_lines: list[int] = []
    for positive_name in positive_calls:
        matches = tuple(
            call
            for call in report_calls
            if isinstance(call.func, ast.Attribute)
            and call.func.attr == positive_name
            and id(call) not in normal_only_nodes
        )
        assert matches
        unconditional_positive_lines.append(min(call.lineno for call in matches))
    unconditional_seals = tuple(
        call
        for call in report_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "seal_generated_evidence_run"
        and id(call) not in normal_only_nodes
    )
    assert len(unconditional_seals) == 1
    assert (
        sum(
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "GeneratedEvidenceAggregate"
            and id(call) not in normal_only_nodes
            for call in report_calls
        )
        == 1
    )
    close_branch = next(
        node
        for node in report.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "close_probe_mode"
        and isinstance(node.test.ops[0], ast.IsNot)
    )
    assert (
        sum(
            isinstance(call.func, ast.Name)
            and call.func.id == "_evidence_report"
            and id(call) not in normal_only_nodes
            and call.lineno < close_branch.lineno
            for call in report_calls
        )
        == 1
    )
    assert max((*unconditional_positive_lines, unconditional_seals[0].lineno)) < (
        close_branch.lineno
    )


def test_report_parent_attestation_is_terminal_and_precedes_authority_data_access() -> None:
    source = Path(harness.__file__).read_text(encoding="utf-8")
    module = ast.parse(source)

    def nested_function(parent: ast.AST, name: str) -> ast.FunctionDef:
        matches = tuple(
            node
            for node in ast.walk(parent)
            if isinstance(node, ast.FunctionDef) and node.name == name
        )
        assert len(matches) == 1
        return matches[0]

    child_builder = nested_function(module, "_build_task064_child_provenance_authority")
    authenticate = nested_function(child_builder, "authenticate_before_fixture")
    authenticate_calls = tuple(
        node for node in ast.walk(authenticate) if isinstance(node, ast.Call)
    )
    parent_attestation = tuple(
        call
        for call in authenticate_calls
        if isinstance(call.func, ast.Name) and call.func.id == "authenticate_report_parent_listener"
    )
    assert len(parent_attestation) == 1
    attestation_line = parent_attestation[0].lineno

    restricted_names = {
        "open_file",
        "published_origin_matches",
        "report_artifact_validator",
        "report_source_fingerprints",
        "schema_fingerprint_provider",
    }
    restricted_lines = {
        call.lineno
        for call in authenticate_calls
        if (isinstance(call.func, ast.Name) and call.func.id in restricted_names)
        or (isinstance(call.func, ast.Attribute) and call.func.attr == "lstat")
        or (
            isinstance(call.func, ast.Name)
            and call.func.id in {"read_file", "stat_file"}
            and call.args
            and isinstance(call.args[0], ast.Name)
            and call.args[0].id == "artifact_descriptor"
        )
    }
    assert restricted_lines
    assert min(restricted_lines) > attestation_line
    sensitive_packet_keys = {
        "artifact_descriptor",
        "artifact_sha256",
        "publication_path",
        "root_path",
        "schema_fingerprint",
        "source_fingerprints",
    }
    sensitive_packet_accesses = tuple(
        node
        for node in ast.walk(authenticate)
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "packet"
        and isinstance(node.slice, ast.Constant)
        and node.slice.value in sensitive_packet_keys
    )
    assert sensitive_packet_accesses
    assert min(node.lineno for node in sensitive_packet_accesses) > attestation_line

    acknowledge = nested_function(child_builder, "acknowledge_registered_child")
    acknowledge_calls = tuple(node for node in ast.walk(acknowledge) if isinstance(node, ast.Call))
    peer_credentials_line = min(
        call.lineno
        for call in acknowledge_calls
        if any(
            isinstance(node, ast.Name) and node.id == "socket_peer_credentials"
            for node in ast.walk(call)
        )
    )
    recvmsg_line = min(
        call.lineno
        for call in acknowledge_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "recvmsg"
    )
    wrong_peer_line = min(
        node.lineno
        for node in ast.walk(acknowledge)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.UnaryOp)
        and isinstance(node.test.op, ast.Not)
        and isinstance(node.test.operand, ast.Name)
        and node.test.operand.id == "expected_peer"
    )
    child_exit_poll_line = min(
        call.lineno
        for call in acknowledge_calls
        if isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "process"
        and call.func.attr == "poll"
    )
    assert child_exit_poll_line < peer_credentials_line < wrong_peer_line < recvmsg_line

    commit_line = min(
        call.lineno
        for call in acknowledge_calls
        if any(isinstance(node, ast.Name) and node.id == "commit" for node in ast.walk(call.func))
    )
    launch_attested_line = next(
        node.lineno
        for node in ast.walk(acknowledge)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Subscript)
        and isinstance(node.targets[0].value, ast.Name)
        and node.targets[0].value.id == "record"
        and isinstance(node.targets[0].slice, ast.Constant)
        and node.targets[0].slice.value == "state"
        and isinstance(node.value, ast.Constant)
        and node.value.value == "ATTESTED"
    )
    attested_send_line = next(
        call.lineno
        for call in acknowledge_calls
        if isinstance(call.func, ast.Name)
        and call.func.id == "send_parent_attestation_response"
        and any(
            keyword.arg == "status"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "ATTESTED"
            for keyword in call.keywords
        )
    )
    acknowledged_close_line = min(
        call.lineno
        for call in acknowledge_calls
        if call.lineno > attested_send_line
        and isinstance(call.func, ast.Name)
        and call.func.id == "close_authority_connection"
    )
    assert commit_line < launch_attested_line < attested_send_line < acknowledged_close_line

    publication_builder = nested_function(
        module,
        "_build_task064_published_report_artifact_authority",
    )
    binder = nested_function(publication_builder, "bind_packet_digest")
    for callback_name in ("attest", "commit"):
        callback = nested_function(binder, callback_name)
        callback_names = {node.id for node in ast.walk(callback) if isinstance(node, ast.Name)}
        assert {
            "capability",
            "deadline_ns",
            "exact_record",
            "mode",
            "read_exact_published_artifact",
            "record_is_current",
        } <= callback_names
    publication_commit = nested_function(binder, "commit")
    publication_commit_reread = max(
        call.lineno
        for call in ast.walk(publication_commit)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "read_exact_published_artifact"
    )
    publication_attested_line = next(
        node.lineno
        for node in ast.walk(publication_commit)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Constant)
        and node.value.value == "ATTESTED"
    )
    assert publication_commit_reread < publication_attested_line

    child_builder_source = ast.get_source_segment(source, child_builder)
    assert child_builder_source is not None
    for obsolete in (
        "authority_descriptor",
        "authority_device",
        "authority_endpoint_is_exact",
        "authority_thread",
        "armed_event",
        "cancel_event",
        "server_closed",
        "socket_pair",
    ):
        assert obsolete not in child_builder_source
    assert "malicious intermediary parent" in child_builder_source
    expected_handshake_lifetime_ns = int(_TASK064_PARENT_HANDSHAKE_TIMEOUT_SECONDS * 1_000_000_000)
    assert (
        "maximum_parent_handshake_lifetime_ns = "
        f"{expected_handshake_lifetime_ns:_}" in child_builder_source
    )

    wrapper_probe_tree = ast.parse(
        textwrap.dedent(inspect.getsource(_launch_task064_wrapper_bypass_probe))
    )
    wrapper_probe = cast(ast.FunctionDef, wrapper_probe_tree.body[0])
    setup_tries = tuple(node for node in wrapper_probe.body if isinstance(node, ast.Try))
    assert len(setup_tries) == 1
    setup_try = setup_tries[0]
    protected_call_names = {
        call.func.attr if isinstance(call.func, ast.Attribute) else call.func.id
        for statement in setup_try.body
        for call in ast.walk(statement)
        if isinstance(call, ast.Call) and isinstance(call.func, (ast.Attribute, ast.Name))
    }
    assert {"mkdir", "write_text", "pipe2", "Popen"} <= protected_call_names
    assert len(setup_try.handlers) == 1
    setup_handler = setup_try.handlers[0]
    assert isinstance(setup_handler.type, ast.Name)
    assert setup_handler.type.id == "BaseException"
    assert any(
        isinstance(node, ast.Raise) and node.exc is None
        for statement in setup_handler.body
        for node in ast.walk(statement)
    )
    cleanup_call_names = tuple(
        call.func.attr if isinstance(call.func, ast.Attribute) else call.func.id
        for statement in setup_handler.body
        for call in ast.walk(statement)
        if isinstance(call, ast.Call) and isinstance(call.func, (ast.Attribute, ast.Name))
    )
    assert cleanup_call_names.count("killpg") == 1
    assert cleanup_call_names.count("unlink") == 1
    assert cleanup_call_names.count("rmdir") == 1
    assert setup_try.finalbody


def test_hostile_probe_spawn_and_thread_start_failures_leave_no_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    marker_path = tmp_path / "task064-probe-spawn-failure-marker.json"
    artifact_path = tmp_path / "task064-probe-spawn-failure-artifact.json"
    marker_path.write_bytes(b"{}")
    artifact_path.write_bytes(b"{}")
    marker_descriptor = os.open(marker_path, os.O_RDWR | os.O_CLOEXEC)
    artifact_descriptor = os.open(artifact_path, os.O_RDONLY | os.O_CLOEXEC)
    marker_details = os.fstat(marker_descriptor)
    root_details = tmp_path.lstat()
    provenance = harness._Task064ChildProvenance(
        envelope="{}",
        descriptor=marker_descriptor,
        artifact_descriptor=artifact_descriptor,
        marker_path=marker_path,
        marker_device=marker_details.st_dev,
        marker_inode=marker_details.st_ino,
        root_path=tmp_path,
        root_device=root_details.st_dev,
        root_inode=root_details.st_ino,
        nonce="0" * 64,
    )

    def injected_setup_failure(stage: str) -> Callable[..., Never]:
        def fail(*_args: object, **_kwargs: object) -> Never:
            raise RuntimeError(f"task064 injected hostile-probe {stage} failure")

        return fail

    def injected_popen_failure(
        stage: str,
        attempts: list[str],
    ) -> Callable[..., Never]:
        def fail(*_args: object, **_kwargs: object) -> Never:
            attempts.append(stage)
            raise RuntimeError(f"task064 injected hostile-probe {stage} failure")

        return fail

    for failure_stage, owner, attribute in (
        ("write_text", Path, "write_text"),
        ("pipe2", os, "pipe2"),
        ("Popen", subprocess, "Popen"),
    ):
        inventory_before_setup = tuple(tmp_path.iterdir())
        descriptors_before_setup = {path.name for path in Path("/proc/self/fd").iterdir()}
        popen_attempts: list[str] = []
        fail_setup = injected_setup_failure(failure_stage)
        fail_popen = injected_popen_failure(failure_stage, popen_attempts)

        with monkeypatch.context() as patch:
            patch.setattr(subprocess, "Popen", fail_popen)
            if owner is not subprocess:
                patch.setattr(owner, attribute, fail_setup)
            with pytest.raises(
                RuntimeError,
                match=rf"injected hostile-probe {failure_stage} failure",
            ):
                _launch_task064_wrapper_bypass_probe(
                    tmp_path,
                    provenance=provenance,
                    target_node_id="tests/fake.py::test_probe",
                    pycache_prefix=tmp_path / "task064-probe-setup-failure-pycache",
                )
        assert popen_attempts == (["Popen"] if failure_stage == "Popen" else [])
        assert tuple(tmp_path.iterdir()) == inventory_before_setup
        assert {path.name for path in Path("/proc/self/fd").iterdir()} == descriptors_before_setup
        assert not tuple(tmp_path.glob("task064-wrapper-bypass-observer-*"))

    os.close(artifact_descriptor)
    os.close(marker_descriptor)
    inventory_before_thread = tuple(tmp_path.iterdir())
    descriptors_before_thread = {path.name for path in Path("/proc/self/fd").iterdir()}

    def fail_thread_start(_thread: threading.Thread) -> Never:
        raise RuntimeError("task064 injected fake-ACK thread start failure")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", fail_thread_start)
        with pytest.raises(RuntimeError, match="injected fake-ACK thread start failure"):
            _launch_task064_synthetic_fake_ack_probe(
                tmp_path,
                target_node_id="tests/fake.py::test_probe",
                mode="staging_close_ambiguity",
                pycache_prefix=tmp_path / "task064-probe-thread-failure-pycache",
            )
    assert tuple(tmp_path.iterdir()) == inventory_before_thread
    assert {path.name for path in Path("/proc/self/fd").iterdir()} == descriptors_before_thread
    assert not tuple(tmp_path.glob("task064-wrapper-bypass-observer-*"))
    assert not tuple(tmp_path.glob(".task064-synthetic-artifact-*"))
    assert not tuple(tmp_path.glob(".task064-child-provenance-*.json"))


def test_report_close_orchestration_keeps_full_positive_evidence_and_parent_negatives() -> None:
    report_tree = ast.parse(
        textwrap.dedent(
            inspect.getsource(test_finite_typical_workload_measurements_and_sanitized_report)
        )
    )
    report = cast(ast.FunctionDef, report_tree.body[0])
    early_child_branch = next(
        node
        for node in report.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "close_probe_mode"
        and isinstance(node.test.ops[0], ast.IsNot)
    )
    child_branch_calls = tuple(
        node
        for statement in early_child_branch.body
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
    )
    assert any(
        isinstance(call.func, ast.Name) and call.func.id == "_run_authenticated_report_close_probe"
        for call in child_branch_calls
    )
    assert any(
        isinstance(node, ast.Return)
        for statement in early_child_branch.body
        for node in ast.walk(statement)
    )
    early_return_line = max(
        node.lineno
        for statement in early_child_branch.body
        for node in ast.walk(statement)
        if isinstance(node, ast.Return)
    )
    report_calls = tuple(node for node in ast.walk(report) if isinstance(node, ast.Call))
    evidence_begin = next(
        call
        for call in report_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "begin_generated_evidence_run"
    )
    fanout = next(
        call
        for call in report_calls
        if isinstance(call.func, ast.Name)
        and call.func.id == "_run_task064_pytest_children_concurrently"
    )
    public_write_lines = tuple(
        call.lineno
        for call in report_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "write_evidence_report"
    )
    artifact_claim = next(
        call
        for call in report_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "_claim_task064_published_report_artifact"
    )
    assert early_return_line < evidence_begin.lineno
    assert public_write_lines and min(public_write_lines) < artifact_claim.lineno < fanout.lineno
    assert any(
        keyword.arg == "report_artifact_capability"
        and isinstance(keyword.value, ast.Name)
        and keyword.value.id == "published_report_artifact"
        for keyword in fanout.keywords
    )

    positive_calls = {
        "collect_backup_restore_evidence",
        "collect_schema_identity_evidence",
        "finalize_bootstrap_path_evidence",
        "collect_runtime_connection_controls_evidence",
        "collect_projection_roundtrip_evidence",
        "finalize_schema_corruption_evidence",
        "finalize_fresh_process_evidence",
        "finalize_atomicity_evidence",
        "finalize_bounded_query_evidence",
        "collect_closed_error_mapping_evidence",
        "collect_generation_copy_evidence",
        "collect_workload_threshold_evidence",
        "seal_generated_evidence_run",
    }
    for positive_name in positive_calls:
        matches = tuple(
            call
            for call in report_calls
            if isinstance(call.func, ast.Attribute) and call.func.attr == positive_name
        )
        assert matches and max(call.lineno for call in matches) < fanout.lineno

    child_tree = ast.parse(
        textwrap.dedent(inspect.getsource(_run_authenticated_report_close_probe))
    )
    forbidden_child_symbols = {
        "_EvidenceLedger",
        "_EvidenceRun",
        "StoreToken",
        "_EvidenceReceipt",
        "GeneratedEvidenceAggregate",
        "begin_generated_evidence_run",
        "bootstrap_store",
        *positive_calls,
    }
    assert not {
        node.attr
        for node in ast.walk(child_tree)
        if isinstance(node, ast.Attribute) and node.attr in forbidden_child_symbols
    }
    assert not {
        node.id
        for node in ast.walk(child_tree)
        if isinstance(node, ast.Name) and node.id in forbidden_child_symbols
    }

    orchestration_tree = ast.parse(
        textwrap.dedent(inspect.getsource(_run_task064_pytest_children_concurrently))
    )
    orchestration_calls = tuple(
        node for node in ast.walk(orchestration_tree) if isinstance(node, ast.Call)
    )
    report_issue = next(
        call
        for call in orchestration_calls
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "_issue_task064_child_provenance"
        and any(keyword.arg == "report_artifact_capability" for keyword in call.keywords)
    )
    assert {keyword.arg for keyword in report_issue.keywords} >= {
        "report_artifact_capability",
        "report_deadline_ns",
    }
    pass_fds = next(
        keyword.value
        for call in orchestration_calls
        if isinstance(call.func, ast.Attribute) and call.func.attr == "Popen"
        for keyword in call.keywords
        if keyword.arg == "pass_fds"
    )
    assert any(
        isinstance(node, ast.Attribute) and node.attr == "artifact_descriptor"
        for node in ast.walk(pass_fds)
    )

    harness_source = Path(harness.__file__).read_text(encoding="utf-8")
    harness_tree = ast.parse(harness_source)
    receipt_authority = next(
        node
        for node in harness_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_build_evidence_receipt_authority"
    )
    issued_receipt_validator = next(
        node
        for node in receipt_authority.body
        if isinstance(node, ast.FunctionDef) and node.name == "validate"
    )
    issued_validator_source = ast.get_source_segment(
        harness_source,
        issued_receipt_validator,
    )
    assert issued_validator_source is not None
    assert issued_validator_source.count("_evidence_payload_digest(") == 1
    for retained_guard in (
        "receipt.evidence is not evidence",
        "gate is not exact_gate",
        "gate.payload is not payload",
        "gate.payload_digest != payload_digest",
        "validate_gate_observation(observation, receipt.run, ordinal)",
        "ledger.observations.get(ordinal) is not observation",
    ):
        assert retained_guard in issued_validator_source

    unbound_receipt_validator = next(
        node
        for node in harness_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_validate_evidence_receipt_unbound"
    )
    unbound_validator_source = ast.get_source_segment(
        harness_source,
        unbound_receipt_validator,
    )
    assert unbound_validator_source is not None
    assert "_evidence_payload_digest(" not in unbound_validator_source
    for retained_exact_mapping in (
        "receipt.evidence is not evidence",
        "receipt_gate.gate != name",
        "receipt_gate.payload is not payload",
    ):
        assert retained_exact_mapping in unbound_validator_source

    gate_payload_helper = next(
        node
        for node in harness_tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_generated_gate_payloads"
    )
    gate_payload_return = next(
        node for node in gate_payload_helper.body if isinstance(node, ast.Return)
    )
    assert isinstance(gate_payload_return.value, ast.Tuple)
    observed_gate_mapping = tuple(
        (
            cast(ast.Constant, item.elts[0]).value,
            cast(ast.Attribute, item.elts[1]).attr,
        )
        for item in gate_payload_return.value.elts
        if isinstance(item, ast.Tuple) and len(item.elts) == 2
    )
    assert observed_gate_mapping == tuple((gate, gate) for gate in harness.GENERATED_EVIDENCE_GATES)


def test_fresh_subprocess_pycache_prefix_excludes_valid_stale_pytest_rewrite_pyc(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module_root = tmp_path / "task064-stale-pytest-rewrite-module"
    module_root.mkdir(mode=0o700)
    source_path = module_root / "test_task064_stale_rewrite_probe.py"
    stale_source = (
        b"def test_task064_marker() -> None:\n    print('TASK064_STALE')\n    assert True\n"
    )
    fresh_source = (
        b"def test_task064_marker() -> None:\n    print('TASK064_FRESH')\n    assert True\n"
    )
    assert len(stale_source) == len(fresh_source)
    source_path.write_bytes(stale_source)
    source_details = source_path.stat()
    pytest_command = (
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-s",
        "--assert=rewrite",
        "-p",
        "no:cacheprovider",
        source_path.name,
    )
    seed_environment = os.environ.copy()
    seed_environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    seed_environment.pop("PYTHONDONTWRITEBYTECODE", None)
    seed_environment.pop("PYTHONPYCACHEPREFIX", None)
    seeded = subprocess.run(
        pytest_command,
        cwd=module_root,
        env=seed_environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert seeded.returncode == 0
    assert "TASK064_STALE" in seeded.stdout
    assert "TASK064_FRESH" not in seeded.stdout
    assert seeded.stderr == ""
    rewrite_caches = tuple(
        (module_root / "__pycache__").glob(
            f"{source_path.stem}.*-pytest-*.pyc",
        )
    )
    assert len(rewrite_caches) == 1
    stale_bytecode = rewrite_caches[0]
    stale_bytecode_raw = stale_bytecode.read_bytes()
    assert stale_bytecode_raw[:4] == importlib.util.MAGIC_NUMBER
    assert int.from_bytes(stale_bytecode_raw[4:8], "little") == 0
    assert (
        int.from_bytes(stale_bytecode_raw[8:12], "little")
        == (source_details.st_mtime_ns // 1_000_000_000) & 0xFFFF_FFFF
    )
    assert int.from_bytes(stale_bytecode_raw[12:16], "little") == len(stale_source)
    stale_bytecode_digest = hashlib.sha256(stale_bytecode_raw).hexdigest()

    source_path.write_bytes(fresh_source)
    os.utime(
        source_path,
        ns=(source_details.st_atime_ns, source_details.st_mtime_ns),
    )
    assert source_path.stat().st_size == source_details.st_size
    assert (source_path.stat().st_mtime_ns // 1_000_000_000) & 0xFFFF_FFFF == int.from_bytes(
        stale_bytecode_raw[8:12],
        "little",
    )
    unisolated_environment = os.environ.copy()
    unisolated_environment["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    unisolated_environment["PYTHONDONTWRITEBYTECODE"] = "1"
    unisolated_environment.pop("PYTHONPYCACHEPREFIX", None)

    monkeypatch.setenv("PYTHONPYCACHEPREFIX", str(stale_bytecode.parent))
    isolated_environment, fresh_prefix = _fresh_subprocess_pycache_environment(
        tmp_path,
        label="task064-hermetic-pycache",
    )
    assert os.environ["PYTHONPYCACHEPREFIX"] == str(stale_bytecode.parent)
    assert isolated_environment["PYTHONPYCACHEPREFIX"] == str(fresh_prefix)
    for environment, expected, rejected in (
        (unisolated_environment, "TASK064_STALE", "TASK064_FRESH"),
        (isolated_environment, "TASK064_FRESH", "TASK064_STALE"),
        (isolated_environment, "TASK064_FRESH", "TASK064_STALE"),
        (unisolated_environment, "TASK064_STALE", "TASK064_FRESH"),
    ):
        result = subprocess.run(
            pytest_command,
            cwd=module_root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0
        assert expected in result.stdout
        assert rejected not in result.stdout
        assert result.stderr == ""
        assert not fresh_prefix.exists()
        assert hashlib.sha256(stale_bytecode.read_bytes()).hexdigest() == (stale_bytecode_digest)

    collision_prefix = tmp_path / "task064-collision-pycache"
    collision_prefix.mkdir(mode=0o700)
    with pytest.raises(AssertionError, match="pycache"):
        _fresh_subprocess_pycache_environment(
            tmp_path,
            label=collision_prefix.name,
        )
    assert os.environ["PYTHONPYCACHEPREFIX"] == str(stale_bytecode.parent)
    assert not fresh_prefix.exists()
    assert stale_bytecode.is_file()


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


def _child_lifecycle_transition(
    prior: ContinuousPublicTradeStreamStoredHistoryEntryV1,
    policy: ContinuousPublicTradePolicy,
    kind: ContinuousPublicTradeTransitionKind,
    *,
    child_job_id: UUID,
    child_policy_fingerprint: str,
) -> ContinuousPublicTradeStreamStoredTransitionV1:
    assert kind in (
        ContinuousPublicTradeTransitionKind.ATTACH,
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
    )
    prior_envelope = prior.successor_envelope.envelope
    prior_checkpoint = prior_envelope.checkpoint
    successor_version = prior_checkpoint.version + 1
    recorded_at = RECORDED_AT + timedelta(microseconds=successor_version)
    if kind is ContinuousPublicTradeTransitionKind.ATTACH:
        plan, payload = finalize_continuous_public_trade_attachment(
            prior_checkpoint,
            policy,
            candidate_job_id=child_job_id,
            child_policy_fingerprint=child_policy_fingerprint,
            now=recorded_at,
        )
        assert plan.attachment is not None
        assert payload is not None
        successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
            prior_checkpoint.model_dump()
            | {
                "attachment": plan.attachment,
                "version": successor_version,
            }
        )
        successor_value = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=successor_checkpoint,
            child_creation_payload=payload,
        )
    else:
        prior_attachment = prior_checkpoint.attachment
        prior_payload = prior_envelope.child_creation_payload
        assert prior_attachment is not None
        assert prior_payload is not None
        assert prior_attachment.job_id == child_job_id
        assert prior_payload.child_checkpoint.policy_fingerprint == child_policy_fingerprint
        successor_checkpoint = ContinuousPublicTradeStreamCheckpoint.model_validate(
            prior_checkpoint.model_dump()
            | {
                "cursor_epoch_ms": prior_attachment.window_end_epoch_ms,
                "attachment": None,
                "version": successor_version,
            }
        )
        successor_value = ContinuousPublicTradeStreamEnvelopeV1(
            checkpoint=successor_checkpoint,
        )
    successor = _stored_envelope(successor_value)
    successor_attachment = successor_checkpoint.attachment
    successor_payload = successor_value.child_creation_payload
    authority_scope = ContinuousPublicTradeEvidenceScopeV1(
        evidence_kind=ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
        stream_id=prior_checkpoint.stream_id,
        transition_kind=kind,
        prior_version=prior_checkpoint.version,
        prior_envelope_digest=prior.successor_envelope.envelope_digest,
        prior_history_root=prior.history_root,
        successor_version=successor_version,
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
        reason_code=None,
        stream_policy=None,
    )
    completion_scope: ContinuousPublicTradeEvidenceScopeV1 | None = None
    if kind is ContinuousPublicTradeTransitionKind.CHILD_COMPLETED:
        prior_attachment = prior_checkpoint.attachment
        prior_payload = prior_envelope.child_creation_payload
        assert prior_attachment is not None
        assert prior_payload is not None
        completion_scope = ContinuousPublicTradeEvidenceScopeV1(
            evidence_kind=ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
            stream_id=prior_checkpoint.stream_id,
            transition_kind=kind,
            prior_version=prior_checkpoint.version,
            prior_envelope_digest=prior.successor_envelope.envelope_digest,
            prior_history_root=prior.history_root,
            successor_version=successor_version,
            successor_envelope_digest=successor.envelope_digest,
            child_job_id=prior_attachment.job_id,
            child_policy_fingerprint=prior_payload.child_checkpoint.policy_fingerprint,
            child_creation_fingerprint=prior_attachment.creation_fingerprint,
            reason_code=None,
            stream_policy=None,
        )
    record = ContinuousPublicTradeStreamTransitionRecordV1(
        stream_id=prior_checkpoint.stream_id,
        prior_version=prior_checkpoint.version,
        successor_version=successor_version,
        transition_kind=kind,
        prior_history_root=prior.history_root,
        prior_envelope_digest=prior.successor_envelope.envelope_digest,
        successor_envelope_hex=successor.canonical_bytes.hex(),
        successor_envelope_digest=successor.envelope_digest,
        reason_code=None,
        transition_authority_reference=_reference(
            ContinuousPublicTradeEvidenceKind.STREAM_TRANSITION_AUTHORITY,
            evidence_scope_digest(authority_scope),
            suffix=f"{kind.value.lower()}-{successor_version}",
            recorded_at=recorded_at,
        ),
        child_completion_reference=(
            None
            if completion_scope is None
            else _reference(
                ContinuousPublicTradeEvidenceKind.CHILD_COMPLETION,
                evidence_scope_digest(completion_scope),
                suffix=f"child-completed-{successor_version}",
                recorded_at=recorded_at,
            )
        ),
        recorded_at=recorded_at,
    )
    stored = ContinuousPublicTradeStreamStoredTransitionV1(
        record=record,
        canonical_bytes=encode_stream_transition_record(record),
        record_digest=stream_transition_digest(record),
        successor_envelope=successor,
        history_root=next_stream_history_root(prior.history_root, record),
        transition_authority_scope=authority_scope,
        child_completion_scope=completion_scope,
    )
    validate_stream_transition_link(
        prior_envelope,
        record,
        policy=policy,
        prior_history_root=prior.history_root,
        prior_recorded_at=prior.record.recorded_at,
        transition_authority_scope=authority_scope,
        child_completion_scope=completion_scope,
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
    fixture_capability: harness._PytestRootCapability,
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

    scoped_root = fixture_capability.roots[1]
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
    harness._revoke_fixture_root(fixture_capability, scoped_root)
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

    registered_root = fixture_capability.roots[2]
    retained_root = registered_root.with_name(f"{registered_root.name}-retained")
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
    harness._revoke_fixture_root(fixture_capability, registered_root)

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
    with contextlib.ExitStack() as replacement_descriptors:
        retained_descriptor = os.open(
            replaced_token._database_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        replacement_descriptors.callback(os.close, retained_descriptor)
        replaced_token._database_path.unlink()
        replacement_descriptor = os.open(
            replaced_token._database_path,
            os.O_CREAT
            | os.O_EXCL
            | os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        replacement_descriptors.callback(os.close, replacement_descriptor)
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
    conflict_transition = _retain(
        mixed_creation,
        mixed_policy,
        reason=(
            "report-atomicity-substituted-mixed-v2-conflict"
            if substitute_semantics
            else "report-atomicity-mixed-v2-conflict"
        ),
    )
    conflict_expectation = _expectation(mixed_policy, mixed_creation)
    conflict_command = ContinuousPublicTradeStreamCompareAndSwapCommandV1(
        expectation=conflict_expectation,
        expected_version=conflict_transition.record.prior_version,
        expected_envelope_digest=conflict_transition.record.prior_envelope_digest,
        expected_history_root=conflict_transition.record.prior_history_root,
        transition=conflict_transition,
    )
    exact_conflict_command = (
        ContinuousPublicTradeStreamCompareAndSwapCommandV1.revalidate_at_boundary(conflict_command)
    )
    conflicting_compare_and_swap = harness.compare_and_swap_stream(
        token,
        exact_conflict_command.transition,
        expectation=exact_conflict_command.expectation,
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
    child_policy_fingerprint: str | None = None,
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
        effective_child_policy_fingerprint=child_policy_fingerprint,
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


def test_compare_and_swap_pre_cache_paths_preserve_outcomes(tmp_path: Path) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=3_412)
    transition = _retain(creation, policy, reason="pre-cache-outcome")
    real_cache_factory = harness._new_history_snapshot_cache
    issued_caches: list[harness._HistorySnapshotCache] = []

    def observe_cache_factory() -> harness._HistorySnapshotCache:
        cache = real_cache_factory()
        issued_caches.append(cache)
        return cache

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_new_history_snapshot_cache", observe_cache_factory)
        conflict = harness.compare_and_swap_stream(token, transition)
    assert conflict.classification is harness.StoreClassification.CONFLICT
    assert conflict.committed is True
    assert issued_caches == []

    def stop_after_lock(seam: str) -> None:
        if seam == "after_lock":
            raise harness.HarnessFailure(harness.HarnessFailureCode.UNAVAILABLE)

    with (
        pytest.MonkeyPatch.context() as patch,
        pytest.raises(harness.HarnessFailure) as early_error,
    ):
        patch.setattr(harness, "_new_history_snapshot_cache", observe_cache_factory)
        harness.compare_and_swap_stream(token, transition, seam_hook=stop_after_lock)
    assert early_error.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert issued_caches == []


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
    regressed_competing = _with_recorded_at(
        creation,
        competing,
        creation.record.recorded_at - timedelta(microseconds=1),
    )
    with pytest.raises(harness.HarnessFailure) as corrupt_competing:
        harness.compare_and_swap_stream(token, regressed_competing)
    assert corrupt_competing.value.code is harness.HarnessFailureCode.CORRUPT
    assert harness.compare_and_swap_stream(token, competing).classification is (
        harness.StoreClassification.CONFLICT
    )
    real_decode = harness._decode_canonical_history_record
    decoded_calls: list[bytes] = []

    def observe_decode(record_bytes: bytes, **kwargs: Any) -> Any:
        decoded_calls.append(record_bytes)
        return real_decode(record_bytes, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_decode_canonical_history_record", observe_decode)
        assert harness.compare_and_swap_stream(token, third).classification is (
            harness.StoreClassification.UPDATED
        )
    assert decoded_calls == [creation.canonical_bytes, accepted.canonical_bytes]

    decoded_calls.clear()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_decode_canonical_history_record", observe_decode)
        assert harness.compare_and_swap_stream(token, accepted).classification is (
            harness.StoreClassification.DUPLICATE
        )
    assert decoded_calls == [
        creation.canonical_bytes,
        accepted.canonical_bytes,
        third.canonical_bytes,
    ]
    assert harness.verify_store(token).history_count == 3


def test_compare_and_swap_replays_historical_attach_after_child_completion_and_v6(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=341)
    child_job_id = UUID("00000000-0000-4000-8000-000000000341")
    child_policy_fingerprint = _digest("task064-historical-child-policy")
    attached = _child_lifecycle_transition(
        creation,
        policy,
        ContinuousPublicTradeTransitionKind.ATTACH,
        child_job_id=child_job_id,
        child_policy_fingerprint=child_policy_fingerprint,
    )
    completed = _child_lifecycle_transition(
        attached,
        policy,
        ContinuousPublicTradeTransitionKind.CHILD_COMPLETED,
        child_job_id=child_job_id,
        child_policy_fingerprint=child_policy_fingerprint,
    )
    fourth = _retain(completed, policy, reason="historical-attach-v4")
    fifth = _retain(fourth, policy, reason="historical-attach-v5")
    sixth = _retain(fifth, policy, reason="historical-attach-v6")

    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert (
        harness.compare_and_swap_stream(
            token,
            attached,
            expectation=_expectation(
                policy,
                creation,
                child_policy_fingerprint=child_policy_fingerprint,
            ),
        ).classification
        is harness.StoreClassification.UPDATED
    )
    assert (
        harness.compare_and_swap_stream(
            token,
            completed,
            expectation=_expectation(
                policy,
                creation,
                child_policy_fingerprint=child_policy_fingerprint,
            ),
        ).classification
        is harness.StoreClassification.UPDATED
    )

    real_transition_decode = decode_stream_transition_record
    transition_decode_calls: list[bytes] = []

    def observe_transition_decode(record_bytes: bytes) -> Any:
        transition_decode_calls.append(record_bytes)
        return real_transition_decode(record_bytes)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "decode_stream_transition_record", observe_transition_decode)
        loaded_after_completion = harness.load_current(
            token,
            stream_id=creation.record.stream_id,
            natural_key=_natural_key(creation),
        )
    assert loaded_after_completion.classification is harness.StoreClassification.FOUND
    assert transition_decode_calls == [attached.canonical_bytes, completed.canonical_bytes]

    for transition in (fourth, fifth, sixth):
        assert (
            harness.compare_and_swap_stream(
                token,
                transition,
                expectation=_expectation(policy, creation),
            ).classification
            is harness.StoreClassification.UPDATED
        )

    replay = harness.compare_and_swap_stream(
        token,
        attached,
        expectation=_expectation(
            policy,
            creation,
            child_policy_fingerprint=child_policy_fingerprint,
        ),
    )
    assert replay.classification is harness.StoreClassification.DUPLICATE
    assert replay.stream_rows == 1
    assert replay.history_rows == 5
    assert replay.committed is True

    drifted_policy = ContinuousPublicTradePolicy.model_validate(
        policy.model_dump() | {"max_jobs_per_invocation": policy.max_jobs_per_invocation + 1}
    )
    drifted_replay = harness.compare_and_swap_stream(
        token,
        attached,
        expectation=_expectation(
            drifted_policy,
            creation,
            child_policy_fingerprint=child_policy_fingerprint,
        ),
    )
    assert drifted_replay.classification is harness.StoreClassification.CONFLICT
    assert drifted_replay.stream_rows == 1
    assert drifted_replay.history_rows == 5
    assert drifted_replay.committed is True
    summary = harness.verify_store(token)
    assert summary.stream_count == 1
    assert summary.history_count == 6


def test_history_snapshot_rebinding_and_decoded_only_predecessors_fail_closed(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    policy, creation = _creation(seed=3_411)
    transition = _retain(creation, policy, reason="snapshot-rebinding")
    assert harness.create_stream(token, creation, policy).classification is (
        harness.StoreClassification.INSERTED
    )
    assert harness.compare_and_swap_stream(token, transition).classification is (
        harness.StoreClassification.UPDATED
    )

    def rebound_row(source: sqlite3.Row, **changes: object) -> sqlite3.Row:
        columns = tuple(source.keys())
        probe = sqlite3.connect(":memory:", autocommit=True)
        probe.row_factory = sqlite3.Row
        try:
            aliases = ", ".join(f'? AS "{column}"' for column in columns)
            row = cast(
                sqlite3.Row | None,
                probe.execute(
                    f"SELECT {aliases}",
                    tuple(changes.get(column, source[column]) for column in columns),
                ).fetchone(),
            )
            assert row is not None
            return row
        finally:
            probe.close()

    connection, _ = harness._connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        harness._verify_operation_snapshot(connection, token, writer=False)
        stream = connection.execute(
            "SELECT * FROM continuous_public_trade_stream WHERE stream_uuid = ?",
            (creation.record.stream_id.bytes,),
        ).fetchone()
        assert stream is not None
        rows = connection.execute(
            "SELECT * FROM continuous_public_trade_history ORDER BY successor_version"
        ).fetchall()
        assert len(rows) == 2
        assert tuple(rows[0].keys()) == harness._HISTORY_ROW_COLUMNS

        def issued_creation() -> tuple[
            harness._HistorySnapshotCache,
            harness._ValidatedHistoryRowSnapshot,
        ]:
            snapshot_cache = harness._new_history_snapshot_cache()
            snapshot = harness._history_snapshot_from_row(
                rows[0],
                policy=policy,
                cache=snapshot_cache,
            )
            return snapshot_cache, snapshot

        def assert_invalid_cache(snapshot_cache: harness._HistorySnapshotCache) -> None:
            assert snapshot_cache.valid is False
            assert type(snapshot_cache.decoded_records) is dict
            assert type(snapshot_cache.decoded_issued) is dict
            assert type(snapshot_cache.validated_rows) is dict
            assert type(snapshot_cache.snapshot_issued) is dict
            assert not snapshot_cache.decoded_records
            assert not snapshot_cache.decoded_issued
            assert not snapshot_cache.validated_rows
            assert not snapshot_cache.snapshot_issued

        snapshot_cache, creation_snapshot = issued_creation()
        transition_snapshot = harness._history_snapshot_from_row(
            rows[1],
            policy=policy,
            predecessor=creation_snapshot,
            cache=snapshot_cache,
        )
        assert transition_snapshot.entry == transition
        assert snapshot_cache.cardinalities() == (2, 2, 2, 2)
        snapshot_cache.retain_boundary(transition_snapshot)
        assert snapshot_cache.cardinalities() == (1, 1, 1, 1)
        assert snapshot_cache.decoded_records == {
            transition.canonical_bytes: transition_snapshot.decoded
        }
        assert tuple(snapshot_cache.validated_rows.values()) == (transition_snapshot,)
        assert tuple(snapshot_cache.snapshot_issued) == (id(transition_snapshot),)
        snapshot_cache.invalidate()

        real_cache_factory = harness._new_history_snapshot_cache
        observed_caches: list[harness._HistorySnapshotCache] = []

        def observe_cache_factory() -> harness._HistorySnapshotCache:
            observed_cache = real_cache_factory()
            observed_caches.append(observed_cache)
            return observed_cache

        def fail_after_decoded_issue(**_kwargs: Any) -> Any:
            assert observed_caches[-1].decoded_records
            raise RuntimeError("unexpected-after-decoded-issue")

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(RuntimeError, match="unexpected-after-decoded-issue"),
        ):
            patch.setattr(harness, "_new_history_snapshot_cache", observe_cache_factory)
            patch.setattr(harness, "_stored_creation_without_scope", fail_after_decoded_issue)
            harness._history_snapshot_from_row(rows[0], policy=policy)
        assert len(observed_caches) == 1
        assert_invalid_cache(observed_caches[0])
        with pytest.raises(harness.HarnessFailure) as unexpected_reuse:
            observed_caches[0].decoded(creation.canonical_bytes)
        assert unexpected_reuse.value.code is harness.HarnessFailureCode.CORRUPT

        observed_caches.clear()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(harness, "_new_history_snapshot_cache", observe_cache_factory)
            standalone_snapshot = harness._history_snapshot_from_row(rows[0], policy=policy)
        assert standalone_snapshot.entry == creation
        assert len(observed_caches) == 1
        assert_invalid_cache(observed_caches[0])

        page_cache = harness._new_history_snapshot_cache()
        real_snapshot_from_row = harness._history_snapshot_from_row

        def fail_after_page_issue(*args: Any, **kwargs: Any) -> None:
            real_snapshot_from_row(*args, **kwargs)
            assert page_cache.cardinalities() == (1, 1, 1, 1)
            raise RuntimeError("unexpected-after-page-issue")

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(RuntimeError, match="unexpected-after-page-issue"),
        ):
            patch.setattr(harness, "_history_snapshot_from_row", fail_after_page_issue)
            harness._validate_history_page_rows(
                stream,
                (rows[0],),
                policy=policy,
                cache=page_cache,
            )
        assert_invalid_cache(page_cache)
        with pytest.raises(harness.HarnessFailure) as page_cache_reuse:
            page_cache.decoded(creation.canonical_bytes)
        assert page_cache_reuse.value.code is harness.HarnessFailureCode.CORRUPT

        def decode_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("oversized page must fail before decoding")

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(harness.HarnessFailure) as oversized_page,
        ):
            snapshot_cache, creation_snapshot = issued_creation()
            patch.setattr(harness, "_history_snapshot_from_row", decode_must_not_run)
            harness._validate_history_page_rows(
                stream,
                (rows[1],) * 101,
                policy=policy,
                preceding_snapshot=creation_snapshot,
                cache=snapshot_cache,
            )
        assert oversized_page.value.code is harness.HarnessFailureCode.BOUNDS_EXCEEDED
        assert_invalid_cache(snapshot_cache)

        conflicting_binding = rebound_row(rows[0], entry_kind=b"transition")
        snapshot_cache, _ = issued_creation()
        with pytest.raises(harness.HarnessFailure) as same_bytes_wrong_binding:
            harness._history_snapshot_from_row(
                conflicting_binding,
                policy=policy,
                cache=snapshot_cache,
            )
        assert same_bytes_wrong_binding.value.code is harness.HarnessFailureCode.CORRUPT

        snapshot_cache, creation_snapshot = issued_creation()
        with pytest.raises(harness.HarnessFailure) as decoded_is_not_physical:
            harness._history_snapshot_from_row(
                rows[1],
                policy=policy,
                predecessor=cast(Any, creation_snapshot.decoded),
                cache=snapshot_cache,
            )
        assert decoded_is_not_physical.value.code is harness.HarnessFailureCode.CORRUPT

        for forged_predecessor in (
            replace(issued_creation()[1]),
            harness._ValidatedHistoryRowSnapshot(
                row=rows[0],
                decoded=issued_creation()[1].decoded,
                predecessor_link=None,
                entry=creation,
            ),
        ):
            snapshot_cache, _ = issued_creation()
            with pytest.raises(harness.HarnessFailure) as unissued_snapshot:
                harness._history_snapshot_from_row(
                    rows[1],
                    policy=policy,
                    predecessor=forged_predecessor,
                    cache=snapshot_cache,
                )
            assert unissued_snapshot.value.code is harness.HarnessFailureCode.CORRUPT

        raw_rebindings = (
            {"history_row_id": rows[0]["history_row_id"] + 1},
            {"stream_row_id": rows[0]["stream_row_id"] + 1},
            {"successor_version": 2},
            {"entry_kind": b"transition"},
            {"record_model_version": b"2.0"},
            {"serialization_version": 2},
            {"record_canonical_bytes": creation.canonical_bytes + b" "},
            {"record_digest": _digest("snapshot-record").encode("ascii")},
            {
                "successor_envelope_canonical_bytes": (
                    creation.successor_envelope.canonical_bytes + b" "
                )
            },
            {"successor_envelope_digest": _digest("snapshot-envelope").encode("ascii")},
            {"successor_history_root": _digest("snapshot-root").encode("ascii")},
            {"prior_version": 1},
            {"prior_envelope_digest": creation.successor_envelope.envelope_digest.encode("ascii")},
            {"prior_history_root": creation.history_root.encode("ascii")},
            {"predecessor_record_canonical_bytes": creation.canonical_bytes},
            {"predecessor_record_digest": creation.record_digest.encode("ascii")},
        )
        for changes in raw_rebindings:
            snapshot_cache, creation_snapshot = issued_creation()
            forged_predecessor = replace(
                creation_snapshot,
                row=rebound_row(rows[0], **changes),
            )
            with pytest.raises(harness.HarnessFailure) as rebound:
                harness._history_snapshot_from_row(
                    rows[1],
                    policy=policy,
                    predecessor=forged_predecessor,
                    cache=snapshot_cache,
                )
            assert rebound.value.code is harness.HarnessFailureCode.CORRUPT

        snapshot_cache, creation_snapshot = issued_creation()
        snapshot_cache.process_id = -1
        with pytest.raises(harness.HarnessFailure) as wrong_process:
            snapshot_cache.require_snapshot(creation_snapshot)
        assert wrong_process.value.code is harness.HarnessFailureCode.CORRUPT
        assert_invalid_cache(snapshot_cache)

        snapshot_cache, creation_snapshot = issued_creation()
        with pytest.raises(harness.HarnessFailure) as forged_mismatch:
            snapshot_cache.require_snapshot(replace(creation_snapshot))
        assert forged_mismatch.value.code is harness.HarnessFailureCode.CORRUPT
        assert_invalid_cache(snapshot_cache)
        with pytest.raises(harness.HarnessFailure) as mismatch_reuse:
            snapshot_cache.require_snapshot(creation_snapshot)
        assert mismatch_reuse.value.code is harness.HarnessFailureCode.CORRUPT

        class HostileDecodedRegistry(dict[bytes, harness._DecodedCanonicalHistoryRecord]):
            clear_calls = 0

            def clear(self) -> None:
                self.clear_calls += 1
                raise RuntimeError("hostile-registry-clear")

        snapshot_cache, creation_snapshot = issued_creation()
        hostile_registry = HostileDecodedRegistry(snapshot_cache.decoded_records)
        snapshot_cache.decoded_records = hostile_registry
        with pytest.raises(harness.HarnessFailure) as nonexact_registry:
            snapshot_cache.cardinalities()
        assert nonexact_registry.value.code is harness.HarnessFailureCode.CORRUPT
        assert hostile_registry.clear_calls == 1
        assert hostile_registry
        assert_invalid_cache(snapshot_cache)
        with pytest.raises(harness.HarnessFailure) as nonexact_registry_reuse:
            snapshot_cache.require_snapshot(creation_snapshot)
        assert nonexact_registry_reuse.value.code is harness.HarnessFailureCode.CORRUPT

        snapshot_cache, creation_snapshot = issued_creation()
        with pytest.raises(harness.HarnessFailure) as failed_compaction:
            snapshot_cache.retain_boundary(replace(creation_snapshot))
        assert failed_compaction.value.code is harness.HarnessFailureCode.CORRUPT
        assert_invalid_cache(snapshot_cache)

        snapshot_cache, creation_snapshot = issued_creation()
        transition_snapshot = harness._history_snapshot_from_row(
            rows[1],
            policy=policy,
            predecessor=creation_snapshot,
            cache=snapshot_cache,
        )

        def fail_after_boundary_swap(
            _snapshot_cache: harness._HistorySnapshotCache,
        ) -> tuple[int, int, int, int]:
            raise RuntimeError("unexpected-after-boundary-swap")

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(RuntimeError, match="unexpected-after-boundary-swap"),
        ):
            patch.setattr(
                harness._HistorySnapshotCache,
                "cardinalities",
                fail_after_boundary_swap,
            )
            snapshot_cache.retain_boundary(transition_snapshot)
        assert_invalid_cache(snapshot_cache)
        with pytest.raises(harness.HarnessFailure) as boundary_reuse:
            snapshot_cache.require_snapshot(transition_snapshot)
        assert boundary_reuse.value.code is harness.HarnessFailureCode.CORRUPT
        connection.execute("ROLLBACK").close()
    finally:
        connection.close()


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
    mixed_policy, mixed_creation = _creation(
        seed=342,
        stream_id_seed=342,
        identity_seed=343,
    )
    mixed_transition = _retain(
        mixed_creation,
        mixed_policy,
        reason="reachable-mixed-identity-v2",
    )
    mixed_expectation = _expectation(mixed_policy, mixed_creation)
    mixed_command = ContinuousPublicTradeStreamCompareAndSwapCommandV1(
        expectation=mixed_expectation,
        expected_version=mixed_transition.record.prior_version,
        expected_envelope_digest=mixed_transition.record.prior_envelope_digest,
        expected_history_root=mixed_transition.record.prior_history_root,
        transition=mixed_transition,
    )
    exact_mixed_command = ContinuousPublicTradeStreamCompareAndSwapCommandV1.revalidate_at_boundary(
        mixed_command
    )
    two_candidates = harness.compare_and_swap_stream(
        mature,
        exact_mixed_command.transition,
        expectation=exact_mixed_command.expectation,
    )
    assert two_candidates.classification is harness.StoreClassification.CONFLICT
    assert two_candidates.stream_rows == 2
    assert two_candidates.history_rows == 6
    assert harness.verify_store(mature).history_count == 6

    _corrupt_creation_history_record(mature, creation_b)
    with pytest.raises(harness.HarnessFailure) as corrupt_retained_candidate:
        harness.compare_and_swap_stream(
            mature,
            exact_mixed_command.transition,
            expectation=exact_mixed_command.expectation,
        )
    assert corrupt_retained_candidate.value.code is harness.HarnessFailureCode.CORRUPT


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
    _active_task064_pytest_root: harness._PytestRootCapability,
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

    foreign_backup_root = _active_task064_pytest_root.roots[1]
    with contextlib.nullcontext():
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
        cyclic_transitions: list[object] = []
        cyclic_transitions.append(cyclic_transitions)
        hostile_transitions = _HostileMapping()
        depth_65_transitions: object = "leaf"
        for _ in range(65):
            depth_65_transitions = [depth_65_transitions]
        for invalid_transitions in (
            cyclic_transitions,
            hostile_transitions,
            depth_65_transitions,
        ):
            with pytest.raises(harness.HarnessFailure) as invalid_shape:
                harness.collect_backup_restore_evidence(
                    run,
                    source,
                    tmp_path,
                    transitions=cast(Any, invalid_transitions),
                    backup_recorded_at_utc="2026-07-29T06:35:00.000000Z",
                    restore_recorded_at_utc="2026-07-29T06:36:00.000000Z",
                )
            assert invalid_shape.value.code is harness.HarnessFailureCode.CORRUPT
        assert hostile_transitions.items_calls == 0
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
        _report_bootstrap_path_evidence(
            tmp_path,
            report_token,
            _active_task064_pytest_root,
        )
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

    foreign_copy_root = _active_task064_pytest_root.roots[3]
    with contextlib.nullcontext():
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
    token_nonces = frozenset(harness._token_authority_snapshot())
    with pytest.raises(harness.HarnessFailure) as malformed:
        harness.online_backup(
            source,
            tmp_path,
            evidence_recorded_at_utc="2026-07-29T06:00:00Z",
        )
    assert malformed.value.code is harness.HarnessFailureCode.CORRUPT
    assert frozenset(harness._token_authority_snapshot()) == token_nonces
    assert harness.verify_store(source).history_count == 0


def test_direct_backup_and_copy_apis_reject_cross_scope_before_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _active_task064_pytest_root: harness._PytestRootCapability,
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
    foreign_root = _active_task064_pytest_root.roots[1]
    foreign_token: harness.StoreToken | None = None
    with contextlib.nullcontext():
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
    assert destination._nonce not in harness._token_authority_snapshot()
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
    with contextlib.ExitStack() as replacement_descriptors:
        retained_descriptor = os.open(
            replaced._database_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        replacement_descriptors.callback(os.close, retained_descriptor)
        replaced._database_path.unlink()
        replacement_descriptor = os.open(
            replaced._database_path,
            os.O_CREAT
            | os.O_EXCL
            | os.O_RDWR
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        replacement_descriptors.callback(os.close, replacement_descriptor)
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
        "_publish_evidence_report_bytes_unbound",
        "_build_evidence_report_writer",
        "_build_task064_published_report_artifact_authority",
        "_register_task064_published_report_artifact",
        "_resolve_task064_published_report_artifact",
        "_validate_task064_report_publication_permit",
        "_prepare_task064_report_publication_permit_consumption",
        "_build_private_gate_receipt_authority",
        "_build_rejection_evidence_authority",
        "_build_evidence_receipt_authority",
        "_EvidenceRunCloseCapability",
        "_PreparedEvidenceRunConsumption",
        "_PreparedEvidenceReceiptConsumption",
        "_close_issued_evidence_run",
        "_is_issued_evidence_run_closed",
        "_prepare_issued_evidence_run_consumption",
        "_prepare_issued_evidence_run_seal",
        "_capture_fork_guard",
        "_build_fork_safe_connection_requirement",
        "_build_protected_token_requirement",
        "_TOKEN_REGISTRY",
        "_ACTIVE_PYTEST_ROOTS",
        "_REVOKED_PYTEST_ROOTS",
        "_CONNECTION_PATH_SNAPSHOTS",
        "_CONNECTION_EVIDENCE_BINDINGS",
        "_store_token_issuing_bootstrap",
        "_build_store_token_authority",
        "_build_pytest_root_authority",
        "_build_connection_authority",
    ):
        assert not hasattr(harness, removed_issuer)
    run = harness.begin_generated_evidence_run(tmp_path)
    assert type(run._pytest_registration.evidence_ledger) is harness._EvidenceLedger
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


@pytest.mark.parametrize(
    ("stage_fault", "rollback_fault"),
    (
        ("after_record_reservation", None),
        ("after_ledger_run", None),
        ("after_ledger_recording", None),
        ("after_context_set", None),
        ("after_context_set", "rollback_context_reset"),
        ("after_ledger_recording", "rollback_ledger_restore"),
    ),
)
def test_evidence_run_begin_is_failure_atomic_and_poisoned_on_uncertain_rollback(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    stage_fault: str,
    rollback_fault: str | None,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    registration = harness._lookup_active_pytest_root(tmp_path)
    assert registration is not None
    assert type(registration.evidence_ledger) is harness._EvidenceLedger
    ledger = registration.evidence_ledger
    ledger_collections = (
        ledger.observations,
        ledger.operation_runs,
        ledger.rejection_runs,
    )
    run_records = inspect.getclosurevars(harness.begin_generated_evidence_run).nonlocals["records"]
    initial_record_ids = frozenset(run_records)
    harness._arm_evidence_run_begin_fault(stage_fault)
    if rollback_fault is not None:
        harness._arm_evidence_run_begin_fault(rollback_fault)
    with pytest.raises(harness.HarnessFailure) as failed_begin:
        harness.begin_generated_evidence_run(tmp_path)
    aborted_records = [
        record for identity, record in run_records.items() if identity not in initial_record_ids
    ]
    assert len(aborted_records) == 1
    assert aborted_records[0]["phase"] == "ABORTED"
    assert aborted_records[0]["run"] is not None

    if rollback_fault is None:
        assert failed_begin.value.code is harness.HarnessFailureCode.CORRUPT
        assert harness._ACTIVE_EVIDENCE_RUN.get() is None
        assert ledger.run is None
        assert ledger.receipt is None
        assert not ledger.recording
        assert not ledger.closed
        assert not ledger.consumed
        assert ledger.observations is ledger_collections[0]
        assert ledger.operation_runs is ledger_collections[1]
        assert ledger.rejection_runs is ledger_collections[2]
        assert all(not collection for collection in ledger_collections)
        assert not harness._pytest_root_authority_uncertain()
        retry = harness.begin_generated_evidence_run(tmp_path)
        assert harness._ACTIVE_EVIDENCE_RUN.get() is retry
        assert harness._validated_evidence_run(retry) is ledger
        return

    assert failed_begin.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert harness._pytest_root_authority_uncertain()
    retained_run = ledger.run
    if retained_run is not None:
        with pytest.raises(harness.HarnessFailure) as retained_authority:
            harness._validated_evidence_run(retained_run)
        assert retained_authority.value.code is harness.HarnessFailureCode.CORRUPT
    with pytest.raises(harness.HarnessFailure) as retry_rejected:
        harness.begin_generated_evidence_run(tmp_path)
    assert retry_rejected.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT


def test_pytest_fixture_authority_has_exactly_two_stdlib_only_issuers() -> None:
    harness_path = Path(harness.__file__).resolve()
    harness_tree = ast.parse(harness_path.read_text(encoding="utf-8"))
    forbidden_names = {"FixtureLookupError", "FixtureRequest", "TempPathFactory"}
    for node in ast.walk(harness_tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imported = (
                tuple(alias.name for alias in node.names)
                if isinstance(node, ast.Import)
                else (() if node.module is None else (node.module,))
            )
            assert all(name != "pytest" and not name.startswith("_pytest") for name in imported)
        assert not (isinstance(node, ast.Name) and node.id in forbidden_names)
        assert not (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "sys"
            and node.attr == "modules"
        )

    builder = next(
        node
        for node in harness_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_build_pytest_root_authority"
    )
    policy_assignment = next(
        node
        for node in builder.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "fixture_policy"
            for target in node.targets
        )
    )
    expected_fixture_policy_prefixes = (
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
        ),
    )
    literal_fixture_policy = ast.literal_eval(policy_assignment.value)
    assert tuple(policy[:-1] for policy in literal_fixture_policy) == (
        expected_fixture_policy_prefixes
    )
    assert all(
        type(policy[-1]) is str
        and len(policy[-1]) == 64
        and set(policy[-1]) <= set("0123456789abcdef")
        for policy in literal_fixture_policy
    )
    expected_fixture_sites = {policy[0]: policy[1:7] for policy in expected_fixture_policy_prefixes}

    test_root = harness_path.parents[1]
    for relative_path in (
        "unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
        "integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
    ):
        source_path = test_root / relative_path
        source = source_path.read_text(encoding="utf-8")
        module_tree = ast.parse(source)
        module_attributes = [
            node.attr for node in ast.walk(module_tree) if isinstance(node, ast.Attribute)
        ]
        assert module_attributes.count("_register_task064_fixture_issuer") == 0
        assert module_attributes.count("_begin_pytest_root_registration") >= 1
        root_scope_attribute_contexts = tuple(
            function.name
            for function in module_tree.body
            if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
            for node in ast.walk(function)
            if isinstance(node, ast.Attribute) and node.attr == "_pytest_root_scope"
        )
        expected_root_scope_contexts = (
            (
                "_build_active_task064_pytest_root_fixture",
                "test_pytest_fixture_authority_has_exactly_two_stdlib_only_issuers",
            )
            if relative_path.startswith("integration/")
            else ("_build_active_task064_pytest_root_fixture",)
        )
        assert root_scope_attribute_contexts == expected_root_scope_contexts
        assert len(root_scope_attribute_contexts) == module_attributes.count("_pytest_root_scope")
        fixture_factory = next(
            node
            for node in module_tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_build_active_task064_pytest_root_fixture"
        )
        fixture = next(
            node
            for node in ast.walk(fixture_factory)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_active_task064_pytest_root"
        )
        assert not any(
            isinstance(decorator, ast.Attribute)
            and decorator.attr == "_register_task064_fixture_issuer"
            for decorator in fixture.decorator_list
        )
        fixture_calls = [node for node in ast.walk(fixture) if isinstance(node, ast.Call)]
        binding_seal_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "bindings_are_exact"
        ]
        authenticate_provenance_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "authenticate_child_provenance"
        ]
        activate_provenance_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "activate_child_provenance"
        ]
        post_return_claim_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name)
            and node.func.id == "claim_post_return_child_provenance"
        ]
        finish_provenance_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "finish_child_provenance"
        ]
        cancel_provenance_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "cancel_child_provenance"
        ]
        begin_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "begin_registration"
        ]
        scope_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "root_scope"
        ]
        cancel_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "cancel_registration"
        ]
        root_calls = [
            node
            for node in fixture_calls
            if isinstance(node.func, ast.Name) and node.func.id == "temp_path_mktemp"
        ]
        yields = [node for node in ast.walk(fixture) if isinstance(node, ast.Yield)]
        assert len(binding_seal_calls) == len(authenticate_provenance_calls) == 1
        assert len(activate_provenance_calls) == len(post_return_claim_calls) == 1
        assert len(finish_provenance_calls) == len(cancel_provenance_calls) == 1
        assert len(begin_calls) == len(scope_calls) == len(yields) == 1
        assert len(cancel_calls) == 1
        assert len(root_calls) == 5
        setup_try = next(node for node in fixture.body if isinstance(node, ast.Try))
        setup_calls = [node for node in ast.walk(setup_try) if isinstance(node, ast.Call)]
        setup_root_calls = [
            node
            for node in setup_calls
            if isinstance(node.func, ast.Name) and node.func.id == "temp_path_mktemp"
        ]
        setup_scope_calls = [
            node
            for node in setup_calls
            if isinstance(node.func, ast.Name) and node.func.id == "root_scope"
        ]
        setup_with = [node for node in ast.walk(setup_try) if isinstance(node, ast.With)]
        setup_cancel_calls = [
            node
            for node in setup_calls
            if isinstance(node.func, ast.Name) and node.func.id == "cancel_registration"
        ]
        assert len(setup_root_calls) == 5
        pre_yield_root_calls = [node for node in setup_root_calls if node.lineno < yields[0].lineno]
        post_yield_root_calls = [
            node for node in setup_root_calls if node.lineno > yields[0].lineno
        ]
        assert len(pre_yield_root_calls) == 4
        assert len(post_yield_root_calls) == 1
        assert setup_scope_calls == scope_calls
        assert len(setup_with) == 1
        assert setup_cancel_calls == cancel_calls
        assert len(setup_try.handlers) == 1
        assert isinstance(setup_try.handlers[0].type, ast.Name)
        assert setup_try.handlers[0].type.id == "base_exception_type"
        assert (
            binding_seal_calls[0].lineno
            < authenticate_provenance_calls[0].lineno
            < setup_try.lineno
            < activate_provenance_calls[0].lineno
            < post_return_claim_calls[0].lineno
            < begin_calls[0].lineno
            <= min(node.lineno for node in pre_yield_root_calls)
            <= max(node.lineno for node in pre_yield_root_calls)
            < yields[0].lineno
            < post_yield_root_calls[0].lineno
            < finish_provenance_calls[0].lineno
        )
        assert scope_calls[0].lineno < yields[0].lineno
        assert cancel_calls[0].lineno < cancel_provenance_calls[0].lineno

    runtime_suffix = (
        "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py"
    )
    runtime_site = expected_fixture_sites[runtime_suffix]
    runtime_fixture_function = inspect.unwrap(_active_task064_pytest_root)
    assert type(runtime_fixture_function) is FunctionType
    runtime_fixture_code = runtime_fixture_function.__code__
    runtime_fixture_closure = inspect.getclosurevars(runtime_fixture_function)
    runtime_fixture_globals = runtime_fixture_closure.globals
    assert set(runtime_fixture_globals) == {"@py_builtins", "@pytest_ar"}
    builtins_module = inspect.getmodule(len)
    assertion_rewrite_module = inspect.getmodule(_rewrite_test)
    assert builtins_module is not None
    assert assertion_rewrite_module is not None
    assert builtins_module.__name__ == "builtins"
    assert assertion_rewrite_module.__name__ == "_pytest.assertion.rewrite"
    assert runtime_fixture_globals["@py_builtins"] is builtins_module
    assert runtime_fixture_globals["@pytest_ar"] is assertion_rewrite_module
    runtime_fixture_nonlocals = runtime_fixture_closure.nonlocals
    assert runtime_fixture_nonlocals["begin_registration"] is (
        harness._begin_pytest_root_registration
    )
    assert runtime_fixture_nonlocals["root_scope"] is harness._pytest_root_scope
    assert runtime_fixture_nonlocals["cancel_registration"] is (
        harness._cancel_pytest_root_registration
    )
    for closure_name, authority in (
        (
            "authenticate_child_provenance",
            harness._authenticate_task064_child_provenance,
        ),
        ("activate_child_provenance", harness._activate_task064_child_provenance),
        (
            "claim_post_return_child_provenance",
            harness._claim_task064_post_return_child_provenance,
        ),
        ("finish_child_provenance", harness._finish_task064_child_provenance),
        ("cancel_child_provenance", harness._cancel_task064_child_provenance),
    ):
        assert runtime_fixture_nonlocals[closure_name] is authority
    bindings_are_exact = cast(
        Callable[[], bool],
        runtime_fixture_nonlocals["bindings_are_exact"],
    )
    assert bindings_are_exact()
    binding_closure = inspect.getclosurevars(bindings_are_exact)
    assert binding_closure.globals == {}
    assert binding_closure.nonlocals["module_globals"] is globals()
    assert binding_closure.nonlocals["pytest_module"] is pytest
    assert binding_closure.nonlocals["harness_module"] is harness
    assert binding_closure.nonlocals["inspect_module"] is inspect
    assert binding_closure.nonlocals["os_module"] is os
    assert binding_closure.nonlocals["contextvars_module"] is contextvars
    assert binding_closure.nonlocals["path_type"] is Path
    assert binding_closure.nonlocals["function_type"] is FunctionType
    assert binding_closure.nonlocals["fixture_request_type"] is pytest.FixtureRequest
    assert binding_closure.nonlocals["temp_path_factory_type"] is pytest.TempPathFactory
    assert binding_closure.nonlocals["temp_path_mktemp"] is pytest.TempPathFactory.mktemp
    for closure_name, sealed_authority in (
        (
            "authenticate_child_provenance",
            harness._authenticate_task064_child_provenance,
        ),
        ("activate_child_provenance", harness._activate_task064_child_provenance),
        (
            "claim_post_return_child_provenance",
            harness._claim_task064_post_return_child_provenance,
        ),
        (
            "claim_child_dispatch_provenance",
            harness._claim_task064_child_dispatch_provenance,
        ),
        ("finish_child_provenance", harness._finish_task064_child_provenance),
        ("cancel_child_provenance", harness._cancel_task064_child_provenance),
    ):
        assert binding_closure.nonlocals[closure_name] is sealed_authority
    assert (
        binding_closure.nonlocals["raw_fixture"] is runtime_fixture_function
        and binding_closure.nonlocals["exported_fixture"] is _active_task064_pytest_root
    )
    begin_closure = inspect.getclosurevars(harness._begin_pytest_root_registration)
    authenticate_fixture_call = cast(
        FunctionType,
        begin_closure.nonlocals["authenticate_fixture_call"],
    )
    authenticate_closure = inspect.getclosurevars(authenticate_fixture_call)
    fixture_policy = authenticate_closure.nonlocals["fixture_policy"]
    allowed_fixture_paths = authenticate_closure.nonlocals["allowed_fixture_paths"]
    assert fixture_policy == literal_fixture_policy
    assert type(fixture_policy) is tuple
    assert type(allowed_fixture_paths) is tuple
    with pytest.raises(TypeError):
        cast(Any, fixture_policy)[0] = fixture_policy[0]
    with pytest.raises(TypeError):
        cast(Any, allowed_fixture_paths)[0] = allowed_fixture_paths[0]
    fingerprint = cast(
        Callable[[CodeType, str], str],
        authenticate_closure.nonlocals["code_fingerprint"],
    )
    for policy, (_, source_path) in zip(
        fixture_policy,
        allowed_fixture_paths,
        strict=True,
    ):
        assert hashlib.sha256(source_path.read_bytes()).hexdigest() == policy[10]
    assert fingerprint(runtime_fixture_code, runtime_suffix) == runtime_site[3]

    def exact_call_offset(closure_name: str) -> int:
        instructions = tuple(dis.get_instructions(runtime_fixture_code))
        loads = tuple(
            index
            for index, instruction in enumerate(instructions)
            if instruction.opname == "LOAD_DEREF" and instruction.argval == closure_name
        )
        assert len(loads) == 1
        return next(
            instruction.offset
            for instruction in instructions[loads[0] + 1 :]
            if instruction.opname == "CALL"
        )

    assert exact_call_offset("begin_registration") == runtime_site[4]
    assert exact_call_offset("root_scope") == runtime_site[5]

    expected_fixture_paths = {
        str((test_root / relative_path).resolve())
        for relative_path in (
            "unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py",
            "integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py",
        )
    }

    def contains_forbidden_identity(value: object, seen: set[int]) -> bool:
        identity = id(value)
        if identity in seen:
            return False
        seen.add(identity)
        value_module = getattr(value, "__module__", "")
        type_module = type(value).__module__
        if any(
            name == "pytest" or name.startswith("_pytest")
            for name in (value_module, type_module)
            if isinstance(name, str)
        ):
            return True
        if type(value) is FunctionType:
            function = value
            if (
                function.__code__.co_name == "_active_task064_pytest_root"
                and function.__code__.co_filename in expected_fixture_paths
            ):
                return True
            closure_values = tuple(
                cell.cell_contents
                for cell in (function.__closure__ or ())
                if hasattr(cell, "cell_contents")
            )
            referenced_globals = tuple(
                function.__globals__[name]
                for name in function.__code__.co_names
                if name in function.__globals__
            )
            return any(
                contains_forbidden_identity(item, seen)
                for item in (
                    function.__defaults__,
                    function.__kwdefaults__,
                    function.__annotations__,
                    closure_values,
                    referenced_globals,
                )
            )
        if type(value) is dict:
            return any(
                contains_forbidden_identity(key, seen) or contains_forbidden_identity(item, seen)
                for key, item in dict.items(value)
            )
        if type(value) is tuple or type(value) is list:
            return any(
                contains_forbidden_identity(item, seen)
                for item in cast(tuple[object, ...] | list[object], value)
            )
        if type(value) is set or type(value) is frozenset:
            return any(
                contains_forbidden_identity(item, seen)
                for item in cast(set[object] | frozenset[object], value)
            )
        slot_names: list[str] = []
        for base in type(value).__mro__:
            slots = getattr(base, "__slots__", ())
            slot_names.extend((slots,) if isinstance(slots, str) else slots)
        return any(
            slot not in {"__dict__", "__weakref__"}
            and hasattr(value, slot)
            and contains_forbidden_identity(getattr(value, slot), seen)
            for slot in slot_names
        )

    authorities = tuple(
        getattr(harness, name)
        for name in (
            "_bind_task064_test_module",
            "_begin_pytest_root_registration",
            "_cancel_pytest_root_registration",
            "_pytest_root_scope",
            "_lookup_active_pytest_root",
            "_validate_pytest_root_identity",
            "_lookup_revoked_pytest_root",
            "_revoke_fixture_root",
            "_pytest_root_session_owns",
            "_pytest_root_authority_fault_hit",
            "_pytest_root_poison_channels",
            "_pytest_root_authority_uncertain",
        )
    )
    for authority in authorities:
        assert not contains_forbidden_identity(authority, set())


def test_fixture_code_fingerprint_is_canonical_bounded_and_domain_separated(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    begin_closure = inspect.getclosurevars(harness._begin_pytest_root_registration)
    authenticate_fixture_call = cast(
        FunctionType,
        begin_closure.nonlocals["authenticate_fixture_call"],
    )
    authenticate_closure = inspect.getclosurevars(authenticate_fixture_call)
    fixture_policy = cast(
        tuple[tuple[object, ...], ...],
        authenticate_closure.nonlocals["fixture_policy"],
    )
    allowed_fixture_paths = cast(
        tuple[tuple[str, Path], ...],
        authenticate_closure.nonlocals["allowed_fixture_paths"],
    )
    fingerprint = cast(
        Callable[[CodeType, str], str],
        authenticate_closure.nonlocals["code_fingerprint"],
    )
    parent_metrics_before = inspect.getclosurevars(fingerprint).nonlocals
    parent_cache_before = cast(
        dict[tuple[int, str], tuple[CodeType, str]],
        parent_metrics_before["fingerprint_cache"],
    )
    parent_cache_entries_before = tuple(
        (key, code, digest) for key, (code, digest) in parent_cache_before.items()
    )
    parent_requests_before = cast(int, parent_metrics_before["fingerprint_requests"])
    parent_computations_before = cast(int, parent_metrics_before["fingerprint_computations"])
    if _run_task064_exec_isolated(request, tmp_path):
        parent_metrics_after = inspect.getclosurevars(fingerprint).nonlocals
        parent_cache_after = cast(
            dict[tuple[int, str], tuple[CodeType, str]],
            parent_metrics_after["fingerprint_cache"],
        )
        assert parent_cache_after is parent_cache_before
        assert cast(int, parent_metrics_after["fingerprint_requests"]) == (parent_requests_before)
        assert cast(int, parent_metrics_after["fingerprint_computations"]) == (
            parent_computations_before
        )
        parent_cache_entries_after = tuple(
            (key, code, digest) for key, (code, digest) in parent_cache_after.items()
        )
        assert len(parent_cache_entries_after) == len(parent_cache_entries_before)
        assert all(
            after_key == before_key and after_code is before_code and after_digest == before_digest
            for (after_key, after_code, after_digest), (
                before_key,
                before_code,
                before_digest,
            ) in zip(
                parent_cache_entries_after,
                parent_cache_entries_before,
                strict=True,
            )
        )
        return
    fingerprint_closure = inspect.getclosurevars(fingerprint).nonlocals
    assert fingerprint_closure["implementation_name"] == sys.implementation.name == "cpython"
    assert fingerprint_closure["implementation_cache_tag"] == sys.implementation.cache_tag
    assert fingerprint_closure["accepted_python_version"] == harness.ACCEPTED_PYTHON_VERSION
    assert fingerprint_closure["runtime_python_version"] == harness.ACCEPTED_PYTHON_VERSION
    assert fingerprint_closure["optimization_level"] == sys.flags.optimize == 0
    assert b"TASK064-CODE-FINGERPRINT-TLV-V3" in fingerprint.__code__.co_consts
    assert b"CONST-GRAPH-ALIAS-PARTITION-V3" in fingerprint.__code__.co_consts
    fingerprint_tree = ast.parse(textwrap.dedent(inspect.getsource(fingerprint)))
    serialize_code_node = next(
        node
        for node in ast.walk(fingerprint_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "serialize_code"
    )
    constant_snapshot_assignments = [
        node
        for node in ast.walk(serialize_code_node)
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id == "constants"
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "snapshot_constants"
        and len(node.value.args) == 1
        and isinstance(node.value.args[0], ast.Name)
        and node.value.args[0].id == "candidate"
    ]
    assert len(constant_snapshot_assignments) == 1
    tagged_candidate_fields: dict[bytes, str] = {}
    for call in ast.walk(serialize_code_node):
        if not (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "frame"
            and len(call.args) == 2
            and isinstance(call.args[0], ast.Constant)
            and type(call.args[0].value) is bytes
            and isinstance(call.args[1], ast.Call)
            and isinstance(call.args[1].func, ast.Name)
            and call.args[1].func.id == "serialize_constant"
            and call.args[1].args
        ):
            continue
        serialized_argument = call.args[1].args[0]
        if (
            isinstance(serialized_argument, ast.Attribute)
            and isinstance(serialized_argument.value, ast.Name)
            and serialized_argument.value.id == "candidate"
        ):
            field_name = serialized_argument.attr
        elif isinstance(serialized_argument, ast.Name) and serialized_argument.id == "constants":
            field_name = "co_consts"
        else:
            continue
        tagged_candidate_fields[call.args[0].value] = field_name
    assert tagged_candidate_fields == {
        b"ac": "co_argcount",
        b"pa": "co_posonlyargcount",
        b"ka": "co_kwonlyargcount",
        b"nl": "co_nlocals",
        b"ss": "co_stacksize",
        b"fg": "co_flags",
        b"bc": "co_code",
        b"cs": "co_consts",
        b"ns": "co_names",
        b"vn": "co_varnames",
        b"nm": "co_name",
        b"qn": "co_qualname",
        b"lt": "co_linetable",
        b"et": "co_exceptiontable",
        b"fv": "co_freevars",
        b"cv": "co_cellvars",
    }
    runtime_suffix = (
        "tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py"
    )
    runtime_fixture = inspect.unwrap(_active_task064_pytest_root)
    assert type(runtime_fixture) is FunctionType
    runtime_fixture_code = runtime_fixture.__code__
    restored_runtime_fixture = marshal.loads(marshal.dumps(runtime_fixture_code))
    assert type(restored_runtime_fixture) is CodeType
    assert restored_runtime_fixture == runtime_fixture_code
    assert restored_runtime_fixture is not runtime_fixture_code
    assert fingerprint(restored_runtime_fixture, runtime_suffix) == fingerprint(
        runtime_fixture_code,
        runtime_suffix,
    )

    def unique_nested_code(module_code: CodeType, name: str) -> CodeType:
        matches: list[CodeType] = []

        def visit(candidate: CodeType) -> None:
            if candidate.co_name == name:
                matches.append(candidate)
            for constant in candidate.co_consts:
                if type(constant) is CodeType:
                    visit(constant)

        visit(module_code)
        assert len(matches) == 1
        return matches[0]

    compiled_sites: dict[tuple[str, str], CodeType] = {}
    for policy, (_, source_path) in zip(
        fixture_policy,
        allowed_fixture_paths,
        strict=True,
    ):
        suffix = cast(str, policy[0])
        _, module_code = _rewrite_test(source_path, request.config)
        restored_module = marshal.loads(marshal.dumps(module_code))
        assert type(restored_module) is CodeType
        for name in (cast(str, policy[2]), cast(str, policy[7])):
            fresh_code = unique_nested_code(module_code, name)
            restored_code = unique_nested_code(restored_module, name)
            assert fresh_code == restored_code
            assert fresh_code is not restored_code
            assert fingerprint(fresh_code, suffix) == fingerprint(restored_code, suffix)
            compiled_sites[(suffix, name)] = fresh_code

    baseline = compiled_sites[(runtime_suffix, "_active_task064_pytest_root")]
    baseline_digest = fingerprint(baseline, runtime_suffix)
    cache_probe = baseline.replace(co_filename=baseline.co_filename)
    assert cache_probe is not baseline
    metrics_before = inspect.getclosurevars(fingerprint).nonlocals
    requests_before = cast(int, metrics_before["fingerprint_requests"])
    computations_before = cast(int, metrics_before["fingerprint_computations"])
    cache_size_before = len(cast(dict[object, object], metrics_before["fingerprint_cache"]))
    cache_probe_digest = fingerprint(cache_probe, runtime_suffix)
    for _ in range(127):
        assert fingerprint(cache_probe, runtime_suffix) == cache_probe_digest
    metrics_after = inspect.getclosurevars(fingerprint).nonlocals
    assert cast(int, metrics_after["fingerprint_requests"]) - requests_before == 128
    assert cast(int, metrics_after["fingerprint_computations"]) - computations_before == 1
    assert (
        len(cast(dict[object, object], metrics_after["fingerprint_cache"]))
        == cache_size_before + 1
        <= cast(int, metrics_after["maximum_cached_fingerprints"])
    )
    extended_varnames = (*baseline.co_varnames, "task064_extra_local")
    coupled_locals_mutation = baseline.replace(
        co_nlocals=baseline.co_nlocals + 1,
        co_varnames=extended_varnames,
    )
    renamed_varnames = (
        *baseline.co_varnames[:-1],
        f"{baseline.co_varnames[-1]}_task064",
    )
    field_mutations = {
        "co_argcount": baseline.replace(co_argcount=baseline.co_argcount - 1),
        "co_posonlyargcount": baseline.replace(co_posonlyargcount=1),
        "co_kwonlyargcount": baseline.replace(co_kwonlyargcount=1),
        "co_nlocals": coupled_locals_mutation,
        "co_stacksize": baseline.replace(co_stacksize=baseline.co_stacksize + 1),
        "co_flags": baseline.replace(co_flags=baseline.co_flags ^ inspect.CO_NESTED),
        "co_code": baseline.replace(co_code=baseline.co_code + b"\x00\x00"),
        "co_consts": baseline.replace(
            co_consts=(*baseline.co_consts, "task064-extra-constant"),
        ),
        "co_names": baseline.replace(co_names=(*baseline.co_names, "task064_extra")),
        "co_varnames": baseline.replace(co_varnames=renamed_varnames),
        "co_name": baseline.replace(co_name=f"{baseline.co_name}_task064"),
        "co_qualname": baseline.replace(co_qualname=f"{baseline.co_qualname}_task064"),
        "co_linetable": baseline.replace(co_linetable=baseline.co_linetable + b"\x00"),
        "co_exceptiontable": baseline.replace(
            co_exceptiontable=baseline.co_exceptiontable + b"\x00",
        ),
        "co_freevars": baseline.replace(
            co_freevars=(*baseline.co_freevars, "task064_extra_free"),
        ),
        "co_cellvars": baseline.replace(
            co_cellvars=(*baseline.co_cellvars, "task064_extra_cell"),
        ),
    }
    assert set(field_mutations) == {
        "co_argcount",
        "co_posonlyargcount",
        "co_kwonlyargcount",
        "co_nlocals",
        "co_stacksize",
        "co_flags",
        "co_code",
        "co_consts",
        "co_names",
        "co_varnames",
        "co_name",
        "co_qualname",
        "co_linetable",
        "co_exceptiontable",
        "co_freevars",
        "co_cellvars",
    }
    assert all(
        fingerprint(mutated, runtime_suffix) != baseline_digest
        for mutated in field_mutations.values()
    )

    relocated = baseline.replace(
        co_filename="/untrusted/task064/location.py",
        co_firstlineno=baseline.co_firstlineno + 10_000,
    )
    assert fingerprint(relocated, runtime_suffix) == baseline_digest
    assert fingerprint(baseline, f"{runtime_suffix}.other") != baseline_digest

    shared_leaf = (b"task064-shared-leaf",)
    distinct_leaf = (bytes(bytearray(shared_leaf[0])),)
    assert distinct_leaf == shared_leaf
    assert distinct_leaf is not shared_leaf
    assert distinct_leaf[0] is not shared_leaf[0]
    aliased = baseline.replace(co_consts=(shared_leaf, shared_leaf))
    unaliased = baseline.replace(co_consts=(shared_leaf, distinct_leaf))
    assert aliased.co_consts == unaliased.co_consts
    assert fingerprint(aliased, runtime_suffix) != fingerprint(unaliased, runtime_suffix)

    metadata_shared_tuple = ("task064-metadata-alias",)
    metadata_distinct_tuple = tuple(["task064-metadata-alias"])
    assert metadata_shared_tuple == metadata_distinct_tuple
    assert metadata_shared_tuple is not metadata_distinct_tuple
    cross_scope_shared = baseline.replace(
        co_consts=metadata_shared_tuple,
        co_names=metadata_shared_tuple,
    )
    cross_scope_distinct = baseline.replace(
        co_consts=metadata_shared_tuple,
        co_names=metadata_distinct_tuple,
    )
    assert cross_scope_shared.co_consts is cross_scope_shared.co_names
    assert cross_scope_distinct.co_consts is not cross_scope_distinct.co_names
    assert cross_scope_shared == cross_scope_distinct
    assert fingerprint(cross_scope_shared, runtime_suffix) == fingerprint(
        cross_scope_distinct,
        runtime_suffix,
    )

    def identity_probe() -> bool:
        pair = (b"left", b"right")
        return pair[0] is pair[1]

    probe_code = identity_probe.__code__
    pair_index = next(
        index
        for index, constant in enumerate(probe_code.co_consts)
        if constant == (b"left", b"right")
    )
    shared_bytes = bytes(bytearray(b"task064-identity"))
    distinct_bytes = bytes(bytearray(shared_bytes))
    assert shared_bytes == distinct_bytes
    assert shared_bytes is not distinct_bytes
    shared_constants = list(probe_code.co_consts)
    distinct_constants = list(probe_code.co_consts)
    shared_constants[pair_index] = (shared_bytes, shared_bytes)
    distinct_constants[pair_index] = (shared_bytes, distinct_bytes)
    shared_code = probe_code.replace(co_consts=tuple(shared_constants))
    distinct_code = probe_code.replace(co_consts=tuple(distinct_constants))
    assert shared_code == distinct_code
    assert shared_code.co_consts == distinct_code.co_consts
    assert FunctionType(shared_code, {})() is True
    assert FunctionType(distinct_code, {})() is False
    assert fingerprint(shared_code, runtime_suffix) != fingerprint(
        distinct_code,
        runtime_suffix,
    )
    for candidate, expected in (
        (shared_code, True),
        (distinct_code, False),
    ):
        restored_candidate = marshal.loads(marshal.dumps(candidate))
        assert type(restored_candidate) is CodeType
        assert FunctionType(restored_candidate, {})() is expected
        assert fingerprint(restored_candidate, runtime_suffix) == fingerprint(
            candidate,
            runtime_suffix,
        )

    def code_with_unretained_leaf() -> CodeType:
        leaf = bytes(bytearray(b"task064-external-reference"))
        return (lambda: None).__code__.replace(co_consts=(None, leaf))

    unreferenced_code = code_with_unretained_leaf()
    externally_referenced_code = code_with_unretained_leaf()
    assert unreferenced_code is not externally_referenced_code
    assert unreferenced_code.co_consts == externally_referenced_code.co_consts
    assert unreferenced_code.co_consts[1] is not externally_referenced_code.co_consts[1]
    externally_referenced_leaf = externally_referenced_code.co_consts[1]
    external_references = [externally_referenced_leaf] * 1_000
    assert external_references[0] is externally_referenced_leaf
    assert fingerprint(unreferenced_code, runtime_suffix) == fingerprint(
        externally_referenced_code,
        runtime_suffix,
    )
    external_reference_control = subprocess.run(
        (
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import marshal

                def make_code():
                    leaf = bytes(bytearray(b"task064-external-reference"))
                    return (lambda: None).__code__.replace(co_consts=(None, leaf))

                code = make_code()
                before = marshal.dumps(code)
                leaf = code.co_consts[1]
                references = [leaf] * 1000
                after = marshal.dumps(code)
                assert references[0] is leaf
                assert before != after
                print("TASK064_EXTERNAL_REF_MARSHAL_CONTROL")
                """
            ),
        ),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert external_reference_control.returncode == 0
    assert external_reference_control.stdout == "TASK064_EXTERNAL_REF_MARSHAL_CONTROL\n"
    assert external_reference_control.stderr == ""

    def digest_for_constants(*constants: object) -> str:
        return fingerprint(
            baseline.replace(co_consts=constants),
            runtime_suffix,
        )

    assert digest_for_constants(("ab", "c")) != digest_for_constants(("a", "bc"))
    assert digest_for_constants(("a", "b")) != digest_for_constants(
        frozenset({"a", "b"}),
    )
    assert digest_for_constants(True) != digest_for_constants(1)
    assert digest_for_constants(-1) != digest_for_constants(1)
    assert digest_for_constants(0.0) != digest_for_constants(-0.0)
    assert digest_for_constants(1 + 2j) != digest_for_constants((1.0, 2.0))
    assert digest_for_constants("task064") != digest_for_constants(b"task064")
    assert len(digest_for_constants(Ellipsis, "\ud800")) == 64
    assert digest_for_constants(frozenset(("alpha", "beta"))) == digest_for_constants(
        frozenset(("beta", "alpha")),
    )
    first_nan = float("nan")
    second_nan = float("nan")
    ambiguous_frozenset = frozenset((first_nan, second_nan))
    assert first_nan is not second_nan
    assert first_nan != second_nan
    assert len(ambiguous_frozenset) == 2
    invalid_cache = cast(
        dict[tuple[int, str], tuple[CodeType, str]],
        inspect.getclosurevars(fingerprint).nonlocals["fingerprint_cache"],
    )
    invalid_cache_entries_before = tuple(
        (key, code, digest) for key, (code, digest) in invalid_cache.items()
    )
    with pytest.raises(harness.HarnessFailure) as ambiguous_frozenset_order:
        digest_for_constants(ambiguous_frozenset)
    assert ambiguous_frozenset_order.value.code is (
        harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    )

    class _UnsupportedInteger(int):
        pass

    unsupported_integer = _UnsupportedInteger(1)
    assert type(unsupported_integer) is _UnsupportedInteger
    for unsupported in (object(), unsupported_integer, NotImplemented):
        with pytest.raises(harness.HarnessFailure) as unsupported_constant:
            digest_for_constants(unsupported)
        assert unsupported_constant.value.code is (
            harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        )

    deep_constant: object = None
    for _ in range(65):
        deep_constant = (deep_constant,)
    with pytest.raises(harness.HarnessFailure) as excessive_depth:
        digest_for_constants(deep_constant)
    assert excessive_depth.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    shared_large_constant = b"x" * (4 * 1024 * 1024)
    with pytest.raises(harness.HarnessFailure) as excessive_materialization:
        digest_for_constants((shared_large_constant, shared_large_constant))
    assert excessive_materialization.value.code is (
        harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    )
    invalid_cache_entries_after = tuple(
        (key, code, digest) for key, (code, digest) in invalid_cache.items()
    )
    assert len(invalid_cache_entries_after) == len(invalid_cache_entries_before)
    assert all(
        after_key == before_key and after_code is before_code and after_digest == before_digest
        for (after_key, after_code, after_digest), (
            before_key,
            before_code,
            before_digest,
        ) in zip(
            invalid_cache_entries_after,
            invalid_cache_entries_before,
            strict=True,
        )
    )

    final_metrics = inspect.getclosurevars(fingerprint).nonlocals
    fingerprint_cache = cast(
        dict[tuple[int, str], tuple[CodeType, str]],
        final_metrics["fingerprint_cache"],
    )
    maximum_cached_fingerprints = cast(
        int,
        final_metrics["maximum_cached_fingerprints"],
    )
    retained_capacity_probes: list[CodeType] = []
    while len(fingerprint_cache) < maximum_cached_fingerprints:
        capacity_probe = baseline.replace(
            co_consts=(
                None,
                f"task064-cache-capacity-{len(retained_capacity_probes):04d}",
            ),
        )
        retained_capacity_probes.append(capacity_probe)
        assert len(fingerprint(capacity_probe, runtime_suffix)) == 64
    assert len(fingerprint_cache) == maximum_cached_fingerprints
    assert all(
        cache_key == (id(cached_code), cache_key[1])
        and cached_code is fingerprint_cache[cache_key][0]
        for cache_key, (cached_code, _digest) in fingerprint_cache.items()
    )
    overflow_probe = baseline.replace(co_consts=(None, "task064-cache-overflow"))
    with pytest.raises(harness.HarnessFailure) as exhausted_cache:
        fingerprint(overflow_probe, runtime_suffix)
    assert exhausted_cache.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    assert len(fingerprint_cache) == maximum_cached_fingerprints
    assert fingerprint(baseline, runtime_suffix) == baseline_digest


def test_fixture_code_replay_with_forged_globals_fails_in_new_context_and_thread(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    runtime_fixture = inspect.unwrap(_active_task064_pytest_root)
    assert type(runtime_fixture) is FunctionType
    copied_globals = dict(runtime_fixture.__globals__)
    copied_globals["__name__"] = runtime_fixture.__module__
    fixture_io_calls = 0

    def forbidden_fixture_io(*_args: object, **_kwargs: object) -> Path:
        nonlocal fixture_io_calls
        fixture_io_calls += 1
        raise AssertionError("forged fixture replay must fail before creating roots")

    copied_globals["_TEMP_PATH_MKTEMP"] = forbidden_fixture_io
    forged_fixture = FunctionType(
        runtime_fixture.__code__,
        copied_globals,
        runtime_fixture.__name__,
        runtime_fixture.__defaults__,
        runtime_fixture.__closure__,
    )
    forged_fixture.__kwdefaults__ = runtime_fixture.__kwdefaults__

    def invoke() -> None:
        generator = forged_fixture(request, tmp_path, tmp_path_factory)
        try:
            next(generator)
        finally:
            generator.close()

    with pytest.raises(harness.HarnessFailure) as empty_context_replay:
        contextvars.Context().run(invoke)
    assert empty_context_replay.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    thread_errors: list[BaseException] = []

    def invoke_in_thread() -> None:
        try:
            invoke()
        except BaseException as error:
            thread_errors.append(error)

    replay_thread = threading.Thread(target=invoke_in_thread)
    replay_thread.start()
    replay_thread.join(timeout=10)
    assert not replay_thread.is_alive()
    assert len(thread_errors) == 1
    assert isinstance(thread_errors[0], harness.HarnessFailure)
    assert thread_errors[0].code is (harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
    assert fixture_io_calls == 0


@pytest.mark.parametrize(
    "target_node_id",
    (
        (
            "tests/unit/"
            "test_task_064_continuous_public_trade_stream_sqlite_schema.py::"
            "test_descriptor_freezes_only_the_permitted_physical_projections"
        ),
        (
            "tests/integration/"
            "test_task_064_continuous_public_trade_stream_sqlite_evidence.py::"
            "test_frozen_minimum_typical_and_maximum_contract_shape_record_sizes"
        ),
    ),
)
def test_exact_fixture_rejects_same_globals_dependency_rebinding_before_begin(
    target_node_id: str,
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    program = textwrap.dedent(
        f"""
        import inspect
        import stat
        import sys
        import pytest

        target_node_id = {target_node_id!r}

        class RebindingProbe:
            def __init__(self):
                self.ran = False

            @pytest.hookimpl(tryfirst=True)
            def pytest_runtest_setup(self, item):
                if item.nodeid != target_node_id:
                    return
                assert not self.ran
                self.ran = True
                module = item.module
                harness = module.harness
                request = item._request
                factory = item.config._tmp_path_factory
                assert isinstance(request, pytest.FixtureRequest)
                assert isinstance(factory, pytest.TempPathFactory)
                real_mktemp = module._TEMP_PATH_MKTEMP
                primary_root = real_mktemp(factory, "task064-same-globals-primary")
                sibling_roots = tuple(
                    real_mktemp(factory, f"task064-same-globals-sibling-{{index}}")
                    for index in range(4)
                )
                all_roots = (primary_root, *sibling_roots)
                for root in all_roots:
                    details = root.lstat()
                    assert stat.S_ISDIR(details.st_mode)
                    assert stat.S_IMODE(details.st_mode) == 0o700
                    assert tuple(root.iterdir()) == ()

                raw_fixture = inspect.unwrap(module._active_task064_pytest_root)
                fixture_globals = raw_fixture.__globals__
                authority = inspect.getclosurevars(
                    harness._cancel_pytest_root_registration
                ).nonlocals
                permits_before = tuple(authority["permits"].values())
                active_before = authority["active_node_lifecycle"]
                tombstones_before = authority["returned_node_tombstones"]
                counters = {{
                    "mktemp": 0,
                    "begin": 0,
                    "scope": 0,
                    "cancel": 0,
                    "ticket": 0,
                }}

                def fake_mktemp(*_args, **_kwargs):
                    counters["mktemp"] += 1
                    raise AssertionError("rebound mktemp executed")

                def fake_begin(*_args, **_kwargs):
                    counters["begin"] += 1
                    raise AssertionError("rebound begin executed")

                def fake_scope(*_args, **_kwargs):
                    counters["scope"] += 1
                    raise AssertionError("rebound scope executed")

                def fake_cancel(*_args, **_kwargs):
                    counters["cancel"] += 1
                    raise AssertionError("rebound cancel executed")

                def fake_ticket(*_args, **_kwargs):
                    counters["ticket"] += 1
                    raise AssertionError("rebound child-ticket authority executed")

                mutation_groups = (
                    (
                        (
                            fixture_globals,
                            "_FIXTURE_REQUEST_TYPE",
                            object,
                        ),
                    ),
                    (
                        (
                            fixture_globals,
                            "_TEMP_PATH_FACTORY_TYPE",
                            object,
                        ),
                    ),
                    (
                        (
                            fixture_globals,
                            "_TEMP_PATH_MKTEMP",
                            fake_mktemp,
                        ),
                    ),
                    ((fixture_globals, "pytest", object()),),
                    ((fixture_globals, "harness", object()),),
                    ((fixture_globals, "inspect", object()),),
                    ((fixture_globals, "os", object()),),
                    ((fixture_globals, "contextvars", object()),),
                    ((fixture_globals, "Path", object),),
                    ((fixture_globals, "FunctionType", object),),
                    (
                        (
                            fixture_globals,
                            "_active_task064_pytest_root",
                            object(),
                        ),
                    ),
                    ((pytest.__dict__, "FixtureRequest", object),),
                    ((pytest.__dict__, "TempPathFactory", object),),
                    ((pytest.__dict__, "fixture", object()),),
                    ((inspect.__dict__, "unwrap", object()),),
                    ((module.os.__dict__, "environ", object()),),
                    ((module.contextvars.__dict__, "Context", object),),
                    (
                        (
                            harness.__dict__,
                            "_begin_pytest_root_registration",
                            fake_begin,
                        ),
                        (
                            harness.__dict__,
                            "_pytest_root_scope",
                            fake_scope,
                        ),
                        (
                            harness.__dict__,
                            "_cancel_pytest_root_registration",
                            fake_cancel,
                        ),
                        (
                            harness.__dict__,
                            "_authenticate_task064_child_provenance",
                            fake_ticket,
                        ),
                        (
                            harness.__dict__,
                            "_activate_task064_child_provenance",
                            fake_ticket,
                        ),
                        (
                            harness.__dict__,
                            "_claim_task064_post_return_child_provenance",
                            fake_ticket,
                        ),
                        (
                            harness.__dict__,
                            "_claim_task064_child_dispatch_provenance",
                            fake_ticket,
                        ),
                        (
                            harness.__dict__,
                            "_finish_task064_child_provenance",
                            fake_ticket,
                        ),
                        (
                            harness.__dict__,
                            "_cancel_task064_child_provenance",
                            fake_ticket,
                        ),
                    ),
                    ((harness.__dict__, "HarnessFailure", object),),
                    ((harness.__dict__, "HarnessFailureCode", object),),
                )
                for mutations in mutation_groups:
                    originals = [
                        (mapping, name, mapping[name])
                        for mapping, name, _replacement in mutations
                    ]
                    generator = None
                    try:
                        for mapping, name, replacement in mutations:
                            mapping[name] = replacement
                        generator = raw_fixture(
                            request,
                            primary_root,
                            factory,
                        )
                        try:
                            next(generator)
                        except RuntimeError as error:
                            assert str(error) == (
                                "invalid TASK064 pytest fixture dependency seal"
                            )
                        else:
                            raise AssertionError(
                                "same-globals rebinding minted fixture authority"
                            )
                    finally:
                        if generator is not None:
                            generator.close()
                        for mapping, name, original in reversed(originals):
                            mapping[name] = original

                assert counters == {{
                    "mktemp": 0,
                    "begin": 0,
                    "scope": 0,
                    "cancel": 0,
                    "ticket": 0,
                }}
                assert tuple(authority["permits"].values()) == permits_before
                assert authority["active_node_lifecycle"] is active_before
                assert authority["returned_node_tombstones"] == tombstones_before
                assert not [
                    record
                    for record in authority["permits"].values()
                    if record["node_id"] == target_node_id
                ]
                for root in all_roots:
                    assert tuple(root.iterdir()) == ()

        probe = RebindingProbe()
        exit_code = pytest.main(
            ["-q", "-p", "no:cacheprovider", target_node_id],
            plugins=[probe],
        )
        assert exit_code == pytest.ExitCode.OK
        assert probe.ran
        """
    )
    environment, pycache_prefix = _fresh_subprocess_pycache_environment(
        tmp_path,
        label="task064-same-globals-rebinding-pycache",
    )
    completed = subprocess.run(
        (sys.executable, "-c", program),
        cwd=repository_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert not pycache_prefix.exists()
    assert completed.returncode == 0, (
        f"same-globals fixture rebinding probe failed for {target_node_id!r}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def test_exact_fixture_callable_replay_is_rejected_after_real_lifecycle_return(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    node_ids = (
        (
            "tests/unit/"
            "test_task_064_continuous_public_trade_stream_sqlite_schema.py::"
            "test_descriptor_freezes_only_the_permitted_physical_projections"
        ),
        (
            "tests/integration/"
            "test_task_064_continuous_public_trade_stream_sqlite_evidence.py::"
            "test_post_return_ticket_is_claimed_locally_once"
        ),
    )
    binding_target_node_id = node_ids[0]
    inventory_before_invalid_binding = tuple(tmp_path.iterdir())
    for issuer_node_id, target_node_id, mode in (
        (
            f"{request.node.nodeid}::wrong-issuer",
            binding_target_node_id,
            binding_target_node_id,
        ),
        (request.node.nodeid, "task064-wrong-target", "task064-wrong-target"),
        (
            request.node.nodeid,
            binding_target_node_id,
            f"{binding_target_node_id}::wrong-mode",
        ),
    ):
        with pytest.raises(harness.HarnessFailure) as invalid_binding:
            harness._issue_task064_child_provenance(
                tmp_path,
                "post_return_fixture_replay",
                issuer_node_id,
                target_node_id,
                mode,
            )
        assert invalid_binding.value.code is (harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT)
        assert tuple(tmp_path.iterdir()) == inventory_before_invalid_binding

    repository_root = Path(__file__).resolve().parents[2]
    for wrong_packet in ("protocol", "target", "mode"):
        environment, pycache_prefix = _fresh_subprocess_pycache_environment(
            tmp_path,
            label=f"task064-wrong-packet-{wrong_packet}-pycache",
        )
        provenance = harness._issue_task064_child_provenance(
            tmp_path,
            "post_return_fixture_replay",
            request.node.nodeid,
            binding_target_node_id,
            binding_target_node_id,
        )
        packet = json.loads(os.pread(provenance.descriptor, 8_192, 0))
        if wrong_packet == "protocol":
            packet["protocol"] = "exec_isolation"
            packet["legacy_environment"] = "WEALTH_TASK064_EXEC_ISOLATION"
        elif wrong_packet == "target":
            packet["target_node_id"] = node_ids[1]
            packet["mode"] = node_ids[1]
        else:
            packet["mode"] = f"{binding_target_node_id}::wrong-mode"
        forged_packet = json.dumps(
            packet,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        os.ftruncate(provenance.descriptor, 0)
        assert os.pwrite(provenance.descriptor, forged_packet, 0) == len(forged_packet)
        os.fsync(provenance.descriptor)
        envelope = json.loads(provenance.envelope)
        envelope["packet_sha256"] = hashlib.sha256(forged_packet).hexdigest()
        environment[harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT] = json.dumps(
            envelope,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        program = textwrap.dedent(
            f"""
            import sys
            import pytest
            import tests.support.continuous_public_trade_stream_sqlite_harness as harness

            target_node_id = {binding_target_node_id!r}
            observed = []
            body_called = False
            watched = {{
                harness._activate_task064_child_provenance.__code__: "activate",
                harness._claim_task064_post_return_child_provenance.__code__: "claim",
                harness._begin_pytest_root_registration.__code__: "begin",
                harness._pytest_root_scope.__code__: "scope",
                harness._cancel_pytest_root_registration.__code__: "permit-cancel",
                harness._finish_task064_child_provenance.__code__: "ticket-finish",
                harness._cancel_task064_child_provenance.__code__: "ticket-cancel",
            }}
            mktemp_code = pytest.TempPathFactory.mktemp.__code__

            def profile(frame, event, arg):
                del arg
                if event != "call":
                    return
                if frame.f_code is mktemp_code:
                    basename = frame.f_locals.get("basename")
                    if isinstance(basename, str) and basename.startswith("task064-"):
                        observed.append("mktemp:" + basename)
                    return
                name = watched.get(frame.f_code)
                if name is not None:
                    observed.append(name)

            class PreBeginProbe:
                checked = False

                @pytest.hookimpl(trylast=True)
                def pytest_configure(self, config):
                    del config
                    sys.setprofile(profile)

                @pytest.hookimpl(tryfirst=True)
                def pytest_runtest_call(self, item):
                    global body_called
                    if item.nodeid == target_node_id:
                        body_called = True

                @pytest.hookimpl(trylast=True)
                def pytest_sessionfinish(self, session, exitstatus):
                    del session, exitstatus
                    sys.setprofile(None)
                    assert observed == []
                    assert body_called is False
                    self.checked = True
                    print("TASK064_WRONG_PACKET_PREBEGIN_OK:{wrong_packet}")

            probe = PreBeginProbe()
            exit_code = pytest.main(
                ["-q", "-s", "-p", "no:cacheprovider", target_node_id],
                plugins=[probe],
            )
            assert exit_code != pytest.ExitCode.OK
            assert probe.checked
            """
        )
        try:
            completed = subprocess.run(
                (sys.executable, "-c", program),
                cwd=repository_root,
                env=environment,
                pass_fds=(provenance.descriptor,),
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
        finally:
            packet_consumed = harness._close_task064_child_provenance(provenance)
        assert not packet_consumed
        assert not provenance.marker_path.exists()
        assert not pycache_prefix.exists()
        assert completed.returncode == 0, (
            f"wrong-packet pre-BEGIN probe failed for {wrong_packet!r}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
        assert f"TASK064_WRONG_PACKET_PREBEGIN_OK:{wrong_packet}" in completed.stdout

    for node_id in node_ids:
        completed, _ = _run_task064_pytest_child(
            tmp_path,
            protocol="post_return_fixture_replay",
            issuer_node_id=request.node.nodeid,
            target_node_id=node_id,
            mode=node_id,
            pycache_label="task064-exact-replay-pycache",
            timeout_seconds=120,
        )
        assert completed.returncode == 0, (
            f"post-return exact fixture replay probe failed for {node_id!r}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def test_adr0032_keeps_concrete_guards_inside_the_controlled_process_boundary() -> None:
    adr_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "decisions"
        / "0032-continuous-public-trade-stream-sqlite-schema-evidence-harness.md"
    )
    decision = adr_path.read_text(encoding="utf-8")
    for required in (
        "private closure cells",
        "closure-owned authority",
        "debugger state",
        "`ctypes`",
        "raw process memory",
        "forged or mutated caller-held capabilities",
        "direct exported",
        "module-global substitution",
        "fixture/node/path/PID/UID/mode/nonce",
        "revocation or cleanup failure",
        "filesystem identity and ownership failure",
        "production blocker",
    ):
        assert required in decision
    assert "controlled pytest/cooperating-process threat model" in decision
    assert "never `PASS`" in decision


@pytest.mark.parametrize(
    "failure_mode",
    (
        "scope_construction",
        "scope_entry",
        "partial_activation",
        "cancellation_uncertainty",
    ),
)
def test_fixture_setup_failure_cancels_exact_permit_and_retains_tombstone(
    failure_mode: str,
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).resolve().parents[2]
    first_node = (
        "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py::"
        "test_descriptor_freezes_only_the_permitted_physical_projections"
    )
    second_node = (
        "tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py::"
        "test_natural_identity_key_is_strict_reversible_and_length_delimited"
    )
    program = textwrap.dedent(
        f"""
        import inspect
        import sys
        import pytest

        mode = {failure_mode!r}
        first_node = {first_node!r}
        second_node = {second_node!r}

        class Recorder:
            def __init__(self):
                self.passed_calls = []
                self.failed_setups = []
                self.module = None
                self.harness = None

            def pytest_collection_finish(self, session):
                del session
                module = sys.modules[
                    "tests.unit.test_task_064_continuous_public_trade_stream_sqlite_schema"
                ]
                harness = module.harness
                self.module = module
                self.harness = harness

                fault_for_mode = {{
                    "scope_construction": "permit_scope_construction",
                    "scope_entry": "permit_scope_entry",
                    "partial_activation": "permit_partial_activation",
                }}.get(mode)
                if mode == "cancellation_uncertainty":
                    harness._arm_pytest_root_authority_fault(
                        "permit_scope_construction"
                    )
                    harness._arm_pytest_root_authority_fault("permit_cancel")
                elif fault_for_mode is not None:
                    harness._arm_pytest_root_authority_fault(fault_for_mode)

            def pytest_runtest_logreport(self, report):
                if report.when == "call" and report.passed:
                    self.passed_calls.append(report.nodeid)
                if report.when == "setup" and report.failed:
                    self.failed_setups.append(report.nodeid)

        recorder = Recorder()
        exit_code = pytest.main(
            ["-q", "-p", "no:cacheprovider", first_node, second_node],
            plugins=[recorder],
        )
        module = recorder.module
        harness = recorder.harness
        assert module is not None
        assert harness is not None
        cancel_closure = inspect.getclosurevars(
            harness._cancel_pytest_root_registration
        ).nonlocals
        permits = tuple(cancel_closure["permits"].values())
        first_records = [
            record for record in permits if record["node_id"] == first_node
        ]
        assert len(first_records) == 1
        first_record = first_records[0]
        assert cancel_closure["active_node_lifecycle"] is None
        assert (
            first_record["owner_process_id"],
            first_record["node_id"],
        ) in cancel_closure["returned_node_tombstones"]
        assert first_node in recorder.failed_setups
        assert exit_code == pytest.ExitCode.TESTS_FAILED

        if mode == "cancellation_uncertainty":
            assert first_record["state"] == "CANCEL_UNCERTAIN"
            assert second_node in recorder.failed_setups
            assert not recorder.passed_calls
            assert harness._pytest_root_authority_uncertain()
        else:
            assert first_record["state"] == "CANCELLED"
            assert recorder.passed_calls == [second_node]
            assert second_node not in recorder.failed_setups
            second_records = [
                record for record in permits if record["node_id"] == second_node
            ]
            assert len(second_records) == 1
            assert second_records[0]["state"] == "RETURNED"
            try:
                harness._pytest_root_scope(first_record["permit"], ())
            except harness.HarnessFailure as error:
                assert (
                    error.code
                    is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
                )
            else:
                raise AssertionError("cancelled permit remained activatable")
        """
    )
    environment, pycache_prefix = _fresh_subprocess_pycache_environment(
        tmp_path,
        label="task064-cancellation-pycache",
    )
    completed = subprocess.run(
        (sys.executable, "-c", program),
        cwd=repository_root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert not pycache_prefix.exists()
    assert completed.returncode == 0, (
        f"fixture cancellation probe {failure_mode!r} failed\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def test_evidence_normalization_rejects_cycles_depth_and_mapping_subclasses(
    tmp_path: Path,
) -> None:
    registration = harness._lookup_active_pytest_root(tmp_path)
    assert registration is not None
    before_inventory = tuple(tmp_path.iterdir())

    cyclic: list[object] = []
    cyclic.append(cyclic)
    hostile = _HostileMapping()
    for candidate in (cyclic, hostile):
        with pytest.raises(harness.HarnessFailure) as digest_failure:
            harness._evidence_payload_digest(candidate)
        assert digest_failure.value.code is harness.HarnessFailureCode.CORRUPT

        with pytest.raises(harness.HarnessFailure) as collection_failure:
            harness._collect_evidence_registrations(candidate, [], [])
        assert collection_failure.value.code is harness.HarnessFailureCode.CORRUPT

        with pytest.raises(harness.HarnessFailure) as registration_failure:
            harness._validate_collector_value_registration(
                candidate,
                registration,
            )
        assert registration_failure.value.code is harness.HarnessFailureCode.CORRUPT

        with pytest.raises(harness.HarnessFailure) as authority_failure:
            harness._validate_collector_value_authority(
                candidate,
                registration,
            )
        assert authority_failure.value.code is harness.HarnessFailureCode.CORRUPT

    assert hostile.items_calls == 0
    depth_64: object = "leaf"
    for _ in range(64):
        depth_64 = [depth_64]
    assert len(harness._evidence_payload_digest(depth_64)) == 64
    depth_65: object = [depth_64]
    with pytest.raises(harness.HarnessFailure) as depth_failure:
        harness._evidence_payload_digest(depth_65)
    assert depth_failure.value.code is harness.HarnessFailureCode.CORRUPT

    shared = ["shared"]
    aliased_dag = [shared, shared]
    assert harness._evidence_payload_digest(aliased_dag) == (
        harness._evidence_payload_digest([["shared"], ["shared"]])
    )
    assert tuple(tmp_path.iterdir()) == before_inventory
    assert not (tmp_path / "task064-evidence.json").exists()
    assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))


def test_token_and_root_authority_rejects_clones_collisions_and_module_mirrors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    registered = harness._lookup_store_token_authority(token)
    assert registered is not None
    snapshot = harness._token_authority_snapshot()
    view = snapshot[token._nonce]
    with pytest.raises(TypeError):
        cast(Any, snapshot)[token._nonce] = view
    assert harness._token_authority_snapshot()[token._nonce] == view
    assert harness._token_authority_snapshot()[token._nonce] is not view

    same_nonce_clone = replace(token)
    assert same_nonce_clone._nonce == token._nonce
    assert same_nonce_clone is not token
    with pytest.raises(harness.HarnessFailure) as cloned:
        harness.verify_store(same_nonce_clone)
    assert cloned.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    with pytest.raises(harness.HarnessFailure) as cloned_cleanup:
        harness._remove_owned_files(same_nonce_clone)
    assert cloned_cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert harness._token_authority_snapshot()[token._nonce] == view

    before_inventory = tuple(sorted(path.name for path in tmp_path.iterdir()))
    before_authority = dict(harness._token_authority_snapshot())
    monkeypatch.setattr(secrets, "token_bytes", lambda _size: token._nonce)
    captured_entropy_token = harness.bootstrap_store(tmp_path)
    assert captured_entropy_token._nonce != token._nonce
    assert captured_entropy_token._nonce not in before_authority
    harness._remove_owned_files(captured_entropy_token)
    monkeypatch.undo()
    assert tuple(sorted(path.name for path in tmp_path.iterdir())) == before_inventory
    assert dict(harness._token_authority_snapshot()) == before_authority

    short_nonce_clone = replace(token, _nonce=b"short-nonce")
    with pytest.raises(harness.HarnessFailure) as short_require:
        harness.verify_store(short_nonce_clone)
    assert short_require.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    with pytest.raises(harness.HarnessFailure) as short_cleanup:
        harness._remove_owned_files(short_nonce_clone)
    assert short_cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE

    first_registered_view = harness._lookup_store_token_authority(token)
    second_registered_view = harness._lookup_store_token_authority(token)
    assert first_registered_view == second_registered_view
    assert first_registered_view is not second_registered_view
    assert first_registered_view is not None
    original_registered_inode = first_registered_view.inode
    object.__setattr__(first_registered_view, "inode", -1)
    assert harness._lookup_store_token_authority(token) == second_registered_view
    assert harness.verify_store(token).stream_count == 0
    object.__setattr__(first_registered_view, "inode", original_registered_inode)

    unauthorized_issuer_name = "_begin_pytest_root_registration"
    exact_node_id = (
        "tests/integration/"
        "test_task_064_continuous_public_trade_stream_sqlite_evidence.py::"
        "test_token_and_root_authority_rejects_clones_collisions_and_module_mirrors"
    )
    with pytest.raises(harness.HarnessFailure) as second_session:
        getattr(harness, unauthorized_issuer_name)(
            exact_node_id,
            tmp_path,
        )
    assert second_session.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    foreign_fixture_source = """
def _active_task064_pytest_root(node_id, pytest_root):
    marker = "foreign-code-object"
    del marker
    return harness._begin_pytest_root_registration(node_id, pytest_root)
"""
    expected_module_name = (
        "tests.integration.test_task_064_continuous_public_trade_stream_sqlite_evidence"
    )
    for forged_filename in (
        str(Path(__file__).resolve()),
        str(
            tmp_path
            / "copied-checkout"
            / "tests"
            / "integration"
            / "test_task_064_continuous_public_trade_stream_sqlite_evidence.py"
        ),
    ):
        foreign_namespace: dict[str, object] = {
            "__name__": expected_module_name,
            "harness": harness,
        }
        exec(
            compile(foreign_fixture_source, forged_filename, "exec"),
            foreign_namespace,
        )
        foreign_fixture = cast(
            Callable[[str, Path], object],
            foreign_namespace["_active_task064_pytest_root"],
        )
        with pytest.raises(harness.HarnessFailure) as foreign_callsite:
            foreign_fixture(exact_node_id, tmp_path)
        assert foreign_callsite.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    cloned_capability = replace(_active_task064_pytest_root)
    assert cloned_capability == _active_task064_pytest_root
    assert cloned_capability is not _active_task064_pytest_root
    with pytest.raises(harness.HarnessFailure) as cloned_capability_revoke:
        harness._revoke_fixture_root(
            cloned_capability,
            _active_task064_pytest_root.roots[4],
        )
    assert cloned_capability_revoke.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    capability_probe = harness.bootstrap_store(_active_task064_pytest_root.roots[4])
    harness._remove_owned_files(capability_probe)

    original_capability_roots = _active_task064_pytest_root.roots
    original_capability_nonce = _active_task064_pytest_root._nonce
    try:
        object.__setattr__(
            _active_task064_pytest_root,
            "roots",
            (
                original_capability_roots[0],
                original_capability_roots[0],
                *original_capability_roots[2:],
            ),
        )
        object.__setattr__(_active_task064_pytest_root, "_nonce", b"mutated")
        with pytest.raises(harness.HarnessFailure) as mutated_capability_revoke:
            harness._revoke_fixture_root(
                _active_task064_pytest_root,
                original_capability_roots[4],
            )
        assert (
            mutated_capability_revoke.value.code
            is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        )
        with pytest.raises(harness.HarnessFailure) as mutated_capability_token:
            harness.verify_store(token)
        assert mutated_capability_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    finally:
        object.__setattr__(
            _active_task064_pytest_root,
            "roots",
            original_capability_roots,
        )
        object.__setattr__(
            _active_task064_pytest_root,
            "_nonce",
            original_capability_nonce,
        )
    assert harness.verify_store(token).stream_count == 0
    mutated_capability_probe = harness.bootstrap_store(original_capability_roots[4])
    harness._remove_owned_files(mutated_capability_probe)

    active_root = harness._lookup_active_pytest_root(tmp_path)
    assert active_root is not None
    original_process_id = active_root.process_id
    object.__setattr__(active_root, "process_id", -1)
    try:
        with pytest.raises(harness.HarnessFailure) as mutated_root:
            harness.bootstrap_store(tmp_path)
        assert mutated_root.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
    finally:
        object.__setattr__(active_root, "process_id", original_process_id)
    assert harness.verify_store(token).stream_count == 0

    monkeypatch.setattr(os, "getpid", lambda: -1)
    monkeypatch.setattr(os, "getppid", lambda: -1)
    monkeypatch.setattr(os, "getuid", lambda: -1)
    assert harness._lookup_active_pytest_root(tmp_path) is active_root
    assert harness._pytest_root_session_owns(active_root, False)
    monkeypatch.undo()
    assert harness.verify_store(token).stream_count == 0

    def missing_context_capability() -> None:
        with pytest.raises(harness.HarnessFailure) as missing_context:
            harness.bootstrap_store(tmp_path)
        assert missing_context.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

    contextvars.Context().run(missing_context_capability)

    harness.__dict__["_TOKEN_REGISTRY"] = {token._nonce: registered}
    harness.__dict__["_ACTIVE_PYTEST_ROOTS"] = {Path(str(tmp_path)): object()}
    try:
        with pytest.raises(harness.HarnessFailure) as mirrored_token:
            harness.verify_store(same_nonce_clone)
        assert mirrored_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
        assert harness.verify_store(token).stream_count == 0
    finally:
        harness.__dict__.pop("_TOKEN_REGISTRY")
        harness.__dict__.pop("_ACTIVE_PYTEST_ROOTS")
    assert harness.verify_store(token).stream_count == 0


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
    assert harness._connection_authority_state(connection) == "CLOSED"
    connection.close()
    assert not harness._has_fork_unsafe_connection_authority()


@pytest.mark.parametrize("name", ("store.sqlite3-wal", "store.sqlite3-shm"))
def test_optional_live_snapshot_seals_reject_every_namespace_drift(
    tmp_path: Path,
    name: str,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    optional_path = token._generation_root / name
    retained_path = tmp_path / f"{token._generation_root.name}-{name}-retained"
    hardlink_path = tmp_path / f"{token._generation_root.name}-{name}-hardlink"
    optional_path.unlink(missing_ok=True)
    identity = harness._require_token(token)
    snapshot = harness._open_operation_path_snapshot(identity)

    def require_unavailable() -> None:
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._revalidate_operation_path_snapshot(identity, snapshot)
        assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE

    try:
        harness._revalidate_operation_path_snapshot(identity, snapshot)
        descriptor = os.open(
            optional_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        harness._revalidate_operation_path_snapshot(identity, snapshot)

        optional_path.chmod(0o400)
        require_unavailable()
        optional_path.chmod(0o600)
        harness._revalidate_operation_path_snapshot(identity, snapshot)

        os.link(optional_path, hardlink_path)
        require_unavailable()
        hardlink_path.unlink()
        harness._revalidate_operation_path_snapshot(identity, snapshot)

        optional_path.rename(retained_path)
        require_unavailable()
        descriptor = os.open(
            optional_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        require_unavailable()
        optional_path.unlink()
        optional_path.symlink_to(retained_path)
        require_unavailable()
        optional_path.unlink()
        retained_path.rename(optional_path)
        harness._revalidate_operation_path_snapshot(identity, snapshot)
    finally:
        hardlink_path.unlink(missing_ok=True)
        if optional_path.is_symlink():
            optional_path.unlink()
        if retained_path.exists():
            optional_path.unlink(missing_ok=True)
            retained_path.rename(optional_path)
        harness._close_operation_path_snapshot(snapshot)


def test_live_snapshot_rejects_root_generation_and_main_replacement(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    identity = harness._require_token(token)
    snapshot = harness._open_operation_path_snapshot(identity)
    retained_root = tmp_path.with_name(f"{tmp_path.name}-retained-root")
    generation_path = token._generation_root
    retained_generation = generation_path.with_name(f"{generation_path.name}-retained")
    database_path = token._database_path
    retained_database = database_path.with_name(f"{database_path.name}-retained")

    def require_unavailable() -> None:
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._revalidate_operation_path_snapshot(identity, snapshot)
        assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE

    try:
        tmp_path.rename(retained_root)
        tmp_path.mkdir(mode=identity.pytest_root_mode)
        require_unavailable()
        tmp_path.rmdir()
        retained_root.rename(tmp_path)
        harness._revalidate_operation_path_snapshot(identity, snapshot)

        generation_path.rename(retained_generation)
        generation_path.symlink_to(retained_generation, target_is_directory=True)
        require_unavailable()
        generation_path.unlink()
        retained_generation.rename(generation_path)
        harness._revalidate_operation_path_snapshot(identity, snapshot)

        database_path.rename(retained_database)
        descriptor = os.open(
            database_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        require_unavailable()
        database_path.unlink()
        retained_database.rename(database_path)
        harness._revalidate_operation_path_snapshot(identity, snapshot)
    finally:
        if database_path.exists() and retained_database.exists():
            database_path.unlink()
        if retained_database.exists():
            retained_database.rename(database_path)
        if generation_path.is_symlink():
            generation_path.unlink()
        if retained_generation.exists():
            retained_generation.rename(generation_path)
        if retained_root.exists():
            if tmp_path.exists():
                tmp_path.rmdir()
            retained_root.rename(tmp_path)
        harness._close_operation_path_snapshot(snapshot)


def test_transaction_final_alias_walk_rejects_captured_ancestor_symlink(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    ancestor = tmp_path.parent
    retained_ancestor = ancestor.with_name(f"{ancestor.name}-task064-alias-retained-{os.getpid()}")
    exact_record = inspect.getclosurevars(harness._consume_connection_immutable_runtime).nonlocals[
        "exact_runtime_record"
    ]
    records = cast(
        dict[int, dict[str, object]],
        inspect.getclosurevars(exact_record).nonlocals["connection_records"],
    )
    alias_paths = cast(tuple[str, ...], records[id(connection)]["alias_paths"])
    assert str(ancestor) in alias_paths
    assert alias_paths.index(str(ancestor)) < len(alias_paths) - 3
    assert not retained_ancestor.exists()
    real_verify_schema_identity = harness._verify_schema_identity
    real_pinned_check = harness._revalidate_connection_path
    real_alias_check = harness._revalidate_connection_alias_free_path
    schema_calls = 0
    pinned_calls = 0
    pinned_completed = 0
    alias_calls = 0
    alias_completed = 0

    def mutate_ancestor_after_schema(
        observed_connection: sqlite3.Connection,
    ) -> str:
        nonlocal schema_calls
        schema_calls += 1
        fingerprint = real_verify_schema_identity(observed_connection)
        ancestor.rename(retained_ancestor)
        ancestor.symlink_to(retained_ancestor, target_is_directory=True)
        return fingerprint

    def observed_pinned_check(
        identity: harness._RegisteredIdentity,
        observed_connection: sqlite3.Connection,
    ) -> None:
        nonlocal pinned_calls, pinned_completed
        pinned_calls += 1
        real_pinned_check(identity, observed_connection)
        pinned_completed += 1

    def observed_alias_check(
        identity: harness._RegisteredIdentity,
        observed_connection: sqlite3.Connection,
    ) -> None:
        nonlocal alias_calls, alias_completed
        alias_calls += 1
        real_alias_check(identity, observed_connection)
        alias_completed += 1

    try:
        connection.execute("BEGIN IMMEDIATE").close()
        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(harness.HarnessFailure) as rejected,
        ):
            patch.setattr(harness, "_verify_schema_identity", mutate_ancestor_after_schema)
            patch.setattr(harness, "_revalidate_connection_path", observed_pinned_check)
            patch.setattr(
                harness,
                "_revalidate_connection_alias_free_path",
                observed_alias_check,
            )
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert schema_calls == 1
        assert (pinned_calls, pinned_completed) == (2, 2)
        assert (alias_calls, alias_completed) == (1, 0)
    finally:
        if ancestor.is_symlink():
            ancestor.unlink()
        if retained_ancestor.exists():
            retained_ancestor.rename(ancestor)
        if connection.in_transaction:
            harness._verify_operation_authority(connection, token)
            connection.execute("ROLLBACK").close()
        connection.close()


@pytest.mark.parametrize(
    "target",
    ("generation_alias", "wal_inode_replacement"),
)
def test_real_final_precommit_authority_rolls_back_late_namespace_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    policy, creation = _creation(seed=79)
    token = harness.bootstrap_store(tmp_path)
    generation_path = token._generation_root
    retained_generation = generation_path.with_name(f"{generation_path.name}-precommit-retained")
    wal_path = generation_path / "store.sqlite3-wal"
    retained_wal = tmp_path / f"{generation_path.name}-wal-precommit-retained"
    real_snapshot_verifier = harness._verify_operation_snapshot
    real_final_authority = harness._verify_operation_authority
    real_execute = harness._MeteredConnection.execute
    real_rollback = harness._rollback_best_effort
    snapshot_completed = 0
    target_seam_calls = 0
    commit_calls = 0
    rollback_sql_calls = 0
    rollback_calls = 0
    rollback_clean = False

    def observed_snapshot_verifier(
        connection: sqlite3.Connection,
        token_value: harness.StoreToken,
        *,
        writer: bool,
    ) -> str:
        nonlocal snapshot_completed
        fingerprint = real_snapshot_verifier(
            connection,
            token_value,
            writer=writer,
        )
        snapshot_completed += 1
        return fingerprint

    def observed_execute(
        connection: harness._MeteredConnection,
        sql: str,
        parameters: Any = (),
        /,
    ) -> sqlite3.Cursor:
        nonlocal commit_calls, rollback_sql_calls
        if sql == "COMMIT":
            commit_calls += 1
        elif sql == "ROLLBACK":
            rollback_sql_calls += 1
        return real_execute(connection, sql, parameters)

    def observed_rollback(connection: sqlite3.Connection) -> None:
        nonlocal rollback_calls, rollback_clean
        rollback_calls += 1
        real_rollback(connection)
        rollback_clean = not connection.in_transaction

    def replace_namespace_at_final_seam(seam: str) -> None:
        nonlocal target_seam_calls
        if seam != "between_creation_insert_and_create_commit":
            return
        target_seam_calls += 1
        assert snapshot_completed == 1
        if target == "generation_alias":
            assert not retained_generation.exists()
            generation_path.rename(retained_generation)
            generation_path.symlink_to(retained_generation, target_is_directory=True)
            return
        assert target == "wal_inode_replacement"
        assert not retained_wal.exists()
        original_wal = wal_path.stat(follow_symlinks=False)
        wal_path.rename(retained_wal)
        descriptor = os.open(
            wal_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        replacement_wal = wal_path.stat(follow_symlinks=False)
        assert (replacement_wal.st_dev, replacement_wal.st_ino) != (
            original_wal.st_dev,
            original_wal.st_ino,
        )

    try:
        monkeypatch.setattr(
            harness,
            "_verify_operation_snapshot",
            observed_snapshot_verifier,
        )
        monkeypatch.setattr(harness._MeteredConnection, "execute", observed_execute)
        monkeypatch.setattr(harness, "_rollback_best_effort", observed_rollback)
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness.create_stream(
                token,
                creation,
                policy,
                seam_hook=replace_namespace_at_final_seam,
            )
        assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert harness._verify_operation_authority is real_final_authority
        assert snapshot_completed == 1
        assert target_seam_calls == 1
        assert commit_calls == 0
        assert rollback_calls == 1
        assert rollback_sql_calls == 1
        assert rollback_clean
    finally:
        monkeypatch.undo()
        if generation_path.is_symlink():
            generation_path.unlink()
        if retained_generation.exists():
            retained_generation.rename(generation_path)
        if retained_wal.exists():
            wal_path.unlink(missing_ok=True)
            retained_wal.rename(wal_path)
    summary = harness.verify_store(token)
    assert summary.stream_count == 0
    assert summary.history_count == 0


def test_transaction_verifier_ignores_mutated_detached_runtime_and_live_views(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, profile = harness._connect(token, writer=True)
    try:
        connection.execute("BEGIN IMMEDIATE").close()
        issued_view = harness._require_live_transaction_authority(
            connection,
            token,
            True,
        )
        copied_view = replace(
            issued_view,
            registered=replace(issued_view.registered),
        )
        object.__setattr__(profile, "role", "forged")
        object.__setattr__(profile, "sqlite_source_id", "forged")
        object.__setattr__(profile, "compile_options", ("FORGED",))
        object.__setattr__(
            copied_view.registered,
            "database_path",
            Path("/forged/store.sqlite3"),
        )
        object.__setattr__(copied_view, "database_list_path", "/forged/store.sqlite3")

        assert harness._verify_operation_snapshot(connection, token, writer=True)
        fresh_view = harness._require_live_transaction_authority(
            connection,
            token,
            True,
        )
        assert fresh_view is not issued_view
        assert fresh_view is not copied_view
        assert fresh_view.registered is not copied_view.registered
        assert fresh_view.registered.database_path == token._database_path
        assert fresh_view.database_list_path == str(token._database_path)
        assert profile.role == "forged"
        assert profile.sqlite_source_id == "forged"
        assert copied_view.database_list_path == "/forged/store.sqlite3"
        connection.execute("ROLLBACK").close()
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_pytest_root_registration_expires_and_rejects_wrong_process_or_node(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    scoped_root = _active_task064_pytest_root.roots[1]
    with contextlib.nullcontext():
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
        monkeypatch.setattr(os, "getppid", lambda: -1)
        module_pid_probe = harness.bootstrap_store(scoped_root)
        assert harness.verify_store(token).stream_count == 0
        harness._remove_owned_files(module_pid_probe)
        monkeypatch.undo()

        def missing_context_authority() -> None:
            with pytest.raises(harness.HarnessFailure) as wrong_node:
                harness.bootstrap_store(scoped_root)
            assert wrong_node.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
            with pytest.raises(harness.HarnessFailure) as wrong_token_node:
                harness.verify_store(token)
            assert wrong_token_node.value.code is harness.HarnessFailureCode.INVALID_TOKEN

        contextvars.Context().run(missing_context_authority)
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
        harness._revoke_fixture_root(_active_task064_pytest_root, scoped_root)
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


def test_inflight_create_rolls_back_when_fixture_authority_expires(
    tmp_path: Path,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    scoped_root = _active_task064_pytest_root.roots[1]
    policy, creation = _creation(seed=77)
    with contextlib.nullcontext():
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
        harness._revoke_fixture_root(_active_task064_pytest_root, scoped_root)

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


def test_root_revocation_flush_and_callback_failures_are_fail_closed(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    probe_modes = (
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
    )
    probe_mode, dispatch_modes = _task064_child_dispatch_plan(
        "root_latch",
        request.node.nodeid,
        probe_modes,
    )
    if probe_mode is None:
        assert dispatch_modes == probe_modes
        for mode in dispatch_modes:
            completed, _ = _run_task064_pytest_child(
                tmp_path,
                protocol="root_latch",
                issuer_node_id=request.node.nodeid,
                target_node_id=request.node.nodeid,
                mode=mode,
                pycache_label="task064-root-latch-pycache",
                timeout_seconds=180,
            )
            assert completed.returncode == 0, (
                f"{mode} isolated latch probe failed\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        return

    assert probe_mode in probe_modes
    assert dispatch_modes == ()
    poison_root = _active_task064_pytest_root.roots[2]
    survivor_root = _active_task064_pytest_root.roots[3]
    poison_token = harness.bootstrap_store(poison_root)
    survivor_token = harness.bootstrap_store(survivor_root)
    policy, creation = _creation(seed=7_464)
    transition = _retain(creation, policy, reason="revocation-uncertainty-latch")
    assert (
        harness.create_stream(survivor_token, creation, policy).classification
        is harness.StoreClassification.INSERTED
    )

    ready_read, ready_write = os.pipe()
    control_read, control_write = os.pipe()
    result_read, result_write = os.pipe()
    child_process_id = os.fork()
    if child_process_id == 0:
        os.close(ready_read)
        os.close(control_write)
        os.close(result_read)
        packet = b"E"
        try:
            os.write(ready_write, b"R")
            if os.read(control_read, 1) != b"C":
                os._exit(71)
            if probe_mode in {
                "poison_mmap_write",
                "poison_mmap_flush",
                "poison_pipe_write",
            }:
                mmap_poisoned, pipe_poisoned = harness._pytest_root_poison_channels()
                expected_channel_observed = (
                    pipe_poisoned
                    if probe_mode in {"poison_mmap_write", "poison_mmap_flush"}
                    else mmap_poisoned
                )
                if expected_channel_observed:
                    packet = b"D"
            else:
                global_uncertainty_observed = harness._pytest_root_authority_uncertain()
                with pytest.raises(harness.HarnessFailure) as revoked:
                    harness.verify_store(survivor_token)
                if (
                    global_uncertainty_observed
                    and revoked.value.code is harness.HarnessFailureCode.INVALID_TOKEN
                ):
                    packet = b"I"
        except BaseException:
            packet = b"E"
        with contextlib.suppress(OSError):
            os.write(result_write, packet)
        os._exit(0 if packet in {b"D", b"I"} else 70)

    os.close(ready_write)
    os.close(control_read)
    os.close(result_write)
    child_reaped = False
    control_open = True
    result_open = True

    def release_and_reap_child(expected_packet: bytes) -> None:
        nonlocal child_reaped
        nonlocal control_open
        nonlocal result_open
        os.write(control_write, b"C")
        os.close(control_write)
        control_open = False
        result_selector = selectors.DefaultSelector()
        result_selector.register(result_read, selectors.EVENT_READ)
        try:
            assert result_selector.select(timeout=10)
            assert os.read(result_read, 1) == expected_packet
        finally:
            result_selector.close()
            os.close(result_read)
            result_open = False
        _, child_status = os.waitpid(child_process_id, 0)
        child_reaped = True
        assert os.WIFEXITED(child_status)
        assert os.WEXITSTATUS(child_status) == 0

    try:
        ready_selector = selectors.DefaultSelector()
        ready_selector.register(ready_read, selectors.EVENT_READ)
        try:
            assert ready_selector.select(timeout=10)
            assert os.read(ready_read, 1) == b"R"
        finally:
            ready_selector.close()
            os.close(ready_read)

        authority_run = harness.begin_generated_evidence_run(poison_root)
        assert (
            cast(
                harness._EvidenceLedger,
                authority_run._pytest_registration.evidence_ledger,
            ).run
            is authority_run
        )
        harness._arm_pytest_root_authority_fault(probe_mode)
        if probe_mode in {"poison_mmap_probe", "poison_pipe_probe"}:
            assert harness._pytest_root_authority_uncertain()
            harness._revoke_fixture_root(
                _active_task064_pytest_root,
                poison_root,
            )
        elif probe_mode in {
            "poison_mmap_write",
            "poison_mmap_flush",
            "poison_pipe_write",
        }:
            harness._latch_pytest_root_authority_uncertainty()
            assert harness._pytest_root_authority_fault_hit(probe_mode)
            release_and_reap_child(b"D")
            assert harness._pytest_root_authority_uncertain()
            harness._revoke_fixture_root(
                _active_task064_pytest_root,
                poison_root,
            )
        else:
            with pytest.raises(harness.HarnessFailure) as revocation_failure:
                harness._revoke_fixture_root(
                    _active_task064_pytest_root,
                    poison_root,
                )
            assert (
                revocation_failure.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
            )
        assert harness._pytest_root_authority_uncertain()
        assert harness._lookup_active_pytest_root(poison_root) is None
        assert harness._lookup_revoked_pytest_root(poison_root) is not None
        assert harness._lookup_active_pytest_root(survivor_root) is None
        poison_registration = authority_run._pytest_registration
        assert not harness._pytest_root_session_owns(poison_registration, False)
        assert harness._ACTIVE_EVIDENCE_RUN.get() is None

        with pytest.raises(harness.HarnessFailure) as old_token:
            harness.verify_store(survivor_token)
        assert old_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
        with pytest.raises(harness.HarnessFailure) as new_bootstrap:
            harness.bootstrap_store(survivor_root)
        assert new_bootstrap.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        with pytest.raises(harness.HarnessFailure) as main_bootstrap:
            harness.bootstrap_store(tmp_path)
        assert main_bootstrap.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        with pytest.raises(harness.HarnessFailure) as old_run:
            harness._validated_evidence_run(authority_run)
        assert old_run.value.code is harness.HarnessFailureCode.CORRUPT
        with pytest.raises(harness.HarnessFailure) as new_run:
            harness.begin_generated_evidence_run(survivor_root)
        assert new_run.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT

        forged_receipt = object.__new__(harness._EvidenceReceipt)
        forged_report = object.__new__(harness.EvidenceReport)
        with pytest.raises(harness.HarnessFailure) as report_publication:
            harness.write_evidence_report(
                survivor_root,
                receipt=forged_receipt,
                report=forged_report,
            )
        assert report_publication.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        assert not (survivor_root / "task064-evidence.json").exists()
        assert not tuple(survivor_root.glob(".task064-evidence-*.tmp"))

        fork_calls = 0

        def forbidden_fork() -> int:
            nonlocal fork_calls
            fork_calls += 1
            raise AssertionError("fork must not run after root-authority uncertainty")

        calls: tuple[Callable[[], object], ...] = (
            lambda: harness.sqlite_result_code_fault_evidence(
                survivor_token,
                seam="readonly",
            ),
            lambda: harness.fresh_process_writer_contention_evidence(
                survivor_token,
                operation="create",
                natural_key=_natural_key(creation),
                creation=creation,
                policy=policy,
            ),
            lambda: harness.fresh_process_two_writer_evidence(
                survivor_token,
                operation="create",
                creations=(creation, creation),
                policy=policy,
            ),
            lambda: harness.fresh_process_kill_evidence(
                survivor_token,
                seam="before_transaction",
                creation=creation,
                policy=policy,
                natural_key=_natural_key(creation),
            ),
            lambda: harness.true_during_commit_evidence(
                survivor_token,
                transition=transition,
                natural_key=_natural_key(creation),
            ),
            lambda: harness.ioerr_write_evidence(
                survivor_token,
                transition=transition,
                natural_key=_natural_key(creation),
            ),
            lambda: harness.max_page_count_evidence(
                survivor_token,
                transition=transition,
                natural_key=_natural_key(creation),
            ),
            lambda: harness.wal_concurrency_evidence(
                survivor_token,
                transitions=(transition,),
                natural_key=_natural_key(creation),
            ),
            lambda: harness.concurrent_write_backup_evidence(
                survivor_token,
                survivor_root,
                transitions=(transition,) * harness.CONCURRENT_BACKUP_TRANSITIONS,
                evidence_recorded_at_utc="2026-07-29T06:42:00.000000Z",
            ),
        )
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fork", forbidden_fork)
            for invoke in calls:
                with pytest.raises(harness.HarnessFailure) as guarded:
                    invoke()
                assert guarded.value.code is harness.HarnessFailureCode.UNPROVEN
        assert len(calls) == 9
        assert fork_calls == 0

        if not child_reaped:
            release_and_reap_child(b"I")

        harness._remove_owned_files(poison_token)
        harness._remove_owned_files(survivor_token)
        harness._revoke_fixture_root(
            _active_task064_pytest_root,
            survivor_root,
        )
        assert harness._lookup_revoked_pytest_root(survivor_root) is not None
        assert harness._pytest_root_authority_uncertain()
    finally:
        if control_open:
            with contextlib.suppress(OSError):
                os.close(control_write)
        if result_open:
            with contextlib.suppress(OSError):
                os.close(result_read)
        if not child_reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child_process_id, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(child_process_id, 0)


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


@pytest.mark.parametrize("fault", ("file_close", "generation_close", "root_close"))
def test_closed_file_manifest_requires_every_close_to_be_proven(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    fault: str,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    expected_manifest = harness._closed_file_manifest(token)
    probe_process_id = os.fork()
    if probe_process_id == 0:
        exit_code = 70
        try:
            module_close_calls = 0

            def forbidden_module_close(_descriptor: int) -> None:
                nonlocal module_close_calls
                module_close_calls += 1
                raise AssertionError("captured close authority must ignore module mutation")

            harness._arm_connection_authority_fault(fault)
            returned_manifest = False
            manifest_failure: harness.HarnessFailure | None = None
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "close", forbidden_module_close)
                try:
                    harness._closed_file_manifest(token)
                    returned_manifest = True
                except harness.HarnessFailure as error:
                    manifest_failure = error
            fork_calls = 0

            def forbidden_fork() -> int:
                nonlocal fork_calls
                fork_calls += 1
                raise AssertionError("fork must not run after manifest close uncertainty")

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "fork", forbidden_fork)
                with pytest.raises(harness.HarnessFailure) as guarded:
                    harness.sqlite_result_code_fault_evidence(
                        token,
                        seam="readonly",
                    )
            if (
                not returned_manifest
                and manifest_failure is not None
                and manifest_failure.code is harness.HarnessFailureCode.UNAVAILABLE
                and str(manifest_failure) == "unavailable"
                and manifest_failure.__cause__ is None
                and module_close_calls == 0
                and guarded.value.code is harness.HarnessFailureCode.UNPROVEN
                and fork_calls == 0
            ):
                exit_code = 0
        except BaseException:
            exit_code = 70
        os._exit(exit_code)
    _, probe_status = os.waitpid(probe_process_id, 0)
    assert os.WIFEXITED(probe_status)
    assert os.WEXITSTATUS(probe_status) == 0
    assert expected_manifest
    assert harness._has_process_cleanup_uncertainty()
    with pytest.raises(harness.HarnessFailure) as poisoned_manifest:
        harness._closed_file_manifest(token)
    assert poisoned_manifest.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    harness._remove_owned_files(token)
    assert not token._generation_root.exists()


@pytest.mark.parametrize(
    ("name", "required", "prepare_wal", "fault"),
    (
        *(
            ("store.sqlite3", True, False, fault)
            for fault in ("file_close", "generation_close", "root_close")
        ),
        *(
            ("store.sqlite3-wal", False, True, fault)
            for fault in ("file_close", "generation_close", "root_close")
        ),
        ("store.sqlite3-wal", False, False, "generation_close"),
        ("store.sqlite3-wal", False, False, "root_close"),
    ),
)
def test_owned_file_size_returns_only_after_every_close_is_proven(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    name: str,
    required: bool,
    prepare_wal: bool,
    fault: str,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    registered = harness._lookup_store_token_authority(token)
    assert registered is not None
    wal_path = token._generation_root / "store.sqlite3-wal"
    if prepare_wal:
        wal_path.write_bytes(b"task064-owned-wal-probe")
        wal_path.chmod(0o600)
    else:
        with contextlib.suppress(FileNotFoundError):
            wal_path.unlink()
    expected = harness._owned_file_size(
        registered,
        name,
        required=required,
        maximum=(
            harness.MAX_TEST_DATABASE_BYTES
            if name == "store.sqlite3"
            else harness.MAX_TEST_WAL_BYTES
        ),
    )

    probe_process_id = os.fork()
    if probe_process_id == 0:
        exit_code = 70
        try:
            module_close_calls = 0

            def forbidden_module_close(_descriptor: int) -> None:
                nonlocal module_close_calls
                module_close_calls += 1
                raise AssertionError("captured close authority must ignore module mutation")

            harness._arm_connection_authority_fault(fault)
            with (
                pytest.MonkeyPatch.context() as patch,
                pytest.raises(harness.HarnessFailure) as size_failure,
            ):
                patch.setattr(os, "close", forbidden_module_close)
                harness._owned_file_size(
                    registered,
                    name,
                    required=required,
                    maximum=(
                        harness.MAX_TEST_DATABASE_BYTES
                        if name == "store.sqlite3"
                        else harness.MAX_TEST_WAL_BYTES
                    ),
                )
            if (
                size_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
                and module_close_calls == 0
                and harness._has_fork_unsafe_connection_authority()
            ):
                exit_code = 0
        except BaseException:
            exit_code = 70
        os._exit(exit_code)

    _, probe_status = os.waitpid(probe_process_id, 0)
    assert os.WIFEXITED(probe_status)
    assert os.WEXITSTATUS(probe_status) == 0
    assert (
        harness._owned_file_size(
            registered,
            name,
            required=required,
            maximum=(
                harness.MAX_TEST_DATABASE_BYTES
                if name == "store.sqlite3"
                else harness.MAX_TEST_WAL_BYTES
            ),
        )
        == expected
    )


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
        assert token._nonce in harness._token_authority_snapshot()
    finally:
        original_generation.unlink()
        retained_generation.rename(original_generation)
        harness._remove_owned_files(token)
        sentinel.unlink()
        sentinel_root.rmdir()
    assert token._nonce not in harness._token_authority_snapshot()
    assert not original_generation.exists()


@pytest.mark.parametrize(
    "fault",
    ("unlink", "rmdir", "generation_close", "root_close"),
)
def test_owned_cleanup_failures_retain_retry_authority(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    fault: str,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    registered = harness._lookup_store_token_authority(token)
    assert registered is not None
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

    if fault in {"generation_close", "root_close"}:
        probe_process_id = os.fork()
        if probe_process_id == 0:
            exit_code = 70
            reused_descriptor = -1
            try:
                close_attempts = 0

                def fail_close(descriptor: int) -> None:
                    nonlocal injected, close_attempts, reused_descriptor
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
                        close_attempts += 1
                        real_close(descriptor)
                        reused_descriptor = os.open("/dev/null", os.O_RDONLY)
                        if reused_descriptor != descriptor:
                            raise AssertionError("numeric descriptor was not reused")
                        raise OSError(errno.EIO, "injected owned close uncertainty")
                    real_close(descriptor)

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(os, "close", fail_close)
                    with pytest.raises(harness.HarnessFailure) as cleanup:
                        harness._remove_owned_files(token)
                reused_descriptor_open = os.fstat(reused_descriptor).st_ino > 0
                fork_calls = 0

                def forbidden_fork() -> int:
                    nonlocal fork_calls
                    fork_calls += 1
                    raise AssertionError("fork must not run after owned close uncertainty")

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(os, "fork", forbidden_fork)
                    with pytest.raises(harness.HarnessFailure) as guarded:
                        harness.sqlite_result_code_fault_evidence(
                            token,
                            seam="readonly",
                        )
                if (
                    injected
                    and cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE
                    and close_attempts == 1
                    and reused_descriptor_open
                    and harness._lookup_store_token_authority(token) == registered
                    and guarded.value.code is harness.HarnessFailureCode.UNPROVEN
                    and fork_calls == 0
                ):
                    exit_code = 0
            except BaseException:
                exit_code = 70
            finally:
                if reused_descriptor >= 0:
                    with contextlib.suppress(OSError):
                        os.close(reused_descriptor)
            os._exit(exit_code)
        _, probe_status = os.waitpid(probe_process_id, 0)
        assert os.WIFEXITED(probe_status)
        assert os.WEXITSTATUS(probe_status) == 0
    else:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                os,
                "unlink" if fault == "unlink" else "rmdir",
                fail_unlink if fault == "unlink" else fail_rmdir,
            )
            with pytest.raises(harness.HarnessFailure) as cleanup:
                harness._remove_owned_files(token)
        assert injected
        assert cleanup.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert harness._lookup_store_token_authority(token) == registered
    harness._remove_owned_files(token)
    assert token._nonce not in harness._token_authority_snapshot()
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


def test_pipe_io_is_centralized_eintr_safe_and_uses_one_deadline() -> None:
    source = Path(harness.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    raw_sites: dict[str, set[str]] = {"read": set(), "write": set()}

    class RawPipeVisitor(ast.NodeVisitor):
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
                and node.func.attr in raw_sites
            ):
                raw_sites[node.func.attr].add(self.current_function)
            self.generic_visit(node)

    RawPipeVisitor().visit(tree)
    assert raw_sites == {
        "read": {
            "_read_process_packet",
            "_closed_file_manifest",
            "_publish_evidence_report_bytes_unbound",
        },
        "write": {"_write_process_packet", "_publish_evidence_report_bytes_unbound"},
    }

    real_selector_factory = selectors.DefaultSelector

    class InterruptingSelector:
        def __init__(self) -> None:
            self.delegate = real_selector_factory()
            self.interrupted = False

        def register(
            self,
            descriptor: int,
            events: int,
            data: object | None = None,
        ) -> object:
            return self.delegate.register(descriptor, events, data)

        def select(self, timeout: float | None = None) -> object:
            if not self.interrupted:
                self.interrupted = True
                raise InterruptedError
            return self.delegate.select(timeout)

        def close(self) -> None:
            self.delegate.close()

    read_descriptor, write_descriptor = os.pipe()
    os.write(write_descriptor, b"done")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(selectors, "DefaultSelector", InterruptingSelector)
        assert harness._read_process_packet("selector_eintr", read_descriptor, 4) == b"done"
    os.close(read_descriptor)
    os.close(write_descriptor)

    read_descriptor, write_descriptor = os.pipe()
    os.write(write_descriptor, b"ab")
    observed_timeouts: list[float | None] = []
    clock_values = iter((0.0, 1.0, 5.0, 10.0))

    class IncompletePacketSelector:
        def __init__(self) -> None:
            self.calls = 0

        def register(self, *_args: object, **_kwargs: object) -> None:
            return None

        def select(self, timeout: float | None = None) -> object:
            self.calls += 1
            observed_timeouts.append(timeout)
            return ((object(), selectors.EVENT_READ),) if self.calls == 1 else ()

        def close(self) -> None:
            return None

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(selectors, "DefaultSelector", IncompletePacketSelector)
        patch.setattr(time, "monotonic", lambda: next(clock_values))
        assert harness._read_process_packet("fixed_deadline", read_descriptor, 4) == b"ab"
    os.close(read_descriptor)
    os.close(write_descriptor)
    assert observed_timeouts == [9.0, 5.0]

    read_descriptor, write_descriptor = os.pipe()
    real_write = os.write
    write_interrupted = False

    def interrupted_partial_write(descriptor: int, payload: object) -> int:
        nonlocal write_interrupted
        if not write_interrupted:
            write_interrupted = True
            raise InterruptedError
        return real_write(descriptor, memoryview(cast(Any, payload))[:2])

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(selectors, "DefaultSelector", InterruptingSelector)
        patch.setattr(os, "write", interrupted_partial_write)
        harness._write_process_packet("partial_write", write_descriptor, b"abcd")
    assert write_interrupted
    assert os.read(read_descriptor, 4) == b"abcd"
    os.close(read_descriptor)
    os.close(write_descriptor)

    read_descriptor, write_descriptor = os.pipe()
    pipe_buffer = os.fpathconf(write_descriptor, "PC_PIPE_BUF")
    large_payload = bytes(index % 251 for index in range(pipe_buffer + 137))
    writer_process_id = os.fork()
    if writer_process_id == 0:
        os.close(read_descriptor)
        exit_code = 70
        try:
            harness._write_process_packet(
                "large_real_pipe_roundtrip",
                write_descriptor,
                large_payload,
            )
            exit_code = 0
        except BaseException:
            exit_code = 70
        finally:
            with contextlib.suppress(OSError):
                os.close(write_descriptor)
        os._exit(exit_code)
    os.close(write_descriptor)
    observed_payload = harness._read_process_packet(
        "large_real_pipe_roundtrip",
        read_descriptor,
        len(large_payload),
    )
    os.close(read_descriptor)
    _, writer_status = os.waitpid(writer_process_id, 0)
    assert observed_payload == large_payload
    assert os.WIFEXITED(writer_status)
    assert os.WEXITSTATUS(writer_status) == 0
    with pytest.raises(ChildProcessError):
        os.waitpid(writer_process_id, os.WNOHANG)

    blocked_read, blocked_write = os.pipe()
    status_read, status_write = os.pipe()
    os.set_blocking(blocked_write, False)
    while True:
        try:
            os.write(blocked_write, b"x" * pipe_buffer)
        except BlockingIOError:
            break
    timeout_process_id = os.fork()
    if timeout_process_id == 0:
        os.close(blocked_read)
        os.close(status_read)
        packet = b"E"
        clock_values = iter((0.0, 9.95, 10.0))
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(
                    time,
                    "monotonic",
                    lambda: next(clock_values, 10.0),
                )
                try:
                    harness._write_process_packet(
                        "real_pipe_backpressure",
                        blocked_write,
                        b"y" * (pipe_buffer + 1),
                    )
                except harness.HarnessFailure as error:
                    if (
                        error.code is harness.HarnessFailureCode.UNAVAILABLE
                        and str(error) == "unavailable"
                        and error.__cause__ is None
                    ):
                        packet = b"T"
        except BaseException:
            packet = b"E"
        with contextlib.suppress(OSError):
            os.write(status_write, packet)
        with contextlib.suppress(OSError):
            os.close(blocked_write)
        with contextlib.suppress(OSError):
            os.close(status_write)
        os._exit(0 if packet == b"T" else 70)
    os.close(status_write)
    timeout_packet = os.read(status_read, 1)
    os.close(status_read)
    _, timeout_status = os.waitpid(timeout_process_id, 0)
    os.close(blocked_write)
    os.close(blocked_read)
    assert timeout_packet == b"T"
    assert os.WIFEXITED(timeout_status)
    assert os.WEXITSTATUS(timeout_status) == 0
    with pytest.raises(ChildProcessError):
        os.waitpid(timeout_process_id, os.WNOHANG)


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

    assert tmp_path.is_dir()


@pytest.mark.parametrize(
    "helper_name",
    ("finalize_process_resources", "wait_or_terminate_owned_process"),
)
def test_process_finalizers_use_the_exact_captured_cleanup_marker(
    request: pytest.FixtureRequest,
    helper_name: str,
    tmp_path: Path,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    fake_marker_calls = 0

    def fake_marker() -> None:
        nonlocal fake_marker_calls
        fake_marker_calls += 1

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_mark_process_cleanup_uncertain", fake_marker)
        if helper_name == "finalize_process_resources":
            patch.setattr(harness, "_close_descriptors", lambda _descriptors: False)
            patch.setattr(harness, "_terminate_and_reap_processes", lambda _processes: True)
            with pytest.raises(harness.HarnessFailure) as cleanup_failure:
                harness._finalize_process_resources((), ())
        else:
            patch.setattr(
                harness,
                "_wait_for_owned_process",
                lambda _process_id, *, timeout_seconds: None,
            )
            patch.setattr(harness, "_terminate_and_reap_processes", lambda _processes: False)
            with pytest.raises(harness.HarnessFailure) as cleanup_failure:
                harness._wait_or_terminate_owned_process(
                    123_467,
                    timeout_seconds=0.01,
                )
    assert cleanup_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert fake_marker_calls == 0
    assert harness._has_process_cleanup_uncertainty()
    assert harness._pytest_root_authority_uncertain()


def test_shared_cleanup_uncertainty_rejects_surviving_child_before_io(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    probe_mode, dispatch_modes = _task064_child_dispatch_plan(
        "shared_cleanup",
        request.node.nodeid,
        ("run",),
    )
    if probe_mode is None:
        assert dispatch_modes == ("run",)
        completed, _ = _run_task064_pytest_child(
            tmp_path,
            protocol="shared_cleanup",
            issuer_node_id=request.node.nodeid,
            target_node_id=request.node.nodeid,
            mode="run",
            pycache_label="task064-shared-cleanup-pycache",
            timeout_seconds=180,
        )
        assert completed.returncode == 0, (
            "isolated shared-cleanup probe failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
        return

    assert probe_mode == "run"
    assert dispatch_modes == ()
    token = harness.bootstrap_store(tmp_path)
    registration = harness._lookup_store_token_authority(token)
    assert registration is not None
    policy, creation = _creation(seed=7_465)
    transition = _retain(creation, policy, reason="shared-cleanup-child-guard")
    assert (
        harness.create_stream(token, creation, policy).classification
        is harness.StoreClassification.INSERTED
    )
    control_read, control_write = os.pipe()
    result_read, result_write = os.pipe()
    child_process_id = os.fork()
    if child_process_id == 0:
        os.close(control_write)
        os.close(result_read)
        packet = b"E"
        try:
            if os.read(control_read, 1) != b"C":
                os._exit(71)
            io_calls = 0

            def forbidden_io(*_args: object, **_kwargs: object) -> Any:
                nonlocal io_calls
                io_calls += 1
                raise AssertionError("cleanup poison must reject before filesystem or SQLite I/O")

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(harness, "_has_process_cleanup_uncertainty", lambda: False)
                patch.setattr(Path, "lstat", forbidden_io)
                patch.setattr(Path, "resolve", forbidden_io)
                patch.setattr(os, "open", forbidden_io)
                patch.setattr(sqlite3, "connect", forbidden_io)
                session_rejected = not harness._pytest_root_session_owns(
                    registration.pytest_registration,
                    True,
                )
                with pytest.raises(harness.HarnessFailure) as token_rejected:
                    harness._require_token(token)
            if (
                session_rejected
                and token_rejected.value.code is harness.HarnessFailureCode.INVALID_TOKEN
                and io_calls == 0
            ):
                packet = b"I"
        except BaseException:
            packet = b"E"
        with contextlib.suppress(OSError):
            os.write(result_write, packet)
        os._exit(0 if packet == b"I" else 70)

    os.close(control_read)
    os.close(result_write)
    child_reaped = False
    control_open = True
    result_open = True
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(harness, "_close_descriptors", lambda _descriptors: False)
            with pytest.raises(harness.HarnessFailure) as cleanup_failure:
                harness._finalize_process_resources((), ())
        assert cleanup_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert harness._has_process_cleanup_uncertainty()
        assert not harness._pytest_root_session_owns(
            registration.pytest_registration,
            False,
        )

        fork_calls = 0

        def forbidden_fork() -> int:
            nonlocal fork_calls
            fork_calls += 1
            raise AssertionError("fork must not run after shared cleanup uncertainty")

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
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(harness, "_mark_process_cleanup_uncertain", lambda: None)
            patch.setattr(harness, "_has_process_cleanup_uncertainty", lambda: False)
            patch.setattr(harness, "_pytest_root_authority_uncertain", lambda: False)
            patch.setattr(harness, "_has_fork_unsafe_connection_authority", lambda: False)
            patch.setattr(harness, "_require_fork_safe_connection_state", lambda: None)
            patch.setattr(os, "fork", forbidden_fork)
            with pytest.raises(harness.HarnessFailure) as token_rejected:
                harness.verify_store(token)
            assert token_rejected.value.code is harness.HarnessFailureCode.INVALID_TOKEN
            forged_receipt = object.__new__(harness._EvidenceReceipt)
            forged_report = object.__new__(harness.EvidenceReport)
            with pytest.raises(harness.HarnessFailure) as report_rejected:
                harness.write_evidence_report(
                    tmp_path,
                    receipt=forged_receipt,
                    report=forged_report,
                )
            assert report_rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
            for invoke in calls:
                with pytest.raises(harness.HarnessFailure) as fork_rejected:
                    invoke()
                assert fork_rejected.value.code is harness.HarnessFailureCode.UNPROVEN
        assert len(calls) == 9
        assert fork_calls == 0

        os.write(control_write, b"C")
        os.close(control_write)
        control_open = False
        result_selector = selectors.DefaultSelector()
        result_selector.register(result_read, selectors.EVENT_READ)
        try:
            assert result_selector.select(timeout=10)
            assert os.read(result_read, 1) == b"I"
        finally:
            result_selector.close()
            os.close(result_read)
            result_open = False
        _, child_status = os.waitpid(child_process_id, 0)
        child_reaped = True
        assert os.WIFEXITED(child_status)
        assert os.WEXITSTATUS(child_status) == 0
        harness._remove_owned_files(token)
        assert not token._generation_root.exists()
    finally:
        if control_open:
            with contextlib.suppress(OSError):
                os.close(control_write)
        if result_open:
            with contextlib.suppress(OSError):
                os.close(result_read)
        if not child_reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child_process_id, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(child_process_id, 0)


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
    fork_apis = (
        harness.sqlite_result_code_fault_evidence,
        harness.fresh_process_writer_contention_evidence,
        harness.fresh_process_two_writer_evidence,
        harness.fresh_process_kill_evidence,
        harness.true_during_commit_evidence,
        harness.ioerr_write_evidence,
        harness.max_page_count_evidence,
        harness.wal_concurrency_evidence,
        harness.concurrent_write_backup_evidence,
    )
    assert len({id(api) for api in fork_apis}) == 9
    for api in fork_apis:
        assert not hasattr(api, "__wrapped__")
        assert inspect.unwrap(api) is api
    before = harness.verify_store(token)
    connection, _ = harness._connect(token, writer=True)
    fork_calls = 0

    def forbidden_fork() -> int:
        nonlocal fork_calls
        fork_calls += 1
        raise AssertionError("fork must not run with a live SQLite connection")

    monkeypatch.setattr(os, "fork", forbidden_fork)
    monkeypatch.setattr(harness, "_mark_process_cleanup_uncertain", lambda: None)
    monkeypatch.setattr(harness, "_has_process_cleanup_uncertainty", lambda: False)
    monkeypatch.setattr(harness, "_pytest_root_authority_uncertain", lambda: False)
    monkeypatch.setattr(harness, "_has_fork_unsafe_connection_authority", lambda: False)
    monkeypatch.setattr(harness, "_require_fork_safe_connection_state", lambda: None)
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


@pytest.mark.parametrize(
    "fault",
    ("after_root_open", "after_generation_open", "after_file_open"),
)
def test_snapshot_acquisition_close_uncertainty_is_registered_before_first_open(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    fault: str,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    probe_process_id = os.fork()
    if probe_process_id == 0:
        exit_code = 70
        try:
            module_open_calls = 0

            def forbidden_module_open(*_args: object, **_kwargs: object) -> int:
                nonlocal module_open_calls
                module_open_calls += 1
                raise AssertionError("captured open authority must ignore module mutation")

            harness._arm_connection_authority_fault(fault)
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "open", forbidden_module_open)
                with pytest.raises(harness.HarnessFailure) as acquisition_failure:
                    harness.verify_store(token)
            fork_calls = 0

            def forbidden_fork() -> int:
                nonlocal fork_calls
                fork_calls += 1
                raise AssertionError("fork must not run after acquisition close uncertainty")

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "fork", forbidden_fork)
                with pytest.raises(harness.HarnessFailure) as guarded:
                    harness.sqlite_result_code_fault_evidence(
                        token,
                        seam="readonly",
                    )
            if (
                acquisition_failure.value.code is harness.HarnessFailureCode.UNAVAILABLE
                and module_open_calls == 0
                and guarded.value.code is harness.HarnessFailureCode.UNPROVEN
                and fork_calls == 0
            ):
                exit_code = 0
        except BaseException:
            exit_code = 70
        os._exit(exit_code)
    _, probe_status = os.waitpid(probe_process_id, 0)
    assert os.WIFEXITED(probe_status)
    assert os.WEXITSTATUS(probe_status) == 0
    assert harness._has_process_cleanup_uncertainty()
    with pytest.raises(harness.HarnessFailure) as poisoned_token:
        harness.verify_store(token)
    assert poisoned_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    harness._remove_owned_files(token)
    assert not token._generation_root.exists()


@pytest.mark.parametrize("fault", ("sqlite_close", "file_close"))
def test_close_uncertainty_is_irreversible_and_blocks_fork_before_execution(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    fault: str,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    probe_process_id = os.fork()
    if probe_process_id == 0:
        exit_code = 70
        try:
            connection, _ = harness._connect(token, writer=True)
            module_close_calls = 0

            def forbidden_module_close(_descriptor: int) -> None:
                nonlocal module_close_calls
                module_close_calls += 1
                raise AssertionError("captured close authority must ignore module mutation")

            harness._arm_connection_authority_fault(fault)
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "close", forbidden_module_close)
                patch.setattr(
                    harness,
                    "_close_descriptors",
                    lambda _descriptors: (_ for _ in ()).throw(
                        AssertionError("module close helper must not be authoritative")
                    ),
                )
                with pytest.raises(harness.HarnessFailure) as first_close:
                    connection.close()
                with pytest.raises(harness.HarnessFailure) as second_close:
                    connection.close()
            post_close_observations = 0

            def forbidden_post_close_observation(
                *_args: object,
                **_kwargs: object,
            ) -> Any:
                nonlocal post_close_observations
                post_close_observations += 1
                raise AssertionError(
                    "close-uncertain state must reject before token, filesystem, or SQL"
                )

            with pytest.MonkeyPatch.context() as patch:
                for name in (
                    "_require_token",
                    "_fetch_all",
                    "_revalidate_connection_path",
                    "_require_live_transaction_authority",
                    "_revalidate_connection_alias_free_path",
                ):
                    patch.setattr(harness, name, forbidden_post_close_observation)
                with pytest.raises(harness.HarnessFailure) as post_close_verify:
                    harness._verify_operation_snapshot(
                        connection,
                        token,
                        writer=True,
                    )
            fork_calls = 0

            def forbidden_fork() -> int:
                nonlocal fork_calls
                fork_calls += 1
                raise AssertionError("fork must not run after close uncertainty")

            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(os, "fork", forbidden_fork)
                with pytest.raises(harness.HarnessFailure) as guarded:
                    harness.sqlite_result_code_fault_evidence(
                        token,
                        seam="readonly",
                    )
            if (
                first_close.value.code is harness.HarnessFailureCode.UNAVAILABLE
                and second_close.value.code is harness.HarnessFailureCode.UNAVAILABLE
                and post_close_verify.value.code is harness.HarnessFailureCode.UNAVAILABLE
                and post_close_observations == 0
                and harness._connection_authority_state(connection) == "CLOSE_UNCERTAIN"
                and module_close_calls == 0
                and guarded.value.code is harness.HarnessFailureCode.UNPROVEN
                and fork_calls == 0
            ):
                exit_code = 0
        except BaseException:
            exit_code = 70
        os._exit(exit_code)
    _, probe_status = os.waitpid(probe_process_id, 0)
    assert os.WIFEXITED(probe_status)
    assert os.WEXITSTATUS(probe_status) == 0
    assert harness._has_process_cleanup_uncertainty()
    with pytest.raises(harness.HarnessFailure) as poisoned_token:
        harness.verify_store(token)
    assert poisoned_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    harness._remove_owned_files(token)
    assert not token._generation_root.exists()


def test_failed_registration_cleanup_uncertainty_is_latched_and_never_reused(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    if _run_task064_exec_isolated(request, tmp_path):
        return
    token = harness.bootstrap_store(tmp_path)
    captured_connections: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect
    sentinel = RuntimeError("synthetic registration rejection")

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        captured_connections.append(connection)
        return connection

    def rejected_register(*_args: Any, **_kwargs: Any) -> None:
        raise sentinel

    harness._arm_connection_authority_fault("sqlite_close")
    with (
        pytest.MonkeyPatch.context() as patch,
        pytest.raises(harness.HarnessFailure) as rejected,
    ):
        patch.setattr(sqlite3, "connect", tracked_connect)
        patch.setattr(harness, "_register_live_connection", rejected_register)
        harness._connect(token, writer=True)
    assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert len(captured_connections) == 1
    connection = captured_connections[0]
    assert harness._connection_authority_state(connection) == "CLOSE_UNCERTAIN"
    assert harness._has_process_cleanup_uncertainty()
    assert harness._has_fork_unsafe_connection_authority()

    exact_record = inspect.getclosurevars(harness._consume_connection_immutable_runtime).nonlocals[
        "exact_runtime_record"
    ]
    records = cast(
        dict[int, dict[str, object]],
        inspect.getclosurevars(exact_record).nonlocals["connection_records"],
    )
    assert records[id(connection)]["state"] == "CLOSE_UNCERTAIN"
    assert all(record["state"] != "LIVE" for record in records.values())
    with pytest.raises(harness.HarnessFailure) as unavailable_runtime:
        harness._consume_connection_immutable_runtime(
            connection,
            token._nonce,
            True,
        )
    assert unavailable_runtime.value.code is harness.HarnessFailureCode.UNAVAILABLE
    with pytest.raises(harness.HarnessFailure) as ambiguous_close:
        connection.close()
    assert ambiguous_close.value.code is harness.HarnessFailureCode.UNAVAILABLE
    with pytest.raises(harness.HarnessFailure) as poisoned_token:
        harness.verify_store(token)
    assert poisoned_token.value.code is harness.HarnessFailureCode.INVALID_TOKEN
    harness._remove_owned_files(token)
    assert not token._generation_root.exists()


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
    real_decode = harness._decode_canonical_history_record
    initial_decode_calls: list[bytes] = []

    def observe_initial_decode(record_bytes: bytes, **kwargs: Any) -> Any:
        initial_decode_calls.append(record_bytes)
        return real_decode(record_bytes, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_decode_canonical_history_record", observe_initial_decode)
        hundred = harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=natural_key,
            limit=100,
        )
    assert initial_decode_calls == [entry.canonical_bytes for entry in entries[:100]]

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
    continuation_decode_calls: list[bytes] = []

    def observe_continuation_decode(record_bytes: bytes, **kwargs: Any) -> Any:
        continuation_decode_calls.append(record_bytes)
        return real_decode(record_bytes, **kwargs)

    mature_anchor = entries[1]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_decode_canonical_history_record", observe_continuation_decode)
        mature_continuation = harness.audit_history(
            token,
            stream_id=creation.record.stream_id,
            natural_key=natural_key,
            limit=100,
            continuation=(
                mature_anchor.record.successor_version,
                mature_anchor.successor_envelope.envelope_digest,
                mature_anchor.history_root,
            ),
        )
    assert continuation_decode_calls == [
        entries[1].canonical_bytes,
        entries[0].canonical_bytes,
        *(entry.canonical_bytes for entry in entries[2:102]),
    ]
    assert len(set(continuation_decode_calls)) == len(continuation_decode_calls)
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
    assert mature_continuation.entries == tuple(entries[1:102])
    assert mature_continuation.query_evidence.history_rows == 101
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
    import gc as garbage_collector

    real_validate_page = harness._validate_history_page_rows
    live_snapshots_before_page: list[int] = []

    def observe_live_page_snapshots(*args: Any, **kwargs: Any) -> Any:
        garbage_collector.collect()
        live_snapshots_before_page.append(
            sum(
                type(item) is harness._ValidatedHistoryRowSnapshot
                for item in garbage_collector.get_objects()
            )
        )
        return real_validate_page(*args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(harness, "_validate_history_page_rows", observe_live_page_snapshots)
        summary = harness.verify_store(token)
    assert live_snapshots_before_page == [0, 1]
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


def _assert_published_report_capability_boundaries(
    *,
    capability: harness._Task064PublishedReportArtifactCapability,
    report_path: Path,
    node_id: str,
    pytest_root: Path,
    root_capability: harness._PytestRootCapability,
) -> None:
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_published_report_artifact
    ).nonlocals
    paths = cast(dict[int, dict[str, object]], claim_closure["paths"])
    record = paths[id(report_path)]
    assert record["path"] is report_path
    assert record["capability"] is capability
    assert record["state"] == "CLAIMED"
    assert record["issued_modes"] == set()
    mode = "staging_close_ambiguity"
    ack_original_raw = report_path.read_bytes()

    issue_closure = inspect.getclosurevars(harness._issue_task064_child_provenance).nonlocals
    resolve_publication = cast(Any, issue_closure["resolve_published_report_artifact"])
    synthetic_child_pid = os.getpid() + 1_000_000

    def armed_ack_session(
        label: str,
    ) -> tuple[
        Callable[[str, int, int], bool],
        Callable[[], None],
        str,
        int,
    ]:
        packet_digest = hashlib.sha256(f"task064-ack-{label}".encode("ascii")).hexdigest()
        _, _, bind_packet_digest = resolve_publication(
            capability,
            pytest_root=pytest_root,
            issuer_node_id=node_id,
            target_node_id=node_id,
            mode=mode,
            deadline_ns=time.monotonic_ns() + 60_000_000_000,
        )
        arm, attest, commit, cancel = cast(
            tuple[
                Callable[[int], bool],
                Callable[[str, int, int], bool],
                Callable[[str, int, int], bool],
                Callable[[], None],
            ],
            bind_packet_digest(packet_digest),
        )
        handshake_deadline_ns = time.monotonic_ns() + 10_000_000_000
        assert arm(synthetic_child_pid)
        assert attest(packet_digest, synthetic_child_pid, handshake_deadline_ns)
        return commit, cancel, packet_digest, handshake_deadline_ns

    def restore_after_rejected_ack(cancel: Callable[[], None]) -> None:
        cancel()
        issued_modes = cast(set[object], record["issued_modes"])
        assert issued_modes == {mode}
        issued_modes.clear()
        record["state"] = "CLAIMED"
        assert record["issued_modes"] == set()

    commit, cancel, packet_digest, handshake_deadline_ns = armed_ack_session("stale-context")
    assert not contextvars.Context().run(
        commit,
        packet_digest,
        synthetic_child_pid,
        handshake_deadline_ns,
    )
    restore_after_rejected_ack(cancel)

    commit, cancel, packet_digest, handshake_deadline_ns = armed_ack_session("stale-capability")
    original_capability = record["capability"]
    record["capability"] = replace(capability)
    try:
        assert not commit(packet_digest, synthetic_child_pid, handshake_deadline_ns)
    finally:
        record["capability"] = original_capability
    restore_after_rejected_ack(cancel)

    commit, cancel, packet_digest, handshake_deadline_ns = armed_ack_session("stale-root")
    original_root_identity = record["root_identity"]
    record["root_identity"] = object()
    try:
        assert not commit(packet_digest, synthetic_child_pid, handshake_deadline_ns)
    finally:
        record["root_identity"] = original_root_identity
    restore_after_rejected_ack(cancel)

    commit, cancel, packet_digest, handshake_deadline_ns = armed_ack_session("stale-artifact")
    original_prefix = report_path.read_bytes()[:1]
    replacement_prefix = b"[" if original_prefix != b"[" else b"{"
    mutation_descriptor = os.open(
        report_path,
        os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        assert os.pwrite(mutation_descriptor, replacement_prefix, 0) == 1
        os.fsync(mutation_descriptor)
        assert not commit(packet_digest, synthetic_child_pid, handshake_deadline_ns)
    finally:
        assert os.pwrite(mutation_descriptor, original_prefix, 0) == 1
        os.fsync(mutation_descriptor)
        os.close(mutation_descriptor)
    restore_after_rejected_ack(cancel)
    commit, cancel, packet_digest, _ = armed_ack_session("expired-deadline")
    assert not commit(
        packet_digest,
        synthetic_child_pid,
        time.monotonic_ns() - 1,
    )
    restore_after_rejected_ack(cancel)

    commit, cancel, packet_digest, handshake_deadline_ns = armed_ack_session("wrong-pid")
    assert not commit(
        packet_digest,
        synthetic_child_pid + 1,
        handshake_deadline_ns,
    )
    restore_after_rejected_ack(cancel)
    assert report_path.read_bytes() == ack_original_raw
    assert record["state"] == "CLAIMED"
    assert record["issued_modes"] == set()
    inventory_before = tuple(pytest_root.iterdir())
    deadline_ns = time.monotonic_ns() + 60_000_000_000

    def reject(
        authority: object = capability,
        *,
        root: Path = pytest_root,
        issuer: str = node_id,
        target: str = node_id,
        requested_mode: str = mode,
        deadline: int = deadline_ns,
        expected_record_state: str = "CLAIMED",
    ) -> None:
        provenance: harness._Task064ChildProvenance | None = None
        try:
            provenance = harness._issue_task064_child_provenance(
                root,
                "report_close",
                issuer,
                target,
                requested_mode,
                report_artifact_capability=cast(Any, authority),
                report_deadline_ns=deadline,
            )
        except harness.HarnessFailure as error:
            assert error.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        else:
            raise AssertionError("hostile report capability minted provenance")
        finally:
            if provenance is not None:
                harness._close_task064_child_provenance(provenance)
        assert tuple(pytest_root.iterdir()) == inventory_before
        assert record["state"] == expected_record_state
        assert record["issued_modes"] == set()

    reject(report_path.read_bytes())
    reject(replace(capability))
    reject(replace(capability, _authority_nonce=b"x" * 32))
    alternate_root = next(root for root in root_capability.roots if root is not pytest_root)
    reject(root=alternate_root)
    reject(issuer=f"{node_id}::wrong-issuer")
    reject(target=f"{node_id}::wrong-target")
    reject(requested_mode="task064-wrong-mode")
    reject(deadline=time.monotonic_ns() - 1)
    reject(deadline=time.monotonic_ns() + 901_000_000_000)

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT, "environment-only-forgery")
        reject()

    for isolated_context in (contextvars.Context(), contextvars.copy_context()):
        isolated_context.run(reject)
    thread_errors: list[BaseException] = []

    def reject_in_thread() -> None:
        try:
            reject()
        except BaseException as error:
            thread_errors.append(error)

    hostile_thread = threading.Thread(target=reject_in_thread)
    hostile_thread.start()
    hostile_thread.join(timeout=10)
    assert not hostile_thread.is_alive()
    assert thread_errors == []

    result_read, result_write = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(result_read)
        result = b"F"
        try:
            try:
                harness._issue_task064_child_provenance(
                    pytest_root,
                    "report_close",
                    node_id,
                    node_id,
                    mode,
                    report_artifact_capability=capability,
                    report_deadline_ns=deadline_ns,
                )
            except harness.HarnessFailure as error:
                if error.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT:
                    result = b"P"
        finally:
            with contextlib.suppress(OSError):
                os.write(result_write, result)
            with contextlib.suppress(OSError):
                os.close(result_write)
            os._exit(0)
    os.close(result_write)
    result_selector = selectors.DefaultSelector()
    result_selector.register(result_read, selectors.EVENT_READ)
    child_reaped = False
    child_status: int | None = None
    try:
        assert result_selector.select(timeout=10)
        assert os.read(result_read, 1) == b"P"
        reap_deadline = time.monotonic() + 10
        while True:
            waited_pid, observed_status = os.waitpid(child_pid, os.WNOHANG)
            if waited_pid == child_pid:
                child_status = observed_status
                child_reaped = True
                break
            assert time.monotonic() < reap_deadline
            time.sleep(0.01)
    finally:
        result_selector.close()
        os.close(result_read)
        if not child_reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(child_pid, signal.SIGKILL)
            with contextlib.suppress(ChildProcessError):
                os.waitpid(child_pid, 0)
    assert child_status is not None
    assert os.WIFEXITED(child_status)
    assert os.WEXITSTATUS(child_status) == 0

    original_raw = report_path.read_bytes()
    replacement = b"[" if original_raw[:1] != b"[" else b"{"
    mutation_descriptor = os.open(
        report_path,
        os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        assert os.pwrite(mutation_descriptor, replacement, 0) == 1
        os.fsync(mutation_descriptor)
        reject()
        assert os.pwrite(mutation_descriptor, original_raw[:1], 0) == 1
        os.fsync(mutation_descriptor)
    finally:
        os.close(mutation_descriptor)
    assert report_path.read_bytes() == original_raw

    for field, hostile_value in (
        ("owner_process_id", os.getpid() + 1),
        ("owner_thread_id", threading.get_ident() + 1),
        ("state", "PREPARED"),
        ("raw", bytes(bytearray(cast(bytes, record["raw"])))),
        ("expires_ns", time.monotonic_ns() - 1),
        ("aggregate", object()),
        ("report", object()),
        ("run", object()),
    ):
        original_value = record[field]
        record[field] = hostile_value
        try:
            reject(
                expected_record_state=(cast(str, hostile_value) if field == "state" else "CLAIMED")
            )
        finally:
            record[field] = original_value
    assert record["state"] == "CLAIMED"
    assert record["issued_modes"] == set()


def _run_authenticated_report_close_probe(
    *,
    mode: str,
    node_id: str,
    pytest_root: Path,
    root_capability: harness._PytestRootCapability,
) -> None:
    publication_root = (
        root_capability.roots[3] if mode == "reentrant_root_revocation" else pytest_root
    )
    for fixture_root in root_capability.roots:
        active_root = harness._lookup_active_pytest_root(fixture_root)
        assert active_root is not None
        assert type(active_root.evidence_ledger) is harness._ReportPublicationRootState
    artifact, permit = harness._claim_task064_report_close_publication(
        node_id,
        publication_root,
    )
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_report_close_publication
    ).nonlocals
    permit_records = cast(
        dict[int, dict[str, object]],
        claim_closure["report_permit_records"],
    )
    terminal_permit_records = cast(
        dict[int, tuple[object, int, str]],
        claim_closure["terminal_report_permit_records"],
    )
    probe_permit = cast(Callable[..., bool], claim_closure["report_permit_is_valid"])
    permit_record = permit_records[id(permit)]
    assert permit_record["permit"] is permit
    assert permit_record["state"] == "ACTIVE"
    assert id(permit) not in terminal_permit_records
    report_path = publication_root / "task064-evidence.json"
    inventory_before = {path.name for path in publication_root.iterdir()}
    assert "task064-evidence.json" not in inventory_before
    assert not tuple(publication_root.glob(".task064-evidence-*.tmp"))
    root_identity = publication_root.lstat()

    def reject_before_publication_io(
        hostile_permit: harness._Task064ReportPublicationPermit,
        hostile_root: Path,
        hostile_artifact: bytes,
    ) -> None:
        calls = {"open": 0, "write": 0, "link": 0}
        real_open = os.open
        real_write = os.write
        real_link = os.link

        def observed_open(*args: Any, **kwargs: Any) -> int:
            calls["open"] += 1
            return real_open(*args, **kwargs)

        def observed_write(*args: Any, **kwargs: Any) -> int:
            calls["write"] += 1
            return real_write(*args, **kwargs)

        def observed_link(*args: Any, **kwargs: Any) -> None:
            calls["link"] += 1
            real_link(*args, **kwargs)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "open", observed_open)
            patch.setattr(os, "write", observed_write)
            patch.setattr(os, "link", observed_link)
            with pytest.raises(harness.HarnessFailure) as rejected:
                harness._publish_task064_report_close_probe(
                    hostile_root,
                    permit=hostile_permit,
                    artifact=hostile_artifact,
                )
        assert rejected.value.code in {
            harness.HarnessFailureCode.CORRUPT,
            harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT,
        }
        assert calls == {"open": 0, "write": 0, "link": 0}
        assert permit_records[id(permit)] is permit_record
        assert permit_record["state"] == "ACTIVE"

    copied_permit = replace(permit)
    reject_before_publication_io(copied_permit, publication_root, artifact)
    reject_before_publication_io(
        replace(permit, _authority_nonce=b"p" * 32),
        publication_root,
        artifact,
    )
    reject_before_publication_io(
        copied_permit,
        Path(str(publication_root)),
        artifact,
    )
    reject_before_publication_io(
        copied_permit,
        publication_root,
        bytes(bytearray(artifact)),
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT, "late-forgery")
        reject_before_publication_io(copied_permit, publication_root, artifact)

    def probe_exact_permit(hostile_root: Path, hostile_artifact: bytes) -> None:
        assert not probe_permit(
            permit,
            hostile_root,
            hostile_artifact,
            terminalize_invalid=False,
        )
        assert permit_records[id(permit)] is permit_record
        assert permit_record["state"] == "ACTIVE"

    probe_exact_permit(Path(str(publication_root)), artifact)
    probe_exact_permit(publication_root, bytes(bytearray(artifact)))
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv(harness._TASK064_CHILD_PROVENANCE_ENVIRONMENT, "late-forgery")
        probe_exact_permit(publication_root, artifact)
    for isolated_context in (contextvars.Context(), contextvars.copy_context()):
        isolated_context.run(probe_exact_permit, publication_root, artifact)

    thread_errors: list[BaseException] = []

    def reject_exact_permit_in_thread() -> None:
        try:
            reject_before_publication_io(permit, publication_root, artifact)
        except BaseException as error:
            thread_errors.append(error)

    hostile_thread = threading.Thread(target=reject_exact_permit_in_thread)
    hostile_thread.start()
    hostile_thread.join(timeout=10)
    assert not hostile_thread.is_alive()
    assert thread_errors == []
    assert permit_record["state"] == "ACTIVE"

    real_close = os.close
    injected_calls = 0

    def close_with_fault(descriptor: int) -> None:
        nonlocal injected_calls
        details = os.fstat(descriptor)
        try:
            descriptor_name = Path(os.readlink(f"/proc/self/fd/{descriptor}")).name
        except OSError:
            descriptor_name = ""
        is_stage = (
            stat.S_ISREG(details.st_mode)
            and descriptor_name.startswith(".task064-evidence-")
            and descriptor_name.endswith(".tmp")
        )
        is_readback = stat.S_ISREG(details.st_mode) and descriptor_name == "task064-evidence.json"
        is_root = (
            stat.S_ISDIR(details.st_mode)
            and details.st_dev == root_identity.st_dev
            and details.st_ino == root_identity.st_ino
            and report_path.exists()
        )
        should_fail = bool(
            injected_calls == 0
            and (
                (mode == "staging_close_ambiguity" and is_stage)
                or (mode == "readback_close_ambiguity" and is_readback)
                or (mode == "readback_verified_root_close_ambiguity" and is_root)
            )
        )
        should_revoke = bool(
            injected_calls == 0 and mode == "reentrant_root_revocation" and is_root
        )
        if should_revoke:
            injected_calls += 1
            harness._revoke_fixture_root(root_capability, publication_root)
        if should_fail or should_revoke:
            assert permit_records[id(permit)] is permit_record
            assert permit_record["state"] == "PREPARED"
        real_close(descriptor)
        if should_fail:
            injected_calls += 1
            raise OSError(errno.EIO, "injected checked report-close ambiguity")

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "close", close_with_fault)
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._publish_task064_report_close_probe(
                publication_root,
                permit=permit,
                artifact=artifact,
            )
    assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert injected_calls == 1
    assert id(permit) not in permit_records
    assert terminal_permit_records[id(permit)] == (
        permit,
        os.getpid(),
        "TERMINALIZED",
    )
    report_expected = mode != "staging_close_ambiguity"
    assert report_path.exists() is report_expected
    assert not tuple(publication_root.glob(".task064-evidence-*.tmp"))
    expected_inventory = set(inventory_before)
    if report_expected:
        expected_inventory.add("task064-evidence.json")
        assert report_path.read_bytes() == artifact
        assert stat_mode(report_path) == 0o600
        assert report_path.stat().st_nlink == 1
    assert {path.name for path in publication_root.iterdir()} == expected_inventory
    assert harness._has_process_cleanup_uncertainty()

    replay_open_calls = 0
    real_open = os.open

    def observe_replay_open(*args: Any, **kwargs: Any) -> int:
        nonlocal replay_open_calls
        replay_open_calls += 1
        return real_open(*args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "open", observe_replay_open)
        with pytest.raises(harness.HarnessFailure) as replayed:
            harness._publish_task064_report_close_probe(
                publication_root,
                permit=permit,
                artifact=artifact,
            )
    assert replayed.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert replay_open_calls == 0
    assert {path.name for path in publication_root.iterdir()} == expected_inventory

    fork_calls = 0

    def forbidden_fork() -> int:
        nonlocal fork_calls
        fork_calls += 1
        raise AssertionError("fork must not run after report publication uncertainty")

    dummy = cast(Any, object())
    guarded_calls: tuple[Callable[[], object], ...] = (
        lambda: harness.sqlite_result_code_fault_evidence(dummy, seam="readonly"),
        lambda: harness.fresh_process_writer_contention_evidence(
            dummy,
            operation="create",
            natural_key=b"",
            creation=dummy,
            policy=dummy,
        ),
        lambda: harness.fresh_process_two_writer_evidence(
            dummy,
            operation="create",
            creations=cast(Any, ()),
            policy=dummy,
        ),
        lambda: harness.fresh_process_kill_evidence(
            dummy,
            seam="before_transaction",
            creation=dummy,
            policy=dummy,
            natural_key=b"",
        ),
        lambda: harness.true_during_commit_evidence(
            dummy,
            transition=dummy,
            natural_key=b"",
        ),
        lambda: harness.ioerr_write_evidence(
            dummy,
            transition=dummy,
            natural_key=b"",
        ),
        lambda: harness.max_page_count_evidence(
            dummy,
            transition=dummy,
            natural_key=b"",
        ),
        lambda: harness.wal_concurrency_evidence(
            dummy,
            transitions=cast(Any, ()),
            natural_key=b"",
        ),
        lambda: harness.concurrent_write_backup_evidence(
            dummy,
            publication_root,
            transitions=cast(Any, ()),
            evidence_recorded_at_utc="invalid",
        ),
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "fork", forbidden_fork)
        for invoke in guarded_calls:
            with pytest.raises(harness.HarnessFailure) as guarded:
                invoke()
            assert guarded.value.code is harness.HarnessFailureCode.UNPROVEN
    assert len(guarded_calls) == 9
    assert fork_calls == 0


def _minimal_canonical_report_artifact() -> bytes:
    recorded_at = "2026-07-29T06:45:00.000000Z"

    def digest(label: str) -> str:
        return f"sha256:{hashlib.sha256(label.encode('ascii')).hexdigest()}"

    document: dict[str, object] = {
        "report_version": 1,
        "task": {
            "task_id": harness.TASK_ID,
            "contract_generation": harness.TASK_CONTRACT_GENERATION,
            "contract_digest": harness.TASK_CONTRACT_DIGEST,
        },
        "schema": {
            "schema_fingerprint": harness.load_schema_fingerprint(),
            "application_id": harness.APPLICATION_ID,
            "user_version": harness.USER_VERSION,
            "schema_generation": harness.SCHEMA_GENERATION,
            "page_size": harness.PAGE_SIZE,
            "storage_marker": harness.STORAGE_MARKER.decode("ascii"),
        },
        "runtime": {
            "python_version": harness.ACCEPTED_PYTHON_VERSION,
            "sqlite_version": harness.ACCEPTED_SQLITE_VERSION,
            "sqlite_source_id": harness.ACCEPTED_SQLITE_SOURCE_ID,
            "threadsafety": harness.ACCEPTED_THREADSAFETY,
            "compile_options": list(harness.ACCEPTED_COMPILE_OPTIONS),
            "connection_profiles": [
                {
                    "role": profile.role,
                    "dbconfig": [list(item) for item in profile.dbconfig],
                    "defensive_available": profile.defensive_available,
                    "defensive_enabled": profile.defensive_enabled,
                    "limits": [list(item) for item in profile.limits],
                    "pragmas": [list(item) for item in profile.pragmas],
                }
                for profile in harness.ACCEPTED_CONNECTION_PROFILES
            ],
        },
        "environment_class": "generated-linux-pytest",
        "evidence_recorded_at_utc": recorded_at,
        "workload_contract": {
            "seed": harness.WORKLOAD_SEED,
            "runs": harness.WORKLOAD_RUNS,
            "record_size_matrix": [list(item) for item in harness.RECORD_SIZE_MATRIX],
            "workload_matrix": [list(item) for item in harness.WORKLOAD_MATRIX],
            "thresholds": {
                "maximum_operation_latency_ns": harness.MAX_OPERATION_LATENCY_NS,
                "maximum_database_bytes": harness.MAX_TEST_DATABASE_BYTES,
                "maximum_wal_bytes": harness.MAX_TEST_WAL_BYTES,
                "maximum_traced_memory_bytes": harness.MAX_TEST_TRACED_MEMORY_BYTES,
                "maximum_open_cursors": harness.MAX_TEST_OPEN_CURSORS,
                "maximum_page_count": harness.MAX_PAGE_COUNT,
                "wal_autocheckpoint_pages": harness.WAL_AUTOCHECKPOINT_PAGES,
            },
        },
        "measurements": {
            "stream_rows": 0,
            "history_rows": 0,
            "query_rows": 0,
            "database_bytes": 1,
            "wal_bytes": 0,
            "page_count": 1,
            "freelist_count": 0,
            "maximum_open_cursors": 1,
            "peak_traced_memory_bytes": 0,
            "latency_samples_ns": [0 for _ in range(harness.WORKLOAD_RUNS)],
        },
        "backup_manifest": {
            "source_generation_id": digest("task064-source-generation"),
            "destination_generation_id": digest("task064-destination-generation"),
            "schema_fingerprint": harness.load_schema_fingerprint(),
            "sqlite_source_id": harness.ACCEPTED_SQLITE_SOURCE_ID,
            "page_size": harness.PAGE_SIZE,
            "source_page_count": 1,
            "destination_page_count": 1,
            "checkpoint_outcome": [0, 0, 0],
            "finalization_outcome": "TRUNCATE_CHECKPOINT_CLOSED_STANDALONE_MAIN",
            "evidence_recorded_at_utc": recorded_at,
            "source_streams": 0,
            "source_history_rows": 0,
            "destination_streams": 0,
            "destination_history_rows": 0,
            "files": [["store.sqlite3", 1, digest("task064-main-file")]],
            "per_stream_tails": [],
        },
        "gates": [
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
        ],
    }
    return harness.canonical_descriptor_bytes(document) + b"\n"


def test_report_artifact_validator_rejects_noncanonical_and_semantic_mutations() -> None:
    public_writer_closure = inspect.getclosurevars(harness.write_evidence_report).nonlocals
    close_writer_closure = inspect.getclosurevars(
        harness._publish_task064_report_close_probe
    ).nonlocals
    assert (
        public_writer_closure["publisher_implementation"]
        is close_writer_closure["publisher_implementation"]
    )
    assert callable(public_writer_closure["writer_implementation"])
    assert callable(public_writer_closure["validate_receipt"])
    assert callable(close_writer_closure["validate_report_permit"])
    assert callable(close_writer_closure["prepare_report_permit_consumption"])
    claim_closure = inspect.getclosurevars(
        harness._claim_task064_report_close_publication
    ).nonlocals
    validator = cast(Callable[[bytes], None], claim_closure["report_artifact_validator"])
    artifact = _minimal_canonical_report_artifact()
    validator(artifact)
    document = cast(dict[str, object], json.loads(artifact))

    reordered_document = {
        "task": document["task"],
        "report_version": document["report_version"],
        **{key: value for key, value in document.items() if key not in {"task", "report_version"}},
    }
    reordered_gate_document = json.loads(artifact)
    first_gate = reordered_gate_document["gates"][0]
    reordered_gate_document["gates"][0] = {
        "disposition": first_gate["disposition"],
        "name": first_gate["name"],
        "reason": first_gate["reason"],
    }
    boolean_schema_document = json.loads(artifact)
    boolean_schema_document["schema"]["application_id"] = True
    bare_digest_document = json.loads(artifact)
    bare_digest_document["backup_manifest"]["source_generation_id"] = "0" * 64
    wrong_finalization_document = json.loads(artifact)
    wrong_finalization_document["backup_manifest"]["finalization_outcome"] = (
        "TRUNCATE_CHECKPOINT_CLOSED_COMPLETE_FILE_SET"
    )
    oversized_file_document = json.loads(artifact)
    oversized_file_document["backup_manifest"]["files"][0][1] = harness.MAX_TEST_DATABASE_BYTES + 1
    same_length_report_version = artifact.replace(b'"report_version":1', b'"report_version":2', 1)
    duplicate_top_level_key = artifact.replace(
        b'{"report_version":1,',
        b'{"report_version":1,"report_version":1,',
        1,
    )
    hostile_artifacts = (
        artifact[:-1],
        artifact + b"\n",
        artifact + b" ",
        same_length_report_version,
        duplicate_top_level_key,
        harness.canonical_descriptor_bytes(reordered_document) + b"\n",
        harness.canonical_descriptor_bytes(reordered_gate_document) + b"\n",
        harness.canonical_descriptor_bytes(boolean_schema_document) + b"\n",
        harness.canonical_descriptor_bytes(bare_digest_document) + b"\n",
        harness.canonical_descriptor_bytes(wrong_finalization_document) + b"\n",
        harness.canonical_descriptor_bytes(oversized_file_document) + b"\n",
    )
    for hostile_artifact in hostile_artifacts:
        with pytest.raises(harness.HarnessFailure) as rejected:
            validator(hostile_artifact)
        assert rejected.value.code is harness.HarnessFailureCode.CORRUPT


def test_synthetic_pass_shaped_bytes_cannot_mint_report_close_authority(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    artifact = _minimal_canonical_report_artifact()
    forged_capability = harness._Task064PublishedReportArtifactCapability(
        _authority_nonce=b"synthetic-report-capability-seal"[:32]
    )
    assert len(forged_capability._authority_nonce) == 32
    inventory_before = tuple(tmp_path.iterdir())
    deadline_ns = time.monotonic_ns() + 60_000_000_000
    hostile_authorities: tuple[object, ...] = (
        artifact,
        forged_capability,
        replace(forged_capability),
    )
    for hostile_authority in hostile_authorities:
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._issue_task064_child_provenance(
                tmp_path,
                "report_close",
                request.node.nodeid,
                request.node.nodeid,
                "staging_close_ambiguity",
                report_artifact_capability=cast(Any, hostile_authority),
                report_deadline_ns=deadline_ns,
            )
        assert rejected.value.code is harness.HarnessFailureCode.INVALID_BOOTSTRAP_ROOT
        assert tuple(tmp_path.iterdir()) == inventory_before
        assert not tuple(tmp_path.glob(".task064-child-provenance-*.json"))
        assert not tuple(tmp_path.glob(".task064-report-artifact-*.json"))


def test_finite_typical_workload_measurements_and_sanitized_report(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    isolated_close_probe_modes = (
        "readback_verified_root_close_ambiguity",
        "staging_close_ambiguity",
        "readback_close_ambiguity",
        "reentrant_root_revocation",
    )
    close_probe_mode, dispatch_modes = _task064_child_dispatch_plan(
        "report_close",
        request.node.nodeid,
        isolated_close_probe_modes,
    )
    if close_probe_mode is not None:
        assert close_probe_mode in isolated_close_probe_modes
        assert dispatch_modes == ()
        _run_authenticated_report_close_probe(
            mode=close_probe_mode,
            node_id=request.node.nodeid,
            pytest_root=tmp_path,
            root_capability=_active_task064_pytest_root,
        )
        return
    assert dispatch_modes == isolated_close_probe_modes

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
        _report_bootstrap_path_evidence(
            tmp_path,
            report_token,
            _active_task064_pytest_root,
        )
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

    def receipt_is_consumed() -> bool:
        return evidence_ledger.consumed

    if close_probe_mode is None:
        harness.__dict__["_ATOMICITY_OPERATION_PRODUCER_SEQUENCE"] = ("bootstrap_store",)
        harness.__dict__["_GATE_OPERATION_PRODUCER_SEQUENCES"] = {
            "atomicity_classification": ("bootstrap_store",)
        }
    try:
        with harness.atomicity_operation_scope(evidence_run):
            _report_atomicity_evidence(tmp_path, fresh_process)
    finally:
        if close_probe_mode is None:
            harness.__dict__.pop("_ATOMICITY_OPERATION_PRODUCER_SEQUENCE")
            harness.__dict__.pop("_GATE_OPERATION_PRODUCER_SEQUENCES")
    if close_probe_mode is None:
        atomicity_ordinal = harness.GENERATED_EVIDENCE_GATES.index("atomicity_classification")
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
            evidence_ledger.operation_runs["atomicity_classification"] = tuple(
                substituted_operations
            )
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
    if close_probe_mode is None:
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
    if close_probe_mode is None:
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
    if close_probe_mode is None:
        harness._arm_evidence_seal_transition_fault()
        with pytest.raises(harness.HarnessFailure) as context_transition_failure:
            harness.seal_generated_evidence_run(
                evidence_run,
                evidence=evidence,
            )
        assert context_transition_failure.value.code is harness.HarnessFailureCode.CORRUPT
        assert evidence_ledger.recording
        assert not evidence_ledger.consumed
        assert evidence_ledger.receipt is None
        assert harness._ACTIVE_EVIDENCE_RUN.get() is evidence_run
        assert harness._validate_issued_evidence_run(evidence_run, ("RECORDING",))

        real_evidence_digest = harness._evidence_payload_digest
        aggregate_digest_calls = 0

        def mismatch_second_aggregate_digest(value: object) -> str:
            nonlocal aggregate_digest_calls
            digest = real_evidence_digest(value)
            if value is evidence:
                aggregate_digest_calls += 1
                if aggregate_digest_calls == 2:
                    return "f" * 64 if digest != "f" * 64 else "0" * 64
            return digest

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(harness.HarnessFailure) as second_pass_mismatch,
        ):
            patch.setattr(
                harness,
                "_evidence_payload_digest",
                mismatch_second_aggregate_digest,
            )
            harness.seal_generated_evidence_run(
                evidence_run,
                evidence=evidence,
            )
        assert second_pass_mismatch.value.code is harness.HarnessFailureCode.CORRUPT
        assert aggregate_digest_calls == 2
        assert evidence_ledger.recording
        assert not evidence_ledger.consumed
        assert evidence_ledger.receipt is None
        assert harness._ACTIVE_EVIDENCE_RUN.get() is evidence_run
        assert harness._validate_issued_evidence_run(evidence_run, ("RECORDING",))

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
    cyclic_report_value: list[object] = []
    cyclic_report_value.append(cyclic_report_value)
    hostile_report_value = _HostileMapping()
    depth_65_report_value: object = "leaf"
    for _ in range(65):
        depth_65_report_value = [depth_65_report_value]
    original_connection_profiles = summary.connection_profiles
    for hostile_candidate in (
        cyclic_report_value,
        hostile_report_value,
        depth_65_report_value,
    ):
        object.__setattr__(
            summary,
            "connection_profiles",
            hostile_candidate,
        )
        try:
            with pytest.raises(harness.HarnessFailure) as hostile_report:
                harness.write_evidence_report(
                    tmp_path,
                    receipt=evidence_receipt,
                    report=complete_report,
                )
            assert hostile_report.value.code is harness.HarnessFailureCode.CORRUPT
        finally:
            object.__setattr__(
                summary,
                "connection_profiles",
                original_connection_profiles,
            )
        assert evidence_ledger.receipt is evidence_receipt
        assert not evidence_ledger.consumed
        assert not report_path.exists()
        assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))
    assert hostile_report_value.items_calls == 0

    reordered_connection_profiles = tuple(reversed(original_connection_profiles))
    assert reordered_connection_profiles != original_connection_profiles
    exact_gate_payloads = tuple(
        getattr(evidence, gate) for gate in harness.GENERATED_EVIDENCE_GATES
    )
    assert len({id(payload) for payload in exact_gate_payloads}) == len(
        harness.GENERATED_EVIDENCE_GATES
    )
    private_validate_receipt = cast(
        Callable[..., object],
        inspect.getclosurevars(harness.write_evidence_report).nonlocals["validate_receipt"],
    )
    live_evidence_digest = harness._evidence_payload_digest
    for require_exact_evidence in (False, True):
        aggregate_digest_calls = 0
        gate_payload_digest_calls = 0

        def count_receipt_validation_digests(value: object) -> str:
            nonlocal aggregate_digest_calls
            nonlocal gate_payload_digest_calls
            if value is evidence:
                aggregate_digest_calls += 1
            if any(value is payload for payload in exact_gate_payloads):
                gate_payload_digest_calls += 1
            return live_evidence_digest(value)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                harness,
                "_evidence_payload_digest",
                count_receipt_validation_digests,
            )
            assert (
                private_validate_receipt(
                    tmp_path,
                    evidence_receipt,
                    evidence,
                    require_exact_evidence,
                )
                is evidence_ledger
            )
        assert aggregate_digest_calls == 1
        assert gate_payload_digest_calls == len(harness.GENERATED_EVIDENCE_GATES)

    copied_evidence = replace(evidence)
    copied_evidence_digest_calls = 0

    def count_copied_evidence_digests(value: object) -> str:
        nonlocal copied_evidence_digest_calls
        copied_evidence_digest_calls += 1
        return live_evidence_digest(value)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            harness,
            "_evidence_payload_digest",
            count_copied_evidence_digests,
        )
        with pytest.raises(harness.HarnessFailure) as copied_exact_evidence:
            private_validate_receipt(
                tmp_path,
                evidence_receipt,
                copied_evidence,
                True,
            )
    assert copied_exact_evidence.value.code is harness.HarnessFailureCode.CORRUPT
    assert copied_evidence_digest_calls == 0

    for mutation_checkpoint in range(1, 5):
        aggregate_checkpoint_calls = 0
        checkpoint_mutated = False

        def mutate_before_receipt_checkpoint(
            value: object,
            *,
            _mutation_checkpoint: int = mutation_checkpoint,
        ) -> str:
            nonlocal aggregate_checkpoint_calls
            nonlocal checkpoint_mutated
            if value is evidence:
                aggregate_checkpoint_calls += 1
                if aggregate_checkpoint_calls == _mutation_checkpoint:
                    object.__setattr__(
                        summary,
                        "connection_profiles",
                        reordered_connection_profiles,
                    )
                    checkpoint_mutated = True
            return live_evidence_digest(value)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                harness,
                "_evidence_payload_digest",
                mutate_before_receipt_checkpoint,
            )
            try:
                with pytest.raises(harness.HarnessFailure) as checkpoint_mutation:
                    harness.write_evidence_report(
                        tmp_path,
                        receipt=evidence_receipt,
                        report=complete_report,
                    )
            finally:
                object.__setattr__(
                    summary,
                    "connection_profiles",
                    original_connection_profiles,
                )
        assert checkpoint_mutated
        assert aggregate_checkpoint_calls == mutation_checkpoint
        assert checkpoint_mutation.value.code is harness.HarnessFailureCode.CORRUPT
        assert not report_path.exists()
        assert not tuple(tmp_path.glob(".task064-evidence-*.tmp"))
    assert evidence_ledger.receipt is evidence_receipt
    assert not evidence_ledger.consumed

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
    real_report_open = os.open
    same_size_mutated = False

    def mutate_before_final_readback(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal same_size_mutated
        if (
            path == "task064-evidence.json"
            and flags & os.O_ACCMODE == os.O_RDONLY
            and dir_fd is not None
            and not same_size_mutated
        ):
            mutation_descriptor = real_report_open(
                path,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=dir_fd,
            )
            try:
                original_byte = os.pread(mutation_descriptor, 1, 0)
                assert len(original_byte) == 1
                replacement = b"[" if original_byte != b"[" else b"{"
                assert os.pwrite(mutation_descriptor, replacement, 0) == 1
                os.fsync(mutation_descriptor)
                same_size_mutated = True
            finally:
                real_report_close(mutation_descriptor)
        return real_report_open(path, flags, mode, dir_fd=dir_fd)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "open", mutate_before_final_readback)
        with pytest.raises(harness.HarnessFailure) as mutated_readback:
            harness.write_evidence_report(
                tmp_path,
                receipt=evidence_receipt,
                report=complete_report,
            )
    assert mutated_readback.value.code is harness.HarnessFailureCode.UNAVAILABLE
    assert same_size_mutated
    assert not report_path.exists()
    assert_no_report_staging_file()
    assert evidence_ledger.receipt is evidence_receipt
    assert not evidence_ledger.consumed

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

    fake_prepare_calls = 0

    def fake_receipt_preparer(*_args: Any, **_kwargs: Any) -> None:
        nonlocal fake_prepare_calls
        fake_prepare_calls += 1

    fake_serializer_calls = 0
    observed_readback_descriptors: set[int] = set()
    readback_calls = 0
    readback_eof_observed = False
    real_report_read = os.read
    real_dynamic_serializer = json.dumps

    def forbidden_dynamic_serializer(
        document: object,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        nonlocal fake_serializer_calls
        if type(document) is dict and "report_version" in document:
            fake_serializer_calls += 1
            raise AssertionError("report writer must use its captured exact serializer")
        return real_dynamic_serializer(document, *args, **kwargs)

    def observe_report_readback_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_report_open(path, flags, mode, dir_fd=dir_fd)
        if path == "task064-evidence.json" and flags & os.O_ACCMODE == os.O_RDONLY:
            observed_readback_descriptors.add(descriptor)
        return descriptor

    def observe_report_readback(descriptor: int, size: int) -> bytes:
        nonlocal readback_calls
        nonlocal readback_eof_observed
        payload = real_report_read(descriptor, size)
        if descriptor in observed_readback_descriptors:
            readback_calls += 1
            if size == 1 and payload == b"":
                readback_eof_observed = True
        return payload

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(os, "close", close_after_ledger_mutation)
        patch.setattr(os, "open", observe_report_readback_open)
        patch.setattr(os, "read", observe_report_readback)
        patch.setattr(cast(Any, harness).json, "dumps", forbidden_dynamic_serializer)
        patch.setattr(
            harness,
            "_prepare_issued_evidence_receipt_consumption",
            fake_receipt_preparer,
            raising=False,
        )
        report = harness.write_evidence_report(
            tmp_path,
            receipt=evidence_receipt,
            report=complete_report,
        )
    assert ledger_mutated_during_root_close
    assert fake_prepare_calls == 0
    assert fake_serializer_calls == 0
    assert len(observed_readback_descriptors) == 1
    assert readback_calls >= 2
    assert readback_eof_observed
    assert evidence_ledger.receipt is evidence_receipt
    assert receipt_is_consumed()
    assert report == report_path
    assert stat_mode(report) == 0o600
    assert report.stat().st_nlink == 1
    assert_no_report_staging_file()
    report_text = report.read_text(encoding="utf-8")
    assert str(tmp_path) not in report_text
    username = os.environ.get("USER")
    assert username is None or username not in report_text
    report_document = json.loads(report_text)
    assert report.read_bytes() == harness.canonical_descriptor_bytes(report_document) + b"\n"
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
    canonical_report_artifact = report.read_bytes()
    published_report_artifact = harness._claim_task064_published_report_artifact(report)
    _assert_published_report_capability_boundaries(
        capability=published_report_artifact,
        report_path=report,
        node_id=request.node.nodeid,
        pytest_root=tmp_path,
        root_capability=_active_task064_pytest_root,
    )
    close_probe_results = _run_task064_pytest_children_concurrently(
        tmp_path,
        protocol="report_close",
        issuer_node_id=request.node.nodeid,
        target_node_id=request.node.nodeid,
        modes=dispatch_modes,
        pycache_label="task064-report-close-pycache",
        timeout_seconds=840,
        report_artifact_capability=published_report_artifact,
    )
    assert tuple(close_probe_results) == isolated_close_probe_modes
    assert all(
        completed.returncode == 0 and elapsed_seconds < 840
        for completed, elapsed_seconds in close_probe_results.values()
    )
    assert report.read_bytes() == canonical_report_artifact
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
