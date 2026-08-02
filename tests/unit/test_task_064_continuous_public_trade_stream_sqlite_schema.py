"""Unit evidence for the TASK-064 version-one SQLite schema."""

from __future__ import annotations

import contextlib
import contextvars
import inspect
import json
import os
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from types import FunctionType
from typing import TYPE_CHECKING, Any, cast

import pytest

from wealth.domain.continuous_public_trade import MAX_CONTRACT_INTEGER

if TYPE_CHECKING:
    import support.continuous_public_trade_stream_sqlite_harness as harness
else:
    import tests.support.continuous_public_trade_stream_sqlite_harness as harness


_TASK064_CAPTURED_OS_EXIT = os._exit


def _guard_task064_reserved_observer_exit(status: int) -> None:
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 191:
        _TASK064_CAPTURED_OS_EXIT(191)


def _bind_task064_harness_module() -> None:
    harness._bind_task064_test_module()


_bind_task064_harness_module()
del _bind_task064_harness_module


EXPECTED_SCHEMA_FINGERPRINT = (
    "sha256:0410c1f08390a411c73427b3d07c542f3d1828def7c6adebab51cd57375355b3"
)
_FIXTURE_REQUEST_TYPE = pytest.FixtureRequest
_TEMP_PATH_FACTORY_TYPE = pytest.TempPathFactory
_TEMP_PATH_MKTEMP = pytest.TempPathFactory.mktemp
type _Task064FixtureCallable = Callable[
    [pytest.FixtureRequest, Path, pytest.TempPathFactory],
    Iterator[harness._PytestRootCapability],
]


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
            replay_mode = claim_post_return_child_provenance(ticket, node_id)
            replay_fixture = unwrap_fixture(sealed_exported_fixture)
            if (
                type_of(replay_fixture) is not function_type
                or replay_fixture is not sealed_raw_fixture
            ):
                raise runtime_error_type("invalid TASK064 pytest fixture callable")
            permit = begin_registration(node_id, tmp_path)
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


def test_golden_descriptor_matches_live_catalog_without_self_blessing(
    tmp_path: Path,
) -> None:
    """The committed golden value is comparison-only during normal tests."""

    raw = harness.SCHEMA_DESCRIPTOR_PATH.read_bytes()
    assert raw.endswith(b"\n")
    assert not raw.endswith(b"\n\n")
    descriptor = harness.load_schema_descriptor()
    assert raw == harness.canonical_descriptor_bytes(descriptor) + b"\n"
    assert json.loads(raw) == descriptor
    assert harness.schema_fingerprint(descriptor) == EXPECTED_SCHEMA_FINGERPRINT
    assert harness.load_schema_fingerprint() == EXPECTED_SCHEMA_FINGERPRINT
    assert harness.provisional_schema_descriptor(tmp_path) == descriptor


def test_schema_fixture_snapshot_scopes_have_exact_single_pass_counters(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    counts = {
        "descriptor_read": 0,
        "fingerprint_read": 0,
        "parse": 0,
        "canonicalize": 0,
        "hash": 0,
        "digest_decode": 0,
        "installed": 0,
    }
    real_read_bytes = Path.read_bytes
    real_parse = harness._parse_schema_descriptor_document
    real_canonicalize = harness.canonical_descriptor_bytes
    real_hash = harness._schema_fingerprint_from_canonical_bytes
    real_digest_decode = harness._digest_bytes
    real_installed = harness.installed_schema_descriptor

    def observed_read_bytes(path: Path) -> bytes:
        if path == harness.SCHEMA_DESCRIPTOR_PATH:
            counts["descriptor_read"] += 1
            events.append("descriptor_read")
        elif path == harness.SCHEMA_FINGERPRINT_PATH:
            counts["fingerprint_read"] += 1
            events.append("fingerprint_read")
        return real_read_bytes(path)

    def observed_parse(document: bytes) -> dict[str, object]:
        counts["parse"] += 1
        events.append("parse")
        return real_parse(document)

    def observed_canonicalize(descriptor: Mapping[str, object]) -> bytes:
        counts["canonicalize"] += 1
        events.append("canonicalize")
        return real_canonicalize(descriptor)

    def observed_hash(canonical_bytes: bytes) -> str:
        counts["hash"] += 1
        events.append("hash")
        return real_hash(canonical_bytes)

    def observed_digest_decode(value: str) -> bytes:
        counts["digest_decode"] += 1
        events.append("digest_decode")
        return real_digest_decode(value)

    def observed_installed(connection: sqlite3.Connection) -> dict[str, object]:
        counts["installed"] += 1
        events.append("installed")
        return real_installed(connection)

    def reset_counts() -> None:
        events.clear()
        for name in counts:
            counts[name] = 0

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "read_bytes", observed_read_bytes)
        patch.setattr(harness, "_parse_schema_descriptor_document", observed_parse)
        patch.setattr(harness, "canonical_descriptor_bytes", observed_canonicalize)
        patch.setattr(harness, "_schema_fingerprint_from_canonical_bytes", observed_hash)
        patch.setattr(harness, "_digest_bytes", observed_digest_decode)
        patch.setattr(harness, "installed_schema_descriptor", observed_installed)

        assert harness.load_schema_descriptor()["descriptor_version"] == 1
        assert counts == {
            "descriptor_read": 1,
            "fingerprint_read": 0,
            "parse": 1,
            "canonicalize": 1,
            "hash": 0,
            "digest_decode": 0,
            "installed": 0,
        }
        assert events == ["descriptor_read", "parse", "canonicalize"]

        reset_counts()
        assert harness.load_schema_fingerprint() == EXPECTED_SCHEMA_FINGERPRINT
        assert counts == {
            "descriptor_read": 1,
            "fingerprint_read": 1,
            "parse": 1,
            "canonicalize": 1,
            "hash": 1,
            "digest_decode": 1,
            "installed": 0,
        }
        assert events == [
            "fingerprint_read",
            "digest_decode",
            "descriptor_read",
            "parse",
            "canonicalize",
            "hash",
        ]

        reset_counts()

        def skip_post_bootstrap_verify(_token: harness.StoreToken) -> None:
            return None

        with pytest.MonkeyPatch.context() as bootstrap_patch:
            bootstrap_patch.setattr(harness, "verify_store", skip_post_bootstrap_verify)
            token = harness.bootstrap_store(tmp_path)
        assert counts == {
            "descriptor_read": 1,
            "fingerprint_read": 1,
            "parse": 1,
            "canonicalize": 1,
            "hash": 1,
            "digest_decode": 1,
            "installed": 1,
        }
        assert events == [
            "installed",
            "descriptor_read",
            "parse",
            "canonicalize",
            "fingerprint_read",
            "digest_decode",
            "hash",
        ]

        connection, _ = harness._connect(token, writer=False)
        try:
            connection.execute("BEGIN").close()
            reset_counts()
            assert harness._verify_schema_identity(connection) == EXPECTED_SCHEMA_FINGERPRINT
            assert counts == {
                "descriptor_read": 1,
                "fingerprint_read": 1,
                "parse": 1,
                "canonicalize": 1,
                "hash": 1,
                "digest_decode": 1,
                "installed": 1,
            }
            assert events == [
                "fingerprint_read",
                "digest_decode",
                "descriptor_read",
                "parse",
                "canonicalize",
                "hash",
                "installed",
            ]
            connection.execute("ROLLBACK").close()
        finally:
            connection.close()


def test_schema_fixture_snapshots_are_distinct_and_failures_are_not_memoized() -> None:
    first = harness._load_schema_fixture_snapshot()
    second = harness._load_schema_fixture_snapshot()
    assert first is not second
    assert first.descriptor is not second.descriptor
    assert first.descriptor == second.descriptor
    assert first.fingerprint == second.fingerprint == EXPECTED_SCHEMA_FINGERPRINT
    assert (
        first.fingerprint_bytes
        == second.fingerprint_bytes
        == EXPECTED_SCHEMA_FINGERPRINT.encode("ascii")
    )

    real_read_bytes = Path.read_bytes
    descriptor_reads = 0
    fingerprint_reads = 0

    def fail_once_then_read_fresh(path: Path) -> bytes:
        nonlocal descriptor_reads, fingerprint_reads
        if path == harness.SCHEMA_DESCRIPTOR_PATH:
            descriptor_reads += 1
        elif path == harness.SCHEMA_FINGERPRINT_PATH:
            fingerprint_reads += 1
            if fingerprint_reads == 1:
                return b"sha256:" + (b"0" * 64) + b"\n"
        return real_read_bytes(path)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "read_bytes", fail_once_then_read_fresh)
        with pytest.raises(harness.HarnessFailure) as first_failure:
            harness.load_schema_fingerprint()
        assert first_failure.value.code is harness.HarnessFailureCode.CORRUPT
        assert harness.load_schema_fingerprint() == EXPECTED_SCHEMA_FINGERPRINT
    assert descriptor_reads == 2
    assert fingerprint_reads == 2


def test_schema_fixture_unexpected_failure_is_rethrown_and_not_memoized() -> None:
    real_read_bytes = Path.read_bytes
    unexpected = RuntimeError("schema-fixture-unexpected")
    descriptor_reads = 0

    def fail_once_then_read_fresh(path: Path) -> bytes:
        nonlocal descriptor_reads
        if path == harness.SCHEMA_DESCRIPTOR_PATH:
            descriptor_reads += 1
            if descriptor_reads == 1:
                raise unexpected
        return real_read_bytes(path)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "read_bytes", fail_once_then_read_fresh)
        with pytest.raises(RuntimeError) as first_failure:
            harness.load_schema_fingerprint()
        assert first_failure.value is unexpected
        assert harness.load_schema_fingerprint() == EXPECTED_SCHEMA_FINGERPRINT
    assert descriptor_reads == 2


def test_bootstrap_and_identity_verification_have_one_descriptor_read_traps(
    tmp_path: Path,
) -> None:
    real_read_bytes = Path.read_bytes
    descriptor_reads = 0

    def reject_a_second_descriptor_read(path: Path) -> bytes:
        nonlocal descriptor_reads
        if path == harness.SCHEMA_DESCRIPTOR_PATH:
            descriptor_reads += 1
            if descriptor_reads > 1:
                raise AssertionError("descriptor fixture read more than once in one scope")
        return real_read_bytes(path)

    def skip_post_bootstrap_verify(_token: harness.StoreToken) -> None:
        return None

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "read_bytes", reject_a_second_descriptor_read)
        patch.setattr(harness, "verify_store", skip_post_bootstrap_verify)
        token = harness.bootstrap_store(tmp_path)
    assert descriptor_reads == 1

    connection, _ = harness._connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        descriptor_reads = 0
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Path, "read_bytes", reject_a_second_descriptor_read)
            assert harness._verify_schema_identity(connection) == EXPECTED_SCHEMA_FINGERPRINT
        assert descriptor_reads == 1
        connection.execute("ROLLBACK").close()
    finally:
        connection.close()


def test_schema_fixture_live_drift_is_observed_by_the_next_identity_scope(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    descriptor_raw = harness.SCHEMA_DESCRIPTOR_PATH.read_bytes()
    descriptor = json.loads(descriptor_raw)
    assert type(descriptor) is dict
    descriptor["page_size"] = harness.PAGE_SIZE * 2
    drifted_raw = harness.canonical_descriptor_bytes(descriptor) + b"\n"
    real_read_bytes = Path.read_bytes
    descriptor_reads = 0

    def drift_between_scopes(path: Path) -> bytes:
        nonlocal descriptor_reads
        if path == harness.SCHEMA_DESCRIPTOR_PATH:
            descriptor_reads += 1
            return descriptor_raw if descriptor_reads == 1 else drifted_raw
        return real_read_bytes(path)

    connection, _ = harness._connect(token, writer=False)
    try:
        connection.execute("BEGIN").close()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Path, "read_bytes", drift_between_scopes)
            assert harness._verify_schema_identity(connection) == EXPECTED_SCHEMA_FINGERPRINT
            with pytest.raises(harness.HarnessFailure) as drifted:
                harness._verify_schema_identity(connection)
        assert drifted.value.code is harness.HarnessFailureCode.CORRUPT
        assert descriptor_reads == 2
        connection.execute("ROLLBACK").close()
    finally:
        connection.close()


def test_descriptor_freezes_only_the_permitted_physical_projections() -> None:
    descriptor = harness.load_schema_descriptor()
    objects = descriptor["objects"]
    assert isinstance(objects, list)
    assert [item["name"] for item in objects] == list(harness.SCHEMA_OBJECT_ORDER)
    assert [item["type"] for item in objects].count("table") == 4
    assert [item["type"] for item in objects].count("index") == 5
    assert [item["type"] for item in objects].count("trigger") == 10

    expected_integer_columns = {
        "singleton_key",
        "physical_format_version",
        "schema_generation",
        "natural_identity_key_version",
        "page_size",
        "stream_row_id",
        "stream_contract_version",
        "creation_successor_version",
        "policy_window_size_ms",
        "policy_settlement_lag_ms",
        "policy_max_catchup_span_ms",
        "policy_max_jobs_per_invocation",
        "policy_max_requests_per_job",
        "policy_max_records_per_job",
        "stream_start_epoch_ms",
        "current_version",
        "history_row_id",
        "successor_version",
        "serialization_version",
        "prior_version",
        "unresolved_singleton_key",
    }
    integer_columns: set[str] = set()
    declared_types: set[str] = set()
    all_columns: set[str] = set()
    normalized_sql: list[str] = []
    for item in objects:
        normalized_sql.append(item["normalized_sql"])
        if item["type"] != "table":
            continue
        for column in item["columns"]:
            all_columns.add(column["name"])
            declared_types.add(column["declared_type"])
            if column["declared_type"] == "INTEGER":
                integer_columns.add(column["name"])
    assert integer_columns == expected_integer_columns
    assert declared_types == {"INTEGER", "BLOB"}
    assert {
        "cursor_epoch_ms",
        "window_start_epoch_ms",
        "window_end_epoch_ms",
        "recorded_at",
    }.isdisjoint(all_columns)
    joined = "\n".join(normalized_sql).lower()
    assert "virtual table" not in joined
    assert "using fts" not in joined
    assert "load_extension" not in joined
    assert " attach " not in f" {joined} "
    assert "writable_schema" not in joined
    assert " collate " not in f" {joined} "


def test_natural_identity_key_is_strict_reversible_and_length_delimited() -> None:
    atoms = {
        "source": "binance.public-rest",
        "venue": "BINANCE",
        "instrument": "BTC-USDT",
        "provider_symbol": "BTCUSDT",
        "instrument_type": "SPOT",
        "request_variant": "aggregate-trades",
    }
    encoded = harness.natural_identity_key(**atoms)
    assert harness.decode_natural_identity_key(encoded) == tuple(atoms.values())
    assert harness.natural_identity_key(
        source="a",
        venue="bc",
        instrument="d",
        provider_symbol="e",
        instrument_type="f",
        request_variant="g",
    ) != harness.natural_identity_key(
        source="ab",
        venue="c",
        instrument="d",
        provider_symbol="e",
        instrument_type="f",
        request_variant="g",
    )
    surrogate_key = harness.natural_identity_key(**(atoms | {"source": "\ud800"}))
    assert harness.decode_natural_identity_key(surrogate_key)[0] == "\ud800"


def test_internal_uri_has_only_fixed_mode_rw_and_fts_is_unreachable() -> None:
    encoded = harness._database_uri(Path("/tmp/operator?mode=rwc&cache=shared.sqlite3"))
    assert encoded.endswith("?mode=rw")
    assert encoded.count("?") == 1
    assert "%3Fmode%3Drwc%26cache%3Dshared" in encoded
    assert sqlite3.SQLITE_CREATE_VTABLE in harness._DENIED_AUTHORIZER_ACTIONS
    assert sqlite3.SQLITE_ATTACH in harness._DENIED_AUTHORIZER_ACTIONS
    assert "ENABLE_FTS5" in harness.ACCEPTED_COMPILE_OPTIONS
    assert "CREATE VIRTUAL TABLE" not in harness.SCHEMA_PATH.read_text(encoding="utf-8").upper()


@pytest.mark.parametrize("code", [11, 267, 523, 779, 26])
def test_exact_corrupt_result_codes_are_closed(code: int) -> None:
    assert harness.sqlite_result_failure_code(code) is harness.HarnessFailureCode.CORRUPT


@pytest.mark.parametrize(
    "code",
    [None, True, 1035, 10, 778, 19, 2067, 5, 517, 8, 1544, 14, 1550, 999_999],
)
def test_operational_and_unknown_result_codes_fail_closed(code: object) -> None:
    assert harness.sqlite_result_failure_code(code) is harness.HarnessFailureCode.UNAVAILABLE


def test_bootstrap_reopens_with_the_exact_runtime_and_empty_schema(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    summary = harness.verify_store(token)
    assert summary.schema_fingerprint == EXPECTED_SCHEMA_FINGERPRINT
    assert summary.stream_count == 0
    assert summary.history_count == 0
    assert summary.profile.sqlite_source_id == harness.ACCEPTED_SQLITE_SOURCE_ID
    assert summary.profile.compile_options == harness.ACCEPTED_COMPILE_OPTIONS
    assert summary.profile.defensive_available
    assert summary.profile.defensive_enabled
    assert dict(summary.profile.pragmas)["max_page_count"] == harness.MAX_PAGE_COUNT
    assert dict(summary.profile.pragmas)["wal_autocheckpoint"] == harness.WAL_AUTOCHECKPOINT_PAGES
    assert dict(summary.profile.pragmas)["cache_size"] == -8192


def _connection_runtime_records() -> dict[int, dict[str, object]]:
    exact_record = inspect.getclosurevars(harness._consume_connection_immutable_runtime).nonlocals[
        "exact_runtime_record"
    ]
    records = inspect.getclosurevars(exact_record).nonlocals["connection_records"]
    assert type(records) is dict
    return records


def test_transaction_reuses_exact_connection_local_immutable_runtime_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, profile = harness._connect(token, writer=True)
    record = _connection_runtime_records()[id(connection)]
    stored_evidence = record["runtime_evidence"]
    consumed = harness._consume_connection_immutable_runtime(
        connection,
        token._nonce,
        True,
    )
    assert consumed == stored_evidence
    assert consumed is not stored_evidence
    assert profile.python_version == consumed.python_version
    assert profile.sqlite_version == consumed.sqlite_version
    assert profile.threadsafety == consumed.threadsafety
    assert profile.sqlite_source_id == consumed.sqlite_source_id
    assert profile.compile_options == consumed.compile_options

    counters = {
        "python_version": 0,
        "sqlite_version": 0,
        "threadsafety": 0,
        "source_id": 0,
        "compile_options": 0,
    }
    real_sys = __import__("sys")
    real_sqlite = sqlite3
    real_fetch_one = harness._fetch_one
    real_fetch_all = harness._fetch_all

    class CountingSystem:
        def __getattr__(self, name: str) -> object:
            if name == "version_info":
                counters["python_version"] += 1
            return getattr(real_sys, name)

    class CountingSqlite:
        def __getattr__(self, name: str) -> object:
            if name in {"sqlite_version", "threadsafety"}:
                counters[name] += 1
            return getattr(real_sqlite, name)

    def counted_fetch_one(
        observed_connection: sqlite3.Connection,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> sqlite3.Row:
        if sql == "SELECT sqlite_source_id()":
            counters["source_id"] += 1
        return real_fetch_one(observed_connection, sql, parameters)

    def counted_fetch_all(
        observed_connection: sqlite3.Connection,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> list[sqlite3.Row]:
        if sql == "PRAGMA compile_options":
            counters["compile_options"] += 1
        return real_fetch_all(observed_connection, sql, parameters)

    try:
        monkeypatch.setattr(harness, "sys", CountingSystem())
        monkeypatch.setattr(harness, "sqlite3", CountingSqlite())
        monkeypatch.setattr(harness, "_fetch_one", counted_fetch_one)
        monkeypatch.setattr(harness, "_fetch_all", counted_fetch_all)

        assert harness._verify_operation_snapshot(connection, token, writer=True)
        assert counters == {
            "python_version": 1,
            "sqlite_version": 1,
            "threadsafety": 1,
            "source_id": 1,
            "compile_options": 1,
        }
        for name in counters:
            counters[name] = 0

        connection.execute("BEGIN IMMEDIATE").close()
        assert harness._verify_operation_snapshot(connection, token, writer=True)
        assert counters == {
            "python_version": 0,
            "sqlite_version": 0,
            "threadsafety": 0,
            "source_id": 0,
            "compile_options": 0,
        }
        connection.execute("ROLLBACK").close()
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_transaction_authority_uses_exact_reduced_check_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    counts = {"full": 0, "cheap": 0, "pinned": 0, "alias": 0}
    events: list[str] = []
    real_require_token = harness._require_token
    real_live_authority = harness._require_live_transaction_authority
    real_revalidate_connection = harness._revalidate_connection_path
    real_alias_walk = harness._revalidate_connection_alias_free_path

    def counted_require_token(token_value: harness.StoreToken) -> harness._RegisteredIdentity:
        counts["full"] += 1
        events.append("full")
        return real_require_token(token_value)

    def counted_live_authority(
        observed_connection: sqlite3.Connection,
        token_value: harness.StoreToken,
        writer: bool,
    ) -> harness._LiveTransactionAuthorityView:
        counts["cheap"] += 1
        events.append("cheap")
        return real_live_authority(observed_connection, token_value, writer)

    def counted_revalidate_connection(
        identity: harness._RegisteredIdentity,
        observed_connection: sqlite3.Connection,
    ) -> None:
        counts["pinned"] += 1
        events.append("pinned")
        real_revalidate_connection(identity, observed_connection)

    def counted_alias_walk(
        identity: harness._RegisteredIdentity,
        observed_connection: sqlite3.Connection,
    ) -> None:
        counts["alias"] += 1
        events.append("alias")
        real_alias_walk(identity, observed_connection)

    monkeypatch.setattr(harness, "_require_token", counted_require_token)
    monkeypatch.setattr(
        harness,
        "_require_live_transaction_authority",
        counted_live_authority,
    )
    monkeypatch.setattr(
        harness,
        "_revalidate_connection_path",
        counted_revalidate_connection,
    )
    monkeypatch.setattr(
        harness,
        "_revalidate_connection_alias_free_path",
        counted_alias_walk,
    )

    connection, _ = harness._connect(token, writer=True)
    try:
        assert counts == {"full": 2, "cheap": 0, "pinned": 0, "alias": 0}
        events.clear()
        connection.execute("BEGIN IMMEDIATE").close()
        assert harness._verify_operation_snapshot(connection, token, writer=True)
        assert counts == {"full": 2, "cheap": 2, "pinned": 2, "alias": 1}
        assert events == ["cheap", "pinned", "cheap", "pinned", "alias"]

        harness._verify_operation_authority(connection, token)
        assert counts == {"full": 3, "cheap": 2, "pinned": 3, "alias": 1}
        assert events[-2:] == ["full", "pinned"]
        terminal_events = tuple(events)
        connection.execute("COMMIT").close()
        assert tuple(events) == terminal_events

        for name in counts:
            counts[name] = 0
        events.clear()
        assert harness._verify_operation_snapshot(connection, token, writer=True)
        assert counts == {"full": 2, "cheap": 0, "pinned": 2, "alias": 0}
        assert events == ["full", "pinned", "full", "pinned"]
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_verify_store_uses_exact_reduced_authority_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    counts = {"full": 0, "cheap": 0, "alias": 0}
    real_require_token = harness._require_token
    real_live_authority = harness._require_live_transaction_authority
    real_alias_walk = harness._revalidate_connection_alias_free_path

    def counted_require_token(token_value: harness.StoreToken) -> harness._RegisteredIdentity:
        counts["full"] += 1
        return real_require_token(token_value)

    def counted_live_authority(
        observed_connection: sqlite3.Connection,
        token_value: harness.StoreToken,
        writer: bool,
    ) -> harness._LiveTransactionAuthorityView:
        counts["cheap"] += 1
        return real_live_authority(observed_connection, token_value, writer)

    def counted_alias_walk(
        identity: harness._RegisteredIdentity,
        observed_connection: sqlite3.Connection,
    ) -> None:
        counts["alias"] += 1
        real_alias_walk(identity, observed_connection)

    monkeypatch.setattr(harness, "_require_token", counted_require_token)
    monkeypatch.setattr(
        harness,
        "_require_live_transaction_authority",
        counted_live_authority,
    )
    monkeypatch.setattr(
        harness,
        "_revalidate_connection_alias_free_path",
        counted_alias_walk,
    )
    summary = harness.verify_store(token)
    assert summary.stream_count == 0
    assert counts == {"full": 8, "cheap": 4, "alias": 2}


def test_transaction_authority_captures_filesystem_and_token_dependencies(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    forbidden_calls = 0

    def forbidden(*_args: object, **_kwargs: object) -> Any:
        nonlocal forbidden_calls
        forbidden_calls += 1
        raise AssertionError("transaction verification used a rebound dependency")

    try:
        connection.execute("BEGIN IMMEDIATE").close()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(harness, "_require_token", forbidden)
            patch.setattr(harness, "_identity_for", forbidden)
            patch.setattr(harness, "_walk_without_aliases", forbidden)
            patch.setattr(Path, "resolve", forbidden)
            patch.setattr(os, "open", forbidden)
            patch.setattr(os, "stat", forbidden)
            patch.setattr(os, "fstat", forbidden)
            patch.setattr(os, "listdir", forbidden)
            assert harness._verify_operation_snapshot(connection, token, writer=True)
        assert forbidden_calls == 0
        connection.execute("ROLLBACK").close()
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_transaction_database_list_must_match_the_sealed_connection_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    real_fetch_all = harness._fetch_all

    def forged_database_list(
        observed_connection: sqlite3.Connection,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> list[sqlite3.Row]:
        if sql == "PRAGMA database_list":
            return cast(
                list[sqlite3.Row],
                [{"name": "main", "file": "/forged/store.sqlite3"}],
            )
        return real_fetch_all(observed_connection, sql, parameters)

    try:
        connection.execute("BEGIN IMMEDIATE").close()
        monkeypatch.setattr(harness, "_fetch_all", forged_database_list)
        with pytest.raises(harness.HarnessFailure) as rejected:
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_transaction_namespace_drift_is_rejected_at_both_authority_seams(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    unexpected_path = token._generation_root / "unexpected-authority-entry"
    real_fetch_all = harness._fetch_all
    sql_calls = 0

    def counted_fetch_all(
        observed_connection: sqlite3.Connection,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> list[sqlite3.Row]:
        nonlocal sql_calls
        sql_calls += 1
        return real_fetch_all(observed_connection, sql, parameters)

    def create_unexpected_entry() -> None:
        descriptor = os.open(
            unexpected_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)

    try:
        connection.execute("BEGIN IMMEDIATE").close()
        create_unexpected_entry()
        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(harness.HarnessFailure) as start_rejected,
        ):
            patch.setattr(harness, "_fetch_all", counted_fetch_all)
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert start_rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert sql_calls == 0
        unexpected_path.unlink()

        real_verify_schema_identity = harness._verify_schema_identity
        schema_calls = 0

        def drift_after_schema(observed_connection: sqlite3.Connection) -> str:
            nonlocal schema_calls
            schema_calls += 1
            fingerprint = real_verify_schema_identity(observed_connection)
            create_unexpected_entry()
            return fingerprint

        with (
            pytest.MonkeyPatch.context() as patch,
            pytest.raises(harness.HarnessFailure) as end_rejected,
        ):
            patch.setattr(harness, "_verify_schema_identity", drift_after_schema)
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert end_rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
        assert schema_calls == 1
    finally:
        unexpected_path.unlink(missing_ok=True)
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


def test_optional_file_seals_are_private_immutable_one_shot_bindings(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    optional_path = token._generation_root / "store.sqlite3-wal"
    optional_path.unlink(missing_ok=True)
    identity = harness._require_token(token)
    snapshot = harness._open_operation_path_snapshot(identity)
    closure = inspect.getclosurevars(harness._revalidate_operation_path_snapshot).nonlocals
    valid_snapshot = cast(Callable[..., object], closure["valid_snapshot"])
    snapshot_records = cast(
        dict[int, dict[str, object]],
        inspect.getclosurevars(valid_snapshot).nonlocals["snapshot_records"],
    )
    optional_seal_states = cast(dict[int, object], closure["optional_seal_states"])
    record = snapshot_records[id(snapshot)]
    original_state = record["optional_seal_state"]
    try:
        original_seals = cast(
            tuple[tuple[str, tuple[int, int, int, int, int]], ...],
            cast(tuple[object, ...], original_state)[1],
        )
        copied_state = (snapshot, tuple(list(original_seals)))
        record["optional_seal_state"] = copied_state
        try:
            with pytest.raises(harness.HarnessFailure) as reconstructed:
                harness._revalidate_operation_path_snapshot(identity, snapshot)
            assert reconstructed.value.code is harness.HarnessFailureCode.UNAVAILABLE
        finally:
            record["optional_seal_state"] = original_state

        record.pop("optional_seal_state")
        try:
            with pytest.raises(harness.HarnessFailure) as deleted:
                harness._revalidate_operation_path_snapshot(identity, snapshot)
            assert deleted.value.code is harness.HarnessFailureCode.UNAVAILABLE
        finally:
            record["optional_seal_state"] = original_state

        descriptor = os.open(
            optional_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        os.close(descriptor)
        harness._revalidate_operation_path_snapshot(identity, snapshot)
        sealed_state = record["optional_seal_state"]
        assert sealed_state is optional_seal_states[id(snapshot)]
        assert sealed_state is not original_state

        record["optional_seal_state"] = original_state
        try:
            with pytest.raises(harness.HarnessFailure) as rolled_back:
                harness._revalidate_operation_path_snapshot(identity, snapshot)
            assert rolled_back.value.code is harness.HarnessFailureCode.UNAVAILABLE
        finally:
            record["optional_seal_state"] = sealed_state
    finally:
        harness._close_operation_path_snapshot(snapshot)
        optional_path.unlink(missing_ok=True)


def test_live_transaction_authority_rejects_hostile_token_and_connection_binding(
    tmp_path: Path,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    first_token = harness.bootstrap_store(tmp_path)
    second_token = harness.bootstrap_store(_active_task064_pytest_root.roots[0])
    first, _ = harness._connect(first_token, writer=True)
    second, _ = harness._connect(second_token, writer=True)
    records = _connection_runtime_records()
    first_record = records[id(first)]
    second_record = records[id(second)]

    def require_code(
        code: harness.HarnessFailureCode,
        action: Callable[[], object],
    ) -> None:
        with pytest.raises(harness.HarnessFailure) as rejected:
            action()
        assert rejected.value.code is code

    try:
        first.execute("BEGIN IMMEDIATE").close()
        second.execute("BEGIN IMMEDIATE").close()

        first_view = harness._require_live_transaction_authority(
            first,
            first_token,
            True,
        )
        second_view = harness._require_live_transaction_authority(
            first,
            first_token,
            True,
        )
        assert first_view == second_view
        assert first_view is not second_view
        assert first_view.registered is not second_view.registered

        clone = harness.StoreToken(
            _nonce=first_token._nonce,
            _pytest_root=first_token._pytest_root,
            _generation_root=first_token._generation_root,
            _database_path=first_token._database_path,
            _device=first_token._device,
            _inode=first_token._inode,
            _uid=first_token._uid,
            _mode=first_token._mode,
            _link_count=first_token._link_count,
        )
        for _ in range(2):
            require_code(
                harness.HarnessFailureCode.INVALID_TOKEN,
                lambda: harness._require_live_transaction_authority(first, clone, True),
            )
        assert harness._require_live_transaction_authority(first, first_token, True)

        original_mode = first_token._mode
        object.__setattr__(first_token, "_mode", original_mode ^ 1)
        try:
            require_code(
                harness.HarnessFailureCode.INVALID_TOKEN,
                lambda: harness._require_live_transaction_authority(
                    first,
                    first_token,
                    True,
                ),
            )
        finally:
            object.__setattr__(first_token, "_mode", original_mode)
        assert harness._require_live_transaction_authority(first, first_token, True)

        require_code(
            harness.HarnessFailureCode.INVALID_TOKEN,
            lambda: contextvars.Context().run(
                harness._require_live_transaction_authority,
                first,
                first_token,
                True,
            ),
        )
        require_code(
            harness.HarnessFailureCode.UNAVAILABLE,
            lambda: harness._require_live_transaction_authority(
                first,
                first_token,
                False,
            ),
        )
        require_code(
            harness.HarnessFailureCode.UNAVAILABLE,
            lambda: harness._require_live_transaction_authority(
                first,
                second_token,
                True,
            ),
        )

        swapped_fields = ("snapshot", "snapshot_record")
        first_values = tuple(first_record[name] for name in swapped_fields)
        second_values = tuple(second_record[name] for name in swapped_fields)
        for name, value in zip(swapped_fields, second_values, strict=True):
            first_record[name] = value
        try:
            require_code(
                harness.HarnessFailureCode.UNAVAILABLE,
                lambda: harness._require_live_transaction_authority(
                    first,
                    first_token,
                    True,
                ),
            )
        finally:
            for name, value in zip(swapped_fields, first_values, strict=True):
                first_record[name] = value

        nonce_fields = ("nonce", "nonce_identity")
        first_values = tuple(first_record[name] for name in nonce_fields)
        second_values = tuple(second_record[name] for name in nonce_fields)
        for name, value in zip(nonce_fields, second_values, strict=True):
            first_record[name] = value
        try:
            require_code(
                harness.HarnessFailureCode.UNAVAILABLE,
                lambda: harness._require_live_transaction_authority(
                    first,
                    first_token,
                    True,
                ),
            )
        finally:
            for name, value in zip(nonce_fields, first_values, strict=True):
                first_record[name] = value

        creator_pid = first_record["creator_pid"]
        first_record["creator_pid"] = os.getpid() + 1
        try:
            require_code(
                harness.HarnessFailureCode.UNAVAILABLE,
                lambda: harness._require_live_transaction_authority(
                    first,
                    first_token,
                    True,
                ),
            )
        finally:
            first_record["creator_pid"] = creator_pid

        read_descriptor, write_descriptor = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_descriptor)
            payload = b"E"
            try:
                child_os: Any = harness.__dict__["os"]
                child_os.getpid = lambda: os.getppid()
                harness._require_live_transaction_authority(first, first_token, True)
            except harness.HarnessFailure as error:
                if error.code is harness.HarnessFailureCode.UNAVAILABLE:
                    payload = b"P"
            except BaseException:
                payload = b"E"
            with contextlib.suppress(OSError):
                os.write(write_descriptor, payload)
            os._exit(0 if payload == b"P" else 70)
        parent_error: BaseException | None = None
        payload = b""
        try:
            os.close(write_descriptor)
        except BaseException as error:
            parent_error = error
        if parent_error is None:
            try:
                payload = os.read(read_descriptor, 1)
            except BaseException as error:
                parent_error = error
        try:
            os.close(read_descriptor)
        except BaseException as error:
            if parent_error is None:
                parent_error = error
        _, child_status = os.waitpid(child_pid, 0)
        _guard_task064_reserved_observer_exit(child_status)
        if parent_error is not None:
            raise parent_error
        assert payload == b"P"
        assert os.WIFEXITED(child_status)
        assert os.WEXITSTATUS(child_status) == 0
        assert harness._require_live_transaction_authority(first, first_token, True)
    finally:
        if first.in_transaction:
            first.execute("ROLLBACK").close()
        if second.in_transaction:
            second.execute("ROLLBACK").close()
        first.close()
        second.close()


def test_connection_runtime_binding_rejects_hostile_lifecycle_and_cross_binding(
    tmp_path: Path,
    _active_task064_pytest_root: harness._PytestRootCapability,
) -> None:
    first_token = harness.bootstrap_store(tmp_path)
    second_token = harness.bootstrap_store(_active_task064_pytest_root.roots[0])
    first, _ = harness._connect(first_token, writer=True)
    second, _ = harness._connect(second_token, writer=False)
    records = _connection_runtime_records()
    first_record = records[id(first)]
    second_record = records[id(second)]
    try:
        for action in (
            lambda: harness._capture_connection_immutable_runtime(first),
            lambda: harness._seal_connection_immutable_runtime(first),
            lambda: harness._consume_connection_immutable_runtime(
                first,
                b"x" * 32,
                True,
            ),
            lambda: harness._consume_connection_immutable_runtime(
                first,
                first_token._nonce,
                False,
            ),
            lambda: harness._consume_connection_immutable_runtime(
                first,
                second_token._nonce,
                True,
            ),
        ):
            with pytest.raises(harness.HarnessFailure) as rejected:
                action()
            assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE

        for field, hostile_value in (
            ("creator_pid", os.getpid() + 1),
            ("state", "CLOSING"),
            ("state", "CLOSE_UNCERTAIN"),
            ("runtime_state", "UNBOUND"),
            ("runtime_state", "CAPTURED"),
        ):
            original = first_record[field]
            first_record[field] = hostile_value
            try:
                with pytest.raises(harness.HarnessFailure) as rejected:
                    harness._consume_connection_immutable_runtime(
                        first,
                        first_token._nonce,
                        True,
                    )
                assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
            finally:
                first_record[field] = original

        swapped_fields = (
            "runtime_evidence",
            "runtime_evidence_fields",
            "runtime_evidence_binding",
            "runtime_seal",
        )
        first_values = tuple(first_record[name] for name in swapped_fields)
        second_values = tuple(second_record[name] for name in swapped_fields)
        for name, value in zip(swapped_fields, second_values, strict=True):
            first_record[name] = value
        for name, value in zip(swapped_fields, first_values, strict=True):
            second_record[name] = value
        try:
            for connection, token, writer in (
                (first, first_token, True),
                (second, second_token, False),
            ):
                with pytest.raises(harness.HarnessFailure) as rejected:
                    harness._consume_connection_immutable_runtime(
                        connection,
                        token._nonce,
                        writer,
                    )
                assert rejected.value.code is harness.HarnessFailureCode.UNAVAILABLE
        finally:
            for name, value in zip(swapped_fields, first_values, strict=True):
                first_record[name] = value
            for name, value in zip(swapped_fields, second_values, strict=True):
                second_record[name] = value

        unregistered = sqlite3.connect(":memory:")
        try:
            with pytest.raises(harness.HarnessFailure) as missing:
                harness._consume_connection_immutable_runtime(
                    unregistered,
                    first_token._nonce,
                    True,
                )
            assert missing.value.code is harness.HarnessFailureCode.UNAVAILABLE
        finally:
            unregistered.close()
    finally:
        first.close()
        second.close()

    with pytest.raises(harness.HarnessFailure) as closed:
        harness._consume_connection_immutable_runtime(
            first,
            first_token._nonce,
            True,
        )
    assert closed.value.code is harness.HarnessFailureCode.UNAVAILABLE


def test_connection_runtime_creator_pid_rejects_inherited_authority(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    read_descriptor, write_descriptor = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_descriptor)
        payload = b"E"
        try:
            child_os: Any = harness.__dict__["os"]
            child_os.getpid = lambda: os.getppid()
            harness._consume_connection_immutable_runtime(
                connection,
                token._nonce,
                True,
            )
        except harness.HarnessFailure as error:
            if error.code is harness.HarnessFailureCode.UNAVAILABLE:
                payload = b"P"
        except BaseException:
            payload = b"E"
        try:
            os.write(write_descriptor, payload)
        finally:
            os.close(write_descriptor)
        os._exit(0 if payload == b"P" else 70)
    parent_error: BaseException | None = None
    payload = b""
    try:
        os.close(write_descriptor)
    except BaseException as error:
        parent_error = error
    if parent_error is None:
        try:
            payload = os.read(read_descriptor, 1)
        except BaseException as error:
            parent_error = error
    try:
        os.close(read_descriptor)
    except BaseException as error:
        if parent_error is None:
            parent_error = error
    _, status = os.waitpid(child_pid, 0)
    _guard_task064_reserved_observer_exit(status)
    if parent_error is not None:
        raise parent_error
    assert payload == b"P"
    assert os.WIFEXITED(status)
    assert os.WEXITSTATUS(status) == 0
    assert (
        harness._consume_connection_immutable_runtime(
            connection,
            token._nonce,
            True,
        ).sqlite_source_id
        == harness.ACCEPTED_SQLITE_SOURCE_ID
    )
    connection.close()
    assert not harness._has_fork_unsafe_connection_authority()


def test_fresh_mutable_control_and_schema_format_checks_survive_runtime_reuse(
    tmp_path: Path,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    connection, _ = harness._connect(token, writer=True)
    try:
        connection.execute("PRAGMA cache_size = -4096").close()
        connection.execute("BEGIN IMMEDIATE").close()
        with pytest.raises(harness.HarnessFailure) as cache_drift:
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert cache_drift.value.code is harness.HarnessFailureCode.UNAVAILABLE
        connection.execute("ROLLBACK").close()

        connection.execute("PRAGMA cache_size = -8192").close()
        connection.execute("PRAGMA user_version = 2").close()
        connection.execute("BEGIN IMMEDIATE").close()
        with pytest.raises(harness.HarnessFailure) as format_drift:
            harness._verify_operation_snapshot(connection, token, writer=True)
        assert format_drift.value.code is harness.HarnessFailureCode.CORRUPT
        connection.execute("ROLLBACK").close()
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK").close()
        connection.close()


@pytest.mark.parametrize("partial_registration", (False, True))
def test_failed_connection_registration_is_failure_atomic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    partial_registration: bool,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    captured_connections: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect
    real_register = harness._register_live_connection
    sentinel = RuntimeError("synthetic registration failure")

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        captured_connections.append(connection)
        return connection

    def rejected_register(*args: Any, **kwargs: Any) -> None:
        if partial_registration:
            real_register(*args, **kwargs)
        raise sentinel

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(harness, "_register_live_connection", rejected_register)
    with pytest.raises(RuntimeError) as rejected:
        harness._connect(token, writer=True)
    assert rejected.value is sentinel
    assert len(captured_connections) == 1
    connection = captured_connections[0]
    assert harness._connection_authority_state(connection) == "CLOSED"
    assert id(connection) not in _connection_runtime_records()
    assert not harness._has_fork_unsafe_connection_authority()


def test_row_factory_failure_after_registration_closes_exact_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token = harness.bootstrap_store(tmp_path)
    captured_connections: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect
    sentinel = RuntimeError("synthetic row-factory failure")

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        captured_connections.append(connection)
        return connection

    def reject_row_factory(
        _connection: sqlite3.Connection,
        _value: object,
    ) -> None:
        raise sentinel

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    monkeypatch.setattr(
        harness._MeteredConnection,
        "row_factory",
        property(fset=reject_row_factory),
    )
    with pytest.raises(RuntimeError) as rejected:
        harness._connect(token, writer=True)
    assert rejected.value is sentinel
    assert len(captured_connections) == 1
    connection = captured_connections[0]
    assert harness._connection_authority_state(connection) == "CLOSED"
    assert id(connection) not in _connection_runtime_records()
    assert not harness._has_fork_unsafe_connection_authority()


def test_audit_bounds_never_evaluate_an_overflowing_sum() -> None:
    initial = harness.audit_bounds(
        current_version=MAX_CONTRACT_INTEGER,
        limit=100,
        continuation_version=None,
    )
    assert initial == harness.AuditBounds(1, 100, 100, 0)

    near_tail = harness.audit_bounds(
        current_version=MAX_CONTRACT_INTEGER,
        limit=100,
        continuation_version=MAX_CONTRACT_INTEGER - 1,
    )
    assert near_tail == harness.AuditBounds(
        MAX_CONTRACT_INTEGER - 1,
        MAX_CONTRACT_INTEGER,
        2,
        1,
    )
    at_tail = harness.audit_bounds(
        current_version=MAX_CONTRACT_INTEGER,
        limit=100,
        continuation_version=MAX_CONTRACT_INTEGER,
    )
    assert at_tail == harness.AuditBounds(
        MAX_CONTRACT_INTEGER,
        MAX_CONTRACT_INTEGER,
        1,
        1,
    )
