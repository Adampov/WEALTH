"""Unit evidence for the TASK-064 version-one SQLite schema."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from wealth.domain.continuous_public_trade import MAX_CONTRACT_INTEGER

if TYPE_CHECKING:
    import support.continuous_public_trade_stream_sqlite_harness as harness
else:
    import tests.support.continuous_public_trade_stream_sqlite_harness as harness

EXPECTED_SCHEMA_FINGERPRINT = (
    "sha256:0410c1f08390a411c73427b3d07c542f3d1828def7c6adebab51cd57375355b3"
)


@pytest.fixture(autouse=True)
def _active_task064_pytest_root(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    with harness._pytest_root_scope(tmp_path, node_id=request.node.nodeid):
        yield


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
