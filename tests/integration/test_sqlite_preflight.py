"""Synthetic-only integration coverage for immutable SQLite fingerprint preflight."""

import hashlib
import os
import shutil
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Never
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.adapters import sqlite_preflight as preflight_adapter
from wealth.adapters.sqlite_collection import SQLiteCollectionCheckpointStore
from wealth.adapters.sqlite_collector_service import SQLiteCollectorServiceHeartbeatStore
from wealth.adapters.sqlite_continuous_collection import (
    SQLiteContinuousCollectionCheckpointStore,
)
from wealth.adapters.sqlite_market import SQLiteCandleStore
from wealth.adapters.sqlite_order_flow import SQLiteOrderFlowStore
from wealth.adapters.sqlite_order_flow_collection import (
    SQLitePublicTradeCollectionCheckpointStore,
)
from wealth.adapters.sqlite_preflight import (
    SQLITE_EXPECTED_STORE_IDENTITIES,
    SQLitePreflightError,
    SQLitePreflightErrorCode,
    _ImmutableSQLiteConnection,
    fingerprint_synthetic_sqlite_fixture,
)
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.adapters.sqlite_reconciliation import SQLiteReconciliationHistoryStore
from wealth.domain.sqlite_preflight import (
    SQLiteMarkerReadStatus,
    SQLitePreflightRequest,
    SQLitePreflightResult,
    SQLitePreflightStatus,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteStoreFingerprint,
)

FixtureFactory = Callable[[Path], None]
Mutation = Callable[[Path], None]


def _create_market(path: Path) -> None:
    SQLiteCandleStore(path)


def _create_order_flow(path: Path) -> None:
    SQLiteOrderFlowStore(path)


def _create_historical_collection(path: Path) -> None:
    SQLiteCollectionCheckpointStore(path)


def _create_continuous_collection(path: Path) -> None:
    SQLiteContinuousCollectionCheckpointStore(path)


def _create_collector_service(path: Path) -> None:
    SQLiteCollectorServiceHeartbeatStore(path)


def _create_public_trade_collection(path: Path) -> None:
    SQLitePublicTradeCollectionCheckpointStore(path)


def _create_rate_budget(path: Path) -> None:
    SQLiteRateBudgetCoordinator(path)


def _create_reconciliation(path: Path) -> None:
    SQLiteReconciliationHistoryStore(path)


FIXTURE_FACTORIES: tuple[tuple[SQLiteStoreFamily, FixtureFactory], ...] = (
    (SQLiteStoreFamily.MARKET, _create_market),
    (SQLiteStoreFamily.ORDER_FLOW, _create_order_flow),
    (SQLiteStoreFamily.HISTORICAL_COLLECTION, _create_historical_collection),
    (SQLiteStoreFamily.CONTINUOUS_COLLECTION, _create_continuous_collection),
    (SQLiteStoreFamily.COLLECTOR_SERVICE, _create_collector_service),
    (SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION, _create_public_trade_collection),
    (SQLiteStoreFamily.RATE_BUDGET, _create_rate_budget),
    (SQLiteStoreFamily.RECONCILIATION, _create_reconciliation),
)

EXPECTED_TABLES: dict[SQLiteStoreFamily, frozenset[str]] = {
    SQLiteStoreFamily.MARKET: frozenset(
        {
            "candle_conflicts",
            "candle_raw_lineage",
            "canonical_candles",
            "raw_market_payloads",
        }
    ),
    SQLiteStoreFamily.ORDER_FLOW: frozenset(
        {
            "canonical_order_flow_records",
            "order_flow_conflicts",
            "order_flow_raw_lineage",
            "order_flow_storage_metadata",
            "raw_order_flow_payloads",
        }
    ),
    SQLiteStoreFamily.HISTORICAL_COLLECTION: frozenset(
        {
            "collection_jobs",
            "collection_transitions",
            "source_health_observations",
        }
    ),
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: frozenset(
        {
            "continuous_collection_checkpoints",
            "continuous_collection_transitions",
        }
    ),
    SQLiteStoreFamily.COLLECTOR_SERVICE: frozenset(
        {
            "collector_service_heartbeats",
            "collector_service_runs",
        }
    ),
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: frozenset(
        {
            "public_trade_collection_jobs",
            "public_trade_collection_leases",
            "public_trade_collection_metadata",
            "public_trade_collection_transitions",
            "public_trade_source_health",
        }
    ),
    SQLiteStoreFamily.RATE_BUDGET: frozenset(
        {
            "rate_budget_reservations",
            "rate_budget_state",
        }
    ),
    SQLiteStoreFamily.RECONCILIATION: frozenset(
        {
            "reconciliation_issue_counts",
            "reconciliation_observations",
            "reconciliation_series",
        }
    ),
}

EXPECTED_INDEX_COUNTS: dict[SQLiteStoreFamily, int] = {
    SQLiteStoreFamily.MARKET: 6,
    SQLiteStoreFamily.ORDER_FLOW: 8,
    SQLiteStoreFamily.HISTORICAL_COLLECTION: 5,
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: 4,
    SQLiteStoreFamily.COLLECTOR_SERVICE: 5,
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: 8,
    SQLiteStoreFamily.RATE_BUDGET: 3,
    SQLiteStoreFamily.RECONCILIATION: 5,
}


def _request(path: Path, family: SQLiteStoreFamily) -> SQLitePreflightRequest:
    return SQLitePreflightRequest(
        source_kind="generated_synthetic_fixture",
        fixture_id=UUID(int=tuple(SQLiteStoreFamily).index(family) + 1),
        fixture_path=path,
        expected_family=family,
        expected_layout_version=1,
    )


def _execute_script(path: Path, sql: str) -> None:
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript(sql)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    ("family", "factory"),
    FIXTURE_FACTORIES,
    ids=lambda value: value.value if isinstance(value, SQLiteStoreFamily) else None,
)
def test_all_eight_registered_layouts_match_deterministically_without_mutation(
    tmp_path: Path,
    family: SQLiteStoreFamily,
    factory: FixtureFactory,
) -> None:
    fixture_path = tmp_path / f"{family.value} fixture # percent% unicodé.sqlite3"
    factory(fixture_path)
    request = _request(fixture_path, family)
    entries_before = tuple(sorted(entry.name for entry in tmp_path.iterdir()))
    hash_before = _sha256(fixture_path)
    stat_before = fixture_path.stat()

    first = fingerprint_synthetic_sqlite_fixture(request)
    second = fingerprint_synthetic_sqlite_fixture(request)

    assert first.status is SQLitePreflightStatus.MATCHED
    assert first.matched_families == (family,)
    assert first.expected_identity.family is family
    assert first.observation.fingerprint == second.observation.fingerprint
    assert first.observation.fingerprint.store_sha256 == first.expected_identity.store_sha256
    assert first.observation.fingerprint.encoding == "UTF-8"
    assert first.observation.fingerprint.application_id == 0
    assert first.observation.fingerprint.user_version == 1
    assert {table.name for table in first.observation.fingerprint.tables} == EXPECTED_TABLES[family]
    assert len(first.observation.fingerprint.indexes) == EXPECTED_INDEX_COUNTS[family]
    assert first.observation.fingerprint.triggers == ()
    assert first.observation.source_before == first.observation.source_after
    assert first.observation.directory_entries_before == entries_before
    assert first.observation.directory_entries_after == entries_before
    assert _sha256(fixture_path) == hash_before
    assert fixture_path.stat().st_size == stat_before.st_size
    assert fixture_path.stat().st_mtime_ns == stat_before.st_mtime_ns
    assert tuple(sorted(entry.name for entry in tmp_path.iterdir())) == entries_before
    assert not any(
        os.path.lexists(fixture_path.with_name(fixture_path.name + suffix))
        for suffix in ("-journal", "-wal", "-shm")
    )


def test_marker_evidence_preserves_storage_class_and_exact_bytes(tmp_path: Path) -> None:
    order_flow_path = tmp_path / "order-flow.sqlite3"
    public_trade_path = tmp_path / "public-trade.sqlite3"
    _create_order_flow(order_flow_path)
    _create_public_trade_collection(public_trade_path)

    order_flow = fingerprint_synthetic_sqlite_fixture(
        _request(order_flow_path, SQLiteStoreFamily.ORDER_FLOW)
    )
    public_trade = fingerprint_synthetic_sqlite_fixture(
        _request(public_trade_path, SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION)
    )

    order_flow_marker = order_flow.observation.fingerprint.markers[0]
    assert order_flow_marker.read_status is SQLiteMarkerReadStatus.READABLE
    assert order_flow_marker.rows[0].values[0].storage_class is SQLiteStorageClass.TEXT
    assert order_flow_marker.rows[0].values[0].blob_hex == b"wealth.order_flow".hex().upper()
    assert order_flow_marker.rows[0].values[1].storage_class is SQLiteStorageClass.INTEGER
    assert order_flow_marker.rows[0].values[1].blob_hex == "31"
    assert (
        public_trade.observation.fingerprint.markers[0].rows[0].values[0].blob_hex
        == b"wealth.public_trade_collection".hex().upper()
    )


def test_wrong_expected_family_is_rejected_before_any_row_scan_authorization(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)

    result = fingerprint_synthetic_sqlite_fixture(
        _request(fixture_path, SQLiteStoreFamily.ORDER_FLOW)
    )

    assert result.status is SQLitePreflightStatus.WRONG_FAMILY
    assert result.matched_families == (SQLiteStoreFamily.MARKET,)
    assert result.expected_identity.family is SQLiteStoreFamily.ORDER_FLOW


def test_result_contract_rejects_a_success_claim_with_different_observed_evidence(
    tmp_path: Path,
) -> None:
    market_path = tmp_path / "market.sqlite3"
    order_flow_path = tmp_path / "order-flow.sqlite3"
    _create_market(market_path)
    _create_order_flow(order_flow_path)
    market = fingerprint_synthetic_sqlite_fixture(_request(market_path, SQLiteStoreFamily.MARKET))
    order_flow = fingerprint_synthetic_sqlite_fixture(
        _request(order_flow_path, SQLiteStoreFamily.ORDER_FLOW)
    )
    contradictory_payload = market.model_dump()
    contradictory_payload["observation"] = order_flow.observation

    with pytest.raises(ValidationError, match="matched SQLite preflight"):
        SQLitePreflightResult.model_validate(contradictory_payload)


@pytest.mark.parametrize(
    "mutation_sql",
    [
        "CREATE TABLE unexpected_table (value TEXT);",
        "ALTER TABLE canonical_candles RENAME TO renamed_canonical_candles;",
        "ALTER TABLE canonical_candles ADD COLUMN unexpected_column TEXT;",
        "DROP INDEX candle_conflicts_stream_index;",
        """
        CREATE INDEX unexpected_index
        ON canonical_candles (source COLLATE NOCASE DESC);
        """,
        """
        CREATE TRIGGER unexpected_trigger
        AFTER INSERT ON canonical_candles
        BEGIN
            SELECT 1;
        END;
        """,
        "CREATE VIEW unexpected_view AS SELECT 1 AS value;",
        "CREATE TABLE sqliteXextra (value TEXT);",
        "PRAGMA user_version = 7;",
        "PRAGMA application_id = 42;",
    ],
)
def test_missing_extra_renamed_altered_and_spoofed_layouts_are_rejected(
    tmp_path: Path,
    mutation_sql: str,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    _execute_script(fixture_path, mutation_sql)

    result = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert result.status is SQLitePreflightStatus.MISMATCH
    assert result.matched_families == ()


@pytest.mark.parametrize(
    "mutation_sql",
    [
        "DELETE FROM order_flow_storage_metadata;",
        """
        UPDATE order_flow_storage_metadata
        SET storage_format = 'wealth.fake_order_flow';
        """,
        """
        UPDATE order_flow_storage_metadata
        SET schema_version = CAST(1.9 AS REAL);
        """,
        """
        INSERT INTO order_flow_storage_metadata (storage_format, schema_version)
        VALUES ('wealth.order_flow.extra', 1);
        """,
        """
        ALTER TABLE order_flow_storage_metadata
        RENAME COLUMN schema_version TO renamed_schema_version;
        """,
    ],
)
def test_marker_spoofing_extra_rows_and_incompatible_columns_are_rejected(
    tmp_path: Path,
    mutation_sql: str,
) -> None:
    fixture_path = tmp_path / "order-flow.sqlite3"
    _create_order_flow(fixture_path)
    _execute_script(fixture_path, mutation_sql)

    result = fingerprint_synthetic_sqlite_fixture(
        _request(fixture_path, SQLiteStoreFamily.ORDER_FLOW)
    )

    assert result.status is SQLitePreflightStatus.MISMATCH
    assert result.matched_families == ()
    if "RENAME COLUMN" in mutation_sql:
        assert (
            result.observation.fingerprint.markers[0].read_status
            is SQLiteMarkerReadStatus.INCOMPATIBLE_COLUMNS
        )


def test_oversized_marker_cell_is_rejected_before_hex_materialization(tmp_path: Path) -> None:
    fixture_path = tmp_path / "order-flow.sqlite3"
    _create_order_flow(fixture_path)
    oversized_marker = "x" * 4097
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute(
            """
            UPDATE order_flow_storage_metadata
            SET storage_format = ?
            """,
            (oversized_marker,),
        )

    with pytest.raises(SQLitePreflightError) as captured:
        fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.ORDER_FLOW))

    assert captured.value.code is SQLitePreflightErrorCode.RESOURCE_LIMIT


def test_user_version_only_spoof_and_combined_layout_match_no_family(tmp_path: Path) -> None:
    spoof_path = tmp_path / "spoof.sqlite3"
    _execute_script(
        spoof_path,
        """
        CREATE TABLE fake (value TEXT);
        PRAGMA user_version = 1;
        """,
    )
    combined_path = tmp_path / "combined.sqlite3"
    _create_market(combined_path)
    _execute_script(
        combined_path,
        """
        CREATE TABLE rate_budget_state (
            budget_key TEXT PRIMARY KEY,
            capacity INTEGER,
            period_seconds REAL,
            interval_microseconds INTEGER,
            theoretical_arrival_us INTEGER,
            last_observed_us INTEGER,
            version INTEGER
        );
        """,
    )

    spoof = fingerprint_synthetic_sqlite_fixture(_request(spoof_path, SQLiteStoreFamily.MARKET))
    combined = fingerprint_synthetic_sqlite_fixture(
        _request(combined_path, SQLiteStoreFamily.MARKET)
    )

    assert spoof.status is SQLitePreflightStatus.MISMATCH
    assert combined.status is SQLitePreflightStatus.MISMATCH


def test_utf16_fixture_is_rejected_even_when_the_layout_and_version_match(tmp_path: Path) -> None:
    fixture_path = tmp_path / "utf16.sqlite3"
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA encoding = 'UTF-16'")
        connection.execute("CREATE TABLE encoding_seed (value TEXT)")
        connection.execute("DROP TABLE encoding_seed")
    _create_market(fixture_path)

    result = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert result.status is SQLitePreflightStatus.MISMATCH
    assert result.observation.fingerprint.encoding in {"UTF-16le", "UTF-16be"}


def test_duplicate_registry_digest_is_rejected_as_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    market_identity = SQLITE_EXPECTED_STORE_IDENTITIES[0]
    duplicate_identity = market_identity.model_copy(update={"family": SQLiteStoreFamily.ORDER_FLOW})
    monkeypatch.setattr(
        preflight_adapter,
        "SQLITE_EXPECTED_STORE_IDENTITIES",
        (*SQLITE_EXPECTED_STORE_IDENTITIES, duplicate_identity),
    )

    result = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert result.status is SQLitePreflightStatus.AMBIGUOUS
    assert result.matched_families == (
        SQLiteStoreFamily.MARKET,
        SQLiteStoreFamily.ORDER_FLOW,
    )


@pytest.mark.parametrize(
    "matched_families",
    [
        (SQLiteStoreFamily.ORDER_FLOW, SQLiteStoreFamily.RATE_BUDGET),
        (SQLiteStoreFamily.MARKET, SQLiteStoreFamily.MARKET),
    ],
)
def test_result_contract_rejects_contradictory_or_duplicate_ambiguity_claims(
    tmp_path: Path,
    matched_families: tuple[SQLiteStoreFamily, SQLiteStoreFamily],
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    matched = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))
    payload = matched.model_dump()
    payload["status"] = SQLitePreflightStatus.AMBIGUOUS
    payload["matched_families"] = matched_families

    with pytest.raises(ValidationError, match="ambiguous SQLite preflight"):
        SQLitePreflightResult.model_validate(payload)


@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
def test_preexisting_sidecars_are_rejected_before_opening(
    tmp_path: Path,
    suffix: str,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    sidecar = fixture_path.with_name(fixture_path.name + suffix)
    sidecar.write_bytes(b"synthetic-sidecar")

    with pytest.raises(SQLitePreflightError) as captured:
        fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert captured.value.code is SQLitePreflightErrorCode.SIDECAR_PRESENT
    assert sidecar.read_bytes() == b"synthetic-sidecar"


def test_active_wal_is_rejected_instead_of_inspecting_a_stale_main_file(tmp_path: Path) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    connection = sqlite3.connect(fixture_path)
    try:
        assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
        connection.execute("PRAGMA wal_autocheckpoint = 0")
        connection.execute("CREATE TABLE committed_only_in_wal (value TEXT)")
        connection.commit()
        assert fixture_path.with_name(fixture_path.name + "-wal").is_file()

        with pytest.raises(SQLitePreflightError) as captured:
            fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

        assert captured.value.code is SQLitePreflightErrorCode.SIDECAR_PRESENT
    finally:
        connection.close()


def test_missing_directory_and_malformed_sources_fail_without_creating_files(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing.sqlite3"
    entries_before = tuple(tmp_path.iterdir())

    with pytest.raises(SQLitePreflightError) as missing:
        fingerprint_synthetic_sqlite_fixture(_request(missing_path, SQLiteStoreFamily.MARKET))
    with pytest.raises(SQLitePreflightError) as directory:
        fingerprint_synthetic_sqlite_fixture(_request(tmp_path, SQLiteStoreFamily.MARKET))

    malformed_path = tmp_path / "malformed.sqlite3"
    malformed_path.write_bytes(b"not a SQLite database")
    with pytest.raises(SQLitePreflightError) as malformed:
        fingerprint_synthetic_sqlite_fixture(_request(malformed_path, SQLiteStoreFamily.MARKET))

    assert missing.value.code is SQLitePreflightErrorCode.MISSING_SOURCE
    assert directory.value.code is SQLitePreflightErrorCode.INVALID_SOURCE
    assert malformed.value.code is SQLitePreflightErrorCode.SQLITE_READ_FAILED
    assert not missing_path.exists()
    assert tuple(entry for entry in tmp_path.iterdir() if entry != malformed_path) == entries_before


def test_symbolic_link_source_is_rejected_when_supported(tmp_path: Path) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    link_path = tmp_path / "linked.sqlite3"
    _create_market(fixture_path)
    try:
        link_path.symlink_to(fixture_path)
    except OSError:
        pytest.skip("symbolic-link creation is not available in this Windows environment")

    with pytest.raises(SQLitePreflightError) as captured:
        fingerprint_synthetic_sqlite_fixture(_request(link_path, SQLiteStoreFamily.MARKET))

    assert captured.value.code is SQLitePreflightErrorCode.SYMLINK_SOURCE


def test_hard_link_source_is_rejected_when_supported(tmp_path: Path) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    hard_link_path = tmp_path / "hard-linked.sqlite3"
    _create_market(fixture_path)
    try:
        os.link(fixture_path, hard_link_path)
    except OSError:
        pytest.skip("hard-link creation is not available in this environment")

    with pytest.raises(SQLitePreflightError) as captured:
        fingerprint_synthetic_sqlite_fixture(_request(hard_link_path, SQLiteStoreFamily.MARKET))

    assert captured.value.code is SQLitePreflightErrorCode.ALIASED_SOURCE


def test_immutable_connection_denies_every_write_attach_and_temp_escape(tmp_path: Path) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    attach_path = tmp_path / "attached.sqlite3"
    _create_market(fixture_path)
    hash_before = _sha256(fixture_path)
    connection = preflight_adapter._open_immutable_connection(fixture_path.resolve())
    try:
        statements = (
            "SELECT record_json FROM canonical_candles",
            "WITH RECURSIVE values_cte(value) AS "
            "(SELECT 1 UNION ALL SELECT value + 1 FROM values_cte WHERE value < 2) "
            "SELECT value FROM values_cte",
            "CREATE TABLE denied_create (value TEXT)",
            "INSERT INTO canonical_candles (record_id) VALUES ('denied')",
            "UPDATE canonical_candles SET record_id = 'denied'",
            "DELETE FROM canonical_candles",
            "DROP TABLE canonical_candles",
            "ALTER TABLE canonical_candles ADD COLUMN denied TEXT",
            "CREATE TEMP TABLE denied_temp (value TEXT)",
            f"ATTACH DATABASE '{attach_path.as_posix()}' AS denied_attach",
            "VACUUM",
            "PRAGMA user_version = 99",
            "PRAGMA query_only = OFF",
        )
        for statement in statements:
            with pytest.raises(sqlite3.DatabaseError):
                connection.execute(statement)
        assert not hasattr(connection, "backup")
        assert not hasattr(connection, "set_authorizer")
        assert not hasattr(connection, "blobopen")
        assert not hasattr(connection, "deserialize")
        assert not hasattr(connection, "enable_load_extension")
        metadata_cursor = connection.execute("PRAGMA encoding")
        assert not hasattr(metadata_cursor, "connection")
    finally:
        connection.close()

    assert not attach_path.exists()
    assert _sha256(fixture_path) == hash_before
    assert not any(
        os.path.lexists(fixture_path.with_name(fixture_path.name + suffix))
        for suffix in ("-journal", "-wal", "-shm")
    )


def test_connection_uses_one_encoded_immutable_read_only_uri(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "fixture space # percent% unicodé.sqlite3"
    _create_market(fixture_path)
    real_connect = sqlite3.connect
    calls: list[tuple[str, bool]] = []

    def recording_connect(database: str, *, uri: bool = False) -> sqlite3.Connection:
        calls.append((database, uri))
        return real_connect(database, uri=uri)

    monkeypatch.setattr(sqlite3, "connect", recording_connect)

    result = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert result.status is SQLitePreflightStatus.MATCHED
    assert len(calls) == 1
    database, uri = calls[0]
    assert uri is True
    assert database.endswith("?mode=ro&immutable=1")
    assert "%20" in database
    assert "%23" in database
    assert "%25" in database


def test_only_schema_metadata_pragmas_and_approved_marker_rows_are_queried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "order-flow.sqlite3"
    _create_order_flow(fixture_path)
    real_open = preflight_adapter._open_immutable_connection
    statements: list[str] = []

    def tracing_open(path: Path) -> _ImmutableSQLiteConnection:
        connection = real_open(path)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(preflight_adapter, "_open_immutable_connection", tracing_open)

    result = fingerprint_synthetic_sqlite_fixture(
        _request(fixture_path, SQLiteStoreFamily.ORDER_FLOW)
    )

    assert result.status is SQLitePreflightStatus.MATCHED
    user_table_reads = tuple(
        statement
        for statement in statements
        if ' FROM main."' in statement and "storage_metadata" not in statement
    )
    assert user_table_reads == ()
    assert sum('FROM main."order_flow_storage_metadata"' in item for item in statements) == 1
    assert not any(
        "raw_order_flow_payloads" in item and 'FROM main."' in item for item in statements
    )
    assert not any(
        "canonical_order_flow_records" in item and 'FROM main."' in item for item in statements
    )


def test_inspection_never_calls_schema_installing_adapter_methods(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    _create_market(fixture_path)
    adapter_classes = (
        SQLiteCandleStore,
        SQLiteOrderFlowStore,
        SQLiteCollectionCheckpointStore,
        SQLiteContinuousCollectionCheckpointStore,
        SQLiteCollectorServiceHeartbeatStore,
        SQLitePublicTradeCollectionCheckpointStore,
        SQLiteRateBudgetCoordinator,
        SQLiteReconciliationHistoryStore,
    )

    def bomb(*args: object, **kwargs: object) -> Never:
        del args, kwargs
        raise AssertionError("preflight invoked a schema-installing adapter")

    for adapter_class in adapter_classes:
        for method_name in ("_connect", "_initialize_schema"):
            if hasattr(adapter_class, method_name):
                monkeypatch.setattr(adapter_class, method_name, bomb)

    result = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert result.status is SQLitePreflightStatus.MATCHED


def test_source_or_directory_changes_during_inspection_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "source-change.sqlite3"
    _create_market(source_path)
    real_fingerprint = preflight_adapter._fingerprint_connection

    def fingerprint_then_change_source(
        connection: _ImmutableSQLiteConnection,
    ) -> SQLiteStoreFingerprint:
        fingerprint = real_fingerprint(connection)
        with source_path.open("ab") as source:
            source.write(b"changed")
        return fingerprint

    monkeypatch.setattr(
        preflight_adapter,
        "_fingerprint_connection",
        fingerprint_then_change_source,
    )
    with pytest.raises(SQLitePreflightError) as source_changed:
        fingerprint_synthetic_sqlite_fixture(_request(source_path, SQLiteStoreFamily.MARKET))
    assert source_changed.value.code is SQLitePreflightErrorCode.SOURCE_CHANGED

    directory_path = tmp_path / "directory-change.sqlite3"
    _create_market(directory_path)
    added_entry = tmp_path / "new-entry"

    def fingerprint_then_change_directory(
        connection: _ImmutableSQLiteConnection,
    ) -> SQLiteStoreFingerprint:
        fingerprint = real_fingerprint(connection)
        added_entry.write_bytes(b"new")
        return fingerprint

    monkeypatch.setattr(
        preflight_adapter,
        "_fingerprint_connection",
        fingerprint_then_change_directory,
    )
    with pytest.raises(SQLitePreflightError) as directory_changed:
        fingerprint_synthetic_sqlite_fixture(_request(directory_path, SQLiteStoreFamily.MARKET))
    assert directory_changed.value.code is SQLitePreflightErrorCode.DIRECTORY_CHANGED


def test_open_connection_bytes_are_bound_to_the_captured_path_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "market.sqlite3"
    substitute_path = tmp_path / "order-flow.sqlite3"
    displaced_original = tmp_path / "displaced-original.sqlite3"
    _create_market(fixture_path)
    _create_order_flow(substitute_path)
    original_hash = _sha256(fixture_path)
    original_stat = fixture_path.stat()
    real_open = preflight_adapter._open_immutable_connection

    def substitute_during_open(path: Path) -> _ImmutableSQLiteConnection:
        os.replace(path, displaced_original)
        shutil.copy2(substitute_path, path)
        connection = real_open(path)
        try:
            os.replace(displaced_original, path)
        except OSError:
            connection.close()
            if path.exists():
                path.unlink()
            os.replace(displaced_original, path)
            pytest.skip("this platform does not permit the adversarial open-file replacement")
        return connection

    monkeypatch.setattr(
        preflight_adapter,
        "_open_immutable_connection",
        substitute_during_open,
    )

    with pytest.raises(SQLitePreflightError) as captured:
        fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))

    assert captured.value.code is SQLitePreflightErrorCode.SOURCE_CHANGED
    assert _sha256(fixture_path) == original_hash
    assert fixture_path.stat().st_size == original_stat.st_size
    assert fixture_path.stat().st_mtime_ns == original_stat.st_mtime_ns
