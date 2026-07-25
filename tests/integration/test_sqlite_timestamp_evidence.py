"""Synthetic-only integration coverage for bounded SQLite timestamp-byte evidence."""

import hashlib
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
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
    SQLITE_TIMESTAMP_EXTRACTION_PLANS,
    SQLitePreflightError,
    SQLitePreflightErrorCode,
    _ImmutableSQLiteConnection,
    extract_synthetic_sqlite_timestamp_evidence,
    fingerprint_synthetic_sqlite_fixture,
)
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.adapters.sqlite_reconciliation import SQLiteReconciliationHistoryStore
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_VALUE_BYTES,
    SQLitePreflightRequest,
    SQLitePreflightStatus,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampExtractionPlan,
    SQLiteTimestampExtractionResult,
    SQLiteTimestampExtractionTarget,
)

FixtureFactory = Callable[[Path], None]


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

EXPECTED_TIMESTAMP_COUNTS: dict[SQLiteStoreFamily, int] = {
    SQLiteStoreFamily.MARKET: 5,
    SQLiteStoreFamily.ORDER_FLOW: 5,
    SQLiteStoreFamily.HISTORICAL_COLLECTION: 3,
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: 4,
    SQLiteStoreFamily.COLLECTOR_SERVICE: 2,
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: 14,
    SQLiteStoreFamily.RATE_BUDGET: 3,
    SQLiteStoreFamily.RECONCILIATION: 1,
}


def _request(path: Path, family: SQLiteStoreFamily) -> SQLitePreflightRequest:
    return SQLitePreflightRequest(
        source_kind="generated_synthetic_fixture",
        fixture_id=UUID(int=100 + tuple(SQLiteStoreFamily).index(family)),
        fixture_path=path,
        expected_family=family,
        expected_layout_version=1,
    )


def _plan(family: SQLiteStoreFamily) -> SQLiteTimestampExtractionPlan:
    return next(plan for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS if plan.family is family)


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _generic_value(
    table_name: str,
    column_name: str,
    declared_type: str,
    row_number: int,
) -> object:
    normalized_type = declared_type.upper()
    if "INT" in normalized_type:
        return row_number + 2
    if any(token in normalized_type for token in ("REAL", "FLOA", "DOUB")):
        return float(row_number + 1)
    if "BLOB" in normalized_type:
        return f"blob-{table_name}-{column_name}-{row_number}".encode()
    return f"value-{table_name}-{column_name}-{row_number}"


def _insert_target_row(
    connection: sqlite3.Connection,
    target: SQLiteTimestampExtractionTarget,
    row_number: int,
    *,
    timestamp_overrides: dict[str, object] | None = None,
) -> None:
    columns = tuple(
        row
        for row in connection.execute(f"PRAGMA table_xinfo({_quote(target.table_name)})")
        if int(row[6]) == 0
    )
    target_timestamps = set(target.timestamp_columns)
    overrides = {} if timestamp_overrides is None else timestamp_overrides
    names: list[str] = []
    values: list[object] = []
    for column in columns:
        name = str(column[1])
        declared_type = str(column[2])
        names.append(name)
        if name in overrides:
            values.append(overrides[name])
        elif name in target_timestamps:
            values.append(
                1_700_000_000_000_000 + row_number
                if "INT" in declared_type.upper()
                else f"2026-01-{row_number + 1:02d}T00:00:00.000000Z"
            )
        else:
            values.append(
                _generic_value(
                    target.table_name,
                    name,
                    declared_type,
                    row_number,
                )
            )
    placeholders = ", ".join("?" for _ in names)
    connection.execute(
        (
            f"INSERT INTO {_quote(target.table_name)} "
            f"({', '.join(_quote(name) for name in names)}) "
            f"VALUES ({placeholders})"
        ),
        tuple(values),
    )


def _seed_all_targets(path: Path, family: SQLiteStoreFamily) -> None:
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        for target in _plan(family).targets:
            _insert_target_row(connection, target, 0)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _target_selects(statements: list[str]) -> list[str]:
    table_names = {
        target.table_name for plan in SQLITE_TIMESTAMP_EXTRACTION_PLANS for target in plan.targets
    }
    return [
        statement
        for statement in statements
        if any(f'FROM main."{table_name}"' in statement for table_name in table_names)
    ]


@pytest.mark.parametrize(
    ("family", "factory"),
    FIXTURE_FACTORIES,
    ids=lambda value: value.value if isinstance(value, SQLiteStoreFamily) else None,
)
def test_all_37_targets_extract_deterministically_from_unchanged_generated_fixtures(
    tmp_path: Path,
    family: SQLiteStoreFamily,
    factory: FixtureFactory,
) -> None:
    fixture_path = tmp_path / f"{family.value} timestamp evidence.sqlite3"
    factory(fixture_path)
    _seed_all_targets(fixture_path, family)
    request = _request(fixture_path, family)
    hash_before = _sha256(fixture_path)
    stat_before = fixture_path.stat()
    entries_before = tuple(sorted(entry.name for entry in tmp_path.iterdir()))

    first = extract_synthetic_sqlite_timestamp_evidence(request)
    second = extract_synthetic_sqlite_timestamp_evidence(request)

    assert first == second
    assert first.preflight.status is SQLitePreflightStatus.MATCHED
    assert first.preflight.matched_families == (family,)
    assert first.plan == _plan(family)
    assert tuple(table.target for table in first.tables) == first.plan.targets
    assert all(len(table.rows) == 1 for table in first.tables)
    assert (
        sum(len(row.timestamp_cells) for table in first.tables for row in table.rows)
        == EXPECTED_TIMESTAMP_COUNTS[family]
    )
    assert all(
        tuple(cell.column_name for cell in row.stable_row_key)
        == table.target.stable_row_key_columns
        for table in first.tables
        for row in table.rows
    )
    assert all(
        tuple(cell.column_name for cell in row.timestamp_cells) == table.target.timestamp_columns
        for table in first.tables
        for row in table.rows
    )
    assert all(
        len(cell.blob_hex) == cell.byte_length * 2 and cell.blob_hex == cell.blob_hex.upper()
        for table in first.tables
        for row in table.rows
        for cell in (*row.stable_row_key, *row.timestamp_cells)
    )
    for table in first.tables:
        for cell in table.rows[0].timestamp_cells:
            if table.target.table_name == "rate_budget_state":
                expected_bytes = b"1700000000000000"
                expected_storage = SQLiteStorageClass.INTEGER
            else:
                expected_bytes = b"2026-01-01T00:00:00.000000Z"
                expected_storage = SQLiteStorageClass.TEXT
            assert cell.storage_class is expected_storage
            assert cell.blob_hex == expected_bytes.hex().upper()
            assert cell.byte_length == len(expected_bytes)
    assert first.snapshot_identity == first.preflight.observation.source_before
    assert first.snapshot_identity == first.preflight.observation.source_after
    assert _sha256(fixture_path) == hash_before
    stat_after = fixture_path.stat()
    assert (
        stat_after.st_size,
        stat_after.st_mtime_ns,
        stat_after.st_dev,
        stat_after.st_ino,
    ) == (
        stat_before.st_size,
        stat_before.st_mtime_ns,
        stat_before.st_dev,
        stat_before.st_ino,
    )
    assert tuple(sorted(entry.name for entry in tmp_path.iterdir())) == entries_before
    assert not any(
        entry.name.endswith(("-journal", "-wal", "-shm", ".json", ".manifest"))
        for entry in tmp_path.iterdir()
    )


def test_extraction_result_contract_rejects_fabricated_linkage_or_table_evidence(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "result-contract.sqlite3"
    _create_market(fixture_path)
    result = extract_synthetic_sqlite_timestamp_evidence(
        _request(fixture_path, SQLiteStoreFamily.MARKET)
    )

    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampExtractionResult.model_validate(
            {**result.model_dump(), "report_path": tmp_path / "report.json"}
        )

    wrong_snapshot = result.model_dump()
    wrong_snapshot["snapshot_identity"] = {
        **wrong_snapshot["snapshot_identity"],
        "sha256": "0" * 64,
    }
    with pytest.raises(ValidationError, match="unchanged snapshot identity"):
        SQLiteTimestampExtractionResult.model_validate(wrong_snapshot)

    missing_table = result.model_dump()
    missing_table["tables"] = missing_table["tables"][:-1]
    with pytest.raises(ValidationError, match="exactly match"):
        SQLiteTimestampExtractionResult.model_validate(missing_table)

    reordered_tables = result.model_dump()
    reordered_tables["tables"] = tuple(reversed(reordered_tables["tables"]))
    with pytest.raises(ValidationError):
        SQLiteTimestampExtractionResult.model_validate(reordered_tables)

    wrong_fixture = result.model_dump()
    wrong_fixture["fixture_id"] = UUID(int=999)
    with pytest.raises(ValidationError, match="exact matched"):
        SQLiteTimestampExtractionResult.model_validate(wrong_fixture)

    with pytest.raises(ValidationError):
        result.source_unchanged = False  # type: ignore[assignment]


def test_hostile_null_integer_real_text_and_blob_cells_remain_exact_raw_evidence(
    tmp_path: Path,
) -> None:
    continuous_path = tmp_path / "continuous.sqlite3"
    _create_continuous_collection(continuous_path)
    continuous_target = _plan(SQLiteStoreFamily.CONTINUOUS_COLLECTION).targets[0]
    with closing(sqlite3.connect(continuous_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        _insert_target_row(
            connection,
            continuous_target,
            0,
            timestamp_overrides={
                "next_window_start": "bad\x00time",
                "active_window_end_exclusive": None,
                "next_retry_at": bytes((0, 255, 128, 65)),
            },
        )
    continuous = extract_synthetic_sqlite_timestamp_evidence(
        _request(continuous_path, SQLiteStoreFamily.CONTINUOUS_COLLECTION)
    )
    continuous_cells = {
        cell.column_name: cell for cell in continuous.tables[0].rows[0].timestamp_cells
    }

    rate_path = tmp_path / "rate.sqlite3"
    _create_rate_budget(rate_path)
    rate_plan = _plan(SQLiteStoreFamily.RATE_BUDGET)
    reservation_target = rate_plan.targets[0]
    rate_target = rate_plan.targets[1]
    with closing(sqlite3.connect(rate_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        _insert_target_row(
            connection,
            reservation_target,
            0,
            timestamp_overrides={"requested_at": "1.5"},
        )
        _insert_target_row(
            connection,
            reservation_target,
            1,
            timestamp_overrides={"requested_at": "replace-me"},
        )
        connection.execute(
            """
            UPDATE rate_budget_reservations
            SET requested_at = CAST(X'80FF' AS TEXT)
            WHERE reservation_id = ?
            """,
            ("value-rate_budget_reservations-reservation_id-1",),
        )
        _insert_target_row(
            connection,
            rate_target,
            0,
            timestamp_overrides={
                "theoretical_arrival_us": -7,
                "last_observed_us": 1.5,
            },
        )
    rate = extract_synthetic_sqlite_timestamp_evidence(
        _request(rate_path, SQLiteStoreFamily.RATE_BUDGET)
    )
    rate_cells = {cell.column_name: cell for cell in rate.tables[1].rows[0].timestamp_cells}
    reservation_cells = tuple(row.timestamp_cells[0] for row in rate.tables[0].rows)

    assert (
        continuous_cells["next_window_start"].storage_class,
        continuous_cells["next_window_start"].blob_hex,
        continuous_cells["next_window_start"].byte_length,
    ) == (SQLiteStorageClass.TEXT, "6261640074696D65", 8)
    assert (
        continuous_cells["active_window_end_exclusive"].storage_class,
        continuous_cells["active_window_end_exclusive"].blob_hex,
        continuous_cells["active_window_end_exclusive"].byte_length,
    ) == (SQLiteStorageClass.NULL, "", 0)
    assert (
        continuous_cells["next_retry_at"].storage_class,
        continuous_cells["next_retry_at"].blob_hex,
        continuous_cells["next_retry_at"].byte_length,
    ) == (SQLiteStorageClass.BLOB, "00FF8041", 4)
    assert (
        rate_cells["theoretical_arrival_us"].storage_class,
        rate_cells["theoretical_arrival_us"].blob_hex,
        rate_cells["theoretical_arrival_us"].byte_length,
    ) == (SQLiteStorageClass.INTEGER, "2D37", 2)
    assert (
        rate_cells["last_observed_us"].storage_class,
        rate_cells["last_observed_us"].blob_hex,
        rate_cells["last_observed_us"].byte_length,
    ) == (SQLiteStorageClass.REAL, "312E35", 3)
    assert {
        (cell.storage_class, cell.blob_hex, cell.byte_length) for cell in reservation_cells
    } == {
        (SQLiteStorageClass.TEXT, "312E35", 3),
        (SQLiteStorageClass.TEXT, "80FF", 2),
    }
    assert rate_cells["last_observed_us"].blob_hex == reservation_cells[0].blob_hex


def test_rows_use_complete_stable_key_order_and_repeat_exactly(tmp_path: Path) -> None:
    fixture_path = tmp_path / "ordered.sqlite3"
    _create_market(fixture_path)
    target = _plan(SQLiteStoreFamily.MARKET).targets[2]
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        for row_number in (2, 0, 1):
            _insert_target_row(connection, target, row_number)

    request = _request(fixture_path, SQLiteStoreFamily.MARKET)
    first = extract_synthetic_sqlite_timestamp_evidence(request)
    second = extract_synthetic_sqlite_timestamp_evidence(request)
    rows = first.tables[2].rows

    assert first == second
    assert tuple(row.row_ordinal for row in rows) == (0, 1, 2)
    assert tuple(row.stable_row_key[0].blob_hex for row in rows) == tuple(
        sorted(row.stable_row_key[0].blob_hex for row in rows)
    )
    assert len({row.stable_row_key for row in rows}) == 3


def test_compound_text_integer_key_uses_every_component_for_order(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "compound-order.sqlite3"
    _create_collector_service(fixture_path)
    target = _plan(SQLiteStoreFamily.COLLECTOR_SERVICE).targets[0]
    desired_keys = {
        0: ("a", 2),
        1: ("b", 0),
        2: ("a", 1),
    }
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        for row_number in (2, 0, 1):
            _insert_target_row(connection, target, row_number)
            run_id, sequence = desired_keys[row_number]
            connection.execute(
                """
                UPDATE collector_service_heartbeats
                SET run_id = ?, sequence = ?
                WHERE heartbeat_id = ?
                """,
                (
                    run_id,
                    sequence,
                    f"value-collector_service_heartbeats-heartbeat_id-{row_number}",
                ),
            )

    result = extract_synthetic_sqlite_timestamp_evidence(
        _request(fixture_path, SQLiteStoreFamily.COLLECTOR_SERVICE)
    )
    assert tuple(
        tuple(cell.blob_hex for cell in row.stable_row_key) for row in result.tables[0].rows
    ) == (
        ("61", "31"),
        ("61", "32"),
        ("62", "30"),
    )


def test_row_limit_is_complete_at_boundary_and_fails_closed_above_it(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "bounded.sqlite3"
    _create_market(fixture_path)
    target = _plan(SQLiteStoreFamily.MARKET).targets[2]
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        for row_number in range(MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET):
            _insert_target_row(connection, target, row_number)

    request = _request(fixture_path, SQLiteStoreFamily.MARKET)
    at_limit = extract_synthetic_sqlite_timestamp_evidence(request)
    assert len(at_limit.tables[2].rows) == MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET

    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        _insert_target_row(connection, target, MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)
    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(request)
    assert captured.value.code is SQLitePreflightErrorCode.RESOURCE_LIMIT


def test_oversized_timestamp_cell_fails_before_hex_evidence_is_returned(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "oversized.sqlite3"
    _create_market(fixture_path)
    target = _plan(SQLiteStoreFamily.MARKET).targets[2]
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        _insert_target_row(
            connection,
            target,
            0,
            timestamp_overrides={
                "observed_at": b"x" * (MAX_SQLITE_TIMESTAMP_VALUE_BYTES + 1),
            },
        )

    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(
            _request(fixture_path, SQLiteStoreFamily.MARKET)
        )
    assert captured.value.code is SQLitePreflightErrorCode.RESOURCE_LIMIT


def test_nullable_sqlite_primary_key_cannot_be_accepted_as_stable_evidence(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "null-keys.sqlite3"
    _create_market(fixture_path)
    target = _plan(SQLiteStoreFamily.MARKET).targets[2]
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        _insert_target_row(connection, target, 0)
        _insert_target_row(connection, target, 1)
        connection.execute("UPDATE raw_market_payloads SET record_id = NULL")

    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(
            _request(fixture_path, SQLiteStoreFamily.MARKET)
        )
    assert captured.value.code is SQLitePreflightErrorCode.INVALID_SCHEMA


@pytest.mark.parametrize("rejection", ["mismatch", "wrong_family", "ambiguous"])
def test_rejected_fingerprints_execute_zero_timestamp_table_selects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rejection: str,
) -> None:
    fixture_path = tmp_path / f"{rejection}.sqlite3"
    _create_market(fixture_path)
    expected_family = SQLiteStoreFamily.MARKET
    if rejection == "mismatch":
        with closing(sqlite3.connect(fixture_path)) as connection, connection:
            connection.execute("CREATE TABLE unexpected_layout(value TEXT)")
    elif rejection == "wrong_family":
        expected_family = SQLiteStoreFamily.ORDER_FLOW
    else:
        market_identity = SQLITE_EXPECTED_STORE_IDENTITIES[0]
        duplicate = market_identity.model_copy(update={"family": SQLiteStoreFamily.ORDER_FLOW})
        monkeypatch.setattr(
            preflight_adapter,
            "SQLITE_EXPECTED_STORE_IDENTITIES",
            (*SQLITE_EXPECTED_STORE_IDENTITIES, duplicate),
        )

    statements: list[str] = []
    real_open = preflight_adapter._open_immutable_connection

    def tracing_open(path: Path) -> _ImmutableSQLiteConnection:
        connection = real_open(path)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(preflight_adapter, "_open_immutable_connection", tracing_open)
    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(_request(fixture_path, expected_family))

    assert captured.value.code is SQLitePreflightErrorCode.FINGERPRINT_NOT_MATCHED
    assert _target_selects(statements) == []


def test_tampered_plan_fails_before_opening_or_reading_the_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "tampered-plan.sqlite3"
    _create_market(fixture_path)
    market_plan = _plan(SQLiteStoreFamily.MARKET)
    tampered_target = market_plan.targets[2].model_copy(
        update={"timestamp_columns": ("lineage_json",)}
    )
    tampered_plan = market_plan.model_copy(
        update={"targets": (*market_plan.targets[:2], tampered_target)}
    )
    monkeypatch.setattr(
        preflight_adapter,
        "SQLITE_TIMESTAMP_EXTRACTION_PLANS",
        (tampered_plan, *SQLITE_TIMESTAMP_EXTRACTION_PLANS[1:]),
    )
    opened = False
    real_open = preflight_adapter._open_immutable_connection

    def recording_open(path: Path) -> _ImmutableSQLiteConnection:
        nonlocal opened
        opened = True
        return real_open(path)

    monkeypatch.setattr(preflight_adapter, "_open_immutable_connection", recording_open)
    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(
            _request(fixture_path, SQLiteStoreFamily.MARKET)
        )

    assert captured.value.code is SQLitePreflightErrorCode.INVALID_EXTRACTION_PLAN
    assert opened is False


def test_plan_validation_rejects_missing_targets_and_nonunique_keys(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "plan-schema-validation.sqlite3"
    _create_market(fixture_path)
    matched = fingerprint_synthetic_sqlite_fixture(_request(fixture_path, SQLiteStoreFamily.MARKET))
    plan = _plan(SQLiteStoreFamily.MARKET)
    target = plan.targets[2]
    invalid_targets = (
        target.model_copy(update={"table_name": "unknown_table"}),
        target.model_copy(update={"timestamp_columns": ("unknown_time",)}),
        target.model_copy(update={"stable_row_key_columns": ("source",)}),
    )

    for invalid_target in invalid_targets:
        invalid_plan = plan.model_copy(update={"targets": (*plan.targets[:2], invalid_target)})
        with pytest.raises(SQLitePreflightError) as captured:
            preflight_adapter._validate_timestamp_plan(
                invalid_plan,
                matched.observation.fingerprint,
                matched.expected_identity,
            )
        assert captured.value.code is SQLitePreflightErrorCode.INVALID_EXTRACTION_PLAN


def test_matched_extraction_reads_targets_only_after_schema_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "trace.sqlite3"
    _create_market(fixture_path)
    statements: list[str] = []
    real_open = preflight_adapter._open_immutable_connection
    real_fingerprint = preflight_adapter._fingerprint_connection
    fingerprint_complete = False
    open_count = 0

    def record_statement(statement: str) -> None:
        if _target_selects([statement]):
            assert fingerprint_complete
        statements.append(statement)

    def tracing_open(path: Path) -> _ImmutableSQLiteConnection:
        nonlocal open_count
        open_count += 1
        connection = real_open(path)
        connection.set_trace_callback(record_statement)
        return connection

    def tracked_fingerprint(
        connection: _ImmutableSQLiteConnection,
    ) -> object:
        nonlocal fingerprint_complete
        fingerprint = real_fingerprint(connection)
        fingerprint_complete = True
        return fingerprint

    monkeypatch.setattr(preflight_adapter, "_open_immutable_connection", tracing_open)
    monkeypatch.setattr(
        preflight_adapter,
        "_fingerprint_connection",
        tracked_fingerprint,
    )
    result = extract_synthetic_sqlite_timestamp_evidence(
        _request(fixture_path, SQLiteStoreFamily.MARKET)
    )

    target_selects = _target_selects(statements)
    assert result.preflight.status is SQLitePreflightStatus.MATCHED
    assert fingerprint_complete is True
    assert open_count == 1
    assert len(target_selects) == 3
    first_target_position = min(statements.index(statement) for statement in target_selects)
    schema_position = next(
        index
        for index, statement in enumerate(statements)
        if "FROM main.sqlite_schema" in statement
    )
    assert first_target_position > schema_position


def test_private_connection_cannot_bypass_fingerprint_and_plan_gate(
    tmp_path: Path,
) -> None:
    fixture_path = tmp_path / "authorizer-gate.sqlite3"
    _create_market(fixture_path)
    connection = preflight_adapter._open_immutable_connection(fixture_path)
    undeclared_target = SQLiteTimestampExtractionTarget(
        table_name="raw_market_payloads",
        stable_row_key_columns=("record_id",),
        timestamp_columns=("payload",),
    )
    try:
        with pytest.raises(SQLitePreflightError) as unauthorized:
            connection.authorize_timestamp_target(undeclared_target)
        with pytest.raises(SQLitePreflightError) as forged_capability:
            connection.bind_validated_timestamp_plan(
                _plan(SQLiteStoreFamily.MARKET),
                object(),
            )
    finally:
        connection.close()

    assert unauthorized.value.code is SQLitePreflightErrorCode.FINGERPRINT_NOT_MATCHED
    assert forged_capability.value.code is SQLitePreflightErrorCode.INVALID_EXTRACTION_PLAN


def test_source_replacement_after_fingerprint_fails_before_timestamp_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "changed-before-evidence.sqlite3"
    _create_market(fixture_path)
    statements: list[str] = []
    real_open = preflight_adapter._open_immutable_connection
    real_fingerprint = preflight_adapter._fingerprint_connection

    def tracing_open(path: Path) -> _ImmutableSQLiteConnection:
        connection = real_open(path)
        connection.set_trace_callback(statements.append)
        return connection

    def fingerprint_then_change(
        connection: _ImmutableSQLiteConnection,
    ) -> object:
        fingerprint = real_fingerprint(connection)
        with fixture_path.open("ab") as fixture:
            fixture.write(b"changed")
        return fingerprint

    monkeypatch.setattr(preflight_adapter, "_open_immutable_connection", tracing_open)
    monkeypatch.setattr(
        preflight_adapter,
        "_fingerprint_connection",
        fingerprint_then_change,
    )
    with pytest.raises(SQLitePreflightError) as captured:
        extract_synthetic_sqlite_timestamp_evidence(
            _request(fixture_path, SQLiteStoreFamily.MARKET)
        )

    assert captured.value.code is SQLitePreflightErrorCode.SOURCE_CHANGED
    assert _target_selects(statements) == []
