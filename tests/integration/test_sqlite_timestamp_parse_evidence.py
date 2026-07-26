"""Synthetic-only integration coverage for pure TASK-032/033/034 timestamp evidence."""

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
    SQLITE_TIMESTAMP_EXTRACTION_PLANS,
    extract_synthetic_sqlite_timestamp_evidence,
)
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.adapters.sqlite_reconciliation import SQLiteReconciliationHistoryStore
from wealth.domain import sqlite_timestamp_candidate as timestamp_candidate
from wealth.domain import sqlite_timestamp_candidate_census as timestamp_census
from wealth.domain import sqlite_timestamp_parse as timestamp_parse
from wealth.domain.sqlite_preflight import (
    SQLitePreflightRequest,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampExtractionPlan,
    SQLiteTimestampExtractionResult,
    SQLiteTimestampExtractionTarget,
)
from wealth.domain.sqlite_timestamp_candidate import (
    SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS,
    SQLiteTimestampCanonicalCandidateError,
    SQLiteTimestampCanonicalCandidateErrorCode,
    SQLiteTimestampCanonicalCandidateResult,
    SQLiteTimestampCanonicalCandidateStatus,
    derive_synthetic_sqlite_timestamp_canonical_candidate_evidence,
)
from wealth.domain.sqlite_timestamp_candidate_census import (
    SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS,
    SQLiteTimestampCandidateCensusError,
    SQLiteTimestampCandidateCensusErrorCode,
    SQLiteTimestampCandidateCensusResult,
    SQLiteTimestampCandidateColumnCensus,
    build_synthetic_sqlite_timestamp_candidate_census_evidence,
)
from wealth.domain.sqlite_timestamp_parse import (
    SQLITE_TIMESTAMP_PARSE_PLANS,
    SQLiteTimestampParseError,
    SQLiteTimestampParseErrorCode,
    SQLiteTimestampParseResult,
    SQLiteTimestampParseStatus,
    SQLiteTimestampRepresentation,
    parse_synthetic_sqlite_timestamp_evidence,
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


def _request(path: Path, family: SQLiteStoreFamily) -> SQLitePreflightRequest:
    return SQLitePreflightRequest(
        source_kind="generated_synthetic_fixture",
        fixture_id=UUID(int=500 + tuple(SQLiteStoreFamily).index(family)),
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
) -> None:
    columns = tuple(
        row
        for row in connection.execute(f"PRAGMA table_xinfo({_quote(target.table_name)})")
        if int(row[6]) == 0
    )
    target_timestamps = set(target.timestamp_columns)
    names: list[str] = []
    values: list[object] = []
    for column in columns:
        name = str(column[1])
        declared_type = str(column[2])
        names.append(name)
        if name in target_timestamps:
            values.append(
                1_700_000_000_000_000 + row_number
                if "INT" in declared_type.upper()
                else f"2026-01-{row_number + 1:02d}T00:00:00.000001+00:00"
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


def _source(
    tmp_path: Path,
    family: SQLiteStoreFamily,
    factory: FixtureFactory,
    *,
    rows: int = 1,
    updates: tuple[str, ...] = (),
) -> SQLiteTimestampExtractionResult:
    fixture_path = tmp_path / f"{family.value} parse evidence.sqlite3"
    factory(fixture_path)
    with closing(sqlite3.connect(fixture_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        for target in _plan(family).targets:
            for row_number in range(rows):
                _insert_target_row(connection, target, row_number)
        for statement in updates:
            connection.execute(statement)
    return extract_synthetic_sqlite_timestamp_evidence(_request(fixture_path, family))


def _column_censuses_by_identity(
    result: SQLiteTimestampCandidateCensusResult,
) -> dict[tuple[str, str], SQLiteTimestampCandidateColumnCensus]:
    return {
        (
            summary.declaration.table_name,
            summary.declaration.source_column_plan.column_name,
        ): summary
        for summary in result.columns
    }


@pytest.mark.parametrize(
    ("family", "factory"),
    FIXTURE_FACTORIES,
    ids=lambda value: value.value if isinstance(value, SQLiteStoreFamily) else None,
)
def test_all_37_declared_cells_parse_and_project_without_replacing_source_evidence(
    tmp_path: Path,
    family: SQLiteStoreFamily,
    factory: FixtureFactory,
) -> None:
    source = _source(tmp_path, family, factory)

    first = parse_synthetic_sqlite_timestamp_evidence(source)
    second = parse_synthetic_sqlite_timestamp_evidence(source)
    first_candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(first)
    second_candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(second)
    first_census = build_synthetic_sqlite_timestamp_candidate_census_evidence(first_candidates)
    second_census = build_synthetic_sqlite_timestamp_candidate_census_evidence(second_candidates)

    assert first == second
    assert first.source == source
    assert first.source.snapshot_identity == source.snapshot_identity
    assert (
        first.source.preflight.observation.fingerprint == source.preflight.observation.fingerprint
    )
    assert tuple(table.table_name for table in first.tables) == tuple(
        table.target.table_name for table in source.tables
    )
    assert tuple(table.target_ordinal for table in first.tables) == tuple(
        table.target_ordinal for table in source.tables
    )
    outcomes = tuple(
        outcome for table in first.tables for row in table.rows for outcome in row.outcomes
    )
    source_cells = tuple(
        cell for table in source.tables for row in table.rows for cell in row.timestamp_cells
    )
    assert tuple(outcome.source_cell for outcome in outcomes) == source_cells
    assert len(outcomes) == sum(len(target.timestamp_columns) for target in source.plan.targets)
    assert sum(
        outcome.status is SQLiteTimestampParseStatus.PARSED_AWARE_TEXT for outcome in outcomes
    ) == len(outcomes) - (2 if family is SQLiteStoreFamily.RATE_BUDGET else 0)
    assert sum(
        outcome.status is SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS
        for outcome in outcomes
    ) == (2 if family is SQLiteStoreFamily.RATE_BUDGET else 0)
    assert all(
        outcome.source_cell.storage_class in (SQLiteStorageClass.TEXT, SQLiteStorageClass.INTEGER)
        for outcome in outcomes
    )
    assert first_candidates == second_candidates
    assert first_candidates.source == first
    candidates = tuple(
        candidate
        for table in first_candidates.tables
        for row in table.rows
        for candidate in row.candidates
    )
    assert tuple(candidate.source_outcome for candidate in candidates) == outcomes
    assert len(candidates) == len(outcomes)
    assert sum(
        candidate.status is SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
        for candidate in candidates
    ) == len(candidates) - (2 if family is SQLiteStoreFamily.RATE_BUDGET else 0)
    assert sum(
        candidate.status is SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
        for candidate in candidates
    ) == (2 if family is SQLiteStoreFamily.RATE_BUDGET else 0)
    assert first_census == second_census
    assert first_census.source == first_candidates
    assert sum(len(plan.columns) for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS) == 37
    expected_column_identities = tuple(
        (target.table_name, column.column_name)
        for target in first.plan.targets
        for column in target.columns
    )
    assert (
        tuple(
            (
                summary.declaration.table_name,
                summary.declaration.source_column_plan.column_name,
            )
            for summary in first_census.columns
        )
        == expected_column_identities
    )
    assert tuple(summary.declaration.summary_ordinal for summary in first_census.columns) == tuple(
        range(len(expected_column_identities))
    )
    for summary in first_census.columns:
        assert summary.total_candidate_count == 1
        assert sum(item.count for item in summary.candidate_status_counts) == 1
        assert sum(item.count for item in summary.source_parse_status_counts) == 1
        candidate_status_counts = {
            item.status: item.count for item in summary.candidate_status_counts
        }
        assert (
            candidate_status_counts[SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT]
            + candidate_status_counts[
                SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
            ]
            == 1
        )
        assert (
            candidate_status_counts[SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE]
            == 0
        )
        assert (
            candidate_status_counts[
                SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW
            ]
            == 0
        )
        assert summary.projectable_epoch_min == summary.projectable_epoch_max
        assert summary.projectable_epoch_min is not None
        if (
            summary.declaration.source_column_plan.representation
            is SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
        ):
            assert tuple(
                (frequency.utc_offset_microseconds, frequency.count)
                for frequency in summary.source_utc_offset_frequencies
            ) == ((0, 1),)
            assert tuple(
                (frequency.fractional_precision, frequency.count)
                for frequency in summary.source_fractional_precision_frequencies
            ) == ((6, 1),)
        else:
            assert summary.source_utc_offset_frequencies == ()
            assert summary.source_fractional_precision_frequencies == ()


def test_zero_row_family_still_emits_every_declared_column_census(
    tmp_path: Path,
) -> None:
    extracted = _source(
        tmp_path,
        SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
        _create_public_trade_collection,
        rows=0,
    )
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)

    result = build_synthetic_sqlite_timestamp_candidate_census_evidence(candidates)

    assert result.source == candidates
    assert len(result.columns) == 14
    assert tuple(
        (
            summary.declaration.table_name,
            summary.declaration.source_column_plan.column_name,
        )
        for summary in result.columns
    ) == tuple(
        (target.table_name, column.column_name)
        for target in parsed.plan.targets
        for column in target.columns
    )
    for summary in result.columns:
        assert summary.total_candidate_count == 0
        assert all(item.count == 0 for item in summary.candidate_status_counts)
        assert all(item.count == 0 for item in summary.source_parse_status_counts)
        assert summary.source_utc_offset_frequencies == ()
        assert summary.source_fractional_precision_frequencies == ()
        assert summary.projectable_epoch_min is None
        assert summary.projectable_epoch_max is None


def test_mixed_column_census_reconciles_statuses_frequencies_extrema_and_duplicates(
    tmp_path: Path,
) -> None:
    extracted = _source(
        tmp_path,
        SQLiteStoreFamily.MARKET,
        _create_market,
        rows=8,
        updates=(
            (
                "UPDATE candle_conflicts SET open_time=CASE rowid "
                "WHEN 1 THEN '1969-12-31T23:59:59.999999+00:00' "
                "WHEN 2 THEN '1970-01-01T00:00:00.000001+00:00' "
                "WHEN 3 THEN '2026-07-25T09:00:15.123456+00:00' "
                "WHEN 4 THEN '2026-07-25T14:30:15.123456+05:30' "
                "WHEN 5 THEN '1970-01-01T00:00:00-00:00:00.000001' "
                "WHEN 6 THEN '2026-01-01T00:00:00' "
                "WHEN 7 THEN '2026-01-01T00:00:00.1+00:00' "
                "WHEN 8 THEN '0001-01-01T00:00:00+00:00:00.000001' END"
            ),
        ),
    )
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)

    result = build_synthetic_sqlite_timestamp_candidate_census_evidence(candidates)
    summary = _column_censuses_by_identity(result)[("candle_conflicts", "open_time")]
    candidate_counts = {item.status: item.count for item in summary.candidate_status_counts}
    parse_counts = {item.status: item.count for item in summary.source_parse_status_counts}

    assert summary.total_candidate_count == 8
    assert candidate_counts == {
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT: 5,
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS: 0,
        SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE: 2,
        SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW: 1,
    }
    assert parse_counts[SQLiteTimestampParseStatus.PARSED_AWARE_TEXT] == 6
    assert parse_counts[SQLiteTimestampParseStatus.NAIVE_TEXT] == 1
    assert parse_counts[SQLiteTimestampParseStatus.MALFORMED_TEXT] == 1
    assert (
        sum(
            count
            for status, count in parse_counts.items()
            if status
            not in {
                SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                SQLiteTimestampParseStatus.NAIVE_TEXT,
                SQLiteTimestampParseStatus.MALFORMED_TEXT,
            }
        )
        == 0
    )
    assert tuple(
        (frequency.utc_offset_microseconds, frequency.count)
        for frequency in summary.source_utc_offset_frequencies
    ) == (
        (-1, 1),
        (0, 3),
        (1, 1),
        (19_800_000_000, 1),
    )
    assert tuple(
        (frequency.fractional_precision, frequency.count)
        for frequency in summary.source_fractional_precision_frequencies
    ) == ((0, 3), (6, 4))
    assert summary.projectable_epoch_min == -1
    assert summary.projectable_epoch_max == 1_784_970_015_123_456

    candidate_table = next(
        table for table in candidates.tables if table.table_name == "candle_conflicts"
    )
    duplicate_epochs = tuple(
        candidate_table.rows[row_ordinal].candidates[0].epoch_microseconds for row_ordinal in (2, 3)
    )
    assert duplicate_epochs == (
        1_784_970_015_123_456,
        1_784_970_015_123_456,
    )
    assert result.source == candidates


def test_multiple_rows_keys_cells_and_ordinals_preserve_exact_source_order(
    tmp_path: Path,
) -> None:
    source = _source(
        tmp_path,
        SQLiteStoreFamily.MARKET,
        _create_market,
        rows=2,
    )
    result = parse_synthetic_sqlite_timestamp_evidence(source)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(result)

    for parse_table, source_table in zip(result.tables, source.tables, strict=True):
        assert tuple(row.row_ordinal for row in parse_table.rows) == tuple(
            row.row_ordinal for row in source_table.rows
        )
        assert tuple(row.stable_row_key for row in parse_table.rows) == tuple(
            row.stable_row_key for row in source_table.rows
        )
        for parse_row, source_row in zip(parse_table.rows, source_table.rows, strict=True):
            assert tuple(outcome.source_cell for outcome in parse_row.outcomes) == (
                source_row.timestamp_cells
            )
    for candidate_table, parse_table in zip(
        candidates.tables,
        result.tables,
        strict=True,
    ):
        assert tuple(row.row_ordinal for row in candidate_table.rows) == tuple(
            row.row_ordinal for row in parse_table.rows
        )
        assert tuple(row.stable_row_key for row in candidate_table.rows) == tuple(
            row.stable_row_key for row in parse_table.rows
        )
        for candidate_row, parse_row in zip(
            candidate_table.rows,
            parse_table.rows,
            strict=True,
        ):
            assert (
                tuple(candidate.source_outcome for candidate in candidate_row.candidates)
                == parse_row.outcomes
            )


@pytest.mark.parametrize(
    ("family", "factory", "updates", "expected_statuses"),
    (
        (
            SQLiteStoreFamily.CONTINUOUS_COLLECTION,
            _create_continuous_collection,
            (
                (
                    "UPDATE continuous_collection_checkpoints "
                    "SET next_window_start='2026-01-01T00:00:00', "
                    "active_window_end_exclusive=NULL, next_retry_at=X'00FF'"
                ),
                ("UPDATE continuous_collection_transitions SET recorded_at=CAST(X'80FF' AS TEXT)"),
            ),
            {
                (
                    "continuous_collection_checkpoints",
                    "next_window_start",
                ): SQLiteTimestampParseStatus.NAIVE_TEXT,
                (
                    "continuous_collection_checkpoints",
                    "active_window_end_exclusive",
                ): SQLiteTimestampParseStatus.DECLARED_ABSENT,
                (
                    "continuous_collection_checkpoints",
                    "next_retry_at",
                ): SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS,
                (
                    "continuous_collection_transitions",
                    "recorded_at",
                ): SQLiteTimestampParseStatus.MALFORMED_UTF8,
            },
        ),
        (
            SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
            _create_public_trade_collection,
            (
                (
                    "UPDATE public_trade_collection_jobs "
                    "SET window_start='2026-01-01T00:00:00+02:00', "
                    "window_end_exclusive='2026-01-01T00:00:00.1+00:00', "
                    "pending_window_end_exclusive=NULL"
                ),
            ),
            {
                (
                    "public_trade_collection_jobs",
                    "window_start",
                ): SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
                (
                    "public_trade_collection_jobs",
                    "window_end_exclusive",
                ): SQLiteTimestampParseStatus.MALFORMED_TEXT,
                (
                    "public_trade_collection_jobs",
                    "pending_window_end_exclusive",
                ): SQLiteTimestampParseStatus.DECLARED_ABSENT,
            },
        ),
        (
            SQLiteStoreFamily.RATE_BUDGET,
            _create_rate_budget,
            (
                ("UPDATE rate_budget_reservations SET requested_at='2026-01-01T00:00:00-03:30'"),
                (
                    "UPDATE rate_budget_state "
                    "SET theoretical_arrival_us=9223372036854775807, "
                    "last_observed_us=1.5"
                ),
            ),
            {
                (
                    "rate_budget_reservations",
                    "requested_at",
                ): SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                (
                    "rate_budget_state",
                    "theoretical_arrival_us",
                ): SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE,
                (
                    "rate_budget_state",
                    "last_observed_us",
                ): SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS,
            },
        ),
    ),
)
def test_public_parser_classifies_hostile_generated_task_031_evidence(
    tmp_path: Path,
    family: SQLiteStoreFamily,
    factory: FixtureFactory,
    updates: tuple[str, ...],
    expected_statuses: dict[tuple[str, str], SQLiteTimestampParseStatus],
) -> None:
    source = _source(
        tmp_path,
        family,
        factory,
        updates=updates,
    )

    result = parse_synthetic_sqlite_timestamp_evidence(source)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(result)
    census = build_synthetic_sqlite_timestamp_candidate_census_evidence(candidates)
    observed = {
        (table.table_name, outcome.source_cell.column_name): outcome.status
        for table in result.tables
        for row in table.rows
        for outcome in row.outcomes
    }
    candidate_statuses = {
        (table.table_name, candidate.source_outcome.source_cell.column_name): (candidate.status)
        for table in candidates.tables
        for row in table.rows
        for candidate in row.candidates
    }

    assert expected_statuses.items() <= observed.items()
    for identity, parse_status in expected_statuses.items():
        if parse_status is SQLiteTimestampParseStatus.PARSED_AWARE_TEXT:
            expected_candidate_status = SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
        elif parse_status is SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS:
            expected_candidate_status = (
                SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
            )
        else:
            expected_candidate_status = (
                SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE
            )
        assert candidate_statuses[identity] is expected_candidate_status
        summary = _column_censuses_by_identity(census)[identity]
        assert summary.total_candidate_count == 1
        assert {item.status: item.count for item in summary.source_parse_status_counts}[
            parse_status
        ] == 1
        assert {item.status: item.count for item in summary.candidate_status_counts}[
            expected_candidate_status
        ] == 1
        offset_occurrences = sum(
            frequency.count for frequency in summary.source_utc_offset_frequencies
        )
        precision_occurrences = sum(
            frequency.count for frequency in summary.source_fractional_precision_frequencies
        )
        assert offset_occurrences == (
            1
            if parse_status
            in {
                SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
            }
            else 0
        )
        assert precision_occurrences == (
            1
            if parse_status
            in {
                SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                SQLiteTimestampParseStatus.NAIVE_TEXT,
                SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
            }
            else 0
        )
        extrema_expected = expected_candidate_status in {
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS,
        }
        assert (summary.projectable_epoch_min is not None) is extrema_expected
        assert (summary.projectable_epoch_max is not None) is extrema_expected
    for parse_table, source_table in zip(result.tables, source.tables, strict=True):
        for parse_row, source_row in zip(parse_table.rows, source_table.rows, strict=True):
            assert parse_row.row_ordinal == source_row.row_ordinal
            assert parse_row.stable_row_key == source_row.stable_row_key
            assert tuple(outcome.source_cell for outcome in parse_row.outcomes) == (
                source_row.timestamp_cells
            )


def test_public_candidate_path_types_calendar_overflow_and_retains_equal_instants(
    tmp_path: Path,
) -> None:
    source = _source(
        tmp_path,
        SQLiteStoreFamily.MARKET,
        _create_market,
        rows=2,
        updates=(
            (
                "UPDATE candle_conflicts SET open_time="
                "CASE rowid "
                "WHEN 1 THEN '0001-01-01T00:00:00+00:00:00.000001' "
                "ELSE '9999-12-31T23:59:59.999999-00:00:00.000001' END"
            ),
            (
                "UPDATE raw_market_payloads "
                "SET observed_at='2026-07-25T09:00:15.123456+00:00' "
                "WHERE record_id='value-raw_market_payloads-record_id-0'"
            ),
            (
                "UPDATE raw_market_payloads "
                "SET observed_at='2026-07-25T14:30:15.123456+05:30' "
                "WHERE record_id='value-raw_market_payloads-record_id-1'"
            ),
        ),
    )
    parsed = parse_synthetic_sqlite_timestamp_evidence(source)

    result = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)
    indexed = {
        (
            table.table_name,
            row.row_ordinal,
            candidate.source_outcome.source_cell.column_name,
        ): candidate
        for table in result.tables
        for row in table.rows
        for candidate in row.candidates
    }

    underflow = indexed[("candle_conflicts", 0, "open_time")]
    overflow = indexed[("candle_conflicts", 1, "open_time")]
    for candidate in (underflow, overflow):
        assert candidate.status is (
            SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW
        )
        assert candidate.canonical_datetime is None
        assert candidate.canonical_text is None
        assert candidate.epoch_microseconds is None

    first = indexed[("raw_market_payloads", 0, "observed_at")]
    second = indexed[("raw_market_payloads", 1, "observed_at")]
    assert first.source_outcome != second.source_outcome
    assert first.status is (SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT)
    assert second.status is (SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT)
    assert first.canonical_text == second.canonical_text == ("2026-07-25T09:00:15.123456Z")
    assert first.epoch_microseconds == second.epoch_microseconds == 1_784_970_015_123_456
    assert first.canonical_datetime == second.canonical_datetime
    assert result.source == parsed


def test_parser_revalidates_forged_task_031_instances_and_exact_type(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    forged_plan = source.plan.model_copy(update={"expected_store_sha256": "x"})
    forged_source = source.model_copy(update={"plan": forged_plan})
    forged_ordinal = source.tables[0].model_copy(update={"target_ordinal": 99})
    forged_tables = source.model_copy(update={"tables": (forged_ordinal, *source.tables[1:])})

    invalid_sources: tuple[object, ...] = (
        {},
        source.tables[0],
        forged_source,
        forged_tables,
    )
    for invalid in invalid_sources:
        with pytest.raises(SQLiteTimestampParseError):
            parse_synthetic_sqlite_timestamp_evidence(invalid)  # type: ignore[arg-type]

    class SourceSubclass(SQLiteTimestampExtractionResult):
        pass

    subclass = SourceSubclass.model_validate(source.model_dump(mode="python"))
    with pytest.raises(SQLiteTimestampParseError):
        parse_synthetic_sqlite_timestamp_evidence(subclass)


def test_model_construct_bypasses_fail_before_any_cell_is_parsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    valid_table = source.tables[0]
    valid_row = valid_table.rows[0]
    valid_cell = valid_row.timestamp_cells[0]
    forged_cell = valid_cell.model_construct(
        column_name="bad name",
        storage_class=valid_cell.storage_class,
        blob_hex=valid_cell.blob_hex.lower(),
        byte_length=valid_cell.byte_length + 1,
    )
    forged_row = valid_row.model_copy(
        update={"timestamp_cells": (forged_cell, *valid_row.timestamp_cells[1:])}
    )
    forged_table = valid_table.model_copy(update={"rows": (forged_row,)})
    forged_nested = source.model_copy(update={"tables": (forged_table, *source.tables[1:])})
    missing_fields = SQLiteTimestampExtractionResult.model_construct()
    forged_preflight = source.preflight.model_construct(
        **{
            **source.preflight.__dict__,
            "source_unchanged": False,
        }
    )
    forged_link = source.model_copy(update={"preflight": forged_preflight})

    def unexpected_parse(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid TASK-031 evidence reached cell parsing")

    monkeypatch.setattr(timestamp_parse, "_parse_outcome", unexpected_parse)
    for invalid in (missing_fields, forged_nested, forged_link):
        with pytest.raises(SQLiteTimestampParseError) as caught:
            parse_synthetic_sqlite_timestamp_evidence(invalid)
        assert caught.value.code is SQLiteTimestampParseErrorCode.INVALID_SOURCE_EVIDENCE


def test_public_registry_reassignment_cannot_change_reviewed_parse_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    original = next(
        plan
        for plan in SQLITE_TIMESTAMP_PARSE_PLANS
        if plan.extraction_plan.family is SQLiteStoreFamily.MARKET
    )
    first_column = original.targets[0].columns[0]
    altered_column = first_column.model_copy(
        update={
            "representation": "epoch_microseconds",
            "offset_policy": None,
            "nullable": True,
        }
    )
    altered_target = original.targets[0].model_copy(
        update={"columns": (altered_column, *original.targets[0].columns[1:])}
    )
    altered_plan = original.model_copy(update={"targets": (altered_target, *original.targets[1:])})
    monkeypatch.setattr(
        timestamp_parse,
        "SQLITE_TIMESTAMP_PARSE_PLANS",
        (altered_plan,),
    )

    result = parse_synthetic_sqlite_timestamp_evidence(source)

    assert result.plan == original
    assert result.tables[0].rows[0].outcomes[0].representation == (first_column.representation)


def test_result_contract_rejects_reordered_missing_and_altered_evidence(
    tmp_path: Path,
) -> None:
    source = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    result = parse_synthetic_sqlite_timestamp_evidence(source)
    payload = result.model_dump(mode="python")
    first_table = result.tables[0]
    first_row = first_table.rows[0]
    altered_outcome = first_row.outcomes[0].model_copy(
        update={"status": SQLiteTimestampParseStatus.MALFORMED_TEXT}
    )
    altered_row = first_row.model_copy(
        update={"outcomes": (altered_outcome, *first_row.outcomes[1:])}
    )
    altered_table = first_table.model_copy(update={"rows": (altered_row,)})

    invalid_tables = (
        tuple(reversed(result.tables)),
        result.tables[:-1],
        (result.tables[0], result.tables[0], *result.tables[2:]),
        (altered_table, *result.tables[1:]),
    )
    for tables in invalid_tables:
        with pytest.raises(ValidationError):
            SQLiteTimestampParseResult.model_validate({**payload, "tables": tables})


def test_candidate_revalidates_forged_task_032_before_any_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extracted = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    first_table = parsed.tables[0]
    first_row = first_table.rows[0]
    forged_outcome = first_row.outcomes[0].model_copy(
        update={"status": SQLiteTimestampParseStatus.MALFORMED_TEXT}
    )
    forged_row = first_row.model_copy(
        update={"outcomes": (forged_outcome, *first_row.outcomes[1:])}
    )
    forged_table = first_table.model_copy(update={"rows": (forged_row,)})
    forged_nested = parsed.model_copy(update={"tables": (forged_table, *parsed.tables[1:])})
    missing_fields = SQLiteTimestampParseResult.model_construct()
    forged_plan = parsed.plan.model_copy(
        update={
            "targets": tuple(reversed(parsed.plan.targets)),
        }
    )
    forged_plan_source = parsed.model_copy(update={"plan": forged_plan})

    def unexpected_projection(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid TASK-032 evidence reached candidate projection")

    monkeypatch.setattr(
        timestamp_candidate,
        "_candidate_outcome",
        unexpected_projection,
    )
    for invalid in (missing_fields, forged_nested, forged_plan_source):
        with pytest.raises(SQLiteTimestampCanonicalCandidateError) as caught:
            derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(invalid)
        assert caught.value.code is (
            SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE
        )

    class ParseResultSubclass(SQLiteTimestampParseResult):
        pass

    subclass = ParseResultSubclass.model_validate(parsed.model_dump(mode="python"))
    with pytest.raises(SQLiteTimestampCanonicalCandidateError) as caught:
        derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(subclass)
    assert caught.value.code is (SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE)


def test_public_candidate_registry_reassignment_cannot_change_reviewed_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extracted = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    original = next(
        plan
        for plan in SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS
        if plan.source_plan.extraction_plan.family is SQLiteStoreFamily.MARKET
    )
    altered = original.model_copy(
        update={
            "projection_kind": "collision_grouping",
            "projectable_source_statuses": tuple(reversed(original.projectable_source_statuses)),
        }
    )
    monkeypatch.setattr(
        timestamp_candidate,
        "SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS",
        (altered,),
    )

    result = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)

    assert result.plan == original


def test_candidate_result_rejects_reordered_missing_and_altered_evidence(
    tmp_path: Path,
) -> None:
    extracted = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    result = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)
    payload = result.model_dump(mode="python")
    first_table = result.tables[0]
    first_row = first_table.rows[0]
    altered_candidate = first_row.candidates[0].model_copy(
        update={"status": (SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE)}
    )
    altered_row = first_row.model_copy(
        update={"candidates": (altered_candidate, *first_row.candidates[1:])}
    )
    altered_table = first_table.model_copy(update={"rows": (altered_row,)})

    invalid_tables = (
        tuple(reversed(result.tables)),
        result.tables[:-1],
        (result.tables[0], result.tables[0], *result.tables[2:]),
        (altered_table, *result.tables[1:]),
    )
    for tables in invalid_tables:
        with pytest.raises(ValidationError):
            SQLiteTimestampCanonicalCandidateResult.model_validate({**payload, "tables": tables})


def test_census_revalidates_forged_task_033_before_any_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    extracted = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)
    first_table = candidates.tables[0]
    first_row = first_table.rows[0]
    forged_candidate = first_row.candidates[0].model_copy(
        update={
            "status": SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE,
        }
    )
    forged_row = first_row.model_copy(
        update={"candidates": (forged_candidate, *first_row.candidates[1:])}
    )
    forged_table = first_table.model_copy(update={"rows": (forged_row,)})
    forged_nested = candidates.model_copy(update={"tables": (forged_table, *candidates.tables[1:])})
    missing_fields = SQLiteTimestampCanonicalCandidateResult.model_construct()
    forged_plan = candidates.plan.model_copy(update={"projection_kind": "collision_grouping"})
    forged_plan_source = candidates.model_copy(update={"plan": forged_plan})

    def unexpected_summary(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid TASK-033 evidence reached census construction")

    monkeypatch.setattr(timestamp_census, "_summaries", unexpected_summary)
    for invalid in (missing_fields, forged_nested, forged_plan_source):
        with pytest.raises(SQLiteTimestampCandidateCensusError) as caught:
            build_synthetic_sqlite_timestamp_candidate_census_evidence(invalid)
        assert caught.value.code is (
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE
        )

    class CandidateResultSubclass(SQLiteTimestampCanonicalCandidateResult):
        pass

    subclass = CandidateResultSubclass.model_validate(candidates.model_dump(mode="python"))
    with pytest.raises(SQLiteTimestampCandidateCensusError) as caught:
        build_synthetic_sqlite_timestamp_candidate_census_evidence(subclass)
    assert caught.value.code is (SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE)


def test_census_result_rejects_reordered_missing_duplicate_and_altered_summaries(
    tmp_path: Path,
) -> None:
    extracted = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(extracted)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)
    result = build_synthetic_sqlite_timestamp_candidate_census_evidence(candidates)
    payload = result.model_dump(mode="python")
    first_summary = result.columns[0]
    altered_summary = first_summary.model_copy(
        update={"total_candidate_count": first_summary.total_candidate_count + 1}
    )

    invalid_columns = (
        tuple(reversed(result.columns)),
        result.columns[:-1],
        (result.columns[0], result.columns[0], *result.columns[2:]),
        (altered_summary, *result.columns[1:]),
    )
    for columns in invalid_columns:
        with pytest.raises(ValidationError):
            SQLiteTimestampCandidateCensusResult.model_validate({**payload, "columns": columns})


def test_pure_parse_candidate_and_census_pipeline_uses_no_io_after_task_031(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source(tmp_path, SQLiteStoreFamily.MARKET, _create_market)
    parsed = parse_synthetic_sqlite_timestamp_evidence(source)
    source_path = source.preflight.observation.snapshot_path
    source_path.unlink()

    def unexpected_io(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("TASK-032 must not perform I/O")

    monkeypatch.setattr(sqlite3, "connect", unexpected_io)
    monkeypatch.setattr(
        preflight_adapter, "extract_synthetic_sqlite_timestamp_evidence", unexpected_io
    )
    monkeypatch.setattr(Path, "open", unexpected_io)
    monkeypatch.setattr(Path, "read_bytes", unexpected_io)
    monkeypatch.setattr(Path, "stat", unexpected_io)
    monkeypatch.setattr(Path, "iterdir", unexpected_io)
    monkeypatch.setattr(
        timestamp_parse,
        "parse_synthetic_sqlite_timestamp_evidence",
        unexpected_io,
    )

    result = parse_synthetic_sqlite_timestamp_evidence(source)
    candidates = derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(parsed)
    census = build_synthetic_sqlite_timestamp_candidate_census_evidence(candidates)

    assert result.source == source
    assert candidates.source == parsed
    assert census.source == candidates
