"""Integration tests for durable reconciliation history and indexed metrics."""

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_reconciliation import (
    SQLiteReconciliationHistoryStore,
    SQLiteReconciliationStorageError,
    SQLiteReconciliationStorageErrorCode,
)
from wealth.application.reconciliation import CandleCrossSourceReconciler
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import CandleStream
from wealth.domain.reconciliation import (
    CandleReconciliationIssueCode,
    CandleReconciliationPolicy,
)
from wealth.domain.reconciliation_history import (
    ReconciliationObservation,
    ReconciliationObservationQuery,
    ReconciliationSummaryQuery,
)
from wealth.ports.reconciliation import (
    ReconciliationWriteConflictCode,
    ReconciliationWriteStatus,
)

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
HISTORY_START = WINDOW_END


def stream(source: str, venue: str) -> CandleStream:
    """Build one comparable BTC-USD stream."""

    return CandleStream(
        source=source,
        venue=venue,
        instrument="BTC-USD",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def candle(
    candle_stream: CandleStream,
    *,
    record_id: int,
    close: str = "100",
) -> CanonicalCandle:
    """Build one canonical reconciliation input."""

    return CanonicalCandle(
        record_id=UUID(int=record_id),
        source=candle_stream.source,
        venue=candle_stream.venue,
        instrument=candle_stream.instrument,
        instrument_type=candle_stream.instrument_type,
        timeframe=candle_stream.timeframe,
        open_time=WINDOW_START,
        close_time=WINDOW_END,
        observed_at=WINDOW_END + timedelta(seconds=1),
        processed_at=WINDOW_END + timedelta(seconds=2),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("10"),
        lineage=(f"fixture:{candle_stream.source}:{record_id}",),
    )


def observation(
    *,
    observation_id: int,
    recorded_hour: int,
    outcome: str,
    reference_source: str = "reference.public-rest",
) -> ReconciliationObservation:
    """Build passing, divergent, or source-blocked durable evidence."""

    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream(reference_source, "REFERENCE")
    primary_records = () if outcome == "blocked" else (candle(primary, record_id=10),)
    reference_close = "102" if outcome == "divergent" else "100"
    report = CandleCrossSourceReconciler().reconcile(
        comparison_key="btc-usd-primary-reference",
        primary_stream=primary,
        reference_stream=reference,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        primary_records=primary_records,
        reference_records=(candle(reference, record_id=20, close=reference_close),),
        policy=CandleReconciliationPolicy(max_price_difference_bps=Decimal("50")),
    )
    report_bytes = report.model_dump_json().encode("utf-8")
    return ReconciliationObservation(
        observation_id=UUID(int=observation_id),
        recorded_at=HISTORY_START + timedelta(hours=recorded_hour),
        report_sha256=sha256(report_bytes).hexdigest(),
        report=report,
        lineage=(f"reconciliation-run:{observation_id}",),
    )


def test_observations_survive_restart_and_summarize_source_quality(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reconciliation.sqlite3"
    store = SQLiteReconciliationHistoryStore(path)
    passed = observation(observation_id=1, recorded_hour=1, outcome="pass")
    divergent = observation(observation_id=2, recorded_hour=2, outcome="divergent")
    blocked = observation(observation_id=3, recorded_hour=3, outcome="blocked")

    assert store.append(passed).status is ReconciliationWriteStatus.INSERTED
    assert store.append(passed).status is ReconciliationWriteStatus.DUPLICATE
    assert store.append(divergent).status is ReconciliationWriteStatus.INSERTED
    assert store.append(blocked).status is ReconciliationWriteStatus.INSERTED

    restarted = SQLiteReconciliationHistoryStore(path)
    assert restarted.get(passed.observation_id) == passed
    query = ReconciliationObservationQuery(
        comparison_key=passed.report.comparison_key,
        recorded_start=HISTORY_START,
        recorded_end_exclusive=HISTORY_START + timedelta(days=1),
        limit=2,
    )
    assert restarted.observations(query) == (passed, divergent)

    summary = restarted.summarize(
        ReconciliationSummaryQuery(
            comparison_key=passed.report.comparison_key,
            recorded_start=HISTORY_START,
            recorded_end_exclusive=HISTORY_START + timedelta(days=1),
        )
    )
    assert summary is not None
    assert summary.observation_count == 3
    assert summary.pass_count == 1
    assert summary.divergent_count == 1
    assert summary.blocked_count == 1
    assert summary.primary_quality_failure_count == 1
    assert summary.reference_quality_failure_count == 0
    assert summary.compared_interval_count == 2
    assert [(item.code, item.count) for item in summary.issue_counts] == [
        (CandleReconciliationIssueCode.CLOSE_PRICE_DIVERGENCE, 1),
        (CandleReconciliationIssueCode.PRIMARY_MISSING, 1),
    ]


def test_identity_and_comparison_key_reuse_fail_without_overwrite(tmp_path: Path) -> None:
    store = SQLiteReconciliationHistoryStore(tmp_path / "reconciliation.sqlite3")
    original = observation(observation_id=1, recorded_hour=1, outcome="pass")
    reused_id = observation(observation_id=1, recorded_hour=2, outcome="pass")
    reused_key = observation(
        observation_id=2,
        recorded_hour=2,
        outcome="pass",
        reference_source="other-reference.public-rest",
    )

    store.append(original)
    id_conflict = store.append(reused_id)
    key_conflict = store.append(reused_key)

    assert id_conflict.status is ReconciliationWriteStatus.CONFLICT
    assert id_conflict.conflict_code is ReconciliationWriteConflictCode.OBSERVATION_ID_REUSE
    assert key_conflict.status is ReconciliationWriteStatus.CONFLICT
    assert key_conflict.conflict_code is ReconciliationWriteConflictCode.COMPARISON_KEY_REUSE
    assert store.get(original.observation_id) == original
    assert store.get(reused_key.observation_id) is None


def test_unknown_series_returns_none_and_empty_known_window_returns_zero_summary(
    tmp_path: Path,
) -> None:
    store = SQLiteReconciliationHistoryStore(tmp_path / "reconciliation.sqlite3")
    known = observation(observation_id=1, recorded_hour=1, outcome="pass")
    store.append(known)
    empty_window = ReconciliationSummaryQuery(
        comparison_key=known.report.comparison_key,
        recorded_start=HISTORY_START + timedelta(days=2),
        recorded_end_exclusive=HISTORY_START + timedelta(days=3),
    )
    unknown = ReconciliationSummaryQuery(
        comparison_key="unknown-comparison",
        recorded_start=HISTORY_START,
        recorded_end_exclusive=HISTORY_START + timedelta(days=1),
    )

    summary = store.summarize(empty_window)

    assert summary is not None
    assert summary.observation_count == 0
    assert summary.issue_counts == ()
    assert store.summarize(unknown) is None


def test_tampered_index_or_issue_metrics_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "reconciliation.sqlite3"
    store = SQLiteReconciliationHistoryStore(path)
    divergent = observation(observation_id=2, recorded_hour=2, outcome="divergent")
    store.append(divergent)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE reconciliation_observations
            SET status = 'blocked'
            WHERE observation_id = ?
            """,
            (str(divergent.observation_id),),
        )

    with pytest.raises(SQLiteReconciliationStorageError) as index_error:
        store.get(divergent.observation_id)

    assert index_error.value.code is SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD

    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE reconciliation_observations
            SET status = 'divergent'
            WHERE observation_id = ?
            """,
            (str(divergent.observation_id),),
        )
        connection.execute(
            """
            UPDATE reconciliation_issue_counts
            SET issue_count = 2
            WHERE observation_id = ?
            """,
            (str(divergent.observation_id),),
        )

    with pytest.raises(SQLiteReconciliationStorageError) as issue_error:
        store.get(divergent.observation_id)

    assert issue_error.value.code is SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD


def test_invalid_path_and_unknown_schema_are_rejected(tmp_path: Path) -> None:
    with pytest.raises(SQLiteReconciliationStorageError) as path_error:
        SQLiteReconciliationHistoryStore(tmp_path)
    assert path_error.value.code is SQLiteReconciliationStorageErrorCode.INVALID_PATH

    database_path = tmp_path / "future.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(SQLiteReconciliationStorageError) as schema_error:
        SQLiteReconciliationHistoryStore(database_path)
    assert schema_error.value.code is SQLiteReconciliationStorageErrorCode.UNSUPPORTED_SCHEMA
