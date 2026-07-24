"""Tests for reconciliation history evidence and aggregate contracts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.application.reconciliation import CandleCrossSourceReconciler
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import CandleStream
from wealth.domain.reconciliation import (
    CandleReconciliationPolicy,
    CandleReconciliationReport,
    CandleReconciliationStatus,
)
from wealth.domain.reconciliation_history import (
    ReconciliationHistorySummary,
    ReconciliationObservation,
    ReconciliationObservationQuery,
)

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)


def stream(source: str, venue: str) -> CandleStream:
    """Build one comparable stream."""

    return CandleStream(
        source=source,
        venue=venue,
        instrument="BTC-USD",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def candle(candle_stream: CandleStream, record_id: int) -> CanonicalCandle:
    """Build one complete canonical candle."""

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
        close=Decimal("100"),
        base_volume=Decimal("10"),
        lineage=(f"fixture:{candle_stream.source}:{record_id}",),
    )


def report() -> CandleReconciliationReport:
    """Build one deterministic passing report."""

    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")
    return CandleCrossSourceReconciler().reconcile(
        comparison_key="btc-usd-primary-reference",
        primary_stream=primary,
        reference_stream=reference,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        primary_records=(candle(primary, 1),),
        reference_records=(candle(reference, 2),),
        policy=CandleReconciliationPolicy(max_price_difference_bps=Decimal("25")),
    )


def observation(
    *,
    digest: str | None = None,
    recorded_at: datetime | None = None,
) -> ReconciliationObservation:
    """Wrap the report in immutable durable evidence."""

    reconciliation = report()
    report_bytes = reconciliation.model_dump_json().encode("utf-8")
    return ReconciliationObservation(
        observation_id=UUID(int=10),
        recorded_at=recorded_at or WINDOW_END + timedelta(hours=1),
        report_sha256=digest or sha256(report_bytes).hexdigest(),
        report=reconciliation,
        lineage=("reconciliation-run:fixture",),
    )


def test_observation_digest_and_json_round_trip_are_stable() -> None:
    evidence = observation()

    reloaded = ReconciliationObservation.model_validate_json(evidence.model_dump_json())

    assert reloaded == evidence
    assert sha256(reloaded.report_bytes).hexdigest() == reloaded.report_sha256
    assert reloaded.report.status is CandleReconciliationStatus.PASS


def test_observation_rejects_digest_mismatch_or_pre_window_time() -> None:
    with pytest.raises(ValidationError, match="report_sha256"):
        observation(digest="0" * 64)

    with pytest.raises(ValidationError, match="before its report window"):
        observation(recorded_at=WINDOW_START)


def test_history_query_rejects_empty_or_unbounded_windows() -> None:
    with pytest.raises(ValidationError, match="must be after"):
        ReconciliationObservationQuery(
            comparison_key="btc-usd-primary-reference",
            recorded_start=WINDOW_END,
            recorded_end_exclusive=WINDOW_END,
        )

    with pytest.raises(ValidationError, match="maximum duration"):
        ReconciliationObservationQuery(
            comparison_key="btc-usd-primary-reference",
            recorded_start=WINDOW_END,
            recorded_end_exclusive=WINDOW_END + timedelta(days=367),
        )


def test_empty_summary_requires_zero_metrics_and_no_timestamps() -> None:
    reconciliation = report()
    summary = ReconciliationHistorySummary(
        comparison_key=reconciliation.comparison_key,
        primary_stream=reconciliation.primary_stream,
        reference_stream=reconciliation.reference_stream,
        recorded_start=WINDOW_END,
        recorded_end_exclusive=WINDOW_END + timedelta(days=1),
        observation_count=0,
        pass_count=0,
        divergent_count=0,
        blocked_count=0,
        primary_quality_failure_count=0,
        reference_quality_failure_count=0,
        compared_interval_count=0,
    )

    assert summary.issue_counts == ()

    invalid = summary.model_copy(update={"pass_count": 1}).model_dump()
    with pytest.raises(ValidationError, match="status counts"):
        ReconciliationHistorySummary.model_validate(invalid)
