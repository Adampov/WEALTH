"""Tests for deterministic cross-source candle reconciliation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from wealth.application.reconciliation import (
    CandleCrossSourceReconciler,
    CandleReconciliationError,
    CandleReconciliationErrorCode,
)
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import CandleStream, DataQualityStatus
from wealth.domain.reconciliation import (
    CandleReconciliationIssueCode,
    CandleReconciliationPolicy,
    CandleReconciliationReport,
    CandleReconciliationStatus,
)

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)


def stream(
    source: str,
    venue: str,
    *,
    instrument: str = "BTC-USD",
    timeframe: CandleTimeframe = CandleTimeframe.ONE_MINUTE,
) -> CandleStream:
    """Build one explicit comparable stream."""

    return CandleStream(
        source=source,
        venue=venue,
        instrument=instrument,
        instrument_type=InstrumentType.SPOT,
        timeframe=timeframe,
    )


def candle(
    candle_stream: CandleStream,
    *,
    record_id: int,
    minute: int,
    open_price: str = "100",
    high: str = "110",
    low: str = "90",
    close: str = "100",
    volume: str = "10",
) -> CanonicalCandle:
    """Build one valid source-specific candle."""

    open_time = WINDOW_START + minute * candle_stream.timeframe.duration
    return CanonicalCandle(
        record_id=UUID(int=record_id),
        source=candle_stream.source,
        venue=candle_stream.venue,
        instrument=candle_stream.instrument,
        instrument_type=candle_stream.instrument_type,
        timeframe=candle_stream.timeframe,
        open_time=open_time,
        close_time=open_time + candle_stream.timeframe.duration,
        observed_at=open_time + candle_stream.timeframe.duration + timedelta(seconds=1),
        processed_at=open_time + candle_stream.timeframe.duration + timedelta(seconds=2),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        base_volume=Decimal(volume),
        lineage=(f"fixture:{candle_stream.source}:{record_id}",),
    )


def reconcile(
    *,
    primary_records: tuple[CanonicalCandle, ...],
    reference_records: tuple[CanonicalCandle, ...],
    price_limit: str = "0",
    volume_limit: str | None = None,
) -> CandleReconciliationReport:
    """Run one two-minute comparison with explicit policy."""

    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")
    return CandleCrossSourceReconciler().reconcile(
        comparison_key="btc-usd-spot-1m-primary-reference",
        primary_stream=primary,
        reference_stream=reference,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        primary_records=primary_records,
        reference_records=reference_records,
        policy=CandleReconciliationPolicy(
            max_price_difference_bps=Decimal(price_limit),
            max_base_volume_difference_bps=(
                Decimal(volume_limit) if volume_limit is not None else None
            ),
        ),
    )


def test_equal_complete_streams_pass_with_zero_symmetric_differences() -> None:
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")

    report = reconcile(
        primary_records=(
            candle(primary, record_id=1, minute=0),
            candle(primary, record_id=2, minute=1),
        ),
        reference_records=(
            candle(reference, record_id=3, minute=0),
            candle(reference, record_id=4, minute=1),
        ),
    )

    assert report.status is CandleReconciliationStatus.PASS
    assert report.compared_count == 2
    assert report.issues == ()
    assert report.primary_quality.status is DataQualityStatus.PASS
    assert report.reference_quality.status is DataQualityStatus.PASS
    assert report.compared_open_times == (
        WINDOW_START,
        WINDOW_START + timedelta(minutes=1),
    )
    assert report.comparisons[0].open_difference_bps == Decimal("0")
    assert report.comparisons[0].base_volume_difference_bps == Decimal("0")


def test_price_and_opt_in_volume_thresholds_create_explicit_findings() -> None:
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")

    report = reconcile(
        primary_records=(
            candle(primary, record_id=1, minute=0),
            candle(primary, record_id=2, minute=1),
        ),
        reference_records=(
            candle(reference, record_id=3, minute=0, close="101", volume="20"),
            candle(reference, record_id=4, minute=1),
        ),
        price_limit="50",
        volume_limit="1000",
    )

    assert report.status is CandleReconciliationStatus.DIVERGENT
    assert [issue.code for issue in report.issues] == [
        CandleReconciliationIssueCode.CLOSE_PRICE_DIVERGENCE,
        CandleReconciliationIssueCode.BASE_VOLUME_DIVERGENCE,
    ]
    assert report.issues[0].difference_bps is not None
    assert report.issues[0].difference_bps > Decimal("50")
    assert report.issues[0].limit_bps == Decimal("50")
    assert report.comparisons[0].base_volume_difference_bps == Decimal("5000")


def test_venue_volume_difference_is_observed_but_not_failed_without_a_limit() -> None:
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")

    report = reconcile(
        primary_records=(
            candle(primary, record_id=1, minute=0, volume="10"),
            candle(primary, record_id=2, minute=1),
        ),
        reference_records=(
            candle(reference, record_id=3, minute=0, volume="20"),
            candle(reference, record_id=4, minute=1),
        ),
    )

    assert report.status is CandleReconciliationStatus.PASS
    assert report.issues == ()
    assert report.comparisons[0].base_volume_difference_bps == Decimal("5000")


def test_missing_interval_blocks_reconciliation_without_inventing_a_candle() -> None:
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")
    reference_second = candle(reference, record_id=4, minute=1)

    report = reconcile(
        primary_records=(candle(primary, record_id=1, minute=0),),
        reference_records=(
            candle(reference, record_id=3, minute=0),
            reference_second,
        ),
    )

    assert report.status is CandleReconciliationStatus.BLOCKED
    assert report.primary_quality.status is DataQualityStatus.FAIL
    assert report.reference_quality.status is DataQualityStatus.PASS
    assert report.compared_count == 1
    assert report.issues[-1].code is CandleReconciliationIssueCode.PRIMARY_MISSING
    assert report.issues[-1].open_time == WINDOW_START + timedelta(minutes=1)
    assert report.issues[-1].primary_record_id is None
    assert report.issues[-1].reference_record_id == reference_second.record_id


def test_conflicting_duplicate_is_not_selected_as_comparison_evidence() -> None:
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")

    report = reconcile(
        primary_records=(
            candle(primary, record_id=1, minute=0),
            candle(primary, record_id=2, minute=0, close="101"),
            candle(primary, record_id=3, minute=1),
        ),
        reference_records=(
            candle(reference, record_id=4, minute=0),
            candle(reference, record_id=5, minute=1),
        ),
    )

    assert report.status is CandleReconciliationStatus.BLOCKED
    assert report.compared_count == 1
    assert report.issues[0].code is CandleReconciliationIssueCode.PRIMARY_MISSING
    assert report.issues[0].open_time == WINDOW_START


def test_same_or_incompatible_streams_fail_before_comparison() -> None:
    reconciler = CandleCrossSourceReconciler()
    primary = stream("primary.public-rest", "PRIMARY")

    with pytest.raises(CandleReconciliationError) as same_error:
        reconciler.reconcile(
            comparison_key="same-stream",
            primary_stream=primary,
            reference_stream=primary,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_END,
            primary_records=(),
            reference_records=(),
            policy=CandleReconciliationPolicy(max_price_difference_bps=Decimal("10")),
        )

    assert same_error.value.code is CandleReconciliationErrorCode.SAME_STREAM

    incompatible = stream(
        "reference.public-rest",
        "REFERENCE",
        instrument="BTC-USDT",
    )
    with pytest.raises(CandleReconciliationError) as incompatible_error:
        reconciler.reconcile(
            comparison_key="different-quotes",
            primary_stream=primary,
            reference_stream=incompatible,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_END,
            primary_records=(),
            reference_records=(),
            policy=CandleReconciliationPolicy(max_price_difference_bps=Decimal("10")),
        )

    assert incompatible_error.value.code is CandleReconciliationErrorCode.INCOMPATIBLE_STREAMS


def test_oversized_window_fails_before_materializing_expected_intervals() -> None:
    reconciler = CandleCrossSourceReconciler()
    primary = stream("primary.public-rest", "PRIMARY")
    reference = stream("reference.public-rest", "REFERENCE")

    with pytest.raises(CandleReconciliationError) as error:
        reconciler.reconcile(
            comparison_key="oversized-window",
            primary_stream=primary,
            reference_stream=reference,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_START + timedelta(minutes=100_001),
            primary_records=(),
            reference_records=(),
            policy=CandleReconciliationPolicy(max_price_difference_bps=Decimal("10")),
        )

    assert error.value.code is CandleReconciliationErrorCode.WINDOW_TOO_LARGE
