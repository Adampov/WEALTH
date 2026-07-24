"""Tests for deterministic candle-sequence quality auditing."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wealth.application.quality import (
    CandleAuditError,
    CandleAuditErrorCode,
    CandleSequenceAuditor,
)
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import (
    CandleQualityCode,
    CandleStream,
    DataQualityStatus,
)

BASE_TIME = datetime(2026, 7, 24, 0, 0, tzinfo=UTC)


def candle(
    minute: int,
    *,
    record_number: int | None = None,
    source: str = "synthetic.quality",
    close: str = "100",
) -> CanonicalCandle:
    """Build one deterministic one-minute candle."""

    open_time = BASE_TIME + timedelta(minutes=minute)
    return CanonicalCandle(
        record_id=UUID(int=record_number if record_number is not None else minute + 1),
        source=source,
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        observed_at=open_time + timedelta(minutes=1, seconds=1),
        processed_at=open_time + timedelta(minutes=1, seconds=2),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        lineage=(f"fixture:{minute}",),
    )


def stream() -> CandleStream:
    """Return the expected synthetic stream."""

    return CandleStream.from_candle(candle(0))


def test_complete_ordered_sequence_passes() -> None:
    report = CandleSequenceAuditor().audit(
        stream=stream(),
        window_start=BASE_TIME,
        window_end_exclusive=BASE_TIME + timedelta(minutes=3),
        records=(candle(0), candle(1), candle(2)),
    )

    assert report.status is DataQualityStatus.PASS
    assert report.input_count == report.accepted_count == 3
    assert report.issues == ()
    assert report.missing_ranges == ()


def test_missing_candles_are_collapsed_into_explicit_ranges() -> None:
    report = CandleSequenceAuditor().audit(
        stream=stream(),
        window_start=BASE_TIME,
        window_end_exclusive=BASE_TIME + timedelta(minutes=6),
        records=(candle(1), candle(4)),
    )

    assert report.status is DataQualityStatus.FAIL
    assert [
        (
            missing.start_open_time,
            missing.end_open_time_exclusive,
            missing.missing_count,
        )
        for missing in report.missing_ranges
    ] == [
        (BASE_TIME, BASE_TIME + timedelta(minutes=1), 1),
        (BASE_TIME + timedelta(minutes=2), BASE_TIME + timedelta(minutes=4), 2),
        (BASE_TIME + timedelta(minutes=5), BASE_TIME + timedelta(minutes=6), 1),
    ]


def test_duplicate_is_usable_but_conflict_fails_closed() -> None:
    original = candle(0, record_number=1)
    duplicate = original.model_copy(update={"record_id": UUID(int=2)})
    first_version = candle(1, record_number=3)
    conflict = first_version.model_copy(update={"record_id": UUID(int=4), "close": Decimal("101")})

    report = CandleSequenceAuditor().audit(
        stream=stream(),
        window_start=BASE_TIME,
        window_end_exclusive=BASE_TIME + timedelta(minutes=2),
        records=(original, duplicate, first_version, conflict),
    )

    assert report.status is DataQualityStatus.FAIL
    assert report.accepted_count == 1
    assert [issue.code for issue in report.issues] == [
        CandleQualityCode.DUPLICATE,
        CandleQualityCode.CONFLICT,
    ]
    assert report.missing_ranges[0].start_open_time == BASE_TIME + timedelta(minutes=1)


def test_mixed_outside_and_out_of_order_records_are_visible_findings() -> None:
    records = (
        candle(1, record_number=1),
        candle(0, record_number=2),
        candle(2, record_number=3, source="other.provider"),
        candle(3, record_number=4),
    )

    report = CandleSequenceAuditor().audit(
        stream=stream(),
        window_start=BASE_TIME,
        window_end_exclusive=BASE_TIME + timedelta(minutes=3),
        records=records,
    )

    assert report.status is DataQualityStatus.FAIL
    assert {issue.code for issue in report.issues} == {
        CandleQualityCode.OUT_OF_ORDER,
        CandleQualityCode.MIXED_STREAM,
        CandleQualityCode.OUT_OF_WINDOW,
    }
    assert report.missing_ranges[0].start_open_time == BASE_TIME + timedelta(minutes=2)


@pytest.mark.parametrize(
    ("start", "end", "code"),
    [
        (
            datetime(2026, 7, 24, 0, 0),
            datetime(2026, 7, 24, 0, 1),
            CandleAuditErrorCode.NAIVE_WINDOW,
        ),
        (BASE_TIME, BASE_TIME, CandleAuditErrorCode.INVALID_WINDOW),
        (
            BASE_TIME,
            BASE_TIME + timedelta(seconds=90),
            CandleAuditErrorCode.MISALIGNED_WINDOW,
        ),
        (
            BASE_TIME + timedelta(seconds=30),
            BASE_TIME + timedelta(seconds=90),
            CandleAuditErrorCode.MISALIGNED_WINDOW,
        ),
    ],
)
def test_invalid_windows_fail_with_reason_codes(
    start: datetime,
    end: datetime,
    code: CandleAuditErrorCode,
) -> None:
    with pytest.raises(CandleAuditError) as error:
        CandleSequenceAuditor().audit(
            stream=stream(),
            window_start=start,
            window_end_exclusive=end,
            records=(),
        )

    assert error.value.code is code


@given(
    window_count=st.integers(min_value=1, max_value=20),
    candidate_indices=st.sets(st.integers(min_value=0, max_value=19)),
)
def test_missing_ranges_exactly_cover_absent_intervals(
    window_count: int,
    candidate_indices: set[int],
) -> None:
    present = sorted(index for index in candidate_indices if index < window_count)
    report = CandleSequenceAuditor().audit(
        stream=stream(),
        window_start=BASE_TIME,
        window_end_exclusive=BASE_TIME + timedelta(minutes=window_count),
        records=(candle(index) for index in present),
    )

    missing_from_report: set[int] = set()
    for missing_range in report.missing_ranges:
        start_index = int((missing_range.start_open_time - BASE_TIME) / timedelta(minutes=1))
        missing_from_report.update(range(start_index, start_index + missing_range.missing_count))

    assert missing_from_report == set(range(window_count)) - set(present)
    assert report.accepted_count == len(present)
