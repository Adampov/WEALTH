"""Tests for bounded historical candle planning and retry policy."""

from datetime import UTC, datetime, timedelta
from itertools import islice, pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wealth.application.pagination import (
    HistoricalCandlePagePlanner,
    HistoricalCandlePaginationError,
    HistoricalCandlePaginationErrorCode,
    HistoricalCandlePaginationPolicy,
    HistoricalCandleRetryPolicy,
    HistoricalCandleRetryStopReason,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.ports.market import HistoricalCandleRequest, HistoricalCandleSourceError

WINDOW_START = datetime(2026, 7, 1, tzinfo=UTC)


def request(candle_count: int) -> HistoricalCandleRequest:
    """Build one aligned historical range."""

    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_START + candle_count * timedelta(minutes=1),
    )


def test_page_planner_covers_range_without_overlap_or_gap() -> None:
    planner = HistoricalCandlePagePlanner(
        HistoricalCandlePaginationPolicy(
            page_size_candles=3,
            max_total_candles=10,
            inter_page_delay_seconds=0,
        )
    )

    pages = planner.plan(request(8))

    assert [page.expected_count for page in pages] == [3, 3, 2]
    assert pages[0].window_start == WINDOW_START
    assert pages[-1].window_end_exclusive == WINDOW_START + timedelta(minutes=8)
    assert all(
        current.window_end_exclusive == following.window_start
        for current, following in pairwise(pages)
    )


@given(
    candle_count=st.integers(min_value=1, max_value=500),
    page_size=st.integers(min_value=1, max_value=1_000),
)
def test_page_planner_invariants_hold_across_bounded_ranges(
    candle_count: int,
    page_size: int,
) -> None:
    planner = HistoricalCandlePagePlanner(
        HistoricalCandlePaginationPolicy(
            page_size_candles=page_size,
            max_total_candles=max(candle_count, page_size),
            inter_page_delay_seconds=0,
        )
    )

    pages = planner.plan(request(candle_count))

    assert sum(page.expected_count for page in pages) == candle_count
    assert all(1 <= page.expected_count <= page_size for page in pages)
    assert pages[0].window_start == WINDOW_START
    assert pages[-1].window_end_exclusive == request(candle_count).window_end_exclusive
    assert all(
        current.window_end_exclusive == following.window_start
        for current, following in pairwise(pages)
    )


def test_page_planner_rejects_unbounded_work_before_fetching() -> None:
    planner = HistoricalCandlePagePlanner(
        HistoricalCandlePaginationPolicy(
            page_size_candles=3,
            max_total_candles=5,
        )
    )

    with pytest.raises(HistoricalCandlePaginationError) as error:
        planner.plan(request(6))

    assert error.value.code is HistoricalCandlePaginationErrorCode.WINDOW_TOO_LARGE


def test_large_page_plan_can_be_streamed_without_materializing_all_pages() -> None:
    planner = HistoricalCandlePagePlanner(
        HistoricalCandlePaginationPolicy(
            page_size_candles=1,
            max_total_candles=100_000,
            inter_page_delay_seconds=0,
        )
    )
    large_request = request(100_000)

    first_three = tuple(islice(planner.iter_pages(large_request), 3))

    assert planner.page_count(large_request) == 100_000
    assert [page.window_start for page in first_three] == [
        WINDOW_START,
        WINDOW_START + timedelta(minutes=1),
        WINDOW_START + timedelta(minutes=2),
    ]


@pytest.mark.parametrize(
    ("policy_kwargs", "message"),
    [
        ({"page_size_candles": 0}, "page_size_candles"),
        ({"page_size_candles": 1_001}, "page_size_candles"),
        ({"page_size_candles": 10, "max_total_candles": 9}, "max_total_candles"),
        ({"inter_page_delay_seconds": -1}, "inter_page_delay_seconds"),
    ],
)
def test_invalid_pagination_policy_fails_closed(
    policy_kwargs: dict[str, int],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        HistoricalCandlePaginationPolicy(**policy_kwargs)


def test_retry_policy_uses_bounded_exponential_delays() -> None:
    policy = HistoricalCandleRetryPolicy(
        max_attempts=4,
        base_delay_seconds=2,
        max_delay_seconds=5,
    )
    transient = HistoricalCandleSourceError(
        "provider_unavailable",
        "safe detail",
        retryable=True,
    )

    assert policy.delay_after(failed_attempt=1, error=transient) == 2
    assert policy.delay_after(failed_attempt=2, error=transient) == 4
    assert policy.delay_after(failed_attempt=3, error=transient) == 5
    assert policy.delay_after(failed_attempt=4, error=transient) is None


def test_retry_policy_rejects_invalid_attempt_numbers() -> None:
    policy = HistoricalCandleRetryPolicy()
    transient = HistoricalCandleSourceError(
        "provider_unavailable",
        "safe detail",
        retryable=True,
    )

    with pytest.raises(ValueError, match="failed_attempt"):
        policy.delay_after(failed_attempt=0, error=transient)
    with pytest.raises(ValueError, match="failed_attempt"):
        policy.stop_reason_after(failed_attempt=0, error=transient)


def test_retry_policy_honors_only_bounded_retry_after_values() -> None:
    policy = HistoricalCandleRetryPolicy(max_retry_after_seconds=60)
    bounded = HistoricalCandleSourceError(
        "rate_limited",
        "safe detail",
        retryable=True,
        retry_after_seconds=17,
    )
    excessive = HistoricalCandleSourceError(
        "rate_limited",
        "safe detail",
        retryable=True,
        retry_after_seconds=61,
    )
    permanent = HistoricalCandleSourceError(
        "invalid_payload",
        "safe detail",
        retryable=False,
    )

    assert policy.delay_after(failed_attempt=1, error=bounded) == 17
    assert policy.delay_after(failed_attempt=1, error=excessive) is None
    assert policy.delay_after(failed_attempt=1, error=permanent) is None
    assert (
        policy.stop_reason_after(failed_attempt=1, error=excessive)
        is HistoricalCandleRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
    )
    assert (
        policy.stop_reason_after(failed_attempt=1, error=permanent)
        is HistoricalCandleRetryStopReason.NON_RETRYABLE
    )
