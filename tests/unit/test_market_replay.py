"""Tests for deterministic, point-in-time market replay."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wealth.application.replay import (
    MarketReplay,
    ReplayErrorCode,
    ReplayValidationError,
)
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType

BASE_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def candle(
    *,
    record_number: int,
    minute: int,
    observed_delay_seconds: int,
    close: str = "100",
) -> CanonicalCandle:
    """Build a deterministic replay record."""

    open_time = BASE_TIME + timedelta(minutes=minute)
    close_time = open_time + timedelta(minutes=1)
    return CanonicalCandle(
        record_id=UUID(int=record_number),
        source="synthetic.replay",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        open_time=open_time,
        close_time=close_time,
        observed_at=close_time + timedelta(seconds=observed_delay_seconds),
        processed_at=close_time + timedelta(seconds=observed_delay_seconds + 1),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        lineage=(f"fixture:{minute}",),
    )


def test_replay_withholds_late_arrivals_until_they_were_observed() -> None:
    on_time = candle(record_number=1, minute=0, observed_delay_seconds=1)
    late = candle(record_number=2, minute=1, observed_delay_seconds=120)
    replay = MarketReplay([late, on_time])

    early_slice = replay.slice_at(BASE_TIME + timedelta(minutes=3))
    late_slice = replay.slice_at(BASE_TIME + timedelta(minutes=4))

    assert early_slice.records == (on_time,)
    assert early_slice.withheld_count == 1
    assert early_slice.next_observation_time == late.observed_at
    assert late_slice.records == (on_time, late)
    assert late_slice.withheld_count == 0


def test_replay_order_is_deterministic_for_unordered_input() -> None:
    first = candle(record_number=1, minute=0, observed_delay_seconds=2)
    second = candle(record_number=2, minute=1, observed_delay_seconds=1)
    cutoff = BASE_TIME + timedelta(minutes=3)

    forward = MarketReplay([first, second]).slice_at(cutoff)
    reversed_input = MarketReplay([second, first]).slice_at(cutoff)

    assert forward.records == reversed_input.records == (first, second)


@given(
    observation_delays=st.lists(
        st.integers(min_value=0, max_value=600),
        min_size=1,
        max_size=12,
    ),
    cutoff_seconds=st.integers(min_value=0, max_value=1_800),
)
def test_replay_never_exposes_records_observed_after_cutoff(
    observation_delays: list[int],
    cutoff_seconds: int,
) -> None:
    records = [
        candle(
            record_number=index + 1,
            minute=index,
            observed_delay_seconds=delay,
        )
        for index, delay in enumerate(observation_delays)
    ]
    evaluation_time = BASE_TIME + timedelta(seconds=cutoff_seconds)

    replay_slice = MarketReplay(reversed(records)).slice_at(evaluation_time)

    expected = tuple(record for record in records if record.observed_at <= evaluation_time)
    assert replay_slice.records == expected
    assert all(record.observed_at <= evaluation_time for record in replay_slice.records)
    assert replay_slice.withheld_count == len(records) - len(expected)


def test_replay_rejects_duplicate_and_conflicting_natural_keys() -> None:
    original = candle(record_number=1, minute=0, observed_delay_seconds=1)
    duplicate = original.model_copy(update={"record_id": UUID(int=2)})
    conflict = original.model_copy(update={"record_id": UUID(int=3), "close": Decimal("101")})

    with pytest.raises(ReplayValidationError) as duplicate_error:
        MarketReplay([original, duplicate])
    assert duplicate_error.value.code is ReplayErrorCode.DUPLICATE_RECORD

    with pytest.raises(ReplayValidationError) as conflict_error:
        MarketReplay([original, conflict])
    assert conflict_error.value.code is ReplayErrorCode.CONFLICTING_RECORD


def test_replay_rejects_naive_evaluation_time() -> None:
    replay = MarketReplay([])

    with pytest.raises(ReplayValidationError) as error:
        replay.slice_at(datetime(2026, 7, 24, 12, 0))

    assert error.value.code is ReplayErrorCode.NAIVE_EVALUATION_TIME
