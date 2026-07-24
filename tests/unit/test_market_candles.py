"""Tests for canonical market-candle invariants."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType

RECORD_ID = UUID("00000000-0000-0000-0000-000000000101")
OPEN_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def build_candle(**overrides: object) -> CanonicalCandle:
    """Build one valid final candle."""

    values: dict[str, object] = {
        "record_id": RECORD_ID,
        "source": "synthetic.test",
        "venue": "TEST",
        "instrument": "BTC-USDT",
        "instrument_type": InstrumentType.SPOT,
        "timeframe": CandleTimeframe.ONE_MINUTE,
        "open_time": OPEN_TIME,
        "close_time": OPEN_TIME + timedelta(minutes=1),
        "observed_at": OPEN_TIME + timedelta(minutes=1, seconds=1),
        "processed_at": OPEN_TIME + timedelta(minutes=1, seconds=2),
        "open": Decimal("100"),
        "high": Decimal("105"),
        "low": Decimal("95"),
        "close": Decimal("102"),
        "base_volume": Decimal("12.5"),
        "quote_volume": Decimal("1275"),
        "trade_count": 42,
        "provider_sequence": 7,
        "lineage": ("fixture:btc-usdt:2026-07-24T12:00Z",),
    }
    values.update(overrides)
    return CanonicalCandle.model_validate(values)


def test_candle_is_strict_immutable_and_versioned() -> None:
    candle = build_candle()

    assert candle.schema_version == "1.0"
    with pytest.raises(ValidationError):
        candle.close = Decimal("103")
    with pytest.raises(ValidationError):
        build_candle(open="100")


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"high": Decimal("99")}, "high must be at least open and close"),
        ({"low": Decimal("103")}, "low must be at most open and close"),
        (
            {"close_time": OPEN_TIME + timedelta(minutes=5)},
            "candle duration must match timeframe",
        ),
        (
            {"observed_at": OPEN_TIME + timedelta(seconds=59)},
            "closed candle cannot be observed before close_time",
        ),
        (
            {"processed_at": OPEN_TIME + timedelta(minutes=1)},
            "observed_at must not be after processed_at",
        ),
    ],
)
def test_candle_rejects_invalid_market_or_time_state(
    override: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        build_candle(**override)


def test_candle_rejects_open_time_off_the_utc_timeframe_grid() -> None:
    open_time = OPEN_TIME + timedelta(seconds=30)

    with pytest.raises(ValidationError, match="must align to the timeframe UTC grid"):
        build_candle(
            open_time=open_time,
            close_time=open_time + timedelta(minutes=1),
            observed_at=open_time + timedelta(minutes=1, seconds=1),
            processed_at=open_time + timedelta(minutes=1, seconds=2),
        )


@given(
    low=st.integers(min_value=1, max_value=99),
    open_price=st.integers(min_value=100, max_value=110),
    close_price=st.integers(min_value=100, max_value=110),
    high=st.integers(min_value=111, max_value=200),
    volume=st.integers(min_value=0, max_value=1_000_000),
)
def test_valid_ohlcv_is_preserved_exactly(
    low: int,
    open_price: int,
    close_price: int,
    high: int,
    volume: int,
) -> None:
    candle = build_candle(
        low=Decimal(low),
        open=Decimal(open_price),
        close=Decimal(close_price),
        high=Decimal(high),
        base_volume=Decimal(volume),
    )

    assert candle.low <= min(candle.open, candle.close)
    assert candle.high >= max(candle.open, candle.close)
    assert candle.base_volume == Decimal(volume)
