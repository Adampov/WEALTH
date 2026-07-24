"""Tests for canonical trade, ticker, and best-bid-ask contracts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.domain.market import InstrumentType
from wealth.domain.order_flow import (
    AggressorSide,
    CanonicalBestBidAsk,
    CanonicalTicker,
    CanonicalTrade,
)

EVENT_TIME = datetime(2026, 7, 24, 17, 0, tzinfo=UTC)


def common_values(**overrides: object) -> dict[str, object]:
    """Build shared point-in-time identity and lineage evidence."""

    values: dict[str, object] = {
        "record_id": UUID(int=1),
        "source": "synthetic.test",
        "venue": "TEST",
        "instrument": "BTC-USDT",
        "instrument_type": InstrumentType.SPOT,
        "event_time": EVENT_TIME,
        "observed_at": EVENT_TIME + timedelta(milliseconds=10),
        "processed_at": EVENT_TIME + timedelta(milliseconds=20),
        "provider_sequence": 100,
        "lineage": ("raw-market-payload:00000000-0000-0000-0000-000000000001",),
    }
    values.update(overrides)
    return values


def build_trade(**overrides: object) -> CanonicalTrade:
    """Build one valid provider-identified trade."""

    values = common_values(
        provider_trade_id="trade-42",
        price=Decimal("100.25"),
        base_quantity=Decimal("0.4"),
        quote_quantity=Decimal("40.10"),
        aggressor_side=AggressorSide.BUY,
    )
    values.update(overrides)
    return CanonicalTrade.model_validate(values)


def build_ticker(**overrides: object) -> CanonicalTicker:
    """Build one valid ticker with an explicit rolling window."""

    values = common_values(
        last_price=Decimal("102"),
        window_start=EVENT_TIME - timedelta(hours=24),
        window_end=EVENT_TIME,
        window_open=Decimal("100"),
        window_high=Decimal("105"),
        window_low=Decimal("95"),
        base_volume=Decimal("1000"),
        quote_volume=Decimal("101000"),
    )
    values.update(overrides)
    return CanonicalTicker.model_validate(values)


def build_best_bid_ask(**overrides: object) -> CanonicalBestBidAsk:
    """Build one valid uncrossed top-of-book snapshot."""

    values = common_values(
        bid_price=Decimal("100"),
        bid_quantity=Decimal("2.5"),
        ask_price=Decimal("100.20"),
        ask_quantity=Decimal("1.75"),
    )
    values.update(overrides)
    return CanonicalBestBidAsk.model_validate(values)


def test_trade_is_strict_immutable_versioned_and_idempotency_addressable() -> None:
    trade = build_trade()

    assert trade.schema_version == "1.0"
    assert trade.natural_key == (
        "synthetic.test",
        "TEST",
        "BTC-USDT",
        InstrumentType.SPOT,
        "trade-42",
    )
    assert trade.calculated_quote_quantity == Decimal("40.100")
    with pytest.raises(ValidationError):
        trade.price = Decimal("101")
    with pytest.raises(ValidationError):
        build_trade(price="100.25")


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"base_quantity": Decimal("0")}, "greater than 0"),
        ({"price": Decimal("-1")}, "greater than 0"),
        ({"provider_trade_id": "trade 42"}, "must not contain whitespace"),
        (
            {"event_time": EVENT_TIME + timedelta(milliseconds=11)},
            "event_time must not be after observed_at",
        ),
        (
            {"processed_at": EVENT_TIME + timedelta(milliseconds=9)},
            "observed_at must not be after processed_at",
        ),
    ],
)
def test_trade_rejects_invalid_identity_quantity_or_point_in_time_state(
    override: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        build_trade(**override)


def test_trade_preserves_unknown_aggressor_instead_of_inventing_a_side() -> None:
    trade = build_trade(
        aggressor_side=AggressorSide.UNKNOWN,
        quote_quantity=None,
    )

    assert trade.aggressor_side is AggressorSide.UNKNOWN
    assert trade.quote_quantity is None


def test_ticker_can_represent_last_price_without_window_statistics() -> None:
    ticker = build_ticker(
        window_start=None,
        window_end=None,
        window_open=None,
        window_high=None,
        window_low=None,
        base_volume=None,
        quote_volume=None,
    )

    assert ticker.last_price == Decimal("102")
    assert ticker.window_start is None


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"window_end": None}, "must be set together"),
        ({"window_high": None}, "window_high and window_low must be set together"),
        ({"window_low": Decimal("106")}, "must not exceed"),
        ({"last_price": Decimal("106")}, "must be inside the window range"),
        ({"window_open": Decimal("94")}, "window_open must be inside"),
        (
            {"window_start": EVENT_TIME, "window_end": EVENT_TIME},
            "window_start must precede",
        ),
        (
            {"window_end": EVENT_TIME + timedelta(seconds=1)},
            "window_end must not be after event_time",
        ),
    ],
)
def test_ticker_rejects_ambiguous_or_contradictory_window_state(
    override: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        build_ticker(**override)


def test_best_bid_ask_exposes_exact_spread_midpoint_and_basis_points() -> None:
    quote = build_best_bid_ask()

    assert quote.spread == Decimal("0.20")
    assert quote.mid_price == Decimal("100.10")
    assert quote.spread_basis_points == Decimal("0.20") / Decimal("100.10") * Decimal("10000")
    assert quote.natural_key == (
        "synthetic.test",
        "TEST",
        "BTC-USDT",
        InstrumentType.SPOT,
        EVENT_TIME,
        100,
    )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"bid_price": Decimal("100.20")}, "strictly below"),
        ({"bid_price": Decimal("101")}, "strictly below"),
        ({"bid_quantity": Decimal("0")}, "greater than 0"),
        ({"ask_quantity": Decimal("-1")}, "greater than 0"),
    ],
)
def test_best_bid_ask_rejects_locked_crossed_or_empty_quotes(
    override: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        build_best_bid_ask(**override)


def test_trade_round_trips_without_decimal_loss() -> None:
    record = build_trade()

    restored = CanonicalTrade.model_validate_json(record.model_dump_json())

    assert restored == record


def test_ticker_round_trips_without_decimal_loss() -> None:
    record = build_ticker()

    restored = CanonicalTicker.model_validate_json(record.model_dump_json())

    assert restored == record


def test_best_bid_ask_round_trips_without_decimal_loss() -> None:
    record = build_best_bid_ask()

    restored = CanonicalBestBidAsk.model_validate_json(record.model_dump_json())

    assert restored == record
