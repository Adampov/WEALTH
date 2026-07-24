"""Tests for the public, read-only Binance aggregate-trade adapter."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256

import pytest
from pydantic import ValidationError

from wealth.adapters.binance_order_flow import (
    BINANCE_SPOT_AGG_TRADES_URL,
    BINANCE_USDM_AGG_TRADES_URL,
    MAX_BINANCE_AGGREGATE_TRADES,
    BinanceAggregateTradeError,
    BinanceAggregateTradeErrorCode,
    BinancePublicAggregateTradeSource,
)
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow import AggressorSide, CanonicalTrade, TradeAggregationKind
from wealth.domain.order_flow_quality import ProviderSequencePolicy
from wealth.ports.http import HttpResponse, HttpTransportError
from wealth.ports.order_flow import PublicTradeWindowRequest

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
REQUEST_TIME = WINDOW_END + timedelta(hours=1)
OBSERVED_AT = REQUEST_TIME + timedelta(seconds=1)
PROCESSED_AT = REQUEST_TIME + timedelta(seconds=2)


class SequenceClock:
    """Return explicit timestamps in call order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now(self) -> datetime:
        return next(self._values)


class StubHttpClient:
    """Capture public HTTP calls and return one configured response."""

    def __init__(
        self,
        response: HttpResponse | None = None,
        *,
        error: HttpTransportError | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.calls.append((url, dict(query), timeout_seconds))
        if self.error is not None:
            raise self.error
        if self.response is None:
            raise AssertionError("test HTTP response was not configured")
        return self.response


def epoch_milliseconds(value: datetime) -> int:
    """Convert a timestamp without floating-point arithmetic."""

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def aggregate_trade(
    *,
    aggregate_id: int = 100,
    event_time: datetime = WINDOW_START,
    price: object = "100.25",
    quantity: object = "0.4",
    first_trade_id: object = 1_000,
    last_trade_id: object = 1_004,
    buyer_is_maker: object = True,
    include_spot_field: bool = True,
    normal_quantity: object | None = None,
) -> dict[str, object]:
    """Build one documented Binance aggregate-trade row."""

    row: dict[str, object] = {
        "a": aggregate_id,
        "p": price,
        "q": quantity,
        "f": first_trade_id,
        "l": last_trade_id,
        "T": epoch_milliseconds(event_time),
        "m": buyer_is_maker,
    }
    if include_spot_field:
        row["M"] = True
    if normal_quantity is not None:
        row["nq"] = normal_quantity
    return row


def response(
    rows: object,
    *,
    status_code: int = 200,
    headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    """Encode one fake provider response."""

    return HttpResponse(
        status_code=status_code,
        headers=headers,
        body=json.dumps(rows).encode(),
    )


def request(
    *,
    instrument_type: InstrumentType = InstrumentType.SPOT,
    provider_symbol: str = "BTCUSDT",
    window_start: datetime = WINDOW_START,
    window_end_exclusive: datetime = WINDOW_END,
) -> PublicTradeWindowRequest:
    """Build one valid, closed public-trade request."""

    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol=provider_symbol,
        instrument_type=instrument_type,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )


def source(http: StubHttpClient) -> BinancePublicAggregateTradeSource:
    """Build an adapter with deterministic request and processing times."""

    return BinancePublicAggregateTradeSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT),
    )


def test_spot_aggregate_trade_is_requested_and_normalized_with_explicit_evidence() -> None:
    provider_response = response([aggregate_trade()])
    http = StubHttpClient(provider_response)

    batch = source(http).fetch(request())

    assert http.calls == [
        (
            BINANCE_SPOT_AGG_TRADES_URL,
            {
                "symbol": "BTCUSDT",
                "startTime": str(epoch_milliseconds(WINDOW_START)),
                "endTime": str(epoch_milliseconds(WINDOW_END) - 1),
                "limit": "1000",
            },
            10.0,
        )
    ]
    assert batch.stream.sequence_policy is ProviderSequencePolicy.MONOTONIC
    assert len(batch.records) == 1
    trade = batch.records[0]
    assert isinstance(trade, CanonicalTrade)
    assert trade.instrument == "BTC-USDT"
    assert trade.instrument_type is InstrumentType.SPOT
    assert trade.event_time == WINDOW_START
    assert trade.provider_trade_id == "100"
    assert trade.provider_sequence == 100
    assert trade.price == Decimal("100.25")
    assert trade.base_quantity == Decimal("0.4")
    assert trade.quote_quantity is None
    assert trade.calculated_quote_quantity == Decimal("40.100")
    assert trade.aggressor_side is AggressorSide.SELL
    assert trade.aggregation_kind is TradeAggregationKind.PROVIDER_DEFINED
    assert trade.provider_first_trade_id == "1000"
    assert trade.provider_last_trade_id == "1004"
    assert trade.observed_at == OBSERVED_AT
    assert trade.processed_at == PROCESSED_AT
    assert batch.raw_payload.payload == provider_response.body
    assert batch.raw_payload.payload_sha256 == sha256(provider_response.body).hexdigest()
    assert batch.raw_payload.lineage_reference in trade.lineage


@pytest.mark.parametrize(
    "instrument_type",
    [InstrumentType.PERPETUAL_FUTURE, InstrumentType.DATED_FUTURE],
)
def test_usdm_futures_use_public_endpoint_and_accept_documented_normal_quantity(
    instrument_type: InstrumentType,
) -> None:
    http = StubHttpClient(
        response(
            [
                aggregate_trade(
                    include_spot_field=False,
                    normal_quantity="0.3",
                    buyer_is_maker=False,
                )
            ]
        )
    )

    batch = source(http).fetch(request(instrument_type=instrument_type))

    assert http.calls[0][0] == BINANCE_USDM_AGG_TRADES_URL
    trade = batch.records[0]
    assert isinstance(trade, CanonicalTrade)
    assert trade.instrument_type is instrument_type
    assert trade.aggressor_side is AggressorSide.BUY


def test_empty_complete_window_retains_raw_evidence_without_inventing_trades() -> None:
    provider_response = response([])

    batch = source(StubHttpClient(provider_response)).fetch(request())

    assert batch.records == ()
    assert batch.raw_payload.payload == provider_response.body


def test_row_cap_fails_closed_instead_of_claiming_a_complete_window() -> None:
    rows = [
        aggregate_trade(
            aggregate_id=index,
            event_time=WINDOW_START + timedelta(milliseconds=index),
            first_trade_id=index,
            last_trade_id=index,
        )
        for index in range(MAX_BINANCE_AGGREGATE_TRADES)
    ]

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source(StubHttpClient(response(rows))).fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.POSSIBLY_TRUNCATED
    assert raised.value.retryable is False


def test_rate_limit_preserves_retry_after_without_untrusted_provider_text() -> None:
    http = StubHttpClient(
        HttpResponse(
            status_code=429,
            headers=(("Retry-After", "17"),),
            body=b'{"code":-1003,"msg":"untrusted provider text"}',
        )
    )

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source(http).fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.RATE_LIMITED
    assert raised.value.retryable is True
    assert raised.value.retry_after_seconds == 17
    assert "untrusted provider text" not in str(raised.value)


@pytest.mark.parametrize("retry_after", [None, "invalid", "9" * 100])
def test_rate_limit_without_usable_retry_after_is_not_automatically_retryable(
    retry_after: str | None,
) -> None:
    headers = () if retry_after is None else (("Retry-After", retry_after),)
    http = StubHttpClient(
        HttpResponse(status_code=418, headers=headers, body=b'{"msg":"untrusted"}')
    )

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source(http).fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.RATE_LIMITED
    assert raised.value.retryable is False
    assert raised.value.retry_after_seconds is None


def test_transport_failure_is_classified_without_network_details() -> None:
    http = StubHttpClient(error=HttpTransportError("sensitive network detail"))

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source(http).fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.TRANSPORT_FAILURE
    assert raised.value.retryable is True
    assert "sensitive network detail" not in str(raised.value)


@pytest.mark.parametrize(
    "payload",
    [
        "not-an-array",
        [["positional", "row"]],
        [{"a": 1}],
        [{**aggregate_trade(), "unknown": "schema-drift"}],
        [aggregate_trade(price=100)],
        [aggregate_trade(quantity=0)],
        [aggregate_trade(first_trade_id=5, last_trade_id=4)],
        [aggregate_trade(buyer_is_maker=1)],
        [aggregate_trade(event_time=WINDOW_END)],
        [
            aggregate_trade(
                include_spot_field=False,
                normal_quantity="-1",
            )
        ],
    ],
)
def test_malformed_or_contradictory_rows_fail_closed(payload: object) -> None:
    http_response = (
        HttpResponse(status_code=200, headers=(), body=b"not-json")
        if payload == "not-an-array"
        else response(payload)
    )

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source(StubHttpClient(http_response)).fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.INVALID_PAYLOAD
    assert raised.value.retryable is False


def test_open_oversized_old_or_implicit_windows_are_rejected_before_network() -> None:
    http = StubHttpClient(response([]))
    future_source = BinancePublicAggregateTradeSource(
        http=http,
        clock=SequenceClock(WINDOW_START + timedelta(seconds=30)),
    )
    with pytest.raises(BinanceAggregateTradeError) as future:
        future_source.fetch(request())
    assert future.value.code is BinanceAggregateTradeErrorCode.INVALID_REQUEST
    assert http.calls == []

    oversized_http = StubHttpClient(response([]))
    oversized = BinancePublicAggregateTradeSource(
        http=oversized_http,
        clock=SequenceClock(REQUEST_TIME),
    )
    with pytest.raises(BinanceAggregateTradeError):
        oversized.fetch(request(window_end_exclusive=WINDOW_START + timedelta(hours=1)))
    assert oversized_http.calls == []

    old_http = StubHttpClient(response([]))
    old_source = BinancePublicAggregateTradeSource(
        http=old_http,
        clock=SequenceClock(WINDOW_START + timedelta(days=2)),
    )
    with pytest.raises(BinanceAggregateTradeError):
        old_source.fetch(request(instrument_type=InstrumentType.PERPETUAL_FUTURE))
    assert old_http.calls == []

    symbol_http = StubHttpClient(response([]))
    with pytest.raises(BinanceAggregateTradeError):
        source(symbol_http).fetch(request(provider_symbol="btcusdt"))
    assert symbol_http.calls == []


def test_clock_regression_fails_closed() -> None:
    http = StubHttpClient(response([]))
    regressing = BinancePublicAggregateTradeSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, REQUEST_TIME - timedelta(seconds=1)),
    )

    with pytest.raises(BinanceAggregateTradeError) as raised:
        regressing.fetch(request())

    assert raised.value.code is BinanceAggregateTradeErrorCode.INVALID_REQUEST


def test_trade_request_requires_positive_millisecond_aligned_window() -> None:
    with pytest.raises(ValidationError):
        request(window_end_exclusive=WINDOW_START)
    with pytest.raises(ValidationError):
        request(window_start=WINDOW_START + timedelta(microseconds=1))


def test_adapter_rejects_non_https_endpoint_configuration() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        BinancePublicAggregateTradeSource(
            http=StubHttpClient(response([])),
            clock=SequenceClock(REQUEST_TIME),
            spot_agg_trades_url="http://example.test/aggTrades",
        )
