"""Tests for the public, read-only Binance candle adapter."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from wealth.adapters.binance import (
    BINANCE_SPOT_KLINES_URL,
    BINANCE_USDM_KLINES_URL,
    BinanceCandleError,
    BinanceCandleErrorCode,
    BinancePublicCandleSource,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.ports.http import HttpResponse, HttpTransportError
from wealth.ports.market import HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)
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
    """Convert a test timestamp without floating-point arithmetic."""

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def kline(open_time: datetime, *, close: str = "102") -> list[int | str]:
    """Build one structurally valid Binance kline row."""

    open_time_ms = epoch_milliseconds(open_time)
    return [
        open_time_ms,
        "100",
        "105",
        "95",
        close,
        "12.5",
        open_time_ms + 59_999,
        "1275",
        42,
        "6",
        "612",
        "0",
    ]


def response(
    rows: list[list[int | str]],
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
) -> HistoricalCandleRequest:
    """Build one valid closed-window request."""

    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol=provider_symbol,
        instrument_type=instrument_type,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )


def source(http: StubHttpClient) -> BinancePublicCandleSource:
    """Build an adapter with deterministic request and processing times."""

    return BinancePublicCandleSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT),
    )


def test_spot_rows_are_requested_in_utc_and_normalized_exactly() -> None:
    provider_response = response([kline(WINDOW_START), kline(WINDOW_START + timedelta(minutes=1))])
    http = StubHttpClient(provider_response)

    batch = source(http).fetch(request())

    assert http.calls == [
        (
            BINANCE_SPOT_KLINES_URL,
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": str(epoch_milliseconds(WINDOW_START)),
                "endTime": str(epoch_milliseconds(WINDOW_END) - 1),
                "limit": "2",
                "timeZone": "0",
            },
            10.0,
        )
    ]
    assert len(batch.records) == 2
    first = batch.records[0]
    assert first.instrument == "BTC-USDT"
    assert first.instrument_type is InstrumentType.SPOT
    assert first.open_time == WINDOW_START
    assert first.close_time == WINDOW_START + timedelta(minutes=1)
    assert str(first.open) == "100"
    assert str(first.quote_volume) == "1275"
    assert first.trade_count == 42
    assert first.observed_at == OBSERVED_AT
    assert first.processed_at == PROCESSED_AT
    assert batch.raw_payload.payload == provider_response.body
    assert batch.raw_payload.payload_sha256 == sha256(provider_response.body).hexdigest()
    assert batch.raw_payload.lineage_reference in first.lineage
    assert first.lineage[1].startswith("binance-public-rest:api/v3/klines:BTCUSDT:1m:")


@pytest.mark.parametrize(
    "instrument_type",
    [InstrumentType.PERPETUAL_FUTURE, InstrumentType.DATED_FUTURE],
)
def test_usdm_futures_use_the_public_futures_endpoint(
    instrument_type: InstrumentType,
) -> None:
    http = StubHttpClient(
        response([kline(WINDOW_START), kline(WINDOW_START + timedelta(minutes=1))])
    )

    batch = source(http).fetch(request(instrument_type=instrument_type))

    assert http.calls[0][0] == BINANCE_USDM_KLINES_URL
    assert "timeZone" not in http.calls[0][1]
    assert all(record.instrument_type is instrument_type for record in batch.records)


def test_rate_limit_preserves_retry_after_without_parsing_untrusted_body() -> None:
    http = StubHttpClient(
        HttpResponse(
            status_code=429,
            headers=(("Retry-After", "17"),),
            body=b'{"code":-1003,"msg":"untrusted provider text"}',
        )
    )

    with pytest.raises(BinanceCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is BinanceCandleErrorCode.RATE_LIMITED
    assert error.value.retry_after_seconds == 17
    assert "untrusted provider text" not in str(error.value)


def test_transport_failure_is_classified_without_network_details() -> None:
    http = StubHttpClient(error=HttpTransportError("sensitive network detail"))

    with pytest.raises(BinanceCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is BinanceCandleErrorCode.TRANSPORT_FAILURE
    assert "sensitive network detail" not in str(error.value)


@pytest.mark.parametrize(
    "body",
    [
        b"not-json",
        b'{"code":-1121,"msg":"Invalid symbol."}',
        json.dumps([["too", "short"]]).encode(),
    ],
)
def test_malformed_provider_payload_fails_closed(body: bytes) -> None:
    http = StubHttpClient(HttpResponse(status_code=200, headers=(), body=body))

    with pytest.raises(BinanceCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is BinanceCandleErrorCode.INVALID_PAYLOAD


def test_provider_interval_mismatch_fails_closed() -> None:
    invalid = kline(WINDOW_START)
    invalid[6] = epoch_milliseconds(WINDOW_START) + 60_000
    http = StubHttpClient(response([invalid]))

    with pytest.raises(BinanceCandleError) as error:
        source(http).fetch(request(window_end_exclusive=WINDOW_START + timedelta(minutes=1)))

    assert error.value.code is BinanceCandleErrorCode.INVALID_PAYLOAD
    assert "close time" in str(error.value)


def test_open_or_oversized_windows_are_rejected_before_network_access() -> None:
    http = StubHttpClient(response([]))
    future_source = BinancePublicCandleSource(
        http=http,
        clock=SequenceClock(WINDOW_START + timedelta(seconds=30)),
    )

    with pytest.raises(BinanceCandleError) as future_error:
        future_source.fetch(request(window_end_exclusive=WINDOW_START + timedelta(minutes=1)))

    assert future_error.value.code is BinanceCandleErrorCode.INVALID_REQUEST
    assert http.calls == []

    oversized_http = StubHttpClient(response([]))
    oversized_source = BinancePublicCandleSource(
        http=oversized_http,
        clock=SequenceClock(WINDOW_START + timedelta(days=2)),
    )
    with pytest.raises(BinanceCandleError) as oversized_error:
        oversized_source.fetch(
            request(window_end_exclusive=WINDOW_START + timedelta(minutes=1_001))
        )

    assert oversized_error.value.code is BinanceCandleErrorCode.INVALID_REQUEST
    assert oversized_http.calls == []


def test_lowercase_or_implicit_provider_symbol_is_rejected() -> None:
    http = StubHttpClient(response([]))

    with pytest.raises(BinanceCandleError) as error:
        source(http).fetch(request(provider_symbol="btcusdt"))

    assert error.value.code is BinanceCandleErrorCode.INVALID_REQUEST
    assert http.calls == []


def test_clock_regression_fails_closed() -> None:
    http = StubHttpClient(response([]))
    regressing_source = BinancePublicCandleSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, REQUEST_TIME - timedelta(seconds=1)),
    )

    with pytest.raises(BinanceCandleError) as error:
        regressing_source.fetch(request())

    assert error.value.code is BinanceCandleErrorCode.INVALID_REQUEST
