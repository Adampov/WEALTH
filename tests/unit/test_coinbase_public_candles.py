"""Tests for the public, read-only Coinbase Exchange candle adapter."""

import json
import sys
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256

import pytest

from wealth.adapters.coinbase import (
    COINBASE_PRODUCTS_URL,
    CoinbaseCandleError,
    CoinbaseCandleErrorCode,
    CoinbasePublicCandleSource,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.ports.http import HttpResponse, HttpTransportError
from wealth.ports.market import HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)
REQUEST_TIME = WINDOW_END + timedelta(hours=1)
OBSERVED_AT = REQUEST_TIME + timedelta(seconds=1)
PROCESSED_AT = REQUEST_TIME + timedelta(seconds=2)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
INVALID_CLOCK_VALUES = (
    pytest.param(REQUEST_TIME.replace(tzinfo=None), id="naive"),
    pytest.param(
        REQUEST_TIME.astimezone(timezone(timedelta(hours=5, minutes=30))),
        id="positive-offset",
    ),
    pytest.param(
        REQUEST_TIME.astimezone(timezone(-timedelta(hours=4))),
        id="negative-offset",
    ),
    pytest.param(
        REQUEST_TIME.replace(tzinfo=timezone(timedelta(0), "named-zero")),
        id="named-zero-offset",
    ),
)


@pytest.fixture
def fixed_json_decoder_limits() -> Iterator[None]:
    """Pin and restore the interpreter-wide JSON decoder resource boundaries."""

    previous_integer_limit = sys.get_int_max_str_digits()
    previous_recursion_limit = sys.getrecursionlimit()
    sys.set_int_max_str_digits(4_300)
    sys.setrecursionlimit(1_000)
    try:
        yield
    finally:
        sys.setrecursionlimit(previous_recursion_limit)
        sys.set_int_max_str_digits(previous_integer_limit)


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


def epoch_seconds(value: datetime) -> int:
    delta = value - UTC_EPOCH
    return delta.days * 86_400 + delta.seconds


def candle(
    open_time: datetime,
    *,
    low: int | float = 95,
    high: int | float = 105,
    open_price: int | float = 100,
    close: int | float = 102,
    volume: int | float = 12.5,
) -> list[int | float]:
    return [
        epoch_seconds(open_time),
        low,
        high,
        open_price,
        close,
        volume,
    ]


def response(
    rows: list[object],
    *,
    status_code: int = 200,
    headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        headers=headers,
        body=json.dumps(rows, separators=(",", ":")).encode(),
    )


def request(
    *,
    instrument_type: InstrumentType = InstrumentType.SPOT,
    timeframe: CandleTimeframe = CandleTimeframe.ONE_MINUTE,
    provider_symbol: str = "BTC-USD",
    window_start: datetime = WINDOW_START,
    window_end_exclusive: datetime = WINDOW_END,
) -> HistoricalCandleRequest:
    return HistoricalCandleRequest(
        instrument="BTC-USD",
        provider_symbol=provider_symbol,
        instrument_type=instrument_type,
        timeframe=timeframe,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )


def source(http: StubHttpClient) -> CoinbasePublicCandleSource:
    return CoinbasePublicCandleSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT),
    )


@pytest.mark.parametrize("invalid_time", INVALID_CLOCK_VALUES)
def test_invalid_initial_clock_fails_typed_before_http(invalid_time: datetime) -> None:
    http = StubHttpClient(response([]))
    adapter = CoinbasePublicCandleSource(http=http, clock=SequenceClock(invalid_time))

    with pytest.raises(CoinbaseCandleError) as error:
        adapter.fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.INVALID_REQUEST
    assert http.calls == []


@pytest.mark.parametrize("invalid_time", INVALID_CLOCK_VALUES)
@pytest.mark.parametrize("clock_position", [2, 3], ids=["observed", "processed"])
def test_invalid_later_clock_fails_typed_after_one_http_without_evidence(
    invalid_time: datetime,
    clock_position: int,
) -> None:
    http = StubHttpClient(response([]))
    values = (
        (REQUEST_TIME, invalid_time)
        if clock_position == 2
        else (REQUEST_TIME, OBSERVED_AT, invalid_time)
    )
    adapter = CoinbasePublicCandleSource(http=http, clock=SequenceClock(*values))

    with pytest.raises(CoinbaseCandleError) as error:
        adapter.fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.INVALID_REQUEST
    assert len(http.calls) == 1


def test_reverse_rows_are_bounded_sorted_and_normalized_exactly() -> None:
    provider_response = response(
        [
            candle(WINDOW_START + timedelta(minutes=1), close=103.25),
            candle(WINDOW_START),
            candle(WINDOW_START - timedelta(minutes=1), close=99),
        ]
    )
    http = StubHttpClient(provider_response)

    batch = source(http).fetch(request())

    assert http.calls == [
        (
            f"{COINBASE_PRODUCTS_URL}/BTC-USD/candles",
            {
                "start": "2026-07-24T10:00:00Z",
                "end": "2026-07-24T10:02:00Z",
                "granularity": "60",
            },
            10.0,
        )
    ]
    assert [record.open_time for record in batch.records] == [
        WINDOW_START,
        WINDOW_START + timedelta(minutes=1),
    ]
    first = batch.records[0]
    assert first.source == "coinbase.exchange-public-rest"
    assert first.instrument == "BTC-USD"
    assert first.instrument_type is InstrumentType.SPOT
    assert first.close_time == WINDOW_START + timedelta(minutes=1)
    assert str(first.open) == "100"
    assert str(first.base_volume) == "12.5"
    assert first.quote_volume is None
    assert first.trade_count is None
    assert first.provider_sequence == epoch_seconds(WINDOW_START)
    assert first.observed_at == OBSERVED_AT
    assert first.processed_at == PROCESSED_AT
    assert batch.raw_payload.payload == provider_response.body
    assert batch.raw_payload.payload_sha256 == sha256(provider_response.body).hexdigest()
    assert batch.raw_payload.lineage_reference in first.lineage
    assert first.lineage[1].startswith("coinbase-exchange-public-rest:products/BTC-USD/candles:1m:")


@pytest.mark.parametrize(
    ("timeframe", "window_start", "granularity"),
    [
        (CandleTimeframe.FIVE_MINUTES, WINDOW_START, "300"),
        (CandleTimeframe.FIFTEEN_MINUTES, WINDOW_START, "900"),
        (
            CandleTimeframe.ONE_HOUR,
            WINDOW_START.replace(minute=0),
            "3600",
        ),
        (
            CandleTimeframe.ONE_DAY,
            WINDOW_START.replace(hour=0, minute=0),
            "86400",
        ),
    ],
)
def test_supported_timeframes_use_official_granularity(
    timeframe: CandleTimeframe,
    window_start: datetime,
    granularity: str,
) -> None:
    http = StubHttpClient(response([]))
    window_end = window_start + timeframe.duration
    configured = CoinbasePublicCandleSource(
        http=http,
        clock=SequenceClock(
            window_end + timedelta(days=1),
            window_end + timedelta(days=1, seconds=1),
            window_end + timedelta(days=1, seconds=2),
        ),
    )

    configured.fetch(
        request(
            timeframe=timeframe,
            window_start=window_start,
            window_end_exclusive=window_end,
        )
    )

    assert http.calls[0][1]["granularity"] == granularity


@pytest.mark.parametrize(
    ("instrument_type", "timeframe", "provider_symbol", "expected_code"),
    [
        (
            InstrumentType.PERPETUAL_FUTURE,
            CandleTimeframe.ONE_MINUTE,
            "BTC-USD",
            CoinbaseCandleErrorCode.UNSUPPORTED_INSTRUMENT,
        ),
        (
            InstrumentType.SPOT,
            CandleTimeframe.FOUR_HOURS,
            "BTC-USD",
            CoinbaseCandleErrorCode.UNSUPPORTED_TIMEFRAME,
        ),
        (
            InstrumentType.SPOT,
            CandleTimeframe.ONE_MINUTE,
            "btc-usd",
            CoinbaseCandleErrorCode.INVALID_REQUEST,
        ),
        (
            InstrumentType.SPOT,
            CandleTimeframe.ONE_MINUTE,
            "BTC/USD",
            CoinbaseCandleErrorCode.INVALID_REQUEST,
        ),
    ],
)
def test_unsupported_or_ambiguous_requests_fail_before_network(
    instrument_type: InstrumentType,
    timeframe: CandleTimeframe,
    provider_symbol: str,
    expected_code: CoinbaseCandleErrorCode,
) -> None:
    http = StubHttpClient(response([]))
    window_start = (
        WINDOW_START.replace(hour=8, minute=0)
        if timeframe is CandleTimeframe.FOUR_HOURS
        else WINDOW_START
    )
    window_end = window_start + timeframe.duration
    configured = CoinbasePublicCandleSource(
        http=http,
        clock=SequenceClock(window_end + timedelta(hours=1)),
    )

    with pytest.raises(CoinbaseCandleError) as error:
        configured.fetch(
            request(
                instrument_type=instrument_type,
                timeframe=timeframe,
                provider_symbol=provider_symbol,
                window_start=window_start,
                window_end_exclusive=window_end,
            )
        )

    assert error.value.code is expected_code
    assert http.calls == []


def test_open_and_oversized_windows_fail_before_network() -> None:
    future_http = StubHttpClient(response([]))
    future_source = CoinbasePublicCandleSource(
        http=future_http,
        clock=SequenceClock(WINDOW_START + timedelta(seconds=30)),
    )

    with pytest.raises(CoinbaseCandleError) as future_error:
        future_source.fetch(request(window_end_exclusive=WINDOW_START + timedelta(minutes=1)))

    assert future_error.value.code is CoinbaseCandleErrorCode.INVALID_REQUEST
    assert future_http.calls == []

    oversized_http = StubHttpClient(response([]))
    oversized_source = CoinbasePublicCandleSource(
        http=oversized_http,
        clock=SequenceClock(WINDOW_START + timedelta(days=2)),
    )
    with pytest.raises(CoinbaseCandleError) as oversized_error:
        oversized_source.fetch(
            request(
                window_end_exclusive=WINDOW_START + timedelta(minutes=301),
            )
        )

    assert oversized_error.value.code is CoinbaseCandleErrorCode.INVALID_REQUEST
    assert oversized_http.calls == []


@pytest.mark.parametrize(
    ("body", "expected_cause"),
    [
        pytest.param(b"\xff", UnicodeDecodeError, id="invalid-utf8"),
        pytest.param(b"not-json", json.JSONDecodeError, id="malformed-json"),
        pytest.param(
            b"[" + (b"1" * 5_000) + b"]",
            ValueError,
            id="integer-conversion-limit",
        ),
        pytest.param(
            (b"[" * 10_000) + (b"]" * 10_000),
            RecursionError,
            id="excessive-nesting",
        ),
    ],
)
def test_decoder_failures_are_sanitized_and_fail_closed(
    body: bytes,
    expected_cause: type[Exception],
    fixed_json_decoder_limits: None,
) -> None:
    http = StubHttpClient(HttpResponse(status_code=200, headers=(), body=body))

    with pytest.raises(CoinbaseCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.INVALID_PAYLOAD
    assert error.value.retryable is False
    assert error.value.retry_after_seconds is None
    assert type(error.value.__cause__) is expected_cause
    assert (
        str(error.value) == "invalid_payload: response could not be decoded as bounded UTF-8 JSON"
    )
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "body",
    [
        b'{"message":"not an array"}',
        json.dumps([["too", "short"]]).encode(),
        json.dumps([[1.5, 95, 105, 100, 102, 12]]).encode(),
        json.dumps([[1, True, 105, 100, 102, 12]]).encode(),
    ],
)
def test_structurally_invalid_provider_payload_fails_closed(body: bytes) -> None:
    http = StubHttpClient(HttpResponse(status_code=200, headers=(), body=body))

    with pytest.raises(CoinbaseCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.INVALID_PAYLOAD
    assert error.value.retryable is False


def test_out_of_window_or_excessive_payload_fails_closed() -> None:
    after_end = StubHttpClient(response([candle(WINDOW_END)]))
    with pytest.raises(CoinbaseCandleError) as range_error:
        source(after_end).fetch(request())

    excessive = StubHttpClient(response([candle(WINDOW_START)] * 301))
    with pytest.raises(CoinbaseCandleError) as size_error:
        source(excessive).fetch(request())

    assert range_error.value.code is CoinbaseCandleErrorCode.INVALID_PAYLOAD
    assert size_error.value.code is CoinbaseCandleErrorCode.INVALID_PAYLOAD


@pytest.mark.parametrize(
    ("retry_after", "retryable", "expected_retry_after"),
    [
        (None, True, None),
        ("17", True, 17),
        ("not-a-number", False, None),
        ("9" * 100, False, None),
    ],
)
def test_rate_limit_classification_uses_only_a_safe_retry_header(
    retry_after: str | None,
    retryable: bool,
    expected_retry_after: int | None,
) -> None:
    headers = () if retry_after is None else (("Retry-After", retry_after),)
    http = StubHttpClient(
        HttpResponse(
            status_code=429,
            headers=headers,
            body=b'{"message":"untrusted provider text"}',
        )
    )

    with pytest.raises(CoinbaseCandleError) as error:
        source(http).fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.RATE_LIMITED
    assert error.value.retryable is retryable
    assert error.value.retry_after_seconds == expected_retry_after
    assert "untrusted provider text" not in str(error.value)


def test_transport_and_provider_failures_are_safely_classified() -> None:
    transport_http = StubHttpClient(error=HttpTransportError("sensitive detail"))
    with pytest.raises(CoinbaseCandleError) as transport_error:
        source(transport_http).fetch(request())

    unavailable_http = StubHttpClient(response([], status_code=503))
    with pytest.raises(CoinbaseCandleError) as unavailable_error:
        source(unavailable_http).fetch(request())

    rejected_http = StubHttpClient(response([], status_code=400))
    with pytest.raises(CoinbaseCandleError) as rejected_error:
        source(rejected_http).fetch(request())

    assert transport_error.value.code is CoinbaseCandleErrorCode.TRANSPORT_FAILURE
    assert transport_error.value.retryable is True
    assert "sensitive detail" not in str(transport_error.value)
    assert unavailable_error.value.code is CoinbaseCandleErrorCode.PROVIDER_UNAVAILABLE
    assert unavailable_error.value.retryable is True
    assert rejected_error.value.code is CoinbaseCandleErrorCode.PROVIDER_REJECTED
    assert rejected_error.value.retryable is False


def test_clock_regression_fails_closed() -> None:
    http = StubHttpClient(response([]))
    regressing_source = CoinbasePublicCandleSource(
        http=http,
        clock=SequenceClock(REQUEST_TIME, REQUEST_TIME - timedelta(seconds=1)),
    )

    with pytest.raises(CoinbaseCandleError) as error:
        regressing_source.fetch(request())

    assert error.value.code is CoinbaseCandleErrorCode.INVALID_REQUEST
