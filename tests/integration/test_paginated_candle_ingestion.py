"""Integration tests for bounded pagination, retries, and resume boundaries."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from wealth.adapters.binance import BinancePublicCandleSource
from wealth.adapters.market import InMemoryCandleStore
from wealth.application.pagination import (
    HistoricalCandlePaginationPolicy,
    HistoricalCandleRetryPolicy,
    HistoricalCandleRetryStopReason,
    PaginatedHistoricalCandleIngestor,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.quality import CandleStream, DataQualityStatus
from wealth.ports.http import HttpResponse, HttpTransportError
from wealth.ports.market import HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
REQUEST_TIME = WINDOW_START + timedelta(days=1)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class FixedClock:
    """Return one safe time after every requested historical window."""

    def now(self) -> datetime:
        return REQUEST_TIME


class RecordingSleeper:
    """Record delays without blocking tests."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)


class ScenarioHttpClient:
    """Return scripted failures or generate exact rows for each page."""

    def __init__(
        self,
        *outcomes: HttpResponse | HttpTransportError | None,
    ) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, str]] = []

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, timeout_seconds
        captured_query = dict(query)
        self.calls.append(captured_query)
        if self._outcomes:
            outcome = self._outcomes.pop(0)
            if isinstance(outcome, HttpTransportError):
                raise outcome
            if outcome is not None:
                return outcome
        start = UTC_EPOCH + timedelta(milliseconds=int(captured_query["startTime"]))
        limit = int(captured_query["limit"])
        rows = [kline(start + index * timedelta(minutes=1)) for index in range(limit)]
        return response(rows)


def epoch_milliseconds(value: datetime) -> int:
    """Convert an aware timestamp without floating-point arithmetic."""

    delta = value - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def kline(open_time: datetime, *, close: str = "102") -> list[int | str]:
    """Build one valid one-minute Binance row."""

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
    """Encode one bounded provider response."""

    return HttpResponse(
        status_code=status_code,
        headers=headers,
        body=json.dumps(rows).encode(),
    )


def request(
    candle_count: int,
    *,
    window_start: datetime = WINDOW_START,
) -> HistoricalCandleRequest:
    """Build one BTC spot range."""

    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=window_start,
        window_end_exclusive=window_start + candle_count * timedelta(minutes=1),
    )


def stream() -> CandleStream:
    """Return the expected canonical stream."""

    return CandleStream(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def ingestor(
    *,
    http: ScenarioHttpClient,
    store: InMemoryCandleStore,
    sleeper: RecordingSleeper,
    page_size: int = 2,
) -> PaginatedHistoricalCandleIngestor:
    """Build the bounded application flow with deterministic test policies."""

    return PaginatedHistoricalCandleIngestor(
        source=BinancePublicCandleSource(http=http, clock=FixedClock()),
        store=store,
        sleeper=sleeper,
        pagination_policy=HistoricalCandlePaginationPolicy(
            page_size_candles=page_size,
            max_total_candles=10,
            inter_page_delay_seconds=0.25,
        ),
        retry_policy=HistoricalCandleRetryPolicy(
            max_attempts=3,
            base_delay_seconds=1,
            max_delay_seconds=10,
            max_retry_after_seconds=60,
        ),
    )


def test_complete_range_is_paged_paced_and_stored_without_overlap() -> None:
    http = ScenarioHttpClient()
    store = InMemoryCandleStore()
    sleeper = RecordingSleeper()

    result = ingestor(http=http, store=store, sleeper=sleeper).ingest(request(5))

    assert result.accepted is True
    assert result.planned_page_count == 3
    assert [page.request.expected_count for page in result.pages] == [2, 2, 1]
    assert [page.attempts for page in result.pages] == [1, 1, 1]
    assert [call["startTime"] for call in http.calls] == [
        str(epoch_milliseconds(WINDOW_START)),
        str(epoch_milliseconds(WINDOW_START + timedelta(minutes=2))),
        str(epoch_milliseconds(WINDOW_START + timedelta(minutes=4))),
    ]
    assert sleeper.delays == [0.25, 0.25]
    assert result.next_window_start == WINDOW_START + timedelta(minutes=5)
    assert len(store.records_for_stream(stream())) == 5


def test_transient_provider_failure_retries_with_exponential_delay() -> None:
    http = ScenarioHttpClient(
        response([], status_code=503),
        response([], status_code=503),
        None,
    )
    sleeper = RecordingSleeper()

    result = ingestor(
        http=http,
        store=InMemoryCandleStore(),
        sleeper=sleeper,
    ).ingest(request(2))

    assert result.accepted is True
    assert result.pages[0].attempts == 3
    assert result.pages[0].retry_delays_seconds == (1.0, 2.0)
    assert sleeper.delays == [1.0, 2.0]
    assert len(http.calls) == 3


def test_rate_limit_honors_bounded_retry_after() -> None:
    http = ScenarioHttpClient(
        response([], status_code=429, headers=(("Retry-After", "17"),)),
        None,
    )
    sleeper = RecordingSleeper()

    result = ingestor(
        http=http,
        store=InMemoryCandleStore(),
        sleeper=sleeper,
    ).ingest(request(2))

    assert result.accepted is True
    assert result.pages[0].attempts == 2
    assert sleeper.delays == [17]


def test_excessive_retry_after_stops_without_sleeping() -> None:
    http = ScenarioHttpClient(
        response([], status_code=429, headers=(("Retry-After", "61"),)),
    )
    sleeper = RecordingSleeper()

    result = ingestor(
        http=http,
        store=InMemoryCandleStore(),
        sleeper=sleeper,
    ).ingest(request(2))

    assert result.accepted is False
    assert result.pages[0].failure is not None
    assert result.pages[0].failure.machine_code == "rate_limited"
    assert (
        result.pages[0].failure.stop_reason
        is HistoricalCandleRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
    )
    assert result.pages[0].attempts == 1
    assert result.next_window_start == WINDOW_START
    assert sleeper.delays == []


def test_nonretryable_payload_failure_stops_before_later_pages() -> None:
    http = ScenarioHttpClient(
        HttpResponse(status_code=200, headers=(), body=b"not-json"),
    )
    store = InMemoryCandleStore()
    sleeper = RecordingSleeper()

    result = ingestor(http=http, store=store, sleeper=sleeper).ingest(request(4))

    assert result.accepted is False
    assert len(result.pages) == 1
    assert result.pages[0].failure is not None
    assert result.pages[0].failure.machine_code == "invalid_payload"
    assert result.pages[0].failure.retryable is False
    assert result.pages[0].failure.stop_reason is HistoricalCandleRetryStopReason.NON_RETRYABLE
    assert sleeper.delays == []
    assert store.records_for_stream(stream()) == ()


def test_exhausted_second_page_retains_first_page_and_resume_boundary() -> None:
    unavailable = response([], status_code=503)
    http = ScenarioHttpClient(None, unavailable, unavailable, unavailable)
    store = InMemoryCandleStore()
    sleeper = RecordingSleeper()

    result = ingestor(http=http, store=store, sleeper=sleeper).ingest(request(5))

    assert result.accepted is False
    assert result.planned_page_count == 3
    assert len(result.pages) == 2
    assert result.pages[0].accepted is True
    assert result.pages[1].failure is not None
    assert result.pages[1].attempts == 3
    assert result.pages[1].failure.stop_reason is HistoricalCandleRetryStopReason.ATTEMPTS_EXHAUSTED
    assert result.next_window_start == WINDOW_START + timedelta(minutes=2)
    assert sleeper.delays == [0.25, 1, 2]
    assert len(store.records_for_stream(stream())) == 2

    resume = ingestor(
        http=ScenarioHttpClient(),
        store=store,
        sleeper=RecordingSleeper(),
    ).ingest(request(3, window_start=result.next_window_start))

    assert resume.accepted is True
    assert len(store.records_for_stream(stream())) == 5


def test_quality_failure_is_not_retried_or_persisted() -> None:
    incomplete_page = response([kline(WINDOW_START)])
    http = ScenarioHttpClient(incomplete_page)
    store = InMemoryCandleStore()
    sleeper = RecordingSleeper()

    result = ingestor(http=http, store=store, sleeper=sleeper).ingest(request(4))

    assert result.accepted is False
    assert len(result.pages) == 1
    assert result.pages[0].failure is None
    assert result.pages[0].ingestion is not None
    assert result.pages[0].ingestion.quality.status is DataQualityStatus.FAIL
    assert result.pages[0].attempts == 1
    assert sleeper.delays == []
    assert store.records_for_stream(stream()) == ()


def test_storage_conflict_stops_without_retrying_or_fetching_later_pages() -> None:
    store = InMemoryCandleStore()
    seed = ingestor(
        http=ScenarioHttpClient(),
        store=store,
        sleeper=RecordingSleeper(),
    ).ingest(request(2))
    changed_first_page = response(
        [
            kline(WINDOW_START, close="103"),
            kline(WINDOW_START + timedelta(minutes=1)),
        ]
    )
    http = ScenarioHttpClient(changed_first_page)
    sleeper = RecordingSleeper()

    result = ingestor(http=http, store=store, sleeper=sleeper).ingest(request(4))

    assert seed.accepted is True
    assert result.accepted is False
    assert len(result.pages) == 1
    assert result.pages[0].failure is None
    assert result.pages[0].ingestion is not None
    assert result.pages[0].ingestion.accepted is False
    assert result.pages[0].attempts == 1
    assert len(http.calls) == 1
    assert sleeper.delays == []
    assert len(store.records_for_stream(stream())) == 2
    assert len(store.conflicts_for_stream(stream())) == 1
