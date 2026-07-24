"""Integration tests for fetch, quality gate, and idempotent storage."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from wealth.adapters.binance import BinancePublicCandleSource
from wealth.adapters.market import InMemoryCandleStore
from wealth.application.ingestion import HistoricalCandleIngestor
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.quality import CandleStream, CandleWriteStatus, DataQualityStatus
from wealth.ports.http import HttpResponse
from wealth.ports.market import HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)


class FixedClock:
    """Return one timestamp suitable for all ingestion phases."""

    def now(self) -> datetime:
        return WINDOW_END + timedelta(hours=1)


class StaticHttpClient:
    """Return a static public payload without network access."""

    def __init__(self, rows: list[list[int | str]]) -> None:
        self._body = json.dumps(rows).encode()

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, query, timeout_seconds
        return HttpResponse(status_code=200, headers=(), body=self._body)


def epoch_milliseconds(value: datetime) -> int:
    """Convert an aware test timestamp to epoch milliseconds."""

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000


def kline(open_time: datetime) -> list[int | str]:
    """Build one complete one-minute public kline."""

    open_time_ms = epoch_milliseconds(open_time)
    return [
        open_time_ms,
        "100",
        "105",
        "95",
        "102",
        "12.5",
        open_time_ms + 59_999,
        "1275",
        42,
        "6",
        "612",
        "0",
    ]


def request() -> HistoricalCandleRequest:
    """Return the expected two-minute BTC spot window."""

    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def stream() -> CandleStream:
    """Return the canonical stream written by the adapter."""

    return CandleStream(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def test_complete_provider_batch_passes_quality_gate_and_is_stored() -> None:
    store = InMemoryCandleStore()
    source = BinancePublicCandleSource(
        http=StaticHttpClient([kline(WINDOW_START), kline(WINDOW_START + timedelta(minutes=1))]),
        clock=FixedClock(),
    )
    ingestor = HistoricalCandleIngestor(source=source, store=store)

    first = ingestor.ingest(request())
    repeated = ingestor.ingest(request())

    assert first.accepted is True
    assert first.quality.status is DataQualityStatus.PASS
    assert [write.status for write in first.writes] == [
        CandleWriteStatus.INSERTED,
        CandleWriteStatus.INSERTED,
    ]
    assert [write.status for write in repeated.writes] == [
        CandleWriteStatus.DUPLICATE,
        CandleWriteStatus.DUPLICATE,
    ]
    assert len(store.records_for_stream(stream())) == 2


def test_incomplete_provider_batch_is_reported_and_not_stored() -> None:
    store = InMemoryCandleStore()
    source = BinancePublicCandleSource(
        http=StaticHttpClient([kline(WINDOW_START)]),
        clock=FixedClock(),
    )

    result = HistoricalCandleIngestor(source=source, store=store).ingest(request())

    assert result.accepted is False
    assert result.quality.status is DataQualityStatus.FAIL
    assert result.quality.missing_ranges[0].missing_count == 1
    assert result.writes == ()
    assert store.records_for_stream(stream()) == ()
