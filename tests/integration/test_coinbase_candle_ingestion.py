"""Integration tests for Coinbase through quality and durable storage."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wealth.adapters.coinbase import CoinbasePublicCandleSource
from wealth.adapters.sqlite_market import SQLiteCandleStore
from wealth.application.ingestion import HistoricalCandleIngestor
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.quality import CandleStream, DataQualityStatus
from wealth.ports.http import HttpResponse
from wealth.ports.market import HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)
REQUEST_TIME = WINDOW_END + timedelta(hours=1)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class FixedClock:
    """Return one safe time after the requested historical window."""

    def now(self) -> datetime:
        return REQUEST_TIME


class StaticHttpClient:
    """Return one exact public payload without network access."""

    def __init__(self, rows: list[list[int | float]]) -> None:
        self.body = json.dumps(rows, separators=(",", ":")).encode()

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, query, timeout_seconds
        return HttpResponse(status_code=200, headers=(), body=self.body)


def epoch_seconds(value: datetime) -> int:
    delta = value - UTC_EPOCH
    return delta.days * 86_400 + delta.seconds


def candle(open_time: datetime) -> list[int | float]:
    return [epoch_seconds(open_time), 95, 105, 100, 102, 12.5]


def request() -> HistoricalCandleRequest:
    return HistoricalCandleRequest(
        instrument="BTC-USD",
        provider_symbol="BTC-USD",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def stream() -> CandleStream:
    return CandleStream(
        source="coinbase.exchange-public-rest",
        venue="COINBASE",
        instrument="BTC-USD",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def test_complete_coinbase_batch_passes_quality_and_survives_restart(
    tmp_path: Path,
) -> None:
    http = StaticHttpClient(
        [
            candle(WINDOW_START + timedelta(minutes=1)),
            candle(WINDOW_START),
            candle(WINDOW_START - timedelta(minutes=1)),
        ]
    )
    database_path = tmp_path / "market.sqlite3"
    result = HistoricalCandleIngestor(
        source=CoinbasePublicCandleSource(http=http, clock=FixedClock()),
        store=SQLiteCandleStore(database_path),
    ).ingest(request())
    restarted = SQLiteCandleStore(database_path)

    assert result.accepted is True
    assert result.quality.status is DataQualityStatus.PASS
    assert len(restarted.records_for_stream(stream())) == 2
    assert restarted.raw_payload(result.batch.raw_payload.record_id) is not None


def test_missing_coinbase_interval_is_explicit_and_not_persisted(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market.sqlite3"
    result = HistoricalCandleIngestor(
        source=CoinbasePublicCandleSource(
            http=StaticHttpClient([candle(WINDOW_START)]),
            clock=FixedClock(),
        ),
        store=SQLiteCandleStore(database_path),
    ).ingest(request())

    assert result.accepted is False
    assert result.quality.status is DataQualityStatus.FAIL
    assert result.quality.missing_ranges[0].missing_count == 1
    assert SQLiteCandleStore(database_path).records_for_stream(stream()) == ()
