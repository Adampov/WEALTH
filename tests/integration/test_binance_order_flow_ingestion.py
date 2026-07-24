"""End-to-end test for public Binance trades through quality and SQLite."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

from wealth.adapters.binance_order_flow import BinancePublicAggregateTradeSource
from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.adapters.sqlite_order_flow import SQLiteOrderFlowStore
from wealth.application.order_flow_ingestion import OrderFlowBatchIngestor
from wealth.domain.market import InstrumentType
from wealth.domain.quality import RawPayloadWriteStatus
from wealth.ports.http import HttpResponse
from wealth.ports.order_flow import PublicTradeWindowRequest

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
REQUEST_TIME = WINDOW_END + timedelta(minutes=1)
OBSERVED_AT = REQUEST_TIME + timedelta(seconds=1)
PROCESSED_AT = REQUEST_TIME + timedelta(seconds=2)


class SequenceClock:
    """Return explicit timestamps in call order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now(self) -> datetime:
        return next(self._values)


class StaticHttpClient:
    """Return one bounded provider payload without network access."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.body = json.dumps(rows).encode()

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, query, timeout_seconds
        return HttpResponse(status_code=200, headers=(), body=self.body)


def epoch_milliseconds(value: datetime) -> int:
    """Convert a timestamp without floating-point arithmetic."""

    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value - epoch
    return delta.days * 86_400_000 + delta.seconds * 1_000


def request() -> PublicTradeWindowRequest:
    """Return one bounded Binance Spot trade window."""

    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def test_public_trades_pass_quality_and_survive_sqlite_restart(tmp_path: Path) -> None:
    rows = [
        {
            "a": 100,
            "p": "100",
            "q": "0.5",
            "f": 1000,
            "l": 1002,
            "T": epoch_milliseconds(WINDOW_START),
            "m": False,
            "M": True,
        },
        {
            "a": 101,
            "p": "101",
            "q": "0.4",
            "f": 1003,
            "l": 1003,
            "T": epoch_milliseconds(WINDOW_START + timedelta(seconds=1)),
            "m": True,
            "M": True,
        },
    ]
    source = BinancePublicAggregateTradeSource(
        http=StaticHttpClient(rows),
        clock=SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT),
    )
    batch = source.fetch(request())
    database_path = tmp_path / "binance-order-flow.sqlite3"

    first = OrderFlowBatchIngestor(
        store=SQLiteOrderFlowStore(database_path),
    ).ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )
    restarted = SQLiteOrderFlowStore(database_path)

    assert first.accepted is True
    assert first.raw_write is not None
    assert first.raw_write.status is RawPayloadWriteStatus.INSERTED
    assert restarted.raw_payload(batch.raw_payload.record_id) == batch.raw_payload
    assert restarted.records_for_stream(batch.stream) == batch.records


def test_empty_public_window_is_accepted_as_raw_evidence_without_records() -> None:
    source = BinancePublicAggregateTradeSource(
        http=StaticHttpClient([]),
        clock=SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT),
    )
    batch = source.fetch(request())
    store = InMemoryOrderFlowStore()

    result = OrderFlowBatchIngestor(store=store).ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.accepted is True
    assert result.writes == ()
    assert store.raw_payload(batch.raw_payload.record_id) == batch.raw_payload
    assert store.records_for_stream(batch.stream) == ()
