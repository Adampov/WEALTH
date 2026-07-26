"""Fail-closed checks for exact historical-candle persistence evidence."""

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from wealth.adapters.binance import BinancePublicCandleSource
from wealth.adapters.market import InMemoryCandleStore
from wealth.application.ingestion import (
    HistoricalCandleIngestionResult,
    HistoricalCandleIngestor,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.quality import (
    CandleWriteResult,
    CandleWriteStatus,
    DataQualityStatus,
    MarketDataBatchWriteResult,
    RawPayloadWriteStatus,
)
from wealth.ports.http import HttpResponse
from wealth.ports.market import CandleFetchBatch, HistoricalCandleRequest

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=2)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class FixedClock:
    """Return one trusted UTC timestamp for every ingestion phase."""

    def now(self) -> datetime:
        return WINDOW_END + timedelta(hours=1)


class StaticHttpClient:
    """Return one deterministic two-candle provider response."""

    def __init__(self) -> None:
        self.body = json.dumps(
            [
                kline(WINDOW_START),
                kline(WINDOW_START + timedelta(minutes=1)),
            ]
        ).encode()

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, query, timeout_seconds
        return HttpResponse(status_code=200, headers=(), body=self.body)


class ScriptedCandleStore(InMemoryCandleStore):
    """Return one exact configured batch outcome without repairing it."""

    def __init__(self, persistence: MarketDataBatchWriteResult) -> None:
        super().__init__()
        self.persistence = persistence
        self.appended_batches: list[CandleFetchBatch] = []

    def append_batch(self, batch: CandleFetchBatch) -> MarketDataBatchWriteResult:
        self.appended_batches.append(batch)
        return self.persistence


def epoch_milliseconds(value: datetime) -> int:
    """Convert an aware fixture timestamp to epoch milliseconds."""

    delta = value - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000


def kline(open_time: datetime) -> list[int | str]:
    """Build one complete public Binance one-minute kline."""

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
    """Return the exact two-candle test window."""

    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def valid_results() -> tuple[
    HistoricalCandleIngestionResult,
    HistoricalCandleIngestionResult,
]:
    """Return exact inserted and idempotent duplicate results."""

    store = InMemoryCandleStore()
    ingestor = HistoricalCandleIngestor(
        source=BinancePublicCandleSource(http=StaticHttpClient(), clock=FixedClock()),
        store=store,
    )
    return ingestor.ingest(request()), ingestor.ingest(request())


def scripted_result(
    persistence: MarketDataBatchWriteResult,
) -> HistoricalCandleIngestionResult:
    """Pass one hostile store result through the real ingestion boundary."""

    store = ScriptedCandleStore(persistence)
    result = HistoricalCandleIngestor(
        source=BinancePublicCandleSource(http=StaticHttpClient(), clock=FixedClock()),
        store=store,
    ).ingest(request())

    assert result.quality.status is DataQualityStatus.PASS
    assert result.raw_write == persistence.raw_payload
    assert result.writes == persistence.candles
    assert store.appended_batches == [result.batch]
    return result


def test_exact_inserted_duplicate_and_mixed_evidence_is_accepted() -> None:
    inserted, duplicate = valid_results()
    raw_write = inserted.raw_write
    assert raw_write is not None
    second_write = inserted.writes[1].model_copy(
        update={
            "status": CandleWriteStatus.DUPLICATE,
            "existing_record_id": UUID(int=800),
        }
    )
    mixed = scripted_result(
        MarketDataBatchWriteResult(
            raw_payload=raw_write,
            candles=(inserted.writes[0], second_write),
        )
    )

    assert inserted.accepted is True
    assert duplicate.accepted is True
    assert mixed.accepted is True


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "short",
        "extra",
        "duplicated",
        "reordered",
        "wrong-id",
    ],
)
def test_missing_extra_or_misattributed_candle_evidence_is_rejected(case: str) -> None:
    result, _ = valid_results()
    raw_write = result.raw_write
    assert raw_write is not None
    first, second = result.writes
    writes: tuple[CandleWriteResult, ...]

    if case == "empty":
        writes = ()
    elif case == "short":
        writes = (first,)
    elif case == "extra":
        writes = (first, second, second)
    elif case == "duplicated":
        writes = (first, first)
    elif case == "reordered":
        writes = (second, first)
    elif case == "wrong-id":
        writes = (
            first,
            second.model_copy(update={"incoming_record_id": UUID(int=900)}),
        )
    else:
        raise AssertionError(f"unhandled test case: {case}")

    malformed = scripted_result(
        MarketDataBatchWriteResult(
            raw_payload=raw_write,
            candles=writes,
        )
    )

    assert malformed.accepted is False


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "wrong-incoming",
        "inserted-with-existing",
        "duplicate-without-existing",
        "duplicate-with-wrong-existing",
        "conflict-with-writes",
        "conflict-without-writes",
    ],
)
def test_missing_or_contradictory_raw_evidence_is_rejected(case: str) -> None:
    result, _ = valid_results()
    raw_write = result.raw_write
    assert raw_write is not None

    if case == "missing":
        malformed = replace(result, raw_write=None)
    elif case == "wrong-incoming":
        malformed_raw = raw_write.model_copy(update={"incoming_record_id": UUID(int=901)})
    elif case == "inserted-with-existing":
        malformed_raw = raw_write.model_copy(update={"existing_record_id": UUID(int=902)})
    elif case == "duplicate-without-existing":
        malformed_raw = raw_write.model_copy(
            update={
                "status": RawPayloadWriteStatus.DUPLICATE,
                "existing_record_id": None,
            }
        )
    elif case == "duplicate-with-wrong-existing":
        malformed_raw = raw_write.model_copy(
            update={
                "status": RawPayloadWriteStatus.DUPLICATE,
                "existing_record_id": UUID(int=903),
            }
        )
    elif case in {"conflict-with-writes", "conflict-without-writes"}:
        malformed_raw = raw_write.model_copy(
            update={
                "status": RawPayloadWriteStatus.CONFLICT,
                "existing_record_id": raw_write.incoming_record_id,
            }
        )
    else:
        raise AssertionError(f"unhandled test case: {case}")

    if case != "missing":
        malformed = scripted_result(
            MarketDataBatchWriteResult(
                raw_payload=malformed_raw,
                candles=() if case == "conflict-without-writes" else result.writes,
            )
        )

    assert malformed.accepted is False


@pytest.mark.parametrize(
    ("status", "existing_record_id"),
    [
        pytest.param(CandleWriteStatus.INSERTED, UUID(int=904), id="inserted-with-existing"),
        pytest.param(CandleWriteStatus.DUPLICATE, None, id="duplicate-without-existing"),
        pytest.param(CandleWriteStatus.CONFLICT, UUID(int=905), id="coherent-conflict"),
        pytest.param(CandleWriteStatus.CONFLICT, None, id="conflict-without-existing"),
    ],
)
def test_contradictory_or_conflicting_candle_status_is_rejected(
    status: CandleWriteStatus,
    existing_record_id: UUID | None,
) -> None:
    result, _ = valid_results()
    malformed_first = result.writes[0].model_copy(
        update={
            "status": status,
            "existing_record_id": existing_record_id,
        }
    )

    raw_write = result.raw_write
    assert raw_write is not None
    malformed = scripted_result(
        MarketDataBatchWriteResult(
            raw_payload=raw_write,
            candles=(malformed_first, result.writes[1]),
        )
    )

    assert malformed.accepted is False
