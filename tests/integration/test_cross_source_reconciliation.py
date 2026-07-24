"""Integration proof for stored cross-source candle reconciliation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from wealth.adapters.market import InMemoryCandleStore
from wealth.application.reconciliation import CandleCrossSourceReconciler
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import CandleStream
from wealth.domain.reconciliation import (
    CandleReconciliationIssueCode,
    CandleReconciliationPolicy,
    CandleReconciliationStatus,
)

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=3)


def stream(source: str, venue: str) -> CandleStream:
    """Build one stored BTC-USD stream."""

    return CandleStream(
        source=source,
        venue=venue,
        instrument="BTC-USD",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def candle(
    candle_stream: CandleStream,
    *,
    record_id: int,
    minute: int,
    close: str = "100",
) -> CanonicalCandle:
    """Build one persisted canonical candle."""

    open_time = WINDOW_START + timedelta(minutes=minute)
    return CanonicalCandle(
        record_id=UUID(int=record_id),
        source=candle_stream.source,
        venue=candle_stream.venue,
        instrument=candle_stream.instrument,
        instrument_type=candle_stream.instrument_type,
        timeframe=candle_stream.timeframe,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        observed_at=open_time + timedelta(minutes=1, seconds=1),
        processed_at=open_time + timedelta(minutes=1, seconds=2),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("10"),
        lineage=(f"fixture:{candle_stream.source}:{record_id}",),
    )


def test_stored_streams_produce_repeatable_divergence_evidence() -> None:
    store = InMemoryCandleStore()
    primary = stream("binance.public-rest", "BINANCE")
    reference = stream("coinbase.exchange-public-rest", "COINBASE")
    for record in (
        candle(primary, record_id=1, minute=0),
        candle(primary, record_id=2, minute=1),
        candle(primary, record_id=3, minute=2),
        candle(reference, record_id=4, minute=0),
        candle(reference, record_id=5, minute=1, close="102"),
        candle(reference, record_id=6, minute=2),
    ):
        store.append(record)

    primary_records = store.records_for_stream(primary)
    reference_records = store.records_for_stream(reference)
    policy = CandleReconciliationPolicy(max_price_difference_bps=Decimal("50"))
    first = CandleCrossSourceReconciler().reconcile(
        comparison_key="btc-usd-spot-1m-binance-coinbase",
        primary_stream=primary,
        reference_stream=reference,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        primary_records=primary_records,
        reference_records=reference_records,
        policy=policy,
    )
    repeated = CandleCrossSourceReconciler().reconcile(
        comparison_key="btc-usd-spot-1m-binance-coinbase",
        primary_stream=primary,
        reference_stream=reference,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        primary_records=primary_records,
        reference_records=reference_records,
        policy=policy,
    )

    assert first == repeated
    assert first.status is CandleReconciliationStatus.DIVERGENT
    assert first.compared_count == 3
    assert [issue.code for issue in first.issues] == [
        CandleReconciliationIssueCode.CLOSE_PRICE_DIVERGENCE
    ]
    assert first.issues[0].open_time == WINDOW_START + timedelta(minutes=1)
