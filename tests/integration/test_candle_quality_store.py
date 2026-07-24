"""Integration tests for idempotent storage and sequence auditing."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from wealth.adapters.market import InMemoryCandleStore
from wealth.application.quality import CandleSequenceAuditor
from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType
from wealth.domain.quality import (
    CandleStream,
    CandleWriteStatus,
    DataQualityStatus,
)

OPEN_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def build_candle(record_id: UUID, *, close: str = "100") -> CanonicalCandle:
    """Build one valid canonical candle."""

    return CanonicalCandle(
        record_id=record_id,
        source="synthetic.integration",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        open_time=OPEN_TIME,
        close_time=OPEN_TIME + timedelta(minutes=1),
        observed_at=OPEN_TIME + timedelta(minutes=1, seconds=1),
        processed_at=OPEN_TIME + timedelta(minutes=1, seconds=2),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        lineage=("fixture:integration",),
    )


def test_store_is_idempotent_and_never_overwrites_conflicts() -> None:
    store = InMemoryCandleStore()
    original = build_candle(UUID(int=1))
    duplicate = build_candle(UUID(int=2))
    conflict = build_candle(UUID(int=3), close="101")
    stream = CandleStream.from_candle(original)

    inserted = store.append(original)
    repeated = store.append(duplicate)
    rejected = store.append(conflict)
    stored_records = store.records_for_stream(stream)
    report = CandleSequenceAuditor().audit(
        stream=stream,
        window_start=OPEN_TIME,
        window_end_exclusive=OPEN_TIME + timedelta(minutes=1),
        records=stored_records,
    )

    assert inserted.status is CandleWriteStatus.INSERTED
    assert repeated.status is CandleWriteStatus.DUPLICATE
    assert repeated.existing_record_id == original.record_id
    assert rejected.status is CandleWriteStatus.CONFLICT
    assert rejected.existing_record_id == original.record_id
    assert stored_records == (original,)
    assert report.status is DataQualityStatus.PASS
