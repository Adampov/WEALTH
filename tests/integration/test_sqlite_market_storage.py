"""Integration tests for durable raw and canonical market-data storage."""

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_market import (
    SQLiteCandleStore,
    SQLiteMarketStorageError,
    SQLiteMarketStorageErrorCode,
)
from wealth.domain.market import (
    CandleTimeframe,
    CanonicalCandle,
    InstrumentType,
    RawMarketPayload,
)
from wealth.domain.quality import (
    CandleStream,
    CandleWriteStatus,
    RawPayloadWriteStatus,
)
from wealth.ports.market import CandleFetchBatch, HistoricalCandleRequest

OPEN_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
OBSERVED_AT = OPEN_TIME + timedelta(minutes=1, seconds=1)
PROCESSED_AT = OBSERVED_AT + timedelta(seconds=1)


def build_batch(
    *,
    raw_id: int = 1,
    candle_id: int = 2,
    body: bytes = b'[["provider","evidence"]]',
    close: str = "102",
) -> CandleFetchBatch:
    """Build one validated raw response and derived canonical candle."""

    raw_payload = RawMarketPayload(
        record_id=UUID(int=raw_id),
        source="synthetic.public-rest",
        venue="TEST",
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        payload_sha256=sha256(body).hexdigest(),
        payload=body,
        lineage=("synthetic-public-rest:klines:BTCUSDT:1m",),
    )
    request = HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=OPEN_TIME,
        window_end_exclusive=OPEN_TIME + timedelta(minutes=1),
    )
    candle = CanonicalCandle(
        record_id=UUID(int=candle_id),
        source=raw_payload.source,
        venue=raw_payload.venue,
        instrument=request.instrument,
        instrument_type=request.instrument_type,
        timeframe=request.timeframe,
        open_time=OPEN_TIME,
        close_time=OPEN_TIME + timedelta(minutes=1),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("5"),
        quote_volume=Decimal("510"),
        trade_count=12,
        provider_sequence=1,
        lineage=(raw_payload.lineage_reference, "synthetic-public-rest:kline:1"),
    )
    return CandleFetchBatch(
        request=request,
        source=raw_payload.source,
        venue=raw_payload.venue,
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw_payload,
        records=(candle,),
    )


def stream() -> CandleStream:
    """Return the canonical stream stored by the fixtures."""

    return CandleStream(
        source="synthetic.public-rest",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def test_raw_and_canonical_records_survive_restart_and_repeat_idempotently(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "market-data.sqlite3"
    batch = build_batch()
    first_store = SQLiteCandleStore(database_path)

    first = first_store.append_batch(batch)
    restarted_store = SQLiteCandleStore(database_path)
    repeated = restarted_store.append_batch(batch)

    assert database_path.is_file()
    assert first.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert first.candles[0].status is CandleWriteStatus.INSERTED
    assert repeated.raw_payload.status is RawPayloadWriteStatus.DUPLICATE
    assert repeated.candles[0].status is CandleWriteStatus.DUPLICATE
    assert restarted_store.raw_payload(batch.raw_payload.record_id) == batch.raw_payload
    assert restarted_store.records_for_stream(stream()) == batch.records
    assert restarted_store.raw_payload_ids_for_candle(batch.records[0].record_id) == (
        batch.raw_payload.record_id,
    )


def test_conflicting_revision_is_quarantined_and_never_overwrites(
    tmp_path: Path,
) -> None:
    store = SQLiteCandleStore(tmp_path / "market-data.sqlite3")
    original = build_batch()
    conflict = build_batch(
        raw_id=3,
        candle_id=4,
        body=b'[["changed","provider","evidence"]]',
        close="103",
    )

    store.append_batch(original)
    first_conflict = store.append_batch(conflict)
    repeated_conflict = store.append_batch(conflict)

    assert first_conflict.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert first_conflict.candles[0].status is CandleWriteStatus.CONFLICT
    assert repeated_conflict.raw_payload.status is RawPayloadWriteStatus.DUPLICATE
    assert repeated_conflict.candles[0].status is CandleWriteStatus.CONFLICT
    assert store.records_for_stream(stream()) == original.records
    assert store.raw_payload(conflict.raw_payload.record_id) == conflict.raw_payload
    quarantined = store.conflicts_for_stream(stream())
    assert len(quarantined) == 1
    assert quarantined[0].incoming_candle == conflict.records[0]
    assert quarantined[0].existing_record_id == original.records[0].record_id


def test_new_raw_capture_of_same_canonical_values_is_not_a_conflict(
    tmp_path: Path,
) -> None:
    store = SQLiteCandleStore(tmp_path / "market-data.sqlite3")
    original = build_batch()
    equivalent_capture = build_batch(
        raw_id=6,
        candle_id=7,
        body=b'[["same","canonical","values","new","capture"]]',
    )

    store.append_batch(original)
    result = store.append_batch(equivalent_capture)

    assert result.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert result.candles[0].status is CandleWriteStatus.DUPLICATE
    assert store.raw_payload(equivalent_capture.raw_payload.record_id) is not None
    assert store.records_for_stream(stream()) == original.records
    assert store.raw_payload_ids_for_candle(original.records[0].record_id) == (
        original.raw_payload.record_id,
        equivalent_capture.raw_payload.record_id,
    )
    assert store.conflicts_for_stream(stream()) == ()


def test_raw_record_identity_conflict_blocks_canonical_writes(tmp_path: Path) -> None:
    store = SQLiteCandleStore(tmp_path / "market-data.sqlite3")
    original = build_batch()
    invalid_reuse = build_batch(
        raw_id=1,
        candle_id=5,
        body=b'[["different","bytes","same","id"]]',
    )

    store.append_batch(original)
    result = store.append_batch(invalid_reuse)

    assert result.raw_payload.status is RawPayloadWriteStatus.CONFLICT
    assert result.candles == ()
    assert store.raw_payload(original.raw_payload.record_id) == original.raw_payload
    assert store.records_for_stream(stream()) == original.records


def test_stored_raw_bytes_are_revalidated_when_reloaded(tmp_path: Path) -> None:
    database_path = tmp_path / "market-data.sqlite3"
    batch = build_batch()
    store = SQLiteCandleStore(database_path)
    store.append_batch(batch)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE raw_market_payloads SET payload = ? WHERE record_id = ?",
            (b"tampered", str(batch.raw_payload.record_id)),
        )

    with pytest.raises(SQLiteMarketStorageError) as error:
        store.raw_payload(batch.raw_payload.record_id)

    assert error.value.code is SQLiteMarketStorageErrorCode.CORRUPT_RECORD


def test_unknown_schema_version_is_rejected_without_migration(tmp_path: Path) -> None:
    database_path = tmp_path / "future.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(SQLiteMarketStorageError) as error:
        SQLiteCandleStore(database_path)

    assert error.value.code is SQLiteMarketStorageErrorCode.UNSUPPORTED_SCHEMA
