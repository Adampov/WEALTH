"""Integration tests for durable raw and canonical order-flow storage."""

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_order_flow import (
    SQLiteOrderFlowStorageError,
    SQLiteOrderFlowStorageErrorCode,
    SQLiteOrderFlowStore,
)
from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow import (
    AggressorSide,
    CanonicalBestBidAsk,
    CanonicalTicker,
    CanonicalTrade,
)
from wealth.domain.order_flow_quality import (
    OrderFlowStream,
    OrderFlowWriteStatus,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus, RawPayloadWriteStatus
from wealth.ports.order_flow import OrderFlowFetchBatch, OrderFlowStore

EVENT_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
OBSERVED_AT = EVENT_TIME + timedelta(minutes=1)
PROCESSED_AT = OBSERVED_AT + timedelta(milliseconds=10)


def build_raw(*, raw_id: int, body: bytes) -> RawMarketPayload:
    """Build exact provider evidence for one storage batch."""

    return RawMarketPayload(
        record_id=UUID(int=raw_id),
        source="synthetic.public",
        venue="TEST",
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        payload_sha256=sha256(body).hexdigest(),
        payload=body,
        lineage=("synthetic-public:order-flow:BTCUSDT",),
    )


def build_trade_batch(
    *,
    raw_id: int = 1,
    record_id: int = 2,
    body: bytes = b'{"trade":"original"}',
    trade_id: str = "trade-1",
    price: str = "100",
    event_offset_seconds: int = 0,
) -> OrderFlowFetchBatch:
    """Build one raw response and canonical trade."""

    raw = build_raw(raw_id=raw_id, body=body)
    trade = CanonicalTrade(
        record_id=UUID(int=record_id),
        source=raw.source,
        venue=raw.venue,
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=EVENT_TIME + timedelta(seconds=event_offset_seconds),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        provider_sequence=100 + event_offset_seconds,
        lineage=(raw.lineage_reference,),
        provider_trade_id=trade_id,
        price=Decimal(price),
        base_quantity=Decimal("0.5"),
        quote_quantity=None,
        aggressor_side=AggressorSide.UNKNOWN,
    )
    return OrderFlowFetchBatch(
        stream=OrderFlowStream.from_record(
            trade,
            sequence_policy=ProviderSequencePolicy.CONTIGUOUS,
        ),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw,
        records=(trade,),
    )


def build_snapshot_batch(
    record: CanonicalTicker | CanonicalBestBidAsk,
    *,
    raw_id: int,
    body: bytes,
) -> OrderFlowFetchBatch:
    """Attach a canonical snapshot to matching raw evidence."""

    raw = build_raw(raw_id=raw_id, body=body)
    record = record.model_copy(update={"lineage": (raw.lineage_reference,)})
    return OrderFlowFetchBatch(
        stream=OrderFlowStream.from_record(record),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw,
        records=(record,),
    )


def build_ticker() -> CanonicalTicker:
    """Build a last-price-only canonical ticker."""

    return CanonicalTicker(
        record_id=UUID(int=20),
        source="synthetic.public",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=EVENT_TIME,
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        provider_sequence=None,
        lineage=("placeholder",),
        last_price=Decimal("100"),
    )


def build_best_bid_ask() -> CanonicalBestBidAsk:
    """Build one uncrossed top-of-book snapshot."""

    return CanonicalBestBidAsk(
        record_id=UUID(int=30),
        source="synthetic.public",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=EVENT_TIME,
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        provider_sequence=500,
        lineage=("placeholder",),
        bid_price=Decimal("99"),
        bid_quantity=Decimal("2"),
        ask_price=Decimal("101"),
        ask_quantity=Decimal("3"),
    )


def test_raw_and_trade_records_survive_restart_and_repeat_idempotently(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "order-flow.sqlite3"
    batch = build_trade_batch()
    first_store: OrderFlowStore = SQLiteOrderFlowStore(database_path)

    first = first_store.append_batch(batch)
    restarted_store: OrderFlowStore = SQLiteOrderFlowStore(database_path)
    repeated = restarted_store.append_batch(batch)
    stored = restarted_store.records_for_stream(batch.stream)
    report = OrderFlowSequenceAuditor().audit(
        stream=batch.stream,
        window_start=EVENT_TIME,
        window_end_exclusive=EVENT_TIME + timedelta(minutes=1),
        records=stored,
    )

    assert database_path.is_file()
    assert first.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert first.records[0].status is OrderFlowWriteStatus.INSERTED
    assert repeated.raw_payload.status is RawPayloadWriteStatus.DUPLICATE
    assert repeated.records[0].status is OrderFlowWriteStatus.DUPLICATE
    assert restarted_store.raw_payload(batch.raw_payload.record_id) == batch.raw_payload
    assert stored == batch.records
    assert restarted_store.raw_payload_ids_for_record(batch.records[0].record_id) == (
        batch.raw_payload.record_id,
    )
    assert report.status is DataQualityStatus.PASS


def test_conflicting_revision_is_quarantined_and_never_overwrites(
    tmp_path: Path,
) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_trade_batch()
    conflict = build_trade_batch(
        raw_id=3,
        record_id=4,
        body=b'{"trade":"changed"}',
        price="101",
    )

    store.append_batch(original)
    first_conflict = store.append_batch(conflict)
    repeated_conflict = store.append_batch(conflict)

    assert first_conflict.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert first_conflict.records[0].status is OrderFlowWriteStatus.CONFLICT
    assert repeated_conflict.raw_payload.status is RawPayloadWriteStatus.DUPLICATE
    assert repeated_conflict.records[0].status is OrderFlowWriteStatus.CONFLICT
    assert store.records_for_stream(original.stream) == original.records
    quarantined = store.conflicts_for_stream(original.stream)
    assert len(quarantined) == 1
    assert quarantined[0].incoming_record == conflict.records[0]
    assert quarantined[0].existing_record_id == original.records[0].record_id
    assert quarantined[0].raw_payload_id == conflict.raw_payload.record_id


def test_new_raw_capture_of_same_values_links_to_the_first_record(tmp_path: Path) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_trade_batch()
    equivalent = build_trade_batch(
        raw_id=5,
        record_id=6,
        body=b'{"trade":"equivalent-new-capture"}',
    )

    store.append_batch(original)
    result = store.append_batch(equivalent)

    assert result.raw_payload.status is RawPayloadWriteStatus.INSERTED
    assert result.records[0].status is OrderFlowWriteStatus.DUPLICATE
    assert store.records_for_stream(original.stream) == original.records
    assert store.raw_payload_ids_for_record(original.records[0].record_id) == (
        original.raw_payload.record_id,
        equivalent.raw_payload.record_id,
    )
    assert store.conflicts_for_stream(original.stream) == ()


def test_raw_identity_conflict_blocks_canonical_writes(tmp_path: Path) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_trade_batch()
    invalid_reuse = build_trade_batch(
        raw_id=1,
        record_id=7,
        body=b'{"different":"bytes-same-id"}',
        trade_id="trade-2",
    )

    store.append_batch(original)
    result = store.append_batch(invalid_reuse)

    assert result.raw_payload.status is RawPayloadWriteStatus.CONFLICT
    assert result.records == ()
    assert store.records_for_stream(invalid_reuse.stream) == original.records


def test_all_canonical_record_families_round_trip_in_isolated_streams(
    tmp_path: Path,
) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    trade_batch = build_trade_batch()
    ticker_batch = build_snapshot_batch(build_ticker(), raw_id=20, body=b'{"ticker":1}')
    quote_batch = build_snapshot_batch(
        build_best_bid_ask(),
        raw_id=30,
        body=b'{"best-bid-ask":1}',
    )

    for batch in (trade_batch, ticker_batch, quote_batch):
        store.append_batch(batch)

    assert store.records_for_stream(trade_batch.stream) == trade_batch.records
    assert store.records_for_stream(ticker_batch.stream) == ticker_batch.records
    assert store.records_for_stream(quote_batch.stream) == quote_batch.records


def test_reused_canonical_record_id_for_another_key_fails_explicitly(
    tmp_path: Path,
) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_trade_batch()
    reused_id = build_trade_batch(
        raw_id=8,
        record_id=2,
        body=b'{"trade":"another-key"}',
        trade_id="trade-2",
    )
    store.append_batch(original)

    with pytest.raises(SQLiteOrderFlowStorageError) as raised:
        store.append_batch(reused_id)

    assert raised.value.code is SQLiteOrderFlowStorageErrorCode.IDENTITY_CONFLICT
    assert store.records_for_stream(original.stream) == original.records
    assert store.raw_payload(reused_id.raw_payload.record_id) is None


@pytest.mark.parametrize("target", ["raw", "canonical"])
def test_stored_evidence_is_revalidated_when_reloaded(
    tmp_path: Path,
    target: str,
) -> None:
    database_path = tmp_path / f"tampered-{target}.sqlite3"
    batch = build_trade_batch()
    store = SQLiteOrderFlowStore(database_path)
    store.append_batch(batch)
    with sqlite3.connect(database_path) as connection:
        if target == "raw":
            connection.execute(
                "UPDATE raw_order_flow_payloads SET payload = ? WHERE record_id = ?",
                (b"tampered", str(batch.raw_payload.record_id)),
            )
        else:
            connection.execute(
                "UPDATE canonical_order_flow_records SET record_json = ? WHERE record_id = ?",
                ("{}", str(batch.records[0].record_id)),
            )

    with pytest.raises(SQLiteOrderFlowStorageError) as raised:
        if target == "raw":
            store.raw_payload(batch.raw_payload.record_id)
        else:
            store.records_for_stream(batch.stream)

    assert raised.value.code is SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD


def test_unknown_schema_version_is_rejected_without_migration(tmp_path: Path) -> None:
    database_path = tmp_path / "future.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(SQLiteOrderFlowStorageError) as raised:
        SQLiteOrderFlowStore(database_path)

    assert raised.value.code is SQLiteOrderFlowStorageErrorCode.UNSUPPORTED_SCHEMA


def test_same_version_database_of_another_type_is_rejected(tmp_path: Path) -> None:
    database_path = tmp_path / "another-store.sqlite3"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE unrelated (value TEXT)")
        connection.execute("PRAGMA user_version = 1")

    with pytest.raises(SQLiteOrderFlowStorageError) as raised:
        SQLiteOrderFlowStore(database_path)

    assert raised.value.code is SQLiteOrderFlowStorageErrorCode.UNSUPPORTED_SCHEMA
