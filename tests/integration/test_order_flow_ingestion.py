"""Integration tests for order-flow quality gating before storage."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.adapters.sqlite_order_flow import SQLiteOrderFlowStore
from wealth.application.order_flow_ingestion import OrderFlowBatchIngestor
from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow import AggressorSide, CanonicalTrade
from wealth.domain.order_flow_quality import (
    OrderFlowQualityCode,
    OrderFlowStream,
    OrderFlowWriteStatus,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus, RawPayloadWriteStatus
from wealth.ports.order_flow import OrderFlowFetchBatch

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
OBSERVED_AT = WINDOW_END
PROCESSED_AT = OBSERVED_AT + timedelta(milliseconds=10)


def build_batch(
    *,
    raw_id: int = 1,
    first_record_id: int = 10,
    body: bytes = b'{"trades":"original"}',
    trade_ids: tuple[str, ...] = ("trade-1", "trade-2"),
    sequences: tuple[int | None, ...] = (100, 101),
    event_offsets: tuple[int, ...] = (0, 1),
    prices: tuple[str, ...] = ("100", "101"),
    sequence_policy: ProviderSequencePolicy = ProviderSequencePolicy.CONTIGUOUS,
) -> OrderFlowFetchBatch:
    """Build one exact raw capture and canonical trade sequence."""

    raw = RawMarketPayload(
        record_id=UUID(int=raw_id),
        source="synthetic.public",
        venue="TEST",
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        payload_sha256=sha256(body).hexdigest(),
        payload=body,
        lineage=("synthetic-public:trades:BTCUSDT",),
    )
    records = tuple(
        CanonicalTrade(
            record_id=UUID(int=first_record_id + index),
            source=raw.source,
            venue=raw.venue,
            instrument="BTC-USDT",
            instrument_type=InstrumentType.SPOT,
            event_time=WINDOW_START + timedelta(seconds=event_offsets[index]),
            observed_at=OBSERVED_AT,
            processed_at=PROCESSED_AT,
            provider_sequence=sequences[index],
            lineage=(raw.lineage_reference,),
            provider_trade_id=trade_id,
            price=Decimal(prices[index]),
            base_quantity=Decimal("0.5"),
            quote_quantity=None,
            aggressor_side=AggressorSide.UNKNOWN,
        )
        for index, trade_id in enumerate(trade_ids)
    )
    return OrderFlowFetchBatch(
        stream=OrderFlowStream.from_record(
            records[0],
            sequence_policy=sequence_policy,
        ),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw,
        records=records,
    )


def test_passing_batch_is_stored_and_repeated_idempotently() -> None:
    store = InMemoryOrderFlowStore()
    ingestor = OrderFlowBatchIngestor(store=store)
    batch = build_batch()

    first = ingestor.ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )
    repeated = ingestor.ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert first.accepted is True
    assert first.quality.status is DataQualityStatus.PASS
    assert first.raw_write is not None
    assert first.raw_write.status is RawPayloadWriteStatus.INSERTED
    assert [write.status for write in first.writes] == [
        OrderFlowWriteStatus.INSERTED,
        OrderFlowWriteStatus.INSERTED,
    ]
    assert repeated.accepted is True
    assert repeated.raw_write is not None
    assert repeated.raw_write.status is RawPayloadWriteStatus.DUPLICATE
    assert all(write.status is OrderFlowWriteStatus.DUPLICATE for write in repeated.writes)
    assert store.records_for_stream(batch.stream) == batch.records


def test_sequence_gap_fails_quality_and_does_not_store_raw_or_canonical_data() -> None:
    store = InMemoryOrderFlowStore()
    batch = build_batch(sequences=(100, 103))

    result = OrderFlowBatchIngestor(store=store).ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.accepted is False
    assert result.quality.status is DataQualityStatus.FAIL
    assert result.quality.missing_sequence_ranges[0].missing_count == 2
    assert result.raw_write is None
    assert result.writes == ()
    assert store.raw_payload(batch.raw_payload.record_id) is None
    assert store.records_for_stream(batch.stream) == ()


def test_duplicate_identity_fails_quality_before_storage() -> None:
    store = InMemoryOrderFlowStore()
    batch = build_batch(
        trade_ids=("trade-1", "trade-1"),
        sequences=(100, 100),
        event_offsets=(0, 0),
        prices=("100", "100"),
    )

    result = OrderFlowBatchIngestor(store=store).ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.accepted is False
    assert [issue.code for issue in result.quality.issues] == [OrderFlowQualityCode.DUPLICATE]
    assert store.raw_payload(batch.raw_payload.record_id) is None


def test_passing_batch_survives_sqlite_restart(tmp_path: Path) -> None:
    database_path = tmp_path / "order-flow.sqlite3"
    batch = build_batch()

    result = OrderFlowBatchIngestor(
        store=SQLiteOrderFlowStore(database_path),
    ).ingest(
        batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )
    restarted = SQLiteOrderFlowStore(database_path)

    assert result.accepted is True
    assert restarted.raw_payload(batch.raw_payload.record_id) == batch.raw_payload
    assert restarted.records_for_stream(batch.stream) == batch.records


def test_existing_storage_conflict_is_quarantined_and_result_is_unaccepted(
    tmp_path: Path,
) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_batch()
    changed = build_batch(
        raw_id=2,
        first_record_id=20,
        body=b'{"trades":"changed"}',
        prices=("102", "101"),
    )
    ingestor = OrderFlowBatchIngestor(store=store)
    ingestor.ingest(
        original,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    result = ingestor.ingest(
        changed,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.quality.status is DataQualityStatus.PASS
    assert result.accepted is False
    assert [write.status for write in result.writes] == [
        OrderFlowWriteStatus.CONFLICT,
        OrderFlowWriteStatus.DUPLICATE,
    ]
    assert store.records_for_stream(original.stream) == original.records
    assert len(store.conflicts_for_stream(original.stream)) == 1


def test_raw_identity_conflict_blocks_all_canonical_writes(tmp_path: Path) -> None:
    store = SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3")
    original = build_batch()
    reused_raw_id = build_batch(
        raw_id=1,
        first_record_id=30,
        body=b'{"trades":"different-raw-same-id"}',
        trade_ids=("trade-3", "trade-4"),
    )
    ingestor = OrderFlowBatchIngestor(store=store)
    ingestor.ingest(
        original,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    result = ingestor.ingest(
        reused_raw_id,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.quality.status is DataQualityStatus.PASS
    assert result.accepted is False
    assert result.raw_write is not None
    assert result.raw_write.status is RawPayloadWriteStatus.CONFLICT
    assert result.writes == ()
    assert store.records_for_stream(original.stream) == original.records
