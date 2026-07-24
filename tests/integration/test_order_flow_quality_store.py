"""Integration tests for order-flow quality and idempotent temporary storage."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow import AggressorSide, CanonicalBestBidAsk, CanonicalTrade
from wealth.domain.order_flow_quality import (
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteStatus,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus
from wealth.ports.order_flow import OrderFlowStore

EVENT_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def build_trade(
    record_id: int,
    *,
    trade_id: str,
    event_offset_seconds: int = 0,
    price: str = "100",
) -> CanonicalTrade:
    """Build a canonical trade for the in-memory storage boundary."""

    event_time = EVENT_TIME + timedelta(seconds=event_offset_seconds)
    return CanonicalTrade(
        record_id=UUID(int=record_id),
        source="synthetic.integration",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=event_time,
        observed_at=event_time + timedelta(milliseconds=10),
        processed_at=event_time + timedelta(milliseconds=20),
        provider_sequence=100 + event_offset_seconds,
        lineage=(f"fixture:trade:{record_id}",),
        provider_trade_id=trade_id,
        price=Decimal(price),
        base_quantity=Decimal("1"),
        quote_quantity=None,
        aggressor_side=AggressorSide.BUY,
    )


def build_best_bid_ask() -> CanonicalBestBidAsk:
    """Build another family to prove exact stream isolation."""

    return CanonicalBestBidAsk(
        record_id=UUID(int=20),
        source="synthetic.integration",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=EVENT_TIME,
        observed_at=EVENT_TIME + timedelta(milliseconds=10),
        processed_at=EVENT_TIME + timedelta(milliseconds=20),
        provider_sequence=100,
        lineage=("fixture:bbo:20",),
        bid_price=Decimal("99"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("101"),
        ask_quantity=Decimal("1"),
    )


def test_store_is_idempotent_never_overwrites_and_returns_a_quality_ready_stream() -> None:
    store: OrderFlowStore = InMemoryOrderFlowStore()
    later = build_trade(1, trade_id="later", event_offset_seconds=1)
    original = build_trade(2, trade_id="original")
    duplicate = build_trade(3, trade_id="original")
    conflict = build_trade(4, trade_id="original", price="101")
    trade_stream = OrderFlowStream.from_record(
        original,
        sequence_policy=ProviderSequencePolicy.CONTIGUOUS,
    )

    assert store.append(later).status is OrderFlowWriteStatus.INSERTED
    assert store.append(original).status is OrderFlowWriteStatus.INSERTED
    repeated = store.append(duplicate)
    rejected = store.append(conflict)
    store.append(build_best_bid_ask())

    stored_records = store.records_for_stream(trade_stream)
    report = OrderFlowSequenceAuditor().audit(
        stream=trade_stream,
        window_start=EVENT_TIME,
        window_end_exclusive=EVENT_TIME + timedelta(minutes=1),
        records=stored_records,
    )

    assert repeated.status is OrderFlowWriteStatus.DUPLICATE
    assert repeated.existing_record_id == original.record_id
    assert rejected.status is OrderFlowWriteStatus.CONFLICT
    assert rejected.existing_record_id == original.record_id
    assert stored_records == (original, later)
    assert all(isinstance(record, CanonicalTrade) for record in stored_records)
    assert report.status is DataQualityStatus.PASS


def test_each_record_family_has_an_independent_storage_namespace() -> None:
    store = InMemoryOrderFlowStore()
    trade = build_trade(1, trade_id="trade")
    quote = build_best_bid_ask()

    store.append(trade)
    store.append(quote)

    trade_records: tuple[OrderFlowRecord, ...] = store.records_for_stream(
        OrderFlowStream.from_record(trade)
    )
    quote_records: tuple[OrderFlowRecord, ...] = store.records_for_stream(
        OrderFlowStream.from_record(quote)
    )
    assert trade_records == (trade,)
    assert quote_records == (quote,)
