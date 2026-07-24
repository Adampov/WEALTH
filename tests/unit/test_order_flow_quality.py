"""Tests for bounded, evidence-aware order-flow quality auditing."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import repeat
from uuid import UUID

import pytest

from wealth.application.order_flow_quality import (
    MAX_ORDER_FLOW_RECORDS,
    OrderFlowAuditError,
    OrderFlowAuditErrorCode,
    OrderFlowSequenceAuditor,
)
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow import AggressorSide, CanonicalBestBidAsk, CanonicalTrade
from wealth.domain.order_flow_quality import (
    OrderFlowQualityCode,
    OrderFlowRecord,
    OrderFlowSequenceReport,
    OrderFlowStream,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=10)


def build_trade(
    record_id: int,
    *,
    provider_trade_id: str | None = None,
    event_offset_seconds: int = 0,
    provider_sequence: int | None = 100,
    price: str = "100",
    source: str = "synthetic.test",
) -> CanonicalTrade:
    """Build one canonical trade observed after the complete audit window."""

    return CanonicalTrade(
        record_id=UUID(int=record_id),
        source=source,
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=WINDOW_START + timedelta(seconds=event_offset_seconds),
        observed_at=WINDOW_END,
        processed_at=WINDOW_END + timedelta(seconds=1),
        provider_sequence=provider_sequence,
        lineage=(f"fixture:trade:{record_id}",),
        provider_trade_id=provider_trade_id or f"trade-{record_id}",
        price=Decimal(price),
        base_quantity=Decimal("0.5"),
        quote_quantity=None,
        aggressor_side=AggressorSide.UNKNOWN,
    )


def build_best_bid_ask(record_id: int) -> CanonicalBestBidAsk:
    """Build another record family on the same provider and instrument."""

    return CanonicalBestBidAsk(
        record_id=UUID(int=record_id),
        source="synthetic.test",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=WINDOW_START,
        observed_at=WINDOW_END,
        processed_at=WINDOW_END + timedelta(seconds=1),
        provider_sequence=100,
        lineage=(f"fixture:bbo:{record_id}",),
        bid_price=Decimal("99"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("101"),
        ask_quantity=Decimal("2"),
    )


def audit(
    records: Iterable[OrderFlowRecord],
    *,
    sequence_policy: ProviderSequencePolicy = ProviderSequencePolicy.UNSPECIFIED,
) -> OrderFlowSequenceReport:
    """Audit a trade stream using the common bounded window."""

    first = build_trade(999)
    stream = OrderFlowStream.from_record(first, sequence_policy=sequence_policy)
    return OrderFlowSequenceAuditor().audit(
        stream=stream,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
        records=records,
    )


def test_ordered_contiguous_trade_stream_passes() -> None:
    report = audit(
        (
            build_trade(1, provider_sequence=100),
            build_trade(2, event_offset_seconds=1, provider_sequence=101),
        ),
        sequence_policy=ProviderSequencePolicy.CONTIGUOUS,
    )

    assert report.status is DataQualityStatus.PASS
    assert report.input_count == 2
    assert report.accepted_count == 2
    assert report.sequenced_count == 2
    assert report.issues == ()
    assert report.missing_sequence_ranges == ()


def test_gap_is_reported_only_when_the_provider_promises_contiguous_sequences() -> None:
    records = (
        build_trade(1, provider_sequence=100),
        build_trade(2, event_offset_seconds=1, provider_sequence=104),
    )

    unspecified = audit(records)
    contiguous = audit(records, sequence_policy=ProviderSequencePolicy.CONTIGUOUS)

    assert unspecified.status is DataQualityStatus.PASS
    assert unspecified.missing_sequence_ranges == ()
    assert contiguous.status is DataQualityStatus.FAIL
    assert contiguous.missing_sequence_ranges[0].start_sequence == 101
    assert contiguous.missing_sequence_ranges[0].end_sequence_inclusive == 103
    assert contiguous.missing_sequence_ranges[0].missing_count == 3


def test_promised_sequence_detects_absence_regression_and_reuse() -> None:
    report = audit(
        (
            build_trade(1, provider_sequence=10),
            build_trade(2, event_offset_seconds=1, provider_sequence=None),
            build_trade(3, event_offset_seconds=2, provider_sequence=9),
            build_trade(4, event_offset_seconds=3, provider_sequence=8),
            build_trade(5, event_offset_seconds=4, provider_sequence=8),
        ),
        sequence_policy=ProviderSequencePolicy.MONOTONIC,
    )

    codes = {issue.code for issue in report.issues}
    assert report.status is DataQualityStatus.FAIL
    assert report.sequenced_count == 4
    assert OrderFlowQualityCode.MISSING_SEQUENCE in codes
    assert OrderFlowQualityCode.SEQUENCE_REGRESSION in codes
    assert OrderFlowQualityCode.SEQUENCE_REUSE in codes
    assert report.missing_sequence_ranges == ()


def test_duplicate_and_conflict_are_distinguished_by_canonical_market_values() -> None:
    original = build_trade(1, provider_trade_id="same-trade")
    duplicate = build_trade(2, provider_trade_id="same-trade")
    conflict = build_trade(3, provider_trade_id="same-trade", price="101")

    duplicate_report = audit((original, duplicate))
    conflict_report = audit((original, conflict))

    assert duplicate_report.accepted_count == 1
    assert [issue.code for issue in duplicate_report.issues] == [OrderFlowQualityCode.DUPLICATE]
    assert conflict_report.accepted_count == 0
    assert [issue.code for issue in conflict_report.issues] == [OrderFlowQualityCode.CONFLICT]


def test_stream_window_and_input_order_are_checked_independently() -> None:
    report = audit(
        (
            build_trade(1, event_offset_seconds=2),
            build_trade(2, event_offset_seconds=1),
            build_trade(3, event_offset_seconds=600),
            build_trade(4, source="another.source"),
            build_best_bid_ask(5),
        )
    )

    codes = [issue.code for issue in report.issues]
    assert report.status is DataQualityStatus.FAIL
    assert report.accepted_count == 2
    assert OrderFlowQualityCode.OUT_OF_ORDER in codes
    assert OrderFlowQualityCode.OUT_OF_WINDOW in codes
    assert codes.count(OrderFlowQualityCode.MIXED_STREAM) == 2


@pytest.mark.parametrize(
    ("window_start", "window_end", "code"),
    [
        (
            WINDOW_START.replace(tzinfo=None),
            WINDOW_END,
            OrderFlowAuditErrorCode.NAIVE_WINDOW,
        ),
        (WINDOW_START, WINDOW_START, OrderFlowAuditErrorCode.INVALID_WINDOW),
    ],
)
def test_audit_rejects_ambiguous_windows(
    window_start: datetime,
    window_end: datetime,
    code: OrderFlowAuditErrorCode,
) -> None:
    stream = OrderFlowStream.from_record(build_trade(1))

    with pytest.raises(OrderFlowAuditError) as raised:
        OrderFlowSequenceAuditor().audit(
            stream=stream,
            window_start=window_start,
            window_end_exclusive=window_end,
            records=(),
        )

    assert raised.value.code is code


def test_audit_consumption_is_hard_bounded() -> None:
    stream = OrderFlowStream.from_record(build_trade(1))

    with pytest.raises(OrderFlowAuditError) as raised:
        OrderFlowSequenceAuditor().audit(
            stream=stream,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_END,
            records=repeat(build_trade(1), MAX_ORDER_FLOW_RECORDS + 1),
        )

    assert raised.value.code is OrderFlowAuditErrorCode.TOO_MANY_RECORDS
