"""Tests for raw-to-canonical order-flow batch evidence."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow import AggressorSide, CanonicalTrade
from wealth.domain.order_flow_quality import OrderFlowStream
from wealth.ports.order_flow import OrderFlowFetchBatch

EVENT_TIME = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
OBSERVED_AT = EVENT_TIME + timedelta(seconds=1)
PROCESSED_AT = OBSERVED_AT + timedelta(milliseconds=10)


def build_raw(*, raw_id: int = 1, source: str = "synthetic.public") -> RawMarketPayload:
    """Build exact provider bytes for one order-flow capture."""

    payload = b'{"trade":"evidence"}'
    return RawMarketPayload(
        record_id=UUID(int=raw_id),
        source=source,
        venue="TEST",
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        payload_sha256=sha256(payload).hexdigest(),
        payload=payload,
        lineage=("synthetic-public:trades:BTCUSDT",),
    )


def build_trade(
    raw: RawMarketPayload,
    *,
    source: str | None = None,
    observed_at: datetime = OBSERVED_AT,
    lineage: tuple[str, ...] | None = None,
) -> CanonicalTrade:
    """Build one canonical record derived from the supplied raw evidence."""

    return CanonicalTrade(
        record_id=UUID(int=2),
        source=source or raw.source,
        venue=raw.venue,
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        event_time=EVENT_TIME,
        observed_at=observed_at,
        processed_at=PROCESSED_AT,
        provider_sequence=100,
        lineage=lineage or (raw.lineage_reference,),
        provider_trade_id="trade-1",
        price=Decimal("100"),
        base_quantity=Decimal("0.5"),
        quote_quantity=Decimal("50"),
        aggressor_side=AggressorSide.BUY,
    )


def test_batch_binds_raw_evidence_to_one_exact_canonical_stream() -> None:
    raw = build_raw()
    trade = build_trade(raw)

    batch = OrderFlowFetchBatch(
        stream=OrderFlowStream.from_record(trade),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw,
        records=(trade,),
    )

    assert batch.raw_payload == raw
    assert batch.records == (trade,)
    assert OrderFlowFetchBatch.model_validate_json(batch.model_dump_json()) == batch


@pytest.mark.parametrize(
    ("raw", "trade", "message"),
    [
        (
            build_raw(source="other.source"),
            build_trade(build_raw()),
            "raw payload identity and timestamps must match",
        ),
        (
            build_raw(),
            build_trade(build_raw(), source="other.source"),
            "batch records must belong",
        ),
        (
            build_raw(),
            build_trade(
                build_raw(),
                observed_at=OBSERVED_AT + timedelta(milliseconds=1),
            ),
            "batch timestamps must match",
        ),
        (
            build_raw(),
            build_trade(build_raw(), lineage=("unrelated:evidence",)),
            "must reference the batch raw payload",
        ),
    ],
)
def test_batch_rejects_mislabeled_or_untraceable_records(
    raw: RawMarketPayload,
    trade: CanonicalTrade,
    message: str,
) -> None:
    stream = OrderFlowStream(
        source="synthetic.public",
        venue="TEST",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        record_type=OrderFlowStream.from_record(build_trade(build_raw())).record_type,
    )

    with pytest.raises(ValidationError, match=message):
        OrderFlowFetchBatch(
            stream=stream,
            observed_at=OBSERVED_AT,
            processed_at=PROCESSED_AT,
            raw_payload=raw,
            records=(trade,),
        )
