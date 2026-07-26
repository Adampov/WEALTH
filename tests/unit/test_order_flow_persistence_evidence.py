"""Fail-closed checks for exact order-flow persistence evidence."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest

from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.application.order_flow_ingestion import (
    OrderFlowBatchIngestor,
    OrderFlowIngestionResult,
)
from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow import AggressorSide, CanonicalTrade
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowRecordType,
    OrderFlowStream,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus, RawPayloadWriteStatus
from wealth.ports.order_flow import OrderFlowFetchBatch

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
OBSERVED_AT = WINDOW_END
PROCESSED_AT = OBSERVED_AT + timedelta(milliseconds=10)


class ScriptedOrderFlowStore(InMemoryOrderFlowStore):
    """Return one configured batch outcome without repairing it."""

    def __init__(self, persistence: OrderFlowBatchWriteResult) -> None:
        super().__init__()
        self.persistence = persistence
        self.appended_batches: list[OrderFlowFetchBatch] = []

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        self.appended_batches.append(batch)
        return self.persistence


def build_batch(
    *,
    raw_id: int = 1,
    first_record_id: int = 10,
    empty: bool = False,
) -> OrderFlowFetchBatch:
    """Build one exact raw response and zero or two canonical trades."""

    body = b"[]" if empty else b'[{"trade":"one"},{"trade":"two"}]'
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
    records = (
        ()
        if empty
        else tuple(
            CanonicalTrade(
                record_id=UUID(int=first_record_id + index),
                source=raw.source,
                venue=raw.venue,
                instrument="BTC-USDT",
                instrument_type=InstrumentType.SPOT,
                event_time=WINDOW_START + timedelta(seconds=index),
                observed_at=OBSERVED_AT,
                processed_at=PROCESSED_AT,
                provider_sequence=100 + index,
                lineage=(raw.lineage_reference,),
                provider_trade_id=f"trade-{index + 1}",
                price=Decimal(100 + index),
                base_quantity=Decimal("0.5"),
                quote_quantity=None,
                aggressor_side=AggressorSide.UNKNOWN,
            )
            for index in range(2)
        )
    )
    return OrderFlowFetchBatch(
        stream=OrderFlowStream(
            source=raw.source,
            venue=raw.venue,
            instrument="BTC-USDT",
            instrument_type=InstrumentType.SPOT,
            record_type=OrderFlowRecordType.TRADE,
            sequence_policy=ProviderSequencePolicy.CONTIGUOUS,
        ),
        observed_at=OBSERVED_AT,
        processed_at=PROCESSED_AT,
        raw_payload=raw,
        records=records,
    )


def valid_results(
    batch: OrderFlowFetchBatch | None = None,
) -> tuple[OrderFlowIngestionResult, OrderFlowIngestionResult]:
    """Return exact inserted and idempotent duplicate results."""

    selected_batch = build_batch() if batch is None else batch
    ingestor = OrderFlowBatchIngestor(store=InMemoryOrderFlowStore())
    return (
        ingestor.ingest(
            selected_batch,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_END,
        ),
        ingestor.ingest(
            selected_batch,
            window_start=WINDOW_START,
            window_end_exclusive=WINDOW_END,
        ),
    )


def scripted_result(
    persistence: OrderFlowBatchWriteResult,
    *,
    batch: OrderFlowFetchBatch | None = None,
) -> OrderFlowIngestionResult:
    """Pass one hostile store result through the real ingestion boundary."""

    selected_batch = build_batch() if batch is None else batch
    store = ScriptedOrderFlowStore(persistence)
    result = OrderFlowBatchIngestor(store=store).ingest(
        selected_batch,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )

    assert result.quality.status is DataQualityStatus.PASS
    assert result.raw_write == persistence.raw_payload
    assert result.writes == persistence.records
    assert store.appended_batches == [selected_batch]
    return result


def persistence_from(result: OrderFlowIngestionResult) -> OrderFlowBatchWriteResult:
    """Rebuild the store boundary result retained by an ingestion result."""

    raw_write = result.raw_write
    assert raw_write is not None
    return OrderFlowBatchWriteResult(raw_payload=raw_write, records=result.writes)


def test_exact_inserted_duplicate_and_mixed_evidence_is_accepted() -> None:
    inserted, duplicate = valid_results()
    raw_write = inserted.raw_write
    assert raw_write is not None
    second_write = inserted.writes[1].model_copy(
        update={
            "status": OrderFlowWriteStatus.DUPLICATE,
            "existing_record_id": UUID(int=800),
        }
    )
    mixed = scripted_result(
        OrderFlowBatchWriteResult(
            raw_payload=raw_write,
            records=(inserted.writes[0], second_write),
        )
    )

    assert inserted.accepted is True
    assert duplicate.accepted is True
    assert mixed.accepted is True


def test_valid_empty_batch_is_accepted_on_insert_and_replay() -> None:
    batch = build_batch(raw_id=2, empty=True)
    inserted, duplicate = valid_results(batch)

    assert inserted.accepted is True
    assert inserted.writes == ()
    assert duplicate.accepted is True
    assert duplicate.writes == ()


def test_empty_batch_still_requires_exact_raw_evidence() -> None:
    batch = build_batch(raw_id=2, empty=True)
    inserted, _ = valid_results(batch)
    raw_write = inserted.raw_write
    assert raw_write is not None
    wrong_raw = raw_write.model_copy(update={"incoming_record_id": UUID(int=999)})

    malformed = scripted_result(
        OrderFlowBatchWriteResult(raw_payload=wrong_raw, records=()),
        batch=batch,
    )

    assert malformed.accepted is False


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "short",
        "extra",
        "duplicated",
        "reordered",
        "wrong-id",
        "wrong-family",
    ],
)
def test_missing_extra_or_misattributed_record_evidence_is_rejected(case: str) -> None:
    result, _ = valid_results()
    raw_write = result.raw_write
    assert raw_write is not None
    first, second = result.writes
    writes: tuple[OrderFlowWriteResult, ...]

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
    elif case == "wrong-family":
        writes = (
            first,
            second.model_copy(update={"record_type": OrderFlowRecordType.TICKER}),
        )
    else:
        raise AssertionError(f"unhandled test case: {case}")

    malformed = scripted_result(OrderFlowBatchWriteResult(raw_payload=raw_write, records=writes))

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
    persistence = persistence_from(result)
    raw_write = persistence.raw_payload

    if case == "missing":
        malformed_persistence = persistence.model_copy(update={"raw_payload": None})
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
        malformed_persistence = OrderFlowBatchWriteResult(
            raw_payload=malformed_raw,
            records=() if case == "conflict-without-writes" else result.writes,
        )

    malformed = scripted_result(malformed_persistence)

    assert malformed.accepted is False


@pytest.mark.parametrize(
    ("status", "existing_record_id"),
    [
        pytest.param(OrderFlowWriteStatus.INSERTED, UUID(int=904), id="inserted-with-existing"),
        pytest.param(OrderFlowWriteStatus.DUPLICATE, None, id="duplicate-without-existing"),
        pytest.param(OrderFlowWriteStatus.CONFLICT, UUID(int=905), id="coherent-conflict"),
        pytest.param(OrderFlowWriteStatus.CONFLICT, None, id="conflict-without-existing"),
    ],
)
def test_contradictory_or_conflicting_record_status_is_rejected(
    status: OrderFlowWriteStatus,
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
    persistence = OrderFlowBatchWriteResult(
        raw_payload=raw_write,
        records=result.writes,
    ).model_copy(update={"records": (malformed_first, result.writes[1])})
    malformed = scripted_result(persistence)

    assert malformed.accepted is False
