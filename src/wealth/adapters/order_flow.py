"""Safe in-memory adapter for canonical order-flow contract validation."""

from dataclasses import dataclass, field

from wealth.domain.order_flow_quality import (
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
    order_flow_record_type,
    order_flow_sort_key,
    order_flow_storage_key,
)


@dataclass(slots=True)
class InMemoryOrderFlowStore:
    """Idempotent temporary store that never replaces conflicting market data."""

    _records: dict[tuple[object, ...], OrderFlowRecord] = field(default_factory=dict)

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Insert one record or report duplicate/conflict without mutation."""

        storage_key = order_flow_storage_key(record)
        existing = self._records.get(storage_key)
        if existing is None:
            self._records[storage_key] = record
            return OrderFlowWriteResult(
                status=OrderFlowWriteStatus.INSERTED,
                record_type=order_flow_record_type(record),
                incoming_record_id=record.record_id,
            )

        status = (
            OrderFlowWriteStatus.DUPLICATE
            if existing.market_values == record.market_values
            else OrderFlowWriteStatus.CONFLICT
        )
        return OrderFlowWriteResult(
            status=status,
            record_type=order_flow_record_type(record),
            incoming_record_id=record.record_id,
            existing_record_id=existing.record_id,
        )

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        """Return one record family sorted deterministically by market time."""

        return tuple(
            sorted(
                (record for record in self._records.values() if stream.contains(record)),
                key=order_flow_sort_key,
            )
        )
