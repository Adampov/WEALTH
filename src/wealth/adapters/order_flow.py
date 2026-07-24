"""Safe in-memory adapter for canonical order-flow contract validation."""

from dataclasses import dataclass, field
from uuid import UUID

from wealth.domain.market import RawMarketPayload
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowConflictRecord,
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
    order_flow_record_type,
    order_flow_sort_key,
    order_flow_storage_key,
)
from wealth.domain.quality import RawPayloadWriteResult, RawPayloadWriteStatus
from wealth.ports.order_flow import OrderFlowFetchBatch


@dataclass(slots=True)
class InMemoryOrderFlowStore:
    """Idempotent temporary store that never replaces conflicting market data."""

    _records: dict[tuple[object, ...], OrderFlowRecord] = field(default_factory=dict)
    _raw_payloads: dict[UUID, RawMarketPayload] = field(default_factory=dict)
    _raw_lineage: dict[UUID, set[UUID]] = field(default_factory=dict)
    _conflicts: dict[tuple[UUID, UUID], OrderFlowConflictRecord] = field(default_factory=dict)

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Insert one record or report duplicate/conflict without mutation."""

        return self._append_record(record=record, raw_payload_id=None)

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        """Persist raw evidence first, then append canonical records safely."""

        raw_write = self._append_raw(batch.raw_payload)
        if raw_write.status is RawPayloadWriteStatus.CONFLICT:
            return OrderFlowBatchWriteResult(raw_payload=raw_write, records=())
        writes = tuple(
            self._append_record(
                record=record,
                raw_payload_id=batch.raw_payload.record_id,
            )
            for record in batch.records
        )
        return OrderFlowBatchWriteResult(raw_payload=raw_write, records=writes)

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        """Return one record family sorted deterministically by market time."""

        return tuple(
            sorted(
                (record for record in self._records.values() if stream.contains(record)),
                key=order_flow_sort_key,
            )
        )

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Return exact provider evidence by ID."""

        return self._raw_payloads.get(record_id)

    def raw_payload_ids_for_record(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one accepted canonical record."""

        return tuple(sorted(self._raw_lineage.get(record_id, set()), key=str))

    def conflicts_for_stream(
        self,
        stream: OrderFlowStream,
    ) -> tuple[OrderFlowConflictRecord, ...]:
        """Return quarantined revisions without promoting them to canonical data."""

        return tuple(
            sorted(
                (
                    conflict
                    for conflict in self._conflicts.values()
                    if stream.contains(conflict.incoming_record)
                ),
                key=lambda conflict: (
                    conflict.incoming_record.event_time,
                    str(conflict.incoming_record.record_id),
                ),
            )
        )

    def _append_record(
        self,
        *,
        record: OrderFlowRecord,
        raw_payload_id: UUID | None,
    ) -> OrderFlowWriteResult:
        storage_key = order_flow_storage_key(record)
        existing = self._records.get(storage_key)
        if existing is None:
            self._records[storage_key] = record
            if raw_payload_id is not None:
                self._link_raw(existing_record_id=record.record_id, raw_payload_id=raw_payload_id)
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
        if status is OrderFlowWriteStatus.DUPLICATE and raw_payload_id is not None:
            self._link_raw(
                existing_record_id=existing.record_id,
                raw_payload_id=raw_payload_id,
            )
        elif status is OrderFlowWriteStatus.CONFLICT:
            conflict = OrderFlowConflictRecord(
                stream=OrderFlowStream.from_record(record),
                existing_record_id=existing.record_id,
                incoming_record=record,
                raw_payload_id=raw_payload_id,
                detected_at=record.processed_at,
            )
            self._conflicts.setdefault(
                (existing.record_id, record.record_id),
                conflict,
            )
        return OrderFlowWriteResult(
            status=status,
            record_type=order_flow_record_type(record),
            incoming_record_id=record.record_id,
            existing_record_id=existing.record_id,
        )

    def _append_raw(self, payload: RawMarketPayload) -> RawPayloadWriteResult:
        existing = self._raw_payloads.get(payload.record_id)
        if existing is None:
            self._raw_payloads[payload.record_id] = payload
            return RawPayloadWriteResult(
                status=RawPayloadWriteStatus.INSERTED,
                incoming_record_id=payload.record_id,
            )

        status = (
            RawPayloadWriteStatus.DUPLICATE
            if existing.content_identity == payload.content_identity
            else RawPayloadWriteStatus.CONFLICT
        )
        return RawPayloadWriteResult(
            status=status,
            incoming_record_id=payload.record_id,
            existing_record_id=existing.record_id,
        )

    def _link_raw(self, *, existing_record_id: UUID, raw_payload_id: UUID) -> None:
        self._raw_lineage.setdefault(existing_record_id, set()).add(raw_payload_id)
