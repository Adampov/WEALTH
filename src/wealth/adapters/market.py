"""Safe in-memory market-data adapters for contract validation."""

from dataclasses import dataclass, field
from uuid import UUID

from wealth.domain.market import CanonicalCandle, RawMarketPayload
from wealth.domain.quality import (
    CandleConflictRecord,
    CandleStream,
    CandleWriteResult,
    CandleWriteStatus,
    MarketDataBatchWriteResult,
    RawPayloadWriteResult,
    RawPayloadWriteStatus,
)
from wealth.ports.market import CandleFetchBatch


@dataclass(slots=True)
class InMemoryCandleStore:
    """Idempotent store that never replaces conflicting canonical data."""

    _records: dict[tuple[object, ...], CanonicalCandle] = field(default_factory=dict)
    _raw_payloads: dict[UUID, RawMarketPayload] = field(default_factory=dict)
    _raw_lineage: dict[UUID, set[UUID]] = field(default_factory=dict)
    _conflicts: dict[tuple[UUID, UUID], CandleConflictRecord] = field(default_factory=dict)

    def append(self, candle: CanonicalCandle) -> CandleWriteResult:
        """Insert one candle or report duplicate/conflict without mutation."""

        existing = self._records.get(candle.natural_key)
        if existing is None:
            self._records[candle.natural_key] = candle
            return CandleWriteResult(
                status=CandleWriteStatus.INSERTED,
                incoming_record_id=candle.record_id,
            )

        status = (
            CandleWriteStatus.DUPLICATE
            if existing.market_values == candle.market_values
            else CandleWriteStatus.CONFLICT
        )
        return CandleWriteResult(
            status=status,
            incoming_record_id=candle.record_id,
            existing_record_id=existing.record_id,
        )

    def append_batch(self, batch: CandleFetchBatch) -> MarketDataBatchWriteResult:
        """Persist raw evidence first, then append canonical records safely."""

        raw_write = self._append_raw(batch.raw_payload)
        if raw_write.status is RawPayloadWriteStatus.CONFLICT:
            return MarketDataBatchWriteResult(raw_payload=raw_write, candles=())

        candle_writes: list[CandleWriteResult] = []
        for candle in batch.records:
            write = self.append(candle)
            candle_writes.append(write)
            if write.status is not CandleWriteStatus.CONFLICT:
                canonical_record_id = write.existing_record_id or candle.record_id
                self._raw_lineage.setdefault(canonical_record_id, set()).add(
                    batch.raw_payload.record_id
                )
            else:
                if write.existing_record_id is None:
                    raise AssertionError("conflict result must identify the existing record")
                conflict = CandleConflictRecord(
                    stream=CandleStream.from_candle(candle),
                    open_time=candle.open_time,
                    existing_record_id=write.existing_record_id,
                    incoming_candle=candle,
                    raw_payload_id=batch.raw_payload.record_id,
                    detected_at=candle.processed_at,
                )
                self._conflicts.setdefault(
                    (conflict.existing_record_id, conflict.incoming_candle.record_id),
                    conflict,
                )
        return MarketDataBatchWriteResult(
            raw_payload=raw_write,
            candles=tuple(candle_writes),
        )

    def records_for_stream(self, stream: CandleStream) -> tuple[CanonicalCandle, ...]:
        """Return one stream sorted deterministically by market time."""

        return tuple(
            sorted(
                (candle for candle in self._records.values() if stream.contains(candle)),
                key=lambda candle: (
                    candle.open_time,
                    candle.close_time,
                    str(candle.record_id),
                ),
            )
        )

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Return exact provider evidence by ID."""

        return self._raw_payloads.get(record_id)

    def raw_payload_ids_for_candle(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one canonical record."""

        return tuple(sorted(self._raw_lineage.get(record_id, set()), key=str))

    def conflicts_for_stream(self, stream: CandleStream) -> tuple[CandleConflictRecord, ...]:
        """Return quarantined revisions for one canonical stream."""

        return tuple(
            sorted(
                (conflict for conflict in self._conflicts.values() if conflict.stream == stream),
                key=lambda conflict: (
                    conflict.open_time,
                    str(conflict.incoming_candle.record_id),
                ),
            )
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
