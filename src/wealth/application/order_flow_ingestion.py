"""Fail-closed ingestion for bounded canonical order-flow batches."""

from dataclasses import dataclass, field
from datetime import datetime

from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.domain.order_flow_quality import (
    OrderFlowSequenceReport,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
    order_flow_record_type,
)
from wealth.domain.quality import (
    DataQualityStatus,
    RawPayloadWriteResult,
    RawPayloadWriteStatus,
)
from wealth.ports.order_flow import OrderFlowFetchBatch, OrderFlowStore


@dataclass(frozen=True, slots=True)
class OrderFlowIngestionResult:
    """Observed batch, quality decision, and explicit persistence outcomes."""

    batch: OrderFlowFetchBatch
    quality: OrderFlowSequenceReport
    raw_write: RawPayloadWriteResult | None
    writes: tuple[OrderFlowWriteResult, ...]

    @property
    def accepted(self) -> bool:
        """Return whether every record passed quality and durable admission."""

        if self.quality.status is not DataQualityStatus.PASS or self.raw_write is None:
            return False

        raw_record_id = self.batch.raw_payload.record_id
        if self.raw_write.incoming_record_id != raw_record_id:
            return False
        if self.raw_write.status is RawPayloadWriteStatus.INSERTED:
            if self.raw_write.existing_record_id is not None:
                return False
        elif self.raw_write.status is RawPayloadWriteStatus.DUPLICATE:
            if self.raw_write.existing_record_id != raw_record_id:
                return False
        else:
            return False

        if len(self.writes) != len(self.batch.records):
            return False
        for write, record in zip(self.writes, self.batch.records, strict=True):
            if (
                write.incoming_record_id != record.record_id
                or write.record_type is not order_flow_record_type(record)
            ):
                return False
            if write.status is OrderFlowWriteStatus.INSERTED:
                if write.existing_record_id is not None:
                    return False
            elif write.status is OrderFlowWriteStatus.DUPLICATE:
                if write.existing_record_id is None:
                    return False
            else:
                return False
        return True


@dataclass(frozen=True, slots=True)
class OrderFlowBatchIngestor:
    """Audit and only then persist one already-observed order-flow batch."""

    store: OrderFlowStore
    auditor: OrderFlowSequenceAuditor = field(default_factory=OrderFlowSequenceAuditor)

    def ingest(
        self,
        batch: OrderFlowFetchBatch,
        *,
        window_start: datetime,
        window_end_exclusive: datetime,
    ) -> OrderFlowIngestionResult:
        """Reject incomplete or ambiguous records before any storage mutation."""

        quality = self.auditor.audit(
            stream=batch.stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            records=batch.records,
        )
        persistence = (
            self.store.append_batch(batch) if quality.status is DataQualityStatus.PASS else None
        )
        return OrderFlowIngestionResult(
            batch=batch,
            quality=quality,
            raw_write=persistence.raw_payload if persistence is not None else None,
            writes=persistence.records if persistence is not None else (),
        )
