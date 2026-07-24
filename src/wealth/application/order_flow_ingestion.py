"""Fail-closed ingestion for bounded canonical order-flow batches."""

from dataclasses import dataclass, field
from datetime import datetime

from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.domain.order_flow_quality import (
    OrderFlowSequenceReport,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
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

        return (
            self.quality.status is DataQualityStatus.PASS
            and self.raw_write is not None
            and self.raw_write.status is not RawPayloadWriteStatus.CONFLICT
            and len(self.writes) == len(self.batch.records)
            and all(write.status is not OrderFlowWriteStatus.CONFLICT for write in self.writes)
        )


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
