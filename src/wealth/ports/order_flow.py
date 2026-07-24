"""Provider batch and persistence ports for canonical order-flow records."""

from typing import Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from wealth.domain.market import RawMarketPayload
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowConflictRecord,
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteResult,
)

MAX_ORDER_FLOW_BATCH_RECORDS = 100_000


class OrderFlowFetchBatch(BaseModel):
    """One exact raw provider response and its canonical record family."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stream: OrderFlowStream
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    raw_payload: RawMarketPayload
    records: tuple[OrderFlowRecord, ...] = Field(
        min_length=1,
        max_length=MAX_ORDER_FLOW_BATCH_RECORDS,
    )

    @model_validator(mode="after")
    def evidence_and_records_are_consistent(self) -> "OrderFlowFetchBatch":
        """Prevent a raw capture from being attached to unrelated canonical data."""

        if self.observed_at > self.processed_at:
            raise ValueError("batch observed_at must not be after processed_at")
        if (
            self.raw_payload.source != self.stream.source
            or self.raw_payload.venue != self.stream.venue
            or self.raw_payload.observed_at != self.observed_at
            or self.raw_payload.processed_at != self.processed_at
        ):
            raise ValueError("raw payload identity and timestamps must match the batch")
        if any(not self.stream.contains(record) for record in self.records):
            raise ValueError("batch records must belong to the exact declared stream")
        if any(
            record.observed_at != self.observed_at or record.processed_at != self.processed_at
            for record in self.records
        ):
            raise ValueError("batch timestamps must match every canonical record")
        if any(self.raw_payload.lineage_reference not in record.lineage for record in self.records):
            raise ValueError("every canonical record must reference the batch raw payload")
        return self


class OrderFlowStore(Protocol):
    """Persist canonical order flow without silent replacement."""

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Insert once or return an explicit idempotency outcome."""

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        """Persist exact raw evidence and its canonical records atomically."""

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        """Return an immutable, deterministically ordered stream snapshot."""

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Return exact provider evidence by ID when present."""

    def raw_payload_ids_for_record(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one accepted canonical record."""

    def conflicts_for_stream(
        self,
        stream: OrderFlowStream,
    ) -> tuple[OrderFlowConflictRecord, ...]:
        """Return quarantined revisions without promoting them to canonical data."""
