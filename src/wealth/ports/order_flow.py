"""Provider requests, batches, and persistence ports for canonical order-flow."""

from datetime import timedelta
from typing import Protocol
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowConflictRecord,
    OrderFlowRecord,
    OrderFlowStream,
    OrderFlowWriteResult,
)

MAX_ORDER_FLOW_BATCH_RECORDS = 100_000


class PublicTradeWindowRequest(BaseModel):
    """One bounded provider-independent event-time request for public trades."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime

    @field_validator("instrument", "provider_symbol")
    @classmethod
    def identifiers_are_unambiguous(cls, value: str) -> str:
        """Reject symbols whose normalization would otherwise be implicit."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("trade request identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def window_is_positive_and_millisecond_aligned(self) -> "PublicTradeWindowRequest":
        """Require an exact half-open window representable by REST milliseconds."""

        if self.window_end_exclusive <= self.window_start:
            raise ValueError("window end must be after window start")
        if any(
            timestamp.microsecond % 1_000
            for timestamp in (self.window_start, self.window_end_exclusive)
        ):
            raise ValueError("trade request window must align to milliseconds")
        return self

    @property
    def duration(self) -> timedelta:
        """Return the exact requested event-time duration."""

        return self.window_end_exclusive - self.window_start


class PublicTradeSourceError(RuntimeError):
    """Expose a safe, provider-independent retry classification."""

    def __init__(
        self,
        machine_code: str,
        detail: str,
        *,
        retryable: bool,
        retry_after_seconds: int | None = None,
        requires_smaller_window: bool = False,
    ) -> None:
        if not machine_code or machine_code != machine_code.strip():
            raise ValueError("machine_code must be non-empty and canonical")
        if retry_after_seconds is not None and retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be non-negative")
        if requires_smaller_window and retryable:
            raise ValueError("a smaller-window failure must not also be retryable unchanged")
        self.machine_code = machine_code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.requires_smaller_window = requires_smaller_window
        super().__init__(f"{machine_code}: {detail}")


class OrderFlowFetchBatch(BaseModel):
    """One exact raw provider response and its canonical record family."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stream: OrderFlowStream
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    raw_payload: RawMarketPayload
    records: tuple[OrderFlowRecord, ...] = Field(max_length=MAX_ORDER_FLOW_BATCH_RECORDS)

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


class PublicTradeWindowSource(Protocol):
    """Fetch one bounded, conservatively complete public trade window."""

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        """Return canonical trade observations or fail explicitly."""


class OrderFlowStore(Protocol):
    """Persist canonical order flow without silent replacement."""

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Insert once or return an explicit idempotency outcome."""

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        """Persist one batch atomically and return exact identity-bound outcomes.

        The raw outcome identifies the batch payload. After an inserted or duplicate
        raw outcome, return exactly one canonical outcome per batch record in the same
        order, with its matching incoming ID and record family. A zero-record batch
        returns no canonical outcomes. Inserted outcomes omit an existing ID; duplicate
        and conflict outcomes identify the retained record.
        """

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
