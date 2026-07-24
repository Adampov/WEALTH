"""Ports and request contracts for canonical market data."""

from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.market import (
    CandleTimeframe,
    CanonicalCandle,
    InstrumentType,
    RawMarketPayload,
)
from wealth.domain.quality import (
    CandleConflictRecord,
    CandleStream,
    CandleWriteResult,
    MarketDataBatchWriteResult,
)

UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class HistoricalCandleSourceError(RuntimeError):
    """Expose a safe, provider-independent retry classification."""

    def __init__(
        self,
        machine_code: str,
        detail: str,
        *,
        retryable: bool,
        retry_after_seconds: int | None = None,
    ) -> None:
        if not machine_code or machine_code != machine_code.strip():
            raise ValueError("machine_code must be non-empty and canonical")
        if retry_after_seconds is not None and retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must be non-negative")
        self.machine_code = machine_code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"{machine_code}: {detail}")


class HistoricalCandleRequest(BaseModel):
    """One bounded, provider-independent request for closed candles."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    instrument: str = Field(min_length=1, max_length=64)
    provider_symbol: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime

    @field_validator("instrument", "provider_symbol")
    @classmethod
    def identifiers_are_unambiguous(cls, value: str) -> str:
        """Reject symbols whose normalization would be implicit."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("instrument identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def window_is_bounded_and_aligned(self) -> "HistoricalCandleRequest":
        """Require an exact positive interval on the timeframe's UTC grid."""

        if self.window_end_exclusive <= self.window_start:
            raise ValueError("window end must be after window start")
        if (self.window_start.astimezone(UTC) - UTC_EPOCH) % self.timeframe.duration:
            raise ValueError("window start must align to the timeframe UTC grid")
        if (self.window_end_exclusive - self.window_start) % self.timeframe.duration:
            raise ValueError("window duration must be an exact timeframe multiple")
        return self

    @property
    def expected_count(self) -> int:
        """Return the number of candles required to cover the request."""

        return (self.window_end_exclusive - self.window_start) // self.timeframe.duration


class CandleFetchBatch(BaseModel):
    """One observed provider response normalized to canonical candles."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    request: HistoricalCandleRequest
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    raw_payload: RawMarketPayload
    records: tuple[CanonicalCandle, ...]

    @field_validator("source", "venue")
    @classmethod
    def source_identifiers_are_unambiguous(cls, value: str) -> str:
        """Reject ambiguous provider identifiers."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("source identifiers must not contain whitespace")
        return value

    @model_validator(mode="after")
    def response_times_and_stream_are_consistent(self) -> "CandleFetchBatch":
        """Ensure the batch cannot mislabel provider records."""

        if self.observed_at > self.processed_at:
            raise ValueError("batch observed_at must not be after processed_at")
        if (
            self.raw_payload.source != self.source
            or self.raw_payload.venue != self.venue
            or self.raw_payload.observed_at != self.observed_at
            or self.raw_payload.processed_at != self.processed_at
        ):
            raise ValueError("raw payload identity and timestamps must match the batch")
        expected_stream = (
            self.source,
            self.venue,
            self.request.instrument,
            self.request.instrument_type,
            self.request.timeframe,
        )
        if any(record.stream_key != expected_stream for record in self.records):
            raise ValueError("batch records must belong to the requested stream")
        if any(
            record.observed_at != self.observed_at or record.processed_at != self.processed_at
            for record in self.records
        ):
            raise ValueError("batch timestamps must match every canonical record")
        if any(self.raw_payload.lineage_reference not in record.lineage for record in self.records):
            raise ValueError("every canonical record must reference the batch raw payload")
        return self


class CandleStore(Protocol):
    """Persist raw and canonical market data without silent replacement."""

    def append(self, candle: CanonicalCandle) -> CandleWriteResult:
        """Insert once or return an explicit idempotency outcome."""

    def append_batch(self, batch: CandleFetchBatch) -> MarketDataBatchWriteResult:
        """Persist exact provider evidence and its canonical records."""

    def records_for_stream(self, stream: CandleStream) -> tuple[CanonicalCandle, ...]:
        """Return an immutable, market-time-ordered stream snapshot."""

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Return exact provider evidence by ID when present."""

    def raw_payload_ids_for_candle(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one accepted canonical record."""

    def conflicts_for_stream(self, stream: CandleStream) -> tuple[CandleConflictRecord, ...]:
        """Return quarantined revisions without promoting them to canonical data."""


class HistoricalCandleSource(Protocol):
    """Fetch a bounded batch of public, closed candles."""

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        """Return canonical records or fail explicitly at the trust boundary."""
