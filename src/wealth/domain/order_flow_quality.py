"""Typed quality and idempotency contracts for canonical order-flow records."""

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.market import InstrumentType
from wealth.domain.order_flow import CanonicalBestBidAsk, CanonicalTicker, CanonicalTrade
from wealth.domain.quality import DataQualityStatus, RawPayloadWriteResult

type OrderFlowRecord = CanonicalTrade | CanonicalTicker | CanonicalBestBidAsk


class OrderFlowRecordType(StrEnum):
    """Canonical record family within one independently audited stream."""

    TRADE = "trade"
    TICKER = "ticker"
    BEST_BID_ASK = "best_bid_ask"


class ProviderSequencePolicy(StrEnum):
    """Provider guarantee that the quality gate is allowed to enforce."""

    UNSPECIFIED = "unspecified"
    MONOTONIC = "monotonic"
    CONTIGUOUS = "contiguous"


class OrderFlowQualityCode(StrEnum):
    """Machine-readable order-flow quality findings."""

    OUT_OF_ORDER = "out_of_order"
    MIXED_STREAM = "mixed_stream"
    OUT_OF_WINDOW = "out_of_window"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"
    MISSING_SEQUENCE = "missing_sequence"
    SEQUENCE_REGRESSION = "sequence_regression"
    SEQUENCE_REUSE = "sequence_reuse"


class OrderFlowWriteStatus(StrEnum):
    """Idempotent outcomes for canonical order-flow persistence."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


def order_flow_record_type(record: OrderFlowRecord) -> OrderFlowRecordType:
    """Return the explicit canonical family for one supported record."""

    if isinstance(record, CanonicalTrade):
        return OrderFlowRecordType.TRADE
    if isinstance(record, CanonicalTicker):
        return OrderFlowRecordType.TICKER
    return OrderFlowRecordType.BEST_BID_ASK


class OrderFlowStream(BaseModel):
    """Provider-scoped identity and sequence promise for one record family."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    record_type: OrderFlowRecordType
    sequence_policy: ProviderSequencePolicy = ProviderSequencePolicy.UNSPECIFIED

    @field_validator("source", "venue", "instrument")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject ambiguous stream identifiers."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("stream identifier must not contain whitespace")
        return value

    @classmethod
    def from_record(
        cls,
        record: OrderFlowRecord,
        *,
        sequence_policy: ProviderSequencePolicy = ProviderSequencePolicy.UNSPECIFIED,
    ) -> Self:
        """Build exact stream identity without inferring a provider sequence promise."""

        return cls(
            source=record.source,
            venue=record.venue,
            instrument=record.instrument,
            instrument_type=record.instrument_type,
            record_type=order_flow_record_type(record),
            sequence_policy=sequence_policy,
        )

    def contains(self, record: OrderFlowRecord) -> bool:
        """Return whether a record belongs to this exact canonical stream."""

        return order_flow_record_type(record) is self.record_type and record.stream_key == (
            self.source,
            self.venue,
            self.instrument,
            self.instrument_type,
        )


class OrderFlowQualityIssue(BaseModel):
    """One explicit and reproducible order-flow quality finding."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: OrderFlowQualityCode
    event_time: AwareDatetime | None = None
    record_ids: tuple[UUID, ...] = ()
    provider_sequence: int | None = Field(default=None, ge=0)
    previous_provider_sequence: int | None = Field(default=None, ge=0)
    detail: str = Field(min_length=1, max_length=512)


class MissingProviderSequenceRange(BaseModel):
    """A provable contiguous range absent from a provider sequence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    start_sequence: int = Field(ge=0)
    end_sequence_inclusive: int = Field(ge=0)
    missing_count: int = Field(gt=0)

    @model_validator(mode="after")
    def range_is_consistent(self) -> Self:
        """Reject inverted ranges or incorrect counts."""

        if self.end_sequence_inclusive < self.start_sequence:
            raise ValueError("missing sequence range end must not precede start")
        if self.missing_count != self.end_sequence_inclusive - self.start_sequence + 1:
            raise ValueError("missing sequence count must match the inclusive range")
        return self


class OrderFlowSequenceReport(BaseModel):
    """Deterministic quality result for one bounded order-flow stream."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stream: OrderFlowStream
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime
    input_count: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    sequenced_count: int = Field(ge=0)
    status: DataQualityStatus
    issues: tuple[OrderFlowQualityIssue, ...] = ()
    missing_sequence_ranges: tuple[MissingProviderSequenceRange, ...] = ()

    @model_validator(mode="after")
    def report_is_self_consistent(self) -> Self:
        """Keep counts, window, and status internally coherent."""

        if self.window_end_exclusive <= self.window_start:
            raise ValueError("report window end must be after start")
        if self.accepted_count > self.input_count:
            raise ValueError("accepted_count must not exceed input_count")
        if self.sequenced_count > self.accepted_count:
            raise ValueError("sequenced_count must not exceed accepted_count")
        has_findings = bool(self.issues or self.missing_sequence_ranges)
        if (self.status is DataQualityStatus.PASS) == has_findings:
            raise ValueError("report status must match its findings")
        if (
            self.missing_sequence_ranges
            and self.stream.sequence_policy is not ProviderSequencePolicy.CONTIGUOUS
        ):
            raise ValueError("sequence gaps require a contiguous provider policy")
        return self


class OrderFlowWriteResult(BaseModel):
    """Outcome of an append attempt that never silently overwrites."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: OrderFlowWriteStatus
    record_type: OrderFlowRecordType
    incoming_record_id: UUID
    existing_record_id: UUID | None = None

    @model_validator(mode="after")
    def existing_identity_matches_status(self) -> Self:
        """Require a prior identity exactly when a write did not insert."""

        if self.status is OrderFlowWriteStatus.INSERTED:
            if self.existing_record_id is not None:
                raise ValueError("inserted writes must not identify an existing record")
        elif self.existing_record_id is None:
            raise ValueError("duplicate and conflict writes must identify the existing record")
        return self


class OrderFlowBatchWriteResult(BaseModel):
    """Persistence outcomes for one raw response and its canonical records."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    raw_payload: RawPayloadWriteResult
    records: tuple[OrderFlowWriteResult, ...]


class OrderFlowConflictRecord(BaseModel):
    """A quarantined revision that was never promoted to canonical evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stream: OrderFlowStream
    existing_record_id: UUID
    incoming_record: OrderFlowRecord
    raw_payload_id: UUID | None = None
    detected_at: AwareDatetime

    @model_validator(mode="after")
    def conflict_is_self_consistent(self) -> Self:
        """Require the revision, stream, timing, and optional raw lineage to agree."""

        if not self.stream.contains(self.incoming_record):
            raise ValueError("incoming record must belong to the conflict stream")
        if self.detected_at < self.incoming_record.observed_at:
            raise ValueError("conflict cannot be detected before observation")
        if self.raw_payload_id is not None:
            expected_lineage = f"raw-market-payload:{self.raw_payload_id}"
            if expected_lineage not in self.incoming_record.lineage:
                raise ValueError("incoming record must reference the raw payload")
        return self


def order_flow_storage_key(record: OrderFlowRecord) -> tuple[object, ...]:
    """Namespace a natural key by record family for collision-safe persistence."""

    return (order_flow_record_type(record), *record.natural_key)


def order_flow_sort_key(record: OrderFlowRecord) -> tuple[datetime, int, int, str]:
    """Return a stable market-time ordering with missing sequences last."""

    sequence_missing = int(record.provider_sequence is None)
    sequence = record.provider_sequence if record.provider_sequence is not None else 0
    return (record.event_time, sequence_missing, sequence, str(record.record_id))
