"""Typed data-quality records for canonical candle streams."""

from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from wealth.domain.market import CandleTimeframe, CanonicalCandle, InstrumentType


class CandleQualityCode(StrEnum):
    """Machine-readable candle quality findings."""

    OUT_OF_ORDER = "out_of_order"
    MIXED_STREAM = "mixed_stream"
    OUT_OF_WINDOW = "out_of_window"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class DataQualityStatus(StrEnum):
    """Overall quality result for a bounded sequence."""

    PASS = "pass"
    FAIL = "fail"


class CandleWriteStatus(StrEnum):
    """Idempotent outcomes for canonical candle persistence."""

    INSERTED = "inserted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


class CandleStream(BaseModel):
    """Provider-scoped identity for one candle series."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe

    @field_validator("source", "venue", "instrument")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject ambiguous stream identifiers."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("stream identifier must not contain whitespace")
        return value

    @classmethod
    def from_candle(cls, candle: CanonicalCandle) -> Self:
        """Build stream identity from a validated candle."""

        return cls(
            source=candle.source,
            venue=candle.venue,
            instrument=candle.instrument,
            instrument_type=candle.instrument_type,
            timeframe=candle.timeframe,
        )

    def contains(self, candle: CanonicalCandle) -> bool:
        """Return whether a candle belongs to this exact stream."""

        return candle.stream_key == (
            self.source,
            self.venue,
            self.instrument,
            self.instrument_type,
            self.timeframe,
        )


class CandleQualityIssue(BaseModel):
    """One explicit, reproducible data-quality finding."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: CandleQualityCode
    open_time: AwareDatetime | None = None
    record_ids: tuple[UUID, ...] = ()
    detail: str = Field(min_length=1, max_length=512)


class MissingCandleRange(BaseModel):
    """A contiguous range of expected but unusable candles."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    start_open_time: AwareDatetime
    end_open_time_exclusive: AwareDatetime
    missing_count: int = Field(gt=0)

    @model_validator(mode="after")
    def range_is_forward(self) -> Self:
        """Reject inverted or empty ranges."""

        if self.end_open_time_exclusive <= self.start_open_time:
            raise ValueError("missing range end must be after start")
        return self


class CandleSequenceReport(BaseModel):
    """Deterministic quality result for one stream and expected window."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stream: CandleStream
    window_start: AwareDatetime
    window_end_exclusive: AwareDatetime
    input_count: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    status: DataQualityStatus
    issues: tuple[CandleQualityIssue, ...] = ()
    missing_ranges: tuple[MissingCandleRange, ...] = ()


class CandleWriteResult(BaseModel):
    """Outcome of an append attempt that never silently overwrites."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: CandleWriteStatus
    incoming_record_id: UUID
    existing_record_id: UUID | None = None
