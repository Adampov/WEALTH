"""Canonical, provider-independent market-data contracts."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

PositiveDecimal = Annotated[Decimal, Field(gt=Decimal("0"))]
NonNegativeDecimal = Annotated[Decimal, Field(ge=Decimal("0"))]
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MAX_RAW_MARKET_PAYLOAD_BYTES = 8 * 1024 * 1024


class InstrumentType(StrEnum):
    """Instrument types supported by the initial market-data contract."""

    SPOT = "spot"
    PERPETUAL_FUTURE = "perpetual_future"
    DATED_FUTURE = "dated_future"


class CandleTimeframe(StrEnum):
    """Closed-candle intervals supported by the first replay slice."""

    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"

    @property
    def duration(self) -> timedelta:
        """Return the exact interval represented by the timeframe."""

        seconds = {
            CandleTimeframe.ONE_MINUTE: 60,
            CandleTimeframe.FIVE_MINUTES: 5 * 60,
            CandleTimeframe.FIFTEEN_MINUTES: 15 * 60,
            CandleTimeframe.ONE_HOUR: 60 * 60,
            CandleTimeframe.FOUR_HOURS: 4 * 60 * 60,
            CandleTimeframe.ONE_DAY: 24 * 60 * 60,
        }
        return timedelta(seconds=seconds[self])


class RawMarketPayload(BaseModel):
    """Exact bounded provider evidence retained before canonicalization."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    record_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    media_type: Literal["application/json"] = "application/json"
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: bytes = Field(min_length=1, max_length=MAX_RAW_MARKET_PAYLOAD_BYTES)
    lineage: tuple[str, ...] = Field(min_length=1)

    @field_validator("source", "venue")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject invisible normalization and ambiguous identifiers."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("identifier must not contain whitespace")
        return value

    @field_validator("lineage")
    @classmethod
    def lineage_entries_are_nonempty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Require explicit provenance for the captured response."""

        if any(not reference.strip() for reference in value):
            raise ValueError("lineage references must be non-empty")
        return value

    @model_validator(mode="after")
    def evidence_invariants_hold(self) -> Self:
        """Reject time regressions and content whose digest is inconsistent."""

        if self.observed_at > self.processed_at:
            raise ValueError("observed_at must not be after processed_at")
        if sha256(self.payload).hexdigest() != self.payload_sha256:
            raise ValueError("payload_sha256 must match the exact payload bytes")
        return self

    @property
    def lineage_reference(self) -> str:
        """Return the canonical reference used by derived records."""

        return f"raw-market-payload:{self.record_id}"

    @property
    def content_identity(self) -> tuple[object, ...]:
        """Return immutable evidence content used for idempotency checks."""

        return (
            self.source,
            self.venue,
            self.media_type,
            self.payload_sha256,
            self.payload,
            self.lineage,
        )


class CanonicalCandle(BaseModel):
    """A validated, final candle with point-in-time lineage."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    record_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    timeframe: CandleTimeframe
    open_time: AwareDatetime
    close_time: AwareDatetime
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    open: PositiveDecimal
    high: PositiveDecimal
    low: PositiveDecimal
    close: PositiveDecimal
    base_volume: NonNegativeDecimal
    quote_volume: NonNegativeDecimal | None = None
    trade_count: int | None = Field(default=None, ge=0)
    provider_sequence: int | None = Field(default=None, ge=0)
    lineage: tuple[str, ...] = Field(min_length=1)
    is_final: Literal[True] = True

    @field_validator("source", "venue", "instrument")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject invisible normalization and ambiguous identifiers."""

        if value != value.strip():
            raise ValueError("identifier must not contain leading or trailing whitespace")
        if any(character.isspace() for character in value):
            raise ValueError("identifier must not contain whitespace")
        return value

    @field_validator("lineage")
    @classmethod
    def lineage_entries_are_nonempty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Require explicit, non-empty lineage references."""

        if any(not reference.strip() for reference in value):
            raise ValueError("lineage references must be non-empty")
        return value

    @model_validator(mode="after")
    def market_invariants_hold(self) -> Self:
        """Enforce OHLC, interval, and point-in-time invariants."""

        if self.close_time - self.open_time != self.timeframe.duration:
            raise ValueError("candle duration must match timeframe")
        if (self.open_time.astimezone(UTC) - UTC_EPOCH) % self.timeframe.duration:
            raise ValueError("candle open_time must align to the timeframe UTC grid")
        if self.high < max(self.open, self.close):
            raise ValueError("high must be at least open and close")
        if self.low > min(self.open, self.close):
            raise ValueError("low must be at most open and close")
        if self.low > self.high:
            raise ValueError("low must not exceed high")
        if self.close_time > self.observed_at:
            raise ValueError("closed candle cannot be observed before close_time")
        if self.observed_at > self.processed_at:
            raise ValueError("observed_at must not be after processed_at")
        return self

    @property
    def natural_key(
        self,
    ) -> tuple[str, str, str, InstrumentType, CandleTimeframe, datetime]:
        """Return the provider-scoped identity used for duplicate detection."""

        return (
            self.source,
            self.venue,
            self.instrument,
            self.instrument_type,
            self.timeframe,
            self.open_time,
        )

    @property
    def stream_key(
        self,
    ) -> tuple[str, str, str, InstrumentType, CandleTimeframe]:
        """Return the identity shared by records in one candle stream."""

        return (
            self.source,
            self.venue,
            self.instrument,
            self.instrument_type,
            self.timeframe,
        )

    @property
    def market_values(self) -> tuple[object, ...]:
        """Return market content used to distinguish duplicates from conflicts."""

        return (
            self.instrument_type,
            self.close_time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.base_volume,
            self.quote_volume,
            self.trade_count,
            self.provider_sequence,
            self.is_final,
        )
