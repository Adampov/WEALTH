"""Provider-independent trade, ticker, and best-bid-ask contracts."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from wealth.domain.market import InstrumentType, NonNegativeDecimal, PositiveDecimal


class AggressorSide(StrEnum):
    """Side that initiated a canonical trade, when the provider proves it."""

    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class TradeAggregationKind(StrEnum):
    """Whether one canonical observation combines provider market trades."""

    NONE = "none"
    PROVIDER_DEFINED = "provider_defined"


class _CanonicalTimedMarketRecord(BaseModel):
    """Shared strict identity, timing, sequence, and lineage evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["1.0"] = "1.0"
    record_id: UUID
    source: str = Field(min_length=1, max_length=128)
    venue: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=64)
    instrument_type: InstrumentType
    event_time: AwareDatetime
    observed_at: AwareDatetime
    processed_at: AwareDatetime
    provider_sequence: int | None = Field(default=None, ge=0)
    lineage: tuple[str, ...] = Field(min_length=1)

    @field_validator("source", "venue", "instrument")
    @classmethod
    def identifiers_are_canonical(cls, value: str) -> str:
        """Reject identifiers that need implicit whitespace normalization."""

        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("market identifier must not contain whitespace")
        return value

    @field_validator("lineage")
    @classmethod
    def lineage_entries_are_nonempty(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Require at least one usable provenance reference."""

        if any(not reference.strip() for reference in value):
            raise ValueError("lineage references must be non-empty")
        return value

    @model_validator(mode="after")
    def point_in_time_order_is_valid(self) -> Self:
        """Keep exchange event, local observation, and processing time ordered."""

        if self.event_time > self.observed_at:
            raise ValueError("event_time must not be after observed_at")
        if self.observed_at > self.processed_at:
            raise ValueError("observed_at must not be after processed_at")
        return self

    @property
    def stream_key(self) -> tuple[str, str, str, InstrumentType]:
        """Return the stable provider-scoped instrument stream identity."""

        return (
            self.source,
            self.venue,
            self.instrument,
            self.instrument_type,
        )


class CanonicalTrade(_CanonicalTimedMarketRecord):
    """One immutable public trade observation with explicit aggregation evidence."""

    provider_trade_id: str = Field(min_length=1, max_length=128)
    price: PositiveDecimal
    base_quantity: PositiveDecimal
    quote_quantity: PositiveDecimal | None = None
    aggressor_side: AggressorSide
    aggregation_kind: TradeAggregationKind = TradeAggregationKind.NONE
    provider_first_trade_id: str | None = Field(default=None, min_length=1, max_length=128)
    provider_last_trade_id: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator(
        "provider_trade_id",
        "provider_first_trade_id",
        "provider_last_trade_id",
    )
    @classmethod
    def provider_trade_id_is_canonical(cls, value: str | None) -> str | None:
        """Keep provider identity suitable for deterministic natural keys."""

        if value is None:
            return None
        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("provider_trade_id must not contain whitespace")
        return value

    @model_validator(mode="after")
    def aggregation_evidence_is_complete(self) -> Self:
        """Keep individual trades distinct from provider-defined aggregates."""

        has_first = self.provider_first_trade_id is not None
        has_last = self.provider_last_trade_id is not None
        if has_first != has_last:
            raise ValueError("aggregate first and last trade identities must be set together")
        if self.aggregation_kind is TradeAggregationKind.NONE and has_first:
            raise ValueError("individual trade observations must not declare an aggregate range")
        if self.aggregation_kind is TradeAggregationKind.PROVIDER_DEFINED and not has_first:
            raise ValueError("provider-defined aggregates require first and last trade identities")
        return self

    @property
    def natural_key(
        self,
    ) -> tuple[str, str, str, InstrumentType, str]:
        """Return the provider-scoped identity used for idempotency."""

        return (*self.stream_key, self.provider_trade_id)

    @property
    def calculated_quote_quantity(self) -> Decimal:
        """Return exact price times base quantity without replacing source evidence."""

        return self.price * self.base_quantity

    @property
    def market_values(self) -> tuple[object, ...]:
        """Return canonical content used to distinguish duplicates from conflicts."""

        return (
            self.event_time,
            self.price,
            self.base_quantity,
            self.quote_quantity,
            self.aggressor_side,
            self.aggregation_kind,
            self.provider_first_trade_id,
            self.provider_last_trade_id,
            self.provider_sequence,
        )


class CanonicalTicker(_CanonicalTimedMarketRecord):
    """One last-price snapshot with optional explicitly-windowed market statistics."""

    last_price: PositiveDecimal
    window_start: AwareDatetime | None = None
    window_end: AwareDatetime | None = None
    window_open: PositiveDecimal | None = None
    window_high: PositiveDecimal | None = None
    window_low: PositiveDecimal | None = None
    base_volume: NonNegativeDecimal | None = None
    quote_volume: NonNegativeDecimal | None = None

    @model_validator(mode="after")
    def optional_window_is_complete_and_consistent(self) -> Self:
        """Reject ambiguous or internally contradictory rolling-window statistics."""

        has_start = self.window_start is not None
        has_end = self.window_end is not None
        if has_start != has_end:
            raise ValueError("ticker window_start and window_end must be set together")
        metrics = (
            self.window_open,
            self.window_high,
            self.window_low,
            self.base_volume,
            self.quote_volume,
        )
        has_metrics = any(value is not None for value in metrics)
        if has_metrics and not has_start:
            raise ValueError("ticker window metrics require an explicit time window")
        if has_start and not has_metrics:
            raise ValueError("ticker time window requires at least one window metric")
        if (self.window_high is None) != (self.window_low is None):
            raise ValueError("ticker window_high and window_low must be set together")

        if self.window_start is not None and self.window_end is not None:
            if self.window_start >= self.window_end:
                raise ValueError("ticker window_start must precede window_end")
            if self.window_end > self.event_time:
                raise ValueError("ticker window_end must not be after event_time")
        if self.window_high is not None and self.window_low is not None:
            if self.window_low > self.window_high:
                raise ValueError("ticker window_low must not exceed window_high")
            if not self.window_low <= self.last_price <= self.window_high:
                raise ValueError("ticker last_price must be inside the window range")
            if self.window_open is not None and not (
                self.window_low <= self.window_open <= self.window_high
            ):
                raise ValueError("ticker window_open must be inside the window range")
        return self

    @property
    def natural_key(
        self,
    ) -> tuple[str, str, str, InstrumentType, datetime, int | None]:
        """Return the provider-scoped snapshot identity."""

        return (*self.stream_key, self.event_time, self.provider_sequence)

    @property
    def market_values(self) -> tuple[object, ...]:
        """Return snapshot content used to distinguish duplicates from conflicts."""

        return (
            self.last_price,
            self.window_start,
            self.window_end,
            self.window_open,
            self.window_high,
            self.window_low,
            self.base_volume,
            self.quote_volume,
        )


class CanonicalBestBidAsk(_CanonicalTimedMarketRecord):
    """One uncrossed top-of-book snapshot with exact sizes and timing."""

    bid_price: PositiveDecimal
    bid_quantity: PositiveDecimal
    ask_price: PositiveDecimal
    ask_quantity: PositiveDecimal

    @model_validator(mode="after")
    def top_of_book_is_uncrossed(self) -> Self:
        """Reject locked or crossed snapshots at the canonical boundary."""

        if self.bid_price >= self.ask_price:
            raise ValueError("best bid must be strictly below best ask")
        return self

    @property
    def natural_key(
        self,
    ) -> tuple[str, str, str, InstrumentType, datetime, int | None]:
        """Return the provider-scoped top-of-book snapshot identity."""

        return (*self.stream_key, self.event_time, self.provider_sequence)

    @property
    def spread(self) -> Decimal:
        """Return the exact absolute bid-ask spread."""

        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> Decimal:
        """Return the exact arithmetic midpoint."""

        return (self.bid_price + self.ask_price) / Decimal("2")

    @property
    def spread_basis_points(self) -> Decimal:
        """Return spread divided by midpoint in basis points."""

        return self.spread / self.mid_price * Decimal("10000")

    @property
    def market_values(self) -> tuple[object, ...]:
        """Return snapshot content used to distinguish duplicates from conflicts."""

        return (
            self.bid_price,
            self.bid_quantity,
            self.ask_price,
            self.ask_quantity,
        )
