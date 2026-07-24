"""Public, read-only Binance aggregate-trade adapter."""

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow import (
    AggressorSide,
    CanonicalTrade,
    TradeAggregationKind,
)
from wealth.domain.order_flow_quality import (
    OrderFlowRecordType,
    OrderFlowStream,
    ProviderSequencePolicy,
)
from wealth.ports.foundation import Clock
from wealth.ports.http import HttpResponse, HttpTransportError, PublicHttpClient
from wealth.ports.order_flow import (
    OrderFlowFetchBatch,
    PublicTradeSourceError,
    PublicTradeWindowRequest,
)

BINANCE_ORDER_FLOW_SOURCE = "binance.public-rest"
BINANCE_ORDER_FLOW_VENUE = "BINANCE"
BINANCE_SPOT_AGG_TRADES_URL = "https://data-api.binance.vision/api/v3/aggTrades"
BINANCE_USDM_AGG_TRADES_URL = "https://fapi.binance.com/fapi/v1/aggTrades"
BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT = 4
BINANCE_USDM_AGG_TRADES_REQUEST_WEIGHT = 20
MAX_BINANCE_AGGREGATE_TRADES = 1_000
MAX_BINANCE_AGGREGATE_TRADE_WINDOW = timedelta(hours=1)
BINANCE_USDM_HISTORY = timedelta(hours=24)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BINANCE_SYMBOL = re.compile(r"^[A-Z0-9_]{2,64}$")


class BinanceAggregateTradeErrorCode(StrEnum):
    """Machine-readable public aggregate-trade failures."""

    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_INSTRUMENT = "unsupported_instrument"
    TRANSPORT_FAILURE = "transport_failure"
    RATE_LIMITED = "rate_limited"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_REJECTED = "provider_rejected"
    POSSIBLY_TRUNCATED = "possibly_truncated"
    INVALID_PAYLOAD = "invalid_payload"


class BinanceAggregateTradeError(PublicTradeSourceError):
    """Fail explicitly when Binance data cannot become canonical evidence."""

    def __init__(
        self,
        code: BinanceAggregateTradeErrorCode,
        detail: str,
        *,
        retry_after_seconds: int | None = None,
        retryable: bool | None = None,
    ) -> None:
        self.code = code
        default_retryable = code in {
            BinanceAggregateTradeErrorCode.TRANSPORT_FAILURE,
            BinanceAggregateTradeErrorCode.RATE_LIMITED,
            BinanceAggregateTradeErrorCode.PROVIDER_UNAVAILABLE,
        }
        super().__init__(
            code.value,
            detail,
            retryable=default_retryable if retryable is None else retryable,
            retry_after_seconds=retry_after_seconds,
            requires_smaller_window=(code is BinanceAggregateTradeErrorCode.POSSIBLY_TRUNCATED),
        )


@dataclass(frozen=True, slots=True)
class _BinanceAggregateTrade:
    """Structurally validated Binance aggregate-trade row."""

    aggregate_trade_id: int
    price: str
    quantity: str
    first_trade_id: int
    last_trade_id: int
    event_time_ms: int
    buyer_is_maker: bool
    best_price_match: bool | None
    normal_quantity: str | None

    @property
    def raw_values(self) -> tuple[object, ...]:
        """Return exact provider values for deterministic record identity."""

        return (
            self.aggregate_trade_id,
            self.price,
            self.quantity,
            self.first_trade_id,
            self.last_trade_id,
            self.event_time_ms,
            self.buyer_is_maker,
            self.best_price_match,
            self.normal_quantity,
        )


@dataclass(frozen=True, slots=True)
class BinancePublicAggregateTradeSource:
    """Fetch conservatively complete aggregate-trade windows from public REST."""

    http: PublicHttpClient
    clock: Clock
    timeout_seconds: float = 10.0
    spot_agg_trades_url: str = BINANCE_SPOT_AGG_TRADES_URL
    usdm_agg_trades_url: str = BINANCE_USDM_AGG_TRADES_URL

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        for url in (self.spot_agg_trades_url, self.usdm_agg_trades_url):
            if not url.startswith("https://"):
                raise ValueError("Binance public endpoints must use HTTPS")

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        """Fetch one bounded aggregate-trade window or reject possible truncation."""

        request_started_at = self.clock.now()
        self._validate_request(request, request_started_at)
        url = self._endpoint_for(request.instrument_type)
        query = self._query_for(request)
        try:
            response = self.http.get(
                url=url,
                query=query,
                timeout_seconds=self.timeout_seconds,
            )
        except HttpTransportError as error:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.TRANSPORT_FAILURE,
                "public aggregate-trade request did not receive a response",
            ) from error

        observed_at = self.clock.now()
        if observed_at < request_started_at:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "clock regressed while observing the provider response",
            )
        self._raise_for_status(response)
        rows = self._parse_payload(response.body)
        if len(rows) >= MAX_BINANCE_AGGREGATE_TRADES:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.POSSIBLY_TRUNCATED,
                "provider response reached the row cap; request a smaller time window",
            )
        processed_at = self.clock.now()
        if processed_at < observed_at:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "clock regressed while processing the provider response",
            )
        raw_payload = self._raw_payload(
            body=response.body,
            request=request,
            query=query,
            url=url,
            observed_at=observed_at,
            processed_at=processed_at,
        )
        records = tuple(
            self._canonicalize(
                row=row,
                row_number=row_number,
                request=request,
                url=url,
                observed_at=observed_at,
                processed_at=processed_at,
                raw_payload_reference=raw_payload.lineage_reference,
            )
            for row_number, row in enumerate(rows)
        )
        return OrderFlowFetchBatch(
            stream=OrderFlowStream(
                source=BINANCE_ORDER_FLOW_SOURCE,
                venue=BINANCE_ORDER_FLOW_VENUE,
                instrument=request.instrument,
                instrument_type=request.instrument_type,
                record_type=OrderFlowRecordType.TRADE,
                sequence_policy=ProviderSequencePolicy.MONOTONIC,
            ),
            observed_at=observed_at,
            processed_at=processed_at,
            raw_payload=raw_payload,
            records=records,
        )

    @staticmethod
    def _validate_request(request: PublicTradeWindowRequest, now: datetime) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "clock must return a timezone-aware timestamp",
            )
        if request.window_end_exclusive > now:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "requested trade window must end before the request",
            )
        if request.duration >= MAX_BINANCE_AGGREGATE_TRADE_WINDOW:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "aggregate-trade window must be shorter than one hour",
            )
        if (
            request.instrument_type
            in {InstrumentType.PERPETUAL_FUTURE, InstrumentType.DATED_FUTURE}
            and request.window_start < now - BINANCE_USDM_HISTORY
        ):
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "USD-M aggregate-trade window is older than the provider history limit",
            )
        if BINANCE_SYMBOL.fullmatch(request.provider_symbol) is None:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_REQUEST,
                "provider_symbol must be an explicit uppercase Binance symbol",
            )

    def _endpoint_for(self, instrument_type: InstrumentType) -> str:
        if instrument_type is InstrumentType.SPOT:
            return self.spot_agg_trades_url
        if instrument_type in {
            InstrumentType.PERPETUAL_FUTURE,
            InstrumentType.DATED_FUTURE,
        }:
            return self.usdm_agg_trades_url
        raise BinanceAggregateTradeError(
            BinanceAggregateTradeErrorCode.UNSUPPORTED_INSTRUMENT,
            f"unsupported instrument type: {instrument_type.value}",
        )

    @staticmethod
    def _query_for(request: PublicTradeWindowRequest) -> dict[str, str]:
        return {
            "symbol": request.provider_symbol,
            "startTime": str(_to_epoch_milliseconds(request.window_start)),
            "endTime": str(_to_epoch_milliseconds(request.window_end_exclusive) - 1),
            "limit": str(MAX_BINANCE_AGGREGATE_TRADES),
        }

    @staticmethod
    def _raise_for_status(response: HttpResponse) -> None:
        if response.status_code == 200:
            return
        if response.status_code in {418, 429}:
            retry_after = _nonnegative_integer(response.header("Retry-After"))
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.RATE_LIMITED,
                f"Binance returned HTTP {response.status_code}",
                retry_after_seconds=retry_after,
                retryable=retry_after is not None,
            )
        if 500 <= response.status_code <= 599:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.PROVIDER_UNAVAILABLE,
                f"Binance returned HTTP {response.status_code}",
            )
        raise BinanceAggregateTradeError(
            BinanceAggregateTradeErrorCode.PROVIDER_REJECTED,
            f"Binance returned HTTP {response.status_code}",
        )

    @staticmethod
    def _parse_payload(body: bytes) -> tuple[_BinanceAggregateTrade, ...]:
        try:
            payload: object = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                "response was not valid UTF-8 JSON",
            ) from error
        if not isinstance(payload, list):
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                "response root must be an array",
            )

        rows: list[_BinanceAggregateTrade] = []
        required_fields = {"a", "p", "q", "f", "l", "T", "m"}
        allowed_fields = required_fields | {"M", "nq"}
        for row_number, candidate in enumerate(payload):
            if not isinstance(candidate, dict):
                raise BinanceAggregateTradeError(
                    BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} must be an object",
                )
            fields = set(candidate)
            if not required_fields <= fields or not fields <= allowed_fields:
                raise BinanceAggregateTradeError(
                    BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} has an unsupported field set",
                )
            rows.append(
                _BinanceAggregateTrade(
                    aggregate_trade_id=_required_integer(
                        candidate,
                        "a",
                        row_number,
                    ),
                    price=_required_string(candidate, "p", row_number),
                    quantity=_required_string(candidate, "q", row_number),
                    first_trade_id=_required_integer(candidate, "f", row_number),
                    last_trade_id=_required_integer(candidate, "l", row_number),
                    event_time_ms=_required_integer(candidate, "T", row_number),
                    buyer_is_maker=_required_boolean(candidate, "m", row_number),
                    best_price_match=_optional_boolean(candidate, "M", row_number),
                    normal_quantity=_optional_string(candidate, "nq", row_number),
                )
            )
        return tuple(rows)

    @staticmethod
    def _raw_payload(
        *,
        body: bytes,
        request: PublicTradeWindowRequest,
        query: dict[str, str],
        url: str,
        observed_at: datetime,
        processed_at: datetime,
    ) -> RawMarketPayload:
        payload_digest = sha256(body).hexdigest()
        endpoint_path = _endpoint_path(url)
        request_identity = json.dumps(
            (
                BINANCE_ORDER_FLOW_SOURCE,
                BINANCE_ORDER_FLOW_VENUE,
                endpoint_path,
                tuple(sorted(query.items())),
                payload_digest,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return RawMarketPayload(
            record_id=uuid5(NAMESPACE_URL, request_identity),
            source=BINANCE_ORDER_FLOW_SOURCE,
            venue=BINANCE_ORDER_FLOW_VENUE,
            observed_at=observed_at,
            processed_at=processed_at,
            payload_sha256=payload_digest,
            payload=body,
            lineage=(
                f"binance-public-rest:{endpoint_path}:{request.provider_symbol}:"
                f"{_to_epoch_milliseconds(request.window_start)}:"
                f"{_to_epoch_milliseconds(request.window_end_exclusive)}",
            ),
        )

    @staticmethod
    def _canonicalize(
        *,
        row: _BinanceAggregateTrade,
        row_number: int,
        request: PublicTradeWindowRequest,
        url: str,
        observed_at: datetime,
        processed_at: datetime,
        raw_payload_reference: str,
    ) -> CanonicalTrade:
        event_time = _from_epoch_milliseconds(row.event_time_ms)
        if not request.window_start <= event_time < request.window_end_exclusive:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                f"row {row_number} event time is outside the requested window",
            )
        if (
            row.aggregate_trade_id < 0
            or row.first_trade_id < 0
            or row.last_trade_id < row.first_trade_id
        ):
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                f"row {row_number} contains an invalid provider identity range",
            )
        if row.normal_quantity is not None:
            try:
                if Decimal(row.normal_quantity) < 0:
                    raise ValueError("normal quantity is negative")
            except (InvalidOperation, ValueError) as error:
                raise BinanceAggregateTradeError(
                    BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} normal quantity is invalid",
                ) from error

        record_identity = json.dumps(
            (
                BINANCE_ORDER_FLOW_SOURCE,
                BINANCE_ORDER_FLOW_VENUE,
                request.instrument,
                request.provider_symbol,
                request.instrument_type.value,
                row.raw_values,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        endpoint_path = _endpoint_path(url)
        try:
            return CanonicalTrade(
                record_id=uuid5(NAMESPACE_URL, record_identity),
                source=BINANCE_ORDER_FLOW_SOURCE,
                venue=BINANCE_ORDER_FLOW_VENUE,
                instrument=request.instrument,
                instrument_type=request.instrument_type,
                event_time=event_time,
                observed_at=observed_at,
                processed_at=processed_at,
                provider_sequence=row.aggregate_trade_id,
                lineage=(
                    raw_payload_reference,
                    f"binance-public-rest:{endpoint_path}:{request.provider_symbol}:"
                    f"aggregate-trade:{row.aggregate_trade_id}",
                ),
                provider_trade_id=str(row.aggregate_trade_id),
                price=Decimal(row.price),
                base_quantity=Decimal(row.quantity),
                quote_quantity=None,
                aggressor_side=(AggressorSide.SELL if row.buyer_is_maker else AggressorSide.BUY),
                aggregation_kind=TradeAggregationKind.PROVIDER_DEFINED,
                provider_first_trade_id=str(row.first_trade_id),
                provider_last_trade_id=str(row.last_trade_id),
            )
        except (InvalidOperation, ValidationError, ValueError) as error:
            raise BinanceAggregateTradeError(
                BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
                f"row {row_number} violates the canonical trade contract",
            ) from error


def _required_integer(row: dict[object, object], key: str, row_number: int) -> int:
    value = row[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise BinanceAggregateTradeError(
            BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
            f"row {row_number} field {key} must be an integer",
        )
    return value


def _required_string(row: dict[object, object], key: str, row_number: int) -> str:
    value = row[key]
    if not isinstance(value, str):
        raise BinanceAggregateTradeError(
            BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
            f"row {row_number} field {key} must be a string",
        )
    return value


def _required_boolean(row: dict[object, object], key: str, row_number: int) -> bool:
    value = row[key]
    if not isinstance(value, bool):
        raise BinanceAggregateTradeError(
            BinanceAggregateTradeErrorCode.INVALID_PAYLOAD,
            f"row {row_number} field {key} must be a boolean",
        )
    return value


def _optional_boolean(
    row: dict[object, object],
    key: str,
    row_number: int,
) -> bool | None:
    if key not in row:
        return None
    return _required_boolean(row, key, row_number)


def _optional_string(
    row: dict[object, object],
    key: str,
    row_number: int,
) -> str | None:
    if key not in row:
        return None
    return _required_string(row, key, row_number)


def _to_epoch_milliseconds(value: datetime) -> int:
    delta = value.astimezone(UTC) - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def _from_epoch_milliseconds(value: int) -> datetime:
    return UTC_EPOCH + timedelta(milliseconds=value)


def _nonnegative_integer(value: str | None) -> int | None:
    if value is None or len(value) > 10 or not value.isascii() or not value.isdecimal():
        return None
    parsed = int(value)
    return parsed if parsed >= 0 else None


def _endpoint_path(url: str) -> str:
    return url.removeprefix("https://").split("/", maxsplit=1)[-1]
