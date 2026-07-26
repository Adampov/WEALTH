"""Public, read-only Binance candle adapter."""

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from wealth.domain.market import CanonicalCandle, InstrumentType, RawMarketPayload
from wealth.ports.foundation import Clock, ClockContractError, require_utc_clock
from wealth.ports.http import HttpResponse, HttpTransportError, PublicHttpClient
from wealth.ports.market import (
    CandleFetchBatch,
    HistoricalCandleRequest,
    HistoricalCandleSourceError,
)

BINANCE_SOURCE = "binance.public-rest"
BINANCE_VENUE = "BINANCE"
BINANCE_SPOT_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_USDM_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
MAX_BINANCE_KLINES = 1_000
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BINANCE_SYMBOL = re.compile(r"^[A-Z0-9_]{2,64}$")


class BinanceCandleErrorCode(StrEnum):
    """Machine-readable public-candle ingestion failures."""

    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_INSTRUMENT = "unsupported_instrument"
    TRANSPORT_FAILURE = "transport_failure"
    RATE_LIMITED = "rate_limited"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_REJECTED = "provider_rejected"
    INVALID_PAYLOAD = "invalid_payload"


class BinanceCandleError(HistoricalCandleSourceError):
    """Fail explicitly when Binance data cannot become canonical records."""

    def __init__(
        self,
        code: BinanceCandleErrorCode,
        detail: str,
        *,
        retry_after_seconds: int | None = None,
        retryable: bool | None = None,
    ) -> None:
        self.code = code
        default_retryable = code in {
            BinanceCandleErrorCode.TRANSPORT_FAILURE,
            BinanceCandleErrorCode.RATE_LIMITED,
            BinanceCandleErrorCode.PROVIDER_UNAVAILABLE,
        }
        super().__init__(
            code.value,
            detail,
            retryable=default_retryable if retryable is None else retryable,
            retry_after_seconds=retry_after_seconds,
        )


@dataclass(frozen=True, slots=True)
class _BinanceKline:
    """Structurally validated positional Binance kline."""

    open_time_ms: int
    open: str
    high: str
    low: str
    close: str
    base_volume: str
    provider_close_time_ms: int
    quote_volume: str
    trade_count: int
    taker_buy_base_volume: str
    taker_buy_quote_volume: str
    ignored: str

    @property
    def raw_values(self) -> tuple[int | str, ...]:
        """Return exact provider values for deterministic record identity."""

        return (
            self.open_time_ms,
            self.open,
            self.high,
            self.low,
            self.close,
            self.base_volume,
            self.provider_close_time_ms,
            self.quote_volume,
            self.trade_count,
            self.taker_buy_base_volume,
            self.taker_buy_quote_volume,
            self.ignored,
        )


@dataclass(frozen=True, slots=True)
class BinancePublicCandleSource:
    """Fetch bounded closed candles from Binance public REST endpoints."""

    http: PublicHttpClient
    clock: Clock
    timeout_seconds: float = 10.0
    spot_klines_url: str = BINANCE_SPOT_KLINES_URL
    usdm_klines_url: str = BINANCE_USDM_KLINES_URL

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        for url in (self.spot_klines_url, self.usdm_klines_url):
            if not url.startswith("https://"):
                raise ValueError("Binance public endpoints must use HTTPS")

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        """Fetch and normalize one complete, closed, bounded candle window."""

        request_started_at = self._clock_now()
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
            raise BinanceCandleError(
                BinanceCandleErrorCode.TRANSPORT_FAILURE,
                "public candle request did not receive a response",
            ) from error

        observed_at = self._clock_now()
        if observed_at < request_started_at:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
                "clock regressed while observing the provider response",
            )
        self._raise_for_status(response)
        rows = self._parse_payload(response.body)
        processed_at = self._clock_now()
        if processed_at < observed_at:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
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
        return CandleFetchBatch(
            request=request,
            source=BINANCE_SOURCE,
            venue=BINANCE_VENUE,
            observed_at=observed_at,
            processed_at=processed_at,
            raw_payload=raw_payload,
            records=records,
        )

    def _clock_now(self) -> datetime:
        try:
            return require_utc_clock(self.clock.now())
        except ClockContractError as error:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
                str(error),
            ) from error

    @staticmethod
    def _validate_request(request: HistoricalCandleRequest, now: datetime) -> None:
        if request.window_end_exclusive > now:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
                "requested window must contain only candles closed before the request",
            )
        if request.expected_count > MAX_BINANCE_KLINES:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
                f"requested window exceeds the {MAX_BINANCE_KLINES}-candle provider limit",
            )
        if BINANCE_SYMBOL.fullmatch(request.provider_symbol) is None:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_REQUEST,
                "provider_symbol must be an explicit uppercase Binance symbol",
            )

    def _endpoint_for(self, instrument_type: InstrumentType) -> str:
        if instrument_type is InstrumentType.SPOT:
            return self.spot_klines_url
        if instrument_type in {
            InstrumentType.PERPETUAL_FUTURE,
            InstrumentType.DATED_FUTURE,
        }:
            return self.usdm_klines_url
        raise BinanceCandleError(
            BinanceCandleErrorCode.UNSUPPORTED_INSTRUMENT,
            f"unsupported instrument type: {instrument_type.value}",
        )

    @staticmethod
    def _query_for(request: HistoricalCandleRequest) -> dict[str, str]:
        query = {
            "symbol": request.provider_symbol,
            "interval": request.timeframe.value,
            "startTime": str(_to_epoch_milliseconds(request.window_start)),
            "endTime": str(_to_epoch_milliseconds(request.window_end_exclusive) - 1),
            "limit": str(request.expected_count),
        }
        if request.instrument_type is InstrumentType.SPOT:
            query["timeZone"] = "0"
        return query

    @staticmethod
    def _raise_for_status(response: HttpResponse) -> None:
        if response.status_code == 200:
            return
        if response.status_code in {418, 429}:
            retry_after = _nonnegative_integer(response.header("Retry-After"))
            raise BinanceCandleError(
                BinanceCandleErrorCode.RATE_LIMITED,
                f"Binance returned HTTP {response.status_code}",
                retry_after_seconds=retry_after,
                retryable=retry_after is not None,
            )
        if 500 <= response.status_code <= 599:
            raise BinanceCandleError(
                BinanceCandleErrorCode.PROVIDER_UNAVAILABLE,
                f"Binance returned HTTP {response.status_code}",
            )
        raise BinanceCandleError(
            BinanceCandleErrorCode.PROVIDER_REJECTED,
            f"Binance returned HTTP {response.status_code}",
        )

    @staticmethod
    def _parse_payload(body: bytes) -> tuple[_BinanceKline, ...]:
        try:
            payload: object = json.loads(body.decode("utf-8"))
        except (ValueError, RecursionError) as error:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_PAYLOAD,
                "response could not be decoded as bounded UTF-8 JSON",
            ) from error
        if not isinstance(payload, list):
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_PAYLOAD,
                "response root must be an array",
            )

        rows: list[_BinanceKline] = []
        for row_number, candidate in enumerate(payload):
            if not isinstance(candidate, list) or len(candidate) != 12:
                raise BinanceCandleError(
                    BinanceCandleErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} must contain exactly 12 values",
                )
            rows.append(
                _BinanceKline(
                    open_time_ms=_required_integer(candidate, 0, row_number),
                    open=_required_string(candidate, 1, row_number),
                    high=_required_string(candidate, 2, row_number),
                    low=_required_string(candidate, 3, row_number),
                    close=_required_string(candidate, 4, row_number),
                    base_volume=_required_string(candidate, 5, row_number),
                    provider_close_time_ms=_required_integer(candidate, 6, row_number),
                    quote_volume=_required_string(candidate, 7, row_number),
                    trade_count=_required_integer(candidate, 8, row_number),
                    taker_buy_base_volume=_required_string(candidate, 9, row_number),
                    taker_buy_quote_volume=_required_string(candidate, 10, row_number),
                    ignored=_required_string(candidate, 11, row_number),
                )
            )
        return tuple(rows)

    @staticmethod
    def _raw_payload(
        *,
        body: bytes,
        request: HistoricalCandleRequest,
        query: dict[str, str],
        url: str,
        observed_at: datetime,
        processed_at: datetime,
    ) -> RawMarketPayload:
        payload_digest = sha256(body).hexdigest()
        endpoint_path = url.removeprefix("https://").split("/", maxsplit=1)[-1]
        request_identity = json.dumps(
            (
                BINANCE_SOURCE,
                BINANCE_VENUE,
                endpoint_path,
                tuple(sorted(query.items())),
                payload_digest,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return RawMarketPayload(
            record_id=uuid5(NAMESPACE_URL, request_identity),
            source=BINANCE_SOURCE,
            venue=BINANCE_VENUE,
            observed_at=observed_at,
            processed_at=processed_at,
            payload_sha256=payload_digest,
            payload=body,
            lineage=(
                f"binance-public-rest:{endpoint_path}:{request.provider_symbol}:"
                f"{request.timeframe.value}:{_to_epoch_milliseconds(request.window_start)}:"
                f"{_to_epoch_milliseconds(request.window_end_exclusive)}",
            ),
        )

    @staticmethod
    def _canonicalize(
        *,
        row: _BinanceKline,
        row_number: int,
        request: HistoricalCandleRequest,
        url: str,
        observed_at: datetime,
        processed_at: datetime,
        raw_payload_reference: str,
    ) -> CanonicalCandle:
        open_time = _from_epoch_milliseconds(row.open_time_ms)
        close_time = open_time + request.timeframe.duration
        expected_provider_close = _to_epoch_milliseconds(close_time) - 1
        if row.provider_close_time_ms != expected_provider_close:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_PAYLOAD,
                f"row {row_number} close time does not match the requested timeframe",
            )
        if row.trade_count < 0:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_PAYLOAD,
                f"row {row_number} trade count must be non-negative",
            )

        record_identity = json.dumps(
            (
                BINANCE_SOURCE,
                BINANCE_VENUE,
                request.instrument,
                request.provider_symbol,
                request.instrument_type.value,
                request.timeframe.value,
                row.raw_values,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        endpoint_path = url.removeprefix("https://").split("/", maxsplit=1)[-1]
        try:
            return CanonicalCandle(
                record_id=uuid5(NAMESPACE_URL, record_identity),
                source=BINANCE_SOURCE,
                venue=BINANCE_VENUE,
                instrument=request.instrument,
                instrument_type=request.instrument_type,
                timeframe=request.timeframe,
                open_time=open_time,
                close_time=close_time,
                observed_at=observed_at,
                processed_at=processed_at,
                open=Decimal(row.open),
                high=Decimal(row.high),
                low=Decimal(row.low),
                close=Decimal(row.close),
                base_volume=Decimal(row.base_volume),
                quote_volume=Decimal(row.quote_volume),
                trade_count=row.trade_count,
                provider_sequence=row.open_time_ms,
                lineage=(
                    raw_payload_reference,
                    f"binance-public-rest:{endpoint_path}:"
                    f"{request.provider_symbol}:{request.timeframe.value}:{row.open_time_ms}",
                ),
            )
        except (InvalidOperation, ValidationError, ValueError) as error:
            raise BinanceCandleError(
                BinanceCandleErrorCode.INVALID_PAYLOAD,
                f"row {row_number} violates the canonical candle contract",
            ) from error


def _required_integer(row: list[object], index: int, row_number: int) -> int:
    value = row[index]
    if not isinstance(value, int) or isinstance(value, bool):
        raise BinanceCandleError(
            BinanceCandleErrorCode.INVALID_PAYLOAD,
            f"row {row_number} value {index} must be an integer",
        )
    return value


def _required_string(row: list[object], index: int, row_number: int) -> str:
    value = row[index]
    if not isinstance(value, str):
        raise BinanceCandleError(
            BinanceCandleErrorCode.INVALID_PAYLOAD,
            f"row {row_number} value {index} must be a string",
        )
    return value


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
