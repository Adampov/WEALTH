"""Public, read-only Coinbase Exchange candle adapter."""

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

from pydantic import ValidationError

from wealth.domain.market import (
    CandleTimeframe,
    CanonicalCandle,
    InstrumentType,
    RawMarketPayload,
)
from wealth.ports.foundation import Clock, ClockContractError, require_utc_clock
from wealth.ports.http import HttpResponse, HttpTransportError, PublicHttpClient
from wealth.ports.market import (
    CandleFetchBatch,
    HistoricalCandleRequest,
    HistoricalCandleSourceError,
)

COINBASE_SOURCE = "coinbase.exchange-public-rest"
COINBASE_VENUE = "COINBASE"
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"
MAX_COINBASE_CANDLES = 300
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
COINBASE_PRODUCT_ID = re.compile(r"^[A-Z0-9]{1,31}-[A-Z0-9]{1,31}$")
COINBASE_GRANULARITY_SECONDS = {
    CandleTimeframe.ONE_MINUTE: 60,
    CandleTimeframe.FIVE_MINUTES: 300,
    CandleTimeframe.FIFTEEN_MINUTES: 900,
    CandleTimeframe.ONE_HOUR: 3_600,
    CandleTimeframe.ONE_DAY: 86_400,
}


class CoinbaseCandleErrorCode(StrEnum):
    """Machine-readable public-candle ingestion failures."""

    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_INSTRUMENT = "unsupported_instrument"
    UNSUPPORTED_TIMEFRAME = "unsupported_timeframe"
    TRANSPORT_FAILURE = "transport_failure"
    RATE_LIMITED = "rate_limited"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_REJECTED = "provider_rejected"
    INVALID_PAYLOAD = "invalid_payload"


class CoinbaseCandleError(HistoricalCandleSourceError):
    """Fail explicitly when Coinbase data cannot become canonical records."""

    def __init__(
        self,
        code: CoinbaseCandleErrorCode,
        detail: str,
        *,
        retry_after_seconds: int | None = None,
        retryable: bool | None = None,
    ) -> None:
        self.code = code
        default_retryable = code in {
            CoinbaseCandleErrorCode.TRANSPORT_FAILURE,
            CoinbaseCandleErrorCode.RATE_LIMITED,
            CoinbaseCandleErrorCode.PROVIDER_UNAVAILABLE,
        }
        super().__init__(
            code.value,
            detail,
            retryable=default_retryable if retryable is None else retryable,
            retry_after_seconds=retry_after_seconds,
        )


@dataclass(frozen=True, slots=True)
class _CoinbaseCandle:
    """Structurally validated positional Coinbase candle."""

    open_time_seconds: int
    low: str
    high: str
    open: str
    close: str
    base_volume: str

    @property
    def raw_values(self) -> tuple[int | str, ...]:
        """Return exact provider values for deterministic record identity."""

        return (
            self.open_time_seconds,
            self.low,
            self.high,
            self.open,
            self.close,
            self.base_volume,
        )


@dataclass(frozen=True, slots=True)
class CoinbasePublicCandleSource:
    """Fetch bounded closed Spot candles from Coinbase Exchange."""

    http: PublicHttpClient
    clock: Clock
    timeout_seconds: float = 10.0
    products_url: str = COINBASE_PRODUCTS_URL

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not self.products_url.startswith("https://"):
            raise ValueError("Coinbase public endpoint must use HTTPS")

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        """Fetch, bound, and normalize one closed Coinbase candle window."""

        request_started_at = self._clock_now()
        self._validate_request(request, request_started_at)
        url = f"{self.products_url.rstrip('/')}/{request.provider_symbol}/candles"
        query = self._query_for(request)
        try:
            response = self.http.get(
                url=url,
                query=query,
                timeout_seconds=self.timeout_seconds,
            )
        except HttpTransportError as error:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.TRANSPORT_FAILURE,
                "public candle request did not receive a response",
            ) from error

        observed_at = self._clock_now()
        if observed_at < request_started_at:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
                "clock regressed while observing the provider response",
            )
        self._raise_for_status(response)
        rows = self._parse_payload(response.body)
        processed_at = self._clock_now()
        if processed_at < observed_at:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
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

        records: list[CanonicalCandle] = []
        for row_number, row in enumerate(rows):
            record = self._canonicalize(
                row=row,
                row_number=row_number,
                request=request,
                url=url,
                observed_at=observed_at,
                processed_at=processed_at,
                raw_payload_reference=raw_payload.lineage_reference,
            )
            if record.open_time < request.window_start:
                continue
            if record.open_time >= request.window_end_exclusive:
                raise CoinbaseCandleError(
                    CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} begins after the requested window",
                )
            records.append(record)
        records.sort(key=lambda record: (record.open_time, str(record.record_id)))

        return CandleFetchBatch(
            request=request,
            source=COINBASE_SOURCE,
            venue=COINBASE_VENUE,
            observed_at=observed_at,
            processed_at=processed_at,
            raw_payload=raw_payload,
            records=tuple(records),
        )

    def _clock_now(self) -> datetime:
        try:
            return require_utc_clock(self.clock.now())
        except ClockContractError as error:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
                str(error),
            ) from error

    @staticmethod
    def _validate_request(
        request: HistoricalCandleRequest,
        now: datetime,
    ) -> None:
        if request.window_end_exclusive > now:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
                "requested window must contain only candles closed before the request",
            )
        if request.expected_count > MAX_COINBASE_CANDLES:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
                f"requested window exceeds the {MAX_COINBASE_CANDLES}-candle provider limit",
            )
        if request.instrument_type is not InstrumentType.SPOT:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.UNSUPPORTED_INSTRUMENT,
                "Coinbase Exchange public candles support Spot requests only",
            )
        if request.timeframe not in COINBASE_GRANULARITY_SECONDS:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.UNSUPPORTED_TIMEFRAME,
                f"unsupported Coinbase timeframe: {request.timeframe.value}",
            )
        if COINBASE_PRODUCT_ID.fullmatch(request.provider_symbol) is None:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_REQUEST,
                "provider_symbol must be an explicit uppercase Coinbase product ID",
            )

    @staticmethod
    def _query_for(request: HistoricalCandleRequest) -> dict[str, str]:
        granularity = COINBASE_GRANULARITY_SECONDS.get(request.timeframe)
        if granularity is None:
            raise AssertionError("validated Coinbase timeframe must have a granularity")
        return {
            "start": _rfc3339(request.window_start),
            "end": _rfc3339(request.window_end_exclusive),
            "granularity": str(granularity),
        }

    @staticmethod
    def _raise_for_status(response: HttpResponse) -> None:
        if response.status_code == 200:
            return
        if response.status_code == 429:
            header_value = response.header("Retry-After")
            retry_after = _nonnegative_integer(header_value)
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.RATE_LIMITED,
                "Coinbase returned HTTP 429",
                retry_after_seconds=retry_after,
                retryable=header_value is None or retry_after is not None,
            )
        if 500 <= response.status_code <= 599:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.PROVIDER_UNAVAILABLE,
                f"Coinbase returned HTTP {response.status_code}",
            )
        raise CoinbaseCandleError(
            CoinbaseCandleErrorCode.PROVIDER_REJECTED,
            f"Coinbase returned HTTP {response.status_code}",
        )

    @staticmethod
    def _parse_payload(body: bytes) -> tuple[_CoinbaseCandle, ...]:
        try:
            payload: object = json.loads(
                body.decode("utf-8"),
                parse_float=str,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                "response was not valid UTF-8 JSON",
            ) from error
        if not isinstance(payload, list):
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                "response root must be an array",
            )
        if len(payload) > MAX_COINBASE_CANDLES:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                f"response exceeded the {MAX_COINBASE_CANDLES}-candle provider limit",
            )

        rows: list[_CoinbaseCandle] = []
        for row_number, candidate in enumerate(payload):
            if not isinstance(candidate, list) or len(candidate) != 6:
                raise CoinbaseCandleError(
                    CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                    f"row {row_number} must contain exactly 6 values",
                )
            rows.append(
                _CoinbaseCandle(
                    open_time_seconds=_required_integer(candidate, 0, row_number),
                    low=_required_decimal_text(candidate, 1, row_number),
                    high=_required_decimal_text(candidate, 2, row_number),
                    open=_required_decimal_text(candidate, 3, row_number),
                    close=_required_decimal_text(candidate, 4, row_number),
                    base_volume=_required_decimal_text(candidate, 5, row_number),
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
                COINBASE_SOURCE,
                COINBASE_VENUE,
                endpoint_path,
                tuple(sorted(query.items())),
                payload_digest,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )
        return RawMarketPayload(
            record_id=uuid5(NAMESPACE_URL, request_identity),
            source=COINBASE_SOURCE,
            venue=COINBASE_VENUE,
            observed_at=observed_at,
            processed_at=processed_at,
            payload_sha256=payload_digest,
            payload=body,
            lineage=(
                f"coinbase-exchange-public-rest:{endpoint_path}:"
                f"{request.timeframe.value}:{_to_epoch_seconds(request.window_start)}:"
                f"{_to_epoch_seconds(request.window_end_exclusive)}",
            ),
        )

    @staticmethod
    def _canonicalize(
        *,
        row: _CoinbaseCandle,
        row_number: int,
        request: HistoricalCandleRequest,
        url: str,
        observed_at: datetime,
        processed_at: datetime,
        raw_payload_reference: str,
    ) -> CanonicalCandle:
        open_time = _from_epoch_seconds(row.open_time_seconds)
        record_identity = json.dumps(
            (
                COINBASE_SOURCE,
                COINBASE_VENUE,
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
                source=COINBASE_SOURCE,
                venue=COINBASE_VENUE,
                instrument=request.instrument,
                instrument_type=request.instrument_type,
                timeframe=request.timeframe,
                open_time=open_time,
                close_time=open_time + request.timeframe.duration,
                observed_at=observed_at,
                processed_at=processed_at,
                open=Decimal(row.open),
                high=Decimal(row.high),
                low=Decimal(row.low),
                close=Decimal(row.close),
                base_volume=Decimal(row.base_volume),
                quote_volume=None,
                trade_count=None,
                provider_sequence=row.open_time_seconds,
                lineage=(
                    raw_payload_reference,
                    f"coinbase-exchange-public-rest:{endpoint_path}:"
                    f"{request.timeframe.value}:{row.open_time_seconds}",
                ),
            )
        except (InvalidOperation, ValidationError, ValueError) as error:
            raise CoinbaseCandleError(
                CoinbaseCandleErrorCode.INVALID_PAYLOAD,
                f"row {row_number} violates the canonical candle contract",
            ) from error


def _required_integer(row: list[object], index: int, row_number: int) -> int:
    value = row[index]
    if not isinstance(value, int) or isinstance(value, bool):
        raise CoinbaseCandleError(
            CoinbaseCandleErrorCode.INVALID_PAYLOAD,
            f"row {row_number} value {index} must be an integer",
        )
    return value


def _required_decimal_text(
    row: list[object],
    index: int,
    row_number: int,
) -> str:
    value = row[index]
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise CoinbaseCandleError(
            CoinbaseCandleErrorCode.INVALID_PAYLOAD,
            f"row {row_number} value {index} must be a JSON number",
        )
    return str(value)


def _rfc3339(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _to_epoch_seconds(value: datetime) -> int:
    delta = value.astimezone(UTC) - UTC_EPOCH
    return delta.days * 86_400 + delta.seconds


def _from_epoch_seconds(value: int) -> datetime:
    return UTC_EPOCH + timedelta(seconds=value)


def _nonnegative_integer(value: str | None) -> int | None:
    if value is None or len(value) > 10 or not value.isascii() or not value.isdecimal():
        return None
    parsed = int(value)
    return parsed if parsed >= 0 else None
