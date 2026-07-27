"""Offline contracts for the versioned public-provider schema fixture corpus."""

import json
import re
import shutil
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Literal, cast

import pytest
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from wealth.adapters.binance import (
    BINANCE_SOURCE,
    BINANCE_SPOT_KLINES_URL,
    BINANCE_USDM_KLINES_URL,
    BINANCE_VENUE,
    BinanceCandleError,
    BinanceCandleErrorCode,
    BinancePublicCandleSource,
)
from wealth.adapters.binance_order_flow import (
    BINANCE_ORDER_FLOW_SOURCE,
    BINANCE_ORDER_FLOW_VENUE,
    BINANCE_SPOT_AGG_TRADES_URL,
    BINANCE_USDM_AGG_TRADES_URL,
    MAX_BINANCE_AGGREGATE_TRADES,
    BinanceAggregateTradeError,
    BinanceAggregateTradeErrorCode,
    BinancePublicAggregateTradeSource,
)
from wealth.adapters.coinbase import (
    COINBASE_PRODUCTS_URL,
    COINBASE_SOURCE,
    COINBASE_VENUE,
    CoinbaseCandleError,
    CoinbaseCandleErrorCode,
    CoinbasePublicCandleSource,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.order_flow import AggressorSide, CanonicalTrade
from wealth.ports.http import HttpResponse
from wealth.ports.market import HistoricalCandleRequest
from wealth.ports.order_flow import PublicTradeWindowRequest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CORPUS_ROOT = REPOSITORY_ROOT / "tests" / "fixtures" / "public_provider_schema" / "v1"
MANIFEST_PATH = CORPUS_ROOT / "manifest.json"
MAX_FIXTURE_BYTES = 1_024
WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
WINDOW_END = WINDOW_START + timedelta(minutes=1)
REQUEST_TIME = WINDOW_END + timedelta(hours=1)
OBSERVED_AT = REQUEST_TIME + timedelta(seconds=1)
PROCESSED_AT = REQUEST_TIME + timedelta(seconds=2)
EXPECTED_IDENTITIES = (
    "binance.spot.candles",
    "binance.usdm.candles",
    "coinbase.exchange.spot.candles",
    "binance.spot.aggregate-trades",
    "binance.usdm.aggregate-trades",
)
EXPECTED_PATHS = (
    "binance_spot_candles.json",
    "binance_usdm_candles.json",
    "coinbase_exchange_spot_candles.json",
    "binance_spot_aggregate_trades.json",
    "binance_usdm_aggregate_trades.json",
)
EXPECTED_REQUIRED_FIELDS = ("T", "a", "f", "l", "m", "p", "q")
EXPECTED_OPTIONAL_FIELDS = ("M", "nq")
EXPECTED_ENTRY_CONTRACTS: dict[str, tuple[object, ...]] = {
    "binance.spot.candles": (
        "binance_spot_candles.json",
        "binance",
        "candles",
        "spot",
        "GET /api/v3/klines",
        "positional-row",
        12,
        (),
        (),
        (),
        "https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market",
    ),
    "binance.usdm.candles": (
        "binance_usdm_candles.json",
        "binance",
        "candles",
        "usd-m",
        "GET /fapi/v1/klines",
        "positional-row",
        12,
        (),
        (),
        (),
        "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-"
        "usd-s-m-futures/api/rest-api/market-data#klinecandlestick-data",
    ),
    "coinbase.exchange.spot.candles": (
        "coinbase_exchange_spot_candles.json",
        "coinbase-exchange",
        "candles",
        "spot",
        "GET /products/{product_id}/candles",
        "positional-row",
        6,
        (),
        (),
        (),
        "https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/"
        "get-product-candles",
    ),
    "binance.spot.aggregate-trades": (
        "binance_spot_aggregate_trades.json",
        "binance",
        "aggregate-trades",
        "spot",
        "GET /api/v3/aggTrades",
        "object-row",
        None,
        EXPECTED_REQUIRED_FIELDS,
        EXPECTED_OPTIONAL_FIELDS,
        ("M",),
        "https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market",
    ),
    "binance.usdm.aggregate-trades": (
        "binance_usdm_aggregate_trades.json",
        "binance",
        "aggregate-trades",
        "usd-m",
        "GET /fapi/v1/aggTrades",
        "object-row",
        None,
        EXPECTED_REQUIRED_FIELDS,
        EXPECTED_OPTIONAL_FIELDS,
        ("nq",),
        "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-"
        "usd-s-m-futures/api/rest-api/market-data#compressed-aggregate-trades-list",
    ),
}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class ManifestError(ValueError):
    """Fail one untrusted fixture-corpus manifest without partial acceptance."""


class ManifestEntry(BaseModel):
    """One strict fixture identity and its reviewed local contract."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    identity: Literal[
        "binance.spot.candles",
        "binance.usdm.candles",
        "coinbase.exchange.spot.candles",
        "binance.spot.aggregate-trades",
        "binance.usdm.aggregate-trades",
    ]
    path: str
    sha256: str
    provider: Literal["binance", "coinbase-exchange"]
    dataset: Literal["candles", "aggregate-trades"]
    market: Literal["spot", "usd-m"]
    request_variant: Literal[
        "GET /api/v3/klines",
        "GET /fapi/v1/klines",
        "GET /products/{product_id}/candles",
        "GET /api/v3/aggTrades",
        "GET /fapi/v1/aggTrades",
    ]
    shape_type: Literal["positional-row", "object-row"]
    positional_width: Literal[6, 12] | None
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...]
    present_optional_fields: tuple[str, ...]
    official_contract_reference: str
    review_date_utc: Literal["2026-07-27"]
    review_status: Literal["reviewed"]

    @model_validator(mode="after")
    def shape_declaration_is_exact(self) -> "ManifestEntry":
        """Keep positional and object declarations mutually exclusive and exact."""

        if self.shape_type == "positional-row":
            if (
                self.positional_width is None
                or self.required_fields
                or self.optional_fields
                or self.present_optional_fields
            ):
                raise ValueError("positional fixture shape is inconsistent")
            return self
        if (
            self.positional_width is not None
            or self.required_fields != EXPECTED_REQUIRED_FIELDS
            or self.optional_fields != EXPECTED_OPTIONAL_FIELDS
            or not set(self.present_optional_fields) <= set(self.optional_fields)
            or len(self.present_optional_fields) != len(set(self.present_optional_fields))
        ):
            raise ValueError("object fixture shape is inconsistent")
        return self


class FixtureManifest(BaseModel):
    """The strict, version-one fixture manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1]
    max_fixture_bytes: Literal[1024]
    fixtures: tuple[ManifestEntry, ...]


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_nonfinite_constant(value: str) -> object:
    raise ManifestError(f"non-finite JSON constant: {value}")


def _load_json_strict(raw: bytes) -> object:
    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ManifestError("fixture JSON is not strict bounded UTF-8 JSON") from error


def load_corpus(root: Path) -> tuple[FixtureManifest, dict[str, bytes]]:
    """Validate a complete local corpus before returning any fixture bytes."""

    manifest_path = root / "manifest.json"
    try:
        manifest_raw = manifest_path.read_bytes()
        parsed_manifest = _load_json_strict(manifest_raw)
        if not isinstance(parsed_manifest, dict):
            raise ManifestError("fixture manifest root must be an object")
        if type(parsed_manifest.get("schema_version")) is not int:
            raise ManifestError("schema_version must be an exact integer")
        if type(parsed_manifest.get("max_fixture_bytes")) is not int:
            raise ManifestError("max_fixture_bytes must be an exact integer")
        manifest = FixtureManifest.model_validate_json(manifest_raw)
    except (OSError, ValidationError, ManifestError) as error:
        raise ManifestError("fixture manifest is invalid") from error

    if len(manifest.fixtures) != 5:
        raise ManifestError("fixture manifest must contain exactly five entries")
    identities = tuple(entry.identity for entry in manifest.fixtures)
    paths = tuple(entry.path for entry in manifest.fixtures)
    if len(identities) != len(set(identities)) or identities != EXPECTED_IDENTITIES:
        raise ManifestError("fixture identities are not unique and exact")
    if len(paths) != len(set(paths)) or paths != EXPECTED_PATHS:
        raise ManifestError("fixture paths are not unique and exact")
    for entry in manifest.fixtures:
        actual_contract = (
            entry.path,
            entry.provider,
            entry.dataset,
            entry.market,
            entry.request_variant,
            entry.shape_type,
            entry.positional_width,
            entry.required_fields,
            entry.optional_fields,
            entry.present_optional_fields,
            entry.official_contract_reference,
        )
        if actual_contract != EXPECTED_ENTRY_CONTRACTS[entry.identity]:
            raise ManifestError("fixture identity metadata is not the reviewed exact contract")

    expected_files = {"manifest.json", *paths}
    actual_files = {
        candidate.relative_to(root).as_posix()
        for candidate in root.rglob("*")
        if candidate.is_file() or candidate.is_symlink()
    }
    if actual_files != expected_files:
        raise ManifestError("fixture directory and manifest are not one-to-one")

    bodies: dict[str, bytes] = {}
    for entry in manifest.fixtures:
        relative = PurePosixPath(entry.path)
        if (
            relative.is_absolute()
            or len(relative.parts) != 1
            or relative.name != entry.path
            or "\\" in entry.path
        ):
            raise ManifestError("fixture path must be one local filename")
        fixture_path = root / entry.path
        if fixture_path.is_symlink():
            raise ManifestError("fixture path must not be a symbolic link")
        try:
            body = fixture_path.read_bytes()
        except OSError as error:
            raise ManifestError("fixture file is unavailable") from error
        if len(body) > manifest.max_fixture_bytes:
            raise ManifestError("fixture exceeds the declared byte bound")
        if SHA256_PATTERN.fullmatch(entry.sha256) is None:
            raise ManifestError("fixture digest is not lowercase SHA-256")
        if sha256(body).hexdigest() != entry.sha256:
            raise ManifestError("fixture digest does not match exact bytes")
        payload = _load_json_strict(body)
        if not isinstance(payload, list) or len(payload) != 1:
            raise ManifestError("fixture must contain exactly one row")
        row = payload[0]
        if entry.shape_type == "positional-row":
            if not isinstance(row, list) or len(row) != entry.positional_width:
                raise ManifestError("fixture positional width does not match its declaration")
        else:
            if not isinstance(row, dict):
                raise ManifestError("fixture object row does not match its declaration")
            actual_fields = set(row)
            declared_fields = set(entry.required_fields) | set(entry.present_optional_fields)
            if actual_fields != declared_fields:
                raise ManifestError("fixture object fields do not match their declaration")
        bodies[entry.identity] = body
    return manifest, bodies


def _copy_corpus(destination: Path) -> Path:
    copied = destination / "v1"
    shutil.copytree(CORPUS_ROOT, copied)
    return copied


def _manifest_payload(root: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads((root / "manifest.json").read_text("utf-8")))


def _entries(payload: dict[str, object]) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], payload["fixtures"])


def _write_manifest(root: Path, payload: dict[str, object]) -> None:
    (root / "manifest.json").write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def test_manifest_and_exact_bytes_cover_the_five_reviewed_variants() -> None:
    manifest, bodies = load_corpus(CORPUS_ROOT)

    assert manifest.schema_version == 1
    assert manifest.max_fixture_bytes == MAX_FIXTURE_BYTES
    assert tuple(bodies) == EXPECTED_IDENTITIES
    assert tuple(entry.path for entry in manifest.fixtures) == EXPECTED_PATHS
    assert tuple(entry.review_status for entry in manifest.fixtures) == ("reviewed",) * 5
    assert all(
        entry.official_contract_reference.startswith("https://") for entry in manifest.fixtures
    )
    assert all(len(body) <= MAX_FIXTURE_BYTES for body in bodies.values())
    spot_trade, usdm_trade = manifest.fixtures[3:]
    assert spot_trade.optional_fields == EXPECTED_OPTIONAL_FIELDS
    assert usdm_trade.optional_fields == EXPECTED_OPTIONAL_FIELDS
    assert spot_trade.present_optional_fields == ("M",)
    assert usdm_trade.present_optional_fields == ("nq",)


@pytest.mark.parametrize(
    "case",
    [
        "missing-root-key",
        "unknown-root-key",
        "missing-entry-key",
        "unknown-entry-key",
        "wrong-version",
        "boolean-version",
        "wrong-status",
        "wrong-runtime-type",
        "wrong-provider",
        "wrong-request-variant",
        "wrong-official-reference",
        "missing-entry",
        "extra-entry",
        "duplicate-identity",
        "duplicate-path",
        "absolute-path",
        "traversal-path",
        "wrong-directory",
        "digest-mismatch",
        "missing-file",
        "extra-file",
        "nested-extra-file",
        "symlink-fixture",
        "oversized-fixture",
        "nonfinite-fixture",
        "fixture-width-mismatch",
        "fixture-field-set-mismatch",
    ],
)
def test_manifest_mutations_fail_closed(tmp_path: Path, case: str) -> None:
    root = _copy_corpus(tmp_path)
    payload = _manifest_payload(root)
    entries = _entries(payload)

    if case == "missing-root-key":
        del payload["schema_version"]
    elif case == "unknown-root-key":
        payload["unknown"] = "rejected"
    elif case == "missing-entry-key":
        del entries[0]["provider"]
    elif case == "unknown-entry-key":
        entries[0]["unknown"] = "rejected"
    elif case == "wrong-version":
        payload["schema_version"] = 2
    elif case == "boolean-version":
        payload["schema_version"] = True
    elif case == "wrong-status":
        entries[0]["review_status"] = "draft"
    elif case == "wrong-runtime-type":
        entries[0]["path"] = 7
    elif case == "wrong-provider":
        entries[0]["provider"] = "coinbase-exchange"
    elif case == "wrong-request-variant":
        entries[0]["request_variant"] = "GET /fapi/v1/klines"
    elif case == "wrong-official-reference":
        entries[0]["official_contract_reference"] = "https://example.invalid/contract"
    elif case == "missing-entry":
        entries.pop()
    elif case == "extra-entry":
        entries.append(dict(entries[0]))
    elif case == "duplicate-identity":
        entries[1]["identity"] = entries[0]["identity"]
    elif case == "duplicate-path":
        entries[1]["path"] = entries[0]["path"]
    elif case == "absolute-path":
        entries[0]["path"] = "/tmp/provider.json"
    elif case == "traversal-path":
        entries[0]["path"] = "../provider.json"
    elif case == "wrong-directory":
        entries[0]["path"] = "nested/provider.json"
    elif case == "digest-mismatch":
        entries[0]["sha256"] = "0" * 64
    elif case == "missing-file":
        (root / EXPECTED_PATHS[0]).unlink()
    elif case == "extra-file":
        (root / "unexpected.json").write_text("[]\n", encoding="utf-8")
    elif case == "nested-extra-file":
        nested = root / "nested"
        nested.mkdir()
        (nested / "unexpected.json").write_text("[]\n", encoding="utf-8")
    elif case == "symlink-fixture":
        fixture_path = root / EXPECTED_PATHS[0]
        exact_body = fixture_path.read_bytes()
        outside_target = tmp_path / "outside.json"
        outside_target.write_bytes(exact_body)
        fixture_path.unlink()
        fixture_path.symlink_to(outside_target)
    elif case == "oversized-fixture":
        oversized_payload = cast(
            list[list[object]], json.loads((root / EXPECTED_PATHS[0]).read_bytes())
        )
        oversized_payload[0][11] = "x" * (MAX_FIXTURE_BYTES + 1)
        oversized = _encoded(oversized_payload)
        (root / EXPECTED_PATHS[0]).write_bytes(oversized)
        entries[0]["sha256"] = sha256(oversized).hexdigest()
    elif case == "nonfinite-fixture":
        fixture_path = root / EXPECTED_PATHS[0]
        nonfinite = fixture_path.read_bytes().replace(b'"100.12500000"', b"NaN", 1)
        fixture_path.write_bytes(nonfinite)
        entries[0]["sha256"] = sha256(nonfinite).hexdigest()
    elif case == "fixture-width-mismatch":
        fixture_path = root / EXPECTED_PATHS[0]
        width_payload = cast(list[list[object]], json.loads(fixture_path.read_bytes()))
        width_payload[0].pop()
        wrong_width = _encoded(width_payload)
        fixture_path.write_bytes(wrong_width)
        entries[0]["sha256"] = sha256(wrong_width).hexdigest()
    elif case == "fixture-field-set-mismatch":
        fixture_path = root / EXPECTED_PATHS[3]
        object_payload = cast(list[dict[str, object]], json.loads(fixture_path.read_bytes()))
        object_payload[0]["unexpected"] = "schema-drift"
        wrong_fields = _encoded(object_payload)
        fixture_path.write_bytes(wrong_fields)
        entries[3]["sha256"] = sha256(wrong_fields).hexdigest()
    else:
        raise AssertionError(f"unhandled manifest mutation: {case}")

    if case not in {"missing-file", "extra-file", "nested-extra-file", "symlink-fixture"}:
        _write_manifest(root, payload)
    with pytest.raises(ManifestError):
        load_corpus(root)


@pytest.mark.parametrize("case", ["duplicate-key", "nan", "infinity"])
def test_non_strict_manifest_json_fails_closed(tmp_path: Path, case: str) -> None:
    root = _copy_corpus(tmp_path)
    manifest_path = root / "manifest.json"
    raw = manifest_path.read_bytes()

    if case == "duplicate-key":
        raw = raw.replace(
            b'"schema_version": 1,',
            b'"schema_version": 1, "schema_version": 1,',
            1,
        )
    elif case == "nan":
        raw = raw.replace(b'"max_fixture_bytes": 1024', b'"max_fixture_bytes": NaN', 1)
    elif case == "infinity":
        raw = raw.replace(b'"max_fixture_bytes": 1024', b'"max_fixture_bytes": Infinity', 1)
    else:
        raise AssertionError(f"unhandled JSON mutation: {case}")
    manifest_path.write_bytes(raw)

    with pytest.raises(ManifestError):
        load_corpus(root)


class SequenceClock:
    """Return explicit UTC timestamps in call order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now(self) -> datetime:
        return next(self._values)


class StubHttpClient:
    """Return exact fixture bytes while retaining the complete request."""

    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        self.calls.append((url, dict(query), timeout_seconds))
        return HttpResponse(status_code=200, headers=(), body=self.body)


def _clock() -> SequenceClock:
    return SequenceClock(REQUEST_TIME, OBSERVED_AT, PROCESSED_AT)


def _candle_request(instrument_type: InstrumentType) -> HistoricalCandleRequest:
    provider_symbol = "BTC-USD" if instrument_type is InstrumentType.SPOT else "BTCUSDT"
    instrument = "BTC-USD" if instrument_type is InstrumentType.SPOT else "BTC-USDT"
    return HistoricalCandleRequest(
        instrument=instrument,
        provider_symbol=provider_symbol,
        instrument_type=instrument_type,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def _binance_candle_request(instrument_type: InstrumentType) -> HistoricalCandleRequest:
    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=instrument_type,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def _trade_request(instrument_type: InstrumentType) -> PublicTradeWindowRequest:
    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=instrument_type,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END,
    )


def _assert_raw_lineage(
    *,
    body: bytes,
    raw_body: bytes,
    raw_digest: str,
    raw_reference: str,
    record_lineage: tuple[str, ...],
) -> None:
    assert raw_body == body
    assert raw_digest == sha256(body).hexdigest()
    assert raw_reference in record_lineage


def test_binance_spot_candle_fixture_flows_through_the_active_request_path() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    body = bodies["binance.spot.candles"]
    http = StubHttpClient(body)
    source = BinancePublicCandleSource(http=http, clock=_clock())

    batch = source.fetch(_binance_candle_request(InstrumentType.SPOT))

    assert http.calls == [
        (
            BINANCE_SPOT_KLINES_URL,
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": "1784887200000",
                "endTime": "1784887259999",
                "limit": "1",
                "timeZone": "0",
            },
            10.0,
        )
    ]
    assert batch.source == BINANCE_SOURCE
    assert batch.venue == BINANCE_VENUE
    assert batch.observed_at == OBSERVED_AT
    assert batch.processed_at == PROCESSED_AT
    assert len(batch.records) == 1
    record = batch.records[0]
    assert record.instrument_type is InstrumentType.SPOT
    assert record.open_time == WINDOW_START
    assert record.close_time == WINDOW_END
    assert record.open == Decimal("100.12500000")
    assert record.high == Decimal("101.50000000")
    assert record.low == Decimal("99.87500000")
    assert record.close == Decimal("101.25000000")
    assert record.trade_count == 42
    _assert_raw_lineage(
        body=body,
        raw_body=batch.raw_payload.payload,
        raw_digest=batch.raw_payload.payload_sha256,
        raw_reference=batch.raw_payload.lineage_reference,
        record_lineage=record.lineage,
    )


def test_binance_usdm_candle_fixture_flows_through_the_active_request_path() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    body = bodies["binance.usdm.candles"]
    http = StubHttpClient(body)
    source = BinancePublicCandleSource(http=http, clock=_clock())

    batch = source.fetch(_binance_candle_request(InstrumentType.PERPETUAL_FUTURE))

    assert http.calls == [
        (
            BINANCE_USDM_KLINES_URL,
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": "1784887200000",
                "endTime": "1784887259999",
                "limit": "1",
            },
            10.0,
        )
    ]
    assert batch.source == BINANCE_SOURCE
    assert batch.venue == BINANCE_VENUE
    record = batch.records[0]
    assert record.instrument_type is InstrumentType.PERPETUAL_FUTURE
    assert record.open == Decimal("200.12500000")
    assert record.close == Decimal("201.25000000")
    assert record.quote_volume == Decimal("1764.00000000")
    _assert_raw_lineage(
        body=body,
        raw_body=batch.raw_payload.payload,
        raw_digest=batch.raw_payload.payload_sha256,
        raw_reference=batch.raw_payload.lineage_reference,
        record_lineage=record.lineage,
    )


def test_coinbase_spot_candle_fixture_flows_through_the_active_request_path() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    body = bodies["coinbase.exchange.spot.candles"]
    http = StubHttpClient(body)
    source = CoinbasePublicCandleSource(http=http, clock=_clock())

    batch = source.fetch(_candle_request(InstrumentType.SPOT))

    assert http.calls == [
        (
            f"{COINBASE_PRODUCTS_URL}/BTC-USD/candles",
            {
                "start": "2026-07-24T10:00:00Z",
                "end": "2026-07-24T10:01:00Z",
                "granularity": "60",
            },
            10.0,
        )
    ]
    assert batch.source == COINBASE_SOURCE
    assert batch.venue == COINBASE_VENUE
    assert batch.observed_at == OBSERVED_AT
    assert batch.processed_at == PROCESSED_AT
    record = batch.records[0]
    assert record.instrument_type is InstrumentType.SPOT
    assert record.open_time == WINDOW_START
    assert record.open == Decimal("100.12500003")
    assert record.high == Decimal("101.50000002")
    assert record.low == Decimal("99.87500001")
    assert record.close == Decimal("101.25000004")
    assert record.base_volume == Decimal("12.34567890")
    _assert_raw_lineage(
        body=body,
        raw_body=batch.raw_payload.payload,
        raw_digest=batch.raw_payload.payload_sha256,
        raw_reference=batch.raw_payload.lineage_reference,
        record_lineage=record.lineage,
    )


def test_binance_spot_aggregate_trade_fixture_flows_through_the_active_request_path() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    body = bodies["binance.spot.aggregate-trades"]
    http = StubHttpClient(body)
    source = BinancePublicAggregateTradeSource(http=http, clock=_clock())

    batch = source.fetch(_trade_request(InstrumentType.SPOT))

    assert http.calls == [
        (
            BINANCE_SPOT_AGG_TRADES_URL,
            {
                "symbol": "BTCUSDT",
                "startTime": "1784887200000",
                "endTime": "1784887259999",
                "limit": str(MAX_BINANCE_AGGREGATE_TRADES),
            },
            10.0,
        )
    ]
    assert batch.stream.source == BINANCE_ORDER_FLOW_SOURCE
    assert batch.stream.venue == BINANCE_ORDER_FLOW_VENUE
    assert batch.observed_at == OBSERVED_AT
    assert batch.processed_at == PROCESSED_AT
    record = batch.records[0]
    assert isinstance(record, CanonicalTrade)
    assert record.instrument_type is InstrumentType.SPOT
    assert record.provider_trade_id == "100"
    assert record.provider_first_trade_id == "1000"
    assert record.provider_last_trade_id == "1004"
    assert record.price == Decimal("100.25000001")
    assert record.base_quantity == Decimal("0.40000002")
    assert record.aggressor_side is AggressorSide.SELL
    assert record.event_time == WINDOW_START + timedelta(milliseconds=123)
    assert record.observed_at == OBSERVED_AT
    assert record.processed_at == PROCESSED_AT
    _assert_raw_lineage(
        body=body,
        raw_body=batch.raw_payload.payload,
        raw_digest=batch.raw_payload.payload_sha256,
        raw_reference=batch.raw_payload.lineage_reference,
        record_lineage=record.lineage,
    )


def test_binance_usdm_aggregate_trade_fixture_flows_through_the_active_request_path() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    body = bodies["binance.usdm.aggregate-trades"]
    http = StubHttpClient(body)
    source = BinancePublicAggregateTradeSource(http=http, clock=_clock())

    batch = source.fetch(_trade_request(InstrumentType.PERPETUAL_FUTURE))

    assert http.calls == [
        (
            BINANCE_USDM_AGG_TRADES_URL,
            {
                "symbol": "BTCUSDT",
                "startTime": "1784887200000",
                "endTime": "1784887259999",
                "limit": str(MAX_BINANCE_AGGREGATE_TRADES),
            },
            10.0,
        )
    ]
    assert batch.stream.source == BINANCE_ORDER_FLOW_SOURCE
    assert batch.stream.venue == BINANCE_ORDER_FLOW_VENUE
    assert batch.observed_at == OBSERVED_AT
    assert batch.processed_at == PROCESSED_AT
    record = batch.records[0]
    assert isinstance(record, CanonicalTrade)
    assert record.instrument_type is InstrumentType.PERPETUAL_FUTURE
    assert record.provider_trade_id == "200"
    assert record.provider_first_trade_id == "2000"
    assert record.provider_last_trade_id == "2003"
    assert record.price == Decimal("200.50000001")
    assert record.base_quantity == Decimal("0.30000002")
    assert record.aggressor_side is AggressorSide.BUY
    assert record.event_time == WINDOW_START + timedelta(milliseconds=456)
    assert record.observed_at == OBSERVED_AT
    assert record.processed_at == PROCESSED_AT
    _assert_raw_lineage(
        body=body,
        raw_body=batch.raw_payload.payload,
        raw_digest=batch.raw_payload.payload_sha256,
        raw_reference=batch.raw_payload.lineage_reference,
        record_lineage=record.lineage,
    )


def _encoded(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode()


@pytest.mark.parametrize(
    "case",
    [
        "width-minus-one",
        "width-plus-one",
        "detectable-reorder",
        "wrong-numeric-type",
        "invalid-decimal",
    ],
)
def test_binance_candle_drift_is_nonretryable_invalid_payload(case: str) -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    payload = cast(list[list[object]], json.loads(bodies["binance.spot.candles"]))
    row = payload[0]
    if case == "width-minus-one":
        row.pop()
    elif case == "width-plus-one":
        row.append("unexpected")
    elif case == "detectable-reorder":
        row[0], row[1] = row[1], row[0]
    elif case == "wrong-numeric-type":
        row[1] = 100
    elif case == "invalid-decimal":
        row[1] = "not-a-decimal"
    else:
        raise AssertionError(f"unhandled Binance candle drift: {case}")
    http = StubHttpClient(_encoded(payload))
    source = BinancePublicCandleSource(http=http, clock=_clock())

    with pytest.raises(BinanceCandleError) as raised:
        source.fetch(_binance_candle_request(InstrumentType.SPOT))

    assert raised.value.code is BinanceCandleErrorCode.INVALID_PAYLOAD
    assert raised.value.retryable is False
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "case",
    [
        "width-minus-one",
        "width-plus-one",
        "detectable-reorder",
        "wrong-numeric-type",
        "invalid-decimal",
    ],
)
def test_coinbase_candle_drift_is_nonretryable_invalid_payload(case: str) -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    payload = cast(list[list[object]], json.loads(bodies["coinbase.exchange.spot.candles"]))
    row = payload[0]
    if case == "width-minus-one":
        row.pop()
    elif case == "width-plus-one":
        row.append(1)
    elif case == "detectable-reorder":
        row[0], row[1] = row[1], row[0]
    elif case == "wrong-numeric-type":
        row[1] = True
    elif case == "invalid-decimal":
        row[1] = "not-a-decimal"
    else:
        raise AssertionError(f"unhandled Coinbase candle drift: {case}")
    http = StubHttpClient(_encoded(payload))
    source = CoinbasePublicCandleSource(http=http, clock=_clock())

    with pytest.raises(CoinbaseCandleError) as raised:
        source.fetch(_candle_request(InstrumentType.SPOT))

    assert raised.value.code is CoinbaseCandleErrorCode.INVALID_PAYLOAD
    assert raised.value.retryable is False
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "case",
    [
        "missing-required",
        "unknown-field",
        "wrong-required-type",
        "wrong-optional-type",
        "invalid-decimal",
    ],
)
def test_aggregate_trade_drift_is_nonretryable_invalid_payload(case: str) -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    payload = cast(list[dict[str, object]], json.loads(bodies["binance.spot.aggregate-trades"]))
    row = payload[0]
    if case == "missing-required":
        del row["p"]
    elif case == "unknown-field":
        row["unexpected"] = "schema-drift"
    elif case == "wrong-required-type":
        row["p"] = 100
    elif case == "wrong-optional-type":
        row["M"] = "true"
    elif case == "invalid-decimal":
        row["p"] = "not-a-decimal"
    else:
        raise AssertionError(f"unhandled aggregate-trade drift: {case}")
    http = StubHttpClient(_encoded(payload))
    source = BinancePublicAggregateTradeSource(http=http, clock=_clock())

    with pytest.raises(BinanceAggregateTradeError) as raised:
        source.fetch(_trade_request(InstrumentType.SPOT))

    assert raised.value.code is BinanceAggregateTradeErrorCode.INVALID_PAYLOAD
    assert raised.value.retryable is False
    assert raised.value.requires_smaller_window is False
    assert len(http.calls) == 1


def test_aggregate_optional_fields_are_shared_across_both_request_variants() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    spot_payload = cast(
        list[dict[str, object]],
        json.loads(bodies["binance.spot.aggregate-trades"]),
    )
    usdm_payload = cast(
        list[dict[str, object]],
        json.loads(bodies["binance.usdm.aggregate-trades"]),
    )
    spot_payload[0]["nq"] = "0.35000000"
    usdm_payload[0]["M"] = False

    spot_body = _encoded(spot_payload)
    usdm_body = _encoded(usdm_payload)
    spot_batch = BinancePublicAggregateTradeSource(
        http=StubHttpClient(spot_body),
        clock=_clock(),
    ).fetch(_trade_request(InstrumentType.SPOT))
    usdm_batch = BinancePublicAggregateTradeSource(
        http=StubHttpClient(usdm_body),
        clock=_clock(),
    ).fetch(_trade_request(InstrumentType.PERPETUAL_FUTURE))

    assert len(spot_batch.records) == 1
    assert len(usdm_batch.records) == 1
    assert spot_batch.records[0].instrument_type is InstrumentType.SPOT
    assert usdm_batch.records[0].instrument_type is InstrumentType.PERPETUAL_FUTURE
    assert set(cast(list[dict[str, object]], json.loads(spot_batch.raw_payload.payload))[0]) == {
        *EXPECTED_REQUIRED_FIELDS,
        *EXPECTED_OPTIONAL_FIELDS,
    }
    assert set(cast(list[dict[str, object]], json.loads(usdm_batch.raw_payload.payload))[0]) == {
        *EXPECTED_REQUIRED_FIELDS,
        *EXPECTED_OPTIONAL_FIELDS,
    }


def test_decimal_scale_alone_is_not_an_adapter_rejection_boundary() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    original_body = bodies["binance.spot.candles"]
    changed_payload = cast(list[list[object]], json.loads(original_body))
    changed_payload[0][1] = "100.125000000"
    changed_body = _encoded(changed_payload)

    original = BinancePublicCandleSource(
        http=StubHttpClient(original_body),
        clock=_clock(),
    ).fetch(_binance_candle_request(InstrumentType.SPOT))
    changed = BinancePublicCandleSource(
        http=StubHttpClient(changed_body),
        clock=_clock(),
    ).fetch(_binance_candle_request(InstrumentType.SPOT))

    assert original.records[0].open == changed.records[0].open
    assert original.raw_payload.payload_sha256 != changed.raw_payload.payload_sha256
    assert original.records[0].record_id != changed.records[0].record_id


def test_same_typed_semantic_reorder_can_parse_but_changes_exact_evidence() -> None:
    _, bodies = load_corpus(CORPUS_ROOT)
    original_body = bodies["coinbase.exchange.spot.candles"]
    changed_payload = cast(list[list[object]], json.loads(original_body))
    changed_payload[0][3], changed_payload[0][4] = changed_payload[0][4], changed_payload[0][3]
    changed_body = _encoded(changed_payload)

    original = CoinbasePublicCandleSource(
        http=StubHttpClient(original_body),
        clock=_clock(),
    ).fetch(_candle_request(InstrumentType.SPOT))
    changed = CoinbasePublicCandleSource(
        http=StubHttpClient(changed_body),
        clock=_clock(),
    ).fetch(_candle_request(InstrumentType.SPOT))

    assert original.records[0].open == changed.records[0].close
    assert original.records[0].close == changed.records[0].open
    assert original.raw_payload.payload_sha256 != changed.raw_payload.payload_sha256
    assert original.records[0].record_id != changed.records[0].record_id
