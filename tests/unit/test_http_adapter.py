"""Tests for the bounded standard-library public HTTP adapter."""

import subprocess
import sys
from collections import Counter
from collections.abc import ItemsView, Iterator, Mapping
from email.message import Message
from http.client import (
    BadStatusLine,
    HTTPException,
    IncompleteRead,
    InvalidURL,
    LineTooLong,
    UnknownProtocol,
)
from io import BytesIO
from types import TracebackType
from typing import Self, cast
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode as stdlib_urlencode
from urllib.request import (
    BaseHandler,
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)
from urllib.response import addinfourl

import pytest

from wealth.adapters import http as http_adapter
from wealth.adapters.binance import BINANCE_SPOT_KLINES_URL, BINANCE_USDM_KLINES_URL
from wealth.adapters.binance_order_flow import (
    BINANCE_SPOT_AGG_TRADES_URL,
    BINANCE_USDM_AGG_TRADES_URL,
)
from wealth.adapters.coinbase import COINBASE_PRODUCTS_URL
from wealth.adapters.http import (
    MAX_PUBLIC_HTTP_RESPONSE_BYTES,
    UrllibPublicHttpClient,
)
from wealth.ports.http import HttpTransportError

INVALID_TIMEOUTS = (
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param(0.0, id="zero"),
    pytest.param(-0.25, id="negative"),
)
INVALID_RESPONSE_LIMITS = (
    pytest.param(True, id="boolean-true"),
    pytest.param(False, id="boolean-false"),
    pytest.param(1.0, id="integral-float"),
    pytest.param(0.5, id="fractional-float"),
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param(0, id="zero"),
    pytest.param(-1, id="negative"),
    pytest.param(MAX_PUBLIC_HTTP_RESPONSE_BYTES + 1, id="above-maximum"),
    pytest.param(10**1_000, id="huge-integer"),
)
PROTOCOL_FAILURE_TYPES = (
    pytest.param(BadStatusLine, id="bad-status-line"),
    pytest.param(LineTooLong, id="line-too-long"),
    pytest.param(UnknownProtocol, id="unknown-protocol"),
)
UNMAPPED_HTTP_EXCEPTION_TYPES = (
    pytest.param(HTTPException, id="base-http-exception"),
    pytest.param(InvalidURL, id="invalid-url"),
)
REDIRECT_STATUS_CODES = (301, 302, 303, 307, 308)
INVALID_INITIAL_URL_MESSAGE = (
    "url must be an absolute credential-free HTTPS endpoint without query or fragment"
)
INVALID_INITIAL_URL_LENGTH_MESSAGE = "url must contain at most 8192 characters"
INVALID_TARGET_PORT_MESSAGE = "url must use the standard HTTPS target port"
INVALID_QUERY_MESSAGE = (
    "query must contain at most 32 built-in string pairs totaling at most 8192 characters"
)
NONSTANDARD_TARGET_PORT_URLS = (
    pytest.param("https://example.test:1/public", id="minimum"),
    pytest.param("https://example.test:80/public", id="http-default"),
    pytest.param("https://example.test:442/public", id="below-standard-https"),
    pytest.param("https://example.test:444/public", id="above-standard-https"),
    pytest.param("https://example.test:8443/public", id="alternate-https"),
    pytest.param("https://example.test:65535/public", id="maximum"),
    pytest.param("https://example.test:00080/public", id="zero-padded-nonstandard"),
    pytest.param("https://[2001:db8::1]:444/public", id="ipv6-nonstandard"),
    pytest.param("https://[v1.example]:444/public", id="ipvfuture-nonstandard"),
)
MIXED_STRUCTURAL_AND_NONSTANDARD_PORT_URLS = (
    pytest.param("http://example.test:444/public", id="non-https"),
    pytest.param("https://user@example.test:444/public", id="userinfo"),
    pytest.param("https://example.test%3A444/public", id="encoded-authority"),
    pytest.param("https://example.test:444/public?query", id="pre-existing-query"),
)
ACTIVE_PROVIDER_DEFAULT_URLS = (
    pytest.param(
        BINANCE_SPOT_KLINES_URL,
        "https://data-api.binance.vision/api/v3/klines",
        45,
        id="binance-spot-candles",
    ),
    pytest.param(
        BINANCE_USDM_KLINES_URL,
        "https://fapi.binance.com/fapi/v1/klines",
        39,
        id="binance-usdm-candles",
    ),
    pytest.param(
        COINBASE_PRODUCTS_URL,
        "https://api.exchange.coinbase.com/products",
        42,
        id="coinbase-spot-candles",
    ),
    pytest.param(
        BINANCE_SPOT_AGG_TRADES_URL,
        "https://data-api.binance.vision/api/v3/aggTrades",
        48,
        id="binance-spot-aggregate-trades",
    ),
    pytest.param(
        BINANCE_USDM_AGG_TRADES_URL,
        "https://fapi.binance.com/fapi/v1/aggTrades",
        42,
        id="binance-usdm-aggregate-trades",
    ),
)
UNICODE_WHITESPACE_CODE_POINTS = (
    0x0085,
    0x00A0,
    0x1680,
    *range(0x2000, 0x200B),
    0x2028,
    0x2029,
    0x202F,
    0x205F,
    0x3000,
)
RAW_FORBIDDEN_INITIAL_URLS = (
    pytest.param("https://example.test/public?symbol=BTC", id="populated-query"),
    pytest.param("https://example.test/public?", id="empty-query"),
    pytest.param("https://example.test/public#part", id="populated-fragment"),
    pytest.param("https://example.test/public#", id="empty-fragment"),
    pytest.param(r"https:\example.test\public", id="backslash-scheme-separator"),
    pytest.param(r"https://example.test\public", id="backslash-authority"),
    pytest.param(r"https://example.test/public\part", id="backslash-path"),
    pytest.param(" https://example.test/public", id="leading-space"),
    pytest.param("https://example.test/public ", id="trailing-space"),
    pytest.param("https://\ud800.test/public", id="lone-high-surrogate-authority"),
    pytest.param("https://example.test/public\udfff", id="lone-low-surrogate-path"),
    *(
        pytest.param(
            f"https://example.test/public{chr(code_point)}part",
            id=f"c0-u{code_point:04x}",
        )
        for code_point in range(0x20)
    ),
    pytest.param("https://example.test/public\x7fpart", id="del-u007f"),
    *(
        pytest.param(
            f"https://example.test/public{chr(code_point)}part",
            id=f"unicode-whitespace-u{code_point:04x}",
        )
        for code_point in UNICODE_WHITESPACE_CODE_POINTS
    ),
)
INVALID_INITIAL_URLS = (
    pytest.param("", id="empty"),
    pytest.param("relative/path", id="relative"),
    pytest.param("//example.test/public", id="scheme-relative"),
    pytest.param("https:example.test/public", id="https-without-authority"),
    pytest.param("http://example.test/public", id="http"),
    pytest.param("HTTP://example.test/public", id="uppercase-http"),
    pytest.param("ftp://example.test/public", id="ftp"),
    pytest.param("file:///tmp/not-contacted", id="file"),
    pytest.param("data:text/plain,not-contacted", id="data"),
    pytest.param("gopher://example.test/public", id="gopher"),
    pytest.param("wss://example.test/public", id="secure-websocket"),
    pytest.param("custom://example.test/public", id="custom-scheme"),
    pytest.param("httpsx://example.test/public", id="https-prefix-only"),
    pytest.param("https://", id="empty-authority"),
    pytest.param("https:///public", id="missing-hostname"),
    pytest.param("https://:443/public", id="port-without-hostname"),
    pytest.param("https://@example.test/public", id="empty-userinfo"),
    pytest.param("https://user@example.test/public", id="username"),
    pytest.param("https://:password@example.test/public", id="password-only"),
    pytest.param("https://user:@example.test/public", id="empty-password"),
    pytest.param("https://user:password@example.test/public", id="username-password"),
    pytest.param("https://example.test@/public", id="userinfo-without-hostname"),
    *RAW_FORBIDDEN_INITIAL_URLS,
    pytest.param("https://[::1/public", id="unmatched-opening-bracket"),
    pytest.param("https://::1/public", id="unbracketed-ipv6"),
    pytest.param("https://[127.0.0.1]/public", id="bracketed-ipv4"),
    pytest.param("https://[vG.example]/public", id="malformed-ipvfuture"),
    pytest.param("https://example\uff0fevil.test/public", id="nfkc-slash"),
    pytest.param("https://example\uff1fevil.test/public", id="nfkc-question"),
    pytest.param("https://example\uff03evil.test/public", id="nfkc-fragment"),
    pytest.param("https://example\uff20evil.test/public", id="nfkc-at"),
    pytest.param("https://example\uff1a443/public", id="nfkc-colon"),
    pytest.param("https://example\ufe68evil.test/public", id="nfkc-small-backslash"),
    pytest.param("https://example\uff3cevil.test/public", id="nfkc-fullwidth-backslash"),
    pytest.param("https://example\ufe6a2Fevil.test/public", id="nfkc-small-percent"),
    pytest.param("https://example\uff052Fevil.test/public", id="nfkc-fullwidth-percent"),
    pytest.param("https://exa\u00a8mple.test/public", id="nfkc-normalized-whitespace"),
    pytest.param("https://example.test:/public", id="empty-port"),
    pytest.param("https://[2001:db8::1]:/public", id="ipv6-empty-port"),
    pytest.param("https://example.test:abc/public", id="alphabetic-port"),
    pytest.param("https://example.test:+1/public", id="signed-positive-port"),
    pytest.param("https://example.test:-1/public", id="negative-port"),
    pytest.param("https://example.test:\uff11\uff12/public", id="unicode-digit-port"),
    pytest.param("https://example.test:0/public", id="zero-port"),
    pytest.param("https://example.test:65536/public", id="above-maximum-port"),
    pytest.param(f"https://example.test:{10**1_000}/public", id="huge-port"),
    pytest.param("https://%65xample.test/public", id="encoded-host-character"),
    pytest.param("https://example.test%3a0/public", id="encoded-zero-port"),
    pytest.param("https://example.test%3A65536/public", id="encoded-large-port"),
    pytest.param("https://user%40example.test/public", id="encoded-userinfo-delimiter"),
    pytest.param("https://example.test%2Fother/public", id="encoded-authority-slash"),
    pytest.param("https://example.test%5Cother/public", id="encoded-authority-backslash"),
    pytest.param("https://example.test%00other/public", id="encoded-authority-control"),
    pytest.param("https://example.test%zz/public", id="malformed-authority-escape"),
    pytest.param("https://[fe80::1%25eth0]/public", id="encoded-ipv6-zone"),
)
VALID_INITIAL_URLS = (
    pytest.param("https://example.test", id="hostname-only"),
    pytest.param("https://example.test/", id="root-path"),
    pytest.param("https://example.test/public", id="ordinary-path"),
    pytest.param("HTTPS://Example.TEST/Mixed", id="mixed-case-scheme-and-host"),
    pytest.param("https://example.test:443/public", id="default-explicit-port"),
    pytest.param("https://example.test:00443/public", id="zero-padded-standard-port"),
    pytest.param("https://[2001:db8::1]:443/public", id="ipv6-standard-port"),
    pytest.param("HTTPS://Example.TEST:00443/Mixed", id="uppercase-zero-padded-standard-port"),
    pytest.param("https://192.0.2.1/public", id="ipv4-without-policy"),
    pytest.param("https://[2001:db8::1]/public", id="bracketed-ipv6-without-policy"),
    pytest.param("https://[v1.example]/public", id="parseable-ipvfuture-without-policy"),
    pytest.param("https://localhost/public", id="localhost-without-policy"),
    pytest.param("https://xn--mnich-kva.test/public", id="punycode-hostname"),
    pytest.param("https://münich.test/מחקר", id="unicode-hostname-and-path"),
    pytest.param("https://example.test./public", id="trailing-dot-hostname"),
    pytest.param(
        "https://example.test/%2F%3F%23%E2%82%AC",
        id="encoded-path-delimiters",
    ),
    pytest.param("https://example.test/a;b//c", id="semicolon-and-double-slash-path"),
    pytest.param("https://example.test/user:password@path", id="userinfo-symbols-in-path"),
)
PARSER_ERROR_INITIAL_URLS = (
    pytest.param("https://[::1/public", id="urlsplit-error"),
    pytest.param("https://example.test:abc/public", id="port-property-error"),
)
REDIRECT_TARGETS = (
    pytest.param(None, None, id="no-location"),
    pytest.param("Location", "", id="empty-location"),
    pytest.param("Location", "/relative-target", id="location-relative"),
    pytest.param("Location", "https://origin.test/same-origin", id="location-same-origin"),
    pytest.param("Location", "https://other.test/cross-origin", id="location-cross-origin"),
    pytest.param("Location", "http://origin.test/downgrade", id="location-https-to-http"),
    pytest.param("Location", "ftp://origin.test/archive", id="location-ftp"),
    pytest.param(
        "Location",
        "data:text/plain,not-contacted",
        id="location-unsupported-scheme",
    ),
    pytest.param("Location", "https://[", id="location-malformed"),
    pytest.param("URI", "", id="empty-uri"),
    pytest.param("URI", "/relative-target", id="uri-relative"),
    pytest.param("URI", "https://origin.test/same-origin", id="uri-same-origin"),
    pytest.param("URI", "https://other.test/cross-origin", id="uri-cross-origin"),
    pytest.param("URI", "http://origin.test/downgrade", id="uri-https-to-http"),
    pytest.param("URI", "ftp://origin.test/archive", id="uri-ftp"),
)


class IntegerSubclass(int):
    """Represent an integer whose runtime type is not the built-in type."""


class StringSubclass(str):
    """Represent text whose runtime type is not the built-in type."""


class LyingLengthUrl(str):
    """Expose a false short length if ordinary polymorphic len() is used."""

    def __len__(self) -> int:
        return 1


class LongLyingLengthUrl(str):
    """Expose a false oversized length if ordinary polymorphic len() is used."""

    def __len__(self) -> int:
        return 100_000


class ExplodingUrl(str):
    """Fail if oversized URL handling dispatches to content or length overrides."""

    def __str__(self) -> str:
        raise AssertionError("oversized URL was coerced")

    def __len__(self) -> int:
        raise AssertionError("URL length override was invoked")

    def __contains__(self, item: object) -> bool:
        del item
        raise AssertionError("oversized URL content membership was inspected")

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("oversized URL character iteration was started")


class PairTupleSubclass(tuple[str, str]):
    """Represent a pair whose runtime type is not the built-in tuple type."""


class QueryOriginError(RuntimeError):
    """Identify exceptions that must escape from caller-controlled mapping work."""


class ScriptedQuery(Mapping[str, str]):
    """Expose query work as an exact, finite, mutation-resistant trace."""

    def __init__(
        self,
        items: tuple[object, ...] = (),
        *,
        endless: bool = False,
        items_error: BaseException | None = None,
        iter_error: BaseException | None = None,
        next_error: BaseException | None = None,
        next_error_at: int | None = None,
        maximum_next_calls: int | None = None,
    ) -> None:
        self.scripted_items = items
        self.endless = endless
        self.items_error = items_error
        self.iter_error = iter_error
        self.next_error = next_error
        self.next_error_at = next_error_at
        self.maximum_next_calls = maximum_next_calls
        self.items_calls = 0
        self.mapping_iter_calls = 0
        self.length_calls = 0
        self.item_iter_calls = 0
        self.item_length_hint_calls = 0
        self.next_calls = 0
        self.yielded_items = 0

    def __getitem__(self, key: str) -> str:
        raise AssertionError(f"bounded query used mapping item access for {key!r}")

    def __iter__(self) -> Iterator[str]:
        self.mapping_iter_calls += 1
        raise AssertionError("bounded query started direct mapping iteration")

    def __len__(self) -> int:
        self.length_calls += 1
        raise AssertionError("bounded query called len(query)")

    def items(self) -> ItemsView[str, str]:
        self.items_calls += 1
        if self.items_calls > 1:
            raise AssertionError("bounded query called items() more than once")
        if self.items_error is not None:
            raise self.items_error
        return cast(ItemsView[str, str], ScriptedQueryItems(self))


class ScriptedQueryItems(Iterator[object]):
    """Yield scripted items while rejecting extra passes, hints, or pulls."""

    def __init__(self, query: ScriptedQuery) -> None:
        self.query = query
        self.index = 0

    def __iter__(self) -> Self:
        self.query.item_iter_calls += 1
        if self.query.item_iter_calls > 1:
            raise AssertionError("bounded query started a second item iteration")
        if self.query.iter_error is not None:
            raise self.query.iter_error
        return self

    def __next__(self) -> object:
        self.query.next_calls += 1
        next_call = self.query.next_calls
        if self.query.maximum_next_calls is not None and next_call > self.query.maximum_next_calls:
            raise AssertionError("bounded query pulled more items than permitted")
        if self.query.next_error_at == next_call:
            if self.query.next_error is None:
                raise AssertionError("scripted next error is missing")
            raise self.query.next_error
        if self.index < len(self.query.scripted_items):
            item = self.query.scripted_items[self.index]
            self.index += 1
        elif self.query.endless:
            item = (f"key-{next_call:02d}", f"value-{next_call:02d}")
        else:
            raise StopIteration
        self.query.yielded_items += 1
        return item

    def __length_hint__(self) -> int:
        self.query.item_length_hint_calls += 1
        raise AssertionError("bounded query requested an item-iterator length hint")


INVALID_QUERY_PAIRS = (
    pytest.param(["key", "value"], id="list-pair"),
    pytest.param(PairTupleSubclass(("key", "value")), id="tuple-subclass"),
    pytest.param((), id="empty-tuple"),
    pytest.param(("key",), id="one-element-tuple"),
    pytest.param(("key", "value", "extra"), id="three-element-tuple"),
    pytest.param((1, "value"), id="non-string-key"),
    pytest.param(("key", 1), id="non-string-value"),
    pytest.param((StringSubclass("key"), "value"), id="string-subclass-key"),
    pytest.param(("key", StringSubclass("value")), id="string-subclass-value"),
)


class ExplodingQuery(Mapping[str, str]):
    """Fail if an invalid URL reaches any query-mapping operation."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getitem__(self, key: str) -> str:
        self.calls.append(f"getitem:{key}")
        raise AssertionError("invalid URL reached query item access")

    def __iter__(self) -> Iterator[str]:
        self.calls.append("iter")
        raise AssertionError("invalid URL reached query iteration")

    def __len__(self) -> int:
        self.calls.append("len")
        raise AssertionError("invalid URL reached query length")

    def items(self) -> ItemsView[str, str]:
        self.calls.append("items")
        raise AssertionError("invalid URL reached query serialization")


def forbid_query_downstream_work(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace every post-query seam with one observable failure."""

    calls: list[str] = []

    def unexpected_downstream_call(*args: object, **kwargs: object) -> object:
        del args, kwargs
        calls.append("called")
        raise AssertionError("invalid query reached downstream work")

    class UnexpectedOpener:
        def open(self, *args: object, **kwargs: object) -> object:
            return unexpected_downstream_call(*args, **kwargs)

    monkeypatch.setattr(http_adapter, "urlencode", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "Request", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "urlopen", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "_NO_REDIRECT_OPENER", UnexpectedOpener())
    return calls


def assert_query_boundary_error(error: BaseException) -> None:
    """Require the exact context-free TASK-051 boundary failure."""

    assert type(error) is ValueError
    assert str(error) == INVALID_QUERY_MESSAGE
    assert error.__cause__ is None
    assert error.__context__ is None


def assert_initial_url_length_error(error: BaseException) -> None:
    """Require the exact context-free TASK-052 boundary failure."""

    assert type(error) is ValueError
    assert str(error) == INVALID_INITIAL_URL_LENGTH_MESSAGE
    assert error.__cause__ is None
    assert error.__context__ is None


class StubUrlResponse:
    """Expose the bounded response surface used by the transport."""

    status = 200

    def __init__(self, body: bytes = b'{"ok":true}') -> None:
        self.body = body
        self.headers = {"Content-Type": "application/json"}
        self.read_limits: list[int] = []
        self.enter_calls = 0
        self.exit_calls = 0

    def __enter__(self) -> Self:
        self.enter_calls += 1
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self.exit_calls += 1

    def read(self, limit: int) -> bytes:
        self.read_limits.append(limit)
        return self.body


class RecordingBytesIO(BytesIO):
    """Record the byte sentinel requested through an HTTPError body."""

    def __init__(self, body: bytes, *, close_error: BaseException | None = None) -> None:
        super().__init__(body)
        self.read_limits: list[int | None] = []
        self.close_calls = 0
        self.close_error = close_error

    def read(self, size: int | None = -1) -> bytes:
        self.read_limits.append(size)
        return super().read(size)

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            close_error = self.close_error
            self.close_error = None
            raise close_error
        super().close()


class SyntheticOpenerResponse(addinfourl):
    """Provide the real urllib response surface without external network work."""

    def __init__(
        self,
        *,
        reader: RecordingBytesIO,
        headers: Message,
        url: str,
        status_code: int,
        message: str,
    ) -> None:
        super().__init__(reader, headers, url, status_code)
        self.msg = message


class SyntheticUrlHandler(BaseHandler):
    """Return one configured response and record any attempted redirect target."""

    handler_order = 100

    def __init__(
        self,
        *,
        status_code: int,
        headers: Message,
        reader: RecordingBytesIO,
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self.reader = reader
        self.requests: list[Request] = []
        self.responses: list[SyntheticOpenerResponse] = []

    def _open(self, request: Request) -> SyntheticOpenerResponse:
        self.requests.append(request)
        if len(self.requests) == 1:
            status_code = self.status_code
            headers = self.headers
            reader = self.reader
            message = "synthetic redirect"
        else:
            status_code = 200
            headers = Message()
            headers["X-Synthetic-Target"] = "contacted"
            reader = RecordingBytesIO(b"target-contacted")
            message = "synthetic target"
        response = SyntheticOpenerResponse(
            reader=reader,
            headers=headers,
            url=request.full_url,
            status_code=status_code,
            message=message,
        )
        self.responses.append(response)
        return response

    def http_open(self, request: Request) -> SyntheticOpenerResponse:
        return self._open(request)

    def https_open(self, request: Request) -> SyntheticOpenerResponse:
        return self._open(request)

    def ftp_open(self, request: Request) -> SyntheticOpenerResponse:
        return self._open(request)


class RaisingBytesIO(RecordingBytesIO):
    """Raise one configured transport failure while recording the read."""

    def __init__(
        self,
        error: Exception,
        *,
        close_error: BaseException | None = None,
    ) -> None:
        super().__init__(b"partial-provider-detail", close_error=close_error)
        self.error = error

    def read(self, size: int | None = -1) -> bytes:
        self.read_limits.append(size)
        raise self.error


class RaisingUrlResponse(StubUrlResponse):
    """Raise one configured transport failure from a successful response read."""

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    def read(self, limit: int) -> bytes:
        self.read_limits.append(limit)
        raise self.error


class ContextFailureUrlResponse(StubUrlResponse):
    """Raise one configured failure at response entry or exit."""

    def __init__(self, *, stage: str, error: Exception) -> None:
        super().__init__(b"err")
        self.stage = stage
        self.error = error
        self.enter_calls = 0
        self.exit_calls = 0

    def __enter__(self) -> Self:
        self.enter_calls += 1
        if self.stage == "enter":
            raise self.error
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback
        self.exit_calls += 1
        if self.stage == "exit":
            raise self.error


class RaisingMessage(Message):
    """Raise one configured failure while materializing HTTP headers."""

    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error
        self.items_calls = 0

    def items(self) -> list[tuple[str, str]]:
        self.items_calls += 1
        raise self.error


class RecordingHTTPError(HTTPError):
    """Count each explicit close attempt on a real HTTP-error response."""

    def __init__(
        self,
        url: str,
        code: int,
        message: str,
        headers: Message,
        reader: RecordingBytesIO,
    ) -> None:
        self.close_calls = 0
        super().__init__(url, code, message, headers, reader)

    def close(self) -> None:
        self.close_calls += 1
        super().close()


def http_error(
    reader: RecordingBytesIO,
    *,
    headers: Message | None = None,
) -> RecordingHTTPError:
    """Build one deterministic public HTTP error with a recording body."""

    if headers is None:
        headers = Message()
        headers["Retry-After"] = "1"
    return RecordingHTTPError(
        "https://example.test/public",
        429,
        "rate limited",
        headers,
        reader,
    )


def use_synthetic_private_opener(
    monkeypatch: pytest.MonkeyPatch,
    *,
    status_code: int,
    headers: Message,
    reader: RecordingBytesIO,
) -> SyntheticUrlHandler:
    """Install one real private opener chain backed by a synthetic transport."""

    transport = SyntheticUrlHandler(
        status_code=status_code,
        headers=headers,
        reader=reader,
    )
    opener = build_opener(
        ProxyHandler({}),
        transport,
        http_adapter._NoRedirectHandler(),
    )
    monkeypatch.setattr(http_adapter, "_NO_REDIRECT_OPENER", opener)
    return transport


@pytest.mark.parametrize(
    "max_response_bytes",
    [
        *INVALID_RESPONSE_LIMITS,
        pytest.param(IntegerSubclass(1), id="integer-subclass"),
    ],
)
def test_invalid_response_limit_fails_during_construction_before_work(
    monkeypatch: pytest.MonkeyPatch,
    max_response_bytes: int,
) -> None:
    calls: list[str] = []

    def unexpected_request_or_network_call(*args: object, **kwargs: object) -> object:
        del args, kwargs
        calls.append("called")
        raise AssertionError("invalid response limit reached request or network work")

    monkeypatch.setattr(http_adapter, "Request", unexpected_request_or_network_call)
    monkeypatch.setattr(http_adapter, "urlopen", unexpected_request_or_network_call)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient(max_response_bytes=max_response_bytes)

    assert str(error.value) == ("max_response_bytes must be an integer between 1 and 2000000")
    assert calls == []


@pytest.mark.parametrize(
    "max_response_bytes",
    [
        pytest.param(1, id="minimum"),
        pytest.param(1_024, id="representative"),
        pytest.param(MAX_PUBLIC_HTTP_RESPONSE_BYTES, id="maximum"),
    ],
)
def test_valid_response_limit_is_retained_exactly(max_response_bytes: int) -> None:
    client = UrllibPublicHttpClient(max_response_bytes=max_response_bytes)

    assert client.max_response_bytes is max_response_bytes


def test_default_response_limit_is_the_hard_maximum() -> None:
    assert UrllibPublicHttpClient().max_response_bytes == MAX_PUBLIC_HTTP_RESPONSE_BYTES


@pytest.mark.parametrize("timeout_seconds", INVALID_TIMEOUTS)
def test_invalid_timeout_precedes_initial_url_length_validation(
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: float,
) -> None:
    url = ExplodingUrl("https://example.test/" + ("a" * 8_200))
    query = ExplodingQuery()
    validator_calls: list[str] = []

    def unexpected_url_validation(value: str) -> None:
        validator_calls.append(value)
        raise AssertionError("invalid timeout reached URL validation")

    monkeypatch.setattr(http_adapter, "_validate_initial_url", unexpected_url_validation)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=timeout_seconds,
        )

    assert str(error.value) == "timeout_seconds must be finite and positive"
    assert validator_calls == []
    assert query.calls == []


@pytest.mark.parametrize(
    "url_type",
    [
        pytest.param(LyingLengthUrl, id="lying-length-override"),
        pytest.param(ExplodingUrl, id="raising-length-and-content-overrides"),
    ],
)
def test_oversized_initial_url_uses_true_builtin_string_length_before_all_other_work(
    monkeypatch: pytest.MonkeyPatch,
    url_type: type[str],
) -> None:
    original_text = "https://example.test/" + ("a" * 8_200)
    url = url_type(original_text)
    assert str.__len__(url) > 8192
    query = ExplodingQuery()
    parser_calls: list[str] = []
    normalization_calls: list[tuple[str, str]] = []

    def unexpected_urlsplit(value: str) -> object:
        parser_calls.append(value)
        raise AssertionError("oversized URL reached parsing")

    def unexpected_normalize(form: str, value: str) -> str:
        normalization_calls.append((form, value))
        raise AssertionError("oversized URL reached NFKC inspection")

    monkeypatch.setattr(http_adapter, "urlsplit", unexpected_urlsplit)
    monkeypatch.setattr(http_adapter, "normalize", unexpected_normalize)
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert_initial_url_length_error(error.value)
    assert parser_calls == []
    assert normalization_calls == []
    assert query.calls == []
    assert downstream_calls == []


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("forbidden-content", id="forbidden-content"),
        pytest.param("malformed-authority", id="malformed-authority"),
        pytest.param("nonstandard-port", id="nonstandard-port"),
    ],
)
def test_oversized_length_error_precedes_structural_and_port_errors(case: str) -> None:
    if case == "forbidden-content":
        prefix = "https://example.test/path?"
    elif case == "malformed-authority":
        prefix = "https://["
    else:
        prefix = "https://example.test:444/"
    url = prefix + ("a" * (8193 - str.__len__(prefix)))
    assert str.__len__(url) == 8193
    query = ExplodingQuery()

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert_initial_url_length_error(error.value)
    assert query.calls == []


@pytest.mark.parametrize(
    ("case", "expected_message", "expects_parser_context"),
    [
        pytest.param(
            "forbidden-content",
            INVALID_INITIAL_URL_MESSAGE,
            False,
            id="structural-error-retained",
        ),
        pytest.param(
            "malformed-authority",
            INVALID_INITIAL_URL_MESSAGE,
            True,
            id="parser-error-retained",
        ),
        pytest.param(
            "nonstandard-port",
            INVALID_TARGET_PORT_MESSAGE,
            False,
            id="port-error-retained",
        ),
    ],
)
def test_at_most_8192_characters_retains_existing_validation_precedence(
    case: str,
    expected_message: str,
    expects_parser_context: bool,
) -> None:
    if case == "forbidden-content":
        prefix = "https://example.test/path?"
    elif case == "malformed-authority":
        prefix = "https://["
    else:
        prefix = "https://example.test:444/"
    url = prefix + ("a" * (8192 - str.__len__(prefix)))
    assert str.__len__(url) == 8192
    query = ExplodingQuery()

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert type(error.value) is ValueError
    assert str(error.value) == expected_message
    assert error.value.__cause__ is None
    if expects_parser_context:
        assert isinstance(error.value.__context__, ValueError)
        assert error.value.__suppress_context__ is True
    else:
        assert error.value.__context__ is None
    assert query.calls == []


@pytest.mark.parametrize(
    "path_character",
    [
        pytest.param("a", id="ascii"),
        pytest.param("💰", id="unicode-code-point"),
    ],
)
def test_exactly_8192_character_initial_url_is_preserved_through_request_work(
    monkeypatch: pytest.MonkeyPatch,
    path_character: str,
) -> None:
    prefix = "https://example.test/"
    url = prefix + (path_character * (8192 - str.__len__(prefix)))
    assert type(url) is str
    assert str.__len__(url) == 8192
    if path_character != "a":
        assert len(url.encode("utf-8")) > 8192
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []
    timeout_seconds = float("0.25")

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout is timeout_seconds
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(
        user_agent="WEALTH/test initial URL length",
        max_response_bytes=2,
    ).get(
        url=url,
        query={"b": "two words", "a": "1"},
        timeout_seconds=timeout_seconds,
    )

    assert [request.full_url for request in requests] == [f"{url}?a=1&b=two+words"]
    assert [request.get_method() for request in requests] == ["GET"]
    assert [request.get_header("Accept") for request in requests] == ["application/json"]
    assert [request.get_header("User-agent") for request in requests] == [
        "WEALTH/test initial URL length"
    ]
    assert response.enter_calls == 1
    assert response.read_limits == [3]
    assert response.exit_calls == 1
    assert result.body == b"ok"


def test_exactly_8192_character_string_subclass_cannot_lie_long(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = "https://example.test/"
    url = LongLyingLengthUrl(prefix + ("a" * (8192 - str.__len__(prefix))))
    assert str.__len__(url) == 8192
    assert len(url) == 100_000
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 1
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url=url,
        query={"a": "1"},
        timeout_seconds=1,
    )

    assert [request.full_url for request in requests] == [f"{url!s}?a=1"]
    assert result.body == b"ok"


@pytest.mark.parametrize(
    "path_character",
    [
        pytest.param("a", id="ascii"),
        pytest.param("💰", id="unicode-code-point"),
    ],
)
def test_8193_character_initial_url_fails_before_query_or_request_work(
    monkeypatch: pytest.MonkeyPatch,
    path_character: str,
) -> None:
    prefix = "https://example.test/"
    url = prefix + (path_character * (8193 - str.__len__(prefix)))
    assert str.__len__(url) == 8193
    query = ExplodingQuery()
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert_initial_url_length_error(error.value)
    assert query.calls == []
    assert downstream_calls == []


def test_exact_length_valid_url_reaches_the_existing_query_boundary_after_url_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = "https://example.test/"
    url = prefix + ("a" * (8192 - str.__len__(prefix)))
    query = ScriptedQuery((["invalid", "list-pair"],), maximum_next_calls=1)
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert_query_boundary_error(error.value)
    assert query.items_calls == 1
    assert query.next_calls == 1
    assert downstream_calls == []


@pytest.mark.parametrize("url", INVALID_INITIAL_URLS)
def test_invalid_initial_url_fails_before_query_request_or_opener_work(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    query = ExplodingQuery()
    downstream_calls: list[str] = []

    def unexpected_downstream_call(*args: object, **kwargs: object) -> object:
        del args, kwargs
        downstream_calls.append("called")
        raise AssertionError("invalid URL reached downstream work")

    class UnexpectedOpener:
        def open(self, *args: object, **kwargs: object) -> object:
            return unexpected_downstream_call(*args, **kwargs)

    monkeypatch.setattr(http_adapter, "urlencode", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "Request", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "urlopen", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "_NO_REDIRECT_OPENER", UnexpectedOpener())

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert type(error.value) is ValueError
    assert str(error.value) == INVALID_INITIAL_URL_MESSAGE
    assert error.value.__cause__ is None
    assert query.calls == []
    assert downstream_calls == []


@pytest.mark.parametrize("url", NONSTANDARD_TARGET_PORT_URLS)
def test_nonstandard_target_port_fails_before_query_request_or_opener_work(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    query = ExplodingQuery()
    downstream_calls: list[str] = []

    def unexpected_downstream_call(*args: object, **kwargs: object) -> object:
        del args, kwargs
        downstream_calls.append("called")
        raise AssertionError("nonstandard target port reached downstream work")

    class UnexpectedOpener:
        def open(self, *args: object, **kwargs: object) -> object:
            return unexpected_downstream_call(*args, **kwargs)

    monkeypatch.setattr(http_adapter, "urlencode", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "Request", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "urlopen", unexpected_downstream_call)
    monkeypatch.setattr(http_adapter, "_NO_REDIRECT_OPENER", UnexpectedOpener())

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert type(error.value) is ValueError
    assert str(error.value) == INVALID_TARGET_PORT_MESSAGE
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert query.calls == []
    assert downstream_calls == []


@pytest.mark.parametrize("url", MIXED_STRUCTURAL_AND_NONSTANDARD_PORT_URLS)
def test_structural_initial_url_error_precedes_nonstandard_target_port_policy(
    url: str,
) -> None:
    query = ExplodingQuery()

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert str(error.value) == INVALID_INITIAL_URL_MESSAGE
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert query.calls == []


@pytest.mark.parametrize("url", PARSER_ERROR_INITIAL_URLS)
def test_initial_url_parser_failure_context_is_suppressed(
    url: str,
) -> None:
    query = ExplodingQuery()

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=1,
        )

    assert str(error.value) == INVALID_INITIAL_URL_MESSAGE
    assert error.value.__cause__ is None
    assert isinstance(error.value.__context__, ValueError)
    assert error.value.__suppress_context__ is True
    assert query.calls == []


@pytest.mark.parametrize("url", RAW_FORBIDDEN_INITIAL_URLS)
def test_raw_forbidden_initial_url_fails_before_url_parsing(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    parser_calls: list[str] = []

    def unexpected_urlsplit(value: str) -> object:
        parser_calls.append(value)
        raise AssertionError("raw-forbidden URL reached parsing")

    monkeypatch.setattr(http_adapter, "urlsplit", unexpected_urlsplit)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=ExplodingQuery(),
            timeout_seconds=1,
        )

    assert str(error.value) == INVALID_INITIAL_URL_MESSAGE
    assert parser_calls == []


@pytest.mark.parametrize("url", VALID_INITIAL_URLS)
def test_valid_initial_url_is_preserved_through_request_construction(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    response = StubUrlResponse(b"ok")
    calls: list[tuple[Request, float]] = []
    timeout_seconds = float("0.25")

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        calls.append((request, timeout))
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(
        user_agent="WEALTH/test initial-target",
        max_response_bytes=2,
    ).get(
        url=url,
        query={"b": "2", "a": "1"},
        timeout_seconds=timeout_seconds,
    )

    assert len(calls) == 1
    request, forwarded_timeout = calls[0]
    assert request.full_url == f"{url}?a=1&b=2"
    assert request.get_method() == "GET"
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == "WEALTH/test initial-target"
    assert forwarded_timeout is timeout_seconds
    assert response.enter_calls == 1
    assert response.read_limits == [3]
    assert response.exit_calls == 1
    assert result.status_code == 200
    assert result.body == b"ok"


@pytest.mark.parametrize(("url", "expected_url", "expected_length"), ACTIVE_PROVIDER_DEFAULT_URLS)
def test_active_provider_default_url_uses_the_standard_https_target_port(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    expected_url: str,
    expected_length: int,
) -> None:
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 1
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    assert url == expected_url
    assert type(url) is str
    assert str.__len__(url) == expected_length
    assert str.__len__(expected_url) == expected_length

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url=url,
        query={},
        timeout_seconds=1,
    )

    assert [request.full_url for request in requests] == [f"{url}?"]
    assert result.status_code == 200
    assert result.body == b"ok"


def test_zero_padded_standard_port_on_ipvfuture_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 1
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url="https://[v1.example]:00443/public",
        query={"a": "1"},
        timeout_seconds=1,
    )

    assert [request.full_url for request in requests] == ["https://[v1.example]:00443/public?a=1"]
    assert result.body == b"ok"


@pytest.mark.parametrize(
    "pair_count",
    [
        pytest.param(0, id="empty"),
        pytest.param(1, id="one"),
        pytest.param(32, id="maximum"),
    ],
)
def test_query_pair_count_boundaries_are_snapshotted_once_and_preserved(
    monkeypatch: pytest.MonkeyPatch,
    pair_count: int,
) -> None:
    pairs = tuple(
        (f"key-{index:02d}", f"value-{index:02d}") for index in reversed(range(pair_count))
    )
    query = ScriptedQuery(pairs)
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 0.25
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url="https://example.test/public",
        query=query,
        timeout_seconds=0.25,
    )

    assert [request.full_url for request in requests] == [
        f"https://example.test/public?{stdlib_urlencode(sorted(pairs))}"
    ]
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == pair_count + 1
    assert query.yielded_items == pair_count
    assert result.body == b"ok"


@pytest.mark.parametrize(
    "endless",
    [
        pytest.param(False, id="finite-thirty-three"),
        pytest.param(True, id="synthetic-endless"),
    ],
)
def test_query_pair_limit_rejects_the_thirty_third_pull_without_a_thirty_fourth(
    monkeypatch: pytest.MonkeyPatch,
    endless: bool,
) -> None:
    finite_items = tuple((f"key-{index:02d}", "v") for index in range(33))
    query = ScriptedQuery(
        () if endless else finite_items,
        endless=endless,
        maximum_next_calls=33,
    )
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query=query,
            timeout_seconds=1,
        )

    assert_query_boundary_error(error.value)
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == 33
    assert query.yielded_items == 33
    assert downstream_calls == []


def test_query_pair_limit_rejects_the_thirty_third_item_before_inspecting_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thirty_third_item = ("must-not", "be-inspected")
    query = ScriptedQuery(
        (
            *(("key", str(index)) for index in range(32)),
            thirty_third_item,
        ),
        maximum_next_calls=33,
    )
    downstream_calls = forbid_query_downstream_work(monkeypatch)
    builtin_type = type

    def guarded_type(value: object) -> type[object]:
        if value is thirty_third_item:
            raise AssertionError("bounded query inspected the thirty-third item")
        return builtin_type(value)

    monkeypatch.setattr(http_adapter, "type", guarded_type, raising=False)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query=query,
            timeout_seconds=1,
        )

    assert_query_boundary_error(error.value)
    assert query.next_calls == 33
    assert query.yielded_items == 33
    assert downstream_calls == []


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("cumulative", id="cumulative"),
        pytest.param("key-only", id="key-only"),
        pytest.param("value-only", id="value-only"),
        pytest.param("unicode-characters", id="unicode-characters"),
    ],
)
def test_query_character_limit_accepts_exactly_8192_characters_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    pairs: tuple[tuple[str, str], ...]
    if case == "cumulative":
        pairs = (("a", "x" * 4095), ("b", "y" * 4095))
    elif case == "key-only":
        pairs = (("k" * 8192, ""),)
    elif case == "value-only":
        pairs = (("", "v" * 8192),)
    else:
        pairs = (("", "💰" * 8192),)
    assert sum(len(key) + len(value) for key, value in pairs) == 8192
    query = ScriptedQuery(cast(tuple[object, ...], pairs))
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 1
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url="https://example.test/public",
        query=query,
        timeout_seconds=1,
    )

    assert [request.full_url for request in requests] == [
        f"https://example.test/public?{stdlib_urlencode(sorted(pairs))}"
    ]
    assert query.items_calls == 1
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == len(pairs) + 1
    assert query.yielded_items == len(pairs)
    assert result.body == b"ok"


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("cumulative", id="cumulative"),
        pytest.param("key-only", id="key-only"),
        pytest.param("value-only", id="value-only"),
        pytest.param("unicode-characters", id="unicode-characters"),
    ],
)
def test_query_character_limit_rejects_the_8193rd_character_immediately(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    violating_pairs: tuple[tuple[str, str], ...]
    if case == "cumulative":
        violating_pairs = (("a", "x" * 4095), ("b", "y" * 4096))
    elif case == "key-only":
        violating_pairs = (("k" * 8193, ""),)
    elif case == "value-only":
        violating_pairs = (("", "v" * 8193),)
    else:
        violating_pairs = (("", "💰" * 8193),)
    assert sum(len(key) + len(value) for key, value in violating_pairs) == 8193
    query = ScriptedQuery(
        (*cast(tuple[object, ...], violating_pairs), ("poison", "must-not-be-pulled")),
        maximum_next_calls=len(violating_pairs),
    )
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query=query,
            timeout_seconds=1,
        )

    assert_query_boundary_error(error.value)
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == len(violating_pairs)
    assert query.yielded_items == len(violating_pairs)
    assert downstream_calls == []


@pytest.mark.parametrize("invalid_pair", INVALID_QUERY_PAIRS)
def test_query_requires_exact_builtin_tuple_and_string_types_and_stops_immediately(
    monkeypatch: pytest.MonkeyPatch,
    invalid_pair: object,
) -> None:
    query = ScriptedQuery(
        (invalid_pair, ("poison", "must-not-be-pulled")),
        maximum_next_calls=1,
    )
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query=query,
            timeout_seconds=1,
        )

    assert_query_boundary_error(error.value)
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == 1
    assert query.yielded_items == 1
    assert downstream_calls == []


@pytest.mark.parametrize(
    "stage",
    [
        pytest.param("items", id="items-call"),
        pytest.param("iter", id="item-iterator-start"),
        pytest.param("next-first", id="first-pull"),
        pytest.param("next-after-two", id="pull-after-two-valid-items"),
        pytest.param("next-thirty-three", id="thirty-third-pull"),
    ],
)
@pytest.mark.parametrize(
    "origin_error_type",
    [
        pytest.param(QueryOriginError, id="runtime-error"),
        pytest.param(ValueError, id="value-error-collision"),
    ],
)
def test_query_mapping_origin_exception_remains_the_same_raw_object(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    origin_error_type: type[Exception],
) -> None:
    origin_error = origin_error_type(
        INVALID_QUERY_MESSAGE if origin_error_type is ValueError else f"origin-{stage}"
    )
    if stage == "items":
        query = ScriptedQuery(items_error=origin_error)
        expected_iter_calls = 0
        expected_next_calls = 0
        expected_yields = 0
    elif stage == "iter":
        query = ScriptedQuery(iter_error=origin_error)
        expected_iter_calls = 1
        expected_next_calls = 0
        expected_yields = 0
    elif stage == "next-first":
        query = ScriptedQuery(
            next_error=origin_error,
            next_error_at=1,
            maximum_next_calls=1,
        )
        expected_iter_calls = 1
        expected_next_calls = 1
        expected_yields = 0
    elif stage == "next-after-two":
        query = ScriptedQuery(
            (("a", "1"), ("b", "2")),
            next_error=origin_error,
            next_error_at=3,
            maximum_next_calls=3,
        )
        expected_iter_calls = 1
        expected_next_calls = 3
        expected_yields = 2
    else:
        query = ScriptedQuery(
            tuple((f"key-{index:02d}", "v") for index in range(32)),
            next_error=origin_error,
            next_error_at=33,
            maximum_next_calls=33,
        )
        expected_iter_calls = 1
        expected_next_calls = 33
        expected_yields = 32
    downstream_calls = forbid_query_downstream_work(monkeypatch)

    with pytest.raises(origin_error_type) as captured:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query=query,
            timeout_seconds=1,
        )

    assert captured.value is origin_error
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == expected_iter_calls
    assert query.item_length_hint_calls == 0
    assert query.next_calls == expected_next_calls
    assert query.yielded_items == expected_yields
    assert downstream_calls == []


@pytest.mark.parametrize(
    ("url", "timeout_seconds", "expected_message"),
    [
        pytest.param(
            "https://example.test/public",
            float("nan"),
            "timeout_seconds must be finite and positive",
            id="timeout-first",
        ),
        pytest.param(
            "http://example.test/public",
            1,
            INVALID_INITIAL_URL_MESSAGE,
            id="structural-target-second",
        ),
        pytest.param(
            "https://example.test:444/public",
            1,
            INVALID_TARGET_PORT_MESSAGE,
            id="target-port-third",
        ),
    ],
)
def test_timeout_target_structure_and_port_precede_all_query_access(
    url: str,
    timeout_seconds: float,
    expected_message: str,
) -> None:
    query = ScriptedQuery(items_error=AssertionError("query boundary ran out of order"))

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url=url,
            query=query,
            timeout_seconds=timeout_seconds,
        )

    assert type(error.value) is ValueError
    assert str(error.value) == expected_message
    assert error.value.__cause__ is None
    assert error.value.__context__ is None
    assert query.items_calls == 0
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 0
    assert query.next_calls == 0


def test_valid_query_snapshot_is_sorted_encoded_once_without_normalization_or_deduplication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pairs = (
        ("b", "space value"),
        ("a", "x/y?"),
        ("a", "alpha"),
        ("é", "💰"),
        ("empty", ""),
    )
    query = ScriptedQuery(cast(tuple[object, ...], pairs))
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []
    urlencode_calls: list[list[tuple[str, str]]] = []

    def recording_urlencode(items: object) -> str:
        bounded_items = cast(list[tuple[str, str]], items)
        urlencode_calls.append(bounded_items.copy())
        return stdlib_urlencode(bounded_items)

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 0.25
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlencode", recording_urlencode)
    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(
        user_agent="WEALTH/test bounded query",
        max_response_bytes=2,
    ).get(
        url="https://example.test/public",
        query=query,
        timeout_seconds=0.25,
    )

    sorted_pairs = sorted(pairs)
    assert urlencode_calls == [sorted_pairs]
    assert [request.full_url for request in requests] == [
        f"https://example.test/public?{stdlib_urlencode(sorted_pairs)}"
    ]
    assert [request.get_method() for request in requests] == ["GET"]
    assert [request.get_header("Accept") for request in requests] == ["application/json"]
    assert [request.get_header("User-agent") for request in requests] == [
        "WEALTH/test bounded query"
    ]
    assert query.items_calls == 1
    assert query.mapping_iter_calls == 0
    assert query.length_calls == 0
    assert query.item_iter_calls == 1
    assert query.item_length_hint_calls == 0
    assert query.next_calls == len(pairs) + 1
    assert query.yielded_items == len(pairs)
    assert response.read_limits == [3]
    assert result.body == b"ok"


@pytest.mark.parametrize(
    ("url", "query"),
    [
        pytest.param(
            "https://data-api.binance.vision/api/v3/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": "1767225600000",
                "endTime": "1767225659999",
                "limit": "1",
                "timeZone": "0",
            },
            id="binance-spot-candles",
        ),
        pytest.param(
            "https://fapi.binance.com/fapi/v1/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": "1767225600000",
                "endTime": "1767225659999",
                "limit": "1",
            },
            id="binance-usdm-candles",
        ),
        pytest.param(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            {
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T00:01:00Z",
                "granularity": "60",
            },
            id="coinbase-spot-candles",
        ),
        pytest.param(
            "https://data-api.binance.vision/api/v3/aggTrades",
            {
                "symbol": "BTCUSDT",
                "startTime": "1767225600000",
                "endTime": "1767225659999",
                "limit": "1000",
            },
            id="binance-spot-aggregate-trades",
        ),
        pytest.param(
            "https://fapi.binance.com/fapi/v1/aggTrades",
            {
                "symbol": "BTCUSDT",
                "startTime": "1767225600000",
                "endTime": "1767225659999",
                "limit": "1000",
            },
            id="binance-usdm-aggregate-trades",
        ),
    ],
)
def test_representative_active_provider_query_is_accepted_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    query: dict[str, str],
) -> None:
    response = StubUrlResponse(b"ok")
    requests: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        assert timeout == 1
        requests.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=2).get(
        url=url,
        query=query,
        timeout_seconds=1,
    )

    assert [request.full_url for request in requests] == [
        f"{url}?{stdlib_urlencode(sorted(query.items()))}"
    ]
    assert result.body == b"ok"


def test_production_private_opener_replaces_only_the_default_redirect_handler() -> None:
    production_handlers = cast(
        list[BaseHandler],
        vars(http_adapter._NO_REDIRECT_OPENER)["handlers"],
    )
    redirect_handlers = [
        handler for handler in production_handlers if isinstance(handler, HTTPRedirectHandler)
    ]
    default_handlers = cast(list[BaseHandler], vars(build_opener())["handlers"])
    expected_handler_types = Counter(type(handler) for handler in default_handlers)
    normalized_production_handler_types = Counter(
        HTTPRedirectHandler if type(handler) is http_adapter._NoRedirectHandler else type(handler)
        for handler in production_handlers
    )

    assert len(redirect_handlers) == 1
    assert type(redirect_handlers[0]) is http_adapter._NoRedirectHandler
    assert normalized_production_handler_types == expected_handler_types


def test_import_does_not_install_the_private_opener_process_wide() -> None:
    probe = (
        "import importlib\n"
        "import os\n"
        "import urllib.request\n"
        "proxy_url = 'http://proxy.test:8080'\n"
        "os.environ['https_proxy'] = proxy_url\n"
        "install_calls = []\n"
        "urllib.request._opener = None\n"
        "urllib.request.install_opener = lambda opener: install_calls.append(opener)\n"
        "http_adapter = importlib.import_module('wealth.adapters.http')\n"
        "assert urllib.request._opener is None\n"
        "assert install_calls == []\n"
        "proxy_handlers = [handler for handler in "
        "http_adapter._NO_REDIRECT_OPENER.handlers "
        "if isinstance(handler, urllib.request.ProxyHandler)]\n"
        "assert len(proxy_handlers) == 1\n"
        "assert proxy_handlers[0].proxies['https'] == proxy_url\n"
    )

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_private_redirect_handler_returns_original_error_before_body_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Location"] = "https://["
    reader = RecordingBytesIO(b"abc")
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=302,
        headers=headers,
        reader=reader,
    )
    request = Request("https://origin.test/public", method="GET")

    with pytest.raises(HTTPError) as captured:
        http_adapter.urlopen(request, timeout=1)

    redirect_error = captured.value
    assert redirect_error.code == 302
    assert redirect_error.headers is headers
    assert len(transport.requests) == 1
    assert reader.read_limits == []
    assert reader.close_calls == 0
    assert reader.closed is False

    redirect_error.close()

    assert reader.read_limits == []
    assert reader.close_calls == 1
    assert reader.closed is True


@pytest.mark.parametrize("status_code", REDIRECT_STATUS_CODES)
@pytest.mark.parametrize(("header_name", "target_url"), REDIRECT_TARGETS)
def test_private_opener_rejects_every_automatic_redirect_before_drain_or_follow(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    header_name: str | None,
    target_url: str | None,
) -> None:
    headers = Message()
    headers["X-Original-Response"] = "retained"
    if header_name is not None:
        assert target_url is not None
        headers[header_name] = target_url
    reader = RecordingBytesIO(b"abc")
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=status_code,
        headers=headers,
        reader=reader,
    )

    result = UrllibPublicHttpClient(max_response_bytes=3).get(
        url="https://origin.test/public",
        query={},
        timeout_seconds=1,
    )

    expected_headers = [("X-Original-Response", "retained")]
    if header_name is not None:
        assert target_url is not None
        expected_headers.append((header_name, target_url))
    assert result.status_code == status_code
    assert result.headers == tuple(expected_headers)
    assert result.body == b"abc"
    assert [request.full_url for request in transport.requests] == ["https://origin.test/public?"]
    assert [request.get_method() for request in transport.requests] == ["GET"]
    assert [request.timeout for request in transport.requests] == [1]
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_nonredirect_http_error_with_location_header_remains_bounded_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Location"] = "https://other.test/not-contacted"
    headers["Retry-After"] = "1"
    reader = RecordingBytesIO(b"err")
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=429,
        headers=headers,
        reader=reader,
    )

    result = UrllibPublicHttpClient(max_response_bytes=3).get(
        url="https://origin.test/public",
        query={},
        timeout_seconds=1,
    )

    assert result.status_code == 429
    assert result.headers == (
        ("Location", "https://other.test/not-contacted"),
        ("Retry-After", "1"),
    )
    assert result.body == b"err"
    assert [request.full_url for request in transport.requests] == ["https://origin.test/public?"]
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_redirect_one_byte_over_limit_uses_existing_bounded_error_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Location"] = "https://other.test/not-contacted"
    reader = RecordingBytesIO(b"abcd")
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=302,
        headers=headers,
        reader=reader,
    )

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://origin.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP error response exceeded the configured limit"
    assert isinstance(error.value.__cause__, HTTPError)
    assert error.value.__cause__.code == 302
    assert [request.full_url for request in transport.requests] == ["https://origin.test/public?"]
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_redirect_body_read_failure_retains_sanitized_direct_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_failure = OSError("redirect-provider-read-detail")
    headers = Message()
    headers["Location"] = "/not-contacted"
    reader = RaisingBytesIO(read_failure)
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=307,
        headers=headers,
        reader=reader,
    )

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://origin.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert str(read_failure) not in str(error.value)
    assert len(transport.requests) == 1
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_redirect_cleanup_failure_retains_sanitized_direct_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_failure = OSError("redirect-provider-close-detail")
    headers = Message()
    headers["Location"] = "ftp://other.test/not-contacted"
    reader = RecordingBytesIO(b"abc", close_error=close_failure)
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=308,
        headers=headers,
        reader=reader,
    )

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://origin.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is close_failure
    assert str(close_failure) not in str(error.value)
    assert len(transport.requests) == 1
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is False


def test_redirect_primary_read_failure_is_not_replaced_by_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_failure = OSError("primary-redirect-read-detail")
    close_failure = OSError("secondary-redirect-close-detail")
    headers = Message()
    headers["Location"] = "https://other.test/not-contacted"
    reader = RaisingBytesIO(read_failure, close_error=close_failure)
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=301,
        headers=headers,
        reader=reader,
    )

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://origin.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert len(transport.requests) == 1
    assert len(transport.responses) == 1
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is False


def test_private_opener_does_not_mutate_process_global_urllib_opener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = Message()
    headers["Content-Type"] = "application/json"
    reader = RecordingBytesIO(b"abc")
    transport = use_synthetic_private_opener(
        monkeypatch,
        status_code=200,
        headers=headers,
        reader=reader,
    )
    process_global_opener = object()
    monkeypatch.setattr(urllib_request, "_opener", process_global_opener)

    result = UrllibPublicHttpClient(
        user_agent="WEALTH/test no-follow",
        max_response_bytes=3,
    ).get(
        url="https://origin.test/public",
        query={"b": "2", "a": "1"},
        timeout_seconds=0.25,
    )

    assert result.status_code == 200
    assert result.headers == (("Content-Type", "application/json"),)
    assert result.body == b"abc"
    assert vars(urllib_request)["_opener"] is process_global_opener
    assert [request.full_url for request in transport.requests] == [
        "https://origin.test/public?a=1&b=2"
    ]
    assert [request.get_method() for request in transport.requests] == ["GET"]
    assert [request.get_header("Accept") for request in transport.requests] == ["application/json"]
    assert [request.get_header("User-agent") for request in transport.requests] == [
        "WEALTH/test no-follow"
    ]
    assert [request.timeout for request in transport.requests] == [0.25]
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_success_response_uses_one_byte_sentinel_and_accepts_exact_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = StubUrlResponse(b"abc")

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient(max_response_bytes=3).get(
        url="https://example.test/public",
        query={},
        timeout_seconds=1,
    )

    assert response.read_limits == [4]
    assert result.body == b"abc"


def test_success_response_one_byte_over_limit_fails_without_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = StubUrlResponse(b"abcd")

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP response exceeded the configured limit"
    assert response.read_limits == [4]


def test_http_error_response_uses_one_byte_sentinel_and_accepts_exact_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = RecordingBytesIO(b"err")
    provider_error = http_error(reader)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    result = UrllibPublicHttpClient(max_response_bytes=3).get(
        url="https://example.test/public",
        query={},
        timeout_seconds=1,
    )

    assert reader.read_limits == [4]
    assert result.status_code == 429
    assert result.headers == (("Retry-After", "1"),)
    assert result.body == b"err"
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True


def test_builtin_http_error_response_is_closed_before_exact_body_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = RecordingBytesIO(b"err")
    headers = Message()
    headers["Retry-After"] = "1"
    provider_error = HTTPError(
        "https://example.test/public",
        429,
        "rate limited",
        headers,
        reader,
    )

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    result = UrllibPublicHttpClient(max_response_bytes=3).get(
        url="https://example.test/public",
        query={},
        timeout_seconds=1,
    )

    assert type(provider_error) is HTTPError
    assert result.status_code == 429
    assert result.headers == (("Retry-After", "1"),)
    assert result.body == b"err"
    assert reader.read_limits == [4]
    assert reader.close_calls == 1
    assert reader.closed is True


def test_http_error_response_one_byte_over_limit_is_typed_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = RecordingBytesIO(b"oops")
    provider_error = http_error(reader)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP error response exceeded the configured limit"
    assert error.value.__cause__ is provider_error
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True


@pytest.mark.parametrize(
    "read_failure",
    [
        pytest.param(URLError("provider-url-detail"), id="url-error"),
        pytest.param(TimeoutError("provider-timeout-detail"), id="timeout-error"),
        pytest.param(OSError("provider-os-detail"), id="os-error"),
    ],
)
def test_http_error_body_read_failure_is_sanitized_typed_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
    read_failure: OSError,
) -> None:
    reader = RaisingBytesIO(read_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert str(read_failure) not in str(error.value)
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True
    assert len(urlopen_calls) == 1


def test_success_response_body_os_error_retains_typed_transport_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_failure = OSError("provider-success-body-detail")
    response = RaisingUrlResponse(read_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert str(read_failure) not in str(error.value)
    assert response.read_limits == [4]
    assert len(urlopen_calls) == 1


def test_success_response_incomplete_read_is_sanitized_without_partial_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial_body = b"partial-provider-secret"
    read_failure = IncompleteRead(partial_body, 99)
    response = RaisingUrlResponse(read_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert read_failure.partial == partial_body
    assert read_failure.expected == 99
    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert partial_body.decode() not in str(error.value)
    assert str(read_failure.expected) not in str(error.value)
    assert response.read_limits == [4]
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("protocol_failure_type", PROTOCOL_FAILURE_TYPES)
def test_success_response_protocol_failure_is_sanitized_typed_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
    protocol_failure_type: type[HTTPException],
) -> None:
    provider_detail = "success-body-provider-protocol-secret"
    read_failure = protocol_failure_type(provider_detail)
    assert provider_detail in str(read_failure)
    response = RaisingUrlResponse(read_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert provider_detail not in str(error.value)
    assert response.enter_calls == 1
    assert response.exit_calls == 1
    assert response.read_limits == [4]
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("unmapped_failure_type", UNMAPPED_HTTP_EXCEPTION_TYPES)
def test_other_success_response_http_exception_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    unmapped_failure_type: type[HTTPException],
) -> None:
    read_failure = unmapped_failure_type("success-body-unmapped-detail")
    response = RaisingUrlResponse(read_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(unmapped_failure_type) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is read_failure
    assert response.read_limits == [4]
    assert len(urlopen_calls) == 1


def test_http_error_body_incomplete_read_is_sanitized_without_partial_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial_body = b"partial-http-error-secret"
    read_failure = IncompleteRead(partial_body, 123)
    reader = RaisingBytesIO(read_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert read_failure.partial == partial_body
    assert read_failure.expected == 123
    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert partial_body.decode() not in str(error.value)
    assert str(read_failure.expected) not in str(error.value)
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("protocol_failure_type", PROTOCOL_FAILURE_TYPES)
def test_http_error_body_protocol_failure_is_sanitized_typed_transport_failure(
    monkeypatch: pytest.MonkeyPatch,
    protocol_failure_type: type[HTTPException],
) -> None:
    provider_detail = "http-error-body-provider-protocol-secret"
    read_failure = protocol_failure_type(provider_detail)
    assert provider_detail in str(read_failure)
    reader = RaisingBytesIO(read_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert provider_detail not in str(error.value)
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("unmapped_failure_type", UNMAPPED_HTTP_EXCEPTION_TYPES)
def test_other_http_error_body_http_exception_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    unmapped_failure_type: type[HTTPException],
) -> None:
    read_failure = unmapped_failure_type("http-error-body-unmapped-detail")
    reader = RaisingBytesIO(read_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(unmapped_failure_type) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is read_failure
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True
    assert len(urlopen_calls) == 1


def test_http_error_header_failure_still_closes_before_propagation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = RecordingBytesIO(b"err")
    header_failure = ValueError("provider-header-detail")
    headers = RaisingMessage(header_failure)
    provider_error = http_error(reader, headers=headers)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is header_failure
    assert headers.items_calls == 1
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is True


@pytest.mark.parametrize(
    "close_failure",
    [
        pytest.param(URLError("close-url-detail"), id="url-error"),
        pytest.param(TimeoutError("close-timeout-detail"), id="timeout-error"),
        pytest.param(OSError("close-os-detail"), id="os-error"),
        pytest.param(IncompleteRead(b"close-partial", 55), id="incomplete-read"),
    ],
)
def test_http_error_close_failure_without_primary_is_sanitized_typed_failure(
    monkeypatch: pytest.MonkeyPatch,
    close_failure: Exception,
) -> None:
    reader = RecordingBytesIO(b"err", close_error=close_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is close_failure
    assert str(close_failure) not in str(error.value)
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False
    assert len(urlopen_calls) == 1


def test_non_os_close_failure_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_failure = RuntimeError("programmer-close-detail")
    reader = RecordingBytesIO(b"err", close_error=close_failure)
    provider_error = http_error(reader)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(RuntimeError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is close_failure
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False


@pytest.mark.parametrize("protocol_failure_type", PROTOCOL_FAILURE_TYPES)
def test_protocol_close_failure_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    protocol_failure_type: type[HTTPException],
) -> None:
    close_failure = protocol_failure_type("close-provider-protocol-detail")
    reader = RecordingBytesIO(b"err", close_error=close_failure)
    provider_error = http_error(reader)
    urlopen_calls: list[Request] = []

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(protocol_failure_type) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is close_failure
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize(
    "close_failure",
    [
        pytest.param(OSError("secondary-close-os-detail"), id="os-error"),
        pytest.param(RuntimeError("secondary-close-runtime-detail"), id="runtime-error"),
        pytest.param(KeyboardInterrupt(), id="keyboard-interrupt"),
    ],
)
def test_http_error_read_failure_remains_primary_when_close_also_fails(
    monkeypatch: pytest.MonkeyPatch,
    close_failure: BaseException,
) -> None:
    read_failure = OSError("primary-read-detail")
    reader = RaisingBytesIO(read_failure, close_error=close_failure)
    provider_error = http_error(reader)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is read_failure
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False


def test_http_error_oversize_failure_remains_primary_when_close_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_failure = OSError("secondary-close-detail")
    reader = RecordingBytesIO(b"oops", close_error=close_failure)
    provider_error = http_error(reader)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP error response exceeded the configured limit"
    assert error.value.__cause__ is provider_error
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False


def test_http_error_header_failure_remains_primary_when_close_also_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    header_failure = ValueError("primary-header-detail")
    close_failure = OSError("secondary-close-detail")
    reader = RecordingBytesIO(b"err", close_error=close_failure)
    headers = RaisingMessage(header_failure)
    provider_error = http_error(reader, headers=headers)

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is header_failure
    assert headers.items_calls == 1
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False


def test_same_http_error_raised_during_processing_remains_primary_on_close_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_failure = OSError("secondary-close-detail")
    reader = RecordingBytesIO(b"err", close_error=close_failure)
    headers = RaisingMessage(RuntimeError("placeholder"))
    provider_error = http_error(reader, headers=headers)
    headers.error = provider_error

    def raise_http_error(request: Request, *, timeout: float) -> StubUrlResponse:
        del request, timeout
        raise provider_error

    monkeypatch.setattr(http_adapter, "urlopen", raise_http_error)

    with pytest.raises(HTTPError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is provider_error
    assert headers.items_calls == 1
    assert reader.read_limits == [4]
    assert provider_error.close_calls == 1
    assert reader.close_calls == 1
    assert reader.closed is False


def test_incomplete_read_raised_directly_by_urlopen_is_sanitized_typed_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partial_body = b"pre-response-partial-secret"
    transport_failure = IncompleteRead(partial_body, 77)
    urlopen_calls: list[Request] = []

    def raise_before_response(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise transport_failure

    monkeypatch.setattr(http_adapter, "urlopen", raise_before_response)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is transport_failure
    assert transport_failure.partial == partial_body
    assert transport_failure.expected == 77
    assert partial_body.decode() not in str(error.value)
    assert str(transport_failure.expected) not in str(error.value)
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("protocol_failure_type", PROTOCOL_FAILURE_TYPES)
def test_protocol_failure_raised_directly_by_urlopen_is_sanitized_typed_failure(
    monkeypatch: pytest.MonkeyPatch,
    protocol_failure_type: type[HTTPException],
) -> None:
    provider_detail = "acquisition-provider-protocol-secret"
    transport_failure = protocol_failure_type(provider_detail)
    assert provider_detail in str(transport_failure)
    urlopen_calls: list[Request] = []

    def raise_before_response(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise transport_failure

    monkeypatch.setattr(http_adapter, "urlopen", raise_before_response)

    with pytest.raises(HttpTransportError) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert str(error.value) == "public HTTP GET failed"
    assert error.value.__cause__ is transport_failure
    assert provider_detail not in str(error.value)
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("unmapped_failure_type", UNMAPPED_HTTP_EXCEPTION_TYPES)
def test_other_http_exception_raised_directly_by_urlopen_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    unmapped_failure_type: type[HTTPException],
) -> None:
    transport_failure = unmapped_failure_type("acquisition-unmapped-detail")
    urlopen_calls: list[Request] = []

    def raise_before_response(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise transport_failure

    monkeypatch.setattr(http_adapter, "urlopen", raise_before_response)

    with pytest.raises(unmapped_failure_type) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is transport_failure
    assert len(urlopen_calls) == 1


@pytest.mark.parametrize("stage", ["enter", "exit"])
def test_incomplete_read_from_response_context_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    transport_failure = IncompleteRead(f"{stage}-partial".encode(), 88)
    response = ContextFailureUrlResponse(stage=stage, error=transport_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(IncompleteRead) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is transport_failure
    assert len(urlopen_calls) == 1
    assert response.enter_calls == 1
    assert response.exit_calls == (1 if stage == "exit" else 0)
    assert response.read_limits == ([4] if stage == "exit" else [])


@pytest.mark.parametrize("protocol_failure_type", PROTOCOL_FAILURE_TYPES)
@pytest.mark.parametrize("stage", ["enter", "exit"])
def test_protocol_failure_from_response_context_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
    protocol_failure_type: type[HTTPException],
    stage: str,
) -> None:
    transport_failure = protocol_failure_type(f"{stage}-provider-protocol-secret")
    response = ContextFailureUrlResponse(stage=stage, error=transport_failure)
    urlopen_calls: list[Request] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    with pytest.raises(protocol_failure_type) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is transport_failure
    assert len(urlopen_calls) == 1
    assert response.enter_calls == 1
    assert response.exit_calls == (1 if stage == "exit" else 0)
    assert response.read_limits == ([4] if stage == "exit" else [])


@pytest.mark.parametrize("timeout_seconds", INVALID_TIMEOUTS)
def test_invalid_timeout_fails_before_request_construction_or_network(
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: float,
) -> None:
    calls: list[str] = []

    def unexpected_request_or_network_call(*args: object, **kwargs: object) -> object:
        del args, kwargs
        calls.append("called")
        raise AssertionError("invalid timeout reached request construction or network")

    monkeypatch.setattr(http_adapter, "Request", unexpected_request_or_network_call)
    monkeypatch.setattr(http_adapter, "urlopen", unexpected_request_or_network_call)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="https://example.test/public",
            query={"symbol": "BTCUSDT"},
            timeout_seconds=timeout_seconds,
        )

    assert str(error.value) == "timeout_seconds must be finite and positive"
    assert calls == []


@pytest.mark.parametrize("timeout_seconds", INVALID_TIMEOUTS)
def test_invalid_timeout_precedes_initial_url_and_query_validation(
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: float,
) -> None:
    validator_calls: list[str] = []
    query = ExplodingQuery()

    def unexpected_url_validation(url: str) -> None:
        validator_calls.append(url)
        raise AssertionError("invalid timeout reached initial URL validation")

    monkeypatch.setattr(http_adapter, "_validate_initial_url", unexpected_url_validation)

    with pytest.raises(ValueError) as error:
        UrllibPublicHttpClient().get(
            url="http://invalid-timeout.test/not-contacted",
            query=query,
            timeout_seconds=timeout_seconds,
        )

    assert str(error.value) == "timeout_seconds must be finite and positive"
    assert validator_calls == []
    assert query.calls == []


@pytest.mark.parametrize(
    "timeout_seconds",
    [
        pytest.param(1, id="integer"),
        pytest.param(10**1_000, id="large-integer"),
        pytest.param(0.25, id="fractional"),
    ],
)
def test_finite_positive_timeout_is_forwarded_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: float,
) -> None:
    response = StubUrlResponse()
    calls: list[tuple[str, str | None, float]] = []

    def stub_urlopen(request: Request, *, timeout: float) -> StubUrlResponse:
        calls.append((request.full_url, request.get_header("User-agent"), timeout))
        return response

    monkeypatch.setattr(http_adapter, "urlopen", stub_urlopen)

    result = UrllibPublicHttpClient().get(
        url="https://example.test/public",
        query={"b": "2", "a": "1"},
        timeout_seconds=timeout_seconds,
    )

    assert calls == [
        (
            "https://example.test/public?a=1&b=2",
            "WEALTH/0.1 public-market-data",
            timeout_seconds,
        )
    ]
    assert calls[0][2] is timeout_seconds
    assert response.read_limits == [2_000_001]
    assert result.status_code == 200
    assert result.headers == (("Content-Type", "application/json"),)
    assert result.body == b'{"ok":true}'
