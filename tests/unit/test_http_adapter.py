"""Tests for the bounded standard-library public HTTP adapter."""

from email.message import Message
from io import BytesIO
from types import TracebackType
from typing import Self
from urllib.error import HTTPError
from urllib.request import Request

import pytest

from wealth.adapters import http as http_adapter
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


class IntegerSubclass(int):
    """Represent an integer whose runtime type is not the built-in type."""


class StubUrlResponse:
    """Expose the bounded response surface used by the transport."""

    status = 200

    def __init__(self, body: bytes = b'{"ok":true}') -> None:
        self.body = body
        self.headers = {"Content-Type": "application/json"}
        self.read_limits: list[int] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, exception, traceback

    def read(self, limit: int) -> bytes:
        self.read_limits.append(limit)
        return self.body


class RecordingBytesIO(BytesIO):
    """Record the byte sentinel requested through an HTTPError body."""

    def __init__(self, body: bytes) -> None:
        super().__init__(body)
        self.read_limits: list[int | None] = []

    def read(self, size: int | None = -1) -> bytes:
        self.read_limits.append(size)
        return super().read(size)


def http_error(reader: RecordingBytesIO) -> HTTPError:
    """Build one deterministic public HTTP error with a recording body."""

    headers = Message()
    headers["Retry-After"] = "1"
    return HTTPError(
        "https://example.test/public",
        429,
        "rate limited",
        headers,
        reader,
    )


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
