"""Tests for the bounded standard-library public HTTP adapter."""

from email.message import Message
from http.client import IncompleteRead
from io import BytesIO
from types import TracebackType
from typing import Self
from urllib.error import HTTPError, URLError
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


def test_incomplete_read_raised_before_body_read_remains_unmapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport_failure = IncompleteRead(b"non-body-partial", 77)
    urlopen_calls: list[Request] = []

    def raise_before_response(request: Request, *, timeout: float) -> StubUrlResponse:
        del timeout
        urlopen_calls.append(request)
        raise transport_failure

    monkeypatch.setattr(http_adapter, "urlopen", raise_before_response)

    with pytest.raises(IncompleteRead) as error:
        UrllibPublicHttpClient(max_response_bytes=3).get(
            url="https://example.test/public",
            query={},
            timeout_seconds=1,
        )

    assert error.value is transport_failure
    assert len(urlopen_calls) == 1


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
