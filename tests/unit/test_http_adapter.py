"""Tests for the bounded standard-library public HTTP adapter."""

from types import TracebackType
from typing import Self
from urllib.request import Request

import pytest

from wealth.adapters import http as http_adapter
from wealth.adapters.http import UrllibPublicHttpClient

INVALID_TIMEOUTS = (
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
    pytest.param(0.0, id="zero"),
    pytest.param(-0.25, id="negative"),
)


class StubUrlResponse:
    """Expose the bounded response surface used by the transport."""

    status = 200

    def __init__(self) -> None:
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
        return b'{"ok":true}'


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
