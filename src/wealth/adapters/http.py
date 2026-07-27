"""Standard-library public HTTP adapter with bounded reads."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from http.client import (
    BadStatusLine,
    HTTPMessage,
    HTTPResponse,
    IncompleteRead,
    LineTooLong,
    UnknownProtocol,
)
from math import isfinite
from typing import IO, NoReturn, Protocol, cast
from unicodedata import normalize
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from wealth.ports.http import (
    MAX_PUBLIC_HTTP_TIMEOUT_SECONDS,
    HttpResponse,
    HttpTransportError,
)

MAX_PUBLIC_HTTP_RESPONSE_BYTES = 2_000_000
_MAX_INITIAL_URL_CHARACTERS = 8_192
_MAX_USER_AGENT_CHARACTERS = 256
_MAX_QUERY_PAIRS = 32
_MAX_QUERY_CHARACTERS = 8_192
_MAX_RESPONSE_HEADER_PAIRS = 100
_MAX_RESPONSE_HEADER_CHARACTERS = 65_536
_INVALID_INITIAL_URL_LENGTH_MESSAGE = "url must contain at most 8192 characters"
_INVALID_INITIAL_URL_MESSAGE = (
    "url must be an absolute credential-free HTTPS endpoint without query or fragment"
)
_INVALID_TARGET_PORT_MESSAGE = "url must use the standard HTTPS target port"
_INVALID_QUERY_MESSAGE = (
    "query must contain at most 32 built-in string pairs totaling at most 8192 characters"
)
_INVALID_USER_AGENT_MESSAGE = (
    "user_agent must be a built-in string of 1 to 256 visible ASCII characters"
)
_RESPONSE_HEADERS_EXCEEDED_MESSAGE = "public HTTP response headers exceeded the configured limit"


class _ResponseHeaders(Protocol):
    def items(self) -> Iterable[tuple[str, str]]:
        """Return response-header pairs in source order."""


def _validate_initial_url(url: str) -> None:
    """Reject unsafe or structurally ambiguous initial public request targets."""

    if str.__len__(url) > _MAX_INITIAL_URL_CHARACTERS:
        raise ValueError(_INVALID_INITIAL_URL_LENGTH_MESSAGE)
    if (
        "?" in url
        or "#" in url
        or "\\" in url
        or any(
            character.isspace()
            or ord(character) < 0x20
            or ord(character) == 0x7F
            or 0xD800 <= ord(character) <= 0xDFFF
            for character in url
        )
    ):
        raise ValueError(_INVALID_INITIAL_URL_MESSAGE)
    try:
        target = urlsplit(url)
        hostname = target.hostname
        username = target.username
        port = target.port
        normalized_netloc = normalize("NFKC", target.netloc)
    except ValueError:
        raise ValueError(_INVALID_INITIAL_URL_MESSAGE) from None
    if (
        target.scheme != "https"
        or not hostname
        or username is not None
        or "%" in normalized_netloc
        or "\\" in normalized_netloc
        or any(
            character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F
            for character in normalized_netloc
        )
        or target.netloc.endswith(":")
        or port == 0
    ):
        raise ValueError(_INVALID_INITIAL_URL_MESSAGE)
    if port not in (None, 443):
        raise ValueError(_INVALID_TARGET_PORT_MESSAGE)


def _snapshot_query(query: Mapping[str, str]) -> list[tuple[str, str]]:
    """Return one bounded, exact query-item snapshot."""

    items_iterator = iter(query.items())
    snapshot: list[tuple[str, str]] = []
    total_characters = 0
    for index in range(_MAX_QUERY_PAIRS + 1):
        try:
            item = next(items_iterator)
        except StopIteration:
            return snapshot
        if index == _MAX_QUERY_PAIRS:
            raise ValueError(_INVALID_QUERY_MESSAGE)
        if type(item) is not tuple or len(item) != 2:
            raise ValueError(_INVALID_QUERY_MESSAGE)
        key, value = item
        if type(key) is not str or type(value) is not str:
            raise ValueError(_INVALID_QUERY_MESSAGE)
        total_characters += len(key) + len(value)
        if total_characters > _MAX_QUERY_CHARACTERS:
            raise ValueError(_INVALID_QUERY_MESSAGE)
        snapshot.append(item)
    return snapshot


def _raise_response_header_limit(limit_cause: BaseException | None) -> NoReturn:
    if limit_cause is None:
        raise HttpTransportError(_RESPONSE_HEADERS_EXCEEDED_MESSAGE)
    raise HttpTransportError(_RESPONSE_HEADERS_EXCEEDED_MESSAGE) from limit_cause


def _snapshot_response_headers(
    headers: _ResponseHeaders,
    *,
    limit_cause: BaseException | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return one bounded, order-preserving response-header snapshot."""

    items_iterator = iter(headers.items())
    snapshot: list[tuple[str, str]] = []
    total_characters = 0
    for index in range(_MAX_RESPONSE_HEADER_PAIRS + 1):
        try:
            item = next(items_iterator)
        except StopIteration:
            return tuple(snapshot)
        if index == _MAX_RESPONSE_HEADER_PAIRS:
            _raise_response_header_limit(limit_cause)
        name, value = item
        total_characters += len(name) + len(value)
        if total_characters > _MAX_RESPONSE_HEADER_CHARACTERS:
            _raise_response_header_limit(limit_cause)
        snapshot.append(item)
    return tuple(snapshot)


class _NoRedirectHandler(HTTPRedirectHandler):
    """Reject automatic redirects before urllib drains or follows them."""

    def http_error_302(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
    ) -> None:
        del req, fp, code, msg, headers
        return None

    http_error_301 = http_error_302
    http_error_303 = http_error_302
    http_error_307 = http_error_302
    http_error_308 = http_error_302


_NO_REDIRECT_OPENER = build_opener(_NoRedirectHandler())


def urlopen(request: Request, *, timeout: float) -> HTTPResponse:
    """Open one public URL without process-global or automatic redirect behavior."""

    return cast(HTTPResponse, _NO_REDIRECT_OPENER.open(request, timeout=timeout))


@dataclass(frozen=True, slots=True)
class UrllibPublicHttpClient:
    """Issue public GET requests without credentials or implicit retries."""

    user_agent: str = "WEALTH/0.1 public-market-data"
    max_response_bytes: int = MAX_PUBLIC_HTTP_RESPONSE_BYTES

    def __post_init__(self) -> None:
        if (
            type(self.max_response_bytes) is not int
            or self.max_response_bytes < 1
            or self.max_response_bytes > MAX_PUBLIC_HTTP_RESPONSE_BYTES
        ):
            raise ValueError("max_response_bytes must be an integer between 1 and 2000000")
        if type(self.user_agent) is not str:
            raise ValueError(_INVALID_USER_AGENT_MESSAGE)
        if not 1 <= len(self.user_agent) <= _MAX_USER_AGENT_CHARACTERS:
            raise ValueError(_INVALID_USER_AGENT_MESSAGE)
        if any(ord(character) < 0x20 or ord(character) > 0x7E for character in self.user_agent):
            raise ValueError(_INVALID_USER_AGENT_MESSAGE)

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        """Return one bounded response, including HTTP error responses."""

        if timeout_seconds <= 0 or (
            not isinstance(timeout_seconds, int) and not isfinite(timeout_seconds)
        ):
            raise ValueError("timeout_seconds must be finite and positive")
        if timeout_seconds > MAX_PUBLIC_HTTP_TIMEOUT_SECONDS:
            raise ValueError("timeout_seconds must be at most 120")
        _validate_initial_url(url)
        query_snapshot = _snapshot_query(query)
        query_string = urlencode(sorted(query_snapshot))
        request = Request(
            f"{url}?{query_string}",
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            method="GET",
        )
        success_header_projection_failure: BaseException | None = None
        try:
            try:
                response_context = urlopen(request, timeout=timeout_seconds)
            except (
                IncompleteRead,
                BadStatusLine,
                LineTooLong,
                UnknownProtocol,
            ) as acquisition_error:
                raise HttpTransportError("public HTTP GET failed") from acquisition_error
            with response_context as response:
                try:
                    body = response.read(self.max_response_bytes + 1)
                except (
                    IncompleteRead,
                    BadStatusLine,
                    LineTooLong,
                    UnknownProtocol,
                ) as read_error:
                    raise HttpTransportError("public HTTP GET failed") from read_error
                if len(body) > self.max_response_bytes:
                    raise HttpTransportError("public HTTP response exceeded the configured limit")
                try:
                    headers = _snapshot_response_headers(response.headers)
                except BaseException as projection_error:
                    success_header_projection_failure = projection_error
                    raise
                return HttpResponse(
                    status_code=response.status,
                    headers=headers,
                    body=body,
                )
        except HTTPError as error:
            if error is success_header_projection_failure:
                raise
            response_materialized = False
            try:
                try:
                    body = error.read(self.max_response_bytes + 1)
                except (
                    URLError,
                    TimeoutError,
                    OSError,
                    IncompleteRead,
                    BadStatusLine,
                    LineTooLong,
                    UnknownProtocol,
                ) as read_error:
                    raise HttpTransportError("public HTTP GET failed") from read_error
                if len(body) > self.max_response_bytes:
                    raise HttpTransportError(
                        "public HTTP error response exceeded the configured limit"
                    ) from error
                headers = _snapshot_response_headers(error.headers, limit_cause=error)
                materialized_response = HttpResponse(
                    status_code=error.code,
                    headers=headers,
                    body=body,
                )
                response_materialized = True
                return materialized_response
            finally:
                try:
                    error.close()
                except (OSError, IncompleteRead) as close_error:
                    if response_materialized:
                        raise HttpTransportError("public HTTP GET failed") from close_error
                except BaseException:
                    if response_materialized:
                        raise
        except (URLError, TimeoutError, OSError) as error:
            if error is success_header_projection_failure:
                raise
            raise HttpTransportError("public HTTP GET failed") from error
