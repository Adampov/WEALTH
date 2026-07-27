"""Standard-library public HTTP adapter with bounded reads."""

from collections.abc import Mapping
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
from typing import IO, cast
from unicodedata import normalize
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from wealth.ports.http import HttpResponse, HttpTransportError

MAX_PUBLIC_HTTP_RESPONSE_BYTES = 2_000_000
_INVALID_INITIAL_URL_MESSAGE = (
    "url must be an absolute credential-free HTTPS endpoint without query or fragment"
)


def _validate_initial_url(url: str) -> None:
    """Reject unsafe or structurally ambiguous initial public request targets."""

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
        _validate_initial_url(url)
        query_string = urlencode(sorted(query.items()))
        request = Request(
            f"{url}?{query_string}",
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            method="GET",
        )
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
                return HttpResponse(
                    status_code=response.status,
                    headers=tuple(response.headers.items()),
                    body=body,
                )
        except HTTPError as error:
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
                materialized_response = HttpResponse(
                    status_code=error.code,
                    headers=tuple(error.headers.items()),
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
            raise HttpTransportError("public HTTP GET failed") from error
