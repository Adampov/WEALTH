"""Standard-library public HTTP adapter with bounded reads."""

from collections.abc import Mapping
from dataclasses import dataclass
from http.client import IncompleteRead
from math import isfinite
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from wealth.ports.http import HttpResponse, HttpTransportError

MAX_PUBLIC_HTTP_RESPONSE_BYTES = 2_000_000


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
            with urlopen(request, timeout=timeout_seconds) as response:
                try:
                    body = response.read(self.max_response_bytes + 1)
                except IncompleteRead as read_error:
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
                except (URLError, TimeoutError, OSError, IncompleteRead) as read_error:
                    raise HttpTransportError("public HTTP GET failed") from read_error
                if len(body) > self.max_response_bytes:
                    raise HttpTransportError(
                        "public HTTP error response exceeded the configured limit"
                    ) from error
                response = HttpResponse(
                    status_code=error.code,
                    headers=tuple(error.headers.items()),
                    body=body,
                )
                response_materialized = True
                return response
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
