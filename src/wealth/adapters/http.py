"""Standard-library public HTTP adapter with bounded reads."""

from collections.abc import Mapping
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from wealth.ports.http import HttpResponse, HttpTransportError


@dataclass(frozen=True, slots=True)
class UrllibPublicHttpClient:
    """Issue public GET requests without credentials or implicit retries."""

    user_agent: str = "WEALTH/0.1 public-market-data"
    max_response_bytes: int = 2_000_000

    def __post_init__(self) -> None:
        if self.max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        """Return one bounded response, including HTTP error responses."""

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
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
                body = response.read(self.max_response_bytes + 1)
                if len(body) > self.max_response_bytes:
                    raise HttpTransportError("public HTTP response exceeded the configured limit")
                return HttpResponse(
                    status_code=response.status,
                    headers=tuple(response.headers.items()),
                    body=body,
                )
        except HTTPError as error:
            body = error.read(self.max_response_bytes + 1)
            if len(body) > self.max_response_bytes:
                raise HttpTransportError(
                    "public HTTP error response exceeded the configured limit"
                ) from error
            return HttpResponse(
                status_code=error.code,
                headers=tuple(error.headers.items()),
                body=body,
            )
        except (URLError, TimeoutError, OSError) as error:
            raise HttpTransportError("public HTTP GET failed") from error
