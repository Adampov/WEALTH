"""Minimal public HTTP boundary used by read-only provider adapters."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


class HttpTransportError(RuntimeError):
    """Report a network failure without exposing provider content."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    """Bounded HTTP response returned to a provider adapter."""

    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def header(self, name: str) -> str | None:
        """Return one case-insensitive response header."""

        expected = name.casefold()
        for header_name, value in self.headers:
            if header_name.casefold() == expected:
                return value
        return None


class PublicHttpClient(Protocol):
    """Perform an unauthenticated HTTP GET with a finite timeout."""

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        """Return a bounded response or raise ``HttpTransportError``."""
