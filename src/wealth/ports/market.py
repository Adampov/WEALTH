"""Ports for canonical market-data persistence."""

from typing import Protocol

from wealth.domain.market import CanonicalCandle
from wealth.domain.quality import CandleStream, CandleWriteResult


class CandleStore(Protocol):
    """Append canonical candles without silently overwriting conflicts."""

    def append(self, candle: CanonicalCandle) -> CandleWriteResult:
        """Insert once or return an explicit idempotency outcome."""

    def records_for_stream(self, stream: CandleStream) -> tuple[CanonicalCandle, ...]:
        """Return an immutable, market-time-ordered stream snapshot."""
