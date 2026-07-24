"""Safe in-memory market-data adapters for contract validation."""

from dataclasses import dataclass, field

from wealth.domain.market import CanonicalCandle
from wealth.domain.quality import (
    CandleStream,
    CandleWriteResult,
    CandleWriteStatus,
)


@dataclass(slots=True)
class InMemoryCandleStore:
    """Idempotent store that never replaces conflicting canonical data."""

    _records: dict[tuple[object, ...], CanonicalCandle] = field(default_factory=dict)

    def append(self, candle: CanonicalCandle) -> CandleWriteResult:
        """Insert one candle or report duplicate/conflict without mutation."""

        existing = self._records.get(candle.natural_key)
        if existing is None:
            self._records[candle.natural_key] = candle
            return CandleWriteResult(
                status=CandleWriteStatus.INSERTED,
                incoming_record_id=candle.record_id,
            )

        status = (
            CandleWriteStatus.DUPLICATE
            if existing.market_values == candle.market_values
            else CandleWriteStatus.CONFLICT
        )
        return CandleWriteResult(
            status=status,
            incoming_record_id=candle.record_id,
            existing_record_id=existing.record_id,
        )

    def records_for_stream(self, stream: CandleStream) -> tuple[CanonicalCandle, ...]:
        """Return one stream sorted deterministically by market time."""

        return tuple(
            sorted(
                (candle for candle in self._records.values() if stream.contains(candle)),
                key=lambda candle: (
                    candle.open_time,
                    candle.close_time,
                    str(candle.record_id),
                ),
            )
        )
