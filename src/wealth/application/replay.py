"""Point-in-time market replay that prevents future-data leakage."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from wealth.domain.market import CanonicalCandle


class ReplayErrorCode(StrEnum):
    """Machine-readable replay validation failures."""

    NAIVE_EVALUATION_TIME = "naive_evaluation_time"
    DUPLICATE_RECORD = "duplicate_record"
    CONFLICTING_RECORD = "conflicting_record"


class ReplayValidationError(ValueError):
    """Fail closed when replay input cannot be interpreted unambiguously."""

    def __init__(self, code: ReplayErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


@dataclass(frozen=True, slots=True)
class ReplaySlice:
    """Records that were knowable at one evaluation time."""

    evaluation_time: datetime
    records: tuple[CanonicalCandle, ...]
    withheld_count: int
    next_observation_time: datetime | None


class MarketReplay:
    """Build deterministic, point-in-time slices from canonical candles."""

    def __init__(self, records: Iterable[CanonicalCandle]) -> None:
        unique: dict[tuple[object, ...], CanonicalCandle] = {}
        for record in records:
            key = record.natural_key
            previous = unique.get(key)
            if previous is not None:
                code = (
                    ReplayErrorCode.DUPLICATE_RECORD
                    if previous.market_values == record.market_values
                    else ReplayErrorCode.CONFLICTING_RECORD
                )
                raise ReplayValidationError(code, repr(key))
            unique[key] = record

        self._records_by_observation = tuple(
            sorted(
                unique.values(),
                key=lambda record: (
                    record.observed_at,
                    record.close_time,
                    record.source,
                    record.venue,
                    record.instrument,
                    str(record.record_id),
                ),
            )
        )

    def slice_at(self, evaluation_time: datetime) -> ReplaySlice:
        """Expose only records observed by the requested point in time."""

        if evaluation_time.tzinfo is None or evaluation_time.utcoffset() is None:
            raise ReplayValidationError(
                ReplayErrorCode.NAIVE_EVALUATION_TIME,
                "evaluation_time must be timezone-aware",
            )

        available_by_observation = tuple(
            record
            for record in self._records_by_observation
            if record.observed_at <= evaluation_time
        )
        visible = tuple(
            sorted(
                available_by_observation,
                key=lambda record: (
                    record.open_time,
                    record.close_time,
                    record.source,
                    record.venue,
                    record.instrument,
                    record.instrument_type,
                    str(record.record_id),
                ),
            )
        )
        withheld = self._records_by_observation[len(available_by_observation) :]
        next_observation = withheld[0].observed_at if withheld else None
        return ReplaySlice(
            evaluation_time=evaluation_time,
            records=visible,
            withheld_count=len(withheld),
            next_observation_time=next_observation,
        )
