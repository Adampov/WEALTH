"""Deterministic candle-sequence quality auditing."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from wealth.domain.market import CanonicalCandle
from wealth.domain.quality import (
    CandleQualityCode,
    CandleQualityIssue,
    CandleSequenceReport,
    CandleStream,
    DataQualityStatus,
    MissingCandleRange,
)

MAX_EXPECTED_CANDLES = 1_000_000
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class CandleAuditErrorCode(StrEnum):
    """Machine-readable invalid audit requests."""

    NAIVE_WINDOW = "naive_window"
    INVALID_WINDOW = "invalid_window"
    MISALIGNED_WINDOW = "misaligned_window"
    WINDOW_TOO_LARGE = "window_too_large"


class CandleAuditError(ValueError):
    """Reject ambiguous or unsafe audit configuration."""

    def __init__(self, code: CandleAuditErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class CandleSequenceAuditor:
    """Audit one expected candle window without inventing missing values."""

    def audit(
        self,
        *,
        stream: CandleStream,
        window_start: datetime,
        window_end_exclusive: datetime,
        records: Iterable[CanonicalCandle],
    ) -> CandleSequenceReport:
        """Return explicit sequence findings and contiguous missing ranges."""

        expected_count = self._validate_window(
            stream=stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
        )
        input_records = tuple(records)
        issues: list[CandleQualityIssue] = []
        candidates: dict[datetime, list[CanonicalCandle]] = defaultdict(list)
        previous_record: CanonicalCandle | None = None

        for record in input_records:
            if not stream.contains(record):
                issues.append(
                    CandleQualityIssue(
                        code=CandleQualityCode.MIXED_STREAM,
                        open_time=record.open_time,
                        record_ids=(record.record_id,),
                        detail="record belongs to a different candle stream",
                    )
                )
                continue
            if not window_start <= record.open_time < window_end_exclusive:
                issues.append(
                    CandleQualityIssue(
                        code=CandleQualityCode.OUT_OF_WINDOW,
                        open_time=record.open_time,
                        record_ids=(record.record_id,),
                        detail="record open_time is outside the expected window",
                    )
                )
                continue
            if previous_record is not None and record.open_time < previous_record.open_time:
                issues.append(
                    CandleQualityIssue(
                        code=CandleQualityCode.OUT_OF_ORDER,
                        open_time=record.open_time,
                        record_ids=(
                            previous_record.record_id,
                            record.record_id,
                        ),
                        detail="input sequence regressed in market time",
                    )
                )
            previous_record = record
            candidates[record.open_time].append(record)

        usable_open_times: set[datetime] = set()
        for open_time, same_interval in sorted(candidates.items()):
            first = same_interval[0]
            if len(same_interval) == 1:
                usable_open_times.add(open_time)
                continue

            record_ids = tuple(sorted((record.record_id for record in same_interval), key=str))
            if all(record.market_values == first.market_values for record in same_interval[1:]):
                usable_open_times.add(open_time)
                issues.append(
                    CandleQualityIssue(
                        code=CandleQualityCode.DUPLICATE,
                        open_time=open_time,
                        record_ids=record_ids,
                        detail="multiple records contain the same canonical market values",
                    )
                )
            else:
                issues.append(
                    CandleQualityIssue(
                        code=CandleQualityCode.CONFLICT,
                        open_time=open_time,
                        record_ids=record_ids,
                        detail="records for the same natural key contain conflicting values",
                    )
                )

        expected_open_times = tuple(
            window_start + index * stream.timeframe.duration for index in range(expected_count)
        )
        missing_open_times = tuple(
            open_time for open_time in expected_open_times if open_time not in usable_open_times
        )
        missing_ranges = self._collapse_missing_ranges(
            missing_open_times,
            stream.timeframe.duration,
        )
        sorted_issues = tuple(
            sorted(
                issues,
                key=lambda issue: (
                    issue.open_time or window_start,
                    issue.code,
                    tuple(str(record_id) for record_id in issue.record_ids),
                ),
            )
        )
        status = (
            DataQualityStatus.PASS
            if not sorted_issues and not missing_ranges
            else DataQualityStatus.FAIL
        )
        return CandleSequenceReport(
            stream=stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            input_count=len(input_records),
            accepted_count=len(usable_open_times),
            status=status,
            issues=sorted_issues,
            missing_ranges=missing_ranges,
        )

    @staticmethod
    def _validate_window(
        *,
        stream: CandleStream,
        window_start: datetime,
        window_end_exclusive: datetime,
    ) -> int:
        for value in (window_start, window_end_exclusive):
            if value.tzinfo is None or value.utcoffset() is None:
                raise CandleAuditError(
                    CandleAuditErrorCode.NAIVE_WINDOW,
                    "audit window timestamps must be timezone-aware",
                )
        if window_end_exclusive <= window_start:
            raise CandleAuditError(
                CandleAuditErrorCode.INVALID_WINDOW,
                "window end must be after window start",
            )
        if (window_start.astimezone(UTC) - UTC_EPOCH) % stream.timeframe.duration != timedelta(0):
            raise CandleAuditError(
                CandleAuditErrorCode.MISALIGNED_WINDOW,
                "window start must align to the candle timeframe UTC grid",
            )

        expected_count, remainder = divmod(
            window_end_exclusive - window_start,
            stream.timeframe.duration,
        )
        if remainder != timedelta(0):
            raise CandleAuditError(
                CandleAuditErrorCode.MISALIGNED_WINDOW,
                "window duration must be an exact multiple of the candle timeframe",
            )
        if expected_count > MAX_EXPECTED_CANDLES:
            raise CandleAuditError(
                CandleAuditErrorCode.WINDOW_TOO_LARGE,
                f"window exceeds {MAX_EXPECTED_CANDLES} expected candles",
            )
        return expected_count

    @staticmethod
    def _collapse_missing_ranges(
        missing_open_times: tuple[datetime, ...],
        duration: timedelta,
    ) -> tuple[MissingCandleRange, ...]:
        if not missing_open_times:
            return ()

        ranges: list[MissingCandleRange] = []
        range_start = missing_open_times[0]
        previous = range_start
        count = 1

        for open_time in missing_open_times[1:]:
            if open_time == previous + duration:
                previous = open_time
                count += 1
                continue
            ranges.append(
                MissingCandleRange(
                    start_open_time=range_start,
                    end_open_time_exclusive=previous + duration,
                    missing_count=count,
                )
            )
            range_start = previous = open_time
            count = 1

        ranges.append(
            MissingCandleRange(
                start_open_time=range_start,
                end_open_time_exclusive=previous + duration,
                missing_count=count,
            )
        )
        return tuple(ranges)
