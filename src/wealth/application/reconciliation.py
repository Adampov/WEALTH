"""Deterministic comparison of quality-audited candle streams."""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Context, Decimal, localcontext
from enum import StrEnum

from wealth.application.quality import CandleSequenceAuditor
from wealth.domain.market import CanonicalCandle
from wealth.domain.quality import CandleStream, DataQualityStatus
from wealth.domain.reconciliation import (
    CandleIntervalComparison,
    CandleReconciliationIssue,
    CandleReconciliationIssueCode,
    CandleReconciliationPolicy,
    CandleReconciliationReport,
    CandleReconciliationStatus,
)

BASIS_POINTS = Decimal("10000")
RECONCILIATION_CONTEXT = Context(prec=34)
MAX_RECONCILIATION_CANDLES = 100_000


class CandleReconciliationErrorCode(StrEnum):
    """Machine-readable invalid reconciliation requests."""

    SAME_STREAM = "same_stream"
    INCOMPATIBLE_STREAMS = "incompatible_streams"
    WINDOW_TOO_LARGE = "window_too_large"


class CandleReconciliationError(ValueError):
    """Reject unsafe or ambiguous cross-source comparisons."""

    def __init__(self, code: CandleReconciliationErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


@dataclass(frozen=True, slots=True)
class CandleCrossSourceReconciler:
    """Compare selected venue streams without treating either as market truth."""

    auditor: CandleSequenceAuditor = field(default_factory=CandleSequenceAuditor)

    def reconcile(
        self,
        *,
        comparison_key: str,
        primary_stream: CandleStream,
        reference_stream: CandleStream,
        window_start: datetime,
        window_end_exclusive: datetime,
        primary_records: Iterable[CanonicalCandle],
        reference_records: Iterable[CanonicalCandle],
        policy: CandleReconciliationPolicy,
    ) -> CandleReconciliationReport:
        """Return bounded quality evidence and symmetric cross-source differences."""

        self._validate_streams(primary_stream, reference_stream)
        self._validate_window_bound(
            stream=primary_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
        )
        primary_input = tuple(primary_records)
        reference_input = tuple(reference_records)
        primary_quality = self.auditor.audit(
            stream=primary_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            records=primary_input,
        )
        reference_quality = self.auditor.audit(
            stream=reference_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            records=reference_input,
        )
        primary_by_time = self._usable_records(
            stream=primary_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            records=primary_input,
        )
        reference_by_time = self._usable_records(
            stream=reference_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            records=reference_input,
        )

        comparisons: list[CandleIntervalComparison] = []
        issues: list[CandleReconciliationIssue] = []
        expected_count = (window_end_exclusive - window_start) // primary_stream.timeframe.duration
        for index in range(expected_count):
            open_time = window_start + index * primary_stream.timeframe.duration
            primary = primary_by_time.get(open_time)
            reference = reference_by_time.get(open_time)
            if primary is None:
                issues.append(
                    CandleReconciliationIssue(
                        code=CandleReconciliationIssueCode.PRIMARY_MISSING,
                        open_time=open_time,
                        reference_record_id=(
                            reference.record_id if reference is not None else None
                        ),
                    )
                )
            if reference is None:
                issues.append(
                    CandleReconciliationIssue(
                        code=CandleReconciliationIssueCode.REFERENCE_MISSING,
                        open_time=open_time,
                        primary_record_id=primary.record_id if primary is not None else None,
                    )
                )
            if primary is None or reference is None:
                continue

            comparison = self._compare_interval(primary, reference)
            comparisons.append(comparison)
            issues.extend(self._threshold_findings(comparison, policy))

        quality_blocked = (
            primary_quality.status is DataQualityStatus.FAIL
            or reference_quality.status is DataQualityStatus.FAIL
        )
        status = (
            CandleReconciliationStatus.BLOCKED
            if quality_blocked
            else (
                CandleReconciliationStatus.DIVERGENT if issues else CandleReconciliationStatus.PASS
            )
        )
        return CandleReconciliationReport(
            comparison_key=comparison_key,
            primary_stream=primary_stream,
            reference_stream=reference_stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            policy=policy,
            primary_quality=primary_quality,
            reference_quality=reference_quality,
            compared_count=len(comparisons),
            status=status,
            comparisons=tuple(comparisons),
            issues=tuple(issues),
        )

    @staticmethod
    def _validate_streams(primary: CandleStream, reference: CandleStream) -> None:
        if primary == reference:
            raise CandleReconciliationError(
                CandleReconciliationErrorCode.SAME_STREAM,
                "primary and reference streams must be distinct",
            )
        primary_market = (primary.instrument, primary.instrument_type, primary.timeframe)
        reference_market = (reference.instrument, reference.instrument_type, reference.timeframe)
        if primary_market != reference_market:
            raise CandleReconciliationError(
                CandleReconciliationErrorCode.INCOMPATIBLE_STREAMS,
                "streams must use the same canonical instrument, type, and timeframe",
            )

    @staticmethod
    def _validate_window_bound(
        *,
        stream: CandleStream,
        window_start: datetime,
        window_end_exclusive: datetime,
    ) -> None:
        if any(
            value.tzinfo is None or value.utcoffset() is None
            for value in (window_start, window_end_exclusive)
        ):
            return
        if window_end_exclusive <= window_start:
            return
        expected_count, remainder = divmod(
            window_end_exclusive - window_start,
            stream.timeframe.duration,
        )
        if remainder == timedelta(0) and expected_count > MAX_RECONCILIATION_CANDLES:
            raise CandleReconciliationError(
                CandleReconciliationErrorCode.WINDOW_TOO_LARGE,
                f"comparison exceeds {MAX_RECONCILIATION_CANDLES} candles",
            )

    @staticmethod
    def _usable_records(
        *,
        stream: CandleStream,
        window_start: datetime,
        window_end_exclusive: datetime,
        records: tuple[CanonicalCandle, ...],
    ) -> dict[datetime, CanonicalCandle]:
        candidates: dict[datetime, list[CanonicalCandle]] = defaultdict(list)
        for record in records:
            if stream.contains(record) and window_start <= record.open_time < window_end_exclusive:
                candidates[record.open_time].append(record)

        usable: dict[datetime, CanonicalCandle] = {}
        for open_time, same_interval in candidates.items():
            first = same_interval[0]
            if len(same_interval) == 1 or all(
                record.market_values == first.market_values for record in same_interval[1:]
            ):
                usable[open_time] = min(same_interval, key=lambda record: str(record.record_id))
        return usable

    @classmethod
    def _compare_interval(
        cls,
        primary: CanonicalCandle,
        reference: CanonicalCandle,
    ) -> CandleIntervalComparison:
        return CandleIntervalComparison(
            open_time=primary.open_time,
            primary_record_id=primary.record_id,
            reference_record_id=reference.record_id,
            open_difference_bps=cls._symmetric_difference_bps(primary.open, reference.open),
            high_difference_bps=cls._symmetric_difference_bps(primary.high, reference.high),
            low_difference_bps=cls._symmetric_difference_bps(primary.low, reference.low),
            close_difference_bps=cls._symmetric_difference_bps(primary.close, reference.close),
            base_volume_difference_bps=cls._symmetric_difference_bps(
                primary.base_volume,
                reference.base_volume,
            ),
        )

    @staticmethod
    def _symmetric_difference_bps(primary: Decimal, reference: Decimal) -> Decimal:
        denominator = max(abs(primary), abs(reference))
        if denominator == 0:
            return Decimal("0")
        with localcontext(RECONCILIATION_CONTEXT):
            return abs(primary - reference) * BASIS_POINTS / denominator

    @staticmethod
    def _threshold_findings(
        comparison: CandleIntervalComparison,
        policy: CandleReconciliationPolicy,
    ) -> tuple[CandleReconciliationIssue, ...]:
        findings: list[CandleReconciliationIssue] = []
        price_metrics = (
            (
                CandleReconciliationIssueCode.OPEN_PRICE_DIVERGENCE,
                comparison.open_difference_bps,
            ),
            (
                CandleReconciliationIssueCode.HIGH_PRICE_DIVERGENCE,
                comparison.high_difference_bps,
            ),
            (
                CandleReconciliationIssueCode.LOW_PRICE_DIVERGENCE,
                comparison.low_difference_bps,
            ),
            (
                CandleReconciliationIssueCode.CLOSE_PRICE_DIVERGENCE,
                comparison.close_difference_bps,
            ),
        )
        for code, difference in price_metrics:
            if difference > policy.max_price_difference_bps:
                findings.append(
                    CandleReconciliationIssue(
                        code=code,
                        open_time=comparison.open_time,
                        primary_record_id=comparison.primary_record_id,
                        reference_record_id=comparison.reference_record_id,
                        difference_bps=difference,
                        limit_bps=policy.max_price_difference_bps,
                    )
                )
        volume_limit = policy.max_base_volume_difference_bps
        if volume_limit is not None and comparison.base_volume_difference_bps > volume_limit:
            findings.append(
                CandleReconciliationIssue(
                    code=CandleReconciliationIssueCode.BASE_VOLUME_DIVERGENCE,
                    open_time=comparison.open_time,
                    primary_record_id=comparison.primary_record_id,
                    reference_record_id=comparison.reference_record_id,
                    difference_bps=comparison.base_volume_difference_bps,
                    limit_bps=volume_limit,
                )
            )
        return tuple(findings)
