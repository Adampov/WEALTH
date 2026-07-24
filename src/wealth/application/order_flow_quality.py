"""Deterministic quality auditing for bounded canonical order-flow streams."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum
from itertools import islice

from wealth.domain.order_flow_quality import (
    MissingProviderSequenceRange,
    OrderFlowQualityCode,
    OrderFlowQualityIssue,
    OrderFlowRecord,
    OrderFlowSequenceReport,
    OrderFlowStream,
    ProviderSequencePolicy,
)
from wealth.domain.quality import DataQualityStatus

MAX_ORDER_FLOW_RECORDS = 100_000


class OrderFlowAuditErrorCode(StrEnum):
    """Machine-readable invalid order-flow audit requests."""

    NAIVE_WINDOW = "naive_window"
    INVALID_WINDOW = "invalid_window"
    TOO_MANY_RECORDS = "too_many_records"


class OrderFlowAuditError(ValueError):
    """Reject ambiguous or unsafe order-flow audit configuration."""

    def __init__(self, code: OrderFlowAuditErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class OrderFlowSequenceAuditor:
    """Audit one bounded stream without inferring undocumented provider guarantees."""

    def audit(
        self,
        *,
        stream: OrderFlowStream,
        window_start: datetime,
        window_end_exclusive: datetime,
        records: Iterable[OrderFlowRecord],
    ) -> OrderFlowSequenceReport:
        """Return ordering, identity, window, and evidenced sequence findings."""

        self._validate_window(window_start, window_end_exclusive)
        input_records = tuple(islice(records, MAX_ORDER_FLOW_RECORDS + 1))
        if len(input_records) > MAX_ORDER_FLOW_RECORDS:
            raise OrderFlowAuditError(
                OrderFlowAuditErrorCode.TOO_MANY_RECORDS,
                f"audit input exceeds {MAX_ORDER_FLOW_RECORDS} records",
            )

        issues: list[OrderFlowQualityIssue] = []
        candidates: dict[tuple[object, ...], list[OrderFlowRecord]] = defaultdict(list)
        candidate_order: list[tuple[object, ...]] = []
        previous_record: OrderFlowRecord | None = None

        for record in input_records:
            if not stream.contains(record):
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.MIXED_STREAM,
                        event_time=record.event_time,
                        record_ids=(record.record_id,),
                        provider_sequence=record.provider_sequence,
                        detail="record belongs to a different order-flow stream",
                    )
                )
                continue
            if not window_start <= record.event_time < window_end_exclusive:
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.OUT_OF_WINDOW,
                        event_time=record.event_time,
                        record_ids=(record.record_id,),
                        provider_sequence=record.provider_sequence,
                        detail="record event_time is outside the expected window",
                    )
                )
                continue
            if previous_record is not None and record.event_time < previous_record.event_time:
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.OUT_OF_ORDER,
                        event_time=record.event_time,
                        record_ids=(previous_record.record_id, record.record_id),
                        provider_sequence=record.provider_sequence,
                        previous_provider_sequence=previous_record.provider_sequence,
                        detail="input sequence regressed in market time",
                    )
                )
            previous_record = record
            natural_key = record.natural_key
            if natural_key not in candidates:
                candidate_order.append(natural_key)
            candidates[natural_key].append(record)

        conflicted_keys: set[tuple[object, ...]] = set()
        for candidate_key, same_identity in candidates.items():
            if len(same_identity) == 1:
                continue
            first = same_identity[0]
            record_ids = tuple(sorted((record.record_id for record in same_identity), key=str))
            if all(record.market_values == first.market_values for record in same_identity[1:]):
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.DUPLICATE,
                        event_time=first.event_time,
                        record_ids=record_ids,
                        provider_sequence=first.provider_sequence,
                        detail="multiple records contain the same canonical market values",
                    )
                )
            else:
                conflicted_keys.add(candidate_key)
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.CONFLICT,
                        event_time=first.event_time,
                        record_ids=record_ids,
                        provider_sequence=first.provider_sequence,
                        detail="records for the same natural key contain conflicting values",
                    )
                )

        usable_records = tuple(
            candidates[natural_key][0]
            for natural_key in candidate_order
            if natural_key not in conflicted_keys
        )
        missing_ranges = self._audit_provider_sequences(
            stream=stream,
            records=usable_records,
            issues=issues,
        )
        sorted_issues = tuple(
            sorted(
                issues,
                key=lambda issue: (
                    issue.event_time or window_start,
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
        return OrderFlowSequenceReport(
            stream=stream,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            input_count=len(input_records),
            accepted_count=len(usable_records),
            sequenced_count=sum(record.provider_sequence is not None for record in usable_records),
            status=status,
            issues=sorted_issues,
            missing_sequence_ranges=missing_ranges,
        )

    @staticmethod
    def _validate_window(window_start: datetime, window_end_exclusive: datetime) -> None:
        for value in (window_start, window_end_exclusive):
            if value.tzinfo is None or value.utcoffset() is None:
                raise OrderFlowAuditError(
                    OrderFlowAuditErrorCode.NAIVE_WINDOW,
                    "audit window timestamps must be timezone-aware",
                )
        if window_end_exclusive <= window_start:
            raise OrderFlowAuditError(
                OrderFlowAuditErrorCode.INVALID_WINDOW,
                "window end must be after window start",
            )

    @staticmethod
    def _audit_provider_sequences(
        *,
        stream: OrderFlowStream,
        records: tuple[OrderFlowRecord, ...],
        issues: list[OrderFlowQualityIssue],
    ) -> tuple[MissingProviderSequenceRange, ...]:
        if stream.sequence_policy is ProviderSequencePolicy.UNSPECIFIED:
            return ()

        missing_ranges: list[MissingProviderSequenceRange] = []
        previous_record: OrderFlowRecord | None = None
        for record in records:
            sequence = record.provider_sequence
            if sequence is None:
                issues.append(
                    OrderFlowQualityIssue(
                        code=OrderFlowQualityCode.MISSING_SEQUENCE,
                        event_time=record.event_time,
                        record_ids=(record.record_id,),
                        detail="record omits the sequence promised by this stream",
                    )
                )
                previous_record = None
                continue

            if previous_record is not None:
                previous_sequence = previous_record.provider_sequence
                if previous_sequence is None:
                    raise AssertionError("previous sequence state must be known")
                if sequence < previous_sequence:
                    issues.append(
                        OrderFlowQualityIssue(
                            code=OrderFlowQualityCode.SEQUENCE_REGRESSION,
                            event_time=record.event_time,
                            record_ids=(previous_record.record_id, record.record_id),
                            provider_sequence=sequence,
                            previous_provider_sequence=previous_sequence,
                            detail="provider sequence regressed",
                        )
                    )
                elif sequence == previous_sequence:
                    issues.append(
                        OrderFlowQualityIssue(
                            code=OrderFlowQualityCode.SEQUENCE_REUSE,
                            event_time=record.event_time,
                            record_ids=(previous_record.record_id, record.record_id),
                            provider_sequence=sequence,
                            previous_provider_sequence=previous_sequence,
                            detail="provider sequence was reused for another natural key",
                        )
                    )
                elif (
                    stream.sequence_policy is ProviderSequencePolicy.CONTIGUOUS
                    and sequence > previous_sequence + 1
                ):
                    start = previous_sequence + 1
                    end = sequence - 1
                    missing_ranges.append(
                        MissingProviderSequenceRange(
                            start_sequence=start,
                            end_sequence_inclusive=end,
                            missing_count=end - start + 1,
                        )
                    )
            previous_record = record
        return tuple(missing_ranges)
