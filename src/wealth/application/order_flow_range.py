"""Bounded public-trade range ingestion with adaptive window splitting."""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite

from wealth.application.order_flow_ingestion import (
    OrderFlowBatchIngestor,
    OrderFlowIngestionResult,
)
from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.ports.foundation import Sleeper
from wealth.ports.order_flow import (
    MAX_ORDER_FLOW_BATCH_RECORDS,
    OrderFlowFetchBatch,
    OrderFlowStore,
    PublicTradeSourceError,
    PublicTradeWindowRequest,
    PublicTradeWindowSource,
)

MILLISECOND = timedelta(milliseconds=1)
DEFAULT_INITIAL_TRADE_WINDOW = timedelta(minutes=30)
DEFAULT_MAX_TRADE_RANGE = timedelta(hours=24)
MAX_TRADE_RANGE = timedelta(days=7)
MAX_TRADE_SOURCE_REQUESTS = 1_024
MAX_TRADE_RETRY_ATTEMPTS = 5
MAX_TRADE_DELAY_SECONDS = 60.0
MAX_TRADE_RETRY_AFTER_SECONDS = 300


class PublicTradeRetryStopReason(StrEnum):
    """Explain why a source retry loop stopped."""

    NON_RETRYABLE = "non_retryable"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    RETRY_AFTER_EXCEEDS_POLICY = "retry_after_exceeds_policy"
    REQUEST_LIMIT_REACHED = "request_limit_reached"


class PublicTradeWindowOutcome(StrEnum):
    """Classify one terminal or adaptive window observation."""

    INGESTED = "ingested"
    SPLIT = "split"
    SOURCE_FAILURE = "source_failure"
    INGESTION_REJECTED = "ingestion_rejected"
    DENSITY_LIMIT = "density_limit"
    RECORD_LIMIT = "record_limit"


class PublicTradeRangeStopReason(StrEnum):
    """Classify the final bounded range outcome."""

    COMPLETED = "completed"
    SOURCE_FAILURE = "source_failure"
    INGESTION_REJECTED = "ingestion_rejected"
    MINIMUM_WINDOW_REACHED = "minimum_window_reached"
    REQUEST_LIMIT_REACHED = "request_limit_reached"
    RECORD_LIMIT_REACHED = "record_limit_reached"


@dataclass(frozen=True, slots=True)
class PublicTradeRangePolicy:
    """Bound initial pages, adaptive splits, work volume, and pacing."""

    initial_window_duration: timedelta = DEFAULT_INITIAL_TRADE_WINDOW
    minimum_window_duration: timedelta = MILLISECOND
    max_range_duration: timedelta = DEFAULT_MAX_TRADE_RANGE
    max_source_requests: int = 256
    max_records_per_run: int = MAX_ORDER_FLOW_BATCH_RECORDS
    inter_request_delay_seconds: float = 0.25

    def __post_init__(self) -> None:
        for name, duration in (
            ("initial_window_duration", self.initial_window_duration),
            ("minimum_window_duration", self.minimum_window_duration),
            ("max_range_duration", self.max_range_duration),
        ):
            if duration < MILLISECOND or not _is_millisecond_aligned(duration):
                raise ValueError(f"{name} must be a positive whole number of milliseconds")
        if self.minimum_window_duration > self.initial_window_duration:
            raise ValueError("minimum_window_duration must not exceed initial_window_duration")
        if self.initial_window_duration > self.max_range_duration:
            raise ValueError("initial_window_duration must not exceed max_range_duration")
        if self.max_range_duration > MAX_TRADE_RANGE:
            raise ValueError(f"max_range_duration must not exceed {MAX_TRADE_RANGE}")
        if not 1 <= self.max_source_requests <= MAX_TRADE_SOURCE_REQUESTS:
            raise ValueError(
                f"max_source_requests must be between 1 and {MAX_TRADE_SOURCE_REQUESTS}"
            )
        if not 1 <= self.max_records_per_run <= MAX_ORDER_FLOW_BATCH_RECORDS:
            raise ValueError(
                f"max_records_per_run must be between 1 and {MAX_ORDER_FLOW_BATCH_RECORDS}"
            )
        if (
            not isfinite(self.inter_request_delay_seconds)
            or self.inter_request_delay_seconds < 0
            or self.inter_request_delay_seconds > MAX_TRADE_DELAY_SECONDS
        ):
            raise ValueError(
                "inter_request_delay_seconds must be finite and between 0 and "
                f"{MAX_TRADE_DELAY_SECONDS}"
            )


@dataclass(frozen=True, slots=True)
class PublicTradeRetryPolicy:
    """Bound retries for failures explicitly classified as transient."""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    max_retry_after_seconds: int = 120

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= MAX_TRADE_RETRY_ATTEMPTS:
            raise ValueError(f"max_attempts must be between 1 and {MAX_TRADE_RETRY_ATTEMPTS}")
        if (
            not isfinite(self.base_delay_seconds)
            or self.base_delay_seconds < 0
            or not isfinite(self.max_delay_seconds)
            or self.max_delay_seconds < self.base_delay_seconds
            or self.max_delay_seconds > MAX_TRADE_DELAY_SECONDS
        ):
            raise ValueError(
                f"retry delays must be finite, ordered, and between 0 and {MAX_TRADE_DELAY_SECONDS}"
            )
        if not 0 <= self.max_retry_after_seconds <= MAX_TRADE_RETRY_AFTER_SECONDS:
            raise ValueError(
                f"max_retry_after_seconds must be between 0 and {MAX_TRADE_RETRY_AFTER_SECONDS}"
            )

    def delay_after(
        self,
        *,
        failed_attempt: int,
        error: PublicTradeSourceError,
    ) -> float | None:
        """Return one bounded delay, or ``None`` when retry must stop."""

        if failed_attempt < 1:
            raise ValueError("failed_attempt must be positive")
        if (
            error.requires_smaller_window
            or not error.retryable
            or failed_attempt >= self.max_attempts
        ):
            return None
        if error.retry_after_seconds is not None:
            if error.retry_after_seconds > self.max_retry_after_seconds:
                return None
            return float(error.retry_after_seconds)
        exponential_delay = self.base_delay_seconds * (2.0 ** (failed_attempt - 1))
        return min(exponential_delay, self.max_delay_seconds)

    def stop_reason_after(
        self,
        *,
        failed_attempt: int,
        error: PublicTradeSourceError,
    ) -> PublicTradeRetryStopReason | None:
        """Return the terminal retry reason, or ``None`` when retry is allowed."""

        if failed_attempt < 1:
            raise ValueError("failed_attempt must be positive")
        if error.requires_smaller_window:
            return None
        if not error.retryable:
            return PublicTradeRetryStopReason.NON_RETRYABLE
        if failed_attempt >= self.max_attempts:
            return PublicTradeRetryStopReason.ATTEMPTS_EXHAUSTED
        if (
            error.retry_after_seconds is not None
            and error.retry_after_seconds > self.max_retry_after_seconds
        ):
            return PublicTradeRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
        return None


@dataclass(frozen=True, slots=True)
class PublicTradeSourceFailureEvidence:
    """Safe provider-neutral failure evidence for one request."""

    machine_code: str
    retryable: bool
    retry_after_seconds: int | None
    requires_smaller_window: bool
    retry_stop_reason: PublicTradeRetryStopReason | None = None

    def __post_init__(self) -> None:
        if not self.machine_code or self.machine_code != self.machine_code.strip():
            raise ValueError("source failure machine_code must be non-empty and canonical")
        if self.retry_after_seconds is not None and self.retry_after_seconds < 0:
            raise ValueError("source failure retry_after_seconds must be non-negative")
        if self.requires_smaller_window and self.retryable:
            raise ValueError("smaller-window evidence must not be retryable unchanged")

    @classmethod
    def from_error(
        cls,
        error: PublicTradeSourceError,
        *,
        retry_stop_reason: PublicTradeRetryStopReason | None = None,
    ) -> "PublicTradeSourceFailureEvidence":
        """Copy only safe structured error attributes."""

        return cls(
            machine_code=error.machine_code,
            retryable=error.retryable,
            retry_after_seconds=error.retry_after_seconds,
            requires_smaller_window=error.requires_smaller_window,
            retry_stop_reason=retry_stop_reason,
        )


@dataclass(frozen=True, slots=True)
class PublicTradeWindowTrace:
    """Record every source attempt and adaptive decision for one window."""

    request: PublicTradeWindowRequest
    outcome: PublicTradeWindowOutcome
    attempts: int
    retry_delays_seconds: tuple[float, ...] = ()
    ingestion: OrderFlowIngestionResult | None = None
    source_failure: PublicTradeSourceFailureEvidence | None = None
    split_children: tuple[PublicTradeWindowRequest, PublicTradeWindowRequest] | None = None
    fetched_batch: OrderFlowFetchBatch | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("window attempts must be positive")
        if len(self.retry_delays_seconds) != self.attempts - 1:
            raise ValueError("retry delays must describe every attempt after the first")
        if any(not isfinite(delay) or delay < 0 for delay in self.retry_delays_seconds):
            raise ValueError("retry delays must be finite and non-negative")
        if self.outcome in {
            PublicTradeWindowOutcome.INGESTED,
            PublicTradeWindowOutcome.INGESTION_REJECTED,
        }:
            if (
                self.ingestion is None
                or self.source_failure is not None
                or self.split_children is not None
                or self.fetched_batch is not None
            ):
                raise ValueError("ingestion traces require only an ingestion result")
            if (self.outcome is PublicTradeWindowOutcome.INGESTED) != self.ingestion.accepted:
                raise ValueError("ingestion trace outcome must match admission status")
        elif self.outcome is PublicTradeWindowOutcome.SPLIT:
            if (
                self.source_failure is None
                or not self.source_failure.requires_smaller_window
                or self.split_children is None
                or self.ingestion is not None
                or self.fetched_batch is not None
            ):
                raise ValueError("split traces require one density failure and two children")
            left, right = self.split_children
            if (
                left.window_start != self.request.window_start
                or left.window_end_exclusive != right.window_start
                or right.window_end_exclusive != self.request.window_end_exclusive
                or not _same_requested_stream(left, self.request)
                or not _same_requested_stream(right, self.request)
            ):
                raise ValueError("split children must exactly partition the parent window")
        elif self.outcome in {
            PublicTradeWindowOutcome.SOURCE_FAILURE,
            PublicTradeWindowOutcome.DENSITY_LIMIT,
        }:
            if (
                self.source_failure is None
                or self.ingestion is not None
                or self.split_children is not None
                or self.fetched_batch is not None
            ):
                raise ValueError("source failure traces require only failure evidence")
            if self.outcome is PublicTradeWindowOutcome.DENSITY_LIMIT and (
                not self.source_failure.requires_smaller_window
                or self.source_failure.retry_stop_reason is not None
            ):
                raise ValueError("density-limit traces require smaller-window evidence")
            if self.outcome is PublicTradeWindowOutcome.SOURCE_FAILURE and (
                self.source_failure.retry_stop_reason is None
                or self.source_failure.requires_smaller_window
            ):
                raise ValueError("terminal source failures require a retry stop reason")
        elif self.outcome is PublicTradeWindowOutcome.RECORD_LIMIT:
            if (
                self.fetched_batch is None
                or self.ingestion is not None
                or self.source_failure is not None
                or self.split_children is not None
            ):
                raise ValueError("record-limit traces require the unadmitted fetched batch")

    @property
    def ingested_record_count(self) -> int:
        """Return admitted records for accepted windows only."""

        if self.outcome is not PublicTradeWindowOutcome.INGESTED or self.ingestion is None:
            return 0
        return len(self.ingestion.batch.records)


@dataclass(frozen=True, slots=True)
class PublicTradeRangeIngestionResult:
    """Complete progress and safe resume evidence for one bounded range."""

    request: PublicTradeWindowRequest
    traces: tuple[PublicTradeWindowTrace, ...]
    stop_reason: PublicTradeRangeStopReason
    pending_window: PublicTradeWindowRequest | None = None

    def __post_init__(self) -> None:
        cursor = self.request.window_start
        for trace in self.traces:
            if (
                not _same_requested_stream(trace.request, self.request)
                or trace.request.window_start < self.request.window_start
                or trace.request.window_end_exclusive > self.request.window_end_exclusive
            ):
                raise ValueError("every trace window must remain inside the requested stream range")
            if trace.outcome is PublicTradeWindowOutcome.INGESTED:
                if trace.request.window_start != cursor:
                    raise ValueError("ingested windows must form one exact chronological prefix")
                cursor = trace.request.window_end_exclusive
        if (self.stop_reason is PublicTradeRangeStopReason.COMPLETED) != (
            self.pending_window is None
        ):
            raise ValueError("only a completed range may omit the pending window")
        next_window_start = cursor
        if self.stop_reason is PublicTradeRangeStopReason.COMPLETED:
            if next_window_start != self.request.window_end_exclusive:
                raise ValueError("completed range traces must cover the full requested window")
        elif (
            self.pending_window is None
            or self.pending_window.window_start != next_window_start
            or not _same_requested_stream(self.pending_window, self.request)
            or self.pending_window.window_end_exclusive > self.request.window_end_exclusive
        ):
            raise ValueError("pending window must start at the exact safe resume boundary")

    @property
    def accepted(self) -> bool:
        """Return whether the entire requested range was admitted."""

        return self.stop_reason is PublicTradeRangeStopReason.COMPLETED

    @property
    def source_request_count(self) -> int:
        """Return every network attempt, including retries and split probes."""

        return sum(trace.attempts for trace in self.traces)

    @property
    def ingested_record_count(self) -> int:
        """Return canonical records admitted during this run."""

        return sum(trace.ingested_record_count for trace in self.traces)

    @property
    def next_window_start(self) -> datetime:
        """Return the first event-time boundary not durably admitted."""

        cursor = self.request.window_start
        for trace in self.traces:
            if (
                trace.outcome is PublicTradeWindowOutcome.INGESTED
                and trace.request.window_start == cursor
            ):
                cursor = trace.request.window_end_exclusive
        return cursor


@dataclass(frozen=True, slots=True)
class AdaptivePublicTradeRangeIngestor:
    """Fetch, split, retry, quality-gate, and persist a bounded trade range."""

    source: PublicTradeWindowSource
    store: OrderFlowStore
    sleeper: Sleeper
    range_policy: PublicTradeRangePolicy = field(default_factory=PublicTradeRangePolicy)
    retry_policy: PublicTradeRetryPolicy = field(default_factory=PublicTradeRetryPolicy)
    auditor: OrderFlowSequenceAuditor = field(default_factory=OrderFlowSequenceAuditor)

    def ingest(
        self,
        request: PublicTradeWindowRequest,
    ) -> PublicTradeRangeIngestionResult:
        """Process chronological windows and stop at the first unsafe boundary."""

        if request.duration > self.range_policy.max_range_duration:
            raise ValueError(
                f"trade range exceeds configured maximum {self.range_policy.max_range_duration}"
            )

        pending = deque(self._initial_windows(request))
        traces: list[PublicTradeWindowTrace] = []
        source_request_count = 0
        ingested_record_count = 0
        batch_ingestor = OrderFlowBatchIngestor(store=self.store, auditor=self.auditor)

        while pending:
            window = pending.popleft()
            if source_request_count >= self.range_policy.max_source_requests:
                return self._result(
                    request=request,
                    traces=traces,
                    stop_reason=PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED,
                    pending_window=window,
                )

            attempts = 0
            retry_delays: list[float] = []
            while True:
                attempts += 1
                source_request_count += 1
                try:
                    batch = self.source.fetch(window)
                except PublicTradeSourceError as error:
                    if error.requires_smaller_window:
                        children = self._split(window)
                        failure = PublicTradeSourceFailureEvidence.from_error(error)
                        if children is None:
                            traces.append(
                                PublicTradeWindowTrace(
                                    request=window,
                                    outcome=PublicTradeWindowOutcome.DENSITY_LIMIT,
                                    attempts=attempts,
                                    retry_delays_seconds=tuple(retry_delays),
                                    source_failure=failure,
                                )
                            )
                            return self._result(
                                request=request,
                                traces=traces,
                                stop_reason=(PublicTradeRangeStopReason.MINIMUM_WINDOW_REACHED),
                                pending_window=window,
                            )
                        traces.append(
                            PublicTradeWindowTrace(
                                request=window,
                                outcome=PublicTradeWindowOutcome.SPLIT,
                                attempts=attempts,
                                retry_delays_seconds=tuple(retry_delays),
                                source_failure=failure,
                                split_children=children,
                            )
                        )
                        pending.appendleft(children[1])
                        pending.appendleft(children[0])
                        self._pace_if_more_requests_are_allowed(
                            pending=pending,
                            source_request_count=source_request_count,
                        )
                        break

                    delay = self.retry_policy.delay_after(
                        failed_attempt=attempts,
                        error=error,
                    )
                    if (
                        delay is not None
                        and source_request_count < self.range_policy.max_source_requests
                    ):
                        retry_delays.append(delay)
                        self.sleeper.sleep(delay)
                        continue
                    stop_reason = (
                        PublicTradeRetryStopReason.REQUEST_LIMIT_REACHED
                        if delay is not None
                        else self.retry_policy.stop_reason_after(
                            failed_attempt=attempts,
                            error=error,
                        )
                    )
                    if stop_reason is None:
                        raise AssertionError(
                            "retry policy returned no delay or stop reason"
                        ) from error
                    traces.append(
                        PublicTradeWindowTrace(
                            request=window,
                            outcome=PublicTradeWindowOutcome.SOURCE_FAILURE,
                            attempts=attempts,
                            retry_delays_seconds=tuple(retry_delays),
                            source_failure=PublicTradeSourceFailureEvidence.from_error(
                                error,
                                retry_stop_reason=stop_reason,
                            ),
                        )
                    )
                    return self._result(
                        request=request,
                        traces=traces,
                        stop_reason=(
                            PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED
                            if stop_reason is PublicTradeRetryStopReason.REQUEST_LIMIT_REACHED
                            else PublicTradeRangeStopReason.SOURCE_FAILURE
                        ),
                        pending_window=window,
                    )

                if (
                    ingested_record_count + len(batch.records)
                    > self.range_policy.max_records_per_run
                ):
                    traces.append(
                        PublicTradeWindowTrace(
                            request=window,
                            outcome=PublicTradeWindowOutcome.RECORD_LIMIT,
                            attempts=attempts,
                            retry_delays_seconds=tuple(retry_delays),
                            fetched_batch=batch,
                        )
                    )
                    return self._result(
                        request=request,
                        traces=traces,
                        stop_reason=PublicTradeRangeStopReason.RECORD_LIMIT_REACHED,
                        pending_window=window,
                    )

                ingestion = batch_ingestor.ingest(
                    batch,
                    window_start=window.window_start,
                    window_end_exclusive=window.window_end_exclusive,
                )
                outcome = (
                    PublicTradeWindowOutcome.INGESTED
                    if ingestion.accepted
                    else PublicTradeWindowOutcome.INGESTION_REJECTED
                )
                traces.append(
                    PublicTradeWindowTrace(
                        request=window,
                        outcome=outcome,
                        attempts=attempts,
                        retry_delays_seconds=tuple(retry_delays),
                        ingestion=ingestion,
                    )
                )
                if not ingestion.accepted:
                    return self._result(
                        request=request,
                        traces=traces,
                        stop_reason=PublicTradeRangeStopReason.INGESTION_REJECTED,
                        pending_window=window,
                    )
                ingested_record_count += len(batch.records)
                self._pace_if_more_requests_are_allowed(
                    pending=pending,
                    source_request_count=source_request_count,
                )
                break

        return self._result(
            request=request,
            traces=traces,
            stop_reason=PublicTradeRangeStopReason.COMPLETED,
            pending_window=None,
        )

    def _initial_windows(
        self,
        request: PublicTradeWindowRequest,
    ) -> tuple[PublicTradeWindowRequest, ...]:
        windows: list[PublicTradeWindowRequest] = []
        window_start = request.window_start
        while window_start < request.window_end_exclusive:
            window_end = min(
                window_start + self.range_policy.initial_window_duration,
                request.window_end_exclusive,
            )
            windows.append(_window_like(request, window_start, window_end))
            window_start = window_end
        return tuple(windows)

    def _split(
        self,
        request: PublicTradeWindowRequest,
    ) -> tuple[PublicTradeWindowRequest, PublicTradeWindowRequest] | None:
        duration_ms = request.duration // MILLISECOND
        minimum_ms = self.range_policy.minimum_window_duration // MILLISECOND
        if duration_ms < minimum_ms * 2:
            return None
        left_ms = max(minimum_ms, duration_ms // 2)
        if duration_ms - left_ms < minimum_ms:
            return None
        split_at = request.window_start + left_ms * MILLISECOND
        return (
            _window_like(request, request.window_start, split_at),
            _window_like(request, split_at, request.window_end_exclusive),
        )

    def _pace_if_more_requests_are_allowed(
        self,
        *,
        pending: deque[PublicTradeWindowRequest],
        source_request_count: int,
    ) -> None:
        if pending and source_request_count < self.range_policy.max_source_requests:
            self.sleeper.sleep(self.range_policy.inter_request_delay_seconds)

    @staticmethod
    def _result(
        *,
        request: PublicTradeWindowRequest,
        traces: list[PublicTradeWindowTrace],
        stop_reason: PublicTradeRangeStopReason,
        pending_window: PublicTradeWindowRequest | None,
    ) -> PublicTradeRangeIngestionResult:
        return PublicTradeRangeIngestionResult(
            request=request,
            traces=tuple(traces),
            stop_reason=stop_reason,
            pending_window=pending_window,
        )


def _window_like(
    request: PublicTradeWindowRequest,
    window_start: datetime,
    window_end_exclusive: datetime,
) -> PublicTradeWindowRequest:
    return PublicTradeWindowRequest(
        instrument=request.instrument,
        provider_symbol=request.provider_symbol,
        instrument_type=request.instrument_type,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )


def _is_millisecond_aligned(duration: timedelta) -> bool:
    return duration % MILLISECOND == timedelta(0)


def _same_requested_stream(
    left: PublicTradeWindowRequest,
    right: PublicTradeWindowRequest,
) -> bool:
    return (
        left.instrument == right.instrument
        and left.provider_symbol == right.provider_symbol
        and left.instrument_type is right.instrument_type
    )
