"""Bounded historical candle pagination, pacing, and retries."""

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from math import isfinite

from wealth.application.ingestion import (
    HistoricalCandleIngestionResult,
    HistoricalCandleIngestor,
)
from wealth.application.quality import CandleSequenceAuditor
from wealth.ports.foundation import Sleeper
from wealth.ports.market import (
    CandleStore,
    HistoricalCandleRequest,
    HistoricalCandleSource,
    HistoricalCandleSourceError,
)

DEFAULT_HISTORICAL_PAGE_SIZE = 1_000
MAX_HISTORICAL_CANDLES_PER_RUN = 100_000
MAX_RETRY_ATTEMPTS = 5
MAX_RETRY_DELAY_SECONDS = 60.0
MAX_RETRY_AFTER_SECONDS = 300


class HistoricalCandlePaginationErrorCode(StrEnum):
    """Machine-readable invalid historical range requests."""

    WINDOW_TOO_LARGE = "window_too_large"


class HistoricalCandlePaginationError(ValueError):
    """Reject historical work that exceeds an explicit application bound."""

    def __init__(self, code: HistoricalCandlePaginationErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


@dataclass(frozen=True, slots=True)
class HistoricalCandlePaginationPolicy:
    """Bound page size, total work, and request pacing."""

    page_size_candles: int = DEFAULT_HISTORICAL_PAGE_SIZE
    max_total_candles: int = MAX_HISTORICAL_CANDLES_PER_RUN
    inter_page_delay_seconds: float = 0.25

    def __post_init__(self) -> None:
        if not 1 <= self.page_size_candles <= DEFAULT_HISTORICAL_PAGE_SIZE:
            raise ValueError(
                f"page_size_candles must be between 1 and {DEFAULT_HISTORICAL_PAGE_SIZE}"
            )
        if not (self.page_size_candles <= self.max_total_candles <= MAX_HISTORICAL_CANDLES_PER_RUN):
            raise ValueError(
                "max_total_candles must be at least one page and no more than "
                f"{MAX_HISTORICAL_CANDLES_PER_RUN}"
            )
        if (
            not isfinite(self.inter_page_delay_seconds)
            or self.inter_page_delay_seconds < 0
            or self.inter_page_delay_seconds > MAX_RETRY_DELAY_SECONDS
        ):
            raise ValueError(
                "inter_page_delay_seconds must be finite and between 0 and "
                f"{MAX_RETRY_DELAY_SECONDS}"
            )


class HistoricalCandleRetryStopReason(StrEnum):
    """Explain why a page-level retry loop stopped."""

    NON_RETRYABLE = "non_retryable"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    RETRY_AFTER_EXCEEDS_POLICY = "retry_after_exceeds_policy"


@dataclass(frozen=True, slots=True)
class HistoricalCandleRetryPolicy:
    """Bound retries for source failures that are explicitly transient."""

    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    max_retry_after_seconds: int = 120

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts <= MAX_RETRY_ATTEMPTS:
            raise ValueError(f"max_attempts must be between 1 and {MAX_RETRY_ATTEMPTS}")
        if (
            not isfinite(self.base_delay_seconds)
            or self.base_delay_seconds < 0
            or not isfinite(self.max_delay_seconds)
            or self.max_delay_seconds < self.base_delay_seconds
            or self.max_delay_seconds > MAX_RETRY_DELAY_SECONDS
        ):
            raise ValueError(
                f"retry delays must be finite, ordered, and between 0 and {MAX_RETRY_DELAY_SECONDS}"
            )
        if not 0 <= self.max_retry_after_seconds <= MAX_RETRY_AFTER_SECONDS:
            raise ValueError(
                f"max_retry_after_seconds must be between 0 and {MAX_RETRY_AFTER_SECONDS}"
            )

    def delay_after(
        self,
        *,
        failed_attempt: int,
        error: HistoricalCandleSourceError,
    ) -> float | None:
        """Return the next safe delay or ``None`` when retry must stop."""

        if failed_attempt < 1:
            raise ValueError("failed_attempt must be positive")
        if not error.retryable or failed_attempt >= self.max_attempts:
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
        error: HistoricalCandleSourceError,
    ) -> HistoricalCandleRetryStopReason | None:
        """Return a terminal reason, or ``None`` when another attempt is allowed."""

        if failed_attempt < 1:
            raise ValueError("failed_attempt must be positive")
        if not error.retryable:
            return HistoricalCandleRetryStopReason.NON_RETRYABLE
        if failed_attempt >= self.max_attempts:
            return HistoricalCandleRetryStopReason.ATTEMPTS_EXHAUSTED
        if (
            error.retry_after_seconds is not None
            and error.retry_after_seconds > self.max_retry_after_seconds
        ):
            return HistoricalCandleRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
        return None


@dataclass(frozen=True, slots=True)
class HistoricalCandlePagePlanner:
    """Split one aligned range into exact, contiguous bounded requests."""

    policy: HistoricalCandlePaginationPolicy = field(
        default_factory=HistoricalCandlePaginationPolicy
    )

    def plan(
        self,
        request: HistoricalCandleRequest,
    ) -> tuple[HistoricalCandleRequest, ...]:
        """Return materialized pages for callers that need a stable snapshot."""

        return tuple(self.iter_pages(request))

    def page_count(self, request: HistoricalCandleRequest) -> int:
        """Return the exact page count without allocating page requests."""

        self._validate_request(request)
        return (
            request.expected_count + self.policy.page_size_candles - 1
        ) // self.policy.page_size_candles

    def iter_pages(
        self,
        request: HistoricalCandleRequest,
    ) -> Iterator[HistoricalCandleRequest]:
        """Yield deterministic pages without overlaps, gaps, or eager allocation."""

        self._validate_request(request)

        page_start = request.window_start
        while page_start < request.window_end_exclusive:
            page_end = min(
                page_start + self.policy.page_size_candles * request.timeframe.duration,
                request.window_end_exclusive,
            )
            yield HistoricalCandleRequest(
                instrument=request.instrument,
                provider_symbol=request.provider_symbol,
                instrument_type=request.instrument_type,
                timeframe=request.timeframe,
                window_start=page_start,
                window_end_exclusive=page_end,
            )
            page_start = page_end

    def _validate_request(self, request: HistoricalCandleRequest) -> None:
        if request.expected_count > self.policy.max_total_candles:
            raise HistoricalCandlePaginationError(
                HistoricalCandlePaginationErrorCode.WINDOW_TOO_LARGE,
                f"window exceeds {self.policy.max_total_candles} candles",
            )


@dataclass(frozen=True, slots=True)
class HistoricalCandleSourceFailure:
    """Safe failure evidence for one exhausted or non-retryable page."""

    machine_code: str
    retryable: bool
    retry_after_seconds: int | None
    stop_reason: HistoricalCandleRetryStopReason


@dataclass(frozen=True, slots=True)
class HistoricalCandlePageIngestionResult:
    """Attempts and final outcome for one planned page."""

    request: HistoricalCandleRequest
    attempts: int
    retry_delays_seconds: tuple[float, ...] = ()
    ingestion: HistoricalCandleIngestionResult | None = None
    failure: HistoricalCandleSourceFailure | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("page attempts must be positive")
        if len(self.retry_delays_seconds) != self.attempts - 1:
            raise ValueError("retry delays must describe every attempt after the first")
        if (self.ingestion is None) == (self.failure is None):
            raise ValueError("page result requires exactly one ingestion or failure")

    @property
    def accepted(self) -> bool:
        """Return whether the page passed source, quality, and storage gates."""

        return self.ingestion is not None and self.ingestion.accepted


@dataclass(frozen=True, slots=True)
class HistoricalCandleRangeIngestionResult:
    """Partial or complete progress across a bounded historical range."""

    request: HistoricalCandleRequest
    planned_page_count: int
    pages: tuple[HistoricalCandlePageIngestionResult, ...]

    @property
    def accepted(self) -> bool:
        """Return whether every planned page completed successfully."""

        return len(self.pages) == self.planned_page_count and all(
            page.accepted for page in self.pages
        )

    @property
    def next_window_start(self) -> datetime:
        """Return the first unaccepted page boundary for safe resumption."""

        for page in self.pages:
            if not page.accepted:
                return page.request.window_start
        if len(self.pages) < self.planned_page_count:
            if not self.pages:
                return self.request.window_start
            return self.pages[-1].request.window_end_exclusive
        return self.request.window_end_exclusive


@dataclass(frozen=True, slots=True)
class RetriedHistoricalCandlePageIngestor:
    """Ingest exactly one page with bounded, observable retry behavior."""

    ingestor: HistoricalCandleIngestor
    sleeper: Sleeper
    retry_policy: HistoricalCandleRetryPolicy = field(default_factory=HistoricalCandleRetryPolicy)

    def ingest(
        self,
        request: HistoricalCandleRequest,
    ) -> HistoricalCandlePageIngestionResult:
        """Return one terminal page outcome and every applied retry delay."""

        attempts = 0
        retry_delays: list[float] = []
        while True:
            attempts += 1
            try:
                ingestion = self.ingestor.ingest(request)
            except HistoricalCandleSourceError as error:
                delay = self.retry_policy.delay_after(
                    failed_attempt=attempts,
                    error=error,
                )
                if delay is None:
                    stop_reason = self.retry_policy.stop_reason_after(
                        failed_attempt=attempts,
                        error=error,
                    )
                    if stop_reason is None:
                        raise AssertionError(
                            "retry policy returned no delay or stop reason"
                        ) from error
                    return HistoricalCandlePageIngestionResult(
                        request=request,
                        attempts=attempts,
                        retry_delays_seconds=tuple(retry_delays),
                        failure=HistoricalCandleSourceFailure(
                            machine_code=error.machine_code,
                            retryable=error.retryable,
                            retry_after_seconds=error.retry_after_seconds,
                            stop_reason=stop_reason,
                        ),
                    )
                retry_delays.append(delay)
                self.sleeper.sleep(delay)
                continue
            return HistoricalCandlePageIngestionResult(
                request=request,
                attempts=attempts,
                retry_delays_seconds=tuple(retry_delays),
                ingestion=ingestion,
            )


@dataclass(frozen=True, slots=True)
class PaginatedHistoricalCandleIngestor:
    """Ingest a bounded range page-by-page with explicit retry policy."""

    source: HistoricalCandleSource
    store: CandleStore
    sleeper: Sleeper
    pagination_policy: HistoricalCandlePaginationPolicy = field(
        default_factory=HistoricalCandlePaginationPolicy
    )
    retry_policy: HistoricalCandleRetryPolicy = field(default_factory=HistoricalCandleRetryPolicy)
    auditor: CandleSequenceAuditor = field(default_factory=CandleSequenceAuditor)

    def ingest(self, request: HistoricalCandleRequest) -> HistoricalCandleRangeIngestionResult:
        """Stop on the first failed page and retain an exact resume boundary."""

        planner = HistoricalCandlePagePlanner(self.pagination_policy)
        planned_page_count = planner.page_count(request)
        page_results: list[HistoricalCandlePageIngestionResult] = []
        page_ingestor = RetriedHistoricalCandlePageIngestor(
            ingestor=HistoricalCandleIngestor(
                source=self.source,
                store=self.store,
                auditor=self.auditor,
            ),
            sleeper=self.sleeper,
            retry_policy=self.retry_policy,
        )

        for page_index, page_request in enumerate(planner.iter_pages(request)):
            page_result = page_ingestor.ingest(page_request)
            page_results.append(page_result)
            if not page_result.accepted:
                break
            if page_index < planned_page_count - 1:
                self.sleeper.sleep(self.pagination_policy.inter_page_delay_seconds)

        return HistoricalCandleRangeIngestionResult(
            request=request,
            planned_page_count=planned_page_count,
            pages=tuple(page_results),
        )
