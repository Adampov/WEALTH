"""Supervised, bounded polling cycles for restart-safe candle collection."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from math import isfinite
from uuid import UUID

from wealth.application.collection import (
    CollectionRunResult,
    CollectionRunStatus,
    RecoverableHistoricalCandleCollector,
)
from wealth.application.pagination import (
    MAX_HISTORICAL_CANDLES_PER_RUN,
    HistoricalCandleRetryStopReason,
)
from wealth.domain.collection import HistoricalCollectionJob
from wealth.domain.continuous_collection import (
    UTC_EPOCH,
    ContinuousCollectionCheckpoint,
    ContinuousCollectionRequest,
    ContinuousCollectionStatus,
)
from wealth.domain.market import CandleTimeframe
from wealth.ports.continuous_collection import (
    ContinuousCollectionCheckpointStore,
    ContinuousCollectionWriteStatus,
)
from wealth.ports.foundation import Clock, IdGenerator, Sleeper
from wealth.ports.market import HistoricalCandleRequest

MAX_CONTINUOUS_CYCLES_PER_RUN = 100
MAX_CONTINUOUS_CONSECUTIVE_FAILURES = 20
MAX_CONTINUOUS_DELAY_SECONDS = 300.0
RECONNECTABLE_FAILURE_CODES = frozenset(
    {
        "provider_unavailable",
        "transport_failure",
    }
)


class ContinuousCollectionCycleStatus(StrEnum):
    """Outcome of one bounded supervised polling cycle."""

    ADVANCED = "advanced"
    CAUGHT_UP = "caught_up"
    WAITING = "waiting"
    RETRY_SCHEDULED = "retry_scheduled"
    PAUSED = "paused"
    ALREADY_RUNNING = "already_running"
    CHECKPOINT_CONFLICT = "checkpoint_conflict"
    LOST_LEASE = "lost_lease"


class ContinuousCollectionNotFoundError(LookupError):
    """Fail clearly when a requested continuous stream does not exist."""


class ContinuousCollectionClockRegressionError(RuntimeError):
    """Fail closed when wall time precedes durable operational state."""


@dataclass(frozen=True, slots=True)
class ContinuousCollectionPolicy:
    """Bound polling work, provider settlement, reconnects, and idle waits."""

    max_candles_per_cycle: int = 1_000
    settlement_delay_seconds: float = 5.0
    idle_poll_seconds: float = 5.0
    reconnect_base_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 60.0
    max_consecutive_failures: int = 5

    def __post_init__(self) -> None:
        if not 1 <= self.max_candles_per_cycle <= MAX_HISTORICAL_CANDLES_PER_RUN:
            raise ValueError(
                f"max_candles_per_cycle must be between 1 and {MAX_HISTORICAL_CANDLES_PER_RUN}"
            )
        delays = (
            self.settlement_delay_seconds,
            self.idle_poll_seconds,
            self.reconnect_base_delay_seconds,
            self.reconnect_max_delay_seconds,
        )
        if any(not isfinite(delay) or delay < 0 for delay in delays):
            raise ValueError("continuous collection delays must be finite and non-negative")
        if self.settlement_delay_seconds > MAX_CONTINUOUS_DELAY_SECONDS:
            raise ValueError("settlement delay exceeds the continuous collection safety bound")
        if not 0 < self.idle_poll_seconds <= MAX_CONTINUOUS_DELAY_SECONDS:
            raise ValueError("idle polling delay must be positive and bounded")
        if not (
            0
            < self.reconnect_base_delay_seconds
            <= self.reconnect_max_delay_seconds
            <= MAX_CONTINUOUS_DELAY_SECONDS
        ):
            raise ValueError("reconnect delays must be positive, ordered, and bounded")
        if not 1 <= self.max_consecutive_failures <= MAX_CONTINUOUS_CONSECUTIVE_FAILURES:
            raise ValueError(
                "max_consecutive_failures must be between 1 and "
                f"{MAX_CONTINUOUS_CONSECUTIVE_FAILURES}"
            )

    def latest_eligible_end(
        self,
        *,
        now: datetime,
        timeframe: CandleTimeframe,
    ) -> datetime:
        """Return the latest UTC boundary whose candle has settled."""

        _require_aware_time(now)
        safe_time = now.astimezone(UTC) - timedelta(seconds=self.settlement_delay_seconds)
        if safe_time < UTC_EPOCH:
            raise ValueError("continuous collection time cannot precede the UTC epoch")
        complete_intervals = (safe_time - UTC_EPOCH) // timeframe.duration
        return UTC_EPOCH + complete_intervals * timeframe.duration

    def reconnect_delay(self, consecutive_failures: int) -> float:
        """Return bounded deterministic exponential reconnect delay."""

        if not 1 <= consecutive_failures <= self.max_consecutive_failures:
            raise ValueError(
                "consecutive_failures must be positive and within the configured limit"
            )
        delay = self.reconnect_base_delay_seconds * (2.0 ** (consecutive_failures - 1))
        return min(delay, self.reconnect_max_delay_seconds)


@dataclass(frozen=True, slots=True)
class ContinuousCollectionCycleResult:
    """Expose one cycle's durable state, progress, and next safe wait."""

    status: ContinuousCollectionCycleStatus
    checkpoint: ContinuousCollectionCheckpoint
    candles_advanced: int = 0
    wait_seconds: float = 0.0
    bounded_run: CollectionRunResult | None = None

    def __post_init__(self) -> None:
        if self.candles_advanced < 0:
            raise ValueError("candles_advanced cannot be negative")
        if (
            not isfinite(self.wait_seconds)
            or self.wait_seconds < 0
            or self.wait_seconds > MAX_CONTINUOUS_DELAY_SECONDS
        ):
            raise ValueError("continuous cycle wait must be finite, non-negative, and bounded")
        if self.status is ContinuousCollectionCycleStatus.ADVANCED:
            if self.candles_advanced < 1 or self.bounded_run is None:
                raise ValueError("advanced cycle requires durable bounded-run evidence")
        elif self.candles_advanced:
            raise ValueError("only an advanced cycle can report candle progress")


@dataclass(frozen=True, slots=True)
class ContinuousCollectionRunResult:
    """Bound one invocation while retaining every polling-cycle outcome."""

    checkpoint: ContinuousCollectionCheckpoint
    cycles: tuple[ContinuousCollectionCycleResult, ...]


@dataclass(frozen=True, slots=True)
class SupervisedContinuousCandleCollector:
    """Poll closed candles through restart-safe bounded historical jobs."""

    bounded_collector: RecoverableHistoricalCandleCollector
    checkpoint_store: ContinuousCollectionCheckpointStore
    clock: Clock
    id_generator: IdGenerator
    sleeper: Sleeper
    policy: ContinuousCollectionPolicy = field(default_factory=ContinuousCollectionPolicy)

    def __post_init__(self) -> None:
        if (
            self.policy.max_candles_per_cycle
            > self.bounded_collector.pagination_policy.max_total_candles
        ):
            raise ValueError(
                "continuous cycle size cannot exceed the bounded collector's total-work limit"
            )

    def create(
        self,
        request: ContinuousCollectionRequest,
    ) -> ContinuousCollectionCheckpoint:
        """Create one pristine durable stream cursor."""

        if (
            request.source != self.bounded_collector.source_name
            or request.venue != self.bounded_collector.venue
        ):
            raise ValueError("continuous stream identity must match its bounded collector")
        now = self.clock.now()
        checkpoint = ContinuousCollectionCheckpoint(
            collection_id=self.id_generator.new(),
            source=request.source,
            venue=request.venue,
            instrument=request.instrument,
            provider_symbol=request.provider_symbol,
            instrument_type=request.instrument_type,
            timeframe=request.timeframe,
            window_start=request.window_start,
            next_window_start=request.window_start,
            status=ContinuousCollectionStatus.ACTIVE,
            created_at=now,
            updated_at=now,
            version=1,
        )
        write = self.checkpoint_store.create(checkpoint)
        if write.status not in {
            ContinuousCollectionWriteStatus.INSERTED,
            ContinuousCollectionWriteStatus.DUPLICATE,
        }:
            raise RuntimeError("continuous collection identifier conflicted with durable state")
        return checkpoint

    def checkpoint(self, collection_id: UUID) -> ContinuousCollectionCheckpoint:
        """Return the current validated cursor for service orchestration."""

        return self._get(collection_id)

    def run(
        self,
        collection_id: UUID,
        *,
        cycle_limit: int,
    ) -> ContinuousCollectionRunResult:
        """Run a bounded number of cycles and stop on operator or concurrency gates."""

        if not 1 <= cycle_limit <= MAX_CONTINUOUS_CYCLES_PER_RUN:
            raise ValueError(f"cycle_limit must be between 1 and {MAX_CONTINUOUS_CYCLES_PER_RUN}")
        cycles: list[ContinuousCollectionCycleResult] = []
        stop_statuses = {
            ContinuousCollectionCycleStatus.PAUSED,
            ContinuousCollectionCycleStatus.ALREADY_RUNNING,
            ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT,
            ContinuousCollectionCycleStatus.LOST_LEASE,
        }
        for cycle_index in range(cycle_limit):
            cycle = self.run_cycle(collection_id)
            cycles.append(cycle)
            if cycle.status in stop_statuses:
                break
            if cycle_index < cycle_limit - 1 and cycle.wait_seconds:
                self.sleeper.sleep(cycle.wait_seconds)
        return ContinuousCollectionRunResult(
            checkpoint=cycles[-1].checkpoint,
            cycles=tuple(cycles),
        )

    def run_cycle(self, collection_id: UUID) -> ContinuousCollectionCycleResult:
        """Attempt one exact catch-up window or report the next safe action."""

        checkpoint = self._get(collection_id)
        if checkpoint.status is ContinuousCollectionStatus.PAUSED:
            return ContinuousCollectionCycleResult(
                status=ContinuousCollectionCycleStatus.PAUSED,
                checkpoint=checkpoint,
            )
        now = self.clock.now()
        _require_aware_time(now)
        if now < checkpoint.updated_at:
            raise ContinuousCollectionClockRegressionError(
                "current time precedes the durable continuous checkpoint"
            )
        if checkpoint.next_retry_at is not None and now < checkpoint.next_retry_at:
            return ContinuousCollectionCycleResult(
                status=ContinuousCollectionCycleStatus.WAITING,
                checkpoint=checkpoint,
                wait_seconds=self._bounded_wait(checkpoint.next_retry_at - now),
            )

        if checkpoint.active_job_id is None:
            eligible_end = self.policy.latest_eligible_end(
                now=now,
                timeframe=checkpoint.timeframe,
            )
            if eligible_end <= checkpoint.next_window_start:
                next_ready_at = (
                    checkpoint.next_window_start
                    + checkpoint.timeframe.duration
                    + timedelta(seconds=self.policy.settlement_delay_seconds)
                )
                return ContinuousCollectionCycleResult(
                    status=ContinuousCollectionCycleStatus.CAUGHT_UP,
                    checkpoint=checkpoint,
                    wait_seconds=min(
                        self.policy.idle_poll_seconds,
                        self._bounded_wait(next_ready_at - now),
                    ),
                )
            target_end = min(
                eligible_end,
                checkpoint.next_window_start
                + self.policy.max_candles_per_cycle * checkpoint.timeframe.duration,
            )
            attached = self._copy(
                checkpoint,
                updated_at=now,
                version=checkpoint.version + 1,
                active_job_id=self.id_generator.new(),
                active_window_end_exclusive=target_end,
            )
            write = self.checkpoint_store.transition(
                attached,
                expected_version=checkpoint.version,
            )
            if write.status is not ContinuousCollectionWriteStatus.UPDATED:
                return ContinuousCollectionCycleResult(
                    status=ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT,
                    checkpoint=self._get(collection_id),
                )
            checkpoint = attached

        request = self._active_request(checkpoint)
        active_job_id = checkpoint.active_job_id
        if active_job_id is None:
            raise AssertionError("active continuous request requires a durable job identity")
        job = self.bounded_collector.checkpoint_store.get(active_job_id)
        if job is None:
            if checkpoint.consecutive_failures:
                raise RuntimeError("failed continuous checkpoint references a missing bounded job")
            job = self.bounded_collector.create_job(
                request,
                job_id=active_job_id,
                created_at=checkpoint.updated_at,
            )
        self._validate_active_job(checkpoint, job)
        bounded_run = self.bounded_collector.run(active_job_id)
        if bounded_run.status is CollectionRunStatus.COMPLETED:
            return self._record_success(checkpoint, bounded_run)
        if bounded_run.status is CollectionRunStatus.FAILED:
            return self._record_failure(checkpoint, bounded_run)
        status = {
            CollectionRunStatus.ALREADY_RUNNING: ContinuousCollectionCycleStatus.ALREADY_RUNNING,
            CollectionRunStatus.CHECKPOINT_CONFLICT: (
                ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT
            ),
            CollectionRunStatus.LOST_LEASE: ContinuousCollectionCycleStatus.LOST_LEASE,
        }.get(bounded_run.status)
        if status is None:
            raise AssertionError("continuous collector received an unsupported bounded-run status")
        return ContinuousCollectionCycleResult(
            status=status,
            checkpoint=checkpoint,
            bounded_run=bounded_run,
        )

    def pause(
        self,
        collection_id: UUID,
        *,
        reason: str = "operator_requested",
    ) -> ContinuousCollectionCheckpoint:
        """Pause future cycles; a racing worker will lose its cursor compare-and-swap."""

        checkpoint = self._get(collection_id)
        if checkpoint.status is ContinuousCollectionStatus.PAUSED:
            return checkpoint
        paused = self._copy(
            checkpoint,
            status=ContinuousCollectionStatus.PAUSED,
            updated_at=self.clock.now(),
            version=checkpoint.version + 1,
            next_retry_at=None,
            pause_reason=reason,
        )
        return self._write_or_reload(checkpoint, paused)

    def resume(self, collection_id: UUID) -> ContinuousCollectionCheckpoint:
        """Resume an operator-reviewed stream without discarding its active job."""

        checkpoint = self._get(collection_id)
        if checkpoint.status is ContinuousCollectionStatus.ACTIVE:
            return checkpoint
        resumed = self._copy(
            checkpoint,
            status=ContinuousCollectionStatus.ACTIVE,
            updated_at=self.clock.now(),
            version=checkpoint.version + 1,
            consecutive_failures=0,
            next_retry_at=None,
            last_failure_code=None,
            last_stop_reason=None,
            pause_reason=None,
        )
        return self._write_or_reload(checkpoint, resumed)

    def _record_success(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        bounded_run: CollectionRunResult,
    ) -> ContinuousCollectionCycleResult:
        active_end = checkpoint.active_window_end_exclusive
        if active_end is None or bounded_run.checkpoint.window_end_exclusive != active_end:
            raise RuntimeError("completed bounded job does not match its continuous active window")
        now = self.clock.now()
        candles_advanced = (
            active_end - checkpoint.next_window_start
        ) // checkpoint.timeframe.duration
        advanced = self._copy(
            checkpoint,
            next_window_start=active_end,
            status=ContinuousCollectionStatus.ACTIVE,
            updated_at=now,
            version=checkpoint.version + 1,
            active_job_id=None,
            active_window_end_exclusive=None,
            cycles_completed=checkpoint.cycles_completed + 1,
            candles_completed=checkpoint.candles_completed + candles_advanced,
            consecutive_failures=0,
            next_retry_at=None,
            last_failure_code=None,
            last_stop_reason=None,
            pause_reason=None,
        )
        write = self.checkpoint_store.transition(
            advanced,
            expected_version=checkpoint.version,
        )
        if write.status is not ContinuousCollectionWriteStatus.UPDATED:
            return ContinuousCollectionCycleResult(
                status=ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT,
                checkpoint=self._get(checkpoint.collection_id),
                bounded_run=bounded_run,
            )
        return ContinuousCollectionCycleResult(
            status=ContinuousCollectionCycleStatus.ADVANCED,
            checkpoint=advanced,
            candles_advanced=candles_advanced,
            bounded_run=bounded_run,
        )

    def _record_failure(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        bounded_run: CollectionRunResult,
    ) -> ContinuousCollectionCycleResult:
        failed_job = bounded_run.checkpoint
        failure_code = failed_job.last_failure_code
        stop_reason = failed_job.last_stop_reason
        if failure_code is None or stop_reason is None:
            raise RuntimeError("failed bounded job lacks reconnect evidence")
        now = self.clock.now()
        failure_count = checkpoint.consecutive_failures + 1
        retryable = (
            failure_code in RECONNECTABLE_FAILURE_CODES
            and stop_reason == HistoricalCandleRetryStopReason.ATTEMPTS_EXHAUSTED.value
        )
        pause_reason: str | None = None
        next_retry_at: datetime | None = None
        status = ContinuousCollectionStatus.ACTIVE
        wait_seconds = 0.0
        if not retryable:
            status = ContinuousCollectionStatus.PAUSED
            pause_reason = "non_reconnectable_failure"
        elif failure_count >= self.policy.max_consecutive_failures:
            status = ContinuousCollectionStatus.PAUSED
            pause_reason = "failure_limit"
        else:
            wait_seconds = self.policy.reconnect_delay(failure_count)
            next_retry_at = now + timedelta(seconds=wait_seconds)

        failed = self._copy(
            checkpoint,
            status=status,
            updated_at=now,
            version=checkpoint.version + 1,
            consecutive_failures=failure_count,
            next_retry_at=next_retry_at,
            last_failure_code=failure_code,
            last_stop_reason=stop_reason,
            pause_reason=pause_reason,
        )
        write = self.checkpoint_store.transition(
            failed,
            expected_version=checkpoint.version,
        )
        if write.status is not ContinuousCollectionWriteStatus.UPDATED:
            return ContinuousCollectionCycleResult(
                status=ContinuousCollectionCycleStatus.CHECKPOINT_CONFLICT,
                checkpoint=self._get(checkpoint.collection_id),
                bounded_run=bounded_run,
            )
        return ContinuousCollectionCycleResult(
            status=(
                ContinuousCollectionCycleStatus.RETRY_SCHEDULED
                if status is ContinuousCollectionStatus.ACTIVE
                else ContinuousCollectionCycleStatus.PAUSED
            ),
            checkpoint=failed,
            wait_seconds=wait_seconds,
            bounded_run=bounded_run,
        )

    def _write_or_reload(
        self,
        previous: ContinuousCollectionCheckpoint,
        current: ContinuousCollectionCheckpoint,
    ) -> ContinuousCollectionCheckpoint:
        write = self.checkpoint_store.transition(
            current,
            expected_version=previous.version,
        )
        if write.status is ContinuousCollectionWriteStatus.UPDATED:
            return current
        return self._get(previous.collection_id)

    def _get(self, collection_id: UUID) -> ContinuousCollectionCheckpoint:
        checkpoint = self.checkpoint_store.get(collection_id)
        if checkpoint is None:
            raise ContinuousCollectionNotFoundError(str(collection_id))
        return checkpoint

    @staticmethod
    def _copy(
        checkpoint: ContinuousCollectionCheckpoint,
        **updates: object,
    ) -> ContinuousCollectionCheckpoint:
        values = checkpoint.model_dump()
        values.update(updates)
        return ContinuousCollectionCheckpoint.model_validate(values)

    @staticmethod
    def _active_request(
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> HistoricalCandleRequest:
        active_end = checkpoint.active_window_end_exclusive
        if active_end is None:
            raise AssertionError("continuous checkpoint has no active window")
        return HistoricalCandleRequest(
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            timeframe=checkpoint.timeframe,
            window_start=checkpoint.next_window_start,
            window_end_exclusive=active_end,
        )

    @staticmethod
    def _validate_active_job(
        checkpoint: ContinuousCollectionCheckpoint,
        job: HistoricalCollectionJob,
    ) -> None:
        expected = (
            checkpoint.active_job_id,
            checkpoint.source,
            checkpoint.venue,
            checkpoint.instrument,
            checkpoint.provider_symbol,
            checkpoint.instrument_type,
            checkpoint.timeframe,
            checkpoint.next_window_start,
            checkpoint.active_window_end_exclusive,
        )
        actual = (
            job.job_id,
            job.source,
            job.venue,
            job.instrument,
            job.provider_symbol,
            job.instrument_type,
            job.timeframe,
            job.window_start,
            job.window_end_exclusive,
        )
        if actual != expected:
            raise RuntimeError("active bounded job does not match its continuous stream")

    def _bounded_wait(self, duration: timedelta) -> float:
        seconds = max(0.0, duration.total_seconds())
        return min(seconds, MAX_CONTINUOUS_DELAY_SECONDS)


def _require_aware_time(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("continuous collection clock must return timezone-aware timestamps")
