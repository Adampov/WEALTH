"""Recoverable orchestration for bounded historical candle collection."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID

from wealth.application.ingestion import HistoricalCandleIngestor
from wealth.application.pagination import (
    HistoricalCandlePageIngestionResult,
    HistoricalCandlePagePlanner,
    HistoricalCandlePaginationPolicy,
    HistoricalCandleRetryPolicy,
    RetriedHistoricalCandlePageIngestor,
)
from wealth.application.quality import CandleSequenceAuditor
from wealth.domain.collection import (
    CollectionJobStatus,
    HistoricalCollectionJob,
    SourceHealthObservation,
    SourceHealthStatus,
)
from wealth.ports.collection import (
    CollectionCheckpointStore,
    CollectionCheckpointWriteStatus,
)
from wealth.ports.foundation import Clock, IdGenerator, Sleeper
from wealth.ports.market import CandleStore, HistoricalCandleRequest, HistoricalCandleSource

MAX_COLLECTION_LEASE = timedelta(hours=1)


class CollectionRunStatus(StrEnum):
    """Terminal result of one explicit collection invocation."""

    COMPLETED = "completed"
    FAILED = "failed"
    ALREADY_RUNNING = "already_running"
    CHECKPOINT_CONFLICT = "checkpoint_conflict"
    LOST_LEASE = "lost_lease"


class CollectionJobNotFoundError(LookupError):
    """Fail clearly when a requested durable collection job does not exist."""


@dataclass(frozen=True, slots=True)
class CollectionRunResult:
    """Return durable progress rather than hiding partial completion."""

    status: CollectionRunStatus
    checkpoint: HistoricalCollectionJob
    pages_attempted: int


@dataclass(frozen=True, slots=True)
class RecoverableHistoricalCandleCollector:
    """Run one bounded job with durable progress after every accepted page."""

    source: HistoricalCandleSource
    market_store: CandleStore
    checkpoint_store: CollectionCheckpointStore
    clock: Clock
    id_generator: IdGenerator
    sleeper: Sleeper
    worker_id: str
    source_name: str
    venue: str
    pagination_policy: HistoricalCandlePaginationPolicy = field(
        default_factory=HistoricalCandlePaginationPolicy
    )
    retry_policy: HistoricalCandleRetryPolicy = field(default_factory=HistoricalCandleRetryPolicy)
    auditor: CandleSequenceAuditor = field(default_factory=CandleSequenceAuditor)
    lease_duration: timedelta = timedelta(minutes=30)

    def __post_init__(self) -> None:
        for value in (self.worker_id, self.source_name, self.venue):
            if not value or value != value.strip() or any(char.isspace() for char in value):
                raise ValueError("collector identifiers must be non-empty and whitespace-free")
        if not timedelta(0) < self.lease_duration <= MAX_COLLECTION_LEASE:
            raise ValueError("lease_duration must be positive and at most one hour")

    def create_job(self, request: HistoricalCandleRequest) -> HistoricalCollectionJob:
        """Create a durable pending job after validating the complete work bound."""

        HistoricalCandlePagePlanner(self.pagination_policy).page_count(request)
        now = self.clock.now()
        job = HistoricalCollectionJob(
            job_id=self.id_generator.new(),
            source=self.source_name,
            venue=self.venue,
            instrument=request.instrument,
            provider_symbol=request.provider_symbol,
            instrument_type=request.instrument_type,
            timeframe=request.timeframe,
            window_start=request.window_start,
            window_end_exclusive=request.window_end_exclusive,
            next_window_start=request.window_start,
            status=CollectionJobStatus.PENDING,
            created_at=now,
            updated_at=now,
            version=1,
        )
        result = self.checkpoint_store.create(job)
        if result.status not in {
            CollectionCheckpointWriteStatus.INSERTED,
            CollectionCheckpointWriteStatus.DUPLICATE,
        }:
            raise RuntimeError("collection job identifier conflicted with existing state")
        return job

    def run(self, job_id: UUID) -> CollectionRunResult:
        """Claim and advance a job; safe retries resume from its durable cursor."""

        checkpoint = self.checkpoint_store.get(job_id)
        if checkpoint is None:
            raise CollectionJobNotFoundError(str(job_id))
        if checkpoint.status is CollectionJobStatus.COMPLETED:
            return CollectionRunResult(
                status=CollectionRunStatus.COMPLETED,
                checkpoint=checkpoint,
                pages_attempted=0,
            )

        claimed = self._claim(checkpoint)
        if claimed is None:
            current = self.checkpoint_store.get(job_id)
            if current is None:
                raise CollectionJobNotFoundError(str(job_id))
            if current.status is CollectionJobStatus.COMPLETED:
                return CollectionRunResult(
                    status=CollectionRunStatus.COMPLETED,
                    checkpoint=current,
                    pages_attempted=0,
                )
            return CollectionRunResult(
                status=CollectionRunStatus.ALREADY_RUNNING,
                checkpoint=current,
                pages_attempted=0,
            )

        remaining_request = self._request_from_cursor(claimed)
        planner = HistoricalCandlePagePlanner(self.pagination_policy)
        remaining_page_count = planner.page_count(remaining_request)
        page_ingestor = RetriedHistoricalCandlePageIngestor(
            ingestor=HistoricalCandleIngestor(
                source=self.source,
                store=self.market_store,
                auditor=self.auditor,
            ),
            sleeper=self.sleeper,
            retry_policy=self.retry_policy,
        )

        current = claimed
        pages_attempted = 0
        for pages_attempted, page_request in enumerate(
            planner.iter_pages(remaining_request),
            start=1,
        ):
            page_result = page_ingestor.ingest(page_request)
            observed_at = self.clock.now()
            if current.lease_expires_at is None or current.lease_expires_at <= observed_at:
                return CollectionRunResult(
                    status=CollectionRunStatus.LOST_LEASE,
                    checkpoint=current,
                    pages_attempted=pages_attempted,
                )

            failure = self._page_failure(current, page_result)
            if failure is not None:
                failure_code, stop_reason, health_status = failure
                failed = self._validated_copy(
                    current,
                    status=CollectionJobStatus.FAILED,
                    updated_at=observed_at,
                    version=current.version + 1,
                    lease_owner=None,
                    lease_expires_at=None,
                    total_attempts=current.total_attempts + page_result.attempts,
                    last_failure_code=failure_code,
                    last_stop_reason=stop_reason,
                )
                health = self._health_observation(
                    current,
                    page_result,
                    observed_at=observed_at,
                    accepted=False,
                    status=health_status,
                    failure_code=failure_code,
                    stop_reason=stop_reason,
                )
                write = self.checkpoint_store.transition(
                    failed,
                    expected_version=current.version,
                    health=health,
                )
                if write.status is not CollectionCheckpointWriteStatus.UPDATED:
                    return CollectionRunResult(
                        status=CollectionRunStatus.CHECKPOINT_CONFLICT,
                        checkpoint=current,
                        pages_attempted=pages_attempted,
                    )
                return CollectionRunResult(
                    status=CollectionRunStatus.FAILED,
                    checkpoint=failed,
                    pages_attempted=pages_attempted,
                )

            next_cursor = page_request.window_end_exclusive
            is_complete = next_cursor == current.window_end_exclusive
            advanced = self._validated_copy(
                current,
                status=(
                    CollectionJobStatus.COMPLETED if is_complete else CollectionJobStatus.RUNNING
                ),
                next_window_start=next_cursor,
                updated_at=observed_at,
                version=current.version + 1,
                lease_owner=None if is_complete else self.worker_id,
                lease_expires_at=(None if is_complete else observed_at + self.lease_duration),
                pages_completed=current.pages_completed + 1,
                candles_completed=(next_cursor - current.window_start)
                // current.timeframe.duration,
                total_attempts=current.total_attempts + page_result.attempts,
            )
            health = self._health_observation(
                current,
                page_result,
                observed_at=observed_at,
                accepted=True,
                status=(
                    SourceHealthStatus.HEALTHY
                    if page_result.attempts == 1
                    else SourceHealthStatus.DEGRADED
                ),
            )
            write = self.checkpoint_store.transition(
                advanced,
                expected_version=current.version,
                health=health,
            )
            if write.status is not CollectionCheckpointWriteStatus.UPDATED:
                return CollectionRunResult(
                    status=CollectionRunStatus.CHECKPOINT_CONFLICT,
                    checkpoint=current,
                    pages_attempted=pages_attempted,
                )
            current = advanced
            if is_complete:
                return CollectionRunResult(
                    status=CollectionRunStatus.COMPLETED,
                    checkpoint=current,
                    pages_attempted=pages_attempted,
                )
            if pages_attempted < remaining_page_count:
                self.sleeper.sleep(self.pagination_policy.inter_page_delay_seconds)

        raise AssertionError("bounded collection request must contain at least one page")

    def _claim(
        self,
        checkpoint: HistoricalCollectionJob,
    ) -> HistoricalCollectionJob | None:
        current = checkpoint
        for _ in range(2):
            now = self.clock.now()
            if current.status is CollectionJobStatus.COMPLETED:
                return None
            if (
                current.status is CollectionJobStatus.RUNNING
                and current.lease_expires_at is not None
                and current.lease_expires_at > now
            ):
                return None
            claimed = self._validated_copy(
                current,
                status=CollectionJobStatus.RUNNING,
                updated_at=now,
                version=current.version + 1,
                lease_owner=self.worker_id,
                lease_expires_at=now + self.lease_duration,
                last_failure_code=None,
                last_stop_reason=None,
            )
            write = self.checkpoint_store.transition(
                claimed,
                expected_version=current.version,
            )
            if write.status is CollectionCheckpointWriteStatus.UPDATED:
                return claimed
            reloaded = self.checkpoint_store.get(current.job_id)
            if reloaded is None:
                raise CollectionJobNotFoundError(str(current.job_id))
            current = reloaded
        return None

    @staticmethod
    def _validated_copy(
        checkpoint: HistoricalCollectionJob,
        **updates: object,
    ) -> HistoricalCollectionJob:
        values = checkpoint.model_dump()
        values.update(updates)
        return HistoricalCollectionJob.model_validate(values)

    @staticmethod
    def _request_from_cursor(
        checkpoint: HistoricalCollectionJob,
    ) -> HistoricalCandleRequest:
        return HistoricalCandleRequest(
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            timeframe=checkpoint.timeframe,
            window_start=checkpoint.next_window_start,
            window_end_exclusive=checkpoint.window_end_exclusive,
        )

    def _page_failure(
        self,
        checkpoint: HistoricalCollectionJob,
        result: HistoricalCandlePageIngestionResult,
    ) -> tuple[str, str, SourceHealthStatus] | None:
        if result.failure is not None:
            unavailable_codes = {
                "provider_unavailable",
                "transport_failure",
            }
            return (
                result.failure.machine_code,
                result.failure.stop_reason.value,
                (
                    SourceHealthStatus.UNAVAILABLE
                    if result.failure.machine_code in unavailable_codes
                    else SourceHealthStatus.DEGRADED
                ),
            )
        if result.ingestion is None:
            raise AssertionError("page result must contain ingestion or failure")
        batch = result.ingestion.batch
        if batch.source != checkpoint.source or batch.venue != checkpoint.venue:
            return (
                "source_identity_mismatch",
                "contract_mismatch",
                SourceHealthStatus.DEGRADED,
            )
        if not result.ingestion.accepted:
            return (
                "page_rejected",
                "quality_or_storage_gate",
                SourceHealthStatus.DEGRADED,
            )
        return None

    def _health_observation(
        self,
        checkpoint: HistoricalCollectionJob,
        result: HistoricalCandlePageIngestionResult,
        *,
        observed_at: datetime,
        accepted: bool,
        status: SourceHealthStatus,
        failure_code: str | None = None,
        stop_reason: str | None = None,
    ) -> SourceHealthObservation:
        return SourceHealthObservation(
            observation_id=self.id_generator.new(),
            job_id=checkpoint.job_id,
            source=checkpoint.source,
            venue=checkpoint.venue,
            instrument=checkpoint.instrument,
            timeframe=checkpoint.timeframe,
            page_start=result.request.window_start,
            page_end_exclusive=result.request.window_end_exclusive,
            observed_at=observed_at,
            status=status,
            accepted=accepted,
            attempts=result.attempts,
            retry_delays_seconds=result.retry_delays_seconds,
            failure_code=failure_code,
            stop_reason=stop_reason,
        )
