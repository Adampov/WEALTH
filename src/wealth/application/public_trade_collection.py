"""Restart-safe orchestration for bounded public-trade collection."""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from typing import Final
from uuid import UUID

from wealth.application.order_flow_quality import OrderFlowSequenceAuditor
from wealth.application.order_flow_range import (
    AdaptivePublicTradeRangeIngestor,
    PublicTradeRangeIngestionResult,
    PublicTradeRangePolicy,
    PublicTradeRangeStopReason,
    PublicTradeRetryPolicy,
    PublicTradeWindowOutcome,
)
from wealth.application.rate_budget import (
    LOCAL_RATE_BUDGET_EXHAUSTED,
    RateBudgetedPublicTradeSource,
)
from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.order_flow_collection import (
    MAX_PUBLIC_TRADE_LEASE_DURATION,
    PublicTradeCollectionCheckpoint,
    PublicTradeSourceHealthObservation,
)
from wealth.domain.order_flow_quality import (
    OrderFlowRecordType,
    OrderFlowWriteStatus,
)
from wealth.domain.quality import DataQualityStatus, RawPayloadWriteStatus
from wealth.domain.rate_budget import RateBudgetPolicy
from wealth.ports.collection import CollectionCheckpointWriteStatus
from wealth.ports.foundation import (
    Clock,
    ClockContractError,
    IdGenerator,
    Sleeper,
    require_utc_clock,
)
from wealth.ports.order_flow import (
    OrderFlowFetchBatch,
    OrderFlowStore,
    PublicTradeSourceError,
    PublicTradeWindowRequest,
    PublicTradeWindowSource,
)
from wealth.ports.order_flow_collection import PublicTradeCollectionCheckpointStore
from wealth.ports.rate_budget import RateBudgetCoordinator

PUBLIC_TRADE_COLLECTION_POLICY_VERSION = "1.0"
DEFAULT_PUBLIC_TRADE_COLLECTION_LEASE = timedelta(minutes=30)
MAX_PUBLIC_TRADE_COLLECTION_SEGMENTS = 2
_SOURCE_UNAVAILABLE_CODES: Final = frozenset(
    {
        "provider_unavailable",
        "transport_failure",
    }
)


class PublicTradeCollectionRunStatus(StrEnum):
    """Durable outcome of one explicit bounded invocation."""

    COMPLETED = "completed"
    PAUSED = "paused"
    FAILED = "failed"
    ALREADY_RUNNING = "already_running"
    CHECKPOINT_CONFLICT = "checkpoint_conflict"
    LOST_LEASE = "lost_lease"


class PublicTradeCollectionFailureCode(StrEnum):
    """Bounded control codes derived from typed range evidence."""

    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_REQUEST_REJECTED = "provider_request_rejected"
    RATE_BUDGET_EXHAUSTED = "rate_budget_exhausted"
    SOURCE_IDENTITY_MISMATCH = "source_identity_mismatch"
    DENSITY_LIMIT = "density_limit"
    QUALITY_REJECTED = "quality_rejected"
    EVIDENCE_CONFLICT = "evidence_conflict"
    EVIDENCE_ADMISSION_REJECTED = "evidence_admission_rejected"


class PublicTradeCollectionJobNotFoundError(LookupError):
    """Fail clearly when a durable public-trade job does not exist."""


class PublicTradeCollectionJobConflictError(RuntimeError):
    """Reject reuse of a job identifier for different immutable work."""


class PublicTradeCollectionPolicyDriftError(RuntimeError):
    """Reject recovery under collection semantics that differ from creation."""


class PublicTradeCollectionSourceDriftError(RuntimeError):
    """Reject a job created for another configured public source."""


class PublicTradeCollectionClockError(RuntimeError):
    """Reject a clock or boundary that is not represented in UTC."""


@dataclass(frozen=True, slots=True)
class PublicTradeCollectionRunResult:
    """Return durable state without overstating uncommitted network work."""

    status: PublicTradeCollectionRunStatus
    checkpoint: PublicTradeCollectionCheckpoint
    range_invocations: int


@dataclass(frozen=True, slots=True)
class PublicTradeCollectionPolicy:
    """Effective range, retry, pacing, and shared request-budget policy."""

    range: PublicTradeRangePolicy
    retry: PublicTradeRetryPolicy
    rate_budget: RateBudgetPolicy
    request_cost: int
    version: str = PUBLIC_TRADE_COLLECTION_POLICY_VERSION

    def __post_init__(self) -> None:
        _validate_policy_version(self.version)
        if (
            not isinstance(self.request_cost, int)
            or isinstance(self.request_cost, bool)
            or not 1 <= self.request_cost <= self.rate_budget.capacity
        ):
            raise ValueError("request_cost must be positive and no greater than budget capacity")

    @property
    def fingerprint(self) -> str:
        """Return one canonical immutable policy identity."""

        return public_trade_collection_policy_fingerprint(
            range_policy=self.range,
            retry_policy=self.retry,
            rate_budget_policy=self.rate_budget,
            request_cost=self.request_cost,
            policy_version=self.version,
        )


@dataclass(frozen=True, slots=True)
class _MappedRangeOutcome:
    status: CollectionJobStatus
    health_status: SourceHealthStatus
    failure_code: PublicTradeCollectionFailureCode | None
    stop_reason: str | None
    retry_delays_seconds: tuple[float, ...]
    windows_completed: int
    splits_completed: int


def public_trade_collection_policy_fingerprint(
    *,
    range_policy: PublicTradeRangePolicy,
    retry_policy: PublicTradeRetryPolicy,
    rate_budget_policy: RateBudgetPolicy,
    request_cost: int,
    policy_version: str = PUBLIC_TRADE_COLLECTION_POLICY_VERSION,
) -> str:
    """Hash every effective bounded collection-policy category canonically."""

    _validate_policy_version(policy_version)
    if (
        not isinstance(request_cost, int)
        or isinstance(request_cost, bool)
        or not 1 <= request_cost <= rate_budget_policy.capacity
    ):
        raise ValueError("request_cost must be positive and no greater than budget capacity")
    payload = {
        "policy_version": policy_version,
        "range": {
            "initial_window_milliseconds": _duration_milliseconds(
                range_policy.initial_window_duration
            ),
            "minimum_window_milliseconds": _duration_milliseconds(
                range_policy.minimum_window_duration
            ),
            "max_range_milliseconds": _duration_milliseconds(range_policy.max_range_duration),
            "max_source_requests": range_policy.max_source_requests,
            "max_records_per_run": range_policy.max_records_per_run,
            "inter_request_delay_seconds": float(range_policy.inter_request_delay_seconds).hex(),
        },
        "retry": {
            "max_attempts": retry_policy.max_attempts,
            "base_delay_seconds": float(retry_policy.base_delay_seconds).hex(),
            "max_delay_seconds": float(retry_policy.max_delay_seconds).hex(),
            "max_retry_after_seconds": retry_policy.max_retry_after_seconds,
        },
        "request_budget": {
            "schema_version": rate_budget_policy.schema_version,
            "budget_key": rate_budget_policy.budget_key,
            "capacity": rate_budget_policy.capacity,
            "period_seconds": rate_budget_policy.period_seconds,
            "request_cost": request_cost,
        },
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{sha256(canonical).hexdigest()}"


@dataclass(frozen=True, slots=True)
class PublicTradeCollectionOrchestrator:
    """Compose evidence, shared capacity, and control state for one bounded job."""

    source: PublicTradeWindowSource
    evidence_store: OrderFlowStore
    checkpoint_store: PublicTradeCollectionCheckpointStore
    rate_budget_coordinator: RateBudgetCoordinator
    clock: Clock
    id_generator: IdGenerator
    sleeper: Sleeper
    worker_id: str
    source_name: str
    venue: str
    policy: PublicTradeCollectionPolicy
    auditor: OrderFlowSequenceAuditor = field(default_factory=OrderFlowSequenceAuditor)
    lease_duration: timedelta = DEFAULT_PUBLIC_TRADE_COLLECTION_LEASE

    def __post_init__(self) -> None:
        _validate_identifier("worker_id", self.worker_id, maximum_length=128)
        _validate_identifier("source_name", self.source_name, maximum_length=128)
        _validate_identifier("venue", self.venue, maximum_length=64)
        if not timedelta(0) < self.lease_duration <= MAX_PUBLIC_TRADE_LEASE_DURATION:
            raise ValueError("lease_duration must be positive and at most one hour")

    @property
    def policy_fingerprint(self) -> str:
        """Return the immutable identity of all effective collection semantics."""

        return self.policy.fingerprint

    def create_job(
        self,
        request: PublicTradeWindowRequest,
        *,
        job_id: UUID | None = None,
        created_at: datetime | None = None,
    ) -> PublicTradeCollectionCheckpoint:
        """Validate and persist one pristine bounded public-trade job."""

        self._validate_request(request)
        now = self._trusted_now() if created_at is None else _require_utc("created_at", created_at)
        checkpoint = PublicTradeCollectionCheckpoint(
            job_id=self.id_generator.new() if job_id is None else job_id,
            source=self.source_name,
            venue=self.venue,
            instrument=request.instrument,
            provider_symbol=request.provider_symbol,
            instrument_type=request.instrument_type,
            policy_fingerprint=self.policy_fingerprint,
            window_start=request.window_start,
            window_end_exclusive=request.window_end_exclusive,
            next_window_start=request.window_start,
            status=CollectionJobStatus.PENDING,
            created_at=now,
            updated_at=now,
            version=1,
        )
        write = self.checkpoint_store.create(checkpoint)
        if write.status is CollectionCheckpointWriteStatus.INSERTED:
            return checkpoint
        if write.status is CollectionCheckpointWriteStatus.DUPLICATE:
            return self.load_job(checkpoint.job_id)
        raise PublicTradeCollectionJobConflictError(str(checkpoint.job_id))

    def load_job(self, job_id: UUID) -> PublicTradeCollectionCheckpoint:
        """Load a job only when its immutable policy and UTC boundary remain valid."""

        checkpoint = self.checkpoint_store.get(job_id)
        if checkpoint is None:
            raise PublicTradeCollectionJobNotFoundError(str(job_id))
        self._validate_checkpoint(checkpoint)
        self._assert_policy(checkpoint)
        return checkpoint

    def run(self, job_id: UUID) -> PublicTradeCollectionRunResult:
        """Claim and advance one explicit, finite, restart-safe collection job."""

        checkpoint = self.load_job(job_id)
        if checkpoint.status is CollectionJobStatus.COMPLETED:
            return PublicTradeCollectionRunResult(
                status=PublicTradeCollectionRunStatus.COMPLETED,
                checkpoint=checkpoint,
                range_invocations=0,
            )

        claimed, blocked = self._claim(checkpoint)
        if blocked is not None:
            return blocked
        if claimed is None:
            raise AssertionError("claim must return durable state or a blocking result")

        current = claimed
        for invocation in range(1, MAX_PUBLIC_TRADE_COLLECTION_SEGMENTS + 1):
            request = self._request_from_checkpoint(current)
            range_result = self._range_ingestor().ingest(request)

            observed_at = self._trusted_now()
            if current.lease_expires_at is None or current.lease_expires_at <= observed_at:
                return PublicTradeCollectionRunResult(
                    status=PublicTradeCollectionRunStatus.LOST_LEASE,
                    checkpoint=current,
                    range_invocations=invocation,
                )

            advanced, health = self._transition_for_result(
                current,
                range_result,
                observed_at=observed_at,
            )
            if current.lease_token is None:
                raise AssertionError("claimed public-trade checkpoint requires a lease token")
            write = self.checkpoint_store.transition(
                advanced,
                expected_version=current.version,
                expected_lease_token=current.lease_token,
                health=health,
            )
            if write.status is not CollectionCheckpointWriteStatus.UPDATED:
                durable = self.checkpoint_store.get(job_id)
                if durable is None:
                    raise PublicTradeCollectionJobNotFoundError(str(job_id))
                self._validate_checkpoint(durable)
                self._assert_policy(durable)
                return PublicTradeCollectionRunResult(
                    status=PublicTradeCollectionRunStatus.CHECKPOINT_CONFLICT,
                    checkpoint=durable,
                    range_invocations=invocation,
                )

            current = advanced
            if current.status is not CollectionJobStatus.RUNNING:
                return PublicTradeCollectionRunResult(
                    status=PublicTradeCollectionRunStatus(current.status.value),
                    checkpoint=current,
                    range_invocations=invocation,
                )
            if invocation == MAX_PUBLIC_TRADE_COLLECTION_SEGMENTS:
                raise AssertionError(
                    "exact pending-leaf recovery may require at most one remaining segment"
                )
            self.sleeper.sleep(self.policy.range.inter_request_delay_seconds)
            before_next_segment = self._trusted_now()
            if current.lease_expires_at is None or current.lease_expires_at <= before_next_segment:
                return PublicTradeCollectionRunResult(
                    status=PublicTradeCollectionRunStatus.LOST_LEASE,
                    checkpoint=current,
                    range_invocations=invocation,
                )

        raise AssertionError("bounded public-trade collection loop must return")

    def _claim(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> tuple[
        PublicTradeCollectionCheckpoint | None,
        PublicTradeCollectionRunResult | None,
    ]:
        now = self._trusted_now()
        if (
            checkpoint.status is CollectionJobStatus.RUNNING
            and checkpoint.lease_expires_at is not None
            and checkpoint.lease_expires_at > now
        ):
            return None, PublicTradeCollectionRunResult(
                status=PublicTradeCollectionRunStatus.ALREADY_RUNNING,
                checkpoint=checkpoint,
                range_invocations=0,
            )

        claimed = self._copy_checkpoint(
            checkpoint,
            status=CollectionJobStatus.RUNNING,
            updated_at=now,
            version=checkpoint.version + 1,
            lease_owner=self.worker_id,
            lease_token=self.id_generator.new(),
            lease_expires_at=now + self.lease_duration,
            last_failure_code=None,
            last_stop_reason=None,
        )
        write = self.checkpoint_store.transition(
            claimed,
            expected_version=checkpoint.version,
        )
        if write.status is CollectionCheckpointWriteStatus.UPDATED:
            return claimed, None

        durable = self.checkpoint_store.get(checkpoint.job_id)
        if durable is None:
            raise PublicTradeCollectionJobNotFoundError(str(checkpoint.job_id))
        self._validate_checkpoint(durable)
        self._assert_policy(durable)
        if durable.status is CollectionJobStatus.COMPLETED:
            status = PublicTradeCollectionRunStatus.COMPLETED
        elif (
            durable.status is CollectionJobStatus.RUNNING
            and durable.lease_expires_at is not None
            and durable.lease_expires_at > now
        ):
            status = PublicTradeCollectionRunStatus.ALREADY_RUNNING
        else:
            status = PublicTradeCollectionRunStatus.CHECKPOINT_CONFLICT
        return None, PublicTradeCollectionRunResult(
            status=status,
            checkpoint=durable,
            range_invocations=0,
        )

    def _transition_for_result(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
        result: PublicTradeRangeIngestionResult,
        *,
        observed_at: datetime,
    ) -> tuple[PublicTradeCollectionCheckpoint, PublicTradeSourceHealthObservation]:
        mapped = _map_range_outcome(result)
        pending_end = (
            None if result.pending_window is None else result.pending_window.window_end_exclusive
        )
        status = mapped.status
        if (
            status is CollectionJobStatus.COMPLETED
            and result.next_window_start < checkpoint.window_end_exclusive
        ):
            if checkpoint.pending_window_end_exclusive is None:
                raise AssertionError("only retained pending-leaf recovery may finish early")
            status = CollectionJobStatus.RUNNING

        retains_lease = status is CollectionJobStatus.RUNNING
        advanced = self._copy_checkpoint(
            checkpoint,
            next_window_start=result.next_window_start,
            pending_window_end_exclusive=pending_end,
            status=status,
            updated_at=observed_at,
            version=checkpoint.version + 1,
            lease_owner=self.worker_id if retains_lease else None,
            lease_token=checkpoint.lease_token if retains_lease else None,
            lease_expires_at=(observed_at + self.lease_duration if retains_lease else None),
            windows_completed=(checkpoint.windows_completed + mapped.windows_completed),
            records_completed=(checkpoint.records_completed + result.ingested_record_count),
            source_requests=(checkpoint.source_requests + result.source_request_count),
            window_traces=checkpoint.window_traces + len(result.traces),
            retry_attempts=(checkpoint.retry_attempts + len(mapped.retry_delays_seconds)),
            splits_completed=(checkpoint.splits_completed + mapped.splits_completed),
            last_failure_code=(None if mapped.failure_code is None else mapped.failure_code.value),
            last_stop_reason=mapped.stop_reason,
        )
        health = PublicTradeSourceHealthObservation(
            observation_id=self.id_generator.new(),
            job_id=checkpoint.job_id,
            checkpoint_version=advanced.version,
            source=checkpoint.source,
            venue=checkpoint.venue,
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            range_start=result.request.window_start,
            range_end_exclusive=result.request.window_end_exclusive,
            next_window_start=result.next_window_start,
            pending_window_end_exclusive=pending_end,
            observed_at=observed_at,
            status=mapped.health_status,
            accepted=result.accepted,
            source_requests=result.source_request_count,
            window_traces=len(result.traces),
            windows_completed=mapped.windows_completed,
            records_completed=result.ingested_record_count,
            splits_completed=mapped.splits_completed,
            retry_delays_seconds=mapped.retry_delays_seconds,
            failure_code=(None if mapped.failure_code is None else mapped.failure_code.value),
            stop_reason=mapped.stop_reason,
        )
        return advanced, health

    def _request_from_checkpoint(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> PublicTradeWindowRequest:
        return PublicTradeWindowRequest(
            instrument=checkpoint.instrument,
            provider_symbol=checkpoint.provider_symbol,
            instrument_type=checkpoint.instrument_type,
            window_start=checkpoint.next_window_start,
            window_end_exclusive=(
                checkpoint.pending_window_end_exclusive or checkpoint.window_end_exclusive
            ),
        )

    def _validate_request(self, request: PublicTradeWindowRequest) -> None:
        _require_utc("window_start", request.window_start)
        _require_utc("window_end_exclusive", request.window_end_exclusive)
        if request.duration > self.policy.range.max_range_duration:
            raise ValueError("public-trade job range exceeds the effective bounded range policy")

    def _validate_checkpoint(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> None:
        for name in (
            "window_start",
            "window_end_exclusive",
            "next_window_start",
            "created_at",
            "updated_at",
        ):
            _require_utc(name, getattr(checkpoint, name))
        if checkpoint.pending_window_end_exclusive is not None:
            _require_utc(
                "pending_window_end_exclusive",
                checkpoint.pending_window_end_exclusive,
            )
        if checkpoint.lease_expires_at is not None:
            _require_utc("lease_expires_at", checkpoint.lease_expires_at)
        if (
            checkpoint.window_end_exclusive - checkpoint.window_start
            > self.policy.range.max_range_duration
        ):
            raise ValueError("durable public-trade job exceeds the effective bounded range policy")

    def _assert_policy(self, checkpoint: PublicTradeCollectionCheckpoint) -> None:
        if checkpoint.source != self.source_name or checkpoint.venue != self.venue:
            raise PublicTradeCollectionSourceDriftError(
                "public-trade job source differs from configured source identity"
            )
        if checkpoint.policy_fingerprint != self.policy_fingerprint:
            raise PublicTradeCollectionPolicyDriftError(
                "public-trade collection policy differs from durable job identity"
            )

    def _range_ingestor(self) -> AdaptivePublicTradeRangeIngestor:
        checked_source = _IdentityCheckedPublicTradeSource(
            source=self.source,
            source_name=self.source_name,
            venue=self.venue,
        )
        budgeted_source = RateBudgetedPublicTradeSource(
            source=checked_source,
            coordinator=self.rate_budget_coordinator,
            policy=self.policy.rate_budget,
            clock=_UtcCheckedClock(self.clock),
            id_generator=self.id_generator,
            request_cost=self.policy.request_cost,
        )
        return AdaptivePublicTradeRangeIngestor(
            source=budgeted_source,
            store=self.evidence_store,
            sleeper=self.sleeper,
            range_policy=self.policy.range,
            retry_policy=self.policy.retry,
            auditor=self.auditor,
        )

    def _trusted_now(self) -> datetime:
        return _require_utc_clock(self.clock)

    @staticmethod
    def _copy_checkpoint(
        checkpoint: PublicTradeCollectionCheckpoint,
        **updates: object,
    ) -> PublicTradeCollectionCheckpoint:
        values = checkpoint.model_dump()
        values.update(updates)
        return PublicTradeCollectionCheckpoint.model_validate(values)


@dataclass(frozen=True, slots=True)
class _IdentityCheckedPublicTradeSource:
    """Reject a source-identity mismatch before evidence can reach storage."""

    source: PublicTradeWindowSource
    source_name: str
    venue: str

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        batch = self.source.fetch(request)
        if (
            batch.stream.source != self.source_name
            or batch.stream.venue != self.venue
            or batch.stream.instrument != request.instrument
            or batch.stream.instrument_type is not request.instrument_type
            or batch.stream.record_type is not OrderFlowRecordType.TRADE
        ):
            raise PublicTradeSourceError(
                "source_identity_mismatch",
                "public-trade source returned a different configured stream",
                retryable=False,
            )
        return batch


@dataclass(frozen=True, slots=True)
class _UtcCheckedClock:
    """Keep every shared-budget reservation on the trusted UTC boundary."""

    clock: Clock

    def now(self) -> datetime:
        return _require_utc_clock(self.clock)


def _map_range_outcome(
    result: PublicTradeRangeIngestionResult,
) -> _MappedRangeOutcome:
    retry_delays = tuple(delay for trace in result.traces for delay in trace.retry_delays_seconds)
    windows_completed = sum(
        trace.outcome is PublicTradeWindowOutcome.INGESTED for trace in result.traces
    )
    splits_completed = sum(
        trace.outcome is PublicTradeWindowOutcome.SPLIT for trace in result.traces
    )
    degraded = bool(retry_delays or splits_completed)

    if result.stop_reason is PublicTradeRangeStopReason.COMPLETED:
        return _MappedRangeOutcome(
            status=CollectionJobStatus.COMPLETED,
            health_status=(SourceHealthStatus.DEGRADED if degraded else SourceHealthStatus.HEALTHY),
            failure_code=None,
            stop_reason=None,
            retry_delays_seconds=retry_delays,
            windows_completed=windows_completed,
            splits_completed=splits_completed,
        )
    terminal = result.traces[-1]
    clean_request_limit = (
        result.stop_reason is PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED
        and terminal.outcome is not PublicTradeWindowOutcome.SOURCE_FAILURE
    )
    if clean_request_limit or result.stop_reason is PublicTradeRangeStopReason.RECORD_LIMIT_REACHED:
        return _MappedRangeOutcome(
            status=CollectionJobStatus.PAUSED,
            health_status=(SourceHealthStatus.DEGRADED if degraded else SourceHealthStatus.HEALTHY),
            failure_code=None,
            stop_reason=result.stop_reason.value,
            retry_delays_seconds=retry_delays,
            windows_completed=windows_completed,
            splits_completed=splits_completed,
        )

    if (
        result.stop_reason is PublicTradeRangeStopReason.SOURCE_FAILURE
        or terminal.outcome is PublicTradeWindowOutcome.SOURCE_FAILURE
    ):
        if terminal.source_failure is None:
            raise AssertionError("source failure requires typed terminal evidence")
        terminal_source_code = terminal.source_failure.machine_code
        if terminal_source_code in _SOURCE_UNAVAILABLE_CODES:
            failure_code = PublicTradeCollectionFailureCode.PROVIDER_UNAVAILABLE
            health_status = SourceHealthStatus.UNAVAILABLE
        elif terminal_source_code == LOCAL_RATE_BUDGET_EXHAUSTED:
            failure_code = PublicTradeCollectionFailureCode.RATE_BUDGET_EXHAUSTED
            health_status = SourceHealthStatus.DEGRADED
        elif terminal_source_code == "source_identity_mismatch":
            failure_code = PublicTradeCollectionFailureCode.SOURCE_IDENTITY_MISMATCH
            health_status = SourceHealthStatus.DEGRADED
        else:
            failure_code = PublicTradeCollectionFailureCode.PROVIDER_REQUEST_REJECTED
            health_status = SourceHealthStatus.DEGRADED
        if terminal.source_failure.retry_stop_reason is None:
            raise AssertionError("terminal source failure requires a typed retry stop")
        stop_reason = terminal.source_failure.retry_stop_reason.value
    elif result.stop_reason is PublicTradeRangeStopReason.MINIMUM_WINDOW_REACHED:
        failure_code = PublicTradeCollectionFailureCode.DENSITY_LIMIT
        health_status = SourceHealthStatus.DEGRADED
        stop_reason = result.stop_reason.value
    elif result.stop_reason is PublicTradeRangeStopReason.INGESTION_REJECTED:
        if terminal.ingestion is None:
            raise AssertionError("admission failure requires typed ingestion evidence")
        if terminal.ingestion.quality.status is DataQualityStatus.FAIL:
            failure_code = PublicTradeCollectionFailureCode.QUALITY_REJECTED
        elif (
            terminal.ingestion.raw_write is not None
            and terminal.ingestion.raw_write.status is RawPayloadWriteStatus.CONFLICT
        ) or any(
            write.status is OrderFlowWriteStatus.CONFLICT for write in terminal.ingestion.writes
        ):
            failure_code = PublicTradeCollectionFailureCode.EVIDENCE_CONFLICT
        else:
            failure_code = PublicTradeCollectionFailureCode.EVIDENCE_ADMISSION_REJECTED
        health_status = SourceHealthStatus.DEGRADED
        stop_reason = result.stop_reason.value
    else:
        raise AssertionError("unsupported typed public-trade range outcome")
    return _MappedRangeOutcome(
        status=CollectionJobStatus.FAILED,
        health_status=health_status,
        failure_code=failure_code,
        stop_reason=stop_reason,
        retry_delays_seconds=retry_delays,
        windows_completed=windows_completed,
        splits_completed=splits_completed,
    )


def _duration_milliseconds(value: timedelta) -> int:
    milliseconds = value // timedelta(milliseconds=1)
    if timedelta(milliseconds=milliseconds) != value:
        raise ValueError("policy duration must use whole milliseconds")
    return milliseconds


def _require_utc(name: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise PublicTradeCollectionClockError(f"{name} must use UTC")
    return value


def _require_utc_clock(clock: Clock) -> datetime:
    try:
        return require_utc_clock(clock.now())
    except ClockContractError as error:
        raise PublicTradeCollectionClockError(f"clock must use datetime.UTC: {error}") from error


def _validate_identifier(name: str, value: str, *, maximum_length: int) -> None:
    if (
        not value
        or len(value) > maximum_length
        or value != value.strip()
        or any(character.isspace() for character in value)
    ):
        raise ValueError(f"{name} must be canonical and at most {maximum_length} characters")


def _validate_policy_version(value: str) -> None:
    _validate_identifier("policy_version", value, maximum_length=32)
