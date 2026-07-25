"""Unit contracts for bounded public-trade collection orchestration."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta, timezone
from typing import cast
from uuid import UUID

import pytest

from wealth.application.order_flow_range import (
    PublicTradeRangePolicy,
    PublicTradeRetryPolicy,
)
from wealth.application.public_trade_collection import (
    PublicTradeCollectionClockError,
    PublicTradeCollectionFailureCode,
    PublicTradeCollectionOrchestrator,
    PublicTradeCollectionPolicy,
    PublicTradeCollectionRunStatus,
    public_trade_collection_policy_fingerprint,
)
from wealth.domain.collection import SourceHealthStatus
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionHealthSummary,
    PublicTradeSourceHealthObservation,
)
from wealth.domain.rate_budget import (
    RateBudgetDecision,
    RateBudgetDecisionStatus,
    RateBudgetPolicy,
    RateBudgetRequest,
    RateBudgetReservationResult,
)
from wealth.ports.collection import (
    CollectionCheckpointWriteResult,
    CollectionCheckpointWriteStatus,
)
from wealth.ports.order_flow import (
    OrderFlowFetchBatch,
    OrderFlowStore,
    PublicTradeSourceError,
    PublicTradeWindowRequest,
)
from wealth.ports.rate_budget import RateBudgetCoordinator

NOW = datetime(2026, 7, 25, 10, 0, tzinfo=UTC)
JOB_ID = UUID(int=1)


def base_policy() -> PublicTradeCollectionPolicy:
    """Return a policy whose every fingerprint field has a valid alternate."""

    return PublicTradeCollectionPolicy(
        range=PublicTradeRangePolicy(
            initial_window_duration=timedelta(seconds=2),
            minimum_window_duration=timedelta(milliseconds=1),
            max_range_duration=timedelta(seconds=10),
            max_source_requests=10,
            max_records_per_run=100,
            inter_request_delay_seconds=0.25,
        ),
        retry=PublicTradeRetryPolicy(
            max_attempts=3,
            base_delay_seconds=1.0,
            max_delay_seconds=8.0,
            max_retry_after_seconds=30,
        ),
        rate_budget=RateBudgetPolicy(
            budget_key="binance-public",
            capacity=50,
            period_seconds=60,
        ),
        request_cost=4,
    )


PolicyMutation = Callable[[PublicTradeCollectionPolicy], PublicTradeCollectionPolicy]


@pytest.mark.parametrize(
    ("category", "mutate"),
    [
        pytest.param(
            "range.initial_window",
            lambda policy: replace(
                policy,
                range=replace(
                    policy.range,
                    initial_window_duration=timedelta(seconds=3),
                ),
            ),
            id="range-initial-window",
        ),
        pytest.param(
            "split.minimum_window",
            lambda policy: replace(
                policy,
                range=replace(
                    policy.range,
                    minimum_window_duration=timedelta(milliseconds=2),
                ),
            ),
            id="split-minimum-window",
        ),
        pytest.param(
            "range.maximum",
            lambda policy: replace(
                policy,
                range=replace(
                    policy.range,
                    max_range_duration=timedelta(seconds=11),
                ),
            ),
            id="range-maximum",
        ),
        pytest.param(
            "range.request_bound",
            lambda policy: replace(
                policy,
                range=replace(policy.range, max_source_requests=11),
            ),
            id="range-request-bound",
        ),
        pytest.param(
            "range.record_bound",
            lambda policy: replace(
                policy,
                range=replace(policy.range, max_records_per_run=101),
            ),
            id="range-record-bound",
        ),
        pytest.param(
            "pacing",
            lambda policy: replace(
                policy,
                range=replace(policy.range, inter_request_delay_seconds=0.5),
            ),
            id="pacing",
        ),
        pytest.param(
            "retry.attempts",
            lambda policy: replace(
                policy,
                retry=replace(policy.retry, max_attempts=4),
            ),
            id="retry-attempts",
        ),
        pytest.param(
            "retry.base_delay",
            lambda policy: replace(
                policy,
                retry=replace(policy.retry, base_delay_seconds=2.0),
            ),
            id="retry-base-delay",
        ),
        pytest.param(
            "retry.maximum_delay",
            lambda policy: replace(
                policy,
                retry=replace(policy.retry, max_delay_seconds=9.0),
            ),
            id="retry-maximum-delay",
        ),
        pytest.param(
            "retry.retry_after",
            lambda policy: replace(
                policy,
                retry=replace(policy.retry, max_retry_after_seconds=31),
            ),
            id="retry-after-bound",
        ),
        pytest.param(
            "budget.key",
            lambda policy: replace(
                policy,
                rate_budget=RateBudgetPolicy(
                    budget_key="binance-public-v2",
                    capacity=policy.rate_budget.capacity,
                    period_seconds=policy.rate_budget.period_seconds,
                ),
            ),
            id="budget-key",
        ),
        pytest.param(
            "budget.capacity",
            lambda policy: replace(
                policy,
                rate_budget=RateBudgetPolicy(
                    budget_key=policy.rate_budget.budget_key,
                    capacity=51,
                    period_seconds=policy.rate_budget.period_seconds,
                ),
            ),
            id="budget-capacity",
        ),
        pytest.param(
            "budget.period",
            lambda policy: replace(
                policy,
                rate_budget=RateBudgetPolicy(
                    budget_key=policy.rate_budget.budget_key,
                    capacity=policy.rate_budget.capacity,
                    period_seconds=61,
                ),
            ),
            id="budget-period",
        ),
        pytest.param(
            "budget.request_cost",
            lambda policy: replace(policy, request_cost=5),
            id="request-cost",
        ),
        pytest.param(
            "policy.version",
            lambda policy: replace(policy, version="1.1"),
            id="policy-version",
        ),
    ],
)
def test_policy_fingerprint_is_sensitive_to_every_effective_category(
    category: str,
    mutate: PolicyMutation,
) -> None:
    policy = base_policy()

    assert mutate(policy).fingerprint != policy.fingerprint, category


def test_policy_fingerprint_is_stable_canonical_and_matches_the_public_function() -> None:
    first = base_policy()
    second = base_policy()

    direct = public_trade_collection_policy_fingerprint(
        range_policy=first.range,
        retry_policy=first.retry,
        rate_budget_policy=first.rate_budget,
        request_cost=first.request_cost,
        policy_version=first.version,
    )

    assert first.fingerprint == second.fingerprint == direct
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", direct)


def test_request_budget_and_provider_cost_have_no_implicit_fallback() -> None:
    with pytest.raises(TypeError):
        PublicTradeCollectionPolicy()  # type: ignore[call-arg]


@dataclass(slots=True)
class FixedClock:
    """Return one deterministic timestamp."""

    value: datetime

    def now(self) -> datetime:
        return self.value


@dataclass(slots=True)
class SequenceIdGenerator:
    """Return deterministic, distinct UUIDs."""

    next_integer: int = 100

    def new(self) -> UUID:
        value = UUID(int=self.next_integer)
        self.next_integer += 1
        return value


@dataclass(slots=True)
class NoopSleeper:
    """Reject unexpected retry or pacing waits in focused unit cases."""

    calls: list[float] = field(default_factory=list)

    def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)


@dataclass(slots=True)
class MemoryCheckpointStore:
    """Minimal optimistic store used to observe public orchestration behavior."""

    checkpoint: PublicTradeCollectionCheckpoint | None = None
    health: list[PublicTradeSourceHealthObservation] = field(default_factory=list)
    create_calls: int = 0

    def create(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> CollectionCheckpointWriteResult:
        self.create_calls += 1
        if self.checkpoint is None:
            self.checkpoint = checkpoint
            status = CollectionCheckpointWriteStatus.INSERTED
        else:
            status = (
                CollectionCheckpointWriteStatus.DUPLICATE
                if self.checkpoint == checkpoint
                else CollectionCheckpointWriteStatus.CONFLICT
            )
        return CollectionCheckpointWriteResult(
            status=status,
            job_id=checkpoint.job_id,
            current_version=self.checkpoint.version,
        )

    def get(self, job_id: UUID) -> PublicTradeCollectionCheckpoint | None:
        if self.checkpoint is None or self.checkpoint.job_id != job_id:
            return None
        return self.checkpoint

    def transition(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        expected_version: int,
        expected_lease_token: UUID | None = None,
        health: PublicTradeSourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        del expected_lease_token
        current = self.checkpoint
        if current is None or current.version != expected_version:
            return CollectionCheckpointWriteResult(
                status=CollectionCheckpointWriteStatus.CONFLICT,
                job_id=checkpoint.job_id,
                current_version=0 if current is None else current.version,
            )
        self.checkpoint = checkpoint
        if health is not None:
            self.health.append(health)
        return CollectionCheckpointWriteResult(
            status=CollectionCheckpointWriteStatus.UPDATED,
            job_id=checkpoint.job_id,
            current_version=checkpoint.version,
        )

    def health_for_job(
        self,
        job_id: UUID,
        *,
        after_checkpoint_version: int | None = None,
        limit: int = 100,
    ) -> tuple[PublicTradeSourceHealthObservation, ...]:
        return tuple(
            observation
            for observation in self.health
            if observation.job_id == job_id
            and (
                after_checkpoint_version is None
                or observation.checkpoint_version > after_checkpoint_version
            )
        )[:limit]

    def health_summary(self, job_id: UUID) -> PublicTradeCollectionHealthSummary:
        raise AssertionError(f"health summary was not expected for {job_id}")


@dataclass(slots=True)
class GrantingRateBudgetCoordinator:
    """Grant each request with valid typed evidence."""

    decisions: list[RateBudgetDecision] = field(default_factory=list)

    def reserve(
        self,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
    ) -> RateBudgetReservationResult:
        decision = RateBudgetDecision(
            reservation_id=request.reservation_id,
            budget_key=request.budget_key,
            requested_at=request.requested_at,
            cost=request.cost,
            capacity=policy.capacity,
            period_seconds=policy.period_seconds,
            status=RateBudgetDecisionStatus.GRANTED,
            reason_code="granted",
            available_capacity=policy.capacity - request.cost,
            theoretical_arrival_at=request.requested_at,
        )
        self.decisions.append(decision)
        return RateBudgetReservationResult(decision=decision)


@dataclass(slots=True)
class FailingPublicTradeSource:
    """Expose one provider-neutral typed terminal failure."""

    machine_code: str
    requires_smaller_window: bool = False
    calls: int = 0

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        del request
        self.calls += 1
        raise PublicTradeSourceError(
            self.machine_code,
            "safe test failure",
            retryable=False,
            requires_smaller_window=self.requires_smaller_window,
        )


def request(
    *,
    window_start: datetime = NOW,
    window_end_exclusive: datetime = NOW + timedelta(seconds=1),
) -> PublicTradeWindowRequest:
    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )


def orchestrator(
    *,
    clock: FixedClock,
    source: FailingPublicTradeSource | None = None,
    checkpoint_store: MemoryCheckpointStore | None = None,
    policy: PublicTradeCollectionPolicy | None = None,
) -> PublicTradeCollectionOrchestrator:
    return PublicTradeCollectionOrchestrator(
        source=source or FailingPublicTradeSource("unused"),
        evidence_store=cast(OrderFlowStore, object()),
        checkpoint_store=checkpoint_store or MemoryCheckpointStore(),
        rate_budget_coordinator=cast(
            RateBudgetCoordinator,
            GrantingRateBudgetCoordinator(),
        ),
        clock=clock,
        id_generator=SequenceIdGenerator(),
        sleeper=NoopSleeper(),
        worker_id="worker-a",
        source_name="binance.public-rest",
        venue="BINANCE",
        policy=policy or base_policy(),
    )


@pytest.mark.parametrize(
    ("window_start", "window_end_exclusive", "field_name"),
    [
        pytest.param(
            NOW.astimezone(timezone(timedelta(hours=2))),
            (NOW + timedelta(seconds=1)).astimezone(timezone(timedelta(hours=2))),
            "window_start",
            id="non-utc-start",
        ),
        pytest.param(
            NOW,
            (NOW + timedelta(seconds=1)).astimezone(timezone(timedelta(hours=2))),
            "window_end_exclusive",
            id="non-utc-end",
        ),
    ],
)
def test_create_job_rejects_non_utc_request_boundaries_before_storage(
    window_start: datetime,
    window_end_exclusive: datetime,
    field_name: str,
) -> None:
    store = MemoryCheckpointStore()
    collector = orchestrator(clock=FixedClock(NOW), checkpoint_store=store)

    with pytest.raises(PublicTradeCollectionClockError, match=field_name):
        collector.create_job(
            request(
                window_start=window_start,
                window_end_exclusive=window_end_exclusive,
            ),
            job_id=JOB_ID,
            created_at=NOW,
        )

    assert store.create_calls == 0


def test_create_job_rejects_a_non_utc_trusted_clock_before_storage() -> None:
    non_utc_now = NOW.astimezone(timezone(timedelta(hours=2)))
    store = MemoryCheckpointStore()
    collector = orchestrator(clock=FixedClock(non_utc_now), checkpoint_store=store)

    with pytest.raises(PublicTradeCollectionClockError, match="clock"):
        collector.create_job(request(), job_id=JOB_ID)

    assert store.create_calls == 0


def test_create_job_rejects_an_explicit_non_utc_creation_boundary() -> None:
    non_utc_now = NOW.astimezone(timezone(timedelta(hours=2)))
    store = MemoryCheckpointStore()
    collector = orchestrator(clock=FixedClock(NOW), checkpoint_store=store)

    with pytest.raises(PublicTradeCollectionClockError, match="created_at"):
        collector.create_job(request(), job_id=JOB_ID, created_at=non_utc_now)

    assert store.create_calls == 0


@pytest.mark.parametrize(
    ("machine_code", "expected_code", "expected_health"),
    [
        pytest.param(
            "provider_unavailable",
            PublicTradeCollectionFailureCode.PROVIDER_UNAVAILABLE,
            SourceHealthStatus.UNAVAILABLE,
            id="provider-unavailable",
        ),
        pytest.param(
            "transport_failure",
            PublicTradeCollectionFailureCode.PROVIDER_UNAVAILABLE,
            SourceHealthStatus.UNAVAILABLE,
            id="transport-failure",
        ),
        pytest.param(
            "local_rate_budget_exhausted",
            PublicTradeCollectionFailureCode.RATE_BUDGET_EXHAUSTED,
            SourceHealthStatus.DEGRADED,
            id="rate-budget",
        ),
        pytest.param(
            "source_identity_mismatch",
            PublicTradeCollectionFailureCode.SOURCE_IDENTITY_MISMATCH,
            SourceHealthStatus.DEGRADED,
            id="source-identity",
        ),
        pytest.param(
            "arbitrary_provider_internal_code",
            PublicTradeCollectionFailureCode.PROVIDER_REQUEST_REJECTED,
            SourceHealthStatus.DEGRADED,
            id="unknown-provider-code-is-bounded",
        ),
    ],
)
def test_terminal_source_failures_map_to_bounded_codes_through_run(
    machine_code: str,
    expected_code: PublicTradeCollectionFailureCode,
    expected_health: SourceHealthStatus,
) -> None:
    store = MemoryCheckpointStore()
    source = FailingPublicTradeSource(machine_code)
    collector = orchestrator(
        clock=FixedClock(NOW),
        source=source,
        checkpoint_store=store,
    )
    collector.create_job(request(), job_id=JOB_ID, created_at=NOW)

    result = collector.run(JOB_ID)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.last_failure_code == expected_code.value
    assert result.checkpoint.last_stop_reason == "non_retryable"
    assert source.calls == 1
    assert len(store.health) == 1
    assert store.health[0].failure_code == expected_code.value
    assert store.health[0].status is expected_health
    if machine_code == "arbitrary_provider_internal_code":
        assert result.checkpoint.last_failure_code != machine_code


def test_minimum_window_density_failure_maps_without_copying_provider_code() -> None:
    one_millisecond_policy = replace(
        base_policy(),
        range=PublicTradeRangePolicy(
            initial_window_duration=timedelta(milliseconds=1),
            minimum_window_duration=timedelta(milliseconds=1),
            max_range_duration=timedelta(milliseconds=1),
            max_source_requests=1,
            max_records_per_run=1,
            inter_request_delay_seconds=0,
        ),
    )
    store = MemoryCheckpointStore()
    source = FailingPublicTradeSource(
        "provider_specific_density_text",
        requires_smaller_window=True,
    )
    collector = orchestrator(
        clock=FixedClock(NOW),
        source=source,
        checkpoint_store=store,
        policy=one_millisecond_policy,
    )
    collector.create_job(
        request(window_end_exclusive=NOW + timedelta(milliseconds=1)),
        job_id=JOB_ID,
        created_at=NOW,
    )

    result = collector.run(JOB_ID)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.last_failure_code == PublicTradeCollectionFailureCode.DENSITY_LIMIT
    assert result.checkpoint.last_stop_reason == "minimum_window_reached"
    assert store.health[0].status is SourceHealthStatus.DEGRADED
