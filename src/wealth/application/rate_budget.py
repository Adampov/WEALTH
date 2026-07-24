"""Application gate for shared provider request budgets."""

from dataclasses import dataclass

from wealth.domain.rate_budget import (
    RateBudgetDecisionStatus,
    RateBudgetPolicy,
    RateBudgetRequest,
)
from wealth.ports.foundation import Clock, IdGenerator
from wealth.ports.market import (
    CandleFetchBatch,
    HistoricalCandleRequest,
    HistoricalCandleSource,
    HistoricalCandleSourceError,
)
from wealth.ports.rate_budget import RateBudgetCoordinator

LOCAL_RATE_BUDGET_EXHAUSTED = "local_rate_budget_exhausted"


@dataclass(frozen=True, slots=True)
class RateBudgetedHistoricalCandleSource:
    """Reserve shared capacity before delegating one public source request."""

    source: HistoricalCandleSource
    coordinator: RateBudgetCoordinator
    policy: RateBudgetPolicy
    clock: Clock
    id_generator: IdGenerator
    request_cost: int = 1

    def __post_init__(self) -> None:
        if not 1 <= self.request_cost <= self.policy.capacity:
            raise ValueError("request_cost must be positive and no greater than capacity")

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        """Fail before network access when the shared local budget is exhausted."""

        reservation = self.coordinator.reserve(
            policy=self.policy,
            request=RateBudgetRequest(
                reservation_id=self.id_generator.new(),
                budget_key=self.policy.budget_key,
                requested_at=self.clock.now(),
                cost=self.request_cost,
            ),
        )
        decision = reservation.decision
        if decision.status is RateBudgetDecisionStatus.DENIED:
            if decision.retry_after_seconds is None:
                raise AssertionError("denied rate-budget decision requires a retry delay")
            raise HistoricalCandleSourceError(
                LOCAL_RATE_BUDGET_EXHAUSTED,
                "shared local request budget is exhausted",
                retryable=True,
                retry_after_seconds=decision.retry_after_seconds,
            )
        return self.source.fetch(request)
