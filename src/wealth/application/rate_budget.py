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
from wealth.ports.order_flow import (
    OrderFlowFetchBatch,
    PublicTradeSourceError,
    PublicTradeWindowRequest,
    PublicTradeWindowSource,
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
        _validate_request_cost(self.request_cost, self.policy)

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        """Fail before network access when the shared local budget is exhausted."""

        retry_after_seconds = _reserve_request_capacity(
            coordinator=self.coordinator,
            policy=self.policy,
            clock=self.clock,
            id_generator=self.id_generator,
            request_cost=self.request_cost,
        )
        if retry_after_seconds is not None:
            raise HistoricalCandleSourceError(
                LOCAL_RATE_BUDGET_EXHAUSTED,
                "shared local request budget is exhausted",
                retryable=True,
                retry_after_seconds=retry_after_seconds,
            )
        return self.source.fetch(request)


@dataclass(frozen=True, slots=True)
class RateBudgetedPublicTradeSource:
    """Reserve shared weighted capacity before one public trade request."""

    source: PublicTradeWindowSource
    coordinator: RateBudgetCoordinator
    policy: RateBudgetPolicy
    clock: Clock
    id_generator: IdGenerator
    request_cost: int

    def __post_init__(self) -> None:
        _validate_request_cost(self.request_cost, self.policy)

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        """Fail before network access when the shared local budget is exhausted."""

        retry_after_seconds = _reserve_request_capacity(
            coordinator=self.coordinator,
            policy=self.policy,
            clock=self.clock,
            id_generator=self.id_generator,
            request_cost=self.request_cost,
        )
        if retry_after_seconds is not None:
            raise PublicTradeSourceError(
                LOCAL_RATE_BUDGET_EXHAUSTED,
                "shared local request budget is exhausted",
                retryable=True,
                retry_after_seconds=retry_after_seconds,
            )
        return self.source.fetch(request)


def _validate_request_cost(request_cost: int, policy: RateBudgetPolicy) -> None:
    if (
        not isinstance(request_cost, int)
        or isinstance(request_cost, bool)
        or not 1 <= request_cost <= policy.capacity
    ):
        raise ValueError("request_cost must be positive and no greater than capacity")


def _reserve_request_capacity(
    *,
    coordinator: RateBudgetCoordinator,
    policy: RateBudgetPolicy,
    clock: Clock,
    id_generator: IdGenerator,
    request_cost: int,
) -> int | None:
    reservation = coordinator.reserve(
        policy=policy,
        request=RateBudgetRequest(
            reservation_id=id_generator.new(),
            budget_key=policy.budget_key,
            requested_at=clock.now(),
            cost=request_cost,
        ),
    )
    decision = reservation.decision
    if decision.status is RateBudgetDecisionStatus.GRANTED:
        return None
    if decision.retry_after_seconds is None:
        raise AssertionError("denied rate-budget decision requires a retry delay")
    return decision.retry_after_seconds
