"""Coordination boundary for shared provider request budgets."""

from typing import Protocol

from wealth.domain.rate_budget import (
    RateBudgetDecision,
    RateBudgetPolicy,
    RateBudgetRequest,
    RateBudgetReservationResult,
    RateBudgetSummary,
)


class RateBudgetCoordinator(Protocol):
    """Reserve request capacity atomically across cooperating workers."""

    def reserve(
        self,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
    ) -> RateBudgetReservationResult:
        """Return a durable grant or bounded denial."""

    def decisions_for_budget(
        self,
        budget_key: str,
    ) -> tuple[RateBudgetDecision, ...]:
        """Return durable decisions in deterministic request order."""

    def summary(self, budget_key: str) -> RateBudgetSummary:
        """Aggregate budget pressure without loading all decisions."""
