"""Integration tests for durable cross-process request-budget coordination."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import UUID

import pytest

from wealth.adapters.sqlite_rate_budget import (
    SQLiteRateBudgetCoordinator,
    SQLiteRateBudgetError,
    SQLiteRateBudgetErrorCode,
)
from wealth.domain.rate_budget import (
    RateBudgetDecisionStatus,
    RateBudgetPolicy,
    RateBudgetRequest,
)

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def policy(
    *,
    budget_key: str = "binance.public-rest.shared-ip",
    capacity: int = 3,
    period_seconds: int = 60,
) -> RateBudgetPolicy:
    return RateBudgetPolicy(
        budget_key=budget_key,
        capacity=capacity,
        period_seconds=period_seconds,
    )


def request(
    reservation_id: int,
    *,
    requested_at: datetime = NOW,
    cost: int = 1,
    budget_key: str = "binance.public-rest.shared-ip",
) -> RateBudgetRequest:
    return RateBudgetRequest(
        reservation_id=UUID(int=reservation_id),
        budget_key=budget_key,
        requested_at=requested_at,
        cost=cost,
    )


def test_weighted_budget_denies_then_recovers_with_exact_metrics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rate-budget.sqlite3"
    first_process = SQLiteRateBudgetCoordinator(path)
    second_process = SQLiteRateBudgetCoordinator(path)
    configured = policy()

    first = first_process.reserve(
        policy=configured,
        request=request(1, cost=2),
    )
    second = second_process.reserve(
        policy=configured,
        request=request(2),
    )
    denied = first_process.reserve(
        policy=configured,
        request=request(3),
    )
    recovered = second_process.reserve(
        policy=configured,
        request=request(4, requested_at=NOW + timedelta(seconds=20)),
    )
    summary = first_process.summary(configured.budget_key)

    assert first.decision.status is RateBudgetDecisionStatus.GRANTED
    assert first.decision.available_capacity == 1
    assert second.decision.status is RateBudgetDecisionStatus.GRANTED
    assert second.decision.available_capacity == 0
    assert denied.decision.status is RateBudgetDecisionStatus.DENIED
    assert denied.decision.retry_after_seconds == 20
    assert recovered.decision.status is RateBudgetDecisionStatus.GRANTED
    assert recovered.decision.available_capacity == 0
    assert summary.reservation_count == 4
    assert summary.granted_count == 3
    assert summary.denied_count == 1
    assert summary.total_requested_cost == 5
    assert summary.total_retry_after_seconds == 20
    assert summary.maximum_retry_after_seconds == 20


def test_replayed_reservation_is_not_charged_twice_and_conflict_fails(
    tmp_path: Path,
) -> None:
    coordinator = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    configured = policy()
    original_request = request(1, cost=2)

    original = coordinator.reserve(policy=configured, request=original_request)
    replayed = coordinator.reserve(policy=configured, request=original_request)

    assert original.replayed is False
    assert replayed.replayed is True
    assert replayed.decision == original.decision
    assert coordinator.summary(configured.budget_key).reservation_count == 1

    with pytest.raises(SQLiteRateBudgetError) as error:
        coordinator.reserve(
            policy=configured,
            request=request(1, cost=1),
        )

    assert error.value.code is SQLiteRateBudgetErrorCode.RESERVATION_CONFLICT


def test_policy_conflict_and_clock_regression_fail_closed(tmp_path: Path) -> None:
    coordinator = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    configured = policy()
    coordinator.reserve(
        policy=configured,
        request=request(1, requested_at=NOW + timedelta(seconds=5)),
    )

    with pytest.raises(SQLiteRateBudgetError) as policy_error:
        coordinator.reserve(
            policy=policy(capacity=4),
            request=request(2, requested_at=NOW + timedelta(seconds=5)),
        )
    with pytest.raises(SQLiteRateBudgetError) as clock_error:
        coordinator.reserve(
            policy=configured,
            request=request(3, requested_at=NOW + timedelta(seconds=4)),
        )

    assert policy_error.value.code is SQLiteRateBudgetErrorCode.POLICY_CONFLICT
    assert clock_error.value.code is SQLiteRateBudgetErrorCode.CLOCK_REGRESSION
    assert coordinator.summary(configured.budget_key).reservation_count == 1


def test_two_workers_competing_at_same_time_cannot_exceed_capacity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rate-budget.sqlite3"
    SQLiteRateBudgetCoordinator(path)
    configured = policy(capacity=1, period_seconds=10)
    barrier = Barrier(2)

    def reserve_from_worker(reservation_id: int) -> RateBudgetDecisionStatus:
        worker = SQLiteRateBudgetCoordinator(path)
        barrier.wait()
        return worker.reserve(
            policy=configured,
            request=request(reservation_id),
        ).decision.status

    with ThreadPoolExecutor(max_workers=2) as executor:
        statuses = tuple(
            executor.map(
                reserve_from_worker,
                (10, 11),
            )
        )

    assert sorted(statuses) == [
        RateBudgetDecisionStatus.DENIED,
        RateBudgetDecisionStatus.GRANTED,
    ]
    summary = SQLiteRateBudgetCoordinator(path).summary(configured.budget_key)
    assert summary.granted_count == 1
    assert summary.denied_count == 1


def test_tampered_decision_index_is_rejected_on_read(tmp_path: Path) -> None:
    path = tmp_path / "rate-budget.sqlite3"
    coordinator = SQLiteRateBudgetCoordinator(path)
    configured = policy()
    coordinator.reserve(policy=configured, request=request(1))
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE rate_budget_reservations
            SET status = 'denied'
            WHERE reservation_id = ?
            """,
            (str(UUID(int=1)),),
        )

    with pytest.raises(SQLiteRateBudgetError) as error:
        coordinator.decisions_for_budget(configured.budget_key)

    assert error.value.code is SQLiteRateBudgetErrorCode.CORRUPT_RECORD
