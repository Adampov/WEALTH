"""Unit tests for bounded public-trade range policy and retry contracts."""

from datetime import timedelta

import pytest

from wealth.application.order_flow_range import (
    MAX_TRADE_RANGE,
    PublicTradeRangePolicy,
    PublicTradeRetryPolicy,
    PublicTradeRetryStopReason,
)
from wealth.ports.order_flow import PublicTradeSourceError


@pytest.mark.parametrize(
    "overrides",
    [
        {"initial_window_duration": timedelta(microseconds=1)},
        {"minimum_window_duration": timedelta(0)},
        {"minimum_window_duration": timedelta(milliseconds=2)},
        {"max_range_duration": MAX_TRADE_RANGE + timedelta(milliseconds=1)},
        {"max_source_requests": 0},
        {"max_source_requests": 1_025},
        {"max_records_per_run": 0},
        {"max_records_per_run": 100_001},
        {"inter_request_delay_seconds": float("inf")},
        {"inter_request_delay_seconds": -1},
    ],
)
def test_invalid_range_policy_is_rejected(overrides: dict[str, object]) -> None:
    values: dict[str, object] = {
        "initial_window_duration": timedelta(milliseconds=1),
        "minimum_window_duration": timedelta(milliseconds=1),
        "max_range_duration": timedelta(seconds=1),
    }
    values.update(overrides)

    with pytest.raises(ValueError):
        PublicTradeRangePolicy(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_attempts": 0},
        {"max_attempts": 6},
        {"base_delay_seconds": -1},
        {"base_delay_seconds": 2, "max_delay_seconds": 1},
        {"max_delay_seconds": 61},
        {"max_retry_after_seconds": -1},
        {"max_retry_after_seconds": 301},
    ],
)
def test_invalid_retry_policy_is_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        PublicTradeRetryPolicy(**overrides)  # type: ignore[arg-type]


def test_retry_policy_honors_explicit_transience_and_bounded_retry_after() -> None:
    policy = PublicTradeRetryPolicy(
        max_attempts=3,
        base_delay_seconds=2,
        max_delay_seconds=10,
        max_retry_after_seconds=60,
    )
    transient = PublicTradeSourceError("unavailable", "safe", retryable=True)
    bounded = PublicTradeSourceError(
        "rate_limited",
        "safe",
        retryable=True,
        retry_after_seconds=17,
    )
    excessive = PublicTradeSourceError(
        "rate_limited",
        "safe",
        retryable=True,
        retry_after_seconds=61,
    )

    assert policy.delay_after(failed_attempt=1, error=transient) == 2
    assert policy.delay_after(failed_attempt=2, error=transient) == 4
    assert policy.delay_after(failed_attempt=1, error=bounded) == 17
    assert policy.delay_after(failed_attempt=1, error=excessive) is None
    assert (
        policy.stop_reason_after(failed_attempt=1, error=excessive)
        is PublicTradeRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
    )


def test_smaller_window_failure_cannot_be_retryable_unchanged() -> None:
    with pytest.raises(ValueError, match="must not also be retryable"):
        PublicTradeSourceError(
            "possibly_truncated",
            "safe",
            retryable=True,
            requires_smaller_window=True,
        )
