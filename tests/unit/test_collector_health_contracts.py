"""Unit tests for collector service health contracts and policy."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.application.collector_health import CollectorServiceHealthPolicy
from wealth.domain.collector_service import (
    CollectorServiceAlertSeverity,
    CollectorServiceHealthAssessment,
    CollectorServiceHealthReport,
    CollectorServiceHealthReportStatus,
    CollectorServiceHealthStatus,
    CollectorServiceHeartbeat,
    CollectorServiceStatus,
)

NOW = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)
START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)


def starting_heartbeat(*, run_id: int, observed_at: datetime) -> CollectorServiceHeartbeat:
    """Build one pristine nonterminal heartbeat."""

    return CollectorServiceHeartbeat(
        heartbeat_id=UUID(int=run_id + 1_000),
        run_id=UUID(int=run_id),
        collection_id=UUID(int=500),
        worker_id="worker-a",
        sequence=1,
        observed_at=observed_at,
        status=CollectorServiceStatus.STARTING,
        cycles_attempted=0,
        checkpoint_version=1,
        next_window_start=START,
    )


def assessment(
    *,
    run_id: int,
    observed_at: datetime,
    status: CollectorServiceHealthStatus,
    alert_code: str | None,
    severity: CollectorServiceAlertSeverity | None,
) -> CollectorServiceHealthAssessment:
    """Build one internally consistent health assessment."""

    return CollectorServiceHealthAssessment(
        heartbeat=starting_heartbeat(run_id=run_id, observed_at=observed_at),
        evaluated_at=NOW,
        heartbeat_age_seconds=(NOW - observed_at).total_seconds(),
        health_status=status,
        alert_code=alert_code,
        alert_severity=severity,
    )


def test_stale_assessment_requires_exact_critical_alert() -> None:
    stale = assessment(
        run_id=1,
        observed_at=NOW - timedelta(seconds=30),
        status=CollectorServiceHealthStatus.STALE,
        alert_code="heartbeat_stale",
        severity=CollectorServiceAlertSeverity.CRITICAL,
    )

    assert stale.alert_code == "heartbeat_stale"
    invalid = stale.model_dump()
    invalid["alert_severity"] = CollectorServiceAlertSeverity.WARNING
    with pytest.raises(ValidationError, match="does not match"):
        CollectorServiceHealthAssessment.model_validate(invalid)


def test_report_rejects_duplicate_runs_and_incorrect_aggregate_state() -> None:
    active = assessment(
        run_id=1,
        observed_at=NOW - timedelta(seconds=1),
        status=CollectorServiceHealthStatus.ACTIVE,
        alert_code=None,
        severity=None,
    )

    with pytest.raises(ValidationError, match="duplicate"):
        CollectorServiceHealthReport(
            collection_id=UUID(int=500),
            evaluated_at=NOW,
            status=CollectorServiceHealthReportStatus.HEALTHY,
            assessments=(active, active),
        )
    with pytest.raises(ValidationError, match="aggregate"):
        CollectorServiceHealthReport(
            collection_id=UUID(int=500),
            evaluated_at=NOW,
            status=CollectorServiceHealthReportStatus.IDLE,
            assessments=(active,),
        )


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 604_801])
def test_health_policy_rejects_unsafe_staleness_thresholds(value: float) -> None:
    with pytest.raises(ValueError, match="stale_after_seconds"):
        CollectorServiceHealthPolicy(stale_after_seconds=value)


def test_default_threshold_exceeds_the_maximum_legal_collector_wait() -> None:
    assert CollectorServiceHealthPolicy().stale_after_seconds == 600
