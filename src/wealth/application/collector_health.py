"""Operational freshness and internal alerts for collector service runs."""

from dataclasses import dataclass, field
from datetime import datetime
from math import isfinite
from uuid import UUID

from wealth.domain.collector_service import (
    CollectorServiceAlertSeverity,
    CollectorServiceHealthAssessment,
    CollectorServiceHealthReport,
    CollectorServiceHealthReportStatus,
    CollectorServiceHealthStatus,
    CollectorServiceHeartbeat,
    CollectorServiceRunQuery,
    CollectorServiceStatus,
)
from wealth.ports.collector_service import CollectorServiceHeartbeatStore
from wealth.ports.foundation import Clock, require_utc_clock

MAX_COLLECTOR_STALE_AFTER_SECONDS = 604_800.0


class CollectorServiceHealthClockRegressionError(RuntimeError):
    """Fail closed when evaluation time precedes durable lifecycle evidence."""


@dataclass(frozen=True, slots=True)
class CollectorServiceHealthPolicy:
    """Define when a nonterminal service heartbeat requires attention."""

    stale_after_seconds: float = 600.0

    def __post_init__(self) -> None:
        if (
            not isfinite(self.stale_after_seconds)
            or not 0 < self.stale_after_seconds <= MAX_COLLECTOR_STALE_AFTER_SECONDS
        ):
            raise ValueError("stale_after_seconds must be finite, positive, and at most seven days")


@dataclass(frozen=True, slots=True)
class CollectorServiceHealthMonitor:
    """Evaluate recent durable run heartbeats without sending external alerts."""

    heartbeat_store: CollectorServiceHeartbeatStore
    clock: Clock
    policy: CollectorServiceHealthPolicy = field(default_factory=CollectorServiceHealthPolicy)

    def report(
        self,
        collection_id: UUID,
        *,
        run_limit: int = 1,
    ) -> CollectorServiceHealthReport:
        """Return a bounded newest-first report and structured internal alerts."""

        query = CollectorServiceRunQuery(
            collection_id=collection_id,
            limit=run_limit,
        )
        now = self._now()
        heartbeats = self.heartbeat_store.recent_runs(query)
        assessments = tuple(self._assess(heartbeat, now) for heartbeat in heartbeats)
        if not assessments:
            status = CollectorServiceHealthReportStatus.NOT_STARTED
        elif any(assessment.alert_code is not None for assessment in assessments):
            status = CollectorServiceHealthReportStatus.ATTENTION_REQUIRED
        elif any(
            assessment.health_status is CollectorServiceHealthStatus.ACTIVE
            for assessment in assessments
        ):
            status = CollectorServiceHealthReportStatus.HEALTHY
        else:
            status = CollectorServiceHealthReportStatus.IDLE
        return CollectorServiceHealthReport(
            collection_id=collection_id,
            evaluated_at=now,
            status=status,
            assessments=assessments,
        )

    def _assess(
        self,
        heartbeat: CollectorServiceHeartbeat,
        now: datetime,
    ) -> CollectorServiceHealthAssessment:
        if now < heartbeat.observed_at:
            raise CollectorServiceHealthClockRegressionError(
                "health evaluation time precedes a durable collector heartbeat"
            )
        age_seconds = (now - heartbeat.observed_at).total_seconds()
        if heartbeat.status in {
            CollectorServiceStatus.STARTING,
            CollectorServiceStatus.RUNNING,
        }:
            stale = age_seconds >= self.policy.stale_after_seconds
            health_status = (
                CollectorServiceHealthStatus.STALE if stale else CollectorServiceHealthStatus.ACTIVE
            )
        else:
            health_status = {
                CollectorServiceStatus.STOPPED: CollectorServiceHealthStatus.STOPPED,
                CollectorServiceStatus.PAUSED: CollectorServiceHealthStatus.PAUSED,
                CollectorServiceStatus.FAILED: CollectorServiceHealthStatus.FAILED,
                CollectorServiceStatus.CYCLE_LIMIT: CollectorServiceHealthStatus.COMPLETED,
            }[heartbeat.status]
        alert = {
            CollectorServiceHealthStatus.STALE: (
                "heartbeat_stale",
                CollectorServiceAlertSeverity.CRITICAL,
            ),
            CollectorServiceHealthStatus.PAUSED: (
                "collector_paused",
                CollectorServiceAlertSeverity.WARNING,
            ),
            CollectorServiceHealthStatus.FAILED: (
                "collector_failed",
                CollectorServiceAlertSeverity.CRITICAL,
            ),
        }.get(health_status)
        return CollectorServiceHealthAssessment(
            heartbeat=heartbeat,
            evaluated_at=now,
            heartbeat_age_seconds=age_seconds,
            health_status=health_status,
            alert_code=None if alert is None else alert[0],
            alert_severity=None if alert is None else alert[1],
        )

    def _now(self) -> datetime:
        return require_utc_clock(self.clock.now())
