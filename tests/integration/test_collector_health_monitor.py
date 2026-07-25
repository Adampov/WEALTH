"""Integration tests for queryable collector health and internal alerts."""

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from wealth.adapters.sqlite_collector_service import (
    SQLiteCollectorServiceHeartbeatStore,
    SQLiteCollectorServiceStorageError,
    SQLiteCollectorServiceStorageErrorCode,
)
from wealth.application.collector_health import (
    CollectorServiceHealthClockRegressionError,
    CollectorServiceHealthMonitor,
    CollectorServiceHealthPolicy,
)
from wealth.domain.collector_service import (
    CollectorCycleStatus,
    CollectorServiceAlertSeverity,
    CollectorServiceHealthReportStatus,
    CollectorServiceHealthStatus,
    CollectorServiceHeartbeat,
    CollectorServiceRunQuery,
    CollectorServiceStatus,
)
from wealth.ports.collector_service import (
    CollectorServiceHeartbeatStore,
    CollectorServiceHeartbeatWriteStatus,
)

NOW = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)
START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
COLLECTION_ID = UUID(int=500)


class MutableClock:
    """Expose deterministic evaluation time."""

    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


class RecordingRecentRunStore:
    """Count health reads without exposing any durable mutation boundary."""

    def __init__(self) -> None:
        self.calls = 0

    def recent_runs(
        self,
        query: CollectorServiceRunQuery,
    ) -> tuple[CollectorServiceHeartbeat, ...]:
        del query
        self.calls += 1
        return ()


def heartbeat(
    *,
    run_id: int,
    sequence: int,
    observed_at: datetime,
    status: CollectorServiceStatus,
    cycles_attempted: int,
    last_cycle_status: CollectorCycleStatus | None = None,
    reason_code: str | None = None,
) -> CollectorServiceHeartbeat:
    """Build one strict durable service observation."""

    return CollectorServiceHeartbeat(
        heartbeat_id=UUID(int=run_id * 10 + sequence),
        run_id=UUID(int=run_id),
        collection_id=COLLECTION_ID,
        worker_id="worker-a",
        sequence=sequence,
        observed_at=observed_at,
        status=status,
        cycles_attempted=cycles_attempted,
        checkpoint_version=1,
        next_window_start=START,
        last_cycle_status=last_cycle_status,
        reason_code=reason_code,
    )


def persist_start(
    store: SQLiteCollectorServiceHeartbeatStore,
    *,
    run_id: int,
    observed_at: datetime,
) -> CollectorServiceHeartbeat:
    """Persist one nonterminal service run."""

    record = heartbeat(
        run_id=run_id,
        sequence=1,
        observed_at=observed_at,
        status=CollectorServiceStatus.STARTING,
        cycles_attempted=0,
    )
    assert store.append(record).status is CollectorServiceHeartbeatWriteStatus.INSERTED
    return record


def persist_terminal(
    store: SQLiteCollectorServiceHeartbeatStore,
    *,
    run_id: int,
    observed_at: datetime,
    status: CollectorServiceStatus,
) -> CollectorServiceHeartbeat:
    """Persist a starting heartbeat followed by one valid terminal outcome."""

    persist_start(
        store,
        run_id=run_id,
        observed_at=observed_at - timedelta(seconds=1),
    )
    cycle_status: CollectorCycleStatus | None
    cycles_attempted: int
    reason_code: str
    if status is CollectorServiceStatus.STOPPED:
        cycle_status = None
        cycles_attempted = 0
        reason_code = "shutdown_requested"
    elif status is CollectorServiceStatus.PAUSED:
        cycle_status = CollectorCycleStatus.PAUSED
        cycles_attempted = 1
        reason_code = "operator_requested"
    elif status is CollectorServiceStatus.FAILED:
        cycle_status = CollectorCycleStatus.ALREADY_RUNNING
        cycles_attempted = 1
        reason_code = "already_running"
    elif status is CollectorServiceStatus.CYCLE_LIMIT:
        cycle_status = CollectorCycleStatus.CAUGHT_UP
        cycles_attempted = 1
        reason_code = "cycle_limit_reached"
    else:
        raise AssertionError("fixture requires a terminal service status")
    record = heartbeat(
        run_id=run_id,
        sequence=2,
        observed_at=observed_at,
        status=status,
        cycles_attempted=cycles_attempted,
        last_cycle_status=cycle_status,
        reason_code=reason_code,
    )
    assert store.append(record).status is CollectorServiceHeartbeatWriteStatus.INSERTED
    return record


def monitor(
    store: SQLiteCollectorServiceHeartbeatStore,
    clock: MutableClock,
    *,
    stale_after_seconds: float = 30,
) -> CollectorServiceHealthMonitor:
    """Compose the real health evaluator over durable SQLite state."""

    return CollectorServiceHealthMonitor(
        heartbeat_store=store,
        clock=clock,
        policy=CollectorServiceHealthPolicy(stale_after_seconds=stale_after_seconds),
    )


def test_collection_without_service_runs_reports_not_started(tmp_path: Path) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")

    report = monitor(store, MutableClock()).report(COLLECTION_ID)

    assert report.status is CollectorServiceHealthReportStatus.NOT_STARTED
    assert report.assessments == ()
    assert report.alerts == ()


def test_invalid_health_monitor_clock_fails_before_store_reads(
    invalid_clock_value: datetime,
) -> None:
    store = RecordingRecentRunStore()
    health = CollectorServiceHealthMonitor(
        heartbeat_store=cast(CollectorServiceHeartbeatStore, store),
        clock=MutableClock(invalid_clock_value),
    )

    with pytest.raises(ValueError):
        health.report(COLLECTION_ID)

    assert store.calls == 0


def test_fresh_run_is_healthy_until_threshold_then_becomes_critical(
    tmp_path: Path,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    persist_start(store, run_id=1, observed_at=NOW - timedelta(seconds=29))
    clock = MutableClock()
    health = monitor(store, clock, stale_after_seconds=30)

    fresh = health.report(COLLECTION_ID)
    clock.value += timedelta(seconds=1)
    stale = health.report(COLLECTION_ID)

    assert fresh.status is CollectorServiceHealthReportStatus.HEALTHY
    assert fresh.assessments[0].health_status is CollectorServiceHealthStatus.ACTIVE
    assert fresh.alerts == ()
    assert stale.status is CollectorServiceHealthReportStatus.ATTENTION_REQUIRED
    assert stale.assessments[0].health_status is CollectorServiceHealthStatus.STALE
    assert stale.alerts[0].alert_code == "heartbeat_stale"
    assert stale.alerts[0].alert_severity is CollectorServiceAlertSeverity.CRITICAL


@pytest.mark.parametrize(
    ("terminal_status", "health_status", "alert_code", "severity"),
    [
        (
            CollectorServiceStatus.STOPPED,
            CollectorServiceHealthStatus.STOPPED,
            None,
            None,
        ),
        (
            CollectorServiceStatus.CYCLE_LIMIT,
            CollectorServiceHealthStatus.COMPLETED,
            None,
            None,
        ),
        (
            CollectorServiceStatus.PAUSED,
            CollectorServiceHealthStatus.PAUSED,
            "collector_paused",
            CollectorServiceAlertSeverity.WARNING,
        ),
        (
            CollectorServiceStatus.FAILED,
            CollectorServiceHealthStatus.FAILED,
            "collector_failed",
            CollectorServiceAlertSeverity.CRITICAL,
        ),
    ],
)
def test_terminal_states_have_deterministic_health_and_alert_mapping(
    tmp_path: Path,
    terminal_status: CollectorServiceStatus,
    health_status: CollectorServiceHealthStatus,
    alert_code: str | None,
    severity: CollectorServiceAlertSeverity | None,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / f"{terminal_status.value}.sqlite3")
    persist_terminal(
        store,
        run_id=1,
        observed_at=NOW - timedelta(seconds=5),
        status=terminal_status,
    )

    report = monitor(store, MutableClock()).report(COLLECTION_ID)
    assessment = report.assessments[0]

    assert assessment.health_status is health_status
    assert assessment.alert_code == alert_code
    assert assessment.alert_severity is severity
    assert report.status is (
        CollectorServiceHealthReportStatus.ATTENTION_REQUIRED
        if alert_code is not None
        else CollectorServiceHealthReportStatus.IDLE
    )


def test_recent_run_query_and_report_are_newest_first_and_bounded(tmp_path: Path) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    persist_terminal(
        store,
        run_id=1,
        observed_at=NOW - timedelta(seconds=20),
        status=CollectorServiceStatus.STOPPED,
    )
    persist_terminal(
        store,
        run_id=2,
        observed_at=NOW - timedelta(seconds=10),
        status=CollectorServiceStatus.CYCLE_LIMIT,
    )
    persist_start(store, run_id=3, observed_at=NOW - timedelta(seconds=1))

    records = store.recent_runs(CollectorServiceRunQuery(collection_id=COLLECTION_ID, limit=2))
    report = monitor(store, MutableClock()).report(COLLECTION_ID, run_limit=2)

    assert [record.run_id for record in records] == [UUID(int=3), UUID(int=2)]
    assert [item.heartbeat.run_id for item in report.assessments] == [
        UUID(int=3),
        UUID(int=2),
    ]
    assert report.status is CollectorServiceHealthReportStatus.HEALTHY


def test_future_heartbeat_fails_closed_instead_of_hiding_clock_regression(
    tmp_path: Path,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    persist_start(store, run_id=1, observed_at=NOW + timedelta(seconds=1))

    with pytest.raises(CollectorServiceHealthClockRegressionError, match="precedes"):
        monitor(store, MutableClock()).report(COLLECTION_ID)


def test_tampered_collection_projection_fails_closed_in_recent_query(
    tmp_path: Path,
) -> None:
    database = tmp_path / "service.sqlite3"
    store = SQLiteCollectorServiceHeartbeatStore(database)
    persist_start(store, run_id=1, observed_at=NOW)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            UPDATE collector_service_runs
            SET collection_id = ?
            WHERE run_id = ?
            """,
            (str(UUID(int=999)), str(UUID(int=1))),
        )

    with pytest.raises(SQLiteCollectorServiceStorageError) as failure:
        store.recent_runs(CollectorServiceRunQuery(collection_id=UUID(int=999), limit=10))

    assert failure.value.code is SQLiteCollectorServiceStorageErrorCode.CORRUPT_RECORD
