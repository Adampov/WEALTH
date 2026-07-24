"""Integration tests for the read-only collector health JSON command."""

import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import JsonValue

from wealth.adapters.sqlite_collector_service import (
    SQLiteCollectorServiceHeartbeatStore,
    SQLiteCollectorServiceStorageError,
    SQLiteCollectorServiceStorageErrorCode,
)
from wealth.collector_health_cli import (
    COLLECTOR_HEALTH_EXIT_CRITICAL,
    COLLECTOR_HEALTH_EXIT_OK,
    COLLECTOR_HEALTH_EXIT_UNKNOWN,
    COLLECTOR_HEALTH_EXIT_WARNING,
    run_collector_health_cli,
)
from wealth.domain.collector_service import (
    CollectorCycleStatus,
    CollectorServiceHeartbeat,
    CollectorServiceStatus,
)
from wealth.ports.collector_service import CollectorServiceHeartbeatWriteStatus

NOW = datetime(2026, 7, 24, 16, 0, tzinfo=UTC)
START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
COLLECTION_ID = UUID(int=500)


class FixedClock:
    """Return one deterministic health-evaluation instant."""

    def now(self) -> datetime:
        return NOW


def starting(*, observed_at: datetime, run_id: int = 1) -> CollectorServiceHeartbeat:
    """Build one pristine nonterminal run heartbeat."""

    return CollectorServiceHeartbeat(
        heartbeat_id=UUID(int=run_id * 10 + 1),
        run_id=UUID(int=run_id),
        collection_id=COLLECTION_ID,
        worker_id="worker-a",
        sequence=1,
        observed_at=observed_at,
        status=CollectorServiceStatus.STARTING,
        cycles_attempted=0,
        checkpoint_version=1,
        next_window_start=START,
    )


def paused(*, observed_at: datetime, run_id: int = 1) -> CollectorServiceHeartbeat:
    """Build one valid warning-level terminal heartbeat."""

    return CollectorServiceHeartbeat(
        heartbeat_id=UUID(int=run_id * 10 + 2),
        run_id=UUID(int=run_id),
        collection_id=COLLECTION_ID,
        worker_id="worker-a",
        sequence=2,
        observed_at=observed_at,
        status=CollectorServiceStatus.PAUSED,
        cycles_attempted=1,
        checkpoint_version=1,
        next_window_start=START,
        last_cycle_status=CollectorCycleStatus.PAUSED,
        reason_code="operator_requested",
    )


def database_with(
    path: Path,
    *heartbeats: CollectorServiceHeartbeat,
) -> Path:
    """Persist a valid lifecycle fixture and return its database path."""

    store = SQLiteCollectorServiceHeartbeatStore(path)
    for heartbeat in heartbeats:
        assert store.append(heartbeat).status is CollectorServiceHeartbeatWriteStatus.INSERTED
    return path


def run_command(
    database: Path,
    *,
    extra: tuple[str, ...] = (),
) -> tuple[int, dict[str, JsonValue], str]:
    """Execute the command and decode its standard output."""

    stdout = StringIO()
    stderr = StringIO()
    exit_code = run_collector_health_cli(
        (
            "--database",
            str(database),
            "--collection-id",
            str(COLLECTION_ID),
            *extra,
        ),
        stdout=stdout,
        stderr=stderr,
        clock=FixedClock(),
    )
    payload: dict[str, JsonValue] = json.loads(stdout.getvalue())
    return exit_code, payload, stderr.getvalue()


def test_fresh_run_prints_stable_ok_json_and_exit_zero(tmp_path: Path) -> None:
    database = database_with(
        tmp_path / "service.sqlite3",
        starting(observed_at=NOW - timedelta(seconds=5)),
    )

    exit_code, payload, error_output = run_command(database)

    assert exit_code == COLLECTOR_HEALTH_EXIT_OK
    assert error_output == ""
    assert payload["schema_version"] == "1.0"
    assert payload["command"] == "collector_health"
    assert payload["status"] == "ok"
    assert payload["alerts"] == []
    report = payload["report"]
    assert isinstance(report, dict)
    assert report["status"] == "healthy"
    assert report["collection_id"] == str(COLLECTION_ID)


def test_paused_run_returns_warning_with_explicit_alert(tmp_path: Path) -> None:
    database = database_with(
        tmp_path / "service.sqlite3",
        starting(observed_at=NOW - timedelta(seconds=2)),
        paused(observed_at=NOW - timedelta(seconds=1)),
    )

    exit_code, payload, _ = run_command(database)

    assert exit_code == COLLECTOR_HEALTH_EXIT_WARNING
    assert payload["status"] == "warning"
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    alert = alerts[0]
    assert isinstance(alert, dict)
    assert alert["alert_code"] == "collector_paused"
    assert alert["alert_severity"] == "warning"


def test_stale_run_returns_critical_exit_for_monitoring(tmp_path: Path) -> None:
    database = database_with(
        tmp_path / "service.sqlite3",
        starting(observed_at=NOW - timedelta(seconds=600)),
    )

    exit_code, payload, _ = run_command(database)

    assert exit_code == COLLECTOR_HEALTH_EXIT_CRITICAL
    assert payload["status"] == "critical"
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    alert = alerts[0]
    assert isinstance(alert, dict)
    assert alert["alert_code"] == "heartbeat_stale"
    assert alert["alert_severity"] == "critical"


def test_highest_selected_alert_severity_controls_exit_code(tmp_path: Path) -> None:
    database = database_with(
        tmp_path / "service.sqlite3",
        starting(observed_at=NOW - timedelta(seconds=802), run_id=1),
        paused(observed_at=NOW - timedelta(seconds=801), run_id=1),
        starting(observed_at=NOW - timedelta(seconds=700), run_id=2),
    )

    exit_code, payload, _ = run_command(
        database,
        extra=("--run-limit", "2"),
    )

    assert exit_code == COLLECTOR_HEALTH_EXIT_CRITICAL
    assert payload["status"] == "critical"
    alerts = payload["alerts"]
    assert isinstance(alerts, list)
    assert len(alerts) == 2


def test_existing_database_without_runs_is_unknown_not_healthy(tmp_path: Path) -> None:
    database = database_with(tmp_path / "service.sqlite3")

    exit_code, payload, error_output = run_command(database)

    assert exit_code == COLLECTOR_HEALTH_EXIT_UNKNOWN
    assert error_output == ""
    assert payload["status"] == "unknown"
    report = payload["report"]
    assert isinstance(report, dict)
    assert report["status"] == "not_started"


def test_missing_database_is_unknown_and_is_never_created(tmp_path: Path) -> None:
    database = tmp_path / "missing.sqlite3"
    stdout = StringIO()
    stderr = StringIO()

    exit_code = run_collector_health_cli(
        (
            "--database",
            str(database),
            "--collection-id",
            str(COLLECTION_ID),
        ),
        stdout=stdout,
        stderr=stderr,
        clock=FixedClock(),
    )
    error: dict[str, JsonValue] = json.loads(stderr.getvalue())

    assert exit_code == COLLECTOR_HEALTH_EXIT_UNKNOWN
    assert stdout.getvalue() == ""
    assert error["status"] == "unknown"
    assert error["error_code"] == "storage_invalid_path"
    assert database.exists() is False


def test_invalid_arguments_use_unknown_instead_of_critical_exit() -> None:
    stdout = StringIO()
    stderr = StringIO()

    exit_code = run_collector_health_cli(
        ("--database", "unused.sqlite3", "--collection-id", "not-a-uuid"),
        stdout=stdout,
        stderr=stderr,
        clock=FixedClock(),
    )
    error: dict[str, JsonValue] = json.loads(stderr.getvalue())

    assert exit_code == COLLECTOR_HEALTH_EXIT_UNKNOWN
    assert stdout.getvalue() == ""
    assert error["error_code"] == "invalid_arguments"


def test_read_only_store_rejects_writes_but_allows_health_queries(tmp_path: Path) -> None:
    record = starting(observed_at=NOW)
    database = database_with(tmp_path / "service.sqlite3", record)
    reader = SQLiteCollectorServiceHeartbeatStore(database, read_only=True)

    assert reader.current(record.run_id) == record
    with pytest.raises(SQLiteCollectorServiceStorageError) as failure:
        reader.append(record)

    assert failure.value.code is SQLiteCollectorServiceStorageErrorCode.READ_ONLY


def test_pretty_output_is_indented_but_remains_valid_json(tmp_path: Path) -> None:
    database = database_with(
        tmp_path / "service.sqlite3",
        starting(observed_at=NOW - timedelta(seconds=1)),
    )
    stdout = StringIO()

    exit_code = run_collector_health_cli(
        (
            "--database",
            str(database),
            "--collection-id",
            str(COLLECTION_ID),
            "--pretty",
        ),
        stdout=stdout,
        stderr=StringIO(),
        clock=FixedClock(),
    )

    assert exit_code == COLLECTOR_HEALTH_EXIT_OK
    assert stdout.getvalue().startswith("{\n  ")
    assert json.loads(stdout.getvalue())["status"] == "ok"
