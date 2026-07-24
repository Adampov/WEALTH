"""Unit tests for collector service lifecycle and shutdown contracts."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from wealth.adapters.foundation import ThreadingShutdownSignal
from wealth.domain.collector_service import (
    CollectorCycleStatus,
    CollectorServiceHeartbeat,
    CollectorServiceStatus,
    validate_collector_service_transition,
)

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)


def heartbeat(
    *,
    heartbeat_id: int,
    sequence: int,
    status: CollectorServiceStatus,
    cycles_attempted: int,
    checkpoint_version: int,
    next_window_start: datetime,
    last_cycle_status: CollectorCycleStatus | None = None,
    reason_code: str | None = None,
) -> CollectorServiceHeartbeat:
    """Build one strict lifecycle observation."""

    return CollectorServiceHeartbeat(
        heartbeat_id=UUID(int=heartbeat_id),
        run_id=UUID(int=100),
        collection_id=UUID(int=200),
        worker_id="worker-a",
        sequence=sequence,
        observed_at=NOW + timedelta(seconds=sequence),
        status=status,
        cycles_attempted=cycles_attempted,
        checkpoint_version=checkpoint_version,
        next_window_start=next_window_start,
        last_cycle_status=last_cycle_status,
        reason_code=reason_code,
    )


def test_start_run_and_interruptible_stop_form_a_valid_history() -> None:
    starting = heartbeat(
        heartbeat_id=1,
        sequence=1,
        status=CollectorServiceStatus.STARTING,
        cycles_attempted=0,
        checkpoint_version=1,
        next_window_start=START,
    )
    running = heartbeat(
        heartbeat_id=2,
        sequence=2,
        status=CollectorServiceStatus.RUNNING,
        cycles_attempted=1,
        checkpoint_version=2,
        next_window_start=START + timedelta(minutes=1),
        last_cycle_status=CollectorCycleStatus.ADVANCED,
    )
    stopped = heartbeat(
        heartbeat_id=3,
        sequence=3,
        status=CollectorServiceStatus.STOPPED,
        cycles_attempted=1,
        checkpoint_version=2,
        next_window_start=START + timedelta(minutes=1),
        last_cycle_status=CollectorCycleStatus.ADVANCED,
        reason_code="shutdown_requested",
    )

    validate_collector_service_transition(starting, running)
    validate_collector_service_transition(running, stopped)


def test_service_transition_rejects_sequence_and_cursor_regressions() -> None:
    starting = heartbeat(
        heartbeat_id=1,
        sequence=1,
        status=CollectorServiceStatus.STARTING,
        cycles_attempted=0,
        checkpoint_version=1,
        next_window_start=START,
    )
    skipped = heartbeat(
        heartbeat_id=2,
        sequence=3,
        status=CollectorServiceStatus.RUNNING,
        cycles_attempted=1,
        checkpoint_version=2,
        next_window_start=START + timedelta(minutes=1),
        last_cycle_status=CollectorCycleStatus.ADVANCED,
    )
    with pytest.raises(ValueError, match="sequence"):
        validate_collector_service_transition(starting, skipped)

    running = skipped.model_copy(update={"sequence": 2})
    regressed = heartbeat(
        heartbeat_id=3,
        sequence=3,
        status=CollectorServiceStatus.STOPPED,
        cycles_attempted=1,
        checkpoint_version=2,
        next_window_start=START,
        last_cycle_status=CollectorCycleStatus.ADVANCED,
        reason_code="shutdown_requested",
    )
    with pytest.raises(ValueError, match="cursor"):
        validate_collector_service_transition(running, regressed)


def test_threading_shutdown_signal_is_immediate_idempotent_and_validated() -> None:
    signal = ThreadingShutdownSignal()

    assert signal.requested() is False
    assert signal.wait(0) is False
    signal.request()
    signal.request()
    assert signal.requested() is True
    assert signal.wait(60) is True
    with pytest.raises(ValueError, match="finite"):
        ThreadingShutdownSignal().wait(float("nan"))
    with pytest.raises(ValueError, match="non-negative"):
        ThreadingShutdownSignal().wait(-1)
