"""Integration tests for durable, interruptible local collector service runs."""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_collector_service import (
    SQLiteCollectorServiceHeartbeatStore,
    SQLiteCollectorServiceStorageError,
    SQLiteCollectorServiceStorageErrorCode,
)
from wealth.application.collector_service import ContinuousCollectorServiceRunner
from wealth.application.continuous_collection import (
    ContinuousCollectionCycleResult,
    ContinuousCollectionCycleStatus,
)
from wealth.domain.collector_service import (
    CollectorServiceHeartbeatQuery,
    CollectorServiceStatus,
)
from wealth.domain.continuous_collection import (
    ContinuousCollectionCheckpoint,
    ContinuousCollectionStatus,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.ports.foundation import Clock

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
COLLECTION_ID = UUID(int=500)


class FixedClock:
    """Keep lifecycle timing deterministic while allowing equal timestamps."""

    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value


class SequenceClock:
    """Return explicit lifecycle timestamps in call order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now(self) -> datetime:
        return next(self._values)


class SequentialIds:
    """Generate deterministic run and heartbeat identifiers."""

    def __init__(self) -> None:
        self.value = 1_000

    def new(self) -> UUID:
        self.value += 1
        return UUID(int=self.value)


class NeverShutdown:
    """Record interruptible waits without requesting shutdown."""

    def __init__(self) -> None:
        self.waits: list[float] = []

    def requested(self) -> bool:
        return False

    def wait(self, timeout_seconds: float) -> bool:
        self.waits.append(timeout_seconds)
        return False


class ShutdownDuringWait(NeverShutdown):
    """Request a stop while the runner is waiting for its next safe cycle."""

    def wait(self, timeout_seconds: float) -> bool:
        self.waits.append(timeout_seconds)
        return True


class AlreadyShutdown(NeverShutdown):
    """Expose a stop request before the first cycle begins."""

    def requested(self) -> bool:
        return True


class ScriptedCollector:
    """Return durable checkpoints and planned cycle outcomes."""

    def __init__(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        cycles: tuple[ContinuousCollectionCycleResult, ...],
    ) -> None:
        self.value = checkpoint
        self.cycles = list(cycles)
        self.checkpoint_calls: list[UUID] = []
        self.cycle_calls: list[UUID] = []

    def checkpoint(self, collection_id: UUID) -> ContinuousCollectionCheckpoint:
        self.checkpoint_calls.append(collection_id)
        return self.value

    def run_cycle(self, collection_id: UUID) -> ContinuousCollectionCycleResult:
        self.cycle_calls.append(collection_id)
        if not self.cycles:
            raise AssertionError("scripted collector exhausted")
        cycle = self.cycles.pop(0)
        self.value = cycle.checkpoint
        return cycle


def checkpoint(
    *,
    status: ContinuousCollectionStatus = ContinuousCollectionStatus.ACTIVE,
    version: int = 1,
    pause_reason: str | None = None,
) -> ContinuousCollectionCheckpoint:
    """Build a valid continuous collection cursor."""

    return ContinuousCollectionCheckpoint(
        collection_id=COLLECTION_ID,
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=START,
        next_window_start=START,
        status=status,
        created_at=NOW,
        updated_at=NOW,
        version=version,
        pause_reason=pause_reason,
    )


def cycle(
    status: ContinuousCollectionCycleStatus,
    *,
    current: ContinuousCollectionCheckpoint | None = None,
    wait_seconds: float = 0,
) -> ContinuousCollectionCycleResult:
    """Build a planned non-advancing cycle."""

    return ContinuousCollectionCycleResult(
        status=status,
        checkpoint=checkpoint() if current is None else current,
        wait_seconds=wait_seconds,
    )


def runner(
    *,
    collector: ScriptedCollector,
    store: SQLiteCollectorServiceHeartbeatStore,
    shutdown: NeverShutdown,
    ids: SequentialIds | None = None,
    clock: Clock | None = None,
    worker_id: str = "worker-a",
) -> ContinuousCollectorServiceRunner:
    """Compose the real runner and SQLite heartbeat boundary."""

    return ContinuousCollectorServiceRunner(
        collector=collector,
        heartbeat_store=store,
        clock=FixedClock() if clock is None else clock,
        id_generator=SequentialIds() if ids is None else ids,
        shutdown=shutdown,
        worker_id=worker_id,
    )


def test_shutdown_interrupts_idle_wait_and_persists_complete_history(tmp_path: Path) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    shutdown = ShutdownDuringWait()
    process = ScriptedCollector(
        checkpoint(),
        (cycle(ContinuousCollectionCycleStatus.CAUGHT_UP, wait_seconds=5),),
    )

    result = runner(collector=process, store=store, shutdown=shutdown).run(
        COLLECTION_ID,
        cycle_limit=5,
    )
    history = store.observations(
        CollectorServiceHeartbeatQuery(run_id=result.run_id),
    )

    assert result.heartbeat.status is CollectorServiceStatus.STOPPED
    assert result.cycles_attempted == 1
    assert shutdown.waits == [5]
    assert [item.status for item in history] == [
        CollectorServiceStatus.STARTING,
        CollectorServiceStatus.RUNNING,
        CollectorServiceStatus.STOPPED,
    ]
    assert store.current(result.run_id) == result.heartbeat


def test_cycle_limit_is_explicit_after_every_safe_cycle(tmp_path: Path) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    process = ScriptedCollector(
        checkpoint(),
        (
            cycle(ContinuousCollectionCycleStatus.CAUGHT_UP),
            cycle(ContinuousCollectionCycleStatus.WAITING),
        ),
    )

    result = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
    ).run(COLLECTION_ID, cycle_limit=2)
    history = store.observations(
        CollectorServiceHeartbeatQuery(run_id=result.run_id),
    )

    assert result.heartbeat.status is CollectorServiceStatus.CYCLE_LIMIT
    assert result.cycles_attempted == 2
    assert [item.status for item in history] == [
        CollectorServiceStatus.STARTING,
        CollectorServiceStatus.RUNNING,
        CollectorServiceStatus.RUNNING,
        CollectorServiceStatus.CYCLE_LIMIT,
    ]


def test_restart_creates_new_run_and_resumes_existing_collection_cursor(
    tmp_path: Path,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    process = ScriptedCollector(
        checkpoint(),
        (
            cycle(ContinuousCollectionCycleStatus.CAUGHT_UP),
            cycle(ContinuousCollectionCycleStatus.CAUGHT_UP),
        ),
    )
    ids = SequentialIds()

    first = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
        ids=ids,
    ).run(COLLECTION_ID, cycle_limit=1)
    second = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
        ids=ids,
    ).run(COLLECTION_ID, cycle_limit=1)

    assert first.run_id != second.run_id
    assert process.checkpoint_calls == [COLLECTION_ID, COLLECTION_ID]
    assert process.cycle_calls == [COLLECTION_ID, COLLECTION_ID]
    assert len(store.observations(CollectorServiceHeartbeatQuery(run_id=first.run_id))) == 3
    assert len(store.observations(CollectorServiceHeartbeatQuery(run_id=second.run_id))) == 3


@pytest.mark.parametrize(
    ("cycle_status", "checkpoint_value", "terminal_status", "reason_code"),
    [
        (
            ContinuousCollectionCycleStatus.PAUSED,
            checkpoint(
                status=ContinuousCollectionStatus.PAUSED,
                version=2,
                pause_reason="operator_requested",
            ),
            CollectorServiceStatus.PAUSED,
            "operator_requested",
        ),
        (
            ContinuousCollectionCycleStatus.ALREADY_RUNNING,
            checkpoint(),
            CollectorServiceStatus.FAILED,
            "already_running",
        ),
    ],
)
def test_pause_and_competing_worker_become_explicit_terminal_evidence(
    tmp_path: Path,
    cycle_status: ContinuousCollectionCycleStatus,
    checkpoint_value: ContinuousCollectionCheckpoint,
    terminal_status: CollectorServiceStatus,
    reason_code: str,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / f"{cycle_status.value}.sqlite3")
    process = ScriptedCollector(
        checkpoint(),
        (cycle(cycle_status, current=checkpoint_value),),
    )

    result = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
    ).run(COLLECTION_ID, cycle_limit=5)

    assert result.heartbeat.status is terminal_status
    assert result.heartbeat.reason_code == reason_code
    assert result.cycles_attempted == 1


def test_shutdown_before_first_cycle_persists_pristine_stop(tmp_path: Path) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    process = ScriptedCollector(checkpoint(), ())

    result = runner(
        collector=process,
        store=store,
        shutdown=AlreadyShutdown(),
    ).run(COLLECTION_ID, cycle_limit=5)

    assert result.heartbeat.status is CollectorServiceStatus.STOPPED
    assert result.cycles_attempted == 0
    assert process.cycle_calls == []


def test_invalid_initial_service_clock_fails_before_ids_or_downstream_calls(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    database = tmp_path / "service.sqlite3"
    store = SQLiteCollectorServiceHeartbeatStore(database)
    process = ScriptedCollector(checkpoint(), ())
    ids = SequentialIds()
    shutdown = NeverShutdown()

    with pytest.raises(ValueError):
        runner(
            collector=process,
            store=store,
            shutdown=shutdown,
            ids=ids,
            clock=FixedClock(invalid_clock_value),
        ).run(COLLECTION_ID, cycle_limit=1)

    assert ids.value == 1_000
    assert process.checkpoint_calls == []
    assert process.cycle_calls == []
    assert shutdown.waits == []
    with sqlite3.connect(database) as connection:
        heartbeat_count = connection.execute(
            "SELECT COUNT(*) FROM collector_service_heartbeats"
        ).fetchone()
    assert heartbeat_count == (0,)


def test_invalid_later_service_clock_stops_before_terminal_id_or_append(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    store = SQLiteCollectorServiceHeartbeatStore(tmp_path / "service.sqlite3")
    process = ScriptedCollector(checkpoint(), ())
    ids = SequentialIds()

    with pytest.raises(ValueError):
        runner(
            collector=process,
            store=store,
            shutdown=AlreadyShutdown(),
            ids=ids,
            clock=SequenceClock(NOW, invalid_clock_value),
        ).run(COLLECTION_ID, cycle_limit=1)

    run_id = UUID(int=1_001)
    history = store.observations(CollectorServiceHeartbeatQuery(run_id=run_id))
    assert ids.value == 1_002
    assert process.checkpoint_calls == [COLLECTION_ID]
    assert process.cycle_calls == []
    assert [item.status for item in history] == [CollectorServiceStatus.STARTING]


def test_tampered_current_projection_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "service.sqlite3"
    store = SQLiteCollectorServiceHeartbeatStore(database)
    process = ScriptedCollector(
        checkpoint(),
        (cycle(ContinuousCollectionCycleStatus.CAUGHT_UP),),
    )
    result = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
    ).run(COLLECTION_ID, cycle_limit=1)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE collector_service_runs SET sequence = 99 WHERE run_id = ?",
            (str(result.run_id),),
        )

    with pytest.raises(SQLiteCollectorServiceStorageError) as failure:
        store.current(result.run_id)

    assert failure.value.code is SQLiteCollectorServiceStorageErrorCode.CORRUPT_RECORD


def test_missing_history_observation_fails_closed(tmp_path: Path) -> None:
    database = tmp_path / "service.sqlite3"
    store = SQLiteCollectorServiceHeartbeatStore(database)
    process = ScriptedCollector(
        checkpoint(),
        (cycle(ContinuousCollectionCycleStatus.CAUGHT_UP),),
    )
    result = runner(
        collector=process,
        store=store,
        shutdown=NeverShutdown(),
    ).run(COLLECTION_ID, cycle_limit=1)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            DELETE FROM collector_service_heartbeats
            WHERE run_id = ? AND sequence = 1
            """,
            (str(result.run_id),),
        )

    with pytest.raises(SQLiteCollectorServiceStorageError) as failure:
        store.observations(CollectorServiceHeartbeatQuery(run_id=result.run_id))

    assert failure.value.code is SQLiteCollectorServiceStorageErrorCode.CORRUPT_RECORD
