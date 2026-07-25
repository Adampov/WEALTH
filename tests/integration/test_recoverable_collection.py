"""Integration tests for durable checkpoints, recovery, leases, and health."""

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.binance import BinancePublicCandleSource
from wealth.adapters.sqlite_collection import (
    SQLiteCollectionCheckpointStore,
    SQLiteCollectionStorageError,
    SQLiteCollectionStorageErrorCode,
)
from wealth.adapters.sqlite_market import SQLiteCandleStore
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.application.collection import (
    CollectionRunStatus,
    RecoverableHistoricalCandleCollector,
)
from wealth.application.pagination import (
    HistoricalCandlePaginationPolicy,
    HistoricalCandleRetryPolicy,
)
from wealth.application.rate_budget import RateBudgetedHistoricalCandleSource
from wealth.domain.collection import (
    CollectionHealthSummary,
    CollectionJobStatus,
    HistoricalCollectionJob,
    SourceHealthObservation,
    SourceHealthStatus,
)
from wealth.domain.market import CandleTimeframe, InstrumentType
from wealth.domain.quality import CandleStream
from wealth.domain.rate_budget import RateBudgetPolicy, RateBudgetRequest
from wealth.ports.collection import (
    CollectionCheckpointStore,
    CollectionCheckpointWriteResult,
    CollectionCheckpointWriteStatus,
)
from wealth.ports.foundation import Clock
from wealth.ports.http import HttpResponse
from wealth.ports.market import HistoricalCandleRequest, HistoricalCandleSource

WINDOW_START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
INITIAL_NOW = WINDOW_START + timedelta(days=1)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class MutableClock:
    """Expose deterministic time and explicit expiry advancement."""

    def __init__(self, now: datetime = INITIAL_NOW) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value

    def advance(self, duration: timedelta) -> None:
        self.value += duration


class SequenceClock:
    """Return explicit collection timestamps in call order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now(self) -> datetime:
        return next(self._values)


class SequentialIds:
    """Generate stable UUIDs without relying on random state."""

    def __init__(self) -> None:
        self.value = 0

    def new(self) -> UUID:
        self.value += 1
        return UUID(int=self.value)


class RecordingSleeper:
    """Record application pacing without slowing tests."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)


class AdvancingSleeper(RecordingSleeper):
    """Record waits and advance the deterministic wall clock."""

    def __init__(self, clock: MutableClock) -> None:
        super().__init__()
        self.clock = clock

    def sleep(self, seconds: float) -> None:
        super().sleep(seconds)
        self.clock.advance(timedelta(seconds=seconds))


class ScenarioHttpClient:
    """Return scripted responses or exact rows for the requested public page."""

    def __init__(self, *outcomes: HttpResponse | None) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, str]] = []

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, timeout_seconds
        captured = dict(query)
        self.calls.append(captured)
        if self._outcomes:
            outcome = self._outcomes.pop(0)
            if outcome is not None:
                return outcome
        start = UTC_EPOCH + timedelta(milliseconds=int(captured["startTime"]))
        rows = [
            kline(start + index * timedelta(minutes=1)) for index in range(int(captured["limit"]))
        ]
        return response(rows)


class CrashingCheckpointStore:
    """Simulate process loss after market data was stored but before checkpointing."""

    def __init__(self, delegate: CollectionCheckpointStore) -> None:
        self.delegate = delegate
        self.fail_next_page_transition = True

    def create(
        self,
        job: HistoricalCollectionJob,
    ) -> CollectionCheckpointWriteResult:
        return self.delegate.create(job)

    def get(self, job_id: UUID) -> HistoricalCollectionJob | None:
        return self.delegate.get(job_id)

    def transition(
        self,
        job: HistoricalCollectionJob,
        *,
        expected_version: int,
        health: SourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        if health is not None and self.fail_next_page_transition:
            self.fail_next_page_transition = False
            raise RuntimeError("simulated process crash")
        return self.delegate.transition(
            job,
            expected_version=expected_version,
            health=health,
        )

    def health_for_job(self, job_id: UUID) -> tuple[SourceHealthObservation, ...]:
        return self.delegate.health_for_job(job_id)

    def health_summary(self, job_id: UUID) -> CollectionHealthSummary:
        return self.delegate.health_summary(job_id)


def epoch_milliseconds(value: datetime) -> int:
    delta = value - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000


def kline(open_time: datetime) -> list[int | str]:
    open_time_ms = epoch_milliseconds(open_time)
    return [
        open_time_ms,
        "100",
        "105",
        "95",
        "102",
        "12.5",
        open_time_ms + 59_999,
        "1275",
        42,
        "6",
        "612",
        "0",
    ]


def response(
    rows: list[list[int | str]],
    *,
    status_code: int = 200,
) -> HttpResponse:
    return HttpResponse(
        status_code=status_code,
        headers=(),
        body=json.dumps(rows).encode(),
    )


def request(candle_count: int) -> HistoricalCandleRequest:
    return HistoricalCandleRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_START + candle_count * timedelta(minutes=1),
    )


def stream() -> CandleStream:
    return CandleStream(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def collector(
    *,
    http: ScenarioHttpClient,
    market_store: SQLiteCandleStore,
    checkpoint_store: CollectionCheckpointStore,
    clock: Clock,
    ids: SequentialIds,
    sleeper: RecordingSleeper,
    worker_id: str,
    market_source: HistoricalCandleSource | None = None,
) -> RecoverableHistoricalCandleCollector:
    return RecoverableHistoricalCandleCollector(
        source=(
            BinancePublicCandleSource(http=http, clock=clock)
            if market_source is None
            else market_source
        ),
        market_store=market_store,
        checkpoint_store=checkpoint_store,
        clock=clock,
        id_generator=ids,
        sleeper=sleeper,
        worker_id=worker_id,
        source_name="binance.public-rest",
        venue="BINANCE",
        pagination_policy=HistoricalCandlePaginationPolicy(
            page_size_candles=2,
            max_total_candles=10,
            inter_page_delay_seconds=0.25,
        ),
        retry_policy=HistoricalCandleRetryPolicy(
            max_attempts=3,
            base_delay_seconds=1,
            max_delay_seconds=10,
            max_retry_after_seconds=60,
        ),
    )


def test_invalid_initial_collection_clock_fails_before_id_or_storage(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    clock = MutableClock(invalid_clock_value)
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = RecordingSleeper()
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )

    with pytest.raises(ValueError):
        service.create_job(request(1))

    assert ids.value == 0
    assert state_store.get(UUID(int=1)) is None
    assert http.calls == []
    assert sleeper.delays == []


def test_invalid_claim_clock_stops_before_checkpoint_transition(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    clock = MutableClock(invalid_clock_value)
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = RecordingSleeper()
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )
    job_id = UUID(int=899)
    created = service.create_job(
        request(1),
        job_id=job_id,
        created_at=INITIAL_NOW,
    )

    with pytest.raises(ValueError):
        service.run(job_id)

    assert state_store.get(job_id) == created
    assert state_store.health_for_job(job_id) == ()
    assert ids.value == 0
    assert http.calls == []
    assert sleeper.delays == []


def test_invalid_post_ingestion_clock_stops_before_health_or_checkpoint_transition(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    clock = SequenceClock(
        INITIAL_NOW,
        INITIAL_NOW,
        INITIAL_NOW + timedelta(seconds=1),
        INITIAL_NOW + timedelta(seconds=2),
        invalid_clock_value,
    )
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = RecordingSleeper()
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )
    job_id = UUID(int=900)
    service.create_job(request(1), job_id=job_id, created_at=INITIAL_NOW)

    with pytest.raises(ValueError):
        service.run(job_id)

    durable = state_store.get(job_id)
    assert durable is not None
    assert durable.status is CollectionJobStatus.RUNNING
    assert durable.version == 2
    assert state_store.health_for_job(job_id) == ()
    assert ids.value == 0
    assert len(http.calls) == 1
    assert sleeper.delays == []


def test_invalid_rate_budget_clock_fails_before_reservation_id_or_provider(
    tmp_path: Path,
    invalid_clock_value: datetime,
) -> None:
    clock = MutableClock(invalid_clock_value)
    ids = SequentialIds()
    http = ScenarioHttpClient()
    configured = RateBudgetPolicy(
        budget_key="binance.public-rest.shared-ip",
        capacity=1,
        period_seconds=10,
    )
    coordinator = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    source = RateBudgetedHistoricalCandleSource(
        source=BinancePublicCandleSource(http=http, clock=clock),
        coordinator=coordinator,
        policy=configured,
        clock=clock,
        id_generator=ids,
    )

    with pytest.raises(ValueError):
        source.fetch(request(1))

    summary = coordinator.summary(configured.budget_key)
    assert ids.value == 0
    assert http.calls == []
    assert summary.reservation_count == 0


def test_completed_job_is_durable_observable_and_idempotent(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = RecordingSleeper()
    market_store = SQLiteCandleStore(tmp_path / "market.sqlite3")
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=market_store,
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )
    job = service.create_job(request(5))

    result = service.run(job.job_id)
    repeated = service.run(job.job_id)
    durable = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3").get(job.job_id)
    summary = state_store.health_summary(job.job_id)

    assert result.status is CollectionRunStatus.COMPLETED
    assert result.pages_attempted == 3
    assert repeated.status is CollectionRunStatus.COMPLETED
    assert repeated.pages_attempted == 0
    assert durable is not None
    assert durable.status is CollectionJobStatus.COMPLETED
    assert durable.pages_completed == 3
    assert durable.candles_completed == 5
    assert durable.total_attempts == 3
    assert len(http.calls) == 3
    assert sleeper.delays == [0.25, 0.25]
    assert len(SQLiteCandleStore(tmp_path / "market.sqlite3").records_for_stream(stream())) == 5
    assert summary.observation_count == 3
    assert summary.healthy_count == 3
    assert summary.accepted_count == 3


def test_compare_and_swap_lease_prevents_duplicate_worker(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    first_store = SQLiteCollectionCheckpointStore(path)
    second_store = SQLiteCollectionCheckpointStore(path)
    clock = MutableClock()
    ids = SequentialIds()
    service = collector(
        http=ScenarioHttpClient(),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=first_store,
        clock=clock,
        ids=ids,
        sleeper=RecordingSleeper(),
        worker_id="worker-a",
    )
    pending = service.create_job(request(2))
    values = pending.model_dump()
    values.update(
        status=CollectionJobStatus.RUNNING,
        updated_at=clock.now(),
        version=2,
        lease_owner="worker-a",
        lease_expires_at=clock.now() + timedelta(minutes=30),
    )
    first_claim = HistoricalCollectionJob.model_validate(values)
    values.update(lease_owner="worker-b")
    stale_second_claim = HistoricalCollectionJob.model_validate(values)

    accepted = first_store.transition(first_claim, expected_version=1)
    rejected = second_store.transition(stale_second_claim, expected_version=1)
    current = first_store.get(pending.job_id)
    assert current is not None
    takeover_values = current.model_dump()
    takeover_values.update(
        version=3,
        lease_owner="worker-b",
        lease_expires_at=clock.now() + timedelta(minutes=30),
    )
    unauthorized_takeover = HistoricalCollectionJob.model_validate(takeover_values)
    with pytest.raises(ValueError, match="active collection lease"):
        second_store.transition(unauthorized_takeover, expected_version=2)
    competing_service = collector(
        http=ScenarioHttpClient(),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=second_store,
        clock=clock,
        ids=ids,
        sleeper=RecordingSleeper(),
        worker_id="worker-b",
    )
    competing_run = competing_service.run(pending.job_id)

    assert accepted.status is CollectionCheckpointWriteStatus.UPDATED
    assert rejected.status is CollectionCheckpointWriteStatus.CONFLICT
    assert rejected.current_version == 2
    assert competing_run.status is CollectionRunStatus.ALREADY_RUNNING
    assert competing_run.pages_attempted == 0


def test_restart_after_checkpoint_crash_refetches_idempotently(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = RecordingSleeper()
    market_store = SQLiteCandleStore(tmp_path / "market.sqlite3")
    durable_state = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    crashing_state = CrashingCheckpointStore(durable_state)
    first_process = collector(
        http=http,
        market_store=market_store,
        checkpoint_store=crashing_state,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )
    job = first_process.create_job(request(5))

    with pytest.raises(RuntimeError, match="simulated process crash"):
        first_process.run(job.job_id)

    checkpoint_after_crash = durable_state.get(job.job_id)
    assert checkpoint_after_crash is not None
    assert checkpoint_after_crash.status is CollectionJobStatus.RUNNING
    assert checkpoint_after_crash.next_window_start == WINDOW_START
    assert len(market_store.records_for_stream(stream())) == 2

    clock.advance(timedelta(minutes=31))
    restarted = collector(
        http=http,
        market_store=market_store,
        checkpoint_store=durable_state,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-b",
    )
    recovered = restarted.run(job.job_id)

    assert recovered.status is CollectionRunStatus.COMPLETED
    assert recovered.checkpoint.pages_completed == 3
    assert recovered.checkpoint.candles_completed == 5
    assert len(market_store.records_for_stream(stream())) == 5
    assert market_store.conflicts_for_stream(stream()) == ()
    assert len(http.calls) == 4
    assert durable_state.health_summary(job.job_id).observation_count == 3


def test_provider_failure_preserves_progress_and_records_unavailability(
    tmp_path: Path,
) -> None:
    unavailable = response([], status_code=503)
    http = ScenarioHttpClient(None, unavailable, unavailable, unavailable)
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = RecordingSleeper()
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
    )
    job = service.create_job(request(4))

    result = service.run(job.job_id)
    observations = state_store.health_for_job(job.job_id)
    summary = state_store.health_summary(job.job_id)

    assert result.status is CollectionRunStatus.FAILED
    assert result.checkpoint.next_window_start == WINDOW_START + timedelta(minutes=2)
    assert result.checkpoint.pages_completed == 1
    assert result.checkpoint.total_attempts == 4
    assert result.checkpoint.last_failure_code == "provider_unavailable"
    assert [observation.status for observation in observations] == [
        SourceHealthStatus.HEALTHY,
        SourceHealthStatus.UNAVAILABLE,
    ]
    assert summary.observation_count == 2
    assert summary.accepted_count == 1
    assert summary.unavailable_count == 1
    assert summary.total_attempts == 4
    assert sleeper.delays == [0.25, 1, 2]


def test_checkpoint_read_rejects_tampered_sqlite_index(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    state_store = SQLiteCollectionCheckpointStore(path)
    service = collector(
        http=ScenarioHttpClient(),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=MutableClock(),
        ids=SequentialIds(),
        sleeper=RecordingSleeper(),
        worker_id="worker-a",
    )
    job = service.create_job(request(2))
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE collection_jobs SET status = 'completed' WHERE job_id = ?",
            (str(job.job_id),),
        )

    with pytest.raises(SQLiteCollectionStorageError) as error:
        state_store.get(job.job_id)

    assert error.value.code is SQLiteCollectionStorageErrorCode.CORRUPT_RECORD


def test_shared_budget_wait_is_retried_and_visible_in_collection_health(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    http = ScenarioHttpClient()
    sleeper = AdvancingSleeper(clock)
    budget_store = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    budget_policy = RateBudgetPolicy(
        budget_key="binance.public-rest.shared-ip",
        capacity=1,
        period_seconds=10,
    )
    budget_store.reserve(
        policy=budget_policy,
        request=RateBudgetRequest(
            reservation_id=UUID(int=999),
            budget_key=budget_policy.budget_key,
            requested_at=clock.now(),
            cost=1,
        ),
    )
    budgeted_source = RateBudgetedHistoricalCandleSource(
        source=BinancePublicCandleSource(http=http, clock=clock),
        coordinator=budget_store,
        policy=budget_policy,
        clock=clock,
        id_generator=ids,
    )
    state_store = SQLiteCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    service = collector(
        http=http,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        checkpoint_store=state_store,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        worker_id="worker-a",
        market_source=budgeted_source,
    )
    job = service.create_job(request(2))

    result = service.run(job.job_id)
    health = state_store.health_for_job(job.job_id)
    budget_summary = budget_store.summary(budget_policy.budget_key)

    assert result.status is CollectionRunStatus.COMPLETED
    assert result.checkpoint.total_attempts == 2
    assert sleeper.delays == [10]
    assert len(http.calls) == 1
    assert len(health) == 1
    assert health[0].status is SourceHealthStatus.DEGRADED
    assert health[0].attempts == 2
    assert health[0].retry_delays_seconds == (10.0,)
    assert budget_summary.reservation_count == 3
    assert budget_summary.granted_count == 2
    assert budget_summary.denied_count == 1
