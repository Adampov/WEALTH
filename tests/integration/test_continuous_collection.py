"""Integration tests for supervised polling, reconnect, and restart recovery."""

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_collection import SQLiteCollectionCheckpointStore
from wealth.adapters.sqlite_continuous_collection import (
    SQLiteContinuousCollectionCheckpointStore,
    SQLiteContinuousCollectionStorageError,
    SQLiteContinuousCollectionStorageErrorCode,
)
from wealth.adapters.sqlite_market import SQLiteCandleStore
from wealth.application.collection import RecoverableHistoricalCandleCollector
from wealth.application.continuous_collection import (
    ContinuousCollectionCycleStatus,
    ContinuousCollectionPolicy,
    SupervisedContinuousCandleCollector,
)
from wealth.application.pagination import (
    HistoricalCandlePaginationPolicy,
    HistoricalCandleRetryPolicy,
)
from wealth.domain.collection import CollectionJobStatus
from wealth.domain.continuous_collection import (
    ContinuousCollectionCheckpoint,
    ContinuousCollectionRequest,
    ContinuousCollectionStatus,
)
from wealth.domain.market import (
    CandleTimeframe,
    CanonicalCandle,
    InstrumentType,
    RawMarketPayload,
)
from wealth.domain.quality import CandleStream
from wealth.ports.continuous_collection import (
    ContinuousCollectionCheckpointStore,
    ContinuousCollectionWriteResult,
)
from wealth.ports.market import (
    CandleFetchBatch,
    HistoricalCandleRequest,
    HistoricalCandleSourceError,
)

START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
INITIAL_NOW = START + timedelta(minutes=3, seconds=5)


class MutableClock:
    """Expose deterministic time to collection and reconnect policies."""

    def __init__(self, now: datetime = INITIAL_NOW) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value

    def advance(self, duration: timedelta) -> None:
        self.value += duration


class SequentialIds:
    """Generate stable unique fixture identifiers."""

    def __init__(self) -> None:
        self.value = 0

    def new(self) -> UUID:
        self.value += 1
        return UUID(int=self.value)


class AdvancingSleeper:
    """Record waits and advance the deterministic clock."""

    def __init__(self, clock: MutableClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(timedelta(seconds=seconds))


class ScriptedCandleSource:
    """Return complete canonical pages or a planned transport disconnect."""

    def __init__(
        self,
        *,
        clock: MutableClock,
        ids: SequentialIds,
        outcomes: tuple[str, ...],
    ) -> None:
        self.clock = clock
        self.ids = ids
        self.outcomes = list(outcomes)
        self.calls: list[HistoricalCandleRequest] = []

    def fetch(self, request: HistoricalCandleRequest) -> CandleFetchBatch:
        self.calls.append(request)
        outcome = self.outcomes.pop(0) if self.outcomes else "success"
        if outcome == "disconnect":
            raise HistoricalCandleSourceError(
                "transport_failure",
                "planned fixture disconnect",
                retryable=True,
            )
        if outcome == "malformed":
            raise HistoricalCandleSourceError(
                "malformed_response",
                "planned non-reconnectable fixture failure",
                retryable=False,
            )
        if outcome == "rate_limited":
            raise HistoricalCandleSourceError(
                "rate_limited",
                "planned rate-limit fixture response",
                retryable=True,
                retry_after_seconds=1,
            )

        observed_at = self.clock.now()
        raw_id = self.ids.new()
        payload = (
            f"{request.provider_symbol}:{request.window_start.isoformat()}:"
            f"{request.window_end_exclusive.isoformat()}"
        ).encode()
        raw = RawMarketPayload(
            record_id=raw_id,
            source="binance.public-rest",
            venue="BINANCE",
            observed_at=observed_at,
            processed_at=observed_at,
            payload_sha256=sha256(payload).hexdigest(),
            payload=payload,
            lineage=(f"scripted-source:{raw_id}",),
        )
        records = tuple(
            self._candle(request, raw, index) for index in range(request.expected_count)
        )
        return CandleFetchBatch(
            request=request,
            source="binance.public-rest",
            venue="BINANCE",
            observed_at=observed_at,
            processed_at=observed_at,
            raw_payload=raw,
            records=records,
        )

    def _candle(
        self,
        request: HistoricalCandleRequest,
        raw: RawMarketPayload,
        index: int,
    ) -> CanonicalCandle:
        open_time = request.window_start + index * request.timeframe.duration
        return CanonicalCandle(
            record_id=self.ids.new(),
            source=raw.source,
            venue=raw.venue,
            instrument=request.instrument,
            instrument_type=request.instrument_type,
            timeframe=request.timeframe,
            open_time=open_time,
            close_time=open_time + request.timeframe.duration,
            observed_at=raw.observed_at,
            processed_at=raw.processed_at,
            open=Decimal("100"),
            high=Decimal("105"),
            low=Decimal("95"),
            close=Decimal("102"),
            base_volume=Decimal("10"),
            lineage=(raw.lineage_reference,),
        )


class CrashOnCursorAdvanceStore:
    """Simulate process loss after a bounded job commits but before cursor advance."""

    def __init__(self, delegate: ContinuousCollectionCheckpointStore) -> None:
        self.delegate = delegate
        self.fail_next_advance = True

    def create(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> ContinuousCollectionWriteResult:
        return self.delegate.create(checkpoint)

    def get(self, collection_id: UUID) -> ContinuousCollectionCheckpoint | None:
        return self.delegate.get(collection_id)

    def transition(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        *,
        expected_version: int,
    ) -> ContinuousCollectionWriteResult:
        previous = self.delegate.get(checkpoint.collection_id)
        if (
            previous is not None
            and checkpoint.next_window_start > previous.next_window_start
            and self.fail_next_advance
        ):
            self.fail_next_advance = False
            raise RuntimeError("simulated continuous cursor crash")
        return self.delegate.transition(checkpoint, expected_version=expected_version)


def collection_request() -> ContinuousCollectionRequest:
    """Build one continuously polled Binance Spot stream."""

    return ContinuousCollectionRequest(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=START,
    )


def stream() -> CandleStream:
    """Return the canonical stream queried from durable market storage."""

    return CandleStream(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
    )


def service(
    *,
    source: ScriptedCandleSource,
    clock: MutableClock,
    ids: SequentialIds,
    sleeper: AdvancingSleeper,
    market_store: SQLiteCandleStore,
    bounded_store: SQLiteCollectionCheckpointStore,
    continuous_store: ContinuousCollectionCheckpointStore,
    max_failures: int = 3,
) -> SupervisedContinuousCandleCollector:
    """Compose the real durable control and market-data paths."""

    bounded = RecoverableHistoricalCandleCollector(
        source=source,
        market_store=market_store,
        checkpoint_store=bounded_store,
        clock=clock,
        id_generator=ids,
        sleeper=sleeper,
        worker_id="worker-a",
        source_name="binance.public-rest",
        venue="BINANCE",
        pagination_policy=HistoricalCandlePaginationPolicy(
            page_size_candles=2,
            max_total_candles=10,
            inter_page_delay_seconds=0,
        ),
        retry_policy=HistoricalCandleRetryPolicy(
            max_attempts=1,
            base_delay_seconds=0,
            max_delay_seconds=0,
        ),
    )
    return SupervisedContinuousCandleCollector(
        bounded_collector=bounded,
        checkpoint_store=continuous_store,
        clock=clock,
        id_generator=ids,
        sleeper=sleeper,
        policy=ContinuousCollectionPolicy(
            max_candles_per_cycle=2,
            settlement_delay_seconds=5,
            idle_poll_seconds=1,
            reconnect_base_delay_seconds=1,
            reconnect_max_delay_seconds=2,
            max_consecutive_failures=max_failures,
        ),
    )


def test_planned_disconnect_reconnects_without_gap_or_duplicate(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = AdvancingSleeper(clock)
    source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("disconnect", "success", "success"),
    )
    market_store = SQLiteCandleStore(tmp_path / "market.sqlite3")
    continuous_store = SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3")
    collector = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=market_store,
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=continuous_store,
    )
    checkpoint = collector.create(collection_request())

    result = collector.run(checkpoint.collection_id, cycle_limit=3)

    assert [cycle.status for cycle in result.cycles] == [
        ContinuousCollectionCycleStatus.RETRY_SCHEDULED,
        ContinuousCollectionCycleStatus.ADVANCED,
        ContinuousCollectionCycleStatus.ADVANCED,
    ]
    assert sleeper.delays == [1]
    assert result.checkpoint.next_window_start == START + timedelta(minutes=3)
    assert result.checkpoint.cycles_completed == 2
    assert result.checkpoint.consecutive_failures == 0
    assert len(market_store.records_for_stream(stream())) == 3
    assert [(call.window_start, call.window_end_exclusive) for call in source.calls] == [
        (START, START + timedelta(minutes=2)),
        (START, START + timedelta(minutes=2)),
        (START + timedelta(minutes=2), START + timedelta(minutes=3)),
    ]


def test_restart_resumes_the_same_failed_job_and_advances_once(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = AdvancingSleeper(clock)
    market_store = SQLiteCandleStore(tmp_path / "market.sqlite3")
    bounded_store = SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3")
    continuous_path = tmp_path / "continuous.sqlite3"
    first_source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("disconnect",),
    )
    first = service(
        source=first_source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=market_store,
        bounded_store=bounded_store,
        continuous_store=SQLiteContinuousCollectionCheckpointStore(continuous_path),
    )
    checkpoint = first.create(collection_request())
    failed = first.run_cycle(checkpoint.collection_id)
    active_job_id = failed.checkpoint.active_job_id
    assert active_job_id is not None

    clock.advance(timedelta(seconds=1))
    restarted_source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("success",),
    )
    restarted = service(
        source=restarted_source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=SQLiteContinuousCollectionCheckpointStore(continuous_path),
    )
    recovered = restarted.run_cycle(checkpoint.collection_id)
    durable_job = bounded_store.get(active_job_id)

    assert recovered.status is ContinuousCollectionCycleStatus.ADVANCED
    assert recovered.checkpoint.next_window_start == START + timedelta(minutes=2)
    assert recovered.checkpoint.cycles_completed == 1
    assert recovered.checkpoint.active_job_id is None
    assert durable_job is not None
    assert durable_job.status is CollectionJobStatus.COMPLETED
    assert len(market_store.records_for_stream(stream())) == 2
    assert len(restarted_source.calls) == 1


def test_recorded_failure_with_missing_bounded_job_fails_closed(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    bounded_path = tmp_path / "bounded.sqlite3"
    source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("disconnect",),
    )
    collector = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=AdvancingSleeper(clock),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(bounded_path),
        continuous_store=SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3"),
    )
    checkpoint = collector.create(collection_request())
    collector.run_cycle(checkpoint.collection_id)
    with sqlite3.connect(bounded_path) as connection:
        connection.execute("DELETE FROM collection_jobs")
    clock.advance(timedelta(seconds=1))

    with pytest.raises(RuntimeError, match="references a missing bounded job"):
        collector.run_cycle(checkpoint.collection_id)

    assert len(source.calls) == 1


def test_cursor_crash_reuses_completed_job_without_refetch(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = AdvancingSleeper(clock)
    source = ScriptedCandleSource(clock=clock, ids=ids, outcomes=("success",))
    market_store = SQLiteCandleStore(tmp_path / "market.sqlite3")
    bounded_store = SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3")
    durable_continuous = SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3")
    crashing = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=market_store,
        bounded_store=bounded_store,
        continuous_store=CrashOnCursorAdvanceStore(durable_continuous),
    )
    checkpoint = crashing.create(collection_request())

    with pytest.raises(RuntimeError, match="simulated continuous cursor crash"):
        crashing.run_cycle(checkpoint.collection_id)

    after_crash = durable_continuous.get(checkpoint.collection_id)
    assert after_crash is not None
    assert after_crash.next_window_start == START
    assert after_crash.active_job_id is not None
    assert len(source.calls) == 1

    restarted = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=market_store,
        bounded_store=bounded_store,
        continuous_store=durable_continuous,
    )
    recovered = restarted.run_cycle(checkpoint.collection_id)

    assert recovered.status is ContinuousCollectionCycleStatus.ADVANCED
    assert recovered.checkpoint.next_window_start == START + timedelta(minutes=2)
    assert len(source.calls) == 1
    assert len(market_store.records_for_stream(stream())) == 2


def test_final_rate_limit_pauses_instead_of_guessing_a_reconnect_wait(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = AdvancingSleeper(clock)
    source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("rate_limited", "success"),
    )
    continuous_store = SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3")
    collector = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=continuous_store,
    )
    checkpoint = collector.create(collection_request())

    failed = collector.run_cycle(checkpoint.collection_id)
    blocked = collector.run_cycle(checkpoint.collection_id)
    calls_before_resume = len(source.calls)
    resumed = collector.resume(checkpoint.collection_id)
    recovered = collector.run_cycle(checkpoint.collection_id)

    assert failed.status is ContinuousCollectionCycleStatus.PAUSED
    assert failed.checkpoint.pause_reason == "non_reconnectable_failure"
    assert blocked.status is ContinuousCollectionCycleStatus.PAUSED
    assert calls_before_resume == 1
    assert resumed.status is ContinuousCollectionStatus.ACTIVE
    assert recovered.status is ContinuousCollectionCycleStatus.ADVANCED
    assert len(source.calls) == 2


def test_repeated_disconnects_reach_the_automatic_failure_limit(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    sleeper = AdvancingSleeper(clock)
    source = ScriptedCandleSource(
        clock=clock,
        ids=ids,
        outcomes=("disconnect", "disconnect"),
    )
    collector = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=sleeper,
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3"),
        max_failures=2,
    )
    checkpoint = collector.create(collection_request())

    result = collector.run(checkpoint.collection_id, cycle_limit=3)

    assert [cycle.status for cycle in result.cycles] == [
        ContinuousCollectionCycleStatus.RETRY_SCHEDULED,
        ContinuousCollectionCycleStatus.PAUSED,
    ]
    assert result.checkpoint.pause_reason == "failure_limit"
    assert result.checkpoint.consecutive_failures == 2
    assert result.checkpoint.next_window_start == START
    assert sleeper.delays == [1]


def test_operator_pause_blocks_network_until_explicit_resume(tmp_path: Path) -> None:
    clock = MutableClock()
    ids = SequentialIds()
    source = ScriptedCandleSource(clock=clock, ids=ids, outcomes=("success",))
    collector = service(
        source=source,
        clock=clock,
        ids=ids,
        sleeper=AdvancingSleeper(clock),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=SQLiteContinuousCollectionCheckpointStore(tmp_path / "continuous.sqlite3"),
    )
    checkpoint = collector.create(collection_request())

    paused = collector.pause(checkpoint.collection_id)
    blocked = collector.run_cycle(checkpoint.collection_id)
    calls_before_resume = len(source.calls)
    resumed = collector.resume(checkpoint.collection_id)
    advanced = collector.run_cycle(checkpoint.collection_id)

    assert paused.pause_reason == "operator_requested"
    assert blocked.status is ContinuousCollectionCycleStatus.PAUSED
    assert calls_before_resume == 0
    assert resumed.status is ContinuousCollectionStatus.ACTIVE
    assert advanced.status is ContinuousCollectionCycleStatus.ADVANCED
    assert len(source.calls) == 1


def test_continuous_checkpoint_rejects_tampered_index(tmp_path: Path) -> None:
    path = tmp_path / "continuous.sqlite3"
    clock = MutableClock()
    ids = SequentialIds()
    store = SQLiteContinuousCollectionCheckpointStore(path)
    collector = service(
        source=ScriptedCandleSource(clock=clock, ids=ids, outcomes=()),
        clock=clock,
        ids=ids,
        sleeper=AdvancingSleeper(clock),
        market_store=SQLiteCandleStore(tmp_path / "market.sqlite3"),
        bounded_store=SQLiteCollectionCheckpointStore(tmp_path / "bounded.sqlite3"),
        continuous_store=store,
    )
    checkpoint = collector.create(collection_request())
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE continuous_collection_checkpoints
            SET status = 'paused'
            WHERE collection_id = ?
            """,
            (str(checkpoint.collection_id),),
        )

    with pytest.raises(SQLiteContinuousCollectionStorageError) as error:
        store.get(checkpoint.collection_id)

    assert error.value.code is SQLiteContinuousCollectionStorageErrorCode.CORRUPT_RECORD
