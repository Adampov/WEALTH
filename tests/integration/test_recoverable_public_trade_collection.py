"""Integration and fault-injection tests for public-trade collection orchestration."""

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.binance_order_flow import (
    BINANCE_ORDER_FLOW_SOURCE,
    BINANCE_ORDER_FLOW_VENUE,
    BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    BinancePublicAggregateTradeSource,
)
from wealth.adapters.sqlite_order_flow import SQLiteOrderFlowStore
from wealth.adapters.sqlite_order_flow_collection import (
    SQLitePublicTradeCollectionCheckpointStore,
)
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.application.order_flow_range import (
    PublicTradeRangePolicy,
    PublicTradeRangeStopReason,
    PublicTradeRetryPolicy,
    PublicTradeRetryStopReason,
)
from wealth.application.public_trade_collection import (
    PublicTradeCollectionFailureCode,
    PublicTradeCollectionOrchestrator,
    PublicTradeCollectionPolicy,
    PublicTradeCollectionPolicyDriftError,
    PublicTradeCollectionRunStatus,
)
from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.market import InstrumentType, RawMarketPayload
from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionHealthSummary,
    PublicTradeSourceHealthObservation,
)
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowConflictRecord,
    OrderFlowRecord,
    OrderFlowRecordType,
    OrderFlowStream,
    OrderFlowWriteResult,
    ProviderSequencePolicy,
)
from wealth.domain.rate_budget import (
    RateBudgetDecision,
    RateBudgetPolicy,
    RateBudgetRequest,
    RateBudgetReservationResult,
    RateBudgetSummary,
)
from wealth.ports.collection import (
    CollectionCheckpointWriteResult,
    CollectionCheckpointWriteStatus,
)
from wealth.ports.foundation import Clock, IdGenerator, Sleeper
from wealth.ports.http import HttpResponse
from wealth.ports.order_flow import (
    OrderFlowFetchBatch,
    OrderFlowStore,
    PublicTradeWindowRequest,
    PublicTradeWindowSource,
)
from wealth.ports.order_flow_collection import PublicTradeCollectionCheckpointStore
from wealth.ports.rate_budget import RateBudgetCoordinator

START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
NOW = START + timedelta(days=1)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BUDGET_KEY = "binance.public-rest.shared-ip"
JOB_ID = UUID(int=10_000)
LEASE_DURATION = timedelta(seconds=5)


class MutableClock:
    """Expose deterministic trusted UTC time and explicit lease expiry."""

    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def now(self) -> datetime:
        return self.value

    def advance(self, duration: timedelta) -> None:
        self.value += duration


class SequentialIds:
    """Generate deterministic UUIDs without random state."""

    def __init__(self, start: int = 1) -> None:
        self._next = start

    def new(self) -> UUID:
        value = UUID(int=self._next)
        self._next += 1
        return value


class RecordingSleeper:
    """Record bounded waits without delaying integration tests."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)


class ScriptedHttpClient:
    """Return exact public responses while exposing every provider request."""

    def __init__(self, *responses: HttpResponse, events: list[str] | None = None) -> None:
        self._responses = list(responses)
        self.events = [] if events is None else events
        self.calls: list[dict[str, str]] = []

    def get(
        self,
        *,
        url: str,
        query: Mapping[str, str],
        timeout_seconds: float,
    ) -> HttpResponse:
        del url, timeout_seconds
        self.calls.append(dict(query))
        self.events.append("http")
        if not self._responses:
            raise AssertionError("unexpected provider request")
        return self._responses.pop(0)


class IdentityMismatchingSource:
    """Return a valid batch whose stream identity differs from configuration."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[PublicTradeWindowRequest] = []

    def fetch(self, request: PublicTradeWindowRequest) -> OrderFlowFetchBatch:
        self.calls.append(request)
        self.events.append("source")
        payload = b"[]"
        observed_at = NOW
        return OrderFlowFetchBatch(
            stream=OrderFlowStream(
                source="unexpected.public-rest",
                venue="OTHER",
                instrument=request.instrument,
                instrument_type=request.instrument_type,
                record_type=OrderFlowRecordType.TRADE,
                sequence_policy=ProviderSequencePolicy.MONOTONIC,
            ),
            observed_at=observed_at,
            processed_at=observed_at,
            raw_payload=RawMarketPayload(
                record_id=UUID(int=90_000),
                source="unexpected.public-rest",
                venue="OTHER",
                observed_at=observed_at,
                processed_at=observed_at,
                payload_sha256=sha256(payload).hexdigest(),
                payload=payload,
                lineage=("unexpected-public-rest:BTCUSDT",),
            ),
            records=(),
        )


class InstrumentedOrderFlowStore:
    """Delegate durable evidence writes with ordering and crash injection."""

    def __init__(
        self,
        delegate: OrderFlowStore,
        events: list[str],
        *,
        crash_before_next_batch: bool = False,
    ) -> None:
        self.delegate = delegate
        self.events = events
        self.crash_before_next_batch = crash_before_next_batch

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        return self.delegate.append(record)

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        if self.crash_before_next_batch:
            self.crash_before_next_batch = False
            raise RuntimeError("simulated crash before evidence commit")
        result = self.delegate.append_batch(batch)
        self.events.append("evidence")
        return result

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        return self.delegate.records_for_stream(stream)

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        return self.delegate.raw_payload(record_id)

    def raw_payload_ids_for_record(self, record_id: UUID) -> tuple[UUID, ...]:
        return self.delegate.raw_payload_ids_for_record(record_id)

    def conflicts_for_stream(
        self,
        stream: OrderFlowStream,
    ) -> tuple[OrderFlowConflictRecord, ...]:
        return self.delegate.conflicts_for_stream(stream)


class WrongFamilySQLiteOrderFlowStore(SQLiteOrderFlowStore):
    """Persist a batch but falsely identify its first canonical record family."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.appended_batches: list[OrderFlowFetchBatch] = []

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        self.appended_batches.append(batch)
        result = super().append_batch(batch)
        if not result.records:
            raise AssertionError("hostile store fixture requires a non-empty batch")
        wrong_first = result.records[0].model_copy(
            update={"record_type": OrderFlowRecordType.TICKER}
        )
        return result.model_copy(update={"records": (wrong_first, *result.records[1:])})


class InstrumentedCheckpointStore:
    """Delegate control writes with outcome-only crash and conflict seams."""

    def __init__(
        self,
        delegate: PublicTradeCollectionCheckpointStore,
        events: list[str],
        *,
        crash_before_next_outcome: bool = False,
        conflict_next_outcome: bool = False,
    ) -> None:
        self.delegate = delegate
        self.events = events
        self.crash_before_next_outcome = crash_before_next_outcome
        self.conflict_next_outcome = conflict_next_outcome

    def create(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> CollectionCheckpointWriteResult:
        return self.delegate.create(checkpoint)

    def get(self, job_id: UUID) -> PublicTradeCollectionCheckpoint | None:
        return self.delegate.get(job_id)

    def transition(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        expected_version: int,
        expected_lease_token: UUID | None = None,
        health: PublicTradeSourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        if health is not None and self.crash_before_next_outcome:
            self.crash_before_next_outcome = False
            raise RuntimeError("simulated crash before health transition")
        if health is not None and self.conflict_next_outcome:
            self.conflict_next_outcome = False
            current = self.delegate.get(checkpoint.job_id)
            return CollectionCheckpointWriteResult(
                status=CollectionCheckpointWriteStatus.CONFLICT,
                job_id=checkpoint.job_id,
                current_version=0 if current is None else current.version,
            )
        result = self.delegate.transition(
            checkpoint,
            expected_version=expected_version,
            expected_lease_token=expected_lease_token,
            health=health,
        )
        self.events.append("control:outcome" if health is not None else "control:claim")
        return result

    def health_for_job(
        self,
        job_id: UUID,
        *,
        after_checkpoint_version: int | None = None,
        limit: int = 100,
    ) -> tuple[PublicTradeSourceHealthObservation, ...]:
        return self.delegate.health_for_job(
            job_id,
            after_checkpoint_version=after_checkpoint_version,
            limit=limit,
        )

    def health_summary(self, job_id: UUID) -> PublicTradeCollectionHealthSummary:
        return self.delegate.health_summary(job_id)


class InstrumentedRateBudgetCoordinator:
    """Record successful durable reservations before source access."""

    def __init__(self, delegate: RateBudgetCoordinator, events: list[str]) -> None:
        self.delegate = delegate
        self.events = events

    def reserve(
        self,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
    ) -> RateBudgetReservationResult:
        result = self.delegate.reserve(policy=policy, request=request)
        self.events.append("budget")
        return result

    def decisions_for_budget(self, budget_key: str) -> tuple[RateBudgetDecision, ...]:
        return self.delegate.decisions_for_budget(budget_key)

    def summary(self, budget_key: str) -> RateBudgetSummary:
        return self.delegate.summary(budget_key)


def epoch_milliseconds(value: datetime) -> int:
    """Convert one UTC timestamp without floating-point arithmetic."""

    delta = value - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def aggregate_trade(event_time: datetime, aggregate_id: int) -> dict[str, object]:
    """Build one structurally valid Binance aggregate trade."""

    return {
        "a": aggregate_id,
        "p": "100",
        "q": "0.5",
        "f": aggregate_id * 10,
        "l": aggregate_id * 10,
        "T": epoch_milliseconds(event_time),
        "m": False,
        "M": True,
    }


def response(*rows: dict[str, object]) -> HttpResponse:
    """Encode one exact successful public provider response."""

    return HttpResponse(
        status_code=200,
        headers=(),
        body=json.dumps(rows).encode(),
    )


def request(duration_ms: int) -> PublicTradeWindowRequest:
    """Build one millisecond-aligned bounded Spot request."""

    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        window_start=START,
        window_end_exclusive=START + timedelta(milliseconds=duration_ms),
    )


def collection_policy(
    *,
    max_source_requests: int = 8,
    max_records_per_run: int = 100,
    retry_max_attempts: int = 1,
) -> PublicTradeCollectionPolicy:
    """Build one permissive local policy whose work bounds remain tiny."""

    return PublicTradeCollectionPolicy(
        range=PublicTradeRangePolicy(
            initial_window_duration=timedelta(milliseconds=1),
            minimum_window_duration=timedelta(milliseconds=1),
            max_range_duration=timedelta(milliseconds=10),
            max_source_requests=max_source_requests,
            max_records_per_run=max_records_per_run,
            inter_request_delay_seconds=0,
        ),
        retry=PublicTradeRetryPolicy(
            max_attempts=retry_max_attempts,
            base_delay_seconds=0,
            max_delay_seconds=0,
            max_retry_after_seconds=0,
        ),
        rate_budget=RateBudgetPolicy(
            budget_key=BUDGET_KEY,
            capacity=1_000,
            period_seconds=1,
        ),
        request_cost=BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    )


def orchestrator(
    *,
    http: ScriptedHttpClient,
    evidence_store: OrderFlowStore,
    checkpoint_store: PublicTradeCollectionCheckpointStore,
    rate_budget: RateBudgetCoordinator,
    clock: Clock,
    ids: IdGenerator,
    policy: PublicTradeCollectionPolicy,
    worker_id: str = "worker-a",
    sleeper: Sleeper | None = None,
    source: PublicTradeWindowSource | None = None,
) -> PublicTradeCollectionOrchestrator:
    """Compose the production orchestrator with deterministic test boundaries."""

    return PublicTradeCollectionOrchestrator(
        source=(
            BinancePublicAggregateTradeSource(http=http, clock=clock) if source is None else source
        ),
        evidence_store=evidence_store,
        checkpoint_store=checkpoint_store,
        rate_budget_coordinator=rate_budget,
        clock=clock,
        id_generator=ids,
        sleeper=RecordingSleeper() if sleeper is None else sleeper,
        worker_id=worker_id,
        source_name=BINANCE_ORDER_FLOW_SOURCE,
        venue=BINANCE_ORDER_FLOW_VENUE,
        policy=policy,
        lease_duration=LEASE_DURATION,
    )


def checkpoint_copy(
    checkpoint: PublicTradeCollectionCheckpoint,
    **updates: object,
) -> PublicTradeCollectionCheckpoint:
    """Create a strict checkpoint successor for deterministic lease setup."""

    values = checkpoint.model_dump()
    values.update(updates)
    return PublicTradeCollectionCheckpoint.model_validate(values)


def evidence_counts(path: Path) -> tuple[int, int, int]:
    """Return durable raw, canonical, and conflict row counts."""

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM raw_order_flow_payloads),
                (SELECT COUNT(*) FROM canonical_order_flow_records),
                (SELECT COUNT(*) FROM order_flow_conflicts)
            """
        ).fetchone()
    if row is None:
        raise AssertionError("SQLite count query must return one row")
    return int(row[0]), int(row[1]), int(row[2])


def assert_budget_precedes_every_http(events: list[str]) -> None:
    """Require one unmatched durable reservation before each source request."""

    available_reservations = 0
    for event in events:
        if event == "budget":
            available_reservations += 1
        elif event == "http":
            assert available_reservations > 0
            available_reservations -= 1
    assert available_reservations == 0


def assert_claimed_without_work(
    checkpoint: PublicTradeCollectionCheckpoint,
    *,
    version: int,
) -> None:
    """Assert that only lease authority, never progress, reached control state."""

    assert checkpoint.status is CollectionJobStatus.RUNNING
    assert checkpoint.version == version
    assert checkpoint.next_window_start == START
    assert checkpoint.pending_window_end_exclusive is None
    assert checkpoint.windows_completed == 0
    assert checkpoint.records_completed == 0
    assert checkpoint.source_requests == 0
    assert checkpoint.window_traces == 0
    assert checkpoint.retry_attempts == 0
    assert checkpoint.splits_completed == 0


def test_completion_orders_evidence_before_control_and_second_run_is_idempotent(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    control_path = tmp_path / "public-trade-control.sqlite3"
    budget_path = tmp_path / "rate-budget.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control_delegate = SQLitePublicTradeCollectionCheckpointStore(control_path)
    control = InstrumentedCheckpointStore(control_delegate, events)
    budget_delegate = SQLiteRateBudgetCoordinator(budget_path)
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
    )
    job = service.create_job(request(1), job_id=JOB_ID)
    events.clear()

    completed = service.run(job.job_id)
    repeated = service.run(job.job_id)
    durable = control_delegate.get(job.job_id)
    health = control_delegate.health_for_job(job.job_id)

    assert completed.status is PublicTradeCollectionRunStatus.COMPLETED
    assert completed.range_invocations == 1
    assert repeated.status is PublicTradeCollectionRunStatus.COMPLETED
    assert repeated.range_invocations == 0
    assert durable == completed.checkpoint
    assert durable is not None
    assert durable.version == 3
    assert durable.status is CollectionJobStatus.COMPLETED
    assert durable.next_window_start == request(1).window_end_exclusive
    assert durable.pending_window_end_exclusive is None
    assert durable.lease_token is None
    assert durable.windows_completed == 1
    assert durable.records_completed == 1
    assert durable.source_requests == 1
    assert durable.window_traces == 1
    assert durable.retry_attempts == 0
    assert durable.splits_completed == 0
    assert len(health) == 1
    assert health[0].checkpoint_version == 3
    assert health[0].accepted is True
    assert health[0].status is SourceHealthStatus.HEALTHY
    assert events == [
        "control:claim",
        "budget",
        "http",
        "evidence",
        "control:outcome",
    ]
    assert_budget_precedes_every_http(events)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert evidence_counts(evidence_path) == (1, 1, 0)


def test_request_limit_pauses_at_the_exact_unrequested_leaf(tmp_path: Path) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(max_source_requests=1),
    )
    job = service.create_job(request(2), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.PAUSED
    assert result.checkpoint.version == 3
    assert result.checkpoint.status is CollectionJobStatus.PAUSED
    assert result.checkpoint.next_window_start == START + timedelta(milliseconds=1)
    assert result.checkpoint.pending_window_end_exclusive == START + timedelta(milliseconds=2)
    assert result.checkpoint.last_failure_code is None
    assert result.checkpoint.last_stop_reason == (
        PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED.value
    )
    assert result.checkpoint.windows_completed == 1
    assert result.checkpoint.records_completed == 1
    assert result.checkpoint.source_requests == 1
    assert result.checkpoint.window_traces == 1
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.HEALTHY
    assert health[0].failure_code is None
    assert evidence_counts(evidence_path) == (1, 1, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert_budget_precedes_every_http(events)


def test_request_limit_during_provider_retry_is_a_typed_failure(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(
        HttpResponse(status_code=503, headers=(), body=b"untrusted"),
        events=events,
    )
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(
            max_source_requests=1,
            retry_max_attempts=2,
        ),
    )
    job = service.create_job(request(1), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.status is CollectionJobStatus.FAILED
    assert result.checkpoint.next_window_start == START
    assert result.checkpoint.pending_window_end_exclusive == (START + timedelta(milliseconds=1))
    assert result.checkpoint.last_failure_code == (
        PublicTradeCollectionFailureCode.PROVIDER_UNAVAILABLE.value
    )
    assert result.checkpoint.last_stop_reason == (
        PublicTradeRetryStopReason.REQUEST_LIMIT_REACHED.value
    )
    assert result.checkpoint.source_requests == 1
    assert result.checkpoint.window_traces == 1
    assert result.checkpoint.retry_attempts == 0
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.UNAVAILABLE
    assert health[0].failure_code == (PublicTradeCollectionFailureCode.PROVIDER_UNAVAILABLE.value)
    assert health[0].stop_reason == (PublicTradeRetryStopReason.REQUEST_LIMIT_REACHED.value)
    assert evidence_counts(evidence_path) == (0, 0, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert_budget_precedes_every_http(events)


def test_record_limit_pauses_without_admitting_the_terminal_batch(tmp_path: Path) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(
        response(aggregate_trade(START, 1)),
        response(aggregate_trade(START + timedelta(milliseconds=1), 2)),
        events=events,
    )
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(max_records_per_run=1),
    )
    job = service.create_job(request(2), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.PAUSED
    assert result.checkpoint.next_window_start == START + timedelta(milliseconds=1)
    assert result.checkpoint.pending_window_end_exclusive == START + timedelta(milliseconds=2)
    assert result.checkpoint.last_failure_code is None
    assert result.checkpoint.last_stop_reason == (
        PublicTradeRangeStopReason.RECORD_LIMIT_REACHED.value
    )
    assert result.checkpoint.windows_completed == 1
    assert result.checkpoint.records_completed == 1
    assert result.checkpoint.source_requests == 2
    assert result.checkpoint.window_traces == 2
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.HEALTHY
    assert health[0].source_requests == 2
    assert health[0].window_traces == 2
    assert evidence_counts(evidence_path) == (1, 1, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 2
    assert len(http.calls) == 2
    assert_budget_precedes_every_http(events)


def test_quality_rejection_fails_without_persisting_ambiguous_evidence(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    duplicate = aggregate_trade(START, 1)
    http = ScriptedHttpClient(response(duplicate, duplicate), events=events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
    )
    job = service.create_job(request(1), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.status is CollectionJobStatus.FAILED
    assert result.checkpoint.next_window_start == START
    assert result.checkpoint.pending_window_end_exclusive == (START + timedelta(milliseconds=1))
    assert result.checkpoint.last_failure_code == (
        PublicTradeCollectionFailureCode.QUALITY_REJECTED.value
    )
    assert result.checkpoint.last_stop_reason == (
        PublicTradeRangeStopReason.INGESTION_REJECTED.value
    )
    assert result.checkpoint.windows_completed == 0
    assert result.checkpoint.records_completed == 0
    assert result.checkpoint.source_requests == 1
    assert result.checkpoint.window_traces == 1
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.DEGRADED
    assert health[0].failure_code == PublicTradeCollectionFailureCode.QUALITY_REJECTED.value
    assert health[0].stop_reason == PublicTradeRangeStopReason.INGESTION_REJECTED.value
    assert evidence_counts(evidence_path) == (0, 0, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert "evidence" not in events
    assert_budget_precedes_every_http(events)


def test_malformed_persistence_evidence_never_advances_or_fetches_a_later_window(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    hostile_store = WrongFamilySQLiteOrderFlowStore(evidence_path)
    evidence = InstrumentedOrderFlowStore(hostile_store, events)
    control = InstrumentedCheckpointStore(
        SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3"),
        events,
    )
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(
        response(aggregate_trade(START, 1)),
        response(aggregate_trade(START + timedelta(milliseconds=1), 2)),
        events=events,
    )
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
    )
    job = service.create_job(request(2), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.status is CollectionJobStatus.FAILED
    assert result.checkpoint.version == 3
    assert result.checkpoint.next_window_start == START
    assert result.checkpoint.pending_window_end_exclusive == (START + timedelta(milliseconds=1))
    assert result.checkpoint.last_failure_code == (
        PublicTradeCollectionFailureCode.EVIDENCE_ADMISSION_REJECTED.value
    )
    assert result.checkpoint.last_stop_reason == (
        PublicTradeRangeStopReason.INGESTION_REJECTED.value
    )
    assert result.checkpoint.windows_completed == 0
    assert result.checkpoint.records_completed == 0
    assert result.checkpoint.source_requests == 1
    assert result.checkpoint.window_traces == 1
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.DEGRADED
    assert health[0].failure_code == (
        PublicTradeCollectionFailureCode.EVIDENCE_ADMISSION_REJECTED.value
    )
    assert health[0].stop_reason == PublicTradeRangeStopReason.INGESTION_REJECTED.value
    assert len(hostile_store.appended_batches) == 1
    assert len(http.calls) == 1
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert evidence_counts(evidence_path) == (1, 1, 0)
    assert events == [
        "control:claim",
        "budget",
        "http",
        "evidence",
        "control:outcome",
    ]
    assert_budget_precedes_every_http(events)


def test_returned_batch_identity_mismatch_is_canonical_and_never_persisted(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(events=events)
    mismatched_source = IdentityMismatchingSource(events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
        source=mismatched_source,
    )
    job = service.create_job(request(1), job_id=JOB_ID)

    result = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.FAILED
    assert result.checkpoint.status is CollectionJobStatus.FAILED
    assert result.checkpoint.next_window_start == START
    assert result.checkpoint.pending_window_end_exclusive == (START + timedelta(milliseconds=1))
    assert result.checkpoint.last_failure_code == (
        PublicTradeCollectionFailureCode.SOURCE_IDENTITY_MISMATCH.value
    )
    assert result.checkpoint.last_stop_reason == PublicTradeRetryStopReason.NON_RETRYABLE.value
    assert result.checkpoint.windows_completed == 0
    assert result.checkpoint.records_completed == 0
    assert result.checkpoint.source_requests == 1
    assert result.checkpoint.window_traces == 1
    assert len(health) == 1
    assert health[0].accepted is False
    assert health[0].status is SourceHealthStatus.DEGRADED
    assert health[0].failure_code == (
        PublicTradeCollectionFailureCode.SOURCE_IDENTITY_MISMATCH.value
    )
    assert health[0].stop_reason == PublicTradeRetryStopReason.NON_RETRYABLE.value
    assert evidence_counts(evidence_path) == (0, 0, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(mismatched_source.calls) == 1
    assert http.calls == []
    assert events == ["budget", "source"]


def test_policy_drift_fails_before_lease_budget_or_network(tmp_path: Path) -> None:
    events: list[str] = []
    evidence = InstrumentedOrderFlowStore(
        SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3"),
        events,
    )
    control = InstrumentedCheckpointStore(
        SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3"),
        events,
    )
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    clock = MutableClock()
    creator = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(),
        policy=collection_policy(max_records_per_run=100),
    )
    job = creator.create_job(request(1), job_id=JOB_ID)
    events.clear()
    changed = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(start=100),
        policy=collection_policy(max_records_per_run=99),
    )

    with pytest.raises(PublicTradeCollectionPolicyDriftError):
        changed.run(job.job_id)

    durable = control.get(job.job_id)
    assert durable == job
    assert durable is not None
    assert durable.status is CollectionJobStatus.PENDING
    assert durable.version == 1
    assert control.health_for_job(job.job_id) == ()
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 0
    assert http.calls == []
    assert events == []


def test_active_lease_blocks_a_competing_worker_before_work(tmp_path: Path) -> None:
    events: list[str] = []
    evidence = InstrumentedOrderFlowStore(
        SQLiteOrderFlowStore(tmp_path / "order-flow.sqlite3"),
        events,
    )
    control_delegate = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    control = InstrumentedCheckpointStore(control_delegate, events)
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    clock = MutableClock()
    configured_policy = collection_policy()
    creator = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(),
        policy=configured_policy,
    )
    pending = creator.create_job(request(1), job_id=JOB_ID)
    claimed = checkpoint_copy(
        pending,
        status=CollectionJobStatus.RUNNING,
        version=2,
        lease_owner="worker-a",
        lease_token=UUID(int=500),
        lease_expires_at=NOW + timedelta(minutes=1),
    )
    write = control_delegate.transition(claimed, expected_version=1)
    assert write.status is CollectionCheckpointWriteStatus.UPDATED
    events.clear()
    competitor = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(start=100),
        policy=configured_policy,
        worker_id="worker-b",
    )

    result = competitor.run(pending.job_id)

    assert result.status is PublicTradeCollectionRunStatus.ALREADY_RUNNING
    assert result.range_invocations == 0
    assert result.checkpoint == claimed
    assert control_delegate.get(pending.job_id) == claimed
    assert control_delegate.health_for_job(pending.job_id) == ()
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 0
    assert http.calls == []
    assert events == []


def test_outcome_compare_and_swap_conflict_does_not_retry_provider_work(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control_delegate = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    control = InstrumentedCheckpointStore(
        control_delegate,
        events,
        conflict_next_outcome=True,
    )
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
    )
    job = service.create_job(request(1), job_id=JOB_ID)

    result = service.run(job.job_id)
    durable = control_delegate.get(job.job_id)

    assert result.status is PublicTradeCollectionRunStatus.CHECKPOINT_CONFLICT
    assert result.range_invocations == 1
    assert durable is not None
    assert_claimed_without_work(durable, version=2)
    assert control_delegate.health_for_job(job.job_id) == ()
    assert evidence_counts(evidence_path) == (1, 1, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert events == ["control:claim", "budget", "http", "evidence"]
    assert_budget_precedes_every_http(events)


def test_resume_fetches_exact_pending_leaf_before_the_remaining_range(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(
        response(aggregate_trade(START, 1)),
        response(aggregate_trade(START + timedelta(milliseconds=1), 2)),
        response(aggregate_trade(START + timedelta(milliseconds=2), 3)),
        events=events,
    )
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(max_source_requests=1),
    )
    job = service.create_job(request(3), job_id=JOB_ID)

    paused = service.run(job.job_id)
    resumed = service.run(job.job_id)
    health = control.health_for_job(job.job_id)

    assert paused.status is PublicTradeCollectionRunStatus.PAUSED
    assert paused.checkpoint.version == 3
    assert paused.checkpoint.next_window_start == START + timedelta(milliseconds=1)
    assert paused.checkpoint.pending_window_end_exclusive == (START + timedelta(milliseconds=2))
    assert resumed.status is PublicTradeCollectionRunStatus.COMPLETED
    assert resumed.range_invocations == 2
    assert resumed.checkpoint.version == 6
    assert resumed.checkpoint.next_window_start == START + timedelta(milliseconds=3)
    assert resumed.checkpoint.pending_window_end_exclusive is None
    assert resumed.checkpoint.windows_completed == 3
    assert resumed.checkpoint.records_completed == 3
    assert resumed.checkpoint.source_requests == 3
    assert resumed.checkpoint.window_traces == 3
    assert [observation.checkpoint_version for observation in health] == [3, 5, 6]
    assert [call["startTime"] for call in http.calls] == [
        str(epoch_milliseconds(START)),
        str(epoch_milliseconds(START + timedelta(milliseconds=1))),
        str(epoch_milliseconds(START + timedelta(milliseconds=2))),
    ]
    assert [call["endTime"] for call in http.calls] == [
        str(epoch_milliseconds(START)),
        str(epoch_milliseconds(START + timedelta(milliseconds=1))),
        str(epoch_milliseconds(START + timedelta(milliseconds=2))),
    ]
    assert evidence_counts(evidence_path) == (3, 3, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 3
    assert len(http.calls) == 3
    assert_budget_precedes_every_http(events)


def test_crash_before_evidence_never_advances_control_state(tmp_path: Path) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(
        SQLiteOrderFlowStore(evidence_path),
        events,
        crash_before_next_batch=True,
    )
    control_delegate = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    control = InstrumentedCheckpointStore(control_delegate, events)
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    http = ScriptedHttpClient(response(aggregate_trade(START, 1)), events=events)
    service = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=MutableClock(),
        ids=SequentialIds(),
        policy=collection_policy(),
    )
    job = service.create_job(request(1), job_id=JOB_ID)

    with pytest.raises(RuntimeError, match="before evidence"):
        service.run(job.job_id)

    durable = control_delegate.get(job.job_id)
    assert durable is not None
    assert_claimed_without_work(durable, version=2)
    assert control_delegate.health_for_job(job.job_id) == ()
    assert evidence_counts(evidence_path) == (0, 0, 0)
    assert budget_delegate.summary(BUDGET_KEY).reservation_count == 1
    assert len(http.calls) == 1
    assert events == ["control:claim", "budget", "http"]
    assert_budget_precedes_every_http(events)


def test_crash_after_evidence_refetches_idempotently_after_expired_takeover(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    evidence_path = tmp_path / "order-flow.sqlite3"
    evidence = InstrumentedOrderFlowStore(SQLiteOrderFlowStore(evidence_path), events)
    control_delegate = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "control.sqlite3")
    control = InstrumentedCheckpointStore(
        control_delegate,
        events,
        crash_before_next_outcome=True,
    )
    budget_delegate = SQLiteRateBudgetCoordinator(tmp_path / "budget.sqlite3")
    budget = InstrumentedRateBudgetCoordinator(budget_delegate, events)
    repeated_response = response(aggregate_trade(START, 1))
    http = ScriptedHttpClient(repeated_response, repeated_response, events=events)
    clock = MutableClock()
    configured_policy = collection_policy()
    first_process = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(),
        policy=configured_policy,
        worker_id="worker-a",
    )
    job = first_process.create_job(request(1), job_id=JOB_ID)

    with pytest.raises(RuntimeError, match="before health"):
        first_process.run(job.job_id)

    after_crash = control_delegate.get(job.job_id)
    assert after_crash is not None
    assert_claimed_without_work(after_crash, version=2)
    assert control_delegate.health_for_job(job.job_id) == ()
    assert evidence_counts(evidence_path) == (1, 1, 0)
    clock.advance(LEASE_DURATION + timedelta(milliseconds=1))
    restarted = orchestrator(
        http=http,
        evidence_store=evidence,
        checkpoint_store=control,
        rate_budget=budget,
        clock=clock,
        ids=SequentialIds(start=100),
        policy=configured_policy,
        worker_id="worker-b",
    )

    recovered = restarted.run(job.job_id)
    durable = control_delegate.get(job.job_id)
    health = control_delegate.health_for_job(job.job_id)

    assert recovered.status is PublicTradeCollectionRunStatus.COMPLETED
    assert recovered.range_invocations == 1
    assert durable == recovered.checkpoint
    assert durable is not None
    assert durable.version == 4
    assert durable.status is CollectionJobStatus.COMPLETED
    assert durable.windows_completed == 1
    assert durable.records_completed == 1
    assert durable.source_requests == 1
    assert durable.window_traces == 1
    assert durable.retry_attempts == 0
    assert durable.splits_completed == 0
    assert len(health) == 1
    assert health[0].checkpoint_version == 4
    assert health[0].accepted is True
    assert evidence_counts(evidence_path) == (1, 1, 0)
    summary = budget_delegate.summary(BUDGET_KEY)
    assert summary.reservation_count == 2
    assert summary.granted_count == 2
    assert len(http.calls) == 2
    assert events.count("budget") == 2
    assert events.count("http") == 2
    assert events.count("evidence") == 2
    assert events.count("control:outcome") == 1
    assert_budget_precedes_every_http(events)
