"""Integration tests for adaptive public-trade range admission."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from wealth.adapters.binance_order_flow import BinancePublicAggregateTradeSource
from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.application.order_flow_range import (
    AdaptivePublicTradeRangeIngestor,
    PublicTradeRangePolicy,
    PublicTradeRangeStopReason,
    PublicTradeRetryPolicy,
    PublicTradeRetryStopReason,
    PublicTradeWindowOutcome,
)
from wealth.domain.market import InstrumentType
from wealth.ports.http import HttpResponse, HttpTransportError
from wealth.ports.order_flow import PublicTradeWindowRequest

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
REQUEST_TIME = WINDOW_START + timedelta(hours=1)
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class FixedClock:
    """Return one safe time after every test range."""

    def now(self) -> datetime:
        return REQUEST_TIME


class RecordingSleeper:
    """Record bounded delays without blocking tests."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)


class ScenarioHttpClient:
    """Return exact scripted public responses and capture their windows."""

    def __init__(self, *outcomes: HttpResponse | HttpTransportError) -> None:
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
        self.calls.append(dict(query))
        if not self._outcomes:
            raise AssertionError("unexpected provider request")
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, HttpTransportError):
            raise outcome
        return outcome


def epoch_milliseconds(value: datetime) -> int:
    """Convert one aware timestamp without floating-point arithmetic."""

    delta = value - UTC_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1_000 + delta.microseconds // 1_000


def trade(event_time: datetime, aggregate_id: int) -> dict[str, object]:
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


def response(
    rows: list[dict[str, object]],
    *,
    status_code: int = 200,
    headers: tuple[tuple[str, str], ...] = (),
) -> HttpResponse:
    """Encode one exact provider response."""

    return HttpResponse(
        status_code=status_code,
        headers=headers,
        body=json.dumps(rows).encode(),
    )


def cap_response() -> HttpResponse:
    """Return the provider row cap without claiming unique market content."""

    return response([trade(WINDOW_START, 1)] * 1_000)


def request(duration_ms: int) -> PublicTradeWindowRequest:
    """Build one canonical Spot event-time range."""

    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_START + timedelta(milliseconds=duration_ms),
    )


def range_ingestor(
    *,
    http: ScenarioHttpClient,
    store: InMemoryOrderFlowStore,
    sleeper: RecordingSleeper,
    initial_window_ms: int,
    minimum_window_ms: int = 1,
    max_source_requests: int = 20,
    max_records: int = 100,
    max_retry_after_seconds: int = 60,
) -> AdaptivePublicTradeRangeIngestor:
    """Build the real adapter and deterministic bounded orchestration."""

    return AdaptivePublicTradeRangeIngestor(
        source=BinancePublicAggregateTradeSource(http=http, clock=FixedClock()),
        store=store,
        sleeper=sleeper,
        range_policy=PublicTradeRangePolicy(
            initial_window_duration=timedelta(milliseconds=initial_window_ms),
            minimum_window_duration=timedelta(milliseconds=minimum_window_ms),
            max_range_duration=timedelta(seconds=1),
            max_source_requests=max_source_requests,
            max_records_per_run=max_records,
            inter_request_delay_seconds=0.25,
        ),
        retry_policy=PublicTradeRetryPolicy(
            max_attempts=3,
            base_delay_seconds=1,
            max_delay_seconds=10,
            max_retry_after_seconds=max_retry_after_seconds,
        ),
    )


def test_range_is_planned_chronologically_and_empty_windows_remain_evidence() -> None:
    http = ScenarioHttpClient(response([]), response([]), response([]))
    store = InMemoryOrderFlowStore()
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=http,
        store=store,
        sleeper=sleeper,
        initial_window_ms=2,
    ).ingest(request(5))

    assert result.accepted is True
    assert result.stop_reason is PublicTradeRangeStopReason.COMPLETED
    assert result.source_request_count == 3
    assert result.ingested_record_count == 0
    assert result.next_window_start == WINDOW_START + timedelta(milliseconds=5)
    assert [trace.request.duration for trace in result.traces] == [
        timedelta(milliseconds=2),
        timedelta(milliseconds=2),
        timedelta(milliseconds=1),
    ]
    assert [trace.outcome for trace in result.traces] == [
        PublicTradeWindowOutcome.INGESTED,
        PublicTradeWindowOutcome.INGESTED,
        PublicTradeWindowOutcome.INGESTED,
    ]
    assert sleeper.delays == [0.25, 0.25]
    assert all(
        trace.ingestion is not None
        and store.raw_payload(trace.ingestion.batch.raw_payload.record_id)
        == trace.ingestion.batch.raw_payload
        for trace in result.traces
    )


def test_dense_window_splits_until_both_halves_are_complete() -> None:
    http = ScenarioHttpClient(
        cap_response(),
        response([trade(WINDOW_START, 10)]),
        response([trade(WINDOW_START + timedelta(milliseconds=2), 11)]),
    )
    store = InMemoryOrderFlowStore()
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=http,
        store=store,
        sleeper=sleeper,
        initial_window_ms=4,
    ).ingest(request(4))

    assert result.accepted is True
    assert result.source_request_count == 3
    assert result.ingested_record_count == 2
    assert [trace.outcome for trace in result.traces] == [
        PublicTradeWindowOutcome.SPLIT,
        PublicTradeWindowOutcome.INGESTED,
        PublicTradeWindowOutcome.INGESTED,
    ]
    split = result.traces[0]
    assert split.source_failure is not None
    assert split.source_failure.machine_code == "possibly_truncated"
    assert split.split_children is not None
    assert [child.duration for child in split.split_children] == [
        timedelta(milliseconds=2),
        timedelta(milliseconds=2),
    ]
    assert [call["startTime"] for call in http.calls] == [
        str(epoch_milliseconds(WINDOW_START)),
        str(epoch_milliseconds(WINDOW_START)),
        str(epoch_milliseconds(WINDOW_START + timedelta(milliseconds=2))),
    ]
    assert sleeper.delays == [0.25, 0.25]


def test_dense_left_half_is_split_recursively_before_right_half() -> None:
    http = ScenarioHttpClient(
        cap_response(),
        cap_response(),
        response([trade(WINDOW_START, 20)]),
        response([trade(WINDOW_START + timedelta(milliseconds=1), 21)]),
        response([trade(WINDOW_START + timedelta(milliseconds=2), 22)]),
    )

    result = range_ingestor(
        http=http,
        store=InMemoryOrderFlowStore(),
        sleeper=RecordingSleeper(),
        initial_window_ms=4,
    ).ingest(request(4))

    assert result.accepted is True
    assert result.source_request_count == 5
    assert [trace.outcome for trace in result.traces] == [
        PublicTradeWindowOutcome.SPLIT,
        PublicTradeWindowOutcome.SPLIT,
        PublicTradeWindowOutcome.INGESTED,
        PublicTradeWindowOutcome.INGESTED,
        PublicTradeWindowOutcome.INGESTED,
    ]
    assert [
        trace.request.window_start
        for trace in result.traces
        if trace.outcome is PublicTradeWindowOutcome.INGESTED
    ] == [
        WINDOW_START,
        WINDOW_START + timedelta(milliseconds=1),
        WINDOW_START + timedelta(milliseconds=2),
    ]


def test_dense_minimum_window_stops_without_accepting_partial_data() -> None:
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=ScenarioHttpClient(cap_response()),
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        initial_window_ms=1,
    ).ingest(request(1))

    assert result.accepted is False
    assert result.stop_reason is PublicTradeRangeStopReason.MINIMUM_WINDOW_REACHED
    assert result.pending_window == request(1)
    assert result.next_window_start == WINDOW_START
    assert result.source_request_count == 1
    assert result.traces[0].outcome is PublicTradeWindowOutcome.DENSITY_LIMIT
    assert sleeper.delays == []


def test_request_limit_stops_at_exact_resume_boundary_without_extra_sleep() -> None:
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=ScenarioHttpClient(response([])),
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        initial_window_ms=2,
        max_source_requests=1,
    ).ingest(request(4))

    assert result.stop_reason is PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED
    assert result.source_request_count == 1
    assert result.next_window_start == WINDOW_START + timedelta(milliseconds=2)
    assert result.pending_window is not None
    assert result.pending_window.window_start == result.next_window_start
    assert sleeper.delays == []


def test_request_limit_blocks_a_retry_before_sleeping_or_exceeding_budget() -> None:
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=ScenarioHttpClient(HttpResponse(status_code=503, headers=(), body=b"untrusted")),
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        initial_window_ms=1,
        max_source_requests=1,
    ).ingest(request(1))

    assert result.stop_reason is PublicTradeRangeStopReason.REQUEST_LIMIT_REACHED
    assert result.source_request_count == 1
    failure = result.traces[0].source_failure
    assert failure is not None
    assert failure.retry_stop_reason is PublicTradeRetryStopReason.REQUEST_LIMIT_REACHED
    assert sleeper.delays == []


def test_record_limit_preserves_fetched_evidence_but_does_not_store_second_window() -> None:
    first_response = response([trade(WINDOW_START, 30)])
    second_response = response([trade(WINDOW_START + timedelta(milliseconds=1), 31)])
    store = InMemoryOrderFlowStore()

    result = range_ingestor(
        http=ScenarioHttpClient(first_response, second_response),
        store=store,
        sleeper=RecordingSleeper(),
        initial_window_ms=1,
        max_records=1,
    ).ingest(request(2))

    assert result.stop_reason is PublicTradeRangeStopReason.RECORD_LIMIT_REACHED
    assert result.ingested_record_count == 1
    assert result.next_window_start == WINDOW_START + timedelta(milliseconds=1)
    limit_trace = result.traces[-1]
    assert limit_trace.outcome is PublicTradeWindowOutcome.RECORD_LIMIT
    assert limit_trace.fetched_batch is not None
    assert store.raw_payload(limit_trace.fetched_batch.raw_payload.record_id) is None
    first_ingestion = result.traces[0].ingestion
    assert first_ingestion is not None
    assert len(store.records_for_stream(first_ingestion.batch.stream)) == 1


def test_storage_conflict_stops_before_later_windows_at_safe_resume_boundary() -> None:
    store = InMemoryOrderFlowStore()
    http = ScenarioHttpClient(
        response([trade(WINDOW_START, 35)]),
        response([trade(WINDOW_START + timedelta(milliseconds=1), 35)]),
        response([trade(WINDOW_START + timedelta(milliseconds=2), 36)]),
    )

    result = range_ingestor(
        http=http,
        store=store,
        sleeper=RecordingSleeper(),
        initial_window_ms=1,
    ).ingest(request(3))

    assert result.stop_reason is PublicTradeRangeStopReason.INGESTION_REJECTED
    assert result.next_window_start == WINDOW_START + timedelta(milliseconds=1)
    assert result.traces[-1].outcome is PublicTradeWindowOutcome.INGESTION_REJECTED
    assert len(http.calls) == 2
    first_ingestion = result.traces[0].ingestion
    assert first_ingestion is not None
    assert len(store.records_for_stream(first_ingestion.batch.stream)) == 1
    assert len(store.conflicts_for_stream(first_ingestion.batch.stream)) == 1


def test_transient_failure_retries_with_bounded_exponential_delay() -> None:
    sleeper = RecordingSleeper()
    http = ScenarioHttpClient(
        HttpResponse(status_code=503, headers=(), body=b"untrusted"),
        response([trade(WINDOW_START, 40)]),
    )

    result = range_ingestor(
        http=http,
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        initial_window_ms=1,
    ).ingest(request(1))

    assert result.accepted is True
    assert result.source_request_count == 2
    assert result.traces[0].attempts == 2
    assert result.traces[0].retry_delays_seconds == (1.0,)
    assert sleeper.delays == [1.0]


def test_excessive_retry_after_stops_without_sleep_or_storage() -> None:
    sleeper = RecordingSleeper()

    result = range_ingestor(
        http=ScenarioHttpClient(
            HttpResponse(
                status_code=429,
                headers=(("Retry-After", "61"),),
                body=b"untrusted",
            )
        ),
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        initial_window_ms=1,
        max_retry_after_seconds=60,
    ).ingest(request(1))

    assert result.stop_reason is PublicTradeRangeStopReason.SOURCE_FAILURE
    failure = result.traces[0].source_failure
    assert failure is not None
    assert failure.machine_code == "rate_limited"
    assert failure.retry_stop_reason is PublicTradeRetryStopReason.RETRY_AFTER_EXCEEDS_POLICY
    assert sleeper.delays == []


def test_range_above_policy_fails_before_network_access() -> None:
    http = ScenarioHttpClient()
    ingestor = range_ingestor(
        http=http,
        store=InMemoryOrderFlowStore(),
        sleeper=RecordingSleeper(),
        initial_window_ms=1,
    )

    with pytest.raises(ValueError, match="exceeds configured maximum"):
        ingestor.ingest(request(1_001))

    assert http.calls == []
