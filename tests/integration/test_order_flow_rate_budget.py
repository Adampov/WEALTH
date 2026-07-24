"""Integration tests for shared weighted public-trade request budgets."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.binance_order_flow import (
    BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    BINANCE_USDM_AGG_TRADES_REQUEST_WEIGHT,
    BinancePublicAggregateTradeSource,
)
from wealth.adapters.order_flow import InMemoryOrderFlowStore
from wealth.adapters.sqlite_rate_budget import SQLiteRateBudgetCoordinator
from wealth.application.order_flow_range import (
    AdaptivePublicTradeRangeIngestor,
    PublicTradeRangePolicy,
    PublicTradeRetryPolicy,
)
from wealth.application.rate_budget import RateBudgetedPublicTradeSource
from wealth.domain.market import InstrumentType
from wealth.domain.rate_budget import RateBudgetPolicy, RateBudgetRequest
from wealth.ports.http import HttpResponse
from wealth.ports.order_flow import PublicTradeSourceError, PublicTradeWindowRequest

WINDOW_START = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
NOW = WINDOW_START + timedelta(hours=1)


class MutableClock:
    """Expose deterministic provider and coordinator time."""

    def __init__(self) -> None:
        self.current = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class SequentialIds:
    """Generate stable reservation identities."""

    def __init__(self) -> None:
        self._next = 1

    def new(self) -> UUID:
        value = UUID(int=self._next)
        self._next += 1
        return value


class AdvancingSleeper:
    """Record waits and advance the shared deterministic clock."""

    def __init__(self, clock: MutableClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)


class EmptyHttpClient:
    """Return one complete empty public response and count network access."""

    def __init__(self) -> None:
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
        return HttpResponse(status_code=200, headers=(), body=b"[]")


def trade_request() -> PublicTradeWindowRequest:
    """Return one already-closed Binance Spot request."""

    return PublicTradeWindowRequest(
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_START + timedelta(minutes=1),
    )


def policy(*, capacity: int, period_seconds: int) -> RateBudgetPolicy:
    """Return one budget shared by public Binance workloads."""

    return RateBudgetPolicy(
        budget_key="binance.public-rest.shared-ip",
        capacity=capacity,
        period_seconds=period_seconds,
    )


def reserve_other_market_data(
    coordinator: SQLiteRateBudgetCoordinator,
    configured: RateBudgetPolicy,
    *,
    cost: int,
) -> None:
    """Consume budget as if another market-data worker already ran."""

    coordinator.reserve(
        policy=configured,
        request=RateBudgetRequest(
            reservation_id=UUID(int=999),
            budget_key=configured.budget_key,
            requested_at=NOW,
            cost=cost,
        ),
    )


def test_weighted_trade_request_shares_budget_and_denial_prevents_network(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    http = EmptyHttpClient()
    coordinator = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    configured = policy(capacity=5, period_seconds=10)
    reserve_other_market_data(coordinator, configured, cost=1)
    source = RateBudgetedPublicTradeSource(
        source=BinancePublicAggregateTradeSource(http=http, clock=clock),
        coordinator=coordinator,
        policy=configured,
        clock=clock,
        id_generator=SequentialIds(),
        request_cost=BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    )

    accepted = source.fetch(trade_request())
    with pytest.raises(PublicTradeSourceError) as raised:
        source.fetch(trade_request())
    summary = coordinator.summary(configured.budget_key)

    assert accepted.records == ()
    assert raised.value.machine_code == "local_rate_budget_exhausted"
    assert raised.value.retryable is True
    assert raised.value.retry_after_seconds is not None
    assert len(http.calls) == 1
    assert summary.reservation_count == 3
    assert summary.granted_count == 2
    assert summary.denied_count == 1
    assert summary.total_requested_cost == 9


def test_adaptive_range_waits_for_local_budget_then_performs_one_network_request(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    sleeper = AdvancingSleeper(clock)
    http = EmptyHttpClient()
    coordinator = SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3")
    configured = policy(capacity=4, period_seconds=4)
    reserve_other_market_data(
        coordinator,
        configured,
        cost=BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    )
    source = RateBudgetedPublicTradeSource(
        source=BinancePublicAggregateTradeSource(http=http, clock=clock),
        coordinator=coordinator,
        policy=configured,
        clock=clock,
        id_generator=SequentialIds(),
        request_cost=BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT,
    )
    ingestor = AdaptivePublicTradeRangeIngestor(
        source=source,
        store=InMemoryOrderFlowStore(),
        sleeper=sleeper,
        range_policy=PublicTradeRangePolicy(
            initial_window_duration=timedelta(minutes=1),
            minimum_window_duration=timedelta(milliseconds=1),
            max_range_duration=timedelta(minutes=1),
            max_source_requests=3,
            max_records_per_run=100,
            inter_request_delay_seconds=0,
        ),
        retry_policy=PublicTradeRetryPolicy(
            max_attempts=2,
            base_delay_seconds=1,
            max_delay_seconds=10,
            max_retry_after_seconds=10,
        ),
    )

    result = ingestor.ingest(trade_request())
    summary = coordinator.summary(configured.budget_key)

    assert result.accepted is True
    assert result.source_request_count == 2
    assert result.traces[0].attempts == 2
    assert result.traces[0].retry_delays_seconds == (4.0,)
    assert sleeper.delays == [4.0]
    assert len(http.calls) == 1
    assert summary.reservation_count == 3
    assert summary.granted_count == 2
    assert summary.denied_count == 1


def test_documented_binance_request_weights_must_fit_explicit_capacity(
    tmp_path: Path,
) -> None:
    configured = policy(capacity=5, period_seconds=10)

    for invalid_cost in (True, BINANCE_USDM_AGG_TRADES_REQUEST_WEIGHT):
        with pytest.raises(ValueError, match="no greater than capacity"):
            RateBudgetedPublicTradeSource(
                source=BinancePublicAggregateTradeSource(
                    http=EmptyHttpClient(),
                    clock=MutableClock(),
                ),
                coordinator=SQLiteRateBudgetCoordinator(tmp_path / "rate-budget.sqlite3"),
                policy=configured,
                clock=MutableClock(),
                id_generator=SequentialIds(),
                request_cost=invalid_cost,
            )

    assert BINANCE_SPOT_AGG_TRADES_REQUEST_WEIGHT == 4
    assert BINANCE_USDM_AGG_TRADES_REQUEST_WEIGHT == 20
