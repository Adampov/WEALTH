"""Tests for continuous collection contracts and bounded timing policy."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.application.continuous_collection import ContinuousCollectionPolicy
from wealth.domain.continuous_collection import (
    ContinuousCollectionCheckpoint,
    ContinuousCollectionRequest,
    ContinuousCollectionStatus,
    validate_continuous_collection_transition,
)
from wealth.domain.market import CandleTimeframe, InstrumentType

START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
NOW = START + timedelta(hours=1)


def request(*, window_start: datetime = START) -> ContinuousCollectionRequest:
    """Build one Binance Spot stream request."""

    return ContinuousCollectionRequest(
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        timeframe=CandleTimeframe.ONE_MINUTE,
        window_start=window_start,
    )


def checkpoint() -> ContinuousCollectionCheckpoint:
    """Build one pristine durable cursor."""

    collection = request()
    return ContinuousCollectionCheckpoint(
        collection_id=UUID(int=1),
        source=collection.source,
        venue=collection.venue,
        instrument=collection.instrument,
        provider_symbol=collection.provider_symbol,
        instrument_type=collection.instrument_type,
        timeframe=collection.timeframe,
        window_start=collection.window_start,
        next_window_start=collection.window_start,
        status=ContinuousCollectionStatus.ACTIVE,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def copy_checkpoint(
    current: ContinuousCollectionCheckpoint,
    **updates: object,
) -> ContinuousCollectionCheckpoint:
    """Create one strict validated successor candidate."""

    values = current.model_dump()
    values.update(updates)
    return ContinuousCollectionCheckpoint.model_validate(values)


def test_request_and_checkpoint_reject_misalignment_or_incomplete_active_work() -> None:
    with pytest.raises(ValidationError, match="align"):
        request(window_start=START + timedelta(seconds=1))

    with pytest.raises(ValidationError, match="set together"):
        copy_checkpoint(
            checkpoint(),
            active_job_id=UUID(int=2),
        )


def test_active_job_attachment_and_exact_success_transition_are_valid() -> None:
    initial = checkpoint()
    attached = copy_checkpoint(
        initial,
        updated_at=NOW + timedelta(seconds=1),
        version=2,
        active_job_id=UUID(int=2),
        active_window_end_exclusive=START + timedelta(minutes=2),
    )
    completed = copy_checkpoint(
        attached,
        next_window_start=START + timedelta(minutes=2),
        updated_at=NOW + timedelta(seconds=2),
        version=3,
        active_job_id=None,
        active_window_end_exclusive=None,
        cycles_completed=1,
        candles_completed=2,
    )

    validate_continuous_collection_transition(initial, attached)
    validate_continuous_collection_transition(attached, completed)


def test_transition_cannot_skip_past_the_active_window() -> None:
    initial = checkpoint()
    attached = copy_checkpoint(
        initial,
        version=2,
        active_job_id=UUID(int=2),
        active_window_end_exclusive=START + timedelta(minutes=2),
    )
    skipped = copy_checkpoint(
        attached,
        next_window_start=START + timedelta(minutes=3),
        version=3,
        active_job_id=None,
        active_window_end_exclusive=None,
        cycles_completed=1,
        candles_completed=3,
    )

    with pytest.raises(ValueError, match="exact active window end"):
        validate_continuous_collection_transition(attached, skipped)


def test_policy_uses_only_settled_boundaries_and_bounded_reconnect_delays() -> None:
    policy = ContinuousCollectionPolicy(
        settlement_delay_seconds=5,
        reconnect_base_delay_seconds=2,
        reconnect_max_delay_seconds=5,
    )

    assert policy.latest_eligible_end(
        now=START + timedelta(minutes=3, seconds=4),
        timeframe=CandleTimeframe.ONE_MINUTE,
    ) == START + timedelta(minutes=2)
    assert policy.latest_eligible_end(
        now=START + timedelta(minutes=3, seconds=5),
        timeframe=CandleTimeframe.ONE_MINUTE,
    ) == START + timedelta(minutes=3)
    assert [policy.reconnect_delay(count) for count in (1, 2, 3, 4)] == [2, 4, 5, 5]
