"""Tests for restart-safe public-trade collection control contracts."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionHealthSummary,
    PublicTradeSourceHealthObservation,
    validate_public_trade_collection_transition,
)

START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
END = START + timedelta(minutes=10)
NOW = START + timedelta(days=1)
POLICY_FINGERPRINT = f"sha256:{'a' * 64}"
LEASE_TOKEN_A = UUID(int=101)
LEASE_TOKEN_B = UUID(int=102)


def checkpoint() -> PublicTradeCollectionCheckpoint:
    """Build one pristine bounded aggregate-trade checkpoint."""

    return PublicTradeCollectionCheckpoint(
        job_id=UUID(int=1),
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        policy_fingerprint=POLICY_FINGERPRINT,
        window_start=START,
        window_end_exclusive=END,
        next_window_start=START,
        status=CollectionJobStatus.PENDING,
        created_at=NOW,
        updated_at=NOW,
        version=1,
    )


def copy_checkpoint(
    current: PublicTradeCollectionCheckpoint,
    **updates: object,
) -> PublicTradeCollectionCheckpoint:
    """Create one strict validated successor candidate."""

    values = current.model_dump()
    values.update(updates)
    return PublicTradeCollectionCheckpoint.model_validate(values)


def running(
    current: PublicTradeCollectionCheckpoint,
    *,
    version: int | None = None,
    updated_at: datetime | None = None,
    lease_owner: str = "worker-a",
    lease_token: UUID = LEASE_TOKEN_A,
) -> PublicTradeCollectionCheckpoint:
    """Claim one checkpoint with a deterministic active lease."""

    transition_time = updated_at or current.updated_at + timedelta(seconds=1)
    return copy_checkpoint(
        current,
        status=CollectionJobStatus.RUNNING,
        updated_at=transition_time,
        version=current.version + 1 if version is None else version,
        lease_owner=lease_owner,
        lease_token=lease_token,
        lease_expires_at=transition_time + timedelta(minutes=5),
        last_failure_code=None,
        last_stop_reason=None,
    )


def test_partial_failure_preserves_exact_pending_leaf_for_restart() -> None:
    initial = checkpoint()
    claimed = running(initial)
    failed = copy_checkpoint(
        claimed,
        next_window_start=START + timedelta(minutes=2),
        pending_window_end_exclusive=START + timedelta(minutes=3),
        status=CollectionJobStatus.FAILED,
        updated_at=NOW + timedelta(seconds=2),
        version=3,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        windows_completed=1,
        records_completed=100,
        source_requests=5,
        window_traces=4,
        retry_attempts=1,
        splits_completed=2,
        last_failure_code="provider_unavailable",
        last_stop_reason="retry_limit",
    )
    resumed = running(
        failed,
        updated_at=NOW + timedelta(seconds=3),
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
    )

    validate_public_trade_collection_transition(initial, claimed)
    validate_public_trade_collection_transition(claimed, failed)
    validate_public_trade_collection_transition(failed, resumed)
    assert resumed.pending_window_end_exclusive == START + timedelta(minutes=3)


def test_rejected_health_records_partial_progress_and_pending_leaf() -> None:
    observation = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=2),
        job_id=UUID(int=1),
        checkpoint_version=3,
        source="binance.public-rest",
        venue="BINANCE",
        instrument="BTC-USDT",
        provider_symbol="BTCUSDT",
        instrument_type=InstrumentType.SPOT,
        range_start=START,
        range_end_exclusive=END,
        next_window_start=START + timedelta(minutes=2),
        pending_window_end_exclusive=START + timedelta(minutes=3),
        observed_at=NOW,
        status=SourceHealthStatus.UNAVAILABLE,
        accepted=False,
        source_requests=5,
        window_traces=4,
        windows_completed=1,
        records_completed=100,
        splits_completed=2,
        retry_delays_seconds=(0.5,),
        failure_code="provider_unavailable",
        stop_reason="retry_limit",
    )

    assert observation.next_window_start == START + timedelta(minutes=2)
    assert observation.pending_window_end_exclusive == START + timedelta(minutes=3)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {"pending_window_end_exclusive": START},
            "pending",
        ),
        (
            {"pending_window_end_exclusive": END + timedelta(milliseconds=1)},
            "pending",
        ),
        (
            {
                "status": CollectionJobStatus.RUNNING,
                "lease_owner": "worker-a",
            },
            "set together",
        ),
        (
            {
                "status": CollectionJobStatus.RUNNING,
                "lease_owner": "worker-a",
                "lease_token": LEASE_TOKEN_A,
                "lease_expires_at": NOW + timedelta(minutes=5),
                "records_completed": 1,
            },
            "records",
        ),
        (
            {
                "status": CollectionJobStatus.RUNNING,
                "lease_owner": "worker-a",
                "lease_token": LEASE_TOKEN_A,
                "lease_expires_at": NOW + timedelta(minutes=5),
                "windows_completed": 1,
                "window_traces": 1,
                "source_requests": 0,
            },
            "source requests",
        ),
    ],
)
def test_checkpoint_rejects_invalid_pending_lease_or_counter_state(
    updates: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        copy_checkpoint(checkpoint(), **updates)


def test_failed_checkpoint_requires_a_pending_leaf() -> None:
    with pytest.raises(ValidationError, match="pending"):
        copy_checkpoint(
            checkpoint(),
            status=CollectionJobStatus.FAILED,
            last_failure_code="provider_unavailable",
            last_stop_reason="retry_limit",
        )


def test_checkpoint_rejects_noncanonical_policy_fingerprint() -> None:
    with pytest.raises(ValidationError, match="policy_fingerprint"):
        copy_checkpoint(checkpoint(), policy_fingerprint="range-policy-v1")


def test_running_checkpoint_rejects_a_lease_longer_than_one_hour() -> None:
    with pytest.raises(ValidationError, match="one hour"):
        running_checkpoint = running(checkpoint())
        copy_checkpoint(
            running_checkpoint,
            lease_expires_at=running_checkpoint.updated_at + timedelta(hours=1, milliseconds=1),
        )


def test_accepted_health_forbids_pending_work() -> None:
    with pytest.raises(ValidationError, match="pending"):
        PublicTradeSourceHealthObservation(
            observation_id=UUID(int=2),
            job_id=UUID(int=1),
            checkpoint_version=2,
            source="binance.public-rest",
            venue="BINANCE",
            instrument="BTC-USDT",
            provider_symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            range_start=START,
            range_end_exclusive=END,
            next_window_start=END,
            pending_window_end_exclusive=END,
            observed_at=NOW,
            status=SourceHealthStatus.HEALTHY,
            accepted=True,
            source_requests=1,
            window_traces=1,
            windows_completed=1,
            records_completed=1,
            splits_completed=0,
        )


def test_transition_rejects_policy_drift_and_active_lease_takeover() -> None:
    initial = checkpoint()
    changed_policy = copy_checkpoint(
        running(initial),
        policy_fingerprint=f"sha256:{'b' * 64}",
    )

    with pytest.raises(ValueError, match="immutable"):
        validate_public_trade_collection_transition(initial, changed_policy)

    claimed = running(initial)
    takeover = copy_checkpoint(
        claimed,
        version=3,
        updated_at=claimed.updated_at + timedelta(seconds=1),
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
        lease_expires_at=claimed.updated_at + timedelta(minutes=6),
    )
    with pytest.raises(ValueError, match="lease"):
        validate_public_trade_collection_transition(claimed, takeover)


def test_transition_rejects_counter_regression() -> None:
    initial = checkpoint()
    progressed = copy_checkpoint(
        running(initial),
        next_window_start=START + timedelta(minutes=1),
        windows_completed=1,
        records_completed=10,
        source_requests=1,
        window_traces=1,
    )
    regressed = copy_checkpoint(
        progressed,
        version=3,
        updated_at=progressed.updated_at + timedelta(seconds=1),
        windows_completed=0,
        records_completed=0,
    )

    with pytest.raises(ValueError, match="counters"):
        validate_public_trade_collection_transition(progressed, regressed)


def test_transition_rejects_work_recorded_after_lease_expiry() -> None:
    initial = checkpoint()
    claimed = running(initial)
    assert claimed.lease_expires_at is not None
    transition_time = claimed.lease_expires_at + timedelta(milliseconds=1)
    stale_work = copy_checkpoint(
        claimed,
        next_window_start=START + timedelta(minutes=1),
        updated_at=transition_time,
        version=3,
        lease_expires_at=transition_time + timedelta(minutes=5),
        windows_completed=1,
        source_requests=1,
        window_traces=1,
    )

    with pytest.raises(ValueError, match="expired"):
        validate_public_trade_collection_transition(claimed, stale_work)


@pytest.mark.parametrize(
    "retry_delays",
    [
        (300.001,),
        (float("inf"),),
        (1.0,) * 1_024,
    ],
)
def test_retry_delay_evidence_rejects_oversized_or_infinite_aggregates(
    retry_delays: tuple[float, ...],
) -> None:
    with pytest.raises(ValidationError, match="retry"):
        PublicTradeSourceHealthObservation(
            observation_id=UUID(int=3),
            job_id=UUID(int=1),
            checkpoint_version=2,
            source="binance.public-rest",
            venue="BINANCE",
            instrument="BTC-USDT",
            provider_symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            range_start=START,
            range_end_exclusive=END,
            next_window_start=START,
            pending_window_end_exclusive=END,
            observed_at=NOW,
            status=SourceHealthStatus.DEGRADED,
            accepted=False,
            source_requests=1 + len(retry_delays),
            window_traces=1,
            windows_completed=0,
            records_completed=0,
            splits_completed=0,
            retry_delays_seconds=retry_delays,
            stop_reason="record_limit",
        )


def test_health_rejects_more_than_one_terminal_trace() -> None:
    with pytest.raises(ValidationError, match="at most one terminal"):
        PublicTradeSourceHealthObservation(
            observation_id=UUID(int=4),
            job_id=UUID(int=1),
            checkpoint_version=2,
            source="binance.public-rest",
            venue="BINANCE",
            instrument="BTC-USDT",
            provider_symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            range_start=START,
            range_end_exclusive=END,
            next_window_start=START,
            pending_window_end_exclusive=END,
            observed_at=NOW,
            status=SourceHealthStatus.HEALTHY,
            accepted=False,
            source_requests=2,
            window_traces=2,
            windows_completed=0,
            records_completed=0,
            splits_completed=0,
            stop_reason="record_limit",
        )


def test_failed_health_requires_exactly_one_terminal_trace() -> None:
    with pytest.raises(ValidationError, match="one terminal"):
        PublicTradeSourceHealthObservation(
            observation_id=UUID(int=5),
            job_id=UUID(int=1),
            checkpoint_version=2,
            source="binance.public-rest",
            venue="BINANCE",
            instrument="BTC-USDT",
            provider_symbol="BTCUSDT",
            instrument_type=InstrumentType.SPOT,
            range_start=START,
            range_end_exclusive=END,
            next_window_start=START + timedelta(minutes=1),
            pending_window_end_exclusive=END,
            observed_at=NOW,
            status=SourceHealthStatus.UNAVAILABLE,
            accepted=False,
            source_requests=1,
            window_traces=1,
            windows_completed=1,
            records_completed=1,
            splits_completed=0,
            failure_code="provider_unavailable",
            stop_reason="retry_limit",
        )


def test_health_summary_rejects_an_infinite_retry_delay_total() -> None:
    with pytest.raises(ValidationError, match="finite"):
        PublicTradeCollectionHealthSummary(
            job_id=UUID(int=1),
            observation_count=0,
            healthy_count=0,
            degraded_count=0,
            unavailable_count=0,
            accepted_count=0,
            total_source_requests=0,
            total_window_traces=0,
            total_retry_attempts=0,
            total_windows_completed=0,
            total_records_completed=0,
            total_splits_completed=0,
            total_retry_delay_seconds=float("inf"),
        )
