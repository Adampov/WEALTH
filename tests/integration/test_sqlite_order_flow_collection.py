"""Integration tests for durable public-trade checkpoints and health evidence."""

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from wealth.adapters.sqlite_order_flow_collection import (
    SQLitePublicTradeCollectionCheckpointStore,
    SQLitePublicTradeCollectionStorageError,
    SQLitePublicTradeCollectionStorageErrorCode,
)
from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeSourceHealthObservation,
)
from wealth.ports.collection import CollectionCheckpointWriteStatus

START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
END = START + timedelta(minutes=10)
NOW = START + timedelta(days=1)
POLICY_FINGERPRINT = f"sha256:{'a' * 64}"
STORAGE_FORMAT = "wealth.public_trade_collection"
DEFAULT_JOB_ID = UUID(int=1)
LEASE_TOKEN_A = UUID(int=101)
LEASE_TOKEN_B = UUID(int=102)
LEASE_TOKEN_INTRUDER = UUID(int=999)


def checkpoint(
    *,
    job_id: UUID = DEFAULT_JOB_ID,
    source: str = "binance.public-rest",
) -> PublicTradeCollectionCheckpoint:
    """Build one pristine public-trade collection checkpoint."""

    return PublicTradeCollectionCheckpoint(
        job_id=job_id,
        source=source,
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
    """Create a strict successor candidate from durable state."""

    values = current.model_dump()
    values.update(updates)
    return PublicTradeCollectionCheckpoint.model_validate(values)


def claim(
    current: PublicTradeCollectionCheckpoint,
    *,
    updated_at: datetime | None = None,
    lease_owner: str = "worker-a",
    lease_token: UUID = LEASE_TOKEN_A,
    lease_duration: timedelta = timedelta(minutes=5),
) -> PublicTradeCollectionCheckpoint:
    """Claim a job with one bounded active lease."""

    transition_time = updated_at or current.updated_at + timedelta(seconds=1)
    return copy_checkpoint(
        current,
        status=CollectionJobStatus.RUNNING,
        updated_at=transition_time,
        version=current.version + 1,
        lease_owner=lease_owner,
        lease_token=lease_token,
        lease_expires_at=transition_time + lease_duration,
        last_failure_code=None,
        last_stop_reason=None,
    )


def record_limit_pause(
    current: PublicTradeCollectionCheckpoint,
    *,
    observation_id: UUID,
    pending_end: datetime,
) -> tuple[PublicTradeCollectionCheckpoint, PublicTradeSourceHealthObservation]:
    """Describe one controlled record-limit stop without classifying an outage."""

    observed_at = current.updated_at + timedelta(seconds=1)
    paused = copy_checkpoint(
        current,
        status=CollectionJobStatus.PAUSED,
        updated_at=observed_at,
        version=current.version + 1,
        pending_window_end_exclusive=pending_end,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        source_requests=current.source_requests + 1,
        window_traces=current.window_traces + 1,
        last_failure_code=None,
        last_stop_reason="record_limit",
    )
    health = PublicTradeSourceHealthObservation(
        observation_id=observation_id,
        job_id=current.job_id,
        checkpoint_version=paused.version,
        source=current.source,
        venue=current.venue,
        instrument=current.instrument,
        provider_symbol=current.provider_symbol,
        instrument_type=current.instrument_type,
        range_start=current.next_window_start,
        range_end_exclusive=current.window_end_exclusive,
        next_window_start=current.next_window_start,
        pending_window_end_exclusive=pending_end,
        observed_at=observed_at,
        status=SourceHealthStatus.HEALTHY,
        accepted=False,
        source_requests=1,
        window_traces=1,
        windows_completed=0,
        records_completed=0,
        splits_completed=0,
        stop_reason="record_limit",
    )
    return paused, health


def accepted_health(
    current: PublicTradeCollectionCheckpoint,
    *,
    observation_id: UUID,
    range_end: datetime,
    observed_at: datetime,
    records_completed: int,
) -> PublicTradeSourceHealthObservation:
    """Build healthy evidence for one accepted, unsplit, unretried window."""

    return PublicTradeSourceHealthObservation(
        observation_id=observation_id,
        job_id=current.job_id,
        checkpoint_version=current.version + 1,
        source=current.source,
        venue=current.venue,
        instrument=current.instrument,
        provider_symbol=current.provider_symbol,
        instrument_type=current.instrument_type,
        range_start=current.next_window_start,
        range_end_exclusive=range_end,
        next_window_start=range_end,
        observed_at=observed_at,
        status=SourceHealthStatus.HEALTHY,
        accepted=True,
        source_requests=1,
        window_traces=1,
        windows_completed=1,
        records_completed=records_completed,
        splits_completed=0,
    )


def test_create_reopen_duplicate_conflict_and_compare_and_swap(tmp_path: Path) -> None:
    path = tmp_path / "public-trade-collection.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()

    inserted = store.create(initial)
    reopened = SQLitePublicTradeCollectionCheckpointStore(path)
    duplicate = reopened.create(initial)
    conflicting = reopened.create(checkpoint(source="other.public-rest"))
    claimed = claim(initial)
    updated = store.transition(claimed, expected_version=1)
    stale = reopened.transition(claimed, expected_version=1)

    assert inserted.status is CollectionCheckpointWriteStatus.INSERTED
    assert reopened.get(initial.job_id) == claimed
    assert duplicate.status is CollectionCheckpointWriteStatus.DUPLICATE
    assert conflicting.status is CollectionCheckpointWriteStatus.CONFLICT
    assert updated.status is CollectionCheckpointWriteStatus.UPDATED
    assert stale.status is CollectionCheckpointWriteStatus.CONFLICT
    assert stale.current_version == 2


def test_partial_failure_and_health_summary_are_durable(tmp_path: Path) -> None:
    path = tmp_path / "public-trade-collection.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)

    first_time = claimed.updated_at + timedelta(seconds=1)
    first_end = START + timedelta(minutes=1)
    progressed = copy_checkpoint(
        claimed,
        next_window_start=first_end,
        updated_at=first_time,
        version=3,
        windows_completed=1,
        records_completed=5,
        source_requests=1,
        window_traces=1,
    )
    healthy = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=10),
        job_id=initial.job_id,
        checkpoint_version=progressed.version,
        source=initial.source,
        venue=initial.venue,
        instrument=initial.instrument,
        provider_symbol=initial.provider_symbol,
        instrument_type=initial.instrument_type,
        range_start=START,
        range_end_exclusive=first_end,
        next_window_start=first_end,
        observed_at=first_time,
        status=SourceHealthStatus.HEALTHY,
        accepted=True,
        source_requests=1,
        window_traces=1,
        windows_completed=1,
        records_completed=5,
        splits_completed=0,
    )
    store.transition(
        progressed,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=healthy,
    )

    failed_time = first_time + timedelta(seconds=1)
    safe_cursor = START + timedelta(minutes=2)
    pending_end = START + timedelta(minutes=3)
    failed = copy_checkpoint(
        progressed,
        next_window_start=safe_cursor,
        pending_window_end_exclusive=pending_end,
        status=CollectionJobStatus.FAILED,
        updated_at=failed_time,
        version=4,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        windows_completed=2,
        records_completed=8,
        source_requests=6,
        window_traces=5,
        retry_attempts=1,
        splits_completed=2,
        last_failure_code="provider_unavailable",
        last_stop_reason="retry_limit",
    )
    unavailable = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=11),
        job_id=initial.job_id,
        checkpoint_version=failed.version,
        source=initial.source,
        venue=initial.venue,
        instrument=initial.instrument,
        provider_symbol=initial.provider_symbol,
        instrument_type=initial.instrument_type,
        range_start=first_end,
        range_end_exclusive=END,
        next_window_start=safe_cursor,
        pending_window_end_exclusive=pending_end,
        observed_at=failed_time,
        status=SourceHealthStatus.UNAVAILABLE,
        accepted=False,
        source_requests=5,
        window_traces=4,
        windows_completed=1,
        records_completed=3,
        splits_completed=2,
        retry_delays_seconds=(0.5,),
        failure_code="provider_unavailable",
        stop_reason="retry_limit",
    )
    result = store.transition(
        failed,
        expected_version=3,
        expected_lease_token=LEASE_TOKEN_A,
        health=unavailable,
    )

    reopened = SQLitePublicTradeCollectionCheckpointStore(path)
    observations = reopened.health_for_job(initial.job_id)
    summary = reopened.health_summary(initial.job_id)

    assert result.status is CollectionCheckpointWriteStatus.UPDATED
    assert reopened.get(initial.job_id) == failed
    assert observations == (healthy, unavailable)
    assert reopened.health_for_job(initial.job_id, limit=1) == (healthy,)
    assert reopened.health_for_job(
        initial.job_id,
        after_checkpoint_version=healthy.checkpoint_version,
        limit=1,
    ) == (unavailable,)
    assert (
        reopened.health_for_job(
            initial.job_id,
            after_checkpoint_version=unavailable.checkpoint_version,
            limit=1,
        )
        == ()
    )
    assert summary.observation_count == 2
    assert summary.healthy_count == 1
    assert summary.unavailable_count == 1
    assert summary.accepted_count == 1
    assert summary.total_source_requests == 6
    assert summary.total_window_traces == 5
    assert summary.total_retry_attempts == 1
    assert summary.total_windows_completed == 2
    assert summary.total_records_completed == 8
    assert summary.total_splits_completed == 2
    assert summary.total_retry_delay_seconds == pytest.approx(0.5)


def test_health_mismatch_rolls_back_checkpoint_and_observation(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    transition_time = claimed.updated_at + timedelta(seconds=1)
    page_end = START + timedelta(minutes=1)
    progressed = copy_checkpoint(
        claimed,
        next_window_start=page_end,
        updated_at=transition_time,
        version=3,
        windows_completed=1,
        records_completed=5,
        source_requests=1,
        window_traces=1,
    )
    wrong_stream = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=20),
        job_id=initial.job_id,
        checkpoint_version=progressed.version,
        source=initial.source,
        venue=initial.venue,
        instrument="ETH-USDT",
        provider_symbol=initial.provider_symbol,
        instrument_type=initial.instrument_type,
        range_start=START,
        range_end_exclusive=page_end,
        next_window_start=page_end,
        observed_at=transition_time,
        status=SourceHealthStatus.HEALTHY,
        accepted=True,
        source_requests=1,
        window_traces=1,
        windows_completed=1,
        records_completed=5,
        splits_completed=0,
    )

    with pytest.raises(ValueError, match="does not match"):
        store.transition(
            progressed,
            expected_version=2,
            expected_lease_token=LEASE_TOKEN_A,
            health=wrong_stream,
        )

    assert store.get(initial.job_id) == claimed
    assert store.health_for_job(initial.job_id) == ()
    assert store.health_summary(initial.job_id).observation_count == 0


def test_work_transition_requires_matching_health_evidence(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    progressed = copy_checkpoint(
        claimed,
        next_window_start=START + timedelta(minutes=1),
        updated_at=claimed.updated_at + timedelta(seconds=1),
        version=3,
        windows_completed=1,
        records_completed=5,
        source_requests=1,
        window_traces=1,
    )

    with pytest.raises(ValueError, match="requires source-health"):
        store.transition(
            progressed,
            expected_version=2,
            expected_lease_token=LEASE_TOKEN_A,
        )

    assert store.get(initial.job_id) == claimed


def test_store_rejects_policy_mutation_and_non_pristine_create(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    changed_policy = copy_checkpoint(
        claim(initial),
        policy_fingerprint=f"sha256:{'b' * 64}",
    )

    with pytest.raises(ValueError, match="immutable"):
        store.transition(changed_policy, expected_version=1)

    not_pristine = copy_checkpoint(
        checkpoint(job_id=UUID(int=2)),
        source_requests=1,
        window_traces=1,
    )
    with pytest.raises(ValueError, match="pristine"):
        store.create(not_pristine)

    assert store.get(initial.job_id) == initial


def test_checkpoint_read_rejects_tampered_projection(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_jobs
            SET status = 'completed'
            WHERE job_id = ?
            """,
            (str(initial.job_id),),
        )

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as raised:
        store.get(initial.job_id)

    assert raised.value.code is SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD


def test_record_limit_pause_is_healthy_and_can_be_reclaimed(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    paused, health = record_limit_pause(
        claimed,
        observation_id=UUID(int=30),
        pending_end=END,
    )

    paused_result = store.transition(
        paused,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=health,
    )
    reused_token_claim = claim(
        paused,
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_A,
    )
    with pytest.raises(ValueError, match="fresh UUID token"):
        store.transition(reused_token_claim, expected_version=3)
    assert store.get(initial.job_id) == paused
    assert store.health_for_job(initial.job_id) == (health,)

    resumed = claim(
        paused,
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
    )
    resumed_result = store.transition(resumed, expected_version=3)

    assert paused_result.status is CollectionCheckpointWriteStatus.UPDATED
    assert health.status is SourceHealthStatus.HEALTHY
    assert not health.accepted
    assert health.failure_code is None
    assert resumed_result.status is CollectionCheckpointWriteStatus.UPDATED
    assert store.get(initial.job_id) == resumed
    assert resumed.pending_window_end_exclusive == END
    assert resumed.last_stop_reason is None
    summary = store.health_summary(initial.job_id)
    assert summary.healthy_count == 1
    assert summary.accepted_count == 0
    with sqlite3.connect(path) as connection:
        lease_rows = connection.execute(
            """
            SELECT lease_token, acquired_version
            FROM public_trade_collection_leases
            WHERE job_id = ?
            ORDER BY acquired_version
            """,
            (str(initial.job_id),),
        ).fetchall()
        actor_token = connection.execute(
            """
            SELECT actor_lease_token
            FROM public_trade_collection_transitions
            WHERE job_id = ? AND version = 3
            """,
            (str(initial.job_id),),
        ).fetchone()
    assert lease_rows == [
        (str(LEASE_TOKEN_A), 2),
        (str(LEASE_TOKEN_B), 4),
    ]
    assert actor_token == (str(LEASE_TOKEN_A),)


def test_non_owner_lease_token_cannot_finalize_active_work(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    paused, health = record_limit_pause(
        claimed,
        observation_id=UUID(int=31),
        pending_end=END,
    )

    with pytest.raises(ValueError, match="active lease token"):
        store.transition(
            paused,
            expected_version=2,
            expected_lease_token=LEASE_TOKEN_INTRUDER,
            health=health,
        )

    assert store.get(initial.job_id) == claimed
    assert store.health_for_job(initial.job_id) == ()


def test_expired_lease_takeover_requires_a_new_token(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    assert claimed.lease_expires_at is not None
    takeover_time = claimed.lease_expires_at + timedelta(milliseconds=1)
    stale_token_takeover = claim(
        claimed,
        updated_at=takeover_time,
        lease_owner="worker-a",
        lease_token=LEASE_TOKEN_A,
    )

    with pytest.raises(ValueError, match="new token"):
        store.transition(stale_token_takeover, expected_version=2)

    valid_takeover = claim(
        claimed,
        updated_at=takeover_time,
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
    )
    result = store.transition(valid_takeover, expected_version=2)

    assert result.status is CollectionCheckpointWriteStatus.UPDATED
    assert store.get(initial.job_id) == valid_takeover


def test_pending_restart_accepts_exact_range_and_rejects_changed_end(
    tmp_path: Path,
) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    pending_end = START + timedelta(minutes=3)
    paused, pause_health = record_limit_pause(
        claimed,
        observation_id=UUID(int=32),
        pending_end=pending_end,
    )
    store.transition(
        paused,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=pause_health,
    )
    resumed = claim(
        paused,
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
    )
    store.transition(resumed, expected_version=3)

    changed_end = START + timedelta(minutes=2)
    observed_at = resumed.updated_at + timedelta(seconds=1)
    changed_range = copy_checkpoint(
        resumed,
        next_window_start=changed_end,
        pending_window_end_exclusive=None,
        updated_at=observed_at,
        version=5,
        windows_completed=resumed.windows_completed + 1,
        records_completed=resumed.records_completed + 2,
        source_requests=resumed.source_requests + 1,
        window_traces=resumed.window_traces + 1,
    )
    changed_health = accepted_health(
        resumed,
        observation_id=UUID(int=33),
        range_end=changed_end,
        observed_at=observed_at,
        records_completed=2,
    )
    with pytest.raises(ValueError, match="exact pending window"):
        store.transition(
            changed_range,
            expected_version=4,
            expected_lease_token=LEASE_TOKEN_B,
            health=changed_health,
        )

    exact_range = copy_checkpoint(
        resumed,
        next_window_start=pending_end,
        pending_window_end_exclusive=None,
        updated_at=observed_at,
        version=5,
        windows_completed=resumed.windows_completed + 1,
        records_completed=resumed.records_completed + 2,
        source_requests=resumed.source_requests + 1,
        window_traces=resumed.window_traces + 1,
    )
    exact_health = accepted_health(
        resumed,
        observation_id=UUID(int=34),
        range_end=pending_end,
        observed_at=observed_at,
        records_completed=2,
    )
    result = store.transition(
        exact_range,
        expected_version=4,
        expected_lease_token=LEASE_TOKEN_B,
        health=exact_health,
    )

    assert result.status is CollectionCheckpointWriteStatus.UPDATED
    assert store.get(initial.job_id) == exact_range
    assert store.health_for_job(initial.job_id) == (pause_health, exact_health)


def test_health_projection_tamper_breaks_history_and_summary(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    paused, health = record_limit_pause(
        claimed,
        observation_id=UUID(int=35),
        pending_end=END,
    )
    store.transition(
        paused,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=health,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_source_health
            SET records_completed = 99
            WHERE observation_id = ?
            """,
            (str(health.observation_id),),
        )

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as history_error:
        store.health_for_job(initial.job_id)
    with pytest.raises(SQLitePublicTradeCollectionStorageError) as summary_error:
        store.health_summary(initial.job_id)

    assert history_error.value.code is SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD
    assert summary_error.value.code is SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD


def test_duplicate_health_id_rolls_back_its_checkpoint_transition(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    observation_id = UUID(int=36)
    first_pause, first_health = record_limit_pause(
        claimed,
        observation_id=observation_id,
        pending_end=END,
    )
    store.transition(
        first_pause,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=first_health,
    )
    resumed = claim(
        first_pause,
        lease_owner="worker-b",
        lease_token=LEASE_TOKEN_B,
    )
    store.transition(resumed, expected_version=3)
    duplicate_pause, duplicate_health = record_limit_pause(
        resumed,
        observation_id=observation_id,
        pending_end=END,
    )

    with pytest.raises(SQLitePublicTradeCollectionStorageError):
        store.transition(
            duplicate_pause,
            expected_version=4,
            expected_lease_token=LEASE_TOKEN_B,
            health=duplicate_health,
        )

    assert store.get(initial.job_id) == resumed
    assert store.health_for_job(initial.job_id) == (first_health,)


def test_non_utc_health_observations_are_ordered_chronologically(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)
    claimed = claim(initial)
    store.transition(claimed, expected_version=1)
    first_end = START + timedelta(minutes=1)
    first_time = (NOW + timedelta(seconds=2)).astimezone(timezone(timedelta(hours=5)))
    first = copy_checkpoint(
        claimed,
        next_window_start=first_end,
        updated_at=first_time,
        version=3,
        windows_completed=1,
        records_completed=1,
        source_requests=1,
        window_traces=1,
    )
    first_health = accepted_health(
        claimed,
        observation_id=UUID(int=41),
        range_end=first_end,
        observed_at=first_time,
        records_completed=1,
    )
    store.transition(
        first,
        expected_version=2,
        expected_lease_token=LEASE_TOKEN_A,
        health=first_health,
    )

    second_end = START + timedelta(minutes=2)
    second_time = first_time.astimezone(timezone(timedelta(hours=-4)))
    second = copy_checkpoint(
        first,
        next_window_start=second_end,
        updated_at=second_time,
        version=4,
        windows_completed=2,
        records_completed=2,
        source_requests=2,
        window_traces=2,
    )
    second_health = accepted_health(
        first,
        observation_id=UUID(int=40),
        range_end=second_end,
        observed_at=second_time,
        records_completed=1,
    )
    store.transition(
        second,
        expected_version=3,
        expected_lease_token=LEASE_TOKEN_A,
        health=second_health,
    )

    observations = store.health_for_job(initial.job_id)
    assert tuple(item.observation_id for item in observations) == (
        first_health.observation_id,
        second_health.observation_id,
    )
    assert tuple(item.checkpoint_version for item in observations) == (3, 4)
    assert observations == (first_health, second_health)


def test_health_pagination_rejects_invalid_limits_and_unknown_cursors(
    tmp_path: Path,
) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")
    initial = checkpoint()
    store.create(initial)

    with pytest.raises(ValueError, match="limit"):
        store.health_for_job(initial.job_id, limit=0)
    with pytest.raises(ValueError, match="limit"):
        store.health_for_job(initial.job_id, limit=1_001)
    with pytest.raises(ValueError, match="cursor"):
        store.health_for_job(initial.job_id, after_checkpoint_version=1)
    with pytest.raises(ValueError, match="cursor"):
        store.health_for_job(initial.job_id, after_checkpoint_version=99)
    with pytest.raises(ValueError, match="cursor"):
        store.health_for_job(initial.job_id, after_checkpoint_version=2**100)


@pytest.mark.parametrize(
    ("storage_format", "schema_version"),
    [
        ("wealth.other_control_store", 1),
        (STORAGE_FORMAT, 99),
        (STORAGE_FORMAT, 1),
    ],
)
def test_wrong_database_marker_or_schema_is_rejected(
    tmp_path: Path,
    storage_format: str,
    schema_version: int,
) -> None:
    path = tmp_path / "wrong.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE public_trade_collection_metadata (
                storage_format TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO public_trade_collection_metadata (
                storage_format, schema_version
            ) VALUES (?, ?)
            """,
            (storage_format, schema_version),
        )
        connection.execute("PRAGMA user_version = 1")

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as raised:
        SQLitePublicTradeCollectionCheckpointStore(path)

    assert raised.value.code is SQLitePublicTradeCollectionStorageErrorCode.UNSUPPORTED_SCHEMA


def test_unknown_sqlite_schema_version_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version = 99")

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as raised:
        SQLitePublicTradeCollectionCheckpointStore(path)

    assert raised.value.code is SQLitePublicTradeCollectionStorageErrorCode.UNSUPPORTED_SCHEMA


def test_same_columns_without_canonical_constraints_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "weak-schema.sqlite3"
    statements = SQLitePublicTradeCollectionCheckpointStore._schema_statements()
    with sqlite3.connect(path) as connection:
        for statement in statements:
            if "CREATE TABLE public_trade_collection_jobs" in statement:
                statement = statement.replace(
                    "version INTEGER NOT NULL",
                    "version INTEGER",
                    1,
                )
            connection.execute(statement)
        connection.execute(
            """
            INSERT INTO public_trade_collection_metadata (
                storage_format, schema_version
            ) VALUES (?, ?)
            """,
            (STORAGE_FORMAT, 1),
        )
        connection.execute("PRAGMA user_version = 1")

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as raised:
        SQLitePublicTradeCollectionCheckpointStore(path)

    assert raised.value.code is SQLitePublicTradeCollectionStorageErrorCode.UNSUPPORTED_SCHEMA
