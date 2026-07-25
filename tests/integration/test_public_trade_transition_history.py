"""Integration tests for bounded public-trade transition-history reads."""

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest

from wealth.adapters.sqlite_order_flow_collection import (
    SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION,
    SQLitePublicTradeCollectionCheckpointStore,
    SQLitePublicTradeCollectionStorageError,
    SQLitePublicTradeCollectionStorageErrorCode,
)
from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.market import InstrumentType
from wealth.domain.order_flow_collection import (
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionTransition,
    PublicTradeSourceHealthObservation,
)
from wealth.ports.collection import CollectionCheckpointWriteStatus
from wealth.ports.order_flow_collection import (
    DEFAULT_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
    MAX_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
)

START = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
END = START + timedelta(minutes=10)
NOW = START + timedelta(days=1)
POLICY_FINGERPRINT = f"sha256:{'a' * 64}"
JOB_ID = UUID(int=1)
LEASE_TOKEN_A = UUID(int=101)
LEASE_TOKEN_B = UUID(int=102)
LEASE_TOKEN_C = UUID(int=103)
LEASE_TOKEN_D = UUID(int=104)

CONTROL_TABLES = (
    "public_trade_collection_metadata",
    "public_trade_collection_jobs",
    "public_trade_collection_transitions",
    "public_trade_collection_leases",
    "public_trade_source_health",
)


def checkpoint(
    *,
    job_id: UUID = JOB_ID,
    created_at: datetime = NOW,
) -> PublicTradeCollectionCheckpoint:
    """Build one pristine bounded public-trade checkpoint."""

    return PublicTradeCollectionCheckpoint(
        job_id=job_id,
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
        created_at=created_at,
        updated_at=created_at,
        version=1,
    )


def copy_checkpoint(
    current: PublicTradeCollectionCheckpoint,
    **updates: object,
) -> PublicTradeCollectionCheckpoint:
    """Create one strict successor or corruption candidate."""

    values = current.model_dump()
    values.update(updates)
    return PublicTradeCollectionCheckpoint.model_validate(values)


def claim(
    current: PublicTradeCollectionCheckpoint,
    *,
    lease_token: UUID,
    lease_owner: str,
    updated_at: datetime | None = None,
    lease_duration: timedelta = timedelta(minutes=5),
) -> PublicTradeCollectionCheckpoint:
    """Claim, resume, or take over a checkpoint with fresh authority."""

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


def renew(current: PublicTradeCollectionCheckpoint) -> PublicTradeCollectionCheckpoint:
    """Renew one active lease without recording collection work."""

    transition_time = current.updated_at + timedelta(seconds=1)
    return copy_checkpoint(
        current,
        updated_at=transition_time,
        version=current.version + 1,
        lease_expires_at=transition_time + timedelta(minutes=5),
    )


def assert_updated(result_status: CollectionCheckpointWriteStatus) -> None:
    """Keep lifecycle fixture writes explicit."""

    assert result_status is CollectionCheckpointWriteStatus.UPDATED


def build_full_lifecycle(
    path: Path,
) -> tuple[
    SQLitePublicTradeCollectionCheckpointStore,
    tuple[PublicTradeCollectionCheckpoint, ...],
]:
    """Persist every supported transition family in one causal job."""

    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    assert store.create(initial).status is CollectionCheckpointWriteStatus.INSERTED

    claimed = claim(
        initial,
        lease_token=LEASE_TOKEN_A,
        lease_owner="worker-a",
    )
    assert_updated(store.transition(claimed, expected_version=initial.version).status)

    renewed = renew(claimed)
    assert_updated(
        store.transition(
            renewed,
            expected_version=claimed.version,
            expected_lease_token=LEASE_TOKEN_A,
        ).status
    )

    pause_time = renewed.updated_at + timedelta(seconds=1)
    paused = copy_checkpoint(
        renewed,
        status=CollectionJobStatus.PAUSED,
        updated_at=pause_time,
        version=renewed.version + 1,
        pending_window_end_exclusive=END,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        source_requests=renewed.source_requests + 1,
        window_traces=renewed.window_traces + 1,
        last_stop_reason="record_limit",
    )
    pause_health = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=201),
        job_id=JOB_ID,
        checkpoint_version=paused.version,
        source=paused.source,
        venue=paused.venue,
        instrument=paused.instrument,
        provider_symbol=paused.provider_symbol,
        instrument_type=paused.instrument_type,
        range_start=START,
        range_end_exclusive=END,
        next_window_start=START,
        pending_window_end_exclusive=END,
        observed_at=pause_time,
        status=SourceHealthStatus.HEALTHY,
        accepted=False,
        source_requests=1,
        window_traces=1,
        windows_completed=0,
        records_completed=0,
        splits_completed=0,
        stop_reason="record_limit",
    )
    assert_updated(
        store.transition(
            paused,
            expected_version=renewed.version,
            expected_lease_token=LEASE_TOKEN_A,
            health=pause_health,
        ).status
    )

    resumed_after_pause = claim(
        paused,
        lease_token=LEASE_TOKEN_B,
        lease_owner="worker-b",
    )
    assert_updated(
        store.transition(
            resumed_after_pause,
            expected_version=paused.version,
        ).status
    )

    failure_time = resumed_after_pause.updated_at + timedelta(seconds=1)
    failed = copy_checkpoint(
        resumed_after_pause,
        status=CollectionJobStatus.FAILED,
        updated_at=failure_time,
        version=resumed_after_pause.version + 1,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        source_requests=resumed_after_pause.source_requests + 1,
        window_traces=resumed_after_pause.window_traces + 1,
        last_failure_code="provider_unavailable",
        last_stop_reason="retry_limit",
    )
    failure_health = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=202),
        job_id=JOB_ID,
        checkpoint_version=failed.version,
        source=failed.source,
        venue=failed.venue,
        instrument=failed.instrument,
        provider_symbol=failed.provider_symbol,
        instrument_type=failed.instrument_type,
        range_start=START,
        range_end_exclusive=END,
        next_window_start=START,
        pending_window_end_exclusive=END,
        observed_at=failure_time,
        status=SourceHealthStatus.UNAVAILABLE,
        accepted=False,
        source_requests=1,
        window_traces=1,
        windows_completed=0,
        records_completed=0,
        splits_completed=0,
        failure_code="provider_unavailable",
        stop_reason="retry_limit",
    )
    assert_updated(
        store.transition(
            failed,
            expected_version=resumed_after_pause.version,
            expected_lease_token=LEASE_TOKEN_B,
            health=failure_health,
        ).status
    )

    resumed_after_failure = claim(
        failed,
        lease_token=LEASE_TOKEN_C,
        lease_owner="worker-c",
        lease_duration=timedelta(seconds=2),
    )
    assert_updated(
        store.transition(
            resumed_after_failure,
            expected_version=failed.version,
        ).status
    )
    assert resumed_after_failure.lease_expires_at is not None

    taken_over = claim(
        resumed_after_failure,
        lease_token=LEASE_TOKEN_D,
        lease_owner="worker-d",
        updated_at=resumed_after_failure.lease_expires_at,
    )
    assert_updated(
        store.transition(
            taken_over,
            expected_version=resumed_after_failure.version,
        ).status
    )

    completion_time = taken_over.updated_at + timedelta(seconds=1)
    completed = copy_checkpoint(
        taken_over,
        status=CollectionJobStatus.COMPLETED,
        updated_at=completion_time,
        version=taken_over.version + 1,
        next_window_start=END,
        pending_window_end_exclusive=None,
        lease_owner=None,
        lease_token=None,
        lease_expires_at=None,
        windows_completed=taken_over.windows_completed + 1,
        records_completed=taken_over.records_completed + 5,
        source_requests=taken_over.source_requests + 1,
        window_traces=taken_over.window_traces + 1,
    )
    completion_health = PublicTradeSourceHealthObservation(
        observation_id=UUID(int=203),
        job_id=JOB_ID,
        checkpoint_version=completed.version,
        source=completed.source,
        venue=completed.venue,
        instrument=completed.instrument,
        provider_symbol=completed.provider_symbol,
        instrument_type=completed.instrument_type,
        range_start=START,
        range_end_exclusive=END,
        next_window_start=END,
        observed_at=completion_time,
        status=SourceHealthStatus.HEALTHY,
        accepted=True,
        source_requests=1,
        window_traces=1,
        windows_completed=1,
        records_completed=5,
        splits_completed=0,
    )
    assert_updated(
        store.transition(
            completed,
            expected_version=taken_over.version,
            expected_lease_token=LEASE_TOKEN_D,
            health=completion_health,
        ).status
    )
    return (
        store,
        (
            initial,
            claimed,
            renewed,
            paused,
            resumed_after_pause,
            failed,
            resumed_after_failure,
            taken_over,
            completed,
        ),
    )


def build_running_history(
    path: Path,
) -> tuple[
    SQLitePublicTradeCollectionCheckpointStore,
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionCheckpoint,
]:
    """Persist creation, claim, and renewal for corruption tests."""

    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    claimed = claim(
        initial,
        lease_token=LEASE_TOKEN_A,
        lease_owner="worker-a",
    )
    store.transition(claimed, expected_version=initial.version)
    renewed = renew(claimed)
    store.transition(
        renewed,
        expected_version=claimed.version,
        expected_lease_token=LEASE_TOKEN_A,
    )
    return store, initial, claimed, renewed


def build_history_to_version(
    path: Path,
    target_version: int,
) -> tuple[
    SQLitePublicTradeCollectionCheckpointStore,
    PublicTradeCollectionCheckpoint,
]:
    """Persist one claim followed by renewals through the requested version."""

    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = checkpoint()
    store.create(initial)
    current = claim(
        initial,
        lease_token=LEASE_TOKEN_A,
        lease_owner="worker-a",
    )
    store.transition(current, expected_version=initial.version)
    while current.version < target_version:
        successor = renew(current)
        store.transition(
            successor,
            expected_version=current.version,
            expected_lease_token=LEASE_TOKEN_A,
        )
        current = successor
    return store, current


def database_snapshot(path: Path) -> tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]:
    """Capture every control table to prove transition reads are non-mutating."""

    with sqlite3.connect(path) as connection:
        return tuple(
            (
                table,
                tuple(connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()),
            )
            for table in CONTROL_TABLES
        )


def assert_corrupt(
    store: SQLitePublicTradeCollectionCheckpointStore,
    *,
    after_checkpoint_version: int | None = None,
    limit: int = DEFAULT_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
) -> None:
    """Require the bounded existing corruption boundary."""

    with pytest.raises(SQLitePublicTradeCollectionStorageError) as raised:
        store.transitions_for_job(
            JOB_ID,
            after_checkpoint_version=after_checkpoint_version,
            limit=limit,
        )
    assert raised.value.code is SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD


def test_transition_history_replays_every_family_and_is_read_only(
    tmp_path: Path,
) -> None:
    path = tmp_path / "collection.sqlite3"
    store, checkpoints = build_full_lifecycle(path)
    before = database_snapshot(path)

    transitions = store.transitions_for_job(JOB_ID)
    after = database_snapshot(path)

    assert tuple(item.checkpoint for item in transitions) == checkpoints
    assert tuple(item.actor_lease_token for item in transitions) == (
        None,
        None,
        LEASE_TOKEN_A,
        LEASE_TOKEN_A,
        None,
        LEASE_TOKEN_B,
        None,
        None,
        LEASE_TOKEN_D,
    )
    assert before == after
    assert store.health_for_job(JOB_ID)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (
            SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION,
        )

    reopened = SQLitePublicTradeCollectionCheckpointStore(path)
    collected: list[PublicTradeCollectionTransition] = []
    cursor: int | None = None
    while True:
        page = reopened.transitions_for_job(
            JOB_ID,
            after_checkpoint_version=cursor,
            limit=3,
        )
        if not page:
            break
        collected.extend(page)
        cursor = page[-1].checkpoint.version
    assert tuple(collected) == transitions
    assert (
        reopened.transitions_for_job(
            JOB_ID,
            after_checkpoint_version=checkpoints[-1].version,
        )
        == ()
    )


def test_transition_pages_enforce_default_and_hard_maximum(tmp_path: Path) -> None:
    path = tmp_path / "collection.sqlite3"
    store, _ = build_history_to_version(path, 102)

    first_page = store.transitions_for_job(JOB_ID)
    remainder = store.transitions_for_job(
        JOB_ID,
        after_checkpoint_version=first_page[-1].checkpoint.version,
        limit=MAX_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
    )

    assert DEFAULT_PUBLIC_TRADE_TRANSITION_PAGE_SIZE == 100
    assert MAX_PUBLIC_TRADE_TRANSITION_PAGE_SIZE == 1_000
    assert tuple(item.checkpoint.version for item in first_page) == tuple(range(1, 101))
    assert tuple(item.checkpoint.version for item in remainder) == (101, 102)
    assert (
        len(
            store.transitions_for_job(
                JOB_ID,
                limit=MAX_PUBLIC_TRADE_TRANSITION_PAGE_SIZE,
            )
        )
        == 102
    )


@pytest.mark.parametrize("limit", [0, 1_001, True, 1.5])
def test_transition_pages_reject_non_integer_or_out_of_range_limits(
    tmp_path: Path,
    limit: object,
) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")

    with pytest.raises(ValueError, match="limit must be an integer"):
        store.transitions_for_job(JOB_ID, limit=cast(Any, limit))


@pytest.mark.parametrize("cursor", [0, 2**100, True, 1.5])
def test_transition_pages_reject_non_integer_or_out_of_range_cursors(
    tmp_path: Path,
    cursor: object,
) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")

    with pytest.raises(ValueError, match="cursor must be a returned"):
        store.transitions_for_job(
            JOB_ID,
            after_checkpoint_version=cast(Any, cursor),
        )


@pytest.mark.parametrize("job_id", [str(JOB_ID), None, True])
def test_transition_pages_reject_non_uuid_job_ids(
    tmp_path: Path,
    job_id: object,
) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")

    with pytest.raises(ValueError, match="job id must be a UUID"):
        store.transitions_for_job(cast(Any, job_id))


def test_missing_jobs_and_empty_tail_have_explicit_semantics(tmp_path: Path) -> None:
    store = SQLitePublicTradeCollectionCheckpointStore(tmp_path / "collection.sqlite3")

    assert store.transitions_for_job(JOB_ID) == ()
    with pytest.raises(ValueError, match="does not identify"):
        store.transitions_for_job(JOB_ID, after_checkpoint_version=1)

    initial = checkpoint()
    store.create(initial)
    assert store.transitions_for_job(JOB_ID, after_checkpoint_version=1) == ()
    with pytest.raises(ValueError, match="does not identify"):
        store.transitions_for_job(JOB_ID, after_checkpoint_version=2)


def test_checkpoint_without_creation_history_is_corrupt_before_cursor_semantics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "missing-history.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    store.create(checkpoint())
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DELETE FROM public_trade_collection_transitions WHERE job_id = ?",
            (str(JOB_ID),),
        )

    assert_corrupt(store, after_checkpoint_version=2)


@pytest.mark.parametrize("hidden_version", [0, -1])
def test_hidden_nonpositive_transition_versions_fail_closed(
    tmp_path: Path,
    hidden_version: int,
) -> None:
    path = tmp_path / f"hidden-{hidden_version}.sqlite3"
    store, initial, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO public_trade_collection_transitions (
                job_id, version, status, recorded_at, actor_lease_token, record_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(JOB_ID),
                hidden_version,
                initial.status.value,
                initial.updated_at.astimezone(UTC).isoformat(),
                None,
                initial.model_dump_json(),
            ),
        )

    assert_corrupt(store)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("job_id", str(UUID(int=999))),
        ("version", 99),
        ("status", CollectionJobStatus.FAILED.value),
        ("recorded_at", "2099-01-01T00:00:00+00:00"),
        ("actor_lease_token", str(LEASE_TOKEN_A)),
    ],
)
def test_transition_projection_or_actor_tamper_fails_closed(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    path = tmp_path / f"{column}.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"""
            UPDATE public_trade_collection_transitions
            SET {column} = ?
            WHERE job_id = ? AND version = 2
            """,
            (value, str(JOB_ID)),
        )

    assert_corrupt(store)


@pytest.mark.parametrize("record_json", ["{", "{}"])
def test_malformed_or_contract_invalid_transition_json_fails_closed(
    tmp_path: Path,
    record_json: str,
) -> None:
    path = tmp_path / "record.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET record_json = ?
            WHERE job_id = ? AND version = 2
            """,
            (record_json, str(JOB_ID)),
        )

    assert_corrupt(store)


def test_semantically_equivalent_noncanonical_json_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "noncanonical.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET record_json = record_json || ' '
            WHERE job_id = ? AND version = 2
            """,
            (str(JOB_ID),),
        )

    assert_corrupt(store)


def test_invalid_actor_uuid_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "actor.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET actor_lease_token = 'not-a-uuid'
            WHERE job_id = ? AND version = 3
            """,
            (str(JOB_ID),),
        )

    assert_corrupt(store)


def test_reused_lease_token_cannot_form_coherent_later_history(tmp_path: Path) -> None:
    path = tmp_path / "reused-lease.sqlite3"
    store, checkpoints = build_full_lifecycle(path)
    corrupted_resume = copy_checkpoint(
        checkpoints[4],
        lease_owner="worker-a",
        lease_token=LEASE_TOKEN_A,
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET record_json = ?
            WHERE job_id = ? AND version = 5
            """,
            (corrupted_resume.model_dump_json(), str(JOB_ID)),
        )
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET actor_lease_token = ?
            WHERE job_id = ? AND version = 6
            """,
            (str(LEASE_TOKEN_A), str(JOB_ID)),
        )

    assert_corrupt(store, after_checkpoint_version=6, limit=1)


def test_fractional_lease_acquisition_version_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "fractional-lease-version.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_leases
            SET acquired_version = 2.5
            WHERE job_id = ? AND lease_token = ?
            """,
            (str(JOB_ID), str(LEASE_TOKEN_A)),
        )

    assert_corrupt(store)


@pytest.mark.parametrize("deleted_version", [2, 3])
def test_version_gap_or_missing_tail_fails_closed(
    tmp_path: Path,
    deleted_version: int,
) -> None:
    path = tmp_path / f"gap-{deleted_version}.sqlite3"
    store, _, _, _ = build_running_history(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            DELETE FROM public_trade_collection_transitions
            WHERE job_id = ? AND version = ?
            """,
            (str(JOB_ID), deleted_version),
        )

    assert_corrupt(store)
    if deleted_version == 2:
        assert_corrupt(store, after_checkpoint_version=2)
    else:
        assert_corrupt(store, limit=2)


@pytest.mark.parametrize("corruption", ["impossible_status", "time_regression", "identity_drift"])
def test_model_valid_but_causally_impossible_transition_fails_closed(
    tmp_path: Path,
    corruption: str,
) -> None:
    path = tmp_path / f"{corruption}.sqlite3"
    store, initial, _, renewed = build_running_history(path)
    updates: dict[str, object]
    if corruption == "impossible_status":
        updates = {
            "status": CollectionJobStatus.PENDING,
            "lease_owner": None,
            "lease_token": None,
            "lease_expires_at": None,
        }
    elif corruption == "time_regression":
        updates = {"updated_at": initial.updated_at}
    else:
        updates = {"source": "other.public-rest"}
    corrupted = copy_checkpoint(renewed, **updates)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET status = ?, recorded_at = ?, record_json = ?
            WHERE job_id = ? AND version = 3
            """,
            (
                corrupted.status.value,
                corrupted.updated_at.astimezone(UTC).isoformat(),
                corrupted.model_dump_json(),
                str(JOB_ID),
            ),
        )

    assert_corrupt(store)


def test_fractional_transition_and_current_versions_fail_closed(tmp_path: Path) -> None:
    transition_path = tmp_path / "fractional-transition.sqlite3"
    transition_store, _, _, _ = build_running_history(transition_path)
    with sqlite3.connect(transition_path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET version = 2.5
            WHERE job_id = ? AND version = 2
            """,
            (str(JOB_ID),),
        )
    assert_corrupt(transition_store)

    current_path = tmp_path / "fractional-current.sqlite3"
    current_store, _, _, _ = build_running_history(current_path)
    with sqlite3.connect(current_path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_jobs
            SET version = 3.5
            WHERE job_id = ?
            """,
            (str(JOB_ID),),
        )
    assert_corrupt(current_store)


def test_blob_checkpoint_text_projection_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "blob-checkpoint-text.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    initial = copy_checkpoint(checkpoint(), source="b'foo'")
    store.create(initial)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_jobs
            SET source = ?
            WHERE job_id = ?
            """,
            (sqlite3.Binary(b"foo"), str(JOB_ID)),
        )

    assert_corrupt(store)


def test_fractional_page_lookahead_version_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "fractional-lookahead.sqlite3"
    store, _ = build_history_to_version(path, 102)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET version = 101.5
            WHERE job_id = ? AND version = 101
            """,
            (str(JOB_ID),),
        )

    assert_corrupt(store)


def test_full_page_rejects_history_beyond_current_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "extra-tail.sqlite3"
    store, current = build_history_to_version(path, 100)
    extra = renew(current)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO public_trade_collection_transitions (
                job_id, version, status, recorded_at, actor_lease_token, record_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(JOB_ID),
                extra.version,
                extra.status.value,
                extra.updated_at.astimezone(UTC).isoformat(),
                str(LEASE_TOKEN_A),
                extra.model_dump_json(),
            ),
        )

    assert_corrupt(store)


def test_non_pristine_creation_and_orphan_history_fail_closed(tmp_path: Path) -> None:
    non_pristine_path = tmp_path / "non-pristine.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(non_pristine_path)
    initial = checkpoint()
    store.create(initial)
    corrupted = copy_checkpoint(
        initial,
        source_requests=1,
        window_traces=1,
    )
    with sqlite3.connect(non_pristine_path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_transitions
            SET record_json = ?
            WHERE job_id = ? AND version = 1
            """,
            (corrupted.model_dump_json(), str(JOB_ID)),
        )
    assert_corrupt(store)

    orphan_path = tmp_path / "orphan.sqlite3"
    orphan_store, _, _, _ = build_running_history(orphan_path)
    with sqlite3.connect(orphan_path) as connection:
        connection.execute(
            "DELETE FROM public_trade_collection_jobs WHERE job_id = ?",
            (str(JOB_ID),),
        )
    assert_corrupt(orphan_store)


def test_non_utc_transition_content_fails_at_the_new_reader_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "non-utc.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    non_utc_time = NOW.astimezone(timezone(timedelta(hours=3)))
    store.create(checkpoint(created_at=non_utc_time))

    assert_corrupt(store)


def test_non_utc_current_checkpoint_fails_at_the_new_reader_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / "non-utc-current.sqlite3"
    store, _, _, current = build_running_history(path)
    offset_current = copy_checkpoint(
        current,
        updated_at=current.updated_at.astimezone(timezone(timedelta(hours=3))),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_jobs
            SET record_json = ?
            WHERE job_id = ?
            """,
            (offset_current.model_dump_json(), str(JOB_ID)),
        )

    assert_corrupt(store)


def test_extreme_offset_current_checkpoint_maps_overflow_to_corruption(
    tmp_path: Path,
) -> None:
    path = tmp_path / "extreme-offset-current.sqlite3"
    store = SQLitePublicTradeCollectionCheckpointStore(path)
    store.create(checkpoint())
    extreme = checkpoint(
        created_at=datetime.min.replace(tzinfo=timezone(timedelta(hours=14))),
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE public_trade_collection_jobs
            SET record_json = ?
            WHERE job_id = ?
            """,
            (extreme.model_dump_json(), str(JOB_ID)),
        )

    assert_corrupt(store)
