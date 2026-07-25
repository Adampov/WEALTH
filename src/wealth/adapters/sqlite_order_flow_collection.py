"""Durable SQLite control state for bounded public-trade collection."""

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.collection import CollectionJobStatus, SourceHealthStatus
from wealth.domain.order_flow_collection import (
    MAX_DURABLE_COUNTER,
    PublicTradeCollectionCheckpoint,
    PublicTradeCollectionHealthSummary,
    PublicTradeSourceHealthObservation,
    validate_public_trade_collection_transition,
)
from wealth.ports.collection import (
    CollectionCheckpointWriteResult,
    CollectionCheckpointWriteStatus,
)
from wealth.ports.order_flow_collection import (
    DEFAULT_PUBLIC_TRADE_HEALTH_PAGE_SIZE,
    MAX_PUBLIC_TRADE_HEALTH_PAGE_SIZE,
)

SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION = 1
SQLITE_PUBLIC_TRADE_COLLECTION_STORAGE_FORMAT = "wealth.public_trade_collection"

_CHECKPOINT_COLUMNS = (
    "job_id",
    "version",
    "status",
    "source",
    "venue",
    "instrument",
    "provider_symbol",
    "instrument_type",
    "policy_fingerprint",
    "window_start",
    "window_end_exclusive",
    "next_window_start",
    "pending_window_end_exclusive",
    "created_at",
    "updated_at",
    "lease_owner",
    "lease_token",
    "lease_expires_at",
    "windows_completed",
    "records_completed",
    "source_requests",
    "window_traces",
    "retry_attempts",
    "splits_completed",
    "last_failure_code",
    "last_stop_reason",
    "record_json",
)

_HEALTH_COLUMNS = (
    "observation_id",
    "job_id",
    "checkpoint_version",
    "source",
    "venue",
    "instrument",
    "provider_symbol",
    "instrument_type",
    "range_start",
    "range_end_exclusive",
    "next_window_start",
    "pending_window_end_exclusive",
    "observed_at",
    "status",
    "accepted",
    "source_requests",
    "window_traces",
    "windows_completed",
    "records_completed",
    "splits_completed",
    "retry_attempts",
    "retry_delay_total_seconds",
    "failure_code",
    "stop_reason",
    "record_json",
)


class SQLitePublicTradeCollectionStorageErrorCode(StrEnum):
    """Machine-readable public-trade control-storage failures."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLitePublicTradeCollectionStorageError(RuntimeError):
    """Fail explicitly whenever durable public-trade control state is untrusted."""

    def __init__(
        self,
        code: SQLitePublicTradeCollectionStorageErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLitePublicTradeCollectionCheckpointStore:
    """File-backed CAS checkpoints plus append-only health evidence."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.INVALID_PATH,
                "durable public-trade control storage requires a dedicated file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def create(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> CollectionCheckpointWriteResult:
        """Insert one pristine pending checkpoint without replacing prior state."""

        if not self._is_pristine(checkpoint):
            raise ValueError(
                "new public-trade collection job must be a pristine version-one checkpoint"
            )
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM public_trade_collection_jobs WHERE job_id = ?",
                    (str(checkpoint.job_id),),
                ).fetchone()
                if row is not None:
                    existing = self._checkpoint_from_row(row)
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=(
                            CollectionCheckpointWriteStatus.DUPLICATE
                            if existing == checkpoint
                            else CollectionCheckpointWriteStatus.CONFLICT
                        ),
                        job_id=checkpoint.job_id,
                        current_version=existing.version,
                    )

                self._insert_checkpoint(connection, checkpoint)
                self._insert_transition(
                    connection,
                    checkpoint,
                    actor_lease_token=None,
                )
                connection.commit()
                return CollectionCheckpointWriteResult(
                    status=CollectionCheckpointWriteStatus.INSERTED,
                    job_id=checkpoint.job_id,
                    current_version=checkpoint.version,
                )
        except SQLitePublicTradeCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("public-trade checkpoint creation failed") from error

    def get(self, job_id: UUID) -> PublicTradeCollectionCheckpoint | None:
        """Reload a checkpoint and fail closed when projections disagree."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    "SELECT * FROM public_trade_collection_jobs WHERE job_id = ?",
                    (str(job_id),),
                ).fetchone()
            return None if row is None else self._checkpoint_from_row(row)
        except SQLitePublicTradeCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("public-trade checkpoint read failed") from error

    def transition(
        self,
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        expected_version: int,
        expected_lease_token: UUID | None = None,
        health: PublicTradeSourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        """Atomically apply one versioned, lease-authorized control transition."""

        if expected_version < 1 or checkpoint.version != expected_version + 1:
            raise ValueError("checkpoint transition requires the exact next version")
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM public_trade_collection_jobs WHERE job_id = ?",
                    (str(checkpoint.job_id),),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=checkpoint.job_id,
                        current_version=0,
                    )
                previous = self._checkpoint_from_row(row)
                if previous.version != expected_version:
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=checkpoint.job_id,
                        current_version=previous.version,
                    )

                validate_public_trade_collection_transition(previous, checkpoint)
                self._validate_lease_authority(
                    previous,
                    checkpoint,
                    expected_lease_token=expected_lease_token,
                )
                if health is None:
                    self._validate_transition_without_health(previous, checkpoint)
                else:
                    self._validate_health_transition(previous, checkpoint, health)
                if self._acquires_new_lease(previous, checkpoint):
                    self._validate_fresh_lease_token(connection, checkpoint)
                    self._insert_lease_acquisition(connection, checkpoint)

                values = self._checkpoint_projection(checkpoint)
                assignments = ", ".join(f"{column} = ?" for column in _CHECKPOINT_COLUMNS[1:])
                cursor = connection.execute(
                    f"""
                    UPDATE public_trade_collection_jobs
                    SET {assignments}
                    WHERE job_id = ? AND version = ?
                    """,
                    (*values[1:], str(checkpoint.job_id), expected_version),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    current = self.get(checkpoint.job_id)
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=checkpoint.job_id,
                        current_version=0 if current is None else current.version,
                    )
                self._insert_transition(
                    connection,
                    checkpoint,
                    actor_lease_token=expected_lease_token,
                )
                if health is not None:
                    self._insert_health(connection, health)
                connection.commit()
                return CollectionCheckpointWriteResult(
                    status=CollectionCheckpointWriteStatus.UPDATED,
                    job_id=checkpoint.job_id,
                    current_version=checkpoint.version,
                )
        except (SQLitePublicTradeCollectionStorageError, ValueError):
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("public-trade checkpoint transition failed") from error

    def health_for_job(
        self,
        job_id: UUID,
        *,
        after_checkpoint_version: int | None = None,
        limit: int = DEFAULT_PUBLIC_TRADE_HEALTH_PAGE_SIZE,
    ) -> tuple[PublicTradeSourceHealthObservation, ...]:
        """Reload one bounded, causally ordered page of health evidence."""

        if not 1 <= limit <= MAX_PUBLIC_TRADE_HEALTH_PAGE_SIZE:
            raise ValueError(
                "public-trade health page limit must be between 1 and "
                f"{MAX_PUBLIC_TRADE_HEALTH_PAGE_SIZE}"
            )
        if after_checkpoint_version is not None and not (
            2 <= after_checkpoint_version <= MAX_DURABLE_COUNTER
        ):
            raise ValueError("public-trade health cursor must be a returned checkpoint version")
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN")
                checkpoint_row = connection.execute(
                    "SELECT * FROM public_trade_collection_jobs WHERE job_id = ?",
                    (str(job_id),),
                ).fetchone()
                predecessor_row = (
                    None
                    if after_checkpoint_version is None
                    else connection.execute(
                        """
                        SELECT *
                        FROM public_trade_source_health
                        WHERE job_id = ? AND checkpoint_version = ?
                        """,
                        (str(job_id), after_checkpoint_version),
                    ).fetchone()
                )
                rows = connection.execute(
                    """
                    SELECT *
                    FROM public_trade_source_health
                    WHERE job_id = ? AND checkpoint_version > ?
                    ORDER BY checkpoint_version
                    LIMIT ?
                    """,
                    (
                        str(job_id),
                        0 if after_checkpoint_version is None else after_checkpoint_version,
                        limit,
                    ),
                ).fetchall()
            checkpoint = (
                None if checkpoint_row is None else self._checkpoint_from_row(checkpoint_row)
            )
            predecessor = (
                None if predecessor_row is None else self._health_from_row(predecessor_row)
            )
            observations = tuple(self._health_from_row(row) for row in rows)
            if checkpoint is None and (predecessor is not None or observations):
                raise SQLitePublicTradeCollectionStorageError(
                    SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                    "public-trade health exists without its checkpoint",
                )
            if after_checkpoint_version is not None and predecessor is None:
                raise ValueError("public-trade health cursor does not identify stored evidence")
            if checkpoint is not None:
                previous = predecessor
                if predecessor is not None:
                    self._validate_persisted_health_identity(checkpoint, predecessor)
                for observation in observations:
                    self._validate_persisted_health_identity(checkpoint, observation)
                    self._validate_health_sequence_step(
                        checkpoint,
                        previous=previous,
                        current=observation,
                    )
                    previous = observation
            return observations
        except SQLitePublicTradeCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("public-trade source health read failed") from error

    def health_summary(self, job_id: UUID) -> PublicTradeCollectionHealthSummary:
        """Stream, validate, and aggregate canonical health in bounded memory."""

        observation_count = 0
        healthy_count = 0
        degraded_count = 0
        unavailable_count = 0
        accepted_count = 0
        total_source_requests = 0
        total_window_traces = 0
        total_retry_attempts = 0
        total_windows_completed = 0
        total_records_completed = 0
        total_splits_completed = 0
        total_retry_delay_seconds = 0.0
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN")
                checkpoint_row = connection.execute(
                    "SELECT * FROM public_trade_collection_jobs WHERE job_id = ?",
                    (str(job_id),),
                ).fetchone()
                checkpoint = (
                    None if checkpoint_row is None else self._checkpoint_from_row(checkpoint_row)
                )
                rows = connection.execute(
                    """
                    SELECT *
                    FROM public_trade_source_health
                    WHERE job_id = ?
                    ORDER BY checkpoint_version
                    """,
                    (str(job_id),),
                )
                previous: PublicTradeSourceHealthObservation | None = None
                for row in rows:
                    observation = self._health_from_row(row)
                    if checkpoint is None:
                        raise SQLitePublicTradeCollectionStorageError(
                            SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                            "public-trade health exists without its checkpoint",
                        )
                    self._validate_persisted_health_identity(checkpoint, observation)
                    self._validate_health_sequence_step(
                        checkpoint,
                        previous=previous,
                        current=observation,
                    )
                    previous = observation
                    observation_count += 1
                    healthy_count += observation.status is SourceHealthStatus.HEALTHY
                    degraded_count += observation.status is SourceHealthStatus.DEGRADED
                    unavailable_count += observation.status is SourceHealthStatus.UNAVAILABLE
                    accepted_count += observation.accepted
                    total_source_requests += observation.source_requests
                    total_window_traces += observation.window_traces
                    total_retry_attempts += len(observation.retry_delays_seconds)
                    total_windows_completed += observation.windows_completed
                    total_records_completed += observation.records_completed
                    total_splits_completed += observation.splits_completed
                    total_retry_delay_seconds += sum(observation.retry_delays_seconds)
                if checkpoint is not None:
                    self._validate_health_total_values(
                        checkpoint,
                        source_requests=total_source_requests,
                        window_traces=total_window_traces,
                        retry_attempts=total_retry_attempts,
                        windows_completed=total_windows_completed,
                        records_completed=total_records_completed,
                        splits_completed=total_splits_completed,
                    )
            return PublicTradeCollectionHealthSummary(
                job_id=job_id,
                observation_count=observation_count,
                healthy_count=healthy_count,
                degraded_count=degraded_count,
                unavailable_count=unavailable_count,
                accepted_count=accepted_count,
                total_source_requests=total_source_requests,
                total_window_traces=total_window_traces,
                total_retry_attempts=total_retry_attempts,
                total_windows_completed=total_windows_completed,
                total_records_completed=total_records_completed,
                total_splits_completed=total_splits_completed,
                total_retry_delay_seconds=total_retry_delay_seconds,
            )
        except SQLitePublicTradeCollectionStorageError:
            raise
        except ValidationError as error:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade source-health summary violates its contract",
            ) from error
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("public-trade source health summary failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {
                    0,
                    SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION,
                }:
                    raise self._unsupported_schema(
                        f"database schema version {version} is not supported"
                    )
                if version == SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION:
                    self._validate_storage_marker(connection)
                    connection.commit()
                    return

                existing_tables = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
                if existing_tables:
                    raise self._unsupported_schema(
                        "unversioned database already contains unrelated tables"
                    )

                for statement in self._schema_statements():
                    connection.execute(statement)
                connection.execute(
                    """
                    INSERT INTO public_trade_collection_metadata (
                        storage_format, schema_version
                    ) VALUES (?, ?)
                    """,
                    (
                        SQLITE_PUBLIC_TRADE_COLLECTION_STORAGE_FORMAT,
                        SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION,
                    ),
                )
                connection.execute(
                    f"PRAGMA user_version = {SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION}"
                )
                connection.commit()
        except SQLitePublicTradeCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure(
                "public-trade control storage initialization failed"
            ) from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _schema_statements() -> tuple[str, ...]:
        return (
            """
            CREATE TABLE public_trade_collection_metadata (
                storage_format TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE public_trade_collection_jobs (
                job_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                venue TEXT NOT NULL,
                instrument TEXT NOT NULL,
                provider_symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                policy_fingerprint TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end_exclusive TEXT NOT NULL,
                next_window_start TEXT NOT NULL,
                pending_window_end_exclusive TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                lease_owner TEXT,
                lease_token TEXT,
                lease_expires_at TEXT,
                windows_completed INTEGER NOT NULL,
                records_completed INTEGER NOT NULL,
                source_requests INTEGER NOT NULL,
                window_traces INTEGER NOT NULL,
                retry_attempts INTEGER NOT NULL,
                splits_completed INTEGER NOT NULL,
                last_failure_code TEXT,
                last_stop_reason TEXT,
                record_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE public_trade_collection_transitions (
                job_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                status TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                actor_lease_token TEXT,
                record_json TEXT NOT NULL,
                PRIMARY KEY (job_id, version),
                FOREIGN KEY (job_id)
                    REFERENCES public_trade_collection_jobs(job_id)
            )
            """,
            """
            CREATE TABLE public_trade_collection_leases (
                job_id TEXT NOT NULL,
                lease_token TEXT NOT NULL,
                lease_owner TEXT NOT NULL,
                acquired_version INTEGER NOT NULL,
                acquired_at TEXT NOT NULL,
                PRIMARY KEY (job_id, lease_token),
                UNIQUE (job_id, acquired_version),
                FOREIGN KEY (job_id)
                    REFERENCES public_trade_collection_jobs(job_id)
            )
            """,
            """
            CREATE TABLE public_trade_source_health (
                observation_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                checkpoint_version INTEGER NOT NULL CHECK (checkpoint_version >= 2),
                source TEXT NOT NULL,
                venue TEXT NOT NULL,
                instrument TEXT NOT NULL,
                provider_symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                range_start TEXT NOT NULL,
                range_end_exclusive TEXT NOT NULL,
                next_window_start TEXT NOT NULL,
                pending_window_end_exclusive TEXT,
                observed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
                source_requests INTEGER NOT NULL CHECK (source_requests > 0),
                window_traces INTEGER NOT NULL CHECK (window_traces > 0),
                windows_completed INTEGER NOT NULL CHECK (windows_completed >= 0),
                records_completed INTEGER NOT NULL CHECK (records_completed >= 0),
                splits_completed INTEGER NOT NULL CHECK (splits_completed >= 0),
                retry_attempts INTEGER NOT NULL CHECK (retry_attempts >= 0),
                retry_delay_total_seconds REAL NOT NULL
                    CHECK (retry_delay_total_seconds >= 0),
                failure_code TEXT,
                stop_reason TEXT,
                record_json TEXT NOT NULL,
                UNIQUE (job_id, checkpoint_version),
                FOREIGN KEY (job_id)
                    REFERENCES public_trade_collection_jobs(job_id)
            )
            """,
            """
            CREATE INDEX public_trade_collection_jobs_status_index
            ON public_trade_collection_jobs (
                status, source, venue, instrument, instrument_type
            )
            """,
        )

    @classmethod
    def _validate_storage_marker(cls, connection: sqlite3.Connection) -> None:
        cls._validate_schema_objects(connection)
        rows = connection.execute(
            """
            SELECT storage_format, schema_version
            FROM public_trade_collection_metadata
            """
        ).fetchall()
        if (
            len(rows) != 1
            or str(rows[0]["storage_format"]) != SQLITE_PUBLIC_TRADE_COLLECTION_STORAGE_FORMAT
            or rows[0]["schema_version"] != SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION
        ):
            raise cls._unsupported_schema("public-trade storage marker is not supported")

    @classmethod
    def _validate_schema_objects(cls, connection: sqlite3.Connection) -> None:
        """Require the exact versioned DDL, including constraints and indexes."""

        statements = cls._schema_statements()
        expected = {
            ("table", "public_trade_collection_metadata"): statements[0],
            ("table", "public_trade_collection_jobs"): statements[1],
            ("table", "public_trade_collection_transitions"): statements[2],
            ("table", "public_trade_collection_leases"): statements[3],
            ("table", "public_trade_source_health"): statements[4],
            ("index", "public_trade_collection_jobs_status_index"): statements[5],
        }
        rows = connection.execute(
            """
            SELECT type, name, sql
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL
            """
        ).fetchall()
        actual = {(str(row["type"]), str(row["name"])): str(row["sql"]) for row in rows}
        if actual.keys() != expected.keys():
            raise cls._unsupported_schema(
                "public-trade control database objects do not match its schema version"
            )
        for identity, expected_sql in expected.items():
            if cls._canonical_schema_sql(actual[identity]) != cls._canonical_schema_sql(
                expected_sql
            ):
                raise cls._unsupported_schema(
                    f"public-trade object {identity[1]} does not match schema version "
                    f"{SQLITE_PUBLIC_TRADE_COLLECTION_SCHEMA_VERSION}"
                )

    @staticmethod
    def _canonical_schema_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _insert_checkpoint(
        cls,
        connection: sqlite3.Connection,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> None:
        columns = ", ".join(_CHECKPOINT_COLUMNS)
        placeholders = ", ".join("?" for _ in _CHECKPOINT_COLUMNS)
        connection.execute(
            f"""
            INSERT INTO public_trade_collection_jobs ({columns})
            VALUES ({placeholders})
            """,
            cls._checkpoint_projection(checkpoint),
        )

    @classmethod
    def _insert_transition(
        cls,
        connection: sqlite3.Connection,
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        actor_lease_token: UUID | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO public_trade_collection_transitions (
                job_id, version, status, recorded_at,
                actor_lease_token, record_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(checkpoint.job_id),
                checkpoint.version,
                checkpoint.status.value,
                cls._timestamp(checkpoint.updated_at),
                cls._uuid_or_none(actor_lease_token),
                checkpoint.model_dump_json(),
            ),
        )

    @classmethod
    def _insert_health(
        cls,
        connection: sqlite3.Connection,
        health: PublicTradeSourceHealthObservation,
    ) -> None:
        columns = ", ".join(_HEALTH_COLUMNS)
        placeholders = ", ".join("?" for _ in _HEALTH_COLUMNS)
        connection.execute(
            f"""
            INSERT INTO public_trade_source_health ({columns})
            VALUES ({placeholders})
            """,
            cls._health_projection(health),
        )

    @classmethod
    def _checkpoint_projection(
        cls,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> tuple[object, ...]:
        return (
            str(checkpoint.job_id),
            checkpoint.version,
            checkpoint.status.value,
            checkpoint.source,
            checkpoint.venue,
            checkpoint.instrument,
            checkpoint.provider_symbol,
            checkpoint.instrument_type.value,
            checkpoint.policy_fingerprint,
            cls._timestamp(checkpoint.window_start),
            cls._timestamp(checkpoint.window_end_exclusive),
            cls._timestamp(checkpoint.next_window_start),
            cls._timestamp_or_none(checkpoint.pending_window_end_exclusive),
            cls._timestamp(checkpoint.created_at),
            cls._timestamp(checkpoint.updated_at),
            checkpoint.lease_owner,
            cls._uuid_or_none(checkpoint.lease_token),
            cls._timestamp_or_none(checkpoint.lease_expires_at),
            checkpoint.windows_completed,
            checkpoint.records_completed,
            checkpoint.source_requests,
            checkpoint.window_traces,
            checkpoint.retry_attempts,
            checkpoint.splits_completed,
            checkpoint.last_failure_code,
            checkpoint.last_stop_reason,
            checkpoint.model_dump_json(),
        )

    @classmethod
    def _health_projection(
        cls,
        health: PublicTradeSourceHealthObservation,
    ) -> tuple[object, ...]:
        return (
            str(health.observation_id),
            str(health.job_id),
            health.checkpoint_version,
            health.source,
            health.venue,
            health.instrument,
            health.provider_symbol,
            health.instrument_type.value,
            cls._timestamp(health.range_start),
            cls._timestamp(health.range_end_exclusive),
            cls._timestamp(health.next_window_start),
            cls._timestamp_or_none(health.pending_window_end_exclusive),
            cls._timestamp(health.observed_at),
            health.status.value,
            int(health.accepted),
            health.source_requests,
            health.window_traces,
            health.windows_completed,
            health.records_completed,
            health.splits_completed,
            len(health.retry_delays_seconds),
            sum(health.retry_delays_seconds),
            health.failure_code,
            health.stop_reason,
            health.model_dump_json(),
        )

    @classmethod
    def _checkpoint_from_row(
        cls,
        row: sqlite3.Row,
    ) -> PublicTradeCollectionCheckpoint:
        checkpoint = cls._checkpoint_from_json(str(row["record_json"]))
        expected = cls._checkpoint_projection(checkpoint)
        try:
            stored: tuple[object, ...] = (
                str(row["job_id"]),
                int(row["version"]),
                str(row["status"]),
                str(row["source"]),
                str(row["venue"]),
                str(row["instrument"]),
                str(row["provider_symbol"]),
                str(row["instrument_type"]),
                str(row["policy_fingerprint"]),
                str(row["window_start"]),
                str(row["window_end_exclusive"]),
                str(row["next_window_start"]),
                cls._stored_optional_text(row["pending_window_end_exclusive"]),
                str(row["created_at"]),
                str(row["updated_at"]),
                cls._stored_optional_text(row["lease_owner"]),
                cls._stored_optional_text(row["lease_token"]),
                cls._stored_optional_text(row["lease_expires_at"]),
                int(row["windows_completed"]),
                int(row["records_completed"]),
                int(row["source_requests"]),
                int(row["window_traces"]),
                int(row["retry_attempts"]),
                int(row["splits_completed"]),
                cls._stored_optional_text(row["last_failure_code"]),
                cls._stored_optional_text(row["last_stop_reason"]),
                str(row["record_json"]),
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade checkpoint projection contains invalid values",
            ) from error
        if expected != stored:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade checkpoint projection does not match canonical content",
            )
        return checkpoint

    @classmethod
    def _health_from_row(
        cls,
        row: sqlite3.Row,
    ) -> PublicTradeSourceHealthObservation:
        health = cls._health_from_json(str(row["record_json"]))
        expected = cls._health_projection(health)
        try:
            stored: tuple[object, ...] = (
                str(row["observation_id"]),
                str(row["job_id"]),
                int(row["checkpoint_version"]),
                str(row["source"]),
                str(row["venue"]),
                str(row["instrument"]),
                str(row["provider_symbol"]),
                str(row["instrument_type"]),
                str(row["range_start"]),
                str(row["range_end_exclusive"]),
                str(row["next_window_start"]),
                cls._stored_optional_text(row["pending_window_end_exclusive"]),
                str(row["observed_at"]),
                str(row["status"]),
                int(row["accepted"]),
                int(row["source_requests"]),
                int(row["window_traces"]),
                int(row["windows_completed"]),
                int(row["records_completed"]),
                int(row["splits_completed"]),
                int(row["retry_attempts"]),
                float(row["retry_delay_total_seconds"]),
                cls._stored_optional_text(row["failure_code"]),
                cls._stored_optional_text(row["stop_reason"]),
                str(row["record_json"]),
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade source-health projection contains invalid values",
            ) from error
        if expected != stored:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade source-health projection does not match canonical content",
            )
        return health

    @staticmethod
    def _checkpoint_from_json(value: str) -> PublicTradeCollectionCheckpoint:
        try:
            return PublicTradeCollectionCheckpoint.model_validate_json(value)
        except ValidationError as error:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored public-trade checkpoint violates its contract",
            ) from error

    @staticmethod
    def _health_from_json(value: str) -> PublicTradeSourceHealthObservation:
        try:
            return PublicTradeSourceHealthObservation.model_validate_json(value)
        except ValidationError as error:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored public-trade source-health observation violates its contract",
            ) from error

    @staticmethod
    def _validate_lease_authority(
        previous: PublicTradeCollectionCheckpoint,
        current: PublicTradeCollectionCheckpoint,
        *,
        expected_lease_token: UUID | None,
    ) -> None:
        if previous.status is not CollectionJobStatus.RUNNING:
            if expected_lease_token is not None:
                raise ValueError("non-running public-trade job has no lease token to authorize")
            return

        if previous.lease_token is None or previous.lease_expires_at is None:
            raise ValueError("running public-trade job is missing durable lease authority")
        expired_takeover = (
            current.status is CollectionJobStatus.RUNNING
            and current.lease_token != previous.lease_token
            and previous.lease_expires_at <= current.updated_at
        )
        if expired_takeover:
            if expected_lease_token is not None:
                raise ValueError("expired public-trade lease takeover must use a new token")
            return
        if expected_lease_token != previous.lease_token:
            raise ValueError("public-trade transition requires the active lease token")

    @staticmethod
    def _acquires_new_lease(
        previous: PublicTradeCollectionCheckpoint,
        current: PublicTradeCollectionCheckpoint,
    ) -> bool:
        """Identify claims and expired takeovers without treating renewals as claims."""

        return current.status is CollectionJobStatus.RUNNING and (
            previous.status is not CollectionJobStatus.RUNNING
            or current.lease_token != previous.lease_token
        )

    @staticmethod
    def _validate_fresh_lease_token(
        connection: sqlite3.Connection,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> None:
        """Reject fencing-token reuse across every claim in one durable job."""

        if checkpoint.lease_token is None:
            raise ValueError("public-trade lease claim is missing its token")
        existing = connection.execute(
            """
            SELECT 1
            FROM public_trade_collection_leases
            WHERE job_id = ? AND lease_token = ?
            """,
            (str(checkpoint.job_id), str(checkpoint.lease_token)),
        ).fetchone()
        if existing is not None:
            raise ValueError("public-trade lease claim requires a fresh UUID token")

    @classmethod
    def _insert_lease_acquisition(
        cls,
        connection: sqlite3.Connection,
        checkpoint: PublicTradeCollectionCheckpoint,
    ) -> None:
        """Append the newly acquired fencing authority in the same transaction."""

        if checkpoint.lease_token is None or checkpoint.lease_owner is None:
            raise ValueError("public-trade lease claim is incomplete")
        connection.execute(
            """
            INSERT INTO public_trade_collection_leases (
                job_id, lease_token, lease_owner, acquired_version, acquired_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(checkpoint.job_id),
                str(checkpoint.lease_token),
                checkpoint.lease_owner,
                checkpoint.version,
                cls._timestamp(checkpoint.updated_at),
            ),
        )

    @staticmethod
    def _validate_transition_without_health(
        previous: PublicTradeCollectionCheckpoint,
        current: PublicTradeCollectionCheckpoint,
    ) -> None:
        outcome_fields = (
            "next_window_start",
            "pending_window_end_exclusive",
            "windows_completed",
            "records_completed",
            "source_requests",
            "window_traces",
            "retry_attempts",
            "splits_completed",
            "last_failure_code",
            "last_stop_reason",
        )
        ignored_fields: set[str] = set()
        if (
            previous.status in {CollectionJobStatus.FAILED, CollectionJobStatus.PAUSED}
            and current.status is CollectionJobStatus.RUNNING
        ):
            ignored_fields = {"last_failure_code", "last_stop_reason"}
        changed = any(
            getattr(previous, field) != getattr(current, field)
            for field in outcome_fields
            if field not in ignored_fields
        )
        if changed:
            raise ValueError("public-trade work transition requires source-health evidence")

    @staticmethod
    def _validate_health_transition(
        previous: PublicTradeCollectionCheckpoint,
        current: PublicTradeCollectionCheckpoint,
        health: PublicTradeSourceHealthObservation,
    ) -> None:
        expected_stream = (
            previous.job_id,
            previous.source,
            previous.venue,
            previous.instrument,
            previous.provider_symbol,
            previous.instrument_type,
        )
        actual_stream = (
            health.job_id,
            health.source,
            health.venue,
            health.instrument,
            health.provider_symbol,
            health.instrument_type,
        )
        if actual_stream != expected_stream:
            raise ValueError("public-trade source health does not match its collection job")
        if health.checkpoint_version != current.version:
            raise ValueError("public-trade source health must identify its checkpoint transition")
        if health.range_start != previous.next_window_start:
            raise ValueError("public-trade health range must begin at the durable cursor")
        if health.range_end_exclusive > previous.window_end_exclusive:
            raise ValueError("public-trade health range exceeds the immutable job")
        if (
            previous.pending_window_end_exclusive is not None
            and health.range_end_exclusive != previous.pending_window_end_exclusive
        ):
            raise ValueError("public-trade health must resume the exact pending window")
        if health.observed_at != current.updated_at:
            raise ValueError("public-trade health time must match its checkpoint transition")

        deltas = (
            current.source_requests - previous.source_requests,
            current.window_traces - previous.window_traces,
            current.retry_attempts - previous.retry_attempts,
            current.windows_completed - previous.windows_completed,
            current.records_completed - previous.records_completed,
            current.splits_completed - previous.splits_completed,
        )
        health_work = (
            health.source_requests,
            health.window_traces,
            len(health.retry_delays_seconds),
            health.windows_completed,
            health.records_completed,
            health.splits_completed,
        )
        if deltas != health_work:
            raise ValueError("public-trade health counters must equal checkpoint work deltas")
        if current.next_window_start != health.next_window_start:
            raise ValueError("public-trade health cursor must match its checkpoint transition")

        if health.accepted:
            if current.pending_window_end_exclusive is not None or current.status in {
                CollectionJobStatus.FAILED,
                CollectionJobStatus.PAUSED,
            }:
                raise ValueError("accepted public-trade health cannot leave work pending")
        elif health.failure_code is None:
            if (
                current.status is not CollectionJobStatus.PAUSED
                or current.pending_window_end_exclusive != health.pending_window_end_exclusive
                or current.last_failure_code is not None
                or current.last_stop_reason != health.stop_reason
            ):
                raise ValueError(
                    "controlled public-trade stop must atomically persist matching paused state"
                )
        elif (
            current.status is not CollectionJobStatus.FAILED
            or current.pending_window_end_exclusive != health.pending_window_end_exclusive
            or current.last_failure_code != health.failure_code
            or current.last_stop_reason != health.stop_reason
        ):
            raise ValueError(
                "failed public-trade health must atomically persist matching failed state"
            )

    @staticmethod
    def _validate_persisted_health_identity(
        checkpoint: PublicTradeCollectionCheckpoint,
        health: PublicTradeSourceHealthObservation,
    ) -> None:
        if (
            health.job_id,
            health.source,
            health.venue,
            health.instrument,
            health.provider_symbol,
            health.instrument_type,
        ) != (
            checkpoint.job_id,
            checkpoint.source,
            checkpoint.venue,
            checkpoint.instrument,
            checkpoint.provider_symbol,
            checkpoint.instrument_type,
        ):
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored public-trade health stream differs from its checkpoint",
            )
        if not (
            checkpoint.window_start
            <= health.range_start
            < health.range_end_exclusive
            <= checkpoint.window_end_exclusive
        ):
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored public-trade health range exceeds its checkpoint",
            )
        if health.checkpoint_version > checkpoint.version:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored public-trade health version exceeds its checkpoint",
            )

    @staticmethod
    def _validate_health_sequence_step(
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        previous: PublicTradeSourceHealthObservation | None,
        current: PublicTradeSourceHealthObservation,
    ) -> None:
        expected_start = checkpoint.window_start if previous is None else previous.next_window_start
        if current.range_start != expected_start:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade health history has a cursor gap",
            )
        if previous is None:
            return
        if current.checkpoint_version <= previous.checkpoint_version:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade health versions are not strictly ordered",
            )
        if current.observed_at < previous.observed_at:
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade health observation time regressed",
            )
        if (
            previous.pending_window_end_exclusive is not None
            and current.range_end_exclusive != previous.pending_window_end_exclusive
        ):
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade health history skipped its retained pending window",
            )

    @staticmethod
    def _validate_health_total_values(
        checkpoint: PublicTradeCollectionCheckpoint,
        *,
        source_requests: int,
        window_traces: int,
        retry_attempts: int,
        windows_completed: int,
        records_completed: int,
        splits_completed: int,
    ) -> None:
        if (
            source_requests,
            window_traces,
            retry_attempts,
            windows_completed,
            records_completed,
            splits_completed,
        ) != (
            checkpoint.source_requests,
            checkpoint.window_traces,
            checkpoint.retry_attempts,
            checkpoint.windows_completed,
            checkpoint.records_completed,
            checkpoint.splits_completed,
        ):
            raise SQLitePublicTradeCollectionStorageError(
                SQLitePublicTradeCollectionStorageErrorCode.CORRUPT_RECORD,
                "public-trade health totals do not match the current checkpoint",
            )

    @staticmethod
    def _is_pristine(checkpoint: PublicTradeCollectionCheckpoint) -> bool:
        return (
            checkpoint.status is CollectionJobStatus.PENDING
            and checkpoint.version == 1
            and checkpoint.created_at == checkpoint.updated_at
            and checkpoint.next_window_start == checkpoint.window_start
            and checkpoint.pending_window_end_exclusive is None
            and checkpoint.lease_owner is None
            and checkpoint.lease_token is None
            and checkpoint.lease_expires_at is None
            and checkpoint.windows_completed == 0
            and checkpoint.records_completed == 0
            and checkpoint.source_requests == 0
            and checkpoint.window_traces == 0
            and checkpoint.retry_attempts == 0
            and checkpoint.splits_completed == 0
            and checkpoint.last_failure_code is None
            and checkpoint.last_stop_reason is None
        )

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    @classmethod
    def _timestamp_or_none(cls, value: datetime | None) -> str | None:
        return None if value is None else cls._timestamp(value)

    @staticmethod
    def _uuid_or_none(value: UUID | None) -> str | None:
        return None if value is None else str(value)

    @staticmethod
    def _stored_optional_text(value: object | None) -> str | None:
        return None if value is None else str(value)

    @staticmethod
    def _unsupported_schema(detail: str) -> SQLitePublicTradeCollectionStorageError:
        return SQLitePublicTradeCollectionStorageError(
            SQLitePublicTradeCollectionStorageErrorCode.UNSUPPORTED_SCHEMA,
            detail,
        )

    @staticmethod
    def _storage_failure(detail: str) -> SQLitePublicTradeCollectionStorageError:
        return SQLitePublicTradeCollectionStorageError(
            SQLitePublicTradeCollectionStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
