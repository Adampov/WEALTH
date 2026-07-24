"""Durable SQLite checkpoints and health evidence for collection jobs."""

import sqlite3
from contextlib import closing
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.collection import (
    CollectionHealthSummary,
    CollectionJobStatus,
    HistoricalCollectionJob,
    SourceHealthObservation,
    validate_collection_transition,
)
from wealth.ports.collection import (
    CollectionCheckpointWriteResult,
    CollectionCheckpointWriteStatus,
)

SQLITE_COLLECTION_SCHEMA_VERSION = 1


class SQLiteCollectionStorageErrorCode(StrEnum):
    """Machine-readable failures at the collection-control boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteCollectionStorageError(RuntimeError):
    """Fail explicitly when collection control state cannot be trusted."""

    def __init__(self, code: SQLiteCollectionStorageErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteCollectionCheckpointStore:
    """File-backed compare-and-swap store with append-only transition evidence."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteCollectionStorageError(
                SQLiteCollectionStorageErrorCode.INVALID_PATH,
                "durable collection storage requires a dedicated file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def create(
        self,
        job: HistoricalCollectionJob,
    ) -> CollectionCheckpointWriteResult:
        """Insert one pristine pending checkpoint without replacing an existing job."""

        if (
            job.status is not CollectionJobStatus.PENDING
            or job.version != 1
            or job.next_window_start != job.window_start
        ):
            raise ValueError("new collection job must be a pristine version-one checkpoint")
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT record_json FROM collection_jobs WHERE job_id = ?",
                    (str(job.job_id),),
                ).fetchone()
                if row is not None:
                    existing = self._job_from_json(str(row["record_json"]))
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=(
                            CollectionCheckpointWriteStatus.DUPLICATE
                            if existing == job
                            else CollectionCheckpointWriteStatus.CONFLICT
                        ),
                        job_id=job.job_id,
                        current_version=existing.version,
                    )

                self._insert_job(connection, job)
                self._insert_transition(connection, job)
                connection.commit()
                return CollectionCheckpointWriteResult(
                    status=CollectionCheckpointWriteStatus.INSERTED,
                    job_id=job.job_id,
                    current_version=job.version,
                )
        except SQLiteCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collection job creation failed") from error

    def get(self, job_id: UUID) -> HistoricalCollectionJob | None:
        """Reload a checkpoint and verify its indexed fields."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT job_id, version, status, source, venue, instrument,
                           timeframe, next_window_start, record_json
                    FROM collection_jobs
                    WHERE job_id = ?
                    """,
                    (str(job_id),),
                ).fetchone()
            return None if row is None else self._job_from_row(row)
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collection checkpoint read failed") from error

    def transition(
        self,
        job: HistoricalCollectionJob,
        *,
        expected_version: int,
        health: SourceHealthObservation | None = None,
    ) -> CollectionCheckpointWriteResult:
        """Atomically replace one expected version and append its audit evidence."""

        if expected_version < 1 or job.version != expected_version + 1:
            raise ValueError("checkpoint transition requires the exact next version")
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT job_id, version, status, source, venue, instrument,
                           timeframe, next_window_start, record_json
                    FROM collection_jobs
                    WHERE job_id = ?
                    """,
                    (str(job.job_id),),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=job.job_id,
                        current_version=0,
                    )
                previous = self._job_from_row(row)
                if previous.version != expected_version:
                    connection.rollback()
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=job.job_id,
                        current_version=previous.version,
                    )

                validate_collection_transition(previous, job)
                if health is not None:
                    self._validate_health_transition(previous, job, health)

                cursor = connection.execute(
                    """
                    UPDATE collection_jobs
                    SET version = ?,
                        status = ?,
                        source = ?,
                        venue = ?,
                        instrument = ?,
                        timeframe = ?,
                        next_window_start = ?,
                        record_json = ?
                    WHERE job_id = ? AND version = ?
                    """,
                    (
                        job.version,
                        job.status.value,
                        job.source,
                        job.venue,
                        job.instrument,
                        job.timeframe.value,
                        job.next_window_start.isoformat(),
                        job.model_dump_json(),
                        str(job.job_id),
                        expected_version,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    current = self.get(job.job_id)
                    return CollectionCheckpointWriteResult(
                        status=CollectionCheckpointWriteStatus.CONFLICT,
                        job_id=job.job_id,
                        current_version=0 if current is None else current.version,
                    )
                self._insert_transition(connection, job)
                if health is not None:
                    self._insert_health(connection, health)
                connection.commit()
                return CollectionCheckpointWriteResult(
                    status=CollectionCheckpointWriteStatus.UPDATED,
                    job_id=job.job_id,
                    current_version=job.version,
                )
        except (SQLiteCollectionStorageError, ValueError):
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collection checkpoint transition failed") from error

    def health_for_job(self, job_id: UUID) -> tuple[SourceHealthObservation, ...]:
        """Reload append-only health evidence in deterministic order."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT record_json
                    FROM source_health_observations
                    WHERE job_id = ?
                    ORDER BY observed_at, observation_id
                    """,
                    (str(job_id),),
                ).fetchall()
            return tuple(self._health_from_json(str(row["record_json"])) for row in rows)
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("source health read failed") from error

    def health_summary(self, job_id: UUID) -> CollectionHealthSummary:
        """Aggregate job health in SQLite without materializing all observations."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS observation_count,
                        SUM(CASE WHEN status = 'healthy' THEN 1 ELSE 0 END)
                            AS healthy_count,
                        SUM(CASE WHEN status = 'degraded' THEN 1 ELSE 0 END)
                            AS degraded_count,
                        SUM(CASE WHEN status = 'unavailable' THEN 1 ELSE 0 END)
                            AS unavailable_count,
                        SUM(accepted) AS accepted_count,
                        SUM(attempts) AS total_attempts
                    FROM source_health_observations
                    WHERE job_id = ?
                    """,
                    (str(job_id),),
                ).fetchone()
            if row is None:
                raise AssertionError("SQLite aggregate must return one row")
            return CollectionHealthSummary(
                job_id=job_id,
                observation_count=int(row["observation_count"] or 0),
                healthy_count=int(row["healthy_count"] or 0),
                degraded_count=int(row["degraded_count"] or 0),
                unavailable_count=int(row["unavailable_count"] or 0),
                accepted_count=int(row["accepted_count"] or 0),
                total_attempts=int(row["total_attempts"] or 0),
            )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("source health summary failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_COLLECTION_SCHEMA_VERSION}:
                    raise SQLiteCollectionStorageError(
                        SQLiteCollectionStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_COLLECTION_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE collection_jobs (
                        job_id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        next_window_start TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    );

                    CREATE TABLE collection_transitions (
                        job_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        PRIMARY KEY (job_id, version),
                        FOREIGN KEY (job_id) REFERENCES collection_jobs(job_id)
                    );

                    CREATE TABLE source_health_observations (
                        observation_id TEXT PRIMARY KEY,
                        job_id TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        status TEXT NOT NULL,
                        accepted INTEGER NOT NULL CHECK (accepted IN (0, 1)),
                        attempts INTEGER NOT NULL CHECK (attempts > 0),
                        record_json TEXT NOT NULL,
                        FOREIGN KEY (job_id) REFERENCES collection_jobs(job_id)
                    );

                    CREATE INDEX collection_jobs_status_index
                    ON collection_jobs (status, source, venue, instrument, timeframe);

                    CREATE INDEX source_health_job_time_index
                    ON source_health_observations (job_id, observed_at, observation_id);
                    """
                )
                connection.execute(f"PRAGMA user_version = {SQLITE_COLLECTION_SCHEMA_VERSION}")
                connection.commit()
        except SQLiteCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collection storage initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _insert_job(
        connection: sqlite3.Connection,
        job: HistoricalCollectionJob,
    ) -> None:
        connection.execute(
            """
            INSERT INTO collection_jobs (
                job_id, version, status, source, venue, instrument,
                timeframe, next_window_start, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(job.job_id),
                job.version,
                job.status.value,
                job.source,
                job.venue,
                job.instrument,
                job.timeframe.value,
                job.next_window_start.isoformat(),
                job.model_dump_json(),
            ),
        )

    @staticmethod
    def _insert_transition(
        connection: sqlite3.Connection,
        job: HistoricalCollectionJob,
    ) -> None:
        connection.execute(
            """
            INSERT INTO collection_transitions (
                job_id, version, status, recorded_at, record_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(job.job_id),
                job.version,
                job.status.value,
                job.updated_at.isoformat(),
                job.model_dump_json(),
            ),
        )

    @staticmethod
    def _insert_health(
        connection: sqlite3.Connection,
        health: SourceHealthObservation,
    ) -> None:
        connection.execute(
            """
            INSERT INTO source_health_observations (
                observation_id, job_id, observed_at, status,
                accepted, attempts, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(health.observation_id),
                str(health.job_id),
                health.observed_at.isoformat(),
                health.status.value,
                int(health.accepted),
                health.attempts,
                health.model_dump_json(),
            ),
        )

    @classmethod
    def _job_from_row(cls, row: sqlite3.Row) -> HistoricalCollectionJob:
        job = cls._job_from_json(str(row["record_json"]))
        indexed_values = (
            str(job.job_id),
            job.version,
            job.status.value,
            job.source,
            job.venue,
            job.instrument,
            job.timeframe.value,
            job.next_window_start.isoformat(),
        )
        stored_values = (
            str(row["job_id"]),
            int(row["version"]),
            str(row["status"]),
            str(row["source"]),
            str(row["venue"]),
            str(row["instrument"]),
            str(row["timeframe"]),
            str(row["next_window_start"]),
        )
        if indexed_values != stored_values:
            raise SQLiteCollectionStorageError(
                SQLiteCollectionStorageErrorCode.CORRUPT_RECORD,
                "collection checkpoint index does not match canonical content",
            )
        return job

    @staticmethod
    def _job_from_json(value: str) -> HistoricalCollectionJob:
        try:
            return HistoricalCollectionJob.model_validate_json(value)
        except ValidationError as error:
            raise SQLiteCollectionStorageError(
                SQLiteCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored collection checkpoint violates its contract",
            ) from error

    @staticmethod
    def _health_from_json(value: str) -> SourceHealthObservation:
        try:
            return SourceHealthObservation.model_validate_json(value)
        except ValidationError as error:
            raise SQLiteCollectionStorageError(
                SQLiteCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored source health observation violates its contract",
            ) from error

    @staticmethod
    def _validate_health_transition(
        previous: HistoricalCollectionJob,
        current: HistoricalCollectionJob,
        health: SourceHealthObservation,
    ) -> None:
        expected_stream = (
            previous.job_id,
            previous.source,
            previous.venue,
            previous.instrument,
            previous.timeframe,
        )
        actual_stream = (
            health.job_id,
            health.source,
            health.venue,
            health.instrument,
            health.timeframe,
        )
        if actual_stream != expected_stream:
            raise ValueError("source health observation does not match its collection job")
        if health.page_start != previous.next_window_start:
            raise ValueError("source health page must begin at the durable cursor")
        if health.observed_at != current.updated_at:
            raise ValueError("source health time must match its checkpoint transition")
        if health.accepted:
            if current.next_window_start != health.page_end_exclusive:
                raise ValueError("accepted health page must advance the durable cursor")
        elif (
            current.next_window_start != previous.next_window_start
            or current.status is not CollectionJobStatus.FAILED
        ):
            raise ValueError("rejected health page must fail without advancing the cursor")

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteCollectionStorageError:
        return SQLiteCollectionStorageError(
            SQLiteCollectionStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
