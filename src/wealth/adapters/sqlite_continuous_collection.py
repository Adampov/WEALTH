"""Durable SQLite cursors for supervised continuous candle polling."""

import sqlite3
from contextlib import closing
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.continuous_collection import (
    ContinuousCollectionCheckpoint,
    ContinuousCollectionStatus,
    validate_continuous_collection_transition,
)
from wealth.ports.continuous_collection import (
    ContinuousCollectionWriteResult,
    ContinuousCollectionWriteStatus,
)

SQLITE_CONTINUOUS_COLLECTION_SCHEMA_VERSION = 1


class SQLiteContinuousCollectionStorageErrorCode(StrEnum):
    """Machine-readable failures at the continuous-control boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteContinuousCollectionStorageError(RuntimeError):
    """Fail explicitly when a continuous cursor cannot be trusted."""

    def __init__(
        self,
        code: SQLiteContinuousCollectionStorageErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteContinuousCollectionCheckpointStore:
    """Compare-and-swap current cursors with append-only transition history."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteContinuousCollectionStorageError(
                SQLiteContinuousCollectionStorageErrorCode.INVALID_PATH,
                "continuous collection storage requires a dedicated file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def create(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> ContinuousCollectionWriteResult:
        """Insert pristine active state without replacing an existing cursor."""

        if (
            checkpoint.status is not ContinuousCollectionStatus.ACTIVE
            or checkpoint.version != 1
            or checkpoint.next_window_start != checkpoint.window_start
            or checkpoint.active_job_id is not None
            or checkpoint.cycles_completed != 0
            or checkpoint.consecutive_failures != 0
        ):
            raise ValueError(
                "new continuous collection must be a pristine active version-one checkpoint"
            )
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT record_json
                    FROM continuous_collection_checkpoints
                    WHERE collection_id = ?
                    """,
                    (str(checkpoint.collection_id),),
                ).fetchone()
                if row is not None:
                    existing = self._checkpoint_from_json(str(row["record_json"]))
                    connection.rollback()
                    return ContinuousCollectionWriteResult(
                        status=(
                            ContinuousCollectionWriteStatus.DUPLICATE
                            if existing == checkpoint
                            else ContinuousCollectionWriteStatus.CONFLICT
                        ),
                        collection_id=checkpoint.collection_id,
                        current_version=existing.version,
                    )

                self._insert_checkpoint(connection, checkpoint)
                self._insert_transition(connection, checkpoint)
                connection.commit()
                return ContinuousCollectionWriteResult(
                    status=ContinuousCollectionWriteStatus.INSERTED,
                    collection_id=checkpoint.collection_id,
                    current_version=checkpoint.version,
                )
        except SQLiteContinuousCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("continuous collection creation failed") from error

    def get(self, collection_id: UUID) -> ContinuousCollectionCheckpoint | None:
        """Reload a current cursor and verify all indexed projections."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT collection_id, version, status, source, venue, instrument,
                           timeframe, next_window_start, active_job_id,
                           active_window_end_exclusive, next_retry_at, record_json
                    FROM continuous_collection_checkpoints
                    WHERE collection_id = ?
                    """,
                    (str(collection_id),),
                ).fetchone()
            return None if row is None else self._checkpoint_from_row(row)
        except SQLiteContinuousCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("continuous collection read failed") from error

    def transition(
        self,
        checkpoint: ContinuousCollectionCheckpoint,
        *,
        expected_version: int,
    ) -> ContinuousCollectionWriteResult:
        """Atomically replace one expected version and retain its transition."""

        if expected_version < 1 or checkpoint.version != expected_version + 1:
            raise ValueError("continuous transition requires the exact next version")
        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    """
                    SELECT collection_id, version, status, source, venue, instrument,
                           timeframe, next_window_start, active_job_id,
                           active_window_end_exclusive, next_retry_at, record_json
                    FROM continuous_collection_checkpoints
                    WHERE collection_id = ?
                    """,
                    (str(checkpoint.collection_id),),
                ).fetchone()
                if row is None:
                    connection.rollback()
                    return ContinuousCollectionWriteResult(
                        status=ContinuousCollectionWriteStatus.CONFLICT,
                        collection_id=checkpoint.collection_id,
                        current_version=0,
                    )
                previous = self._checkpoint_from_row(row)
                if previous.version != expected_version:
                    connection.rollback()
                    return ContinuousCollectionWriteResult(
                        status=ContinuousCollectionWriteStatus.CONFLICT,
                        collection_id=checkpoint.collection_id,
                        current_version=previous.version,
                    )
                validate_continuous_collection_transition(previous, checkpoint)

                cursor = connection.execute(
                    """
                    UPDATE continuous_collection_checkpoints
                    SET version = ?,
                        status = ?,
                        source = ?,
                        venue = ?,
                        instrument = ?,
                        timeframe = ?,
                        next_window_start = ?,
                        active_job_id = ?,
                        active_window_end_exclusive = ?,
                        next_retry_at = ?,
                        record_json = ?
                    WHERE collection_id = ? AND version = ?
                    """,
                    (
                        checkpoint.version,
                        checkpoint.status.value,
                        checkpoint.source,
                        checkpoint.venue,
                        checkpoint.instrument,
                        checkpoint.timeframe.value,
                        checkpoint.next_window_start.isoformat(),
                        (
                            None
                            if checkpoint.active_job_id is None
                            else str(checkpoint.active_job_id)
                        ),
                        (
                            None
                            if checkpoint.active_window_end_exclusive is None
                            else checkpoint.active_window_end_exclusive.isoformat()
                        ),
                        (
                            None
                            if checkpoint.next_retry_at is None
                            else checkpoint.next_retry_at.isoformat()
                        ),
                        checkpoint.model_dump_json(),
                        str(checkpoint.collection_id),
                        expected_version,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    current = self.get(checkpoint.collection_id)
                    return ContinuousCollectionWriteResult(
                        status=ContinuousCollectionWriteStatus.CONFLICT,
                        collection_id=checkpoint.collection_id,
                        current_version=0 if current is None else current.version,
                    )
                self._insert_transition(connection, checkpoint)
                connection.commit()
                return ContinuousCollectionWriteResult(
                    status=ContinuousCollectionWriteStatus.UPDATED,
                    collection_id=checkpoint.collection_id,
                    current_version=checkpoint.version,
                )
        except (SQLiteContinuousCollectionStorageError, ValueError):
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("continuous collection transition failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_CONTINUOUS_COLLECTION_SCHEMA_VERSION}:
                    raise SQLiteContinuousCollectionStorageError(
                        SQLiteContinuousCollectionStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_CONTINUOUS_COLLECTION_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE continuous_collection_checkpoints (
                        collection_id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('active', 'paused')),
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        next_window_start TEXT NOT NULL,
                        active_job_id TEXT,
                        active_window_end_exclusive TEXT,
                        next_retry_at TEXT,
                        record_json TEXT NOT NULL
                    );

                    CREATE TABLE continuous_collection_transitions (
                        collection_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        PRIMARY KEY (collection_id, version),
                        FOREIGN KEY (collection_id)
                            REFERENCES continuous_collection_checkpoints(collection_id)
                    );

                    CREATE INDEX continuous_collection_status_stream_index
                    ON continuous_collection_checkpoints (
                        status,
                        source,
                        venue,
                        instrument,
                        timeframe
                    );

                    CREATE INDEX continuous_collection_retry_index
                    ON continuous_collection_checkpoints (status, next_retry_at);
                    """
                )
                connection.execute(
                    f"PRAGMA user_version = {SQLITE_CONTINUOUS_COLLECTION_SCHEMA_VERSION}"
                )
                connection.commit()
        except SQLiteContinuousCollectionStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("continuous collection initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _insert_checkpoint(
        connection: sqlite3.Connection,
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> None:
        connection.execute(
            """
            INSERT INTO continuous_collection_checkpoints (
                collection_id, version, status, source, venue, instrument,
                timeframe, next_window_start, active_job_id,
                active_window_end_exclusive, next_retry_at, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(checkpoint.collection_id),
                checkpoint.version,
                checkpoint.status.value,
                checkpoint.source,
                checkpoint.venue,
                checkpoint.instrument,
                checkpoint.timeframe.value,
                checkpoint.next_window_start.isoformat(),
                None,
                None,
                None,
                checkpoint.model_dump_json(),
            ),
        )

    @staticmethod
    def _insert_transition(
        connection: sqlite3.Connection,
        checkpoint: ContinuousCollectionCheckpoint,
    ) -> None:
        connection.execute(
            """
            INSERT INTO continuous_collection_transitions (
                collection_id, version, status, recorded_at, record_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(checkpoint.collection_id),
                checkpoint.version,
                checkpoint.status.value,
                checkpoint.updated_at.isoformat(),
                checkpoint.model_dump_json(),
            ),
        )

    @classmethod
    def _checkpoint_from_row(
        cls,
        row: sqlite3.Row,
    ) -> ContinuousCollectionCheckpoint:
        checkpoint = cls._checkpoint_from_json(str(row["record_json"]))
        indexed_values = (
            str(checkpoint.collection_id),
            checkpoint.version,
            checkpoint.status.value,
            checkpoint.source,
            checkpoint.venue,
            checkpoint.instrument,
            checkpoint.timeframe.value,
            checkpoint.next_window_start.isoformat(),
            None if checkpoint.active_job_id is None else str(checkpoint.active_job_id),
            (
                None
                if checkpoint.active_window_end_exclusive is None
                else checkpoint.active_window_end_exclusive.isoformat()
            ),
            None if checkpoint.next_retry_at is None else checkpoint.next_retry_at.isoformat(),
        )
        stored_values = (
            str(row["collection_id"]),
            int(row["version"]),
            str(row["status"]),
            str(row["source"]),
            str(row["venue"]),
            str(row["instrument"]),
            str(row["timeframe"]),
            str(row["next_window_start"]),
            None if row["active_job_id"] is None else str(row["active_job_id"]),
            (
                None
                if row["active_window_end_exclusive"] is None
                else str(row["active_window_end_exclusive"])
            ),
            None if row["next_retry_at"] is None else str(row["next_retry_at"]),
        )
        if indexed_values != stored_values:
            raise SQLiteContinuousCollectionStorageError(
                SQLiteContinuousCollectionStorageErrorCode.CORRUPT_RECORD,
                "continuous collection index does not match canonical content",
            )
        return checkpoint

    @staticmethod
    def _checkpoint_from_json(value: str) -> ContinuousCollectionCheckpoint:
        try:
            return ContinuousCollectionCheckpoint.model_validate_json(value)
        except ValidationError as error:
            raise SQLiteContinuousCollectionStorageError(
                SQLiteContinuousCollectionStorageErrorCode.CORRUPT_RECORD,
                "stored continuous collection checkpoint violates its contract",
            ) from error

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteContinuousCollectionStorageError:
        return SQLiteContinuousCollectionStorageError(
            SQLiteContinuousCollectionStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
