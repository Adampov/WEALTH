"""Durable SQLite lifecycle evidence for local collector service runs."""

import sqlite3
from contextlib import closing
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.collector_service import (
    CollectorServiceHeartbeat,
    CollectorServiceHeartbeatQuery,
    CollectorServiceRunQuery,
    CollectorServiceStatus,
    validate_collector_service_transition,
)
from wealth.ports.collector_service import (
    CollectorServiceHeartbeatWriteResult,
    CollectorServiceHeartbeatWriteStatus,
)

SQLITE_COLLECTOR_SERVICE_SCHEMA_VERSION = 1


class SQLiteCollectorServiceStorageErrorCode(StrEnum):
    """Machine-readable failures at the service-lifecycle boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteCollectorServiceStorageError(RuntimeError):
    """Fail explicitly when collector service evidence cannot be trusted."""

    def __init__(
        self,
        code: SQLiteCollectorServiceStorageErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteCollectorServiceHeartbeatStore:
    """Append lifecycle heartbeats while retaining one validated run projection."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteCollectorServiceStorageError(
                SQLiteCollectorServiceStorageErrorCode.INVALID_PATH,
                "collector service storage requires a dedicated file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def append(
        self,
        heartbeat: CollectorServiceHeartbeat,
    ) -> CollectorServiceHeartbeatWriteResult:
        """Append an exact next observation or expose duplicate/conflict."""

        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                id_row = connection.execute(
                    """
                    SELECT record_json
                    FROM collector_service_heartbeats
                    WHERE heartbeat_id = ?
                    """,
                    (str(heartbeat.heartbeat_id),),
                ).fetchone()
                if id_row is not None:
                    existing = self._heartbeat_from_json(str(id_row["record_json"]))
                    connection.rollback()
                    return self._result(
                        heartbeat,
                        (
                            CollectorServiceHeartbeatWriteStatus.DUPLICATE
                            if existing == heartbeat
                            else CollectorServiceHeartbeatWriteStatus.CONFLICT
                        ),
                        current_sequence=existing.sequence,
                    )

                row = connection.execute(
                    """
                    SELECT run_id, collection_id, worker_id, sequence, status,
                           observed_at, record_json
                    FROM collector_service_runs
                    WHERE run_id = ?
                    """,
                    (str(heartbeat.run_id),),
                ).fetchone()
                if row is None:
                    if (
                        heartbeat.status is not CollectorServiceStatus.STARTING
                        or heartbeat.sequence != 1
                    ):
                        connection.rollback()
                        return self._result(
                            heartbeat,
                            CollectorServiceHeartbeatWriteStatus.CONFLICT,
                            current_sequence=0,
                        )
                    self._insert_run(connection, heartbeat)
                    self._insert_heartbeat(connection, heartbeat)
                    connection.commit()
                    return self._result(
                        heartbeat,
                        CollectorServiceHeartbeatWriteStatus.INSERTED,
                        current_sequence=heartbeat.sequence,
                    )

                previous = self._heartbeat_from_row(row)
                if (
                    heartbeat.run_id != previous.run_id
                    or heartbeat.collection_id != previous.collection_id
                    or heartbeat.worker_id != previous.worker_id
                    or heartbeat.sequence != previous.sequence + 1
                ):
                    connection.rollback()
                    return self._result(
                        heartbeat,
                        CollectorServiceHeartbeatWriteStatus.CONFLICT,
                        current_sequence=previous.sequence,
                    )
                validate_collector_service_transition(previous, heartbeat)
                cursor = connection.execute(
                    """
                    UPDATE collector_service_runs
                    SET sequence = ?,
                        status = ?,
                        observed_at = ?,
                        record_json = ?
                    WHERE run_id = ? AND sequence = ?
                    """,
                    (
                        heartbeat.sequence,
                        heartbeat.status.value,
                        heartbeat.observed_at.isoformat(),
                        heartbeat.model_dump_json(),
                        str(heartbeat.run_id),
                        previous.sequence,
                    ),
                )
                if cursor.rowcount != 1:
                    connection.rollback()
                    return self._result(
                        heartbeat,
                        CollectorServiceHeartbeatWriteStatus.CONFLICT,
                        current_sequence=previous.sequence,
                    )
                self._insert_heartbeat(connection, heartbeat)
                connection.commit()
                return self._result(
                    heartbeat,
                    CollectorServiceHeartbeatWriteStatus.INSERTED,
                    current_sequence=heartbeat.sequence,
                )
        except (SQLiteCollectorServiceStorageError, ValueError):
            raise
        except sqlite3.IntegrityError:
            current = self.current(heartbeat.run_id)
            return self._result(
                heartbeat,
                CollectorServiceHeartbeatWriteStatus.CONFLICT,
                current_sequence=0 if current is None else current.sequence,
            )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collector service heartbeat append failed") from error

    def current(self, run_id: UUID) -> CollectorServiceHeartbeat | None:
        """Reload and validate the latest observation for one run."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT run_id, collection_id, worker_id, sequence, status,
                           observed_at, record_json
                    FROM collector_service_runs
                    WHERE run_id = ?
                    """,
                    (str(run_id),),
                ).fetchone()
            return None if row is None else self._heartbeat_from_row(row)
        except SQLiteCollectorServiceStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collector service current read failed") from error

    def observations(
        self,
        query: CollectorServiceHeartbeatQuery,
    ) -> tuple[CollectorServiceHeartbeat, ...]:
        """Return validated history in strict sequence order."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT run_id, heartbeat_id, sequence, status, observed_at, record_json
                    FROM collector_service_heartbeats
                    WHERE run_id = ?
                    ORDER BY sequence ASC
                    LIMIT ?
                    """,
                    (str(query.run_id), query.limit),
                ).fetchall()
            records = tuple(self._history_heartbeat_from_row(row) for row in rows)
            current = self.current(query.run_id)
            if current is None and records:
                raise self._corrupt_record(
                    "collector service history exists without its current projection"
                )
            if current is not None and len(records) != min(current.sequence, query.limit):
                raise self._corrupt_record(
                    "collector service history is missing one or more expected observations"
                )
            if records and (
                records[0].sequence != 1 or records[0].status is not CollectorServiceStatus.STARTING
            ):
                raise self._corrupt_record(
                    "collector service history must begin with starting sequence one"
                )
            for previous, current in pairwise(records):
                validate_collector_service_transition(previous, current)
            return records
        except (SQLiteCollectorServiceStorageError, ValueError):
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collector service history read failed") from error

    def recent_runs(
        self,
        query: CollectorServiceRunQuery,
    ) -> tuple[CollectorServiceHeartbeat, ...]:
        """Return validated newest-first current heartbeats for one collection."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT run_id, collection_id, worker_id, sequence, status,
                           observed_at, record_json
                    FROM collector_service_runs
                    WHERE collection_id = ?
                    ORDER BY julianday(observed_at) DESC, rowid DESC
                    LIMIT ?
                    """,
                    (str(query.collection_id), query.limit),
                ).fetchall()
            records = tuple(self._heartbeat_from_row(row) for row in rows)
            if any(record.collection_id != query.collection_id for record in records):
                raise self._corrupt_record(
                    "collector service run query returned a different collection"
                )
            return records
        except SQLiteCollectorServiceStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collector service run query failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_COLLECTOR_SERVICE_SCHEMA_VERSION}:
                    raise SQLiteCollectorServiceStorageError(
                        SQLiteCollectorServiceStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_COLLECTOR_SERVICE_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE collector_service_runs (
                        run_id TEXT PRIMARY KEY,
                        collection_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        status TEXT NOT NULL CHECK (
                            status IN (
                                'starting',
                                'running',
                                'stopped',
                                'paused',
                                'failed',
                                'cycle_limit'
                            )
                        ),
                        observed_at TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    );

                    CREATE TABLE collector_service_heartbeats (
                        heartbeat_id TEXT PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        UNIQUE (run_id, sequence),
                        FOREIGN KEY (run_id) REFERENCES collector_service_runs(run_id)
                    );

                    CREATE INDEX collector_service_collection_status_index
                    ON collector_service_runs (collection_id, status, observed_at);

                    CREATE INDEX collector_service_history_time_index
                    ON collector_service_heartbeats (run_id, observed_at);
                    """
                )
                connection.execute(
                    f"PRAGMA user_version = {SQLITE_COLLECTOR_SERVICE_SCHEMA_VERSION}"
                )
                connection.commit()
        except SQLiteCollectorServiceStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("collector service initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _insert_run(
        connection: sqlite3.Connection,
        heartbeat: CollectorServiceHeartbeat,
    ) -> None:
        connection.execute(
            """
            INSERT INTO collector_service_runs (
                run_id, collection_id, worker_id, sequence, status, observed_at, record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(heartbeat.run_id),
                str(heartbeat.collection_id),
                heartbeat.worker_id,
                heartbeat.sequence,
                heartbeat.status.value,
                heartbeat.observed_at.isoformat(),
                heartbeat.model_dump_json(),
            ),
        )

    @staticmethod
    def _insert_heartbeat(
        connection: sqlite3.Connection,
        heartbeat: CollectorServiceHeartbeat,
    ) -> None:
        connection.execute(
            """
            INSERT INTO collector_service_heartbeats (
                heartbeat_id, run_id, sequence, status, observed_at, record_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(heartbeat.heartbeat_id),
                str(heartbeat.run_id),
                heartbeat.sequence,
                heartbeat.status.value,
                heartbeat.observed_at.isoformat(),
                heartbeat.model_dump_json(),
            ),
        )

    @classmethod
    def _heartbeat_from_row(cls, row: sqlite3.Row) -> CollectorServiceHeartbeat:
        heartbeat = cls._heartbeat_from_json(str(row["record_json"]))
        expected = (
            str(heartbeat.run_id),
            str(heartbeat.collection_id),
            heartbeat.worker_id,
            heartbeat.sequence,
            heartbeat.status.value,
            heartbeat.observed_at.isoformat(),
        )
        actual = (
            str(row["run_id"]),
            str(row["collection_id"]),
            str(row["worker_id"]),
            int(row["sequence"]),
            str(row["status"]),
            str(row["observed_at"]),
        )
        if actual != expected:
            raise cls._corrupt_record("collector service current projection does not match JSON")
        return heartbeat

    @classmethod
    def _history_heartbeat_from_row(
        cls,
        row: sqlite3.Row,
    ) -> CollectorServiceHeartbeat:
        heartbeat = cls._heartbeat_from_json(str(row["record_json"]))
        expected = (
            str(heartbeat.run_id),
            str(heartbeat.heartbeat_id),
            heartbeat.sequence,
            heartbeat.status.value,
            heartbeat.observed_at.isoformat(),
        )
        actual = (
            str(row["run_id"]),
            str(row["heartbeat_id"]),
            int(row["sequence"]),
            str(row["status"]),
            str(row["observed_at"]),
        )
        if actual != expected:
            raise cls._corrupt_record("collector service history projection does not match JSON")
        return heartbeat

    @staticmethod
    def _heartbeat_from_json(value: str) -> CollectorServiceHeartbeat:
        try:
            return CollectorServiceHeartbeat.model_validate_json(value)
        except ValidationError as error:
            raise SQLiteCollectorServiceHeartbeatStore._corrupt_record(
                "collector service JSON failed schema validation"
            ) from error

    @staticmethod
    def _result(
        heartbeat: CollectorServiceHeartbeat,
        status: CollectorServiceHeartbeatWriteStatus,
        *,
        current_sequence: int,
    ) -> CollectorServiceHeartbeatWriteResult:
        return CollectorServiceHeartbeatWriteResult(
            status=status,
            run_id=heartbeat.run_id,
            heartbeat_id=heartbeat.heartbeat_id,
            current_sequence=current_sequence,
        )

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteCollectorServiceStorageError:
        return SQLiteCollectorServiceStorageError(
            SQLiteCollectorServiceStorageErrorCode.STORAGE_FAILURE,
            detail,
        )

    @staticmethod
    def _corrupt_record(detail: str) -> SQLiteCollectorServiceStorageError:
        return SQLiteCollectorServiceStorageError(
            SQLiteCollectorServiceStorageErrorCode.CORRUPT_RECORD,
            detail,
        )
