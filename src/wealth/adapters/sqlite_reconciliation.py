"""Durable SQLite reconciliation evidence and indexed quality metrics."""

import sqlite3
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import cast
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.quality import CandleStream, DataQualityStatus
from wealth.domain.reconciliation import CandleReconciliationIssueCode
from wealth.domain.reconciliation_history import (
    ReconciliationHistorySummary,
    ReconciliationIssueCount,
    ReconciliationObservation,
    ReconciliationObservationQuery,
    ReconciliationSummaryQuery,
)
from wealth.ports.reconciliation import (
    ReconciliationWriteConflictCode,
    ReconciliationWriteResult,
    ReconciliationWriteStatus,
)

SQLITE_RECONCILIATION_SCHEMA_VERSION = 1


class SQLiteReconciliationStorageErrorCode(StrEnum):
    """Machine-readable reconciliation storage failures."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteReconciliationStorageError(RuntimeError):
    """Fail explicitly when durable reconciliation evidence cannot be trusted."""

    def __init__(self, code: SQLiteReconciliationStorageErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteReconciliationHistoryStore:
    """Append-only observations with immutable comparison-series identity."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.INVALID_PATH,
                "durable reconciliation storage requires a dedicated file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def append(self, observation: ReconciliationObservation) -> ReconciliationWriteResult:
        """Append one observation without replacing history or reusing a series key."""

        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing_row = self._select_observation(connection, observation.observation_id)
                if existing_row is not None:
                    existing = self._observation_from_row(connection, existing_row)
                    connection.rollback()
                    return ReconciliationWriteResult(
                        status=(
                            ReconciliationWriteStatus.DUPLICATE
                            if existing == observation
                            else ReconciliationWriteStatus.CONFLICT
                        ),
                        observation_id=observation.observation_id,
                        conflict_code=(
                            None
                            if existing == observation
                            else ReconciliationWriteConflictCode.OBSERVATION_ID_REUSE
                        ),
                    )

                series_row = connection.execute(
                    """
                    SELECT primary_stream_json, reference_stream_json
                    FROM reconciliation_series
                    WHERE comparison_key = ?
                    """,
                    (observation.report.comparison_key,),
                ).fetchone()
                if series_row is None:
                    self._insert_series(connection, observation)
                else:
                    primary, reference = self._streams_from_series_row(series_row)
                    if (
                        primary != observation.report.primary_stream
                        or reference != observation.report.reference_stream
                    ):
                        connection.rollback()
                        return ReconciliationWriteResult(
                            status=ReconciliationWriteStatus.CONFLICT,
                            observation_id=observation.observation_id,
                            conflict_code=(ReconciliationWriteConflictCode.COMPARISON_KEY_REUSE),
                        )

                self._insert_observation(connection, observation)
                self._insert_issue_counts(connection, observation)
                connection.commit()
                return ReconciliationWriteResult(
                    status=ReconciliationWriteStatus.INSERTED,
                    observation_id=observation.observation_id,
                )
        except SQLiteReconciliationStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("reconciliation observation write failed") from error

    def get(self, observation_id: UUID) -> ReconciliationObservation | None:
        """Reload one observation and revalidate its indexed projections."""

        try:
            with closing(self._connect()) as connection:
                row = self._select_observation(connection, observation_id)
                return None if row is None else self._observation_from_row(connection, row)
        except SQLiteReconciliationStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("reconciliation observation read failed") from error

    def observations(
        self,
        query: ReconciliationObservationQuery,
    ) -> tuple[ReconciliationObservation, ...]:
        """Return a bounded history slice in deterministic observation order."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    f"""
                    {self._observation_select()}
                    WHERE comparison_key = ?
                      AND recorded_at >= ?
                      AND recorded_at < ?
                    ORDER BY recorded_at, observation_id
                    LIMIT ?
                    """,
                    (
                        query.comparison_key,
                        self._utc_iso(query.recorded_start),
                        self._utc_iso(query.recorded_end_exclusive),
                        query.limit,
                    ),
                ).fetchall()
                return tuple(self._observation_from_row(connection, row) for row in rows)
        except SQLiteReconciliationStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("reconciliation history read failed") from error

    def summarize(
        self,
        query: ReconciliationSummaryQuery,
    ) -> ReconciliationHistorySummary | None:
        """Aggregate indexed source-quality and reconciliation metrics."""

        start = self._utc_iso(query.recorded_start)
        end = self._utc_iso(query.recorded_end_exclusive)
        try:
            with closing(self._connect()) as connection:
                series_row = connection.execute(
                    """
                    SELECT primary_stream_json, reference_stream_json
                    FROM reconciliation_series
                    WHERE comparison_key = ?
                    """,
                    (query.comparison_key,),
                ).fetchone()
                if series_row is None:
                    return None
                primary_stream, reference_stream = self._streams_from_series_row(series_row)
                aggregate = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS observation_count,
                        SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS pass_count,
                        SUM(CASE WHEN status = 'divergent' THEN 1 ELSE 0 END)
                            AS divergent_count,
                        SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END)
                            AS blocked_count,
                        SUM(primary_quality_failed) AS primary_quality_failure_count,
                        SUM(reference_quality_failed) AS reference_quality_failure_count,
                        SUM(compared_count) AS compared_interval_count,
                        MIN(recorded_at) AS first_recorded_at,
                        MAX(recorded_at) AS last_recorded_at
                    FROM reconciliation_observations
                    WHERE comparison_key = ?
                      AND recorded_at >= ?
                      AND recorded_at < ?
                    """,
                    (query.comparison_key, start, end),
                ).fetchone()
                if aggregate is None:
                    raise AssertionError("SQLite aggregate must return one row")
                issue_rows = connection.execute(
                    """
                    SELECT issue_code, SUM(issue_count) AS issue_count
                    FROM reconciliation_issue_counts
                    JOIN reconciliation_observations USING (observation_id)
                    WHERE comparison_key = ?
                      AND recorded_at >= ?
                      AND recorded_at < ?
                    GROUP BY issue_code
                    ORDER BY issue_code
                    """,
                    (query.comparison_key, start, end),
                ).fetchall()

            first_value = aggregate["first_recorded_at"]
            last_value = aggregate["last_recorded_at"]
            return ReconciliationHistorySummary(
                comparison_key=query.comparison_key,
                primary_stream=primary_stream,
                reference_stream=reference_stream,
                recorded_start=query.recorded_start,
                recorded_end_exclusive=query.recorded_end_exclusive,
                observation_count=int(aggregate["observation_count"] or 0),
                pass_count=int(aggregate["pass_count"] or 0),
                divergent_count=int(aggregate["divergent_count"] or 0),
                blocked_count=int(aggregate["blocked_count"] or 0),
                primary_quality_failure_count=int(aggregate["primary_quality_failure_count"] or 0),
                reference_quality_failure_count=int(
                    aggregate["reference_quality_failure_count"] or 0
                ),
                compared_interval_count=int(aggregate["compared_interval_count"] or 0),
                first_recorded_at=(
                    None if first_value is None else datetime.fromisoformat(str(first_value))
                ),
                last_recorded_at=(
                    None if last_value is None else datetime.fromisoformat(str(last_value))
                ),
                issue_counts=tuple(
                    ReconciliationIssueCount(
                        code=CandleReconciliationIssueCode(str(row["issue_code"])),
                        count=int(row["issue_count"]),
                    )
                    for row in issue_rows
                ),
            )
        except (ValueError, ValidationError) as error:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "stored reconciliation summary violates its contract",
            ) from error
        except SQLiteReconciliationStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("reconciliation summary failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_RECONCILIATION_SCHEMA_VERSION}:
                    raise SQLiteReconciliationStorageError(
                        SQLiteReconciliationStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_RECONCILIATION_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE reconciliation_series (
                        comparison_key TEXT PRIMARY KEY,
                        primary_stream_json TEXT NOT NULL,
                        reference_stream_json TEXT NOT NULL
                    );

                    CREATE TABLE reconciliation_observations (
                        observation_id TEXT PRIMARY KEY,
                        comparison_key TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        status TEXT NOT NULL
                            CHECK (status IN ('pass', 'divergent', 'blocked')),
                        primary_quality_failed INTEGER NOT NULL
                            CHECK (primary_quality_failed IN (0, 1)),
                        reference_quality_failed INTEGER NOT NULL
                            CHECK (reference_quality_failed IN (0, 1)),
                        compared_count INTEGER NOT NULL CHECK (compared_count >= 0),
                        report_sha256 TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        FOREIGN KEY (comparison_key)
                            REFERENCES reconciliation_series(comparison_key)
                    );

                    CREATE TABLE reconciliation_issue_counts (
                        observation_id TEXT NOT NULL,
                        issue_code TEXT NOT NULL,
                        issue_count INTEGER NOT NULL CHECK (issue_count > 0),
                        PRIMARY KEY (observation_id, issue_code),
                        FOREIGN KEY (observation_id)
                            REFERENCES reconciliation_observations(observation_id)
                    );

                    CREATE INDEX reconciliation_observations_series_time_index
                    ON reconciliation_observations (
                        comparison_key,
                        recorded_at,
                        observation_id
                    );

                    CREATE INDEX reconciliation_observations_status_time_index
                    ON reconciliation_observations (status, recorded_at);
                    """
                )
                connection.execute(f"PRAGMA user_version = {SQLITE_RECONCILIATION_SCHEMA_VERSION}")
                connection.commit()
        except SQLiteReconciliationStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("reconciliation storage initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _insert_series(
        connection: sqlite3.Connection,
        observation: ReconciliationObservation,
    ) -> None:
        connection.execute(
            """
            INSERT INTO reconciliation_series (
                comparison_key,
                primary_stream_json,
                reference_stream_json
            ) VALUES (?, ?, ?)
            """,
            (
                observation.report.comparison_key,
                observation.report.primary_stream.model_dump_json(),
                observation.report.reference_stream.model_dump_json(),
            ),
        )

    @classmethod
    def _insert_observation(
        cls,
        connection: sqlite3.Connection,
        observation: ReconciliationObservation,
    ) -> None:
        report = observation.report
        connection.execute(
            """
            INSERT INTO reconciliation_observations (
                observation_id,
                comparison_key,
                recorded_at,
                status,
                primary_quality_failed,
                reference_quality_failed,
                compared_count,
                report_sha256,
                record_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(observation.observation_id),
                report.comparison_key,
                cls._utc_iso(observation.recorded_at),
                report.status.value,
                int(report.primary_quality.status is DataQualityStatus.FAIL),
                int(report.reference_quality.status is DataQualityStatus.FAIL),
                report.compared_count,
                observation.report_sha256,
                observation.model_dump_json(),
            ),
        )

    @staticmethod
    def _insert_issue_counts(
        connection: sqlite3.Connection,
        observation: ReconciliationObservation,
    ) -> None:
        counts = Counter(issue.code for issue in observation.report.issues)
        connection.executemany(
            """
            INSERT INTO reconciliation_issue_counts (
                observation_id,
                issue_code,
                issue_count
            ) VALUES (?, ?, ?)
            """,
            (
                (str(observation.observation_id), code.value, count)
                for code, count in sorted(counts.items(), key=lambda item: item[0].value)
            ),
        )

    @classmethod
    def _select_observation(
        cls,
        connection: sqlite3.Connection,
        observation_id: UUID,
    ) -> sqlite3.Row | None:
        return cast(
            sqlite3.Row | None,
            connection.execute(
                f"""
                {cls._observation_select()}
                WHERE observation_id = ?
                """,
                (str(observation_id),),
            ).fetchone(),
        )

    @staticmethod
    def _observation_select() -> str:
        return """
            SELECT observation_id, comparison_key, recorded_at, status,
                   primary_quality_failed, reference_quality_failed,
                   compared_count, report_sha256, record_json
            FROM reconciliation_observations
        """

    @classmethod
    def _observation_from_row(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> ReconciliationObservation:
        try:
            observation = ReconciliationObservation.model_validate_json(str(row["record_json"]))
        except ValidationError as error:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "stored reconciliation observation violates its contract",
            ) from error

        report = observation.report
        expected_index = (
            str(observation.observation_id),
            report.comparison_key,
            cls._utc_iso(observation.recorded_at),
            report.status.value,
            int(report.primary_quality.status is DataQualityStatus.FAIL),
            int(report.reference_quality.status is DataQualityStatus.FAIL),
            report.compared_count,
            observation.report_sha256,
        )
        stored_index = (
            str(row["observation_id"]),
            str(row["comparison_key"]),
            str(row["recorded_at"]),
            str(row["status"]),
            int(row["primary_quality_failed"]),
            int(row["reference_quality_failed"]),
            int(row["compared_count"]),
            str(row["report_sha256"]),
        )
        if expected_index != stored_index:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "reconciliation observation index does not match canonical content",
            )

        rows = connection.execute(
            """
            SELECT issue_code, issue_count
            FROM reconciliation_issue_counts
            WHERE observation_id = ?
            ORDER BY issue_code
            """,
            (str(observation.observation_id),),
        ).fetchall()
        expected_counts = Counter(issue.code for issue in report.issues)
        try:
            stored_counts = {
                CandleReconciliationIssueCode(str(issue_row["issue_code"])): int(
                    issue_row["issue_count"]
                )
                for issue_row in rows
            }
        except ValueError as error:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "stored reconciliation issue code is unsupported",
            ) from error
        if dict(expected_counts) != stored_counts:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "reconciliation issue metrics do not match canonical content",
            )
        return observation

    @staticmethod
    def _streams_from_series_row(row: sqlite3.Row) -> tuple[CandleStream, CandleStream]:
        try:
            return (
                CandleStream.model_validate_json(str(row["primary_stream_json"])),
                CandleStream.model_validate_json(str(row["reference_stream_json"])),
            )
        except ValidationError as error:
            raise SQLiteReconciliationStorageError(
                SQLiteReconciliationStorageErrorCode.CORRUPT_RECORD,
                "stored reconciliation series violates its stream contract",
            ) from error

    @staticmethod
    def _utc_iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat()

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteReconciliationStorageError:
        return SQLiteReconciliationStorageError(
            SQLiteReconciliationStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
