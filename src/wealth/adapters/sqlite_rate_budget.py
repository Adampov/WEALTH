"""Durable cross-process GCRA coordination for provider request budgets."""

import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import ValidationError

from wealth.domain.rate_budget import (
    RateBudgetDecision,
    RateBudgetDecisionStatus,
    RateBudgetPolicy,
    RateBudgetRequest,
    RateBudgetReservationResult,
    RateBudgetSummary,
)

SQLITE_RATE_BUDGET_SCHEMA_VERSION = 1
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SQLiteRateBudgetErrorCode(StrEnum):
    """Machine-readable failures at the shared-budget boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"
    RESERVATION_CONFLICT = "reservation_conflict"
    POLICY_CONFLICT = "policy_conflict"
    CLOCK_REGRESSION = "clock_regression"
    COST_EXCEEDS_CAPACITY = "cost_exceeds_capacity"


class SQLiteRateBudgetError(RuntimeError):
    """Fail closed when a shared budget cannot be coordinated safely."""

    def __init__(self, code: SQLiteRateBudgetErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteRateBudgetCoordinator:
    """File-backed constant-state rate coordination with idempotent reservations."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.INVALID_PATH,
                "shared rate-budget coordination requires a file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def reserve(
        self,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
    ) -> RateBudgetReservationResult:
        """Atomically grant or deny one idempotent GCRA reservation."""

        if request.budget_key != policy.budget_key:
            raise ValueError("rate-budget request and policy keys must match")
        if request.cost > policy.capacity:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.COST_EXCEEDS_CAPACITY,
                "reservation cost exceeds the configured budget capacity",
            )

        try:
            with closing(self._connect()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                replay = self._replay_if_present(
                    connection,
                    policy=policy,
                    request=request,
                )
                if replay is not None:
                    connection.rollback()
                    return replay

                requested_at_us = self._to_epoch_microseconds(request.requested_at)
                state = connection.execute(
                    """
                    SELECT capacity, period_seconds, interval_microseconds,
                           theoretical_arrival_us, last_observed_us, version
                    FROM rate_budget_state
                    WHERE budget_key = ?
                    """,
                    (policy.budget_key,),
                ).fetchone()
                if state is None:
                    theoretical_arrival_us = requested_at_us
                    version = 0
                else:
                    self._validate_state(state, policy)
                    last_observed_us = int(state["last_observed_us"])
                    if requested_at_us < last_observed_us:
                        raise SQLiteRateBudgetError(
                            SQLiteRateBudgetErrorCode.CLOCK_REGRESSION,
                            "reservation time precedes the last coordinated observation",
                        )
                    theoretical_arrival_us = int(state["theoretical_arrival_us"])
                    version = int(state["version"])

                interval_us = policy.interval_microseconds
                allowed_at_us = (
                    theoretical_arrival_us - (policy.capacity - request.cost) * interval_us
                )
                reason_code: Literal["granted", "budget_exhausted"]
                if requested_at_us >= allowed_at_us:
                    status = RateBudgetDecisionStatus.GRANTED
                    new_theoretical_arrival_us = (
                        max(requested_at_us, theoretical_arrival_us) + request.cost * interval_us
                    )
                    retry_after_seconds = None
                    reason_code = "granted"
                else:
                    status = RateBudgetDecisionStatus.DENIED
                    new_theoretical_arrival_us = theoretical_arrival_us
                    retry_delay_us = allowed_at_us - requested_at_us
                    retry_after_seconds = (retry_delay_us + 999_999) // 1_000_000
                    reason_code = "budget_exhausted"

                decision = RateBudgetDecision(
                    reservation_id=request.reservation_id,
                    budget_key=request.budget_key,
                    requested_at=request.requested_at,
                    cost=request.cost,
                    capacity=policy.capacity,
                    period_seconds=policy.period_seconds,
                    status=status,
                    reason_code=reason_code,
                    retry_after_seconds=retry_after_seconds,
                    available_capacity=self._available_capacity(
                        capacity=policy.capacity,
                        interval_us=interval_us,
                        theoretical_arrival_us=new_theoretical_arrival_us,
                        observed_at_us=requested_at_us,
                    ),
                    theoretical_arrival_at=self._from_epoch_microseconds(
                        new_theoretical_arrival_us
                    ),
                )
                self._write_state(
                    connection,
                    policy=policy,
                    theoretical_arrival_us=new_theoretical_arrival_us,
                    last_observed_us=requested_at_us,
                    prior_version=version,
                )
                self._insert_reservation(
                    connection,
                    policy=policy,
                    request=request,
                    decision=decision,
                )
                connection.commit()
                return RateBudgetReservationResult(decision=decision)
        except SQLiteRateBudgetError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("rate-budget reservation failed") from error

    def decisions_for_budget(
        self,
        budget_key: str,
    ) -> tuple[RateBudgetDecision, ...]:
        """Reload durable decisions in request-time order."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT reservation_id, budget_key, requested_at, cost,
                           status, decision_json
                    FROM rate_budget_reservations
                    WHERE budget_key = ?
                    ORDER BY requested_at, reservation_id
                    """,
                    (budget_key,),
                ).fetchall()
            return tuple(self._decision_from_row(row) for row in rows)
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("rate-budget decision read failed") from error

    def summary(self, budget_key: str) -> RateBudgetSummary:
        """Aggregate request pressure without materializing decision history."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS reservation_count,
                        SUM(CASE WHEN status = 'granted' THEN 1 ELSE 0 END)
                            AS granted_count,
                        SUM(CASE WHEN status = 'denied' THEN 1 ELSE 0 END)
                            AS denied_count,
                        SUM(cost) AS total_requested_cost,
                        SUM(COALESCE(retry_after_seconds, 0))
                            AS total_retry_after_seconds,
                        MAX(COALESCE(retry_after_seconds, 0))
                            AS maximum_retry_after_seconds
                    FROM rate_budget_reservations
                    WHERE budget_key = ?
                    """,
                    (budget_key,),
                ).fetchone()
            if row is None:
                raise AssertionError("SQLite aggregate must return one row")
            return RateBudgetSummary(
                budget_key=budget_key,
                reservation_count=int(row["reservation_count"] or 0),
                granted_count=int(row["granted_count"] or 0),
                denied_count=int(row["denied_count"] or 0),
                total_requested_cost=int(row["total_requested_cost"] or 0),
                total_retry_after_seconds=int(row["total_retry_after_seconds"] or 0),
                maximum_retry_after_seconds=int(row["maximum_retry_after_seconds"] or 0),
            )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("rate-budget summary failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_RATE_BUDGET_SCHEMA_VERSION}:
                    raise SQLiteRateBudgetError(
                        SQLiteRateBudgetErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_RATE_BUDGET_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE rate_budget_state (
                        budget_key TEXT PRIMARY KEY,
                        capacity INTEGER NOT NULL,
                        period_seconds INTEGER NOT NULL,
                        interval_microseconds INTEGER NOT NULL,
                        theoretical_arrival_us INTEGER NOT NULL,
                        last_observed_us INTEGER NOT NULL,
                        version INTEGER NOT NULL
                    );

                    CREATE TABLE rate_budget_reservations (
                        reservation_id TEXT PRIMARY KEY,
                        budget_key TEXT NOT NULL,
                        requested_at TEXT NOT NULL,
                        cost INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        retry_after_seconds INTEGER,
                        policy_json TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        decision_json TEXT NOT NULL,
                        FOREIGN KEY (budget_key) REFERENCES rate_budget_state(budget_key)
                    );

                    CREATE INDEX rate_budget_history_index
                    ON rate_budget_reservations (
                        budget_key,
                        requested_at,
                        reservation_id
                    );
                    """
                )
                connection.execute(f"PRAGMA user_version = {SQLITE_RATE_BUDGET_SCHEMA_VERSION}")
                connection.commit()
        except SQLiteRateBudgetError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("rate-budget initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @classmethod
    def _replay_if_present(
        cls,
        connection: sqlite3.Connection,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
    ) -> RateBudgetReservationResult | None:
        row = connection.execute(
            """
            SELECT policy_json, request_json, reservation_id, budget_key,
                   requested_at, cost, status, decision_json
            FROM rate_budget_reservations
            WHERE reservation_id = ?
            """,
            (str(request.reservation_id),),
        ).fetchone()
        if row is None:
            return None
        try:
            existing_policy = RateBudgetPolicy.model_validate_json(str(row["policy_json"]))
            existing_request = RateBudgetRequest.model_validate_json(str(row["request_json"]))
        except ValidationError as error:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.CORRUPT_RECORD,
                "stored reservation identity violates its contract",
            ) from error
        if existing_policy != policy or existing_request != request:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.RESERVATION_CONFLICT,
                "reservation identifier was reused with different content",
            )
        return RateBudgetReservationResult(
            decision=cls._decision_from_row(row),
            replayed=True,
        )

    @staticmethod
    def _validate_state(row: sqlite3.Row, policy: RateBudgetPolicy) -> None:
        stored_policy = (
            int(row["capacity"]),
            int(row["period_seconds"]),
            int(row["interval_microseconds"]),
        )
        expected_policy = (
            policy.capacity,
            policy.period_seconds,
            policy.interval_microseconds,
        )
        if stored_policy != expected_policy:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.POLICY_CONFLICT,
                "shared budget key already uses a different policy",
            )
        theoretical_arrival_us = int(row["theoretical_arrival_us"])
        last_observed_us = int(row["last_observed_us"])
        if theoretical_arrival_us <= last_observed_us or int(row["version"]) < 1:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.CORRUPT_RECORD,
                "stored rate-budget state violates GCRA invariants",
            )

    @staticmethod
    def _write_state(
        connection: sqlite3.Connection,
        *,
        policy: RateBudgetPolicy,
        theoretical_arrival_us: int,
        last_observed_us: int,
        prior_version: int,
    ) -> None:
        if prior_version == 0:
            connection.execute(
                """
                INSERT INTO rate_budget_state (
                    budget_key, capacity, period_seconds, interval_microseconds,
                    theoretical_arrival_us, last_observed_us, version
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    policy.budget_key,
                    policy.capacity,
                    policy.period_seconds,
                    policy.interval_microseconds,
                    theoretical_arrival_us,
                    last_observed_us,
                ),
            )
            return
        cursor = connection.execute(
            """
            UPDATE rate_budget_state
            SET theoretical_arrival_us = ?,
                last_observed_us = ?,
                version = ?
            WHERE budget_key = ? AND version = ?
            """,
            (
                theoretical_arrival_us,
                last_observed_us,
                prior_version + 1,
                policy.budget_key,
                prior_version,
            ),
        )
        if cursor.rowcount != 1:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.STORAGE_FAILURE,
                "rate-budget state changed during its atomic reservation",
            )

    @staticmethod
    def _insert_reservation(
        connection: sqlite3.Connection,
        *,
        policy: RateBudgetPolicy,
        request: RateBudgetRequest,
        decision: RateBudgetDecision,
    ) -> None:
        connection.execute(
            """
            INSERT INTO rate_budget_reservations (
                reservation_id, budget_key, requested_at, cost, status,
                retry_after_seconds, policy_json, request_json, decision_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(request.reservation_id),
                request.budget_key,
                request.requested_at.isoformat(),
                request.cost,
                decision.status.value,
                decision.retry_after_seconds,
                policy.model_dump_json(),
                request.model_dump_json(),
                decision.model_dump_json(),
            ),
        )

    @staticmethod
    def _decision_from_row(row: sqlite3.Row) -> RateBudgetDecision:
        try:
            decision = RateBudgetDecision.model_validate_json(str(row["decision_json"]))
        except ValidationError as error:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.CORRUPT_RECORD,
                "stored rate-budget decision violates its contract",
            ) from error
        indexed = (
            str(decision.reservation_id),
            decision.budget_key,
            decision.requested_at.isoformat(),
            decision.cost,
            decision.status.value,
        )
        stored = (
            str(row["reservation_id"]),
            str(row["budget_key"]),
            str(row["requested_at"]),
            int(row["cost"]),
            str(row["status"]),
        )
        if indexed != stored:
            raise SQLiteRateBudgetError(
                SQLiteRateBudgetErrorCode.CORRUPT_RECORD,
                "rate-budget decision index does not match canonical content",
            )
        return decision

    @staticmethod
    def _available_capacity(
        *,
        capacity: int,
        interval_us: int,
        theoretical_arrival_us: int,
        observed_at_us: int,
    ) -> int:
        debt_us = max(0, theoretical_arrival_us - observed_at_us)
        reserved_units = (debt_us + interval_us - 1) // interval_us
        return max(0, capacity - reserved_units)

    @staticmethod
    def _to_epoch_microseconds(value: datetime) -> int:
        delta = value.astimezone(UTC) - UTC_EPOCH
        return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds

    @staticmethod
    def _from_epoch_microseconds(value: int) -> datetime:
        return UTC_EPOCH + timedelta(microseconds=value)

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteRateBudgetError:
        return SQLiteRateBudgetError(
            SQLiteRateBudgetErrorCode.STORAGE_FAILURE,
            detail,
        )
