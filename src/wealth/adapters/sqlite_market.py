"""Durable SQLite storage for raw and canonical market data."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.market import CanonicalCandle, RawMarketPayload
from wealth.domain.quality import (
    CandleConflictRecord,
    CandleStream,
    CandleWriteResult,
    CandleWriteStatus,
    MarketDataBatchWriteResult,
    RawPayloadWriteResult,
    RawPayloadWriteStatus,
)
from wealth.ports.market import CandleFetchBatch

SQLITE_MARKET_SCHEMA_VERSION = 1


class SQLiteMarketStorageErrorCode(StrEnum):
    """Machine-readable failures at the durable-storage boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteMarketStorageError(RuntimeError):
    """Fail explicitly when durable evidence cannot be trusted."""

    def __init__(self, code: SQLiteMarketStorageErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteCandleStore:
    """SQLite-backed evidence store with idempotency and conflict quarantine."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteMarketStorageError(
                SQLiteMarketStorageErrorCode.INVALID_PATH,
                "durable market storage requires a file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def append(self, candle: CanonicalCandle) -> CandleWriteResult:
        """Append one canonical candle without silently replacing prior data."""

        try:
            with closing(self._connect()) as connection, connection:
                return self._append_candle(
                    connection=connection,
                    candle=candle,
                    raw_payload_id=None,
                )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("canonical candle write failed") from error

    def append_batch(self, batch: CandleFetchBatch) -> MarketDataBatchWriteResult:
        """Persist one raw response and its derived canonical records atomically."""

        try:
            with closing(self._connect()) as connection, connection:
                raw_write = self._append_raw(connection, batch.raw_payload)
                if raw_write.status is RawPayloadWriteStatus.CONFLICT:
                    return MarketDataBatchWriteResult(
                        raw_payload=raw_write,
                        candles=(),
                    )
                candle_writes = tuple(
                    self._append_candle(
                        connection=connection,
                        candle=candle,
                        raw_payload_id=batch.raw_payload.record_id,
                    )
                    for candle in batch.records
                )
                return MarketDataBatchWriteResult(
                    raw_payload=raw_write,
                    candles=candle_writes,
                )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("market-data batch write failed") from error

    def records_for_stream(self, stream: CandleStream) -> tuple[CanonicalCandle, ...]:
        """Reload a validated, deterministic stream snapshot from disk."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT record_json
                    FROM canonical_candles
                    WHERE source = ?
                      AND venue = ?
                      AND instrument = ?
                      AND instrument_type = ?
                      AND timeframe = ?
                    ORDER BY open_time, record_id
                    """,
                    (
                        stream.source,
                        stream.venue,
                        stream.instrument,
                        stream.instrument_type.value,
                        stream.timeframe.value,
                    ),
                ).fetchall()
            records = tuple(self._candle_from_json(str(row["record_json"])) for row in rows)
            if any(not stream.contains(record) for record in records):
                raise SQLiteMarketStorageError(
                    SQLiteMarketStorageErrorCode.CORRUPT_RECORD,
                    "stored candle index does not match canonical content",
                )
            return records
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("canonical stream read failed") from error

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Reload exact provider bytes and validated provenance by ID."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT record_id, source, venue, media_type, observed_at, processed_at,
                           payload_sha256, payload, lineage_json
                    FROM raw_market_payloads
                    WHERE record_id = ?
                    """,
                    (str(record_id),),
                ).fetchone()
            return None if row is None else self._raw_payload_from_row(row)
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("raw market payload read failed") from error

    def raw_payload_ids_for_candle(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one accepted canonical record."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT raw_payload_id
                    FROM candle_raw_lineage
                    WHERE canonical_record_id = ?
                    ORDER BY raw_payload_id
                    """,
                    (str(record_id),),
                ).fetchall()
            try:
                return tuple(UUID(str(row["raw_payload_id"])) for row in rows)
            except ValueError as error:
                raise SQLiteMarketStorageError(
                    SQLiteMarketStorageErrorCode.CORRUPT_RECORD,
                    "stored raw-lineage reference is not a UUID",
                ) from error
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("canonical raw-lineage read failed") from error

    def conflicts_for_stream(self, stream: CandleStream) -> tuple[CandleConflictRecord, ...]:
        """Return quarantined revisions without promoting them to canonical data."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT open_time, existing_record_id, incoming_candle_json,
                           raw_payload_id, detected_at
                    FROM candle_conflicts
                    WHERE source = ?
                      AND venue = ?
                      AND instrument = ?
                      AND instrument_type = ?
                      AND timeframe = ?
                    ORDER BY open_time, incoming_record_id
                    """,
                    (
                        stream.source,
                        stream.venue,
                        stream.instrument,
                        stream.instrument_type.value,
                        stream.timeframe.value,
                    ),
                ).fetchall()
            try:
                return tuple(
                    CandleConflictRecord(
                        stream=stream,
                        open_time=datetime.fromisoformat(str(row["open_time"])),
                        existing_record_id=UUID(str(row["existing_record_id"])),
                        incoming_candle=self._candle_from_json(str(row["incoming_candle_json"])),
                        raw_payload_id=UUID(str(row["raw_payload_id"])),
                        detected_at=datetime.fromisoformat(str(row["detected_at"])),
                    )
                    for row in rows
                )
            except (ValueError, ValidationError) as error:
                raise SQLiteMarketStorageError(
                    SQLiteMarketStorageErrorCode.CORRUPT_RECORD,
                    "stored candle conflict violates its contract",
                ) from error
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("candle conflict read failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection, connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_MARKET_SCHEMA_VERSION}:
                    raise SQLiteMarketStorageError(
                        SQLiteMarketStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_MARKET_SCHEMA_VERSION:
                    return
                connection.executescript(
                    """
                    CREATE TABLE raw_market_payloads (
                        record_id TEXT PRIMARY KEY,
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        media_type TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        processed_at TEXT NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        payload BLOB NOT NULL,
                        lineage_json TEXT NOT NULL
                    );

                    CREATE TABLE canonical_candles (
                        record_id TEXT NOT NULL UNIQUE,
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        instrument_type TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open_time TEXT NOT NULL,
                        record_json TEXT NOT NULL,
                        PRIMARY KEY (
                            source,
                            venue,
                            instrument,
                            instrument_type,
                            timeframe,
                            open_time
                        )
                    );

                    CREATE TABLE candle_conflicts (
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        instrument_type TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open_time TEXT NOT NULL,
                        existing_record_id TEXT NOT NULL,
                        incoming_record_id TEXT NOT NULL,
                        incoming_candle_json TEXT NOT NULL,
                        raw_payload_id TEXT NOT NULL,
                        detected_at TEXT NOT NULL,
                        PRIMARY KEY (existing_record_id, incoming_record_id),
                        FOREIGN KEY (existing_record_id)
                            REFERENCES canonical_candles(record_id),
                        FOREIGN KEY (raw_payload_id)
                            REFERENCES raw_market_payloads(record_id)
                    );

                    CREATE TABLE candle_raw_lineage (
                        canonical_record_id TEXT NOT NULL,
                        raw_payload_id TEXT NOT NULL,
                        PRIMARY KEY (canonical_record_id, raw_payload_id),
                        FOREIGN KEY (canonical_record_id)
                            REFERENCES canonical_candles(record_id),
                        FOREIGN KEY (raw_payload_id)
                            REFERENCES raw_market_payloads(record_id)
                    );

                    CREATE INDEX candle_conflicts_stream_index
                    ON candle_conflicts (
                        source,
                        venue,
                        instrument,
                        instrument_type,
                        timeframe,
                        open_time
                    );
                    """
                )
                connection.execute(f"PRAGMA user_version = {SQLITE_MARKET_SCHEMA_VERSION}")
        except SQLiteMarketStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("market storage initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _append_raw(
        connection: sqlite3.Connection,
        payload: RawMarketPayload,
    ) -> RawPayloadWriteResult:
        row = connection.execute(
            """
            SELECT record_id, source, venue, media_type, observed_at, processed_at,
                   payload_sha256, payload, lineage_json
            FROM raw_market_payloads
            WHERE record_id = ?
            """,
            (str(payload.record_id),),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO raw_market_payloads (
                    record_id, source, venue, media_type, observed_at, processed_at,
                    payload_sha256, payload, lineage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.record_id),
                    payload.source,
                    payload.venue,
                    payload.media_type,
                    payload.observed_at.isoformat(),
                    payload.processed_at.isoformat(),
                    payload.payload_sha256,
                    payload.payload,
                    json.dumps(payload.lineage, ensure_ascii=True, separators=(",", ":")),
                ),
            )
            return RawPayloadWriteResult(
                status=RawPayloadWriteStatus.INSERTED,
                incoming_record_id=payload.record_id,
            )

        existing = SQLiteCandleStore._raw_payload_from_row(row)
        status = (
            RawPayloadWriteStatus.DUPLICATE
            if existing.content_identity == payload.content_identity
            else RawPayloadWriteStatus.CONFLICT
        )
        return RawPayloadWriteResult(
            status=status,
            incoming_record_id=payload.record_id,
            existing_record_id=existing.record_id,
        )

    @staticmethod
    def _append_candle(
        *,
        connection: sqlite3.Connection,
        candle: CanonicalCandle,
        raw_payload_id: UUID | None,
    ) -> CandleWriteResult:
        row = connection.execute(
            """
            SELECT record_id, record_json
            FROM canonical_candles
            WHERE source = ?
              AND venue = ?
              AND instrument = ?
              AND instrument_type = ?
              AND timeframe = ?
              AND open_time = ?
            """,
            (
                candle.source,
                candle.venue,
                candle.instrument,
                candle.instrument_type.value,
                candle.timeframe.value,
                candle.open_time.isoformat(),
            ),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO canonical_candles (
                    record_id, source, venue, instrument, instrument_type,
                    timeframe, open_time, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(candle.record_id),
                    candle.source,
                    candle.venue,
                    candle.instrument,
                    candle.instrument_type.value,
                    candle.timeframe.value,
                    candle.open_time.isoformat(),
                    candle.model_dump_json(),
                ),
            )
            if raw_payload_id is not None:
                SQLiteCandleStore._link_raw_payload(
                    connection,
                    canonical_record_id=candle.record_id,
                    raw_payload_id=raw_payload_id,
                )
            return CandleWriteResult(
                status=CandleWriteStatus.INSERTED,
                incoming_record_id=candle.record_id,
            )

        existing = SQLiteCandleStore._candle_from_json(str(row["record_json"]))
        if existing.market_values == candle.market_values:
            if raw_payload_id is not None:
                SQLiteCandleStore._link_raw_payload(
                    connection,
                    canonical_record_id=existing.record_id,
                    raw_payload_id=raw_payload_id,
                )
            return CandleWriteResult(
                status=CandleWriteStatus.DUPLICATE,
                incoming_record_id=candle.record_id,
                existing_record_id=existing.record_id,
            )

        if raw_payload_id is not None:
            connection.execute(
                """
                INSERT OR IGNORE INTO candle_conflicts (
                    source, venue, instrument, instrument_type, timeframe,
                    open_time, existing_record_id, incoming_record_id,
                    incoming_candle_json, raw_payload_id, detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candle.source,
                    candle.venue,
                    candle.instrument,
                    candle.instrument_type.value,
                    candle.timeframe.value,
                    candle.open_time.isoformat(),
                    str(existing.record_id),
                    str(candle.record_id),
                    candle.model_dump_json(),
                    str(raw_payload_id),
                    candle.processed_at.isoformat(),
                ),
            )
        return CandleWriteResult(
            status=CandleWriteStatus.CONFLICT,
            incoming_record_id=candle.record_id,
            existing_record_id=existing.record_id,
        )

    @staticmethod
    def _link_raw_payload(
        connection: sqlite3.Connection,
        *,
        canonical_record_id: UUID,
        raw_payload_id: UUID,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO candle_raw_lineage (
                canonical_record_id,
                raw_payload_id
            ) VALUES (?, ?)
            """,
            (str(canonical_record_id), str(raw_payload_id)),
        )

    @staticmethod
    def _raw_payload_from_row(row: sqlite3.Row) -> RawMarketPayload:
        try:
            lineage_value: object = json.loads(str(row["lineage_json"]))
            if not isinstance(lineage_value, list) or not all(
                isinstance(item, str) for item in lineage_value
            ):
                raise ValueError("raw payload lineage is not a string list")
            if str(row["media_type"]) != "application/json":
                raise ValueError("raw payload media type is unsupported")
            return RawMarketPayload(
                record_id=UUID(str(row["record_id"])),
                source=str(row["source"]),
                venue=str(row["venue"]),
                media_type="application/json",
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                processed_at=datetime.fromisoformat(str(row["processed_at"])),
                payload_sha256=str(row["payload_sha256"]),
                payload=bytes(row["payload"]),
                lineage=tuple(lineage_value),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise SQLiteMarketStorageError(
                SQLiteMarketStorageErrorCode.CORRUPT_RECORD,
                "stored raw market payload violates its contract",
            ) from error

    @staticmethod
    def _candle_from_json(value: str) -> CanonicalCandle:
        try:
            return CanonicalCandle.model_validate_json(value)
        except ValidationError as error:
            raise SQLiteMarketStorageError(
                SQLiteMarketStorageErrorCode.CORRUPT_RECORD,
                "stored canonical candle violates its contract",
            ) from error

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteMarketStorageError:
        return SQLiteMarketStorageError(
            SQLiteMarketStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
