"""Durable SQLite storage for raw and canonical order-flow evidence."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import ValidationError

from wealth.domain.market import RawMarketPayload
from wealth.domain.order_flow import CanonicalBestBidAsk, CanonicalTicker, CanonicalTrade
from wealth.domain.order_flow_quality import (
    OrderFlowBatchWriteResult,
    OrderFlowConflictRecord,
    OrderFlowRecord,
    OrderFlowRecordType,
    OrderFlowStream,
    OrderFlowWriteResult,
    OrderFlowWriteStatus,
    order_flow_record_type,
    order_flow_sort_key,
)
from wealth.domain.quality import RawPayloadWriteResult, RawPayloadWriteStatus
from wealth.ports.order_flow import OrderFlowFetchBatch

SQLITE_ORDER_FLOW_SCHEMA_VERSION = 1


class SQLiteOrderFlowStorageErrorCode(StrEnum):
    """Machine-readable failures at the durable order-flow boundary."""

    INVALID_PATH = "invalid_path"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    IDENTITY_CONFLICT = "identity_conflict"
    STORAGE_FAILURE = "storage_failure"
    CORRUPT_RECORD = "corrupt_record"


class SQLiteOrderFlowStorageError(RuntimeError):
    """Fail explicitly when durable order-flow evidence cannot be trusted."""

    def __init__(self, code: SQLiteOrderFlowStorageErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteOrderFlowStore:
    """SQLite-backed evidence store with idempotency and conflict quarantine."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(path) == ":memory:" or (self.path.exists() and self.path.is_dir()):
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.INVALID_PATH,
                "durable order-flow storage requires a file path",
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def append(self, record: OrderFlowRecord) -> OrderFlowWriteResult:
        """Append one canonical record without silently replacing prior data."""

        try:
            with closing(self._connect()) as connection, connection:
                return self._append_record(
                    connection=connection,
                    record=record,
                    raw_payload_id=None,
                )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("canonical order-flow write failed") from error

    def append_batch(self, batch: OrderFlowFetchBatch) -> OrderFlowBatchWriteResult:
        """Persist one raw response and its canonical records atomically."""

        try:
            with closing(self._connect()) as connection, connection:
                raw_write = self._append_raw(connection, batch.raw_payload)
                if raw_write.status is RawPayloadWriteStatus.CONFLICT:
                    return OrderFlowBatchWriteResult(
                        raw_payload=raw_write,
                        records=(),
                    )
                writes = tuple(
                    self._append_record(
                        connection=connection,
                        record=record,
                        raw_payload_id=batch.raw_payload.record_id,
                    )
                    for record in batch.records
                )
                return OrderFlowBatchWriteResult(
                    raw_payload=raw_write,
                    records=writes,
                )
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("order-flow batch write failed") from error

    def records_for_stream(self, stream: OrderFlowStream) -> tuple[OrderFlowRecord, ...]:
        """Reload a validated, deterministic exact-stream snapshot from disk."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT natural_key_json, record_type, record_json
                    FROM canonical_order_flow_records
                    WHERE source = ?
                      AND venue = ?
                      AND instrument = ?
                      AND instrument_type = ?
                      AND record_type = ?
                    ORDER BY event_time, record_id
                    """,
                    (
                        stream.source,
                        stream.venue,
                        stream.instrument,
                        stream.instrument_type.value,
                        stream.record_type.value,
                    ),
                ).fetchall()
            records = tuple(
                self._validated_record_row(
                    record_type_value=str(row["record_type"]),
                    record_json=str(row["record_json"]),
                    natural_key_json=str(row["natural_key_json"]),
                )
                for row in rows
            )
            if any(not stream.contains(record) for record in records):
                raise SQLiteOrderFlowStorageError(
                    SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                    "stored order-flow index does not match canonical content",
                )
            return tuple(sorted(records, key=order_flow_sort_key))
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("canonical order-flow stream read failed") from error

    def raw_payload(self, record_id: UUID) -> RawMarketPayload | None:
        """Reload exact provider bytes and validated provenance by ID."""

        try:
            with closing(self._connect()) as connection:
                row = connection.execute(
                    """
                    SELECT record_id, source, venue, media_type, observed_at, processed_at,
                           payload_sha256, payload, lineage_json
                    FROM raw_order_flow_payloads
                    WHERE record_id = ?
                    """,
                    (str(record_id),),
                ).fetchone()
            return None if row is None else self._raw_payload_from_row(row)
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("raw order-flow payload read failed") from error

    def raw_payload_ids_for_record(self, record_id: UUID) -> tuple[UUID, ...]:
        """Return every raw capture linked to one accepted canonical record."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT raw_payload_id
                    FROM order_flow_raw_lineage
                    WHERE canonical_record_id = ?
                    ORDER BY raw_payload_id
                    """,
                    (str(record_id),),
                ).fetchall()
            try:
                return tuple(UUID(str(row["raw_payload_id"])) for row in rows)
            except ValueError as error:
                raise SQLiteOrderFlowStorageError(
                    SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                    "stored order-flow raw-lineage reference is not a UUID",
                ) from error
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("order-flow raw-lineage read failed") from error

    def conflicts_for_stream(
        self,
        stream: OrderFlowStream,
    ) -> tuple[OrderFlowConflictRecord, ...]:
        """Return quarantined revisions without promoting them to canonical data."""

        try:
            with closing(self._connect()) as connection:
                rows = connection.execute(
                    """
                    SELECT existing_record_id, incoming_record_type, incoming_record_json,
                           raw_payload_id, detected_at
                    FROM order_flow_conflicts
                    WHERE source = ?
                      AND venue = ?
                      AND instrument = ?
                      AND instrument_type = ?
                      AND record_type = ?
                    ORDER BY event_time, incoming_record_id
                    """,
                    (
                        stream.source,
                        stream.venue,
                        stream.instrument,
                        stream.instrument_type.value,
                        stream.record_type.value,
                    ),
                ).fetchall()
            try:
                conflicts = tuple(
                    OrderFlowConflictRecord(
                        stream=stream,
                        existing_record_id=UUID(str(row["existing_record_id"])),
                        incoming_record=self._record_from_json(
                            record_type_value=str(row["incoming_record_type"]),
                            value=str(row["incoming_record_json"]),
                        ),
                        raw_payload_id=(
                            None
                            if row["raw_payload_id"] is None
                            else UUID(str(row["raw_payload_id"]))
                        ),
                        detected_at=datetime.fromisoformat(str(row["detected_at"])),
                    )
                    for row in rows
                )
            except (ValueError, ValidationError) as error:
                raise SQLiteOrderFlowStorageError(
                    SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                    "stored order-flow conflict violates its contract",
                ) from error
            return conflicts
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("order-flow conflict read failed") from error

    def _initialize_schema(self) -> None:
        try:
            with closing(self._connect()) as connection, connection:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0]) if version_row is not None else 0
                if version not in {0, SQLITE_ORDER_FLOW_SCHEMA_VERSION}:
                    raise SQLiteOrderFlowStorageError(
                        SQLiteOrderFlowStorageErrorCode.UNSUPPORTED_SCHEMA,
                        f"database schema version {version} is not supported",
                    )
                if version == SQLITE_ORDER_FLOW_SCHEMA_VERSION:
                    self._validate_schema_identity(connection)
                    return
                connection.executescript(
                    """
                    CREATE TABLE order_flow_storage_metadata (
                        storage_format TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );

                    INSERT INTO order_flow_storage_metadata (
                        storage_format,
                        schema_version
                    ) VALUES ('wealth.order_flow', 1);

                    CREATE TABLE raw_order_flow_payloads (
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

                    CREATE TABLE canonical_order_flow_records (
                        natural_key_json TEXT PRIMARY KEY,
                        record_id TEXT NOT NULL UNIQUE,
                        record_type TEXT NOT NULL,
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        instrument_type TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    );

                    CREATE TABLE order_flow_conflicts (
                        source TEXT NOT NULL,
                        venue TEXT NOT NULL,
                        instrument TEXT NOT NULL,
                        instrument_type TEXT NOT NULL,
                        record_type TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        existing_record_id TEXT NOT NULL,
                        incoming_record_id TEXT NOT NULL,
                        incoming_record_type TEXT NOT NULL,
                        incoming_record_json TEXT NOT NULL,
                        raw_payload_id TEXT,
                        detected_at TEXT NOT NULL,
                        PRIMARY KEY (existing_record_id, incoming_record_id),
                        FOREIGN KEY (existing_record_id)
                            REFERENCES canonical_order_flow_records(record_id),
                        FOREIGN KEY (raw_payload_id)
                            REFERENCES raw_order_flow_payloads(record_id)
                    );

                    CREATE TABLE order_flow_raw_lineage (
                        canonical_record_id TEXT NOT NULL,
                        raw_payload_id TEXT NOT NULL,
                        PRIMARY KEY (canonical_record_id, raw_payload_id),
                        FOREIGN KEY (canonical_record_id)
                            REFERENCES canonical_order_flow_records(record_id),
                        FOREIGN KEY (raw_payload_id)
                            REFERENCES raw_order_flow_payloads(record_id)
                    );

                    CREATE INDEX canonical_order_flow_stream_index
                    ON canonical_order_flow_records (
                        source,
                        venue,
                        instrument,
                        instrument_type,
                        record_type,
                        event_time
                    );

                    CREATE INDEX order_flow_conflicts_stream_index
                    ON order_flow_conflicts (
                        source,
                        venue,
                        instrument,
                        instrument_type,
                        record_type,
                        event_time
                    );
                    """
                )
                connection.execute(f"PRAGMA user_version = {SQLITE_ORDER_FLOW_SCHEMA_VERSION}")
        except SQLiteOrderFlowStorageError:
            raise
        except sqlite3.DatabaseError as error:
            raise self._storage_failure("order-flow storage initialization failed") from error

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _validate_schema_identity(connection: sqlite3.Connection) -> None:
        table = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'order_flow_storage_metadata'
            """
        ).fetchone()
        if table is None:
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.UNSUPPORTED_SCHEMA,
                "database is not a WEALTH order-flow store",
            )
        row = connection.execute(
            """
            SELECT storage_format, schema_version
            FROM order_flow_storage_metadata
            """
        ).fetchone()
        try:
            valid_metadata = (
                row is not None
                and str(row["storage_format"]) == "wealth.order_flow"
                and int(row["schema_version"]) == SQLITE_ORDER_FLOW_SCHEMA_VERSION
            )
        except (TypeError, ValueError):
            valid_metadata = False
        if not valid_metadata:
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.UNSUPPORTED_SCHEMA,
                "order-flow storage metadata is not supported",
            )

    @staticmethod
    def _append_raw(
        connection: sqlite3.Connection,
        payload: RawMarketPayload,
    ) -> RawPayloadWriteResult:
        row = connection.execute(
            """
            SELECT record_id, source, venue, media_type, observed_at, processed_at,
                   payload_sha256, payload, lineage_json
            FROM raw_order_flow_payloads
            WHERE record_id = ?
            """,
            (str(payload.record_id),),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO raw_order_flow_payloads (
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

        existing = SQLiteOrderFlowStore._raw_payload_from_row(row)
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
    def _append_record(
        *,
        connection: sqlite3.Connection,
        record: OrderFlowRecord,
        raw_payload_id: UUID | None,
    ) -> OrderFlowWriteResult:
        natural_key_json = SQLiteOrderFlowStore._natural_key_json(record)
        row = connection.execute(
            """
            SELECT natural_key_json, record_id, record_type, record_json
            FROM canonical_order_flow_records
            WHERE natural_key_json = ?
            """,
            (natural_key_json,),
        ).fetchone()
        if row is None:
            reused_identity = connection.execute(
                """
                SELECT natural_key_json
                FROM canonical_order_flow_records
                WHERE record_id = ?
                """,
                (str(record.record_id),),
            ).fetchone()
            if reused_identity is not None:
                raise SQLiteOrderFlowStorageError(
                    SQLiteOrderFlowStorageErrorCode.IDENTITY_CONFLICT,
                    "canonical record_id is already attached to another natural key",
                )
            connection.execute(
                """
                INSERT INTO canonical_order_flow_records (
                    natural_key_json, record_id, record_type, source, venue,
                    instrument, instrument_type, event_time, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    natural_key_json,
                    str(record.record_id),
                    order_flow_record_type(record).value,
                    record.source,
                    record.venue,
                    record.instrument,
                    record.instrument_type.value,
                    record.event_time.isoformat(),
                    record.model_dump_json(),
                ),
            )
            if raw_payload_id is not None:
                SQLiteOrderFlowStore._link_raw_payload(
                    connection,
                    canonical_record_id=record.record_id,
                    raw_payload_id=raw_payload_id,
                )
            return OrderFlowWriteResult(
                status=OrderFlowWriteStatus.INSERTED,
                record_type=order_flow_record_type(record),
                incoming_record_id=record.record_id,
            )

        existing = SQLiteOrderFlowStore._validated_record_row(
            record_type_value=str(row["record_type"]),
            record_json=str(row["record_json"]),
            natural_key_json=str(row["natural_key_json"]),
        )
        if existing.market_values == record.market_values:
            if raw_payload_id is not None:
                SQLiteOrderFlowStore._link_raw_payload(
                    connection,
                    canonical_record_id=existing.record_id,
                    raw_payload_id=raw_payload_id,
                )
            return OrderFlowWriteResult(
                status=OrderFlowWriteStatus.DUPLICATE,
                record_type=order_flow_record_type(record),
                incoming_record_id=record.record_id,
                existing_record_id=existing.record_id,
            )

        connection.execute(
            """
            INSERT OR IGNORE INTO order_flow_conflicts (
                source, venue, instrument, instrument_type, record_type,
                event_time, existing_record_id, incoming_record_id,
                incoming_record_type, incoming_record_json, raw_payload_id, detected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.source,
                record.venue,
                record.instrument,
                record.instrument_type.value,
                order_flow_record_type(record).value,
                record.event_time.isoformat(),
                str(existing.record_id),
                str(record.record_id),
                order_flow_record_type(record).value,
                record.model_dump_json(),
                None if raw_payload_id is None else str(raw_payload_id),
                record.processed_at.isoformat(),
            ),
        )
        return OrderFlowWriteResult(
            status=OrderFlowWriteStatus.CONFLICT,
            record_type=order_flow_record_type(record),
            incoming_record_id=record.record_id,
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
            INSERT OR IGNORE INTO order_flow_raw_lineage (
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
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                "stored raw order-flow payload violates its contract",
            ) from error

    @staticmethod
    def _record_from_json(
        *,
        record_type_value: str,
        value: str,
    ) -> OrderFlowRecord:
        try:
            record_type = OrderFlowRecordType(record_type_value)
            if record_type is OrderFlowRecordType.TRADE:
                return CanonicalTrade.model_validate_json(value)
            if record_type is OrderFlowRecordType.TICKER:
                return CanonicalTicker.model_validate_json(value)
            return CanonicalBestBidAsk.model_validate_json(value)
        except (ValueError, ValidationError) as error:
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                "stored canonical order-flow record violates its contract",
            ) from error

    @staticmethod
    def _validated_record_row(
        *,
        record_type_value: str,
        record_json: str,
        natural_key_json: str,
    ) -> OrderFlowRecord:
        record = SQLiteOrderFlowStore._record_from_json(
            record_type_value=record_type_value,
            value=record_json,
        )
        if (
            order_flow_record_type(record).value != record_type_value
            or SQLiteOrderFlowStore._natural_key_json(record) != natural_key_json
        ):
            raise SQLiteOrderFlowStorageError(
                SQLiteOrderFlowStorageErrorCode.CORRUPT_RECORD,
                "stored order-flow identity does not match canonical content",
            )
        return record

    @staticmethod
    def _natural_key_json(record: OrderFlowRecord) -> str:
        common: dict[str, object] = {
            "record_type": order_flow_record_type(record).value,
            "source": record.source,
            "venue": record.venue,
            "instrument": record.instrument,
            "instrument_type": record.instrument_type.value,
        }
        if isinstance(record, CanonicalTrade):
            common["provider_trade_id"] = record.provider_trade_id
        else:
            common["event_time"] = record.event_time.isoformat()
            common["provider_sequence"] = record.provider_sequence
        return json.dumps(common, ensure_ascii=True, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _storage_failure(detail: str) -> SQLiteOrderFlowStorageError:
        return SQLiteOrderFlowStorageError(
            SQLiteOrderFlowStorageErrorCode.STORAGE_FAILURE,
            detail,
        )
