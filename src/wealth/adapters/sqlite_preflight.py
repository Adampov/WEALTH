"""Immutable fingerprinting for generated synthetic SQLite fixtures only.

This module intentionally talks to SQLite directly. It never imports or instantiates the
schema-installing adapters and is not connected to a CLI, service, or active runtime path.
"""

import hashlib
import json
import sqlite3
import stat
import tempfile
from collections.abc import Callable, Iterable
from enum import StrEnum
from pathlib import Path
from typing import Final, Never, cast

from pydantic import BaseModel

from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_MARKER_ROWS,
    MAX_SQLITE_MARKER_VALUE_BYTES,
    SQLiteColumnFingerprint,
    SQLiteExpectedStoreIdentity,
    SQLiteForeignKeyFingerprint,
    SQLiteIndexColumnFingerprint,
    SQLiteIndexFingerprint,
    SQLiteMarkerFingerprint,
    SQLiteMarkerReadStatus,
    SQLiteMarkerRowFingerprint,
    SQLiteMarkerValueFingerprint,
    SQLiteObjectType,
    SQLitePreflightRequest,
    SQLitePreflightResult,
    SQLitePreflightStatus,
    SQLiteSchemaObjectFingerprint,
    SQLiteSnapshotIdentity,
    SQLiteSnapshotObservation,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteStoreFingerprint,
    SQLiteTableFingerprint,
    SQLiteTriggerFingerprint,
)

__all__ = [
    "SQLITE_EXPECTED_STORE_IDENTITIES",
    "SQLitePreflightError",
    "SQLitePreflightErrorCode",
    "fingerprint_synthetic_sqlite_fixture",
]

_SCHEMA_HASH_DOMAIN: Final[bytes] = b"wealth.sqlite-schema-fingerprint.v1\0"
_STORE_HASH_DOMAIN: Final[bytes] = b"wealth.sqlite-store-fingerprint.v1\0"
_HASH_CHUNK_SIZE: Final[int] = 1024 * 1024
_MAX_SCHEMA_OBJECTS: Final[int] = 512
_MAX_COLUMNS_PER_TABLE: Final[int] = 512
_MAX_FOREIGN_KEYS_PER_TABLE: Final[int] = 512
_MAX_INDEXES_PER_TABLE: Final[int] = 512
_MAX_INDEX_TERMS: Final[int] = 512
_MAX_MATERIALIZED_ROWS: Final[int] = _MAX_SCHEMA_OBJECTS + 1
_MAX_DDL_CHARACTERS: Final[int] = 1_000_000
_MAX_DIRECTORY_ENTRIES: Final[int] = 1024
_MAX_FIXTURE_BYTES: Final[int] = 128 * 1024 * 1024
_SIDECAR_SUFFIXES: Final[tuple[str, ...]] = ("-journal", "-wal", "-shm")

_MARKER_SPECS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    (
        "order_flow_storage_metadata",
        ("storage_format", "schema_version"),
    ),
    (
        "public_trade_collection_metadata",
        ("storage_format", "schema_version"),
    ),
)

_ALLOWED_PRAGMAS: Final[frozenset[str]] = frozenset(
    {
        "application_id",
        "encoding",
        "page_count",
        "table_list",
        "user_version",
    }
)
_ALLOWED_ARGUMENT_PRAGMAS: Final[frozenset[str]] = frozenset(
    {
        "foreign_key_list",
        "index_list",
        "index_xinfo",
        "table_xinfo",
    }
)
_ALLOWED_FUNCTIONS: Final[frozenset[str]] = frozenset(
    {
        "hex",
        "length",
        "pragma_foreign_key_list",
        "pragma_index_list",
        "pragma_index_xinfo",
        "pragma_table_xinfo",
        "typeof",
    }
)
_ALLOWED_READ_COLUMNS: Final[dict[str, frozenset[str]]] = {
    "sqlite_master": frozenset({"name", "sql", "tbl_name", "type"}),
    "sqlite_schema": frozenset({"name", "sql", "tbl_name", "type"}),
    "pragma_table_xinfo": frozenset(
        {"cid", "dflt_value", "hidden", "name", "notnull", "pk", "type"}
    ),
    "pragma_foreign_key_list": frozenset(
        {"from", "id", "match", "on_delete", "on_update", "seq", "table", "to"}
    ),
    "pragma_index_list": frozenset({"name", "origin", "partial", "seq", "unique"}),
    "pragma_index_xinfo": frozenset({"cid", "coll", "desc", "key", "name", "seqno"}),
    "order_flow_storage_metadata": frozenset({"schema_version", "storage_format"}),
    "public_trade_collection_metadata": frozenset({"schema_version", "storage_format"}),
}

SQLiteParameter = str | int | float | bytes | None


class _ImmutableSQLiteCursor:
    """Expose already materialized rows without retaining a raw SQLite cursor."""

    __slots__ = ("__position", "__rows")

    def __init__(self, rows: tuple[sqlite3.Row, ...]) -> None:
        self.__rows = rows
        self.__position = 0

    def fetchone(self) -> sqlite3.Row | None:
        """Fetch one metadata row."""

        if self.__position >= len(self.__rows):
            return None
        row = self.__rows[self.__position]
        self.__position += 1
        return row

    def fetchmany(self, size: int) -> list[sqlite3.Row]:
        """Fetch a bounded metadata page."""

        if type(size) is not int or not 1 <= size <= _MAX_MATERIALIZED_ROWS:
            raise ValueError("immutable SQLite fetch size is outside the bounded range")
        end = min(self.__position + size, len(self.__rows))
        rows = list(self.__rows[self.__position : end])
        self.__position = end
        return rows


class _ImmutableSQLiteConnection:
    """Expose only the operations required by fingerprinting.

    Keeping the raw connection private prevents callers from replacing the authorizer, opening
    writable blobs, loading extensions, deserializing another database, or using ``backup()`` to
    create a new file.
    """

    __slots__ = ("__connection",)

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.__connection = connection

    def execute(
        self,
        sql: str,
        parameters: tuple[SQLiteParameter, ...] = (),
    ) -> _ImmutableSQLiteCursor:
        """Execute one authorizer-checked statement."""

        cursor = self.__connection.execute(sql, parameters)
        try:
            rows = cast(
                tuple[sqlite3.Row, ...],
                tuple(cursor.fetchmany(_MAX_MATERIALIZED_ROWS)),
            )
        finally:
            cursor.close()
        return _ImmutableSQLiteCursor(rows)

    def serialize(self) -> bytes:
        """Return the exact main-database image opened by SQLite."""

        return self.__connection.serialize(name="main")

    def set_trace_callback(
        self,
        trace_callback: Callable[[str], None] | None,
    ) -> None:
        """Permit read-only test tracing without exposing authorizer mutation."""

        self.__connection.set_trace_callback(trace_callback)

    def close(self) -> None:
        """Close the private raw SQLite connection."""

        self.__connection.close()


class SQLitePreflightErrorCode(StrEnum):
    """Bounded operational failures that cannot yield trustworthy evidence."""

    MISSING_SOURCE = "missing_source"
    INVALID_SOURCE = "invalid_source"
    SYMLINK_SOURCE = "symlink_source"
    ALIASED_SOURCE = "aliased_source"
    SIDECAR_PRESENT = "sidecar_present"
    SOURCE_READ_FAILED = "source_read_failed"
    SOURCE_CHANGED = "source_changed"
    DIRECTORY_CHANGED = "directory_changed"
    SQLITE_OPEN_FAILED = "sqlite_open_failed"
    SQLITE_READ_FAILED = "sqlite_read_failed"
    INVALID_SCHEMA = "invalid_schema"
    RESOURCE_LIMIT = "resource_limit"


class SQLitePreflightError(RuntimeError):
    """Fail closed whenever immutable preflight evidence is not trustworthy."""

    def __init__(self, code: SQLitePreflightErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


def _marker_value(storage_class: SQLiteStorageClass, value: str) -> SQLiteMarkerValueFingerprint:
    encoded = value.encode("utf-8")
    return SQLiteMarkerValueFingerprint(
        storage_class=storage_class,
        blob_hex=encoded.hex().upper(),
        byte_length=len(encoded),
    )


def _expected_marker(table_name: str, storage_format: str) -> SQLiteMarkerFingerprint:
    return SQLiteMarkerFingerprint(
        table_name=table_name,
        column_names=("storage_format", "schema_version"),
        read_status=SQLiteMarkerReadStatus.READABLE,
        rows=(
            SQLiteMarkerRowFingerprint(
                values=(
                    _marker_value(SQLiteStorageClass.TEXT, storage_format),
                    _marker_value(SQLiteStorageClass.INTEGER, "1"),
                )
            ),
        ),
    )


_ORDER_FLOW_MARKER: Final[SQLiteMarkerFingerprint] = _expected_marker(
    "order_flow_storage_metadata",
    "wealth.order_flow",
)
_PUBLIC_TRADE_MARKER: Final[SQLiteMarkerFingerprint] = _expected_marker(
    "public_trade_collection_metadata",
    "wealth.public_trade_collection",
)

# The reviewed digests are pinned below after generation from all eight empty canonical fixtures.
# They are logical layout identities, not whole-file hashes.
SQLITE_EXPECTED_STORE_IDENTITIES: tuple[SQLiteExpectedStoreIdentity, ...] = (
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.MARKET,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="c9ebf4efe2c754f2cad9924aef26b400886ccbbc2f8e0b95bc5629445a738d13",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.ORDER_FLOW,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(_ORDER_FLOW_MARKER,),
        store_sha256="250e7ab52ed7f26406c6dee0359a71fd913885ed7a1b594b33bb9868378cc706",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.HISTORICAL_COLLECTION,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="5b8f1b2e8ab532725f8a516acc7dd779e74a2fa0abeab7dc5265412738e4e86a",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.CONTINUOUS_COLLECTION,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="7baf5c988445caa212cc9eae0e58d5439452aa02a7a9f4010a5a6f497f0a9441",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.COLLECTOR_SERVICE,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="8894378abc8b649a2efd46a7f41d380a018866f0e04a17e5c83041ae47afc09c",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(_PUBLIC_TRADE_MARKER,),
        store_sha256="3d3cb73e1a5a7086f91748c4dcb92fc33aee5c3f59b5e62e9df966df00d2a9c1",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.RATE_BUDGET,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="89172f205882739f00629f61619151d17acf31a8f3f09f5c1f8bf9ea13526b10",
    ),
    SQLiteExpectedStoreIdentity(
        family=SQLiteStoreFamily.RECONCILIATION,
        layout_version=1,
        encoding="UTF-8",
        application_id=0,
        user_version=1,
        markers=(),
        store_sha256="a8cee5c79a8b7a4420195ff825268022095f24fb1f52ecfbebc71ab13a31d636",
    ),
)


def fingerprint_synthetic_sqlite_fixture(
    request: SQLitePreflightRequest,
) -> SQLitePreflightResult:
    """Fingerprint one generated fixture and match it against the pinned registry."""

    expected_identity = next(
        (
            identity
            for identity in SQLITE_EXPECTED_STORE_IDENTITIES
            if identity.family is request.expected_family
            and identity.layout_version == request.expected_layout_version
        ),
        None,
    )
    if expected_identity is None:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "the requested SQLite family and layout version are not registered",
        )

    observation = _observe_synthetic_sqlite_fixture(request.fixture_path)
    matched_families = tuple(
        identity.family
        for identity in SQLITE_EXPECTED_STORE_IDENTITIES
        if identity.store_sha256 == observation.fingerprint.store_sha256
    )
    if len(matched_families) > 1:
        status = SQLitePreflightStatus.AMBIGUOUS
    elif not matched_families:
        status = SQLitePreflightStatus.MISMATCH
    elif matched_families[0] is not request.expected_family:
        status = SQLitePreflightStatus.WRONG_FAMILY
    else:
        status = SQLitePreflightStatus.MATCHED

    return SQLitePreflightResult(
        source_kind=request.source_kind,
        fixture_id=request.fixture_id,
        status=status,
        expected_identity=expected_identity,
        matched_families=matched_families,
        observation=observation,
    )


def _observe_synthetic_sqlite_fixture(path: Path) -> SQLiteSnapshotObservation:
    resolved_path = _resolve_existing_regular_file(path)
    directory_entries_before = _directory_entries(resolved_path.parent)
    _reject_sidecars(resolved_path)
    source_before = _capture_stable_identity(resolved_path)

    connection: _ImmutableSQLiteConnection | None = None
    original_error: Exception | None = None
    fingerprint: SQLiteStoreFingerprint | None = None
    try:
        connection = _open_immutable_connection(resolved_path)
        _assert_connection_matches_snapshot(connection, source_before)
        fingerprint = _fingerprint_connection(connection)
    except Exception as error:
        original_error = error
    finally:
        if connection is not None:
            connection.close()

    try:
        _reject_sidecars(resolved_path)
        source_after = _capture_stable_identity(resolved_path)
        directory_entries_after = _directory_entries(resolved_path.parent)
        if source_after != source_before:
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.SOURCE_CHANGED,
                "the SQLite fixture changed while it was being inspected",
            )
        if directory_entries_after != directory_entries_before:
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.DIRECTORY_CHANGED,
                "the fixture directory changed while SQLite was being inspected",
            )
    except SQLitePreflightError as invariant_error:
        if original_error is not None:
            raise invariant_error from original_error
        raise

    if original_error is not None:
        _raise_inspection_error(original_error)
    if fingerprint is None:
        raise AssertionError("SQLite inspection completed without a fingerprint or an error")

    return SQLiteSnapshotObservation(
        snapshot_path=resolved_path,
        source_before=source_before,
        source_after=source_after,
        directory_entries_before=directory_entries_before,
        directory_entries_after=directory_entries_after,
        fingerprint=fingerprint,
    )


def _raise_inspection_error(error: Exception) -> Never:
    if isinstance(error, SQLitePreflightError):
        raise error
    if isinstance(error, sqlite3.Error):
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SQLITE_READ_FAILED,
            "SQLite metadata could not be read exactly",
        ) from error
    raise error


def _resolve_existing_regular_file(path: Path) -> Path:
    if not isinstance(path, Path):
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SOURCE,
            "fixture_path must be a pathlib.Path",
        )
    _reject_reparse_path(path)
    try:
        resolved_path = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.MISSING_SOURCE,
            "the generated SQLite fixture does not exist",
        ) from error
    except OSError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SOURCE,
            "the generated SQLite fixture path cannot be resolved",
        ) from error

    try:
        temporary_root = Path(tempfile.gettempdir()).resolve(strict=True)
        resolved_path.relative_to(temporary_root)
    except (FileNotFoundError, OSError, ValueError) as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SOURCE,
            "generated SQLite fixtures must reside under the system temporary root",
        ) from error

    try:
        source_stat = resolved_path.stat()
    except OSError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SOURCE,
            "the generated SQLite fixture cannot be inspected",
        ) from error
    if not stat.S_ISREG(source_stat.st_mode):
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SOURCE,
            "the generated SQLite fixture must be a regular file",
        )
    if source_stat.st_nlink != 1:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.ALIASED_SOURCE,
            "a generated SQLite fixture may not be a hard-linked alias",
        )
    return resolved_path


def _reject_reparse_path(path: Path) -> None:
    absolute_path = path.absolute()
    for candidate in (absolute_path, *absolute_path.parents):
        try:
            candidate_stat = candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.INVALID_SOURCE,
                "the generated SQLite fixture path cannot be inspected safely",
            ) from error
        file_attributes = getattr(candidate_stat, "st_file_attributes", 0)
        reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if stat.S_ISLNK(candidate_stat.st_mode) or (
            reparse_attribute and file_attributes & reparse_attribute
        ):
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.SYMLINK_SOURCE,
                "a generated SQLite fixture path may not traverse a link or reparse point",
            )


def _directory_entries(directory: Path) -> tuple[str, ...]:
    try:
        entries: list[str] = []
        for entry in directory.iterdir():
            entries.append(entry.name)
            if len(entries) > _MAX_DIRECTORY_ENTRIES:
                raise SQLitePreflightError(
                    SQLitePreflightErrorCode.RESOURCE_LIMIT,
                    "fixture directory exceeded the synthetic-fixture entry limit",
                )
        return tuple(sorted(entries))
    except OSError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SOURCE_READ_FAILED,
            "the fixture directory cannot be snapshotted",
        ) from error


def _reject_sidecars(path: Path) -> None:
    for suffix in _SIDECAR_SUFFIXES:
        sibling = path.with_name(path.name + suffix)
        try:
            sibling.lstat()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.SOURCE_READ_FAILED,
                f"the possible {suffix} sidecar cannot be inspected safely",
            ) from error
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SIDECAR_PRESENT,
            f"immutable inspection rejects the existing {suffix} sidecar",
        )


def _capture_stable_identity(path: Path) -> SQLiteSnapshotIdentity:
    try:
        stat_before = path.stat()
        if stat_before.st_size > _MAX_FIXTURE_BYTES:
            raise SQLitePreflightError(
                SQLitePreflightErrorCode.RESOURCE_LIMIT,
                "SQLite fixture exceeded the synthetic-fixture byte limit",
            )
        digest = hashlib.sha256()
        byte_count = 0
        with path.open("rb") as source:
            while chunk := source.read(_HASH_CHUNK_SIZE):
                byte_count += len(chunk)
                if byte_count > _MAX_FIXTURE_BYTES:
                    raise SQLitePreflightError(
                        SQLitePreflightErrorCode.RESOURCE_LIMIT,
                        "SQLite fixture grew beyond the synthetic-fixture byte limit",
                    )
                digest.update(chunk)
        stat_after = path.stat()
    except OSError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SOURCE_READ_FAILED,
            "the SQLite fixture cannot be hashed",
        ) from error

    before_fields = (
        stat_before.st_dev,
        stat_before.st_ino,
        stat_before.st_size,
        stat_before.st_mtime_ns,
    )
    after_fields = (
        stat_after.st_dev,
        stat_after.st_ino,
        stat_after.st_size,
        stat_after.st_mtime_ns,
    )
    if before_fields != after_fields or byte_count != stat_after.st_size:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SOURCE_CHANGED,
            "the SQLite fixture changed while its identity was being captured",
        )

    return SQLiteSnapshotIdentity(
        size_bytes=stat_after.st_size,
        modified_time_ns=stat_after.st_mtime_ns,
        sha256=digest.hexdigest(),
        device=stat_after.st_dev,
        inode=stat_after.st_ino,
    )


def _assert_connection_matches_snapshot(
    connection: _ImmutableSQLiteConnection,
    source_identity: SQLiteSnapshotIdentity,
) -> None:
    """Bind SQLite's actual open database image to the path evidence captured before open."""

    try:
        database_image = connection.serialize()
    except sqlite3.Error as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SQLITE_READ_FAILED,
            "SQLite could not serialize the immutable fixture image",
        ) from error
    if (
        len(database_image) != source_identity.size_bytes
        or hashlib.sha256(database_image).hexdigest() != source_identity.sha256
    ):
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SOURCE_CHANGED,
            "SQLite opened bytes that do not match the captured fixture snapshot",
        )


def _open_immutable_connection(path: Path) -> _ImmutableSQLiteConnection:
    uri = f"{path.as_uri()}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True)
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, _MAX_DDL_CHARACTERS)
        connection.row_factory = sqlite3.Row
        connection.set_authorizer(_read_only_authorizer)
        return _ImmutableSQLiteConnection(connection)
    except sqlite3.Error as error:
        if "connection" in locals():
            connection.close()
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.SQLITE_OPEN_FAILED,
            "the generated fixture could not be opened as an immutable SQLite database",
        ) from error


def _read_only_authorizer(
    action: int,
    argument_one: str | None,
    argument_two: str | None,
    database_name: str | None,
    trigger_name: str | None,
) -> int:
    del database_name, trigger_name
    if action == sqlite3.SQLITE_SELECT:
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_READ:
        table_name = "" if argument_one is None else argument_one.casefold()
        column_name = "" if argument_two is None else argument_two.casefold()
        if column_name in _ALLOWED_READ_COLUMNS.get(table_name, frozenset()):
            return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_PRAGMA:
        pragma_name = "" if argument_one is None else argument_one.casefold()
        if (pragma_name in _ALLOWED_PRAGMAS and argument_two is None) or (
            pragma_name in _ALLOWED_ARGUMENT_PRAGMAS and argument_two is not None
        ):
            return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_FUNCTION:
        function_name = argument_two if argument_two is not None else argument_one
        if function_name is not None and function_name.casefold() in _ALLOWED_FUNCTIONS:
            return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def _fingerprint_connection(
    connection: _ImmutableSQLiteConnection,
) -> SQLiteStoreFingerprint:
    encoding = _scalar_pragma_text(connection, "PRAGMA encoding")
    application_id = _scalar_pragma_int(connection, "PRAGMA application_id")
    user_version = _scalar_pragma_int(connection, "PRAGMA user_version")
    schema_rows = _bounded_rows(
        connection.execute(
            """
            SELECT type, name, tbl_name, sql
            FROM main.sqlite_schema
            ORDER BY type, name, tbl_name
            LIMIT ?
            """,
            (_MAX_SCHEMA_OBJECTS + 1,),
        ),
        maximum=_MAX_SCHEMA_OBJECTS,
        label="schema objects",
    )

    schema_objects = tuple(_schema_object(row) for row in schema_rows)
    table_flags = _table_flags(connection)
    tables = tuple(
        _table_fingerprint(connection, row, table_flags)
        for row in schema_rows
        if row["type"] == SQLiteObjectType.TABLE.value
    )
    indexes = tuple(
        sorted(
            (
                index
                for table in tables
                for index in _indexes_for_table(
                    connection,
                    table.name,
                    schema_objects,
                )
            ),
            key=lambda item: (item.table_name, item.name),
        )
    )
    triggers = tuple(
        SQLiteTriggerFingerprint(
            name=_required_text(row["name"], "trigger name"),
            table_name=_required_text(row["tbl_name"], "trigger table"),
            normalized_ddl=_required_ddl(row["sql"], "trigger"),
        )
        for row in schema_rows
        if row["type"] == SQLiteObjectType.TRIGGER.value
    )
    markers = _marker_fingerprints(connection, tables)

    schema_payload = {
        "schema_version": "1.0",
        "schema_objects": _json_models(schema_objects),
        "tables": _json_models(tables),
        "indexes": _json_models(indexes),
        "triggers": _json_models(triggers),
    }
    schema_sha256 = _canonical_digest(_SCHEMA_HASH_DOMAIN, schema_payload)
    store_payload = {
        "schema_version": "1.0",
        "encoding": encoding,
        "application_id": application_id,
        "user_version": user_version,
        "markers": _json_models(markers),
        "schema_sha256": schema_sha256,
    }
    store_sha256 = _canonical_digest(_STORE_HASH_DOMAIN, store_payload)
    return SQLiteStoreFingerprint(
        encoding=encoding,
        application_id=application_id,
        user_version=user_version,
        markers=markers,
        schema_objects=schema_objects,
        tables=tables,
        indexes=indexes,
        triggers=triggers,
        schema_sha256=schema_sha256,
        store_sha256=store_sha256,
    )


def _scalar_pragma_text(connection: _ImmutableSQLiteConnection, sql: str) -> str:
    row = connection.execute(sql).fetchone()
    if row is None or type(row[0]) is not str or not row[0]:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned an invalid text pragma",
        )
    return row[0]


def _scalar_pragma_int(connection: _ImmutableSQLiteConnection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    if row is None or type(row[0]) is not int:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned an invalid integer pragma",
        )
    return row[0]


def _bounded_rows(
    cursor: _ImmutableSQLiteCursor,
    *,
    maximum: int,
    label: str,
) -> tuple[sqlite3.Row, ...]:
    rows = tuple(cursor.fetchmany(maximum + 1))
    if len(rows) > maximum:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.RESOURCE_LIMIT,
            f"SQLite {label} exceeded the synthetic-fixture inspection limit",
        )
    return rows


def _schema_object(row: sqlite3.Row) -> SQLiteSchemaObjectFingerprint:
    try:
        object_type = SQLiteObjectType(_required_text(row["type"], "schema object type"))
    except ValueError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned an unsupported schema object type",
        ) from error
    raw_ddl = row["sql"]
    if raw_ddl is not None and type(raw_ddl) is not str:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned a non-text schema definition",
        )
    return SQLiteSchemaObjectFingerprint(
        object_type=object_type,
        name=_required_text(row["name"], "schema object name"),
        table_name=_required_text(row["tbl_name"], "schema object table"),
        normalized_ddl=None if raw_ddl is None else _normalize_ddl(raw_ddl),
    )


def _table_flags(
    connection: _ImmutableSQLiteConnection,
) -> dict[str, tuple[bool, bool]]:
    rows = _bounded_rows(
        connection.execute("PRAGMA main.table_list"),
        maximum=_MAX_SCHEMA_OBJECTS,
        label="table-list rows",
    )
    flags: dict[str, tuple[bool, bool]] = {}
    for row in rows:
        if row["schema"] == "main" and row["type"] in {"table", "shadow"}:
            name = _required_text(row["name"], "table-list name")
            flags[name] = (
                _sqlite_bool(row["wr"], "without-rowid"),
                _sqlite_bool(row["strict"], "strict"),
            )
    return flags


def _table_fingerprint(
    connection: _ImmutableSQLiteConnection,
    schema_row: sqlite3.Row,
    flags: dict[str, tuple[bool, bool]],
) -> SQLiteTableFingerprint:
    table_name = _required_text(schema_row["name"], "table name")
    columns = tuple(
        SQLiteColumnFingerprint(
            cid=_required_int(row["cid"], "column cid"),
            name=_required_text(row["name"], "column name"),
            declared_type=_required_text_allow_empty(row["type"], "declared column type"),
            not_null=_sqlite_bool(row["notnull"], "column not-null"),
            default_sql=_optional_text(row["dflt_value"], "column default"),
            primary_key_ordinal=_nonnegative_int(row["pk"], "primary-key ordinal"),
            hidden=_nonnegative_int(row["hidden"], "column hidden flag"),
        )
        for row in _bounded_rows(
            connection.execute(
                """
                SELECT cid, name, type, "notnull", dflt_value, pk, hidden
                FROM pragma_table_xinfo(?)
                ORDER BY cid
                """,
                (table_name,),
            ),
            maximum=_MAX_COLUMNS_PER_TABLE,
            label=f"columns for {table_name}",
        )
    )
    foreign_keys = tuple(
        SQLiteForeignKeyFingerprint(
            identifier=_nonnegative_int(row["id"], "foreign-key id"),
            sequence=_nonnegative_int(row["seq"], "foreign-key sequence"),
            referenced_table=_required_text(row["table"], "foreign-key table"),
            from_column=_required_text(row["from"], "foreign-key source column"),
            to_column=_optional_text(row["to"], "foreign-key target column"),
            on_update=_required_text(row["on_update"], "foreign-key update action"),
            on_delete=_required_text(row["on_delete"], "foreign-key delete action"),
            match=_required_text(row["match"], "foreign-key match action"),
        )
        for row in _bounded_rows(
            connection.execute(
                """
                SELECT id, seq, "table", "from", "to", on_update, on_delete, "match"
                FROM pragma_foreign_key_list(?)
                ORDER BY id, seq
                """,
                (table_name,),
            ),
            maximum=_MAX_FOREIGN_KEYS_PER_TABLE,
            label=f"foreign keys for {table_name}",
        )
    )
    raw_ddl = schema_row["sql"]
    normalized_ddl = (
        None if raw_ddl is None else _normalize_ddl(_required_text(raw_ddl, "table DDL"))
    )
    try:
        without_rowid, strict = flags[table_name]
    except KeyError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite table-list evidence is incomplete",
        ) from error
    return SQLiteTableFingerprint(
        name=table_name,
        normalized_ddl=normalized_ddl,
        columns=columns,
        foreign_keys=foreign_keys,
        without_rowid=without_rowid,
        strict=strict,
    )


def _indexes_for_table(
    connection: _ImmutableSQLiteConnection,
    table_name: str,
    schema_objects: tuple[SQLiteSchemaObjectFingerprint, ...],
) -> tuple[SQLiteIndexFingerprint, ...]:
    ddl_by_name = {
        item.name: item.normalized_ddl
        for item in schema_objects
        if item.object_type is SQLiteObjectType.INDEX
    }
    indexes: list[SQLiteIndexFingerprint] = []
    rows = _bounded_rows(
        connection.execute(
            """
            SELECT seq, name, "unique", origin, partial
            FROM pragma_index_list(?)
            ORDER BY name, seq
            """,
            (table_name,),
        ),
        maximum=_MAX_INDEXES_PER_TABLE,
        label=f"indexes for {table_name}",
    )
    for row in rows:
        index_name = _required_text(row["name"], "index name")
        terms = tuple(
            SQLiteIndexColumnFingerprint(
                sequence=_nonnegative_int(term["seqno"], "index term sequence"),
                cid=_required_int(term["cid"], "index term cid"),
                name=_optional_text(term["name"], "index term name"),
                descending=_sqlite_bool(term["desc"], "index descending flag"),
                collation=_optional_text(term["coll"], "index collation"),
                key=_sqlite_bool(term["key"], "index key flag"),
            )
            for term in _bounded_rows(
                connection.execute(
                    """
                    SELECT seqno, cid, name, "desc", coll, "key"
                    FROM pragma_index_xinfo(?)
                    ORDER BY seqno
                    """,
                    (index_name,),
                ),
                maximum=_MAX_INDEX_TERMS,
                label=f"terms for {index_name}",
            )
        )
        indexes.append(
            SQLiteIndexFingerprint(
                table_name=table_name,
                name=index_name,
                unique=_sqlite_bool(row["unique"], "index unique flag"),
                origin=_required_text(row["origin"], "index origin"),
                partial=_sqlite_bool(row["partial"], "index partial flag"),
                normalized_ddl=ddl_by_name.get(index_name),
                columns=terms,
            )
        )
    return tuple(indexes)


def _marker_fingerprints(
    connection: _ImmutableSQLiteConnection,
    tables: tuple[SQLiteTableFingerprint, ...],
) -> tuple[SQLiteMarkerFingerprint, ...]:
    table_by_name = {table.name: table for table in tables}
    markers: list[SQLiteMarkerFingerprint] = []
    for table_name, column_names in _MARKER_SPECS:
        table = table_by_name.get(table_name)
        if table is None:
            continue
        if tuple(column.name for column in table.columns) != column_names:
            markers.append(
                SQLiteMarkerFingerprint(
                    table_name=table_name,
                    column_names=column_names,
                    read_status=SQLiteMarkerReadStatus.INCOMPATIBLE_COLUMNS,
                    rows=(),
                )
            )
            continue
        markers.append(_read_marker(connection, table_name, column_names))
    return tuple(markers)


def _read_marker(
    connection: _ImmutableSQLiteConnection,
    table_name: str,
    column_names: tuple[str, ...],
) -> SQLiteMarkerFingerprint:
    expressions = ", ".join(
        (
            f'typeof("{column_name}") AS "type_{position}", '
            f'CASE WHEN length(CAST("{column_name}" AS BLOB)) '
            f"<= {MAX_SQLITE_MARKER_VALUE_BYTES} "
            f'THEN hex(CAST("{column_name}" AS BLOB)) END AS "hex_{position}", '
            f'length(CAST("{column_name}" AS BLOB)) AS "length_{position}"'
        )
        for position, column_name in enumerate(column_names)
    )
    sql = f'SELECT {expressions} FROM main."{table_name}" LIMIT {MAX_SQLITE_MARKER_ROWS + 1}'
    raw_rows = _bounded_rows(
        connection.execute(sql),
        maximum=MAX_SQLITE_MARKER_ROWS,
        label=f"marker rows for {table_name}",
    )
    rows: list[SQLiteMarkerRowFingerprint] = []
    for raw_row in raw_rows:
        values = tuple(
            _marker_value_from_row(raw_row, position) for position in range(len(column_names))
        )
        rows.append(SQLiteMarkerRowFingerprint(values=values))
    rows.sort(
        key=lambda row: tuple(
            (value.storage_class.value, value.blob_hex, value.byte_length) for value in row.values
        )
    )
    return SQLiteMarkerFingerprint(
        table_name=table_name,
        column_names=column_names,
        read_status=SQLiteMarkerReadStatus.READABLE,
        rows=tuple(rows),
    )


def _marker_value_from_row(
    row: sqlite3.Row,
    position: int,
) -> SQLiteMarkerValueFingerprint:
    storage_class = _storage_class(row[f"type_{position}"])
    byte_length = _nullable_length(row[f"length_{position}"])
    if byte_length > MAX_SQLITE_MARKER_VALUE_BYTES:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.RESOURCE_LIMIT,
            "SQLite marker cell exceeded the synthetic-fixture byte limit",
        )
    raw_hex = row[f"hex_{position}"]
    if raw_hex is None and storage_class is SQLiteStorageClass.NULL:
        blob_hex = ""
    else:
        blob_hex = _blob_hex(raw_hex)
    return SQLiteMarkerValueFingerprint(
        storage_class=storage_class,
        blob_hex=blob_hex,
        byte_length=byte_length,
    )


def _storage_class(value: object) -> SQLiteStorageClass:
    if type(value) is not str:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned an invalid marker storage class",
        )
    try:
        return SQLiteStorageClass(value)
    except ValueError as error:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned an unknown marker storage class",
        ) from error


def _blob_hex(value: object) -> str:
    if type(value) is not str:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned invalid marker byte evidence",
        )
    if len(value) % 2 or any(character not in "0123456789ABCDEF" for character in value):
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite returned malformed marker byte evidence",
        )
    return value


def _nullable_length(value: object) -> int:
    if value is None:
        return 0
    return _nonnegative_int(value, "marker byte length")


def _json_models(models: Iterable[BaseModel]) -> list[object]:
    return [model.model_dump(mode="json") for model in models]


def _canonical_digest(domain: bytes, payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(domain + encoded).hexdigest()


def _normalize_ddl(sql: str) -> str:
    """Collapse only unquoted whitespace/comments while preserving quoted bytes."""

    if "\x00" in sql:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite DDL may not contain a NUL character",
        )
    if len(sql) > _MAX_DDL_CHARACTERS:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.RESOURCE_LIMIT,
            "SQLite DDL exceeded the synthetic-fixture inspection limit",
        )

    output: list[str] = []
    pending_space = False
    state = "normal"
    position = 0
    while position < len(sql):
        character = sql[position]
        following = sql[position + 1] if position + 1 < len(sql) else ""
        if state == "normal":
            if character.isspace():
                pending_space = True
            elif character == "-" and following == "-":
                pending_space = True
                state = "line_comment"
                position += 1
            elif character == "/" and following == "*":
                pending_space = True
                state = "block_comment"
                position += 1
            else:
                if pending_space and output:
                    output.append(" ")
                pending_space = False
                output.append(character)
                if character == "'":
                    state = "single_quote"
                elif character == '"':
                    state = "double_quote"
                elif character == "`":
                    state = "backtick_quote"
                elif character == "[":
                    state = "bracket_quote"
        elif state == "line_comment":
            if character in "\r\n":
                state = "normal"
        elif state == "block_comment":
            if character == "*" and following == "/":
                state = "normal"
                position += 1
        elif state in {"single_quote", "double_quote", "backtick_quote"}:
            output.append(character)
            quote = {
                "single_quote": "'",
                "double_quote": '"',
                "backtick_quote": "`",
            }[state]
            if character == quote:
                if following == quote:
                    output.append(following)
                    position += 1
                else:
                    state = "normal"
        else:
            output.append(character)
            if character == "]":
                state = "normal"
        position += 1

    if state == "line_comment":
        state = "normal"
    if state != "normal":
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite DDL contains an unterminated quote or comment",
        )
    normalized = "".join(output).strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    if not normalized:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            "SQLite DDL must not normalize to empty text",
        )
    return normalized


def _required_text(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned an invalid {label}",
        )
    return value


def _required_text_allow_empty(value: object, label: str) -> str:
    if type(value) is not str:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned an invalid {label}",
        )
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned an invalid {label}",
        )
    return value


def _required_ddl(value: object, label: str) -> str:
    return _normalize_ddl(_required_text(value, f"{label} DDL"))


def _required_int(value: object, label: str) -> int:
    if type(value) is not int:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned an invalid {label}",
        )
    return value


def _nonnegative_int(value: object, label: str) -> int:
    integer = _required_int(value, label)
    if integer < 0:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned a negative {label}",
        )
    return integer


def _sqlite_bool(value: object, label: str) -> bool:
    integer = _required_int(value, label)
    if integer not in {0, 1}:
        raise SQLitePreflightError(
            SQLitePreflightErrorCode.INVALID_SCHEMA,
            f"SQLite returned an invalid {label}",
        )
    return bool(integer)
