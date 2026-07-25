"""Strict contracts for synthetic, immutable SQLite preflight evidence.

The contracts deliberately keep a trusted expected store identity separate from the
fingerprint observed from one snapshot. They are not wired into any runtime path and do not
authorize operator-database or row-level inspection.
"""

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Final, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

__all__ = [
    "MAX_SQLITE_MARKER_COLUMNS",
    "MAX_SQLITE_MARKER_ROWS",
    "MAX_SQLITE_MARKER_VALUE_BYTES",
    "SQLiteColumnFingerprint",
    "SQLiteExpectedStoreIdentity",
    "SQLiteForeignKeyFingerprint",
    "SQLiteIndexColumnFingerprint",
    "SQLiteIndexFingerprint",
    "SQLiteMarkerFingerprint",
    "SQLiteMarkerReadStatus",
    "SQLiteMarkerRowFingerprint",
    "SQLiteMarkerValueFingerprint",
    "SQLiteObjectType",
    "SQLitePreflightRequest",
    "SQLitePreflightResult",
    "SQLitePreflightStatus",
    "SQLiteSchemaObjectFingerprint",
    "SQLiteSnapshotIdentity",
    "SQLiteSnapshotObservation",
    "SQLiteStorageClass",
    "SQLiteStoreFamily",
    "SQLiteStoreFingerprint",
    "SQLiteTableFingerprint",
    "SQLiteTriggerFingerprint",
]

MAX_SQLITE_MARKER_COLUMNS: Final[int] = 16
MAX_SQLITE_MARKER_ROWS: Final[int] = 64
MAX_SQLITE_MARKER_VALUE_BYTES: Final[int] = 4096
ContractVersion = Literal["1.0"]
Sha256Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
NonEmptyText = Annotated[str, Field(min_length=1)]


class _StrictContract(BaseModel):
    """Apply the same fail-closed boundary to every nested evidence object."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SQLiteStorageClass(StrEnum):
    """SQLite storage classes represented without Python coercion."""

    NULL = "null"
    INTEGER = "integer"
    REAL = "real"
    TEXT = "text"
    BLOB = "blob"


class SQLiteObjectType(StrEnum):
    """Object kinds stored in ``main.sqlite_schema``."""

    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    TRIGGER = "trigger"


class SQLiteMarkerReadStatus(StrEnum):
    """Whether an approved storage-marker table could be read exactly."""

    READABLE = "readable"
    INCOMPATIBLE_COLUMNS = "incompatible_columns"


class SQLiteStoreFamily(StrEnum):
    """The eight current, explicitly registered SQLite layout families."""

    MARKET = "market"
    ORDER_FLOW = "order_flow"
    HISTORICAL_COLLECTION = "historical_collection"
    CONTINUOUS_COLLECTION = "continuous_collection"
    COLLECTOR_SERVICE = "collector_service"
    PUBLIC_TRADE_COLLECTION = "public_trade_collection"
    RATE_BUDGET = "rate_budget"
    RECONCILIATION = "reconciliation"


class SQLitePreflightStatus(StrEnum):
    """Outcome of matching observed evidence against trusted identities."""

    MATCHED = "matched"
    MISMATCH = "mismatch"
    WRONG_FAMILY = "wrong_family"
    AMBIGUOUS = "ambiguous"


class SQLiteMarkerValueFingerprint(_StrictContract):
    """One marker cell encoded without losing its SQLite storage class."""

    storage_class: SQLiteStorageClass
    blob_hex: Annotated[
        str,
        Field(
            max_length=MAX_SQLITE_MARKER_VALUE_BYTES * 2,
            pattern=r"^(?:[0-9A-F]{2})*$",
        ),
    ]
    byte_length: Annotated[int, Field(ge=0, le=MAX_SQLITE_MARKER_VALUE_BYTES)]

    @model_validator(mode="after")
    def byte_length_matches_hex_evidence(self) -> Self:
        """Reject marker evidence whose claimed length disagrees with its exact bytes."""

        if len(self.blob_hex) != self.byte_length * 2:
            raise ValueError("marker byte_length must equal the exact blob_hex length")
        if self.storage_class is SQLiteStorageClass.NULL and (self.blob_hex or self.byte_length):
            raise ValueError("a SQLite NULL marker must have empty byte evidence")
        return self


class SQLiteMarkerRowFingerprint(_StrictContract):
    """One deterministically ordered row from an approved marker table."""

    values: tuple[SQLiteMarkerValueFingerprint, ...]

    @model_validator(mode="after")
    def marker_row_is_bounded(self) -> Self:
        """Keep standalone row evidence finite."""

        if len(self.values) > MAX_SQLITE_MARKER_COLUMNS:
            raise ValueError("marker row exceeds the bounded column count")
        return self


class SQLiteMarkerFingerprint(_StrictContract):
    """Bounded evidence from one explicitly approved storage-marker table."""

    table_name: NonEmptyText
    column_names: tuple[NonEmptyText, ...]
    read_status: SQLiteMarkerReadStatus
    rows: tuple[SQLiteMarkerRowFingerprint, ...]

    @model_validator(mode="after")
    def marker_shape_is_consistent(self) -> Self:
        """Reject ambiguous marker evidence shapes."""

        if not self.column_names or len(self.column_names) != len(set(self.column_names)):
            raise ValueError("marker column_names must be non-empty and unique")
        if len(self.column_names) > MAX_SQLITE_MARKER_COLUMNS:
            raise ValueError("marker column_names exceed the bounded column count")
        if len(self.rows) > MAX_SQLITE_MARKER_ROWS:
            raise ValueError("marker rows exceed the bounded row count")
        if len(self.table_name) > 255 or any(
            len(column_name) > 255 for column_name in self.column_names
        ):
            raise ValueError("marker identifiers may not exceed 255 characters")
        if self.read_status is SQLiteMarkerReadStatus.INCOMPATIBLE_COLUMNS and self.rows:
            raise ValueError("an incompatible marker table cannot contain observed rows")
        if any(len(row.values) != len(self.column_names) for row in self.rows):
            raise ValueError("every marker row must match the declared marker columns")
        return self


class SQLiteColumnFingerprint(_StrictContract):
    """Exact ``PRAGMA table_xinfo`` evidence for one column."""

    cid: int
    name: NonEmptyText
    declared_type: str
    not_null: bool
    default_sql: str | None
    primary_key_ordinal: Annotated[int, Field(ge=0)]
    hidden: Annotated[int, Field(ge=0)]


class SQLiteForeignKeyFingerprint(_StrictContract):
    """Exact ``PRAGMA foreign_key_list`` evidence for one relationship."""

    identifier: Annotated[int, Field(ge=0)]
    sequence: Annotated[int, Field(ge=0)]
    referenced_table: NonEmptyText
    from_column: NonEmptyText
    to_column: str | None
    on_update: NonEmptyText
    on_delete: NonEmptyText
    match: NonEmptyText


class SQLiteTableFingerprint(_StrictContract):
    """Logical structure of one SQLite table."""

    name: NonEmptyText
    normalized_ddl: str | None
    columns: tuple[SQLiteColumnFingerprint, ...]
    foreign_keys: tuple[SQLiteForeignKeyFingerprint, ...]
    without_rowid: bool
    strict: bool


class SQLiteIndexColumnFingerprint(_StrictContract):
    """Exact ``PRAGMA index_xinfo`` evidence, including implicit expressions."""

    sequence: Annotated[int, Field(ge=0)]
    cid: int
    name: str | None
    descending: bool
    collation: str | None
    key: bool


class SQLiteIndexFingerprint(_StrictContract):
    """Logical structure of one explicit or implicit SQLite index."""

    table_name: NonEmptyText
    name: NonEmptyText
    unique: bool
    origin: NonEmptyText
    partial: bool
    normalized_ddl: str | None
    columns: tuple[SQLiteIndexColumnFingerprint, ...]


class SQLiteTriggerFingerprint(_StrictContract):
    """Exact trigger identity and normalized definition."""

    name: NonEmptyText
    table_name: NonEmptyText
    normalized_ddl: NonEmptyText


class SQLiteSchemaObjectFingerprint(_StrictContract):
    """Every row of ``main.sqlite_schema`` without physical root-page noise."""

    object_type: SQLiteObjectType
    name: NonEmptyText
    table_name: NonEmptyText
    normalized_ddl: str | None


class SQLiteStoreFingerprint(_StrictContract):
    """Deterministic logical fingerprint observed from one SQLite snapshot."""

    schema_version: ContractVersion = "1.0"
    encoding: NonEmptyText
    application_id: int
    user_version: int
    markers: tuple[SQLiteMarkerFingerprint, ...]
    schema_objects: tuple[SQLiteSchemaObjectFingerprint, ...]
    tables: tuple[SQLiteTableFingerprint, ...]
    indexes: tuple[SQLiteIndexFingerprint, ...]
    triggers: tuple[SQLiteTriggerFingerprint, ...]
    schema_sha256: Sha256Digest
    store_sha256: Sha256Digest


class SQLiteExpectedStoreIdentity(_StrictContract):
    """Trusted store label and exact logical digest, independent of observations."""

    schema_version: ContractVersion = "1.0"
    family: SQLiteStoreFamily
    layout_version: Literal[1]
    encoding: Literal["UTF-8"]
    application_id: Literal[0]
    user_version: Literal[1]
    markers: tuple[SQLiteMarkerFingerprint, ...]
    store_sha256: Sha256Digest


class SQLiteSnapshotIdentity(_StrictContract):
    """Whole-file identity evidence kept outside the logical store fingerprint."""

    schema_version: ContractVersion = "1.0"
    size_bytes: Annotated[int, Field(ge=0)]
    modified_time_ns: Annotated[int, Field(ge=0)]
    sha256: Sha256Digest
    device: Annotated[int, Field(ge=0)]
    inode: Annotated[int, Field(ge=0)]


class SQLiteSnapshotObservation(_StrictContract):
    """Immutable inspection evidence plus before/after source invariants."""

    schema_version: ContractVersion = "1.0"
    snapshot_path: Path
    source_before: SQLiteSnapshotIdentity
    source_after: SQLiteSnapshotIdentity
    directory_entries_before: tuple[str, ...]
    directory_entries_after: tuple[str, ...]
    fingerprint: SQLiteStoreFingerprint

    @model_validator(mode="after")
    def snapshot_remained_unchanged(self) -> Self:
        """A successful observation can never describe a changing source."""

        if self.source_before != self.source_after:
            raise ValueError("successful SQLite observation requires an unchanged source")
        if self.directory_entries_before != self.directory_entries_after:
            raise ValueError("successful SQLite observation requires unchanged directory entries")
        return self


class SQLitePreflightRequest(_StrictContract):
    """One explicitly labelled generated fixture and its expected registered family."""

    schema_version: ContractVersion = "1.0"
    source_kind: Literal["generated_synthetic_fixture"]
    fixture_id: UUID
    fixture_path: Path
    expected_family: SQLiteStoreFamily
    expected_layout_version: Literal[1]


class SQLitePreflightResult(_StrictContract):
    """Exact identity-match result; rejected evidence never authorizes row scans."""

    schema_version: ContractVersion = "1.0"
    source_kind: Literal["generated_synthetic_fixture"]
    fixture_id: UUID
    status: SQLitePreflightStatus
    expected_identity: SQLiteExpectedStoreIdentity
    matched_families: tuple[SQLiteStoreFamily, ...]
    observation: SQLiteSnapshotObservation
    source_unchanged: Literal[True] = True

    @model_validator(mode="after")
    def match_result_is_consistent(self) -> Self:
        """Prevent contradictory success, rejection, or ambiguity claims."""

        observed = self.observation.fingerprint
        expected = self.expected_identity
        observed_matches_expected = (
            observed.store_sha256 == expected.store_sha256
            and observed.encoding == expected.encoding
            and observed.application_id == expected.application_id
            and observed.user_version == expected.user_version
            and observed.markers == expected.markers
        )
        if self.status is SQLitePreflightStatus.MATCHED:
            if self.matched_families != (expected.family,) or not observed_matches_expected:
                raise ValueError("matched SQLite preflight requires the one expected family")
        elif self.status is SQLitePreflightStatus.MISMATCH:
            if self.matched_families or observed_matches_expected:
                raise ValueError("a fingerprint mismatch cannot contain a matching family")
        elif self.status is SQLitePreflightStatus.WRONG_FAMILY:
            if (
                len(self.matched_families) != 1
                or self.matched_families[0] is expected.family
                or observed_matches_expected
            ):
                raise ValueError("wrong_family requires one different registered family")
        elif (
            len(self.matched_families) < 2
            or len(self.matched_families) != len(set(self.matched_families))
            or (expected.family in self.matched_families) != observed_matches_expected
        ):
            raise ValueError(
                "ambiguous SQLite preflight requires multiple evidence-consistent families"
            )
        return self
