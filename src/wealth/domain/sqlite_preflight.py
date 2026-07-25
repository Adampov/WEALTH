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
    "MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET",
    "MAX_SQLITE_TIMESTAMP_KEY_COLUMNS",
    "MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET",
    "MAX_SQLITE_TIMESTAMP_TARGETS",
    "MAX_SQLITE_TIMESTAMP_VALUE_BYTES",
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
    "SQLiteTimestampCellEvidence",
    "SQLiteTimestampExtractionPlan",
    "SQLiteTimestampExtractionResult",
    "SQLiteTimestampExtractionTarget",
    "SQLiteTimestampRowEvidence",
    "SQLiteTimestampTableEvidence",
    "SQLiteTriggerFingerprint",
]

MAX_SQLITE_MARKER_COLUMNS: Final[int] = 16
MAX_SQLITE_MARKER_ROWS: Final[int] = 64
MAX_SQLITE_MARKER_VALUE_BYTES: Final[int] = 4096
MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET: Final[int] = 16
MAX_SQLITE_TIMESTAMP_KEY_COLUMNS: Final[int] = 8
MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET: Final[int] = 64
MAX_SQLITE_TIMESTAMP_TARGETS: Final[int] = 32
MAX_SQLITE_TIMESTAMP_VALUE_BYTES: Final[int] = 4096
ContractVersion = Literal["1.0"]
Sha256Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
NonEmptyText = Annotated[str, Field(min_length=1)]
SQLiteIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
]


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


class SQLiteTimestampExtractionTarget(_StrictContract):
    """One trusted table target with a schema-enforced stable row key."""

    table_name: SQLiteIdentifier
    stable_row_key_columns: tuple[SQLiteIdentifier, ...]
    timestamp_columns: tuple[SQLiteIdentifier, ...]

    @model_validator(mode="after")
    def target_is_unambiguous(self) -> Self:
        """Reject aliases, overlaps, and duplicate identifiers."""

        forbidden_row_keys = {"rowid", "_rowid_", "oid"}
        folded_row_keys = tuple(column.casefold() for column in self.stable_row_key_columns)
        folded_timestamps = tuple(column.casefold() for column in self.timestamp_columns)
        if (
            not self.stable_row_key_columns
            or len(self.stable_row_key_columns) > MAX_SQLITE_TIMESTAMP_KEY_COLUMNS
            or len(folded_row_keys) != len(set(folded_row_keys))
            or any(column in forbidden_row_keys for column in folded_row_keys)
        ):
            raise ValueError("stable row-key columns must be non-empty, unique, and declared")
        if (
            not self.timestamp_columns
            or len(self.timestamp_columns) > MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET
            or len(folded_timestamps) != len(set(folded_timestamps))
        ):
            raise ValueError("timestamp columns must be non-empty and unique")
        if set(folded_row_keys) & set(folded_timestamps):
            raise ValueError("timestamp columns may not be used as stable row-key columns")
        return self


class SQLiteTimestampExtractionPlan(_StrictContract):
    """Pinned, caller-independent extraction plan for one TASK-030 layout."""

    schema_version: ContractVersion = "1.0"
    family: SQLiteStoreFamily
    layout_version: Literal[1]
    expected_store_sha256: Sha256Digest
    max_rows_per_target: Literal[64] = 64
    targets: tuple[SQLiteTimestampExtractionTarget, ...]

    @model_validator(mode="after")
    def plan_is_complete_and_deterministic(self) -> Self:
        """Require one bounded, canonically ordered declaration per table."""

        if not self.targets:
            raise ValueError("timestamp extraction plan must contain at least one target")
        if len(self.targets) > MAX_SQLITE_TIMESTAMP_TARGETS:
            raise ValueError("timestamp extraction plan exceeds the bounded target count")
        table_names = tuple(target.table_name for target in self.targets)
        folded_table_names = tuple(table_name.casefold() for table_name in table_names)
        if len(folded_table_names) != len(set(folded_table_names)):
            raise ValueError("timestamp extraction plan may target each table only once")
        if folded_table_names != tuple(sorted(folded_table_names)):
            raise ValueError("timestamp extraction targets must use canonical table order")
        return self


class SQLiteTimestampCellEvidence(_StrictContract):
    """One SQLite cell preserved as storage class plus exact cast-byte evidence."""

    column_name: SQLiteIdentifier
    storage_class: SQLiteStorageClass
    blob_hex: Annotated[
        str,
        Field(
            max_length=MAX_SQLITE_TIMESTAMP_VALUE_BYTES * 2,
            pattern=r"^(?:[0-9A-F]{2})*$",
        ),
    ]
    byte_length: Annotated[int, Field(ge=0, le=MAX_SQLITE_TIMESTAMP_VALUE_BYTES)]

    @model_validator(mode="after")
    def byte_length_matches_hex_evidence(self) -> Self:
        """Reject evidence whose declared length or NULL representation is inconsistent."""

        if len(self.blob_hex) != self.byte_length * 2:
            raise ValueError("timestamp byte_length must equal the exact blob_hex length")
        if self.storage_class is SQLiteStorageClass.NULL and (self.blob_hex or self.byte_length):
            raise ValueError("a SQLite NULL cell must have empty byte evidence")
        return self


class SQLiteTimestampRowEvidence(_StrictContract):
    """One deterministically ordered row without materializing raw Python values."""

    row_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]
    stable_row_key: tuple[SQLiteTimestampCellEvidence, ...]
    timestamp_cells: tuple[SQLiteTimestampCellEvidence, ...]

    @model_validator(mode="after")
    def row_shapes_are_unambiguous(self) -> Self:
        """Require non-empty, duplicate-free key and timestamp evidence."""

        key_names = tuple(cell.column_name for cell in self.stable_row_key)
        timestamp_names = tuple(cell.column_name for cell in self.timestamp_cells)
        folded_key_names = tuple(name.casefold() for name in key_names)
        folded_timestamp_names = tuple(name.casefold() for name in timestamp_names)
        if (
            not key_names
            or len(key_names) > MAX_SQLITE_TIMESTAMP_KEY_COLUMNS
            or len(folded_key_names) != len(set(folded_key_names))
        ):
            raise ValueError("row-key evidence must be non-empty and unique")
        if any(cell.storage_class is SQLiteStorageClass.NULL for cell in self.stable_row_key):
            raise ValueError("stable row-key evidence may not contain SQLite NULL")
        if (
            not timestamp_names
            or len(timestamp_names) > MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET
            or len(folded_timestamp_names) != len(set(folded_timestamp_names))
        ):
            raise ValueError("timestamp evidence must be non-empty and unique")
        if set(folded_key_names) & set(folded_timestamp_names):
            raise ValueError("row-key and timestamp evidence may not overlap")
        return self


def _timestamp_row_sort_key(
    row: SQLiteTimestampRowEvidence,
) -> tuple[tuple[str, str, int], ...]:
    return tuple(
        (cell.storage_class.value, cell.blob_hex, cell.byte_length) for cell in row.stable_row_key
    )


class SQLiteTimestampTableEvidence(_StrictContract):
    """Complete bounded evidence for one declared table target."""

    target_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_TARGETS)]
    target: SQLiteTimestampExtractionTarget
    rows: tuple[SQLiteTimestampRowEvidence, ...]

    @model_validator(mode="after")
    def rows_match_target_and_order(self) -> Self:
        """Reject missing columns, duplicate keys, truncation, or unstable ordering."""

        if len(self.rows) > MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET:
            raise ValueError("timestamp evidence exceeds the bounded row count")
        if tuple(row.row_ordinal for row in self.rows) != tuple(range(len(self.rows))):
            raise ValueError("timestamp row ordinals must be contiguous")
        expected_keys = self.target.stable_row_key_columns
        expected_timestamps = self.target.timestamp_columns
        for row in self.rows:
            if tuple(cell.column_name for cell in row.stable_row_key) != expected_keys:
                raise ValueError("row-key evidence does not match its declared target")
            if tuple(cell.column_name for cell in row.timestamp_cells) != expected_timestamps:
                raise ValueError("timestamp evidence does not match its declared target")
        sort_keys = tuple(_timestamp_row_sort_key(row) for row in self.rows)
        if len(sort_keys) != len(set(sort_keys)):
            raise ValueError("stable row-key evidence must be unique")
        if sort_keys != tuple(sorted(sort_keys)):
            raise ValueError("timestamp rows must use deterministic stable-key order")
        return self


class SQLiteTimestampExtractionResult(_StrictContract):
    """Raw timestamp evidence linked to the exact unchanged TASK-030 snapshot."""

    schema_version: ContractVersion = "1.0"
    source_kind: Literal["generated_synthetic_fixture"]
    fixture_id: UUID
    plan: SQLiteTimestampExtractionPlan
    preflight: SQLitePreflightResult
    snapshot_identity: SQLiteSnapshotIdentity
    tables: tuple[SQLiteTimestampTableEvidence, ...]
    source_unchanged: Literal[True] = True

    @model_validator(mode="after")
    def result_is_bound_to_matched_snapshot(self) -> Self:
        """Require exact plan, fingerprint, fixture, and whole-file identity linkage."""

        preflight = self.preflight
        if (
            preflight.status is not SQLitePreflightStatus.MATCHED
            or preflight.source_kind != self.source_kind
            or preflight.fixture_id != self.fixture_id
            or preflight.matched_families != (self.plan.family,)
            or preflight.expected_identity.family is not self.plan.family
            or preflight.expected_identity.layout_version != self.plan.layout_version
            or preflight.expected_identity.store_sha256 != self.plan.expected_store_sha256
            or preflight.observation.fingerprint.store_sha256 != self.plan.expected_store_sha256
        ):
            raise ValueError("timestamp evidence requires one exact matched TASK-030 identity")
        observation = preflight.observation
        if (
            observation.source_before != self.snapshot_identity
            or observation.source_after != self.snapshot_identity
        ):
            raise ValueError("timestamp evidence must link to the unchanged snapshot identity")
        if tuple(table.target_ordinal for table in self.tables) != tuple(range(len(self.tables))):
            raise ValueError("timestamp table ordinals must be contiguous")
        if tuple(table.target for table in self.tables) != self.plan.targets:
            raise ValueError("timestamp table evidence must exactly match the pinned plan")
        if any(len(table.rows) > self.plan.max_rows_per_target for table in self.tables):
            raise ValueError("timestamp table evidence exceeds the pinned row bound")
        return self
