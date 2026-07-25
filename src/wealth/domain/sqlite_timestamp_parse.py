"""Pure typed interpretation of synthetic SQLite timestamp-byte evidence.

This module consumes only an already validated TASK-031 result. It performs no filesystem,
SQLite, adapter, runtime, migration, normalization, or repair work and preserves every source
byte and source-evidence object unchanged.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Annotated, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
    from_epoch_microseconds,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_KEY_COLUMNS,
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_TARGETS,
    MAX_SQLITE_TIMESTAMP_VALUE_BYTES,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampCellEvidence,
    SQLiteTimestampExtractionPlan,
    SQLiteTimestampExtractionResult,
    SQLiteTimestampExtractionTarget,
)

__all__ = [
    "SQLITE_TIMESTAMP_PARSE_PLANS",
    "SQLiteTimestampColumnParsePlan",
    "SQLiteTimestampOffsetPolicy",
    "SQLiteTimestampParseError",
    "SQLiteTimestampParseErrorCode",
    "SQLiteTimestampParseOutcome",
    "SQLiteTimestampParsePlan",
    "SQLiteTimestampParseResult",
    "SQLiteTimestampParseStatus",
    "SQLiteTimestampParseTableEvidence",
    "SQLiteTimestampParseTarget",
    "SQLiteTimestampParsedRowEvidence",
    "SQLiteTimestampRepresentation",
    "parse_synthetic_sqlite_timestamp_evidence",
]

ContractVersion = Literal["1.0"]
FractionalPrecision = Literal[0, 6]
Sha256Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SQLiteIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
]
_MAX_OFFSET_MICROSECONDS: Final[int] = 86_400_000_000
_MIN_SQLITE_INTEGER: Final[int] = -(2**63)
_MAX_SQLITE_INTEGER: Final[int] = 2**63 - 1
_LEGACY_TIMESTAMP_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?P<year>[0-9]{4})-"
    r"(?P<month>[0-9]{2})-"
    r"(?P<day>[0-9]{2})T"
    r"(?P<hour>[0-9]{2}):"
    r"(?P<minute>[0-9]{2}):"
    r"(?P<second>[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{6}))?"
    r"(?P<offset>"
    r"[+-][0-9]{2}:[0-9]{2}"
    r"(?::[0-9]{2}(?:\.[0-9]{6})?)?"
    r")?"
)
_CANONICAL_EPOCH_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:0|[1-9][0-9]*|-[1-9][0-9]*)")


class _StrictContract(BaseModel):
    """Apply one strict immutable boundary to every public parse object."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class SQLiteTimestampRepresentation(StrEnum):
    """The two direct scalar timestamp representations in current SQLite layouts."""

    LEGACY_ISO8601_TEXT = "legacy_iso8601_text"
    EPOCH_MICROSECONDS = "epoch_microseconds"


class SQLiteTimestampOffsetPolicy(StrEnum):
    """The exact offset language emitted by the owning timestamp writer."""

    ANY_AWARE_OFFSET = "any_aware_offset"
    FIXED_UTC_OFFSET = "fixed_utc_offset"


class SQLiteTimestampParseStatus(StrEnum):
    """One deterministic interpretation outcome for every source timestamp cell."""

    PARSED_AWARE_TEXT = "parsed_aware_text"
    PARSED_EPOCH_MICROSECONDS = "parsed_epoch_microseconds"
    DECLARED_ABSENT = "declared_absent"
    NAIVE_TEXT = "naive_text"
    OFFSET_POLICY_MISMATCH = "offset_policy_mismatch"
    MALFORMED_UTF8 = "malformed_utf8"
    MALFORMED_TEXT = "malformed_text"
    MALFORMED_EPOCH_BYTES = "malformed_epoch_bytes"
    EPOCH_OUT_OF_RANGE = "epoch_out_of_range"
    UNEXPECTED_STORAGE_CLASS = "unexpected_storage_class"


class SQLiteTimestampParseErrorCode(StrEnum):
    """Fail-closed errors for an invalid top-level TASK-031 evidence boundary."""

    INVALID_SOURCE_EVIDENCE = "invalid_source_evidence"
    UNREGISTERED_PLAN = "unregistered_plan"


class SQLiteTimestampParseError(ValueError):
    """Reject evidence that is not exactly one registered TASK-031 result."""

    def __init__(self, code: SQLiteTimestampParseErrorCode, detail: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteTimestampColumnParsePlan(_StrictContract):
    """Expected representation and nullability for one direct timestamp column."""

    column_name: SQLiteIdentifier
    representation: SQLiteTimestampRepresentation
    offset_policy: SQLiteTimestampOffsetPolicy | None
    nullable: bool

    @model_validator(mode="after")
    def offset_policy_matches_representation(self) -> Self:
        """Text declarations require one policy; integer declarations forbid one."""

        has_policy = self.offset_policy is not None
        expects_policy = self.representation is SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
        if has_policy is not expects_policy:
            raise ValueError("offset policy must be declared exactly for text timestamps")
        return self


class SQLiteTimestampParseTarget(_StrictContract):
    """Column parse declarations for one TASK-031 table target."""

    table_name: SQLiteIdentifier
    columns: Annotated[
        tuple[SQLiteTimestampColumnParsePlan, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET),
    ]

    @model_validator(mode="after")
    def columns_are_unique(self) -> Self:
        """SQLite identifiers are case-insensitive and may be declared only once."""

        names = tuple(column.column_name.casefold() for column in self.columns)
        if len(names) != len(set(names)):
            raise ValueError("timestamp parse columns must be unique")
        return self


class SQLiteTimestampParsePlan(_StrictContract):
    """Pinned pure parse plan tied to one exact TASK-031 extraction plan."""

    schema_version: ContractVersion = "1.0"
    extraction_plan: SQLiteTimestampExtractionPlan
    targets: Annotated[
        tuple[SQLiteTimestampParseTarget, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_TARGETS),
    ]

    @model_validator(mode="after")
    def targets_match_extraction_plan(self) -> Self:
        """Require one exact representation declaration for every extracted column."""

        try:
            revalidated_plan = SQLiteTimestampExtractionPlan.model_validate(
                self.extraction_plan.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("extraction plan must pass deep validation") from exc
        if revalidated_plan != self.extraction_plan:
            raise ValueError("extraction plan changed during deep validation")
        if len(self.targets) != len(self.extraction_plan.targets):
            raise ValueError("parse targets must exactly match the extraction plan")
        table_names = tuple(target.table_name.casefold() for target in self.targets)
        if len(table_names) != len(set(table_names)):
            raise ValueError("parse targets may declare each table only once")
        for parse_target, extraction_target in zip(
            self.targets,
            self.extraction_plan.targets,
            strict=True,
        ):
            if (
                parse_target.table_name != extraction_target.table_name
                or tuple(column.column_name for column in parse_target.columns)
                != extraction_target.timestamp_columns
            ):
                raise ValueError("parse targets must exactly match the extraction plan")
        return self


@dataclass(frozen=True, slots=True)
class _Interpretation:
    status: SQLiteTimestampParseStatus
    decoded_text: str | None = None
    parsed_datetime: datetime | None = None
    epoch_microseconds: int | None = None
    utc_offset_microseconds: int | None = None
    fractional_precision: FractionalPrecision | None = None


def _cell_bytes(cell: SQLiteTimestampCellEvidence) -> bytes:
    return bytes.fromhex(cell.blob_hex)


def _fraction_to_microseconds(
    fraction: str | None,
) -> tuple[int, FractionalPrecision]:
    if fraction is None:
        return 0, 0
    return int(fraction), 6


def _parse_offset_microseconds(offset: str) -> int | None:
    sign = 1 if offset[0] == "+" else -1
    components = offset[1:].split(":")
    hour = int(components[0])
    minute = int(components[1])
    second = 0
    microsecond = 0
    if len(components) == 3:
        second_text = components[2]
        if "." in second_text:
            whole_second, fraction = second_text.split(".", maxsplit=1)
            second = int(whole_second)
            microsecond = int(fraction)
        else:
            second = int(second_text)
    if hour > 23 or minute > 59 or second > 59:
        return None
    absolute = hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + microsecond
    if absolute >= _MAX_OFFSET_MICROSECONDS:
        return None
    return sign * absolute


def _interpret_legacy_text(
    cell: SQLiteTimestampCellEvidence,
    offset_policy: SQLiteTimestampOffsetPolicy,
) -> _Interpretation:
    try:
        decoded = _cell_bytes(cell).decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return _Interpretation(status=SQLiteTimestampParseStatus.MALFORMED_UTF8)

    match = _LEGACY_TIMESTAMP_PATTERN.fullmatch(decoded)
    if match is None:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
            decoded_text=decoded,
        )
    parts = match.groupdict()
    microsecond, precision = _fraction_to_microseconds(parts["fraction"])
    try:
        wall_time = datetime(
            int(parts["year"]),
            int(parts["month"]),
            int(parts["day"]),
            int(parts["hour"]),
            int(parts["minute"]),
            int(parts["second"]),
            microsecond,
        )
    except ValueError:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
            decoded_text=decoded,
        )

    offset = parts["offset"]
    if offset is None:
        if wall_time.isoformat() != decoded:
            return _Interpretation(
                status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
                decoded_text=decoded,
            )
        return _Interpretation(
            status=SQLiteTimestampParseStatus.NAIVE_TEXT,
            decoded_text=decoded,
            parsed_datetime=wall_time,
            fractional_precision=precision,
        )
    offset_microseconds = _parse_offset_microseconds(offset)
    if offset_microseconds is None:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
            decoded_text=decoded,
        )
    try:
        parsed = wall_time.replace(tzinfo=timezone(timedelta(microseconds=offset_microseconds)))
    except ValueError:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
            decoded_text=decoded,
        )
    if parsed.isoformat() != decoded:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_TEXT,
            decoded_text=decoded,
        )
    if offset_policy is SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET and offset_microseconds != 0:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
            decoded_text=decoded,
            parsed_datetime=parsed,
            utc_offset_microseconds=offset_microseconds,
            fractional_precision=precision,
        )
    return _Interpretation(
        status=SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
        decoded_text=decoded,
        parsed_datetime=parsed,
        utc_offset_microseconds=offset_microseconds,
        fractional_precision=precision,
    )


def _interpret_epoch(cell: SQLiteTimestampCellEvidence) -> _Interpretation:
    try:
        decoded = _cell_bytes(cell).decode("ascii", errors="strict")
    except UnicodeDecodeError:
        return _Interpretation(status=SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES)
    if _CANONICAL_EPOCH_PATTERN.fullmatch(decoded) is None:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
            decoded_text=decoded,
        )
    if len(decoded.removeprefix("-")) > 19:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
            decoded_text=decoded,
        )
    try:
        value = int(decoded)
    except ValueError:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
            decoded_text=decoded,
        )
    if decoded != str(value) or not _MIN_SQLITE_INTEGER <= value <= _MAX_SQLITE_INTEGER:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
            decoded_text=decoded,
        )
    if not MIN_EPOCH_MICROSECONDS <= value <= MAX_EPOCH_MICROSECONDS:
        return _Interpretation(
            status=SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE,
            decoded_text=decoded,
            epoch_microseconds=value,
        )
    return _Interpretation(
        status=SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS,
        decoded_text=decoded,
        parsed_datetime=from_epoch_microseconds(value),
        epoch_microseconds=value,
        utc_offset_microseconds=0,
    )


def _interpret_timestamp_cell(
    cell: SQLiteTimestampCellEvidence,
    representation: SQLiteTimestampRepresentation,
    offset_policy: SQLiteTimestampOffsetPolicy | None,
    nullable: bool,
) -> _Interpretation:
    if (representation is SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT) is not (
        offset_policy is not None
    ):
        raise ValueError("timestamp representation and offset policy disagree")
    if cell.storage_class is SQLiteStorageClass.NULL:
        if nullable:
            return _Interpretation(status=SQLiteTimestampParseStatus.DECLARED_ABSENT)
        return _Interpretation(status=SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS)
    if representation is SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT:
        if cell.storage_class is not SQLiteStorageClass.TEXT:
            return _Interpretation(status=SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS)
        if offset_policy is None:
            raise AssertionError("validated text plans always declare an offset policy")
        return _interpret_legacy_text(cell, offset_policy)
    if cell.storage_class is not SQLiteStorageClass.INTEGER:
        return _Interpretation(status=SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS)
    return _interpret_epoch(cell)


def _datetime_identity(value: datetime | None) -> object:
    if value is None:
        return None
    if type(value) is not datetime:
        return ("invalid_datetime_type", type(value))
    tz = value.tzinfo
    if tz is not None and type(tz) is not timezone:
        return ("invalid_timezone_type", type(tz))
    offset = None if tz is None else tz.utcoffset(value)
    zone_name = None if tz is None else tz.tzname(value)
    return (
        type(value),
        type(tz),
        value.year,
        value.month,
        value.day,
        value.hour,
        value.minute,
        value.second,
        value.microsecond,
        value.fold,
        offset,
        zone_name,
    )


class SQLiteTimestampParseOutcome(_StrictContract):
    """One self-validating typed interpretation that retains its exact source cell."""

    schema_version: ContractVersion = "1.0"
    source_cell: SQLiteTimestampCellEvidence
    representation: SQLiteTimestampRepresentation
    offset_policy: SQLiteTimestampOffsetPolicy | None
    nullable: bool
    status: SQLiteTimestampParseStatus
    decoded_text: (
        Annotated[
            str,
            Field(max_length=MAX_SQLITE_TIMESTAMP_VALUE_BYTES),
        ]
        | None
    ) = None
    parsed_datetime: datetime | None = None
    epoch_microseconds: int | None = None
    utc_offset_microseconds: (
        Annotated[
            int,
            Field(
                gt=-_MAX_OFFSET_MICROSECONDS,
                lt=_MAX_OFFSET_MICROSECONDS,
            ),
        ]
        | None
    ) = None
    fractional_precision: FractionalPrecision | None = None

    @model_validator(mode="after")
    def outcome_matches_source_bytes(self) -> Self:
        """Recompute interpretation so altered or contradictory evidence cannot validate."""

        try:
            revalidated_cell = SQLiteTimestampCellEvidence.model_validate(
                self.source_cell.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source timestamp cell must pass deep validation") from exc
        if revalidated_cell != self.source_cell:
            raise ValueError("source timestamp cell changed during deep validation")
        expected = _interpret_timestamp_cell(
            self.source_cell,
            self.representation,
            self.offset_policy,
            self.nullable,
        )
        actual_fields = (
            self.status,
            self.decoded_text,
            _datetime_identity(self.parsed_datetime),
            self.epoch_microseconds,
            self.utc_offset_microseconds,
            self.fractional_precision,
        )
        expected_fields = (
            expected.status,
            expected.decoded_text,
            _datetime_identity(expected.parsed_datetime),
            expected.epoch_microseconds,
            expected.utc_offset_microseconds,
            expected.fractional_precision,
        )
        if actual_fields != expected_fields:
            raise ValueError("timestamp parse outcome does not match its exact source bytes")
        return self


class SQLiteTimestampParsedRowEvidence(_StrictContract):
    """One source row key plus one ordered outcome per timestamp cell."""

    row_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]
    stable_row_key: Annotated[
        tuple[SQLiteTimestampCellEvidence, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_KEY_COLUMNS),
    ]
    outcomes: Annotated[
        tuple[SQLiteTimestampParseOutcome, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET),
    ]

    @model_validator(mode="after")
    def row_shape_is_unambiguous(self) -> Self:
        """Require unique, non-null key cells and unique timestamp outcomes."""

        for cell in self.stable_row_key:
            try:
                revalidated_cell = SQLiteTimestampCellEvidence.model_validate(
                    cell.model_dump(mode="python"),
                    strict=True,
                )
            except (TypeError, ValueError, ValidationError) as exc:
                raise ValueError("parse row-key cells must pass deep validation") from exc
            if revalidated_cell != cell:
                raise ValueError("parse row-key cells changed during deep validation")
        key_names = tuple(cell.column_name.casefold() for cell in self.stable_row_key)
        outcome_names = tuple(
            outcome.source_cell.column_name.casefold() for outcome in self.outcomes
        )
        if len(key_names) != len(set(key_names)) or any(
            cell.storage_class is SQLiteStorageClass.NULL for cell in self.stable_row_key
        ):
            raise ValueError("parse row keys must be unique and non-null")
        if len(outcome_names) != len(set(outcome_names)):
            raise ValueError("parse row outcomes must have unique source columns")
        if set(key_names) & set(outcome_names):
            raise ValueError("parse row keys and timestamp outcomes may not overlap")
        return self


class SQLiteTimestampParseTableEvidence(_StrictContract):
    """Pure parse evidence for one table in the unchanged source ordering."""

    target_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_TARGETS)]
    table_name: SQLiteIdentifier
    rows: Annotated[
        tuple[SQLiteTimestampParsedRowEvidence, ...],
        Field(max_length=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET),
    ]

    @model_validator(mode="after")
    def rows_are_contiguous_and_unique(self) -> Self:
        """Require contiguous ordinals and unique stable keys."""

        if tuple(row.row_ordinal for row in self.rows) != tuple(range(len(self.rows))):
            raise ValueError("parse row ordinals must be contiguous")
        stable_keys = tuple(row.stable_row_key for row in self.rows)
        if len(stable_keys) != len(set(stable_keys)):
            raise ValueError("parse row stable keys must be unique")
        return self


class SQLiteTimestampParseResult(_StrictContract):
    """One-to-one pure interpretation linked to the complete TASK-031 result."""

    schema_version: ContractVersion = "1.0"
    source: SQLiteTimestampExtractionResult
    plan: SQLiteTimestampParsePlan
    tables: Annotated[
        tuple[SQLiteTimestampParseTableEvidence, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_TARGETS),
    ]

    @model_validator(mode="after")
    def evidence_reconciles_one_to_one(self) -> Self:
        """Reject any missing, reordered, replaced, or duplicated source evidence."""

        try:
            revalidated_source = SQLiteTimestampExtractionResult.model_validate(
                self.source.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-031 evidence must pass deep validation") from exc
        if revalidated_source != self.source:
            raise ValueError("source TASK-031 evidence changed during deep validation")
        if self.plan != _pinned_plan_for_source(self.source):
            raise ValueError("parse plan must equal the reviewed immutable declaration")
        if self.plan.extraction_plan != self.source.plan:
            raise ValueError("parse plan must preserve the exact TASK-031 extraction plan")
        if len(self.tables) != len(self.source.tables):
            raise ValueError("parse tables must reconcile one-to-one with TASK-031")
        for parse_target, parse_table, source_table in zip(
            self.plan.targets,
            self.tables,
            self.source.tables,
            strict=True,
        ):
            if (
                parse_table.target_ordinal != source_table.target_ordinal
                or parse_table.table_name != source_table.target.table_name
                or parse_target.table_name != source_table.target.table_name
                or len(parse_table.rows) != len(source_table.rows)
            ):
                raise ValueError("parse tables must preserve TASK-031 ordering and identity")
            for parse_row, source_row in zip(
                parse_table.rows,
                source_table.rows,
                strict=True,
            ):
                if (
                    parse_row.row_ordinal != source_row.row_ordinal
                    or parse_row.stable_row_key != source_row.stable_row_key
                    or len(parse_row.outcomes) != len(source_row.timestamp_cells)
                ):
                    raise ValueError("parse rows must preserve TASK-031 row evidence")
                for column_plan, outcome, source_cell in zip(
                    parse_target.columns,
                    parse_row.outcomes,
                    source_row.timestamp_cells,
                    strict=True,
                ):
                    if (
                        outcome.source_cell != source_cell
                        or outcome.source_cell.column_name != column_plan.column_name
                        or outcome.representation is not column_plan.representation
                        or outcome.offset_policy is not column_plan.offset_policy
                        or outcome.nullable is not column_plan.nullable
                    ):
                        raise ValueError(
                            "parse outcomes must preserve every TASK-031 timestamp cell"
                        )
        return self


ColumnSpec = tuple[
    str,
    SQLiteTimestampRepresentation,
    SQLiteTimestampOffsetPolicy | None,
    bool,
]
TargetSpec = tuple[str, tuple[str, ...], tuple[ColumnSpec, ...]]
_TEXT: Final[SQLiteTimestampRepresentation] = SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
_EPOCH: Final[SQLiteTimestampRepresentation] = SQLiteTimestampRepresentation.EPOCH_MICROSECONDS
_ANY: Final[SQLiteTimestampOffsetPolicy] = SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET
_UTC: Final[SQLiteTimestampOffsetPolicy] = SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET


def _aware(column_name: str, nullable: bool = False) -> ColumnSpec:
    return column_name, _TEXT, _ANY, nullable


def _utc(column_name: str, nullable: bool = False) -> ColumnSpec:
    return column_name, _TEXT, _UTC, nullable


def _epoch(column_name: str) -> ColumnSpec:
    return column_name, _EPOCH, None, False


_STORE_SHA256_BY_FAMILY: Final[dict[SQLiteStoreFamily, str]] = {
    SQLiteStoreFamily.MARKET: ("c9ebf4efe2c754f2cad9924aef26b400886ccbbc2f8e0b95bc5629445a738d13"),
    SQLiteStoreFamily.ORDER_FLOW: (
        "250e7ab52ed7f26406c6dee0359a71fd913885ed7a1b594b33bb9868378cc706"
    ),
    SQLiteStoreFamily.HISTORICAL_COLLECTION: (
        "5b8f1b2e8ab532725f8a516acc7dd779e74a2fa0abeab7dc5265412738e4e86a"
    ),
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: (
        "7baf5c988445caa212cc9eae0e58d5439452aa02a7a9f4010a5a6f497f0a9441"
    ),
    SQLiteStoreFamily.COLLECTOR_SERVICE: (
        "8894378abc8b649a2efd46a7f41d380a018866f0e04a17e5c83041ae47afc09c"
    ),
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: (
        "3d3cb73e1a5a7086f91748c4dcb92fc33aee5c3f59b5e62e9df966df00d2a9c1"
    ),
    SQLiteStoreFamily.RATE_BUDGET: (
        "89172f205882739f00629f61619151d17acf31a8f3f09f5c1f8bf9ea13526b10"
    ),
    SQLiteStoreFamily.RECONCILIATION: (
        "a8cee5c79a8b7a4420195ff825268022095f24fb1f52ecfbebc71ab13a31d636"
    ),
}
_PARSE_TARGET_SPECS: Final[dict[SQLiteStoreFamily, tuple[TargetSpec, ...]]] = {
    SQLiteStoreFamily.MARKET: (
        (
            "candle_conflicts",
            ("existing_record_id", "incoming_record_id"),
            (_aware("open_time"), _aware("detected_at")),
        ),
        (
            "canonical_candles",
            ("record_id",),
            (_aware("open_time"),),
        ),
        (
            "raw_market_payloads",
            ("record_id",),
            (_aware("observed_at"), _aware("processed_at")),
        ),
    ),
    SQLiteStoreFamily.ORDER_FLOW: (
        (
            "canonical_order_flow_records",
            ("record_id",),
            (_aware("event_time"),),
        ),
        (
            "order_flow_conflicts",
            ("existing_record_id", "incoming_record_id"),
            (_aware("event_time"), _aware("detected_at")),
        ),
        (
            "raw_order_flow_payloads",
            ("record_id",),
            (_aware("observed_at"), _aware("processed_at")),
        ),
    ),
    SQLiteStoreFamily.HISTORICAL_COLLECTION: (
        (
            "collection_jobs",
            ("job_id",),
            (_aware("next_window_start"),),
        ),
        (
            "collection_transitions",
            ("job_id", "version"),
            (_aware("recorded_at"),),
        ),
        (
            "source_health_observations",
            ("observation_id",),
            (_aware("observed_at"),),
        ),
    ),
    SQLiteStoreFamily.CONTINUOUS_COLLECTION: (
        (
            "continuous_collection_checkpoints",
            ("collection_id",),
            (
                _aware("next_window_start"),
                _aware("active_window_end_exclusive", nullable=True),
                _aware("next_retry_at", nullable=True),
            ),
        ),
        (
            "continuous_collection_transitions",
            ("collection_id", "version"),
            (_aware("recorded_at"),),
        ),
    ),
    SQLiteStoreFamily.COLLECTOR_SERVICE: (
        (
            "collector_service_heartbeats",
            ("run_id", "sequence"),
            (_aware("observed_at"),),
        ),
        (
            "collector_service_runs",
            ("run_id",),
            (_aware("observed_at"),),
        ),
    ),
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION: (
        (
            "public_trade_collection_jobs",
            ("job_id",),
            (
                _utc("window_start"),
                _utc("window_end_exclusive"),
                _utc("next_window_start"),
                _utc("pending_window_end_exclusive", nullable=True),
                _utc("created_at"),
                _utc("updated_at"),
                _utc("lease_expires_at", nullable=True),
            ),
        ),
        (
            "public_trade_collection_leases",
            ("job_id", "lease_token"),
            (_utc("acquired_at"),),
        ),
        (
            "public_trade_collection_transitions",
            ("job_id", "version"),
            (_utc("recorded_at"),),
        ),
        (
            "public_trade_source_health",
            ("job_id", "checkpoint_version"),
            (
                _utc("range_start"),
                _utc("range_end_exclusive"),
                _utc("next_window_start"),
                _utc("pending_window_end_exclusive", nullable=True),
                _utc("observed_at"),
            ),
        ),
    ),
    SQLiteStoreFamily.RATE_BUDGET: (
        (
            "rate_budget_reservations",
            ("reservation_id",),
            (_aware("requested_at"),),
        ),
        (
            "rate_budget_state",
            ("budget_key",),
            (
                _epoch("theoretical_arrival_us"),
                _epoch("last_observed_us"),
            ),
        ),
    ),
    SQLiteStoreFamily.RECONCILIATION: (
        (
            "reconciliation_observations",
            ("observation_id",),
            (_utc("recorded_at"),),
        ),
    ),
}


def _extraction_target(spec: TargetSpec) -> SQLiteTimestampExtractionTarget:
    table_name, stable_key, columns = spec
    return SQLiteTimestampExtractionTarget(
        table_name=table_name,
        stable_row_key_columns=stable_key,
        timestamp_columns=tuple(column_name for column_name, _, _, _ in columns),
    )


def _parse_target(spec: TargetSpec) -> SQLiteTimestampParseTarget:
    table_name, _, columns = spec
    return SQLiteTimestampParseTarget(
        table_name=table_name,
        columns=tuple(
            SQLiteTimestampColumnParsePlan(
                column_name=column_name,
                representation=representation,
                offset_policy=offset_policy,
                nullable=nullable,
            )
            for column_name, representation, offset_policy, nullable in columns
        ),
    )


_PINNED_PARSE_PLANS: Final[tuple[SQLiteTimestampParsePlan, ...]] = tuple(
    SQLiteTimestampParsePlan(
        extraction_plan=SQLiteTimestampExtractionPlan(
            family=family,
            layout_version=1,
            expected_store_sha256=_STORE_SHA256_BY_FAMILY[family],
            targets=tuple(_extraction_target(spec) for spec in _PARSE_TARGET_SPECS[family]),
        ),
        targets=tuple(_parse_target(spec) for spec in _PARSE_TARGET_SPECS[family]),
    )
    for family in SQLiteStoreFamily
)
SQLITE_TIMESTAMP_PARSE_PLANS: tuple[SQLiteTimestampParsePlan, ...] = _PINNED_PARSE_PLANS


def _pinned_plan_for_source(
    source: SQLiteTimestampExtractionResult,
) -> SQLiteTimestampParsePlan:
    matches = tuple(
        plan
        for plan in _PINNED_PARSE_PLANS
        if plan.extraction_plan.family is source.plan.family
        and plan.extraction_plan.layout_version == source.plan.layout_version
    )
    if len(matches) != 1:
        raise SQLiteTimestampParseError(
            SQLiteTimestampParseErrorCode.UNREGISTERED_PLAN,
            "the source family must have exactly one timestamp parse plan",
        )
    plan = matches[0]
    if plan.extraction_plan != source.plan:
        raise SQLiteTimestampParseError(
            SQLiteTimestampParseErrorCode.INVALID_SOURCE_EVIDENCE,
            "the TASK-031 extraction plan differs from the reviewed parse declaration",
        )
    return plan


def _validated_source(
    source: SQLiteTimestampExtractionResult,
) -> SQLiteTimestampExtractionResult:
    try:
        revalidated = SQLiteTimestampExtractionResult.model_validate(
            source.model_dump(mode="python"),
            strict=True,
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise SQLiteTimestampParseError(
            SQLiteTimestampParseErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must pass deep TASK-031 contract validation",
        ) from exc
    if revalidated != source:
        raise SQLiteTimestampParseError(
            SQLiteTimestampParseErrorCode.INVALID_SOURCE_EVIDENCE,
            "source changed during deep TASK-031 contract validation",
        )
    return source


def _parse_outcome(
    cell: SQLiteTimestampCellEvidence,
    column_plan: SQLiteTimestampColumnParsePlan,
) -> SQLiteTimestampParseOutcome:
    interpreted = _interpret_timestamp_cell(
        cell,
        column_plan.representation,
        column_plan.offset_policy,
        column_plan.nullable,
    )
    return SQLiteTimestampParseOutcome(
        source_cell=cell,
        representation=column_plan.representation,
        offset_policy=column_plan.offset_policy,
        nullable=column_plan.nullable,
        status=interpreted.status,
        decoded_text=interpreted.decoded_text,
        parsed_datetime=interpreted.parsed_datetime,
        epoch_microseconds=interpreted.epoch_microseconds,
        utc_offset_microseconds=interpreted.utc_offset_microseconds,
        fractional_precision=interpreted.fractional_precision,
    )


def parse_synthetic_sqlite_timestamp_evidence(
    source: SQLiteTimestampExtractionResult,
) -> SQLiteTimestampParseResult:
    """Interpret every TASK-031 timestamp cell without any I/O or normalization."""

    if type(source) is not SQLiteTimestampExtractionResult:
        raise SQLiteTimestampParseError(
            SQLiteTimestampParseErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must be one exact SQLiteTimestampExtractionResult",
        )
    source = _validated_source(source)
    plan = _pinned_plan_for_source(source)
    tables = tuple(
        SQLiteTimestampParseTableEvidence(
            target_ordinal=source_table.target_ordinal,
            table_name=source_table.target.table_name,
            rows=tuple(
                SQLiteTimestampParsedRowEvidence(
                    row_ordinal=source_row.row_ordinal,
                    stable_row_key=source_row.stable_row_key,
                    outcomes=tuple(
                        _parse_outcome(cell, column_plan)
                        for cell, column_plan in zip(
                            source_row.timestamp_cells,
                            parse_target.columns,
                            strict=True,
                        )
                    ),
                )
                for source_row in source_table.rows
            ),
        )
        for parse_target, source_table in zip(
            plan.targets,
            source.tables,
            strict=True,
        )
    )
    return SQLiteTimestampParseResult(
        source=source,
        plan=plan,
        tables=tables,
    )
