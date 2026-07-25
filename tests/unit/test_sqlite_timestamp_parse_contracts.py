"""Strict pure-contract coverage for TASK-032 timestamp parse evidence."""

import ast
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from wealth.adapters.sqlite_preflight import SQLITE_TIMESTAMP_EXTRACTION_PLANS
from wealth.domain import sqlite_timestamp_parse as timestamp_parse
from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
)
from wealth.domain.sqlite_preflight import (
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampCellEvidence,
)
from wealth.domain.sqlite_timestamp_parse import (
    SQLITE_TIMESTAMP_PARSE_PLANS,
    SQLiteTimestampColumnParsePlan,
    SQLiteTimestampOffsetPolicy,
    SQLiteTimestampParsedRowEvidence,
    SQLiteTimestampParseOutcome,
    SQLiteTimestampParsePlan,
    SQLiteTimestampParseStatus,
    SQLiteTimestampRepresentation,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PARSE_MODULE_PATH = REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_parse.py"

EXPECTED_NULLABLE_COLUMNS = {
    ("continuous_collection_checkpoints", "active_window_end_exclusive"),
    ("continuous_collection_checkpoints", "next_retry_at"),
    ("public_trade_collection_jobs", "pending_window_end_exclusive"),
    ("public_trade_collection_jobs", "lease_expires_at"),
    ("public_trade_source_health", "pending_window_end_exclusive"),
}
EXPECTED_FIXED_UTC_TABLES = {
    "public_trade_collection_jobs",
    "public_trade_collection_leases",
    "public_trade_collection_transitions",
    "public_trade_source_health",
    "reconciliation_observations",
}


def _cell(
    raw: bytes,
    *,
    column_name: str = "observed_at",
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
) -> SQLiteTimestampCellEvidence:
    return SQLiteTimestampCellEvidence(
        column_name=column_name,
        storage_class=storage_class,
        blob_hex=raw.hex().upper(),
        byte_length=len(raw),
    )


def _column_plan(
    *,
    representation: SQLiteTimestampRepresentation = (
        SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
    ),
    offset_policy: SQLiteTimestampOffsetPolicy | None = (
        SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET
    ),
    nullable: bool = False,
) -> SQLiteTimestampColumnParsePlan:
    return SQLiteTimestampColumnParsePlan(
        column_name="observed_at",
        representation=representation,
        offset_policy=offset_policy,
        nullable=nullable,
    )


def _outcome(
    raw: bytes,
    *,
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
    representation: SQLiteTimestampRepresentation = (
        SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
    ),
    offset_policy: SQLiteTimestampOffsetPolicy | None = (
        SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET
    ),
    nullable: bool = False,
) -> SQLiteTimestampParseOutcome:
    return timestamp_parse._parse_outcome(
        _cell(raw, storage_class=storage_class),
        _column_plan(
            representation=representation,
            offset_policy=offset_policy,
            nullable=nullable,
        ),
    )


def test_parse_registry_is_exact_complete_and_matches_task_031() -> None:
    assert tuple(plan.extraction_plan for plan in SQLITE_TIMESTAMP_PARSE_PLANS) == (
        SQLITE_TIMESTAMP_EXTRACTION_PLANS
    )
    assert tuple(plan.extraction_plan.family for plan in SQLITE_TIMESTAMP_PARSE_PLANS) == tuple(
        SQLiteStoreFamily
    )
    assert len(SQLITE_TIMESTAMP_PARSE_PLANS) == 8
    assert sum(len(plan.targets) for plan in SQLITE_TIMESTAMP_PARSE_PLANS) == 20

    columns = tuple(
        (target.table_name, column)
        for plan in SQLITE_TIMESTAMP_PARSE_PLANS
        for target in plan.targets
        for column in target.columns
    )
    assert len(columns) == 37
    assert (
        sum(
            column.representation is SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT
            for _, column in columns
        )
        == 35
    )
    assert (
        sum(
            column.representation is SQLiteTimestampRepresentation.EPOCH_MICROSECONDS
            for _, column in columns
        )
        == 2
    )
    assert (
        sum(
            column.offset_policy is SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET
            for _, column in columns
        )
        == 20
    )
    assert (
        sum(
            column.offset_policy is SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET
            for _, column in columns
        )
        == 15
    )
    assert {
        (table_name, column.column_name) for table_name, column in columns if column.nullable
    } == EXPECTED_NULLABLE_COLUMNS
    assert {
        table_name
        for table_name, column in columns
        if column.offset_policy is SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET
    } == EXPECTED_FIXED_UTC_TABLES


def test_column_plan_is_strict_frozen_and_representation_complete() -> None:
    valid = _column_plan()
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampColumnParsePlan.model_validate({**valid.model_dump(), "normalizer": "utc"})
    with pytest.raises(ValidationError, match="offset policy"):
        SQLiteTimestampColumnParsePlan(
            column_name="observed_at",
            representation=SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT,
            offset_policy=None,
            nullable=False,
        )
    with pytest.raises(ValidationError, match="offset policy"):
        SQLiteTimestampColumnParsePlan(
            column_name="observed_at",
            representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
            offset_policy=SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET,
            nullable=False,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampColumnParsePlan.model_validate(
            {
                **valid.model_dump(),
                "nullable": 0,
            }
        )
    with pytest.raises(ValidationError):
        valid.nullable = True


def test_parse_plan_deeply_revalidates_embedded_task_031_plan() -> None:
    plan = SQLITE_TIMESTAMP_PARSE_PLANS[0]
    forged_extraction = plan.extraction_plan.model_copy(update={"expected_store_sha256": "x"})

    with pytest.raises(ValidationError, match="deep validation"):
        SQLiteTimestampParsePlan(
            extraction_plan=forged_extraction,
            targets=plan.targets,
        )


@pytest.mark.parametrize(
    "value",
    (
        "0001-01-01T00:00:00+00:00",
        "9999-12-31T23:59:59.999999+23:59:59.999999",
        "2024-02-29T12:34:56-03:30",
        "2026-01-02T03:04:05.000001+00:00",
        "2026-01-02T03:04:05+05:45",
        "2026-01-02T03:04:05+00:00:01",
        "2026-01-02T03:04:05+00:00:00.000001",
        "2026-01-02T03:04:05-00:00:00.000001",
    ),
)
def test_exact_python_isoformat_aware_writer_language_is_parsed_without_normalizing(
    value: str,
) -> None:
    outcome = _outcome(value.encode())

    assert outcome.status is SQLiteTimestampParseStatus.PARSED_AWARE_TEXT
    assert outcome.decoded_text == value
    assert outcome.source_cell.blob_hex == value.encode().hex().upper()
    assert outcome.parsed_datetime is not None
    assert outcome.parsed_datetime.isoformat() == value
    assert outcome.parsed_datetime.utcoffset() is not None


@pytest.mark.parametrize(
    "value",
    (
        "2026-01-02T03:04:05",
        "2026-01-02T03:04:05.000001",
    ),
)
def test_exact_naive_writer_forms_are_distinct(value: str) -> None:
    outcome = _outcome(value.encode())

    assert outcome.status is SQLiteTimestampParseStatus.NAIVE_TEXT
    assert outcome.decoded_text == value
    assert outcome.parsed_datetime is not None
    assert outcome.parsed_datetime.tzinfo is None
    assert outcome.parsed_datetime.isoformat() == value


@pytest.mark.parametrize(
    "value",
    (
        "2026-01-02T03:04:05Z",
        "2026-01-02t03:04:05+00:00",
        "2026-01-02 03:04:05+00:00",
        "20260102T030405+00:00",
        "2026-W01-5T03:04:05+00:00",
        "2026-01-02T03:04:05,000001+00:00",
        "2026-01-02T03:04:05.1+00:00",
        "2026-01-02T03:04:05.000+00:00",
        "2026-01-02T03:04:05.000000+00:00",
        "2026-01-02T03:04:05+00:00:00",
        "2026-01-02T03:04:05+00:00:00.000000",
        "2026-01-02T03:04:05-00:00",
        "2026-01-02T03:04:05+24:00",
        "2026-01-02T03:04:05+00:60",
        "2026-01-02T03:04:05+00:00:60",
        "2026-02-29T03:04:05+00:00",
        "0000-01-02T03:04:05+00:00",
        "2026-01-02T24:00:00+00:00",
        "2026-01-02T03:04:60+00:00",
        " 2026-01-02T03:04:05+00:00",
        "2026-01-02T03:04:05+00:00 ",
        "2026-01-02T03:04:05+00:00\x00",
        "\ufeff2026-01-02T03:04:05+00:00",
        "\uff12\uff10\uff12\uff16-01-02T03:04:05+00:00",
    ),
)
def test_non_writer_text_forms_are_malformed(value: str) -> None:
    outcome = _outcome(value.encode())

    assert outcome.status is SQLiteTimestampParseStatus.MALFORMED_TEXT
    assert outcome.decoded_text == value
    assert outcome.parsed_datetime is None


@pytest.mark.parametrize(
    "raw",
    (
        b"\x80",
        b"\xc0\xaf",
        b"\xed\xa0\x80",
        b"\xf0\x80\x80\xaf",
        b"\xf4\x90\x80\x80",
        b"\xe2\x82",
    ),
)
def test_invalid_utf8_is_typed_without_text_replacement(raw: bytes) -> None:
    outcome = _outcome(raw)

    assert outcome.status is SQLiteTimestampParseStatus.MALFORMED_UTF8
    assert outcome.decoded_text is None
    assert outcome.source_cell.blob_hex == raw.hex().upper()


def test_fixed_utc_policy_accepts_only_exact_current_writer_offset() -> None:
    utc = _outcome(
        b"2026-01-02T03:04:05.000001+00:00",
        offset_policy=SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET,
    )
    non_utc = _outcome(
        b"2026-01-02T03:04:05.000001+02:30",
        offset_policy=SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET,
    )

    assert utc.status is SQLiteTimestampParseStatus.PARSED_AWARE_TEXT
    assert utc.parsed_datetime is not None
    assert utc.parsed_datetime.tzinfo is UTC
    assert non_utc.status is SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH
    assert non_utc.decoded_text == "2026-01-02T03:04:05.000001+02:30"
    assert non_utc.utc_offset_microseconds == 9_000_000_000
    assert non_utc.parsed_datetime is not None
    assert non_utc.parsed_datetime.isoformat() == non_utc.decoded_text


@pytest.mark.parametrize(
    ("value", "status"),
    (
        (MIN_EPOCH_MICROSECONDS, SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS),
        (MAX_EPOCH_MICROSECONDS, SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS),
        (-1, SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS),
        (0, SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS),
        (1, SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS),
        (MIN_EPOCH_MICROSECONDS - 1, SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE),
        (MAX_EPOCH_MICROSECONDS + 1, SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE),
        (-(2**63), SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE),
        (2**63 - 1, SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE),
        (-(2**63) - 1, SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES),
        (2**63, SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES),
    ),
)
def test_epoch_boundaries_are_typed(value: int, status: SQLiteTimestampParseStatus) -> None:
    outcome = _outcome(
        str(value).encode(),
        storage_class=SQLiteStorageClass.INTEGER,
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
    )

    assert outcome.status is status
    assert outcome.decoded_text == str(value)
    if status is SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES:
        assert outcome.epoch_microseconds is None
    else:
        assert outcome.epoch_microseconds == value
    if status is SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS:
        assert outcome.parsed_datetime is not None
        assert outcome.parsed_datetime.tzinfo is UTC
    else:
        assert outcome.parsed_datetime is None


@pytest.mark.parametrize(
    "raw",
    (
        b"+1",
        b"-0",
        b"00",
        b"01",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e3",
        b"1_000",
        "\u0661".encode(),
        b"1\x00",
        b"\xff",
    ),
)
def test_noncanonical_epoch_cast_bytes_are_malformed(raw: bytes) -> None:
    outcome = _outcome(
        raw,
        storage_class=SQLiteStorageClass.INTEGER,
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
    )

    assert outcome.status is SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES
    assert outcome.parsed_datetime is None


def test_oversized_canonical_epoch_is_malformed_without_integer_conversion() -> None:
    raw = b"9" * 4096
    outcome = _outcome(
        raw,
        storage_class=SQLiteStorageClass.INTEGER,
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
    )

    assert outcome.status is SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES
    assert outcome.decoded_text == raw.decode()
    assert outcome.epoch_microseconds is None


@pytest.mark.parametrize(
    "storage_class",
    (
        SQLiteStorageClass.INTEGER,
        SQLiteStorageClass.REAL,
        SQLiteStorageClass.BLOB,
    ),
)
def test_text_storage_mismatch_has_precedence(
    storage_class: SQLiteStorageClass,
) -> None:
    outcome = _outcome(
        b"2026-01-02T03:04:05+00:00",
        storage_class=storage_class,
    )
    assert outcome.status is SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS
    assert outcome.decoded_text is None


def test_nullability_and_epoch_storage_precedence_are_exact() -> None:
    nullable_null = _outcome(
        b"",
        storage_class=SQLiteStorageClass.NULL,
        nullable=True,
    )
    required_null = _outcome(
        b"",
        storage_class=SQLiteStorageClass.NULL,
    )
    text_integer = _outcome(
        b"0",
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
    )

    assert nullable_null.status is SQLiteTimestampParseStatus.DECLARED_ABSENT
    assert required_null.status is SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS
    assert text_integer.status is SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS


def test_outcome_revalidation_rejects_status_datetime_and_source_tampering() -> None:
    outcome = _outcome(b"2026-01-02T03:04:05+02:00")
    replacements = (
        {"status": SQLiteTimestampParseStatus.MALFORMED_TEXT},
        {"utc_offset_microseconds": 0},
        {
            "source_cell": _cell(
                b"2026-01-02T01:04:05+00:00",
            )
        },
        {"offset_policy": SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET},
    )

    for replacement in replacements:
        tampered = outcome.model_copy(update=replacement)
        with pytest.raises(ValidationError, match="does not match"):
            SQLiteTimestampParseOutcome.model_validate(tampered)


def test_outcome_rejects_custom_timezone_name_and_datetime_subclass_without_calling_it() -> None:
    outcome = _outcome(b"2026-01-02T03:04:05+02:00")
    assert outcome.parsed_datetime is not None
    named_zone = timezone(timedelta(hours=2), "CUSTOM")
    named_datetime = outcome.parsed_datetime.replace(tzinfo=named_zone)
    with pytest.raises(ValidationError, match="does not match"):
        SQLiteTimestampParseOutcome.model_validate(
            outcome.model_copy(update={"parsed_datetime": named_datetime})
        )

    class HostileDatetime(datetime):
        def utcoffset(self) -> timedelta | None:
            raise AssertionError("hostile datetime method must not run")

    hostile = HostileDatetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        tzinfo=timezone(timedelta(hours=2)),
    )
    with pytest.raises(ValidationError, match="does not match"):
        SQLiteTimestampParseOutcome.model_validate(
            outcome.model_copy(update={"parsed_datetime": hostile})
        )


def test_row_contract_deeply_revalidates_external_task_031_key_cells() -> None:
    key = _cell(b"key", column_name="record_id")
    forged_key = key.model_copy(update={"column_name": "bad name"})
    outcome = _outcome(b"2026-01-02T03:04:05+00:00")

    with pytest.raises(ValidationError, match="deep validation"):
        SQLiteTimestampParsedRowEvidence(
            row_ordinal=0,
            stable_row_key=(forged_key,),
            outcomes=(outcome,),
        )


def test_parse_module_is_pure_and_has_no_normalization_or_runtime_consumer() -> None:
    tree = ast.parse(PARSE_MODULE_PATH.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    called_names: set[str] = set()
    called_attributes: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_attributes.add(node.func.attr)

    assert "sqlite3" not in imported_roots
    assert "pathlib" not in imported_roots
    assert "os" not in imported_roots
    assert not any(name.startswith("wealth.adapters") for name in imported_roots)
    assert "open" not in called_names
    assert "fromisoformat" not in called_attributes
    assert "astimezone" not in called_attributes

    consumers = []
    for path in (REPOSITORY_ROOT / "src" / "wealth").rglob("*.py"):
        if path == PARSE_MODULE_PATH:
            continue
        text = path.read_text(encoding="utf-8")
        if "sqlite_timestamp_parse" in text:
            consumers.append(path)
    assert consumers == []
