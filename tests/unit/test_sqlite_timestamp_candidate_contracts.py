"""Strict pure-contract coverage for TASK-033 canonical candidate evidence."""

import ast
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path

import pytest
from pydantic import ValidationError

from wealth.domain import sqlite_timestamp_candidate as timestamp_candidate
from wealth.domain import sqlite_timestamp_parse as timestamp_parse
from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
    from_epoch_microseconds,
    parse_canonical_utc,
    serialize_canonical_utc,
    to_epoch_microseconds,
)
from wealth.domain.sqlite_preflight import (
    SQLiteStorageClass,
    SQLiteTimestampCellEvidence,
)
from wealth.domain.sqlite_timestamp_candidate import (
    SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS,
    SQLiteTimestampCanonicalCandidateOutcome,
    SQLiteTimestampCanonicalCandidatePlan,
    SQLiteTimestampCanonicalCandidateStatus,
)
from wealth.domain.sqlite_timestamp_parse import (
    SQLITE_TIMESTAMP_PARSE_PLANS,
    SQLiteTimestampColumnParsePlan,
    SQLiteTimestampOffsetPolicy,
    SQLiteTimestampParseOutcome,
    SQLiteTimestampParseStatus,
    SQLiteTimestampRepresentation,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CANDIDATE_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate.py"
)
CANDIDATE_CENSUS_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census.py"
)
CANDIDATE_CENSUS_BUNDLE_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census_bundle.py"
)


def _cell(
    raw: bytes,
    *,
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
    column_name: str = "observed_at",
) -> SQLiteTimestampCellEvidence:
    return SQLiteTimestampCellEvidence(
        column_name=column_name,
        storage_class=storage_class,
        blob_hex=raw.hex().upper(),
        byte_length=len(raw),
    )


def _parse_outcome(
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
    column_plan = SQLiteTimestampColumnParsePlan(
        column_name="observed_at",
        representation=representation,
        offset_policy=offset_policy,
        nullable=nullable,
    )
    return timestamp_parse._parse_outcome(
        _cell(raw, storage_class=storage_class),
        column_plan,
    )


def _candidate(
    source: SQLiteTimestampParseOutcome,
) -> SQLiteTimestampCanonicalCandidateOutcome:
    return timestamp_candidate._candidate_outcome(source)


def test_candidate_registry_is_exact_complete_and_pinned_to_task_032() -> None:
    assert len(SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS) == 8
    assert (
        tuple(plan.source_plan for plan in SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS)
        == SQLITE_TIMESTAMP_PARSE_PLANS
    )
    assert all(
        plan.schema_version == "1.0"
        and plan.projection_kind == "fixed_utc_text_epoch_microseconds"
        and plan.projectable_source_statuses
        == (
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
            SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS,
        )
        and set(
            (
                *plan.projectable_source_statuses,
                *plan.nonprojectable_source_statuses,
            )
        )
        == set(SQLiteTimestampParseStatus)
        for plan in SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS
    )
    assert (
        sum(
            len(target.columns)
            for plan in SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS
            for target in plan.source_plan.targets
        )
        == 37
    )


def test_candidate_plan_is_strict_frozen_and_deeply_revalidates_task_032() -> None:
    plan = SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS[0]
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampCanonicalCandidatePlan.model_validate(
            {**plan.model_dump(), "collision_policy": "merge"}
        )
    forged_extraction = plan.source_plan.extraction_plan.model_copy(
        update={"expected_store_sha256": "x"}
    )
    forged_source_plan = plan.source_plan.model_copy(update={"extraction_plan": forged_extraction})
    with pytest.raises(ValidationError, match="deep validation"):
        SQLiteTimestampCanonicalCandidatePlan(source_plan=forged_source_plan)
    valid_wrong_extraction = plan.source_plan.extraction_plan.model_copy(
        update={"expected_store_sha256": "0" * 64}
    )
    valid_wrong_digest = plan.source_plan.model_copy(
        update={"extraction_plan": valid_wrong_extraction}
    )
    first_target = plan.source_plan.targets[0]
    first_column = first_target.columns[0]
    altered_column = first_column.model_copy(update={"nullable": not first_column.nullable})
    altered_target = first_target.model_copy(
        update={"columns": (altered_column, *first_target.columns[1:])}
    )
    altered_declaration = plan.source_plan.model_copy(
        update={"targets": (altered_target, *plan.source_plan.targets[1:])}
    )
    for altered_plan in (valid_wrong_digest, altered_declaration):
        with pytest.raises(ValidationError, match="reviewed immutable"):
            SQLiteTimestampCanonicalCandidatePlan(source_plan=altered_plan)
    with pytest.raises(ValidationError, match="status partition"):
        SQLiteTimestampCanonicalCandidatePlan(
            source_plan=plan.source_plan,
            projectable_source_statuses=tuple(reversed(plan.projectable_source_statuses)),
        )
    with pytest.raises(ValidationError):
        plan.projection_kind = "changed"  # type: ignore[assignment]


@pytest.mark.parametrize(
    ("source_text", "canonical_text"),
    (
        (
            "2026-01-02T03:04:05.123456+00:00",
            "2026-01-02T03:04:05.123456Z",
        ),
        (
            "2026-01-02T05:34:05.123456+02:30",
            "2026-01-02T03:04:05.123456Z",
        ),
        (
            "2026-01-01T23:34:05.123456-03:30",
            "2026-01-02T03:04:05.123456Z",
        ),
        (
            "2026-01-02T03:04:05+00:00:00.000001",
            "2026-01-02T03:04:04.999999Z",
        ),
        (
            "2026-01-02T03:04:05-00:00:00.000001",
            "2026-01-02T03:04:05.000001Z",
        ),
    ),
)
def test_aware_text_projects_to_exact_round_tripping_canonical_evidence(
    source_text: str,
    canonical_text: str,
) -> None:
    source = _parse_outcome(source_text.encode())
    candidate = _candidate(source)

    assert candidate.status is (SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT)
    assert candidate.source_outcome == source
    assert candidate.canonical_text == canonical_text
    assert candidate.canonical_datetime == parse_canonical_utc(canonical_text)
    assert candidate.canonical_datetime is not None
    assert type(candidate.canonical_datetime) is datetime
    assert candidate.canonical_datetime.tzinfo is UTC
    assert candidate.canonical_datetime.fold == 0
    assert serialize_canonical_utc(candidate.canonical_datetime) == canonical_text
    assert to_epoch_microseconds(candidate.canonical_datetime) == (candidate.epoch_microseconds)
    assert candidate.epoch_microseconds is not None
    assert from_epoch_microseconds(candidate.epoch_microseconds) == (candidate.canonical_datetime)


@pytest.mark.parametrize(
    ("source_text", "status", "canonical_text", "epoch_microseconds"),
    (
        (
            "0001-01-01T00:00:00+00:00:00.000001",
            SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW,
            None,
            None,
        ),
        (
            "0001-01-01T00:00:00.000001+00:00:00.000001",
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            "0001-01-01T00:00:00.000000Z",
            MIN_EPOCH_MICROSECONDS,
        ),
        (
            "9999-12-31T23:59:59.999999-00:00:00.000001",
            SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW,
            None,
            None,
        ),
        (
            "9999-12-31T23:59:59.999998-00:00:00.000001",
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            "9999-12-31T23:59:59.999999Z",
            MAX_EPOCH_MICROSECONDS,
        ),
    ),
)
def test_one_microsecond_calendar_edges_are_projected_or_typed_without_clipping(
    source_text: str,
    status: SQLiteTimestampCanonicalCandidateStatus,
    canonical_text: str | None,
    epoch_microseconds: int | None,
) -> None:
    source = _parse_outcome(source_text.encode())
    candidate = _candidate(source)

    assert candidate.status is status
    assert candidate.canonical_text == canonical_text
    assert candidate.epoch_microseconds == epoch_microseconds
    if canonical_text is None:
        assert candidate.canonical_datetime is None
    else:
        assert candidate.canonical_datetime == parse_canonical_utc(canonical_text)


@pytest.mark.parametrize(
    "epoch_microseconds",
    (MIN_EPOCH_MICROSECONDS, -1, 0, 1, MAX_EPOCH_MICROSECONDS),
)
def test_epoch_sources_project_exactly_without_changing_the_integer(
    epoch_microseconds: int,
) -> None:
    source = _parse_outcome(
        str(epoch_microseconds).encode(),
        storage_class=SQLiteStorageClass.INTEGER,
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
    )
    candidate = _candidate(source)

    assert candidate.status is (
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
    )
    assert candidate.epoch_microseconds == epoch_microseconds
    assert candidate.canonical_datetime == from_epoch_microseconds(epoch_microseconds)
    assert candidate.canonical_datetime is not None
    assert candidate.canonical_text == serialize_canonical_utc(candidate.canonical_datetime)


def test_equal_instants_from_distinct_spellings_remain_distinct_source_evidence() -> None:
    utc_source = _parse_outcome(b"2026-01-02T03:04:05.123456+00:00")
    offset_source = _parse_outcome(b"2026-01-02T05:34:05.123456+02:30")
    utc_candidate = _candidate(utc_source)
    offset_candidate = _candidate(offset_source)

    assert utc_candidate.source_outcome != offset_candidate.source_outcome
    assert utc_candidate.canonical_datetime == offset_candidate.canonical_datetime
    assert utc_candidate.canonical_text == offset_candidate.canonical_text
    assert utc_candidate.epoch_microseconds == offset_candidate.epoch_microseconds


def _nonprojectable_sources() -> tuple[SQLiteTimestampParseOutcome, ...]:
    return (
        _parse_outcome(
            b"",
            storage_class=SQLiteStorageClass.NULL,
            nullable=True,
        ),
        _parse_outcome(b"2026-01-02T03:04:05"),
        _parse_outcome(
            b"2026-01-02T03:04:05+02:00",
            offset_policy=SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET,
        ),
        _parse_outcome(b"\x80"),
        _parse_outcome(b"not-a-timestamp"),
        _parse_outcome(
            b"+1",
            storage_class=SQLiteStorageClass.INTEGER,
            representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
            offset_policy=None,
        ),
        _parse_outcome(
            str(MAX_EPOCH_MICROSECONDS + 1).encode(),
            storage_class=SQLiteStorageClass.INTEGER,
            representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
            offset_policy=None,
        ),
        _parse_outcome(
            b"2026-01-02T03:04:05+00:00",
            storage_class=SQLiteStorageClass.BLOB,
        ),
    )


def test_every_non_success_task_032_status_remains_nonprojectable() -> None:
    sources = _nonprojectable_sources()

    assert {source.status for source in sources} == {
        SQLiteTimestampParseStatus.DECLARED_ABSENT,
        SQLiteTimestampParseStatus.NAIVE_TEXT,
        SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
        SQLiteTimestampParseStatus.MALFORMED_UTF8,
        SQLiteTimestampParseStatus.MALFORMED_TEXT,
        SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
        SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE,
        SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS,
    }
    for source in sources:
        candidate = _candidate(source)
        assert candidate.status is (SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE)
        assert candidate.canonical_datetime is None
        assert candidate.canonical_text is None
        assert candidate.epoch_microseconds is None
        assert candidate.source_outcome == source


def test_candidate_outcome_rejects_field_source_and_datetime_tampering() -> None:
    source = _parse_outcome(b"2026-01-02T03:04:05.123456+00:00")
    candidate = _candidate(source)
    assert candidate.canonical_datetime is not None

    class TimestampSubclass(datetime):
        pass

    subclass = TimestampSubclass(
        2026,
        1,
        2,
        3,
        4,
        5,
        123456,
        tzinfo=UTC,
    )

    class HostileTimezone(tzinfo):
        def utcoffset(self, value: datetime | None) -> timedelta:
            del value
            raise AssertionError("hostile timezone method must not run")

        def dst(self, value: datetime | None) -> timedelta:
            del value
            raise AssertionError("hostile timezone method must not run")

        def tzname(self, value: datetime | None) -> str:
            del value
            raise AssertionError("hostile timezone method must not run")

        def __eq__(self, value: object) -> bool:
            del value
            raise AssertionError("hostile timezone equality must not run")

    hostile_timezone_datetime = datetime(
        2026,
        1,
        2,
        3,
        4,
        5,
        123456,
        tzinfo=HostileTimezone(),
    )
    forged_source = source.model_copy(update={"status": SQLiteTimestampParseStatus.MALFORMED_TEXT})
    replacements = (
        {"status": (SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE)},
        {"canonical_text": "2026-01-02T03:04:05.123457Z"},
        {"epoch_microseconds": candidate.epoch_microseconds + 1},  # type: ignore[operator]
        {"canonical_datetime": subclass},
        {"canonical_datetime": hostile_timezone_datetime},
        {"source_outcome": forged_source},
    )
    for replacement in replacements:
        tampered = candidate.model_copy(update=replacement)
        with pytest.raises(ValidationError):
            SQLiteTimestampCanonicalCandidateOutcome.model_validate(tampered)


def test_candidate_module_is_pure_and_has_no_runtime_consumer() -> None:
    tree = ast.parse(CANDIDATE_MODULE_PATH.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    called_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_roots.add(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called_names.add(node.func.id)

    assert "sqlite3" not in imported_roots
    assert "pathlib" not in imported_roots
    assert "os" not in imported_roots
    assert not any(name.startswith("wealth.adapters") for name in imported_roots)
    assert "open" not in called_names

    consumers = []
    for path in (REPOSITORY_ROOT / "src" / "wealth").rglob("*.py"):
        if path in {
            CANDIDATE_MODULE_PATH,
            CANDIDATE_CENSUS_MODULE_PATH,
            CANDIDATE_CENSUS_BUNDLE_MODULE_PATH,
        }:
            continue
        if "sqlite_timestamp_candidate" in path.read_text(encoding="utf-8"):
            consumers.append(path)
    assert consumers == []
