"""Strict pure-contract coverage for TASK-034 candidate census evidence."""

import ast
from pathlib import Path

import pytest
from pydantic import ValidationError

from wealth.domain import sqlite_timestamp_candidate as timestamp_candidate
from wealth.domain import sqlite_timestamp_candidate_census as candidate_census
from wealth.domain import sqlite_timestamp_parse as timestamp_parse
from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    SQLiteStorageClass,
    SQLiteStoreFamily,
    SQLiteTimestampCellEvidence,
)
from wealth.domain.sqlite_timestamp_candidate import (
    SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS,
    SQLiteTimestampCanonicalCandidateOutcome,
    SQLiteTimestampCanonicalCandidateResult,
    SQLiteTimestampCanonicalCandidateStatus,
    SQLiteTimestampCanonicalCandidateTableEvidence,
)
from wealth.domain.sqlite_timestamp_candidate_census import (
    SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS,
    SQLiteTimestampCandidateCensusColumnPlan,
    SQLiteTimestampCandidateCensusError,
    SQLiteTimestampCandidateCensusErrorCode,
    SQLiteTimestampCandidateCensusPlan,
    SQLiteTimestampCandidateColumnCensus,
    SQLiteTimestampCandidateStatusCount,
    SQLiteTimestampFractionalPrecisionFrequency,
    SQLiteTimestampParseStatusCount,
    SQLiteTimestampUtcOffsetFrequency,
    build_synthetic_sqlite_timestamp_candidate_census_evidence,
)
from wealth.domain.sqlite_timestamp_parse import (
    SQLiteTimestampOffsetPolicy,
    SQLiteTimestampParseStatus,
    SQLiteTimestampRepresentation,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CENSUS_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census.py"
)
BUNDLE_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census_bundle.py"
)
AUTHORIZATION_REQUEST_MODULE_PATH = (
    REPOSITORY_ROOT
    / "src"
    / "wealth"
    / "domain"
    / "sqlite_operator_preflight_authorization_request.py"
)


def _cell(
    raw: bytes,
    *,
    column_name: str,
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
) -> SQLiteTimestampCellEvidence:
    return SQLiteTimestampCellEvidence(
        column_name=column_name,
        storage_class=storage_class,
        blob_hex=raw.hex().upper(),
        byte_length=len(raw),
    )


def _candidate(
    declaration: SQLiteTimestampCandidateCensusColumnPlan,
    raw: bytes,
    *,
    storage_class: SQLiteStorageClass = SQLiteStorageClass.TEXT,
) -> SQLiteTimestampCanonicalCandidateOutcome:
    column_plan = declaration.source_column_plan
    source = timestamp_parse._parse_outcome(
        _cell(
            raw,
            column_name=column_plan.column_name,
            storage_class=storage_class,
        ),
        column_plan,
    )
    return timestamp_candidate._candidate_outcome(source)


def _declaration(
    *,
    representation: SQLiteTimestampRepresentation,
    offset_policy: SQLiteTimestampOffsetPolicy | None,
    nullable: bool | None = None,
) -> SQLiteTimestampCandidateCensusColumnPlan:
    for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS:
        for declaration in plan.columns:
            source = declaration.source_column_plan
            if (
                source.representation is representation
                and source.offset_policy is offset_policy
                and (nullable is None or source.nullable is nullable)
            ):
                return declaration
    raise AssertionError("the reviewed census registry must contain the requested declaration")


def _count_by_candidate_status(
    summary: SQLiteTimestampCandidateColumnCensus,
) -> dict[SQLiteTimestampCanonicalCandidateStatus, int]:
    return {item.status: item.count for item in summary.candidate_status_counts}


def _count_by_parse_status(
    summary: SQLiteTimestampCandidateColumnCensus,
) -> dict[SQLiteTimestampParseStatus, int]:
    return {item.status: item.count for item in summary.source_parse_status_counts}


def _mixed_aware_summary() -> tuple[
    SQLiteTimestampCandidateColumnCensus,
    tuple[SQLiteTimestampCanonicalCandidateOutcome, ...],
]:
    declaration = _declaration(
        representation=SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT,
        offset_policy=SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET,
        nullable=False,
    )
    candidates = (
        _candidate(
            declaration,
            b"2026-01-02T05:34:05.123456+02:30",
        ),
        _candidate(
            declaration,
            b"2026-01-01T23:34:05-03:30",
        ),
        _candidate(
            declaration,
            b"2026-01-03T05:34:05.000001+02:30",
        ),
        _candidate(
            declaration,
            b"0001-01-01T00:00:00+00:00:00.000001",
        ),
        _candidate(
            declaration,
            b"2026-01-02T03:04:05",
        ),
        _candidate(
            declaration,
            b"not-a-timestamp",
        ),
    )
    return candidate_census._summarize_column(declaration, candidates), candidates


def _empty_candidate_source(
    plan: SQLiteTimestampCandidateCensusPlan,
) -> SQLiteTimestampCanonicalCandidateResult:
    tables = tuple(
        SQLiteTimestampCanonicalCandidateTableEvidence(
            target_ordinal=target_ordinal,
            table_name=target.table_name,
            rows=(),
        )
        for target_ordinal, target in enumerate(plan.source_plan.source_plan.targets)
    )
    return SQLiteTimestampCanonicalCandidateResult.model_construct(
        schema_version="1.0",
        plan=plan.source_plan,
        tables=tables,
    )


def test_census_registry_is_exact_family_scoped_and_collectively_covers_37_columns() -> None:
    assert len(SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS) == 8
    assert (
        tuple(plan.source_plan for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS)
        == SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS
    )
    assert tuple(
        plan.source_plan.source_plan.extraction_plan.family
        for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS
    ) == tuple(SQLiteStoreFamily)

    identities: list[tuple[SQLiteStoreFamily, str, str]] = []
    for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS:
        family = plan.source_plan.source_plan.extraction_plan.family
        expected = tuple(
            (
                summary_ordinal,
                target_ordinal,
                column_ordinal,
                target.table_name,
                column,
            )
            for summary_ordinal, (target_ordinal, column_ordinal, target, column) in enumerate(
                (
                    (target_ordinal, column_ordinal, target, column)
                    for target_ordinal, target in enumerate(plan.source_plan.source_plan.targets)
                    for column_ordinal, column in enumerate(target.columns)
                )
            )
        )
        observed = tuple(
            (
                declaration.summary_ordinal,
                declaration.target_ordinal,
                declaration.column_ordinal,
                declaration.table_name,
                declaration.source_column_plan,
            )
            for declaration in plan.columns
        )
        assert observed == expected
        assert plan.candidate_status_order == (
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS,
            SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE,
            SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW,
        )
        assert plan.source_parse_status_order == (
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
            SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS,
            SQLiteTimestampParseStatus.DECLARED_ABSENT,
            SQLiteTimestampParseStatus.NAIVE_TEXT,
            SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
            SQLiteTimestampParseStatus.MALFORMED_UTF8,
            SQLiteTimestampParseStatus.MALFORMED_TEXT,
            SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
            SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE,
            SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS,
        )
        assert set(plan.candidate_status_order) == set(SQLiteTimestampCanonicalCandidateStatus)
        assert set(plan.source_parse_status_order) == set(SQLiteTimestampParseStatus)
        assert plan.projected_candidate_statuses == (
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS,
        )
        assert plan.offset_frequency_source_statuses == (
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
            SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
        )
        assert plan.precision_frequency_source_statuses == (
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
            SQLiteTimestampParseStatus.NAIVE_TEXT,
            SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
        )
        identities.extend(
            (
                family,
                declaration.table_name,
                declaration.source_column_plan.column_name,
            )
            for declaration in plan.columns
        )

    assert len(identities) == 37
    assert len(set(identities)) == 37


def test_every_family_plan_emits_one_zero_census_for_each_genuinely_empty_column() -> None:
    observed_declarations: list[SQLiteTimestampCandidateCensusColumnPlan] = []
    for plan in SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS:
        source = _empty_candidate_source(plan)

        first = candidate_census._summaries(source, plan)
        second = candidate_census._summaries(source, plan)

        assert first == second
        assert len(first) == len(plan.columns)
        assert tuple(summary.declaration for summary in first) == plan.columns
        for summary in first:
            assert summary.total_candidate_count == 0
            assert all(item.count == 0 for item in summary.candidate_status_counts)
            assert all(item.count == 0 for item in summary.source_parse_status_counts)
            assert summary.source_utc_offset_frequencies == ()
            assert summary.source_fractional_precision_frequencies == ()
            assert summary.projectable_epoch_min is None
            assert summary.projectable_epoch_max is None
        observed_declarations.extend(summary.declaration for summary in first)

    assert len(observed_declarations) == 37


def test_census_plan_is_strict_frozen_private_and_rejects_altered_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[0]
    private_plans = candidate_census._PINNED_CENSUS_PLANS

    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampCandidateCensusPlan.model_validate(
            {**plan.model_dump(mode="python"), "collision_policy": "merge"},
            strict=True,
        )
    with pytest.raises(ValidationError):
        plan.census_kind = "changed"  # type: ignore[assignment]

    first = plan.columns[0]
    altered_column = first.model_copy(update={"table_name": "altered_table"})
    invalid_columns = (
        plan.columns[:-1],
        tuple(reversed(plan.columns)),
        (plan.columns[0], plan.columns[0], *plan.columns[2:]),
        (altered_column, *plan.columns[1:]),
    )
    for columns in invalid_columns:
        with pytest.raises(ValidationError, match="flatten"):
            SQLiteTimestampCandidateCensusPlan(
                source_plan=plan.source_plan,
                columns=columns,
            )

    invalid_semantics = (
        {"candidate_status_order": tuple(reversed(plan.candidate_status_order))},
        {"source_parse_status_order": tuple(reversed(plan.source_parse_status_order))},
        {"projected_candidate_statuses": tuple(reversed(plan.projected_candidate_statuses))},
        {
            "offset_frequency_source_statuses": tuple(
                reversed(plan.offset_frequency_source_statuses)
            )
        },
        {
            "precision_frequency_source_statuses": tuple(
                reversed(plan.precision_frequency_source_statuses)
            )
        },
    )
    for update in invalid_semantics:
        with pytest.raises(ValidationError, match="status and frequency semantics"):
            SQLiteTimestampCandidateCensusPlan.model_validate(
                {
                    **plan.model_dump(mode="python"),
                    **update,
                },
                strict=True,
            )
    for field_name in (
        "candidate_status_order",
        "source_parse_status_order",
        "projected_candidate_statuses",
        "offset_frequency_source_statuses",
        "precision_frequency_source_statuses",
    ):
        values = getattr(plan, field_name)
        for malformed_values in (values[:-1], (*values, values[0])):
            with pytest.raises(ValidationError):
                SQLiteTimestampCandidateCensusPlan.model_validate(
                    {
                        **plan.model_dump(mode="python"),
                        field_name: malformed_values,
                    },
                    strict=True,
                )

    forged_source_plan = plan.source_plan.model_copy(update={"projection_kind": "changed"})
    with pytest.raises(ValidationError):
        SQLiteTimestampCandidateCensusPlan(
            source_plan=forged_source_plan,
            columns=plan.columns,
        )

    monkeypatch.setattr(
        candidate_census,
        "SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS",
        (),
    )
    assert candidate_census._PINNED_CENSUS_PLANS is private_plans
    assert len(private_plans) == 8


def test_column_declaration_and_frequency_models_are_strict_and_frozen() -> None:
    declaration = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[0].columns[0]
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampCandidateCensusColumnPlan.model_validate(
            {**declaration.model_dump(mode="python"), "row_group": "forbidden"},
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampCandidateCensusColumnPlan.model_validate(
            {**declaration.model_dump(mode="python"), "summary_ordinal": False},
            strict=True,
        )
    with pytest.raises(ValidationError):
        declaration.table_name = "changed"

    with pytest.raises(ValidationError):
        SQLiteTimestampCandidateStatusCount(
            status=SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
            count=-1,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampParseStatusCount.model_validate(
            {
                "status": SQLiteTimestampParseStatus.PARSED_AWARE_TEXT.value,
                "count": 1,
            },
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampUtcOffsetFrequency(
            utc_offset_microseconds=-86_400_000_000,
            count=1,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampUtcOffsetFrequency(
            utc_offset_microseconds=0,
            count=0,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampFractionalPrecisionFrequency(
            fractional_precision=3,  # type: ignore[arg-type]
            count=1,
        )
    for coerced_precision in (False, 0.0, 6.0):
        with pytest.raises(ValidationError, match="exact integer"):
            SQLiteTimestampFractionalPrecisionFrequency.model_validate(
                {
                    "fractional_precision": coerced_precision,
                    "count": 1,
                },
                strict=True,
            )
    with pytest.raises(ValidationError):
        SQLiteTimestampFractionalPrecisionFrequency(
            fractional_precision=6,
            count=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET + 1,
        )


def test_mixed_aware_summary_reconciles_statuses_offsets_precision_and_epoch_range() -> None:
    summary, candidates = _mixed_aware_summary()
    repeated = candidate_census._summarize_column(
        summary.declaration,
        tuple(reversed(candidates)),
    )
    candidate_counts = _count_by_candidate_status(summary)
    parse_counts = _count_by_parse_status(summary)

    assert repeated == summary
    assert summary.total_candidate_count == 6
    assert candidate_counts == {
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT: 3,
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS: 0,
        SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE: 2,
        SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW: 1,
    }
    assert parse_counts[SQLiteTimestampParseStatus.PARSED_AWARE_TEXT] == 4
    assert parse_counts[SQLiteTimestampParseStatus.NAIVE_TEXT] == 1
    assert parse_counts[SQLiteTimestampParseStatus.MALFORMED_TEXT] == 1
    assert sum(parse_counts.values()) == 6
    assert tuple(
        (item.utc_offset_microseconds, item.count) for item in summary.source_utc_offset_frequencies
    ) == (
        (-12_600_000_000, 1),
        (1, 1),
        (9_000_000_000, 2),
    )
    assert tuple(
        (item.fractional_precision, item.count)
        for item in summary.source_fractional_precision_frequencies
    ) == ((0, 3), (6, 2))

    projected_epochs = tuple(
        candidate.epoch_microseconds
        for candidate in candidates
        if candidate.status is SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
    )
    assert all(value is not None for value in projected_epochs)
    exact_epochs = tuple(value for value in projected_epochs if value is not None)
    assert summary.projectable_epoch_min == min(exact_epochs)
    assert summary.projectable_epoch_max == max(exact_epochs)


def test_epoch_summary_uses_exact_bounds_and_excludes_nonprojectable_values() -> None:
    declaration = _declaration(
        representation=SQLiteTimestampRepresentation.EPOCH_MICROSECONDS,
        offset_policy=None,
        nullable=False,
    )
    values = (
        MIN_EPOCH_MICROSECONDS,
        -1,
        0,
        1,
        MAX_EPOCH_MICROSECONDS,
    )
    candidates = (
        *(
            _candidate(
                declaration,
                str(value).encode(),
                storage_class=SQLiteStorageClass.INTEGER,
            )
            for value in values
        ),
        _candidate(
            declaration,
            str(MAX_EPOCH_MICROSECONDS + 1).encode(),
            storage_class=SQLiteStorageClass.INTEGER,
        ),
        _candidate(
            declaration,
            b"+1",
            storage_class=SQLiteStorageClass.INTEGER,
        ),
    )

    summary = candidate_census._summarize_column(declaration, candidates)
    candidate_counts = _count_by_candidate_status(summary)
    parse_counts = _count_by_parse_status(summary)

    assert summary.total_candidate_count == 7
    assert (
        candidate_counts[SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS] == 5
    )
    assert parse_counts[SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS] == 5
    assert parse_counts[SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE] == 1
    assert parse_counts[SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES] == 1
    assert summary.source_utc_offset_frequencies == ()
    assert summary.source_fractional_precision_frequencies == ()
    assert summary.projectable_epoch_min == MIN_EPOCH_MICROSECONDS
    assert summary.projectable_epoch_max == MAX_EPOCH_MICROSECONDS


def test_fixed_utc_mismatch_naive_and_nullable_absence_frequency_semantics() -> None:
    fixed = _declaration(
        representation=SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT,
        offset_policy=SQLiteTimestampOffsetPolicy.FIXED_UTC_OFFSET,
        nullable=False,
    )
    candidates = (
        _candidate(fixed, b"2026-01-02T03:04:05.000001+00:00"),
        _candidate(fixed, b"2026-01-02T03:04:05+02:00"),
        _candidate(fixed, b"2026-01-02T03:04:05"),
        _candidate(fixed, b"malformed"),
    )
    summary = candidate_census._summarize_column(fixed, candidates)

    assert tuple(
        (item.utc_offset_microseconds, item.count) for item in summary.source_utc_offset_frequencies
    ) == ((0, 1), (7_200_000_000, 1))
    assert tuple(
        (item.fractional_precision, item.count)
        for item in summary.source_fractional_precision_frequencies
    ) == ((0, 2), (6, 1))
    assert _count_by_parse_status(summary)[SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH] == 1

    nullable = _declaration(
        representation=SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT,
        offset_policy=SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET,
        nullable=True,
    )
    absent = candidate_census._summarize_column(
        nullable,
        (
            _candidate(
                nullable,
                b"",
                storage_class=SQLiteStorageClass.NULL,
            ),
        ),
    )
    assert (
        _count_by_candidate_status(absent)[
            SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE
        ]
        == 1
    )
    assert _count_by_parse_status(absent)[SQLiteTimestampParseStatus.DECLARED_ABSENT] == 1
    assert absent.source_utc_offset_frequencies == ()
    assert absent.source_fractional_precision_frequencies == ()
    assert absent.projectable_epoch_min is None
    assert absent.projectable_epoch_max is None


def test_duplicate_instants_are_counted_separately_without_grouping() -> None:
    declaration = _declaration(
        representation=SQLiteTimestampRepresentation.LEGACY_ISO8601_TEXT,
        offset_policy=SQLiteTimestampOffsetPolicy.ANY_AWARE_OFFSET,
        nullable=False,
    )
    candidates = (
        _candidate(
            declaration,
            b"2026-07-25T09:00:15.123456+00:00",
        ),
        _candidate(
            declaration,
            b"2026-07-25T14:30:15.123456+05:30",
        ),
    )

    summary = candidate_census._summarize_column(declaration, candidates)

    assert candidates[0].source_outcome != candidates[1].source_outcome
    assert candidates[0].epoch_microseconds == candidates[1].epoch_microseconds
    assert summary.total_candidate_count == 2
    assert (
        _count_by_candidate_status(summary)[
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
        ]
        == 2
    )
    assert summary.projectable_epoch_min == summary.projectable_epoch_max
    assert tuple(
        (item.utc_offset_microseconds, item.count) for item in summary.source_utc_offset_frequencies
    ) == ((0, 1), (19_800_000_000, 1))
    forbidden_fields = {
        name
        for name in SQLiteTimestampCandidateColumnCensus.model_fields
        if any(
            token in name
            for token in ("row", "instant", "collision", "group", "merge", "quarantine")
        )
    }
    assert forbidden_fields == set()


def test_summary_contract_rejects_hostile_count_frequency_and_extrema_tampering() -> None:
    summary, _ = _mixed_aware_summary()
    candidate_counts = summary.candidate_status_counts
    parse_counts = summary.source_parse_status_counts
    offsets = summary.source_utc_offset_frequencies
    precisions = summary.source_fractional_precision_frequencies

    shifted_candidate_counts = tuple(
        item.model_copy(
            update={
                "count": (
                    item.count - 1
                    if item.status is SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
                    else item.count + 1
                    if item.status
                    is SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
                    else item.count
                )
            }
        )
        for item in candidate_counts
    )
    changed_parse_counts = (
        parse_counts[0].model_copy(update={"count": parse_counts[0].count + 1}),
        *parse_counts[1:],
    )
    invalid_updates: tuple[dict[str, object], ...] = (
        {"total_candidate_count": summary.total_candidate_count + 1},
        {"candidate_status_counts": tuple(reversed(candidate_counts))},
        {"candidate_status_counts": (*candidate_counts, candidate_counts[0])},
        {"candidate_status_counts": shifted_candidate_counts},
        {"source_parse_status_counts": tuple(reversed(parse_counts))},
        {"source_parse_status_counts": (*parse_counts, parse_counts[0])},
        {"source_parse_status_counts": changed_parse_counts},
        {"source_utc_offset_frequencies": tuple(reversed(offsets))},
        {
            "source_utc_offset_frequencies": (
                offsets[0],
                offsets[0],
                *offsets[1:],
            )
        },
        {
            "source_utc_offset_frequencies": (
                offsets[0].model_copy(update={"count": offsets[0].count + 1}),
                *offsets[1:],
            )
        },
        {"source_fractional_precision_frequencies": tuple(reversed(precisions))},
        {
            "source_fractional_precision_frequencies": (
                precisions[0].model_copy(update={"count": precisions[0].count + 1}),
                *precisions[1:],
            )
        },
        {"projectable_epoch_min": None},
        {
            "projectable_epoch_min": summary.projectable_epoch_max,
            "projectable_epoch_max": summary.projectable_epoch_min,
        },
        {"total_candidate_count": True},
    )

    for update in invalid_updates:
        tampered = summary.model_copy(update=update)
        with pytest.raises(ValidationError):
            SQLiteTimestampCandidateColumnCensus.model_validate(
                tampered,
                strict=True,
            )

    with pytest.raises(ValidationError):
        summary.total_candidate_count = 0


def test_public_boundary_rejects_wrong_types_and_model_construct_before_summarizing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = SQLiteTimestampCanonicalCandidateResult.model_construct()
    partial = SQLiteTimestampCanonicalCandidateResult.model_construct(
        plan=SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS[0],
        tables=(),
    )

    class CandidateResultSubclass(SQLiteTimestampCanonicalCandidateResult):
        pass

    subclass = CandidateResultSubclass.model_construct()

    def unexpected_summarization(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid TASK-033 evidence reached census summarization")

    monkeypatch.setattr(
        candidate_census,
        "_summaries",
        unexpected_summarization,
    )
    invalid_sources: tuple[object, ...] = (
        {},
        SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS[0],
        missing,
        partial,
        subclass,
    )
    for invalid in invalid_sources:
        with pytest.raises(SQLiteTimestampCandidateCensusError) as caught:
            build_synthetic_sqlite_timestamp_candidate_census_evidence(
                invalid  # type: ignore[arg-type]
            )
        assert caught.value.code is (
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE
        )


def test_private_summary_linkage_rejects_reordered_tables_and_declarations() -> None:
    plan = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[0]
    source = _empty_candidate_source(plan)
    reversed_source = source.model_copy(update={"tables": tuple(reversed(source.tables))})
    with pytest.raises(AssertionError, match="table linkage"):
        candidate_census._summaries(reversed_source, plan)

    altered_declaration = plan.columns[0].model_copy(
        update={"column_ordinal": plan.columns[0].column_ordinal + 1}
    )
    altered_plan = plan.model_copy(update={"columns": (altered_declaration, *plan.columns[1:])})
    with pytest.raises(AssertionError, match="declaration"):
        candidate_census._summaries(source, altered_plan)


def test_census_module_is_pure_and_has_no_runtime_consumer() -> None:
    tree = ast.parse(CENSUS_MODULE_PATH.read_text(encoding="utf-8"))
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
    assert "json" not in imported_roots
    assert not any(name.startswith("wealth.adapters") for name in imported_roots)
    assert "open" not in called_names
    assert (
        not {
            "connect",
            "model_dump_json",
            "read_bytes",
            "read_text",
            "write_bytes",
            "write_text",
        }
        & called_attributes
    )

    consumers = []
    for path in (REPOSITORY_ROOT / "src" / "wealth").rglob("*.py"):
        if path in {
            CENSUS_MODULE_PATH,
            BUNDLE_MODULE_PATH,
            AUTHORIZATION_REQUEST_MODULE_PATH,
        }:
            continue
        if "sqlite_timestamp_candidate_census" in path.read_text(encoding="utf-8"):
            consumers.append(path)
    assert consumers == []
