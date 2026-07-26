"""Strict pure-contract coverage for TASK-035 all-family census bundles."""

import ast
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pytest
from pydantic import ValidationError

from wealth.domain import sqlite_timestamp_candidate_census as candidate_census
from wealth.domain import sqlite_timestamp_candidate_census_bundle as census_bundle
from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    SQLiteStoreFamily,
)
from wealth.domain.sqlite_timestamp_candidate import (
    SQLiteTimestampCanonicalCandidateResult,
    SQLiteTimestampCanonicalCandidateStatus,
)
from wealth.domain.sqlite_timestamp_candidate_census import (
    SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS,
    SQLiteTimestampCandidateCensusPlan,
    SQLiteTimestampCandidateCensusResult,
    SQLiteTimestampCandidateColumnCensus,
    SQLiteTimestampCandidateStatusCount,
    SQLiteTimestampFractionalPrecisionFrequency,
    SQLiteTimestampParseStatusCount,
    SQLiteTimestampUtcOffsetFrequency,
)
from wealth.domain.sqlite_timestamp_candidate_census_bundle import (
    SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN,
    SQLiteTimestampBundleCandidateStatusCount,
    SQLiteTimestampBundleFractionalPrecisionFrequency,
    SQLiteTimestampBundleParseStatusCount,
    SQLiteTimestampBundleUtcOffsetFrequency,
    SQLiteTimestampCandidateCensusBundleAggregate,
    SQLiteTimestampCandidateCensusBundleError,
    SQLiteTimestampCandidateCensusBundleErrorCode,
    SQLiteTimestampCandidateCensusBundlePlan,
    SQLiteTimestampCandidateCensusBundleResult,
    build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence,
)
from wealth.domain.sqlite_timestamp_parse import SQLiteTimestampParseStatus

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_MODULE_PATH = (
    REPOSITORY_ROOT / "src" / "wealth" / "domain" / "sqlite_timestamp_candidate_census_bundle.py"
)
MAX_BUNDLE_CANDIDATES = 37 * MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET


def _candidate_status_counts(
    values: Mapping[SQLiteTimestampCanonicalCandidateStatus, int] | None = None,
) -> tuple[SQLiteTimestampCandidateStatusCount, ...]:
    values = {} if values is None else values
    return tuple(
        SQLiteTimestampCandidateStatusCount(
            status=status,
            count=values.get(status, 0),
        )
        for status in SQLiteTimestampCanonicalCandidateStatus
    )


def _parse_status_counts(
    values: Mapping[SQLiteTimestampParseStatus, int] | None = None,
) -> tuple[SQLiteTimestampParseStatusCount, ...]:
    values = {} if values is None else values
    return tuple(
        SQLiteTimestampParseStatusCount(
            status=status,
            count=values.get(status, 0),
        )
        for status in SQLiteTimestampParseStatus
    )


def _column_summary(
    declaration: candidate_census.SQLiteTimestampCandidateCensusColumnPlan,
    *,
    candidate_counts: Mapping[SQLiteTimestampCanonicalCandidateStatus, int] | None = None,
    parse_counts: Mapping[SQLiteTimestampParseStatus, int] | None = None,
    offsets: tuple[tuple[int, int], ...] = (),
    precisions: tuple[tuple[Literal[0, 6], int], ...] = (),
    epoch_min: int | None = None,
    epoch_max: int | None = None,
) -> SQLiteTimestampCandidateColumnCensus:
    candidate_counts = {} if candidate_counts is None else candidate_counts
    return SQLiteTimestampCandidateColumnCensus(
        declaration=declaration,
        total_candidate_count=sum(candidate_counts.values()),
        candidate_status_counts=_candidate_status_counts(candidate_counts),
        source_parse_status_counts=_parse_status_counts(parse_counts),
        source_utc_offset_frequencies=tuple(
            SQLiteTimestampUtcOffsetFrequency(
                utc_offset_microseconds=offset,
                count=count,
            )
            for offset, count in offsets
        ),
        source_fractional_precision_frequencies=tuple(
            SQLiteTimestampFractionalPrecisionFrequency(
                fractional_precision=precision,
                count=count,
            )
            for precision, count in precisions
        ),
        projectable_epoch_min=epoch_min,
        projectable_epoch_max=epoch_max,
    )


def _internal_source(
    plan: SQLiteTimestampCandidateCensusPlan,
    overrides: Mapping[int, SQLiteTimestampCandidateColumnCensus] | None = None,
    *,
    nested_source: object | None = None,
) -> SQLiteTimestampCandidateCensusResult:
    """Build a safe aggregation fixture without pretending it is public evidence."""

    overrides = {} if overrides is None else overrides
    source = (
        SQLiteTimestampCanonicalCandidateResult.model_construct()
        if nested_source is None
        else nested_source
    )
    return SQLiteTimestampCandidateCensusResult.model_construct(
        schema_version="1.0",
        source=source,
        plan=plan,
        columns=tuple(
            overrides.get(ordinal, _column_summary(declaration))
            for ordinal, declaration in enumerate(plan.columns)
        ),
    )


def _internal_sources(
    overrides: Mapping[int, Mapping[int, SQLiteTimestampCandidateColumnCensus]] | None = None,
    *,
    nested_source: object | None = None,
) -> tuple[SQLiteTimestampCandidateCensusResult, ...]:
    overrides = {} if overrides is None else overrides
    return tuple(
        _internal_source(
            plan,
            overrides.get(family_ordinal),
            nested_source=nested_source,
        )
        for family_ordinal, plan in enumerate(SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS)
    )


def _mixed_sources() -> tuple[SQLiteTimestampCandidateCensusResult, ...]:
    market_declaration = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[0].columns[0]
    market = _column_summary(
        market_declaration,
        candidate_counts={
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT: 5,
            SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE: 2,
            SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW: 1,
        },
        parse_counts={
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT: 6,
            SQLiteTimestampParseStatus.NAIVE_TEXT: 1,
            SQLiteTimestampParseStatus.MALFORMED_TEXT: 1,
        },
        offsets=((-1, 1), (0, 3), (1, 1), (19_800_000_000, 1)),
        precisions=((0, 3), (6, 4)),
        epoch_min=-1,
        epoch_max=1_784_970_015_123_456,
    )
    rate_budget_declaration = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[6].columns[1]
    rate_budget = _column_summary(
        rate_budget_declaration,
        candidate_counts={
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS: 3,
            SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE: 1,
        },
        parse_counts={
            SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS: 3,
            SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE: 1,
        },
        epoch_min=MIN_EPOCH_MICROSECONDS,
        epoch_max=MAX_EPOCH_MICROSECONDS,
    )
    return _internal_sources({0: {0: market}, 6: {1: rate_budget}})


def _bundle_candidate_counts(
    aggregate: SQLiteTimestampCandidateCensusBundleAggregate,
) -> dict[SQLiteTimestampCanonicalCandidateStatus, int]:
    return {item.status: item.count for item in aggregate.candidate_status_counts}


def _bundle_parse_counts(
    aggregate: SQLiteTimestampCandidateCensusBundleAggregate,
) -> dict[SQLiteTimestampParseStatus, int]:
    return {item.status: item.count for item in aggregate.source_parse_status_counts}


def test_bundle_plan_is_exact_private_and_covers_reviewed_8_20_37_registry() -> None:
    plan = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN
    expected_shapes = (
        (SQLiteStoreFamily.MARKET, 3, 5),
        (SQLiteStoreFamily.ORDER_FLOW, 3, 5),
        (SQLiteStoreFamily.HISTORICAL_COLLECTION, 3, 3),
        (SQLiteStoreFamily.CONTINUOUS_COLLECTION, 2, 4),
        (SQLiteStoreFamily.COLLECTOR_SERVICE, 2, 2),
        (SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION, 4, 14),
        (SQLiteStoreFamily.RATE_BUDGET, 2, 3),
        (SQLiteStoreFamily.RECONCILIATION, 1, 1),
    )
    observed_shapes = tuple(
        (
            source_plan.source_plan.source_plan.extraction_plan.family,
            len(source_plan.source_plan.source_plan.targets),
            len(source_plan.columns),
        )
        for source_plan in plan.source_plans
    )

    assert plan is census_bundle._PINNED_BUNDLE_PLAN
    assert plan.source_plans == candidate_census._PINNED_CENSUS_PLANS
    assert observed_shapes == expected_shapes
    assert sum(shape[1] for shape in observed_shapes) == 20
    assert sum(shape[2] for shape in observed_shapes) == 37
    assert plan.expected_family_count == 8
    assert plan.expected_table_count == 20
    assert plan.expected_column_count == 37
    assert plan.family_order == tuple(SQLiteStoreFamily)
    assert plan.candidate_status_order == tuple(SQLiteTimestampCanonicalCandidateStatus)
    assert plan.source_parse_status_order == tuple(SQLiteTimestampParseStatus)
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


def test_bundle_plan_is_strict_frozen_and_rejects_altered_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN
    private_plan = census_bundle._PINNED_BUNDLE_PLAN

    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampCandidateCensusBundlePlan.model_validate(
            {**plan.model_dump(mode="python"), "collision_policy": "merge"},
            strict=True,
        )
    with pytest.raises(ValidationError):
        plan.bundle_kind = "changed"  # type: ignore[assignment]

    invalid_updates: tuple[dict[str, object], ...] = (
        {"source_plans": plan.source_plans[:-1]},
        {"source_plans": tuple(reversed(plan.source_plans))},
        {"source_plans": (plan.source_plans[0], plan.source_plans[0], *plan.source_plans[2:])},
        {"family_order": tuple(reversed(plan.family_order))},
        {"expected_family_count": False},
        {"expected_table_count": 21},
        {"expected_column_count": 36},
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
    for update in invalid_updates:
        with pytest.raises(ValidationError):
            SQLiteTimestampCandidateCensusBundlePlan.model_validate(
                {**plan.model_dump(mode="python"), **update},
                strict=True,
            )

    forged_source_plan = plan.source_plans[0].model_copy(
        update={
            "candidate_status_order": tuple(reversed(plan.source_plans[0].candidate_status_order))
        }
    )
    with pytest.raises(ValidationError):
        SQLiteTimestampCandidateCensusBundlePlan(
            source_plans=(forged_source_plan, *plan.source_plans[1:]),
        )

    monkeypatch.setattr(
        census_bundle,
        "SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN",
        plan.model_copy(update={"expected_column_count": 0}),
    )
    assert census_bundle._PINNED_BUNDLE_PLAN is private_plan
    assert private_plan.source_plans == candidate_census._PINNED_CENSUS_PLANS


def test_bundle_count_and_frequency_models_are_strict_bounded_and_frozen() -> None:
    candidate = SQLiteTimestampBundleCandidateStatusCount(
        status=SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
        count=MAX_BUNDLE_CANDIDATES,
    )
    parse = SQLiteTimestampBundleParseStatusCount(
        status=SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
        count=MAX_BUNDLE_CANDIDATES,
    )
    assert candidate.count == MAX_BUNDLE_CANDIDATES
    assert parse.count == MAX_BUNDLE_CANDIDATES

    for invalid_count in (-1, MAX_BUNDLE_CANDIDATES + 1, False):
        with pytest.raises(ValidationError):
            SQLiteTimestampBundleCandidateStatusCount.model_validate(
                {
                    "status": SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
                    "count": invalid_count,
                },
                strict=True,
            )
        with pytest.raises(ValidationError):
            SQLiteTimestampBundleParseStatusCount.model_validate(
                {
                    "status": SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                    "count": invalid_count,
                },
                strict=True,
            )
    with pytest.raises(ValidationError):
        SQLiteTimestampBundleCandidateStatusCount.model_validate(
            {"status": "projected_aware_text", "count": 1},
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteTimestampBundleParseStatusCount.model_validate(
            {"status": "parsed_aware_text", "count": 1},
            strict=True,
        )

    for offset in (-86_399_999_999, 86_399_999_999):
        offset_frequency = SQLiteTimestampBundleUtcOffsetFrequency(
            utc_offset_microseconds=offset,
            count=MAX_BUNDLE_CANDIDATES,
        )
        assert offset_frequency.utc_offset_microseconds == offset
    for invalid_offset in (-86_400_000_000, 86_400_000_000, False):
        with pytest.raises(ValidationError):
            SQLiteTimestampBundleUtcOffsetFrequency.model_validate(
                {"utc_offset_microseconds": invalid_offset, "count": 1},
                strict=True,
            )
    for invalid_count in (0, MAX_BUNDLE_CANDIDATES + 1, False):
        with pytest.raises(ValidationError):
            SQLiteTimestampBundleUtcOffsetFrequency.model_validate(
                {"utc_offset_microseconds": 0, "count": invalid_count},
                strict=True,
            )

    for precision in (0, 6):
        precision_frequency = SQLiteTimestampBundleFractionalPrecisionFrequency(
            fractional_precision=precision,
            count=MAX_BUNDLE_CANDIDATES,
        )
        assert precision_frequency.fractional_precision == precision
    for invalid_precision in (False, 0.0, 6.0, 3):
        with pytest.raises(ValidationError, match="exact integer"):
            SQLiteTimestampBundleFractionalPrecisionFrequency.model_validate(
                {"fractional_precision": invalid_precision, "count": 1},
                strict=True,
            )
    for invalid_count in (0, MAX_BUNDLE_CANDIDATES + 1, False):
        with pytest.raises(ValidationError):
            SQLiteTimestampBundleFractionalPrecisionFrequency.model_validate(
                {"fractional_precision": 6, "count": invalid_count},
                strict=True,
            )

    with pytest.raises(ValidationError):
        candidate.count = 0
    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampBundleParseStatusCount.model_validate(
            {
                "status": SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                "count": 1,
                "row_count": 1,
            },
            strict=True,
        )


def test_empty_all_family_internal_aggregate_is_exact_deterministic_and_column_only() -> None:
    class HostileNestedSource:
        def __getattribute__(self, name: str) -> object:
            raise AssertionError(f"aggregate traversed forbidden nested source attribute {name}")

    sources = _internal_sources(nested_source=HostileNestedSource())
    census_bundle._require_exact_source_sequence(sources)

    first = census_bundle._aggregate_sources(sources)
    second = census_bundle._aggregate_sources(sources)

    assert first == second
    assert first.family_count == 8
    assert first.table_count == 20
    assert first.column_count == 37
    assert first.total_candidate_count == 0
    assert tuple(item.status for item in first.candidate_status_counts) == tuple(
        SQLiteTimestampCanonicalCandidateStatus
    )
    assert tuple(item.status for item in first.source_parse_status_counts) == tuple(
        SQLiteTimestampParseStatus
    )
    assert all(item.count == 0 for item in first.candidate_status_counts)
    assert all(item.count == 0 for item in first.source_parse_status_counts)
    assert first.source_utc_offset_frequencies == ()
    assert first.source_fractional_precision_frequencies == ()
    assert first.projectable_epoch_min is None
    assert first.projectable_epoch_max is None


def test_mixed_internal_aggregate_sums_vectors_frequencies_and_exact_epoch_bounds() -> None:
    sources = _mixed_sources()
    original_columns = tuple(source.columns for source in sources)

    aggregate = census_bundle._aggregate_sources(sources)

    assert tuple(source.columns for source in sources) == original_columns
    assert aggregate.family_count == 8
    assert aggregate.table_count == 20
    assert aggregate.column_count == 37
    assert aggregate.total_candidate_count == 12
    assert _bundle_candidate_counts(aggregate) == {
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT: 5,
        SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS: 3,
        SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE: 3,
        SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW: 1,
    }
    assert _bundle_parse_counts(aggregate) == {
        SQLiteTimestampParseStatus.PARSED_AWARE_TEXT: 6,
        SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS: 3,
        SQLiteTimestampParseStatus.DECLARED_ABSENT: 0,
        SQLiteTimestampParseStatus.NAIVE_TEXT: 1,
        SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH: 0,
        SQLiteTimestampParseStatus.MALFORMED_UTF8: 0,
        SQLiteTimestampParseStatus.MALFORMED_TEXT: 1,
        SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES: 0,
        SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE: 1,
        SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS: 0,
    }
    assert tuple(
        (item.utc_offset_microseconds, item.count)
        for item in aggregate.source_utc_offset_frequencies
    ) == ((-1, 1), (0, 3), (1, 1), (19_800_000_000, 1))
    assert tuple(
        (item.fractional_precision, item.count)
        for item in aggregate.source_fractional_precision_frequencies
    ) == ((0, 3), (6, 4))
    assert aggregate.projectable_epoch_min == MIN_EPOCH_MICROSECONDS
    assert aggregate.projectable_epoch_max == MAX_EPOCH_MICROSECONDS


def test_duplicate_instants_count_twice_without_grouping_or_deduplication_fields() -> None:
    declaration = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS[0].columns[0]
    exact_epoch = 1_753_434_015_123_456
    duplicate_instant_summary = _column_summary(
        declaration,
        candidate_counts={
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT: 2,
        },
        parse_counts={
            SQLiteTimestampParseStatus.PARSED_AWARE_TEXT: 2,
        },
        offsets=((0, 1), (19_800_000_000, 1)),
        precisions=((6, 2),),
        epoch_min=exact_epoch,
        epoch_max=exact_epoch,
    )

    aggregate = census_bundle._aggregate_sources(
        _internal_sources({0: {0: duplicate_instant_summary}})
    )

    assert aggregate.total_candidate_count == 2
    assert (
        _bundle_candidate_counts(aggregate)[
            SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT
        ]
        == 2
    )
    assert aggregate.projectable_epoch_min == exact_epoch
    assert aggregate.projectable_epoch_max == exact_epoch
    forbidden_tokens = ("row", "instant", "collision", "group", "merge", "quarantine", "dedup")
    for model in (
        SQLiteTimestampCandidateCensusBundleAggregate,
        SQLiteTimestampCandidateCensusBundleResult,
    ):
        assert {
            field_name
            for field_name in model.model_fields
            if any(token in field_name for token in forbidden_tokens)
        } == set()


def test_aggregate_contract_rejects_hostile_shape_count_frequency_and_extrema_tampering() -> None:
    aggregate = census_bundle._aggregate_sources(_mixed_sources())
    candidate_counts = aggregate.candidate_status_counts
    parse_counts = aggregate.source_parse_status_counts
    offsets = aggregate.source_utc_offset_frequencies
    precisions = aggregate.source_fractional_precision_frequencies
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
    shifted_parse_counts = tuple(
        item.model_copy(
            update={
                "count": (
                    item.count - 1
                    if item.status is SQLiteTimestampParseStatus.PARSED_AWARE_TEXT
                    else item.count + 1
                    if item.status is SQLiteTimestampParseStatus.MALFORMED_TEXT
                    else item.count
                )
            }
        )
        for item in parse_counts
    )
    invalid_updates: tuple[dict[str, object], ...] = (
        {"family_count": 7},
        {"table_count": 21},
        {"column_count": 36},
        {"total_candidate_count": aggregate.total_candidate_count + 1},
        {"total_candidate_count": True},
        {"candidate_status_counts": tuple(reversed(candidate_counts))},
        {"candidate_status_counts": candidate_counts[:-1]},
        {
            "candidate_status_counts": (
                candidate_counts[0],
                candidate_counts[0],
                *candidate_counts[2:],
            )
        },
        {"candidate_status_counts": shifted_candidate_counts},
        {"source_parse_status_counts": tuple(reversed(parse_counts))},
        {"source_parse_status_counts": parse_counts[:-1]},
        {
            "source_parse_status_counts": (
                parse_counts[0],
                parse_counts[0],
                *parse_counts[2:],
            )
        },
        {"source_parse_status_counts": shifted_parse_counts},
        {"source_utc_offset_frequencies": tuple(reversed(offsets))},
        {
            "source_utc_offset_frequencies": (
                offsets[0],
                offsets[0],
                *offsets[2:],
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
            "projectable_epoch_min": aggregate.projectable_epoch_max,
            "projectable_epoch_max": aggregate.projectable_epoch_min,
        },
        {"projectable_epoch_min": MIN_EPOCH_MICROSECONDS - 1},
        {"projectable_epoch_max": MAX_EPOCH_MICROSECONDS + 1},
    )

    for update in invalid_updates:
        tampered = aggregate.model_copy(update=update)
        with pytest.raises(ValidationError):
            SQLiteTimestampCandidateCensusBundleAggregate.model_validate(
                tampered,
                strict=True,
            )

    with pytest.raises(ValidationError, match="Extra inputs"):
        SQLiteTimestampCandidateCensusBundleAggregate.model_validate(
            {**aggregate.model_dump(mode="python"), "group_count": 1},
            strict=True,
        )
    with pytest.raises(ValidationError):
        aggregate.total_candidate_count = 0


def test_result_contract_recomputes_aggregate_instead_of_trusting_valid_shaped_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = _mixed_sources()
    aggregate = census_bundle._aggregate_sources(sources)
    assert aggregate.projectable_epoch_min is not None
    altered_aggregate = SQLiteTimestampCandidateCensusBundleAggregate.model_validate(
        aggregate.model_copy(update={"projectable_epoch_min": aggregate.projectable_epoch_min + 1}),
        strict=True,
    )

    def trust_safe_internal_fixtures(
        source_values: tuple[SQLiteTimestampCandidateCensusResult, ...],
    ) -> tuple[SQLiteTimestampCandidateCensusResult, ...]:
        return source_values

    monkeypatch.setattr(
        census_bundle,
        "_deeply_validated_sources",
        trust_safe_internal_fixtures,
    )
    result = SQLiteTimestampCandidateCensusBundleResult.model_construct(
        schema_version="1.0",
        sources=sources,
        plan=census_bundle._PINNED_BUNDLE_PLAN,
        aggregate=altered_aggregate,
    )

    with pytest.raises(ValueError, match="aggregate must exactly reconcile"):
        result.result_reconciles_exactly_to_sources()  # type: ignore[operator]


def test_source_sequence_rejects_missing_reordered_duplicate_and_shape_equivalent_families() -> (
    None
):
    sources = _internal_sources()
    census_bundle._require_exact_source_sequence(sources)
    invalid_sequences = (
        sources[:-1],
        (*sources, sources[-1]),
        tuple(reversed(sources)),
        (sources[1], sources[0], *sources[2:]),
        (sources[0], sources[0], *sources[2:]),
    )

    for invalid in invalid_sequences:
        with pytest.raises(SQLiteTimestampCandidateCensusBundleError) as caught:
            census_bundle._require_exact_source_sequence(invalid)
        assert caught.value.code is (
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE
        )


def test_public_boundary_requires_exact_builtin_tuple_and_exact_element_types_before_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incomplete = tuple(SQLiteTimestampCandidateCensusResult.model_construct() for _ in range(8))

    class SourceTuple(tuple[SQLiteTimestampCandidateCensusResult, ...]):
        pass

    class CensusResultSubclass(SQLiteTimestampCandidateCensusResult):
        pass

    subclass = CensusResultSubclass.model_construct()

    def unexpected_aggregation(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("invalid TASK-034 evidence reached bundle aggregation")

    monkeypatch.setattr(census_bundle, "_aggregate_sources", unexpected_aggregation)
    invalid_inputs: tuple[object, ...] = (
        list(incomplete),
        (item for item in incomplete),
        SourceTuple(incomplete),
        incomplete[:-1],
        (*incomplete, incomplete[-1]),
        (object(), *incomplete[1:]),
        (subclass, *incomplete[1:]),
    )
    for invalid in invalid_inputs:
        with pytest.raises(SQLiteTimestampCandidateCensusBundleError) as caught:
            build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence(
                invalid  # type: ignore[arg-type]
            )
        assert caught.value.code is (
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE
        )

    with pytest.raises(
        SQLiteTimestampCandidateCensusBundleError,
        match="deep TASK-034 contract validation",
    ) as caught:
        build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence(incomplete)
    assert (
        caught.value.code is SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE
    )


def test_deep_source_validation_rejects_model_construct_and_nested_tamper_before_aggregation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal = _internal_sources()
    missing = SQLiteTimestampCandidateCensusResult.model_construct()
    forged_plan = internal[0].plan.model_copy(
        update={"columns": tuple(reversed(internal[0].plan.columns))}
    )
    forged = internal[0].model_copy(update={"plan": forged_plan})
    invalid_source_sets = (
        (missing, *internal[1:]),
        internal,
        (forged, *internal[1:]),
    )

    def unexpected_aggregation(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("deep-invalid TASK-034 evidence reached aggregation")

    monkeypatch.setattr(census_bundle, "_aggregate_sources", unexpected_aggregation)
    for invalid_sources in invalid_source_sets:
        with pytest.raises(SQLiteTimestampCandidateCensusBundleError) as caught:
            build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence(invalid_sources)
        assert caught.value.code is (
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE
        )


def test_bundle_result_public_shape_is_minimal_strict_and_frozen() -> None:
    assert tuple(SQLiteTimestampCandidateCensusBundleResult.model_fields) == (
        "schema_version",
        "sources",
        "plan",
        "aggregate",
    )
    shell = SQLiteTimestampCandidateCensusBundleResult.model_construct(
        schema_version="1.0",
        sources=(),
        plan=SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN,
        aggregate=None,
    )
    with pytest.raises(ValidationError):
        shell.plan = SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN
    with pytest.raises(ValidationError):
        SQLiteTimestampCandidateCensusBundleResult.model_validate(
            {
                "schema_version": "1.0",
                "sources": (),
                "plan": SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN,
                "aggregate": None,
                "write_path": "/tmp/bundle.json",
            },
            strict=True,
        )


def test_bundle_module_is_pure_and_has_no_runtime_consumer() -> None:
    tree = ast.parse(BUNDLE_MODULE_PATH.read_text(encoding="utf-8"))
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
        if path == BUNDLE_MODULE_PATH:
            continue
        if "sqlite_timestamp_candidate_census_bundle" in path.read_text(encoding="utf-8"):
            consumers.append(path)
    assert consumers == []
