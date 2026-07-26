"""Pure all-family reconciliation over eight exact TASK-034 census results.

The bundle retains every family result unchanged and aggregates only existing census counts,
frequency keys, and projectable epoch extrema. Bundle aggregation never directly traverses rows
or candidate outcomes; deep source-contract validation remains transitive.
"""

from collections import Counter
from enum import StrEnum
from typing import Annotated, Final, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    SQLiteStoreFamily,
)
from wealth.domain.sqlite_timestamp_candidate import (
    SQLiteTimestampCanonicalCandidateStatus,
)
from wealth.domain.sqlite_timestamp_candidate_census import (
    _PINNED_CENSUS_PLANS,
    SQLiteTimestampCandidateCensusPlan,
    SQLiteTimestampCandidateCensusResult,
)
from wealth.domain.sqlite_timestamp_parse import SQLiteTimestampParseStatus

__all__ = [
    "SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN",
    "SQLiteTimestampBundleCandidateStatusCount",
    "SQLiteTimestampBundleFractionalPrecisionFrequency",
    "SQLiteTimestampBundleParseStatusCount",
    "SQLiteTimestampBundleUtcOffsetFrequency",
    "SQLiteTimestampCandidateCensusBundleAggregate",
    "SQLiteTimestampCandidateCensusBundleError",
    "SQLiteTimestampCandidateCensusBundleErrorCode",
    "SQLiteTimestampCandidateCensusBundlePlan",
    "SQLiteTimestampCandidateCensusBundleResult",
    "build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence",
]

ContractVersion = Literal["1.0"]
BundleKind = Literal["all_family_candidate_census_bundle"]
EpochMicroseconds = Annotated[
    int,
    Field(ge=MIN_EPOCH_MICROSECONDS, le=MAX_EPOCH_MICROSECONDS),
]
FractionalPrecision = Literal[0, 6]
_EXPECTED_FAMILY_COUNT: Final[int] = 8
_EXPECTED_TABLE_COUNT: Final[int] = 20
_EXPECTED_COLUMN_COUNT: Final[int] = 37
_MAX_BUNDLE_CANDIDATES: Final[int] = _EXPECTED_COLUMN_COUNT * MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET
_MAX_OFFSET_MICROSECONDS: Final[int] = 86_400_000_000
_FAMILY_ORDER: Final[tuple[SQLiteStoreFamily, ...]] = (
    SQLiteStoreFamily.MARKET,
    SQLiteStoreFamily.ORDER_FLOW,
    SQLiteStoreFamily.HISTORICAL_COLLECTION,
    SQLiteStoreFamily.CONTINUOUS_COLLECTION,
    SQLiteStoreFamily.COLLECTOR_SERVICE,
    SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
    SQLiteStoreFamily.RATE_BUDGET,
    SQLiteStoreFamily.RECONCILIATION,
)
_CANDIDATE_STATUS_ORDER: Final[tuple[SQLiteTimestampCanonicalCandidateStatus, ...]] = (
    SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
    SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS,
    SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE,
    SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW,
)
_PARSE_STATUS_ORDER: Final[tuple[SQLiteTimestampParseStatus, ...]] = (
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
_PROJECTED_CANDIDATE_STATUSES: Final[tuple[SQLiteTimestampCanonicalCandidateStatus, ...]] = (
    SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT,
    SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS,
)
_OFFSET_FREQUENCY_SOURCE_STATUSES: Final[tuple[SQLiteTimestampParseStatus, ...]] = (
    SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
    SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
)
_PRECISION_FREQUENCY_SOURCE_STATUSES: Final[tuple[SQLiteTimestampParseStatus, ...]] = (
    SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
    SQLiteTimestampParseStatus.NAIVE_TEXT,
    SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
)


class _StrictContract(BaseModel):
    """Apply one immutable, strict, recursively revalidated contract boundary."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class SQLiteTimestampCandidateCensusBundleErrorCode(StrEnum):
    """Fail-closed errors for an invalid all-family TASK-034 source boundary."""

    INVALID_SOURCE_EVIDENCE = "invalid_source_evidence"


class SQLiteTimestampCandidateCensusBundleError(ValueError):
    """Reject inputs that are not the complete reviewed TASK-034 result set."""

    def __init__(
        self,
        code: SQLiteTimestampCandidateCensusBundleErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteTimestampCandidateCensusBundlePlan(_StrictContract):
    """One pinned all-family reconciliation declaration over TASK-034 plans."""

    schema_version: ContractVersion = "1.0"
    bundle_kind: BundleKind = "all_family_candidate_census_bundle"
    source_plans: Annotated[
        tuple[SQLiteTimestampCandidateCensusPlan, ...],
        Field(
            min_length=_EXPECTED_FAMILY_COUNT,
            max_length=_EXPECTED_FAMILY_COUNT,
        ),
    ]
    family_order: Annotated[
        tuple[SQLiteStoreFamily, ...],
        Field(
            min_length=_EXPECTED_FAMILY_COUNT,
            max_length=_EXPECTED_FAMILY_COUNT,
        ),
    ] = _FAMILY_ORDER
    expected_family_count: Annotated[
        int,
        Field(ge=_EXPECTED_FAMILY_COUNT, le=_EXPECTED_FAMILY_COUNT),
    ] = _EXPECTED_FAMILY_COUNT
    expected_table_count: Annotated[
        int,
        Field(ge=_EXPECTED_TABLE_COUNT, le=_EXPECTED_TABLE_COUNT),
    ] = _EXPECTED_TABLE_COUNT
    expected_column_count: Annotated[
        int,
        Field(ge=_EXPECTED_COLUMN_COUNT, le=_EXPECTED_COLUMN_COUNT),
    ] = _EXPECTED_COLUMN_COUNT
    candidate_status_order: Annotated[
        tuple[SQLiteTimestampCanonicalCandidateStatus, ...],
        Field(min_length=4, max_length=4),
    ] = _CANDIDATE_STATUS_ORDER
    source_parse_status_order: Annotated[
        tuple[SQLiteTimestampParseStatus, ...],
        Field(min_length=10, max_length=10),
    ] = _PARSE_STATUS_ORDER
    projected_candidate_statuses: Annotated[
        tuple[SQLiteTimestampCanonicalCandidateStatus, ...],
        Field(min_length=2, max_length=2),
    ] = _PROJECTED_CANDIDATE_STATUSES
    offset_frequency_source_statuses: Annotated[
        tuple[SQLiteTimestampParseStatus, ...],
        Field(min_length=2, max_length=2),
    ] = _OFFSET_FREQUENCY_SOURCE_STATUSES
    precision_frequency_source_statuses: Annotated[
        tuple[SQLiteTimestampParseStatus, ...],
        Field(min_length=3, max_length=3),
    ] = _PRECISION_FREQUENCY_SOURCE_STATUSES

    @model_validator(mode="after")
    def declaration_is_exact_and_reviewed(self) -> Self:
        """Reject altered plans, family order, shape, or aggregate semantics."""

        for source_plan in self.source_plans:
            try:
                revalidated = SQLiteTimestampCandidateCensusPlan.model_validate(
                    source_plan.model_dump(mode="python"),
                    strict=True,
                )
            except (TypeError, ValueError, ValidationError) as exc:
                raise ValueError("every source TASK-034 plan must pass deep validation") from exc
            if revalidated != source_plan:
                raise ValueError("a source TASK-034 plan changed during deep validation")
        if self.source_plans != _PINNED_CENSUS_PLANS:
            raise ValueError("bundle source plans must equal the complete reviewed plan sequence")
        if (
            set(_FAMILY_ORDER) != set(SQLiteStoreFamily)
            or set(_CANDIDATE_STATUS_ORDER) != set(SQLiteTimestampCanonicalCandidateStatus)
            or set(_PARSE_STATUS_ORDER) != set(SQLiteTimestampParseStatus)
        ):
            raise AssertionError(
                "bundle v1 family and status vectors must cover reviewed enums exactly"
            )
        plan_families = tuple(
            plan.source_plan.source_plan.extraction_plan.family for plan in self.source_plans
        )
        table_count = sum(len(plan.source_plan.source_plan.targets) for plan in self.source_plans)
        column_count = sum(len(plan.columns) for plan in self.source_plans)
        if (
            self.family_order != _FAMILY_ORDER
            or plan_families != _FAMILY_ORDER
            or self.expected_family_count != _EXPECTED_FAMILY_COUNT
            or self.expected_table_count != _EXPECTED_TABLE_COUNT
            or self.expected_column_count != _EXPECTED_COLUMN_COUNT
            or len(self.source_plans) != self.expected_family_count
            or table_count != self.expected_table_count
            or column_count != self.expected_column_count
        ):
            raise ValueError("bundle plan must preserve the exact reviewed 8/20/37 declarations")
        if (
            self.candidate_status_order != _CANDIDATE_STATUS_ORDER
            or self.source_parse_status_order != _PARSE_STATUS_ORDER
            or self.projected_candidate_statuses != _PROJECTED_CANDIDATE_STATUSES
            or self.offset_frequency_source_statuses != _OFFSET_FREQUENCY_SOURCE_STATUSES
            or self.precision_frequency_source_statuses != _PRECISION_FREQUENCY_SOURCE_STATUSES
        ):
            raise ValueError("bundle plan must preserve exact status and frequency semantics")
        return self


class SQLiteTimestampBundleCandidateStatusCount(_StrictContract):
    """One exhaustive bundle-level candidate-status bucket."""

    status: SQLiteTimestampCanonicalCandidateStatus
    count: Annotated[int, Field(ge=0, le=_MAX_BUNDLE_CANDIDATES)]


class SQLiteTimestampBundleParseStatusCount(_StrictContract):
    """One exhaustive bundle-level source-parse-status bucket."""

    status: SQLiteTimestampParseStatus
    count: Annotated[int, Field(ge=0, le=_MAX_BUNDLE_CANDIDATES)]


class SQLiteTimestampBundleUtcOffsetFrequency(_StrictContract):
    """One observed source offset and its all-family occurrence count."""

    utc_offset_microseconds: Annotated[
        int,
        Field(gt=-_MAX_OFFSET_MICROSECONDS, lt=_MAX_OFFSET_MICROSECONDS),
    ]
    count: Annotated[int, Field(ge=1, le=_MAX_BUNDLE_CANDIDATES)]


class SQLiteTimestampBundleFractionalPrecisionFrequency(_StrictContract):
    """One observed text precision and its all-family occurrence count."""

    fractional_precision: FractionalPrecision
    count: Annotated[int, Field(ge=1, le=_MAX_BUNDLE_CANDIDATES)]

    @field_validator("fractional_precision", mode="before")
    @classmethod
    def precision_is_an_exact_supported_integer(cls, value: object) -> object:
        """Prevent Literal coercion from accepting booleans or integral floats."""

        if type(value) is not int or value not in (0, 6):
            raise ValueError("fractional precision must be the exact integer 0 or 6")
        return value


class SQLiteTimestampCandidateCensusBundleAggregate(_StrictContract):
    """One exact global aggregate over the complete family census sequence."""

    schema_version: ContractVersion = "1.0"
    family_count: Annotated[
        int,
        Field(ge=_EXPECTED_FAMILY_COUNT, le=_EXPECTED_FAMILY_COUNT),
    ]
    table_count: Annotated[
        int,
        Field(ge=_EXPECTED_TABLE_COUNT, le=_EXPECTED_TABLE_COUNT),
    ]
    column_count: Annotated[
        int,
        Field(ge=_EXPECTED_COLUMN_COUNT, le=_EXPECTED_COLUMN_COUNT),
    ]
    total_candidate_count: Annotated[
        int,
        Field(ge=0, le=_MAX_BUNDLE_CANDIDATES),
    ]
    candidate_status_counts: Annotated[
        tuple[SQLiteTimestampBundleCandidateStatusCount, ...],
        Field(min_length=4, max_length=4),
    ]
    source_parse_status_counts: Annotated[
        tuple[SQLiteTimestampBundleParseStatusCount, ...],
        Field(min_length=10, max_length=10),
    ]
    source_utc_offset_frequencies: Annotated[
        tuple[SQLiteTimestampBundleUtcOffsetFrequency, ...],
        Field(max_length=_MAX_BUNDLE_CANDIDATES),
    ]
    source_fractional_precision_frequencies: Annotated[
        tuple[SQLiteTimestampBundleFractionalPrecisionFrequency, ...],
        Field(max_length=2),
    ]
    projectable_epoch_min: EpochMicroseconds | None = None
    projectable_epoch_max: EpochMicroseconds | None = None

    @model_validator(mode="after")
    def aggregate_is_internally_consistent(self) -> Self:
        """Require exact shape, status equations, frequencies, and extrema."""

        if (
            self.family_count != _EXPECTED_FAMILY_COUNT
            or self.table_count != _EXPECTED_TABLE_COUNT
            or self.column_count != _EXPECTED_COLUMN_COUNT
        ):
            raise ValueError("bundle aggregate must preserve the exact reviewed 8/20/37 shape")
        if tuple(item.status for item in self.candidate_status_counts) != _CANDIDATE_STATUS_ORDER:
            raise ValueError("candidate status counts must be exhaustive and ordered")
        if tuple(item.status for item in self.source_parse_status_counts) != _PARSE_STATUS_ORDER:
            raise ValueError("source parse status counts must be exhaustive and ordered")
        candidate_counts = {item.status: item.count for item in self.candidate_status_counts}
        parse_counts = {item.status: item.count for item in self.source_parse_status_counts}
        if (
            sum(candidate_counts.values()) != self.total_candidate_count
            or sum(parse_counts.values()) != self.total_candidate_count
        ):
            raise ValueError("both exhaustive status vectors must sum to the bundle total")
        projected_count = sum(candidate_counts[status] for status in _PROJECTED_CANDIDATE_STATUSES)
        if (
            candidate_counts[SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT]
            + candidate_counts[SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW]
            != parse_counts[SQLiteTimestampParseStatus.PARSED_AWARE_TEXT]
            or candidate_counts[
                SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
            ]
            != parse_counts[SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS]
            or candidate_counts[SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE]
            != sum(
                parse_counts[status]
                for status in _PARSE_STATUS_ORDER
                if status
                not in {
                    SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
                    SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS,
                }
            )
        ):
            raise ValueError("bundle candidate and source-parse status counts must agree")

        offsets = tuple(
            frequency.utc_offset_microseconds for frequency in self.source_utc_offset_frequencies
        )
        precisions = tuple(
            frequency.fractional_precision
            for frequency in self.source_fractional_precision_frequencies
        )
        if offsets != tuple(sorted(set(offsets))):
            raise ValueError("bundle source offset frequencies must be unique and ascending")
        if precisions != tuple(sorted(set(precisions))):
            raise ValueError("bundle source precision frequencies must be unique and ascending")
        if sum(frequency.count for frequency in self.source_utc_offset_frequencies) != sum(
            parse_counts[status] for status in _OFFSET_FREQUENCY_SOURCE_STATUSES
        ):
            raise ValueError(
                "bundle source offset frequencies must reconcile eligible text outcomes"
            )
        if sum(
            frequency.count for frequency in self.source_fractional_precision_frequencies
        ) != sum(parse_counts[status] for status in _PRECISION_FREQUENCY_SOURCE_STATUSES):
            raise ValueError(
                "bundle source precision frequencies must reconcile parsed text outcomes"
            )

        extrema_present = (
            self.projectable_epoch_min is not None,
            self.projectable_epoch_max is not None,
        )
        if extrema_present not in {(False, False), (True, True)}:
            raise ValueError("bundle projectable epoch extrema must be both present or both absent")
        if projected_count == 0 and extrema_present != (False, False):
            raise ValueError("an empty projectable bundle cannot have epoch extrema")
        if projected_count > 0 and extrema_present != (True, True):
            raise ValueError("a projectable bundle requires both epoch extrema")
        if (
            self.projectable_epoch_min is not None
            and self.projectable_epoch_max is not None
            and self.projectable_epoch_min > self.projectable_epoch_max
        ):
            raise ValueError("bundle projectable epoch minimum may not exceed its maximum")
        return self


class SQLiteTimestampCandidateCensusBundleResult(_StrictContract):
    """One exact reconciliation retaining all eight TASK-034 family results."""

    schema_version: ContractVersion = "1.0"
    sources: Annotated[
        tuple[SQLiteTimestampCandidateCensusResult, ...],
        Field(
            min_length=_EXPECTED_FAMILY_COUNT,
            max_length=_EXPECTED_FAMILY_COUNT,
        ),
    ]
    plan: SQLiteTimestampCandidateCensusBundlePlan
    aggregate: SQLiteTimestampCandidateCensusBundleAggregate

    @model_validator(mode="after")
    def result_reconciles_exactly_to_sources(self) -> Self:
        """Deeply revalidate all sources and recompute the complete aggregate."""

        sources = _deeply_validated_sources(self.sources)
        _require_exact_source_sequence(sources)
        if self.plan != _PINNED_BUNDLE_PLAN:
            raise ValueError("bundle result plan must equal the reviewed immutable declaration")
        if tuple(source.plan for source in sources) != self.plan.source_plans:
            raise ValueError("bundle result must preserve the exact TASK-034 plan sequence")
        if self.aggregate != _aggregate_sources(sources):
            raise ValueError("bundle aggregate must exactly reconcile all TASK-034 sources")
        return self


_PINNED_BUNDLE_PLAN: Final[SQLiteTimestampCandidateCensusBundlePlan] = (
    SQLiteTimestampCandidateCensusBundlePlan(
        source_plans=_PINNED_CENSUS_PLANS,
    )
)
SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN = _PINNED_BUNDLE_PLAN


def _deeply_validated_sources(
    sources: tuple[SQLiteTimestampCandidateCensusResult, ...],
) -> tuple[SQLiteTimestampCandidateCensusResult, ...]:
    validated: list[SQLiteTimestampCandidateCensusResult] = []
    for source in sources:
        try:
            revalidated = SQLiteTimestampCandidateCensusResult.model_validate(
                source.model_dump(mode="python"),
                strict=True,
            )
        except (AttributeError, TypeError, ValueError, ValidationError) as exc:
            raise SQLiteTimestampCandidateCensusBundleError(
                SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE,
                "every source must pass deep TASK-034 contract validation",
            ) from exc
        if revalidated != source:
            raise SQLiteTimestampCandidateCensusBundleError(
                SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE,
                "a source changed during deep TASK-034 contract validation",
            )
        validated.append(source)
    return tuple(validated)


def _require_exact_source_sequence(
    sources: tuple[SQLiteTimestampCandidateCensusResult, ...],
) -> None:
    if len(sources) != _EXPECTED_FAMILY_COUNT:
        raise SQLiteTimestampCandidateCensusBundleError(
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE,
            "the bundle requires exactly eight TASK-034 family results",
        )
    source_plans = tuple(source.plan for source in sources)
    source_families = tuple(
        source.plan.source_plan.source_plan.extraction_plan.family for source in sources
    )
    table_count = sum(len(source.plan.source_plan.source_plan.targets) for source in sources)
    column_count = sum(len(source.columns) for source in sources)
    if (
        source_plans != _PINNED_CENSUS_PLANS
        or source_families != _FAMILY_ORDER
        or table_count != _EXPECTED_TABLE_COUNT
        or column_count != _EXPECTED_COLUMN_COUNT
    ):
        raise SQLiteTimestampCandidateCensusBundleError(
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE,
            "sources must be one exact result per reviewed family in canonical order",
        )


def _aggregate_sources(
    sources: tuple[SQLiteTimestampCandidateCensusResult, ...],
) -> SQLiteTimestampCandidateCensusBundleAggregate:
    candidate_counter: Counter[SQLiteTimestampCanonicalCandidateStatus] = Counter()
    parse_counter: Counter[SQLiteTimestampParseStatus] = Counter()
    offset_counter: Counter[int] = Counter()
    precision_counter: Counter[FractionalPrecision] = Counter()
    projectable_minima: list[int] = []
    projectable_maxima: list[int] = []
    total_candidate_count = 0

    for source in sources:
        for column in source.columns:
            total_candidate_count += column.total_candidate_count
            candidate_counter.update(
                {item.status: item.count for item in column.candidate_status_counts}
            )
            parse_counter.update(
                {item.status: item.count for item in column.source_parse_status_counts}
            )
            offset_counter.update(
                {
                    item.utc_offset_microseconds: item.count
                    for item in column.source_utc_offset_frequencies
                }
            )
            precision_counter.update(
                {
                    item.fractional_precision: item.count
                    for item in column.source_fractional_precision_frequencies
                }
            )
            if column.projectable_epoch_min is not None:
                projectable_minima.append(column.projectable_epoch_min)
            if column.projectable_epoch_max is not None:
                projectable_maxima.append(column.projectable_epoch_max)

    return SQLiteTimestampCandidateCensusBundleAggregate(
        family_count=len(sources),
        table_count=sum(len(source.plan.source_plan.source_plan.targets) for source in sources),
        column_count=sum(len(source.columns) for source in sources),
        total_candidate_count=total_candidate_count,
        candidate_status_counts=tuple(
            SQLiteTimestampBundleCandidateStatusCount(
                status=status,
                count=candidate_counter[status],
            )
            for status in _CANDIDATE_STATUS_ORDER
        ),
        source_parse_status_counts=tuple(
            SQLiteTimestampBundleParseStatusCount(
                status=status,
                count=parse_counter[status],
            )
            for status in _PARSE_STATUS_ORDER
        ),
        source_utc_offset_frequencies=tuple(
            SQLiteTimestampBundleUtcOffsetFrequency(
                utc_offset_microseconds=offset,
                count=offset_counter[offset],
            )
            for offset in sorted(offset_counter)
        ),
        source_fractional_precision_frequencies=tuple(
            SQLiteTimestampBundleFractionalPrecisionFrequency(
                fractional_precision=precision,
                count=precision_counter[precision],
            )
            for precision in sorted(precision_counter)
        ),
        projectable_epoch_min=(min(projectable_minima) if projectable_minima else None),
        projectable_epoch_max=(max(projectable_maxima) if projectable_maxima else None),
    )


def build_synthetic_sqlite_timestamp_candidate_census_bundle_evidence(
    sources: tuple[SQLiteTimestampCandidateCensusResult, ...],
) -> SQLiteTimestampCandidateCensusBundleResult:
    """Reconcile the exact all-family TASK-034 result set without I/O."""

    if (
        type(sources) is not tuple
        or len(sources) != _EXPECTED_FAMILY_COUNT
        or any(type(source) is not SQLiteTimestampCandidateCensusResult for source in sources)
    ):
        raise SQLiteTimestampCandidateCensusBundleError(
            SQLiteTimestampCandidateCensusBundleErrorCode.INVALID_SOURCE_EVIDENCE,
            "sources must be an exact built-in tuple of eight exact TASK-034 results",
        )
    sources = _deeply_validated_sources(sources)
    _require_exact_source_sequence(sources)
    return SQLiteTimestampCandidateCensusBundleResult(
        sources=sources,
        plan=_PINNED_BUNDLE_PLAN,
        aggregate=_aggregate_sources(sources),
    )
