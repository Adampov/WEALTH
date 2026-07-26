"""Pure per-column census evidence over one exact TASK-033 result.

The census is family-scoped, retains the complete candidate result, and performs no I/O,
serialization, collision grouping, replacement selection, or runtime work.
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
    MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_TARGETS,
)
from wealth.domain.sqlite_timestamp_candidate import (
    _PINNED_CANDIDATE_PLANS,
    SQLiteTimestampCanonicalCandidateOutcome,
    SQLiteTimestampCanonicalCandidatePlan,
    SQLiteTimestampCanonicalCandidateResult,
    SQLiteTimestampCanonicalCandidateStatus,
)
from wealth.domain.sqlite_timestamp_parse import (
    SQLiteTimestampColumnParsePlan,
    SQLiteTimestampParseStatus,
)

__all__ = [
    "SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS",
    "SQLiteTimestampCandidateCensusColumnPlan",
    "SQLiteTimestampCandidateCensusError",
    "SQLiteTimestampCandidateCensusErrorCode",
    "SQLiteTimestampCandidateCensusPlan",
    "SQLiteTimestampCandidateCensusResult",
    "SQLiteTimestampCandidateColumnCensus",
    "SQLiteTimestampCandidateStatusCount",
    "SQLiteTimestampFractionalPrecisionFrequency",
    "SQLiteTimestampParseStatusCount",
    "SQLiteTimestampUtcOffsetFrequency",
    "build_synthetic_sqlite_timestamp_candidate_census_evidence",
]

ContractVersion = Literal["1.0"]
CensusKind = Literal["per_declared_column_candidate_census"]
SQLiteIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
]
EpochMicroseconds = Annotated[
    int,
    Field(ge=MIN_EPOCH_MICROSECONDS, le=MAX_EPOCH_MICROSECONDS),
]
FractionalPrecision = Literal[0, 6]
_MAX_OFFSET_MICROSECONDS: Final[int] = 86_400_000_000
_MAX_CENSUS_COLUMNS_PER_RESULT: Final[int] = (
    MAX_SQLITE_TIMESTAMP_TARGETS * MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET
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


class SQLiteTimestampCandidateCensusErrorCode(StrEnum):
    """Fail-closed errors for an invalid top-level TASK-033 evidence boundary."""

    INVALID_SOURCE_EVIDENCE = "invalid_source_evidence"
    UNREGISTERED_PLAN = "unregistered_plan"


class SQLiteTimestampCandidateCensusError(ValueError):
    """Reject evidence that is not exactly one registered TASK-033 result."""

    def __init__(
        self,
        code: SQLiteTimestampCandidateCensusErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteTimestampCandidateCensusColumnPlan(_StrictContract):
    """One ordered census declaration tied to an exact TASK-032 column plan."""

    summary_ordinal: Annotated[int, Field(ge=0, lt=_MAX_CENSUS_COLUMNS_PER_RESULT)]
    target_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_TARGETS)]
    column_ordinal: Annotated[
        int,
        Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET),
    ]
    table_name: SQLiteIdentifier
    source_column_plan: SQLiteTimestampColumnParsePlan


class SQLiteTimestampCandidateCensusPlan(_StrictContract):
    """One exact family-scoped census plan over a reviewed TASK-033 plan."""

    schema_version: ContractVersion = "1.0"
    census_kind: CensusKind = "per_declared_column_candidate_census"
    source_plan: SQLiteTimestampCanonicalCandidatePlan
    columns: Annotated[
        tuple[SQLiteTimestampCandidateCensusColumnPlan, ...],
        Field(min_length=1, max_length=_MAX_CENSUS_COLUMNS_PER_RESULT),
    ]
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
        """Reject altered source plans, status semantics, or column declarations."""

        try:
            revalidated_source = SQLiteTimestampCanonicalCandidatePlan.model_validate(
                self.source_plan.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-033 plan must pass deep validation") from exc
        if revalidated_source != self.source_plan:
            raise ValueError("source TASK-033 plan changed during deep validation")
        if self.source_plan not in _PINNED_CANDIDATE_PLANS:
            raise ValueError("source TASK-033 plan must equal one reviewed immutable plan")
        if set(_CANDIDATE_STATUS_ORDER) != set(SQLiteTimestampCanonicalCandidateStatus) or set(
            _PARSE_STATUS_ORDER
        ) != set(SQLiteTimestampParseStatus):
            raise AssertionError("census status vectors must cover the reviewed enums exactly")
        if (
            self.candidate_status_order != _CANDIDATE_STATUS_ORDER
            or self.source_parse_status_order != _PARSE_STATUS_ORDER
            or self.projected_candidate_statuses != _PROJECTED_CANDIDATE_STATUSES
            or self.offset_frequency_source_statuses != _OFFSET_FREQUENCY_SOURCE_STATUSES
            or self.precision_frequency_source_statuses != _PRECISION_FREQUENCY_SOURCE_STATUSES
        ):
            raise ValueError("census plan must preserve the exact status and frequency semantics")
        if self.columns != _column_declarations(self.source_plan):
            raise ValueError("census columns must exactly flatten the source-family declarations")
        return self


class SQLiteTimestampCandidateStatusCount(_StrictContract):
    """One exhaustive candidate-status bucket, including zero counts."""

    status: SQLiteTimestampCanonicalCandidateStatus
    count: Annotated[int, Field(ge=0, le=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]


class SQLiteTimestampParseStatusCount(_StrictContract):
    """One exhaustive source-parse-status bucket, including zero counts."""

    status: SQLiteTimestampParseStatus
    count: Annotated[int, Field(ge=0, le=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]


class SQLiteTimestampUtcOffsetFrequency(_StrictContract):
    """One observed aware-text source offset and its positive occurrence count."""

    utc_offset_microseconds: Annotated[
        int,
        Field(gt=-_MAX_OFFSET_MICROSECONDS, lt=_MAX_OFFSET_MICROSECONDS),
    ]
    count: Annotated[int, Field(ge=1, le=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]


class SQLiteTimestampFractionalPrecisionFrequency(_StrictContract):
    """One observed parsed-text precision and its positive occurrence count."""

    fractional_precision: FractionalPrecision
    count: Annotated[int, Field(ge=1, le=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]

    @field_validator("fractional_precision", mode="before")
    @classmethod
    def precision_is_an_exact_supported_integer(cls, value: object) -> object:
        """Prevent Literal coercion from accepting booleans or integral floats."""

        if type(value) is not int or value not in (0, 6):
            raise ValueError("fractional precision must be the exact integer 0 or 6")
        return value


class SQLiteTimestampCandidateColumnCensus(_StrictContract):
    """One internally reconciled census for an exact declared timestamp column."""

    schema_version: ContractVersion = "1.0"
    declaration: SQLiteTimestampCandidateCensusColumnPlan
    total_candidate_count: Annotated[
        int,
        Field(ge=0, le=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET),
    ]
    candidate_status_counts: Annotated[
        tuple[SQLiteTimestampCandidateStatusCount, ...],
        Field(min_length=4, max_length=4),
    ]
    source_parse_status_counts: Annotated[
        tuple[SQLiteTimestampParseStatusCount, ...],
        Field(min_length=10, max_length=10),
    ]
    source_utc_offset_frequencies: Annotated[
        tuple[SQLiteTimestampUtcOffsetFrequency, ...],
        Field(max_length=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET),
    ]
    source_fractional_precision_frequencies: Annotated[
        tuple[SQLiteTimestampFractionalPrecisionFrequency, ...],
        Field(max_length=2),
    ]
    projectable_epoch_min: EpochMicroseconds | None = None
    projectable_epoch_max: EpochMicroseconds | None = None

    @model_validator(mode="after")
    def counts_and_ranges_are_internally_consistent(self) -> Self:
        """Require complete ordered buckets, exact totals, and paired epoch extrema."""

        if tuple(item.status for item in self.candidate_status_counts) != (_CANDIDATE_STATUS_ORDER):
            raise ValueError("candidate status counts must be exhaustive and ordered")
        if tuple(item.status for item in self.source_parse_status_counts) != (_PARSE_STATUS_ORDER):
            raise ValueError("source parse status counts must be exhaustive and ordered")
        candidate_counts = {item.status: item.count for item in self.candidate_status_counts}
        parse_counts = {item.status: item.count for item in self.source_parse_status_counts}
        if (
            sum(candidate_counts.values()) != self.total_candidate_count
            or sum(parse_counts.values()) != self.total_candidate_count
        ):
            raise ValueError("both exhaustive status vectors must sum to the total")
        projected_count = sum(candidate_counts[status] for status in _PROJECTED_CANDIDATE_STATUSES)
        if (
            projected_count
            + candidate_counts[SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE]
            + candidate_counts[SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW]
            != self.total_candidate_count
        ):
            raise ValueError("candidate dispositions must reconcile exactly")
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
            raise ValueError("candidate and source-parse status counts must agree")

        offsets = tuple(
            frequency.utc_offset_microseconds for frequency in self.source_utc_offset_frequencies
        )
        precisions = tuple(
            frequency.fractional_precision
            for frequency in self.source_fractional_precision_frequencies
        )
        if offsets != tuple(sorted(set(offsets))):
            raise ValueError("source offset frequencies must be unique and ascending")
        if precisions != tuple(sorted(set(precisions))):
            raise ValueError("source precision frequencies must be unique and ascending")
        if sum(frequency.count for frequency in self.source_utc_offset_frequencies) != sum(
            parse_counts[status] for status in _OFFSET_FREQUENCY_SOURCE_STATUSES
        ):
            raise ValueError("source offset frequencies must reconcile eligible text outcomes")
        if sum(
            frequency.count for frequency in self.source_fractional_precision_frequencies
        ) != sum(parse_counts[status] for status in _PRECISION_FREQUENCY_SOURCE_STATUSES):
            raise ValueError("source precision frequencies must reconcile parsed text outcomes")

        extrema_present = (
            self.projectable_epoch_min is not None,
            self.projectable_epoch_max is not None,
        )
        if extrema_present not in {(False, False), (True, True)}:
            raise ValueError("projectable epoch extrema must be both present or both absent")
        if projected_count == 0 and extrema_present != (False, False):
            raise ValueError("an empty projectable population cannot have epoch extrema")
        if projected_count > 0 and extrema_present != (True, True):
            raise ValueError("a projectable population requires both epoch extrema")
        if (
            self.projectable_epoch_min is not None
            and self.projectable_epoch_max is not None
            and self.projectable_epoch_min > self.projectable_epoch_max
        ):
            raise ValueError("projectable epoch minimum may not exceed its maximum")
        return self


class SQLiteTimestampCandidateCensusResult(_StrictContract):
    """One exact family-scoped census linked to the complete TASK-033 result."""

    schema_version: ContractVersion = "1.0"
    source: SQLiteTimestampCanonicalCandidateResult
    plan: SQLiteTimestampCandidateCensusPlan
    columns: Annotated[
        tuple[SQLiteTimestampCandidateColumnCensus, ...],
        Field(min_length=1, max_length=_MAX_CENSUS_COLUMNS_PER_RESULT),
    ]

    @model_validator(mode="after")
    def census_reconciles_exactly_to_source(self) -> Self:
        """Deeply revalidate and recompute every summary from unchanged source evidence."""

        try:
            revalidated_source = SQLiteTimestampCanonicalCandidateResult.model_validate(
                self.source.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-033 result must pass deep validation") from exc
        if revalidated_source != self.source:
            raise ValueError("source TASK-033 result changed during deep validation")
        if self.plan != _pinned_plan_for_source(self.source):
            raise ValueError("census plan must equal the reviewed immutable declaration")
        if self.plan.source_plan != self.source.plan:
            raise ValueError("census plan must preserve the exact TASK-033 candidate plan")
        if self.columns != _summaries(self.source, self.plan):
            raise ValueError("column censuses must exactly reconcile the TASK-033 result")
        return self


def _column_declarations(
    source_plan: SQLiteTimestampCanonicalCandidatePlan,
) -> tuple[SQLiteTimestampCandidateCensusColumnPlan, ...]:
    declarations: list[SQLiteTimestampCandidateCensusColumnPlan] = []
    for target_ordinal, target in enumerate(source_plan.source_plan.targets):
        for column_ordinal, column in enumerate(target.columns):
            declarations.append(
                SQLiteTimestampCandidateCensusColumnPlan(
                    summary_ordinal=len(declarations),
                    target_ordinal=target_ordinal,
                    column_ordinal=column_ordinal,
                    table_name=target.table_name,
                    source_column_plan=column,
                )
            )
    return tuple(declarations)


_PINNED_CENSUS_PLANS: Final[tuple[SQLiteTimestampCandidateCensusPlan, ...]] = tuple(
    SQLiteTimestampCandidateCensusPlan(
        source_plan=source_plan,
        columns=_column_declarations(source_plan),
    )
    for source_plan in _PINNED_CANDIDATE_PLANS
)
SQLITE_TIMESTAMP_CANDIDATE_CENSUS_PLANS: tuple[SQLiteTimestampCandidateCensusPlan, ...] = (
    _PINNED_CENSUS_PLANS
)


def _status_counts(
    candidates: tuple[SQLiteTimestampCanonicalCandidateOutcome, ...],
) -> tuple[
    tuple[SQLiteTimestampCandidateStatusCount, ...],
    tuple[SQLiteTimestampParseStatusCount, ...],
]:
    candidate_counter = Counter(candidate.status for candidate in candidates)
    parse_counter = Counter(candidate.source_outcome.status for candidate in candidates)
    return (
        tuple(
            SQLiteTimestampCandidateStatusCount(
                status=status,
                count=candidate_counter[status],
            )
            for status in _CANDIDATE_STATUS_ORDER
        ),
        tuple(
            SQLiteTimestampParseStatusCount(
                status=status,
                count=parse_counter[status],
            )
            for status in _PARSE_STATUS_ORDER
        ),
    )


def _summarize_column(
    declaration: SQLiteTimestampCandidateCensusColumnPlan,
    candidates: tuple[SQLiteTimestampCanonicalCandidateOutcome, ...],
) -> SQLiteTimestampCandidateColumnCensus:
    candidate_status_counts, parse_status_counts = _status_counts(candidates)
    offset_counter = Counter(
        candidate.source_outcome.utc_offset_microseconds
        for candidate in candidates
        if candidate.source_outcome.status in _OFFSET_FREQUENCY_SOURCE_STATUSES
    )
    precision_counter = Counter(
        candidate.source_outcome.fractional_precision
        for candidate in candidates
        if candidate.source_outcome.status in _PRECISION_FREQUENCY_SOURCE_STATUSES
    )
    if None in offset_counter or None in precision_counter:
        raise AssertionError("eligible TASK-032 outcomes always contain frequency evidence")
    epoch_values = tuple(
        candidate.epoch_microseconds
        for candidate in candidates
        if candidate.status in _PROJECTED_CANDIDATE_STATUSES
    )
    if any(value is None for value in epoch_values):
        raise AssertionError("projected TASK-033 candidates always contain epoch evidence")
    projected_epoch_values = tuple(value for value in epoch_values if value is not None)
    return SQLiteTimestampCandidateColumnCensus(
        declaration=declaration,
        total_candidate_count=len(candidates),
        candidate_status_counts=candidate_status_counts,
        source_parse_status_counts=parse_status_counts,
        source_utc_offset_frequencies=tuple(
            SQLiteTimestampUtcOffsetFrequency(
                utc_offset_microseconds=offset,
                count=offset_counter[offset],
            )
            for offset in sorted(value for value in offset_counter if value is not None)
        ),
        source_fractional_precision_frequencies=tuple(
            SQLiteTimestampFractionalPrecisionFrequency(
                fractional_precision=precision,
                count=precision_counter[precision],
            )
            for precision in sorted(value for value in precision_counter if value is not None)
        ),
        projectable_epoch_min=(min(projected_epoch_values) if projected_epoch_values else None),
        projectable_epoch_max=(max(projected_epoch_values) if projected_epoch_values else None),
    )


def _summaries(
    source: SQLiteTimestampCanonicalCandidateResult,
    plan: SQLiteTimestampCandidateCensusPlan,
) -> tuple[SQLiteTimestampCandidateColumnCensus, ...]:
    summaries: list[SQLiteTimestampCandidateColumnCensus] = []
    declaration_index = 0
    for target_ordinal, (target, source_table) in enumerate(
        zip(
            plan.source_plan.source_plan.targets,
            source.tables,
            strict=True,
        )
    ):
        if (
            source_table.target_ordinal != target_ordinal
            or source_table.table_name != target.table_name
        ):
            raise AssertionError("validated TASK-033 table linkage must match its plan")
        for column_ordinal, column in enumerate(target.columns):
            declaration = plan.columns[declaration_index]
            if (
                declaration.target_ordinal != target_ordinal
                or declaration.column_ordinal != column_ordinal
                or declaration.table_name != target.table_name
                or declaration.source_column_plan != column
            ):
                raise AssertionError("validated census declaration must match its plan")
            candidates = tuple(row.candidates[column_ordinal] for row in source_table.rows)
            summaries.append(_summarize_column(declaration, candidates))
            declaration_index += 1
    if declaration_index != len(plan.columns):
        raise AssertionError("every census declaration must be visited exactly once")
    return tuple(summaries)


def _pinned_plan_for_source(
    source: SQLiteTimestampCanonicalCandidateResult,
) -> SQLiteTimestampCandidateCensusPlan:
    matches = tuple(
        plan
        for plan in _PINNED_CENSUS_PLANS
        if plan.source_plan.source_plan.extraction_plan.family
        is source.plan.source_plan.extraction_plan.family
        and plan.source_plan.source_plan.extraction_plan.layout_version
        == source.plan.source_plan.extraction_plan.layout_version
    )
    if len(matches) != 1:
        raise SQLiteTimestampCandidateCensusError(
            SQLiteTimestampCandidateCensusErrorCode.UNREGISTERED_PLAN,
            "the source family must have exactly one candidate census plan",
        )
    plan = matches[0]
    if plan.source_plan != source.plan:
        raise SQLiteTimestampCandidateCensusError(
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE,
            "the TASK-033 candidate plan differs from the reviewed census declaration",
        )
    return plan


def _validated_source(
    source: SQLiteTimestampCanonicalCandidateResult,
) -> SQLiteTimestampCanonicalCandidateResult:
    try:
        revalidated = SQLiteTimestampCanonicalCandidateResult.model_validate(
            source.model_dump(mode="python"),
            strict=True,
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise SQLiteTimestampCandidateCensusError(
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must pass deep TASK-033 contract validation",
        ) from exc
    if revalidated != source:
        raise SQLiteTimestampCandidateCensusError(
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE,
            "source changed during deep TASK-033 contract validation",
        )
    return source


def build_synthetic_sqlite_timestamp_candidate_census_evidence(
    source: SQLiteTimestampCanonicalCandidateResult,
) -> SQLiteTimestampCandidateCensusResult:
    """Build one family-scoped census from exact TASK-033 evidence without I/O."""

    if type(source) is not SQLiteTimestampCanonicalCandidateResult:
        raise SQLiteTimestampCandidateCensusError(
            SQLiteTimestampCandidateCensusErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must be one exact SQLiteTimestampCanonicalCandidateResult",
        )
    source = _validated_source(source)
    plan = _pinned_plan_for_source(source)
    return SQLiteTimestampCandidateCensusResult(
        source=source,
        plan=plan,
        columns=_summaries(source, plan),
    )
