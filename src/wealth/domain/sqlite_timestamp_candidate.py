"""Pure canonical-instant candidates from exact TASK-032 parse evidence.

This module performs no I/O and has no runtime consumer. It retains the complete TASK-032 result
and derives replacement-free candidate evidence only for already successful parse outcomes.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from wealth.domain.canonical_utc import (
    MAX_EPOCH_MICROSECONDS,
    MIN_EPOCH_MICROSECONDS,
    CanonicalUtcError,
    from_epoch_microseconds,
    normalize_aware_to_utc,
    parse_canonical_utc,
    serialize_canonical_utc,
    to_epoch_microseconds,
)
from wealth.domain.sqlite_preflight import (
    MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_KEY_COLUMNS,
    MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET,
    MAX_SQLITE_TIMESTAMP_TARGETS,
    SQLiteStorageClass,
    SQLiteTimestampCellEvidence,
)
from wealth.domain.sqlite_timestamp_parse import (
    _PINNED_PARSE_PLANS,
    SQLiteTimestampParseOutcome,
    SQLiteTimestampParsePlan,
    SQLiteTimestampParseResult,
    SQLiteTimestampParseStatus,
)

__all__ = [
    "SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS",
    "SQLiteTimestampCanonicalCandidateError",
    "SQLiteTimestampCanonicalCandidateErrorCode",
    "SQLiteTimestampCanonicalCandidateOutcome",
    "SQLiteTimestampCanonicalCandidatePlan",
    "SQLiteTimestampCanonicalCandidateResult",
    "SQLiteTimestampCanonicalCandidateRowEvidence",
    "SQLiteTimestampCanonicalCandidateStatus",
    "SQLiteTimestampCanonicalCandidateTableEvidence",
    "derive_synthetic_sqlite_timestamp_canonical_candidate_evidence",
]

ContractVersion = Literal["1.0"]
ProjectionKind = Literal["fixed_utc_text_epoch_microseconds"]
CanonicalText = Annotated[
    str,
    Field(
        min_length=27,
        max_length=27,
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
            r"[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$"
        ),
    ),
]
SQLiteIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"),
]
EpochMicroseconds = Annotated[
    int,
    Field(ge=MIN_EPOCH_MICROSECONDS, le=MAX_EPOCH_MICROSECONDS),
]
_PROJECTABLE_SOURCE_STATUSES: Final[tuple[SQLiteTimestampParseStatus, ...]] = (
    SQLiteTimestampParseStatus.PARSED_AWARE_TEXT,
    SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS,
)
_NONPROJECTABLE_SOURCE_STATUSES: Final[tuple[SQLiteTimestampParseStatus, ...]] = (
    SQLiteTimestampParseStatus.DECLARED_ABSENT,
    SQLiteTimestampParseStatus.NAIVE_TEXT,
    SQLiteTimestampParseStatus.OFFSET_POLICY_MISMATCH,
    SQLiteTimestampParseStatus.MALFORMED_UTF8,
    SQLiteTimestampParseStatus.MALFORMED_TEXT,
    SQLiteTimestampParseStatus.MALFORMED_EPOCH_BYTES,
    SQLiteTimestampParseStatus.EPOCH_OUT_OF_RANGE,
    SQLiteTimestampParseStatus.UNEXPECTED_STORAGE_CLASS,
)


class _StrictContract(BaseModel):
    """Apply one immutable, strict, recursively revalidated contract boundary."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


class SQLiteTimestampCanonicalCandidateStatus(StrEnum):
    """One deterministic candidate disposition for every TASK-032 outcome."""

    PROJECTED_AWARE_TEXT = "projected_aware_text"
    PROJECTED_EPOCH_MICROSECONDS = "projected_epoch_microseconds"
    SOURCE_NOT_PROJECTABLE = "source_not_projectable"
    UTC_NORMALIZATION_OVERFLOW = "utc_normalization_overflow"


class SQLiteTimestampCanonicalCandidateErrorCode(StrEnum):
    """Fail-closed errors for an invalid top-level TASK-032 evidence boundary."""

    INVALID_SOURCE_EVIDENCE = "invalid_source_evidence"
    UNREGISTERED_PLAN = "unregistered_plan"


class SQLiteTimestampCanonicalCandidateError(ValueError):
    """Reject evidence that is not exactly one registered TASK-032 result."""

    def __init__(
        self,
        code: SQLiteTimestampCanonicalCandidateErrorCode,
        detail: str,
    ) -> None:
        self.code = code
        super().__init__(f"{code.value}: {detail}")


class SQLiteTimestampCanonicalCandidatePlan(_StrictContract):
    """One pinned canonical projection declaration for an exact TASK-032 plan."""

    schema_version: ContractVersion = "1.0"
    projection_kind: ProjectionKind = "fixed_utc_text_epoch_microseconds"
    source_plan: SQLiteTimestampParsePlan
    projectable_source_statuses: tuple[SQLiteTimestampParseStatus, ...] = (
        _PROJECTABLE_SOURCE_STATUSES
    )
    nonprojectable_source_statuses: tuple[SQLiteTimestampParseStatus, ...] = (
        _NONPROJECTABLE_SOURCE_STATUSES
    )

    @model_validator(mode="after")
    def source_plan_is_deeply_valid(self) -> Self:
        """Reject a TASK-032 plan instance constructed around invalid nested evidence."""

        try:
            revalidated = SQLiteTimestampParsePlan.model_validate(
                self.source_plan.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-032 plan must pass deep validation") from exc
        if revalidated != self.source_plan:
            raise ValueError("source TASK-032 plan changed during deep validation")
        if self.source_plan not in _PINNED_PARSE_PLANS:
            raise ValueError("source TASK-032 plan must equal one reviewed immutable plan")
        if (
            self.projectable_source_statuses != _PROJECTABLE_SOURCE_STATUSES
            or self.nonprojectable_source_statuses != _NONPROJECTABLE_SOURCE_STATUSES
        ):
            raise ValueError("candidate plan must declare the exact TASK-032 status partition")
        if set((*_PROJECTABLE_SOURCE_STATUSES, *_NONPROJECTABLE_SOURCE_STATUSES)) != set(
            SQLiteTimestampParseStatus
        ):
            raise AssertionError("candidate status partition must cover TASK-032 exactly")
        return self


@dataclass(frozen=True, slots=True)
class _CandidateInterpretation:
    status: SQLiteTimestampCanonicalCandidateStatus
    canonical_datetime: datetime | None = None
    canonical_text: str | None = None
    epoch_microseconds: int | None = None


def _datetime_identity(value: datetime | None) -> object:
    if value is None:
        return None
    if type(value) is not datetime:
        return ("invalid_datetime_type", type(value))
    timezone = datetime.tzinfo.__get__(value)
    if timezone is not UTC:
        return ("invalid_timezone_identity", type(timezone))
    return (
        type(value),
        "datetime.UTC",
        datetime.year.__get__(value),
        datetime.month.__get__(value),
        datetime.day.__get__(value),
        datetime.hour.__get__(value),
        datetime.minute.__get__(value),
        datetime.second.__get__(value),
        datetime.microsecond.__get__(value),
        datetime.fold.__get__(value),
    )


def _interpret_candidate(
    source: SQLiteTimestampParseOutcome,
) -> _CandidateInterpretation:
    if source.status in _NONPROJECTABLE_SOURCE_STATUSES:
        return _CandidateInterpretation(
            status=SQLiteTimestampCanonicalCandidateStatus.SOURCE_NOT_PROJECTABLE
        )
    if source.status not in _PROJECTABLE_SOURCE_STATUSES:
        raise AssertionError("unsupported TASK-032 parse status")
    if source.parsed_datetime is None:
        raise AssertionError("validated successful parse outcomes always contain a datetime")
    if source.status is SQLiteTimestampParseStatus.PARSED_EPOCH_MICROSECONDS:
        if source.epoch_microseconds is None:
            raise AssertionError("validated epoch outcomes always contain epoch microseconds")
        canonical_datetime = from_epoch_microseconds(source.epoch_microseconds)
        if canonical_datetime != source.parsed_datetime:
            raise AssertionError("TASK-032 epoch datetime must equal its exact inverse decoding")
        projected_status = SQLiteTimestampCanonicalCandidateStatus.PROJECTED_EPOCH_MICROSECONDS
    else:
        try:
            canonical_datetime = normalize_aware_to_utc(source.parsed_datetime)
        except CanonicalUtcError:
            return _CandidateInterpretation(
                status=SQLiteTimestampCanonicalCandidateStatus.UTC_NORMALIZATION_OVERFLOW
            )
        projected_status = SQLiteTimestampCanonicalCandidateStatus.PROJECTED_AWARE_TEXT

    canonical_text = serialize_canonical_utc(canonical_datetime)
    epoch_microseconds = to_epoch_microseconds(canonical_datetime)
    if (
        parse_canonical_utc(canonical_text) != canonical_datetime
        or from_epoch_microseconds(epoch_microseconds) != canonical_datetime
    ):
        raise AssertionError("canonical candidate primitives must round-trip exactly")
    return _CandidateInterpretation(
        status=projected_status,
        canonical_datetime=canonical_datetime,
        canonical_text=canonical_text,
        epoch_microseconds=epoch_microseconds,
    )


class SQLiteTimestampCanonicalCandidateOutcome(_StrictContract):
    """One self-validating candidate that retains the exact TASK-032 outcome."""

    schema_version: ContractVersion = "1.0"
    source_outcome: SQLiteTimestampParseOutcome
    status: SQLiteTimestampCanonicalCandidateStatus
    canonical_datetime: datetime | None = None
    canonical_text: CanonicalText | None = None
    epoch_microseconds: EpochMicroseconds | None = None

    @model_validator(mode="after")
    def candidate_matches_source_outcome(self) -> Self:
        """Deeply revalidate and recompute the candidate from its exact source."""

        try:
            revalidated_source = SQLiteTimestampParseOutcome.model_validate(
                self.source_outcome.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-032 outcome must pass deep validation") from exc
        if revalidated_source != self.source_outcome:
            raise ValueError("source TASK-032 outcome changed during deep validation")

        expected = _interpret_candidate(self.source_outcome)
        actual_fields = (
            self.status,
            _datetime_identity(self.canonical_datetime),
            self.canonical_text,
            self.epoch_microseconds,
        )
        expected_fields = (
            expected.status,
            _datetime_identity(expected.canonical_datetime),
            expected.canonical_text,
            expected.epoch_microseconds,
        )
        if actual_fields != expected_fields:
            raise ValueError("canonical candidate does not match its exact TASK-032 outcome")
        if self.canonical_datetime is not None and (
            type(self.canonical_datetime) is not datetime
            or datetime.tzinfo.__get__(self.canonical_datetime) is not UTC
        ):
            raise ValueError("projected candidate datetime must be exact built-in datetime.UTC")
        return self


class SQLiteTimestampCanonicalCandidateRowEvidence(_StrictContract):
    """One source row key plus one ordered candidate per TASK-032 outcome."""

    row_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET)]
    stable_row_key: Annotated[
        tuple[SQLiteTimestampCellEvidence, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_KEY_COLUMNS),
    ]
    candidates: Annotated[
        tuple[SQLiteTimestampCanonicalCandidateOutcome, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_COLUMNS_PER_TARGET),
    ]

    @model_validator(mode="after")
    def row_shape_is_unambiguous(self) -> Self:
        """Require deeply valid unique keys and unique candidate source columns."""

        for cell in self.stable_row_key:
            try:
                revalidated_cell = SQLiteTimestampCellEvidence.model_validate(
                    cell.model_dump(mode="python"),
                    strict=True,
                )
            except (TypeError, ValueError, ValidationError) as exc:
                raise ValueError("candidate row-key cells must pass deep validation") from exc
            if revalidated_cell != cell:
                raise ValueError("candidate row-key cells changed during deep validation")
        key_names = tuple(cell.column_name.casefold() for cell in self.stable_row_key)
        candidate_names = tuple(
            candidate.source_outcome.source_cell.column_name.casefold()
            for candidate in self.candidates
        )
        if len(key_names) != len(set(key_names)) or any(
            cell.storage_class is SQLiteStorageClass.NULL for cell in self.stable_row_key
        ):
            raise ValueError("candidate row keys must be unique and non-null")
        if len(candidate_names) != len(set(candidate_names)):
            raise ValueError("candidate outcomes must have unique source columns")
        if set(key_names) & set(candidate_names):
            raise ValueError("candidate row keys and timestamp outcomes may not overlap")
        return self


class SQLiteTimestampCanonicalCandidateTableEvidence(_StrictContract):
    """Pure candidate evidence for one table in the unchanged source ordering."""

    target_ordinal: Annotated[int, Field(ge=0, lt=MAX_SQLITE_TIMESTAMP_TARGETS)]
    table_name: SQLiteIdentifier
    rows: Annotated[
        tuple[SQLiteTimestampCanonicalCandidateRowEvidence, ...],
        Field(max_length=MAX_SQLITE_TIMESTAMP_ROWS_PER_TARGET),
    ]

    @model_validator(mode="after")
    def rows_are_contiguous_and_unique(self) -> Self:
        """Require contiguous ordinals and unique stable keys."""

        if tuple(row.row_ordinal for row in self.rows) != tuple(range(len(self.rows))):
            raise ValueError("candidate row ordinals must be contiguous")
        stable_keys = tuple(row.stable_row_key for row in self.rows)
        if len(stable_keys) != len(set(stable_keys)):
            raise ValueError("candidate row stable keys must be unique")
        return self


class SQLiteTimestampCanonicalCandidateResult(_StrictContract):
    """One-to-one canonical candidate evidence linked to one complete TASK-032 result."""

    schema_version: ContractVersion = "1.0"
    source: SQLiteTimestampParseResult
    plan: SQLiteTimestampCanonicalCandidatePlan
    tables: Annotated[
        tuple[SQLiteTimestampCanonicalCandidateTableEvidence, ...],
        Field(min_length=1, max_length=MAX_SQLITE_TIMESTAMP_TARGETS),
    ]

    @model_validator(mode="after")
    def evidence_reconciles_one_to_one(self) -> Self:
        """Reject missing, reordered, replaced, or duplicated TASK-032 evidence."""

        try:
            revalidated_source = SQLiteTimestampParseResult.model_validate(
                self.source.model_dump(mode="python"),
                strict=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise ValueError("source TASK-032 result must pass deep validation") from exc
        if revalidated_source != self.source:
            raise ValueError("source TASK-032 result changed during deep validation")
        if self.plan != _pinned_plan_for_source(self.source):
            raise ValueError("candidate plan must equal the reviewed immutable declaration")
        if self.plan.source_plan != self.source.plan:
            raise ValueError("candidate plan must preserve the exact TASK-032 parse plan")
        if len(self.tables) != len(self.source.tables):
            raise ValueError("candidate tables must reconcile one-to-one with TASK-032")
        for candidate_table, source_table in zip(
            self.tables,
            self.source.tables,
            strict=True,
        ):
            if (
                candidate_table.target_ordinal != source_table.target_ordinal
                or candidate_table.table_name != source_table.table_name
                or len(candidate_table.rows) != len(source_table.rows)
            ):
                raise ValueError("candidate tables must preserve TASK-032 ordering and identity")
            for candidate_row, source_row in zip(
                candidate_table.rows,
                source_table.rows,
                strict=True,
            ):
                if (
                    candidate_row.row_ordinal != source_row.row_ordinal
                    or candidate_row.stable_row_key != source_row.stable_row_key
                    or len(candidate_row.candidates) != len(source_row.outcomes)
                ):
                    raise ValueError("candidate rows must preserve TASK-032 row evidence")
                for candidate, source_outcome in zip(
                    candidate_row.candidates,
                    source_row.outcomes,
                    strict=True,
                ):
                    if candidate.source_outcome != source_outcome:
                        raise ValueError(
                            "candidate outcomes must preserve every TASK-032 parse outcome"
                        )
        return self


_PINNED_CANDIDATE_PLANS: Final[tuple[SQLiteTimestampCanonicalCandidatePlan, ...]] = tuple(
    SQLiteTimestampCanonicalCandidatePlan(source_plan=parse_plan)
    for parse_plan in _PINNED_PARSE_PLANS
)
SQLITE_TIMESTAMP_CANONICAL_CANDIDATE_PLANS: tuple[SQLiteTimestampCanonicalCandidatePlan, ...] = (
    _PINNED_CANDIDATE_PLANS
)


def _pinned_plan_for_source(
    source: SQLiteTimestampParseResult,
) -> SQLiteTimestampCanonicalCandidatePlan:
    matches = tuple(
        plan
        for plan in _PINNED_CANDIDATE_PLANS
        if plan.source_plan.extraction_plan.family is source.plan.extraction_plan.family
        and plan.source_plan.extraction_plan.layout_version
        == source.plan.extraction_plan.layout_version
    )
    if len(matches) != 1:
        raise SQLiteTimestampCanonicalCandidateError(
            SQLiteTimestampCanonicalCandidateErrorCode.UNREGISTERED_PLAN,
            "the source family must have exactly one canonical candidate plan",
        )
    plan = matches[0]
    if plan.source_plan != source.plan:
        raise SQLiteTimestampCanonicalCandidateError(
            SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE,
            "the TASK-032 parse plan differs from the reviewed candidate declaration",
        )
    return plan


def _validated_source(
    source: SQLiteTimestampParseResult,
) -> SQLiteTimestampParseResult:
    try:
        revalidated = SQLiteTimestampParseResult.model_validate(
            source.model_dump(mode="python"),
            strict=True,
        )
    except (TypeError, ValueError, ValidationError) as exc:
        raise SQLiteTimestampCanonicalCandidateError(
            SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must pass deep TASK-032 contract validation",
        ) from exc
    if revalidated != source:
        raise SQLiteTimestampCanonicalCandidateError(
            SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE,
            "source changed during deep TASK-032 contract validation",
        )
    return source


def _candidate_outcome(
    source_outcome: SQLiteTimestampParseOutcome,
) -> SQLiteTimestampCanonicalCandidateOutcome:
    interpreted = _interpret_candidate(source_outcome)
    return SQLiteTimestampCanonicalCandidateOutcome(
        source_outcome=source_outcome,
        status=interpreted.status,
        canonical_datetime=interpreted.canonical_datetime,
        canonical_text=interpreted.canonical_text,
        epoch_microseconds=interpreted.epoch_microseconds,
    )


def derive_synthetic_sqlite_timestamp_canonical_candidate_evidence(
    source: SQLiteTimestampParseResult,
) -> SQLiteTimestampCanonicalCandidateResult:
    """Derive candidates from one exact TASK-032 result without I/O or replacement."""

    if type(source) is not SQLiteTimestampParseResult:
        raise SQLiteTimestampCanonicalCandidateError(
            SQLiteTimestampCanonicalCandidateErrorCode.INVALID_SOURCE_EVIDENCE,
            "source must be one exact SQLiteTimestampParseResult",
        )
    source = _validated_source(source)
    plan = _pinned_plan_for_source(source)
    tables = tuple(
        SQLiteTimestampCanonicalCandidateTableEvidence(
            target_ordinal=source_table.target_ordinal,
            table_name=source_table.table_name,
            rows=tuple(
                SQLiteTimestampCanonicalCandidateRowEvidence(
                    row_ordinal=source_row.row_ordinal,
                    stable_row_key=source_row.stable_row_key,
                    candidates=tuple(
                        _candidate_outcome(source_outcome) for source_outcome in source_row.outcomes
                    ),
                )
                for source_row in source_table.rows
            ),
        )
        for source_table in source.tables
    )
    return SQLiteTimestampCanonicalCandidateResult(
        source=source,
        plan=plan,
        tables=tables,
    )
