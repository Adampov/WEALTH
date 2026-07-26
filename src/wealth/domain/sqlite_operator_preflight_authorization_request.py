"""Pure synthetic proposal contract for a possible later operator preflight.

The eight symbolic path slots prove canonical family coverage for this synthetic contract only.
They do not assert that a future operator deployment has one database path per family. A later
populated request must establish its own reviewed cardinality and requires explicit project-owner
approval before any operator-data access.

Successful construction, validation, testing, review, or merge grants no authority, records no
human approval, and does not satisfy the Stage 3 gate.
"""

from typing import Annotated, Final, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from wealth.domain.sqlite_preflight import SQLiteStoreFamily
from wealth.domain.sqlite_timestamp_candidate_census_bundle import (
    _PINNED_BUNDLE_PLAN,
    SQLiteTimestampCandidateCensusBundlePlan,
)

__all__ = [
    "SQLITE_OPERATOR_PREFLIGHT_AUTHORIZATION_REQUEST_PLAN",
    "SQLiteOperatorPreflightAuthorizationRequestPlan",
    "SQLiteOperatorPreflightAuthorizationRequestProposal",
    "SQLiteOperatorPreflightFamilyPathPlaceholder",
    "SQLiteOperatorPreflightReportDestinationPlaceholder",
    "SQLiteOperatorPreflightRetentionDisposalPlaceholder",
    "SQLiteOperatorPreflightSnapshotMethodPlaceholder",
    "SQLiteOperatorPreflightSyntheticPathPlaceholder",
    "build_synthetic_sqlite_operator_preflight_authorization_request_proposal",
]

ContractVersion = Literal["1.0"]
PlanKind = Literal["operator_preflight_authorization_request_proposal"]
ProposalKind = Literal["synthetic_operator_preflight_authorization_request_proposal"]
ProposalStatus = Literal["proposal_only"]
AuthorityEffect = Literal["none_proposal_only"]
HumanApprovalState = Literal["not_recorded"]
OperatorDataAccessState = Literal["not_authorized"]
Stage3GateState = Literal["not_satisfied"]
ProposedAccessMode = Literal["read_only"]
SQLiteOperatorPreflightSyntheticPathPlaceholder = Literal[
    "synthetic_path_slot_market",
    "synthetic_path_slot_order_flow",
    "synthetic_path_slot_historical_collection",
    "synthetic_path_slot_continuous_collection",
    "synthetic_path_slot_collector_service",
    "synthetic_path_slot_public_trade_collection",
    "synthetic_path_slot_rate_budget",
    "synthetic_path_slot_reconciliation",
]

_EXPECTED_FAMILY_PATH_ENTRY_COUNT: Final[int] = 8
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
_FAMILY_VALUE_ORDER: Final[tuple[str, ...]] = (
    "market",
    "order_flow",
    "historical_collection",
    "continuous_collection",
    "collector_service",
    "public_trade_collection",
    "rate_budget",
    "reconciliation",
)
_FAMILY_NAME_ORDER: Final[tuple[str, ...]] = (
    "MARKET",
    "ORDER_FLOW",
    "HISTORICAL_COLLECTION",
    "CONTINUOUS_COLLECTION",
    "COLLECTOR_SERVICE",
    "PUBLIC_TRADE_COLLECTION",
    "RATE_BUDGET",
    "RECONCILIATION",
)
_PATH_PLACEHOLDER_ORDER: Final[tuple[SQLiteOperatorPreflightSyntheticPathPlaceholder, ...]] = (
    "synthetic_path_slot_market",
    "synthetic_path_slot_order_flow",
    "synthetic_path_slot_historical_collection",
    "synthetic_path_slot_continuous_collection",
    "synthetic_path_slot_collector_service",
    "synthetic_path_slot_public_trade_collection",
    "synthetic_path_slot_rate_budget",
    "synthetic_path_slot_reconciliation",
)


class _StrictContract(BaseModel):
    """Apply one immutable, strict, recursively revalidated contract boundary."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
    )


_PATH_SLOT_DECLARATIONS: Final[
    tuple[
        tuple[int, SQLiteStoreFamily, SQLiteOperatorPreflightSyntheticPathPlaceholder],
        ...,
    ]
] = (
    (
        0,
        SQLiteStoreFamily.MARKET,
        "synthetic_path_slot_market",
    ),
    (
        1,
        SQLiteStoreFamily.ORDER_FLOW,
        "synthetic_path_slot_order_flow",
    ),
    (
        2,
        SQLiteStoreFamily.HISTORICAL_COLLECTION,
        "synthetic_path_slot_historical_collection",
    ),
    (
        3,
        SQLiteStoreFamily.CONTINUOUS_COLLECTION,
        "synthetic_path_slot_continuous_collection",
    ),
    (
        4,
        SQLiteStoreFamily.COLLECTOR_SERVICE,
        "synthetic_path_slot_collector_service",
    ),
    (
        5,
        SQLiteStoreFamily.PUBLIC_TRADE_COLLECTION,
        "synthetic_path_slot_public_trade_collection",
    ),
    (
        6,
        SQLiteStoreFamily.RATE_BUDGET,
        "synthetic_path_slot_rate_budget",
    ),
    (
        7,
        SQLiteStoreFamily.RECONCILIATION,
        "synthetic_path_slot_reconciliation",
    ),
)


class SQLiteOperatorPreflightFamilyPathPlaceholder(_StrictContract):
    """One exact family-linked non-path slot with a proposed read-only constraint."""

    ordinal: Annotated[
        int,
        Field(ge=0, lt=_EXPECTED_FAMILY_PATH_ENTRY_COUNT),
    ]
    family: SQLiteStoreFamily
    path_placeholder: SQLiteOperatorPreflightSyntheticPathPlaceholder
    proposed_access_mode: ProposedAccessMode

    @field_validator("path_placeholder", mode="before")
    @classmethod
    def path_placeholder_is_an_exact_builtin_string(cls, value: object) -> object:
        """Reject path objects and mutable or path-like string subclasses."""

        if type(value) is not str:
            raise ValueError("path placeholder must be an exact built-in symbolic string")
        return value

    @model_validator(mode="after")
    def placeholder_is_linked_to_its_exact_family(self) -> Self:
        """Reject any altered ordinal, family, or symbolic-slot relationship."""

        declaration = (self.ordinal, self.family, self.path_placeholder)
        if declaration not in _PATH_SLOT_DECLARATIONS:
            raise ValueError("family path placeholder must match one reviewed symbolic slot")
        return self


class SQLiteOperatorPreflightSnapshotMethodPlaceholder(_StrictContract):
    """An unselected slot for a later writer-fenced SQLite-safe snapshot procedure."""

    placeholder_id: Literal["synthetic_writer_fenced_sqlite_safe_immutable_snapshot_method_slot"]
    required_snapshot_property: Literal["immutable"]
    actual_method_state: Literal["not_selected"]


class SQLiteOperatorPreflightReportDestinationPlaceholder(_StrictContract):
    """An unselected symbolic slot for the later owner-reviewed report destination."""

    placeholder_id: Literal["synthetic_report_destination_slot"]
    actual_destination_state: Literal["not_selected"]


class SQLiteOperatorPreflightRetentionDisposalPlaceholder(_StrictContract):
    """An unselected symbolic slot for the later evidence lifecycle boundary."""

    placeholder_id: Literal["synthetic_evidence_retention_disposal_boundary_slot"]
    actual_boundary_state: Literal["not_selected"]


def _require_deeply_valid_exact_model(
    value: BaseModel,
    model_type: type[BaseModel],
    detail: str,
) -> None:
    """Reject subclasses, construction bypasses, and altered nested declarations."""

    if type(value) is not model_type:
        raise ValueError(f"{detail} must use its exact reviewed contract type")
    try:
        revalidated = model_type.model_validate(
            value.model_dump(mode="python"),
            strict=True,
        )
    except (AttributeError, TypeError, ValueError, ValidationError) as exc:
        raise ValueError(f"{detail} must pass deep strict validation") from exc
    if revalidated != value:
        raise ValueError(f"{detail} changed during deep strict validation")


class SQLiteOperatorPreflightAuthorizationRequestPlan(_StrictContract):
    """One proposal-only plan pinned to the exact TASK-035 bundle declaration."""

    schema_version: ContractVersion
    plan_kind: PlanKind
    source_bundle_plan: SQLiteTimestampCandidateCensusBundlePlan
    family_order: Annotated[
        tuple[SQLiteStoreFamily, ...],
        Field(
            min_length=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
            max_length=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
        ),
    ]
    expected_family_path_entry_count: Annotated[
        int,
        Field(
            ge=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
            le=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
        ),
    ]

    @field_validator("source_bundle_plan", mode="before")
    @classmethod
    def source_bundle_plan_does_not_hide_a_subclass(cls, value: object) -> object:
        """Reject a model subclass before Pydantic can normalize it to the base type."""

        if isinstance(value, BaseModel) and type(value) is not (
            SQLiteTimestampCandidateCensusBundlePlan
        ):
            raise ValueError("source bundle plan must use its exact reviewed contract type")
        return value

    @model_validator(mode="after")
    def plan_is_exact_and_proposal_only(self) -> Self:
        """Deeply validate and pin all TASK-035 lineage and family semantics."""

        _require_deeply_valid_exact_model(
            self.source_bundle_plan,
            SQLiteTimestampCandidateCensusBundlePlan,
            "source bundle plan",
        )
        if self.source_bundle_plan != _PINNED_BUNDLE_PLAN:
            raise ValueError("source bundle plan must equal the private reviewed TASK-035 plan")
        source_family_order = tuple(
            source_plan.source_plan.source_plan.extraction_plan.family
            for source_plan in self.source_bundle_plan.source_plans
        )
        if (
            set(_FAMILY_ORDER) != set(SQLiteStoreFamily)
            or tuple(family.name for family in _FAMILY_ORDER) != _FAMILY_NAME_ORDER
            or tuple(family.value for family in _FAMILY_ORDER) != _FAMILY_VALUE_ORDER
            or tuple(declaration[2] for declaration in _PATH_SLOT_DECLARATIONS)
            != _PATH_PLACEHOLDER_ORDER
            or self.family_order != _FAMILY_ORDER
            or source_family_order != _FAMILY_ORDER
            or self.expected_family_path_entry_count != _EXPECTED_FAMILY_PATH_ENTRY_COUNT
        ):
            raise ValueError("authorization-request plan must preserve exact family coverage")
        return self


_PINNED_AUTHORIZATION_REQUEST_PLAN: Final[SQLiteOperatorPreflightAuthorizationRequestPlan] = (
    SQLiteOperatorPreflightAuthorizationRequestPlan(
        schema_version="1.0",
        plan_kind="operator_preflight_authorization_request_proposal",
        source_bundle_plan=_PINNED_BUNDLE_PLAN,
        family_order=_FAMILY_ORDER,
        expected_family_path_entry_count=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
    )
)
SQLITE_OPERATOR_PREFLIGHT_AUTHORIZATION_REQUEST_PLAN = _PINNED_AUTHORIZATION_REQUEST_PLAN

_PINNED_FAMILY_PATH_PLACEHOLDERS: Final[
    tuple[SQLiteOperatorPreflightFamilyPathPlaceholder, ...]
] = tuple(
    SQLiteOperatorPreflightFamilyPathPlaceholder(
        ordinal=ordinal,
        family=family,
        path_placeholder=path_placeholder,
        proposed_access_mode="read_only",
    )
    for ordinal, family, path_placeholder in _PATH_SLOT_DECLARATIONS
)
_PINNED_SNAPSHOT_METHOD_PLACEHOLDER: Final[SQLiteOperatorPreflightSnapshotMethodPlaceholder] = (
    SQLiteOperatorPreflightSnapshotMethodPlaceholder(
        placeholder_id="synthetic_writer_fenced_sqlite_safe_immutable_snapshot_method_slot",
        required_snapshot_property="immutable",
        actual_method_state="not_selected",
    )
)
_PINNED_REPORT_DESTINATION_PLACEHOLDER: Final[
    SQLiteOperatorPreflightReportDestinationPlaceholder
] = SQLiteOperatorPreflightReportDestinationPlaceholder(
    placeholder_id="synthetic_report_destination_slot",
    actual_destination_state="not_selected",
)
_PINNED_RETENTION_DISPOSAL_PLACEHOLDER: Final[
    SQLiteOperatorPreflightRetentionDisposalPlaceholder
] = SQLiteOperatorPreflightRetentionDisposalPlaceholder(
    placeholder_id="synthetic_evidence_retention_disposal_boundary_slot",
    actual_boundary_state="not_selected",
)


class SQLiteOperatorPreflightAuthorizationRequestProposal(_StrictContract):
    """One exact synthetic envelope whose authority effect is always none."""

    schema_version: ContractVersion
    proposal_kind: ProposalKind
    plan: SQLiteOperatorPreflightAuthorizationRequestPlan
    family_path_placeholders: Annotated[
        tuple[SQLiteOperatorPreflightFamilyPathPlaceholder, ...],
        Field(
            min_length=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
            max_length=_EXPECTED_FAMILY_PATH_ENTRY_COUNT,
        ),
    ]
    snapshot_method_placeholder: SQLiteOperatorPreflightSnapshotMethodPlaceholder
    report_destination_placeholder: SQLiteOperatorPreflightReportDestinationPlaceholder
    retention_disposal_placeholder: SQLiteOperatorPreflightRetentionDisposalPlaceholder
    proposal_status: ProposalStatus
    authority_effect: AuthorityEffect
    human_approval_state: HumanApprovalState
    operator_data_access_state: OperatorDataAccessState
    stage_3_gate_state: Stage3GateState

    @field_validator("plan", mode="before")
    @classmethod
    def plan_does_not_hide_a_subclass(cls, value: object) -> object:
        """Reject a model subclass before Pydantic can normalize it to the base type."""

        if isinstance(value, BaseModel) and type(value) is not (
            SQLiteOperatorPreflightAuthorizationRequestPlan
        ):
            raise ValueError("proposal plan must use its exact reviewed contract type")
        return value

    @field_validator("family_path_placeholders", mode="before")
    @classmethod
    def path_entries_do_not_hide_subclasses(cls, value: object) -> object:
        """Reject nested model subclasses before tuple item normalization."""

        if isinstance(value, tuple):
            for item in value:
                if isinstance(item, BaseModel) and type(item) is not (
                    SQLiteOperatorPreflightFamilyPathPlaceholder
                ):
                    raise ValueError(
                        "family path placeholders must use their exact reviewed contract type"
                    )
        return value

    @field_validator("snapshot_method_placeholder", mode="before")
    @classmethod
    def snapshot_placeholder_does_not_hide_a_subclass(cls, value: object) -> object:
        """Reject a snapshot placeholder subclass before normalization."""

        if isinstance(value, BaseModel) and type(value) is not (
            SQLiteOperatorPreflightSnapshotMethodPlaceholder
        ):
            raise ValueError("snapshot method placeholder must use its exact contract type")
        return value

    @field_validator("report_destination_placeholder", mode="before")
    @classmethod
    def report_placeholder_does_not_hide_a_subclass(cls, value: object) -> object:
        """Reject a report placeholder subclass before normalization."""

        if isinstance(value, BaseModel) and type(value) is not (
            SQLiteOperatorPreflightReportDestinationPlaceholder
        ):
            raise ValueError("report destination placeholder must use its exact contract type")
        return value

    @field_validator("retention_disposal_placeholder", mode="before")
    @classmethod
    def lifecycle_placeholder_does_not_hide_a_subclass(cls, value: object) -> object:
        """Reject a retention/disposal placeholder subclass before normalization."""

        if isinstance(value, BaseModel) and type(value) is not (
            SQLiteOperatorPreflightRetentionDisposalPlaceholder
        ):
            raise ValueError("retention and disposal placeholder must use its exact contract type")
        return value

    @model_validator(mode="after")
    def proposal_is_exact_without_authority(self) -> Self:
        """Reject forged declarations and preserve the explicit no-authority boundary."""

        _require_deeply_valid_exact_model(
            self.plan,
            SQLiteOperatorPreflightAuthorizationRequestPlan,
            "authorization-request plan",
        )
        if self.plan != _PINNED_AUTHORIZATION_REQUEST_PLAN:
            raise ValueError("proposal plan must equal the private reviewed declaration")

        for placeholder in self.family_path_placeholders:
            _require_deeply_valid_exact_model(
                placeholder,
                SQLiteOperatorPreflightFamilyPathPlaceholder,
                "family path placeholder",
            )
        if self.family_path_placeholders != _PINNED_FAMILY_PATH_PLACEHOLDERS:
            raise ValueError("proposal must preserve all eight exact symbolic path slots")
        proposal_family_order = tuple(
            placeholder.family for placeholder in self.family_path_placeholders
        )
        if proposal_family_order != self.plan.family_order:
            raise ValueError("proposal family path slots must follow the pinned plan order")

        nested_placeholders: tuple[
            tuple[BaseModel, type[BaseModel], str],
            ...,
        ] = (
            (
                self.snapshot_method_placeholder,
                SQLiteOperatorPreflightSnapshotMethodPlaceholder,
                "snapshot method placeholder",
            ),
            (
                self.report_destination_placeholder,
                SQLiteOperatorPreflightReportDestinationPlaceholder,
                "report destination placeholder",
            ),
            (
                self.retention_disposal_placeholder,
                SQLiteOperatorPreflightRetentionDisposalPlaceholder,
                "retention and disposal placeholder",
            ),
        )
        for value, model_type, detail in nested_placeholders:
            _require_deeply_valid_exact_model(value, model_type, detail)
        if (
            self.snapshot_method_placeholder != _PINNED_SNAPSHOT_METHOD_PLACEHOLDER
            or self.report_destination_placeholder != _PINNED_REPORT_DESTINATION_PLACEHOLDER
            or self.retention_disposal_placeholder != _PINNED_RETENTION_DISPOSAL_PLACEHOLDER
        ):
            raise ValueError("proposal must preserve all exact unselected boundary placeholders")
        return self


def build_synthetic_sqlite_operator_preflight_authorization_request_proposal() -> (
    SQLiteOperatorPreflightAuthorizationRequestProposal
):
    """Build the exact deterministic proposal without I/O or granted authority."""

    return SQLiteOperatorPreflightAuthorizationRequestProposal(
        schema_version="1.0",
        proposal_kind="synthetic_operator_preflight_authorization_request_proposal",
        plan=_PINNED_AUTHORIZATION_REQUEST_PLAN,
        family_path_placeholders=_PINNED_FAMILY_PATH_PLACEHOLDERS,
        snapshot_method_placeholder=_PINNED_SNAPSHOT_METHOD_PLACEHOLDER,
        report_destination_placeholder=_PINNED_REPORT_DESTINATION_PLACEHOLDER,
        retention_disposal_placeholder=_PINNED_RETENTION_DISPOSAL_PLACEHOLDER,
        proposal_status="proposal_only",
        authority_effect="none_proposal_only",
        human_approval_state="not_recorded",
        operator_data_access_state="not_authorized",
        stage_3_gate_state="not_satisfied",
    )
