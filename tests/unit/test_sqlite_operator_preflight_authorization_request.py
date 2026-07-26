"""Strict purity and non-authority coverage for the TASK-036 proposal contract."""

import ast
import inspect
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Literal, get_args, get_origin

import pytest
from pydantic import BaseModel, ValidationError

from wealth.domain import (
    sqlite_operator_preflight_authorization_request as authorization_request,
)
from wealth.domain import (
    sqlite_timestamp_candidate_census_bundle as candidate_census_bundle,
)
from wealth.domain.sqlite_operator_preflight_authorization_request import (
    SQLITE_OPERATOR_PREFLIGHT_AUTHORIZATION_REQUEST_PLAN,
    SQLiteOperatorPreflightAuthorizationRequestPlan,
    SQLiteOperatorPreflightAuthorizationRequestProposal,
    SQLiteOperatorPreflightFamilyPathPlaceholder,
    SQLiteOperatorPreflightReportDestinationPlaceholder,
    SQLiteOperatorPreflightRetentionDisposalPlaceholder,
    SQLiteOperatorPreflightSnapshotMethodPlaceholder,
    SQLiteOperatorPreflightSyntheticPathPlaceholder,
    build_synthetic_sqlite_operator_preflight_authorization_request_proposal,
)
from wealth.domain.sqlite_preflight import SQLiteStoreFamily
from wealth.domain.sqlite_timestamp_candidate_census_bundle import (
    SQLiteTimestampCandidateCensusBundlePlan,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
AUTHORIZATION_REQUEST_MODULE_PATH = (
    REPOSITORY_ROOT
    / "src"
    / "wealth"
    / "domain"
    / "sqlite_operator_preflight_authorization_request.py"
)


def _assert_strict_frozen_and_extra_forbidden(model: BaseModel) -> None:
    model_type = type(model)
    assert model_type.model_config["strict"] is True
    assert model_type.model_config["frozen"] is True
    assert model_type.model_config["extra"] == "forbid"
    assert model_type.model_config["revalidate_instances"] == "always"

    with pytest.raises(ValidationError, match="Extra inputs"):
        model_type.model_validate(
            {**model.model_dump(mode="python"), "unsupported_runtime_value": "forbidden"},
            strict=True,
        )
    field_name = next(iter(model_type.model_fields))
    with pytest.raises(ValidationError):
        setattr(model, field_name, getattr(model, field_name))


def test_exact_canonical_proposal_covers_symbolic_slots_and_task_035_plan() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    plan = proposal.plan
    expected_families = tuple(SQLiteStoreFamily)
    expected_path_values = tuple(
        f"synthetic_path_slot_{family.value}" for family in expected_families
    )
    source_families = tuple(
        source_plan.source_plan.source_plan.extraction_plan.family
        for source_plan in plan.source_bundle_plan.source_plans
    )

    assert type(proposal) is SQLiteOperatorPreflightAuthorizationRequestProposal
    assert proposal.schema_version == "1.0"
    assert proposal.proposal_kind == "synthetic_operator_preflight_authorization_request_proposal"
    assert plan.source_bundle_plan == candidate_census_bundle._PINNED_BUNDLE_PLAN
    assert plan.schema_version == "1.0"
    assert plan.plan_kind == "operator_preflight_authorization_request_proposal"
    assert plan.family_order == expected_families
    assert source_families == expected_families
    assert plan.expected_family_path_entry_count == 8

    assert get_origin(SQLiteOperatorPreflightSyntheticPathPlaceholder) is Literal
    assert get_args(SQLiteOperatorPreflightSyntheticPathPlaceholder) == expected_path_values
    assert tuple(
        (
            entry.ordinal,
            entry.family,
            entry.path_placeholder,
            entry.proposed_access_mode,
        )
        for entry in proposal.family_path_placeholders
    ) == tuple(
        (
            ordinal,
            family,
            expected_path_values[ordinal],
            "read_only",
        )
        for ordinal, family in enumerate(expected_families)
    )
    assert all(
        entry.family
        is plan.source_bundle_plan.source_plans[
            entry.ordinal
        ].source_plan.source_plan.extraction_plan.family
        for entry in proposal.family_path_placeholders
    )
    assert proposal.snapshot_method_placeholder.model_dump(mode="python") == {
        "placeholder_id": ("synthetic_writer_fenced_sqlite_safe_immutable_snapshot_method_slot"),
        "required_snapshot_property": "immutable",
        "actual_method_state": "not_selected",
    }
    assert proposal.report_destination_placeholder.model_dump(mode="python") == {
        "placeholder_id": "synthetic_report_destination_slot",
        "actual_destination_state": "not_selected",
    }
    assert proposal.retention_disposal_placeholder.model_dump(mode="python") == {
        "placeholder_id": "synthetic_evidence_retention_disposal_boundary_slot",
        "actual_boundary_state": "not_selected",
    }


def test_every_public_model_is_strict_frozen_revalidated_and_extra_forbidden() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    models: tuple[BaseModel, ...] = (
        proposal.plan,
        *proposal.family_path_placeholders,
        proposal.snapshot_method_placeholder,
        proposal.report_destination_placeholder,
        proposal.retention_disposal_placeholder,
        proposal,
    )

    for model in models:
        _assert_strict_frozen_and_extra_forbidden(model)

    entry = proposal.family_path_placeholders[0]
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightFamilyPathPlaceholder.model_validate(
            {**entry.model_dump(mode="python"), "ordinal": False},
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightFamilyPathPlaceholder.model_validate(
            {**entry.model_dump(mode="python"), "family": entry.family.value},
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightFamilyPathPlaceholder.model_validate(
            {
                **entry.model_dump(mode="python"),
                "path_placeholder": "synthetic_path_slot_unsupported",
            },
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightAuthorizationRequestPlan.model_validate(
            {
                **proposal.plan.model_dump(mode="python"),
                "family_order": list(proposal.plan.family_order),
            },
            strict=True,
        )
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
            {
                **proposal.model_dump(mode="python"),
                "family_path_placeholders": list(proposal.family_path_placeholders),
            },
            strict=True,
        )


def test_every_public_contract_field_is_required() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    models: tuple[BaseModel, ...] = (
        proposal.plan,
        *proposal.family_path_placeholders,
        proposal.snapshot_method_placeholder,
        proposal.report_destination_placeholder,
        proposal.retention_disposal_placeholder,
        proposal,
    )

    for model in models:
        model_type = type(model)
        payload = model.model_dump(mode="python")
        for field_name in model_type.model_fields:
            missing_field_payload = {
                key: value for key, value in payload.items() if key != field_name
            }
            with pytest.raises(ValidationError):
                model_type.model_validate(missing_field_payload, strict=True)


def test_plan_rejects_altered_family_semantics_and_deep_source_plan_tampering() -> None:
    plan = SQLITE_OPERATOR_PREFLIGHT_AUTHORIZATION_REQUEST_PLAN
    family_order = plan.family_order
    forged_bundle_plan = plan.source_bundle_plan.model_copy(update={"expected_column_count": 36})
    constructed_bundle_plan = SQLiteTimestampCandidateCensusBundlePlan.model_construct()
    invalid_updates: tuple[dict[str, object], ...] = (
        {"family_order": family_order[:-1]},
        {"family_order": tuple(reversed(family_order))},
        {"family_order": (family_order[0], family_order[0], *family_order[2:])},
        {"expected_family_path_entry_count": 7},
        {"expected_family_path_entry_count": False},
        {"source_bundle_plan": forged_bundle_plan},
        {"source_bundle_plan": constructed_bundle_plan},
        {"schema_version": "2.0"},
        {"plan_kind": "operator_preflight_authorization"},
    )

    for update in invalid_updates:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestPlan.model_validate(
                plan.model_copy(update=update),
                strict=True,
            )


def test_proposal_rejects_missing_extra_reordered_and_duplicate_family_slots() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    entries = proposal.family_path_placeholders
    invalid_sequences = (
        entries[:-1],
        (*entries, entries[-1]),
        tuple(reversed(entries)),
        (entries[1], entries[0], *entries[2:]),
        (entries[0], entries[0], *entries[2:]),
    )

    for invalid_entries in invalid_sequences:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                proposal.model_copy(update={"family_path_placeholders": invalid_entries}),
                strict=True,
            )

    altered_entries = (
        entries[0].model_copy(update={"ordinal": 1}),
        entries[0].model_copy(update={"family": SQLiteStoreFamily.ORDER_FLOW}),
        entries[0].model_copy(update={"path_placeholder": "synthetic_path_slot_order_flow"}),
        entries[0].model_copy(update={"proposed_access_mode": "read_write"}),
    )
    for altered_entry in altered_entries:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                proposal.model_copy(
                    update={
                        "family_path_placeholders": (
                            altered_entry,
                            *entries[1:],
                        )
                    }
                ),
                strict=True,
            )


def test_family_path_placeholder_rejects_raw_paths_uris_traversal_and_path_objects() -> None:
    class PathLikeString(str):
        pass

    entry = build_synthetic_sqlite_operator_preflight_authorization_request_proposal().family_path_placeholders[
        0
    ]
    raw_or_path_like_values: tuple[object, ...] = (
        "operator.sqlite3",
        "/var/lib/wealth/operator.sqlite3",
        r"C:\wealth\operator.sqlite3",
        r"\\server\share\operator.sqlite3",
        "file:///var/lib/wealth/operator.sqlite3",
        "sqlite:///var/lib/wealth/operator.sqlite3",
        "../operator.sqlite3",
        Path("/var/lib/wealth/operator.sqlite3"),
        PathLikeString(entry.path_placeholder),
    )

    for invalid_path in raw_or_path_like_values:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightFamilyPathPlaceholder.model_validate(
                {
                    **entry.model_dump(mode="python"),
                    "path_placeholder": invalid_path,
                },
                strict=True,
            )

    for update in (
        {"ordinal": 1},
        {"family": SQLiteStoreFamily.ORDER_FLOW},
        {"path_placeholder": "synthetic_path_slot_order_flow"},
        {"proposed_access_mode": "write"},
    ):
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightFamilyPathPlaceholder.model_validate(
                {**entry.model_dump(mode="python"), **update},
                strict=True,
            )


def test_symbolic_path_type_has_no_mutable_enum_singleton_surface() -> None:
    canonical_tokens = get_args(SQLiteOperatorPreflightSyntheticPathPlaceholder)

    assert get_origin(SQLiteOperatorPreflightSyntheticPathPlaceholder) is Literal
    assert canonical_tokens == authorization_request._PATH_PLACEHOLDER_ORDER
    assert not hasattr(SQLiteOperatorPreflightSyntheticPathPlaceholder, "__members__")
    assert all(type(token) is str and not hasattr(token, "_value_") for token in canonical_tokens)
    with pytest.raises(AttributeError):
        canonical_tokens[0]._value_ = Path("/var/lib/wealth/operator.sqlite3")


def test_mutated_store_family_value_fails_closed_in_an_isolated_process() -> None:
    script = """
from pathlib import Path

from wealth.domain.sqlite_operator_preflight_authorization_request import (
    build_synthetic_sqlite_operator_preflight_authorization_request_proposal,
)
from wealth.domain.sqlite_preflight import SQLiteStoreFamily

SQLiteStoreFamily.MARKET._value_ = Path("/var/lib/wealth/operator.sqlite3")
try:
    build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
except (TypeError, ValueError):
    pass
else:
    raise AssertionError("mutated family enum value was accepted")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr


def test_nested_boundary_placeholders_reject_unsupported_and_constructed_tampering() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    direct_invalid_models: tuple[tuple[BaseModel, dict[str, object]], ...] = (
        (
            proposal.snapshot_method_placeholder,
            {"actual_method_state": "selected"},
        ),
        (
            proposal.snapshot_method_placeholder,
            {"required_snapshot_property": "mutable"},
        ),
        (
            proposal.report_destination_placeholder,
            {"actual_destination_state": "selected"},
        ),
        (
            proposal.retention_disposal_placeholder,
            {"actual_boundary_state": "selected"},
        ),
    )
    for model, update in direct_invalid_models:
        with pytest.raises(ValidationError):
            type(model).model_validate(
                {**model.model_dump(mode="python"), **update},
                strict=True,
            )

    forged_snapshot = SQLiteOperatorPreflightSnapshotMethodPlaceholder.model_construct(
        placeholder_id="operator_snapshot_method",
        required_snapshot_property="immutable",
        actual_method_state="not_selected",
    )
    forged_report = SQLiteOperatorPreflightReportDestinationPlaceholder.model_construct(
        placeholder_id="/tmp/operator-report.json",
        actual_destination_state="not_selected",
    )
    forged_retention = SQLiteOperatorPreflightRetentionDisposalPlaceholder.model_construct(
        placeholder_id="retain_forever",
        actual_boundary_state="not_selected",
    )
    invalid_updates: tuple[dict[str, object], ...] = (
        {"snapshot_method_placeholder": forged_snapshot},
        {"report_destination_placeholder": forged_report},
        {"retention_disposal_placeholder": forged_retention},
    )
    for update in invalid_updates:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                proposal.model_copy(update=update),
                strict=True,
            )


def test_nested_subclasses_and_model_construct_bypasses_are_rejected() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()

    class BundlePlanSubclass(SQLiteTimestampCandidateCensusBundlePlan):
        pass

    class RequestPlanSubclass(SQLiteOperatorPreflightAuthorizationRequestPlan):
        pass

    class FamilyPathSubclass(SQLiteOperatorPreflightFamilyPathPlaceholder):
        pass

    class SnapshotSubclass(SQLiteOperatorPreflightSnapshotMethodPlaceholder):
        pass

    class ReportSubclass(SQLiteOperatorPreflightReportDestinationPlaceholder):
        pass

    class RetentionSubclass(SQLiteOperatorPreflightRetentionDisposalPlaceholder):
        pass

    bundle_plan_subclass = BundlePlanSubclass.model_validate(
        proposal.plan.source_bundle_plan.model_dump(mode="python"),
        strict=True,
    )
    with pytest.raises(ValidationError):
        SQLiteOperatorPreflightAuthorizationRequestPlan.model_validate(
            {
                **proposal.plan.model_dump(mode="python"),
                "source_bundle_plan": bundle_plan_subclass,
            },
            strict=True,
        )

    request_plan_subclass = RequestPlanSubclass.model_validate(
        proposal.plan.model_dump(mode="python"),
        strict=True,
    )
    entry_subclass = FamilyPathSubclass.model_validate(
        proposal.family_path_placeholders[0].model_dump(mode="python"),
        strict=True,
    )
    snapshot_subclass = SnapshotSubclass.model_validate(
        proposal.snapshot_method_placeholder.model_dump(mode="python"),
        strict=True,
    )
    report_subclass = ReportSubclass.model_validate(
        proposal.report_destination_placeholder.model_dump(mode="python"),
        strict=True,
    )
    retention_subclass = RetentionSubclass.model_validate(
        proposal.retention_disposal_placeholder.model_dump(mode="python"),
        strict=True,
    )
    subclass_updates: tuple[dict[str, object], ...] = (
        {"plan": request_plan_subclass},
        {
            "family_path_placeholders": (
                entry_subclass,
                *proposal.family_path_placeholders[1:],
            )
        },
        {"snapshot_method_placeholder": snapshot_subclass},
        {"report_destination_placeholder": report_subclass},
        {"retention_disposal_placeholder": retention_subclass},
    )
    for update in subclass_updates:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                {**proposal.model_dump(mode="python"), **update},
                strict=True,
            )

    constructed_entry = SQLiteOperatorPreflightFamilyPathPlaceholder.model_construct(
        ordinal=0,
        family=SQLiteStoreFamily.MARKET,
        path_placeholder="synthetic_path_slot_order_flow",
        proposed_access_mode="read_only",
    )
    constructed_plan = SQLiteOperatorPreflightAuthorizationRequestPlan.model_construct(
        schema_version="1.0",
        plan_kind="operator_preflight_authorization_request_proposal",
        source_bundle_plan=proposal.plan.source_bundle_plan,
        family_order=tuple(reversed(proposal.plan.family_order)),
        expected_family_path_entry_count=8,
    )
    constructed_updates: tuple[dict[str, object], ...] = (
        {"plan": constructed_plan},
        {
            "family_path_placeholders": (
                constructed_entry,
                *proposal.family_path_placeholders[1:],
            )
        },
    )
    for update in constructed_updates:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                proposal.model_copy(update=update),
                strict=True,
            )


def test_nonapproval_invariant_is_explicit_and_cannot_be_promoted() -> None:
    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()

    assert proposal.proposal_status == "proposal_only"
    assert proposal.authority_effect == "none_proposal_only"
    assert proposal.human_approval_state == "not_recorded"
    assert proposal.operator_data_access_state == "not_authorized"
    assert proposal.stage_3_gate_state == "not_satisfied"
    assert not {
        "approval_id",
        "approved_by",
        "approved_at",
        "authorization_signature",
        "authorization_expiry",
        "actual_operator_path",
        "actual_snapshot_method",
        "actual_report_destination",
        "actual_retention_boundary",
        "actual_disposal_boundary",
    } & set(SQLiteOperatorPreflightAuthorizationRequestProposal.model_fields)

    promotion_attempts = (
        {"proposal_status": "approved"},
        {"authority_effect": "grants_operator_access"},
        {"human_approval_state": "recorded"},
        {"operator_data_access_state": "authorized"},
        {"stage_3_gate_state": "satisfied"},
    )
    for update in promotion_attempts:
        with pytest.raises(ValidationError):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                proposal.model_copy(update=update),
                strict=True,
            )

    for extra_field in (
        "approval_id",
        "approved_by",
        "authorization_signature",
        "actual_operator_path",
        "scanner_callback",
    ):
        with pytest.raises(ValidationError, match="Extra inputs"):
            SQLiteOperatorPreflightAuthorizationRequestProposal.model_validate(
                {
                    **proposal.model_dump(mode="python"),
                    extra_field: "forbidden",
                },
                strict=True,
            )


def test_public_plan_alias_isolation_and_zero_argument_factory_are_deterministic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_plan = authorization_request._PINNED_AUTHORIZATION_REQUEST_PLAN
    private_bundle_plan = candidate_census_bundle._PINNED_BUNDLE_PLAN
    forged_plan = private_plan.model_copy(update={"expected_family_path_entry_count": 7})
    forged_bundle_plan = private_bundle_plan.model_copy(update={"expected_column_count": 36})
    monkeypatch.setattr(
        authorization_request,
        "SQLITE_OPERATOR_PREFLIGHT_AUTHORIZATION_REQUEST_PLAN",
        forged_plan,
    )
    monkeypatch.setattr(
        candidate_census_bundle,
        "SQLITE_TIMESTAMP_CANDIDATE_CENSUS_BUNDLE_PLAN",
        forged_bundle_plan,
    )

    first = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()
    second = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()

    assert first == second
    assert first.plan == private_plan
    assert first.plan.source_bundle_plan == private_bundle_plan
    assert authorization_request._PINNED_AUTHORIZATION_REQUEST_PLAN is private_plan
    assert candidate_census_bundle._PINNED_BUNDLE_PLAN is private_bundle_plan
    assert (
        tuple(
            inspect.signature(
                build_synthetic_sqlite_operator_preflight_authorization_request_proposal
            ).parameters
        )
        == ()
    )
    with pytest.raises(TypeError):
        build_synthetic_sqlite_operator_preflight_authorization_request_proposal(
            private_plan  # type: ignore[call-arg]
        )
    with pytest.raises(TypeError):
        build_synthetic_sqlite_operator_preflight_authorization_request_proposal(
            plan=private_plan  # type: ignore[call-arg]
        )


def test_construction_performs_no_path_sqlite_serialization_or_output_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_operation(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("proposal construction attempted a forbidden operation")

    monkeypatch.setattr("builtins.open", forbidden_operation)
    monkeypatch.setattr(Path, "exists", forbidden_operation)
    monkeypatch.setattr(Path, "is_file", forbidden_operation)
    monkeypatch.setattr(Path, "stat", forbidden_operation)
    monkeypatch.setattr(Path, "resolve", forbidden_operation)
    monkeypatch.setattr(Path, "open", forbidden_operation)
    monkeypatch.setattr(Path, "read_text", forbidden_operation)
    monkeypatch.setattr(Path, "read_bytes", forbidden_operation)
    monkeypatch.setattr(Path, "write_text", forbidden_operation)
    monkeypatch.setattr(Path, "write_bytes", forbidden_operation)
    monkeypatch.setattr(sqlite3, "connect", forbidden_operation)
    monkeypatch.setattr(json, "dump", forbidden_operation)
    monkeypatch.setattr(json, "dumps", forbidden_operation)
    monkeypatch.setattr(BaseModel, "model_dump_json", forbidden_operation)

    proposal = build_synthetic_sqlite_operator_preflight_authorization_request_proposal()

    assert proposal.proposal_status == "proposal_only"
    assert proposal.operator_data_access_state == "not_authorized"


def test_module_ast_is_pure_and_has_no_runtime_io_or_serialization_calls() -> None:
    tree = ast.parse(AUTHORIZATION_REQUEST_MODULE_PATH.read_text(encoding="utf-8"))
    imported_modules: set[str] = set()
    called_names: set[str] = set()
    called_attributes: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported_modules.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_attributes.add(node.func.attr)

    assert not {"pathlib", "sqlite3", "os", "json"} & imported_modules
    assert not any(module.startswith("wealth.adapters") for module in imported_modules)
    assert "open" not in called_names
    assert (
        not {
            "backup",
            "connect",
            "execute",
            "executemany",
            "exists",
            "is_file",
            "model_dump_json",
            "model_validate_json",
            "open",
            "read_bytes",
            "read_text",
            "resolve",
            "stat",
            "write_bytes",
            "write_text",
        }
        & called_attributes
    )


def test_public_surface_is_minimal_and_module_has_no_runtime_consumer() -> None:
    assert authorization_request.__all__ == [
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
    assert tuple(SQLiteOperatorPreflightAuthorizationRequestPlan.model_fields) == (
        "schema_version",
        "plan_kind",
        "source_bundle_plan",
        "family_order",
        "expected_family_path_entry_count",
    )
    assert tuple(SQLiteOperatorPreflightAuthorizationRequestProposal.model_fields) == (
        "schema_version",
        "proposal_kind",
        "plan",
        "family_path_placeholders",
        "snapshot_method_placeholder",
        "report_destination_placeholder",
        "retention_disposal_placeholder",
        "proposal_status",
        "authority_effect",
        "human_approval_state",
        "operator_data_access_state",
        "stage_3_gate_state",
    )
    public_field_names = {
        field_name
        for model_type in (
            SQLiteOperatorPreflightAuthorizationRequestPlan,
            SQLiteOperatorPreflightAuthorizationRequestProposal,
            SQLiteOperatorPreflightFamilyPathPlaceholder,
            SQLiteOperatorPreflightSnapshotMethodPlaceholder,
            SQLiteOperatorPreflightReportDestinationPlaceholder,
            SQLiteOperatorPreflightRetentionDisposalPlaceholder,
        )
        for field_name in model_type.model_fields
    }
    assert (
        not {
            "actual_path",
            "database_path",
            "operator_path",
            "source_result",
            "bundle_result",
            "scan_result",
            "report",
            "manifest",
            "serialized_output",
            "runtime",
            "migration",
            "repair",
            "schema_change",
        }
        & public_field_names
    )

    consumers = []
    module_name = "sqlite_operator_preflight_authorization_request"
    for path in (REPOSITORY_ROOT / "src" / "wealth").rglob("*.py"):
        if path == AUTHORIZATION_REQUEST_MODULE_PATH:
            continue
        if module_name in path.read_text(encoding="utf-8"):
            consumers.append(path)
    assert consumers == []
