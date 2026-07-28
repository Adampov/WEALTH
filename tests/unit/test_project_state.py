"""Tests for the canonical machine-readable project-state contract."""

import json
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from wealth.domain.project_state import ProjectState, load_project_state

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROJECT_STATE_PATH = REPOSITORY_ROOT / "PROJECT_STATE.json"
ROOT_README_PATH = REPOSITORY_ROOT / "README.md"
RISK_REGISTER_PATH = REPOSITORY_ROOT / "RISK_REGISTER.md"
BACKLOG_PATH = REPOSITORY_ROOT / "BACKLOG.md"
MARKET_DATA_CONTRACT_PATH = REPOSITORY_ROOT / "docs" / "contracts" / "MARKET_DATA.md"
ROADMAP_PATH = REPOSITORY_ROOT / "docs" / "ROADMAP.md"
DECISION_INDEX_PATH = REPOSITORY_ROOT / "docs" / "decisions" / "README.md"
ADR_0029_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "decisions"
    / "0029-continuous-public-trade-stream-persistence-contract.md"
)
ADR_0030_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "decisions"
    / "0030-continuous-public-trade-stream-store-port-contract.md"
)


def load_payload() -> dict[str, Any]:
    """Load a mutable copy for negative contract tests."""

    return cast(dict[str, Any], json.loads(PROJECT_STATE_PATH.read_text(encoding="utf-8")))


def validate_payload(payload: dict[str, Any]) -> ProjectState:
    """Validate a modified payload through the same JSON boundary as the canonical file."""

    return ProjectState.model_validate_json(json.dumps(payload))


def collapse_whitespace(value: str) -> str:
    """Normalize prose wrapping while preserving semantic governance assertions."""

    return " ".join(value.split())


def test_repository_project_state_is_valid_and_names_one_next_action() -> None:
    state = load_project_state(PROJECT_STATE_PATH)

    assert state.project_id == "WEALTH"
    assert state.current_phase == "PHASE_2_MARKET_DATA"
    assert state.operating_mode == "research"
    assert state.current_risk_state == "NO_TRADING_CAPABILITY"
    assert state.active_strategies == ()
    assert state.champion_strategy is None
    assert state.challenger_strategies == ()
    assert state.open_positions == ()
    assert state.open_orders == ()
    assert state.pending_approvals == (
        "TASK-037 project-owner decision plus independent Risk and Security reviews for the exact "
        "operator-preflight authorization package",
    )
    assert "public_trade_checkpoint_orchestration" in state.active_components
    assert "public_trade_transition_history_reader" in state.active_components
    assert "canonical_utc_clock_boundary_enforcement" in state.active_components
    assert "canonical_utc_codec_primitives" in state.active_components
    assert "canonical_utc_epoch_microsecond_primitives" in state.active_components
    assert "canonical_utc_preflight_fingerprint_foundation" in state.active_components
    assert "canonical_utc_preflight_timestamp_evidence_foundation" in state.active_components
    assert "canonical_utc_preflight_timestamp_parse_evidence_foundation" in (
        state.active_components
    )
    assert (
        "canonical_utc_preflight_timestamp_canonical_candidate_evidence_foundation"
        in state.active_components
    )
    assert (
        "canonical_utc_preflight_timestamp_candidate_census_evidence_foundation"
        in state.active_components
    )
    assert (
        "canonical_utc_preflight_timestamp_candidate_census_bundle_evidence_foundation"
        in state.active_components
    )
    assert (
        "canonical_utc_preflight_operator_authorization_request_contract_foundation"
        in state.active_components
    )
    assert "public_provider_payload_failure_boundary_hardening" in state.active_components
    assert "exact_candle_persistence_evidence_validation" in state.active_components
    assert "exact_order_flow_persistence_evidence_validation" in state.active_components
    assert "finite_public_http_timeout_boundary_validation" in state.active_components
    assert "strict_public_http_response_byte_limit_validation" in state.active_components
    assert "typed_public_http_error_response_read_failure_mapping" in state.active_components
    assert "typed_public_http_incomplete_body_read_failure_mapping" in state.active_components
    assert "deterministic_public_http_error_response_resource_closure" in (state.active_components)
    assert "typed_public_http_pre_response_incomplete_read_failure_mapping" in (
        state.active_components
    )
    assert "typed_public_http_response_protocol_failure_mapping" in state.active_components
    assert "fail_closed_public_http_automatic_redirect_rejection" in state.active_components
    assert "fail_closed_public_http_initial_request_target_validation" in state.active_components
    assert "fail_closed_public_http_standard_https_target_port_policy" in state.active_components
    assert "fail_closed_public_http_bounded_query_serialization" in state.active_components
    assert "fail_closed_public_http_initial_target_length_bound" in state.active_components
    assert "fail_closed_public_http_bounded_user_agent_validation" in state.active_components
    assert "fail_closed_public_http_maximum_timeout_policy" in state.active_components
    assert "fail_closed_public_http_bounded_response_header_projection" in (state.active_components)
    assert "public_trade_disconnect_sparse_window_restart_recovery_drill" in state.active_components
    assert (
        "versioned_public_provider_schema_fixture_contract_and_drift_runbook"
        in state.active_components
    )
    assert "continuous_public_trade_collection_operating_contract" in state.active_components
    assert "continuous_public_trade_closed_window_planner_contracts" in state.active_components
    assert "continuous_public_trade_stream_persistence_contract" in state.active_components
    assert "continuous_public_trade_stream_persistence_codec_contracts" in state.active_components
    assert "continuous_public_trade_stream_store_port_contracts" in state.active_components
    assert len(state.open_tasks) == 2
    task_037, task_063 = state.open_tasks
    assert task_037.task_id == "TASK-037"
    assert task_037.status == "blocked"
    assert task_037.risk_tier == 3
    assert task_037.requires_human_approval is True
    assert task_063.task_id == "TASK-063"
    assert task_063.action == "phase2.continuous_public_trade_stream_physical_store_architecture"
    assert task_063.status == "ready"
    assert task_063.risk_tier == 1
    assert task_063.requires_human_approval is False
    assert state.blockers == (
        "TASK-037 awaits owner-supplied exact restricted-package inputs in an approved governance "
        "location before independent Risk and Security review and the project-owner decision; "
        "authorization remains denied.",
    )
    assert state.next_action.task_id == "TASK-063"
    assert (
        state.next_action.action
        == "phase2.continuous_public_trade_stream_physical_store_architecture"
    )
    assert any(decision.decision_id == "ADR-0029" for decision in state.recent_decisions)
    assert any(decision.decision_id == "ADR-0030" for decision in state.recent_decisions)


def test_project_state_references_existing_governance_artifacts() -> None:
    state = load_project_state(PROJECT_STATE_PATH)
    root_readme = ROOT_README_PATH.read_text(encoding="utf-8")
    risk_register = RISK_REGISTER_PATH.read_text(encoding="utf-8")
    backlog = BACKLOG_PATH.read_text(encoding="utf-8")
    market_data_contract = MARKET_DATA_CONTRACT_PATH.read_text(encoding="utf-8")
    roadmap = ROADMAP_PATH.read_text(encoding="utf-8")
    decision_index = DECISION_INDEX_PATH.read_text(encoding="utf-8")
    adr_0029 = ADR_0029_PATH.read_text(encoding="utf-8")
    adr_0030 = ADR_0030_PATH.read_text(encoding="utf-8")

    for decision in state.recent_decisions:
        assert (REPOSITORY_ROOT / decision.artifact).is_file()
    for risk_id in state.known_risks:
        assert f"| {risk_id} |" in risk_register

    next_action_section = backlog.split("## Next Action", maxsplit=1)[1].split(
        "## Blocked, Awaiting Owner-Supplied Restricted Inputs", maxsplit=1
    )[0]
    blocked_section = backlog.split(
        "## Blocked, Awaiting Owner-Supplied Restricted Inputs",
        maxsplit=1,
    )[1].split(
        "## Recently Completed",
        maxsplit=1,
    )[0]
    completed_section = backlog.split("## Recently Completed", maxsplit=1)[1].split(
        "## Queued, Not Yet Approved", maxsplit=1
    )[0]
    queued_section = backlog.split("## Queued, Not Yet Approved", maxsplit=1)[1].split(
        "## Backlog Rules", maxsplit=1
    )[0]
    task_031_section = completed_section.split("### TASK-031 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_032_section = completed_section.split("### TASK-032 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_033_section = completed_section.split("### TASK-033 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_034_section = completed_section.split("### TASK-034 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_035_section = completed_section.split("### TASK-035 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_036_section = completed_section.split("### TASK-036 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_038_section = completed_section.split("### TASK-038 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_039_section = completed_section.split("### TASK-039 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_040_section = completed_section.split("### TASK-040 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_041_section = completed_section.split("### TASK-041 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_042_section = completed_section.split("### TASK-042 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_043_section = completed_section.split("### TASK-043 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_044_section = completed_section.split("### TASK-044 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_045_section = completed_section.split("### TASK-045 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_046_section = completed_section.split("### TASK-046 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_047_section = completed_section.split("### TASK-047 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_048_section = completed_section.split("### TASK-048 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_049_section = completed_section.split("### TASK-049 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_050_section = completed_section.split("### TASK-050 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_051_section = completed_section.split("### TASK-051 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_052_section = completed_section.split("### TASK-052 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_053_section = completed_section.split("### TASK-053 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_054_section = completed_section.split("### TASK-054 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_055_section = completed_section.split("### TASK-055 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_056_section = completed_section.split("### TASK-056 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_057_section = completed_section.split("### TASK-057 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_058_section = completed_section.split("### TASK-058 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_059_section = completed_section.split("### TASK-059 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_060_section = completed_section.split("### TASK-060 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_061_section = completed_section.split("### TASK-061 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_062_section = completed_section.split("### TASK-062 ", maxsplit=1)[1].split(
        "### TASK-", maxsplit=1
    )[0]
    task_063_section = next_action_section
    next_action_prose = collapse_whitespace(next_action_section)
    task_060_prose = collapse_whitespace(task_060_section)
    task_061_prose = collapse_whitespace(task_061_section)
    task_062_prose = collapse_whitespace(task_062_section)
    root_readme_prose = collapse_whitespace(root_readme)
    market_data_prose = collapse_whitespace(market_data_contract)
    risk_register_prose = collapse_whitespace(risk_register)
    roadmap_prose = collapse_whitespace(roadmap)
    adr_0029_prose = collapse_whitespace(adr_0029)
    adr_0030_prose = collapse_whitespace(adr_0030)
    assert next_action_section.count("### TASK-") == 1
    assert f"### {state.next_action.task_id} " in next_action_section
    assert f"`{state.next_action.action}`" in next_action_section
    assert "- **Status:** READY" in next_action_section
    assert "- **Risk tier:** RISK 1" in next_action_section
    assert "- **Human approval:** NOT REQUIRED" in next_action_section
    assert "`phase2.continuous_public_trade_stream_physical_store_architecture`" in (
        task_063_section
    )
    assert "TASK-062 is complete after exact owner approval" in next_action_prose
    assert "PR #63 merge commit `6b959670e1737bd10585b437786d06408a22e31d`" in (next_action_prose)
    assert "successful required target-branch CI run `30373228787`" in next_action_prose
    assert (
        "architecture, evidence planning, documentation, and coordinated governance only"
        in next_action_prose
    )
    assert "physical stream-store architecture and evidence plan" in next_action_prose
    assert "exact epoch representation" in next_action_prose
    assert "transaction/schema/index mapping" in next_action_prose
    assert "retention/compaction" in next_action_prose
    assert "migration" in next_action_prose
    assert "backup/restore" in next_action_prose
    assert "crash evidence" in next_action_prose
    assert "finite capacity/performance evidence" in next_action_prose
    assert "No production source or adapter" in next_action_prose
    assert "database or schema creation" in next_action_prose
    assert "filesystem/network/provider I/O" in next_action_prose
    assert "Do not alter TASK-059 behavior, TASK-061 bytes, or TASK-062 port semantics" in (
        next_action_prose
    )
    assert "TASK-037" in next_action_prose
    assert "authorization remains denied" in next_action_prose
    assert (
        "The decision adds no physical capability, I/O, dependency, lockfile change, runtime import"
        in next_action_prose
    )
    assert "- **Status:** COMPLETE" in task_062_section
    assert "`phase2.continuous_public_trade_stream_store_port_contracts`" in task_062_section
    assert "Owner-approved PR" in task_062_prose
    assert "accepted head `21c3171de68ee5ec42eeff82ce1171008f2e854b`" in task_062_prose
    assert "merge commit `6b959670e1737bd10585b437786d06408a22e31d`" in task_062_prose
    assert "successful required target-branch CI run" in task_062_prose
    assert "unused provider-independent port module" in task_062_prose
    assert "exact finalized TASK-061 creation and transition artifacts" in task_062_prose
    assert "`validate_continuous_public_trade_stream_audit_page`" in task_062_section
    assert "Original TASK-061 bytes remain authoritative" in task_062_prose
    assert "UUID and natural-identity uniqueness" in task_062_prose
    assert "Audit results return 1 through 100 new records" in task_062_prose
    assert "Exact historical retries are `DUPLICATE`" in task_062_prose
    assert "competing mutations are `CONFLICT`" in task_062_prose
    assert (
        "Absence, identity conflict, unsupported versions, corruption, anchor conflict, and "
        "storage unavailability remain distinct"
    ) in task_062_prose
    assert "Every output is a store-local classification" in task_062_prose
    assert (
        "only accepted receipts, `FOUND`, `PAGE`, and a validated `AT_TAIL` anchor carry bounded "
        "structural evidence"
    ) in task_062_prose
    assert "`UNAVAILABLE` carries no coherent classification" in task_062_prose
    assert "100-new-record plus one-overlap bound" in task_062_prose
    assert "A conforming future adapter must separately prove" in task_062_prose
    assert "The port performs no I/O and is not imported by runtime composition" in task_062_prose
    assert "TASK-063 may define" not in queued_section
    assert "### TASK-063" not in queued_section
    assert queued_section.count("\n- ") == 2
    assert "Implement a physical continuous public-trade stream store" in queued_section
    assert "Implement continuous public-trade runtime collection" in queued_section
    assert "- **Status:** COMPLETE" in task_061_section
    assert "`phase2.continuous_public_trade_stream_persistence_codec_contracts`" in task_061_section
    assert "side-effect-free domain module" in task_061_prose
    assert "Five canonical UTF-8 JSON codecs" in task_061_prose
    assert "Six distinct SHA-256 domains" in task_061_prose
    assert "field by field even when a fingerprint is reused" in task_061_prose
    assert "two-pass finalizer" in task_061_prose
    assert "full-range unattached TASK-059 epoch milliseconds" in task_061_prose
    assert "No port, repository, adapter, SQLite/DDL/schema" in task_061_prose
    assert blocked_section.count("### TASK-") == 1
    assert "### TASK-037 " in blocked_section
    assert "- **Status:** BLOCKED" in blocked_section
    assert "- **Risk tier:** RISK 3" in blocked_section
    assert "- **Human approval:** REQUIRED" in blocked_section
    assert "independent Risk and Security reviews" in blocked_section
    assert "Authorization remains `DENIED`" in blocked_section
    assert "Return TASK-037 to `READY` only after" in blocked_section
    assert "project-owner `APPROVE`, `REJECT`, or `REVISE` decision" in blocked_section
    assert "real deployment" in blocked_section
    assert "writer-fenced consistent/immutable snapshot procedure" in blocked_section
    assert "report destination" in blocked_section
    assert "retention/disposal boundary" in blocked_section
    assert "monitoring, and tested rollback evidence" in blocked_section
    assert "Do not inspect, resolve, check, or" in blocked_section
    assert "governance-artifact writes are the only filesystem mutation" in blocked_section
    assert "add serialization or scanner code" in blocked_section
    assert "Any approved scanner remains a separately scoped later task" in blocked_section
    assert "- **Status:** COMPLETE" in task_031_section
    assert "`phase2.canonical_utc_preflight_timestamp_evidence_foundation`" in task_031_section
    assert "- **Status:** COMPLETE" in task_032_section
    assert (
        "`phase2.canonical_utc_preflight_timestamp_parse_evidence_foundation`" in task_032_section
    )
    assert "- **Status:** COMPLETE" in task_033_section
    assert (
        "`phase2.canonical_utc_preflight_timestamp_canonical_candidate_evidence_foundation`"
        in task_033_section
    )
    assert "- **Status:** COMPLETE" in task_034_section
    assert (
        "`phase2.canonical_utc_preflight_timestamp_candidate_census_evidence_foundation`"
        in task_034_section
    )
    assert "- **Status:** COMPLETE" in task_035_section
    assert (
        "`phase2.canonical_utc_preflight_timestamp_candidate_census_bundle_evidence_foundation`"
        in task_035_section
    )
    assert "- **Status:** COMPLETE" in task_036_section
    assert (
        "`phase2.canonical_utc_preflight_operator_authorization_request_contract_foundation`"
        in task_036_section
    )
    assert "private exact TASK-035" in task_036_section
    assert "eight symbolic slots prove synthetic family coverage only" in task_036_section
    assert "do not assert that a future real" in task_036_section
    assert "- **Status:** COMPLETE" in task_038_section
    assert "`phase2.public_provider_payload_failure_boundary_hardening`" in task_038_section
    assert "UTF-8, malformed JSON" in task_038_section
    assert "excessive nesting" in task_038_section
    assert "non-retryable typed `INVALID_PAYLOAD`" in task_038_section
    assert "TASK-037 authority" in task_038_section
    assert "- **Status:** COMPLETE" in task_039_section
    assert "`phase2.exact_candle_persistence_evidence_validation`" in task_039_section
    assert "`src/wealth/application/ingestion.py`" in task_039_section
    assert "`src/wealth/ports/market.py`" in task_039_section
    assert "`tests/unit/test_historical_candle_persistence_evidence.py`" in task_039_section
    assert "one ordered status-coherent" in task_039_section
    assert "hostile-store tests" in task_039_section
    assert "physically wrote the first page" in task_039_section
    assert "does not independently prove physical durability" in task_039_section
    assert "order-flow ingestion" in task_039_section
    assert "- **Status:** COMPLETE" in task_040_section
    assert "`phase2.exact_order_flow_persistence_evidence_validation`" in task_040_section
    assert "`src/wealth/application/order_flow_ingestion.py`" in task_040_section
    assert "`src/wealth/ports/order_flow.py`" in task_040_section
    assert "`tests/unit/test_order_flow_persistence_evidence.py`" in task_040_section
    assert "one ordered status-coherent outcome" in task_040_section
    assert "wrong-family" in task_040_section
    assert "valid zero-record" in task_040_section
    assert "physically wrote the first window" in task_040_section
    assert "independently prove physical durability" in task_040_section
    assert "- **Status:** COMPLETE" in task_041_section
    assert "`phase2.finite_public_http_timeout_boundary_validation`" in task_041_section
    assert "`src/wealth/adapters/http.py`" in task_041_section
    assert "`tests/unit/test_http_adapter.py`" in task_041_section
    assert "`NaN`, positive and negative infinity" in task_041_section
    assert "never reach `Request`, `urlopen`" in task_041_section
    assert "literal integer and" in task_041_section
    assert "total wall-clock deadline" in task_041_section
    assert "runtime wiring" in task_041_section
    assert "- **Status:** COMPLETE" in task_042_section
    assert "`phase2.strict_public_http_response_byte_limit_validation`" in task_042_section
    assert "`src/wealth/adapters/http.py`" in task_042_section
    assert "`tests/unit/test_http_adapter.py`" in task_042_section
    assert "built-in integer response limit" in task_042_section
    assert "current/default hard ceiling" in task_042_section
    assert "integer subclasses" in task_042_section
    assert "`cap + 1`" in task_042_section
    assert "real `HTTPError` paths" in task_042_section
    assert "not a total wall-clock" in task_042_section
    assert "- **Status:** COMPLETE" in task_043_section
    assert "`phase2.typed_public_http_error_response_read_failure_mapping`" in task_043_section
    assert "`src/wealth/adapters/http.py`" in task_043_section
    assert "`tests/unit/test_http_adapter.py`" in task_043_section
    assert "`URLError`, `TimeoutError`, and `OSError`" in task_043_section
    assert '`HttpTransportError("public HTTP GET failed")`' in task_043_section
    assert "one `cap + 1` body read and one `urlopen` call" in task_043_section
    assert "no retry or partial response" in task_043_section
    assert "`IncompleteRead`, which is not an" in task_043_section
    assert "resource closure" in task_043_section
    assert "- **Status:** COMPLETE" in task_044_section
    assert "`phase2.typed_public_http_incomplete_body_read_failure_mapping`" in task_044_section
    assert "`src/wealth/adapters/http.py`" in task_044_section
    assert "`tests/unit/test_http_adapter.py`" in task_044_section
    assert "real `http.client.IncompleteRead`" in task_044_section
    assert '`HttpTransportError("public HTTP GET failed")`' in task_044_section
    assert "one `cap + 1` read and one `urlopen` call" in task_044_section
    assert "partial provider bytes" in task_044_section
    assert "only around `response.read`" in task_044_section
    assert "raised during response-context entry remains" in task_044_section
    assert "unmapped" in task_044_section
    assert "resource closure" in task_044_section
    assert "- **Status:** COMPLETE" in task_045_section
    assert "`phase2.deterministic_public_http_error_response_resource_closure`" in task_045_section
    assert "`src/wealth/adapters/http.py`" in task_045_section
    assert "`tests/unit/test_http_adapter.py`" in task_045_section
    assert "one explicit cleanup attempt" in task_045_section
    assert "real built-in `HTTPError` smoke test" in task_045_section
    assert "`HTTPError` subclass prove exact-limit" in task_045_section
    assert "`OSError`-family or `IncompleteRead`" in task_045_section
    assert "no cleanup failure can replace" in task_045_section
    assert "unsupported close-only failures remain unchanged" in task_045_section
    assert "not claimed to have closed" in task_045_section
    assert "- **Status:** COMPLETE" in task_046_section
    assert (
        "`phase2.typed_public_http_pre_response_incomplete_read_failure_mapping`"
        in task_046_section
    )
    assert "`src/wealth/adapters/http.py`" in task_046_section
    assert "`tests/unit/test_http_adapter.py`" in task_046_section
    assert "raised directly by `urlopen`" in task_046_section
    assert '`HttpTransportError("public HTTP GET failed")`' in task_046_section
    assert "one acquisition call" in task_046_section
    assert "partial provider bytes" in task_046_section
    assert "response entry or exit" in task_046_section
    assert "base `HTTPException` remain raw" in task_046_section
    assert "HTTP-error cleanup" in task_046_section
    assert "redirects remain enabled" in task_046_section
    assert "response cap" in task_046_section
    assert "changed destination" in task_046_section
    assert "governed redirect-policy task" in task_046_section
    assert "- **Status:** COMPLETE" in task_047_section
    assert "`phase2.typed_public_http_response_protocol_failure_mapping`" in task_047_section
    assert "`src/wealth/adapters/http.py`" in task_047_section
    assert "`tests/unit/test_http_adapter.py`" in task_047_section
    assert "`BadStatusLine`, `LineTooLong`" in task_047_section
    assert "`UnknownProtocol`" in task_047_section
    assert "directly by `urlopen`" in task_047_section
    assert "successful-response body read" in task_047_section
    assert "`HTTPError` body read" in task_047_section
    assert '`HttpTransportError("public HTTP GET failed")`' in task_047_section
    assert "original exception as direct cause" in task_047_section
    assert "three-exception-by-three-seam matrix" in task_047_section
    assert "one configured" in task_047_section
    assert "one cleanup attempt" in task_047_section
    assert "no retry or partial" in task_047_section
    assert "no provider protocol detail" in task_047_section
    assert "base `HTTPException` and `InvalidURL` remain raw at all three seams" in task_047_section
    assert "response entry or exit" in task_047_section
    assert "HTTP-error cleanup" in task_047_section
    assert "Default redirects remain enabled" in task_047_section
    assert "outside the adapter cap" in task_047_section
    assert "changed destination" in task_047_section
    assert "TASK-048 governs" in task_047_section
    assert "- **Status:** COMPLETE" in task_048_section
    assert "`phase2.fail_closed_public_http_automatic_redirect_rejection`" in task_048_section
    assert "`src/wealth/adapters/http.py`" in task_048_section
    assert "`tests/unit/test_http_adapter.py`" in task_048_section
    for status_code in (301, 302, 303, 307, 308):
        assert str(status_code) in task_048_section
    assert "before parsing `Location` or" in task_048_section
    assert "`URI`, reading or closing the redirect body" in task_048_section
    assert "unsupported-scheme, and" in task_048_section
    assert "malformed targets" in task_048_section
    assert "five-status-by-fifteen-target real-opener matrix" in task_048_section
    assert "one original GET" in task_048_section
    assert "one `max_response_bytes + 1` read" in task_048_section
    assert "one cleanup attempt" in task_048_section
    assert "no retry, no second" in task_048_section
    assert "primary-failure precedence" in task_048_section
    assert "proxy/TLS handler defaults" in task_048_section
    assert "process-global urllib opener is neither installed nor mutated" in task_048_section
    assert "Initial request-target validation remains incomplete" in task_048_section
    assert "non-HTTPS scheme handlers" in task_048_section
    assert "TASK-049 governs that residual risk" in task_048_section
    assert "- **Status:** COMPLETE" in task_049_section
    assert "`phase2.fail_closed_public_http_initial_request_target_validation`" in task_049_section
    assert "`src/wealth/adapters/http.py`" in task_049_section
    assert "`tests/unit/test_http_adapter.py`" in task_049_section
    assert "`docs/ROADMAP.md`" in task_049_section
    assert "After finite-positive timeout validation" in task_049_section
    assert "before any query-mapping operation" in task_049_section
    assert "absolute credential-free HTTPS" in task_049_section
    assert "lone surrogate code point" in task_049_section
    assert "context-suppressed" in task_049_section
    assert (
        '`ValueError("url must be an absolute credential-free HTTPS endpoint without query or '
        'fragment")`' in task_049_section
    )
    assert "Every percent sign in the authority is rejected" in task_049_section
    assert "NFKC inspection of the authority" in task_049_section
    assert "IDNA could emit as a percent sign" in task_049_section
    assert "backslash, whitespace, C0, or DEL" in task_049_section
    assert "accepted URL is never reconstructed or normalized" in task_049_section
    assert "117-target invalid corpus" in task_049_section
    assert "query remains untouched" in task_049_section
    assert "`urlencode`, `Request`, the private opener, DNS, network" in task_049_section
    assert "63-target raw subset fails before `urlsplit`" in task_049_section
    assert "parser tests prove failure context suppression" in task_049_section
    assert "five invalid timeout cases retain their earlier" in task_049_section
    assert "18 valid targets preserve their exact text" in task_049_section
    assert "not a hostname, DNS, IP-routability, or SSRF guarantee" in task_049_section
    assert "explicit ports 1 through 65,535 remain accepted" in task_049_section
    assert "TASK-050 governs the remaining target-port" in task_049_section
    assert "- **Status:** COMPLETE" in task_050_section
    assert "`phase2.fail_closed_public_http_standard_https_target_port_policy`" in task_050_section
    assert "`src/wealth/adapters/http.py`" in task_050_section
    assert "`tests/unit/test_http_adapter.py`" in task_050_section
    assert "After TASK-049 structural validation" in task_050_section
    assert "before any query-mapping operation" in task_050_section
    assert "omitted caller target port" in task_050_section
    assert "parses as 443" in task_050_section
    assert "ports 1, 80, 442, 444, 8,443, and 65,535" in task_050_section
    assert "zero-padded nonstandard port" in task_050_section
    assert "nonstandard IPv6 and IPvFuture ports" in task_050_section
    assert '`ValueError("url must use the standard HTTPS target port")`' in task_050_section
    assert "no direct cause or hidden" in task_050_section
    assert "no query access or serialization" in task_050_section
    assert "`Request` construction, private-opener" in task_050_section
    assert "malformed, percent-encoded, empty, non-numeric, signed" in task_050_section
    assert "greater-than-65,535 ports retain TASK-049's exact structural error" in task_050_section
    assert "Implicit port 443 and explicit numeric 443" in task_050_section
    assert "preserve the exact original URL text" in task_050_section
    assert "all five active provider default endpoints remain accepted" in task_050_section
    assert "configured proxy peer may use a non-443 port" in task_050_section
    assert "No provider or hostname allowlist" in task_050_section
    assert "Query serialization remains unbounded" in task_050_section
    assert "TASK-051 governs that residual finite-work risk" in task_050_section
    assert "- **Status:** COMPLETE" in task_051_section
    assert "`phase2.fail_closed_public_http_bounded_query_serialization`" in task_051_section
    assert "`src/wealth/adapters/http.py`" in task_051_section
    assert "`tests/unit/test_http_adapter.py`" in task_051_section
    assert "After timeout, TASK-049 structural-target" in task_051_section
    assert "TASK-050 target-port validation" in task_051_section
    assert "before `urlencode`" in task_051_section
    assert "one bounded query snapshot" in task_051_section
    assert "calls `items()`" in task_051_section
    assert "starts its iterator once" in task_051_section
    assert "does not call `len(query)`" in task_051_section
    assert "directly iterate the mapping" in task_051_section
    assert "second item pass" in task_051_section
    assert "length hint" in task_051_section
    assert "at most 33 yielded items" in task_051_section
    assert "zero" in task_051_section
    assert "through 32 exact built-in tuple pairs" in task_051_section
    assert "8,192 Python characters" in task_051_section
    assert "tuple subclass" in task_051_section
    assert "string-subclass component" in task_051_section
    assert (
        '`ValueError("query must contain at most 32 built-in string pairs totaling at most 8192 '
        'characters")`' in task_051_section
    )
    assert "no `urlencode`, `Request`, private-opener" in task_051_section
    assert "including `ValueError`" in task_051_section
    assert "same raw objects" in task_051_section
    assert "Forty-two new deterministic cases" in task_051_section
    assert "424-test" in task_051_section
    assert "count rejection before 33rd-item inspection" in task_051_section
    assert "finite and synthetic-unbounded 33rd items" in task_051_section
    assert "Unicode exact-8,192 and 8,193 character boundaries" in task_051_section
    assert "nine" in task_051_section
    assert "invalid pair/type forms" in task_051_section
    assert "five" in task_051_section
    assert "mapping-failure seams" in task_051_section
    assert "custom runtime error and raw" in task_051_section
    assert "all five active provider request" in task_051_section
    assert "three through six pairs" in task_051_section
    assert "no query-content, normalization, or multi-value policy" in task_051_section
    assert "no configured size bound" in task_051_section
    assert "TASK-052 governs that residual finite-work risk" in task_051_section
    assert "- **Status:** COMPLETE" in task_052_section
    assert "`phase2.fail_closed_public_http_initial_target_length_bound`" in task_052_section
    assert "`src/wealth/adapters/http.py`" in task_052_section
    assert "`tests/unit/test_http_adapter.py`" in task_052_section
    assert "After finite-positive timeout validation" in task_052_section
    assert "first line of the private" in task_052_section
    assert "non-polymorphic `str.__len__`" in task_052_section
    assert "longer than 8,192 Python characters" in task_052_section
    assert '`ValueError("url must contain at most 8192 characters")`' in task_052_section
    assert "before literal" in task_052_section
    assert "membership or character scanning" in task_052_section
    assert "`urlsplit`, hostname, username, port, or NFKC inspection" in task_052_section
    assert "Lying-length and raising-length/content `str` subclasses" in task_052_section
    assert "without dispatching to caller overrides" in task_052_section
    assert "intentionally precedes TASK-049 structure" in task_052_section
    assert "every target at or below the limit retains" in task_052_section
    assert "ASCII and multi-byte Unicode" in task_052_section
    assert "exactly 8,192 characters preserve every original character" in task_052_section
    assert "8,193-character targets fail without query or request work" in task_052_section
    assert "Nineteen new deterministic" in task_052_section
    assert "443-test adapter suite" in task_052_section
    assert "all five invalid timeouts" in task_052_section
    assert "two adversarial" in task_052_section
    assert "oversized subclasses and one exact-limit false-long subclass" in task_052_section
    assert "three oversized combined-error" in task_052_section
    assert "three exact-limit prior-error forms" in task_052_section
    assert "39, 42, 42, 45, and 48" in task_052_section
    assert "Python characters rather than encoded bytes" in task_052_section
    assert "no request-line compatibility or total-wall-clock claim" in task_052_section
    assert "User-Agent remains unbounded" in task_052_section
    assert "TASK-053 governs that residual request-construction risk" in task_052_section
    assert "- **Status:** COMPLETE" in task_053_section
    assert "`phase2.fail_closed_public_http_bounded_user_agent_validation`" in task_053_section
    assert "`src/wealth/adapters/http.py`" in task_053_section
    assert "`tests/unit/test_http_adapter.py`" in task_053_section
    assert "preserving `max_response_bytes` validation and its first precedence" in task_053_section
    assert "exact built-in" in task_053_section
    assert "`str`" in task_053_section
    assert "1 through 256 Python characters" in task_053_section
    assert "U+0020 through U+007E" in task_053_section
    assert (
        '`ValueError("user_agent must be a built-in string of 1 to 256 visible ASCII '
        'characters")`' in task_053_section
    )
    assert "before URL, query, `urlencode`, `Request`, private-opener" in task_053_section
    assert "Exact-type rejection dispatches no caller string hooks" in task_053_section
    assert "257-character values" in task_053_section
    assert "fail before character inspection" in task_053_section
    assert "Sixty-six new deterministic cases" in task_053_section
    assert "509-test" in task_053_section
    assert "twelve invalid-response-limit precedence combinations" in task_053_section
    assert "five invalid types" in task_053_section
    assert "all 32 C0 controls" in task_053_section
    assert "five representative non-ASCII characters" in task_053_section
    assert "two lone surrogates" in task_053_section
    assert "sweeping DEL through every position 0 through 255" in task_053_section
    assert "four accepted" in task_053_section
    assert "lengths 1, 255, and 256" in task_053_section
    assert "complete visible-ASCII range" in task_053_section
    assert '"WEALTH/0.1 public-market-data"' in task_053_section
    assert "leading and trailing spaces and punctuation" in task_053_section
    assert "forwarded" in task_053_section
    assert "exactly once as the sole `User-Agent` header" in task_053_section
    assert "No value is normalized, trimmed, truncated, repaired" in task_053_section
    assert "no privacy or total-header-block guarantee" in task_053_section
    assert "timeouts remain without an upper bound" in task_053_section
    assert "TASK-054 governs that residual per-operation wait risk" in task_053_section
    assert "- **Status:** COMPLETE" in task_054_section
    assert "`phase2.fail_closed_public_http_maximum_timeout_policy`" in task_054_section
    assert "`src/wealth/ports/http.py`" in task_054_section
    assert "`src/wealth/adapters/http.py`" in task_054_section
    assert "`src/wealth/adapters/binance.py`" in task_054_section
    assert "`src/wealth/adapters/coinbase.py`" in task_054_section
    assert "`src/wealth/adapters/binance_order_flow.py`" in task_054_section
    assert "`tests/unit/test_http_adapter.py`" in task_054_section
    assert "`tests/unit/test_binance_public_candles.py`" in task_054_section
    assert "`tests/unit/test_coinbase_public_candles.py`" in task_054_section
    assert "`tests/unit/test_binance_public_aggregate_trades.py`" in task_054_section
    assert "`MAX_PUBLIC_HTTP_TIMEOUT_SECONDS = 120.0`" in task_054_section
    assert "all four boundaries" in task_054_section
    assert "follows TASK-041's finite-positive check" in task_054_section
    assert '`ValueError("timeout_seconds must be finite and positive")`' in task_054_section
    assert '`ValueError("timeout_seconds must be at most 120")`' in task_054_section
    assert "before URL length or content, query, `Request`, opener" in task_054_section
    assert "before endpoint validation, clock, query, injected HTTP" in task_054_section
    assert "Forty new deterministic cases" in task_054_section
    assert "640 to 680 tests" in task_054_section
    assert "`test_http_adapter.py` has 518 tests (+9)" in task_054_section
    assert "Binance candle has 49 (+11)" in task_054_section
    assert "Coinbase candle has 54 (+9)" in task_054_section
    assert "Binance aggregate-trade has 59 (+11)" in task_054_section
    assert "next float above 120" in task_054_section
    assert "largest finite float" in task_054_section
    assert "1,001-digit integer" in task_054_section
    assert "next float below 120" in task_054_section
    assert "exact built-in integer and float 120" in task_054_section
    assert "float subclass at" in task_054_section
    assert "including subclass identity" in task_054_section
    assert "All five active provider" in task_054_section
    assert "isolated mutation audit killed all 14 of 14 mutants" in task_054_section
    assert "with zero" in task_054_section
    assert "survivors and zero harness errors" in task_054_section
    assert "removed or changed cap" in task_054_section
    assert "`>=` off-by-one" in task_054_section
    assert "hardcoded or unshared module policy" in task_054_section
    assert "float-subclass" in task_054_section
    assert "coercion or identity loss" in task_054_section
    assert "wrong message, injected cause" in task_054_section
    assert "provider clock work before the cap" in task_054_section
    assert "default drift" in task_054_section
    assert "forwarded-timeout drift" in task_054_section
    assert "task adds no" in task_054_section
    assert "exact numeric-type, subclass" in task_054_section
    assert "does not separately" in task_054_section
    assert "bound DNS, multiple operations" in task_054_section
    assert "header projection" in task_054_section
    assert "still has no adapter-level pair-count" in task_054_section
    assert "TASK-055 governs that" in task_054_section
    assert "residual response-metadata risk" in task_054_section
    assert "- **Status:** COMPLETE" in task_055_section
    assert "`phase2.fail_closed_public_http_bounded_response_header_projection`" in task_055_section
    assert "`src/wealth/adapters/http.py`" in task_055_section
    assert "`tests/unit/test_http_adapter.py`" in task_055_section
    assert "`docs/contracts/MARKET_DATA.md`" in task_055_section
    assert "one-byte-sentinel body read and body-size decision" in task_055_section
    assert "one shared bounded header snapshot" in task_055_section
    assert "calls `headers.items()` once" in task_055_section
    assert "starts its iterator once" in task_055_section
    assert "requests no" in task_055_section
    assert "length hint" in task_055_section
    assert "no direct message iteration or second pass" in task_055_section
    assert "pulls at most 101 times" in task_055_section
    assert "Zero through 100 yielded pairs" in task_055_section
    assert "`len(name) + len(value)`" in task_055_section
    assert "65,536 Python characters" in task_055_section
    assert "101st pair fails before" in task_055_section
    assert "pair is unpacked or either component is inspected" in task_055_section
    assert "65,537th cumulative character fails" in task_055_section
    assert (
        '`HttpTransportError("public HTTP response headers exceeded the configured limit")`'
        in task_055_section
    )
    assert "Accepted order, duplicate names, original casing" in task_055_section
    assert "`Retry-After` behavior remain exact" in task_055_section
    assert "Body-read failure and body" in task_055_section
    assert "oversize retain precedence without header access" in task_055_section
    assert "has no" in task_055_section
    assert "direct cause or hidden context" in task_055_section
    assert "originating provider error as both direct cause and active context" in task_055_section
    assert "cleanup exactly once" in task_055_section
    assert "cleanup failure cannot replace it" in task_055_section
    assert "same raw objects" in task_055_section
    assert "natural implicit provider-error context" in task_055_section
    assert "Forty-one new deterministic cases" in task_055_section
    assert "from 518 to 559 tests" in task_055_section
    assert "finite and endless 101st yields" in task_055_section
    assert "pair-unpack poisoning" in task_055_section
    assert "cumulative Unicode" in task_055_section
    assert "An isolated mutation audit killed all 24 of 24 mutants" in task_055_section
    assert "zero survivors and zero harness" in task_055_section
    assert "errors" in task_055_section
    assert "101st-pair guard ordering" in task_055_section
    assert "`>=` drift" in task_055_section
    assert "omitted name or value volume" in task_055_section
    assert "non-cumulative and UTF-8-byte counting" in task_055_section
    assert "projection before body read or size decision" in task_055_section
    assert "normalization, reordering, wrong message or cause" in task_055_section
    assert "cleanup replacing" in task_055_section
    assert "raw-origin wrapping" in task_055_section
    assert "`HTTPError`/`OSError` subclass-identity bypass" in task_055_section
    assert "complete suite passed 1,649 tests" in task_055_section
    assert "lockfile, formatting, lint, strict typing, dependency" in task_055_section
    assert "local health checks also passed" in task_055_section
    assert "only an adapter-controlled projection bound" in task_055_section
    assert "after standard-library parsing and prior" in task_055_section
    assert "allocation" in task_055_section
    assert "does not bound wire-header bytes" in task_055_section
    assert "adds no privacy, redaction" in task_055_section
    assert "TASK-037 authority" in task_055_section
    assert "- **Status:** COMPLETE" in task_060_section
    assert (
        "`phase2.continuous_public_trade_stream_persistence_contract_decision`" in task_060_section
    )
    assert (
        "`docs/decisions/0029-continuous-public-trade-stream-persistence-contract.md`"
        in task_060_section
    )
    assert "design-only persistence contract" in task_060_prose
    assert "exact TASK-059 checkpoint is the durable current state" in task_060_prose
    assert "versioned compare-and-swap transitions fail closed" in task_060_prose
    assert "complete canonical deterministic `child_creation_payload`" in task_060_prose
    assert "creation fingerprint is intentionally non-invertible" in task_060_prose
    assert "pause reason is not authority evidence" in task_060_prose
    assert "completed child ID is not accepted completion proof" in task_060_prose
    assert "unknown commit outcome" in task_060_prose
    assert "six distinct child, stream-envelope" in task_060_prose
    assert "complete immutable stream-policy projection" in task_060_prose
    assert "anchored history attestation" in task_060_prose
    assert "accepted history root, successor version, candidate child" in task_060_prose
    assert "non-integrity operational hold" in task_060_prose
    assert "ambiguity stops canonical admission and progress" in task_060_prose
    assert "pure two-pass plan/payload/replan proof" in task_060_prose
    assert "disable-to-current-bounded-flow rollback" in task_060_prose
    assert "cannot be represented as Python datetimes" in task_060_prose
    assert "No production source, codec, port/repository/adapter" in task_060_prose
    assert "TASK-061 is the separately governed" in task_060_prose
    assert "TASK-037 remains blocked and authorization remains" in task_060_prose
    assert "- **Status:** COMPLETE" in task_059_section
    assert "`phase2.continuous_public_trade_closed_window_planner_contracts`" in task_059_section
    assert "`src/wealth/domain/continuous_public_trade.py`" in task_059_section
    assert "`tests/unit/test_continuous_public_trade_contracts.py`" in task_059_section
    assert "`ContinuousPublicTradePolicy`" in task_059_section
    assert "`ContinuousPublicTradeStreamCheckpoint`" in task_059_section
    assert "`plan_continuous_public_trade_window`" in task_059_section
    assert "`HELD`" in task_059_section
    assert "`WAITING`" in task_059_section
    assert "`ATTACHED_JOB`" in task_059_section
    assert "performs no I/O" in task_059_section
    assert "module" in task_059_section
    assert "unused" in task_059_section
    assert "no repository/adapter, SQLite or schema" in task_059_section
    assert "TASK-060 was the" in task_059_section
    assert "separate design-only persistence-contract decision" in task_059_section
    assert "complete under ADR-0029" in task_059_section
    assert "TASK-037 remains blocked with authorization" in task_059_section
    assert "- **Status:** COMPLETE" in task_058_section
    assert "`phase2.continuous_public_trade_collection_operating_contract_decision`" in (
        task_058_section
    )
    assert "`docs/decisions/0028-continuous-public-trade-collection-operating-contract.md`" in (
        task_058_section
    )
    assert "one conceptual single-host composition" in task_058_section
    assert "unselected external trigger" in task_058_section
    assert "finite-run" in task_058_section
    assert "existing explicitly invoked bounded orchestrator" in task_058_section
    assert "outer fresh-UUID fence" in task_058_section
    assert "exact pending leaf" in task_058_section
    assert "evidence-first checkpoint" in task_058_section
    assert "idempotent refetch" in task_058_section
    assert "shared durable" in task_058_section
    assert "single-host request-budget gate" in task_058_section
    assert "separates three conceptual layers" in task_058_section
    assert "Durable stream" in task_058_section
    assert "is `ACTIVE` or `PAUSED`" in task_058_section
    assert "schema drift is a scoped pause reason" in task_058_section
    assert "external disabled-by-default posture" in task_058_section
    assert "finite service run moves from `STARTING` to `RUNNING`" in task_058_section
    assert "`STOPPED`, `PAUSED`, `FAILED`, or `RUN_LIMIT`" in task_058_section
    assert "bounded job keeps" in task_058_section
    assert "`PENDING`, `RUNNING`, `PAUSED`, `FAILED`, and `COMPLETED`" in task_058_section
    assert "`waiting`, `caught_up`, and" in task_058_section
    assert "`work_limit_reached` are outcomes rather than lifecycle states" in task_058_section
    assert "clean bounded-job `PAUSED` keeps the" in task_058_section
    assert "stream `ACTIVE` and its exact attachment" in task_058_section
    assert "failure, conflict, lost lease" in task_058_section
    assert "requires a manual stream pause" in task_058_section
    assert "clean service stop" in task_058_section
    assert "leaves stream state, cursor, and attachment unchanged" in task_058_section
    assert "closed epoch-aligned half-open UTC windows" in task_058_section
    assert "no self-scheduling state" in task_058_section
    assert "A reopen must use fresh outer and child" in task_058_section
    assert "continuous cursor only" in task_058_section
    assert "cross-database" in task_058_section
    assert "manual exact-variant or inseparable-parser hold" in task_058_section
    assert "no detector, automatic pause, remediation, or" in task_058_section
    assert "Source health remains causal" in task_058_section
    assert "health would separately distinguish" in task_058_section
    assert "complete provider/cadence/backlog/range/" in task_058_section
    assert "disable-to-current-bounded-flow rollback" in task_058_section
    assert "documentation and governance only" in task_058_section
    assert "selected future component is not implemented" in task_058_section
    assert "TASK-059 is a separately governed pure-contract increment" in task_058_section
    assert "TASK-037 remains" in task_058_section
    assert "blocked and authorization remains denied" in task_058_section
    assert "- **Status:** COMPLETE" in task_057_section
    assert "`phase2.versioned_public_provider_schema_fixtures_and_drift_runbook`" in (
        task_057_section
    )
    assert "`tests/fixtures/public_provider_schema/v1/manifest.json`" in task_057_section
    assert "exactly five synthetic JSON" in task_057_section
    assert "`tests/unit/test_public_provider_schema_fixtures.py`" in task_057_section
    assert "`docs/runbooks/PUBLIC_PROVIDER_SCHEMA_DRIFT.md`" in task_057_section
    assert "One strict version-1 manifest" in task_057_section
    assert "one-to-one to five minimal, bounded, secret-free synthetic fixture files" in (
        task_057_section
    )
    assert "exact-byte SHA-256" in task_057_section
    assert "1,024-byte per-fixture maximum" in task_057_section
    assert "Binance Spot and USD-M 12-position candle rows" in task_057_section
    assert "Coinbase Exchange Spot six-position" in task_057_section
    assert "shared parser contract" in task_057_section
    assert "required fields are exactly `T`, `a`, `f`, `l`, `m`, `p`" in task_057_section
    assert "optional fields are exactly `M` and `nq`" in task_057_section
    assert "Spot fixture contains `M`" in task_057_section
    assert "USD-M fixture contains `nq`" in task_057_section
    assert "does not invent a market-specific parser rule" in task_057_section
    assert "unknown fields remain rejected" in task_057_section
    assert "Offline deterministic HTTP stubs and fixed UTC clocks" in task_057_section
    assert "exact bytes through" in task_057_section
    assert "active existing production adapter and request path" in task_057_section
    assert "exact raw-byte lineage without" in task_057_section
    assert "a network call" in task_057_section
    assert "Strict manifest tests reject unknown or missing keys" in task_057_section
    assert "absolute/traversing/mislocated paths" in task_057_section
    assert "digest mismatch, and oversized fixtures" in task_057_section
    assert "Representative detectable adapter drift" in task_057_section
    assert "selected detectable reorder" in task_057_section
    assert "wrong numeric types" in task_057_section
    assert "invalid decimal values" in task_057_section
    assert "invalid present optional-field values" in task_057_section
    assert "non-retryable `INVALID_PAYLOAD`" in task_057_section
    assert "without partial" in task_057_section
    assert "raw or canonical evidence" in task_057_section
    assert "Decimal precision alone has no adapter-level bound" in task_057_section
    assert "some same-typed semantic positional" in task_057_section
    assert "reorder can remain canonically valid" in task_057_section
    assert "parser acceptance is not compatibility evidence" in task_057_section
    assert "requires fail-closed pause and official-contract review" in task_057_section
    assert "manual pause and containment" in task_057_section
    assert "without overwriting old versions" in task_057_section
    assert "not copied into repository files, logs, issues, or fixtures" in task_057_section
    assert "adds no production source, network, runtime" in task_057_section
    assert "automatic detection/pause/" in task_057_section
    assert "continuous collector, deployment, or readiness claim" in task_057_section
    assert "TASK-037 remains" in task_057_section
    assert "blocked and authorization remains denied" in task_057_section
    assert "- **Status:** COMPLETE" in task_056_section
    assert (
        "`phase2.public_trade_disconnect_sparse_window_restart_recovery_drill`" in task_056_section
    )
    assert "`tests/integration/test_recoverable_public_trade_collection.py`" in task_056_section
    assert "One new deterministic generated-fixture integration case" in task_056_section
    assert "evidence, checkpoint, and shared rate-budget SQLite adapters" in task_056_section
    assert "exactly two scripted retryable `HttpTransportError` outcomes" in task_056_section
    assert "one 0.125-second retry" in task_056_section
    assert "`FAILED` checkpoint version 3" in task_056_section
    assert "`UNAVAILABLE` health" in task_056_section
    assert "`provider_unavailable`" in task_056_section
    assert "`attempts_exhausted`" in task_056_section
    assert "exact first one-millisecond pending leaf" in task_056_section
    assert "Hostile upstream detail is absent" in task_056_section
    assert "newly constructed adapters on the same three generated databases" in task_056_section
    assert "fresh UUID fence" in task_056_section
    assert "exact empty, one-valid-trade, and empty" in task_056_section
    assert "Completion is checkpoint version 6" in task_056_section
    assert "five lifetime source" in task_056_section
    assert "four traces, one retry, three completed windows" in task_056_section
    assert "one canonical record, three raw" in task_056_section
    assert "captures, and zero conflicts" in task_056_section
    assert "exact six transition statuses" in task_056_section
    assert "worker A, absent" in task_056_section
    assert "worker B, and worker B" in task_056_section
    assert "health exists only at versions 3, 5, and 6" in task_056_section
    assert "five unique granted durable reservations precede five provider attempts" in (
        task_056_section
    )
    assert "one 0.125-second retry plus two 0.25-second pacing waits" in task_056_section
    assert "completed rerun performs zero range invocations" in task_056_section
    assert "focused recovery integration" in task_056_section
    assert "file passes 14 tests, previously 13" in task_056_section
    assert "no production" in task_056_section
    assert "An isolated mutation audit killed all 30 of 30" in task_056_section
    assert "zero survivors and zero harness errors" in task_056_section
    assert "adapter and UUID-fence reuse" in task_056_section
    assert "empty raw-capture admission" in task_056_section
    assert "hostile-detail persistence" in task_056_section
    assert "completed-rerun work" in task_056_section
    assert "complete suite passed 1,650 tests" in task_056_section
    assert "dependency audit, and local health checks also passed" in task_056_section
    assert "cross-database-atomicity" in task_056_section
    assert "continuous-operation" in task_056_section
    assert "TASK-037 remains blocked and authorization remains" in task_056_section
    assert "denied" in task_056_section
    assert "private urllib opener with a no-follow redirect handler" in market_data_contract
    assert "performs no body" in market_data_contract
    assert "read or cleanup" in market_data_contract
    assert "No process-global opener is installed or" in market_data_contract
    assert "mutated" in market_data_contract
    assert "before any query-mapping operation" in market_data_contract
    assert "absolute credential-free HTTPS URL" in market_data_contract
    assert "lone surrogate code point" in market_data_contract
    assert "context-suppressed" in market_data_contract
    assert "Every percent sign in the authority also fails" in market_data_contract
    assert "authority is inspected under NFKC" in market_data_contract
    assert "accepted URL itself is never normalized" in market_data_contract
    assert "A rejected target performs" in market_data_contract
    assert "no query iteration or serialization" in market_data_contract
    assert "caller target" in market_data_contract
    assert "must be omitted or parse as numeric 443" in market_data_contract
    assert '`ValueError("url must use the standard HTTPS target port")`' in market_data_contract
    assert "without query access, serialization, request construction" in market_data_contract
    assert "ports retain the earlier exact" in market_data_contract
    assert "structural error and precedence" in market_data_contract
    assert "accepted implicit, explicit, or zero-padded 443 target retains" in market_data_contract
    assert "structural and caller-authority policies, not hostname or SSRF" in market_data_contract
    assert "configured proxy" in market_data_contract
    assert "peer may use a non-443 port" in market_data_contract
    assert "one bounded query snapshot" in market_data_contract
    assert "calls `items()` and starts its iterator once" in market_data_contract
    assert "does" in market_data_contract
    assert "not call `len(query)`" in market_data_contract
    assert "at most 33 yielded items" in market_data_contract
    assert "Zero through 32 exact built-in tuple" in market_data_contract
    assert "8,192 Python characters" in market_data_contract
    assert (
        '`ValueError("query must contain at most 32 built-in string pairs totaling at most 8192 '
        'characters")`' in market_data_contract
    )
    assert "Mapping-originated" in market_data_contract
    assert "including `ValueError`, remain the same raw objects" in market_data_contract
    assert "duplicates and empty or Unicode content" in market_data_contract
    assert "not a total wall-clock bound" in market_data_contract
    assert "non-polymorphic `str.__len__`" in market_data_contract
    assert "More than 8,192 Python" in market_data_contract
    assert '`ValueError("url must contain at most 8192 characters")`' in market_data_contract
    assert "before literal membership or character scanning" in market_data_contract
    assert "Caller" in market_data_contract
    assert "length and content overrides are not dispatched" in market_data_contract
    assert "Exact-limit ASCII and multi-byte Unicode" in market_data_contract
    assert "not a provider request-line compatibility or total-wall-clock guarantee" in (
        market_data_contract
    )
    assert "one shared maximum of 120 seconds" in market_data_contract
    assert '`ValueError("timeout_seconds must be at most 120")`' in market_data_contract
    assert "Exact integer and float 120" in market_data_contract
    assert "float subclass at exactly 120" in market_data_contract
    assert "identity because no exact numeric-type policy was added" in market_data_contract
    assert "not separately bound DNS, multiple operations" in market_data_contract
    assert "After either bounded body read and its body-size decision" in market_data_contract
    assert "one bounded" in market_data_contract
    assert "response-header snapshot before constructing `HttpResponse`" in market_data_contract
    assert "call `headers.items()` once" in market_data_contract
    assert "start its iterator once" in market_data_contract
    assert "perform no direct message iteration or" in market_data_contract
    assert "second pass" in market_data_contract
    assert "pull at most 101 times" in market_data_contract
    assert "Zero through 100 yielded pairs" in market_data_contract
    assert "`len(name) + len(value)`" in market_data_contract
    assert "65,536 Python characters" in market_data_contract
    assert "101st pair fails before unpacking or inspecting it" in market_data_contract
    assert "65,537th cumulative character" in market_data_contract
    assert (
        '`HttpTransportError("public HTTP response headers exceeded the configured limit")`'
        in market_data_contract
    )
    assert "Accepted pair order, duplicate names, original casing" in market_data_contract
    assert "`Retry-After` behavior" in market_data_contract
    assert "Body-read failures and body" in market_data_contract
    assert "oversize retain precedence without header enumeration" in market_data_contract
    assert "no direct cause or hidden context" in market_data_contract
    assert "originating provider error is the header-limit failure's direct cause" in (
        market_data_contract
    )
    assert "cleanup attempt whose failure cannot replace" in market_data_contract
    assert "same raw objects" in market_data_contract
    assert "natural implicit provider-error" in market_data_contract
    assert "adapter-controlled projection bound after standard-library parsing" in (
        market_data_contract
    )
    assert "does not bound wire-header bytes, parser work or memory" in market_data_contract
    assert "no privacy, redaction" in market_data_contract
    assert "One composed generated-fixture drill now exercises" in market_data_contract
    assert "exhausted" in market_data_contract
    assert "disconnect, sparse one-millisecond windows" in market_data_contract
    assert "newly constructed evidence, checkpoint, and shared" in market_data_contract
    assert "pending-leaf recovery from failed checkpoint version 3" in market_data_contract
    assert "completed version 6" in market_data_contract
    assert "five" in market_data_contract
    assert "budgeted requests, one retry, two pacing waits" in market_data_contract
    assert "three raw captures, one canonical trade, zero" in market_data_contract
    assert "conflicts, and a no-work completed rerun" in market_data_contract
    assert "does not prove cross-database atomicity, physical" in market_data_contract
    assert "## Versioned Public-Provider Schema Fixtures" in market_data_contract
    assert "contains exactly one minimal payload" in market_data_contract
    assert "strict manifest binds every unique identity" in market_data_contract
    assert "both aggregate-trade variants require exactly" in market_data_contract
    assert "shared parser's optional set is exactly `M` and `nq`" in market_data_contract
    assert "Fixture presence does not create a market-specific parser" in market_data_contract
    assert "selected detectable positional reorder" in market_data_contract
    assert "Decimal precision alone is not adapter-bounded" in market_data_contract
    assert "positional reorder can remain canonically valid" in market_data_contract
    assert "semantic drift that requires pause and contract review" in market_data_contract
    assert "five active provider payload variants" in market_data_contract
    assert "schema-drift response runbook" in market_data_contract
    assert "Versioned synthetic fixtures now cover" in market_data_contract
    assert "Neither supplies automatic detection, pause, remediation, resume" in (
        market_data_contract
    )
    assert "## Continuous Public-Trade Operating Contract (Design Only)" in market_data_contract
    assert "current public-trade component owns a" in market_data_contract
    assert "continuous lifecycle, cadence, automatic restart" in market_data_contract
    assert "separates three layers" in market_data_contract
    assert "stream control is `active` or `paused`" in market_data_contract
    assert "`schema_drift_hold` is a scoped pause reason" in market_data_contract
    assert "external" in market_data_contract
    assert "disabled-by-default posture" in market_data_contract
    assert "`starting`, `running`, and exactly" in market_data_contract
    assert "`stopped`, `paused`, `failed`, or `run_limit`" in market_data_contract
    assert "`waiting`, `caught_up`, and" in market_data_contract
    assert "`work_limit_reached` are cycle outcomes, not lifecycle states" in market_data_contract
    assert "clean bounded-job `paused` outcome" in market_data_contract
    assert "leaves the stream active for later bounded continuation" in market_data_contract
    assert "conflict/lost-lease, corrupt state, or source/policy drift" in market_data_contract
    assert "service run as" in market_data_contract
    assert "failed and places or keeps the stream on manual hold" in market_data_contract
    assert "Clean stop leaves stream status, cursor, counters, and attachment unchanged" in (
        market_data_contract
    )
    assert "fully closed half-open UTC windows `[start, end)`" in market_data_contract
    assert "persist the exact child UUID, target end, creation input" in market_data_contract
    assert "bounded-policy fingerprint" in market_data_contract
    assert "never replan an attached end" in market_data_contract
    assert "This contract adds no production code, runtime wiring, network call" in (
        market_data_contract
    )
    assert "does not establish physical" in market_data_contract
    assert "continuous" in market_data_contract
    assert "operation, operational readiness, or Phase 2 completion" in market_data_contract
    assert "After preserving `max_response_bytes` validation" in market_data_contract
    assert "exact built-in `str` of 1 through 256 Python characters" in market_data_contract
    assert "U+0020 through U+007E" in market_data_contract
    assert (
        '`ValueError("user_agent must be a built-in string of 1 to 256 visible ASCII '
        'characters")`' in market_data_contract
    )
    assert "oversized exact string" in market_data_contract
    assert "fails before scanning its characters" in market_data_contract
    assert "leading or trailing spaces and punctuation" in market_data_contract
    assert "sole `User-Agent` header" in market_data_contract
    assert '"WEALTH/0.1 public-market-data"' in market_data_contract
    assert "no privacy or" in market_data_contract
    assert "total-header-block guarantee" in market_data_contract
    assert "0029-continuous-public-trade-stream-persistence-contract.md" in decision_index
    assert "0030-continuous-public-trade-stream-store-port-contract.md" in decision_index
    assert "## Continuous Public-Trade Persistence Records and Codecs (Unused)" in (
        root_readme_prose
    )
    assert "TASK-059 attachment's `creation_fingerprint` is non-invertible" in root_readme_prose
    assert "canonical `child_creation_payload`" in root_readme_prose
    assert "does not replace or redefine the existing bounded-child store serializer" in (
        root_readme_prose
    )
    assert (
        "separate stream-creation record with lowercase-hex exact version-one envelope bytes"
        in (root_readme_prose)
    )
    assert "complete canonical stream-policy projection" in root_readme_prose
    assert "domain-separated rolling history root" in root_readme_prose
    assert "Six distinct domain-separated contracts cover the child-creation fingerprint" in (
        root_readme_prose
    )
    assert "current load/planning alone grants no action" in root_readme_prose
    assert "ATTACH authority is intentionally time-independent" in root_readme_prose
    assert "accepted history root, successor version, candidate" in root_readme_prose
    assert "ambiguous hold classification stops canonical admission/progress" in root_readme_prose
    assert "Canonical reason scope is required for RETAIN and MANUAL_HOLD" in root_readme_prose
    assert "exact pure two-pass proof" in root_readme_prose
    assert "trusted instant is the planner's `now`" in root_readme_prose
    assert "Every transition retains lowercase-hex exact successor-envelope bytes" in (
        root_readme_prose
    )
    assert "Compare-and-swap is not the outer UUID fence" in root_readme_prose
    assert "TASK-061 is implemented only as that unused pure domain increment" in (
        root_readme_prose
    )
    assert "Deterministic golden-byte, hostile-input, transition-matrix" in root_readme_prose
    assert "## Continuous Public-Trade Logical Stream-Store Port (Unused)" in root_readme_prose
    assert "lower-level logical store boundary for finalized TASK-061 artifacts" in (
        root_readme_prose
    )
    assert "embedded successor is the sole successor" in root_readme_prose
    assert "returns `AT_TAIL` instead of an empty page" in root_readme_prose
    assert "`validate_continuous_public_trade_stream_audit_page` function" in root_readme_prose
    assert "Every output is a store-local classification" in root_readme_prose
    assert "`UNAVAILABLE` explicitly means that no coherent store-local classification" in (
        root_readme_prose
    )
    assert "TASK-062 is complete only as this unused logical contract increment" in (
        root_readme_prose
    )
    assert "canonical next action is TASK-063" in root_readme_prose
    assert "TASK-063 remains queued until TASK-062 is `COMPLETE`" not in root_readme_prose
    assert "## Continuous Public-Trade Persistence Records and Codecs (Unused)" in (
        market_data_prose
    )
    assert "creation fingerprint is not reversible" in market_data_prose
    assert "companion canonical `child_creation_payload`" in market_data_prose
    assert "versioned compare-and-swap" in market_data_prose
    assert "a pause reason alone is not authority" in market_data_prose
    assert "signed-64-bit epoch microseconds and Python's calendar" in market_data_prose
    assert "stream-creation record with explicit null prior" in market_data_prose
    assert "exact canonical successor-envelope bytes/digest" in market_data_prose
    assert "complete canonical projection of every stream-policy field" in market_data_prose
    assert "prior domain-separated rolling history root" in market_data_prose
    assert "Six distinct domain-separated contracts cover the child-creation fingerprint" in (
        market_data_prose
    )
    assert "current load and planning alone grant no action" in market_data_prose
    assert "ATTACH transition authority binds exact prior version" in market_data_prose
    assert "accepted history root, successor version, candidate child UUID" in market_data_prose
    assert "ambiguous classification stops canonical admission/progress" in market_data_prose
    assert "Canonical reason scope is required for `RETAIN` and `MANUAL_HOLD`" in market_data_prose
    assert "pure two-pass proof" in market_data_prose
    assert "trusted instant is the planner's `now`" in market_data_prose
    assert "TASK-061 is complete only as the unused pure domain increment" in market_data_prose
    assert "## Continuous Public-Trade Logical Stream-Store Port (Unused)" in market_data_prose
    assert "lower-level atomic logical store protocol for finalized TASK-061 artifacts" in (
        market_data_prose
    )
    assert "original canonical envelope, creation-record, or transition-record bytes" in (
        market_data_prose
    )
    assert "no second successor, timestamp, child payload, or successor digest" in (
        market_data_prose
    )
    assert "`EXACT_REQUEST_ONLY` describes the unchanged shape" in market_data_prose
    assert "`validate_continuous_public_trade_stream_audit_page`" in market_data_prose
    assert "All outputs are store-local classifications" in market_data_prose
    assert "`UNAVAILABLE` explicitly carries no coherent classification" in market_data_prose
    assert "TASK-062 is complete only as this unused logical contract increment" in (
        market_data_prose
    )
    assert "canonical next action is TASK-063" in market_data_prose
    assert "TASK-063 remains queued until TASK-062 is `COMPLETE`" not in market_data_prose
    assert "# ADR 0029: Continuous Public-Trade Stream Persistence Contract" in adr_0029
    assert "- **Status:** Accepted" in adr_0029
    assert "### Exact durable TASK-059 stream state" in adr_0029
    assert "### Required durable companion child-creation record" in adr_0029
    assert "This is a new `child_creation_payload` evidence contract" in adr_0029_prose
    assert "does not redefine that store's serializer" in adr_0029_prose
    assert "### Canonical persistence envelope" in adr_0029
    assert "wealth.continuous_public_trade.child_creation/v1" in adr_0029
    assert "wealth.continuous_public_trade.stream_record/v1" in adr_0029
    assert "wealth.continuous_public_trade.stream_creation/v1" in adr_0029
    assert "wealth.continuous_public_trade.stream_transition/v1" in adr_0029
    assert "wealth.continuous_public_trade.evidence_scope/v1" in adr_0029
    assert "wealth.continuous_public_trade.history_root/v1" in adr_0029
    assert "no nonexistent `CREATE` transition kind is invented" in adr_0029_prose
    assert "version-one stream-policy projection freezes exactly" in adr_0029_prose
    assert "`max_requests_per_job`, `max_records_per_job`" in adr_0029_prose
    assert "`successor_envelope_hex`" in adr_0029
    assert "even-length lowercase hexadecimal string" in adr_0029_prose
    assert "digest input is the decoded canonical envelope bytes" in adr_0029_prose
    assert "### Create" in adr_0029
    assert "governed-create evidence reference" in adr_0029_prose
    assert "exact canonical projection of every validated" in adr_0029_prose
    assert "load/CAS require exact field-for-field equality" in adr_0029_prose
    assert "store-local natural identity is" in adr_0029_prose
    assert "same natural stream identity under a different stream ID" in adr_0029_prose
    assert "### Exact-identity load" in adr_0029
    assert "Current load reads a constant number" in adr_0029_prose
    assert "pages of 1 through 100" in adr_0029_prose
    assert "one immutable predecessor creation/transition record as an overlap" in adr_0029_prose
    assert "immediately prior validated page or held in an accepted attestation" in adr_0029_prose
    assert "creation-record overlap uses the version-one creation-root formula" in adr_0029_prose
    assert "no call reads more than 101 stream-history records" in adr_0029_prose
    assert "externally anchored history attestation" in adr_0029_prose
    assert "budget reservation, provider request, evidence admission" in adr_0029_prose
    assert "already crossed every pre-request gate" in adr_0029_prose
    assert "may admit only its already-returning evidence" in adr_0029_prose
    assert "exception never applies to schema/contract drift" in adr_0029_prose
    assert "only an explicitly governed quarantine/attention-evidence path" in adr_0029_prose
    assert "### Versioned compare-and-swap transition" in adr_0029
    assert "caller does not supply `recorded_at`" in adr_0029_prose
    assert "constructs the complete successor at exactly" in adr_0029_prose
    assert "internal store-level compare-and-swap command" in adr_0029_prose
    assert "exactly one `STREAM_TRANSITION_AUTHORITY` reference" in adr_0029_prose
    assert "prior version/digest/history root or `null`" in adr_0029_prose
    assert "scope for `ATTACH` is intentionally independent" in adr_0029_prose
    assert "convergent alternate history" in adr_0029_prose
    assert "`reason_code` is required for `RETAIN` and `MANUAL_HOLD`" in adr_0029_prose
    assert "reason code follows the same exact transition-kind matrix" in adr_0029_prose
    assert "exact canonical successor-envelope bytes" in adr_0029_prose
    assert "`valid_from <= recorded_at < expires_at`" in adr_0029
    assert "later expiry does not corrupt an accepted historical transition" in adr_0029_prose
    assert "Caller override and backdating are forbidden" in adr_0029_prose
    assert "regressing trusted clock fails before mutation" in adr_0029_prose
    assert "pristine child's `created_at` and `updated_at` both equal" in adr_0029_prose
    assert "Every datetime inside `child_creation_payload`" in adr_0029_prose
    assert "### Pure two-pass attachment finalization" in adr_0029
    assert "fixed in-memory all-zero provisional value" in adr_0029_prose
    assert "provisional value is never durable evidence" in adr_0029_prose
    assert "### Required pure-codec safety bounds" in adr_0029
    assert "16,384 decoded canonical bytes per stream envelope" in adr_0029_prose
    assert "32,768 even lowercase ASCII characters" in adr_0029_prose
    assert "## Transition Evidence and Preconditions" in adr_0029
    assert "## Crash-Seam Matrix" in adr_0029
    assert "retained pending leaf when one exists" in adr_0029_prose
    assert "## Versioning, Compatibility, and Migration" in adr_0029
    assert "## Retention and Rollback" in adr_0029
    assert "## Safety and Authority Boundary" in adr_0029
    assert "TASK-037 remains blocked and authorization remains denied" in adr_0029_prose
    assert "# ADR 0030: Continuous Public-Trade Stream Store Port Contract" in adr_0030
    assert "- **Status:** Accepted" in adr_0030
    assert "### Boundary selection" in adr_0030
    assert "lower-level atomic logical store boundary" in adr_0030_prose
    assert "accepts finalized TASK-061 artifacts" in adr_0030_prose
    assert "exactly one `ContinuousPublicTradeStreamTransitionRecordV1`" in adr_0030_prose
    assert "no second successor" in adr_0030_prose
    assert "### Exact public values" in adr_0030
    assert "Original canonical bytes remain authoritative" in adr_0030_prose
    assert "### Logical atomic ownership" in adr_0030
    assert "There is one winner" in adr_0030_prose
    assert "### Closed outcomes" in adr_0030
    assert "`ANCHOR_CONFLICT`" in adr_0030
    assert "### Bounded current view" in adr_0030
    assert "### Bounded audit state machine" in adr_0030
    assert "`validate_continuous_public_trade_stream_audit_page`" in adr_0030
    assert "absolute maximum of 101 returned history records" in adr_0030_prose
    assert (
        "must separately prove that it obtains each page without reading beyond" in adr_0030_prose
    )
    assert "### Retry disposition" in adr_0030
    assert "`EXACT_REQUEST_ONLY` is not retry authority" in adr_0030_prose
    assert "No result means that external evidence bodies were loaded" in adr_0030_prose
    assert "TASK-037 remains blocked and authorization remains denied" in adr_0030_prose
    assert "Automatic 301, 302, 303, 307, and 308 redirects are rejected" in risk_register
    assert "process-global opener is untouched" in risk_register
    assert "original initial target must be an absolute credential-free HTTPS URL" in risk_register
    assert "lone surrogate" in risk_register
    assert "NFKC authority ambiguity" in risk_register
    assert "accepted target is never reconstructed or normalized" in risk_register
    assert "target port must be omitted or parse as numeric 443" in risk_register
    assert "structurally valid nonstandard target port fails" in risk_register
    assert (
        "accepted implicit, explicit, and zero-padded 443 text remains unchanged" in risk_register
    )
    assert "does not constrain a configured proxy peer" in risk_register
    assert "performs no hostname allowlisting, DNS resolution" in risk_register
    assert "Query serialization now takes one adapter-bounded snapshot" in risk_register
    assert "32 exact built-in-string pairs" in risk_register
    assert "8,192 cumulative key-plus-value characters" in risk_register
    assert "33 yielded items" in risk_register
    assert "mapping-originated failures remain raw" in risk_register
    assert "non-polymorphic built-in string length is capped" in risk_register
    assert "at 8,192 Python characters" in risk_register
    assert "before scanning, parsing, query, or request work" in risk_register
    assert "caller length/content overrides are never dispatched" in risk_register
    assert "configured User-Agent is now an exact built-in string" in risk_register
    assert "1 through 256 visible-ASCII characters" in risk_register
    assert "without normalization, fallback, or privacy claims" in risk_register
    assert (
        "Finite-positive public-HTTP timeouts now also share a 120-second maximum" in risk_register
    )
    assert "without changing their standard per-operation semantics" in risk_register
    assert "After the bounded body-size decision" in risk_register
    assert "successful and HTTP-error response headers now use one adapter-bounded snapshot" in (
        risk_register
    )
    assert "calls `headers.items()` and starts its iterator once" in risk_register
    assert "pulls at most 101 times" in risk_register
    assert "accepts at most 100 pairs and 65,536 cumulative" in risk_register
    assert "fails a yielded 101st pair before unpacking or inspection" in risk_register
    assert "Accepted order, duplicates, casing, and content remain exact" in risk_register
    assert "raw header-origin failures are preserved" in risk_register
    assert "Standard-library parsing and prior allocations occur before" in risk_register
    assert "no wire-header, parser-allocation, total-memory" in risk_register
    assert "TASK-056 now adds one deterministic generated-fixture drill" in risk_register
    assert "exact pending leaf at failed version 3" in risk_register
    assert "completes sparse windows at version 6 under a fresh fence" in risk_register
    assert "Five durable reservations precede five requests" in risk_register
    assert "completed rerun performs no work" in risk_register
    assert "Preserve the TASK-056 disconnect and sparse-recovery evidence" in risk_register
    assert "TASK-057 adds one strict exact-byte, SHA-256-pinned version-1 manifest" in risk_register
    assert "five bounded synthetic fixtures" in risk_register
    assert "shared exact required set" in risk_register
    assert "optional set `M`, `nq`" in risk_register
    assert "do not create market-specific parser rules" in risk_register
    assert "Representative detectable width, selected detectable reorder" in risk_register
    assert "Decimal precision alone and some same-typed semantic reorder may still parse" in (
        risk_register
    )
    assert "acceptance is not compatibility proof" in risk_register
    assert "manual pause/review/resume procedure" in risk_register
    assert "adds no automatic detector" in risk_register
    assert "ADR-0028 preserves that exact manual hold and governed resume boundary" in risk_register
    assert "TASK-059 models a validated paused checkpoint as a pure input" in risk_register
    assert "planner outcome is only `HELD`" in risk_register
    assert "does not inspect payloads, detect drift, write a hold" in risk_register
    assert "ADR-0028 records only a conceptual finite-run single-host composition" in risk_register
    assert "TASK-059 now adds only a pure unused fixed-UTC closed-window planner" in risk_register
    assert "results are `HELD`, `WAITING`, or `ATTACHED_JOB`" in risk_register
    assert "operational capacity remain unproven" in risk_register
    assert "ADR-0028 requires one shared durable single-host budget" in risk_register
    assert "TASK-059 validates only finite pure policy and planner bounds" in risk_register
    assert "do not claim capacity adequacy, multi-host safety" in risk_register
    assert "ADR-0028 requires an attached child to retain its exact pending leaf" in risk_register
    assert "TASK-059 adds only pure attachment and transition validation" in risk_register
    assert "ADR-0029 now defines only the conceptual stream persistence boundary" in (
        risk_register_prose
    )
    assert "a fingerprint alone is not reconstructable" in risk_register_prose
    assert "TASK-061 now implements only an unused persistence-record" in risk_register_prose
    assert "five canonical JSON codecs" in risk_register_prose
    assert "six domain-separated digest/history contracts" in risk_register_prose
    assert "TASK-062 now freezes only an unused logical store port" in risk_register_prose
    assert "one-winner compare-and-swap ownership" in risk_register_prose
    assert "TASK-063 is now the canonical next design-only" in risk_register_prose
    assert (
        "it authorizes no adapter, database, implementation, capacity, durability, or readiness "
        "claim"
    ) in risk_register_prose
    assert "TASK-063 remains queued until TASK-062 is `COMPLETE`" not in risk_register_prose
    assert "TASK-062 adds only strict finalized-record storage commands/results" in (
        risk_register_prose
    )
    assert "TASK-062's logical store results and retry dispositions grant no request budget" in (
        risk_register_prose
    )
    assert "TASK-062 now defines only logical atomic create/CAS ownership" in risk_register_prose
    assert "A pause reason alone is not authorization" in risk_register_prose
    assert "stream attachment and compare-and-swap grant no capacity" in risk_register_prose
    assert "complete immutable stream-policy projection in creation evidence" in (
        risk_register_prose
    )
    assert "An unknown commit is resolved only by exact reload" in risk_register_prose
    assert "compare-and-swap is not a fresh UUID fence" in risk_register_prose
    assert "Six separate digest contracts bind child creation" in risk_register_prose
    assert "Current load/planning grants no action" in risk_register_prose
    assert "externally anchored attestation matching the exact version" in risk_register_prose
    assert "non-integrity operational hold" in risk_register_prose
    assert "ATTACH authority itself is time-independent" in risk_register_prose
    assert "accepted history root, successor version" in risk_register_prose
    assert "cannot always be converted to Python datetimes" in risk_register_prose
    assert "must never truncate, wrap, or silently normalize" in risk_register_prose
    assert "TASK-056 adds one composed generated-fixture drill" in risk_register
    assert "newly constructed evidence, checkpoint, and rate-budget SQLite adapters" in (
        risk_register
    )
    assert "worker A fails at version 3 with the exact pending leaf" in risk_register
    assert "worker B completes at version 6 under a fresh fence" in risk_register
    assert "completed rerun changes nothing" in risk_register
    assert "keep cross-database atomicity and physical durability" in risk_register
    assert "Within the canonical UTC migration track" in roadmap
    assert "TASK-037 remains" in roadmap
    assert "blocked and authorization remains denied" in roadmap
    assert "current executable next action is" in roadmap
    assert "maintained in `PROJECT_STATE.json`" in roadmap
    assert "separately governed RISK-1 fail-closed work" in roadmap
    assert "TASK-056 now supplies one composed deterministic" in roadmap
    assert "failed checkpoint version 3 through completed version 6" in roadmap
    assert "does not" in roadmap
    assert "establish cross-database atomicity, physical durability" in roadmap
    assert "TASK-057 now supplies one strict manifest" in roadmap
    assert "all five active provider payload variants" in roadmap
    assert "manual schema-drift containment/review/resume runbook" in roadmap
    assert "shared optional parser set `M`, `nq`" in roadmap
    assert "precision-only changes and some same-typed semantic reorder may parse" in roadmap
    assert "manual pause and contract review" in roadmap
    assert "TASK-058 now records a design-only ADR and" in roadmap
    assert "operating contract for a possible future single-host continuous public-trade" in roadmap
    assert "UTC closed-window cadence, bounded catch-up, fencing and restart" in roadmap
    assert "preserves today's immutable policy, exact pending-leaf, evidence-first" in roadmap
    assert "does not implement or approve production code, runtime" in roadmap
    assert "scheduler, daemon, process manager, service, deployment" in roadmap
    assert "Continuous-operation" in roadmap
    assert "Phase 2 readiness remain unproven" in roadmap
    assert "implementation requires a" in roadmap
    assert "separately governed future task" in roadmap
    assert "TASK-059 now adds only the unused provider-independent domain boundary" in roadmap
    assert "A paused stream yields only `HELD`" in roadmap
    assert "no runtime imports the module" in roadmap
    assert "TASK-060 records" in roadmap_prose
    assert "ADR-0029" in roadmap_prose
    assert "TASK-059 creation fingerprint alone is not reversible" in roadmap_prose
    assert "pause reason or child ID" in roadmap_prose
    assert "TASK-059 epoch milliseconds remain exact" in roadmap_prose
    assert "six distinct child, stream-envelope" in roadmap_prose
    assert "complete stream-policy projection for field-level drift checks" in roadmap_prose
    assert "ATTACH authority binds exact prior version/digest/history root" in roadmap_prose
    assert "explicit non-integrity classification" in roadmap_prose
    assert "TASK-061 is complete as one pure, unused RISK-1 domain increment" in roadmap_prose
    assert "strict version-one child-creation payload, stream-envelope, stream-creation" in (
        roadmap_prose
    )
    assert "ADR-0030 now freezes the current TASK-062 RISK-1 increment" in roadmap_prose
    assert "one embedded successor per compare-and-swap" in roadmap_prose
    assert "`validate_continuous_public_trade_stream_audit_page` function" in roadmap_prose
    assert "Every output is a store-local classification" in roadmap_prose
    assert "`UNAVAILABLE` carries no coherent classification" in roadmap_prose
    assert "TASK-062 is complete only as this unused logical contract increment" in roadmap_prose
    assert "TASK-063 is the canonical next action" in roadmap_prose
    assert "TASK-063 remains queued until TASK-062 is `COMPLETE`" not in roadmap_prose
    assert "grants no physical implementation authority" in roadmap_prose
    assert "The canonical next action is TASK-037" not in roadmap


def test_project_state_forbids_unknown_fields() -> None:
    payload = load_payload()
    payload["unexpected"] = True

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        validate_payload(payload)


def test_project_state_rejects_non_utc_timestamp() -> None:
    payload = load_payload()
    payload["last_updated_utc"] = "2026-07-25T14:00:00+03:00"

    with pytest.raises(ValidationError, match="last_updated_utc must use UTC"):
        validate_payload(payload)


@pytest.mark.parametrize(
    "flag",
    [
        "live_trading_enabled",
        "leverage_enabled",
        "withdrawals_enabled",
        "external_notifications_enabled",
        "autonomous_live_execution_enabled",
    ],
)
def test_project_state_rejects_unsafe_control_flags(flag: str) -> None:
    payload = load_payload()
    control_flags = payload["control_flags"]
    assert isinstance(control_flags, dict)
    control_flags[flag] = True

    with pytest.raises(ValidationError):
        validate_payload(payload)


def test_project_state_rejects_trading_state_during_phase_two() -> None:
    payload = load_payload()
    payload["open_positions"] = ["position-not-permitted"]

    with pytest.raises(ValidationError, match="Phase 2 must not declare positions or orders"):
        validate_payload(payload)


def test_project_state_rejects_duplicate_governance_references() -> None:
    payload = load_payload()
    known_risks = payload["known_risks"]
    assert isinstance(known_risks, list)
    known_risks.append("RISK-001")

    with pytest.raises(ValidationError, match="known_risks must contain unique values"):
        validate_payload(payload)


def test_project_state_rejects_duplicate_provider_datasets() -> None:
    payload = load_payload()
    data_sources = payload["active_data_sources"]
    assert isinstance(data_sources, list)
    binance = data_sources[0]
    assert isinstance(binance, dict)
    datasets = binance["datasets"]
    assert isinstance(datasets, list)
    datasets.append("candles")

    with pytest.raises(ValidationError, match="active data-source datasets must be unique"):
        validate_payload(payload)


def test_project_state_is_immutable() -> None:
    state = load_project_state(PROJECT_STATE_PATH)

    with pytest.raises(ValidationError):
        state.project_goal = "changed"
