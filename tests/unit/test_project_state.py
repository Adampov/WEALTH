"""Tests for the canonical machine-readable project-state contract."""

import json
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from wealth.domain.project_state import ProjectState, load_project_state

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROJECT_STATE_PATH = REPOSITORY_ROOT / "PROJECT_STATE.json"
RISK_REGISTER_PATH = REPOSITORY_ROOT / "RISK_REGISTER.md"
BACKLOG_PATH = REPOSITORY_ROOT / "BACKLOG.md"


def load_payload() -> dict[str, Any]:
    """Load a mutable copy for negative contract tests."""

    return cast(dict[str, Any], json.loads(PROJECT_STATE_PATH.read_text(encoding="utf-8")))


def validate_payload(payload: dict[str, Any]) -> ProjectState:
    """Validate a modified payload through the same JSON boundary as the canonical file."""

    return ProjectState.model_validate_json(json.dumps(payload))


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
    assert len(state.open_tasks) == 2
    task_037, task_047 = state.open_tasks
    assert task_037.task_id == "TASK-037"
    assert task_037.status == "blocked"
    assert task_037.risk_tier == 3
    assert task_037.requires_human_approval is True
    assert task_047.task_id == "TASK-047"
    assert task_047.status == "ready"
    assert task_047.risk_tier == 1
    assert task_047.requires_human_approval is False
    assert state.blockers == (
        "TASK-037 awaits owner-supplied exact restricted-package inputs in an approved governance "
        "location before independent Risk and Security review and the project-owner decision; "
        "authorization remains denied.",
    )
    assert state.next_action.task_id == "TASK-047"
    assert state.next_action.action == "phase2.typed_public_http_response_protocol_failure_mapping"
    assert any(decision.decision_id == "ADR-0027" for decision in state.recent_decisions)


def test_project_state_references_existing_governance_artifacts() -> None:
    state = load_project_state(PROJECT_STATE_PATH)
    risk_register = RISK_REGISTER_PATH.read_text(encoding="utf-8")
    backlog = BACKLOG_PATH.read_text(encoding="utf-8")

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
    assert next_action_section.count("### TASK-") == 1
    assert f"### {state.next_action.task_id} " in next_action_section
    assert f"`{state.next_action.action}`" in next_action_section
    assert "- **Status:** READY" in next_action_section
    assert "- **Risk tier:** RISK 1" in next_action_section
    assert "- **Human approval:** NOT REQUIRED" in next_action_section
    assert "`http.client.BadStatusLine`" in next_action_section
    assert "`LineTooLong`" in next_action_section
    assert "`UnknownProtocol`" in next_action_section
    assert "provider-supplied line" in next_action_section
    assert '`HttpTransportError("public HTTP GET failed")`' in next_action_section
    assert "original exception as direct cause" in next_action_section
    assert "one `max_response_bytes + 1` read" in next_action_section
    assert "Base `HTTPException`, `InvalidURL`" in next_action_section
    assert "Do not broadly catch" in next_action_section
    assert "TASK-037 remains blocked and authorization remains denied" in next_action_section
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
