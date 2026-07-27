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
MARKET_DATA_CONTRACT_PATH = REPOSITORY_ROOT / "docs" / "contracts" / "MARKET_DATA.md"
ROADMAP_PATH = REPOSITORY_ROOT / "docs" / "ROADMAP.md"


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
    assert "typed_public_http_response_protocol_failure_mapping" in state.active_components
    assert "fail_closed_public_http_automatic_redirect_rejection" in state.active_components
    assert "fail_closed_public_http_initial_request_target_validation" in state.active_components
    assert "fail_closed_public_http_standard_https_target_port_policy" in state.active_components
    assert "fail_closed_public_http_bounded_query_serialization" in state.active_components
    assert "fail_closed_public_http_initial_target_length_bound" in state.active_components
    assert "fail_closed_public_http_bounded_user_agent_validation" in state.active_components
    assert "fail_closed_public_http_maximum_timeout_policy" in state.active_components
    assert len(state.open_tasks) == 2
    task_037, task_055 = state.open_tasks
    assert task_037.task_id == "TASK-037"
    assert task_037.status == "blocked"
    assert task_037.risk_tier == 3
    assert task_037.requires_human_approval is True
    assert task_055.task_id == "TASK-055"
    assert task_055.action == "phase2.fail_closed_public_http_bounded_response_header_projection"
    assert task_055.status == "ready"
    assert task_055.risk_tier == 1
    assert task_055.requires_human_approval is False
    assert state.blockers == (
        "TASK-037 awaits owner-supplied exact restricted-package inputs in an approved governance "
        "location before independent Risk and Security review and the project-owner decision; "
        "authorization remains denied.",
    )
    assert state.next_action.task_id == "TASK-055"
    assert (
        state.next_action.action
        == "phase2.fail_closed_public_http_bounded_response_header_projection"
    )
    assert any(decision.decision_id == "ADR-0027" for decision in state.recent_decisions)


def test_project_state_references_existing_governance_artifacts() -> None:
    state = load_project_state(PROJECT_STATE_PATH)
    risk_register = RISK_REGISTER_PATH.read_text(encoding="utf-8")
    backlog = BACKLOG_PATH.read_text(encoding="utf-8")
    market_data_contract = MARKET_DATA_CONTRACT_PATH.read_text(encoding="utf-8")
    roadmap = ROADMAP_PATH.read_text(encoding="utf-8")

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
    assert next_action_section.count("### TASK-") == 1
    assert f"### {state.next_action.task_id} " in next_action_section
    assert f"`{state.next_action.action}`" in next_action_section
    assert "- **Status:** READY" in next_action_section
    assert "- **Risk tier:** RISK 1" in next_action_section
    assert "- **Human approval:** NOT REQUIRED" in next_action_section
    assert "bounded public-HTTP response-header projection" in next_action_section
    assert "`tuple(headers.items())`" in next_action_section
    assert "without an adapter-level pair-count or cumulative character bound" in (
        next_action_section
    )
    assert "Standard-library header parsing occurs before" in next_action_section
    assert "at most 100 pairs" in next_action_section
    assert "65,536 Python characters" in next_action_section
    assert "at most a 101st" in next_action_section
    assert "`src/wealth/adapters/http.py`" in next_action_section
    assert "`tests/unit/test_http_adapter.py`" in next_action_section
    assert (
        '`HttpTransportError("public HTTP response headers exceeded the configured limit")`'
        in next_action_section
    )
    assert "Call `headers.items()` once" in next_action_section
    assert "start its iterator once" in next_action_section
    assert "do not call" in next_action_section
    assert "`len(headers)`" in next_action_section
    assert "Preserve accepted pair order, duplicate names, original casing" in next_action_section
    assert "do not normalize, unfold, combine, deduplicate, filter, reorder" in next_action_section
    assert "body read and oversize decisions before header" in next_action_section
    assert "caller-originated header-enumeration failures" in next_action_section
    assert "only an" in next_action_section
    assert "adapter-controlled projection bound" in next_action_section
    assert "does not bound wire bytes, standard-library parsing" in next_action_section
    assert "header allowlist, denylist, content-type" in next_action_section
    assert "privacy, or" in next_action_section
    assert "sensitive-data guarantee" in next_action_section
    assert "hostname/provider allowlists" in next_action_section
    assert "SSRF" in next_action_section
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
    assert "currently projects every pair returned by" in market_data_contract
    assert "`headers.items()`" in market_data_contract
    assert "TASK-055 governs an application-level snapshot" in market_data_contract
    assert "at most 100 pairs and 65,536 cumulative" in market_data_contract
    assert "name-plus-value Python characters" in market_data_contract
    assert "projection" in market_data_contract
    assert "bound only" in market_data_contract
    assert "no wire-header, parser, total-response-memory, privacy" in market_data_contract
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
    assert "every pair returned by `headers.items()` is still projected" in risk_register
    assert "without an adapter-level pair-count or cumulative-character limit" in risk_register
    assert "Add an application-level successful/HTTP-error header snapshot" in risk_register
    assert "100 pairs and 65,536 cumulative name-plus-value Python characters" in risk_register
    assert "do not claim a wire-header, parser-allocation, total-memory" in risk_register
    assert "Within the canonical UTC migration track" in roadmap
    assert "TASK-037 remains" in roadmap
    assert "blocked and authorization remains denied" in roadmap
    assert "current executable next action is" in roadmap
    assert "maintained in `PROJECT_STATE.json`" in roadmap
    assert "separately governed RISK-1 fail-closed work" in roadmap
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
