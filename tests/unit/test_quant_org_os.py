"""Governance assertions for the durable QUANT ORG OS prompt."""

import json
from pathlib import Path
from typing import Any, cast

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
OPERATING_PROMPT_PATH = REPOSITORY_ROOT / "docs" / "QUANT_ORG_OS.md"
AGENTS_PATH = REPOSITORY_ROOT / "AGENTS.md"
PROJECT_STATE_PATH = REPOSITORY_ROOT / "PROJECT_STATE.json"


def _project_state() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(PROJECT_STATE_PATH.read_text(encoding="utf-8")),
    )


def test_root_guidance_requires_the_durable_operating_prompt() -> None:
    guidance = AGENTS_PATH.read_text(encoding="utf-8")
    normalized_guidance = " ".join(guidance.split())

    assert "## Governing Operating Prompt" in guidance
    assert "`docs/QUANT_ORG_OS.md` is the durable operating constitution" in guidance
    assert "Chat history is context, not durable authority." in normalized_guidance
    assert "one writable owner per file" in guidance
    assert "cloud work only from a pushed commit" in guidance
    assert "Never hardcode current phase, capability, approval, or risk state" in guidance
    assert guidance.index("1. `PROJECT_STATE.json`") < guidance.index("3. `docs/QUANT_ORG_OS.md`")

    policy = (REPOSITORY_ROOT / "docs" / "POLICIES.md").read_text(encoding="utf-8")
    assert "## Local Development Review Delegation" in policy
    assert "`DEV-REVIEW-V1`" in policy
    assert "`LOCAL_DEVELOPMENT_APPROVAL`" in policy


def test_operating_prompt_preserves_current_fail_closed_truth() -> None:
    prompt = OPERATING_PROMPT_PATH.read_text(encoding="utf-8")
    state = _project_state()

    assert state["operating_mode"] == "research"
    assert state["current_risk_state"] == "NO_TRADING_CAPABILITY"
    assert all(value is False for value in state["control_flags"].values())
    assert (
        "Current operating mode and capabilities always come from `PROJECT_STATE.json`." in prompt
    )
    assert "OPERATING_MODE = PAPER_TRADING" not in prompt
    assert "live trading is disabled;" in prompt
    assert "autonomous live execution is disabled;" in prompt
    assert "leverage is disabled;" in prompt
    assert "withdrawals are permanently outside the platform's authority" in prompt
    assert "a final Risk rejection cannot be overridden;" in prompt
    assert "wash trade" in prompt

    policy = " ".join(
        (REPOSITORY_ROOT / "docs" / "POLICIES.md").read_text(encoding="utf-8").split()
    )
    assert "Eligibility depends on actual effects" in policy
    for exclusion in (
        "production and operational permissions",
        "paper/shadow execution authority",
        "restricted inputs and TASK-037 authorization",
        "credentials, secrets and private accounts",
        "financial or runtime Risk/Execution/Audit authority",
        "external notifications; deployment; and merge",
    ):
        assert exclusion in policy


def test_operating_prompt_freezes_the_hybrid_multi_agent_boundary() -> None:
    prompt = OPERATING_PROMPT_PATH.read_text(encoding="utf-8")

    required_sections = (
        "## 8. Master Orchestrator",
        "## 9. Hybrid Multi-Agent Engineering Workflow",
        "### 9.1 Execution Surfaces",
        "### 9.2 Task Graph",
        "### 9.3 Agent Roles",
        "### 9.4 Parallelize or Serialize",
        "### 9.5 File and Branch Ownership",
        "### 9.6 Handoff Packet",
        "### 9.7 Integration",
        "### 9.8 Failure and Recovery",
        "## 15. Future Runtime Multi-Agent Workflow",
    )
    assert all(section in prompt for section in required_sections)
    assert "Local execution stops when the computer sleeps" in prompt
    assert "A cloud task cannot see uncommitted local or WSL changes." in prompt
    assert "Pushing is an external disclosure" in prompt
    assert "Only one agent owns a writable node at a time." in prompt
    assert "frozen contract digest and generation" in prompt
    assert "`PAUSED`, `REVIEW`, `STALE`" in prompt
    assert "transitively marks every dependent node and output" in prompt
    assert "A manual lease becomes `RETURNED` when the worker hands off" in prompt
    assert "If safe ownership cannot be expressed, do not parallelize the writes." in prompt
    assert "An agent completion message never substitutes for these steps." in prompt
    assert "Free-form text may accompany evidence but" in prompt
    assert "cannot carry permissions, risk limits, quantities, order instructions" in prompt

    policy = " ".join(
        (REPOSITORY_ROOT / "docs" / "POLICIES.md").read_text(encoding="utf-8").split()
    )
    assert "at least two distinct named read-only reviewers" in policy
    assert "one explicitly responsible for both Risk and Security" in policy
    assert "another responsible for QA" in policy
    assert "Neither reviewer may be an author" in policy
    assert "or its integrator" in policy
    assert "against the originating agent session/tool record" in policy
    assert "never replace a reviewer merely to obtain approval" in policy


def test_operating_prompt_closes_financial_and_governance_bypasses() -> None:
    prompt = OPERATING_PROMPT_PATH.read_text(encoding="utf-8")
    normalized_prompt = " ".join(prompt.split())

    assert (
        "The orchestrator may propose a classification but cannot independently approve"
        in normalized_prompt
    )
    assert "Pre-action Audit" in prompt
    assert "if this write fails, no action occurs." in prompt
    assert "A Risk decision, pre-action audit record, and Execution request" in prompt
    assert "A rejected proposal ID and digest cannot be retried." in prompt
    assert "Resume is not an emergency-priority action." in prompt
    assert "Use these lifecycle terms exactly:" in prompt
    assert "exact pull request and current head commit" in normalized_prompt
    assert "modified or newly added checks as the sole proof" in normalized_prompt
    assert "action_id` and `action_digest" in prompt
    assert "atomically increments the kill-switch generation" in normalized_prompt
    assert "approved pre-action audit sink" in normalized_prompt

    policy = " ".join(
        (REPOSITORY_ROOT / "docs" / "POLICIES.md").read_text(encoding="utf-8").split()
    )
    assert "Required-gate or approval-semantic changes remain at least RISK 3." in policy
    assert "exact current owner approval for the bounded change" in policy
    assert "An unmerged policy cannot unblock another task." in policy
    assert "dissenting required reviews block advancement" in policy
    assert "immutable base and candidate commit/tree" in policy
    assert "Any scope, code, contract, policy or material evidence change invalidates" in policy
    assert "Exact PR/head owner approval remains required for merge" in policy
    assert "Textual policy tests do not implement such a service." in policy


def test_operating_prompt_is_compact_english_and_directly_reusable() -> None:
    prompt_bytes = OPERATING_PROMPT_PATH.read_bytes()
    prompt = prompt_bytes.decode("utf-8")

    assert prompt.startswith("# QUANT ORG OS v2\n")
    assert len(prompt_bytes) < 50_000
    assert "## 14. Quantitative Research Discipline" in prompt
    assert "Communicate with the project owner in English" in prompt
    assert "Never expose hidden chain-of-thought." in prompt

    decision = (
        REPOSITORY_ROOT / "docs" / "decisions" / "0033-local-development-review-delegation.md"
    ).read_text(encoding="utf-8")
    assert "repository adoption pending exact review and merge" in decision
    assert "earlier development-agent findings do not become approvals retroactively" in decision
