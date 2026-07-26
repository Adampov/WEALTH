"""Fail-closed checks for the placeholder-only TASK-037 governance artifact."""

import hashlib
import json
import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BACKLOG_PATH = REPOSITORY_ROOT / "BACKLOG.md"
PROJECT_STATE_PATH = REPOSITORY_ROOT / "PROJECT_STATE.json"
TEMPLATE_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "governance"
    / "TASK-037-operator-preflight-authorization-package.template.md"
)

EXPECTED_FAMILY_ROWS = (
    (0, "market", "synthetic_path_slot_market"),
    (1, "order_flow", "synthetic_path_slot_order_flow"),
    (2, "historical_collection", "synthetic_path_slot_historical_collection"),
    (3, "continuous_collection", "synthetic_path_slot_continuous_collection"),
    (4, "collector_service", "synthetic_path_slot_collector_service"),
    (5, "public_trade_collection", "synthetic_path_slot_public_trade_collection"),
    (6, "rate_budget", "synthetic_path_slot_rate_budget"),
    (7, "reconciliation", "synthetic_path_slot_reconciliation"),
)
EXPECTED_NORMALIZED_TEMPLATE_SHA256 = (
    "9a964bdd16c9b0f312ec18190608f8b70287957061cd15f1fb6b40c9eeffd8bb"
)
EXPECTED_NORMALIZED_NEXT_ACTION_SHA256 = (
    "9de77cb9f91366e9b90807c1dd739ed488a8a72f18e50f5a2db60b8103d3afe6"
)


def _template_text() -> str:
    return TEMPLATE_PATH.read_text(encoding="utf-8")


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def test_template_exists_but_backlog_keeps_task_037_ready_and_unapproved() -> None:
    source = _template_text()
    backlog = BACKLOG_PATH.read_text(encoding="utf-8")
    project_state = json.loads(PROJECT_STATE_PATH.read_text(encoding="utf-8"))
    next_action = backlog.split("## Next Action", maxsplit=1)[1].split(
        "## Recently Completed",
        maxsplit=1,
    )[0]

    assert TEMPLATE_PATH.is_file()
    assert "### TASK-037 " in next_action
    assert "- **Risk tier:** RISK 3" in next_action
    assert "- **Status:** READY" in next_action
    assert "- **Human approval:** REQUIRED" in next_action
    assert tuple(line for line in next_action.splitlines() if line.startswith("- **Status:**")) == (
        "- **Status:** READY",
    )
    assert tuple(
        line for line in next_action.splitlines() if line.startswith("- **Human approval:**")
    ) == (
        "- **Human approval:** REQUIRED — project owner plus independent Risk and Security review.",
    )
    assert not any(
        line.startswith(("- **Owner decision:**", "- **Authorization disposition:**"))
        for line in next_action.splitlines()
    )
    assert str(TEMPLATE_PATH.relative_to(REPOSITORY_ROOT)).replace("\\", "/") in next_action
    assert "cannot satisfy any acceptance gate or approval requirement" in next_action
    assert "Repository placeholder only — grants no authority." in source
    assert (
        hashlib.sha256(next_action.replace("\r\n", "\n").encode("utf-8")).hexdigest()
        == EXPECTED_NORMALIZED_NEXT_ACTION_SHA256
    )
    assert project_state["next_action"] == {
        "task_id": "TASK-037",
        "action": ("phase2.canonical_utc_preflight_operator_authorization_package_owner_decision"),
    }
    assert project_state["open_tasks"] == [
        {
            "task_id": "TASK-037",
            "action": (
                "phase2.canonical_utc_preflight_operator_authorization_package_owner_decision"
            ),
            "status": "ready",
            "risk_tier": 3,
            "requires_human_approval": True,
        }
    ]
    assert project_state["pending_approvals"] == [
        "TASK-037 project-owner decision plus independent Risk and Security reviews for the exact "
        "operator-preflight authorization package"
    ]


def test_normalized_template_matches_the_reviewed_content_digest() -> None:
    normalized_source = _template_text().replace("\r\n", "\n")

    assert (
        hashlib.sha256(normalized_source.encode("utf-8")).hexdigest()
        == EXPECTED_NORMALIZED_TEMPLATE_SHA256
    )


def test_template_control_record_is_explicitly_non_authorizing() -> None:
    source = _template_text()
    required_lines = {
        "classification: PLACEHOLDER_ONLY",
        "project_id: WEALTH",
        "task_id: TASK-037",
        "risk_tier: 3",
        "package_id: NOT_ASSIGNED",
        "package_revision: NOT_ASSIGNED",
        "restricted_package_reference: NOT_RECORDED",
        "owner_decision: NOT_RECORDED",
        "authorization_disposition: DENIED",
        "authority_effect: NONE",
        "operator_access: NOT_AUTHORIZED",
        "stage3_gate: NOT_SATISFIED",
        "automatic_execution: false",
        "scanner_authorized: false",
        "snapshot_execution_state: NOT_EXECUTED",
        "report_creation_state: NOT_CREATED",
        "real_paths_allowed_in_repository_copy: false",
    }

    for line in required_lines:
        assert source.count(line) == 1
    assert "owner_decision: APPROVE" not in source
    assert "authorization_disposition: APPROVED" not in source
    assert "operator_access: AUTHORIZED" not in source
    assert "stage3_gate: SATISFIED" not in source
    assert "scanner_authorized: true" not in source
    assert "automatic_execution: true" not in source
    current_owner_decision_lines = tuple(
        line for line in source.splitlines() if line.startswith("Current project-owner decision:")
    )
    assert current_owner_decision_lines == ("Current project-owner decision: `NOT_RECORDED`.",)


def test_family_inventory_is_exact_but_never_claims_real_cardinality() -> None:
    source = _template_text()
    collapsed_source = _collapse_whitespace(source)
    matches = re.findall(
        r"^\| ([0-7]) \| `([^`]+)` \| `([^`]+)` "
        r"\| `OWNER_REQUIRED` \| `OWNER_REQUIRED` \|$",
        source,
        flags=re.MULTILINE,
    )
    rows = tuple((int(ordinal), family, token) for ordinal, family, token in matches)

    assert rows == EXPECTED_FAMILY_ROWS
    for _, _, token in EXPECTED_FAMILY_ROWS:
        assert source.count(f"`{token}`") == 1
    assert "They do not state which families are" in source
    assert "deployed, how many physical databases exist" in collapsed_source
    assert "must never default to eight" in collapsed_source
    assert "deployed families may have multiple path entries" in collapsed_source


def test_repository_copy_contains_no_real_path_or_location_value() -> None:
    source = _template_text()
    forbidden_fragments = (
        "file://",
        "sqlite://",
        "/home/",
        "/mnt/",
        "/private/",
        "/tmp/",
        "/var/",
        "$HOME",
        "${HOME}",
        "%USERPROFILE%",
        "\\\\",
        ".db",
        ".sqlite",
        ".sqlite3",
        ".wal",
        ".shm",
    )

    assert not re.search(r"(?i)\b[a-z]:[\\/]", source)
    assert not re.search(r"(?i)\b[a-z][a-z0-9+.-]*://", source)
    for fragment in forbidden_fragments:
        assert fragment not in source
    assert "Do not enter a real database path" in source
    assert "Populate a separate copy only inside a Security-approved restricted" in source
    assert "Paths must be retained exactly as the owner supplies them." in source
    assert "must not resolve, normalize,\nopen, inspect, or test them" in source


def test_restricted_package_requires_every_risk_3_boundary() -> None:
    source = _template_text()
    collapsed_source = _collapse_whitespace(source).lower()
    required_headings = (
        "### Identity and environment",
        "### Exact read-only path scope",
        "### Snapshot procedure",
        "### Report, manifest, and external anchor",
        "### Evidence retention and disposal",
        "### Monitoring and revocation",
        "### Tested rollback",
        "### Risk review",
        "### Security review",
        "## Project-owner decision",
        "## Fail-closed rules",
    )
    required_phrases = (
        "writer-fence steps",
        "generation or watermark",
        "WAL and checkpoint policy",
        "Immutability mechanism",
        "Exact report destination",
        "Separate external anchor destination",
        "Exact retention trigger and duration",
        "Disposal trigger, method",
        "Approval expiry, revocation, and package-revision monitoring",
        "Monitoring cadence, alert thresholds, automatic halt criteria",
        "Binding of every monitor and alert to the exact package revision",
        "Source-unchanged verification",
        "exact tested procedure and package scope",
        "Exact maximum age of rollback-test evidence",
        "passing tested-rollback evidence",
        "reviewer's authority basis",
        "immutable evidence reference and digest",
        "exact requested scope",
        "independent of package preparation and the owner decision",
        "`DEPLOYED` requires at least one path entry and `NOT_DEPLOYED` requires none",
        "Both outcomes must be `APPROVE`",
        "exact UTC RFC 3339 `Z` times",
        "expiry later than its effective time",
        "`APPROVE` requires at least one real path and one `DEPLOYED` family",
        "Any package change invalidates both reviews.",
        "Current populated-package Risk review: `NOT_PERFORMED`.",
        "Current populated-package Security review: `NOT_PERFORMED`.",
        "Current project-owner decision: `NOT_RECORDED`.",
        "`NOT_APPLICABLE` is prohibited for every approval-gate field",
        "`APPROVE` is invalid and only `REJECT` or `REVISE` may be recorded",
    )

    for heading in required_headings:
        assert source.count(heading) == 1
    for phrase in required_phrases:
        assert _collapse_whitespace(phrase).lower() in collapsed_source


def test_template_keeps_scanner_and_stage_3_out_of_scope() -> None:
    source = _template_text()

    assert "It does not execute anything, perform Stage 3," in source
    assert "A scanner remains a separate later task." in source
    assert (
        "TASK-037 performs only approved governance-artifact writes: no operator-path inspection,"
        in source
    )
    assert "SQLite access, report creation, scanner, runtime, migration, schema change" in source
    assert source.rstrip().endswith("`DENIED`.")
