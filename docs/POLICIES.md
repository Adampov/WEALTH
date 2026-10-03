# Governance and Approval Policy

## Authority and Source of Truth

The project owner retains approval authority. Codex, agents, application code, tests, and CI may
produce evidence and proposals; they cannot approve their own elevation of financial, security, or
operational authority.

Use these sources in order:

1. `docs/PROJECT_CHARTER.md`, `docs/AI_DEPARTMENTS.md`, `docs/ORGANIZATION.md`,
   `docs/ARCHITECTURE.md`, and accepted ADRs define approved direction, authority, and invariants.
2. `docs/SECURITY_POLICY.md`, `docs/RISK_POLICY.md`, and `docs/EXECUTION_POLICY.md` define control
   policy.
3. `docs/DATA_CONTRACTS.md` and `docs/DATA_CATALOG.md` index approved schemas and sources.
4. `PROJECT_STATE.json` is the validated current-state snapshot and canonical `next_action`.
5. `BACKLOG.md` defines the acceptance contract for that action.
6. `RISK_REGISTER.md` records known unresolved risk.

A task or chat transcript does not silently override these artifacts. Conflicts stop the affected
work until the owner records a decision.

## Current Operating Baseline

| Control | Current value |
|---|---|
| Phase | Phase 2 — Reliable Market Data Platform |
| Runtime environment / mode | `development` / `research` |
| Target mode after a real simulator exists | `paper` |
| Primary market / trading type | `crypto` / `spot` |
| Architecture | `modular_monolith` |
| System time / user display time | `UTC` / `Asia/Jerusalem` |
| Base currency | `USD` |
| Live trading | Disabled |
| Leverage | Disabled |
| Withdrawals | Disabled permanently for application credentials |
| External notifications | Disabled |
| Autonomous live execution | Disabled |

The `research` runtime is an approved temporary safer deviation from the desired paper-first
baseline: Phase 2 has no paper exchange or simulator, so labeling the runtime as paper would be
false. Paper remains the target after the Phase 5 simulator exists and passes its gates. This
deviation grants no execution permission.

All internal event, decision, audit, and persisted control timestamps must be timezone-aware UTC.
`Asia/Jerusalem` is presentation context only.

## Explicit Human Approval Matrix

Absence, ambiguity, expiry, or conflicting approval means **denied**. Approval must identify the
change, scope, environment, evidence, approver, time, expiry or review trigger, and rollback.

| Change | Required approval |
|---|---|
| Enable or expand live trading or autonomous live execution | Project owner after applicable roadmap exit gates and independent Risk/Security evidence |
| Add or change a live venue, account, sub-account, or private exchange integration | Project owner plus Security and Risk review |
| Enable leverage or increase an approved leverage limit | Project owner plus deterministic Risk policy change |
| Enable derivatives, futures, shorting, or a new instrument type | Project owner plus Risk, Execution, and data-contract review |
| Increase position, exposure, concentration, loss, drawdown, or other risk limits | Project owner plus versioned Risk policy and replay/fault evidence |
| Increase capital allocation or the capacity assigned to a strategy, model, asset, or venue | Project owner plus versioned Risk and portfolio-capacity evidence |
| Disable, bypass, or weaken a kill switch or safety halt | Project owner plus independent Risk and Security review; never an emergency shortcut |
| Expand connector, service, credential, filesystem, network, account, or withdrawal permissions | Project owner plus Security review; withdrawal permission remains prohibited |
| Change secret storage, access, rotation, logging, or retention policy | Project owner plus Security review |
| Resume after a risk, security, reconciliation, execution, or operational halt | Project owner after root cause, reconciliation, and recovery evidence |
| Promote a champion strategy or model into production or a higher-risk mode | Project owner plus independent out-of-sample, Risk, Audit, and rollback evidence |
| Use real money after a material strategy, model, prompt, data, parameter, or decision-logic change | Fresh project-owner authorization after paper/shadow, out-of-sample, Risk, Audit, and rollback evidence |
| Introduce or materially change an order-capable execution engine | Project owner plus Risk, Security, Execution, Audit, and fault-injection evidence |
| Change a canonical truth source for project state, market evidence, portfolio, orders, fills, accounting, or audit | Project owner plus owner(s) of affected contracts and migration/reconciliation evidence |
| Perform a major architecture, database, schema, retention, or state migration | Project owner plus an ADR, backup, validation, rollback, and affected control-owner review |

Approval of code review or merge is not approval to deploy, promote mode, access credentials, or
trade unless the recorded approval explicitly says so.

## Local Development Review Delegation

`DEV-REVIEW-V1` implements the owner's explicit decision recorded in ADR-0033. It changes who may
perform development review, not what the application or its agents may do. It becomes effective
only through the unchanged exact-candidate review, owner-authorized merge and verified adoption
process. A draft policy, its authors, or its proposed delegates cannot activate it. The amendment
itself receives independent engineering review under the owner's instruction, not approval using
powers it proposes to grant. An unmerged policy cannot unblock another task.

### Eligibility and separation

Eligibility depends on actual effects, not a `local`, `test-only`, or risk-tier label. The change
must be confined to an isolated development checkout and approved generated/public test inputs.
It may change development code, tests, documentation, or test-only validation machinery; existing
approved read-only CI may supply evidence. It must not create operational or financial authority.

For each eligible task, the orchestrator records at least two distinct named read-only reviewers:
one explicitly responsible for both Risk and Security, and another responsible for QA. Neither
reviewer may be an author of any part of the reviewed change or its integrator. They may share the
same root orchestrator, but root coordination, role labels, model agreement and task completion
messages are not approval decisions. The root verifies evidence and integrates; it cannot count
as either required reviewer. A reviewer who contributed implementation must be replaced before
review, with that conflict recorded. Reviewers cannot grant, expand or renew their own mandate.
Two role names or session labels for one agent do not establish distinct reviewers.

The delegates may issue a scope-bound `LOCAL_DEVELOPMENT_APPROVAL` only after reviewing the raw
contract, exact immutable diff, relevant unchanged baseline, check inventory and actual evidence.
The decision approves that eligible development patch only, never execution of future behavior.
Independent engineering and any task-specific additional reviews remain required.

### Evidence and decision validity

Use the existing task review packet; no new approval service is implied. Each review records:

- task ID, exact action/environment and file scope, contract generation and digest;
- policy version, immutable base and candidate commit/tree, raw-diff digest, and evidence/check-
  inventory identifiers and digests;
- reviewer identity, assigned Risk/Security or QA role, read-only status and author/integrator
  independence;
- findings, limitations, explicit decision, UTC issuance, expiry or review trigger, and rollback.

The orchestrator must verify reviewer identity and the actual decision against the originating
agent session/tool record, check author independence and every immutable binding, and preserve
that provenance in the packet. A copied role label, self-written approval, caller-supplied success
flag or unattested summary is insufficient. If the originating record cannot be verified, deny.
This is a manually checked engineering protocol, not cryptographic identity or a runtime authority
validator. Textual policy tests do not implement such a service.

Missing, stale, expired, mismatched, ambiguous, conditional, unverifiable, abstaining or dissenting required
reviews block advancement. Preserve negative decisions and findings; never replace a reviewer
merely to obtain approval. A correction requires a new exact candidate and fresh required reviews;
record how each prior finding was resolved. Any scope, code, contract, policy or material evidence
change invalidates the prior decision. Review completion and successful CI are not automatic
acceptance or merge authority.

### Unchanged controls and exclusions

Required-gate or approval-semantic changes remain at least RISK 3. They still require exact current
owner approval for the bounded change, independent Risk/Security and QA decisions, baseline and
candidate check inventories, monitoring, reconciliation, and tested rollback. No removed, skipped,
weakened or replaced check may be hidden. Modified checks cannot be the sole proof that changing
their own semantics is safe. This delegation does not waive a check, contract or lifecycle gate.

The delegation excludes production and operational permissions; paper/shadow execution authority;
restricted inputs and TASK-037 authorization; credentials, secrets and private accounts; network,
filesystem or connector permission expansion; financial or runtime Risk/Execution/Audit authority;
capital, exposure, leverage and kill-switch changes; external notifications; deployment; and merge.
It cannot authorize live trading, withdrawals, accepting high/critical residual operational risk,
or modifying these exclusions. Their existing organizational and human approval requirements stay
in force. Risk classification is not downgraded merely because reviewers are development agents.

Before rollback or revocation, stop issuing delegated decisions and invalidate dependent unconsumed
decisions. Reverting the exact policy commit restores the prior reviewer rules without deleting
evidence or changing data/runtime state. Exact PR/head owner approval remains required for merge;
normal dependency and source-of-truth rules still control use of an adopted policy in another task.

## Live Authorization Record

Any future live or real-money authority must be a versioned, auditable record—not a chat inference
or standing blanket permission. At minimum it identifies:

- Authorized markets and assets, venues and accounts, and strategies or execution engine.
- Maximum capital, gross and net exposure, leverage, per-trade and aggregate loss, and drawdown.
- Permitted order types and any price, size, liquidity, or session restrictions.
- Effective time window, explicit expiry, and reauthorization trigger.
- Required monitoring, reconciliation, alerting, and kill-switch state.
- Human approver, supporting Risk/Security/Audit evidence, and tested rollback.

An absent, expired, mismatched, or superseded record blocks real-money action. A material strategy
change invalidates the prior authorization until fresh approval is recorded.

## Change Control

- Every implementation uses a bounded Task Contract and dedicated branch.
- Material decisions use an ADR. Current state, risks, and backlog are updated in the same change.
- CI evidence is necessary but not sufficient for higher-risk promotion.
- Safer rollback, disablement, or rejection remains available at every gate.
