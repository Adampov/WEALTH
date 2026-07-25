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

### Live authorization record

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
