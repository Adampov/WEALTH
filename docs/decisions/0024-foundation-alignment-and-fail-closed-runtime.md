# ADR 0024: Foundation Alignment and Fail-Closed Runtime Authority

- **Status:** Accepted
- **Date:** 2026-07-25
- **Decision owners:** Project owner, Engineering Department, Security Department, Risk
  Department, Execution Department, and Audit Department

## Task Contract

### Goal

Make the approved project direction, current state, next action, permissions, and safety posture
explicit, machine-checkable where practical, and fail closed without adding financial capability.

### Context

The repository has a strong Phase 2 market-data foundation and no order path, but its current state
and next action are distributed across prose. Runtime settings identify environment and mode
without declaring every sensitive permission. Central security, risk, execution, approval,
backlog, and risk-register artifacts are not yet canonical.

### Scope

- Add a validated root `PROJECT_STATE.json` contract and repository fixture.
- Add `BACKLOG.md`, `RISK_REGISTER.md`, and compact governance, security, risk, and execution
  policies.
- Expand runtime identity with explicit market, timezone, currency, architecture, and sensitive
  control flags that fail closed.
- Require UTC for covered internal event and current-state timestamps.
- Update source-of-truth navigation, roadmap status, and ADR index.

### Constraints

- Preserve the accepted modular monolith and Phase 2 scope.
- Public read-only market data and local non-account evidence only.
- No paper simulator, strategy, AI model, portfolio, private exchange access, credential, order
  interface, deployment, external notification, leverage, or live/autonomous execution.
- Document remaining gaps honestly; do not claim global closure where uncovered paths still need an
  audit.

### Done When

- The repository state fixture validates strictly and names exactly one next action.
- Sensitive runtime flags default disabled and invalid escalations are rejected.
- Current authority, approval requirements, risks, and execution prohibition are explicit and
  linked from the README.
- Unit, format, lint, type, lockfile, health-slice, dependency-audit, and CI checks pass.

### Not Included

- Implementing TASK-024, normalizing every uncovered timestamp or persistence path, starting a
  collector, changing
  deployment, or adding any trading capability.

## Decision

### Canonical current state

Use root `PROJECT_STATE.json` as the compact, schema-validated snapshot of phase, operating mode,
architecture, components, integrations, data sources, strategies, risk state, financial state,
tasks, blockers, known risks, pending approvals, recent decisions, next action, and update time.
It is not an event log and does not replace the charter, architecture, ADRs, policies, backlog, or
risk register.

The fixture references TASK-024,
`phase2.public_trade_checkpoint_orchestrator`, as the one next action. State changes must validate
and update related governance artifacts in the same review.

### Safe runtime baseline

Declare the baseline as development/research, crypto Spot, USD, modular monolith, UTC internal
time, and Asia/Jerusalem user display time. Live trading, leverage, withdrawals, external
notifications, and autonomous live execution default disabled and fail closed under the current
Phase 2 authority.

The desired paper-first operating baseline is retained as a target, but **the runtime remains
`research` as an approved temporary safer deviation until a real paper exchange or simulator
exists**. Calling the current runtime paper would create false operational evidence. Moving to
paper requires the Phase 5 capability and gates; this exception grants no execution authority.

Timezone-aware UTC is the canonical form for internal events, audit records, and persisted control
state. Asia/Jerusalem may be used only at a presentation boundary. Covered contracts reject
non-UTC values; the remaining uncovered-path inventory and normalization are tracked as
`RISK-005`.

### Governance and authority

Adopt `docs/POLICIES.md`, `docs/SECURITY_POLICY.md`, `docs/RISK_POLICY.md`, and
`docs/EXECUTION_POLICY.md`. Missing or ambiguous approval is denial. Code, CI, and agents may
validate and recommend but cannot authorize live operation, greater permissions, risk increases,
execution, or their own promotion.

## Consequences

### Positive

- A new session can resolve current phase, authority, risk, and next work without relying on chat
  history.
- Sensitive capabilities are explicit negative permissions rather than assumptions.
- The safer research deviation is truthful and has a defined review gate.
- Governance and implementation can cross-reference stable task and risk identifiers.

### Negative

- State and policy artifacts require deliberate maintenance in material changes.
- Strict settings can reject configurations that were previously parsed even though no order path
  existed.
- Global UTC conformance is not declared complete until `RISK-005` is closed.

## Alternatives Considered

### Default to paper immediately

Rejected. No paper exchange or simulator exists, so the label would misrepresent actual behavior.

### Rely on prose and the absence of order code

Rejected. Implicit permissions and distributed current state are difficult to audit and fail open
as the system grows.

### Add future trading contracts now

Rejected as speculative and outside Phase 2. Typed intent, portfolio, Risk, and execution
contracts belong to later roadmap gates.

## Review Triggers

Review when TASK-024 completes, a real paper simulator is proposed, any sensitive control becomes
enabled, the canonical truth source changes, `RISK-005` closes, or a private/order-capable boundary
is introduced.
