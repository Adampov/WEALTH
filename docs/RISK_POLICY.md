# Risk Policy

## Current State

- **Policy version:** 1.0
- **Change risk tier:** RISK 1 — DEVELOPMENT
- **Financial risk state:** `NO_TRADING_CAPABILITY`
- **Positions / open orders / exchange balances:** none and inaccessible

This policy protects evidence and future capital boundaries. It does not claim that Phase 2 market
data is complete or that any strategy is safe or profitable.

## Task Risk Classification

Every task records one tier before work begins:

| Tier | Meaning | Minimum control |
|---|---|---|
| RISK 0 — READ ONLY | Research, inspection, planning, or reporting with no state change | Verify sources and preserve confidentiality |
| RISK 1 — DEVELOPMENT | Code, schema, test, documentation, or non-production infrastructure change | Isolated branch, Task Contract, automated checks, review, and rollback |
| RISK 2 — SIMULATED EXECUTION | Backtest, paper, or shadow execution | Independent QA, replay/fault evidence, complete audit, and explicit scope |
| RISK 3 — PRODUCTION AFFECTING | Production, permission, risk-limit, or live-infrastructure change | Explicit owner approval, independent Risk/Security review, monitoring, and tested rollback |
| RISK 4 — LIVE CAPITAL | An action that can directly affect real money | A current live-authorization record plus every deterministic Risk, Execution, Audit, and kill-switch gate |

Higher tiers inherit all lower-tier controls and require stronger evidence, narrower autonomy, and
more explicit approval. Ambiguity is classified upward and fails closed until resolved.

## Non-Negotiable Rules

- Missing, stale, malformed, conflicting, unreconciled, or unauthorized critical state results in
  rejection, pause, quarantine, or halt—not an optimistic default.
- A strategy, model, committee, operator urgency, or expected return cannot override deterministic
  Risk rejection.
- Risk may reduce, reject, expire, revoke, or halt. It cannot increase exposure beyond versioned
  policy.
- Every future financial action requires a current intent, portfolio state, deterministic Risk
  approval, execution permission, and complete audit correlation.
- Risk policy, limits, approvals, exceptions, and state transitions are versioned and timestamped
  in UTC.
- Leverage and live trading remain disabled. Withdrawal permission is prohibited.

## Phase 2 Data-Risk Controls

Public collection must enforce explicit finite bounds on time range, rows, requests, weighted rate
budget, retries, waits, splits, memory, leases, and query pages. Data admission requires canonical
validation and deterministic quality evidence before storage. Conflicts are quarantined rather
than overwritten; gaps and unknowns remain explicit.

Checkpoint progress must never advance before accepted market evidence is durable. When two stores
cannot commit atomically, recovery favors a safe idempotent refetch over a permanent evidence gap.
Unsupported failure, corrupt trusted state, inconsistent identity, expired authority, or an
exhausted failure bound pauses or fails the workflow for operator review.

## Risk Register and Change

`RISK_REGISTER.md` is the current risk inventory. Each material task reviews affected entries,
adds newly discovered risk, and records residual limitations. A high or critical residual risk
requires explicit owner acceptance with scope, expiry, monitoring, and rollback; code or an agent
cannot self-accept it.

Increasing a financial limit, enabling leverage or derivatives, changing the kill switch, or
resuming after a halt requires the approval in `docs/POLICIES.md`. Future executable limits must
be machine-enforced and property/fault tested before paper operation.

## Review Triggers

Review before introducing portfolio state, a risk gateway, simulator, strategy, private account
data, order intent, execution, leverage, derivatives, or any higher-risk operating mode.
