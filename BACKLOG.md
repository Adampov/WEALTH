# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-024 — Public-trade checkpoint orchestrator

- **Key:** `phase2.public_trade_checkpoint_orchestrator`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Compose the existing bounded public-trade range collector, market-evidence store,
  durable request budget, and restart-safe checkpoint store into one explicit application flow.
- **Scope:** One operator-invoked bounded job; checkpoint create/load, lease acquisition and
  release, policy-fingerprint validation, evidence-first persistence, compare-and-swap progress,
  source-health classification, and restart recovery.
- **Constraints:** Public read-only data only; injected trusted UTC clock; all work, retries,
  waits, records, and ranges remain bounded; no credentials, scheduler, daemon, WebSocket,
  signal, portfolio, risk approval, order, or financial action.

Acceptance gates:

1. A new or existing job can acquire a fresh UUID-fenced lease and reject stale workers,
   conflicting versions, or a changed policy fingerprint.
2. Recovery resumes from the durable cursor and exact pending adaptive leaf. A crash after market
   evidence commits but before control state advances causes an idempotent refetch, never a gap.
3. Market evidence is durable before the matching checkpoint transition. The transition and
   source-health observation commit atomically in the control database.
4. Clean outer request- or record-limit stops become resumable `PAUSED` outcomes. Health is
   `HEALTHY` or `DEGRADED` from typed trace evidence; typed terminal source or admission failures
   become `FAILED` with a bounded canonical failure code.
5. The shared durable provider-rate budget is reserved before every provider request. The job does
   not claim crash-durable per-job request accounting unless a reservation design proves it.
6. Unit and fault-injection tests cover clean completion, pause, typed failure, policy drift,
   lease conflict, compare-and-swap conflict, and both sides of the evidence/checkpoint crash seam.
7. Format, lint, type, test, lockfile, health-slice, dependency-audit, and CI gates pass.
8. The ADR, roadmap, backlog, risk register, and `PROJECT_STATE.json` are updated with the result
   and the next bounded action.

## Queued, Not Yet Approved

- Add a typed bounded reader for append-only public-trade checkpoint transition history.
- Audit and normalize all remaining uncovered timestamp contracts and persistence paths to
  canonical UTC
  (`RISK-005`).
- Design continuous public-trade collection only after TASK-024 recovery evidence is accepted.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
