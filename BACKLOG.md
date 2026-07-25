# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-025 — Typed public-trade transition-history reader

- **Key:** `phase2.public_trade_transition_history_reader`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Expose the existing append-only public-trade checkpoint transitions through one
  strict, bounded, causally ordered audit contract.
- **Scope:** An immutable typed transition record, a read-only store port, bounded SQLite pages
  using checkpoint version as the exclusive cursor, full projection and record validation, and
  restart, pagination, and corruption tests.
- **Constraints:** Reuse the existing transition table without schema migration; perform no
  control-state mutation, repair, collection, network request, scheduling, credential access,
  signal, portfolio, risk approval, order, or financial action.

Acceptance gates:

1. A transition record exposes the validated canonical checkpoint and the actor fencing token
   retained for that transition without duplicating mutable control state.
2. `transitions_for_job` returns ascending, contiguous checkpoint versions with an exclusive
   version cursor, a default page of 100, and a hard maximum of 1,000.
3. Initial creation, lease claim, renewal, pause, resume, failure, takeover, and completion records
   retain their exact causal order and actor authority.
4. Malformed canonical JSON, projection mismatch, version gap, impossible transition, or invalid
   actor token fails with the bounded existing control-storage error boundary.
5. Reads remain bounded and do not update the database, infer order from timestamps, or duplicate
   the separate source-health history contract.
6. Unit and integration tests cover pagination, restart, empty and missing jobs, every transition
   family, and targeted corruption of each indexed projection and canonical record.
7. Format, lint, type, test, lockfile, health-slice, dependency-audit, and CI gates pass.
8. The ADR, roadmap, backlog, risk register, data-contract index, and `PROJECT_STATE.json` are
   updated with the result and the next bounded action.

## Recently Completed

### TASK-024 — Public-trade checkpoint orchestrator

- **Key:** `phase2.public_trade_checkpoint_orchestrator`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0025-bounded-public-trade-checkpoint-orchestration.md`
- **Result:** One explicitly invoked bounded application flow now composes the public-trade range
  collector, durable request budget, market-evidence admission, and restart-safe checkpoint
  control with policy validation, UUID fencing, evidence-first progress, typed outcomes, and
  injected UTC time.

## Queued, Not Yet Approved

- Audit and normalize all remaining uncovered timestamp contracts and persistence paths to
  canonical UTC
  (`RISK-005`).
- Design continuous public-trade collection only after typed transition audit access and
  operational recovery drills are accepted.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
