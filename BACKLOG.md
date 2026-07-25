# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-026 — Canonical UTC boundary inventory and migration plan

- **Key:** `phase2.canonical_utc_boundary_inventory_and_migration_plan`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Produce an evidence-backed inventory of every timestamp boundary still covered by
  `RISK-005` and one reviewable, staged plan for converging them on canonical UTC.
- **Scope:** Catalog timestamp-bearing domain, port, application, adapter, persistence, JSON, and
  text boundaries; record their validation, normalization, serialization, comparison, sorting,
  indexing, and test behavior; then define compatibility, quarantine, rollback, migration-order,
  and regression-test requirements.
- **Constraints:** Inventory and plan only. Do not change a runtime contract, schema, stored data,
  migration, or application behavior; do not write to a project database, repair data, call a
  provider, schedule work, access credentials, produce a signal, make a portfolio or Risk
  decision, submit an order, or perform any financial action.

Acceptance gates:

1. The inventory names every timestamp-bearing canonical model and persistence path discoverable
   from the repository, its owner, representation, and current UTC guarantee or gap.
2. Stored JSON/text, indexed projections, SQL ordering, cursor logic, comparisons, clocks, and
   provider inputs are traced separately so an indexed-UTC projection cannot hide
   offset-preserving canonical content.
3. Each uncovered boundary has evidence links, an impact classification, compatibility concerns,
   and an explicit proposed treatment; unknown behavior remains marked unknown rather than
   inferred.
4. The plan defines one canonical UTC representation and staged contract, storage, and test work,
   including quarantine, rollback, backward-compatibility, and migration verification.
5. The output identifies decisions or approvals required before any incompatible contract,
   schema, or stored-data migration begins.
6. Repository state remains unchanged apart from inventory, planning, and governance artifacts;
   no migration or runtime implementation is authorized by completing this task.
7. Relevant format, lint, link, state-validation, and CI gates pass.
8. The roadmap, backlog, risk register, data-contract index, and `PROJECT_STATE.json` are updated
   with the planning result and the next bounded action.

## Recently Completed

### TASK-025 — Typed public-trade transition-history reader

- **Key:** `phase2.public_trade_transition_history_reader`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0026-typed-public-trade-transition-history.md`
- **Result:** The existing append-only SQLite transition ledger is now exposed through an
  immutable typed record and a read-only port with ascending contiguous checkpoint-version pages,
  an exclusive cursor, actor-authority and lifecycle validation, strict bounds, restart behavior,
  and fail-closed projection, canonical-record, continuity, and corruption checks. The existing
  schema is unchanged.

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

- Design continuous public-trade collection only after typed transition audit access and
  operational recovery drills are accepted.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
