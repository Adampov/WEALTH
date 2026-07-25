# Risk Register

The current system has no account access, positions, orders, or execution path. These risks concern
Phase 2 public market data and engineering control state. Policy is defined in
`docs/RISK_POLICY.md`; material changes require an ADR and the approvals in `docs/POLICIES.md`.

| ID | Risk | State | Current controls | Next treatment |
|---|---|---|---|---|
| RISK-001 | Public sources can be unavailable, slow, incomplete, reordered, or semantically inconsistent. | Open / monitored | Bounded requests, classified retries, settlement delay, quality gates, explicit gaps, source-health evidence, and fail-closed admission. | Expand deterministic disconnect, sparse-window, and recovery evidence in each new collection flow. |
| RISK-002 | A provider can change payload shape, limits, precision, ordering, or endpoint semantics without notice. | Open / monitored | Strict adapter validation, provider-independent canonical contracts, raw-byte lineage, response-cap checks, and contract tests. | Add fixture/version review and an operator-visible schema-drift runbook before continuous collection. |
| RISK-003 | Cooperating collectors can exceed provider rate limits or local work bounds, especially across crashes or future hosts. | Open / controlled locally | The bounded orchestrator retains shared durable weighted budget gating before provider access; ranges, requests, records, splits, retries, and waits remain finite. | Preserve pre-request gating in any continuous collector; do not claim multi-host or crash-durable per-job hard limits without a separate reservation design. |
| RISK-004 | Market evidence and checkpoint control state can diverge because their SQLite databases cannot commit atomically. | Open / recovery tested and inspectable locally | Evidence-first orchestration, idempotent market storage, exact pending-leaf recovery, typed terminal mapping, compare-and-swap transitions, UUID fencing, both crash-seam tests, and bounded typed causal inspection of every retained actor transition. | Keep this risk open; require operational recovery drills and accepted evidence before continuous collection. |
| RISK-005 | Remaining timestamp contracts and persisted JSON or text may preserve a caller offset, including newer stores whose indexed projections are UTC. Mixed textual offsets can also sort incorrectly. | Open / inventoried and planned | `DomainEvent`, `ProjectState`, bounded public-trade orchestration, and typed public-trade transition reads are zero-offset-strict today, though they do not yet prove fixed `datetime.UTC` zone identity; provider market times and selected projections normalize UTC; most remaining models are aware-only. The [TASK-026 inventory and plan](docs/CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md) records the complete repository evidence without changing runtime or data. | TASK-027 enforces fixed `datetime.UTC` at injected clock boundaries without changing persisted models. Then add the canonical codec, read-only legacy preflight, compatibility readers, approved version-2 schemas, quarantine, backup/rollback, and migration verification before declaring global closure. |

## Escalation

- A new critical risk, suspected credential exposure, corrupt trusted evidence, or bypassed safety
  control stops the affected workflow and is recorded immediately.
- High or critical residual risk cannot be silently accepted by code, CI, an agent, or a task
  author. It requires explicit project-owner approval and a documented expiry or treatment.
- Resuming after a safety halt requires the human approval specified in `docs/POLICIES.md`.

Review this register whenever a provider contract, storage schema, permission, operating mode,
execution boundary, or canonical source of truth changes.
