# Risk Register

The current system has no account access, positions, orders, or execution path. These risks concern
Phase 2 public market data and engineering control state. Policy is defined in
`docs/RISK_POLICY.md`; material changes require an ADR and the approvals in `docs/POLICIES.md`.

| ID | Risk | State | Current controls | Next treatment |
|---|---|---|---|---|
| RISK-001 | Public sources can be unavailable, slow, incomplete, reordered, or semantically inconsistent. | Open / monitored | Bounded requests, classified retries, settlement delay, quality gates, explicit gaps, source-health evidence, and fail-closed admission. | Expand deterministic disconnect, sparse-window, and recovery evidence in each new collection flow. |
| RISK-002 | A provider can change payload shape, limits, precision, ordering, or endpoint semantics without notice. | Open / monitored | Strict adapter validation, provider-independent canonical contracts, raw-byte lineage, response-cap checks, and contract tests. | Add fixture/version review and an operator-visible schema-drift runbook before continuous collection. |
| RISK-003 | Cooperating collectors can exceed provider rate limits or local work bounds, especially across crashes or future hosts. | Open / controlled locally | Shared durable weighted budget before network access; bounded ranges, requests, records, splits, retries, and waits. | Preserve pre-request budget gating in TASK-024; do not claim multi-host or crash-durable per-job hard limits. |
| RISK-004 | Market evidence and checkpoint control state can diverge because their SQLite databases cannot commit atomically. | Open / designed recovery | Evidence-first ordering, idempotent market storage, exact pending-leaf recovery, compare-and-swap transitions, UUID fencing, and append-only health evidence. | Prove both crash seams and typed terminal classification in TASK-024. |
| RISK-005 | Remaining timestamp contracts and persisted JSON or text may preserve a caller offset, including newer stores whose indexed projections are UTC. Mixed textual offsets can also sort incorrectly. | Open | Wired clocks and providers produce UTC; UTC is required for new or modified contracts; covered projections normalize UTC; timezone-aware validation prevents naive time in covered paths. | Inventory every uncovered domain, port, application, and persistence boundary; define one canonical UTC type and migration behavior; add regression and migration tests before declaring global closure. |

## Escalation

- A new critical risk, suspected credential exposure, corrupt trusted evidence, or bypassed safety
  control stops the affected workflow and is recorded immediately.
- High or critical residual risk cannot be silently accepted by code, CI, an agent, or a task
  author. It requires explicit project-owner approval and a documented expiry or treatment.
- Resuming after a safety halt requires the human approval specified in `docs/POLICIES.md`.

Review this register whenever a provider contract, storage schema, permission, operating mode,
execution boundary, or canonical source of truth changes.
