# ADR 0025: Bounded Public-Trade Checkpoint Orchestration

- **Status:** Accepted
- **Date:** 2026-07-25
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Task Contract

### Goal

Compose the existing bounded public-trade range collector, market-evidence admission, durable
request budget, and restart-safe checkpoint store into one explicit application flow that can
recover without skipping evidence.

### Context

ADRs 0021–0023 establish adaptive bounded range collection, shared durable pre-request capacity
control, and restart-safe public-trade checkpoints and source-health evidence. They intentionally
leave application composition unresolved. Without that boundary, no component validates a job's
immutable policy, claims its fenced lease, maps typed range outcomes to durable lifecycle state,
or enforces evidence-before-checkpoint recovery ordering.

### Scope

- Create or load one bounded public-trade job and validate its immutable stream, range, and policy
  fingerprint.
- Claim or recover the job through the existing compare-and-swap store with a fresh UUID fencing
  token and bounded lease.
- Invoke one finite adaptive range collection from the durable cursor, or at most two bounded
  segments when recovery must finish the exact pending leaf before the remaining range.
- Map typed completion, clean work-bound pause, and terminal source or admission failure into one
  checkpoint transition and matching source-health observation.
- Advance control only after market evidence is durable, using injected UTC time and identifiers.
- Prove policy drift, stale authority, compare-and-swap conflict, typed classification, and both
  sides of the evidence/control crash seam.

### Constraints

- Public, unauthenticated, read-only provider access only.
- Every range, request, record count, split, retry, wait, lease, and invocation remains finite.
- Preserve the accepted modular monolith, provider ports, canonical quality gate, idempotent
  evidence store, shared durable request budget, and dedicated control database.
- Never infer lifecycle from free-form stop text or copy arbitrary upstream machine codes into
  durable control state.
- Do not claim cross-database atomicity or crash-durable per-job hard request accounting.

### Done When

- New, paused, and failed jobs can be claimed with fresh fencing authority; active or stale workers
  cannot advance them without the expected version and lease token.
- Recovery uses the exact durable cursor and retained pending-leaf end and rejects a changed policy
  fingerprint.
- Accepted market evidence is durable before checkpoint cursor or counter advancement. The lease
  claim is a preceding control-only transition. A crash after the evidence write but before the
  work-progress commit causes a safe idempotent refetch.
- Clean outer request- or record-limit stops become `PAUSED`; true terminal source or admission
  failures become `FAILED`; complete ranges become `COMPLETED`.
- When lease authority remains current and compare-and-swap succeeds, the checkpoint transition
  and matching health observation commit atomically inside the control database with bounded
  canonical failure evidence. Lost authority or a version conflict leaves the durable cursor
  unchanged.
- Relevant unit, integration, fault, formatting, lint, type, lockfile, health-slice, dependency
  audit, and CI gates pass before acceptance.

### Not Included

- A typed reader for append-only checkpoint transition history.
- Crash-durable per-job attempt reservations beyond the existing shared provider-rate budget.
- A scheduler, daemon, operating-system service, automatic resume, continuous polling, live
  WebSockets, provider gap recovery, or multi-host coordination.
- Uncovered UTC normalization work tracked by `RISK-005`.
- Credentials, private or account data, strategies, signals, portfolio state, Risk decisions,
  orders, execution, or any financial action.

## Decision

Add one application-level orchestrator for an explicitly requested bounded job. It owns lifecycle
composition but does not replace the existing provider, ingestion, storage, budget, or control
contracts.

The flow:

1. Loads or creates the job and verifies its durable immutable identity and policy fingerprint.
2. Claims it with a new UUID fencing token through the exact checkpoint version.
3. Selects the exact retained pending leaf when present; otherwise it begins at the durable cursor.
4. Invokes the bounded adaptive range path behind the durable shared request budget and
   fail-closed market-evidence admission. If the retained leaf completes before the immutable job
   end, it commits that progress and may invoke the remaining range once under the same renewed
   lease.
5. Maps the typed result into a bounded lifecycle outcome and source-health observation.
6. Commits the new checkpoint and matching health record through lease-authorized
   compare-and-swap.

The range flow admits each accepted raw/canonical batch before returning it as progress. After the
control-only lease claim, the orchestrator therefore advances the checkpoint cursor and work
counters only after evidence is durable. It never reverses that order to avoid a refetch: skipping
evidence is worse than safely repeating an idempotent leaf.

A clean local outer request or record limit is a resumable `PAUSED` outcome. Its health is
`HEALTHY` only when no retry or adaptive split degraded the admitted path; otherwise it is
`DEGRADED`. A typed terminal source or admission failure becomes `FAILED` and is translated into a
bounded canonical control code. Complete coverage becomes `COMPLETED`.

The orchestrator receives time from an injected trusted UTC clock. The store continues to enforce
monotonic transition time, lease bounds, UUID non-reuse, immutable identity, and exact
compare-and-swap authority.

The complete collection policy is mandatory construction input. In particular, the shared budget
key, capacity, period, and provider request cost have no implicit cost-one or separate-key
fallback.

The source boundary checks returned source, venue, canonical instrument, instrument type, and trade
record family before evidence storage. The request carries the durable provider symbol, but the
returned batch contract does not independently expose that symbol as a response field.

## Safety Boundary

This decision authorizes one explicitly invoked bounded public-data workflow. It does not start a
background process, schedule another invocation, use a credential, access an account, produce a
trading signal, make a portfolio or Risk decision, or submit an order. Runtime controls for live
trading, leverage, withdrawals, external notifications, and autonomous execution remain disabled.

## Consequences

### Positive

- Public-trade range progress can resume from durable state without an evidence gap.
- Policy drift, stale workers, and ambiguous terminal classification fail closed.
- Operational health and checkpoint progress share one causal control transition.
- Crash behavior is explicit: each interrupted attempt can cause a bounded idempotent refetch;
  repeated crashes may repeat the same retained work without creating a gap.

### Negative

- Evidence and control state still cannot commit atomically across their separate SQLite
  databases.
- A request completed before an uncommitted transition may not appear in per-job lifetime
  counters; the shared durable provider budget remains the pre-request capacity protection.
- The stored actor transition ledger still lacks a typed read port.
- Explicit invocation remains an operator responsibility; no automatic recovery service exists.

## Alternatives Considered

### Advance the checkpoint before market evidence

Rejected because a crash could permanently skip accepted market evidence.

### Infer lifecycle from stop-reason text

Rejected because free-form text is not a stable control contract and could misclassify a true
source or admission failure as a clean pause.

### Run collection automatically after claim

Rejected because scheduling, daemon lifecycle, continuous recovery, and deployment are separate
operational capabilities with different failure and authorization boundaries.

### Treat checkpoint counters as crash-durable hard request limits

Rejected because a network request and the separate control database cannot commit atomically.
The shared durable rate budget protects provider capacity; stronger per-job reservation requires a
separate design.

## Review Triggers

Review when adding the typed transition-history reader, crash-durable per-job attempt reservation,
automatic recovery, continuous collection, live streaming, multi-host coordination, private data,
or any order-capable workflow.
