# Execution Policy

## Current Prohibition

WEALTH has no execution engine, order port, private exchange adapter, account access, credential,
portfolio position, or open order. Order creation, submission, amendment, cancellation, and
financial execution are prohibited in the current Phase 2 system.

Changing configuration cannot grant execution authority. `research` is the current operating mode;
paper is a future target only after a real simulator exists and Phase 5 gates are satisfied.

## Currently Permitted Actions

- Bounded reads from approved public market-data endpoints without credentials.
- Deterministic validation, reconciliation, replay, and local persistence of non-account market
  evidence.
- Synthetic local health events and read-only operational health queries.
- Reviewable development changes and tests on an isolated task branch.

These actions cannot produce a signal with financial authority or an exchange order.

## Future Execution Invariants

If an execution capability is later approved, the only valid chain is:

`versioned intent → current portfolio state → unexpired deterministic Risk approval → permitted execution request → venue acknowledgement/fill → reconciliation → immutable audit`

An opinion, signal, model output, committee decision, or human message alone is not an order.
Execution must:

- Reject missing, expired, mismatched, enlarged, duplicated, stale, or unreconciled authority.
- Use idempotent client identifiers and bounded retries; uncertainty halts further action rather
  than guessing.
- Never change side, instrument, quantity, limit, leverage, venue, or expiry beyond approval.
- Revalidate market, portfolio, risk, permission, and kill-switch state immediately before action.
- Record correlation, lineage, policy/configuration versions, request, acknowledgement, fill,
  rejection, cancellation, fee, and reconciliation timestamps in UTC.
- Prevent new risk while halted and require explicit human approval to resume.
- Use restricted credentials without withdrawal permission and keep research, paper, and live
  records distinguishable.

## Promotion Gate

Introducing or changing an order-capable component requires a separate accepted ADR, the applicable
roadmap gates, deterministic Risk and Audit boundaries, reconciliation and fault-injection
evidence, rollback, and the explicit approvals in `docs/POLICIES.md`. Code merge alone never
authorizes execution or deployment. Any future real-money action also requires a current live
authorization record whose market, asset, venue, account, strategy, capital, risk, order-type,
time-window, monitoring, and kill-switch envelope matches the proposed action.

## Review Triggers

Review before adding an order intent, simulator, private adapter, account state, credential,
execution engine, human approval interface, kill switch, or higher-risk operating mode.
