# Organization

## Canonical Definition

The authoritative department mandates, inputs, outputs, authority boundaries, metrics, and
relationships are defined in [`AI_DEPARTMENTS.md`](AI_DEPARTMENTS.md). This file is the stable
organization entry point for operators and tooling; it does not duplicate those mandates.

## Current Operating Organization

Phase 2 activates only the responsibilities needed to build and assure the public market-data
platform:

| Function | Current responsibility | Authority |
| --- | --- | --- |
| Market Data | Acquire, validate, canonicalize, reconcile, and retain public evidence | Read approved public sources only |
| Audit and Assurance | Preserve lineage, quality results, failures, and operator-visible health | May reject incomplete evidence; cannot alter it |
| Risk and Security | Keep unavailable permissions disabled and stop unsafe transitions | May reject or halt; cannot enable execution |
| Engineering | Implement bounded, tested changes through reviewed branches | Cannot authorize investment or execution behavior |
| Executive governance | Approve material scope, architecture, permissions, and promotion | Cannot override a final deterministic Risk rejection |

All analytical, strategy, portfolio, paper-execution, and real-execution departments remain
architecturally defined but inactive. No current component may issue a signal, authorize exposure,
access an account, or place an order.

## Separation of Authority

Information is not a recommendation, a recommendation is not an approval, and an approval is not
an order. When later phases activate those boundaries, each transition must use a typed,
versioned, auditable contract and the approvals defined in [`POLICIES.md`](POLICIES.md).
