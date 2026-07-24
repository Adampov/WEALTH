# ADR 0019: Fail-Closed Order-Flow Ingestion Gate

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, and Audit Department

## Context

Order-flow contracts, quality auditing, and durable storage now exist as separate replaceable
boundaries. A future adapter could still call storage directly and accidentally persist a mixed,
duplicated, conflicting, out-of-window, out-of-order, or sequence-incomplete batch.

The application needs one ordinary admission path that makes the correct order mandatory:
canonical batch, deterministic quality decision, then storage.

## Decision

Add `OrderFlowBatchIngestor` as the fail-closed application boundary for an already-observed
`OrderFlowFetchBatch`.

The caller supplies one explicit timezone-aware half-open event-time window. The ingestor:

1. Audits every batch record against the batch's exact stream and declared provider sequence
   policy.
2. Performs no storage call when the quality report fails.
3. Calls the storage batch transaction only when quality passes.
4. Returns the original batch, complete quality report, optional raw write, and every canonical
   write outcome.

`OrderFlowIngestionResult.accepted` is true only when:

- Quality passed.
- A raw write outcome exists and is not a conflict.
- Storage returned exactly one write outcome for every batch record.
- No canonical write is a conflict.

Equivalent raw and canonical repeats remain accepted idempotent outcomes. A storage conflict after
quality passes keeps its evidence and quarantine record but makes the overall ingestion
unaccepted. A raw identity conflict blocks canonical writes and also makes ingestion unaccepted.

This ingestor consumes a canonical batch. Provider fetching, transport policy, payload parsing, and
provider-specific sequence guarantees remain adapter responsibilities.

## Safety Boundary

This decision does not authorize:

- A public REST or WebSocket order-flow adapter.
- Starting any automatic collection process.
- Bypassing the quality gate for provider data.
- Repairing, sorting, deduplicating, or filling rejected provider records silently.
- Treating accepted data as a feature, signal, recommendation, or order.
- Private exchange access, credentials, portfolio state, or execution.

## Consequences

### Positive

- Future adapters receive one tested path from raw evidence to durable admission.
- Quality failures leave both raw and canonical stores unchanged.
- Duplicate, gap, raw-conflict, canonical-conflict, restart, and idempotency behavior is explicit.
- Operational code can distinguish data-quality rejection from durable-storage conflict.

### Negative

- Direct access to the storage port remains possible for maintenance and tests; repository guidance
  must require provider flows to use the ingestor.
- The caller must choose a correct bounded event-time window.
- A canonical storage conflict can coexist with non-conflicting idempotent outcomes in the same
  durable transaction, while the overall result remains unaccepted.
- No rejected raw response is retained when quality fails; governed failure-evidence storage
  remains future work.

## Alternatives Considered

### Let every adapter orchestrate audit and storage

Rejected. Repeating admission logic would invite provider-specific differences and accidental
quality bypasses.

### Persist raw evidence before quality auditing

Deferred for a future rejected-evidence policy. Mixing rejected evidence with admitted evidence
without explicit status and retention contracts would be ambiguous.

### Repair a failed batch automatically

Rejected. Sorting, deduplication, conflict selection, or sequence filling would conceal provider
behavior and break reproducibility.

## Review Triggers

Revisit this decision when adding the first provider adapter, rejected-evidence retention,
live-stream micro-batching, provider gap recovery, or point-in-time order-flow replay.
