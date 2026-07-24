# ADR 0018: Durable SQLite Order-Flow Evidence Storage

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Context

The accepted order-flow quality and in-memory idempotency boundary proves how canonical trades,
tickers, and best-bid-ask records are identified and rejected, but memory does not survive restart.
Future provider adapters require exact raw response bytes, raw-to-canonical lineage, transactional
writes, restart-safe idempotency, and durable conflict evidence before their data can be trusted.

This phase still targets one local process and bounded batches. It does not require distributed or
high-volume analytical storage yet.

## Decision

Add a dedicated versioned SQLite implementation of the order-flow storage port.

### Raw-to-canonical batch

`OrderFlowFetchBatch` binds one exact `RawMarketPayload` to one exact canonical record family. It
requires:

- One source, venue, instrument, instrument type, and record type.
- Matching batch, raw observation, and processing timestamps.
- Every canonical record to use the batch timestamps.
- Every canonical record to reference the raw payload identity in lineage.
- At least one and at most 100,000 canonical records.

The batch contract does not claim that quality passed. A future ingestion application must audit
the bounded records before calling storage.

### Dedicated schema identity

`SQLiteOrderFlowStore` requires a file-backed database and owns a dedicated schema. Both
`PRAGMA user_version` and an explicit `wealth.order_flow` metadata marker are validated. A database
with the same integer version but another purpose is rejected without migration.

### Transactional evidence

One batch transaction:

1. Inserts or validates the exact raw provider payload.
2. Stops without canonical writes when a raw identity conflicts.
3. Inserts canonical records under record-family-namespaced natural keys.
4. Links every accepted or equivalent canonical record to the raw capture.
5. Quarantines changed canonical values for an existing natural key.

The first canonical record is never replaced. Equivalent repeats return `DUPLICATE`; changed values
return `CONFLICT`. A reused canonical UUID for another natural key fails explicitly and rolls back
the whole batch.

Natural keys are stored as deterministic canonical JSON. Canonical record JSON remains the source
of complete typed content; indexed source, venue, instrument, instrument type, record type, and
event time support exact-stream reads.

### Read-time verification

Every canonical record, raw payload, and conflict is revalidated against its domain contract when
read. Canonical record type, natural key, and stream indexes must agree with the canonical content.
Raw bytes must still match their stored SHA-256 digest. Corrupt evidence fails explicitly rather
than being returned partially.

SQLite uses foreign keys, a five-second busy timeout, write-ahead logging, and full synchronous
durability. Exact-stream query results are sorted through the shared deterministic market-time
ordering.

## Safety Boundary

This decision does not authorize:

- Collecting public order flow from an exchange.
- Admitting a provider batch without the quality gate.
- Live WebSocket operation, reconnect, resubscription, or gap recovery.
- Automatic retention, compaction, backup, replication, or migration.
- Treating stored data as a signal, recommendation, risk instruction, or order.
- Private exchange access, credentials, portfolio state, or trading.

## Consequences

### Positive

- Raw and canonical order-flow evidence survives restart.
- Exact repeated captures are idempotent across processes and restarts.
- Multiple raw captures can support one accepted canonical identity.
- Conflicting revisions remain inspectable without rewriting history.
- Wrong database types, unknown versions, reused identities, and tampered content fail explicitly.
- All three canonical record families share one provider-independent durable port.

### Negative

- SQLite remains a single-host local storage baseline.
- Canonical JSON is revalidated on reads, adding CPU cost.
- Natural-key JSON and full record JSON duplicate some indexed identity values.
- Rejected malformed provider payloads still need a separate governed failure-evidence policy.
- No retention or capacity policy exists for event-level data volume.

## Alternatives Considered

### Extend the candle tables

Rejected. Event-level records have different natural keys, volumes, retention needs, and future
stream-recovery behavior. Sharing one database path would also couple independent schema
migrations.

### Store canonical records without raw bytes

Rejected. Provider payloads are required to reproduce normalization and investigate conflicts.

### Overwrite on duplicate provider identity

Rejected. Changed historical values are conflict evidence and cannot silently replace the first
accepted record.

### Select a distributed event store now

Deferred. Provider throughput, retention, query, replay, and multi-host requirements need measured
evidence before adding operational complexity.

## Review Triggers

Revisit this decision when measured volume exceeds the SQLite capacity envelope, adding the first
provider adapter, defining retention and backup, implementing point-in-time replay, or introducing
multi-process or multi-host order-flow ingestion.
