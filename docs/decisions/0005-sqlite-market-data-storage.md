# ADR 0005: SQLite Market-Data Storage

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Audit Department, and Engineering
  Department

## Context

Phase 2 requires raw and canonical market data to survive process restarts while preserving exact
provider evidence, lineage, idempotency, and visible conflicts. The existing in-memory store proves
the append contract but cannot support restart recovery or durable inspection.

This slice needs one dependable local implementation without prematurely selecting the final
operational database, analytical warehouse, object store, cloud provider, or distributed
architecture. It must not add credentials, account access, order capability, or a continuously
running collector.

## Decision

Add a replaceable SQLite adapter for the existing market-data storage port:

- Use Python's standard-library `sqlite3` module, adding no runtime dependency.
- Require a file-backed database; the durable adapter rejects `:memory:`.
- Store exact successful provider-response bytes in a raw-evidence table with SHA-256 validation,
  timestamps, source identity, and provenance.
- Store canonical candles separately as strict serialized domain records with a unique natural key.
- Require every canonical candle in a fetched batch to reference its raw payload ID.
- Preserve a many-to-one link from equivalent raw captures to the accepted canonical record.
- Commit one raw response and all of its canonical write outcomes in one SQLite transaction.
- Treat repeated equivalent raw and canonical records as idempotent duplicates.
- Never replace a canonical record when the same natural key arrives with different values.
- Preserve the new raw evidence and quarantine each conflicting canonical revision in a separate
  table for inspection.
- Revalidate raw hashes and canonical schemas when records are read from storage.
- Enable foreign-key enforcement, write-ahead logging, a finite busy timeout, and full synchronous
  durability.
- Version the database schema through SQLite's `user_version` and fail closed on unknown versions.

The existing in-memory adapter remains available for unit tests and contract-level use. Both
adapters implement the same application-facing port.

## Safety Boundary

This decision stores public market data only. It does not authorize:

- Private exchange data, credentials, balances, positions, or orders.
- Continuous collection, live WebSocket ingestion, scheduling, or automatic retries.
- Automated conflict resolution or historical rewriting.
- Use of a conflicted revision as canonical data.
- Production readiness, high availability, or shared multi-node operation.

An incomplete batch still fails the sequence-quality gate before any raw or canonical data is
written by the historical ingestor.

## Consequences

### Positive

- Raw evidence and canonical records survive restart and can be independently verified.
- Exact provider bytes remain available for incident analysis and future normalization replay.
- Duplicate fetches do not multiply canonical records.
- Conflicting provider revisions remain visible without overwriting accepted history.
- SQLite transactions provide a strong local durability baseline without a new service or package.
- The adapter can be replaced later without changing provider or application contracts.

### Negative

- SQLite is a single-file operational adapter, not the final high-volume analytical platform.
- Conflict quarantine is preserved but not automatically adjudicated.
- Backup, retention, compaction, encryption, and disaster-recovery procedures remain future work.
- Raw evidence is captured only after a successful HTTP response has passed provider parsing; a
  separate failure-evidence policy is still required for malformed responses.

## Alternatives Considered

### PostgreSQL

Deferred. It is a credible future operational store, but this bounded slice does not yet justify a
database service, deployment configuration, credentials, migrations framework, or network failure
surface.

### Parquet and object storage

Deferred. They are strong candidates for analytical history and replay export but do not alone
provide the small transactional idempotency and conflict-quarantine boundary needed here.

### JSON or CSV files

Rejected. Ad hoc files do not provide the same transactional uniqueness, restart-safe conflict
handling, schema version check, or indexed stream reads.

### Continue using only memory

Rejected. Restart recovery and durable lineage are explicit Phase 2 requirements.

## Review Triggers

Revisit this decision when measured volume, concurrent writers, shared services, retention,
backups, analytical workloads, encryption, replication, or continuous collection exceed the
documented SQLite boundary.
