# ADR 0026: Typed Public-Trade Transition History

- **Status:** Accepted
- **Date:** 2026-07-25
- **Decision owners:** Project owner, Market Data Department, Engineering Department, Security
  Department, and Audit Department

## Task Contract

### Goal

Expose the existing append-only public-trade checkpoint ledger through one immutable, bounded,
causally ordered read contract without changing control state or its SQLite schema.

### Context

ADR 0023 retains every public-trade checkpoint transition and the fencing token that authorized
it. ADR 0025 composes those checkpoints into an evidence-first bounded collection flow. Before
this decision, callers could inspect current checkpoint and source-health state, but no typed port
could replay the actor transition ledger or fail closed when its canonical record, indexed
projection, authority, or causal sequence was damaged.

Timestamps cannot safely establish lifecycle order because equal, skewed, or offset-preserving
values are possible at uncovered boundaries. Checkpoint version is already the durable causal
order and is therefore the only pagination cursor for this history.

### Scope

- Add an immutable `PublicTradeCollectionTransition` containing the full validated checkpoint and
  the actor lease token retained for that transition.
- Add a read-only `PublicTradeCollectionTransitionReader` port with
  `transitions_for_job`.
- Implement ascending SQLite pages after an exclusive checkpoint-version cursor, using 100 records
  by default and rejecting limits above 1,000.
- Validate canonical checkpoint JSON, exact SQLite storage types, every indexed transition
  projection, immutable job identity, UTC content, pristine creation, contiguous versions,
  lifecycle causality, actor authority against the durable lease-acquisition ledger, page
  boundaries, the history tail, and agreement with the current checkpoint.
- Cover creation, claim, renewal, pause, resume, failure, expired-lease takeover, completion,
  pagination, restart, missing jobs, empty tails, invalid inputs, and targeted corruption.

### Constraints

- Reuse the existing `public_trade_collection_transitions` table and database schema exactly; no
  migration is introduced.
- Reads must remain finite, side-effect free, and ordered by checkpoint version rather than
  timestamps or record identifiers.
- Do not duplicate the separate source-health observation or summary contract.
- Do not repair or mutate control state, start collection, call a provider, schedule work, access
  credentials, produce a signal, make a portfolio or Risk decision, submit an order, or perform
  any financial action.

### Done When

- Initial reads return an ascending contiguous page beginning at version 1; subsequent pages begin
  strictly after a previously returned checkpoint version.
- A missing job without a cursor and a cursor at the validated tail return an empty tuple; a
  cursor that does not identify stored history is rejected explicitly.
- The default and hard page limits are 100 and 1,000, and boolean, non-integer, zero, negative, or
  oversized limits and cursors are rejected.
- Every returned transition exposes the exact checkpoint record and applicable actor authority.
  Creation, unleased claims or expired-lease takeovers have no prior actor token; transitions
  authorized by a running lease retain that prior fencing token.
- Malformed or noncanonical JSON, malformed SQLite storage types, projection disagreement,
  invalid or reused actor UUIDs, lease-acquisition disagreement, orphan history, missing creation
  or tail records, version gaps, immutable-identity drift, non-UTC content, and impossible
  lifecycle steps fail through the existing bounded `CORRUPT_RECORD` storage boundary.
- Reading and replaying after reopening the database leave all control tables and the schema
  version unchanged.
- Relevant unit, integration, restart, corruption, formatting, lint, type, lockfile, health-slice,
  dependency-audit, and CI gates pass before acceptance.

### Not Included

- Any schema or stored-data migration, repair command, retention or compaction policy, operator
  CLI, dashboard, or external audit export.
- Global UTC normalization or migration work tracked by `RISK-005`.
- Automatic recovery, scheduling, continuous public-trade polling, live WebSockets, or multi-host
  coordination.
- Crash-durable per-job request reservations beyond the existing shared durable provider budget.
- Credentials, private or account data, strategies, signals, portfolio state, Risk approvals,
  orders, execution, or any financial action.

## Decision

Define `PublicTradeCollectionTransition` as a strict frozen envelope around the existing canonical
checkpoint plus the optional actor fencing token already stored beside it. This avoids creating a
second mutable lifecycle projection or copying source-health evidence into the audit record.

Define `PublicTradeCollectionTransitionReader.transitions_for_job(job_id, *,
after_checkpoint_version=None, limit=100)` as the read boundary. The cursor is exclusive and must
be the positive version of a transition already retained for that job. Pages are ordered by
version, never time, and the hard limit is 1,000.

For each call, the SQLite adapter reads a consistent transaction snapshot and validates the
current checkpoint, cursor boundary, requested page, and enough adjacent history to establish
continuity. It reconstructs each transition from canonical checkpoint JSON and the retained actor
token, checks exact SQLite storage types and every indexed projection, binds each authority token
to its durable lease-acquisition row, replays the same domain lifecycle rules used at write time,
and derives the expected actor from the predecessor's lease authority. It also checks the page
lookahead or terminal tail and requires the latest transition to equal the current checkpoint.

The reader distinguishes absence from corruption. A genuinely missing job has empty history when
no cursor is supplied. An orphan ledger, a missing stored cursor, a gap, a missing tail, or a
current checkpoint without creation history is corrupt and fails closed. The reader never fills a
gap, infers a predecessor, or repairs storage.

## Safety Boundary

This is a local inspection capability over existing public-market-data control state. It has no
write, network, credential, account, signal, portfolio, approval, order, or execution capability.
Runtime controls for live trading, leverage, withdrawals, external notifications, and autonomous
execution remain disabled.

## Consequences

### Positive

- Callers and future audit tooling can reconstruct the exact checkpoint lifecycle and fencing
  authority through a typed, bounded port.
- Pagination remains stable and causal even when timestamps are equal or textual offsets would
  sort differently.
- Record, projection, actor, continuity, and current-state corruption inside each validated read
  boundary becomes explicit instead of producing trusted rows from that boundary.
- Existing databases need no schema change.

### Negative

- Deep histories require repeated bounded calls and are still stored only in local SQLite.
- Each page performs validation beyond the rows returned so it can establish its cursor and tail
  boundaries.
- The read contract detects corruption but deliberately offers no repair path.
- `RISK-004` remains open because typed inspection does not make market-evidence and control
  database commits atomic.

## Alternatives Considered

### Return raw transition rows

Rejected because callers would need to reinterpret checkpoint JSON, projections, authority, and
causality independently and could trust corrupt history.

### Order or paginate by transition time

Rejected because timestamps do not provide the durable causal sequence and uncovered
offset-preserving boundaries remain tracked by `RISK-005`.

### Add a new normalized transition table

Rejected because the existing append-only ledger contains the required record and actor evidence;
a migration would add risk without improving this bounded read contract.

### Return health observations with transition records

Rejected because health has a separate bounded checkpoint-version history and aggregation
contract. Combining them would duplicate evidence and blur absence semantics.

## Review Triggers

Review when changing checkpoint lifecycle or actor-authority rules, altering the transition
schema, adding retention or repair, exposing this history outside the local process, introducing
continuous collection, or beginning any UTC migration identified by `RISK-005`.
