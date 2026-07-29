# ADR 0032: Continuous Public-Trade Stream SQLite Schema and Evidence Harness

- **Status:** Accepted
- **Date:** 2026-07-29
- **Decision owners:** Project owner, Engineering Department, Security Department, Risk
  Department, Data Department, and Audit and Assurance Department

## Task Contract

### Goal

Freeze one exact executable version-one SQLite schema and an isolated generated-data harness that
tests the physical design selected by ADR 0031 without creating a production adapter or operational
storage path.

### Scope

This decision governs only:

- the text fixtures under
  `tests/fixtures/continuous_public_trade_stream_store/v1/`;
- the test-support module
  `tests/support/continuous_public_trade_stream_sqlite_harness.py`;
- generated databases created by that module only for the exact TASK-064 unit and integration test
  modules beneath their fixture-scoped pytest temporary roots and cooperating same-UID child
  processes;
- generated schema, transaction, corruption, crash, query-bound, backup/restore,
  same-format-generation-copy, and finite-workload evidence; and
- a test-only facade that revalidates and returns the exact frozen TASK-062 command, query,
  receipt, page, view, outcome, and retry-disposition models.

The controlling TASK-064 contract is generation 3 with normalized SHA-256
`86e3650608f2f1c96a9aa272b2b9cd597bc3d5ac188a39937afb974536d11ccb`.

### Out of Scope

This decision adds no `src/` adapter, runtime import, composition, operator path or data, provider
or network access, evidence-body or accepted-attestation access, time authority, fence, lease,
request budget, retry loop, repair, routing, cutover, deployment, retention deletion, capacity
claim, durability claim, RPO/RTO claim, readiness claim, or trading capability. It changes no
TASK-059 behavior, TASK-061 bytes or digest domain, TASK-062 port semantics, ADR 0031 requirement,
control flag, operating mode, or TASK-037 denial.

## Context

ADR 0031 selects a dedicated local SQLite generation behind the unused TASK-062 logical port. It
requires authoritative TASK-061 BLOBs, exact signed-64-bit projections, reversible identity keys,
one-winner writer transactions, bounded readers, immutable history, deferred tail ownership,
closed failures, process-fault evidence, Online Backup, separate-generation copy evidence, and
finite thresholds before a production adapter can be considered.

TASK-064 is deliberately narrower than an adapter. It makes the schema and test protocols
executable only inside the two named TASK-064 pytest modules. A fixture-scoped process-local
registration binds the exact pytest node, original `Path` object, process ID, resolved root,
device, inode, UID, mode, and random nonce; registration is revoked when the fixture exits. A
plain or reconstructed `Path` grants no authority. The harness owns every database it opens and
accepts neither a database path nor SQL nor URI options at an operation boundary.

The SQLite documentation identifies the application ID as an application-format marker and points
to the source-tree `magic.txt` list. A contemporaneous 2026-07-29 review found no listed collision
for `0x57505431`; this local selection is not a claim of external registration. The exact accepted
SQLite source is 3.53.1. SQLite's release history confirms that this source is later than the
3.53.0 WAL-reset fix required by ADR 0031. SQLite also records FTS5 defects fixed in 3.53.2. FTS5 is
compiled into the accepted Python build, so this decision treats it as unreachable rather than
safe: there is no virtual table in the schema, no caller SQL, extension loading is disabled,
`SQLITE_DBCONFIG_DEFENSIVE` is on, and virtual-table/schema actions are denied. This narrow
test-harness acceptance is not production runtime approval.

Primary references:

- [SQLite database file format and application ID](https://www.sqlite.org/fileformat.html)
- [SQLite 3.53.1 source ID and 3.53.0 WAL-reset fix](https://www.sqlite.org/changes.html)
- [SQLite WAL mode and the WAL-reset bug](https://www.sqlite.org/wal.html)
- [SQLite POSIX lock and file-descriptor cautions](https://www.sqlite.org/howtocorrupt.html)
- [SQLite WAL-index shared-memory format](https://www.sqlite.org/walformat.html)
- [SQLite documented vulnerability preconditions and fixes](https://www.sqlite.org/cves.html)
- [SQLite Online Backup API](https://www.sqlite.org/backup.html)
- [SQLite defensive connection configuration](https://www.sqlite.org/c3ref/c_dbconfig_defensive.html)

## Decision

### Normative fixture identity

The exact executable DDL is the UTF-8/LF text file
`tests/fixtures/continuous_public_trade_stream_store/v1/schema.sql`. Its raw SHA-256 is
`1263b831a3e73bfc730beef6df7df48fce0e3654aa806672650de9f40b8a3e37`. No generated database,
WAL, shared-memory file, or record fixture is committed.

The ordered live-catalog descriptor is
`tests/fixtures/continuous_public_trade_stream_store/v1/schema_descriptor.json`; its raw file
SHA-256 is `bb33dc9cb549be484c5dc7855abace6d851682e50883e51919a9169cdaae431a`.
The comparison-only golden fingerprint is:

```text
sha256:0410c1f08390a411c73427b3d07c542f3d1828def7c6adebab51cd57375355b3
```

The descriptor fingerprint is:

```text
SHA-256(
  b"wealth.continuous_public_trade.stream_store_schema/v1\x00"
  + canonical_descriptor_json
)
```

Canonical descriptor JSON uses `json.dumps` with `allow_nan=False`, `ensure_ascii=True`, and
separators `(",", ":")`, encoded as UTF-8 with no trailing newline. The committed JSON file adds
exactly one LF. Catalog SQL normalization rejects NUL and SQL comments, preserves quoted and
bracketed text byte-for-byte, collapses only unquoted ASCII whitespace to one space, trims outer
whitespace, and removes one terminal semicolon. Tables also include ordered `table_xinfo` and
`foreign_key_list` projections; indexes include ordered `index_xinfo` projections. Tests derive a
fresh descriptor and compare it to the committed value; normal tests never rewrite or bless the
golden files.

### File and schema markers

The version-one format freezes:

| Marker | Exact value |
|---|---|
| `application_id` | `0x57505431` (`WPT1`) |
| `user_version` | `1` |
| metadata storage marker | `wealth.continuous_public_trade.stream_store/sqlite/v1` as BLOB |
| schema generation | exact built-in integer `1` |
| natural-key version | exact built-in integer `1` |
| encoding | UTF-8 |
| page size | 4,096 bytes |
| journal mode | WAL |
| auto-vacuum | `NONE` |

Metadata must be the single exact row with key `1`; the schema, marker, versions, page size, and
golden fingerprint must agree. An unknown recognized generation is `UNSUPPORTED_VERSION`. A file
claiming generation one with any marker, catalog, type, constraint, index, trigger, or fingerprint
disagreement is `CORRUPT`.

### Ordered object inventory

The descriptor contains exactly four tables, five explicit indexes, and ten triggers in this order:

1. `stream_store_metadata`
2. `continuous_public_trade_stream`
3. `continuous_public_trade_history`
4. `stream_tail_commit_guard`
5. `ux_cpt_stream_uuid`
6. `ux_cpt_stream_natural_key`
7. `ux_cpt_history_stream_version`
8. `ux_cpt_history_record_binding`
9. `ux_cpt_history_tail_binding`
10. `trg_metadata_no_update`
11. `trg_metadata_no_delete`
12. `trg_stream_insert_shape`
13. `trg_stream_current_update`
14. `trg_stream_transition_finalize`
15. `trg_stream_no_delete`
16. `trg_history_insert_binding`
17. `trg_history_transition_pending`
18. `trg_history_no_update`
19. `trg_history_no_delete`

All four tables are `STRICT`. `stream_store_metadata` and `stream_tail_commit_guard` are
`WITHOUT ROWID`; `continuous_public_trade_stream` and `continuous_public_trade_history` are
ordinary rowid tables, as selected by ADR 0031 for their large authoritative BLOB rows.
Schema-local keys and the logical projections explicitly allowed by the TASK-064 contract use
`INTEGER`; all authoritative values and markers use `BLOB`. No logical `TEXT` or `REAL` column
exists. Digest/root BLOB checks require exact 71-byte lowercase `sha256:` encodings, including an
explicit 64-byte text-length guard before the GLOB check so an embedded NUL cannot truncate
validation.

The stream table uniquely binds the 16-byte UUID and reversible natural key, exact creation
witness, policy projections, stream-start epoch, current record/envelope witnesses, current
version, and current history root. The history table uniquely binds `(stream, successor_version)`,
the record digest, and the tail witness. Creation has SQL `NULL` predecessor fields; a transition
has exact prior version, envelope digest, history root, predecessor record bytes, and predecessor
record digest. History and metadata are immutable, and stream deletion is denied.

### Natural identity

The exact natural-key byte domain is
`b"wealth.continuous_public_trade.natural_identity_key/v1\x00"`, followed in fixed order by
`source`, `venue`, `instrument`, `provider_symbol`, `instrument_type`, and `request_variant`.
Each atom is encoded using Python UTF-8 with `surrogatepass` as required by ADR 0031, preceded by
one unsigned four-byte big-endian byte length. Empty atoms, extra bytes, incomplete lengths, or a
non-exact decode/re-encode round trip are corrupt. The key is reversible and never a digest or a
text-collation authority.

### Bootstrap and path ownership

Only `bootstrap_store(pytest_root)` may create a database. It requires the active exact
fixture-scoped registration described above. It opens and identity-checks the root directory,
creates a random-suffixed private `0700` generation through descriptor-relative operations, opens
and pins that generation descriptor, and creates `store.sqlite3` exactly once through
descriptor-relative `O_CREAT|O_EXCL|O_RDWR|O_NOFOLLOW` with exact `0600` mode. It records the
root, generation, main-file identities, modes and ownership plus a random opaque store token.
That token retains the exact originating registration object and its anonymous one-byte
process-shared revocation flag. Every operation requires that same registration and an unrevoked
flag for the exact pytest node and parent or immediate fork-child process. Fixture exit flips the
shared flag before unregistering the parent, so an inherited child cannot open afterward; harness
protocols start only with no registered live parent connection, track each exact child immediately
after fork, and boundedly reap every child before return. Cleanup probes child ownership with
`waitpid(..., WNOHANG)` before signaling, so it never signals a stale numeric PID. Descriptor-safe
owned-file cleanup remains available after revocation. Every transaction repeats the token and
pinned root/generation/file identity check at its last pre-commit boundary; revocation raises the
closed token failure and the mutation rolls back rather than committing after fixture exit.

Every later open pins the registered root and generation descriptors; rejects unexpected entries
and every observed main/WAL/SHM alias, non-regular file, ownership/mode/link mismatch, or identity
change before and after SQLite open; and retains the generation descriptor for the connection
lifetime. While a SQLite connection is live, repeat identity and size checks use only retained-fd
`fstat` plus descriptor-relative non-following `stat`; they never open or close another
main/WAL/SHM descriptor and therefore do not release SQLite's process-scoped POSIX locks.
`PRAGMA database_list` must contain only `main` at the same validated database. The only operation
URI is internally constructed through the pinned generation descriptor for the already validated
file and has the sole fixed query option `mode=rw`. A caller supplies no path, URI, query option, or
SQL. A missing file cannot be created by an operation open. Bootstrap failure removes only its
exact new generated main/WAL/SHM files and generation directory.

This is a controlled pytest/cooperating-process threat model. Python's standard SQLite binding
cannot descriptor-pin SQLite's own main/WAL/SHM opens or eliminate a hostile same-UID process
racing pathname replacement. That hostile race and target/VFS isolation are target/deployment
`NOT_APPLICABLE` evidence here, never `PASS`, and independently block production use until
separately governed target/VFS isolation evidence exists.

### Exact accepted runtime

The accepted runtime is Python `3.13.14`, SQLite `3.53.1`, and the exact SQLite source ID:

```text
2026-05-05 10:34:17 c88b22011a54b4f6fbd149e9f8e4de77658ce58143a1af0e3785e4e6475127e9
```

Python must report serialized SQLite threadsafety `3`. The sorted compile-option tuple is exactly:

```text
ATOMIC_INTRINSICS=1
COMPILER=clang-22.1.3
DEFAULT_AUTOVACUUM
DEFAULT_CACHE_SIZE=-2000
DEFAULT_FILE_FORMAT=4
DEFAULT_JOURNAL_SIZE_LIMIT=-1
DEFAULT_MMAP_SIZE=0
DEFAULT_PAGE_SIZE=4096
DEFAULT_PCACHE_INITSZ=20
DEFAULT_RECURSIVE_TRIGGERS
DEFAULT_SECTOR_SIZE=4096
DEFAULT_SYNCHRONOUS=2
DEFAULT_WAL_AUTOCHECKPOINT=1000
DEFAULT_WAL_SYNCHRONOUS=2
DEFAULT_WORKER_THREADS=0
DIRECT_OVERFLOW_READ
ENABLE_DBSTAT_VTAB
ENABLE_FTS3
ENABLE_FTS3_PARENTHESIS
ENABLE_FTS4
ENABLE_FTS5
ENABLE_GEOPOLY
ENABLE_MATH_FUNCTIONS
ENABLE_PERCENTILE
ENABLE_RTREE
MALLOC_SOFT_LIMIT=1024
MAX_ATTACHED=10
MAX_COLUMN=2000
MAX_COMPOUND_SELECT=500
MAX_DEFAULT_PAGE_SIZE=8192
MAX_EXPR_DEPTH=1000
MAX_FUNCTION_ARG=1000
MAX_LENGTH=1000000000
MAX_LIKE_PATTERN_LENGTH=50000
MAX_MMAP_SIZE=0x7fff0000
MAX_PAGE_COUNT=0xfffffffe
MAX_PAGE_SIZE=65536
MAX_SQL_LENGTH=1000000000
MAX_TRIGGER_DEPTH=1000
MAX_VARIABLE_NUMBER=32766
MAX_VDBE_OP=250000000
MAX_WORKER_THREADS=8
MUTEX_PTHREADS
SYSTEM_MALLOC
TEMP_STORE=1
THREADSAFE=1
```

Any source, tuple, or thread-mode difference is `UNAVAILABLE`. `THREADSAFE=0`,
`OMIT_FOREIGN_KEY`, `OMIT_TRIGGER`, and `OMIT_AUTHORIZATION` are independently rejected.

Each connection establishes and reads back one exact ordered role profile. Both reader and writer
profiles bind the complete DBCONFIG tuple, defensive availability/enabled state, connection limits,
and common PRAGMAs: busy timeout `0`; foreign keys on; trusted schema,
read-uncommitted, recursive triggers, and ignored checks off; cell-size checking on; mmap `0`;
temporary storage in memory; cache size `-8192`; normal locking; maximum page count `16,384`; WAL
auto-checkpoint `4` pages. The reader profile additionally requires `query_only=ON` and one
explicit finite transaction; the writer profile requires `query_only=OFF` and
`synchronous=FULL`.

Required `sqlite3_db_config` controls disable load extension, FTS3 tokenizers, double-quoted string
literals, trusted/writable schema, legacy alter/file formats, reset database, and views; and enable
defensive mode, foreign keys, triggers, query-planner stability, and no checkpoint on close.
Required connection limits are: attached databases `0`, value length `1,048,576`, SQL length
`262,144`, columns `128`, expression depth `128`, compound-select terms `1`, function arguments
`32`, LIKE pattern length `128`, trigger depth `8`, variables `64`, VDBE operations `100,000`, and
worker threads `0`.

The operation authorizer denies schema DDL, ATTACH/DETACH, virtual tables, views, REINDEX, mutation
of critical PRAGMAs, and `load_extension`. It allows the guard-table insert and delete only when
SQLite reports the exact owning schema trigger.

### Atomic create and compare-and-swap

Create and compare-and-swap each:

1. revalidate the exact TASK-062 boundary model before opening SQLite;
2. use one `BEGIN IMMEDIATE` transaction with busy timeout zero;
3. validate every located retained candidate before classification;
4. distinguish exact historical duplicate from coherent conflict;
5. perform no upsert, replacement, implicit transaction, repair, retry, or alternate successor;
6. commit one current/history state or none; and
7. resolve unknown acknowledgement only through exact reload.

Creation inserts the stream and version-one history entry together. A transition first inserts one
immutable history row and then advances the stream row with an exact current-state predicate.
`trg_history_transition_pending` inserts a guard row whose deferred foreign key deliberately cannot
satisfy at commit. The exact stream-current update trigger proves the new history tail and
`trg_stream_transition_finalize` deletes that guard. A history-only transition, a current-only
transition, or the wrong pair therefore cannot commit through the normal schema.

The test-only TASK-062 facade returns only the existing typed outcomes and retry dispositions. It
does not become a production adapter and grants no retry authority.

### Bounded current and audit reads

Current load materializes at most two identity candidates and, for a selected stream, exactly the
creation, current, and direct-predecessor versions needed by the frozen view: at most three distinct
history rows independent of history length.

Historical duplicate classification materializes at most five history rows: at most three for the
bounded current witness and at most two for the requested predecessor/successor pair. A
two-candidate identity conflict materializes exactly two stream rows and at most three current
history rows per candidate, for an explicit ceiling of six history rows. A not-found identity
materializes zero history rows. Create conflict and compare-and-swap expectation-conflict paths use
the same two-stream/six-history ceiling; all remaining classified failure paths use the stated
current or audit bounds and no unbounded query.

Audit uses the unique `(stream_row_id, successor_version)` index and no count, offset, lookahead,
unbounded iterator, or hidden history query. Initial pages contain exactly `min(current, limit)` new
rows. Continuation bounds derive `remaining = current - through`, then
`new = min(remaining, limit)` and `high = through + new`, so no overflowing addition is evaluated.
A continuation materializes one overlap plus zero through the requested maximum of 100 new rows.
`AT_TAIL` contains exactly the validated overlap and zero new rows. A continuation above the tail,
missing anchor, or valid digest/root mismatch is `ANCHOR_CONFLICT`; a coherent identity mismatch is
`IDENTITY_CONFLICT`; malformed retained state is `CORRUPT`.

Query-plan evidence is pinned at limits `1` and `100` and rejects a history-table scan.

### Closed SQLite result-code mapping

The only exact SQLite codes classified as retained corruption without an independent retained-row
proof are:

| Decimal code | Symbol | Outcome |
|---:|---|---|
| 11 | `SQLITE_CORRUPT` | `CORRUPT` |
| 267 | `SQLITE_CORRUPT_VTAB` | `CORRUPT` |
| 523 | `SQLITE_CORRUPT_SEQUENCE` | `CORRUPT` |
| 779 | `SQLITE_CORRUPT_INDEX` | `CORRUPT` |
| 26 | `SQLITE_NOTADB` | `CORRUPT` |

Every missing, non-exact, unknown, or other primary/extended code is sanitized `UNAVAILABLE`.
That includes `ERROR`, `PERM`, `ABORT`, `BUSY`, `LOCKED`, `NOMEM`, `READONLY`, `INTERRUPT`, every
`IOERR` primary/extended code, `FULL`, `CANTOPEN`, `PROTOCOL`, `SCHEMA`, `TOOBIG`, `CONSTRAINT`,
`MISMATCH`, `MISUSE`, `AUTH`, `RANGE`, and future unrecognized values. A statement constraint can
become `CORRUPT` only when the same coherent transaction independently proves a retained
generation-one contradiction. Exception text contains only the closed harness code; numeric
`sqlite_errorcode` may be retained as sanitized evidence, while SQLite text, SQL, paths, and record
values are not propagated.

### Fault and recovery evidence

Generated fresh-process tests cover:

- before transaction;
- after writer lock;
- between stream and creation-history insert;
- between creation-history insert and create commit;
- between transition-history insert and current update;
- between current update and compare-and-swap commit;
- true during commit;
- after commit before acknowledgement;
- real overlapping two-writer winner/one-attempt-BUSY contention followed separately by
  deterministic post-commit duplicate/conflict classification;
- fresh-process read-only and exact `SQLITE_BUSY` results;
- a closed numeric mapping showing every `SQLITE_LOCKED*` result becomes sanitized `UNAVAILABLE`;
- read-only path/permission failure;
- `SQLITE_FULL` through test-local maximum page count;
- exact `SQLITE_IOERR_WRITE` through a child `RLIMIT_FSIZE` WAL write;
- a long reader with writer/checkpointer concurrency; and
- fresh-open old/new/duplicate verification after each applicable interruption.

The true-during-commit probe is bounded Linux x86-64 test evidence. A parent uses `ptrace` to stop a
child at the WAL `pwrite64` system call only after a complete 24-byte frame header write, kills the
child, then opens a fresh connection and requires a fully valid old or new state. A mock seam before
or after `COMMIT` does not substitute for this proof. Unsupported mechanics yield `UNPROVEN`, which
fails TASK-064 rather than skipping or xpassing.
Pipe allocation or process-spawn failure also yields sanitized `UNPROVEN`; every successfully
opened descriptor is closed and every already-spawned child is killed and reaped before return.
An ordinary finite-IPC packet failure yields `FAIL`, closes every pipe, and boundedly reaps the
remaining exact children. WAL evidence counts only writer acknowledgements actually received,
rather than configured transitions that were never observed.

Every reopen verifies format/schema identity, `integrity_check`, an empty `foreign_key_check`,
bounded current state, and complete history/root pagination. The only evidence dispositions are
`PASS`, `FAIL`, `UNPROVEN`, and `NOT_APPLICABLE`; every in-scope seam must be `PASS`.

### Backup, restore, and same-format copy

Online backup uses `sqlite3.Connection.backup` into a fresh bootstrap-owned generation, including
while source appends force page growth during a nonterminal backup callback. It binds the terminal
`SQLITE_DONE` page total to both source-snapshot and verified destination page counts, rejects a
missing or mismatched terminal count, verifies a prefix-consistent source snapshot, closes all
connections, and records a complete manifest: distinct source/destination generation IDs, schema
fingerprint, SQLite source ID, page size, source/destination page counts, checkpoint and
finalization outcomes, injected exact fixed-UTC evidence time, source-snapshot and destination
stream/history counts, exact closed files with sizes/digests, and per-stream tails. It reruns
schema, integrity, foreign-key, current, full-history, and tail checks. A restore drill uses
another isolated generation. Raw live-file copying and a main-file-only digest are not accepted.

Same-format generation copy reads exact validated version-one values and writes them into a
separate empty version-one generation. It compares every original record/envelope byte, digest,
root, identity, count, and tail, and proves the source is unchanged. It is not incompatible
migration, shadow read, routing, cutover, rollback, compaction, or deletion evidence.

### Frozen generated-workload thresholds

Measurements are test-environment gates, not operational capacity evidence:

| Item | Frozen value |
|---|---:|
| deterministic seed | `64,064` |
| repeated latency samples | `5` |
| maximum one-operation sample | `2,000,000,000 ns` |
| maximum generated database bytes | `16 MiB` |
| maximum generated WAL bytes | `2 MiB` |
| maximum traced Python bytes | `64 MiB` |
| maximum open cursors in a report | `8` |
| test-local `max_page_count` | `16,384` |
| WAL auto-checkpoint | `4` pages |

The exact generated record-size matrix columns are `(label, transition kind, creation-record
bytes, transition-record bytes, creation-envelope bytes, transition-envelope bytes,
child-payload bytes)`:

| Class | Kind | Creation | Transition | Creation envelope | Transition envelope | Child |
|---|---|---:|---:|---:|---:|---:|
| `minimum` | `RETAIN` | 2,283 | 2,118 | 529 | 529 | 0 |
| `typical` | `RETAIN` | 2,511 | 2,291 | 594 | 594 | 0 |
| `maximum_contract_shape` | `ATTACH` | 18,595 | 26,512 | 5,900 | 12,662 | 6,467 |

`minimum` uses one-character ASCII identity/evidence atoms and minimum policy integers.
`typical` is seed `64,064` through the ordinary generated create/retain helpers.
`maximum_contract_shape` uses every identity atom at its character maximum with U+1F4B1,
128-character evidence IDs, maximum legal bounded policy integers, and a complete ATTACH child
payload. It is a frozen stress shape, not a mathematical claim that it is the largest value across
every legal transition. Independent TASK-061 hard caps remain 65,536 record bytes, 16,384 envelope
bytes, 8,192 child-payload bytes, and 32,768 envelope-hex characters.

The exact matrix entries are `(label, streams, retained versions per stream, audit limit)`:
`("minimum", 1, 1, 1)`, `("typical", 3, 9, 10)`, and
`("maximum_query", 1, 103, 100)`.

Latency samples are taken without Python allocation tracing; one separate equivalent bounded load
is executed under `tracemalloc` for the memory ceiling. This keeps timing and allocation
instrumentation as independent frozen gates rather than charging tracing overhead to the latency
measurement.

Generated canonical JSON reports exist only beneath the validated pytest root. They bind the
TASK-064 task/generation/digest, schema/application/version/marker identity, Python and SQLite
versions, source ID, thread mode, full compile tuple, the ordered exact observed reader and writer
DBCONFIG/defensive/limit/PRAGMA profiles, sanitized environment class
`generated-linux-pytest`, externally supplied exact UTC evidence time, seed, run count, both
matrices, every threshold, row/query counts, database/WAL/page/freelist sizes, cursor and traced
memory bounds, five latency samples, and the complete cross-validated backup manifest above.

The generated gate inventory is exactly `schema_identity`, `bootstrap_path_ownership`,
`runtime_connection_controls`, `projection_roundtrip`, `schema_constraints_corruption`,
`atomicity_classification`, `fresh_process_faults`, `bounded_queries`, `closed_error_mapping`,
`backup_restore`, `generation_copy`, and `workload_thresholds`; all twelve must be `PASS` with no
reason. The exact target/deployment inventory is `target_filesystem_power_loss`,
`target_permissions_alias_locking_sync`, `target_sqlite_runtime`,
`operational_capacity_checkpoint_latency`, `production_backup_retention_rpo_rto`,
`production_monitoring_stop_thresholds`, and `incompatible_generation_migration`; all seven must
be `NOT_APPLICABLE` with the fixed reason `outside_task_target_deployment_evidence`. Missing,
duplicate, reordered, extra, negative, or differently reasoned gates are rejected. Reports reject
arbitrary PRAGMA names and user/host/device/path identifiers. No committed report claims to bind
its own future commit; the durable exact-head evidence is the draft pull request and CI logs.

## Security and Authority Boundary

All records are synthetic. No test may open a pre-existing or non-harness-owned database. The
harness imports only standard-library modules and frozen pure TASK-061/062 types and validators.
Production source must not import test support. There is no caller SQL, extension, UDF, collation,
shared cache, provider, credential, operator path, accepted evidence body, time authority, fence,
budget, notification, or action.

The accepted 3.53.1 source includes the required WAL-reset fix but predates FTS5 fixes documented
for 3.53.2. Its narrow acceptance depends on all mechanical reachability barriers remaining intact.
Any caller-controlled SQL, virtual schema object, disabled defensive mode, authorizer widening,
source/compile change, production import, or non-generated database invalidates this decision and
blocks the task.

The registration and descriptor checks establish only the controlled pytest/cooperating same-UID
boundary. They do not prove resistance to hostile same-UID TOCTOU, descriptor-capable target VFS
isolation, target path policy, or production runtime ownership. Those are explicit independent
production blockers and target/deployment `NOT_APPLICABLE` evidence, never generated `PASS`
evidence.

## Consequences

Positive:

- The ADR 0031 physical design now has one executable, fingerprinted schema.
- Exact TASK-062 boundary semantics are exercised without adding a runtime adapter.
- Tail/history ownership, corruption classification, row bounds, crash seams, backup, restore, and
  same-format copy have deterministic generated-data proofs.
- Golden schema evidence cannot self-bless during ordinary tests.

Costs and residuals:

- The schema is intentionally redundant because bounded validation requires exact creation,
  current, and predecessor witnesses.
- The harness is tied to one Python/SQLite source and compile tuple.
- `ptrace` and `RLIMIT_FSIZE` evidence is platform-specific and must pass on the exact CI target.
- Hostile same-UID pathname races and target/VFS isolation remain outside the controlled pytest
  threat model and independently block production.
- Test filesystem results do not establish target filesystem, device, power-loss, capacity,
  latency, backup-destination, retention, monitoring, or recovery behavior.
- The compiled but unreachable FTS surface remains a production blocker pending a separately
  governed current runtime.

## Verification and Completion Gate

TASK-064 remains the current task until the exact immutable candidate:

1. passes formatting, lint, strict typing, complete tests, lockfile verification, dependency audit,
   health slice, and final-diff inspection;
2. has independent Engineering, Security/Risk, and QA approvals bound to the same generation-3
   digest, candidate head SHA, and CI evidence;
3. is published as a draft pull request with all in-scope evidence `PASS`;
4. receives exact owner merge authorization naming that pull request and its current head SHA;
5. merges without changing the authorized head; and
6. passes required target-branch CI before a separate completion-governance transition.

Until then, no production adapter or later incompatible-migration task is unblocked. TASK-037
remains blocked and authorization remains denied.

## Rollback

Rollback is a revert of the exact TASK-064 candidate. It removes only the committed text schema,
descriptor, fingerprint, harness, tests, and documentation. Pytest cleanup removes only exact
harness-owned generated artifacts. Rollback never opens, repairs, migrates, routes, truncates, or
deletes an operator database and changes no production runtime.

## Review Triggers

Review or supersede this decision before changing:

- TASK-059 epoch bounds, TASK-061 bytes/codecs/digests/roots, TASK-062 outcomes or audit semantics,
  or ADR 0031;
- application ID, marker, DDL, object order, descriptor normalization/domain/fingerprint, page
  size, WAL/synchronous behavior, projections, keys, constraints, indexes, triggers, or guards;
- Python/SQLite source ID, compile options, DBCONFIG controls, limits, PRAGMAs, authorizer, FTS or
  extension reachability, or error-code mapping;
- bootstrap root, filename, URI, ownership, permissions, alias handling, or cleanup;
- transaction statements, seam definitions, bounded-read arithmetic, query plans, backup/restore,
  copy, workload matrix, thresholds, report fields, or dispositions; or
- any production import, adapter, runtime, operator path/data, migration, retention, deployment,
  durability, capacity, RPO/RTO, readiness, or trading capability.
