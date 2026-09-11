# ADR 0032: Continuous Public-Trade Stream SQLite Schema and Evidence Harness

- **Status:** Accepted
- **Date:** 2026-07-29
- **Decision owners:** Project owner, Engineering Department, Security Department, Risk
  Department, Data Department, and Audit and Assurance Department
- **Generation-6 amendment:** `FROZEN`; normalized TASK-064 contract SHA-256 `ec89a1df740805cc9b43e6f2530e940c0bf9b66e8f25ed878d3207d091c4bcb8`.
  Generation 5, normalized SHA-256
  `ba258296d4ffde716dc6ec02bbf606ca3f08cb48258dfca8061ca597f9e649e2`, frozen at
  documentation commit `0167a6544b5d7735529f9e4bf9783e5e59ab1d51`, is superseded before
  integration or acceptance. Linux/ext4 and Linux/9p reproduction proved its exact cross-validation
  SHM timestamp equality unsatisfiable even while every protected identity and byte remained exact.
  Independent runner reviews also proved its descriptor-identity close retries, pathname packet
  deletion, post-reap process-group operations, and pathname-recursive private-root cleanup unsafe
  under fd/PID/name reuse. Generation-5 implementation outputs and result commits are historical
  evidence only and cannot be integrated. Any reused design must be freshly produced under a
  prospective active generation-6 lease and then independently reviewed and revalidated.
  Generation 4, normalized SHA-256
  `b079966273ef43d726ecc7aa64693189e7234a94397e965e334fe215eac60aa4`, is
  superseded before implementation; generation 6 starts only from the functionally passing
  generation-3 candidate `9ff70a8aa34e5bf154679957c913bb21e93a3d5e`.

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

The controlling TASK-064 contract is generation 6. It becomes executable only when its BACKLOG
generation line is `STATUS=FROZEN`, records the exact normalized SHA-256, and all three independent
contract reviews pass. Until then, generation 3 with normalized SHA-256
`86e3650608f2f1c96a9aa272b2b9cd597bc3d5ac188a39937afb974536d11ccb` remains historical
functional evidence, not current write authority.

### Generation-6 amendment

Generation 6 preserves every generation-3 schema, fixture, database, query, transaction, fault,
backup, copy, authority, receipt, unprimed publication, and test semantic and every generation-5
constraint except the unsatisfiable cross-validation equality of SHM timestamps. The exact
unchanged raw fixture SHA-256 values are:

- `schema.sql`:
  `1263b831a3e73bfc730beef6df7df48fce0e3654aa806672650de9f40b8a3e37`;
- `schema_descriptor.json`:
  `bb33dc9cb549be484c5dc7855abace6d851682e50883e51919a9169cdaae431a`; and
- `schema_fingerprint.txt`:
  `8a2508de6e018c67e9b18393cb0a5517cef3bea5df785bf432299a49a2a12ef8`, containing exactly
  `sha256:0410c1f08390a411c73427b3d07c542f3d1828def7c6adebab51cd57375355b3`
  followed by one LF.

The amendment retains the two bounded outcomes designed in generation 5:

1. the normal parent may explicitly prime one receipt-local report-validation entry before calling
   the unchanged successful writer; and
2. CI runs the frozen 2,298-node suite as five separate ordinary-pytest jobs instead of one
   monolithic job.

Neither outcome adds production code, runtime authority, a database path, a dependency, a lockfile
change, xdist, execnet, an in-process test worker, a retry, or a new pytest node. Generation-4
compact workers, prefetch, sandbox, cgroup, benchmark protocols, and implementation branches are
not carried forward.

For report validation, generation 6 changes only the primed live-epoch comparison: ordinary SQLite validation may change
`store.sqlite3-shm` `mtime_ns` and `ctime_ns` across the pre/post boundary, so only those two fields
are volatile there. The post-validation values become the exact retained epoch and are immutable
for cached publication. Every database/WAL field and SHM presence, identity, permissions, size, and
complete raw digest remain exact. No generation-5 result commit is accepted; any reused design must
be freshly produced under an eligible prospective generation-6 lease and exact-digest review.

Generation 6 also replaces the generation-5 runner's unsafe cleanup mechanics without changing its
manifest, partition, packet schemas, proof modes, output bounds, default inertness, or five-job
outcome. Observation packets become parent-created anonymous `O_TMPFILE` descriptors passed
explicitly with bounded positional I/O; every owned handle is detached, poisoned, and closed once;
`GITHUB_OUTPUT` rollback opens a fresh authenticated descriptor; child identity remains anchored by
pidfd until group cleanup and one consuming reap; and private-root cleanup retains directory
descriptors and removes only fd-relative authenticated names. Replacement or cleanup uncertainty
is preserved as residue and failure, never repaired by retry or pathname-recursive deletion.

The generation-6 write scope additionally includes
`tests/unit/test_task_064_continuous_public_trade_stream_sqlite_schema.py` only for one import-time
exact `os._exit` capture, one private reserved-`191` raw-wait guard, immediate guard calls at the two
existing direct `waitpid` consumers in
`test_live_transaction_authority_rejects_hostile_token_and_connection_binding` and
`test_connection_runtime_creator_pid_rejects_inherited_authority`, and narrow static-inventory
assertions. Each parent block reaches its sole wait/guard through unconditional cleanup before its
payload or ordinary-status assertion even if pipe read/close fails; pipe-close and wait attempts are
independent and prior failure is latched until after the guard, so observer-fatal child exit cannot
skip reaping. Existing
child exits `0`/`70`, node and parameter IDs, ordinary assertions, and every unrelated module
byte/semantic remain generation-3 exact. No node is added and the 2,298-node manifest is unchanged.

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

The two fixture and binder call sites are sealed with an external-reference-independent,
domain-versioned TLV fingerprint over every Python `CodeType` field, with filename and first line
normalized to the named source suffix and line one. The domain binds CPython, its cache tag, exact
Python `3.13.14`, and optimization level. Constants have exact type tags, recursive code/tuple
encoding, sorted frozenset members, strict depth/node/cumulative-byte limits, and fail closed on
unsupported or canonically ambiguous types.

A separate occurrence-ordinal partition authenticates identity sharing within the selected
executable constant graph rooted at each snapshotted `co_consts` tuple, including nested code,
tuples, frozensets, and supported scalar constants. It preserves behavior-observable internal
aliases without incorporating external reference counts or raw object IDs. Root-code identity and
aliases between constants and nonconstant metadata are outside this identity partition; every
metadata value remains authenticated by the value TLV. Whole-object `marshal` bytes are not an
identity because their unused reference flags can differ after a valid pytest assertion-rewrite
cache round trip while every selected value and internal constant-graph alias agrees.

Fingerprint memoization is an optimization only. Its bounded table keys the retained exact
`CodeType` identity together with the normalized source suffix and fingerprint domain, retains the
code object itself so an object-ID collision cannot alias a result, and fails closed when its
fixed capacity is exhausted. Request and computation counters prove repeated authentication of
one exact code object reuses one computation. External-reference invariance is proved separately
with genuinely distinct, equivalent, previously uncached code objects; a cache hit is not accepted
as that proof. The serializer, capacity, and exhaustion stress runs only in a fresh authenticated
exec-isolated pytest child with a fresh harness authority. The parent snapshots and proves the
identity and ordered entries of its cache plus its request/computation counters are unchanged; no
capacity proof may fill or directly mutate the shared parent cache, and rejected oversized or
unsupported inputs must leave the child cache entries unchanged.

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

Each of the two pytest root fixtures is built by a private factory and executes only through
closure-sealed exact dependencies. Before provenance handling, permit registration, or auxiliary
root I/O, it revalidates the original module globals, pytest request and temporary-path types,
temporary-root callable, `Path` and function types, environment and context modules, unwrap and
fixture decorators, exact exported fixture and raw callable, harness authority functions, and
failure types. Rebinding the authenticated globals dictionary, replacing an underlying module
attribute, or substituting the exported fixture therefore fails before `BEGIN` or filesystem
effects. This is an enforcement of the existing module-global-substitution boundary, not an
expansion of the same-interpreter exclusion.

All five internal nested-pytest protocols—execution isolation, post-return fixture replay, root
latch probes, shared cleanup, and report-close probes—use one common inherited-descriptor
provenance packet. The canonical packet binds the validated `0700` root and `0600` marker
identities, parent PID, exact independently supplied issuer node, exact target child node,
protocol, mode, random nonce, and packet digest. Issuance proves the active node is the issuer;
consumption proves the packet issuer against the independently expected issuer and the packet
target against the current child node. Same-node protocols require issuer and target equality;
post-return replay alone binds its exact orchestrator issuer to a distinct exact target. Successful
consumption unlinks the marker, closes the inherited descriptor, records the nonce, and scrubs
every provenance and legacy mode variable. Bare legacy environment values are scrubbed but cannot
select or shorten a parent matrix, malformed Unicode envelopes become closed harness failures,
replay fails, and every nested pytest invocation disables its cache provider.

Packet consumption precedes `BEGIN`, auxiliary-root creation, root-scope construction, and fixture
I/O, and is distinct from later ticket claim. Successful authentication registers one opaque
closure-owned ticket; the fixture binds it to the exact target node, PID, original temporary-root
object and identity, and originating `ContextVar` token before `BEGIN`. Post-return replay claims
that ticket locally in the fixture. Every other protocol leaves it only in the private context and
registry until the test body supplies the exact closed protocol/mode matrix while the exact root
session is active. Claim first validates issuer, target, mode, provenance root, fixture root,
session, PID, reserved-environment absence, and state, then resets the activation token as the
exact-context gate and installs a distinct finalize token. A copied/new context, thread, fork,
wrong protocol/node/mode, replay, or late envelope cannot mutate claim state.
Hostile code that runs before fixture authentication and deliberately rewrites the inherited marker
descriptor, canonical packet, envelope, and digest is likewise outside this controlled/cooperating-
process boundary. Within the accepted boundary, the exact parent-issued mode inventory and result
attribution are preserved; an out-of-policy mode or caller-supplied mode mismatch fails before
ticket state changes.

Every authenticated ticket reaches exactly one bounded terminal record: `RETURNED` after a claimed
successful lifecycle, `CANCELLED` after any setup/body/exit failure, or `UNCLAIMED` when a body
returns without consuming its ticket. Unclaimed teardown is a test failure. Fixture failure paths
independently attempt permit cancellation and ticket cancellation; token-reset, descriptor-close,
or other cleanup ambiguity latches root-authority uncertainty and fails closed. Causal nested-pytest
evidence covers all five protocols, wrong packets before `BEGIN`, every auxiliary-root creation,
scope construction/entry, body and teardown failure, one-shot claim/replay, context/thread/PID
separation, and zero live ticket-registry/context residue after failure.

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

The controlled-process model also excludes hostile code already executing in the same Python
interpreter that deliberately traverses or mutates private closure cells, closure-owned authority
registries, debugger state, `ctypes`, or raw process memory. This narrow same-interpreter exclusion
does not relax any rejection of forged or mutated caller-held capabilities, direct exported
wrapper bypasses, module-global substitution, fixture/node/path/PID/UID/mode/nonce mismatches,
revocation or cleanup failure, filesystem identity and ownership failure, or any independent
production blocker.

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

The normal parent completes one generated evidence run, derives all twelve positive gates once,
seals the exact receipt, publishes and readback-verifies the canonical report, and then claims one
closure-owned opaque published-artifact capability. That capability remains valid only while its
consumed receipt and ledger, exact run and aggregate/report identities, pytest-root and report-path
identities, process/thread/node and context binding, source fingerprints, contract
generation/digest, schema fingerprint, exact artifact bytes/digest, and expiry remain current.
Report-shaped bytes, copied or forged capabilities, and child processes cannot mint this authority
or semantic gate evidence.

From that capability the parent sequentially pre-issues four distinct one-shot, mode-bound
provenance packets with separate unlinked read-only artifact descriptors and fresh bytecode-cache
prefixes. It launches each independent pytest child through the authenticated launcher and waits
for that child's bounded kernel-credential attestation before launching the next. Launcher
attestations are therefore serialized; simultaneous child overlap is neither claimed nor required.
After the launch sequence, output drains run concurrently with fixed per-stream and aggregate byte
ceilings under one shared deadline. Any failure terminates and boundedly reaps the remaining set and
requires exact marker, descriptor, process, and bytecode-cache cleanup.

Each child revalidates the inherited parent artifact and its complete publication binding before
claiming one child-local publication permit, then exercises only its assigned destructive
close-publication state transition: staging-descriptor close ambiguity, readback-descriptor close
ambiguity, readback-verified root-descriptor close ambiguity, or reentrant root revocation. Each
branch verifies the exact staging/report inventory, terminal publication-permit state,
cleanup-uncertainty or root-revocation state, and nine protected fork guards before returning. No
child begins an evidence run, builds gates, constructs a report, seals a receipt, or claims the
parent capability. Tamper, splice, shallow-query, seal-transition, second-digest, generic negative
report regressions, and the successful publication path remain in the normal parent and are not
repeated by the close children.

### Generation-6 bounded report prime

The optional API is exactly:

```python
def prime_evidence_report_validation(
    pytest_root: Path,
    *,
    receipt: _EvidenceReceipt,
    report: EvidenceReport,
) -> None:
    ...
```

Prime performs the complete generation-3 receipt, root, semantic report, canonical-byte,
live-source, gate, backup-manifest, source-fingerprint, and cleanup validation. It writes no file,
consumes nothing, returns no token or authority, and returns exactly `None`. It is never implicit.
An unprimed `write_evidence_report` retains the complete generation-3 successful path.

One typed closure-local entry may remain for one exact receipt, at most one active attempt, and an
absolute 120-monotonic-second lifetime. It binds the exact root path object and registration; run,
ledger, receipt, receipt evidence, and source report objects; PID, thread, node, and active context;
generation/digest/schema/source identities;
receipt-evidence and report-core digests; complete canonical report bytes/length/digest; and the
complete live epoch. `TASK064-REPORT-CORE-V1` covers the 40 non-evidence report fields in dataclass
declaration order followed by the receipt evidence digest through the existing bounded canonical
evidence normalizer. `TASK064-REPORT-LIVE-VALIDATION-V1` separately binds the four
ordinal-ordered fresh summaries/tails and the ordinal-zero current projection.

`TASK064-REPORT-LIVE-EPOCH-V2` uses exact role order `bootstrap`, `backup_source`, `backup`,
`restore`, `concurrent_source`, `concurrent_backup`, `generation_source`,
`generation_destination` and exact token-identity alias vector `[0,1,0,2,1,0,0,3]`. Exactly four
unique token objects bind their registrations, generation IDs, summaries, tails, and complete
descriptor-validated closed-file manifests. The canonical payload is compact sorted-key ASCII JSON
with no LF and exact keys `domain`, `roles`, `entries`. Each ordinal entry binds exact scalar
pytest-root, generation, and database registration identity; summary and tail
`_evidence_payload_digest` values; and fixed-order `store.sqlite3`, WAL, and SHM observations.
Absent files use `present=false` plus null metadata. Present files bind descriptor/path agreement,
device, inode, UID, mode `0o600`, one link, size, mtime/ctime nanoseconds, and complete raw SHA-256.
The main file is bounded at 16 MiB and WAL/SHM at 2 MiB, read in 65,536-byte blocks; no other
generation entry is accepted. Within each individual capture, every present descriptor proves
descriptor/path agreement and stable device, inode, UID, mode, link count, size, mtime/ctime
nanoseconds, complete bounded read, and trailing EOF.

Prime captures the four pre-validation file sets, performs live SQLite validation exactly once to
obtain the summaries/tails, and captures the four post-validation file sets. Across that boundary,
every database and WAL observation remains completely equal. SHM preserves exact presence, device,
inode, UID, mode, link count, size, and complete raw SHA-256; only SHM `mtime_ns` and `ctime_ns` may
differ. An absent SHM remains the exact all-null object. The canonical V2 live epoch is built only
from the exact post-validation observations, including the observed SHM timestamps.

A cached writer checks all registrations and complete retained post-validation file observations
at entry and immediately before link. Both checks require complete equality, including the retained
SHM `mtime_ns` and `ctime_ns`. Token, registration, root, generation, and database path object
identities remain outside the digest and must match exactly. Semantic/type/alias/value disagreement
is `CORRUPT`; OS, descriptor, inventory, protected-field drift, post-snapshot drift, or cleanup
failure is sanitized `UNAVAILABLE`.

This exception cannot be implemented by connection warmup, a held SQLite connection, changed open
mode, URI or connection semantics, a different VFS, clone validation, forced or altered checkpoint,
repair, metadata restoration/touch, or an alternate validation path. None is authorized.

The only states are `EMPTY`, `VALIDATING`, `READY`, and `PUBLISHING`. Prime is legal only from
`EMPTY`; it installs `VALIDATING` first and clears on failure. After successful post-validation it
captures the sealed `time.monotonic_ns`, issues one exact nonnegative nanosecond timestamp, and
checked-adds `120_000_000_000`. An entry is live only while observed time is strictly below expiry;
every later clock value must be an exact nonnegative built-in integer no lower than the prior
accepted value. Clock failure/regression is `UNAVAILABLE`; equality expiry clears and rejects
`CORRUPT` without receipt consumption. Duplicate exact prime rejects without refresh. An unrelated
object/thread/node/context rejects without selecting or changing the owner's valid entry; owner
object/source/receipt drift clears. The fork-child hook clears only the inherited child copy.
Reentrant prime/write in `VALIDATING` or `PUBLISHING` latches cleanup uncertainty, clears, and makes
both calls fail.

A matching writer enters `PUBLISHING` before C0. Existing receipt checkpoints C0 through C3 remain
unchanged and there is no fifth checkpoint. Clean rollback may retain `READY` only with no remaining
publication, certain cleanup, every binding and live epoch unchanged, and the original unrefreshed
deadline. Link-then-fail requires the owned inode absent from both names; collision additionally
requires the foreign final inode and bytes unchanged. Success, any failure after readback begins,
remaining publication, terminal receipt, ambiguous cleanup, or root teardown clears.

The same report node has no new parameter ID. Inert all-or-none mode, 64-hex nonce, and inherited
anonymous-observation-descriptor CLI options select only `primed-full`, `unprimed-success`,
`expired-entry`, or `ready-teardown` proof flow.
`_observe_task064_report_validation_for_test` exposes only state/presence/issue/expiry scalars and
`_force_task064_report_validation_expiry_for_test` can only shorten the exact owner's deadline.
The test body retains only a pending scalar record. The authenticated fixture finalizer captures
the asserted state, clears the entry before root-scope exit/revocation, proves `EMPTY`, and only
then exclusively publishes canonical `TASK064-REPORT-PROOF-OBSERVATION-V1` through its inherited
anonymous proof descriptor, bound to nonce and child/runner PIDs. The runner validates it after
zero exit, pidfd-anchored group cleanup, and the one consuming reap, then wraps it in
`TASK064-REPORT-PROOF-RESULT-V1`; the report shard's output identity derives only from its
`primed-full` observation. Separate bounded proof results bind
unprimed success, equality expiry, and READY teardown. Separately generated reports intentionally
carry unique generation identities, so their digests are not compared. Each route instead proves
that its own exact input report is serialized by the same captured pure serializer and that those
exact returned bytes are published unchanged.

The unchanged report node contains the complete generation-6 regression matrix without a new node
or parameter ID. On Linux/ext4, its positive branch requires ordinary live validation to change at
least one present unique store's SHM `mtime_ns` or `ctime_ns`, proves every protected field equal,
retains the exact post snapshot in `READY`, and completes primed publication. Closed negative
branches independently reject any database/WAL pre/post drift, any SHM protected-field drift, any
per-capture instability including SHM timestamp instability around its own read, and any later
writer-entry or before-link change from the retained post snapshot including SHM timestamps. Each
prime-side rejection is `UNAVAILABLE`, clears to `EMPTY`, consumes no receipt, and publishes
nothing. The later writer regressions also permit no successful publication. Test-local deterministic
observation/substitution may exercise the negatives only inside the existing two TASK-064 modules;
it creates no alternate validation route or public API.

### Generation-6 native CI split

Every job independently collects and validates the exact sorted canonical ASCII JSON full manifest:

```text
domain: TASK064-NODE-MANIFEST-V1
count: 2298
bytes: 296078
sha256: 96a15ecb6af6469f6da82bace28350163b99d48b8226d867830f7aec5816b483
```

The exact report node is forced into `report`. Every other node uses the unsigned big-endian first
eight bytes of SHA-256 over its ASCII node ID modulo four and enters `remainder-0` through
`remainder-3`, while its original collection order is preserved for execution. Shard manifests use
domain `TASK064-NODE-SHARD-V1` and bind the full digest, shard ID, and sorted node IDs:

| Shard | Nodes | Canonical bytes | SHA-256 |
|---|---:|---:|---|
| `report` | 1 | 302 | `f0530be0d219c64bbd1b9eb4df635dd138a345e8685172d188d02e177e488a3e` |
| `remainder-0` | 600 | 76,664 | `9946638e834ffa44c0cd1e511c348b1f8226fc078ef79eb9e4100e770de80044` |
| `remainder-1` | 563 | 72,767 | `e6814eb76767d9462ed9bfa82c85d8e7daebd7265027883290ca88842365dfd0` |
| `remainder-2` | 588 | 75,756 | `8d86911d7a7cfc4f32022d68be1eaa3d6b459d5a968a462deea188df2369ed5b` |
| `remainder-3` | 546 | 71,332 | `69e69516345c674cadf552202b80c392f8297b74b46328278b098b462e25b4ca` |

The runner accepts only lowercase 40-hex Git identities. Tested `HEAD` equals `GITHUB_SHA`.
Pull-request checkout has exactly two parents and exact second parent
`TASK064_CANDIDATE_SHA`; push/manual checkout is the candidate itself. Candidate ancestry and both
candidate/checkout tree objects are proven with complete fetched history before collection.

Inherited pytest option/plugin/autoload variables and Python import-path overrides are rejected.
Repository `addopts` must equal the frozen ordered
`--strict-config`, `--strict-markers`, `--import-mode=importlib` tuple. The sole exact external
plugin inventory is
`[["hypothesis","6.157.1","pytest11","hypothesispytest","_hypothesis_pytestplugin"]]`.
The worker-indicator count covers pytest worker input, the three named xdist environment signals,
and every loaded xdist/execnet plugin/module indicator and must be zero. Scheduler/retry/plugin
injection, surviving non-main threads, initial-group process residue, and failed existing
nested-child cleanup evidence are rejected mechanically.

The parent retains no-follow directory descriptors plus exact filesystem and Linux mount identities
for the selected temp parent and one random mode-`0o700` private root. Children and cleanup are
created, classified, enumerated, and removed fd-relatively. Classification uses no-follow stat plus
`O_PATH|O_NOFOLLOW|O_CLOEXEC`, never a content open, so FIFO/device content cannot block or run.
Only authenticated same-UID directories on the retained root mount and same-UID regular files on
that mount with `st_nlink == 1`, no set-UID/set-GID/sticky bit, and ordinary mode in
`0o000..0o777` are removable. Symlink, socket, FIFO, device, hardlink, mount/bind identity change,
unknown type/mount identity, or ownership/mode disagreement is preserved as residue and failure.
Only an authenticated directory receives a separate no-follow directory traversal open. The
original root name is removed only while it still agrees
with the retained root descriptor and the authenticated inventory is empty. A missing or mismatched
name, replacement already present before authentication, unexpected entry, or close ambiguity is
preserved as residue and failure. The runner never uses pathname-recursive removal, follows a
symlink, or deletes an observed unproven replacement. Relative authentication plus `unlink`/`rmdir`
is not a compare-and-swap; post-authentication hostile same-UID interposition is outside the
controlled cooperating-process claim and is not exercised as an atomic-preservation negative.

Fd-relative traversal has exact caps: depth `64`, entries `100_000`, cumulative encoded component
bytes `16_777_216`, cleanup operations `500_000`, per-name bytes `255`, and one absolute
`60_000_000_000` monotonic-nanosecond deadline. Scan acquisition, each yielded entry, no-follow
stat, open, unlink, and rmdir each charge one operation before acting. Invalid component encoding,
clock error/regression, cap, or deadline stops destructive work, closes each already owned handle
once, preserves the remainder as nonzero residue, and fails.

The exact observer option is `--task064-ci-observation-fd=<canonical decimal fd>`; proof mode adds
`--task064-report-proof-observation-fd=<canonical decimal fd>`. Collection/remainder passes exactly
one descriptor and report/proof passes exactly two distinct descriptors in CI-then-proof order as
the complete `pass_fds` tuple with `close_fds=True`. The parent creates every object anonymously
below the retained child-root descriptor with `O_RDWR|O_TMPFILE|O_CLOEXEC`, mode `0o600`, and
requires regular same-UID, zero-link, zero-size, unique identity before launch. No observation
pathname, link, rename, unlink, truncation, or directory `fsync` exists.

The observer requires exact phase, 64-hex nonce, anonymous descriptor option, and an execution-only
closed shard/proof identity, or is wholly inert. It uses only collection, deselection,
collect-report, runtest start/report/finish, interrupt/internal-error, and final session hooks.
Compact canonical ASCII packets bind nonce/PIDs, plugin/worker state, collection errors,
assigned/collected/started/finished ordered IDs, and every setup/call/teardown outcome. The exec
child captures its PID as sole writer plus captured `os._exit`, `signal.pidfd_send_signal`, and
`signal.SIGKILL` authorities and, before hooks or collection, uses exact `F_GETFD`,
`F_SETFD(prior|FD_CLOEXEC)`, and equal readback to restore CLOEXEC on each CI/proof fd made
inheritable by `pass_fds`. It creates a zeroed/readback-verified one-byte anonymous shared-mmap
fork-poison latch. It opens a self-pidfd for its captured PID, requires an exact fd greater than two
with number/fstat identity distinct from standard and observation handles, exact-restores CLOEXEC,
and validates the owner with exact-`None` `pidfd_send_signal(fd, 0, None, 0)`. It then registers an
after-fork hook that detaches, poisons, and closes each raw-fork child's inherited CI/proof fd once
before child code, attempting both independently, while poisoning plugin access to but not closing
the inherited self-pidfd. Any observation-close ambiguity sets/readbacks poison one after both
attempts. Successful exact-one readback immediately calls captured `os._exit(191)`; latch failure
instead invokes captured `pidfd_send_signal(inherited_fd, SIGKILL, None, 0)` against the anchored
owner, never a numeric owner signal, catches signal-call exception/non-`None`, and still calls
captured `os._exit(191)`. Neither ambiguity branch returns or runs user/later cleanup. Simultaneous latch-mutation and anchored-
signal failure is cleanup uncertainty, not a guaranteed owner kill; reserved terminal propagation
still makes it non-publishable.
Only total observation-close success returns to child code. The child leaves its poisoned pidfd copy
to process-exit reclamation; later exec drops it through CLOEXEC. PID/owner gates forbid a descendant
from writing, selecting, restoring, or reviving a packet. Normal exit `191` is globally observer-
fatal across the complete TASK-064 process tree. Before any expected-death/nonzero/signal/exception
classification, every raw waitpid consumer rejects `WIFEXITED && WEXITSTATUS==191`, every waitid
consumer rejects `CLD_EXITED && si_status==191`, and every Popen/subprocess-facing consumer rejects
exact integer/non-bool returncode 191. Intermediate descendants propagate captured `_exit(191)`
after their sole consume and before user cleanup/publication. After its sole consume, the pytest exec
owner invokes the already-captured `_exit(191)` exactly; no consumer may raise/remap/return/continue
through catchable expected-failure code. Runner exact nonzero plus missing final CI rejects any
earlier proof. A complete static call-site
inventory and sentinel injection cover every raw wait, including the two named direct unit-module
consumers, nested-helper, and Popen/subprocess terminal route; no pre-existing or alternate semantic
use of 191 is allowed. The proof gate requires poison zero but
retains latch/pidfd through teardown. Final try-last CI session-finish, only after all tracked raw
children are reaped, rechecks zero, detaches/poisons/closes the owner's self-pidfd once, closes the
latch once, and writes only if both closes are certain. A reserved exit 191, close ambiguity, or
earlier proof packet without the final CI packet cannot pass; no raw fork is permitted after this
terminal gate.

The child writes at checked offsets from zero with positive-progress `pwrite` chunks of at most
65,536 bytes, within the exact 400,000/1,500,000/4,096-byte domain cap, then file-syncs. It and the
parent separately reproduce the exact bytes with bounded explicit-offset `pread`, followed by one
EOF read. The child proves its own post-write device/inode/UID/mode/zero-link/size/mtime/ctime
snapshot stable locally. After terminal status, the parent takes a separate snapshot and proves
every device/inode/UID/mode/link-count/size/mtime/ctime field stable across its own read; unchanged
packet schemas carry no child timestamps, so no false
cross-process timestamp equality is required. Only mtime/ctime may change from the initial empty
snapshot during the sole child write/sync. Plain
read/write, seek, append, truncate, mmap, link, and a second write pass are forbidden. The child
closes its inherited owner once after write/readback; the parent neither reads nor closes its owner
until authenticated terminal status and the consuming reap, then readback-validates and closes it
once before root cleanup or result construction. The observer never selects, orders, marks,
schedules, retries, launches, or changes an outcome.

The spawn type is private `_NoImplicitWaitPopen(Popen[bytes])`. Before first spawn, one exact
built-in one-element owner list per permitted launch ordinal is preallocated and retained in a fixed
closure tuple through runner exit. The runtime requires captured exact CPython 3.13.14 built-in
`Popen`/`_fork_exec`, one main thread, and no trace/profile. Every catchable Python handler is exact
`SIG_DFL`/`SIG_IGN`, except exact `signal.default_int_handler` for SIGINT and exact `SIG_DFL` for
SIGCHLD; SIGCHLD `SIG_IGN` and custom handlers reject. On the main thread immediately before each
permitted Popen and before its sensitive fds, `getsignal(SIGCHLD) is SIG_DFL`, idempotent
`signal(SIGCHLD,SIG_DFL)` returning exact DFL, and exact-DFL immediate readback are mandatory. On frozen
CPython/Linux this reset clears ordinary `SA_NOCLDWAIT`; handler identity alone is not a flag query.
No validation child is added, so only the frozen collection/execution pytest children launch. As the
first subclass-initialization operations, before base construction or child creation, it pre-seeds
`self.pid` to a private noninteger sentinel and `self._child_created=False`, stores its designated slot/index, performs allocation-
free/non-user-code `owner_slot[0] = self`, and requires identity readback. Without that identity base
init has not begun and no child exists; with it, the destructor is a no-op. A temporary preallocated
non-raising SIGINT latch, never a blocked mask, spans base initialization; exec resets the child-side
caught disposition, the parent restores its exact handler, and a latched event fails through the
already-owned path. Any later base-constructor/async
exception retrieves and retains the exact partial object, independently closes all five raw fds,
and never relies on base cleanup. Exact positive built-in `pid` OR true `_child_created` means a
possible child because CPython assigns PID before the flag; true-without-valid-PID is uncertainty.
Before pidfd/proc/signal action, one `waitid(P_PID,pid,WEXITED|WNOHANG|WNOWAIT)` probe is mandatory:
`ECHILD` proves no owned child and forbids another PID/wait action, `None` proves live unreaped, and a
terminal tuple proves an owned zombie. Other outcomes are uncertainty with no PID action; returncode
alone is never trusted. Live/terminal ownership then permits pidfd plus `/proc` PID/PPID/UID/start-
time binding, pidfd SIGKILL of a live leader, group SIGKILL only after SID=PGID=PID proof, leader-only
otherwise because exec/setsid is unproven, and exactly one consuming wait. Pidfd-unavailable fallback
retains probe ownership, group-SIGKILLs only after the same SID=PGID=PID proof or otherwise PID-
SIGKILLs only a live direct child, and makes at most one
`waitid(P_PID, WEXITED)`. No TERM/user-code cleanup or status/PASS claim is made on ambiguity; the
slot remains retained through runner exit. Successful constructor return is already strong adoption.
Parent-created CLOEXEC stdin-devnull and
`pipe2(O_CLOEXEC)` stdout/stderr fds are greater than two, pairwise distinct, and replace Popen
stream wrappers. Constructor/async failure retrieves the already slot-owned partial object,
independently closes all five caller-owned fds, and performs the provisional direct-child cleanup
above when required; it never relies on base cleanup. Successful return closes all three child-side originals once
and only retained read ends become nonblocking. The slot-owned object remains strongly quarantined to
runner exit so GC or a later Popen cannot register it in or clean it through `subprocess._active`.
The child uses `start_new_session=True`. Before output handling, pidfd and `/proc` bind exact PID,
UID, start time, session, and PGID, with session/PGID equal to the leader PID. Pidfd readiness and
the pidfd's number and fstat identity must differ from every then-open owned handle. One
`waitid(P_PIDFD, WEXITED|WNOWAIT)` observes the terminal tuple without consuming. A clean exit
sends no signal; failure/timeout/initial-group residue alone permits anchored bounded TERM/KILL.

Before reap, a bounded `/proc` scan proves zero same-session/same-PGID non-leader survivors while
binding their UID/start time. `survivor_count` is only that initial-group count. Existing
authenticated TASK-064 harness proofs remain responsible for nested children that intentionally
`setsid`; the runner makes no namespace-wide survivor claim. Exactly one consuming
`waitid(P_PIDFD, WEXITED)` must reproduce the WNOWAIT tuple and is locally decoded without reading
or writing `Popen.returncode` on the normal returned-child path, only when `si_code` is one member of the closed alternative set
`{CLD_EXITED, CLD_KILLED, CLD_DUMPED}`. An exception is never retried or interpreted, and the no-op object
remains quarantined to process exit. A pidfd-open failure is FAIL-only and permits at most one
direct-child `waitid(P_PID, WEXITED)` attempt while its unreaped identity anchors bounded cleanup.
No PID/PGID/proc/wait operation occurs after either consuming attempt. Every descriptor/stream/
selector is detached and its number poisoned before one close; close ambiguity blocks PASS and
process exit is the final boundary.

Each selected vector is an argv array below the frozen 131,072-byte ceiling, never shell text.
The exact final argv/environment projection includes every NUL and pointer plus a fixed 32,768-byte
reserve and must fit positive `SC_ARG_MAX`; each string is independently within the Linux
131,072-byte including-NUL ceiling. Acceptance requires exact assigned/collected/started/finished
sequence equality, one passing setup/call/teardown per node, all 2,298 nodes across the split, zero
unknown/duplicate/fail/error/skip/xfail/xpass/deselect/interrupt/unaccounted outcomes, collection
within 60 seconds, test execution within 720 seconds, zero initial-group survivor count, and passing
existing nested-child cleanup assertions.

GitHub Actions retains every non-test quality/security gate and uses five statically named fresh
Ubuntu jobs—one per exact shard—with ordinary pytest. Checkout fetches complete history and does
not persist credentials. Each named job runs one collect-only process and one test-executing pytest
parent, then exposes only standard-base64 canonical `TASK064-CI-SHARD-RESULT-V1` plus its digest.
No matrix, artifact splice, conditional omission, masked failure, output default, retry, or
`continue-on-error` is accepted.

One final stable `Quality and security` aggregator explicitly needs the non-test gates plus all five
named jobs. It requires every result `success`, decodes and fully validates every exact packet,
hard-binds the five expected shard identities, proves shared Git/contract/runtime/manifest
identities, five distinct shards, exact sums of 2,298 for every phase, zero anomalies, and complete
cleanup. Base64 input is capped before strict decoding and must reproduce exactly on re-encoding.
The aggregator receives the six results and ten packet/digest values only through the frozen
`TASK064_AGG_*` environment names, never shell interpolation or defaults.

Before children, each GitHub command file is descriptor/path validated below `RUNNER_TEMP` and
bound by identity, mode, size, mtime/ctime, and raw digest; the four normalized absolute path byte
strings and four `(st_dev, st_ino)` pairs are each pairwise distinct, and every descriptor/path mount
identity agrees with the retained `RUNNER_TEMP` mount. Same-path, hardlink/inode, bind-mount, or mount-
identity alias fails before child launch or publication. `GITHUB_OUTPUT` is initially empty.
Every child must leave those snapshots unchanged. Only a fully passing parent appends the two exact
output lines through its retained no-follow/CLOEXEC descriptor, syncs, exact-readbacks, detaches,
poisons, and closes it once; the other command files are never written. A publication failure can
only attempt rollback through a newly opened and newly path/descriptor-authenticated handle for the
original inode whose current bytes are an exact prefix of the intended output. That fresh handle
alone may truncate, sync, read back empty, detach, poison, and close once. The stale publication fd
is never inspected, retried, or closed again; replacement/non-prefix/rollback uncertainty is
preserved and fails, and rollback never converts the invocation to PASS. A replacement already
present before fresh open/authentication is never mutated; after authentication, mutation remains
fd-pinned and final path reauthentication fails on a swap, but this is not an atomic hostile
same-UID path-continuity claim. Failed rollback may leave partial or complete untrusted
`GITHUB_OUTPUT` residue; the job exits nonzero and the aggregator requires its exact job result
`success` before accepting outputs, so parsed residue cannot pass. Non-GitHub failure emits no PASS
stdout. The shard jobs remain visible. No workflow success grants merge,
deployment, production, or trading authority.

Existing nodes, without a new parameter ID, inject and reject unavailable/invalid/aliased/nonempty
anonymous descriptors; bounded positional-I/O, metadata, payload, sync, EOF, close, and CLOEXEC-
restore/readback failures; self-pidfd acquisition/identity/CLOEXEC/zero-signal/final-close failure;
and fd-number reuse after an ambiguous close. Raw-fork cases prove independent observation closes,
poisoned-but-kernel-retained child self-pidfd, non-owner PID gating, reserved-191 inspection before
any intentionally accepted other death, shared poison blocking, and successful anchored pidfd `SIGKILL` blocking owner
publication when latch write/readback is forced to fail, without numeric owner signaling. A separate
dual latch-plus-signal failure propagates 191, emits no PASS packet, and makes no owner-kill claim.
Direct, expected-other-death, and nested multi-level sentinel cases plus static inventory cover every
raw wait, including both named unit-module consumers, nested-helper, and Popen/subprocess consumer. A broad `BaseException` catcher proves exec-owner
captured `_exit(191)` cannot be caught/continued; unguarded, raise/remap, or alternate-191 use rejects.
An earlier proof packet
without the final CI packet is rejected.

Private-root cases cover pre-authentication replacement, symlink/socket/FIFO/device/hardlink,
foreign mount/bind or unknown identity/type, ownership/mode disagreement, and each exact
depth/entry/name-byte/operation/per-name/deadline/clock-regression bound; FIFO/device content is never
opened and every replacement/unvisited entry is preserved as nonzero residue. Process cases cover
leader session/PGID/start-time mismatch; pidfd, WNOWAIT, signal, initial-group survivor, and single-
consuming-wait failures; already-owned constructor/async-boundary failure, all-five-fd rollback,
post-fork/pre-child-flag positive-PID interruption, pending-SIGINT latch, ECHILD/live/zombie non-
consuming probes, already-consumed exec-error with no double wait or returncode-only trust,
provisional pidfd/numeric leader/group SIGKILL plus at-most-one consume, and no-
child fail-only quarantine; and GC plus later-Popen implicit-poll prevention after successful identity
return. They reject wrong CPython/Popen identity, extra threads, trace/profile, unexpected async-
raising handlers, SIGCHLD `SIG_IGN`, and reset return/readback disagreement. Isolated cases install
actual `SA_NOCLDWAIT`, prove unreset ECHILD, prove reset restores wait ownership, and force persistent
post-reset auto-reap to reject ECHILD. They also prove slot identity precedes base child creation so no partial/returned child is
unowned. GitHub cases cover
same normalized-path alias, hardlink/device-inode alias, mount/bind identity disagreement, partial or
pre-authentication-replaced output publication, and every fresh rollback failure. Together they prove
reused fds are untouched, every independently owned handle receives at most one close attempt, no
numeric PID/PGID operation follows the consuming wait, no forbidden Popen method, named packet,
content-opening cleanup, or recursive pathname cleanup exists, and no uncertain cleanup is accepted
as PASS. Post-authentication same-UID interposition is outside these cooperating-process failure-
injection proofs.

#### R-AUTH-MAP: private-root cleanup traceability

This documentation-only allocation maps the existing R-family inventory to the frozen
generation-6 obligations; it adds no requirement, runtime wiring, fixture, gate, or execution
evidence. Its source baseline is candidate `735e3db150aaa87f739b12c71674532b241ec365`, under
the unchanged TASK-064 contract digest
`ec89a1df740805cc9b43e6f2530e940c0bf9b66e8f25ed878d3207d091c4bcb8`. The controlling basis is
acceptance gate 18's [private-root authentication/classification and traversal bounds](https://github.com/Adampov/WEALTH/blob/735e3db150aaa87f739b12c71674532b241ec365/BACKLOG.md#L547-L578),
[one-attempt descriptor close rule](https://github.com/Adampov/WEALTH/blob/735e3db150aaa87f739b12c71674532b241ec365/BACKLOG.md#L679-L685),
and [independent private-root negatives](https://github.com/Adampov/WEALTH/blob/735e3db150aaa87f739b12c71674532b241ec365/BACKLOG.md#L968-L977),
also stated in this ADR's native-CI-split section above.

R01-R12 retain the meanings of the existing
[`_GENERATION6_R_REJECTIONS` registry](https://github.com/Adampov/WEALTH/blob/735e3db150aaa87f739b12c71674532b241ec365/tests/ci_shard_runner.py#L7042-L7065).
R13-R24 are a documentation allocation of the remaining obligations, not previously implemented
case meanings: their source status remains `UNWIRED`. All R execution remains blocked:
[`_selftest_r_case`](https://github.com/Adampov/WEALTH/blob/735e3db150aaa87f739b12c71674532b241ec365/tests/ci_shard_runner.py#L22334-L22337)
raises the integration-blocked error and R is absent from the case-body dispatch. Every row below
is **not executed by this mapping step**. A registry entry, inert namespace-journal scaffold, or
green ordinary test run is not evidence that its R driver ran.

For every row the required cleanup result is `FAIL` with `residue_count > 0`, including conservative
uncertainty accounting, and no PASS publication. The journal column describes the operation and
observation that a future isolated driver must account for, not a new API or a claim that the
current scaffolds implement teardown. Each detected replacement, non-removable entry, or unvisited
remainder is preserved at the failed-cleanup boundary; a missing name is not falsely reported as
an observed surviving entry. Every independently owned handle still receives its one permitted
detach/poison/close attempt. No row permits stale-fd inspection, close retry, content-opening
classification, pathname-recursive removal, or a post-authentication hostile-race claim.

| ID | Existing obligation and controlled mutation | Journal operation or observation | Required failure/residue observation |
|---|---|---|---|
| R01 | Retained parent replacement before authentication. | Record retained parent identity and the substituted named parent before the subject cleanup. | Preserve the replacement and remaining private-root state; no search for the displaced original. |
| R02 | Private-root replacement before authentication. | Record the original root descriptor/name binding and the replacement under the retained parent. | Do not remove the replacement root or traverse it as the original. |
| R03 | Known child replacement before authentication. | Record the retained child binding and substituted relative name. | Preserve the observed replacement child and its contents. |
| R04 | Unexpected root entry. | Record the entry absent from the authenticated root inventory and the failed inventory check. | Preserve the unexpected entry; do not report an empty root. |
| R05 | Descriptor-close ambiguity followed by immediate numeric-fd reuse. | Record the detached/poisoned owner, its sole close attempt, and separately owned reused descriptor. | Block PASS and preserve uncertainty; neither inspect nor close the reused number through the old owner. |
| R06 | Symlink entry. | Record descriptor-relative no-follow classification. | Preserve the link and target; never follow the link. |
| R07 | Socket entry. | Record no-follow type/identity classification without a content open. | Preserve the socket entry as non-removable residue. |
| R08 | FIFO entry. | Record no-follow classification and absence of a payload open. | Preserve the FIFO; classification must not block on its contents. |
| R09 | Hard-linked regular entry. | Record regular type and link count greater than one. | Preserve all observed links; the entry is not removable. |
| R10 | Regular entry with set-UID, set-GID, or sticky bit. | Record the special-mode classification. | Preserve the entry; ordinary permission bits alone remain allowed in `0o000..0o777`. |
| R11 | Foreign mount/bind identity. | Record disagreement between the retained root mount and entry handle/relative-name mount. | Preserve the entry even when device/inode values otherwise appear acceptable. |
| R12 | Unknown entry type. | Record the unclassifiable type without opening its contents. | Preserve the entry; unknown type grants no traversal/removal authority. |
| R13 | Block-device and character-device entry observations, independently. | Record no-follow device classification and absence of payload opens. | Preserve each device entry; no device content is opened. |
| R14 | Wrong ownership or descriptor/name ownership/mode disagreement, independently. | Record the exact retained, handle, and relative-name metadata comparison. | Preserve the disagreeing entry/directory; do not weaken same-UID or exact-binding checks. |
| R15 | Missing name, unopenable entry, or handle/name identity disagreement, independently. | Record the failed no-follow stat/open/authentication boundary and any handles already acquired. | Preserve all remaining or substituted state; do not search by another name or infer successful cleanup from absence. |
| R16 | Unknown or unavailable mount identity. | Record failure to establish the exact retained Linux mount identity. | Preserve the unproven entry; no device/inode-only fallback. |
| R17 | Depth would exceed `64`, with root depth zero. | Record the next depth before its charged traversal operation. | Stop destructive work at the bound; preserve deeper/unvisited state. |
| R18 | Yielded-entry count would exceed `100_000`. | Record the next entry count and pre-operation budget rejection. | Stop destructive work; preserve the unvisited remainder. |
| R19 | Cumulative encoded component bytes would exceed `16_777_216`. | Record cumulative `os.fsencode(name)` bytes before the next charge. | Stop destructive work; preserve remaining state without ignoring name bytes. |
| R20 | Cleanup-operation count would exceed `500_000`. | Account separately for scan acquisition, yielded entry, no-follow stat, open, unlink, and rmdir attempts. | Reject the over-budget operation before acting; no later destructive work. |
| R21 | Invalid component or encoded name longer than `255` bytes, independently. | Record rejection of empty/dot/dot-dot, NUL/separator, invalid encoding, or oversized component before use. | No rejected component reaches a relative filesystem operation; remaining state is nonzero residue. |
| R22 | Absolute traversal deadline reached or exceeded. | Record the captured-clock comparison to the original `60_000_000_000` ns deadline. | Stop destructive work without refreshing the deadline; preserve remaining state. |
| R23 | Captured clock raises, returns an invalid value, or regresses, independently. | Record clock failure or a value below the prior accepted exact nonnegative integer. | Stop destructive work; no fallback clock or successful cleanup inference. |
| R24 | An entry remains after attempted cleanup, including a failed authenticated unlink/rmdir. | Record the attempted relative removal and subsequent nonempty/unproven inventory at the root-removal gate. | Do not remove the root without authenticated empty inventory; preserve the remainder and fail. |

Rows grouping alternatives require independent observations for those alternatives; the 24 IDs do
not replace the frozen obligations with a smaller set of checks. Failure observations precede any
separately authorized fixture teardown or rollback, which cannot turn the subject's failure into
PASS or erase its residue evidence. Such teardown must itself prove ownership and its final
disposition; zero fixture residue is not established by this table. GitHub command-file
publication and fresh-descriptor rollback remain the separate C-family obligations, not R cases.
The future driver, source-headroom work, reproducible static-gate builder, exact-candidate runtime
evidence, and independent implementation reviews remain outstanding. This allocation grants no
authority to change the contract, source gates, case registry, immutable fixtures, node manifest,
financial controls, or production boundaries.

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

The CI runner's anonymous observation files eliminate its prior named-packet replacement/deletion
claim, and retained directory descriptors prevent cleanup from switching to a mismatched root name.
They do not make relative name authentication and removal atomic against a hostile same-UID process,
nor do pidfd/process-group evidence create a production sandbox. The runner therefore preserves a
detected replacement or residue and fails; it claims only correct cleanup among cooperating
same-UID pytest processes on the required Linux/ext4 acceptance host.

For report-close children, the abstract `AF_UNIX` `SOCK_SEQPACKET` exchange uses kernel peer and
message credentials to bind the real parent PID, the exact registered `Popen` child PID, same UID,
packet digest, target node, mode, and bounded handshake/shared deadlines before artifact access.
The parent commits the one-shot attestation state before emitting `ATTESTED`; a response-send or
socket-close ambiguity cannot reopen it and latches authority uncertainty. A missing or malformed
`ATTESTED` response or required EOF fails the child and batch; when the parent has already
committed, that one-shot state is never reopened. A destructive publication close or root-revocation
ambiguity terminalizes the child-local permit, latches cleanup uncertainty, and blocks replay and
later fork/publication. Unprovable marker, descriptor, process, pipe, or bytecode-cache cleanup
fails the batch and boundedly terminates and reaps remaining children. These kernel checks
strengthen only the controlled cooperating-process protocol; they do not resolve same-interpreter
authority mutation, hostile same-UID pathname races, target/VFS isolation, or production runtime
ownership.

## Consequences

Positive:

- The ADR 0031 physical design now has one executable, fingerprinted schema.
- Exact TASK-062 boundary semantics are exercised without adding a runtime adapter.
- Tail/history ownership, corruption classification, row bounds, crash seams, backup, restore, and
  same-format copy have deterministic generated-data proofs.
- Golden schema evidence cannot self-bless during ordinary tests.
- Unchanged report semantics avoid one repeated live-store validation, and the full test suite fits
  the bounded CI window without putting raw-fork tests inside threaded workers.
- The live epoch now models SQLite's observed SHM timestamp behavior narrowly while retaining exact
  database/WAL observations, SHM identity/content, and post-validation publication binding.

Costs and residuals:

- The schema is intentionally redundant because bounded validation requires exact creation,
  current, and predecessor witnesses.
- The harness is tied to one Python/SQLite source and compile tuple.
- `ptrace` and `RLIMIT_FSIZE` evidence is platform-specific and must pass on the exact CI target.
- Anonymous packet transport, pidfd/WNOWAIT lifecycle evidence, and fd-relative cleanup are
  Linux/ext4 acceptance-host requirements; no named-packet or portable fallback is authorized.
- Hostile same-UID pathname races and target/VFS isolation remain outside the controlled pytest
  threat model and independently block production.
- Test filesystem results do not establish target filesystem, device, power-loss, capacity,
  latency, backup-destination, retention, monitoring, or recovery behavior.
- The compiled but unreachable FTS surface remains a production blocker pending a separately
  governed current runtime.
- The frozen node manifest and hash partition must be reviewed whenever collection changes; static
  shard-split jobs are visible in addition to the stable final required status.

## Verification and Completion Gate

TASK-064 remains the current task until the exact immutable candidate:

1. passes formatting, lint, strict typing, one unchanged full serial correctness-parity run with a
   2,100-second deadline, two complete Linux/ext4 five-job shard-split attempts, the three exact
   Linux/ext4 report-proof modes, lockfile verification, dependency audit, health slice, and
   final-diff inspection with all 2,298 nodes accounted for and each shard/proof execution at most
   720 seconds; both ext4 report shards must exercise the exact positive and negative generation-6
   timestamp regressions, each positive run must observe a real ordinary-live-validation SHM
   timestamp change; every fresh shard/proof invocation must use zero-link parent-created
   `O_TMPFILE` observations, the exact `pass_fds` tuple, pidfd/WNOWAIT anchoring, one consuming reap,
   fd-relative zero-residue cleanup, and one-attempt poisoned close; and every descriptor-reuse,
   packet-I/O, root-replacement, process-lifecycle, and fresh-only `GITHUB_OUTPUT` rollback negative
   must execute with no named/fallback path and no discarded, skipped, or substituted branch;
2. has independent Engineering, Security/Risk, and QA approvals bound to the same generation-6
   digest, candidate head/tree, full/shard manifests, serial evidence, both five-job shard-split
   attempts, report-proof results, runner cleanup/failure-injection evidence, and final CI evidence;
3. is published as a draft pull request with all in-scope evidence `PASS`;
4. receives exact owner merge authorization naming that pull request and its current head SHA;
5. merges without changing the authorized head; and
6. passes required target-branch CI before a separate completion-governance transition.

Until then, no production adapter or later incompatible-migration task is unblocked. TASK-037
remains blocked and authorization remains denied.

## Rollback

Rollback is a revert of the exact generation-6 candidate. It removes only the bounded report-prime
changes, inert observer, CI runner/workflow changes, tests, and contract amendment, restoring the
functionally passing generation-3 implementation. Pytest cleanup removes only exact harness-owned
generated artifacts. Rollback never opens, repairs, migrates, routes, truncates, or deletes an
operator database and changes no production runtime.

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
  copy, workload matrix, thresholds, report fields, dispositions, prime lifetime/bindings/state,
  node-manifest serialization/digest, partition function, shard identity, outcome accounting,
  timeout, observation transport, descriptor-close semantics, command-file rollback, private-root
  cleanup, pidfd/process-group lifecycle, or workflow job graph; or
- any production import, adapter, runtime, operator path/data, migration, retention, deployment,
  durability, capacity, RPO/RTO, readiness, or trading capability.
