# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; blocked work remains open without authority, and later items are directional until
promoted through review.

## Next Action

### TASK-064 — Test-only continuous public-trade SQLite schema and evidence harness

- **Key:** `phase2.continuous_public_trade_stream_sqlite_schema_evidence_harness`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Contract generation:** 5 | STATUS=FROZEN | SHA256=ba258296d4ffde716dc6ec02bbf606ca3f08cb48258dfca8061ca597f9e649e2
- **Contract normalization:** This item is not executable until every independent contract review
  is `PASS` and the generation line records `STATUS=FROZEN` plus its recomputed normalized SHA-256.
  The normalized digest input is the UTF-8 TASK-064 section from its first level-three heading
  whose heading text begins `TASK-064` through the byte before the next level-three heading. Convert
  CRLF and CR to LF; require exactly one generation line matching ASCII
  `^- \*\*Contract generation:\*\* 5 \| STATUS=(DRAFT|FROZEN) \| SHA256=(PENDING|[0-9a-f]{64})$`;
  replace only its two captured mutable values with the literal tokens `<STATUS>` and `<SHA256>`;
  remove trailing ASCII space and tab from every line; remove leading and trailing blank lines; and
  append exactly one LF byte. Every other byte of this section, including this algorithm and the
  complete generation history, remains in the digest preimage.
- **Generation history:** Generation 4, normalized SHA-256
  `b079966273ef43d726ecc7aa64693189e7234a94397e965e334fe215eac60aa4`, frozen at
  documentation commit `1324b618ada5f84efd41ba632eb7ab2ab74d8ab8`, is `SUPERSEDED`
  before implementation. Independent compatibility and security review found that it omitted the
  exact immutable `schema_descriptor.json` and `schema_fingerprint.txt` identities and depended on
  under-specified compact-worker, prefetch, sandbox, cgroup, and benchmark protocols that were not
  available in the accepted environment. Every generation-4 implementation lease was revoked
  without a result commit; no generation-4 implementation write is an integration source.
  Generation 3, normalized SHA-256
  `86e3650608f2f1c96a9aa272b2b9cd597bc3d5ac188a39937afb974536d11ccb`, remains the
  functional base at exact candidate `9ff70a8aa34e5bf154679957c913bb21e93a3d5e`: all 2,298
  tests passed, but the full suite required 1,587.74 seconds and the isolated report node required
  780.69 seconds, exceeding the unchanged 900-second job and 720-second test-execution ceilings.
  Generation 5 starts only from that generation-3 candidate. Disposable report and shard-split
  prototypes are non-integrable measurement evidence until independently rebound to this exact
  generation-5 contract. Generation 1, normalized SHA-256
  `2ba9d4d70bde04c5225649d1c3e4f70e86b5085c46a370ffcfbe716887bef836`, was
  `SUPERSEDED` before writable activation because its SQL `INTEGER` projection constraint
  contradicted accepted ADR-0031. Generation 2, normalized SHA-256
  `5c48f313870bc729b3e8fcc66777df086c3bd9cde4e3818703aed285ad3570dc`, was
  `SUPERSEDED` during writable activation, before any result commit, after review proved that a
  plain `Path` cannot authenticate pytest provenance and the standard-library SQLite binding
  cannot descriptor-pin SQLite main/WAL/SHM opens. Earlier outputs are evidence only and cannot
  count as generation-5 acceptance unless revalidated and rebound to generation 5.
- **Human approval:** NOT REQUIRED for the bounded test-only implementation, verification, review,
  and draft publication. Exact owner approval naming the pull request and current head commit
  remains required before merge.
- **Context:** TASK-063 is complete after owner-approved PR #66, accepted head
  `8def515c29f6b778540e1ac2d6c55b0006c59b1b`, merge commit
  `cf5f69c818185c54cb1e4c701bf6e390cdf96237`, and successful required target-branch CI run
  `30401016909`. ADR-0031 selects a dedicated local SQLite generation and freezes a non-executable
  physical descriptor plus fail-closed schema, transaction, crash, bounded-query, backup/restore,
  migration, retention, and capacity evidence requirements. No executable schema, database,
  adapter, path, or runtime exists. This TASK-064 contract becomes executable only after the
  TASK-063 completion-governance pull request itself is merged and its required target-branch CI
  succeeds; the governance transition creates no ADR-0032, schema, harness, database, or TASK-064
  active component.
- **Goal:** Freeze one exact executable version-one SQLite schema and build an isolated,
  generated-data evidence harness that proves the bounded physical design and its failure
  classifications without creating a production adapter or operational path.
- **Scope:** Add ADR-0032; exact version-one DDL, ordered schema descriptor, canonical descriptor
  fingerprint, and generated fixtures; and test-only bootstrap/open, transaction, validation,
  corruption, subprocess crash, bounded-query, online-backup/restore, same-format generation-copy,
  and finite synthetic-workload evidence helpers. Exercise only generated non-operator databases
  beneath pytest temporary directories from the two exact repository-owned TASK-064 test modules.
  The generated evidence assumes a controlled pytest process and cooperating same-UID processes;
  hostile same-UID replacement, target-host isolation, and descriptor-capable VFS evidence are
  explicitly outside this test-only task and remain production blockers. Coordinate documentation
  and governance without importing the harness from production source. Generation 5 preserves all
  generation-3 schema, transaction, fault, query, backup, copy, authority, and publication
  semantics. It adds one optional bounded report-validation prime that avoids repeating unchanged
  live-store validation in the normal parent, and replaces the single monolithic CI test step with
  five independent ordinary-pytest jobs: one exact report job and four deterministic remainder
  shards. It adds no in-process test worker, scheduler, retry, or production surface.
- **Files:** Generation-5 changes are restricted to `BACKLOG.md`,
  `docs/decisions/0032-continuous-public-trade-stream-sqlite-schema-evidence-harness.md`,
  `.github/workflows/ci.yml`, `tests/conftest.py`, `tests/ci_shard_runner.py`,
  `tests/support/continuous_public_trade_stream_sqlite_harness.py`,
  `tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py`, and the
  narrow generation/digest, report-prime, shard-split, and workflow assertions in
  `tests/unit/test_project_state.py`. The exact version-one fixtures
  `schema.sql`, `schema_descriptor.json`, and `schema_fingerprint.txt`, every production file,
  `pyproject.toml`, `uv.lock`, and every other file remain byte-identical to generation 3.
  Their exact raw SHA-256 values are respectively
  `1263b831a3e73bfc730beef6df7df48fce0e3654aa806672650de9f40b8a3e37`,
  `bb33dc9cb549be484c5dc7855abace6d851682e50883e51919a9169cdaae431a`, and
  `8a2508de6e018c67e9b18393cb0a5517cef3bea5df785bf432299a49a2a12ef8`; the fingerprint
  file's exact single LF-terminated value is
  `sha256:0410c1f08390a411c73427b3d07c542f3d1828def7c6adebab51cd57375355b3`.
- **Constraints:** Test-only generated data and evidence. No `src/` adapter, repository port
  change, production fake, runtime import or composition, operator path or data, pre-existing or
  non-harness-owned database, network/provider/account access, domain/runtime wall-clock sampling
  or time authority, accepted-attestation access, domain/runtime outer fence or lease,
  request-budget use, retry/recovery action, repair, routing or cutover, semantic history/source
  deletion or compaction, credential, permission, notification, dependency or lockfile change,
  deployment, operational capacity, durability, recovery/readiness, operating-mode, Phase 2
  completion, or risk-closure claim. `pytest-xdist`, `execnet`, and every other in-process or
  threaded worker plugin are prohibited: the raw-fork TASK-064 evidence remains inside ordinary,
  single-process pytest parents, and concurrency exists only between separate GitHub-hosted jobs.
  No shard may retry, restart, batch, or silently omit a selected node. Every operated database
  must have been created by the same
  test bootstrap beneath an actively registered pytest temporary root from one of the two exact
  allowed TASK-064 test modules. A plain path grants no authority: the harness requires a
  fixture-scoped process-local registration bound to the exact pytest node, path object, PID,
  resolved root, device, inode, UID, mode, and a random nonce, and revokes it when the fixture
  exits. The harness opens the root and generation through validated directory descriptors,
  creates through `openat`-style relative operations, rejects every observed main/WAL/SHM alias or
  unexpected entry before and after open, and keeps the generation descriptor pinned for the
  connection lifetime. This controlled-test evidence does not claim protection against a hostile
  same-UID process racing the standard-library SQLite pathname open; that residual is recorded as
  target/deployment `NOT_APPLICABLE` evidence and independently blocks production use. Test-only
  monotonic timing, an injected or externally recorded evidence timestamp, and cleanup of
  pytest-owned temporary artifacts are allowed only for the declared evidence. Production code
  must never import the harness; test support may import only standard-library modules and the
  frozen pure TASK-061/062 types and validators. The generation-5 CI runner, its inert-by-default
  `tests/conftest.py` observation hooks, and the exact report-proof fixture-finalizer observation
  are the sole exception: they may use the already locked pytest API only to collect IDs and
  observe outcomes/proof scalars, never to select, deselect, reorder, schedule, retry, or change
  pytest configuration/selection/outcomes, and never to add a thread, child, or network action. No
  extension loading, `ATTACH`,
  `writable_schema`,
  caller-supplied SQL, UDF or collation dependency, shared cache, or caller-controlled URI-option
  injection. Operation opens may use only a correctly encoded, harness-internally constructed
  `file:` URI for the descriptor-pinned generation and already validated database with the sole
  fixed option `mode=rw`; callers may supply neither a URI nor URI/query options. This narrow
  construction prevents missing-file creation and does not relax the ban on URI-option injection.
  Do not alter TASK-059 behavior, TASK-061 bytes or digest domains, TASK-062 port semantics, or
  ADR-0031.

  SQL `INTEGER` columns are permitted only for (a) schema-local singleton and internal row keys,
  internal foreign keys, and physical-format, schema-generation, natural-key-version, and page-size
  markers, and (b) these non-authoritative ADR-0031 logical projections:
  `stream_contract_version`, `stream_start_epoch_ms`, current/successor causal versions,
  `prior_version` (SQL `NULL` only for creation), integer `serialization_version`, and policy
  `window_size_ms`, `settlement_lag_ms`, `max_catchup_span_ms`, `max_jobs_per_invocation`,
  `max_requests_per_job`, and `max_records_per_job`. Every logical `INTEGER` projection accepts and
  binds only an exact built-in `int`; `bool` and subclasses are rejected. BLOB columns carry the
  storage marker and schema fingerprint, UUID, reversible natural-identity key, policy
  `schema_version` and fingerprint, entry kind, record `model_version`, authoritative canonical
  record/envelope/witness bytes, and exact digests and roots. Natural-identity atoms have no
  separate columns. `cursor_epoch_ms`, optional attachment `window_start_epoch_ms` and
  `window_end_epoch_ms`, `recorded_at`, and every other unlisted TASK-061 value have no scalar
  columns and remain only within authoritative TASK-061 BLOBs. No logical `REAL` or `TEXT`
  projection is allowed.

  Schema, descriptor, fingerprint, and report-contract fixtures are text only: no `.db`, `.sqlite`,
  `.sqlite3`, `-wal`, `-shm`, or generated record fixture may be committed. Preserve TASK-037 denial
  and Stage 3. Every one of the existing 2,298 pytest node IDs, names, parameter IDs, assertions,
  and default plain-`uv run pytest` behavior remains present; generation 5 adds no collected node,
  skip, xfail, xpass, deselection, or weakened timeout.
- **Research boundary:** Read-only official SQLite documentation, vulnerability, release, source-ID,
  and application-ID registry research may be cited as review evidence. The harness and generated
  tests perform no network or provider I/O.
- **Rollback:** Failure, rejection, or revert leaves production source/runtime and every
  pre-existing database untouched, preserves the recorded generated-evidence disposition, and
  removes only harness-owned pytest temporary artifacts. Rollback is a revert of the exact
  TASK-064 candidate; it is never database repair, migration, routing, or cutover.

Acceptance gates:

1. ADR-0032 freezes the exact non-colliding `application_id`, executable DDL, ordered descriptor
   canonicalization, domain-separated golden SHA-256 fingerprint, object inventory, constraints,
   indexes, triggers, bootstrap/open contract, and closed SQLite extended-result-code matrix.
2. Only an explicit test bootstrap under the active fixture-scoped registration may create a
   database, and only beneath the exact registered pytest temporary directory. Unregistered,
   reconstructed, sibling, nested, expired, wrong-node, wrong-PID, replaced, or aliased roots are
   rejected before mutation. Every operation open requires the registered existing regular
   database, uses the pinned generation descriptor, and rejects an observed main/WAL/SHM alias,
   hard link, non-regular file, replacement, or unexpected entry before and after open. Generated
   PASS evidence is limited to this controlled pytest/cooperating-process threat model; hostile
   same-UID TOCTOU and target-host/VFS isolation are not claimed and remain production blockers.
3. Every coherent operation verifies exact `application_id`, `user_version`, metadata marker,
   schema fingerprint, UTF-8 encoding, 4,096-byte pages, WAL, `synchronous=FULL` writers, foreign
   keys on, shared cache never enabled, `read_uncommitted` off, `trusted_schema` off, busy timeout
   zero with no busy-handler retry, `auto_vacuum=NONE`, exact patched SQLite source ID, compile
   options, and connection limits. `THREADSAFE=0`, `OMIT_FOREIGN_KEY`, and `OMIT_TRIGGER` are
   rejected; extension loading is disabled; and `SQLITE_DBCONFIG_DEFENSIVE` is enabled when the
   accepted binding exposes it. Readers use an explicit finite transaction with `query_only=ON`
   and close every cursor before return. The packet records defensive-mode availability and never
   treats an unavailable control as passing; missing or mismatched required settings fail closed.
4. Generated boundary records prove `stream_start_epoch_ms` over exact `0` through `2**63 - 1`,
   causal versions over exact `1` through `2**63 - 1`, and an optional prior version that is SQL
   `NULL` only for creation. They round-trip authoritative TASK-061 BLOBs, reversible UUID/natural
   keys, digests, roots, and policy projections. Booleans, scalar subclasses, alternate encodings,
   and any projection/byte disagreement are rejected. Current cursor and attachment-window epochs
   have no scalar columns.
5. Executable constraints and hostile tests prove deferred creation/current-tail/predecessor
   bindings, immutable history and witnesses, UUID and natural-identity uniqueness, exact
   byte-equality guards, and rejection of normal update/delete, orphan, gap, alternate-successor,
   short-page, overflow, and unsupported-generation states.
6. Generated create and compare-and-swap prototypes each use one explicit `BEGIN IMMEDIATE`
   transaction and prove atomic current/history ownership, one winner under two writers, exact
   duplicate versus conflict classification, unknown-acknowledgement reload, and no upsert,
   replacement, repair, implicit transaction, or hidden retry.
7. Fresh-process evidence covers every ADR-0031 named seam: before transaction; after lock; between
   stream insert and creation insert; between creation insert and create commit; between transition
   insert and current update; between current update and compare-and-swap commit; true during
   commit; after commit before acknowledgement; writer/checkpointer concurrency; disk full or
   `max_page_count`; readonly; one-attempt busy contention; and injected I/O failure. Because
   shared cache and schema DDL are prohibited, raw `SQLITE_LOCKED` is unreachable on the compliant
   operation path and must not be manufactured by disabling controls; the closed numeric matrix
   proves every `SQLITE_LOCKED*` code maps fail-closed to sanitized `UNAVAILABLE`. A new-connection reopen
   verifies application/format/schema identity, `integrity_check`, empty `foreign_key_check`,
   bounded current state, and complete paginated history/root state, and may establish only exact
   old, new, duplicate, or unavailable state. No mocked before-commit or after-commit substitute
   counts as true during-commit evidence. Results use only `PASS`, `FAIL`, `UNPROVEN`, or
   `NOT_APPLICABLE`. Every in-scope declared seam must be `PASS`; `FAIL`, `UNPROVEN`, skipped, or
   xfailed evidence blocks TASK-064 completion and every adapter/runtime proposal.
   `NOT_APPLICABLE` is allowed only for named target/deployment evidence outside this task, includes
   a reason, and never implies a pass.
8. Exact query-plan, statement-count, and row-materialization evidence proves a current load reads
   one stream row and at most three distinct history rows, historical duplicate classification
   materializes at most five rows, and a two-row identity-conflict path plus every failure path has
   an explicit finite budget. Initial audit returns exactly 1 through the requested limit new rows
   with limit at most 100; continuation returns exactly one overlap plus 0 through the requested
   limit new rows with at most 101 total; `AT_TAIL` returns exactly one overlap and zero new rows.
   Query-plan evidence is pinned at limits 1 and 100. No count, lookahead, offset, unbounded
   iteration, hidden history query, or extra row is permitted.
9. Every retained value is validated before classification. Deterministic SQLite extended result
   codes map to the frozen closed outcomes; an unmapped operational failure is sanitized
   `UNAVAILABLE`, while a malformed claimed version-one generation or retained value is `CORRUPT`.
10. Generated concurrent-write backup evidence uses SQLite Online Backup into a fresh destination,
    closes and manifests the exact complete required file set, reruns schema, integrity,
    foreign-key, bounded-current, and full paginated audit checks, and completes an isolated restore
    drill. Raw copying and a main-file-only digest while WAL state may be required are rejected.
11. Same-format generation-copy evidence copies one version-one generated source into a separate
    empty version-one generation, revalidates every byte, value, digest, root, identity, count, and
    tail, and leaves the source authoritative. This is a same-format rehearsal only and never
    incompatible migration, cutover, rollback, routing-marker, reverse-converter, semantic
    deletion, compaction, or production migration proof or authority. All ADR-0031
    incompatible-generation migration, shadow-read, cutover, routing-marker, reverse-converter,
    rollback, and production-migration evidence is explicitly deferred to a separately frozen later
    task.
12. Before measurement, ADR-0032 freezes the numeric pass thresholds, run count, seed,
    minimum/typical/maximum record-size and workload/concurrency matrix, test-local
    `max_page_count` and checkpoint parameters, long-reader WAL-starvation/growth case, and memory
    and open-cursor bounds. Reports record the exact contract digest, runtime/source ID, compile
    options, exact observed reader and writer DBCONFIG/limit/PRAGMA profiles, sanitized environment
    class, row/query counts, database/WAL/page/freelist sizes, measured Python DB-API cursor bounds,
    latency samples, complete backup-manifest bindings and injected fixed-UTC evidence times, and
    the four-state disposition for every gate. Generated
    JSON reports exist only beneath pytest temporary directories. The durable evidence channel is
    the draft PR plus exact CI check/run logs bound after publication to the immutable candidate
    head SHA; it records no user path, host/user name, device serial, credential, or operator
    identifier. A committed report must not claim to self-bind its own commit. These test
    measurements are not target-filesystem, capacity, durability, RPO/RTO, or readiness evidence.
13. No existing validation is removed, skipped, weakened, or replaced. Formatting, lint, strict
    typing, complete tests, lockfile verification, dependency audit, health slice, and final-diff
    inspection pass. Before material work, the merged base, contract generation, and normalized
    contract digest are recorded with a contemporaneous writable lease ID, issue/expiry,
    `ASSIGNED` to `ACTIVE` to `RETURNED` timestamps, and exact result-commit binding; no
    retrospective packet is valid. Independent Engineering, Security/Risk, and QA reviews bind the
    same exact committed SHA, contract generation/digest, and CI evidence.
14. A production adapter remains blocked until separately approved target path/filesystem,
    permissions, SQLite source ID, sync/locking behavior, checkpoint/WAL bounds, capacity and
    latency envelope, backup destination, retention/disposal, RPO/RTO, restore cadence, monitoring,
    stop thresholds, and operational evidence all pass without expanding this task.
15. Generation 5 adds exactly one optional test-only API:
    `prime_evidence_report_validation(pytest_root: Path, *, receipt: _EvidenceReceipt,
    report: EvidenceReport) -> None`. An explicit prime performs the complete generation-3
    receipt, root, report-shape, report-value, canonical-byte, live-source, gate, backup-manifest,
    and cleanup validation. It creates no stage or final file, consumes or terminalizes nothing,
    grants no publication capability, returns exactly `None`, and leaves the receipt sealed.
    Priming is never implicit. When no cache entry exists, `write_evidence_report` follows the
    complete unchanged generation-3 path and can publish successfully; it must not silently prime.
16. The prime may retain exactly one typed, closure-owned, nonserializable entry for one exact
    receipt, with at most one active publication attempt and an absolute lifetime of exactly
    `120_000_000_000` monotonic nanoseconds. The entry binds by
    exact object identity the original pytest-root `Path`, active registration, evidence run,
    ledger, receipt, receipt-owned evidence, and source report; exact PID, thread ID, pytest node,
    and active root-session context; generation 5 and this digest; schema fingerprint; receipt
    evidence digest; source fingerprints; canonical report bytes, length, and SHA-256; and the
    report-core digest
    `_evidence_payload_digest(("TASK064-REPORT-CORE-V1", fields, evidence_digest))`, where `fields`
    is the exact tuple of the 40 non-evidence `EvidenceReport` values in dataclass declaration
    order; and the live-validation digest
    `_evidence_payload_digest(("TASK064-REPORT-LIVE-VALIDATION-V1", observations))`, where
    `observations` is the four ordinal-ordered tuples
    `(ordinal, verification_summary, tails, current_projection_or_none)` and only ordinal zero
    carries the freshly loaded current projection. No object ID, serialized value, caller value,
    path string, or child can create, select, inherit, or revive an entry.

    The live epoch domain is `TASK064-REPORT-LIVE-EPOCH-V1`. Its exact role order is `bootstrap`,
    `backup_source`, `backup`, `restore`, `concurrent_source`, `concurrent_backup`,
    `generation_source`, `generation_destination`, with exact token-identity alias vector
    `[0,1,0,2,1,0,0,3]` and exactly four distinct token objects. Its canonical preimage is
    `json.dumps(payload, allow_nan=False, ensure_ascii=True, sort_keys=True,
    separators=(",", ":")).encode("ascii")`, with no terminal LF. `payload` has exact keys
    `domain`, `roles`, and `entries`; `roles` is the eight `[role, ordinal]` pairs above; and
    `entries` is four objects in ordinal order. Each entry has exact keys `ordinal`,
    `generation_id`, `registration`, `summary_sha256`, `tails_sha256`, and `files`.
    `summary_sha256` and `tails_sha256` are the lowercase results of
    `_evidence_payload_digest` over the freshly verified `VerificationSummary` and exact ordered
    tail tuple. `registration` has exact arrays `pytest_root=[device,inode,uid,mode,process_id,
    node_id]`, `generation=[device,inode,uid,mode]`, and
    `database=[device,inode,uid,mode,link_count]`. Token, registration, pytest-root,
    generation-root, and database-path object identities remain outside the digest and must also
    match the retained entry exactly.

    Every entry's `files` has exactly three objects in fixed order `store.sqlite3`,
    `store.sqlite3-wal`, `store.sqlite3-shm`. Each has exact keys `name`, `present`, `device`,
    `inode`, `uid`, `mode`, `link_count`, `size`, `mtime_ns`, `ctime_ns`, and `sha256`. An absent
    file sets `present=false` and every later field to JSON `null`. A present file sets
    `present=true`, records exact nonnegative integer metadata, requires a regular same-UID
    descriptor and path to agree, mode `0o600`, one link, and records
    `sha256:` plus 64 lowercase hex over the complete raw bytes. Reads use 65,536-byte blocks;
    `store.sqlite3` is bounded by 16 MiB and WAL/SHM by 2 MiB each. A generation contains no entry
    outside those three names.

    Prime captures one descriptor-validated file snapshot for each unique token, performs the
    complete live validation exactly once to obtain and validate the four summaries and tails, and
    captures the file snapshots again. The pre/post identity, presence, metadata, size, and raw
    digest vectors must be identical; every present descriptor has stable before/after metadata,
    a bounded complete read, and an explicit trailing EOF read. A cached writer reopens and rechecks every registration and
    complete file snapshot at writer entry and immediately before link; equal immutable raw file
    bytes preserve the primed summary/tail observation without a second SQLite validation.
    Type, report, schema, alias, registration, generation-ID, summary, tail, or canonical-value
    disagreement maps to sanitized `CORRUPT`. Open/read/stat/close failure, unexpected inventory,
    descriptor/path disagreement, filesystem drift, or cleanup uncertainty maps to sanitized
    `UNAVAILABLE`; no raw exception or path escapes.

    The sole states are `EMPTY -> VALIDATING -> READY -> PUBLISHING`. The builder captures the
    exact original `time.monotonic_ns` callable. Every clock result must be an exact nonnegative
    built-in integer at least the prior accepted result; exception, invalid value, or regression
    clears and fails `UNAVAILABLE`. Prime is legal only from `EMPTY`, installs
    `VALIDATING` before any validation, and on every validation failure clears all references and
    restores `EMPTY` while preserving the underlying sanitized failure code. Only after the equal
    post snapshot succeeds does it call the captured clock, require an exact nonnegative built-in
    integer, set `issued_ns` to that value, checked-add `120_000_000_000`, and enter `READY`.
    An entry is live exactly while a fresh captured-clock value is strictly less than
    `expires_ns`; equality is expired. Expiry clears the entry and makes that call fail
    `CORRUPT` without consuming the receipt; only a later explicit prime may create a new entry. A duplicate exact prime
    while `READY` fails `CORRUPT` without refreshing or changing the entry. A call supplying a
    different root, run, ledger, receipt, evidence, report, PID, thread, node, or context fails
    `CORRUPT` without selecting, altering, or falling back around an otherwise valid `READY`
    entry. Drift observed through the exact owner objects, consumption, source/root mutation, or
    deadline failure clears and fails closed. `os.register_at_fork(after_in_child=...)` clears only
    the inherited child copy; it cannot affect or revive the parent's entry.

    Reentrant prime or writer entry during `VALIDATING` or `PUBLISHING` latches process cleanup
    uncertainty, clears every retained reference, and makes both inner and outer operations fail
    `UNAVAILABLE`. A matching writer changes `READY -> PUBLISHING` before its first receipt
    checkpoint. The existing receipt checkpoints remain exactly C0 writer entry, C1 after semantic
    validation/canonical serialization, C2 publisher entry, and C3 after stage
    write/fsync/stat/close immediately before link; no fifth checkpoint is added. A proven clean
    rollback with no remaining publication may return `PUBLISHING -> READY` only when every bound
    object, digest, source fingerprint, root/generation/schema binding, original absolute deadline,
    and complete live epoch still matches and cleanup is certain; rollback never refreshes the
    deadline. Before-link failure and link-then-fail qualify only when the exact owned staged inode
    is absent from both names and the directory is synced. Collision qualifies only when that
    owned staged inode is absent from both names and the pre-existing foreign final entry is proven
    byte-for-byte and identity-unchanged. Success, any failure after readback begins, ambiguous
    cleanup, any remaining publication, terminal receipt state, or root teardown clears every
    retained reference and returns to `EMPTY`.

    Two private proof-only functions expose no authority:
    `_observe_task064_report_validation_for_test() -> tuple[str, bool, int | None, int | None]`
    returns only state, entry-presence, issued nanoseconds, and expiry nanoseconds, and
    `_force_task064_report_validation_expiry_for_test() -> None` may only shorten the exact owner's
    `READY` deadline to the current captured-clock value; it can never extend, restore, or select
    an entry. The report node has inert-by-default proof options
    `--task064-report-proof-mode`, `--task064-report-proof-nonce=<64 lowercase hex>`, and
    `--task064-report-proof-observation=<absolute private path>`. All three or none are required;
    the only modes are `primed-full`, `unprimed-success`, `expired-entry`, and `ready-teardown`,
    and an active mode is legal only when that exact report node is the sole selected node.
    `primed-full` follows the complete normal test and primed success; `unprimed-success` branches
    immediately after the sealed receipt to the unchanged successful unprimed writer;
    `expired-entry` primes once, forces equality expiry, proves writer rejection and `EMPTY`; and
    `ready-teardown` primes once and returns with `READY`. The exact node records a closure-owned
    pending scalar proof but never writes the observation. The authenticated fixture finalizer
    requires that pending record, captures its asserted state, always invokes the captured cache
    teardown before root-scope exit, evidence-root revocation, context reset, or child-provenance
    completion, clears references first, proves `EMPTY`, and only then exclusively publishes the
    nonce/PID-bound proof observation. It latches cleanup uncertainty and emits no PASS observation
    on any callback, state, publication, or close failure. Thus a zero-exit, validated
    `ready-teardown` observation proves finalizer cleanup. These modes add no pytest node or
    parameter ID. Because separately
    generated evidence has intentionally unique generation identities, cross-process report
    digests need not match. Instead, each route proves against its own exact input report: the
    primed route caches precisely the captured pure serializer result and publishes those bytes
    unchanged, while the unprimed route freshly invokes that same captured serializer and
    publishes its returned bytes unchanged. Both prove identical one-shot publication semantics.
17. The frozen full-node manifest is exactly 2,298 unique, nonempty ASCII node IDs. Collection
    rejects non-`str`, NUL, CR, LF, surrogate, duplicate, unknown, or empty values, sorts by raw
    ASCII bytes, and serializes with Python `json.dumps` insertion order `domain`, then `nodes`,
    `ensure_ascii=True`, `separators=(",", ":")`, no key sorting, and no terminal LF:
    `{"domain":"TASK064-NODE-MANIFEST-V1","nodes":[...sorted node IDs...]}`. The result is exactly
    296,078 bytes with SHA-256
    `96a15ecb6af6469f6da82bace28350163b99d48b8226d867830f7aec5816b483`.
    The exact report node
    `tests/integration/test_task_064_continuous_public_trade_stream_sqlite_evidence.py::test_finite_typical_workload_measurements_and_sanitized_report`
    occurs once and is removed once into the dedicated `report` shard. Every remaining node is
    assigned exactly to `remainder-i`, for `i` in `0..3`, by
    `int.from_bytes(SHA256(nodeid.encode("ascii")).digest()[:8], "big", signed=False) % 4`.
    Membership is hashed but execution retains original pytest collection order. The five
    nonempty shards are pairwise disjoint and their exact union is the full manifest.

    Each shard canonicalizes as ASCII JSON with insertion order `domain`,
    `full_manifest_sha256`, `shard_id`, `nodes`, compact separators, sorted nodes, and no terminal
    LF:
    `{"domain":"TASK064-NODE-SHARD-V1","full_manifest_sha256":"96a15ecb6af6469f6da82bace28350163b99d48b8226d867830f7aec5816b483","shard_id":...,"nodes":[...]}`.
    The frozen identities are:

    | shard | nodes | bytes | SHA-256 | selector bytes including NUL |
    |---|---:|---:|---|---:|
    | `report` | 1 | 302 | `f0530be0d219c64bbd1b9eb4df635dd138a345e8685172d188d02e177e488a3e` | 146 |
    | `remainder-0` | 600 | 76,664 | `9946638e834ffa44c0cd1e511c348b1f8226fc078ef79eb9e4100e770de80044` | 75,290 |
    | `remainder-1` | 563 | 72,767 | `e6814eb76767d9462ed9bfa82c85d8e7daebd7265027883290ca88842365dfd0` | 71,471 |
    | `remainder-2` | 588 | 75,756 | `8d86911d7a7cfc4f32022d68be1eaa3d6b459d5a968a462deea188df2369ed5b` | 74,394 |
    | `remainder-3` | 546 | 71,332 | `69e69516345c674cadf552202b80c392f8297b74b46328278b098b462e25b4ca` | 70,049 |

    Every selector vector is passed as a Python argv array, never shell text, `eval`, `xargs`, or
    response interpolation. Each node is at most 308 encoded bytes; each frozen vector is at most
    131,072 bytes including NUL. Every argument and environment `name=value` is ASCII or
    filesystem-encoded without NUL and is at most 131,072 bytes including its terminal NUL.
    Before each `execve`, the runner computes
    `sum(len(os.fsencode(arg))+1) + sum(len(os.fsencode(name))+1+len(os.fsencode(value))+1)
    + (argc+envc+2)*struct.calcsize("P") + 32_768`; it requires exact built-in integer
    `os.sysconf("SC_ARG_MAX")`, a positive value, and projection at most that value. Any encoding,
    per-string, selector-vector, or total-projection failure rejects before launching pytest.
18. `uv run python tests/ci_shard_runner.py --shard <shard-id>` is the sole shard command; the only
    other runner modes are `--report-proof <mode>` for the three acceptance-only report probes and
    `--aggregate-static` for the final workflow job. The workflow sets
    `TASK064_CANDIDATE_SHA` to `${{ github.event.pull_request.head.sha || github.sha }}`. Every
    commit or tree identity must be exactly 40 lowercase hexadecimal characters and resolve to the
    stated Git object. The tested checkout commit must equal both `HEAD` and `GITHUB_SHA`. On
    `pull_request`, `HEAD` has exactly two parents, `HEAD^2` equals `TASK064_CANDIDATE_SHA`, and
    that candidate is proven an ancestor of `HEAD`. On `push` and `workflow_dispatch`,
    `TASK064_CANDIDATE_SHA`, `GITHUB_SHA`, and `HEAD` are equal. Any missing object, shallow-history
    uncertainty, extra parent, malformed identity, mismatch, or failed ancestry proof fails before
    collection. The result binds both tested-checkout commit/tree and candidate commit/tree.

    Before collection the runner also proves a clean checkout; Python `3.13.14`; pytest `9.1.1`;
    exact `.python-version` bytes `3.13\n` and raw SHA-256
    `02e735b3dfe1c32833eb550b7ff8ffa17f5f2bc3fa1e7bae61a8f5a3883ce398`; and unchanged
    `uv.lock` raw SHA-256
    `86f4e40b898d8585b32f50c5db5a87d73766c6b364480e596b07768c7320f733`.
    Presence of inherited `PYTEST_ADDOPTS`, `PYTEST_PLUGINS`, `PYTEST_DISABLE_PLUGIN_AUTOLOAD`,
    `PYTHONPATH`, or `PYTHONHOME` rejects before either child. The observer rejects configured
    `addopts` unless its exact ordered value is
    `("--strict-config","--strict-markers","--import-mode=importlib")`, and separately validates
    the frozen invocation argv. `external_plugins` is exactly the sorted ASCII list
    `[["hypothesis","6.157.1","pytest11","hypothesispytest",
    "_hypothesis_pytestplugin"]]`; any other external distribution or entry point rejects. The
    exact `worker_indicator_count` counts present `config.workerinput`,
    `PYTEST_XDIST_WORKER`, `PYTEST_XDIST_WORKER_COUNT`, `PYTEST_XDIST_TESTRUNUID`, and each loaded
    xdist/execnet plugin or module indicator, and must be zero. The runner argv contains no
    worker, scheduler, rerun, timeout-plugin, coverage-worker, or plugin-injection option beyond
    built-in `-p no:cacheprovider` and the exact repository observer/proof options. Observer checkpoints
    require zero worker indicators and no surviving non-main thread. An unknown plugin,
    distribution/version/entry-point mismatch, injected option, or worker/scheduler residue fails.

    In `--shard` mode the runner first launches exactly
    `[sys.executable,"-m","pytest","--collect-only","-q","-p","no:cacheprovider",
    "--basetemp=<collect-basetemp>","--task064-ci-phase=collect",
    "--task064-ci-nonce=<nonce>","--task064-ci-observation=<collect-result>"]`, with each displayed
    `name=value` represented as one argv element. It recomputes the complete manifest and all five
    partitions, then launches exactly one execution child with the same prefix minus
    `--collect-only`, with phase `execute`, fresh nonce/result/basetemp, exact
    `--task064-ci-shard-id=<shard-id>`, and the one shard's node IDs appended in original collection
    order. The `report` execution alone inserts before its node ID
    the exact proof mode `primed-full`, fresh proof nonce, and proof-observation path options;
    remainder executions insert none. In `--report-proof` mode the runner launches one execution
    child for the exact report node with one of the three acceptance-only modes plus fresh proof
    nonce/path, CI shard ID `report-proof-<mode>`, and the same cache/observer controls; it does not
    count that invocation as a shard.
    Every argv and `SC_ARG_MAX` projection uses the final augmented vector. It never uses shell
    text, a response file, assignment
    file, retry, restart, or second batch. `TMPDIR`, `TEMP`, and `TMP` name one fresh mode-`0o700`
    private root below resolved `RUNNER_TEMP`; outside GitHub the runner creates that root below
    `tempfile.gettempdir()`. `HYPOTHESIS_STORAGE_DIRECTORY`, `PYTHONPYCACHEPREFIX`, and
    `COVERAGE_FILE` name private children; bytecode writing is disabled. Both children receive no
    `GITHUB_OUTPUT`, `GITHUB_ENV`, `GITHUB_PATH`, or `GITHUB_STEP_SUMMARY`, and those command files
    remain unchanged while a child runs. Each child starts a fresh process group. Timeout or
    residue triggers bounded `SIGTERM`, then `SIGKILL`, complete reap, and failure if any member
    survives. Standard output/error are each bounded to 2 MiB and removed after validation.

    `tests/conftest.py` accepts exactly the core observer options
    `--task064-ci-phase={collect,execute}`, `--task064-ci-nonce=<64 lowercase hex>`, and
    `--task064-ci-observation=<absolute private path>`. All three or none are required; collect mode
    forbids a shard option, while execute mode requires exactly one
    `--task064-ci-shard-id` in the closed set `report`, `remainder-0` through `remainder-3`,
    `report-proof-unprimed-success`, `report-proof-expired-entry`, or
    `report-proof-ready-teardown`. Absence is completely inert, including plain pytest and nested
    TASK-064 children, and any partial, duplicated, malformed, inconsistent mode/shard, or
    foreign-parent path fails before collection. The observer uses only
    `pytest_addoption`, `pytest_collection_finish`, `pytest_deselected`, `pytest_collectreport`,
    `pytest_runtest_logstart`, `pytest_runtest_logreport`, `pytest_runtest_logfinish`,
    `pytest_keyboard_interrupt`, `pytest_internalerror`, and try-last `pytest_sessionfinish`. It
    records but never selects, deselects, reorders, marks, schedules, retries, launches, changes
    fixtures/outcomes, or mutates the environment.

    Every private observer JSON uses the stated key insertion order, Python `json.dumps` with
    `ensure_ascii=True`, `separators=(",", ":")`, no key sorting, and no terminal LF. It permits
    no optional/extra/duplicate key, BOM, CR/LF, non-ASCII string, non-finite value, bool-as-int, or
    decode/re-encode difference. The collection packet's exact order is `domain`, `nonce`,
    `observer_pid`, `observer_parent_pid`, `pytest_exitstatus`, `collect_failed_count`,
    `collect_skipped_count`, `deselected_nodes`, `interrupted_count`, `internal_error_count`,
    `external_plugins`, `worker_indicator_count`, `surviving_non_main_thread_count`, `nodes`;
    domain is `TASK064-CI-COLLECTION-OBSERVATION-V1`. The execution packet adds after the two PIDs
    exact `shard_id`, then uses `pytest_exitstatus`, the same collect/deselect/interrupt/internal,
    plugin/worker/thread fields, `unknown_report_count`, `assigned_nodes`, `collected_nodes`,
    `started_nodes`, `finished_nodes`, `reports`; its domain is
    `TASK064-CI-EXECUTION-OBSERVATION-V1`. Each report is exactly
    `[nodeid,when,outcome,wasxfail]`, with `when` in `setup|call|teardown`, `outcome` in
    `passed|failed|skipped`, and exact bool `wasxfail`, in hook order. Collection packets are at
    most 400,000 bytes and execution packets at most 1,500,000 bytes.

    The result path must not exist. The observer opens it relative to its validated private parent
    with `O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW|O_CLOEXEC`, mode `0o600`, performs bounded complete
    write, file `fsync`, proven close, parent `fsync`, and no later mutation. The runner reopens
    no-follow, requires regular same-UID mode-`0o600` one-link identity, expected nonce/PID/parent
    PID, bounded size, exact canonical bytes, then unlinks and syncs. A missing, stale, partial,
    replaced, malformed, or post-exit-inconsistent packet fails.

    Proof mode uses the same private-parent, fresh nonce, exclusive no-follow mode-`0o600`
    write/fsync/close/parent-fsync, runner readback, unlink, and cleanup protocol. Its child-written
    packet uses the same compact ASCII/no-LF/no-extra-key canonical rule, has domain
    `TASK064-REPORT-PROOF-OBSERVATION-V1`, maximum 4,096 bytes, with
    exact order `domain`, `nonce`, `observer_pid`, `observer_parent_pid`, `mode`, `node_id`,
    `contract_generation`, `contract_sha256`, `publication_status`, `output_bytes`,
    `output_sha256`, `cache_state_at_assertion`, `cache_state_after_teardown`,
    `cache_teardown_completed`, `receipt_consumed`, `final_file_present`, `stage_residue_count`,
    `status`. It is emitted only by the authenticated fixture finalizer after matching the pending
    exact-node record, running cache teardown, and proving `EMPTY`. `primed-full` and
    `unprimed-success` require `PUBLISHED`, positive output length, lowercase SHA-256, asserted
    `EMPTY`, consumed receipt, final file present, and no stage residue. `expired-entry` requires
    `NONE`, null output fields, asserted `EMPTY`, unconsumed receipt, no final file, and no stage
    residue. `ready-teardown` requires `NONE`, null outputs, asserted `READY`, unconsumed receipt,
    no final file, and no stage residue. Every mode requires post-teardown `EMPTY`, exact true
    `cache_teardown_completed`, and `PASS`. Missing pending state or an observation written before
    cache teardown is a failure.

    A passing shard emits canonical `TASK064-CI-SHARD-RESULT-V1` JSON with exact top-level order:
    `domain`, `shard_id`, `contract_generation`, `contract_sha256`, `event_name`,
    `candidate_relation`, `candidate_commit_sha`, `candidate_tree_sha`,
    `tested_checkout_commit_sha`, `tested_checkout_tree_sha`, `tested_checkout_parent_count`,
    `tested_checkout_second_parent_sha`, `python_version`, `pytest_version`,
    `python_version_file_sha256`, `uv_lock_sha256`, `full_manifest_count`,
    `full_manifest_bytes`, `full_manifest_sha256`, `partition_status`, `shard_manifest_count`,
    `shard_manifest_bytes`, `shard_manifest_sha256`, `selector_bytes_including_nul`,
    `report_output_bytes`, `report_output_sha256`, `counts`, `sequence_sha256`, `anomalies`,
    `collection_exit_code`, `test_exit_code`, `collection_elapsed_ns`, `test_elapsed_ns`,
    `pre_clean`, `post_clean`, `cleanup_status`, `survivor_count`, `status`. It uses the same
    compact canonical rule, is at most 8,192 bytes, and has SHA-256 over those exact bytes.
    `candidate_relation` is `SECOND_PARENT` or `SELF`; the second-parent field is candidate SHA for
    the former and JSON `null` for the latter. `partition_status`, `cleanup_status`, and `status`
    are `PASS`. The `report` execution alone supplies proof mode `primed-full`; its output
    length/SHA derive only from the validated proof observation. Remainder executions receive no
    proof option and both result fields are JSON `null`.

    `counts` has exact order `assigned`, `collected`, `started`, `finished`, `setup_passed`,
    `call_passed`, `teardown_passed`, each equal to the shard count. `sequence_sha256` has exact
    order `assigned`, `collected`, `started`, `finished`; each hashes compact insertion-order
    `{"domain":"TASK064-NODE-SEQUENCE-V1","nodes":[...ordered IDs...]}` with no LF, and all four
    hashes are equal. `anomalies` has exact order `unknown`, `duplicate`, `failed`, `error`,
    `skipped`, `xfailed`, `xpassed`, `deselected`, `interrupted`, `unaccounted`, all zero.
    Ordinary call failure is `failed`; setup/teardown/collection/internal failure is `error`;
    `wasxfail` plus skipped/passed is `xfailed`/`xpassed`; ordinary skip is `skipped`; invalid or
    unassigned identity/enum is `unknown`; repeated identity/start/finish/phase is `duplicate`; and
    any missing, extra, mismatched phase/sequence/exit/packet is `unaccounted`. Both child exits are
    zero, collection is at most 60,000,000,000 ns, execution at most 720,000,000,000 ns, checkout
    is clean before and after, and survivor count is zero.

    On GitHub the parent first requires each of `GITHUB_OUTPUT`, `GITHUB_ENV`, `GITHUB_PATH`, and
    `GITHUB_STEP_SUMMARY` to be an absolute path beneath resolved `RUNNER_TEMP`, and opens it
    no-follow/CLOEXEC. Each is a regular same-UID one-link file with no group/world write. The
    parent retains the exact descriptor, requires `GITHUB_OUTPUT` initially zero bytes, and before
    and after every child compares path and descriptor device/inode/UID/mode/link count,
    size/mtime/ctime nanoseconds, and a bounded complete raw SHA-256 snapshot. Any change or
    descriptor/path disagreement fails; CLOEXEC and child-environment scrubbing are not treated as
    the proof by themselves.

    Only after all validation and cleanup does a GitHub shard append exactly two single-line ASCII
    outputs through the retained `GITHUB_OUTPUT` descriptor:
    `task064_packet_b64=<standard padded base64 of canonical packet>\n` and
    `task064_packet_sha256=<64 lowercase hex>\n`. It file-syncs, exact-readbacks the complete two
    lines, revalidates descriptor/path identity, and proven-closes. It never writes the other three
    command files. Outside GitHub it emits only those two lines to otherwise-clean stdout. Failure
    emits no PASS packet/output and exits nonzero.

    After zero child exit and complete reap, the runner wraps the validated observation as canonical
    `TASK064-REPORT-PROOF-RESULT-V1`, at most 4,096 bytes, with exact order `domain`, `mode`,
    `contract_generation`, `contract_sha256`, `candidate_commit_sha`, `candidate_tree_sha`,
    `tested_checkout_commit_sha`, `tested_checkout_tree_sha`, `node_id`,
    `observation_sha256`, `publication_status`, `output_bytes`, `output_sha256`,
    `cache_state_at_assertion`, `cache_state_after_teardown`, `cache_teardown_completed`,
    `receipt_consumed`, `final_file_present`, `stage_residue_count`, `test_exit_code`,
    `test_elapsed_ns`, `cleanup_status`, `survivor_count`, `status`. It reproduces the exact child
    facts, binds the inner canonical digest, and requires zero exit, at most 720,000,000,000 ns,
    cleanup/status `PASS`, and zero survivors. Every proof child is subject to the same Git,
    environment, plugin, thread/process, private-temp, bounded-output, command-file snapshot,
    timeout, and cleanup controls as a shard child. A passing `--report-proof` invocation emits
    exactly `task064_report_proof_b64=<standard padded base64>\n` and
    `task064_report_proof_sha256=<64 lowercase hex>\n` to otherwise-clean stdout; failure emits no
    PASS result.
19. `.github/workflows/ci.yml` retains checkout, locked installation, lock verification,
    formatting, lint, strict typing, dependency audit, and foundation health in a non-test
    `quality_gates` job. It defines exactly five statically named shard jobs—`report`,
    `remainder_0`, `remainder_1`, `remainder_2`, and `remainder_3`—rather than a dynamic matrix.
    Every job uses `actions/checkout` with `fetch-depth: 0` and `persist-credentials: false`, the
    same immutable checkout and locked dependencies on a fresh Ubuntu host. Each shard hard-codes
    its one matching argument and exposes only the two exact runner outputs. There is no job or step
    `continue-on-error`, conditional shard omission, masked exit (`|| true` or equivalent), output
    default, retry, artifact splice, or conditional PASS publication.

    A final job ID `quality`, display name exactly `Quality and security`, uses `if: always()` and
    explicitly needs `quality_gates`, `report`, `remainder_0`, `remainder_1`, `remainder_2`, and
    `remainder_3`. It first requires all six `needs.*.result` values to be exactly `success`. For
    each named shard it requires both outputs nonempty; caps encoded base64 at 10,924 ASCII
    characters; rejects whitespace, nonalphabet, or invalid padding; strictly decodes at most
    8,192 bytes; requires `base64.b64encode(decoded)` byte-equal to the supplied text; verifies the
    output digest; strictly decodes and byte-for-byte re-encodes the canonical packet; and validates
    the complete schema.

    `--aggregate-static` consumes exactly these fixed workflow-environment names:
    `TASK064_AGG_QUALITY_GATES_RESULT`, `TASK064_AGG_REPORT_RESULT`,
    `TASK064_AGG_REMAINDER_0_RESULT`, `TASK064_AGG_REMAINDER_1_RESULT`,
    `TASK064_AGG_REMAINDER_2_RESULT`, `TASK064_AGG_REMAINDER_3_RESULT`,
    `TASK064_AGG_REPORT_PACKET_B64`, `TASK064_AGG_REPORT_PACKET_SHA256`,
    `TASK064_AGG_REMAINDER_0_PACKET_B64`, `TASK064_AGG_REMAINDER_0_PACKET_SHA256`,
    `TASK064_AGG_REMAINDER_1_PACKET_B64`, `TASK064_AGG_REMAINDER_1_PACKET_SHA256`,
    `TASK064_AGG_REMAINDER_2_PACKET_B64`, `TASK064_AGG_REMAINDER_2_PACKET_SHA256`,
    `TASK064_AGG_REMAINDER_3_PACKET_B64`, and
    `TASK064_AGG_REMAINDER_3_PACKET_SHA256`. The workflow assigns every expression as a
    fixed `env:` value, never shell interpolation; the runner rejects any missing/empty value,
    default, or other `TASK064_AGG_` name. Its hard-coded job mapping
    is `report -> (report,1,302,f0530be0d219c64bbd1b9eb4df635dd138a345e8685172d188d02e177e488a3e,146)`,
    `remainder_0 -> (remainder-0,600,76664,9946638e834ffa44c0cd1e511c348b1f8226fc078ef79eb9e4100e770de80044,75290)`,
    `remainder_1 -> (remainder-1,563,72767,e6814eb76767d9462ed9bfa82c85d8e7daebd7265027883290ca88842365dfd0,71471)`,
    `remainder_2 -> (remainder-2,588,75756,8d86911d7a7cfc4f32022d68be1eaa3d6b459d5a968a462deea188df2369ed5b,74394)`,
    and
    `remainder_3 -> (remainder-3,546,71332,69e69516345c674cadf552202b80c392f8297b74b46328278b098b462e25b4ca,70049)`.

    Across packets the aggregator requires identical contract, event, candidate commit/tree,
    checkout commit/tree, Python/pytest/version-file/lock/full-manifest identities; packet candidate
    equals workflow `TASK064_CANDIDATE_SHA`; checkout equals `GITHUB_SHA`; and relation is
    `SECOND_PARENT` for pull requests or `SELF` otherwise. It requires five distinct expected
    shard IDs; each exact count, sequence, anomaly, exit, time, clean-checkout, and cleanup
    invariant; sums assigned, collected, started, finished, setup, call, and teardown passes each
    to exactly 2,298; and requires exactly one report output. Only then may `quality` pass. It is
    the stable required status while the five shard jobs remain visible. No green workflow grants
    merge, deployment, production, or live-trading authority.

    Before acceptance, the exact generation-5 candidate must pass one unchanged full serial
    `uv run pytest` correctness-parity run and two fresh Linux/ext4 executions of every shard with
    no discarded attempt. The serial run is explicitly exempt from the shard performance ceiling
    and has a 2,100-second correctness-only deadline. Every shard execution is at most 720 seconds
    and every complete CI shard job at most 900 seconds. The exact candidate also passes one fresh
    Linux/ext4 `unprimed-success`, `expired-entry`, and `ready-teardown` proof invocation, each at
    most 720 seconds with no discarded attempt. The primed and unprimed results each bind their own
    exact input report's serializer output length/SHA and prove unchanged publication of those
    bytes; unique generated identities prevent a false cross-process digest-equality requirement.

    The existing report node and those option branches prove prime is file-free and non-consuming;
    one successful explicit prime per primed invocation; duplicate-prime rejection without
    refresh; four unique live observations and alias vector `[0,1,0,2,1,0,0,3]`; stale, forced
    equality-expired, fork/thread/context, copy, source/core/raw/epoch, receipt, success, rollback,
    collision, post-link, cleanup-uncertainty, and root-finalizer teardown transitions; unchanged
    successful unprimed publication; and exact same-serializer bytes on both routes. No new test node,
    hidden parameter ID, or weakened 30-second authenticated child-handshake deadline is allowed.
    Independent Engineering, Security/Risk, and QA reviews bind this exact contract digest,
    candidate commit/tree, source and fixture identities, full/shard manifests, serial evidence,
    both five-job shard-split attempts, all three proof results, and final CI run.

## Blocked, Awaiting Owner-Supplied Restricted Inputs

### TASK-037 — Operator-preflight authorization package and project-owner decision

- **Key:** `phase2.canonical_utc_preflight_operator_authorization_package_owner_decision`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 3 — PRODUCTION AFFECTING (authorization decision only)
- **Status:** BLOCKED
- **Human approval:** REQUIRED — project owner plus independent Risk and Security review.
- **Blocking condition:** Owner-supplied exact restricted-package inputs are not available in a
  Security-approved governance location; independent Risk and Security reviews and the
  project-owner decision therefore remain unperformed. Authorization remains `DENIED`.
- **Resume condition:** Return TASK-037 to `READY` only after the exact required inputs are
  supplied through the approved restricted boundary; repository placeholders or TASK-038
  completion cannot satisfy this condition.
- **Preparation artifact:** The repository contains only the non-authorizing placeholder template
  at `docs/governance/TASK-037-operator-preflight-authorization-package.template.md`. It prohibits
  real deployment values and cannot satisfy any acceptance gate or approval requirement.
- **Goal:** Prepare the exact operator-preflight authorization package and obtain an explicit
  project-owner `APPROVE`, `REJECT`, or `REVISE` decision without accessing operator data.
- **Scope:** Record the proposed exact read-only database/path list and its real deployment
  cardinality, the writer-fenced consistent/immutable snapshot procedure, report destination,
  evidence retention/disposal boundary, and the change, environment, evidence, approver, UTC
  decision time, expiry or review trigger, monitoring, and tested rollback evidence required by
  policy.
- **Constraints:** Governance preparation and decision only. Do not inspect, resolve, check, or
  open any proposed operator path or database; access SQLite or operator data; scan rows; invoke
  an adapter; create an operational report or manifest; add serialization or scanner code; wire a
  runtime; migrate or repair data; alter a schema; or perform or claim Stage 3. Approved
  governance-artifact writes are the only filesystem mutation in scope. Do not store sensitive
  path metadata in an unapproved location. Missing, ambiguous, expired, conflicting, rejected, or
  revise-required authority remains denial.

Acceptance gates:

1. The package distinguishes the real proposed path-list cardinality from TASK-036's eight
   synthetic family-coverage slots and makes every exact family/path entry explicit.
2. The exact snapshot procedure is writer-fenced and SQLite-safe and specifies its consistency,
   immutability, and WAL/checkpoint handling without executing it.
3. The exact report destination and evidence retention/disposal boundary are identified through
   an approved handling location, with independent Risk and Security review recorded.
4. The decision identifies the change, scope, environment, evidence, project-owner approver, UTC
   decision time, expiry or review trigger, monitoring, tested rollback evidence, and one explicit
   `APPROVE`, `REJECT`, or `REVISE` outcome. Anything else fails closed.
5. No proposed operator path or database is inspected, resolved, checked, or opened; no SQLite or
   operator-data access, scan, adapter, operational report or manifest, serializer, scanner code,
   runtime action, migration, repair, schema change, or Stage 3 action occurs. Only approved
   governance-artifact writes are allowed.
6. Any approved scanner remains a separately scoped later task with its own risk review; no
   approval outcome automatically runs or authorizes code beyond its exact recorded scope, and
   all repository gates pass.

## Recently Completed

### TASK-063 — Continuous public-trade physical stream-store architecture and evidence plan

- **Key:** `phase2.continuous_public_trade_stream_physical_store_architecture`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Merge evidence:** Owner-approved PR
  [#66](https://github.com/Adampov/WEALTH/pull/66), accepted head
  `8def515c29f6b778540e1ac2d6c55b0006c59b1b`, merge commit
  `cf5f69c818185c54cb1e4c701bf6e390cdf96237`, and successful required target-branch CI run
  [30401016909](https://github.com/Adampov/WEALTH/actions/runs/30401016909).
- **Files:** `docs/decisions/0031-continuous-public-trade-stream-physical-store-architecture.md`,
  `docs/decisions/README.md`, `README.md`, `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`,
  `PROJECT_STATE.json`, `BACKLOG.md`, `RISK_REGISTER.md`, `tests/unit/test_project_state.py`, and
  `tests/unit/test_task_063_physical_store_architecture.py`.
- **Result:** ADR-0031 selects one dedicated local SQLite generation behind the unused ADR-0030
  port and freezes a non-executable physical descriptor and fail-closed preimplementation evidence
  plan. Original TASK-061 BLOBs remain authoritative. Among TASK-059 epoch coordinates, only
  `stream_start_epoch_ms` receives an exact signed-64-bit SQL `INTEGER` projection; current cursor
  and optional attachment-window epochs deliberately have no scalar columns. Causal versions and
  the ADR-0031-enumerated non-epoch integer policy, version, key, and format fields also use exact
  SQL `INTEGER` representations without becoming authority.

  The design maps reversible UUID and natural-identity keys, strict metadata, immutable history,
  constraint-bound creation/current/predecessor witnesses, atomic create, one-winner
  compare-and-swap, closed failure classification, constant-size current loads, and audit pages of
  1 through 100 new rows plus only the permitted overlap. Preserve-all retention,
  separate-generation migration, Online Backup plus independent restore, crash and
  lost-acknowledgement testing, exact query bounds, target-runtime/filesystem verification, and
  finite capacity thresholds remain mandatory gates.

  TASK-063 creates no executable schema, database, adapter, path, configuration, I/O, runtime,
  clock, fence, budget, retry or recovery action, permission, durability, capacity, readiness,
  deployment, Phase 2 completion, or risk closure. TASK-037 remains blocked and authorization
  remains denied.

### TASK-062 — Pure continuous public-trade logical stream-store port and outcome contracts

- **Key:** `phase2.continuous_public_trade_stream_store_port_contracts`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Merge evidence:** Owner-approved PR
  [#63](https://github.com/Adampov/WEALTH/pull/63), accepted head
  `21c3171de68ee5ec42eeff82ce1171008f2e854b`, merge commit
  `6b959670e1737bd10585b437786d06408a22e31d`, and successful required target-branch CI run
  [30373228787](https://github.com/Adampov/WEALTH/actions/runs/30373228787).
- **Files:** `docs/decisions/0030-continuous-public-trade-stream-store-port-contract.md`,
  `docs/decisions/README.md`, `src/wealth/ports/continuous_public_trade_stream_store.py`,
  `tests/unit/test_continuous_public_trade_stream_store_port_contracts.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, `PROJECT_STATE.json`, `BACKLOG.md`,
  `RISK_REGISTER.md`, and governance tests only.
- **Result:** ADR-0030 and one unused provider-independent port module freeze a lower-level logical
  store boundary around exact finalized TASK-061 creation and transition artifacts. Strict frozen
  identity, complete-policy expectation, original canonical-envelope and history-record wrappers,
  create/load/compare-and-swap commands, separate bounded audit queries, receipts, current views,
  pages, closed outcomes, retry dispositions, and the public
  `validate_continuous_public_trade_stream_audit_page` query/page validator reject coercion, hidden
  state, malformed bytes/digests/roots, incomplete policy, and cross-command disagreement.

  Create logically owns one atomic current-plus-creation insertion under UUID and natural-identity
  uniqueness. Compare-and-swap logically owns one exact current replacement plus one immutable
  transition append with one winner. The finalized transition record is the sole successor source;
  callers cannot supply a second successor, timestamp, attachment payload, or independent
  successor digest. Exact historical retries are `DUPLICATE`; stale, missing, mismatched, or
  competing mutations are `CONFLICT`. Absence, identity conflict, unsupported versions,
  corruption, anchor conflict, and storage unavailability remain distinct.

  Current views require only constant-size creation/current/direct-predecessor material. Audit
  results return 1 through 100 new records, use no overlap initially and exactly one predecessor on
  continuation, and return `AT_TAIL` instead of an empty page. A conforming future adapter must
  separately prove that it obtains each page without reading beyond the same 100-new-record plus
  one-overlap bound. Original TASK-061 bytes remain authoritative throughout. Every output is a
  store-local classification; only accepted receipts, `FOUND`, `PAGE`, and a validated `AT_TAIL`
  anchor carry bounded structural evidence, while `UNAVAILABLE` carries no coherent
  classification.

  The port performs no I/O and is not imported by runtime composition. It validates only
  store-local structure; it does not sample time, construct a successor, access evidence bodies,
  validate or create an accepted attestation, grant authority, retry automatically, implement a
  physical store, or claim capacity, durability, recovery, multi-host safety, continuous-operation
  readiness, deployment, Phase 2 completion, or risk closure. TASK-037 remains blocked and
  authorization remains denied.

### TASK-061 — Pure versioned continuous public-trade persistence-record and canonical-codec contracts

- **Key:** `phase2.continuous_public_trade_stream_persistence_codec_contracts`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/domain/continuous_public_trade_persistence.py`,
  `tests/unit/test_continuous_public_trade_persistence_contracts.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, `PROJECT_STATE.json`, `BACKLOG.md`,
  `RISK_REGISTER.md`, and governance tests only.
- **Result:** One unused, provider-independent, side-effect-free domain module now freezes
  ADR-0029's version-one stream-policy projection, complete pristine bounded-child creation
  payload, exact checkpoint envelope, stream-creation record, typed evidence references and
  scopes, and immutable transition record. Strict construction and public boundaries reject
  coercion, booleans, polymorphic scalars, bypass-constructed hidden fields, malformed canonical
  values, policy/identity drift, invalid attachment material, and cross-record disagreement.

  Five canonical UTF-8 JSON codecs retain compact sorted keys, explicit nulls, exact lowercase
  UUID/digest/hex values, fixed six-fractional-digit UTC timestamps, and exact integer tokens.
  Typed sanitized decoding rejects malformed UTF-8/JSON, duplicate/missing/extra keys, unsupported
  versions, noncanonical bytes, floats/exponents, and bounded parser abuse. The frozen caps are
  65,536 raw record bytes, 8,192 child-payload bytes, 16,384 decoded envelope bytes, 32,768
  successor-hex characters, 8,192 other string-token bytes, 16 object levels, 128 members,
  64-byte ASCII keys, and 19 integer digits.

  Six distinct SHA-256 domains bind child creation, stream envelopes, stream creation, transitions,
  evidence scopes, and initial/chained rolling history roots. Pure validators bind complete
  effective stream policy field by field even when a fingerprint is reused, immutable load
  identity, attached child-policy identity, exact version-one creation bytes, transition scopes,
  predecessor/successor envelopes, TASK-059 transition causality, non-regressing record time, and
  history-root continuation. The two-pass finalizer plans with one in-memory all-zero provisional
  fingerprint, builds the exact pristine child with one fixed UTC instant, replans with the real
  payload digest, and requires exact non-fingerprint equality without persisting or acting on the
  provisional value.

  Deterministic golden, round-trip, hostile, boundary, mutation, property, and transition tests
  remain offline and secret-free while preserving full-range unattached TASK-059 epoch
  milliseconds and failing closed on unrepresentable attached child material. TASK-059 behavior
  and every bounded-job contract remain unchanged. No port, repository, adapter, SQLite/DDL/schema,
  migration, filesystem/network I/O, runtime import, dependency, operator data, authority,
  automatic action, capacity, durability, recovery, multi-host, continuous-operation, deployment,
  readiness, or Phase 2 claim was added. TASK-037 remains blocked and authorization remains denied.

### TASK-060 — Continuous public-trade stream persistence-contract decision

- **Key:** `phase2.continuous_public_trade_stream_persistence_contract_decision`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `docs/decisions/0029-continuous-public-trade-stream-persistence-contract.md`,
  `docs/decisions/README.md`, `README.md`, `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`,
  `PROJECT_STATE.json`, `BACKLOG.md`, `RISK_REGISTER.md`, and governance tests only.
- **Result:** ADR-0029 now records the design-only persistence contract for a possible future
  single-host continuous public-trade stream. The exact TASK-059 checkpoint is the durable current
  state; planning results, service-run state, fences, bounded-job control, market evidence,
  source health, request-budget state, and invocation-local actors remain separate domains.
  Conceptual creation, exact-identity load, and versioned compare-and-swap transitions fail closed
  on missing, unknown, corrupt, stale, or conflicting state and never authorize action by
  themselves.

  A committed attachment must bind the complete canonical deterministic `child_creation_payload`,
  including a fixed-UTC creation time, because TASK-059's SHA-256 creation fingerprint is
  intentionally non-invertible. This new evidence payload does not redefine the existing
  bounded-child store serializer. The continuous-stream and bounded-child policy fingerprints stay
  distinct. A pause reason is not authority evidence, and a completed child ID is not accepted
  completion proof. ADR-0029 preserves bounded external actor/governance references, accepted child
  completion evidence, exact bounded-child recovery, evidence-first ordering, one shared durable
  pre-request budget, and fresh independent UUID fences without moving those controls into the
  current checkpoint.

  The decision pins canonical versioned serialization plus six distinct child, stream-envelope,
  stream-creation, transition-record, evidence-scope, and rolling-history-root digest contracts.
  Separate creation/transition records retain successor-envelope bytes and the complete immutable
  stream-policy projection; typed external evidence scopes, trusted non-regressing command time,
  and an anchored history attestation prevent current load/planning alone from authorizing work.
  ATTACH authority binds the exact prior version, envelope digest, accepted history root, successor
  version, candidate child, and effective child-policy fingerprint before time sampling while the
  finalized transition/root binds its time-dependent successor. Only a separately validated
  non-integrity operational hold can preserve admission of an already-returning in-flight response;
  drift, invalid payload, quality/evidence failure, corruption, or ambiguity stops canonical
  admission and progress.
  A pure two-pass plan/payload/replan proof resolves TASK-059's fingerprint-before-range API without
  persisting its provisional value. ADR-0029 enumerates crash seams around attachment, child
  creation, request reservation, evidence, child checkpointing, completion, stream advancement,
  manual hold, and governed resume and requires exact reload after an unknown commit outcome. It
  specifies compatibility, quarantine, migration and restore prerequisites, causal retention, and
  disable-to-current-bounded-flow rollback before a physical repository is selected. TASK-059
  epoch milliseconds remain exact even where they cannot be represented as Python datetimes or
  signed-64-bit epoch microseconds.

  No production source, codec, port/repository/adapter, SQLite/DDL/migration/schema, filesystem
  state, runtime or network path, dependency, scheduler/service/deployment, operator data,
  credential, permission, automatic action, capacity, durability, recovery, multi-host,
  continuous-operation, readiness, or Phase 2 claim was added. TASK-061 is the separately governed
  pure-record and canonical-codec increment. TASK-037 remains blocked and authorization remains
  denied.

### TASK-059 — Pure continuous public-trade closed-window planner and lifecycle contracts

- **Key:** `phase2.continuous_public_trade_closed_window_planner_contracts`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/domain/continuous_public_trade.py`,
  `tests/unit/test_continuous_public_trade_contracts.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, `PROJECT_STATE.json`, `BACKLOG.md`,
  `RISK_REGISTER.md`, and governance tests only.
- **Result:** One provider-independent, frozen, side-effect-free domain boundary now exposes
  `ContinuousPublicTradePolicy`, `ContinuousPublicTradeAttachment`,
  `ContinuousPublicTradeStreamCheckpoint`, `ContinuousPublicTradePlan`, their explicit stream,
  service, plan, and transition enums, `plan_continuous_public_trade_window`, and pure stream and
  service transition validators. It accepts no clock implicitly and performs no I/O.

  Policy validation requires exact built-in integer millisecond values, rejects booleans and
  non-positive, misaligned, or out-of-range work limits, requires the finite catch-up span to be a
  whole number of windows, and preserves a complete lowercase SHA-256 policy fingerprint. Stream
  validation preserves immutable stream and market identity, the exact request variant, policy
  fingerprint, epoch-millisecond start and cursor, monotonic version, whitespace-free manual pause
  evidence, and attachment range consistency. Pure transition validators allow only explicit
  `RETAIN`, `ATTACH`, `CHILD_COMPLETED`, `MANUAL_HOLD`, and `MANUAL_RESUME` stream transitions and
  the finite service path `STARTING` to `RUNNING` to one terminal status. They reject unknown,
  version-skipping, identity-changing, fingerprint-changing, cursor-regressing,
  attachment-widening, or causally invalid transitions without mutating either value.

  The pure closed-window planner accepts one validated checkpoint and one explicit
  `datetime.UTC` instant. A paused stream returns only `HELD` while preserving its cursor and any
  attachment. An active stream with an existing attachment returns only `ATTACHED_JOB` with that
  exact immutable child identity, policy fingerprint, and half-open range. A caught-up stream
  returns only `WAITING`. Otherwise the planner returns one `ATTACHED_JOB` candidate beginning
  exactly at the durable cursor and ending at
  `min(latest_eligible_end, cursor + max_catchup_span)`. Eligibility is epoch-aligned in exact
  whole UTC milliseconds after the configured non-negative settlement lag, so no result rounds a
  cursor forward, includes an open/future window, overlaps prior work, skips a gap, widens an
  existing attachment, or exceeds the finite catch-up bound.

  These contracts are unused. They do not create, attach, persist, start, claim, invoke, schedule,
  retry, pause, resume, recover, or supervise a real job or stream. No existing runtime
  composition imports the module. The work adds no repository/adapter, SQLite or schema, network
  or provider access, wait/sleep or request-budget behavior, trigger, scheduler, daemon, service,
  CLI, dashboard, deployment, configuration loading, operator path/data, credential, permission,
  notification, dependency, lockfile, or automatic action. It establishes no cross-database
  atomicity, physical durability, capacity adequacy, multi-host exclusivity, continuous-operation,
  recovery, deployment, or Phase 2 readiness. ADR-0028 remains unchanged; TASK-060 was the
  separate design-only persistence-contract decision and is now complete under ADR-0029; and
  TASK-037 remains blocked with authorization denied.

### TASK-058 — Continuous public-trade collection operating-contract decision

- **Key:** `phase2.continuous_public_trade_collection_operating_contract_decision`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `docs/decisions/0028-continuous-public-trade-collection-operating-contract.md`,
  `docs/decisions/README.md`, `README.md`, `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`,
  `PROJECT_STATE.json`, `BACKLOG.md`, `RISK_REGISTER.md`, and governance tests only.
- **Result:** ADR-0028 accepts one conceptual single-host composition for a possible future
  continuous public-trade collector: an unselected external trigger may invoke a finite-run
  coordinator around the existing explicitly invoked bounded orchestrator only after separately
  governed implementation and deployment. The future coordinator would own continuous stream
  selection, an immutable attached child, and an outer fresh-UUID fence; the existing child keeps
  its independent fence, exact pending leaf, evidence-first checkpoint, idempotent refetch,
  causal transition/source-health evidence, adaptive finite work, and the one shared durable
  single-host request-budget gate.

  The decision separates three conceptual layers without adding serialized state. Durable stream
  control is `ACTIVE` or `PAUSED`; schema drift is a scoped pause reason, while enablement remains
  an external disabled-by-default posture. A finite service run moves from `STARTING` to `RUNNING`
  and exactly one of `STOPPED`, `PAUSED`, `FAILED`, or `RUN_LIMIT`. The existing bounded job keeps
  `PENDING`, `RUNNING`, `PAUSED`, `FAILED`, and `COMPLETED`; `waiting`, `caught_up`, and
  `work_limit_reached` are outcomes rather than lifecycle states. A clean bounded-job `PAUSED` keeps the
  stream `ACTIVE` and its exact attachment, while bounded-job failure, conflict, lost lease,
  corruption, or drift fails the service and requires a manual stream pause. A clean service stop
  leaves stream state, cursor, and attachment unchanged.

  The contract requires closed epoch-aligned half-open UTC windows, explicit settlement lag, a
  durable cursor, bounded catch-up and per-invocation work, no overlap or gap, cooperative clean
  stop and shutdown checks, and no self-scheduling state. A reopen must use fresh outer and child
  authority, retain the immutable child, finish its exact pending leaf first, and advance the
  continuous cursor only after accepted evidence and exact child completion.
  It does not claim cross-database atomicity, crash-durable per-job attempt reservations, physical
  durability, automatic recovery, or multi-host exclusivity.

  Suspected schema drift remains a manual exact-variant or inseparable-parser hold under the
  TASK-057 runbook. Governed resume requires contract review, synthetic evidence, complete
  regressions, rollback, and applicable authority; no detector, automatic pause, remediation, or
  resume was added. Source health remains causal to bounded child work, while future service
  health would separately distinguish waiting/caught-up, stale, paused, drift-held, work-limited,
  stopped, and failed observations with bounded audit correlation. The ADR identifies operator
  decisions, escalation, finite internal-alert evidence, complete provider/cadence/backlog/range/
  shared-budget/storage/control/recovery capacity inputs and measurements, failure dispositions,
  disable-to-current-bounded-flow rollback, review triggers, and the deterministic, operational,
  deployment, and rollback evidence required before implementation.

  This work changes documentation and governance only. It adds no production source, runtime,
  scheduler, service, network, persistence or SQLite schema, dependency or lockfile, deployment,
  operator path/data, credential, permission, notification, automatic action, or readiness claim.
  The selected future component is not implemented, enabled, deployed, scheduled, monitored, or
  capacity-approved. TASK-059 is a separately governed pure-contract increment; TASK-037 remains
  blocked and authorization remains denied.

### TASK-057 — Versioned public-provider schema fixtures and drift-response runbook

- **Key:** `phase2.versioned_public_provider_schema_fixtures_and_drift_runbook`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `tests/fixtures/public_provider_schema/v1/manifest.json`, exactly five synthetic JSON
  fixtures in that directory, `tests/unit/test_public_provider_schema_fixtures.py`,
  `docs/runbooks/PUBLIC_PROVIDER_SCHEMA_DRIFT.md`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, plus coordinated governance files and governance tests.
- **Result:** One strict version-1 manifest maps exactly five unique active request identities
  one-to-one to five minimal, bounded, secret-free synthetic fixture files with relative paths,
  exact-byte SHA-256, a 1,024-byte per-fixture maximum, provider/dataset/market/request identity,
  shape metadata, official contract reference, UTC review date, and reviewed status. The corpus
  covers Binance Spot and USD-M 12-position candle rows, Coinbase Exchange Spot six-position
  candle rows, and Binance Spot and USD-M aggregate-trade objects. Both aggregate variants use
  the existing shared parser contract: required fields are exactly `T`, `a`, `f`, `l`, `m`, `p`,
  and `q`, and optional fields are exactly `M` and `nq`; the Spot fixture contains `M`, while the
  USD-M fixture contains `nq`. Fixture presence does not invent a market-specific parser rule,
  and unknown fields remain rejected.

  Offline deterministic HTTP stubs and fixed UTC clocks feed every fixture's exact bytes through
  its active existing production adapter and request path. Tests pin the request variant,
  canonical values, provider identity, UTC/event-time behavior, and exact raw-byte lineage without
  a network call. Strict manifest tests reject unknown or missing keys, invalid versions/statuses
  or types, duplicate identities/paths, absolute/traversing/mislocated paths, extra or missing
  corpus files, digest mismatch, and oversized fixtures. Representative detectable adapter drift
  covers positional width minus/plus one, selected detectable reorder, wrong numeric types,
  invalid decimal values, missing required fields, invalid present optional-field values, and
  unknown fields through the existing non-retryable `INVALID_PAYLOAD` boundary without partial
  raw or canonical evidence.

  Decimal precision alone has no adapter-level bound, and some same-typed semantic positional
  reorder can remain canonically valid; either may be accepted. The tests and documentation retain
  that limitation explicitly: parser acceptance is not compatibility evidence, and either
  unreviewed change requires fail-closed pause and official-contract review. No rounding,
  normalization, parser widening, or provider-contract change was added.

  The new fixture module passes 54 tests; the focused fixture-plus-adapter regression passes 216,
  and the governance slice passes 20. An initial independent 42-mutant audit killed 25, classified
  nine as equivalent because exact metadata and redundant guards already rejected the mutation,
  and exposed eight real test gaps. After adding isolated entry-count, symlink, valid-oversize,
  non-finite fixture, corpus-shape, and cross-variant optional-field evidence, a targeted re-audit
  killed all 15 of 15 gap and sanity mutants with zero survivors or harness errors. The complete
  suite passes 1,704 tests; lockfile, formatting, lint, strict typing, dependency audit, and local
  health checks also pass.

  The linked runbook defines signals/classification, manual pause and containment, safe evidence
  handling, official-document re-review, synthetic versioning without overwriting old versions,
  regression commands, escalation, governed resume gates, and rollback. Real payload content is
  not copied into repository files, logs, issues, or fixtures; any real evidence requires an
  approved secret-free handling boundary. The work adds no production source, network, runtime,
  schema, dependency, operator path/data, credential, permission, automatic detection/pause/
  remediation/resume, continuous collector, deployment, or readiness claim. TASK-037 remains
  blocked and authorization remains denied.

### TASK-056 — Deterministic public-trade disconnect, sparse-window, and restart-recovery drill evidence

- **Key:** `phase2.public_trade_disconnect_sparse_window_restart_recovery_drill`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `tests/integration/test_recoverable_public_trade_collection.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, plus coordinated governance files and
  governance tests.
- **Result:** One new deterministic generated-fixture integration case composes the existing
  public-trade evidence, checkpoint, and shared rate-budget SQLite adapters across a process-style
  reopen. Worker A receives exactly two scripted retryable `HttpTransportError` outcomes, records
  one 0.125-second retry without wall-clock sleep, writes no market evidence, and reaches
  `FAILED` checkpoint version 3 with `UNAVAILABLE` health, `provider_unavailable`,
  `attempts_exhausted`, two source requests, one trace, one retry, the original cursor, and the
  exact first one-millisecond pending leaf. Hostile upstream detail is absent from durable
  checkpoint, health, and transition text.

  Worker B uses newly constructed adapters on the same three generated databases, the unchanged
  policy fingerprint, and a fresh UUID fence. It finishes the pending leaf first and then the
  remaining range in two bounded invocations from exact empty, one-valid-trade, and empty
  one-millisecond responses. Completion is checkpoint version 6 with five lifetime source
  requests, four traces, one retry, three completed windows, one canonical record, three raw
  captures, and zero conflicts. The exact six transition statuses are `PENDING`, `RUNNING`,
  `FAILED`, `RUNNING`, `RUNNING`, and `COMPLETED`; actors are absent, absent, worker A, absent,
  worker B, and worker B; matching health exists only at versions 3, 5, and 6.

  Across both workers, five unique granted durable reservations precede five provider attempts.
  The combined sleeper evidence is one 0.125-second retry plus two 0.25-second pacing waits.
  A completed rerun performs zero range invocations and leaves checkpoint, transition, health,
  budget, HTTP, evidence, and sleeper observations unchanged. The focused recovery integration
  file passes 14 tests, previously 13. Only test helpers and the new case changed; no production
  defect or production-source change was found. An isolated mutation audit killed all 30 of 30
  mutants with zero survivors and zero harness errors. It covered budget ordering and bypass,
  retry count and delay, pending-leaf and response chronology, adapter and UUID-fence reuse,
  failure/status/version/health/actor drift, both pacing waits, empty raw-capture admission,
  hostile-detail persistence, and completed-rerun work. The complete suite passed 1,650 tests;
  lockfile, formatting, lint, strict typing, dependency audit, and local health checks also passed.
  The drill uses no network, wall-clock sleep, operator path, operator data, or credential and
  makes no cross-database-atomicity, physical-durability, continuous-operation,
  automatic-recovery, or Phase 2 exit claim. TASK-037 remains blocked and authorization remains
  denied.

### TASK-055 — Fail-closed bounded public-HTTP response-header projection

- **Key:** `phase2.fail_closed_public_http_bounded_response_header_projection`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After the existing one-byte-sentinel body read and body-size decision, successful and
  `HTTPError` responses now use one shared bounded header snapshot before `HttpResponse`
  construction. Each path calls `headers.items()` once, starts its iterator once, requests no
  length hint, performs no direct message iteration or second pass, and pulls at most 101 times.
  Zero through 100 yielded pairs are accepted only while cumulative
  `len(name) + len(value)` is at most 65,536 Python characters. A yielded 101st pair fails before
  the pair is unpacked or either component is inspected; a 65,537th cumulative character fails
  immediately. Both limits raise exact sanitized
  `HttpTransportError("public HTTP response headers exceeded the configured limit")` without a
  partial response, retry, second body read, or later pull.

  Accepted order, duplicate names, original casing, empty strings, leading and trailing content,
  punctuation, Unicode, and `Retry-After` behavior remain exact. Body-read failure and body
  oversize retain precedence without header access. A successful-response limit failure has no
  direct cause or hidden context and exits its response context once. An `HTTPError` limit failure
  retains the originating provider error as both direct cause and active context, then attempts
  cleanup exactly once; a cleanup failure cannot replace it. Exceptions from `headers.items()`,
  iterator creation, and consumed pulls remain the same raw objects. On `HTTPError`, such a raw
  failure retains only the natural implicit provider-error context rather than being wrapped.
  Existing acquisition/read/protocol mappings, subclass identity boundaries, redirect behavior,
  cleanup-only mappings, and primary-failure precedence remain unchanged.

  Forty-one new deterministic cases bring the focused adapter suite from 518 to 559 tests. They
  cover zero, one, and 100 pairs; finite and endless 101st yields; pair-unpack poisoning; exact
  65,536 and immediate 65,537 character boundaries across names, values, cumulative Unicode, and
  multi-pair input; one-pass instrumentation; exact preservation; every header-origin seam; both
  body-precedence outcomes; both response paths; cause/context identity; and cleanup precedence.
  An isolated mutation audit killed all 24 of 24 mutants with zero survivors and zero harness
  errors. It covered removed or changed count and character limits, 101st-pair guard ordering,
  `>=` drift, omitted name or value volume, non-cumulative and UTF-8-byte counting, full
  materialization, second or direct iteration, projection before body read or size decision,
  success/error-path drift, normalization, reordering, wrong message or cause, cleanup replacing
  the primary failure, raw-origin wrapping, and `HTTPError`/`OSError` subclass-identity bypass.
  The complete suite passed 1,649 tests; lockfile, formatting, lint, strict typing, dependency
  audit, and local health checks also passed.

  This is only an adapter-controlled projection bound after standard-library parsing and prior
  allocation. It does not bound wire-header bytes, parser work or memory, total response or process
  memory, total wall-clock time, or provider work and adds no privacy, redaction,
  content-type/length/encoding, allowlist, hostname, DNS, IP-routability, or SSRF guarantee. No
  request, retry, endpoint, provider, dependency, runtime, credential, permission, TLS/proxy,
  TASK-037 authority, migration, or Stage 3 behavior changed.

### TASK-054 — Fail-closed public-HTTP maximum timeout policy

- **Key:** `phase2.fail_closed_public_http_maximum_timeout_policy`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/ports/http.py`, `src/wealth/adapters/http.py`,
  `src/wealth/adapters/binance.py`, `src/wealth/adapters/coinbase.py`,
  `src/wealth/adapters/binance_order_flow.py`, `tests/unit/test_http_adapter.py`,
  `tests/unit/test_binance_public_candles.py`,
  `tests/unit/test_coinbase_public_candles.py`,
  `tests/unit/test_binance_public_aggregate_trades.py`, `docs/contracts/MARKET_DATA.md`, plus the
  coordinated governance files and governance tests.
- **Result:** One provider-independent
  `MAX_PUBLIC_HTTP_TIMEOUT_SECONDS = 120.0` constant now governs the shared client and the Binance
  candle, Coinbase candle, and Binance aggregate-trade constructors. At all four boundaries, the
  new comparison follows TASK-041's finite-positive check. `NaN`, positive or negative infinity,
  zero, and negative values therefore retain exact context- and cause-free
  `ValueError("timeout_seconds must be finite and positive")`; an otherwise valid finite-positive
  value greater than 120 raises exact context- and cause-free
  `ValueError("timeout_seconds must be at most 120")`. The shared client rejects over-limit values
  before URL length or content, query, `Request`, opener, DNS, network, or filesystem work. Each
  provider constructor rejects them before endpoint validation, clock, query, injected HTTP,
  provider, or record work. Forty new deterministic cases bring the four focused files from
  640 to 680 tests: `test_http_adapter.py` has 518 tests (+9), Binance candle has 49 (+11),
  Coinbase candle has 54 (+9), and Binance aggregate-trade has 59 (+11). The over-limit corpus
  covers the next float above 120, 120.0001, the largest finite float, and a 1,001-digit integer at
  every boundary. Accepted identity coverage pins integer 1, fractional 0.25, the exact 10.0
  default, the next float below 120, exact built-in integer and float 120, and a float subclass at
  120. All five active provider request paths forward the configured accepted object unchanged,
  including subclass identity. An isolated mutation audit killed all 14 of 14 mutants with zero
  survivors and zero harness errors, covering a removed or changed cap, `>=` off-by-one, reordered
  finite validation, hardcoded or unshared module policy, a missing provider cap, float-subclass
  coercion or identity loss, wrong message, injected cause, late shared or provider validation,
  provider clock work before the cap, default drift, and forwarded-timeout drift. The task adds no
  exact numeric-type, subclass,
  coercion, rounding, unit-conversion, fallback, or total-wall-clock policy and does not separately
  bound DNS, multiple operations, caller/provider work, retries, waits, pacing, or rate budgets.
  URL/query/User-Agent, response body, header projection, redirect, cleanup, provider, endpoint,
  dependency, runtime, credential, permission, hostname/DNS/IP/SSRF, TASK-037 authority,
  migration, and Stage 3 behavior remain unchanged. Successful and `HTTPError` header projection
  still has no adapter-level pair-count or cumulative-character bound; TASK-055 governs that
  residual response-metadata risk.

### TASK-053 — Fail-closed public-HTTP bounded User-Agent validation

- **Key:** `phase2.fail_closed_public_http_bounded_user_agent_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After preserving `max_response_bytes` validation and its first precedence, the
  shared client now validates `user_agent` during construction by requiring an exact built-in
  `str`, then a length of 1 through 256 Python characters, then only inclusive visible ASCII
  U+0020 through U+007E. Every violation raises the exact context- and cause-free
  `ValueError("user_agent must be a built-in string of 1 to 256 visible ASCII characters")`
  before URL, query, `urlencode`, `Request`, private-opener, handler, DNS, network, or filesystem
  work. Exact-type rejection dispatches no caller string hooks; empty and 257-character values
  fail before character inspection. Sixty-six new deterministic cases within the 509-test
  adapter suite cover twelve invalid-response-limit precedence combinations; five invalid types,
  including a hostile string subclass; both invalid length boundaries; all 32 C0 controls, DEL,
  five representative non-ASCII characters, and two lone surrogates; one independent audit
  sweeping DEL through every position 0 through 255 in a maximum-length value; four accepted
  visible-ASCII cases covering lengths 1, 255, and 256 plus the complete visible-ASCII range; the
  exact 29-character default
  `"WEALTH/0.1 public-market-data"`; and one exact custom-header preservation path. One-character
  and 256-character values and the complete U+0020-through-U+007E range retain identity. An
  accepted custom value containing leading and trailing spaces and punctuation is forwarded
  exactly once as the sole `User-Agent` header while GET, `Accept`, URL, bounded sorted query,
  timeout, one bounded response read, one acquisition, redirect, error, cleanup, and direct-cause
  behavior remains unchanged. No value is normalized, trimmed, truncated, repaired, retried,
  replaced, redacted, or synthesized, and no privacy or total-header-block guarantee is made. No
  URL/query policy, hostname/provider allowlist, DNS/IP/public-routability or SSRF claim, IDNA,
  certificate, TLS/proxy, endpoint, dependency, provider, runtime, TASK-037 authority, migration,
  or Stage 3 behavior changed. Finite positive public-HTTP timeouts remain without an upper bound;
  TASK-054 governs that residual per-operation wait risk.

### TASK-052 — Fail-closed public-HTTP initial-target length bound

- **Key:** `phase2.fail_closed_public_http_initial_target_length_bound`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After finite-positive timeout validation and at the first line of the private
  initial-target validator, the shared client now measures the original string with
  non-polymorphic `str.__len__`. A target longer than 8,192 Python characters raises the exact
  context- and cause-free `ValueError("url must contain at most 8192 characters")` before literal
  membership or character scanning, `urlsplit`, hostname, username, port, or NFKC inspection,
  query access or serialization, `Request`, private-opener or handler work, DNS lookup, network
  access, or filesystem access. Lying-length and raising-length/content `str` subclasses prove
  that the true built-in string length is used without dispatching to caller overrides. Length
  intentionally precedes TASK-049 structure and TASK-050 port errors for an oversized target,
  while every target at or below the limit retains TASK-049 structure, parser-context suppression,
  TASK-050 port, and TASK-051 query precedence and exact errors. ASCII and multi-byte Unicode
  targets of exactly 8,192 characters preserve every original character through the existing
  sorted query, GET, `Accept`, `User-Agent`, timeout, one bounded response read, and one
  acquisition; corresponding 8,193-character targets fail without query or request work. An
  exact-limit valid target still reaches the existing query boundary. Nineteen new deterministic
  cases within the 443-test adapter suite cover all five invalid timeouts; two adversarial
  oversized subclasses and one exact-limit false-long subclass; three oversized combined-error
  forms; three exact-limit prior-error forms; ASCII and Unicode exact-8,192 and 8,193 boundaries;
  and exact-limit query precedence. The five active
  provider defaults are pinned to their exact unchanged lengths of 39, 42, 42, 45, and 48
  characters and remain accepted. The control counts Python characters rather than encoded bytes
  and makes no request-line compatibility or total-wall-clock claim. No target is normalized,
  truncated, repaired, retried, replaced, or redirected, and no URL-content, hostname, DNS, IP,
  SSRF, IDNA, TLS/proxy, endpoint, dependency, provider, runtime, TASK-037 authority, migration, or
  Stage 3 behavior changed. The configured User-Agent remains unbounded and without an exact
  runtime type or character policy; TASK-053 governs that residual request-construction risk.

### TASK-051 — Fail-closed public-HTTP bounded query serialization

- **Key:** `phase2.fail_closed_public_http_bounded_query_serialization`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After timeout, TASK-049 structural-target, and TASK-050 target-port validation and
  before `urlencode`, the shared client now takes one bounded query snapshot. It calls `items()`
  and starts its iterator once; it does not call `len(query)`, directly iterate the mapping, start
  a second item pass, or request a length hint, and it pulls at most 33 yielded items. It accepts zero
  through 32 exact built-in tuple pairs whose keys and values are exact built-in strings and whose
  combined key-plus-value length is at most 8,192 Python characters. A 33rd item, invalid pair
  shape or tuple subclass, non-string or string-subclass component, or 8,193rd character raises
  the exact context- and cause-free
  `ValueError("query must contain at most 32 built-in string pairs totaling at most 8192 characters")`.
  Rejection performs no `urlencode`, `Request`, private-opener, handler, DNS, network, or
  filesystem work and never partially serializes, repairs, or retries a query. Caller-originated
  failures, including `ValueError`, from `items()`, iterator creation, and the first, later, or
  33rd pull remain the same raw objects. Forty-two new deterministic cases within the 424-test
  adapter suite cover zero, one, and 32 pairs; finite and synthetic-unbounded 33rd items;
  count rejection before 33rd-item inspection;
  cumulative, key-only, value-only, and Unicode exact-8,192 and 8,193 character boundaries; nine
  invalid pair/type forms; five mapping-failure seams with both a custom runtime error and raw
  `ValueError`; all three earlier precedence boundaries; duplicate and content preservation
  through one sorted standard-library encoding; and all five active provider request variants
  with three through six pairs. Accepted request text, GET, `Accept`, `User-Agent`,
  timeout, response limit, HTTP-error, redirect, cleanup, and direct-cause behavior remains
  unchanged. The boundary adds no query-content, normalization, or multi-value policy and makes no
  total-wall-clock, hostname, DNS, IP-routability, or SSRF claim. The original initial URL text
  still has no configured size bound; TASK-052 governs that residual finite-work risk. Provider
  endpoints, dependencies, runtime wiring, TASK-037 authority, migration, and Stage 3 remain
  unchanged.

### TASK-050 — Fail-closed public-HTTP standard HTTPS target-port policy

- **Key:** `phase2.fail_closed_public_http_standard_https_target_port_policy`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After TASK-049 structural validation and before any query-mapping operation, the
  shared client now accepts only an omitted caller target port or an explicit numeric port that
  parses as 443. Structurally valid explicit ports 1, 80, 442, 444, 8,443, and 65,535, a
  zero-padded nonstandard port, and nonstandard IPv6 and IPvFuture ports raise the exact
  `ValueError("url must use the standard HTTPS target port")` with no direct cause or hidden
  context. They perform no query access or serialization, `Request` construction, private-opener
  work, DNS lookup, network access, or filesystem-handler work. Because the policy follows the
  complete structural validator, malformed, percent-encoded, empty, non-numeric, signed,
  Unicode-digit, zero, and greater-than-65,535 ports retain TASK-049's exact structural error and
  precedence. Implicit port 443 and explicit numeric 443, including zero-padded, mixed-case,
  IPv6, and IPvFuture forms, preserve the exact original URL text, sorted query, GET method,
  `Accept`, `User-Agent`, finite-positive timeout, one bounded read, and one acquisition. Tests
  also prove all five active provider default endpoints remain accepted. The policy constrains
  only the caller's target authority: a configured proxy peer may use a non-443 port, and default
  proxy and TLS behavior remains unchanged. No provider or hostname allowlist, DNS resolution,
  IP/public-routability or SSRF guarantee, IDNA policy, certificate pin, endpoint, dependency,
  provider behavior, response mapping, cleanup rule, runtime wiring, TASK-037 authority,
  migration, or Stage 3 behavior changed. Query serialization remains unbounded in item count and
  string volume; TASK-051 governs that residual finite-work risk.

### TASK-049 — Fail-closed public-HTTP initial request-target validation

- **Key:** `phase2.fail_closed_public_http_initial_request_target_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, and the coordinated governance files and
  governance tests.
- **Result:** After finite-positive timeout validation and before any query-mapping operation, the
  shared client now validates the original initial target as an absolute credential-free HTTPS
  URL with a non-empty CPython-parser hostname. It rejects every literal `?`, `#`, or backslash;
  every C0 or DEL control, Unicode whitespace character, and lone surrogate code point; relative,
  scheme-relative, and non-HTTPS targets; absent or malformed authorities; any userinfo; and
  empty, non-numeric,
  signed, Unicode-digit, zero, or greater-than-65,535 explicit ports. Parser and port failures
  become the exact context-suppressed
  `ValueError("url must be an absolute credential-free HTTPS endpoint without query or fragment")`.
  Every percent sign in the authority is rejected, covering encoded host characters, ports,
  userinfo delimiters, slashes, backslashes, and controls, as well as malformed escapes and IPv6
  zones, before urllib can reinterpret the authority. NFKC inspection of the authority
  additionally rejects compatibility characters that IDNA could emit as a percent sign,
  backslash, whitespace, C0, or DEL; the accepted URL is never reconstructed or normalized. A
  117-target invalid corpus
  proves the query remains untouched and `urlencode`, `Request`, the private opener, DNS, network,
  and filesystem handlers receive no work. A 63-target raw subset fails before `urlsplit`; focused
  parser tests prove failure context suppression; five invalid timeout cases retain their earlier
  precedence; and 18 valid targets preserve their exact text, sorted query, GET, `Accept`,
  `User-Agent`, timeout identity, one bounded response read, and one acquisition. This is a
  structural boundary, not a hostname, DNS, IP-routability, or SSRF guarantee: localhost, IPv4,
  IPv6, parser-accepted IPvFuture and DNS-label forms, Unicode and trailing-dot hostnames, and
  explicit ports 1 through 65,535 remain accepted. TASK-050 governs the remaining target-port
  policy. Proxy/TLS defaults, endpoints, TASK-043 through TASK-048 response and redirect mappings,
  provider behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, TASK-037
  authority, migration, and Stage 3 remain unchanged.

### TASK-048 — Fail-closed public-HTTP automatic redirect rejection

- **Key:** `phase2.fail_closed_public_http_automatic_redirect_rejection`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport now uses one private urllib opener whose no-follow
  handler rejects original 301, 302, 303, 307, and 308 responses before parsing `Location` or
  `URI`, reading or closing the redirect body, or opening another destination. Missing, empty,
  relative, same-origin, cross-origin, HTTPS-to-HTTP downgrade, FTP, unsupported-scheme, and
  malformed targets all remain the original response. The original 3xx enters the existing
  bounded `HTTPError` materialization path; an exact-limit body returns its original status,
  headers, and exact bytes, while a one-byte-oversize body raises
  `HttpTransportError("public HTTP error response exceeded the configured limit")` without
  truncated evidence. A deterministic five-status-by-fifteen-target real-opener matrix proves
  one original GET, one `max_response_bytes + 1` read, one cleanup attempt, no retry, no second
  read, and no follow. Additional tests preserve sanitized redirect-body read and cleanup
  mappings, direct causes, and primary-failure precedence; prove successful and non-redirect
  HTTP-error behavior, query, method, headers, timeout, proxy/TLS handler defaults; and prove in a
  fresh process that the process-global urllib opener is neither installed nor mutated. Response
  limits, endpoints, TASK-043 through TASK-047 mappings and exclusions, provider behavior,
  parsing, quality, storage, schemas, dependencies, runtime wiring, TASK-037 authority, migration,
  and Stage 3 remain unchanged. Initial request-target validation remains incomplete: the shared
  client does not yet parse and constrain its caller-supplied initial URL before request
  construction, and its private standard-library opener retains non-HTTPS scheme handlers;
  TASK-049 governs that residual risk.

### TASK-047 — Typed public-HTTP response-protocol failure mapping

- **Key:** `phase2.typed_public_http_response_protocol_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport now converts each real `BadStatusLine`, `LineTooLong`,
  and `UnknownProtocol` raised directly by `urlopen`, a successful-response body read, or an
  `HTTPError` body read into the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause. A
  deterministic three-exception-by-three-seam matrix proves one acquisition, one configured
  sentinel read on each body path, one cleanup attempt on the HTTP-error path, no retry or partial
  response, and no provider protocol detail in the public message. Adjacent-boundary tests prove
  direct base `HTTPException` and `InvalidURL` remain raw at all three seams, the three mapped
  protocol failures remain raw when raised by response entry or exit, and the same failures remain
  raw when raised only by HTTP-error cleanup. TASK-043 through TASK-046 mappings, TASK-045 cleanup
  and primary-failure precedence, response limits, timeouts, endpoints, queries, headers, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. Default redirects remain enabled: urllib may still
  drain a redirect body outside the adapter cap and contact a changed destination before returning
  a handle; TASK-048 governs that residual risk.

### TASK-046 — Typed pre-response public-HTTP incomplete-read mapping

- **Key:** `phase2.typed_public_http_pre_response_incomplete_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** A real `IncompleteRead` raised directly by `urlopen` now becomes the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause.
  Deterministic tests prove one acquisition call, no response handle or adapter body read, no
  retry, and absence of partial provider bytes and the expected-byte count from the public
  message. Adjacent-boundary tests prove `IncompleteRead` from response entry or exit and a direct
  base `HTTPException` remain raw, preventing a broad protocol catch. Existing body-read mappings,
  HTTP-error cleanup, response limits, timeouts, endpoints, queries, headers, retries, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. This task sanitizes only the resulting failure: default
  redirects remain enabled, the standard library may read a redirect body outside the adapter's
  response cap and follow a changed destination before returning a handle, and a separate
  governed redirect-policy task is still required.

### TASK-045 — Deterministic public-HTTP error-response resource closure

- **Key:** `phase2.deterministic_public_http_error_response_resource_closure`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** Every `HTTPError` processing path now makes one explicit cleanup attempt after no
  more than one `cap + 1` body read. A real built-in `HTTPError` smoke test and a counting
  `HTTPError` subclass prove exact-limit return, oversize failure, every supported read failure,
  later header failure, and close failure each invoke `close` exactly once; successful cleanup
  closes the underlying body before return or propagation. An `OSError`-family or `IncompleteRead`
  close-only failure becomes `HttpTransportError("public HTTP GET failed")` with that close error
  as direct cause; unsupported close-only failures remain unchanged. If any primary read,
  oversize, header, or processing failure already exists, no cleanup failure can replace its type,
  public message, or direct cause. Existing status, headers, body, read count, response limits,
  timeouts, endpoints, queries, retries, successful-response context management, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. A failed cleanup attempt is not claimed to have closed
  its resource.

### TASK-044 — Typed public-HTTP incomplete-body read-failure mapping

- **Key:** `phase2.typed_public_http_incomplete_body_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** A real `http.client.IncompleteRead` raised by either the successful-response body
  read or the `HTTPError` body read now becomes the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause.
  Deterministic path-symmetric tests prove one `cap + 1` read and one `urlopen` call, no retry or
  partial response, and absence of partial provider bytes and the expected-byte count from the
  public message. The successful-path handler is scoped only around `response.read`; a separate
  regression test proves an `IncompleteRead` raised during response-context entry remains
  unmapped. Existing
  TASK-043 mappings, exact-limit and oversize behavior, response limits, timeouts, endpoints,
  queries, headers, retries, resource closure, provider behavior, parsing, quality, storage,
  schemas, dependencies, runtime wiring, operator authority, migration, and Stage 3 remain
  unchanged.

### TASK-043 — Typed public-HTTP error-body read-failure mapping

- **Key:** `phase2.typed_public_http_error_response_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared transport now converts `URLError`, `TimeoutError`, and `OSError` raised
  while reading an `HTTPError` body into the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the read failure as the direct cause.
  Deterministic tests prove one `cap + 1` body read and one `urlopen` call, absence of untrusted
  detail from the public message, and no retry or partial response. A symmetric successful-body
  `OSError` regression test preserves the existing mapping, while the exact-limit and
  one-byte-oversize success and HTTP-error paths remain covered. `IncompleteRead`, which is not an
  `OSError`, remains outside this task and is the separately bounded next action. Response limits,
  timeouts, endpoints, queries, headers, retries, provider behavior, resource closure, parsing,
  quality, storage, schemas, dependencies, runtime wiring, operator authority, migration, and
  Stage 3 remain unchanged.

### TASK-042 — Strict bounded public-HTTP response-byte-limit validation

- **Key:** `phase2.strict_public_http_response_byte_limit_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public HTTP client now accepts only a built-in integer response limit from
  1 through the current/default hard ceiling of 2,000,000 bytes. Booleans, integer subclasses,
  integral and fractional floats, `NaN`, infinities, non-positive values, and larger integers fail
  during construction with one exact error. Deterministic tests preserve minimum, representative,
  default, and maximum valid limits and prove both successful and real `HTTPError` paths read
  `cap + 1`, accept an exact-limit body, and reject a one-byte-oversize body without returning
  truncation as evidence. This is a body-byte cap, not a total wall-clock or all-metadata memory
  bound. Provider constructors, timeouts, endpoints, queries, headers, retries, response content,
  parsing, canonical evidence, quality, storage, schemas, dependencies, runtime wiring, operator
  authority, migration, and Stage 3 remain unchanged.

### TASK-041 — Finite public HTTP timeout-boundary validation

- **Key:** `phase2.finite_public_http_timeout_boundary_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `src/wealth/adapters/binance.py`,
  `src/wealth/adapters/coinbase.py`, `src/wealth/adapters/binance_order_flow.py`,
  `tests/unit/test_http_adapter.py`, the three public-provider unit-test files,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport and all three active provider constructors now reject
  `NaN`, positive and negative infinity, zero, and negative timeouts with one exact error before
  request construction or provider HTTP work. A deterministic four-boundary matrix proves invalid
  values never reach `Request`, `urlopen`, or an injected HTTP client, while literal integer and
  fractional positive values are forwarded unchanged. The contract preserves standard urllib
  timeout semantics and does not claim a total wall-clock deadline. Endpoints, queries, headers,
  retry, pagination, range, budgets, parsing, canonical evidence, quality, storage, schemas,
  dependencies, runtime wiring, operator authority, migration, and Stage 3 remain unchanged.

### TASK-040 — Exact order-flow persistence-evidence validation

- **Key:** `phase2.exact_order_flow_persistence_evidence_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/application/order_flow_ingestion.py`,
  `src/wealth/ports/order_flow.py`, `tests/unit/test_order_flow_persistence_evidence.py`,
  `tests/integration/test_recoverable_public_trade_collection.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** Order-flow admission now requires a passing quality report, an exact coherent raw
  identity, and one ordered status-coherent outcome per canonical batch record, each bound to the
  corresponding incoming ID and record family. Missing, extra, duplicated, reordered,
  misidentified, wrong-family, contradictory, and conflicting evidence fails closed.
  Deterministic hostile-store tests cover the complete matrix while preserving valid zero-record
  windows, and recoverable public-trade collection proves malformed returned evidence leaves its
  durable cursor and completion counters unchanged and prevents a later request even when the
  nonconforming store physically wrote the first window. Returned-outcome validation does not
  independently prove physical durability, undo store mutations, or make the evidence and control
  databases atomic. Provider, network, retry, range, quality, canonical models, raw bytes, store
  implementations, schemas, dependencies, operator authority, migration, and Stage 3 remain
  unchanged.

### TASK-039 — Exact candle persistence-evidence validation

- **Key:** `phase2.exact_candle_persistence_evidence_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/application/ingestion.py`, `src/wealth/ports/market.py`,
  `tests/unit/test_historical_candle_persistence_evidence.py`,
  `tests/integration/test_recoverable_collection.py`, `docs/contracts/MARKET_DATA.md`, and the
  coordinated governance files and governance tests.
- **Result:** Historical candle admission now requires a passing quality report, an exact coherent
  raw-write identity, and one ordered status-coherent write outcome per batch candle, each
  matching its corresponding incoming ID. Missing, extra, duplicated, reordered, misattributed,
  contradictory, and conflicting outcomes fail closed. Deterministic hostile-store tests cover
  the full evidence matrix, and a recoverable collection test proves that incomplete returned
  evidence leaves the durable cursor and completion counters unchanged and prevents a later page
  even when the nonconforming store physically wrote the first page. The contract documentation
  states that returned-outcome validation does not independently prove physical durability, undo
  store mutations, or make the market and checkpoint databases atomic. Provider, retry,
  pagination, quality, canonical records, raw bytes, digests, lineage, store implementations,
  SQLite schemas and transactions, dependencies, order-flow ingestion, operator authority,
  migration, and Stage 3 remain unchanged.

### TASK-038 — Public-provider payload failure-boundary hardening

- **Key:** `phase2.public_provider_payload_failure_boundary_hardening`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Binance candles, Coinbase candles, and Binance aggregate trades now convert invalid
  UTF-8, malformed JSON, excessive nesting, and decoder `ValueError` failures into each
  provider's sanitized, non-retryable typed `INVALID_PAYLOAD` error. Deterministic no-network
  tests pin every decoder cause, exact public detail, preserved cause type, and aggregate-trade
  split semantics. Endpoints, queries, byte bounds, timeouts, HTTP retry behavior, valid-payload
  canonicalization, raw-byte lineage, digests, provider schemas, storage, runtime wiring,
  dependencies, operator paths, SQLite, TASK-037 authority, migration, and Stage 3 remain
  unchanged.

### TASK-036 — Synthetic operator-preflight authorization-request contract foundation

- **Key:** `phase2.canonical_utc_preflight_operator_authorization_request_contract_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One unused strict frozen proposal plan is pinned to the private exact TASK-035
  all-family bundle plan. A zero-argument pure builder emits exactly eight ordered immutable
  literal slots linked to the reviewed families plus fixed unselected snapshot-method,
  report-destination, and retention/disposal placeholders. Fixed states remain `proposal_only`,
  `none_proposal_only`, human approval `not_recorded`, operator-data access `not_authorized`, and
  the Stage 3 gate `not_satisfied`; successful construction, validation, review, or merge grants
  no authority. Deep strict validation rejects altered plans, ordinals, family/slot mappings,
  placeholders, subclasses, bypassed construction, extras, and any authority-state forgery. The
  eight symbolic slots prove synthetic family coverage only and do not assert that a future real
  deployment has one path per family; a later populated package must establish its own reviewed
  cardinality. No real path, path check, filesystem, SQLite, operator data, scan, adapter, report,
  manifest, serialization, runtime consumer, scanner, migration, schema change, or Stage 3 action
  was added.

### TASK-035 — Synthetic all-family candidate-census bundle reconciliation evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_candidate_census_bundle_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure bundle plan now pins the complete reviewed TASK-034 plan sequence
  and accepts only an exact built-in tuple of eight deeply valid family census results in
  canonical order. The result retains all eight TASK-034 inputs and their nested evidence
  unchanged while exactly reconciling eight families, 20 tables, 37 columns, total and exhaustive
  status counts, sorted source-offset and precision frequencies, and projectable epoch extrema.
  Deep validation rejects missing, duplicate, reordered, forged, subclassed, or altered sources,
  plans, counts, frequencies, and extrema before aggregation. Synthetic tests cover the exact
  one-row aggregate, all-empty families, mixed outcomes and bounds, hostile source sequences,
  deep forgery before aggregation, and post-TASK-034 no-I/O behavior. No filesystem, SQLite,
  operator scan, stored-projection comparison, row/instant/collision grouping, serialization,
  report, manifest, adapter, replacement, runtime consumer, migration, schema change, or Stage 3
  completion was added.

### TASK-034 — Synthetic canonical-candidate census evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_candidate_census_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Eight immutable pure family-scoped census plans now flatten the exact TASK-033
  declarations into one ordered summary per source-family timestamp column, collectively covering
  all eight families, 20 tables, and 37 columns, including genuinely empty columns. Every summary
  exactly reconciles its total, exhaustive candidate and parse status counts, bounded sorted
  source-offset and fractional-precision frequencies, and projectable epoch extrema while
  retaining the complete TASK-033 and nested TASK-030/031/032 evidence unchanged. Deep validation
  rejects forged plans, candidates, declarations, summaries, counts, frequencies, extrema,
  registry replacement, and reordered or missing evidence. Synthetic tests cover all families,
  empty and mixed columns, malformed and nullable inputs, signed and subminute offsets, precision,
  duplicate instants, calendar overflow, epoch bounds, and post-snapshot no-I/O behavior. No
  report, operator scan, grouping, collision identity, replacement, runtime consumer, migration,
  schema change, or Stage 3 completion was added.

### TASK-033 — Synthetic canonical-instant candidate evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_canonical_candidate_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure registry now wraps the exact eight TASK-032 plans and freezes the
  complete two-success/eight-nonprojectable status partition. Every ordered parse outcome retains
  its source evidence and receives either an exact built-in `datetime.UTC`, exact 27-character
  six-fractional-digit `Z` text, and exact epoch-microsecond triple; a typed year-boundary
  normalization overflow; or a source-not-projectable disposition. Epoch and text candidates
  round-trip through the TASK-028/029 primitives. Tests cover all eight families and 37 columns,
  positive, negative, and subminute offsets, exact calendar and epoch bounds, equal instants with
  distinct spellings retained separately, every prior failure status, forged nested evidence,
  registry replacement, ordering, and post-snapshot no-I/O behavior. No collision grouping,
  report, operator scan, replacement byte, runtime consumer, migration, or Stage 3 completion was
  added.

### TASK-032 — Synthetic SQLite timestamp parse-evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_parse_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure registry now binds the exact TASK-031 plans for all eight store
  families to 20 offset-preserving Python `isoformat` text columns, 15 fixed-UTC `isoformat` text
  columns, two signed epoch-microsecond integer columns, and the exact five nullable declarations.
  Manual component parsing plus exact writer round trips preserve offset spelling and subsecond
  offsets without normalization. Every source cell receives one typed outcome for aware text,
  fixed-UTC policy mismatch, naive text, declared absence, malformed UTF-8/text/epoch bytes,
  calendar-range overflow, or unexpected SQLite storage. Deep validation rejects forged plans,
  snapshots, rows, keys, cells, outcomes, and public-registry replacement before parsing.
  Synthetic end-to-end tests cover all 37 columns and hostile TEXT, NULL, INTEGER, REAL, and BLOB
  evidence while retaining exact bytes, row order, TASK-030 fingerprint, TASK-031 plan, and
  snapshot identity. The module performs no I/O, has no runtime consumer, scans no operator data,
  emits no replacement bytes, and does not complete Stage 3.

### TASK-031 — Synthetic SQLite timestamp-byte evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One pinned strict extraction plan per TASK-030 family now declares all 20 direct
  timestamp-bearing tables and 37 timestamp columns. The unused generated-fixture-only inspector
  fingerprints and extracts through the same immutable connection and exact whole-file snapshot,
  fails before row access unless exactly one expected family matches, and temporarily authorizes
  only each declared stable key and timestamp target. Bounded deterministic evidence preserves
  SQLite `typeof`, exact `hex(CAST(column AS BLOB))`, byte length, row-key bytes, and snapshot
  linkage without materializing a raw timestamp value. Tests cover all layouts, NULL, INTEGER,
  REAL, TEXT, BLOB, malformed text bytes, ordering, bounds, oversized cells, mismatches,
  wrong-family and ambiguity rejection, and unchanged source/directory evidence. No operator
  database, parser, report, manifest, runtime consumer, migration, or repair was added, and Stage
  3 remains incomplete.

### TASK-030 — Synthetic read-only SQLite preflight fingerprint foundation

- **Key:** `phase2.canonical_utc_preflight_fingerprint_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Strict frozen version-1 contracts now keep the expected family identity separate
  from observed evidence. A direct `mode=ro&immutable=1` inspector fingerprints encoding,
  application and user versions, exact typed marker bytes, normalized DDL, every schema object,
  tables, columns, foreign keys, explicit and implicit indexes, and triggers for all eight
  generated SQLite layouts. Exact pinned digests reject missing, extra, renamed, altered,
  spoofed, combined, wrong-family, or ambiguous layouts before timestamp rows can be read. Source
  hash, size, modification time, file identity, directory entries, and sidecar absence are
  reverified; an authorizer denies writes, temporary objects, `ATTACH`, and write pragmas. The
  foundation remains unused, scans no operator database or timestamp row, writes no report, and
  does not complete Stage 3.

### TASK-029 — Additive exact epoch-microsecond projection primitives

- **Key:** `phase2.canonical_utc_epoch_microsecond_primitives`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** The isolated canonical-UTC module now exposes exact signed bounds plus integer-only
  projection and inverse decoding between strict fixed-`datetime.UTC` values and Unix-epoch
  microseconds. Strict type and range handling rejects booleans, non-integers, and values outside
  Python's calendar; deterministic, property-style, and hostile-subclass tests prove exact
  negative/zero/positive landmarks, full-range round trips, one-microsecond distinction, and
  monotonic order. No runtime consumer, model, serialized byte, digest, identity, schema, query,
  projection, migration, or stored record changed.

### TASK-028 — Additive canonical UTC codec primitives

- **Key:** `phase2.canonical_utc_codec_primitives`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One isolated pure module now provides strict fixed-`datetime.UTC` validation,
  explicit aware-input normalization, exact six-fractional-digit RFC 3339 `Z` serialization, and a
  strict canonical parser. Exhaustive deterministic and property-style tests cover offsets,
  named/rule-based zones, folds, calendar limits, malformed text, exact round trips, and hostile
  datetime subclasses. No existing runtime path imports or calls the helpers, and no model,
  serializer, digest, identity, schema, projection, query, or stored record changed.

### TASK-027 — Canonical UTC clock-boundary enforcement

- **Key:** `phase2.canonical_utc_clock_boundary_enforcement`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One shared exact-`datetime.UTC` assertion now guards every direct injected-clock
  read in the scoped foundation, application, rate-budget, provider, and public-trade boundaries.
  Invalid initial values fail before IDs or downstream mutations; invalid later reads fail before
  the next side effect; provider and application error mappings remain typed. Persisted models,
  request acceptance, JSON, digests, keys, schemas, projections, and stored data are unchanged.

### TASK-026 — Canonical UTC boundary inventory and migration plan

- **Key:** `phase2.canonical_utc_boundary_inventory_and_migration_plan`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0027-canonical-utc-boundary-and-migration-plan.md`
- **Result:** The repository now has an evidence-backed inventory of every discovered
  timestamp-bearing model, clock, provider edge, JSON/text boundary, SQLite projection, order,
  index, cursor, and test path. It selects Python datetimes in the fixed `datetime.UTC` zone, fixed
  microsecond-precision RFC 3339 `Z` text, and derived epoch-microsecond SQL projections as the
  target, with staged compatibility readers, preflight, quarantine, collision handling, digest
  versioning, backup, rollback, and migration verification. No runtime, schema, or data migration
  was performed.
- **Inventory:** `docs/CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md`

### TASK-025 — Typed public-trade transition-history reader

- **Key:** `phase2.public_trade_transition_history_reader`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0026-typed-public-trade-transition-history.md`
- **Result:** The existing append-only SQLite transition ledger is now exposed through an
  immutable typed record and a read-only port with ascending contiguous checkpoint-version pages,
  an exclusive cursor, actor-authority and lifecycle validation, strict bounds, restart behavior,
  and fail-closed projection, canonical-record, continuity, and corruption checks. The existing
  schema is unchanged.

### TASK-024 — Public-trade checkpoint orchestrator

- **Key:** `phase2.public_trade_checkpoint_orchestrator`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0025-bounded-public-trade-checkpoint-orchestration.md`
- **Result:** One explicitly invoked bounded application flow now composes the public-trade range
  collector, durable request budget, market-evidence admission, and restart-safe checkpoint
  control with policy validation, UUID fencing, evidence-first progress, typed outcomes, and
  injected UTC time.

## Queued, Not Yet Approved

- Implement a production physical continuous public-trade stream-store adapter only after
  TASK-064 passes its exact generated/test-environment schema, transaction, crash, query-bound,
  backup/restore, same-format generation-copy, and synthetic-workload gates, and a separately
  governed task approves the target path/VFS/filesystem and power-loss evidence, operational
  capacity/checkpoint values, backup boundary, retention, and any incompatible migration with the
  required reviews and approvals.
- Implement continuous public-trade runtime collection only through a separately promoted task
  with the complete ADR-0028/0029 operational evidence and approvals.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
