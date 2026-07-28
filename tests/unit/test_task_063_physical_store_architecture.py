"""Governance checks for the non-executable TASK-063 physical-store decision."""

import re
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ADR_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "decisions"
    / "0031-continuous-public-trade-stream-physical-store-architecture.md"
)


def _source() -> str:
    return ADR_PATH.read_text(encoding="utf-8")


def _prose() -> str:
    return " ".join(_source().split())


def test_adr_0031_selects_one_bounded_dedicated_sqlite_design() -> None:
    source = _source()
    prose = _prose()

    assert "# ADR 0031: Continuous Public-Trade Stream Physical Store Architecture" in source
    assert "- **Status:** Accepted" in source
    assert "one dedicated SQLite database per physical stream-store generation" in prose
    assert "Python's standard-library `sqlite3` binding" in prose
    assert "local to one host" in prose
    assert "no network filesystem" in prose
    assert "| journal mode | WAL" in source
    assert "| synchronous | `FULL`" in source
    assert "| writer behavior | explicit `BEGIN IMMEDIATE`" in source
    assert "zero busy timeout and no busy-handler retry" in prose
    assert "Exact source ID is recorded and must contain the WAL-reset fix" in prose


def test_adr_0031_preserves_exact_bytes_identity_and_full_epoch_range() -> None:
    source = _source()
    prose = _prose()

    assert "epoch milliseconds: `0` through `9223372036854775807`" in prose
    assert "causal versions: `1` through `9223372036854775807`" in prose
    assert "selects only `stream_start_epoch_ms` as an epoch-valued SQL scalar projection" in prose
    assert (
        "Current `cursor_epoch_ms` and optional attachment `window_start_epoch_ms` and "
        "`window_end_epoch_ms` are deliberately not SQL scalar projections"
    ) in prose
    assert "History rows likewise have no scalar cursor or attachment-window columns" in prose
    assert (
        "Every TASK-059 epoch-millisecond coordinate and every causal version is stored"
        not in source
    )
    assert "No epoch value is converted to `datetime`" in prose
    assert "floating point" in prose
    assert "epoch microseconds" in prose
    assert "Original record and envelope BLOBs are authoritative" in prose
    assert "exact 16 bytes of `UUID.bytes` in network byte order" in prose
    assert "UTF-8 and the `surrogatepass` error handler" in prose
    assert "unsigned four-byte big-endian length" in prose
    assert "fixed field count, order, and length framing make the projection injective" in prose


def test_adr_0031_maps_the_frozen_port_without_redefining_it() -> None:
    source = _source()
    prose = _prose()
    public_values = (
        "ContinuousPublicTradeStreamIdentityV1",
        "ContinuousPublicTradeStreamExpectationV1",
        "ContinuousPublicTradeStreamStoredEnvelopeV1",
        "ContinuousPublicTradeStreamStoredCreationV1",
        "ContinuousPublicTradeStreamStoredTransitionV1",
    )

    for public_value in public_values:
        assert f"`{public_value}`" in source
    for mapping in (
        "create/CAS commands",
        "load/audit queries",
        "create/CAS receipts",
        "current view",
        "audit continuation",
        "audit page",
        "outcome and retry disposition",
        "typed evidence scopes",
        "external evidence/attestation",
    ):
        assert f"| {mapping} |" in source
    assert "It does not alter TASK-059 behavior, TASK-061 bytes or digest domains" in prose
    assert "ADR 0030 values or outcomes" in prose
    assert "new physical digest may replace an original TASK-061 BLOB" in prose


def test_adr_0031_constraint_binds_creation_tail_and_predecessor_witnesses() -> None:
    prose = _prose()

    assert "creation witness is an immutable bounded-read copy" in prose
    assert "deferred composite foreign key binds its stream key, fixed version one" in prose
    assert (
        "current version, current-record digest, current-envelope digest, and current history root"
        in prose
    )
    assert "current-record and current-envelope BLOBs to equal the tail row's original record" in (
        prose
    )
    assert "direct-predecessor-record witness BLOB and its recomputed record digest" in prose
    assert "self-referential, non-cascading foreign key" in prose
    assert "requires the witness BLOB, entry kind, version, and digest" in prose
    assert "predecessor witness is deliberate bounded-read redundancy" in prose
    assert "witness is not returned as another page record" in prose
    assert "requires the witness to equal that row's canonical record bytes" in prose
    assert "Arbitrary out-of-band page-file tampering is not universally detectable" in prose


def test_adr_0031_keeps_reads_and_writes_physically_bounded() -> None:
    prose = _prose()

    assert "A successful selected-stream view uses one stream row and at most three" in prose
    assert "candidate predecessor produce at most five distinct history rows" in prose
    assert "new_count = min(limit, current_version)" in prose
    assert "new_count = min(limit, remaining)" in prose
    assert "never evaluates the potentially overflowing expression" in prose
    assert "exactly one overlap plus exactly `new_count` new rows" in prose
    assert "absolute maximum of 101" in prose
    assert "missing or extra required retained row is `CORRUPT`" in prose
    for prohibited_shape in (
        "`LIMIT n+1`",
        "`COUNT`",
        "maximum-version discovery",
        "unbounded iteration",
        "total count",
        "lookahead",
        "offset pagination",
        "a second history-row query",
    ):
        assert prohibited_shape in prose
    assert "No `REPLACE`, `INSERT OR IGNORE`, merge, repair, normalization, or upsert" in prose


def test_adr_0031_keeps_closed_outcomes_and_retries_fail_closed() -> None:
    source = _source()
    prose = _prose()
    outcomes = (
        "UNSUPPORTED_VERSION",
        "CORRUPT",
        "UNAVAILABLE",
        "CONFLICT",
        "IDENTITY_CONFLICT",
        "NOT_FOUND",
        "ANCHOR_CONFLICT",
        "DUPLICATE",
        "INSERTED",
        "UPDATED",
        "FOUND",
        "PAGE",
        "AT_TAIL",
    )

    for outcome in outcomes:
        assert f"`{outcome}`" in source
    assert "any malformed located material is `CORRUPT`" in prose
    assert "only completely valid disagreement can become" in prose
    assert "classify a missing candidate row or missing required candidate predecessor" in prose
    assert "retained history gap and therefore `CORRUPT`" in prose
    assert "| Operation | `NOT_REQUIRED` | `DO_NOT_RETRY` | `EXACT_REQUEST_ONLY` |" in source
    assert "Only `UNAVAILABLE` carries `EXACT_REQUEST_ONLY`" in prose
    assert "grants no retry authority, delay, loop, recovery, or mutation" in prose


def test_adr_0031_freezes_crash_and_lost_ack_evidence_before_adapter() -> None:
    prose = _prose()

    for seam in (
        "before transaction or before first write",
        "after stream/current insert but before creation-history insert",
        "after creation-history insert but before create commit",
        "after transition-history insert but before current update",
        "after current update but before CAS commit",
        "during commit with an injected connection/process failure",
        "after commit before accepted result reaches the caller",
        "two same-natural-identity creates",
        "two CAS commands for one prior",
        "writer/checkpointer concurrency",
        "disk full, readonly, busy, lock, and injected I/O failure",
    ):
        assert seam in prose
    assert "exact old or exact new state; never partial" in prose
    assert "the unchanged request is historical `DUPLICATE`" in prose
    assert "Process-kill evidence is not a power-loss durability claim" in prose


def test_adr_0031_requires_verified_backup_restore_and_generation_migration() -> None:
    prose = _prose()

    assert "SQLite's Online Backup API" in prose
    assert "Raw copying, copying only the main file" in prose
    assert "`integrity_check` with exact `ok` result" in prose
    assert "an empty `foreign_key_check`" in prose
    assert "complete paginated decoding of original bytes" in prose
    assert "an independent restore drill into another isolated generation" in prose
    assert "No in-place schema migration or automatic open-time upgrade is allowed" in prose
    assert "creates a separate destination generation" in prose
    assert "shadow-reads old and new generations at one recorded watermark" in prose
    assert "changes an external atomic routing marker only after separate approval" in prose
    assert "failed migration never repairs, deletes, rewinds, advances, or normalizes" in prose


def test_adr_0031_is_preserve_all_and_requires_finite_capacity_evidence() -> None:
    prose = _prose()

    assert "Version one is preserve-all" in prose
    assert "reject normal update or delete of history and reject stream deletion" in prose
    assert "no logical compaction, downsampling, root-only substitution" in prose
    assert "theoretical limits and a 4,096-byte page size are not capacity evidence" in prose
    assert "number of streams and natural identities" in prose
    assert "database, WAL, backup, restore, and side-by-side migration byte budgets" in prose
    assert "uses only generated non-operator records" in prose
    assert "maximum physical rows including creation, current, and predecessor witness copies" in (
        prose
    )
    assert "missing, stale, target-mismatched, or capacity-exceeding result blocks" in prose


def test_adr_0031_names_owned_fail_closed_preimplementation_gates() -> None:
    source = _source()
    prose = _prose()
    gates = (
        "exact schema and fingerprint",
        "SQLite/runtime security",
        "path/filesystem identity",
        "atomicity and lost acknowledgement",
        "bounded reads",
        "corruption mapping",
        "backup and independent restore",
        "migration and rollback",
        "retention",
        "capacity and checkpoints",
        "authority exclusions",
    )

    assert "| Gate | Owner | Evidence required | Failure disposition |" in source
    for gate in gates:
        assert f"| {gate} |" in source
    assert "remain named prerequisites" in prose
    assert "does not invent deployment values" in prose


def test_adr_0031_contains_no_executable_schema_or_capability_claim() -> None:
    source = _source()
    prose = _prose()

    assert "This ADR intentionally contains no executable DDL" in prose
    assert "```sql" not in source.lower()
    assert (
        re.search(
            r"(?im)^\s*(?:CREATE|ALTER|DROP)\s+(?:TABLE|INDEX|TRIGGER)\b",
            source,
        )
        is None
    )
    assert "This decision adds no production source or adapter" in prose
    assert "database or schema creation" in prose
    assert "filesystem or provider I/O" in prose
    assert "No physical capability or readiness claim enters the repository" in prose
    assert "## Explicit Non-Goals" in source
    assert "TASK-037" in prose
    assert "authorization remains denied" in prose
