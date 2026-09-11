# Runtime revalidation — 2026-09-11

## Bounded task contract

The project owner requested an independent fresh assessment, efficiency improvements,
cross-platform validation, GitHub inspection, and connection checks on 2026-09-11.
This review starts from main `6a43b99e20b2d8621802ac8c799715ff752be7c5`,
on branch `agent/astra-revalidation`. It does not consume or change draft PR #68,
TASK-064 evidence generations, TASK-037 authorization, or production data.

Scope: bounded public-trade window planning, retained SQLite candle identity checks,
Windows development setup and CI, and focused regression tests. No schema migration,
new provider, trading capability, credentials, deployment, or automatic collection.
Acceptance: focused and full tests, format, lint, type checks, dependency audit,
local health, independent review, and both CI platforms. This is a review candidate,
not an accepted merge or production-readiness claim.

## Changes and rationale

1. **Bound initial window allocation.** A valid seven-day request with one-millisecond
   windows previously constructed 604,800,000 window objects before checking even a
   one-request budget. Initial windows are now lazy; only adaptive split children
   are queued. Chronological admission, retry/pacing limits, and the exact pending
   resume window are preserved. A guarded regression fails on a third construction
   instead of risking an out-of-memory reproduction. Planning clips the duration
   before datetime addition, including at the maximum supported datetime.
2. **Validate SQLite retained identity.** Both stream reads and duplicate/conflict
   classification now bind all seven stored identity projections to canonical JSON.
   Valid JSON with inconsistent identity fails with `CORRUPT_RECORD`; batch rollback
   retains existing lineage and does not admit a new raw payload. No storage schema,
   timestamp normalization, or legitimate duplicate semantics changed.
3. **Make Windows checks reproducible.** `.gitattributes` pins text to LF; a Windows-only
   development `tzdata` dependency supplies the database required by existing tests.
   Python documents this platform requirement in its
   [zoneinfo data-source guidance](https://docs.python.org/3.13/library/zoneinfo.html#data-sources).
   CI adds Windows while preserving the existing Ubuntu check name and every gate.
4. **Report an unavailable OS test precisely.** The fixture symlink test skips only
   when creation actually fails with Windows error 1314 (missing symlink privilege).
   Every other error still fails. The test remains active on capable Windows hosts
   and Linux; no machine security setting was changed.

## Validation

- Initial checkout: 141 Python format failures caused solely by CRLF conversion;
  LF normalization resolved them without content changes.
- Initial test collection failed because Windows lacked `tzdata`; the declared,
  locked dependency resolved collection.
- Focused range/contracts/recovery suite: 46 passed before the additional datetime
  boundary test was added.
- First full suite: 2,099 passed, 2 skipped, 1 failed in 401.22 seconds. The failure
  was the unavailable symlink privilege addressed above.
- Final full suite: **2,100 passed, 3 skipped in 295.83 seconds**. The skips are
  two unavailable Windows symlink-creation cases and one adversarial open-file
  replacement prohibited by Windows. They are not counted as passing.
- Formatting: 141 files formatted; lint passed; mypy passed on 141 source files.
- Lockfile check passed. Dependency audit found no known vulnerabilities or adverse
  project statuses among the 22 audited packages.
- Synthetic local `wealth-health` passed; collector-health help entry point works.
  Collector-health behavior is covered by the integration suite, not a production DB probe.
- Independent static review covered lazy ordering, pacing, checkpoint boundaries,
  SQLite transaction behavior, dependency scope, LF, and CI portability. Its datetime
  overflow finding was fixed and re-reviewed without remaining blockers.

## Public-provider smoke and limitations

Two bounded, unauthenticated Spot candle adapter probes requested one closed minute,
2026-07-24 12:00:00Z through 12:01:00Z. No database, account, or financial action was used.
No real response body was retained, copied into fixtures, or published.

- Binance Spot: one expected canonical candle, **PASS**.
- Coinbase Exchange Spot: **FAIL**, typed `invalid_payload`, a returned row began at
  or after the exclusive requested end. A diagnostic reproduction confirmed the
  same typed boundary rejection. This is not a successful provider integration.

The current [official candle contract](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles),
reviewed 2026-09-11, describes bucket start times and start/end timestamps but does
not resolve the needed end-boundary/rounding semantics. A development proposal to
encode the query end one second earlier preserves bucket starts under simple
inclusive/exclusive timestamp selection, but does not prove upstream rounding or
whole-bucket aggregation behavior. **That proposal was not implemented.**
Coinbase operational collection was not started or resumed; its code and v1 fixtures
remain unchanged, following the schema-drift runbook. Resolve the provider contract
and review a bounded query-translation change before claiming compatibility.

Other public request variants were exercised offline by the suite, not all live.
No authenticated exchange workflow, live collector, trading, deployment, or broad
repository security scan was performed. Existing draft PR #68 remains unmerged and
not acceptance-ready despite its earlier green CI.

## Rollback and next decisions

Rejecting this branch leaves main unchanged. After an accepted merge, revert this
candidate commit and re-sync its locked dependencies; no data migration or repair
is required. Keep provider incident evidence and do not rewrite stored records.

Required follow-ups: review this exact candidate and CI; resolve Coinbase query
semantics separately; continue TASK-064 only within its own acceptance contract.
Do not infer TASK-064/TASK-037 approval or production readiness from these fixes.
