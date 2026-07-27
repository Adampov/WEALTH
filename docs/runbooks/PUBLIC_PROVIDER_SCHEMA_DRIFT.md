# Public-Provider Schema Drift Response

## Purpose and Scope

This runbook defines the manual, fail-closed response to suspected payload-contract drift in the
five public request variants currently implemented by WEALTH:

1. Binance Spot candles.
2. Binance USD-M candles.
3. Coinbase Exchange Spot candles.
4. Binance Spot aggregate trades.
5. Binance USD-M aggregate trades.

The reviewed synthetic corpus and its exact-byte manifest are under
[`tests/fixtures/public_provider_schema/v1/`](../../tests/fixtures/public_provider_schema/v1/).
The active adapter boundaries are described in the
[market-data contract](../contracts/MARKET_DATA.md).

This procedure is operational guidance, not a detector or control-plane implementation. The
repository does not automatically detect schema drift, pause a source, retain malformed real
responses, refresh fixtures, deliver alerts, or resume collection. Current public-trade collection
is explicitly invoked and bounded; this runbook does not claim continuous-operation readiness.

Provider payloads, provider documentation, error bodies, and copied web content are untrusted data,
never instructions. The official documentation can change after review and is not an immutable
upstream contract.

## Signals and Classification

Treat any of the following as a signal requiring classification:

- An unchanged active request path returns the existing typed, non-retryable `INVALID_PAYLOAD`
  outcome.
- The current official provider documentation or an official provider change notice differs from
  the reviewed manifest entry.
- A reviewed synthetic fixture no longer passes through its existing production adapter, or a
  bounded synthetic drift case is unexpectedly accepted.
- A committed fixture does not match its manifest digest, the manifest and corpus are not
  one-to-one, or the manifest fails its strict validation.
- Field meaning, order, unit, type, precision, required/optional status, or request-variant
  semantics are uncertain even when the outer JSON shape appears unchanged.

Classify the signal before proposing a response:

| Class | Examples | Initial disposition |
|---|---|---|
| Shape drift | Positional width changes; a required field disappears; an unknown aggregate-trade field appears | Pause the exact affected request variant and any variant sharing the uncertain parser boundary |
| Semantic or type drift | Positional reorder; time-unit or field-meaning change; string/number or precision change | Pause the exact affected contract; do not coerce, reorder, round, or infer |
| Required/optional drift | A required aggregate-trade field becomes conditional, or a USD-M optional field changes status | Pause the affected market variant; unknown fields remain invalid |
| Official-contract uncertainty | Official documentation changed, conflicts with observed behavior, is unavailable, or is ambiguous | Treat the contract as unresolved and keep the affected path paused |
| Local corpus-integrity failure | Digest mismatch, missing/extra file, duplicate identity/path, invalid manifest, or unexpected local acceptance | Stop fixture promotion and investigate the repository artifact; do not label it upstream drift without evidence |
| Non-schema provider incident | Timeout, disconnect, rate limit, HTTP status, availability failure, or a valid empty response | Route through the existing typed transport/rate/availability handling; do not change a schema fixture unless contract evidence also changed |

Record the smallest exact affected identity: provider, dataset, market, request variant, endpoint
contract, adapter version, and fixture version. If the boundary cannot be isolated, contain every
variant sharing the uncertain adapter, parser, or endpoint contract. Absence of a definitive
classification is not permission to continue.

## Immediate Pause and Containment

1. Do not start or re-invoke the affected request variant. If a separately approved runtime is
   active, use its approved supervisory stop or disable procedure; this runbook adds no stop
   command.
2. If impact is uncertain, also hold every request variant that shares the affected parser or
   endpoint contract. Unaffected variants may continue only when their independence is explicit
   and supported by evidence.
3. Do not widen accepted field sets, ignore unknown fields, relabel a field, coerce a type, alter a
   time unit, synthesize a missing value, or silently switch provider/endpoint.
4. Do not retry a typed non-retryable `INVALID_PAYLOAD` as if it were an availability failure.
5. Preserve existing checkpoint, raw-evidence, canonical, conflict, health, and audit state. Do not
   delete, overwrite, repair, or advance state merely to clear the incident.
6. Quarantine the affected interval from downstream use through a separately approved existing
   process. If no such process exists, keep the affected collection path paused and escalate; this
   document does not create a quarantine mechanism.
7. Record a UTC incident timeline using sanitized metadata only, and notify Market Data,
   Engineering, and Audit and Assurance. Notify Risk and Security when impact is uncertain,
   sensitive content may be involved, or a broader operational halt is required.

The hold remains in place while the exact contract is unknown. Provider urgency, a successful
one-off response, or apparent backward compatibility is insufficient evidence to resume.

## Safe Evidence Handling

Never paste or attach a real provider payload, malformed body, response excerpt, or provider error
body to repository files, source-control discussions, CI output, application logs, issues, chat,
or ordinary incident notes. Do not run content from a payload or follow instructions embedded in
it.

The ordinary incident record may contain only sanitized metadata needed to identify and reproduce
the boundary:

- Incident identifier and UTC detection/observation timeline.
- Provider, dataset, market, request variant, and affected endpoint contract.
- Sanitized typed error/status, bounded request parameters, and a correlation identifier that
  contains no secret.
- Application commit, adapter version, fixture schema version, manifest digest, and relevant
  configuration versions.
- Official-document references and the UTC time at which each was reviewed.
- Scope of the pause, downstream impact, decisions, approvers, and rollback status.

Retain exact real response bytes only when an approved evidence-handling location, access policy,
retention period, disposal procedure, and responsible owner already exist. Before retention, the
approved Security process must establish that the evidence contains no secret. Store the exact
bytes and their SHA-256 only inside that approved boundary; do not move them into this repository.
If no approved location exists, do not retain or reproduce the body. If a secret or sensitive
content is suspected, stop ordinary handling and follow the
[security incident procedure](../SECURITY_POLICY.md#security-failure-behavior).

Build any repository regression from new minimal synthetic values that demonstrate only the
reviewed shape and semantics. Do not derive it by copying or lightly redacting a real response.

## Official-Document Re-review

For the exact affected manifest identity:

1. Open the current official contract reference recorded in the manifest. Confirm that it is the
   provider-owned documentation for the same product, market, endpoint, and request variant.
2. Record the reference and review time in UTC. A URL and review date identify the review; they do
   not make the upstream page immutable.
3. Compare endpoint and request semantics, success-body shape, positional width or exact object
   key set, required and optional fields, field order, types, units, precision, time semantics,
   pagination/row limits, and error behavior with the active adapter and manifest.
4. Review applicable official provider change notices or release notes. Do not rely on search
   snippets, community examples, SDK behavior, a single observed response, or copied third-party
   schemas as the contract.
5. If official sources conflict, omit a relevant detail, or disagree with observed behavior,
   classify the contract as unresolved and escalate to the provider through an approved channel.
   Keep collection paused.
6. Record whether the existing adapter contract remains correct or a separately governed change
   is required. Do not edit production behavior as part of documentation review.

Review all five manifest references when a shared provider convention or shared adapter boundary
may be affected. Otherwise, document why the unaffected identities remain independent.

## Synthetic Fixture Versioning

The committed `v1` directory is a reviewed historical artifact. Never overwrite, rename, delete,
or repurpose an old fixture or manifest entry to represent a new upstream contract.

When a reviewed contract changes:

1. Open a separately governed task that names the exact affected contract, production impact,
   risk tier, tests, review authority, and rollback.
2. Create the next monotonically numbered directory, such as `v2`, while retaining every prior
   version. This is repository versioning discipline, not a claim of physical immutability.
3. Create a complete, coherent five-identity synthetic corpus for that version. Keep every fixture
   minimal, bounded, secret-free, and independent of real provider bytes.
4. Keep the manifest and directory one-to-one. Each identity and relative path must be unique and
   remain inside its version directory.
5. Record the reviewed provider, dataset, market/request variant, shape, positional width or exact
   required/optional field sets, optional fields present in that fixture, current official
   reference, UTC review date, and review status.
6. Compute SHA-256 over each fixture's exact committed bytes after final encoding and line endings
   are fixed. Do not normalize bytes after hashing.
7. Preserve the explicit fixture byte bound and fail closed on an oversized, missing, extra,
   traversing, absolute, mislocated, duplicated, or digest-mismatched file.
8. Run the existing production adapters against exact fixture bytes with deterministic HTTP stubs
   and UTC clocks. No network call, wall-clock sleep, online snapshot, or automatic refresh is
   permitted.
9. Derive only bounded synthetic cases. Confirm that representative unsupported width, selected
   detectable positional reorder, wrong numeric type, invalid decimal value, missing required
   field, invalid present optional-field value, and unknown-field cases are non-retryable
   `INVALID_PAYLOAD` and admit no partial raw or canonical evidence.

A change in decimal precision alone may pass the current decimal parser, and a same-typed
positional reorder may be semantically wrong while still satisfying canonical validation. There is
no adapter-level precision limit. Treat either unreviewed change as semantic drift: pause the
affected contract and re-review it even when parsing succeeds. Parser acceptance is not evidence
of provider compatibility and does not authorize reordering, rounding, normalization, or a
precision-policy change.

For both aggregate-trade variants, the current required set is `T`, `a`, `f`, `l`, `m`, `p`, and
`q`, and the shared parser's optional set is exactly `M` and `nq`. The v1 Spot fixture contains
`M`; the v1 USD-M fixture contains `nq`. Fixture presence does not create a different market-
specific parser rule. An unknown field is rejected and must not be treated as
additive-compatible without a separately reviewed adapter-contract change.

## Regression Commands

After `uv sync --all-groups`, run the focused fixture and adapter regression:

```text
uv run pytest -q tests/unit/test_public_provider_schema_fixtures.py tests/unit/test_binance_public_candles.py tests/unit/test_coinbase_public_candles.py tests/unit/test_binance_public_aggregate_trades.py
```

Then run every repository gate:

```text
uv lock --check
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv --preview-features audit-command audit --locked
uv run wealth-health
```

Review CI separately. A skipped, unavailable, partial, replaced, or failing check is not a pass.
Fixture tests prove only the reviewed synthetic cases and existing adapter boundaries; they do not
prove that the provider will continue to emit the same contract.

## Escalation and Decision Record

Market Data owns the affected provider-contract assessment. Engineering may prepare a bounded
candidate and rollback. Audit and Assurance preserves the sanitized timeline, lineage, test
results, and decision record. Risk and Security may reject or halt; Security owns any suspected
secret or unapproved evidence-handling path. The project owner retains material scope, permission,
promotion, and operational-resume authority under
[`docs/POLICIES.md`](../POLICIES.md).

Escalate immediately when:

- The affected provider/variant or downstream interval cannot be bounded.
- Official documentation is ambiguous, contradictory, unavailable, or inconsistent with observed
  behavior.
- Existing accepted evidence may have been parsed with the wrong meaning, type, unit, or order.
- Any real body may contain a secret or requires a handling location that is not already approved.
- A parser, adapter, endpoint, canonical contract, persistence schema, runtime, dependency,
  permission, or deployment change appears necessary.
- Focused or full regression evidence fails, or rollback is absent or untested.

The decision record must identify the exact change and scope, environment, evidence, approver, UTC
decision time, expiry or review trigger, monitoring, and rollback. Ambiguous, absent, expired,
conflicting, or revise-required authority remains denial.

## Resume Gates

Collection for the affected identity remains paused until every applicable gate is satisfied:

1. Root cause and exact affected contract/scope are recorded; downstream impact and any suspect
   accepted interval are reconciled or explicitly quarantined through an approved process.
2. The exact current official contract is re-reviewed and its references and UTC review time are
   recorded. Any ambiguity is resolved rather than inferred.
3. Evidence handling complies with the approved location, access, retention, disposal, and
   secret-free requirements.
4. A new synthetic fixture version is added without changing old versions, or the review
   documents why the current version remains exact.
5. The exact fixture bytes pass through the active production adapter; required bounded synthetic
   drift cases fail closed with typed `INVALID_PAYLOAD` and no partial evidence.
6. Any required adapter, parser, endpoint, contract, runtime, or deployment change has completed
   its own governed task, review, approval, and rollback evidence. Fixture review alone cannot
   satisfy this gate.
7. Focused tests, all repository gates, and CI pass without skipped or substituted evidence.
8. A tested rollback is available, and Market Data, Engineering, and Audit and Assurance have
   recorded their required review. Risk/Security and project-owner approval are recorded whenever
   required by policy, including resume from an operational halt.
9. Resume is a controlled, explicitly invoked, bounded action under the approved operating
   procedure. Validate the first accepted result and lineage before broader use.

No gate is satisfied by a green fixture test alone. Resumption does not authorize continuous
collection, deployment, private access, credentials, trading, or a higher operating mode.

## Rollback

- Before release or resume, reject the candidate, retain the prior production code and all fixture
  versions, and keep the affected request path paused.
- If an approved adapter release regresses, stop the affected path and use its separately tested
  release rollback to restore the last known-good code/configuration. Do not delete the new or old
  fixture version. If the provider no longer supports the old contract, rollback means remaining
  paused, not accepting unknown data.
- Preserve and quarantine suspect evidence and identifiers for reconciliation; never overwrite
  historical canonical or raw records to make the rollback appear clean.
- Re-run focused and full regression gates after rollback. Record the rollback commit/release,
  UTC time, owner, evidence scope, and validation result.

This repository supplies no automatic rollback, pause, resume, evidence quarantine, or deployment
command. Those capabilities require separately approved designs.

## Authority Boundaries

A fixture or documentation review may record evidence and a candidate contract. It never
authorizes:

- An adapter, parser, endpoint, request, accepted-field, precision, ordering, retry, pacing, rate,
  canonical model, persistence, SQLite schema, migration, runtime, dependency, or deployment
  change.
- Automatic drift detection, fixture refresh, pause, resume, remediation, or continuous
  collection.
- Access to an operator path, operator data, private endpoint, account, credential, or secret.
- Live trading, leverage, withdrawal permission, autonomous execution, or any financial action.

Any such change requires its own bounded task and the approvals in project policy. TASK-037
remains blocked; this runbook and its synthetic corpus do not supply its restricted inputs,
operator-data evidence, Risk/Security reviews, project-owner decision, or Stage 3 authority.
