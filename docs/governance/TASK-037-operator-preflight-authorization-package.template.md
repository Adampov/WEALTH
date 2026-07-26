# TASK-037 Operator-Preflight Authorization Package Template

> **Repository placeholder only — grants no authority.**
>
> Do not enter a real database path, hostname, username, principal, mount, share, bucket,
> destination, credential, retention value, person identity, or decision timestamp in this
> repository copy. Populate a separate copy only inside a Security-approved restricted governance
> location. The repository may later retain only an approved opaque artifact reference and the
> non-sensitive decision state.

This template prepares the fields required by TASK-037. It is not the populated authorization
package, is not a project-owner decision, and cannot authorize an operator-data scan or scanner
implementation. Missing values and every placeholder below mean denied.

## Template control record

```text
template_schema_version: 1.0
classification: PLACEHOLDER_ONLY
project_id: WEALTH
task_id: TASK-037
task_action: phase2.canonical_utc_preflight_operator_authorization_package_owner_decision
risk_tier: 3
package_id: NOT_ASSIGNED
package_revision: NOT_ASSIGNED
restricted_package_reference: NOT_RECORDED
owner_decision: NOT_RECORDED
authorization_disposition: DENIED
authority_effect: NONE
operator_access: NOT_AUTHORIZED
stage3_gate: NOT_SATISFIED
automatic_execution: false
scanner_authorized: false
snapshot_execution_state: NOT_EXECUTED
report_creation_state: NOT_CREATED
real_paths_allowed_in_repository_copy: false
```

## Repository-known lineage

The populated restricted package must identify the exact reviewed revisions of:

- ADR-0027 and the canonical UTC boundary inventory and migration plan;
- the explicit human-approval, Risk, and Security policies;
- the exact TASK-036 proposal contract and its private TASK-035 bundle-plan lineage; and
- the repository commit reviewed by the owner, Risk reviewer, and Security reviewer.

No repository baseline, development setting, synthetic fixture, or symbolic slot supplies a real
deployment value.

## Canonical family inventory

These eight rows define vocabulary and coverage only. They do not state which families are
deployed, how many physical databases exist, or whether one family has multiple database paths.
The restricted package must mark every family `DEPLOYED` or `NOT_DEPLOYED` and must list every real
path separately.

| Ordinal | Family | TASK-036 symbolic slot | Restricted deployment state | Restricted path-entry references |
|---:|---|---|---|---|
| 0 | `market` | `synthetic_path_slot_market` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 1 | `order_flow` | `synthetic_path_slot_order_flow` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 2 | `historical_collection` | `synthetic_path_slot_historical_collection` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 3 | `continuous_collection` | `synthetic_path_slot_continuous_collection` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 4 | `collector_service` | `synthetic_path_slot_collector_service` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 5 | `public_trade_collection` | `synthetic_path_slot_public_trade_collection` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 6 | `rate_budget` | `synthetic_path_slot_rate_budget` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |
| 7 | `reconciliation` | `synthetic_path_slot_reconciliation` | `OWNER_REQUIRED` | `OWNER_REQUIRED` |

## Restricted package fields

Every field in this section is required in the Security-approved restricted copy.
`NOT_APPLICABLE` is prohibited for every approval-gate field below. If any required value cannot
be populated exactly, `APPROVE` is invalid and only `REJECT` or `REVISE` may be recorded.

### Identity and environment

- Unique package ID and monotonically increasing revision.
- Exact project, task, change, scope, and repository revision.
- Exact deployment, environment, and host or service boundary.
- Approved restricted handling location and its owner.
- Confidentiality classification and access-control group.

### Exact read-only path scope

- Actual `real_path_count`; it must equal the number of path entries and must never default to
  eight.
- `APPROVE` requires at least one `DEPLOYED` family and `real_path_count` greater than zero. An
  empty real path list permits only `REJECT` or `REVISE`.
- For each entry: unique ordinal and ID, canonical family, exact owner-supplied path text,
  deployment role, access principal reference, `READ_ONLY` access mode, and snapshot-profile
  reference.
- Every `DEPLOYED` family must reference at least one path entry. Every `NOT_DEPLOYED` family must
  reference zero path entries.
- Explicit treatment of multiple paths per family, aliases, duplicate physical targets, and
  families that are not deployed.
- Evidence that no wildcard, directory-wide scope, write, delete, repair, schema, migration,
  `ATTACH`, or WAL-changing permission is requested.

Paths must be retained exactly as the owner supplies them. TASK-037 must not resolve, normalize,
open, inspect, or test them.

### Snapshot procedure

- Procedure name, version, owner, and execution authority.
- Writer-fence steps and the exact generation or watermark boundary.
- SQLite-safe consistency mechanism.
- WAL and checkpoint policy, including treatment of sidecar files.
- Immutability mechanism and destination access controls.
- Abort behavior for a failed fence, incomplete snapshot, integrity uncertainty, or changed
  generation.
- `execution_state: NOT_EXECUTED` throughout TASK-037.

Copying database, WAL, and SHM files separately is not sufficient evidence of a consistent
snapshot.

### Report, manifest, and external anchor

- Exact report destination and owner.
- Exact manifest destination.
- Separate external anchor destination.
- Access controls, encryption, redaction, and permitted readers.
- Destination-write audit and failure behavior.
- `creation_state: NOT_CREATED` throughout TASK-037.

### Evidence retention and disposal

- Evidence classes covered by the rule.
- Exact retention trigger and duration.
- Legal or investigation hold behavior.
- Disposal trigger, method, responsible owner, and verification evidence.
- Approved location for the disposal-verification record.

### Monitoring and revocation

- Source access audit and proof that source databases remain unchanged.
- Destination-write audit and integrity monitoring.
- Approval expiry, revocation, and package-revision monitoring.
- Monitoring cadence, alert thresholds, automatic halt criteria, responder, escalation route, and
  response-time objective.
- Binding of every monitor and alert to the exact package revision and authorized path scope.
- Evidence that monitoring is ready before any approval can become effective.

### Tested rollback

- Exact revocation and access-removal steps.
- Evidence containment and approved disposal steps.
- Source-unchanged verification.
- Test environment, exact tested procedure and package scope, test time in UTC, result, reviewer,
  and immutable evidence reference and digest.
- Exact maximum age of rollback-test evidence at owner-decision time and the event that requires a
  fresh test.
- Failure behavior when rollback evidence is missing, stale, or unsuccessful.

## Independent reviews

### Risk review

The independent Risk record must identify the reviewer, exact package revision, UTC review time,
`APPROVE`, `REJECT`, or `REVISE` outcome, residual risks, monitoring decision, tested-rollback
decision, immutable evidence reference and digest, and expiry or concrete review trigger. It must
bind the review to the exact requested scope, include the reviewer's authority basis, and include
an attestation that the reviewer is independent of package preparation and the owner decision.
Conditions or unresolved findings mean `REVISE`.

Current populated-package Risk review: `NOT_PERFORMED`.

### Security review

The independent Security record must identify the reviewer, exact package revision, UTC review
time, `APPROVE`, `REJECT`, or `REVISE` outcome, and findings for least privilege, path handling,
credentials, destinations, retention, disposal, and revocation. It must include the reviewer's
authority basis, immutable evidence reference and digest, expiry or concrete review trigger,
exact requested-scope binding, and an attestation that the reviewer is independent of package
preparation and the owner decision. Conditions or unresolved findings mean `REVISE`.

Current populated-package Security review: `NOT_PERFORMED`.

Any package change invalidates both reviews.
Owner `APPROVE` is valid only when both exact-revision review outcomes are `APPROVE`. Any
`REJECT`, `REVISE`, missing, stale, expired, conditional, or conflicting review means denied.

## Project-owner decision

Only the project owner may record the final decision after both independent reviews are complete.
The record must identify:

- exact package ID and revision;
- owner identity and authority basis;
- one explicit `APPROVE`, `REJECT`, or `REVISE` outcome;
- rationale and immutable references to both independent reviews;
- UTC decision and effective times;
- explicit expiry later than the effective time or a concrete review trigger;
- monitoring and tested-rollback acceptance; and
- any superseded decision and revocation state.

Current project-owner decision: `NOT_RECORDED`.

Every review and decision time must use exact UTC RFC 3339 `Z` form. Effective time cannot precede
decision time.

`APPROVE` authorizes only the exact recorded scope. It does not execute anything, perform Stage 3,
or authorize scanner implementation. A scanner remains a separate later task.

## Fail-closed rules

1. Every populated-package field is required; extras, ambiguity, coercion, unsupported values, and
   repository placeholders fail.
2. The real path count must equal the exact number of entries and never defaults to eight.
3. `APPROVE` requires at least one real path and one `DEPLOYED` family. Every canonical family has
   one deployment-state record; deployed families may have multiple path entries. `DEPLOYED`
   requires at least one path entry and `NOT_DEPLOYED` requires none.
4. Every path entry maps to one family; duplicate ordinals, duplicate physical targets, wildcards,
   synthetic TASK-036 tokens, and directory-wide grants fail.
5. Any access broader than exact read-only scope fails.
6. Missing writer fencing, consistency, immutability, generation or watermark, WAL/checkpoint
   handling, or abort behavior fails.
7. Report, manifest, anchor, retention, and disposal destinations require exact Security-reviewed
   values.
8. Secrets and credential values are prohibited; only approved indirect references are allowed.
9. Risk and Security reviews must be independent, final, unexpired, and bound to the exact package
   revision and scope. Both outcomes must be `APPROVE`; any other or conflicting outcome means
   denied.
10. `APPROVE` is invalid without ready monitoring and passing tested-rollback evidence.
11. The owner decision must follow both reviews, use exact UTC RFC 3339 `Z` times, and include an
    expiry later than its effective time or a concrete review trigger.
12. A package change invalidates prior reviews and approval.
13. `REJECT`, `REVISE`, missing, expired, revoked, conflicting, superseded, conditional, or stale
    authority means denied.
14. Approval grants only its exact recorded scope and never runs code.
15. Scanner design, implementation, and execution remain separate reviewed actions.
16. TASK-037 performs only approved governance-artifact writes: no operator-path inspection,
    SQLite access, report creation, scanner, runtime, migration, schema change, or Stage 3 action.

## Current disposition

This repository template is `PLACEHOLDER_ONLY`. No populated package, independent Risk review,
independent Security review, or project-owner decision is recorded. Authorization remains
`DENIED`.
