# ADR-0033: Bounded local-development review delegation

- Status: Owner-authorized proposal; repository adoption pending exact review and merge.
- Owner decision recorded: 2026-10-03.
- Task: DEV-REVIEW-20261003, generation 1, RISK 3 (approval semantics).
- Base: `6a43b99e20b2d8621802ac8c799715ff752be7c5`.
- Branch: `agent/local-development-review`.

## Context and owner decision

Existing policy allows separate engineering agents but prevents them from satisfying required
organizational reviews. For a local development project this leaves no designated development
Risk/Security or QA approval path. The owner explicitly answered yes to changing the rules so
separate review agents may approve local-development changes only, without changing live-trading
permissions. This is a specific new policy request, not an inference from earlier blanket approval.

The decision authorizes preparation and review of this amendment. It does not approve its eventual
pull request/head for merge, certify any test result or adopt an unreviewed candidate. Reviewers of
this amendment act as independent engineering reviewers of owner-requested governance work; they
do not use the proposed delegation to approve their own appointment.

## Decision

Add `DEV-REVIEW-V1` to `docs/POLICIES.md` as the single authoritative definition. Reconcile only the
conflicting reviewer restriction and its references in `docs/QUANT_ORG_OS.md`. For eligible isolated
development work, require at least two distinct named read-only agents: a combined Risk/Security
reviewer and a separate QA reviewer, both independent of every author and the integrator.

Require exact scope, immutable candidate/contract/evidence bindings, session provenance, explicit
decisions, review triggers and retained negative findings. A manually checked task packet is
sufficient for this development workflow only; no authentication service, runtime permission system
or external organizational identity is claimed. A disagreement blocks advancement rather than
being resolved by majority vote or reviewer replacement.

Required-gate changes remain at least RISK 3 and retain exact owner approval, baseline inventory,
independent review, validation, monitoring, reconciliation and tested rollback. No financial,
production, operational, restricted-input, paper/shadow execution, credential, deployment or merge
authority is delegated. Runtime deterministic controls and all existing financial prohibitions are
unchanged.

The amendment is prospective. It becomes usable only after unchanged exact-candidate adoption
requirements are satisfied. A draft branch cannot activate this rule for TASK-064 or any other
task. An adopted policy must be brought into the target checkout through its normal governed
integration; earlier development-agent findings do not become approvals retroactively.

## Scope and independence

Only these files change:

- `docs/POLICIES.md`;
- `docs/QUANT_ORG_OS.md`;
- this ADR; and
- `tests/unit/test_quant_org_os.py`, with appended assertions inside the five existing nodes.

The work starts from main, not from an unmerged runtime candidate. It consumes no output from
PRs 68, 69 or 70. ADR-0032 remains reserved for the separate SQLite evidence-harness work. No
TASK-064 contract, runner, workflow, fixture, manifest, dependency, production source or runtime
changes. The canonical next action, capability flags, financial state, TASK-037 denial and open
risk states remain unchanged, so no completion or risk-closure update is made to their state files.

## Validation and review criteria

Retain every original governance assertion and all five node names. Run the unchanged baseline
assertions as well as the additive candidate assertions; inspect exact file scope, financial
prohibitions and the existing 50,000-byte prompt bound. Separate reviewers inspect the immutable
candidate and evidence before publication/adoption; CI remains unchanged. These checks validate
policy text and reviewed decisions, not empirical agent compliance or runtime enforcement.

| Scenario | Required disposition |
|---|---|
| Eligible local patch; two independent roles; exact evidence | Scope-bound development decision only, after adoption |
| Author or root integrator offers a required approval | Reject that review; no self-approval |
| One agent uses two role labels | Reject; two distinct reviewers are required |
| Same root, two distinct read-only agents with verified provenance | Eligible only for the delegated development scope |
| Candidate, contract, scope or material evidence changes | Invalidate old decisions; require fresh reviews |
| Reviewer abstains, rejects or cannot be authenticated to its session | Block advancement and retain the finding |
| Required-gate change is called test-only | Keep RISK 3 and exact owner approval; no omitted checks |
| Test patch introduces production/credential/runtime authority | Outside delegation; existing approvals remain required |
| Green CI or a development approval without exact owner merge approval | No merge |
| Draft amendment is offered as authority for another task | Reject; no prospective-policy bootstrap |

Validation results belong to the immutable candidate's review packet and CI, not to invented
success claims in this proposal. The implementation should keep one scoped rule and short
cross-references, consistent with the [official guidance on repository instructions](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra),
consulted 2026-10-03. That guidance does not confer project approval authority.

## Risks and rollback

Agents under one root may share context and failure modes. Separate roles, raw evidence, retained
dissent, exact identity checks and owner-controlled adoption reduce but do not eliminate this risk.
No organizational independence or production assurance is claimed. Unverifiable provenance or
ambiguous effects blocks the affected decision.

Before adoption, rejection leaves the previous policy in force. After adoption, stop new delegated
decisions and invalidate dependent unconsumed decisions before reverting the exact amendment
commit. Preserve all evidence. No data migration, database repair, host cleanup, runtime action or
live-mode transition is part of rollback.
