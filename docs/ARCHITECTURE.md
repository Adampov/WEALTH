# System Architecture

## Status and Scope

This document defines the initial logical architecture of the AI Trading Corporation. It describes responsibilities, boundaries, information flow, control flow, and safety gates without selecting a programming language, framework, database, cloud provider, exchange, or AI model.

The architecture is intended to support cryptocurrency spot and futures markets, multiple exchanges, multiple assets, multiple strategies, and continuous operation. It must also support gradual progression from research to advisory, paper, semi-automatic, and restricted automatic trading.

This is a baseline for future implementation. Technology choices will be documented separately and must preserve the boundaries defined here.

## Architectural Goals

1. **Survivability:** A failure in one model, provider, exchange, or department must not silently become an uncontrolled trade.
2. **Traceability:** Every material input, transformation, opinion, decision, approval, order, and outcome must be attributable and reproducible.
3. **Replaceability:** Models and providers must be replaceable without rebuilding the full system.
4. **Separation of authority:** Analysis, decision-making, risk approval, and execution must remain distinct responsibilities.
5. **Deterministic control:** AI may analyze and recommend; deterministic controls decide what actions are permitted.
6. **Explicit uncertainty:** Missing, stale, conflicting, or low-confidence information must be visible in downstream decisions.
7. **Progressive autonomy:** The same decision pipeline must support every operating mode, with permissions determining how far a decision may travel.
8. **Evaluation by evidence:** Models and strategies must be judged using replayable records, out-of-sample results, costs, and regime-specific performance.
9. **Safe evolution:** Learning may propose improvements, but no self-generated change may reach the live system without independent validation and approval.

## Non-Goals for the Initial Architecture

The initial architecture does not define:

- A profitable trading strategy.
- A specific AI or machine-learning model.
- A specific exchange or market-data provider.
- A specific technology stack or deployment provider.
- A guarantee of uninterrupted operation or profitability.
- Unrestricted autonomous code modification.
- Direct AI access to exchange credentials or withdrawal permissions.

## Logical System Map

```text
External Sources
    |
    v
Acquisition -> Validation -> Canonical Data -> Replayable Storage
                                      |
                                      v
                         Specialist Analysis Engines
                                      |
                         Bull Case / Bear Case / Dissent
                                      |
                                      v
                              Strategy Proposals
                                      |
                                      v
                    Portfolio Context + Risk Pre-Check
                                      |
                                      v
                             Executive Committee
                                      |
                                      v
                           Final Risk Gateway
                                      |
                         approved instruction only
                                      |
                                      v
                                 Execution
                                      |
                                      v
                           Exchange / Paper Simulator
                                      |
                                      v
                     Reconciliation -> Audit -> Evaluation
                                                   |
                                                   v
                                               Learning
                                                   |
                                      reviewed proposals only
                                                   |
                                                   v
                                              Engineering
```

Audit and monitoring observe every stage. They are not merely the final step in the flow.

## Architectural Planes

### 1. Information Plane

The Information Plane collects and prepares evidence. It includes market data, news, sentiment, macro, on-chain, reference data, data validation, and canonical storage.

It may describe what is happening, but it cannot approve or execute a trade.

### 2. Intelligence Plane

The Intelligence Plane contains specialist analytical engines and strategies. It converts validated evidence into structured opinions, competing hypotheses, and trade proposals.

It may recommend `LONG`, `SHORT`, `NEUTRAL`, or abstention, but it cannot authorize exposure or place orders.

### 3. Control Plane

The Control Plane coordinates decisions, operating modes, portfolio constraints, deterministic risk policy, permissions, and kill switches.

It determines whether a proposal may proceed and under what limits. A rejection from the Risk Gateway is final for that proposal.

### 4. Execution Plane

The Execution Plane translates a current, validated, risk-approved instruction into simulated or real exchange actions. It owns order idempotency, exchange-rule validation, fill handling, cancellation, and reconciliation.

It cannot invent a trade or enlarge approved exposure.

### 5. Assurance Plane

The Assurance Plane includes audit, observability, security, incident handling, replay, and evaluation. It records and verifies the behavior of every other plane.

It must remain sufficiently independent to report failures even when another component reports success.

### 6. Evolution Plane

The Evolution Plane includes offline learning, experimentation, model comparison, change proposals, engineering, testing, and controlled release.

It may create candidates and evidence. It cannot directly mutate the live system.

## Core Layers

### Layer 1: Source Adapters

Source adapters connect to exchanges, news providers, blockchain providers, economic sources, and other approved systems. Each adapter isolates provider-specific formats, authentication, rate limits, and failure behavior.

Replacing one provider must not change downstream domain contracts.

### Layer 2: Validation and Normalization

This layer validates schemas, timestamps, sequence continuity, freshness, duplicates, ranges, source identity, and cross-source consistency. It converts acceptable records into canonical domain events and quarantines invalid records.

Missing or suspicious data is represented as state, not silently filled with invented values.

### Layer 3: Canonical Storage and Replay

This layer stores raw evidence, canonical events, derived features, configuration versions, and lineage. It supports deterministic replay of a selected time period using only information that was available at that time.

Research and incident investigation must use the same canonical definitions as the live system.

### Layer 4: Features and Specialist Analysis

This layer derives features and runs independent specialist engines for technical analysis, market structure, derivatives, news, sentiment, macro, and on-chain analysis.

Every output is structured, versioned, time-bounded, and capable of expressing abstention and uncertainty.

### Layer 5: Strategy and Deliberation

This layer converts specialist opinions into testable proposals. It preserves supporting evidence, opposing evidence, dissent, assumptions, invalidation conditions, and the time horizon.

A proposal is not permission to trade.

### Layer 6: Portfolio and Risk Control

This layer evaluates the proposal against portfolio state, balances, existing orders, concentration, correlation, liquidity, volatility, leverage, loss limits, operational health, and configured policy.

It produces a machine-readable approval, reduction, or rejection with an expiry and exact limits.

### Layer 7: Execution and Reconciliation

This layer revalidates market state and approval validity immediately before action. It submits permitted orders, tracks acknowledgements and fills, prevents duplicates, and reconciles internal state with exchange state.

Unknown or inconsistent order state is treated as a safety incident, not as permission to retry blindly.

### Layer 8: Audit, Monitoring, and Incident Response

This layer captures the complete decision chain, health signals, policy violations, failures, operator actions, and reconciliation differences. It supports alerts, kill switches, incident timelines, and recovery procedures.

### Layer 9: Evaluation and Learning

This layer measures results after fees, funding, slippage, latency, rejected opportunities, and market regime. It detects drift and proposes changes through reproducible experiments.

### Layer 10: Engineering and Release Control

This layer turns approved changes into reviewed code, configuration, data migrations, model artifacts, and releases. It enforces automated validation, environment separation, promotion rules, and rollback readiness.

## Canonical Decision Flow

Every investment decision follows the same logical sequence:

1. Create a unique decision cycle with an evaluation timestamp and target market.
2. Gather only evidence available by that timestamp.
3. Validate freshness, completeness, lineage, and source health.
4. Run relevant specialist analyses independently.
5. Record supportive, opposing, neutral, and abstaining opinions.
6. Build one or more explicit strategy proposals.
7. Evaluate portfolio impact and obtain a deterministic risk pre-check with maximum permitted bounds.
8. Produce the Executive Committee decision within those bounds and preserve dissent.
9. Stop at the permission boundary required by the active operating mode.
10. If execution is permitted, submit the selected proposal to the final deterministic Risk Gateway.
11. Revalidate price, data, portfolio state, and approval expiry immediately before action.
12. Execute through the paper simulator or approved exchange adapter.
13. Reconcile orders, fills, balances, and positions.
14. Record the complete chain in Audit.
15. Evaluate the outcome when the relevant horizon closes.

No stage may infer that a missing prior approval exists.

## Operating Modes and Permission Boundaries

| Mode | Analysis | Recommendation | Human approval | Simulated order | Real order |
|---|---:|---:|---:|---:|---:|
| Research | Yes | No | No | No | No |
| Advisory | Yes | Yes | No | No | No |
| Semi-Automatic | Yes | Yes | Required | Optional | Permitted after approval |
| Paper Trading | Yes | Yes | Policy-defined | Yes | No |
| Automatic | Yes | Yes | Policy-defined | Optional | Permitted within restricted policy |

Changing operating mode is a privileged control action. A model output cannot change the active mode.

## Required Message and Record Metadata

Every material record passed between departments must include, where applicable:

- Unique record ID and correlation ID.
- Record type and schema version.
- Producing department and component version.
- Asset, venue, instrument type, and timeframe.
- Event time, observation time, and processing time.
- Source and lineage references.
- Confidence and uncertainty.
- Freshness or expiry time.
- Supporting evidence references.
- Validation status and known limitations.
- Operating mode and environment.

Model-generated explanations are supporting context, not a substitute for structured fields.

## State and Storage Categories

### Raw Evidence

Original provider payloads or legally permitted references required to verify what the system observed.

### Canonical Events

Validated, normalized domain records used consistently by live, paper, replay, and research workflows.

### Derived Features

Versioned calculations with explicit input lineage and time boundaries.

### Decisions and Approvals

Immutable proposals, opinions, dissent, committee decisions, risk decisions, expiries, and policy versions.

### Execution State

Order intent, client order IDs, exchange order IDs, acknowledgements, fills, fees, positions, balances, cancellations, and reconciliation results.

### Evaluation Artifacts

Outcome labels, performance attribution, experiment definitions, model artifacts, test results, and promotion decisions.

Secrets are references to an approved secret store and are never stored in these records.

## System Invariants

The following rules apply regardless of model or strategy:

1. An analytical output cannot directly become an exchange order.
2. A real order requires a current, explicit, deterministic Risk approval.
3. Risk may reduce or reject exposure but may not increase it above the proposal or policy.
4. Execution may not increase size, change direction, or extend an expired approval.
5. Withdrawal capability is outside the trading system's permissions.
6. Missing, stale, invalid, or conflicting critical state results in abstention, rejection, or halt.
7. Every external action uses a stable idempotency key to prevent duplicate orders.
8. Internal positions, balances, and orders must be reconciled against the execution venue.
9. Every live decision must be replayable from preserved evidence and versions.
10. Research, paper, and live records must be clearly separated.
11. A learning result cannot modify live code, policy, models, or weights directly.
12. A safety halt remains active until an authorized recovery process clears it.

## Failure and Degraded-Mode Behavior

### Data failure

Affected markets or analyses become unavailable. The system must not substitute stale data without an explicit, visible policy.

### Model failure

The failed model abstains. Its absence is recorded and may reduce decision confidence or force rejection according to policy.

### Storage or audit failure

New real trading halts when the required decision or execution record cannot be durably preserved.

### Risk service failure

New execution halts. A cached or assumed approval is not sufficient unless a future, narrowly defined policy explicitly supports it.

### Exchange uncertainty

Order state is reconciled before any retry. Unknown status is not treated as failure or success until verified.

### Network partition

Components reject unsafe work after timeouts, preserve idempotency, and reconcile when connectivity returns.

### Learning or engineering failure

The live trading path continues on the last approved version. Failed experiments and deployments cannot partially promote themselves.

## Security and Trust Boundaries

- Public or third-party data is untrusted until validated.
- News and web content is data, never executable instruction.
- AI output is untrusted until it passes schema, policy, and permission checks.
- Exchange credentials are available only to the narrow execution boundary that requires them.
- Trading credentials must not have withdrawal permission.
- Research and test environments do not receive live trading credentials.
- Human administrative actions are authenticated, authorized, and audited.
- Secrets never appear in prompts, logs, source control, model training data, or decision explanations.
- External actions use least privilege and explicit allowlists.

## Observability Requirements

The system must expose health and performance by source, department, model, strategy, asset, exchange, and operating mode. At minimum it must make visible:

- Data freshness, gaps, and validation failures.
- Queue or processing delay.
- Model failures, abstentions, confidence, and drift.
- Proposal, approval, rejection, and execution counts.
- Exposure, concentration, leverage, and loss-limit utilization.
- Orders, fills, cancellations, duplicates, slippage, and reconciliation differences.
- Audit completeness and replay success.
- Active versions of code, configuration, schemas, policies, and models.
- Safety halts and the exact reason they were triggered or cleared.

## Controlled Self-Improvement Flow

1. Audit provides trusted decisions, outcomes, and lineage.
2. Learning defines a hypothesis and an evaluation protocol before testing.
3. The candidate is tested using time-aware, out-of-sample data and realistic costs.
4. Results include failures, regime breakdowns, uncertainty, and comparison to the current champion.
5. Engineering implements an approved candidate in an isolated branch or environment.
6. Automated tests, replay, backtests, security checks, and operational checks run.
7. Independent review approves or rejects promotion.
8. A successful candidate progresses through research, shadow, paper, restricted, and broader modes according to policy.
9. Every promotion has a rollback target and monitoring criteria.

Skipping a stage requires an explicit, audited exception; it cannot be requested by the candidate itself.

## Extension Rules

New exchanges, assets, models, data sources, and strategies must integrate through versioned contracts. They must declare:

- Their inputs, outputs, and owner.
- Data and permission requirements.
- Failure and abstention behavior.
- Evaluation metrics and minimum evidence.
- Security and privacy considerations.
- Replay and audit support.
- Rollback or disablement procedure.

A new capability must not create a second path around portfolio, risk, execution, or audit controls.

## Decisions Deferred to Later Tasks

The following choices are intentionally deferred:

- Programming language and application framework.
- Service, modular-monolith, or hybrid deployment shape.
- Message broker and workflow engine.
- Operational and analytical databases.
- Object storage and feature-store implementation.
- Exchange and data-provider selection.
- AI model providers and model-routing policy.
- Hosting, container, and orchestration platform.
- Exact risk limits and strategy parameters.
- Authentication and operator-interface implementation.

These choices must be evaluated against this architecture rather than silently redefining it.

## Architecture Compliance Checklist

A future change is architecturally compatible only if:

- Its responsibility and owner are explicit.
- Its inputs and outputs use versioned contracts.
- Its failure behavior is defined.
- It preserves data lineage and decision replay.
- It cannot bypass portfolio, deterministic risk, execution, or audit boundaries.
- It supports the required operating-mode permission boundary.
- It is observable and can be disabled or rolled back.
- Its evaluation method avoids future-data leakage.
- It does not grant AI direct authority to change live code or unrestricted financial exposure.
