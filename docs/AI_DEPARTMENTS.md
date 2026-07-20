# AI Departments

## Purpose

This document defines the initial organizational map for the AI Trading Corporation. Each department has a narrow mandate, explicit inputs and outputs, clear boundaries, measurable performance, and named relationships with other departments.

Departments may contain one or more specialist agents. Their models, strategies, and internal methods may change over time, but their responsibilities and interfaces should remain explicit and reviewable.

No analytical department may place an exchange order. Only the Execution Department may submit orders, and only after the Risk Department has issued a valid approval.

## 1. Market Data Department

**Mission:** Collect, normalize, timestamp, and distribute reliable market data across supported exchanges, assets, and timeframes.

**Inputs:** Exchange REST and WebSocket feeds, OHLCV data, trades, order books, funding rates, open interest, instrument metadata, and exchange status information.

**Outputs:** Normalized market events, synchronized candles, order-book snapshots, data freshness indicators, source metadata, and availability status.

**Allowed actions:** Connect to approved read-only data sources, normalize formats, deduplicate events, cache data, and quarantine malformed records.

**Forbidden actions:** Generate trade recommendations, alter source values without traceability, hide missing data, or submit exchange orders.

**Evaluation metrics:** Completeness, freshness, latency, duplicate rate, gap rate, reconciliation accuracy, and uptime by source.

**Relationships:** Supplies verified market data to Technical Analysis, Market Structure, Derivatives, Strategy, Portfolio, Execution, and Audit. Reports quality incidents to Audit and Engineering.

## 2. Technical Analysis Department

**Mission:** Identify trends, momentum, volatility, price patterns, and possible entry, exit, and invalidation levels across multiple timeframes.

**Inputs:** Validated OHLCV data, volume, derived indicators, asset metadata, and market-regime context.

**Outputs:** Direction (`LONG`, `SHORT`, or `NEUTRAL`), confidence, timeframe, suggested levels, invalidation conditions, detected regime, supporting evidence, and uncertainty.

**Allowed actions:** Calculate indicators, compare timeframes, generate analytical opinions, and abstain when evidence is insufficient.

**Forbidden actions:** Choose final position size, bypass contradictory evidence, place orders, or present estimates as guaranteed outcomes.

**Evaluation metrics:** Signal expectancy, calibration, precision by regime, drawdown contribution, stability across timeframes, and abstention quality.

**Relationships:** Receives data from Market Data; exchanges context with Market Structure and Derivatives; submits opinions to Strategy and the Executive Committee; sends outcomes to Learning.

## 3. Market Structure Department

**Mission:** Analyze liquidity, price formation, support and resistance, order flow, market microstructure, and structural shifts.

**Inputs:** Trades, order books, volume profiles, liquidation data, spreads, depth, volatility, and multi-timeframe price structure.

**Outputs:** Market regime, liquidity zones, structural bias, breakout or reversal conditions, slippage risk, and evidence of abnormal market behavior.

**Allowed actions:** Detect structural changes, flag illiquid conditions, estimate execution difficulty, and challenge technical signals.

**Forbidden actions:** Manufacture missing order-book history, approve risk, select final exposure, or submit orders.

**Evaluation metrics:** Regime-detection accuracy, liquidity-risk calibration, false-breakout rate, slippage forecast error, and usefulness to trade selection.

**Relationships:** Receives data from Market Data; challenges Technical Analysis and Strategy; provides liquidity context to Risk, Portfolio, Execution, and the Executive Committee.

## 4. Derivatives Department

**Mission:** Evaluate cryptocurrency futures and perpetual markets, including leverage conditions, positioning, funding, basis, liquidations, and crowding.

**Inputs:** Futures prices, spot prices, funding rates, open interest, basis, liquidations, long-short ratios, margin rules, and contract metadata.

**Outputs:** Derivatives bias, crowding score, leverage stress, funding impact, liquidation zones, basis conditions, and contract-specific risks.

**Allowed actions:** Compare spot and derivatives markets, detect leverage imbalances, identify crowded positioning, and recommend lower exposure or abstention.

**Forbidden actions:** Set account leverage, assume exchange rules are unchanged without verification, approve a trade, or place orders.

**Evaluation metrics:** Crowding-signal expectancy, liquidation-risk accuracy, funding-cost forecast error, basis-model accuracy, and performance by volatility regime.

**Relationships:** Receives data from Market Data; shares context with Market Structure and Strategy; informs Risk, Portfolio, Execution, and the Executive Committee.

## 5. Sentiment Department

**Mission:** Measure market psychology and changes in attention without treating popularity as proof of investment value.

**Inputs:** Approved social sources, search trends, community activity, sentiment feeds, message volume, source reputation, language, and timestamps.

**Outputs:** Sentiment direction, intensity, rate of change, disagreement, source coverage, confidence, manipulation risk, and supporting evidence.

**Allowed actions:** Classify sentiment, compare sources, detect unusual attention, discount unreliable sources, and abstain when coverage is poor.

**Forbidden actions:** Treat anonymous claims as facts, expose personal data, use unapproved sources, place orders, or determine position size.

**Evaluation metrics:** Calibration against subsequent returns and volatility, source reliability, manipulation-detection rate, coverage, drift, and false-positive rate.

**Relationships:** Receives contextual events from News Intelligence; provides opinions to Strategy and the Executive Committee; sends source-quality concerns to Audit.

## 6. News Intelligence Department

**Mission:** Collect, verify, classify, and summarize market-moving news and events with source traceability and time awareness.

**Inputs:** Approved news feeds, official project announcements, exchange notices, regulatory publications, security disclosures, and event calendars.

**Outputs:** Structured events, affected assets, event type, publication time, event time, novelty, source reliability, possible impact, uncertainty, and citations.

**Allowed actions:** Cross-check sources, distinguish fact from commentary, identify conflicting reports, track corrections, and flag urgent events.

**Forbidden actions:** Invent facts or citations, rely on a single weak source for critical claims, copy restricted content excessively, place orders, or set exposure.

**Evaluation metrics:** Event-detection latency, precision, source accuracy, duplicate rate, correction handling, asset-mapping accuracy, and impact calibration.

**Relationships:** Provides events to Sentiment, Macro, Strategy, Risk, Audit, and the Executive Committee. Receives source-policy requirements from Audit.

## 7. Macro Department

**Mission:** Assess macroeconomic and cross-market conditions that may affect cryptocurrency liquidity, risk appetite, volatility, and correlations.

**Inputs:** Economic calendars, interest rates, inflation releases, liquidity indicators, currencies, equities, bonds, commodities, policy statements, and cross-asset prices.

**Outputs:** Macro regime, risk-on or risk-off assessment, event risk windows, cross-asset confirmation or divergence, confidence, and uncertainty.

**Allowed actions:** Compare markets, identify scheduled risk events, assess correlation shifts, and recommend waiting during high uncertainty.

**Forbidden actions:** Use stale releases as current facts, claim causation from correlation alone, place orders, or override Risk.

**Evaluation metrics:** Regime classification accuracy, event-risk calibration, correlation forecast stability, contribution to drawdown avoidance, and data freshness.

**Relationships:** Receives verified events from News Intelligence and data from approved providers; informs Strategy, Portfolio, Risk, and the Executive Committee.

## 8. On-Chain Department

**Mission:** Analyze blockchain activity and flows while accounting for attribution uncertainty and chain-specific limitations.

**Inputs:** Approved node or data-provider feeds, transaction activity, exchange flows, wallet labels, stablecoin supply, fees, active addresses, staking, and protocol metrics.

**Outputs:** Flow signals, network-activity trends, concentration risks, exchange-flow estimates, label confidence, data coverage, and uncertainty.

**Allowed actions:** Compare on-chain metrics over time, flag unusual flows, challenge weak address labels, and abstain when chain coverage is incomplete.

**Forbidden actions:** Treat wallet labels as certain when they are probabilistic, deanonymize individuals, place orders, or determine final exposure.

**Evaluation metrics:** Data coverage, label accuracy, anomaly precision, signal expectancy, revision rate, and performance by asset and chain.

**Relationships:** Supplies evidence to Strategy, Risk, and the Executive Committee; reports provider or attribution issues to Audit and Engineering.

## 9. Strategy Department

**Mission:** Convert evidence from specialist departments into explicit, testable trade proposals or decisions to abstain.

**Inputs:** Technical, structural, derivatives, sentiment, news, macro, and on-chain opinions; portfolio context; approved strategy definitions; and historical evaluation results.

**Outputs:** Trade thesis, direction, entry conditions, invalidation conditions, target logic, time horizon, supporting and opposing evidence, confidence, and abstention reason.

**Allowed actions:** Combine evidence, run approved strategies, compare alternative hypotheses, request missing analysis, and reject weak opportunities.

**Forbidden actions:** Submit orders, choose exposure beyond Risk limits, conceal dissenting evidence, change a live strategy without approval, or evaluate itself using future information.

**Evaluation metrics:** Expectancy after costs, calibration, drawdown, stability by regime, turnover, rejection quality, and out-of-sample performance.

**Relationships:** Receives opinions from all analysis departments; submits proposals to Risk, Portfolio, Audit, and the Executive Committee; sends results to Learning.

## 10. Risk Department

**Mission:** Protect capital through deterministic, enforceable limits that are independent of model confidence and expected profit.

**Inputs:** Trade proposals, portfolio state, balances, exposure, volatility, liquidity, correlations, exchange limits, operational health, and configured risk policy.

**Outputs:** `APPROVE`, `REJECT`, or `REDUCE`; maximum position size; leverage limit; stop requirements; portfolio impact; expiry; and machine-readable rejection reasons.

**Allowed actions:** Reduce or reject exposure, halt trading, enforce loss and leverage limits, require fresh data, and revoke approval when conditions change.

**Forbidden actions:** Increase exposure above policy, loosen limits to rescue a trade, optimize for model confidence, submit orders, or silently change risk policy.

**Evaluation metrics:** Limit-breach count, prevented loss events, drawdown containment, exposure accuracy, approval latency, false approvals, false rejections, and policy consistency.

**Relationships:** Receives proposals from Strategy and context from Market Structure, Derivatives, Macro, Portfolio, and Audit. Issues approvals to Execution. Reports all decisions to Audit and the Executive Committee.

## 11. Portfolio Department

**Mission:** Manage total exposure across assets, exchanges, strategies, and time horizons as one coordinated portfolio.

**Inputs:** Positions, balances, pending orders, correlations, volatility, strategy allocations, liquidity, risk approvals, and performance attribution.

**Outputs:** Portfolio state, concentration warnings, allocation recommendations, net and gross exposure, correlation clusters, rebalance proposals, and capacity limits.

**Allowed actions:** Recommend allocation, reduce concentration, reserve capacity, reconcile portfolio state, and reject internally inconsistent position assumptions.

**Forbidden actions:** Override Risk, fabricate balances, place orders directly, or allocate capital to an unapproved strategy.

**Evaluation metrics:** Risk-adjusted return, maximum drawdown, concentration, utilization, attribution accuracy, turnover cost, correlation risk, and reconciliation error rate.

**Relationships:** Provides portfolio context to Strategy, Risk, Execution, Learning, and the Executive Committee. Receives confirmed fills from Execution and reconciliation evidence from Audit.

## 12. Execution Department

**Mission:** Translate valid, current, risk-approved instructions into safe and traceable exchange orders.

**Inputs:** Signed trade instruction, unexpired Risk approval, portfolio state, exchange rules, current market data, credentials reference, and execution policy.

**Outputs:** Order requests, acknowledgements, fills, rejections, cancellations, slippage, fees, final execution status, and reconciliation records.

**Allowed actions:** Validate approvals, submit and cancel permitted orders, retry only under idempotent rules, stop on inconsistent state, and trigger the kill switch.

**Forbidden actions:** Invent trades, modify direction or size beyond approval, use withdrawal permissions, execute expired instructions, hide partial fills, or continue after a safety halt.

**Evaluation metrics:** Fill quality, slippage, fee accuracy, rejection rate, duplicate-order count, reconciliation accuracy, execution latency, and safety incidents.

**Relationships:** Receives approvals from Risk and portfolio constraints from Portfolio; uses data from Market Data; sends results to Portfolio, Audit, Learning, and the Executive Committee.

## 13. Audit Department

**Mission:** Preserve an independent, reproducible record of data, decisions, approvals, actions, model versions, and outcomes.

**Inputs:** Events and decisions from every department, configuration versions, model versions, source metadata, orders, fills, errors, and operator actions.

**Outputs:** Immutable audit trail, decision replay package, lineage reports, incident reports, policy violations, reconciliation results, and evidence completeness status.

**Allowed actions:** Reject incomplete records, flag policy breaches, request replay, verify lineage, quarantine suspect outputs, and escalate incidents.

**Forbidden actions:** Rewrite historical records, approve its own exceptions, place orders, or suppress failures to improve reported performance.

**Evaluation metrics:** Record completeness, replay success, lineage coverage, reconciliation accuracy, incident-detection latency, and unresolved exception count.

**Relationships:** Observes every department independently; reports material issues to Risk, Engineering, and the Executive Committee; supplies trusted records to Learning.

## 14. Learning Department

**Mission:** Evaluate outcomes, identify where agents succeed or fail, and propose evidence-based improvements without modifying the live system directly.

**Inputs:** Audit records, market replay, trade outcomes, rejected proposals, model predictions, regime labels, costs, and experiment results.

**Outputs:** Performance attribution, calibration reports, drift alerts, agent-weight proposals, parameter experiments, candidate models, and documented hypotheses.

**Allowed actions:** Run offline experiments, backtests, shadow evaluations, and champion-challenger comparisons; propose changes with evidence.

**Forbidden actions:** Train on unavailable future data, alter live code or weights directly, approve its own promotion, hide negative experiments, or place orders.

**Evaluation metrics:** Out-of-sample improvement, reproducibility, leakage rate, drift-detection quality, experiment validity, and live-versus-backtest divergence.

**Relationships:** Receives trusted data from Audit and outcomes from Strategy, Portfolio, and Execution; submits proposals to Engineering and the Executive Committee.

## 15. Engineering Department

**Mission:** Build, test, deploy, monitor, and recover the software platform while preserving separation between research and live trading.

**Inputs:** Approved requirements, incident reports, learning proposals, code, tests, dependency information, infrastructure state, and deployment policy.

**Outputs:** Reviewed code changes, test results, releases, deployment records, monitoring, rollback plans, security findings, and operational runbooks.

**Allowed actions:** Develop in isolated environments, automate tests, open review requests, deploy approved releases, roll back failures, and maintain observability.

**Forbidden actions:** Commit secrets, deploy unreviewed self-generated code to production, weaken controls to pass tests, change live trading policy silently, or place discretionary trades.

**Evaluation metrics:** Test reliability, change-failure rate, recovery time, deployment traceability, vulnerability remediation time, uptime, and incident recurrence.

**Relationships:** Implements approved proposals from Learning and the Executive Committee; receives incidents from Audit and all operational departments; provides platform capabilities without controlling investment decisions.

## 16. Executive Committee

**Mission:** Produce the final investment decision by weighing specialist evidence, explicit disagreement, portfolio context, uncertainty, and risk constraints.

**Inputs:** Strategy proposals, specialist opinions, Bull and Bear cases, data-quality status, portfolio state, Risk decisions, Audit warnings, and operating-mode policy.

**Outputs:** `REJECT`, `RESEARCH`, `ADVISE`, `REQUEST_APPROVAL`, or `EXECUTE`; decision rationale; dissent summary; confidence; expiry; and required next action.

**Allowed actions:** Request additional analysis, reject weak proposals, select among approved alternatives, record dissent, and route a permitted instruction to the next stage.

**Forbidden actions:** Override a Risk rejection, conceal uncertainty, execute without the required operating-mode authorization, change live code, or place an order directly.

**Evaluation metrics:** Decision expectancy, calibration, drawdown contribution, abstention quality, policy compliance, decision latency, and performance by market regime.

**Relationships:** Receives evidence from every analytical and control department. Sends permitted decisions to Risk and, only after Risk approval, to Execution. Sends every decision and dissent record to Audit and Learning.

## Department Boundary Summary

- Analysis departments produce evidence and opinions, not orders.
- Strategy produces testable proposals, not permission to trade.
- The Executive Committee selects a course of action, but cannot override Risk.
- Risk is the final deterministic permission gate and may always reduce, reject, or halt.
- Execution can act only on a valid, current, risk-approved instruction.
- Audit records and verifies the complete chain independently.
- Learning and Engineering may propose and test improvements, but cannot change the live system without review and approval.
