# AI Trading Corporation - Project Charter

## Vision

Build a financial research and trading system that operates as a corporation of independent AI agents.

Each agent will have a defined professional role, defined data sources, and a clear area of responsibility. The agents will analyze markets from different perspectives, challenge one another, and submit their conclusions to a central decision-making system.

The system's final output will be one of the following:

1. A detailed investment recommendation.
2. A trade signal for human approval.
3. Automatic trade execution.
4. A decision to avoid a trade.

## Markets

The system will initially focus on cryptocurrency markets and support:

- Spot trading.
- Cryptocurrency futures.
- Multiple assets in parallel.
- Multiple exchanges in parallel.
- Long and short positions.
- Multiple strategies and models in parallel.

## Operating Model

The system will operate 24 hours a day and continuously perform the following cycle:

1. Collect data.
2. Validate data quality.
3. Analyze the market through independent engines.
4. Build arguments for and against a trade.
5. Estimate the market regime and level of uncertainty.
6. Calculate risk.
7. Make a decision.
8. Execute the trade or send a recommendation.
9. Track the outcome.
10. Learn from the decision.
11. Propose improvements to models, configuration, or code.

## Core Objectives

- Long-term profitability.
- Survival under changing market conditions.
- Capital protection.
- The ability to inspect and reproduce every decision.
- The ability to replace models without rebuilding the system.
- Learning from both real outcomes and simulated trading.
- Gradual evolution from recommendations to automated trading.

## Architectural Principle

The system will not depend on a single model.

Every decision will be informed by multiple independent engines. Each engine must be able to:

- Support a trade.
- Oppose a trade.
- Provide a confidence level.
- Explain its analysis.
- Report that it lacks sufficient information.

The central system will consider disagreement between agents, not only an average of their recommendations.

## Controlled Self-Improvement

The system will be able to:

- Analyze successful and unsuccessful trades.
- Identify the market conditions in which each model succeeds or fails.
- Adjust weights between models.
- Propose new parameters.
- Propose new strategies.
- Propose code changes.
- Run tests and backtests for proposed changes.

Code changes will never enter the live system directly. They must pass through an experimental environment, automated tests, comparison against the current system, and approval before activation.

## Operating Modes

### Research

Analysis only, without signals or execution.

### Advisory

A detailed investment recommendation is delivered to the user.

### Semi-Automatic

A complete proposed trade is prepared and sent to the user for approval.

### Paper Trading

Simulated execution under live market conditions.

### Automatic

Real trade execution under explicit permissions and deterministic risk controls.
