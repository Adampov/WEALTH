# ADR 0010: Deterministic Cross-Source Candle Reconciliation

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The market-data platform now stores canonical historical candles from two public providers.
Provider-specific quality checks prove that each bounded sequence is internally valid, but they do
not show whether two venues observed materially different prices, whether one source omitted an
interval, or whether an apparent difference is inside an explicitly accepted operating tolerance.

Different venues are not interchangeable sources of one authoritative price. They can have
different participants, liquidity, quote assets, and volume. Reconciliation must preserve those
differences, retain both records as evidence, and avoid silently choosing, averaging, repairing, or
promoting either source as market truth.

## Decision

Add a provider-independent application service and versioned domain report for selected,
historical candle windows:

- A comparison has one explicit key, primary stream, reference stream, aligned window, and
  versioned policy.
- Streams must be distinct and must use the same exact canonical instrument, instrument type, and
  timeframe.
- Cross-quote comparison such as `BTC-USD` against `BTC-USDT` fails before analysis. Currency,
  stablecoin, or instrument conversion requires a future approved metadata and conversion policy.
- Each source passes through the existing deterministic sequence-quality auditor independently.
- A failed source-quality result makes the reconciliation status `blocked`.
- Missing intervals remain explicit primary- or reference-missing findings and are never filled.
- Intervals that contain one unambiguous record from each source retain both record identifiers and
  symmetric differences for open, high, low, close, and base volume.
- Symmetric difference in basis points is
  `abs(primary - reference) / max(abs(primary), abs(reference)) * 10,000`.
- Two zero values have zero difference. The metric is independent of which source is called primary.
- Price tolerance is required and explicit. Base-volume tolerance is optional because venue volume
  is inherently venue-specific; volume difference is always measured but becomes a finding only
  when the policy enables a limit.
- A difference exactly at the configured limit is accepted. A greater difference creates a
  machine-readable field-specific finding.
- A complete, quality-valid window with no threshold finding is `pass`.
- A complete, quality-valid window with a threshold finding is `divergent`.
- A window with failed source quality is `blocked`, even when some aligned intervals can still be
  compared for diagnostic evidence.
- One reconciliation is capped at 100,000 expected candles before materializing the report.
- Given the same records, order, streams, window, and policy, the service returns the same report.

The first slice compares selected canonical records supplied by the caller. It does not create a
continuous job, choose a preferred provider, persist a blended candle, or change accepted source
records.

## Safety Boundary

This decision authorizes deterministic comparison of already available canonical candle evidence.
It does not authorize:

- Treating either provider as unquestionable truth.
- Averaging, replacing, repairing, or deleting source records.
- Comparing instruments or quote assets through an implicit conversion.
- Automatically changing tolerances from observed data.
- Using a reconciliation result as a trade signal or Risk approval.
- Private data, credentials, orders, balances, positions, or execution.
- Continuous collection, scheduling, live streams, alerts, or automated incident response.

## Consequences

### Positive

- Missing-source and cross-venue differences become explicit, reproducible evidence.
- Every comparison remains traceable to the two original canonical record identifiers.
- The same quality gate and report contract work for current and future providers.
- Symmetric basis-point metrics avoid privileging the primary source mathematically.
- Price and volume policies remain visible, versioned, testable, and replaceable.
- Unsafe cross-instrument comparison fails before producing a misleading result.

### Negative

- Reconciliation reports differences but does not determine which source is correct.
- Exact canonical instrument matching prevents `BTC-USD` versus `BTC-USDT` comparison until an
  approved instrument-metadata and conversion boundary exists.
- The first slice does not persist reports or expose dashboards and alerts.
- Static tolerances require calibration and governance before operational use.
- A duplicate or incomplete source sequence blocks a clean reconciliation outcome.

## Alternatives Considered

### Select one preferred provider

Rejected. Provider preference hides disagreement and creates a single point of data-quality
failure.

### Average provider prices

Rejected. An average can conceal stale, missing, manipulated, or structurally different venue data
and would create a new synthetic record without an approved contract.

### Compare percentage change instead of candle values

Deferred. Return comparison can complement absolute cross-venue differences, but it does not
replace interval-level OHLC and missing-source evidence.

### Apply one default tolerance

Rejected. A hidden global tolerance would mix operational policy with the comparison algorithm and
could silently become inappropriate across assets, venues, and timeframes.

## Review Triggers

Revisit this decision when adding instrument metadata, quote conversion, persisted reconciliation
history, provider scoring, automatic alerts, live streams, adaptive thresholds, or a governed
source-selection policy.
