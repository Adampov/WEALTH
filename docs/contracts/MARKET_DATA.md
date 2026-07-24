# Market Data Contract — First Slice

## Purpose

This contract establishes the first provider-independent market-data record and the point-in-time
boundary required for honest research, replay, and backtesting.

It does not define a trading strategy and does not connect to an exchange.

## Canonical Candle

`CanonicalCandle` represents one final OHLCV interval. Every record includes:

- Schema version and unique record ID.
- Source, venue, instrument, and instrument type.
- Timeframe, open time, and close time.
- Observation and processing times.
- Exact decimal OHLCV values.
- Provider sequence when available.
- One or more lineage references.

The contract rejects:

- Unknown fields or mutable records.
- Non-positive prices or negative volume.
- OHLC values that contradict one another.
- Intervals that do not match their declared timeframe.
- Candles reported as observed before they closed.
- Processing timestamps earlier than observation timestamps.
- Partial candles.

## Point-in-Time Replay

`MarketReplay.slice_at(evaluation_time)` returns only records whose `observed_at` is less than or
equal to the evaluation time. A candle that closed earlier but arrived late remains unavailable
until its actual observation time.

Replay input is sorted deterministically. Duplicate natural keys and conflicting revisions fail
closed with machine-readable reason codes.

This boundary is mandatory for future features, signals, strategies, backtests, evaluation, and
learning. No analytical component may receive the replay object's complete future record set.

## Current Limitations

- Only final candles are modeled.
- No exchange adapter or historical downloader exists.
- No gap detector, correction stream, or cross-source reconciliation exists yet.
- The in-memory replay slice is for contract validation, not large-scale storage.
- Trades, ticker, order book, funding, open interest, and liquidation schemas remain future work.
