# Context — firstrate_data

Glossary for the FirstRate Data local loader. Terms only, no implementation.

## FirstRateData

The base loader. Owns a managed on-disk `_directory` and the shared machinery to
fetch, unzip, and persist FirstRate Data archives into it coherently. Named for
the dataset/vendor; "loading" is what its methods do, not its identity. Not used
directly — one subclass per asset type fixes `_asset_type`.

## Asset type

The instrument class being requested, fixed by the subclass rather than passed as
an argument. `FirstRateStocks` (stock) and `FirstRateFutures` (futures) are built;
`etf`/`index`/`crypto`/`fx`/`options` are added as subclasses lazily. See ADR 0001.

The API is **not uniform** across asset types: the same endpoint takes different
parameters depending on the type. This is why the loaders diverge rather than
sharing one signature. See ADR 0002.

## Equities

Stocks and ETFs, taken together. They are one family because they share three
things no other asset type has: the split/dividend **adjustments**, the splits and
dividends **metafiles**, and the **ticker range**.

## Historical data archive

What `data_file` returns: a **zip** grouping one or more `.txt` files in CSV
format. The unzipped `.txt` payloads are the actual bars.

## Period

Span of the request: `full` (entire archive), `month` (last 30 days), `week`
(current trading week from Monday), `day` (last trading day).

## Ticker range

First letter of the ticker (`A`–`Z`), which partitions the full archive so it can
be pulled letter-by-letter. **Equities only**, and valid only when `period=full`.
Futures have no ticker range: their `full` request needs nothing extra.

## Timeframe

Bar granularity: `1min`, `5min`, `30min`, `1hour`, `1day`. Bars with zero
volume are omitted by the source. Futures `1day` bars carry a seventh column,
**open interest**; intraday futures bars do not.

## Adjustment

Price adjustment applied to the data. Required by `data_file`, and the accepted
values depend on the asset type — an equity adjustment is meaningless for futures
and vice versa:

- **Equities**: `adj_split`, `adj_splitdiv`, `UNADJUSTED` — corrects for
  corporate actions.
- **Futures**: `contin_adj_ratio`, `contin_adj_absolute`, `contin_UNadj` — corrects
  for roll dates (below).

## Continuous series

The default futures dataset: front-month contracts **stitched** end-to-end into one
unbroken price series per ticker. It is a construction, not a traded instrument.

## Roll date

The point where the continuous series switches from the expiring front-month
contract to the next one. Because the two contracts trade at different prices, the
switch introduces an artificial price jump — which is what a futures *adjustment*
removes, either by ratio or in absolute terms.

## Individual contract

A single real futures contract, as opposed to the continuous series built from many
of them. Delivered by its own endpoint, in two halves: the **archive** (everything
pre-2026) and the **update** (2026 onward, refreshed daily).

## Continuous series audit file

The record of *which* individual contracts were stitched into the continuous series
and when. It is how a continuous series is made falsifiable — without it, the
stitching is unverifiable.
