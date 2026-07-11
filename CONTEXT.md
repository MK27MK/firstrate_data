# Context — firstrate_data

Glossary for the FirstRate Data local loader. Terms only, no implementation.

## FirstRateData

The base loader. Owns a managed on-disk `_directory` and the shared machinery to
fetch, unzip, and persist FirstRate Data archives into it coherently. Named for
the dataset/vendor; "loading" is what its methods do, not its identity. Not used
directly — one subclass per asset type fixes `_asset_type`.

## Asset type

The instrument class being requested, fixed by the subclass rather than passed as
an argument. `FirstRateStocks` (stock) is the only one built so far;
`etf`/`index`/`futures`/`crypto`/`fx`/`options` are added as subclasses lazily.
See ADR 0001.

## Historical data archive

What `data_file` returns: a **zip** grouping one or more `.txt` files in CSV
format. The unzipped `.txt` payloads are the actual bars.

## Period

Span of the request: `full` (entire archive), `month` (last 30 days), `week`
(current trading week from Monday), `day` (last trading day).

## Ticker range

First letter of the ticker (`A`–`Z`). Valid **only** when `period=full`; it
partitions the full archive so it can be pulled letter-by-letter.

## Timeframe

Bar granularity: `1min`, `5min`, `30min`, `1hour`, `1day`. Bars with zero
volume are omitted by the source.

## Adjustment

Price adjustment applied: `adj_split`, `adj_splitdiv`, `UNADJUSTED`. Required by
`data_file`.
