# Context — firstrate_data

Glossary for the FirstRate Data local loader. Terms only, no implementation.

## FirstRateData

The base loader. Owns the credentials and the fetching: it asks FirstRate Data for
an archive and hands it to the **catalog** to keep. Named for the dataset/vendor;
"loading" is what its methods do, not its identity. Not used directly — one
subclass per asset type fixes the asset type.

## Catalog

The local store, and the only thing that knows its layout. Every **request** maps
to exactly one location, derived from every parameter of that request, so two
different requests never resolve to the same place and no archive can overwrite
another's. A store is written now and read back later, so the catalog is where
that mapping is stated once rather than at each call site. See ADR 0004.

## Request

One call to one endpoint: the parameters that identify a slice of the dataset,
together with the endpoint they are meaningful for. The two are inseparable — the
same `timeframe` and `adjustment` mean one thing at the listed endpoint and another
at the delisted one — which is why a request is a single thing and not a loose bag
of arguments. Internal: callers of `download_*` never build one.

## Asset type

The instrument class being requested, fixed by the subclass rather than passed as
an argument. `FirstRateStocks` (stock) and `FirstRateFutures` (futures) are built;
`etf`/`index`/`crypto`/`fx`/`options` are added as subclasses lazily. See ADR 0001.

The API is **not uniform** across asset types: the same endpoint takes different
parameters depending on the type. This is why the loaders diverge rather than
sharing one signature. See ADR 0002.

## Dataset

Which body of data within an asset type a bar belongs to, once it is at rest:
`listed` or `delisted` for stocks, `continuous` for futures, `contract` for the
individual contracts. Each is fetched on its own terms — a different endpoint, or
different parameters — but they describe one universe, so a question asked of the
whole universe should not have to name them one by one. See ADR 0005.

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

## Vintage

The moment a slice was fetched. Two pulls of the same **period** at different times
are the same *request* and different data, so the vintage is what tells them apart.
It matters because the vendor's answer to an unchanged question changes over time —
history grows at one end, and **adjusted** history is rewritten at the other.

## Ticker range

First letter of the ticker (`A`–`Z`), which partitions the full archive so it can
be pulled letter-by-letter. **Equities only**, and valid only when `period=full`.
Futures have no ticker range: their `full` request needs nothing extra.

## Timeframe

Bar granularity: `1min`, `5min`, `30min`, `1hour`, `1day`. Bars with zero
volume are omitted by the source. Futures `1day` bars carry a seventh column,
**open interest**; intraday futures bars do not.

## Delisted ticker data

Data for tickers that no longer trade. **Stocks only.** It is a separate dataset from
the listed one, not a filter over it: it has no *period* and no *ticker range*, and is
partitioned instead by a **delisted selector**. Without it the listed archive is a
survivorship-biased view of the market.

Separate is how it is *fetched*, not how it is *known*. At rest it is one **dataset**
among others of the same asset type, and the market is the union — so the plain
question is the unbiased one and the bias is what you have to ask for. The distinction
still has to survive: a ticker can be reused by a later company, and a dead namesake
must not be mistaken for its own early history.

## Delisted selector

Which slice of the delisted dataset is being asked for — always exactly one of:

- **Delisted archive** — a numbered slice of the pre-2026 history, downloaded on its
  own. Frozen: the past does not gain new delistings.
- **Delisted update** — the 2026-onward delistings, as either the whole `year` or just
  the last `week`. Refreshed at the end of each week (Sunday). `week` is contained in
  `year`.

## Complete bundle

Everything there is to know about the stock universe at a given timeframe and
adjustment: the full listed archive across every ticker range, the entire delisted
history, and the splits and dividends that explain the price adjustments in both. A
complete pull takes `year` and not `week`, since the latter is already inside it.

## Adjustment

Price adjustment applied to the data. Required by `data_file`, and the accepted
values depend on the asset type — an equity adjustment is meaningless for futures
and vice versa:

- **Equities**: `adj_split`, `adj_splitdiv`, `UNADJUSTED` — corrects for
  corporate actions.
- **Futures**: `contin_adj_ratio`, `contin_adj_absolute`, `contin_UNadj` — corrects
  for roll dates (below).

`UNADJUSTED` is not offered at every timeframe, and the offer differs by request:
listed data has it at `1min` and `1day`, delisted data at `1min` only. So an
adjustment is not meaningful on its own — only paired with a timeframe.

## Adjustment basis

The corporate actions a given **adjusted** series already accounts for — in effect,
the date it was computed as of. Every new split or dividend rewrites all the history
before it, so the same bar of the same ticker at the same timestamp has different
adjusted prices depending on when it was fetched. Two series on different bases are
not comparable and must not be joined end-to-end: the seam would read as a price move
that never happened. Unadjusted prices have no basis — nothing restates them — which
is why they are the only ones that can simply be extended. See ADR 0005.

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
