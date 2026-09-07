# firstrate_data

Downloads [FirstRate Data](https://firstratedata.com) archives into a local
parquet store and queries them with DuckDB.

An active FirstRate Data subscription is required. The client fetches only the
data your own credentials entitle you to, and every download is subject to
FirstRate Data's terms of service and licence. Redistributing the downloaded
data is your responsibility, not this project's.

## Disclaimer

This is an unofficial, independent project. It is not affiliated with,
endorsed by, sponsored by, or supported by FirstRate Data. "FirstRate Data" and
any related marks belong to their owner and are used here to name the service
the client talks to.

The vendor's API can change without notice and break this client. The software
is provided as is, without warranty of any kind, and the authors accept no
liability for lost data, missed trades, or any other loss arising from its use.

## Setup

```bash
uv sync
```

`.env`:

```
FIRSTRATE_DATA_PATH=/path/to/data   # the store goes in FIRSTRATE_DATA_PATH/firstrate_data
FIRSTRATE_USERID=your-userid
FIRSTRATE_BASE_URL=...              # optional; defaults to https://firstratedata.com/api
```

A missing setting raises `firstrate_data.config.MissingSettingError`, which is a
`KeyError`.

## Download

One `Client`. `from_env()` reads the credentials and builds the `Store` the
client files into. Every `download_*()` call returns an `Ingested`.

```python
from firstrate_data import Client, EquitiesAdjustment, Period, Timeframe

client = Client.from_env()

# last trading day, 1-minute bars
client.download_stocks_bars(
    Period.DAY, Timeframe.MIN_1, EquitiesAdjustment.UNADJUSTED
)

# full archive -- takes a ticker_range letter (A-Z)
client.download_stocks_bars(
    Period.FULL,
    Timeframe.DAY_1,
    EquitiesAdjustment.SPLIT_AND_DIVIDEND,
    ticker_range="C",
)

client.download_etf_bars(Period.WEEK, Timeframe.DAY_1, EquitiesAdjustment.SPLIT)
client.download_splits()
client.download_dividends()
client.close()
```

Only stocks have a delisted endpoint. The pre-2026 history is five archives you
fetch one at a time. 2026 onward is an update:

```python
from firstrate_data import DelistedArchive, DelistedUpdate

client.download_delisted_bars(
    DelistedArchive.ARCHIVE_1, Timeframe.MIN_1, EquitiesAdjustment.SPLIT
)
client.download_delisted_bars(
    DelistedUpdate.YEAR, Timeframe.MIN_1, EquitiesAdjustment.SPLIT
)
```

Futures come as a continuous series and as the individual contracts behind it:

```python
from firstrate_data import ContinuousFuturesAdjustment, ContractFiles

client.download_futures_continuous_bars(
    Period.FULL, Timeframe.MIN_1, ContinuousFuturesAdjustment.RATIO
)
client.download_contract_dates()  # which contracts were stitched, and when

client.download_futures_contract_bars(
    ContractFiles.ARCHIVE, Timeframe.MIN_1
)  # stopped before 2026
client.download_futures_contract_bars(
    ContractFiles.UPDATE, Timeframe.DAY_1
)  # trading since 2026
```

The contract endpoint takes no `period` and no `adjustment`. Indices take a
`period` and a `timeframe`, and no adjustment or `ticker_range`:

```python
client.download_index_bars(Period.FULL, Timeframe.DAY_1)
```

Two endpoints serve text rather than an archive, and take the asset type:

```python
from firstrate_data import AssetType

client.last_update(AssetType.STOCK)  # date, or datetime when the vendor states a time
client.download_ticker_listing(AssetType.STOCK)
# [TickerListing(ticker, full_name, start_date, end_date, is_delisted)]
```

The vendor marks a delisted listing row by suffixing its ticker,
`ACTU-DELISTED`, and marks a live one not at all. `TickerListing.ticker` is the
bare symbol and `is_delisted` carries the suffix, so a symbol that outlived the
company behind it comes back as two rows keyed on the same ticker. Only stocks
have a delisted endpoint.

`download_ticker_listing()` writes the rows to the store as well as returning
them, which is what `store.ticker_listing()` reads. The rows are stored as
served: the vendor lists 169 stock symbols twice and leaves the name empty on
14% of the rows, so the name is a label and not a key, and collapsing the rows
would be the library guessing on your behalf.

## The store

Every `download_*()` call:

1. fetches its archive to a spool file
2. unzips it into a temporary directory
3. copies the bars into parquet
4. deletes the unzipped files

The call returns an `Ingested`:

```python
ingested = client.download_stocks_bars(
    Period.WEEK, Timeframe.DAY_1, EquitiesAdjustment.UNADJUSTED
)

ingested.tickers  # int, tickers the archive named; None for a metafile, which has no ticker
ingested.rows  # int, rows written
```

A `Store` holds one DuckDB connection from `Store.from_env()` until
`store.close()`. A `with` block closes it at the end of the block. The read
methods return lazy relations, so read them while the store is open. A client
never closes the store handed to it; `client.close()` closes the HTTP session.

> [!WARNING]
> Parquet is the only copy. Nothing keeps the vendor's CSV and there is no
> offline rebuild. Download a slice again to repair it.

### How the store partitions the bars

Bars land in a uniform seven-column schema: `ts, open, high, low, close,
volume, open_interest`, the last NULL where the source omits it. Every read
selector is also a directory level. The metafiles are tables at the store root,
the ticker listing one table per asset type under that asset type's level:

```
FIRSTRATE_DATA_PATH/firstrate_data/bars/asset_type=stock/adjustment={…}/timeframe={…}/ticker={…}/{date}_{ingest}_{uuid}.parquet
FIRSTRATE_DATA_PATH/firstrate_data/bars/asset_type=futures/adjustment={…}/timeframe={…}/ticker={…}/{date}_{ingest}_{uuid}.parquet
FIRSTRATE_DATA_PATH/firstrate_data/catalog.parquet
FIRSTRATE_DATA_PATH/firstrate_data/bars/asset_type={…}/ticker_listing.parquet
FIRSTRATE_DATA_PATH/firstrate_data/splits.parquet
FIRSTRATE_DATA_PATH/firstrate_data/dividends.parquet
FIRSTRATE_DATA_PATH/firstrate_data/contin_audit.parquet
```

A path names every level, including the ones no endpoint asks about: the store
files an index bar under `adjustment=UNADJUSTED`. A futures continuous series
and the individual contracts it was stitched from are told apart by their
adjustment, `contin_adj_ratio` against `UNADJUSTED`. The levels are the fields
of `BarType` in `firstrate_data.domain`, and a read answers with all four as
columns whatever it spans.

Each bar's own identity picks its directory, not the request that fetched it,
so two fetches of one ticker land in the same directory. A stock is filed under
its bare symbol whichever bundle carried it, listed or delisted, so a symbol
that two companies held over disjoint years reads back as one continuous
series. Which company held it over which days is `store.ticker_listing()`'s to
say.

The store writes only inside its own `firstrate_data/` subdirectory, so
the directory `FIRSTRATE_DATA_PATH` names can hold other tenants. `spool/`,
`.duckdb_temp/` and one `.ingest-*` per archive the store reads sit in there
too, since an archive needs as much free space as the bars it becomes.

### One ticker, one copy

A ticker is filed once. Every ingest weighs the span it carries for a ticker
against the span the catalog already holds:

- same first bar, last bar no earlier than the held one — an update, or the
  same archive re-fetched. It replaces the ticker's files, which is what makes
  a re-run of an interrupted bundle resume rather than refuse
- anything else — a different start under one name, or an archive ending before
  what is filed — is a conflict. The ingest's own files are removed and it
  raises `ConflictingBarsError`, a `ValueError`, having filed nothing

The last rule is what a shorter period runs into: a `WEEK` fetched on Wednesday
starts on Monday, not where the ticker's held history starts, so it is refused
rather than spliced. Re-fetch with `period=FULL`.

It is also what a stock symbol two companies held over *overlapping* years runs
into — 187 of them, where the vendor's listed and delisted bundles both carry
bars for the same minutes. The store cannot tell one company's re-served
history from another's, so it files neither.
`store.ticker_listing(ticker=...)` names the companies and the days each held
the symbol, which is what deciding between them takes.

### The catalog

`catalog.parquet` holds one row per ticker per bar type — the bar type's four
levels, the first and last bar filed, and how many. Every ingest keeps it in
step, and it is what makes the conflict check above cost a read of one small
file rather than a walk of the tree.

```python
store.catalog()  # the whole thing, as a relation
store.last_bar(BarType(AssetType.STOCK, timeframe=Timeframe.MIN_1))
```

### Restated series take `period=FULL` only

The vendor rewrites the history of `adj_split`, `adj_splitdiv`,
`contin_adj_ratio` and `contin_adj_absolute` backwards when a corporate action
or a roll lands, so two fetches taken either side of one sit on different bases.
Asking for one of those four with any other period raises `NotOfferedError`, a
`ValueError`, before the request goes out. `UNADJUSTED` and `contin_UNadj` are
never restated.

### Damaged bars

A few vendor payloads have spliced bytes: a bar cut off mid-stamp with a bar
from days later running into it. Such a line aborts the scan and the ingest
raises, having filed nothing.

### Timestamps

`ts` is a `TIMESTAMPTZ` instant. The vendor delivers a naive stamp in Eastern
time, and UTC for crypto. Ingest localizes it once, on the way in, so the
repeated hour of the DST fall-back resolves to the standard-time offset.

The store pins the session timezone to `America/New_York`, because DuckDB reads
a naive literal in the session timezone and that otherwise comes from the
machine's locale. Pinned, `ts >= '2024-01-02 09:30:00'` selects the same bars in
Rome and in New York.

## Queries

One read method for every bar in the tree. Every omitted keyword spans all its
values, and the result is a lazy `duckdb.DuckDBPyRelation` you filter,
aggregate, join, or hand to pandas or Arrow.

```python
from firstrate_data import (
    AssetType,
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    Store,
    Timeframe,
    TradingHours,
    Unadjusted,
)

store = Store.from_env()  # reads FIRSTRATE_DATA_PATH

aapl = store.bars(
    asset_type=AssetType.STOCK,
    timeframe=Timeframe.DAY_1,
    adjustment=EquitiesAdjustment.SPLIT,
    ticker="AAPL",
)
aapl.aggregate("min(ts), max(ts), count(*)").show()
frame = aapl.df()  # pandas DataFrame, materialized only now (needs pandas installed)

store.bars(
    asset_type=AssetType.FUTURES,
    timeframe=Timeframe.DAY_1,
    adjustment=ContinuousFuturesAdjustment.RATIO,
    ticker="ES",
)
# the individual contracts, told apart from the continuous series by adjustment
store.bars(
    asset_type=AssetType.FUTURES,
    timeframe=Timeframe.DAY_1,
    adjustment=Unadjusted.UNADJUSTED,
    ticker=["ESH24", "ESM24"],
)

# across the whole tree; every level is also a column
store.bars(timeframe=Timeframe.DAY_1).aggregate(
    "asset_type, adjustment, count(*)"
).show()

store.splits()  # ticker, date, ratio
store.dividends()  # ticker, date, amount
store.contract_dates()

store.ticker_listing(ticker="ABX")  # who held the symbol, and over which days
```

`start`, `end` and `hours` narrow a read past the tree (`from datetime import date`). Both dates name a whole
day and both are kept; `hours=TradingHours.REGULAR` keeps 09:30–16:00 Eastern
and raises `ValueError` for an asset type that defines no session — crypto, FX,
futures, or a read that named no asset type at all:

```python
store.bars(
    asset_type=AssetType.STOCK,
    timeframe=Timeframe.MIN_1,
    adjustment=EquitiesAdjustment.SPLIT,
    ticker="AAPL",
    start=date(2026, 1, 2),
    end=date(2026, 3, 31),
    hours=TradingHours.REGULAR,
)
```

The vendor serves splits and dividends as an archive of one headerless file per
ticker, and each file holds the ticker in its name alone. The store declares the
columns and takes the ticker from the file name. No fetch of
`contract_dates()` has run here, so its shape is whatever the DuckDB sniffer
reads (issue #15).

A bars selector that matches nothing returns an empty relation of the right
shape. A metafile or ticker listing that was never fetched raises
`FileNotFoundError`, having no fixed shape to return an empty relation of.

The selectors build the glob, which is what makes a fine slice fast: about 970x
over a wide glob with a `WHERE`, measured at 3000 partitions. Prefer a selector
over `.filter(...)` for anything that's a level of the tree. `start`, `end` and
`hours` are `WHERE` clauses, because neither the calendar nor the clock is a
level.

macOS writes an AppleDouble `._*` sidecar beside every parquet file on exFAT and
NTFS volumes, and `._2026-07-17_3f2a.parquet` matches a bare `*.parquet`. The
store names every file for the date that produced it, so the read glob leads
with a digit class, `[0-9]*.parquet`, which no dotfile matches.

## Progress

`firstrate_data.download.progress` draws one tqdm bar per fetch and disables
itself when stderr isn't a terminal. Nothing to configure, and nothing to pass.

## Bundles

A bundle is one asset type's universe, described as a config and swept in one
call. FirstRate sells its data in the same shape, so one config downloads the
bundle you bought. The config classes are plain frozen dataclasses in
`firstrate_data.download.bundles`, and need no credentials and no store to
build:

- `BundleConfig` — asset type and timeframes. Enough for indices, FX and crypto
- `EquitiesBundleConfig` — adds the adjustment, the ticker ranges, and the
  splits and dividends metafiles. Use it for ETFs
- `StocksBundleConfig` — adds the pre-2026 delisted archives
- `FuturesBundleConfig` — adds the roll adjustment, the individual contracts,
  and the contract-dates audit file

`None` timeframes means every timeframe, `None` ticker range means `A`–`Z`.
`include_delisted_archives` and `include_individual_contracts` take `True` for
all of them, `False` for none, or an iterable to select.

```python
from firstrate_data import AssetType, Client, EquitiesAdjustment, Timeframe
from firstrate_data.download.bundles import StocksBundleConfig

BUNDLE = StocksBundleConfig(
    asset_type=AssetType.STOCK,
    timeframes=[Timeframe.DAY_1, Timeframe.MIN_1],
    adjustment=EquitiesAdjustment.UNADJUSTED,
    ticker_range=None,
    include_splits=True,
    include_dividends=True,
    include_company_profiles=False,
    include_delisted_archives=True,
)

client = Client.from_env()
try:
    for ingested in client.download_bundle(BUNDLE):
        print(ingested)
finally:
    client.close()
```

`download_bundle()` fetches the next archives on a small thread pool while the
current one is written, since a fetch waits on the vendor and a write on
DuckDB. Writes stay on the calling thread: the store holds one connection, and
an unbounded prefetch would spool the whole bundle to disk at once. `prefetch`
(default 2) is how many archives may be in flight.

A full-history archive whose last filed bar already reaches the vendor's
`last_update` is not fetched again, and is absent from what the call returns.
`refresh=True` fetches every archive the bundle names regardless. Delisted
archives, contract halves and metafiles are always fetched: their tickers stop
trading, or they leave no catalog row to judge from.

Combinations the vendor doesn't serve are dropped from the plan rather than
raised — `UNADJUSTED` outside 1min and 1day, the restated adjustments outside
`period=FULL`. Anything else that goes wrong propagates: a bad response or an
archive that won't file stops the sweep, and re-running resumes.

`bundle_requests(config)` yields the same plan as `Request` objects without
fetching anything, which is the dry run.

## Parameters

| Enum | Values |
| --- | --- |
| `Period` | `FULL`, `MONTH`, `WEEK`, `DAY` |
| `Timeframe` | `MIN_1`, `MIN_5`, `MIN_30`, `HOUR_1`, `DAY_1` |
| `EquitiesAdjustment` | `SPLIT`, `SPLIT_AND_DIVIDEND`, `UNADJUSTED` |
| `ContinuousFuturesAdjustment` | `RATIO`, `ABSOLUTE`, `UNADJUSTED` |
| `Unadjusted` | `UNADJUSTED`, for indices and futures contracts. The endpoint sends none |
| `AssetType` | `STOCK`, `ETF`, `INDEX`, `FUTURES`, `CRYPTO`, `FX`, `OPTIONS` |
| `TradingHours` | `ALL`, `REGULAR` |
| `DelistedArchive` | `ARCHIVE_1` .. `ARCHIVE_5` (pre-2026) |
| `DelistedUpdate` | `YEAR` (2026+), `WEEK` (last week only) |
| `ContractFiles` | `ARCHIVE` (pre-2026, frozen), `UPDATE` (2026+, daily) |
| `OtherData` | `SPLITS`, `DIVIDENDS`, `COMPANY_PROFILES`, `CONTRACT_DATES` |

The API differs across asset types, and one `download_*` method serves each.
Splits, dividends and `ticker_range` exist for stocks and ETFs only, and
`ticker_range` only with `period=FULL`. Each request is an object that carries
its own endpoint and refuses a combination the vendor doesn't serve. It tells
the store where the bars belong and whether they replace what's there.
