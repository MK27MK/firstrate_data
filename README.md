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

One client class per asset type. `from_env()` reads the credentials and builds
the `Store` the client files into.

```python
from firstrate_data import EquitiesAdjustment, Period, StockClient, Timeframe

stocks = StockClient.from_env()

# last trading day, 1-minute bars
stocks.download_historical_bars(
    Period.DAY, Timeframe.MIN_1, EquitiesAdjustment.UNADJUSTED
)

# full archive -- takes a ticker_range letter (A-Z)
stocks.download_historical_bars(
    Period.FULL,
    Timeframe.DAY_1,
    EquitiesAdjustment.SPLIT_AND_DIVIDEND,
    ticker_range="C",
)

stocks.download_splits()
stocks.download_dividends()
```

Only stocks have a delisted endpoint. The pre-2026
history is five archives you fetch one at a time. 2026 onward is an update:

```python
from firstrate_data import DelistedArchive, DelistedUpdate

stocks.download_delisted_bars(
    DelistedArchive.ARCHIVE_1, Timeframe.MIN_1, EquitiesAdjustment.SPLIT
)
stocks.download_delisted_bars(
    DelistedUpdate.YEAR, Timeframe.MIN_1, EquitiesAdjustment.SPLIT
)
```

Futures come as a continuous series and as the individual contracts behind it:

```python
from firstrate_data import ContinuousFuturesAdjustment, ContractFiles, FuturesClient

futures = FuturesClient.from_env()

futures.download_historical_bars(
    Period.FULL, Timeframe.MIN_1, ContinuousFuturesAdjustment.RATIO
)
futures.download_continuous_audit()  # which contracts were stitched, and when

futures.download_contract_bars(
    ContractFiles.ARCHIVE, Timeframe.MIN_1
)  # stopped before 2026
futures.download_contract_bars(
    ContractFiles.UPDATE, Timeframe.DAY_1
)  # trading since 2026
```

The contract endpoint takes no `period` and no `adjustment`. The contracts are
their own dataset, which `futures_contract_bars()` reads and `futures_bars()`
doesn't.

Indices take one call. The index endpoint accepts `type`, `period` and
`timeframe`, and no adjustment or `ticker_range`:

```python
from firstrate_data import IndexClient

indices = IndexClient.from_env()
indices.download_historical_bars(Period.FULL, Timeframe.DAY_1)
```

Two endpoints serve text rather than an archive. Both live on the base client,
return a value, and write nothing to the store:

```python
stocks.download_last_update()  # date, or datetime when the vendor states a time
stocks.download_ticker_listing()  # [TickerListing(ticker, name, start_date, end_date)]
```

## The store

Every `download_*()` call:

1. fetches its archives to a spool file
2. unzips each archive into a temporary directory
3. copies the bars into parquet
4. deletes the unzipped files

The call returns an `Ingested`:

```python
ingested = stocks.download_historical_bars(
    Period.WEEK, Timeframe.DAY_1, EquitiesAdjustment.UNADJUSTED
)

ingested.tickers  # int, tickers the archive named; None for a metafile, which has no ticker
ingested.rows  # int, rows written -- fewer than the archive holds, for an increment
ingested.rejected  # int, lines DuckDB could not parse. Above zero means rows are missing
ingested.suspect  # int, written rows that break a bar's own arithmetic
```

A `Store` holds one DuckDB connection from `Store.from_env()` until
`store.close()`. A `with` block closes it at the end of the block. The read
methods return lazy relations, so read them while the store is open. A client
never closes the store handed to it.

> [!WARNING]
> Parquet is the only copy. Nothing keeps the vendor's CSV and there is no
> offline rebuild. Download a slice again to repair it.

### How the store partitions the bars

Bars land in a uniform seven-column schema: `ts, open, high, low, close,
volume, open_interest`, the last NULL where the source omits it. Every read
selector is also a directory level. The metafiles are tables at the store root:

```
FIRSTRATE_DATA_PATH/firstrate_data/bars/asset_type={…}/dataset={…}/adjustment={…}/timeframe={…}/ticker={…}/{date}_{ingest}_{uuid}.parquet
FIRSTRATE_DATA_PATH/firstrate_data/splits.parquet
FIRSTRATE_DATA_PATH/firstrate_data/dividends.parquet
FIRSTRATE_DATA_PATH/firstrate_data/contin_audit.parquet
```

Every path names all five levels, including the ones no endpoint asks about: the
store files an index bar under `adjustment=UNADJUSTED`, a futures contract under
`dataset=contract/adjustment=UNADJUSTED`. The levels are the fields of
`BarType` in `firstrate_data.domain`. Each bar's own identity picks its
directory, not the request that fetched it, so two fetches that overlap land in
the same directory. Run an interrupted download again to recover from it.

The store writes only inside its own `firstrate_data/` subdirectory, so
the directory `FIRSTRATE_DATA_PATH` names can hold other tenants. `spool/`,
`.duckdb_temp/` and one `.ingest-*` per archive the store reads sit in there
too, since an archive needs as much free space as the bars it becomes.
`Store(directory, spool=…)` puts the spool on another volume, which is what
`--spool-dir` and `Client.from_env(spool_dir=…)` do for you.

### Replace or append

A `period=FULL` fetch, every delisted fetch and every contract fetch carries the
whole history of every ticker it names. Each of those deletes a ticker's bars
before it writes them. Every other fetch appends. The delete is per ticker, so
the contract `archive` and the contract `update`, which name disjoint tickers,
don't erase each other.

A shorter period starts from its own beginning: a `WEEK` fetched on Wednesday
re-serves Monday and Tuesday. An increment keeps only the bars newer than the
last one the store holds for that ticker, so fetching the same week twice
writes nothing the second time. The per-ticker cutoff comes from the parquet
footers, which takes milliseconds. `max(ts)` is a full column scan and costs one
to two minutes over the real store.

### Restated series take `period=FULL` only

The vendor rewrites the history of `adj_split`, `adj_splitdiv`,
`contin_adj_ratio` and `contin_adj_absolute` backwards when a corporate action
or a roll lands, so two fetches taken either side of one sit on different bases.
Asking for one of those four with any other period raises `NotOfferedError`, a
`ValueError`, before the request goes out. `UNADJUSTED` and `contin_UNadj` are
never restated.

### Damaged bars

A few vendor payloads have spliced bytes: a bar cut off mid-stamp with a bar
from days later running into it. Such a line loses its own row, the rest of the
scan proceeds, and the line goes to the quarantine that `store.quarantined()`
reads. The quarantine spans every ingest the store has done and nothing ever
leaves it. `ingested.rejected` counts lines, not DuckDB reject rows, of which
one splice produces more than one.

A splice that breaks on a comma parses cleanly and gives the store a bar whose
high is below its low, or whose volume is negative. Every ingest scans the rows
it just wrote for arithmetic a bar can't break: `high < low`, a high below the
open or close, a low greater than either, a negative volume. The count lands in
`ingested.suspect`. The rows stay in the store and
`store.suspect_bars(ticker="ABC")` reads them back.

### Timestamps

`ts` is a `TIMESTAMPTZ` instant. The vendor delivers a naive stamp in Eastern
time, and UTC for crypto. Ingest localizes it once, on the way in, so the
repeated hour of the DST fall-back resolves to the standard-time offset.

The store pins the session timezone to `America/New_York`, because DuckDB reads
a naive literal in the session timezone and that otherwise comes from the
machine's locale. Pinned, `ts >= '2024-01-02 09:30:00'` selects the same bars in
Rome and in New York.

## Queries

Reads return a lazy `duckdb.DuckDBPyRelation`. Filter, aggregate, join, or hand
it to pandas or Arrow.

```python
from firstrate_data import (
    ContinuousFuturesAdjustment,
    Dataset,
    EquitiesAdjustment,
    Store,
    Timeframe,
)

store = Store.from_env()  # reads FIRSTRATE_DATA_PATH

aapl = store.stock_bars(Timeframe.DAY_1, EquitiesAdjustment.SPLIT, ticker="AAPL")
aapl.aggregate("min(ts), max(ts), count(*)").show()
recent = aapl.filter("ts >= DATE '2026-01-01'").order("ts")
frame = aapl.df()  # pandas DataFrame, materialized only now (needs pandas installed)

# delisted tickers are in by default; Dataset.LISTED drops them
store.stock_bars(Timeframe.DAY_1, EquitiesAdjustment.SPLIT, dataset=Dataset.LISTED)

store.futures_bars(Timeframe.DAY_1, ContinuousFuturesAdjustment.RATIO, ticker="ES")
store.futures_contract_bars(Timeframe.DAY_1, ticker="ESH24")  # takes no adjustment
store.index_bars(Timeframe.DAY_1, ticker="SPX")  # takes no adjustment, no dataset

# across the whole tree; every level is also a column
store.bars(timeframe=Timeframe.DAY_1).aggregate(
    "asset_type, adjustment, count(*)"
).show()

store.splits()  # ticker, date, ratio
store.dividends()  # ticker, date, amount
store.contin_audit()
```

The vendor serves splits and dividends as an archive of one headerless file per
ticker, and each file holds the ticker in its name alone. The store declares the
columns and takes the ticker from the file name. No fetch of `contin_audit()`
has run here, so its shape is whatever the DuckDB sniffer reads (issue #15).

A bars selector that matches nothing returns an empty relation of the right
shape. A metafile that was never fetched raises `FileNotFoundError`, having no
fixed shape to return an empty relation of.

The selectors build the glob, which is what makes a fine slice fast: about 970x
over a wide glob with a `WHERE`, measured at 3000 partitions. Prefer a selector
over `.filter(...)` for anything that's a level of the tree. The asset type is
in the method name, so a futures read can't take an equities adjustment.

macOS writes an AppleDouble `._*` sidecar beside every parquet file on exFAT and
NTFS volumes, and `._2026-07-17_3f2a.parquet` matches a bare `*.parquet`. The
store names every file for the date that produced it, so the read glob leads
with a digit class, `[0-9]*.parquet`, which no dotfile matches.

## Progress

Clients draw nothing by default. Pass a reporter to get bars, or write your own
`ProgressReporter`, which is one `track(label, total, unit)` method, to send
progress elsewhere. `TqdmProgress` nests the sweep on the top line and each
archive it fetches beneath it, and disables itself when stderr isn't a
terminal.

```python
from firstrate_data.download.progress import NullProgress, TqdmProgress

stocks = StockClient.from_env(progress=TqdmProgress())
```

## The complete bundles

A bundle is one asset type's universe, swept in one call. FirstRate sells its
data in the same shape, so one command downloads the bundle you bought.

- `stocks`: for every timeframe x adjustment pair, the listed archive of all
  26 ticker ranges. Then the five pre-2026 delisted archives, the 2026 delisted
  archive, and the splits and dividends behind both.
- `indices`: one archive per timeframe.
- `futures`: one archive per timeframe x roll adjustment, plus the audit file.
  The individual contracts are the largest part of the pull and arrive only with
  `--contracts`: both halves when the period is `FULL`, the 2026 update alone
  otherwise.

```bash
firstrate bundle stocks --timeframes 1min 1day --adjustments adj_splitdiv UNADJUSTED
firstrate bundle indices --timeframes 1min 1day
firstrate bundle futures --timeframes 1day --contracts
firstrate bundle stocks --dry-run --timeframes 1day   # print the plan, fetch nothing
```

`Bundle.stocks(...)`, `Bundle.indices(...)` and `Bundle.futures(...)` build the
same plans from Python, and need no credentials and no store to do it.
`BundleDownloader` sweeps one, fetching more than one archive at once and
filing them one at a time:

```python
from firstrate_data.download.bundle import Bundle, BundleDownloader

report = BundleDownloader(max_workers=4).sweep(
    Bundle.stocks(
        Period.FULL, [Timeframe.DAY_1], [EquitiesAdjustment.SPLIT_AND_DIVIDEND]
    )
)

report.ingested  # [(name, Ingested)]  what each cell left in the store
report.failed  # [(name, error)]     the API should have served these -- retry
report.skipped  # [(name, why)]       the API does not offer these -- never retry
report.rejected  # int                 unparseable lines across the whole sweep
report.downloaded  # int                 bytes fetched
report.seconds  # float               wall clock
report.megabytes_per_second  # end-to-end throughput, ingest included
```

`sweep()` builds its own client from the environment, since each cell says which
client serves it. Pass `client=` to hand it one you already hold.
`ticker_ranges` defaults to the whole alphabet. Narrow it to sweep a slice.
`progress` defaults to `TqdmProgress()` here.

Nothing in a sweep raises. A bad response, an archive that won't file, and a
fetch that dies on something other than HTTP each count as one `failed` cell.
The report names it, so a targeted retry is a narrower bundle. `skipped` cells
are combinations the API doesn't offer: `UNADJUSTED` outside 1min and 1day
listed, `UNADJUSTED` outside 1min delisted, and the restated adjustments for
anything but `period=FULL`. The sweep fetches splits and dividends once per run,
since they carry no timeframe.

A re-run fetches every cell again. A `FULL` cell replaces what it wrote last
time, and an interrupted archive resumes.

## Parameters

| Enum | Values |
| --- | --- |
| `Period` | `FULL`, `MONTH`, `WEEK`, `DAY` |
| `Timeframe` | `MIN_1`, `MIN_5`, `MIN_30`, `HOUR_1`, `DAY_1` |
| `EquitiesAdjustment` | `SPLIT`, `SPLIT_AND_DIVIDEND`, `UNADJUSTED` |
| `ContinuousFuturesAdjustment` | `RATIO`, `ABSOLUTE`, `UNADJUSTED` |
| `FuturesContractAdjustment` | `UNADJUSTED`, store-side only. The endpoint sends none |
| `IndexAdjustment` | `UNADJUSTED`, store-side only. The endpoint sends none |
| `DelistedArchive` | `ARCHIVE_1` .. `ARCHIVE_5` (pre-2026) |
| `DelistedUpdate` | `YEAR` (2026+), `WEEK` (last week only) |
| `ContractFiles` | `ARCHIVE` (pre-2026, frozen), `UPDATE` (2026+, daily) |
| `Dataset` | `LISTED`, `DELISTED`, `CONTINUOUS`, `CONTRACT` |
| `MetafileType` | `SPLITS`, `DIVIDENDS`, `CONTIN_AUDIT` |

The API differs across asset types. One client class serves each asset type,
over a `Client` base that holds what they share. Splits, dividends and
`ticker_range` exist for stocks and ETFs only, and `ticker_range` only with
`period=FULL`. Each request is an object that carries its own endpoint and
refuses a combination the vendor doesn't serve. It tells the store where the
bars belong and whether they replace what's there.
