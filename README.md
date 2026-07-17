# firstrate-data

Downloads [FirstRate Data](https://firstratedata.com) archives into a local directory.

## Setup

```bash
uv sync
```

`.env`:

```
DATA_PATH=/path/to/data      # archives are unzipped under DATA_PATH/raw
FIRSTRATE_USERID=your-userid
FIRSTRATE_BASE_URL=...       # optional; defaults to https://firstratedata.com/api
```

## Usage

```python
from firstrate_data.stock import FirstRateStocks
from firstrate_data.query_parameters import Period, EquitiesAdjustment, Timeframe

stocks = FirstRateStocks.from_env()

# last trading day, 1-minute bars
stocks.download_historical_bars(
    Period.DAY, Timeframe.MIN_1, EquitiesAdjustment.SPLIT_AND_DIVIDEND
)

# full archive -- requires a ticker_range letter (A-Z)
stocks.download_historical_bars(
    Period.FULL, Timeframe.DAY_1, EquitiesAdjustment.UNADJUSTED, ticker_range="C"
)

stocks.download_splits()
stocks.download_dividends()
```

Tickers that no longer trade live behind their own endpoint, and are stock-only. The
pre-2026 history is split into archives you fetch one at a time; 2026+ is an `update`:

```python
from firstrate_data.query_parameters import DelistedArchive, DelistedUpdate

stocks.download_delisted_bars_archive(
    DelistedArchive.ARCHIVE_1, Timeframe.MIN_1, EquitiesAdjustment.SPLIT_AND_DIVIDEND
)
stocks.download_delisted_bars_archive(
    DelistedUpdate.YEAR, Timeframe.MIN_1, EquitiesAdjustment.SPLIT_AND_DIVIDEND
)
```

One `selector` argument rather than two optional ones, so "both" and "neither" cannot
be written (`docs/adr/0003`).

Futures take a different adjustment enum and have **no** `ticker_range` — their
full archive needs nothing extra:

```python
from firstrate_data.futures import FirstRateFutures
from firstrate_data.query_parameters import (
    ContinuousFuturesAdjustment,
    ContractFiles,
    Period,
    Timeframe,
)

futures = FirstRateFutures.from_env()

# continuous series: front-month contracts stitched together
futures.download_historical_bars(
    Period.FULL, Timeframe.MIN_1, ContinuousFuturesAdjustment.RATIO
)

# the individual contracts behind it
futures.download_contracts(ContractFiles.UPDATE, Timeframe.MIN_1)

# which contracts were stitched, and when
futures.download_continuous_audit()
```

Each call returns the `Path` it wrote. Every download lands under the date it was
fetched — its **snapshot date** — because the vendor's answer to an unchanged
question changes over time.

```
DATA_PATH/raw/{asset}/{period}/{timeframe}/{adjustment}[/{ticker_range}]/{date}/
DATA_PATH/raw/{asset}/meta/{metafile}/{date}/           splits, dividends, contin_audit
DATA_PATH/raw/futures/contracts/{archive|update}/{timeframe}/{date}/
DATA_PATH/raw/stock/delisted/{archive|update}/{selector}/{timeframe}/{adjustment}/{date}/
```

Each snapshot directory carries a `_snapshot.json` file recording the request that
produced it. Retention is one number: one date for a `full` (a newer one wholly
contains it), every date for the increments.

Archives are unzipped to the side and swapped in whole, so a folder either holds one
complete archive or does not exist — a Ctrl-C mid-unzip never leaves a truncated one.

## The parquet catalog

The queryable side is Hive-partitioned Parquet derived from the raw snapshots.
While the snapshots are on disk, `rm -rf` the parquet tree and `sync()` rebuilds
it with no network.

### Converting raw data

`sync()` scans `raw/`, diffs against a manifest of what is already ingested, and
converts whatever is missing. It never raises: every directory's outcome lands in
the returned `SyncReport`.

```python
from firstrate_data.catalog import Catalog

catalog = Catalog.from_env()  # reads DATA_PATH

report = catalog.sync()  # raw/ -> parquet. Idempotent: also the rebuild.

print(f"ingested {len(report.ingested)} snapshot(s)")
for source, reason in report.skipped:  # nothing to do, and re-running won't change that
    print(f"skipped {source}: {reason}")
for source, error in report.failed:  # should have ingested and didn't
    print(f"FAILED {source}: {error}")
```

Bars land in a uniform seven-column schema (`ts, open, high, low, close, volume,
open_interest` — the last NULL where the source omits it), partitioned so every
selector is also a directory level:

```
DATA_PATH/parquet/{asset_type}/{dataset}/{adjustment}/{timeframe}/{ticker}/{date}_{i}.parquet
```

Snapshots are **replaced, never merged**. Adjusted prices are not append-only —
every corporate action rewrites the history before it — so a newer `full` replaces
an older one, and `sync()` refuses to append an increment to an adjusted series
(it lands in `report.skipped`; re-fetch `period=full`). Unadjusted series are
extended in place, minus whatever the partition already covers, so re-syncing an
overlapping `week` never doubles a bar.

`ts` is stored exactly as the vendor delivers it, naive: the source does not state
a timezone, so any conversion is a view the caller applies.

### Queries

Reads return a lazy `duckdb.DuckDBPyRelation` — the engine, not rows. Filter,
aggregate, join, or hand it to pandas/Arrow without materializing 400GB.

```python
from firstrate_data.query_parameters import (
    ContinuousFuturesAdjustment,
    Dataset,
    EquitiesAdjustment,
    Timeframe,
)

# one ticker, daily, split-adjusted
aapl = catalog.stock_bars(Timeframe.DAY_1, EquitiesAdjustment.SPLIT, ticker="AAPL")
aapl.aggregate("min(ts), max(ts), count(*)").show()
frame = aapl.df()  # pandas DataFrame, materialized only now (needs pandas installed)

# SQL over a relation
recent = aapl.filter("ts >= DATE '2026-01-01'").order("ts")

# delisted tickers are in by default: the plain question is the unbiased one,
# and asking for `Dataset.LISTED` is what costs you survivorship
listed_only = catalog.stock_bars(
    Timeframe.DAY_1, EquitiesAdjustment.SPLIT, dataset=Dataset.LISTED
)

# futures continuous series
es = catalog.futures_bars(
    Timeframe.DAY_1, ContinuousFuturesAdjustment.RATIO, ticker="ES"
)

# across the whole tree; every partition key is also a column, so an
# unfiltered call carries one row per adjustment a bar was fetched under
catalog.bars(timeframe=Timeframe.DAY_1).aggregate(
    "asset_type, adjustment, count(*)"
).show()
```

A selector that matches nothing returns an empty relation of the right shape, not
an error — "no data" is an answer.

The selectors **build the glob** rather than filter it, which is what makes a fine
slice fast (~970x over a wide glob with a `WHERE`, measured at 3000 partitions).
Prefer them over `.filter(...)` for anything that is a partition key. The asset
type is in the method name (`stock_bars` / `futures_bars`), so pairing futures
with an equities adjustment is unrepresentable.

See `docs/adr/0005-hive-parquet-read-side-and-snapshot-reconciliation.md`.

## Progress

Loaders draw nothing by default: a library used from another program should not write
to your terminal unasked. Pass a reporter to get bars, or implement `ProgressReporter`
(one `track(label, total, unit)` method) to send progress somewhere else entirely.

```python
from firstrate_data.progress import TqdmProgress

stocks = FirstRateStocks.from_env(progress=TqdmProgress())
```

`TqdmProgress` nests: the bundle sweep on the top line, the archive in flight beneath
it. Bars disable themselves when stderr is not a terminal.

## The complete stocks bundle

`download_stocks_complete` sweeps, for every timeframe x adjustment pair, the listed
archive of all 26 ticker ranges, the five pre-2026 delisted archives, the 2026
delisted archive, and the splits and dividends behind both.

```python
from firstrate_data.downloader import download_stocks_complete
from firstrate_data.query_parameters import EquitiesAdjustment, Period, Timeframe

report = download_stocks_complete(
    period=Period.FULL,
    timeframes=[Timeframe.MIN_1, Timeframe.DAY_1],
    adjustments=[EquitiesAdjustment.SPLIT_AND_DIVIDEND, EquitiesAdjustment.UNADJUSTED],
)

report.downloaded  # [Path]              folders written
report.failed      # [(cell, error)]     the API should have served these -- retry
report.skipped     # [(cell, why)]       the API does not offer these -- never retry
```

`ticker_ranges` defaults to the whole alphabet, which is what makes the bundle
complete; narrow it to sweep a slice. `progress` defaults to `TqdmProgress()` here —
unlike a loader, a sweep that runs for hours and says nothing is indistinguishable
from a hung one. Pass `NullProgress()` for silence.

The sweep is hours long, so a bad response must not discard the rest of it: nothing
raises, and every cell's outcome lands in the returned `BundleReport` instead. Only
HTTP errors are collected, though — a corrupt zip or a full disk still aborts the run.

There is no resume: a re-run fetches every cell again, including the ones already on
disk. To retry just what failed, drive the loader from `report.failed` yourself rather
than re-running the sweep.

`skipped` cells are not failures and never become downloadable: they are combinations
the API does not offer (`UNADJUSTED` outside 1min/1day listed, outside 1min delisted).
Splits and dividends are fetched once per run, being timeframe-agnostic.

## Parameters

| Enum | Values |
|---|---|
| `Period` | `FULL`, `MONTH`, `WEEK`, `DAY` |
| `Timeframe` | `MIN_1`, `MIN_5`, `MIN_30`, `HOUR_1`, `DAY_1` |
| `EquitiesAdjustment` | `SPLIT`, `SPLIT_AND_DIVIDEND`, `UNADJUSTED` |
| `ContinuousFuturesAdjustment` | `RATIO`, `ABSOLUTE`, `UNADJUSTED` |
| `ContractFiles` | `ARCHIVE` (pre-2026), `UPDATE` (2026+, refreshed daily) |
| `DelistedArchive` | `ARCHIVE_1` .. `ARCHIVE_5` (pre-2026) |
| `DelistedUpdate` | `YEAR` (2026+), `WEEK` (last week only) |

The API is not uniform across asset types, so neither are the loaders: one subclass
per asset type (`docs/adr/0001`), over a base generic on the adjustment enum
(`docs/adr/0002`). Splits and dividends exist for stocks and ETFs only, as does
`ticker_range` — and only with `period=FULL`. Each request is an object that carries
its own endpoint, and the `Catalog` derives the on-disk layout from it, so where a
request lands is stated once (`docs/adr/0004`).
