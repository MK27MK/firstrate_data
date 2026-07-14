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
```

## Usage

```python
from firstrate_data.stock import FirstRateStocks
from firstrate_data.query_parameters import Period, EquitiesAdjustment, Timeframe

stocks = FirstRateStocks.from_data_path()

# last trading day, 1-minute bars
stocks.download_historical_data(
    Period.DAY, Timeframe.MIN_1, EquitiesAdjustment.SPLIT_AND_DIVIDEND
)

# full archive -- requires a ticker_range letter (A-Z)
stocks.download_historical_data(
    Period.FULL, Timeframe.DAY_1, EquitiesAdjustment.UNADJUSTED, ticker_range="C"
)

stocks.download_splits()
stocks.download_dividends()
```

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

futures = FirstRateFutures.from_data_path()

# continuous series: front-month contracts stitched together
futures.download_historical_data(
    Period.FULL, Timeframe.MIN_1, ContinuousFuturesAdjustment.RATIO
)

# the individual contracts behind it
futures.download_contracts(ContractFiles.UPDATE, Timeframe.MIN_1)

# which contracts were stitched, and when
futures.download_continuous_audit()
```

Each call returns the `Path` it wrote and replaces whatever was there before.

```
DATA_PATH/raw/{asset}/{period}/{timeframe}/{adjustment}[/{ticker_range}]   bars
DATA_PATH/raw/{asset}/meta/                        splits, dividends, contin_audit
DATA_PATH/raw/futures/contracts/{archive|update}/{timeframe}/   individual contracts
```

## The complete stocks bundle

`download_stocks_complete` sweeps, for every timeframe x adjustment pair, the full
listed archive of all 26 ticker ranges, the five pre-2026 delisted archives, the 2026
delisted archive, and the splits and dividends behind both.

```python
from firstrate_data.downloader import download_stocks_complete
from firstrate_data.query_parameters import EquitiesAdjustment, Timeframe

report = download_stocks_complete(
    timeframes=[Timeframe.MIN_1, Timeframe.DAY_1],
    adjustments=[EquitiesAdjustment.SPLIT_AND_DIVIDEND, EquitiesAdjustment.UNADJUSTED],
)

report.downloaded  # [Path]              folders written
report.failed      # [(cell, error)]     the API should have served these -- retry
report.skipped     # [(cell, why)]       the API does not offer these -- never retry
```

The sweep is hours long, so a bad response must not discard the rest of it: nothing
raises, and every cell's outcome lands in the returned `BundleReport` instead.

### Pausing and resuming

Ctrl-C whenever you like. To resume, re-run **the same call** with
`skip_existing=True` — already-populated request folders are left alone, so only what
is missing gets fetched:

```python
report = download_stocks_complete(
    timeframes=[Timeframe.MIN_1, Timeframe.DAY_1],
    adjustments=[EquitiesAdjustment.SPLIT_AND_DIVIDEND, EquitiesAdjustment.UNADJUSTED],
    skip_existing=True,  # resume: keep what is on disk, fetch the rest
)
```

Re-run until `report.failed` is empty. Every way a run can stop leaves disk in a state
the next run reads correctly:

- a cell that **failed** never got as far as writing a folder, so it is re-fetched;
- a cell **interrupted mid-unzip** did not write its folder either — archives are
  unzipped to the side and swapped in whole — so it is re-fetched, not left truncated;
- a cell that **succeeded** is skipped.

Two things to know before you lean on it:

- **It resumes a run; it does not refresh one.** `skip_existing=True` keeps whatever
  is on disk, and the listed `full` archive is rebuilt daily — so a sweep resumed a
  week later mixes vintages. Leave it off (the default) to build a bundle fresh.
- **Only HTTP errors are collected.** A corrupt zip or a full disk still aborts the
  sweep. That is not a problem to solve, just a re-run with `skip_existing=True`.

`skipped` cells are not failures and never become downloadable: they are combinations
the API does not offer (`UNADJUSTED` outside 1min/1day listed, outside 1min delisted).
Splits and dividends are re-fetched on every run regardless of `skip_existing` — two
small files, and timeframe-agnostic.

## Parameters

| Enum | Values |
|---|---|
| `Period` | `FULL`, `MONTH`, `WEEK`, `DAY` |
| `Timeframe` | `MIN_1`, `MIN_5`, `MIN_30`, `HOUR_1`, `DAY_1` |
| `EquitiesAdjustment` | `SPLIT`, `SPLIT_AND_DIVIDEND`, `UNADJUSTED` |
| `ContinuousFuturesAdjustment` | `RATIO`, `ABSOLUTE`, `UNADJUSTED` |
| `ContractFiles` | `ARCHIVE` (pre-2026), `UPDATE` (2026+, refreshed daily) |

The API is not uniform across asset types, so neither are the loaders: one subclass
per asset type (`docs/adr/0001`), over a base generic on the adjustment enum
(`docs/adr/0002`). Splits and dividends exist for stocks and ETFs only, as does
`ticker_range` — and only with `period=FULL`.
