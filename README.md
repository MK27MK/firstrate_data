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
