from collections.abc import Iterable
from dataclasses import dataclass

from firstrate_data.domain.enums import (
    ContinuousFuturesAdjustment,
    DelistedArchive,
    EquitiesAdjustment,
    Timeframe,
)


@dataclass(frozen=True)
class BundleConfig:
    """Enough for Indices, FX and Crypto.

    [Index docs](https://firstratedata.com/_readme/index.txt)
    [FX docs](https://firstratedata.com/_readme/fx.txt)
    [Crypto docs](https://firstratedata.com/_readme/crypto.txt)
    """

    timeframes: Iterable[Timeframe] | None


@dataclass(frozen=True)
class EquitiesBundleConfig(BundleConfig):
    """Use this for ETFs.

    [docs](https://firstratedata.com/_readme/etf.txt).
    """

    adjustment: EquitiesAdjustment
    # None behaves like ["A", ..., "Z"]
    ticker_range: Iterable[str] | None
    include_splits: bool
    include_dividends: bool


UNADJUSTED_DELISTED_TIMEFRAMES = (Timeframe.DAY_1, Timeframe.MIN_1)


@dataclass(frozen=True)
class StocksBundleConfig(EquitiesBundleConfig):
    """[Stocks docs](https://firstratedata.com/_readme/stock.txt)."""

    include_company_profiles: bool
    # false -> no archives are downloaded, true -> all of them,
    # otherwise pass an iterable to select them.
    include_delisted_archives: bool | Iterable[DelistedArchive]


@dataclass(frozen=True)
class FuturesBundleConfig(BundleConfig):
    """[Future docs](https://firstratedata.com/_readme/futures.txt)."""

    adjustment: ContinuousFuturesAdjustment
    include_individual_contracts: bool | Iterable[Timeframe]
    # Downloads a folder with one .txt per contract.
    # Each row follows the YYYY-MM-DD,CONTRACT_CODE format.
    # Useful to know rollover dates used by FirstRate in contract construction.
    include_contract_dates: bool
