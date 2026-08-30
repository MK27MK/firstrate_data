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

    def __post_init__(self) -> None:
        self.raise_on_unavalaible_delisted_timeframes()

    def raise_on_unavalaible_delisted_timeframes(self) -> None:
        if not self.include_delisted_archives:
            return
        if self.adjustment is not EquitiesAdjustment.UNADJUSTED:
            return
        requested_timeframes = (
            tuple(Timeframe) if self.timeframes is None else self.timeframes
        )
        unavailable_timeframes = [
            timeframe
            for timeframe in requested_timeframes
            if timeframe not in UNADJUSTED_DELISTED_TIMEFRAMES
        ]
        if unavailable_timeframes:
            allowed = ", ".join(UNADJUSTED_DELISTED_TIMEFRAMES)
            rejected = ", ".join(unavailable_timeframes)
            msg = f"Unadjusted delisted archives exist for {allowed} "
            f"only;requested {rejected}."
            raise ValueError(msg)


@dataclass(frozen=True)
class FuturesBundleConfig(BundleConfig):
    """[Future docs](https://firstratedata.com/_readme/futures.txt)."""

    adjustment: ContinuousFuturesAdjustment
    include_individual_contracts: bool | Iterable[Timeframe]
    # Downloads a folder with one .txt per contract.
    # Each row follows the YYYY-MM-DD,CONTRACT_CODE format.
    # Useful to know rollover dates used by FirstRate in contract construction.
    include_contract_dates: bool
