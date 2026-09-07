from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from string import ascii_uppercase

from firstrate_data.domain import AssetType, BarType
from firstrate_data.domain.enums import (
    ContinuousFuturesAdjustment,
    ContractFiles,
    DelistedArchive,
    EquitiesAdjustment,
    OtherData,
    Period,
    Timeframe,
    Unadjusted,
)
from firstrate_data.download.requests import (
    BarsRequest,
    ContractBarsRequest,
    DelistedBarsRequest,
    NotOfferedError,
    OtherDataRequest,
    Request,
)


@dataclass(frozen=True)
class BundleConfig:
    """Enough for Indices, FX and Crypto.

    [Index docs](https://firstratedata.com/_readme/index.txt)
    [FX docs](https://firstratedata.com/_readme/fx.txt)
    [Crypto docs](https://firstratedata.com/_readme/crypto.txt)
    """

    asset_type: AssetType
    # None -> all the available timeframes
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


def bundle_requests(config: BundleConfig) -> Iterator[Request]:
    """Yield every request the bundle downloads, in download order."""
    for timeframe in config.timeframes or tuple(Timeframe):
        yield from _bar_requests(config, timeframe)
    yield from _meta_requests(config)


def _bar_requests(config: BundleConfig, timeframe: Timeframe) -> Iterator[Request]:
    if isinstance(config, FuturesBundleConfig):
        yield from _futures_requests(config, timeframe)
    elif isinstance(config, EquitiesBundleConfig):
        yield from _equities_requests(config, timeframe)
    else:
        # UNADJUSTED is the store's word for it, not the vendor's: index, FX
        # and crypto carry no adjustment on the wire, and the bar type path
        # names one at every level
        yield from _offered(
            lambda: BarsRequest(
                BarType(
                    config.asset_type,
                    timeframe=timeframe,
                    adjustment=Unadjusted.UNADJUSTED,
                ),
                Period.FULL,
            ),
        )


def _equities_requests(
    config: EquitiesBundleConfig,
    timeframe: Timeframe,
) -> Iterator[Request]:
    bar_type = BarType(
        config.asset_type,
        timeframe=timeframe,
        adjustment=config.adjustment,
    )
    for letter in config.ticker_range or ascii_uppercase:
        yield from _offered(
            lambda letter=letter: BarsRequest(
                bar_type,
                Period.FULL,
                ticker_range=letter,
            ),
        )

    if not isinstance(config, StocksBundleConfig):
        return
    for archive in _delisted_archives(selection=config.include_delisted_archives):
        yield from _offered(
            lambda archive=archive: DelistedBarsRequest(bar_type, selector=archive),
        )


def _futures_requests(
    config: FuturesBundleConfig,
    timeframe: Timeframe,
) -> Iterator[Request]:
    yield from _offered(
        lambda: BarsRequest(
            BarType(
                AssetType.FUTURES,
                timeframe=timeframe,
                adjustment=config.adjustment,
            ),
            Period.FULL,
        ),
    )

    if not _wants_contracts(timeframe, selection=config.include_individual_contracts):
        return
    # both halves name different contracts, so neither one contains the other
    for half in ContractFiles:
        yield ContractBarsRequest(
            BarType(AssetType.FUTURES, timeframe=timeframe),
            contract_files=half,
        )


def _meta_requests(config: BundleConfig) -> Iterator[Request]:
    # ponytail: include_company_profiles is unserved -- the vendor documents no
    # endpoint for it. Yield an OtherDataRequest here once one exists.
    if isinstance(config, EquitiesBundleConfig):
        if config.include_splits:
            yield OtherDataRequest(config.asset_type, OtherData.SPLITS)
        if config.include_dividends:
            yield OtherDataRequest(config.asset_type, OtherData.DIVIDENDS)
    if isinstance(config, FuturesBundleConfig) and config.include_contract_dates:
        yield OtherDataRequest(AssetType.FUTURES, OtherData.CONTRACT_DATES)


def _delisted_archives(
    *,
    selection: bool | Iterable[DelistedArchive],
) -> tuple[DelistedArchive, ...]:
    if selection is True:
        return tuple(DelistedArchive)
    if selection is False:
        return ()
    return tuple(selection)


def _wants_contracts(
    timeframe: Timeframe,
    *,
    selection: bool | Iterable[Timeframe],
) -> bool:
    if isinstance(selection, bool):
        return selection
    return timeframe in tuple(selection)


def _offered(build: Callable[[], Request]) -> Iterator[Request]:
    """Yield the request, or nothing when the vendor doesn't serve it.

    A bundle names timeframes and adjustments across a whole universe, and the
    vendor serves some pairs and not others. The pairs it refuses are dropped
    so the rest of the bundle still comes down.
    """
    try:
        yield build()
    except NotOfferedError:
        return
