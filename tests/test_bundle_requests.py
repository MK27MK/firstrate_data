"""One runnable check of what a bundle plans to download."""

from firstrate_data.domain.enums import (
    AssetType,
    ContinuousFuturesAdjustment,
    DelistedArchive,
    EquitiesAdjustment,
    Timeframe,
)
from firstrate_data.download.bundles import (
    BundleConfig,
    FuturesBundleConfig,
    StocksBundleConfig,
    bundle_requests,
)


def _endpoints(config: BundleConfig) -> list[tuple[str, tuple[str, ...]]]:
    return [
        (request.endpoint, tuple(request.to_params().items()))
        for request in bundle_requests(config)
    ]


def main() -> None:
    stocks = StocksBundleConfig(
        asset_type=AssetType.STOCK,
        timeframes=[Timeframe.DAY_1, Timeframe.HOUR_1],
        adjustment=EquitiesAdjustment.UNADJUSTED,
        ticker_range=["A", "B"],
        include_splits=True,
        include_dividends=False,
        include_company_profiles=True,
        include_delisted_archives=[DelistedArchive.ARCHIVE_1],
    )
    planned = _endpoints(stocks)
    bars = [params for endpoint, params in planned if endpoint == "data_file"]
    assert len(bars) == 2, "1hour UNADJUSTED is unserved, so only 1day's letters plan"
    assert dict(bars[0])["ticker_range"] == "A"
    assert [e for e, _ in planned].count("delisted_data_file") == 1, (
        "one archive, on the one timeframe the vendor serves unadjusted"
    )
    assert [e for e, _ in planned].count("meta_file") == 1, "splits only, no dividends"

    futures = FuturesBundleConfig(
        asset_type=AssetType.FUTURES,
        timeframes=[Timeframe.DAY_1],
        adjustment=ContinuousFuturesAdjustment.RATIO,
        include_individual_contracts=True,
        include_contract_dates=True,
    )
    planned = _endpoints(futures)
    assert [e for e, _ in planned] == [
        "data_file",
        "futures_contract",
        "futures_contract",
        "meta_file",
    ], "the continuous series, both contract halves, then the audit file"

    crypto = BundleConfig(asset_type=AssetType.CRYPTO, timeframes=[Timeframe.MIN_1])
    (params,) = [params for _, params in _endpoints(crypto)]
    assert "adjustment" not in dict(params), "crypto carries no adjustment on the wire"

    print("bundle requests OK")


if __name__ == "__main__":
    main()
