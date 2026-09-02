from firstrate_data import Client
from firstrate_data.domain.enums import AssetType, EquitiesAdjustment, Timeframe
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


def main() -> None:
    client = Client.from_env()
    try:
        for ingested in client.download_bundle(BUNDLE):
            print(ingested)
    finally:
        client.close()


if __name__ == "__main__":
    main()
