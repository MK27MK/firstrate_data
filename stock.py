from pathlib import Path

from firstrate import FirstRateData
from query_parameters import AssetType


class FirstRateStocks(FirstRateData):
    """Loader for FirstRate stock data."""

    _asset_type = AssetType.STOCK

    def download_splits(self) -> Path: ...

    def download_dividends(self) -> Path: ...
