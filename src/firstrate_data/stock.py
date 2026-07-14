from firstrate_data.firstrate import FirstRateEquities
from firstrate_data.query_parameters import AssetType


class FirstRateStocks(FirstRateEquities):
    """Loader for FirstRate stock data."""

    _asset_type = AssetType.STOCK
