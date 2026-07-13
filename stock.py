from firstrate import FirstRateCorporateActions
from query_parameters import AssetType


class FirstRateStocks(FirstRateCorporateActions):
    """Loader for FirstRate stock data."""

    _asset_type = AssetType.STOCK
