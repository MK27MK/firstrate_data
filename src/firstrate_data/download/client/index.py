from firstrate_data.domain import (
    AssetType,
    BarType,
    IndexAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.client.client import Client
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store.store import Ingested


class IndexClient(Client):
    """Loader for FirstRate index data.

    An index level is a published number rather than a price, so the endpoint
    offers neither an ``adjustment`` nor a ``ticker_range``.
    """

    _asset_type = AssetType.INDEX

    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
    ) -> Ingested:
        """Ingest index OHLCV bars into the store.

        A ``full`` replaces the bars of every ticker it names. A shorter
        period downloads only the bars newer than the store already holds.

        Parameters
        ----------
        period : Period
            Scope of the ingest: a full replace or an incremental window.
        timeframe : ``Timeframe``
            Bars with zero volume aren't served.

        Returns
        -------
        Ingested
            Tickers seen, rows written, and lines quarantined.

        """
        # UNADJUSTED is the store's word for it, not the vendor's: the request
        # carries no ``adjustment`` on the wire, and the bar type path needs one
        request = BarsRequest(
            BarType(
                self._asset_type,
                timeframe=timeframe,
                adjustment=IndexAdjustment.UNADJUSTED,
            ),
            period,
        )
        return self._download(request)
