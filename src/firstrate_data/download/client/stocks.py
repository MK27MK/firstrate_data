from firstrate_data.domain import (
    AssetType,
    BarType,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    OtherData,
    Period,
    Timeframe,
)
from firstrate_data.download.client.client import Client
from firstrate_data.download.requests import (
    BarsRequest,
    DelistedBarsRequest,
    OtherDataRequest,
)
from firstrate_data.store.store import Ingested


class StockClient(Client):
    """Loader for FirstRate stock data.

    Stocks alone carry split/dividend adjustments, the splits/dividends
    metafiles, ``ticker_range``, and the delisted-ticker endpoint.
    """

    _asset_type = AssetType.STOCK

    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        ticker_range: str | None = None,
    ) -> Ingested:
        """Ingest listed-stock OHLCV bars into the store.

        A ``full`` replaces the bars of every ticker it names. A shorter
        period downloads only the bars newer than the store already holds.

        Parameters
        ----------
        period : Period
            ``full`` requires ``ticker_range``.
        timeframe : ``Timeframe``
            Bars with zero volume aren't served.
        adjustment : EquitiesAdjustment
            The vendor serves ``UNADJUSTED`` only on 1min and 1day. Every
            corporate action restates an adjusted series, so the vendor offers
            it only with ``period=full``.
        ticker_range : str | None
            First letter of the tickers to request, for example ``"C"``.
            Required with ``period=full``, rejected otherwise.

        Returns
        -------
        Ingested
            Tickers seen, rows written, and lines quarantined.

        Raises
        ------
        NotOffered
            If the vendor doesn't serve this combination.

        """
        request = BarsRequest(
            BarType(self._asset_type, timeframe=timeframe, adjustment=adjustment),
            period,
            ticker_range=ticker_range,
        )
        return self._download(request)

    # Splits / Dividends Requests --------------------------------------

    def download_splits(self) -> Ingested:
        """Historical splits: {date,split-ratio}, ratio of new to old shares.

        Replaces the ``splits`` table whole. Read it back with
        ``Store.splits()``.
        """
        return self._download(OtherDataRequest(self._asset_type, OtherData.SPLITS))

    def download_dividends(self) -> Ingested:
        """Historical dividends: {ex-dividend date,dividend amount}.

        Replaces the ``dividends`` table whole. Read it back with
        ``Store.dividends()``.
        """
        return self._download(OtherDataRequest(self._asset_type, OtherData.DIVIDENDS))

    # Delisted Ticker Data ---------------------------------------------

    def download_delisted_bars(
        self,
        selector: DelistedArchive | DelistedUpdate,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
    ) -> Ingested:
        """Ingest bars for delisted tickers into the store.

        Every delisted payload is one ticker's whole history, so the fetch
        replaces the bars it names.

        Parameters
        ----------
        selector : DelistedArchive | DelistedUpdate
            Which slice of the delisted dataset to fetch: one of the pre-2026
            archives, or a recent update window.
        timeframe : ``Timeframe``
            Bars with zero volume aren't served.
        adjustment : EquitiesAdjustment
            The vendor serves ``UNADJUSTED`` only on 1min.

        Returns
        -------
        Ingested
            Tickers seen, rows written, and lines quarantined.

        Raises
        ------
        NotOffered
            If the vendor doesn't serve this combination.

        """
        return self._download(
            DelistedBarsRequest(
                BarType(self._asset_type, timeframe=timeframe, adjustment=adjustment),
                selector=selector,
            ),
        )
