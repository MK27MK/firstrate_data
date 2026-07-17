from pathlib import Path

from firstrate_data.firstrate import FirstRateData
from firstrate_data.query_parameters import (
    AssetType,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    MetaDataType,
    Period,
    Timeframe,
)
from firstrate_data.request import BarsRequest, DelistedRequest, MetafileRequest


class FirstRateEquities(FirstRateData[EquitiesAdjustment]):
    """Loader for stocks and ETFs, which share three things futures do not: the
    split/dividend adjustments, the splits/dividends metafiles, and ticker_range."""

    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        ticker_range: str | None = None,
    ) -> Path:
        """This function returns historical data archives (.txt files in csv format which are grouped into zip archives).

        The archive is extracted into a request-scoped folder under the loader's
        raw directory, keyed by every request parameter, and that folder's Path
        is returned. If the folder already exists it is wiped and replaced, so it
        always reflects exactly one archive.

        Parameters
        ----------
        period : Period
            Specifies the period to request data for. 'full' requests the entire historical archive, 'month' requests the last 30 days, 'week' requests the current trading week (starting on Monday), 'day' requests the last trading day.

            To request the full historical archive you also need to specify a ticker_range parameter (see below).
        timeframe : Timeframe
            Specifies the period the timeframe of the data. '1min' will request 1-minute intraday bars, '5min' requests 5-minute bars etc.
            Note : bars with zero volumes are not included
        adjustment : EquitiesAdjustment
            Specifies the type of adjustment. 'adj_split' is data adjusted for splits only, 'adj_splitdiv' is data adjusted for both splits and dividends, 'UNADJUSTED' is raw data without any splits or dividend adjustments. UNADJUSTED data is only available in the 1min and 1day timeframes.
        ticker_range : str | None
            Only to be used when requesting the full historical dataset (ie 'period=full'). This parameter specifies the first letter of the ticker, for example 'ticker_range=C' will request all tickers beginning with the letter C

            This parameter can only be used when requesting the full historical archive (ie 'period=full')
        """
        # the delisted endpoint allows UNADJUSTED on 1min *only* -- same enum,
        # narrower rule, so each endpoint guards its own
        if adjustment is EquitiesAdjustment.UNADJUSTED and timeframe not in (
            Timeframe.MIN_1,
            Timeframe.DAY_1,
        ):
            raise ValueError(
                "UNADJUSTED data is only available in the 1min and 1day timeframes"
            )
        if period is Period.FULL and ticker_range is None:
            raise ValueError("ticker_range (A-Z) is required when period=full")
        if ticker_range is not None:
            if period is not Period.FULL:
                raise ValueError("ticker_range can only be used when period=full")
            ticker_range = ticker_range.upper()
            if len(ticker_range) != 1 or not ticker_range.isalpha():
                raise ValueError("ticker_range must be a single letter A-Z")

        return self._fetch_and_persist_historical_bars(
            BarsRequest(self._asset_type, period, timeframe, adjustment, ticker_range)
        )

    # Splits / Dividends Requests --------------------------------------

    def download_splits(self) -> Path:
        """Historical splits: {date,split-ratio}, ratio of new to old shares."""
        return self._fetch_and_persist_metafile(
            MetafileRequest(self._asset_type, MetaDataType.SPLITS)
        )

    def download_dividends(self) -> Path:
        """Historical dividends: {ex-dividend date,dividend amount}."""
        return self._fetch_and_persist_metafile(
            MetafileRequest(self._asset_type, MetaDataType.DIVIDENDS)
        )


class FirstRateStocks(FirstRateEquities):
    """Loader for FirstRate stock data.

    Delisted ticker data lives here rather than on ``FirstRateEquities`` because
    it is stock-only: no other asset type's docs page carries the endpoint."""

    _asset_type = AssetType.STOCK

    # Delisted Ticker Data ---------------------------------------------

    def download_delisted_bars_archive(
        self,
        selector: DelistedArchive | DelistedUpdate,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
    ) -> Path:
        """This function returns data for tickers which have been delisted.

        The pre-2026 historical dataset is split into four archives which need to be individually downloaded. Additionally there is an archive for 2026 (which is requested using the 'update' parameter)

        The 2026 archive and the current week archives are updated at the end of each each (on a Sunday).

        The archive is extracted into ``raw/stock/delisted/{archive|update}/{selector}/
        {timeframe}/{adjustment}``, wiped and replaced if it already exists, and that
        folder's Path is returned.

        Parameters
        ----------
        selector : DelistedArchive | DelistedUpdate
            Which slice of the delisted dataset to fetch. The endpoint exposes this as two
            separate optional parameters, 'archive_number' and 'update', each documented as
            not needing to be used if the other is -- i.e. exactly one of them applies. A
            single union-typed argument makes that exclusivity unrepresentable rather than
            merely checked, so 'both' and 'neither' cannot be written.

            archive_number : Specifies the archive number. This is used to download one of the four historical (pre-2026) archives. This parameter does not need to be used if the 'update' parameter is used.

            update : Specifies the update period. 'year' will return all delisted data in 2026, 'week' return only the last week's delisted data. The delisted data is updated at the end of each week by Sunday 11pm EST. This parameter does not need to be used if the 'archive' parameter is used.
        timeframe : Timeframe
            Specifies the period the timeframe of the data. '1min' will request 1-minute intraday bars, '5min' requests 5-minute bars etc.
        adjustment : EquitiesAdjustment
            Specifies the type of adjustment. 'adj_split' is data adjusted for splits only, 'adj_splitdiv' is data adjusted for both splits and dividends, 'UNADJUSTED' is raw data without any splits or dividend adjustments. UNADJUSTED data is only available in the 1min timeframe.
        """
        # narrower than the listed rule, which also allows UNADJUSTED on 1day
        if (
            adjustment is EquitiesAdjustment.UNADJUSTED
            and timeframe is not Timeframe.MIN_1
        ):
            raise ValueError(
                "UNADJUSTED delisted data is only available in the 1min timeframe"
            )

        request = DelistedRequest(selector, timeframe, adjustment)
        zip_file = self._get(request)

        return self._catalog.write_raw_delisted(
            zip_file, request, self._snapshot_date()
        )
