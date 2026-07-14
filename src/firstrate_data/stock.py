from pathlib import Path

from firstrate_data.firstrate import FirstRateEquities
from firstrate_data.query_parameters import (
    AssetType,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    Timeframe,
)


class FirstRateStocks(FirstRateEquities):
    """Loader for FirstRate stock data.

    Delisted ticker data lives here rather than on ``FirstRateEquities`` because
    it is stock-only: no other asset type's docs page carries the endpoint."""

    _asset_type = AssetType.STOCK

    # Delisted Ticker Data ---------------------------------------------

    def download_delisted_historical_data(
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

        is_archive = isinstance(selector, DelistedArchive)
        kind = "archive" if is_archive else "update"

        # this endpoint takes no `type` param -- it is stock-only by definition
        params = {
            "archive_number" if is_archive else "update": selector.value,
            "timeframe": timeframe.value,
            "adjustment": adjustment.value,
        }
        target = (
            self._raw_directory
            / self._asset_type.value
            / "delisted"
            / kind
            / selector.value
            / timeframe.value
            / adjustment.value
        )
        return self._fetch_archive("delisted_data_file", params, target)
