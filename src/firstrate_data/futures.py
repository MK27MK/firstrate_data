from pathlib import Path

from firstrate_data.firstrate import FirstRateData
from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    MetaDataType,
    Period,
    Timeframe,
)
from firstrate_data.request import BarsRequest, ContractsRequest, MetafileRequest


class FirstRateFutures(FirstRateData[ContinuousFuturesAdjustment]):
    """Loader for FirstRate futures data.

    The historical data is the *continuous* series (front-month contracts stitched
    together); the individual contracts it was stitched from are a separate
    endpoint. Futures have no ticker_range: 'period=full' needs nothing extra."""

    _asset_type = AssetType.FUTURES

    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: ContinuousFuturesAdjustment,
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
        timeframe : Timeframe
            Specifies the period the timeframe of the data. '1min' will request 1-minute intraday bars, '5min' requests 5-minute bars etc.

            Note : bars with zero volumes are not included

            Note: 1day data also includes open-interest in the final file. (therefore the data format for 1day futures data is { DateTime, Open, High, Low, Close, Volume, Open Interest })
        adjustment : ContinuousFuturesAdjustment
            Specifies the type of adjustment applied to the continuous data. (note the continuous data is created by stiching together front-month contracts into a single continuous series - more details on this and the adjustments can be found on our Futures Adjustment Info Page

            'contin_UNadj' is the raw unadjusted trade data.
            'contin_adj_ratio' is the ratio-adjusted data to avoid artifical price jumps on roll dates.
            'contin_adj_absolute' is the absolute-adjusted data to avoid artifical price jumps on roll dates.
        """
        return self._fetch_and_persist_historical_bars(
            BarsRequest(self._asset_type, period, timeframe, adjustment)
        )

    def download_contracts(
        self, contract_files: ContractFiles, timeframe: Timeframe
    ) -> Path:
        """This function returns data for individual futures contracts.

        All contracts are aggregated into a single zip file - one zip file can be requested for the archive data (pre 2026) and one file is for updated for 2026 data.

        The archive is extracted into ``raw/futures/contracts/{contract_files}/{timeframe}``,
        wiped and replaced if it already exists, and that folder's Path is returned.

        Parameters
        ----------
        contract_files : ContractFiles
            An 'archive' request returns the pre-2026 data as a single zip archive of all individual futures contracts. 'update' returns a zip of the 2026 + contracts which is updated daily.
        timeframe : Timeframe
            Specifies the period the timeframe of the data. '1min' will request 1-minute intraday bars, '1day' requests 1-day bars etc.

            Note that bars with zero volumes are not included.

            Note : 1day data also includes open-interest in the final file. (therefore the data format for 1day futures data is {DateTime, Open, High, Low, Close, Volume,Open Interest})
        """

        request = ContractsRequest(contract_files, timeframe)
        zip_file = self._get(request)

        return self._catalog.write_raw_contracts(zip_file, request)

    def download_continuous_audit(self) -> Path:
        """This function returns the individual futures contracts used in constructing the continuous data series."""
        return self._fetch_and_persist_metafile(
            MetafileRequest(self._asset_type, MetaDataType.CONTIN_AUDIT)
        )
