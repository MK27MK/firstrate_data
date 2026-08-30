from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    OtherData,
    Period,
    Timeframe,
)
from firstrate_data.download.client.client import Client
from firstrate_data.download.requests import (
    BarsRequest,
    ContractBarsRequest,
    OtherDataRequest,
)
from firstrate_data.store.store import Ingested


class FuturesClient(Client):
    """Loader for FirstRate futures data.

    The historical data is the continuous series: front-month contracts
    stitched together. The individual contracts that built it have their
    own endpoint. The client offers no ``ticker_range``.
    """

    _asset_type = AssetType.FUTURES

    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: ContinuousFuturesAdjustment,
    ) -> Ingested:
        """Ingest continuous-futures OHLCV bars into the store.

        A ``period=full`` request replaces the bars of every ticker it names.
        A shorter period keeps only the bars newer than the store already
        holds.

        Parameters
        ----------
        period : Period
            Span of history to fetch: ``full``, or one of ``month``,
            ``week``, ``day`` for the bars newer than the store already
            holds.
        timeframe : ``Timeframe``
            Bars with zero volume aren't served. 1day carries a seventh
            column, open interest.
        adjustment : ContinuousFuturesAdjustment
            Every roll restates both adjusted series, so the client offers
            them only with ``period=full``.

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
        )
        return self._download(request)

    # Individual Contract Data -----------------------------------------

    def download_contract_bars(
        self,
        contract_files: ContractFiles,
        timeframe: Timeframe,
    ) -> Ingested:
        """Ingest bars for individual futures contracts into the store.

        Every payload is one contract's whole life, so the fetch replaces the
        bars it names. The archive and the update name different
        contracts, so neither contains the other and fetching one never drops
        the other's bars.

        Parameters
        ----------
        contract_files : ContractFiles
            Which half of the dataset to fetch: the frozen pre-2026 archive,
            or the contracts trading from 2026, refreshed daily.
        timeframe : ``Timeframe``
            Bars with zero volume aren't served. 1day carries a seventh
            column, open interest.

        Returns
        -------
        Ingested
            Tickers seen, rows written, and lines quarantined. The bars land
            in the ``contract`` dataset, read back with
            ``Store.futures_contract_bars(...)``.

        Notes
        -----
        Raises no ``NotOffered``, alone among the bars fetches: the endpoint
        serves every timeframe on both halves, so there is no combination of
        these two arguments the vendor refuses.

        """
        return self._download(
            ContractBarsRequest(
                BarType(self._asset_type, timeframe=timeframe),
                contract_files=contract_files,
            ),
        )

    def download_continuous_audit(self) -> Ingested:
        """Ingest the individual contracts that make up the continuous series.

        Replaces the ``contin_audit`` table whole. Read it back with
        ``Store.contin_audit()``.
        """
        return self._download(
            OtherDataRequest(self._asset_type, OtherData.CONTRACT_DATES),
        )
