"""A restated series has no meaningful increment, and the guard has to say so
before the download, not after it.

The vendor rewrites an adjusted series' history backwards when a corporate
action or a roll lands, so two fetches taken either side of one sit on
different bases: appending the second to the first splices them together. The
old code caught this at ingest -- after paying for an hour-long download.
"""

import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    DelistedArchive,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.client.fetcher import ArchiveFetcher
from firstrate_data.download.client.futures import FuturesClient
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.download.requests import BarsRequest, NotOfferedError
from firstrate_data.store.store import Store


class FetchedError(Exception):
    """Raised in place of an HTTP request, so a test can see one happen."""


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def fetch(*_args: object, **_kwargs: object) -> None:
        raise FetchedError

    # the transport's one entry point, so a guard that lets a request through is
    # caught wherever in the loader it was let through from
    monkeypatch.setattr(ArchiveFetcher, "fetch", fetch)


@pytest.fixture
def stocks(store: Store, no_network: None) -> StockClient:  # noqa: ARG001 - fixture applied for its side effect: blocks network access.
    return StockClient("test-user", store)


@pytest.fixture
def futures(store: Store, no_network: None) -> FuturesClient:  # noqa: ARG001 - fixture applied for its side effect: blocks network access.
    return FuturesClient("test-user", store)


class TestARestatedIncrementIsRefusedBeforeTheDownload:
    @pytest.mark.parametrize(
        "adjustment",
        [EquitiesAdjustment.SPLIT, EquitiesAdjustment.SPLIT_AND_DIVIDEND],
    )
    def test_stocks(self, stocks: StockClient, adjustment: EquitiesAdjustment) -> None:
        with pytest.raises(ValueError, match="period=full"):
            stocks.download_historical_bars(Period.WEEK, Timeframe.DAY_1, adjustment)

    @pytest.mark.parametrize(
        "adjustment",
        [ContinuousFuturesAdjustment.RATIO, ContinuousFuturesAdjustment.ABSOLUTE],
    )
    def test_futures(
        self,
        futures: FuturesClient,
        adjustment: ContinuousFuturesAdjustment,
    ) -> None:
        with pytest.raises(ValueError, match="period=full"):
            futures.download_historical_bars(Period.DAY, Timeframe.MIN_1, adjustment)

    def test_the_message_names_the_fix(self, stocks: StockClient) -> None:
        """A refusal the caller cannot act on is only half an error."""
        with pytest.raises(ValueError, match="Re-fetch it with period=full") as refused:
            stocks.download_historical_bars(
                Period.MONTH,
                Timeframe.DAY_1,
                EquitiesAdjustment.SPLIT,
            )

        assert "adj_split" in str(refused.value)
        assert "Re-fetch it with period=full" in str(refused.value)


class TestTheGuardIsOnTheRequestItself:
    """Not on a loader method every subclass has to remember to call.

    A guard that runs only when a caller routes through the right ``plan_*``
    is a guard that silently does not run for the asset type someone adds next.
    """

    def test_a_restated_increment_cannot_be_built_at_all(self) -> None:
        with pytest.raises(NotOfferedError, match="period=full"):
            BarsRequest(
                BarType(
                    AssetType.FUTURES,
                    timeframe=Timeframe.DAY_1,
                    adjustment=ContinuousFuturesAdjustment.RATIO,
                ),
                Period.WEEK,
            )

    def test_it_is_a_value_error_so_existing_callers_still_catch_it(self) -> None:
        """The narrower name is what a sweep skips a cell on; the wider one is
        what the loaders' ``Raises`` sections have always promised.

        """
        assert issubclass(NotOfferedError, ValueError)


class TestWhatTheGuardMustNotRefuse:
    def test_a_restated_full_goes_out(self, stocks: StockClient) -> None:
        """``full`` is the whole history on one basis: exactly what to fetch."""
        with pytest.raises(FetchedError):
            stocks.download_historical_bars(
                Period.FULL,
                Timeframe.DAY_1,
                EquitiesAdjustment.SPLIT,
                ticker_range="A",
            )

    def test_an_unadjusted_increment_goes_out(self, stocks: StockClient) -> None:
        """Raw prices are never restated, which is what makes them cheap to keep
        current -- refusing them would leave no incremental fetch at all.

        """
        with pytest.raises(FetchedError):
            stocks.download_historical_bars(
                Period.WEEK,
                Timeframe.DAY_1,
                EquitiesAdjustment.UNADJUSTED,
            )

    def test_an_unadjusted_futures_increment_goes_out(
        self,
        futures: FuturesClient,
    ) -> None:
        with pytest.raises(FetchedError):
            futures.download_historical_bars(
                Period.DAY,
                Timeframe.MIN_1,
                ContinuousFuturesAdjustment.UNADJUSTED,
            )

    def test_a_restated_delisted_fetch_goes_out(self, stocks: StockClient) -> None:
        """Delisted data has no period: every payload is a whole history, so
        there is no increment to splice and nothing to guard against.

        """
        with pytest.raises(FetchedError):
            stocks.download_delisted_bars(
                DelistedArchive.ARCHIVE_1,
                Timeframe.MIN_1,
                EquitiesAdjustment.SPLIT_AND_DIVIDEND,
            )
