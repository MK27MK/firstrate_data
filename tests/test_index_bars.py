"""An index is asked for with two parameters and stored with five.

The vendor's index page documents neither an adjustment nor a ticker_range, so
the request carries neither -- while the tree names an adjustment at every
level, which is what ``IndexAdjustment`` is for. The two must not leak into each
other: a wire parameter the endpoint never documented, or a bar type path with
a hole in it, would each be a way of getting this wrong.
"""

from collections.abc import Generator

import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    IndexAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.client.index import IndexClient
from firstrate_data.download.requests import BarsRequest, NotOfferedError
from firstrate_data.store.store import Store
from tests.conftest import BARS, Spool, archive
from tests.vendor import Vendor, serving

INDEX_BARS = archive(
    {
        "SPX_full_1day.txt": BARS,
        "NDX_full_1day.txt": BARS,
    },
)

# the vendor's real index format: { DateTime, O, H, L, C } and no volume, there
# being no trade behind a published level. Five columns, not the usual six.
LEVELS = """2024-01-02 00:00:00,100.0,101.0,99.5,100.5
2024-01-03 00:00:00,101.5,103.0,101.0,102.5
"""


@pytest.fixture
def vendor() -> Generator[Vendor]:
    yield from serving(Vendor(payload=INDEX_BARS))


@pytest.fixture
def index(store: Store, vendor: Vendor) -> Generator[IndexClient]:
    loader = IndexClient("test-user", store, vendor.url)
    yield loader
    loader.close()


class TestTheRequestCarriesOnlyWhatTheEndpointDocuments:
    def test_no_adjustment_and_no_ticker_range(self) -> None:
        planned = BarsRequest(
            BarType(
                AssetType.INDEX,
                timeframe=Timeframe.HOUR_1,
                adjustment=IndexAdjustment.UNADJUSTED,
            ),
            Period.FULL,
        )

        assert planned.to_params() == {
            "type": "index",
            "period": "full",
            "timeframe": "1hour",
        }

    def test_the_wire_carries_the_same(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        asked = vendor.asked[0]
        assert asked.endpoint == "data_file"
        assert asked.params["type"] == ["index"]
        assert asked.params["userid"] == ["test-user"]
        assert "adjustment" not in asked.params
        assert "ticker_range" not in asked.params

    def test_the_store_still_gets_an_adjustment(
        self,
        index: IndexClient,
        store: Store,
    ) -> None:
        """The bar type names one at every level, so an index has one too."""
        index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        stored = store.index_bars(Timeframe.DAY_1)
        assert set(stored.df()["adjustment"]) == {IndexAdjustment.UNADJUSTED.value}

    def test_an_index_request_refuses_a_ticker_range_outright(self) -> None:
        """``plan_historical_bars`` has no such parameter to pass, so the rule is
        on the request: an index full archive is served whole.

        """
        with pytest.raises(NotOfferedError, match="equities-only"):
            BarsRequest(
                BarType(
                    AssetType.INDEX,
                    timeframe=Timeframe.DAY_1,
                    adjustment=IndexAdjustment.UNADJUSTED,
                ),
                Period.FULL,
                ticker_range="A",
            )


class TestAFiveColumnPayload:
    """The declared columns are positional, so a six-column schema read against
    the vendor's five-column index file aborts the whole scan on the sniffer.

    """

    def _ingest(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(
            spool(archive({"SPX_full_1day.txt": LEVELS})),
            BarsRequest(
                BarType(
                    AssetType.INDEX,
                    timeframe=Timeframe.DAY_1,
                    adjustment=IndexAdjustment.UNADJUSTED,
                ),
                Period.FULL,
            ),
        )

    def test_it_ingests(self, store: Store, spool: Spool) -> None:
        self._ingest(store, spool)

        spx = store.index_bars(Timeframe.DAY_1, ticker="SPX")

        assert spx.order("ts").select("open, high, low, close").fetchall() == [
            (100.0, 101.0, 99.5, 100.5),
            (101.5, 103.0, 101.0, 102.5),
        ]

    def test_volume_reads_back_null(self, store: Store, spool: Spool) -> None:
        """Absent from the source, so absent here -- but the column stays, as
        ``open_interest`` does for a stock.

        """
        self._ingest(store, spool)

        bars = store.index_bars(Timeframe.DAY_1, ticker="SPX")

        assert bars.select("volume").distinct().fetchall() == [(None,)]


class TestAnIndexArchiveIsQueryableAfterwards:
    def test_the_bars_are_in_the_store(self, index: IndexClient, store: Store) -> None:
        ingested = index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        assert (ingested.tickers, ingested.rows, ingested.rejected) == (2, 6, 0)
        assert store.index_bars(Timeframe.DAY_1).count("*").fetchone() == (6,)

    def test_one_ticker_reads_back_whole(
        self,
        index: IndexClient,
        store: Store,
    ) -> None:
        index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        spx = store.index_bars(Timeframe.DAY_1, ticker="SPX")

        assert spx.order("ts").select("close").fetchall() == [
            (100.5,),
            (101.5,),
            (102.5,),
        ]

    def test_it_lands_under_its_own_asset_type(
        self,
        index: IndexClient,
        store: Store,
    ) -> None:
        """``UNADJUSTED`` is a value stocks use too; ``index`` is what separates
        them.
        """
        index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        assert store.bars().select("asset_type").distinct().fetchall() == [("index",)]
        assert store.bars(adjustment=IndexAdjustment.UNADJUSTED).count(
            "*",
        ).fetchone() == (6,)

    def test_an_increment_does_not_double_a_bar(
        self,
        index: IndexClient,
        store: Store,
    ) -> None:
        """An index is never restated, so a shorter period is a legal ask -- and
        the vendor re-serves bars the store already holds when it is made.

        """
        index.download_historical_bars(Period.FULL, Timeframe.DAY_1)

        index.download_historical_bars(Period.WEEK, Timeframe.DAY_1)

        assert store.index_bars(Timeframe.DAY_1).count("*").fetchone() == (6,)
