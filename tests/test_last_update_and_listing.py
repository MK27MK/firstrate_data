"""The two endpoints that answer with a value rather than an archive.

``last_update`` and ``ticker_listing`` both take a ``type`` and both appear on
every asset type's docs page, so they live on the base loader and every asset
type gets them. Nothing here reaches the store: a date is not a bar, and a
listing has no place in a tree partitioned by ticker.

What comes back is parsed rather than handed over as a string -- so a body that
is not a date, or a row that is not a listing, has to fail here rather than
downstream of whoever believed it.
"""

from collections.abc import Generator
from datetime import date, datetime

import pytest

from firstrate_data.domain import TickerListing
from firstrate_data.download.client.client import Client
from firstrate_data.download.client.futures import FuturesClient
from firstrate_data.download.client.index import IndexClient
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.store.store import Store
from tests.conftest import bars_archive
from tests.vendor import Vendor, serving


@pytest.fixture
def vendor() -> Generator[Vendor]:
    yield from serving(Vendor(payload=bars_archive("AAPL")))


@pytest.fixture
def index(store: Store, vendor: Vendor) -> Generator[IndexClient]:
    loader = IndexClient("test-user", store, vendor.url)
    yield loader
    loader.close()


class TestLastUpdate:
    def test_the_date_comes_back_as_a_date(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        vendor.last_update = b"2026-07-31"

        assert index.download_last_update() == date(2026, 7, 31)

    def test_a_body_carrying_a_time_keeps_it(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        vendor.last_update = b"2026-07-31 22:00:00"

        # naive on purpose: Client._parse_last_update reads the vendor's body
        # with datetime.fromisoformat, which stamps no timezone the vendor
        # never stated
        assert index.download_last_update() == datetime(2026, 7, 31, 22, 0)  # noqa: DTZ001

    def test_the_request_names_the_asset_type(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        index.download_last_update()

        asked = vendor.asked[0]
        assert asked.endpoint == "last_update"
        assert asked.params["type"] == ["index"]
        assert asked.params["userid"] == ["test-user"]

    def test_the_full_update_flag_is_only_sent_when_asked_about(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        """The docs do not say which way it defaults, so an unasked question
        stays unasked rather than being answered with a guess.

        """
        index.download_last_update()
        assert "is_full_update" not in vendor.asked[0].params

        index.download_last_update(is_full_update=True)
        assert vendor.asked[1].params["is_full_update"] == ["true"]

        index.download_last_update(is_full_update=False)
        assert vendor.asked[2].params["is_full_update"] == ["false"]


class TestTickerListing:
    def test_every_row_is_parsed(self, index: IndexClient, vendor: Vendor) -> None:
        vendor.ticker_listing = (
            b"SPX,S&P 500 Index,2005-01-03,2026-07-31\n"
            b"NDX,Nasdaq 100 Index,2005-01-03,2026-07-30\n"
            b"DJI,Dow Jones Industrial Average,1999-01-04,2026-07-31\n"
        )

        listed = index.download_ticker_listing()

        assert listed == [
            TickerListing("SPX", "S&P 500 Index", date(2005, 1, 3), date(2026, 7, 31)),
            TickerListing(
                "NDX",
                "Nasdaq 100 Index",
                date(2005, 1, 3),
                date(2026, 7, 30),
            ),
            TickerListing(
                "DJI",
                "Dow Jones Industrial Average",
                date(1999, 1, 4),
                date(2026, 7, 31),
            ),
        ]

    def test_the_csv_is_asked_for_rather_than_the_page(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        index.download_ticker_listing()

        asked = vendor.asked[0]
        assert asked.endpoint == "ticker_listing"
        assert asked.params["html"] == ["false"]
        assert asked.params["type"] == ["index"]

    def test_a_name_holding_a_comma_stays_one_name(self) -> None:
        """The vendor's own format has no escape for it, so the fields are read
        from both ends and the middle is the name.

        """
        listed = TickerListing.from_csv(
            "SPXT,S&P 500, Total Return,2005-01-03,2026-07-31",
        )

        assert listed == [
            TickerListing(
                "SPXT",
                "S&P 500, Total Return",
                date(2005, 1, 3),
                date(2026, 7, 31),
            ),
        ]


class TestEveryAssetTypeHasThem:
    """One `type` parameter, one implementation: the base's, not each loader's."""

    def test_stocks(self, store: Store, vendor: Vendor) -> None:
        stocks = StockClient("test-user", store, vendor.url)

        stocks.download_last_update()
        stocks.download_ticker_listing()

        assert [asked.params["type"] for asked in vendor.asked] == [
            ["stock"],
            ["stock"],
        ]
        stocks.close()

    def test_futures(self, store: Store, vendor: Vendor) -> None:
        futures = FuturesClient("test-user", store, vendor.url)

        assert futures.download_last_update() == date(2026, 7, 31)
        assert [listed.ticker for listed in futures.download_ticker_listing()] == [
            "SPX",
            "NDX",
        ]
        futures.close()


class TestAJunkBodyIsRefusedRatherThanBelieved:
    @pytest.mark.parametrize(
        "body",
        ["", "   \n", "<html>Invalid userid</html>", "last friday"],
    )
    def test_last_update(self, body: str) -> None:
        with pytest.raises(ValueError, match="last_update"):
            Client._parse_last_update(body)

    def test_last_update_through_the_loader(
        self,
        index: IndexClient,
        vendor: Vendor,
    ) -> None:
        """The loader must not hand back a string it could not read as a date."""
        vendor.last_update = b"<html>Invalid userid</html>"

        with pytest.raises(ValueError, match="not a date"):
            index.download_last_update()

    def test_an_empty_listing(self) -> None:
        with pytest.raises(ValueError, match="no rows"):
            TickerListing.from_csv("\n\n")

    def test_a_row_short_of_its_fields(self) -> None:
        with pytest.raises(ValueError, match="startDate"):
            TickerListing.from_csv("SPX,S&P 500 Index,2005-01-03\n")

    def test_a_row_whose_dates_are_not_dates(self) -> None:
        with pytest.raises(ValueError, match="where a date belongs"):
            TickerListing.from_csv("SPX,S&P 500 Index,2005-01-03,ongoing\n")

    def test_a_page_where_a_listing_belongs(self) -> None:
        """An error page is CSV too, as far as a splitter is concerned."""
        with pytest.raises(ValueError, match="startDate"):
            TickerListing.from_csv("<html><body>Invalid userid</body></html>")
