"""Where a bar lands on disk, and that the read side still agrees with it.

The tree's levels *are* the read selectors, in order: ingest writes the path and
every read globs it, so the layout is the one thing both sides of the store
depend on. Nothing fails loudly when a level is renamed, reordered or dropped --
a read simply stops matching -- so the shape is pinned here rather than left to
be inferred from whichever test happens to break.
"""

import re
from pathlib import PurePosixPath

import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    IndexAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store.store import PARQUET_FILES, Store
from tests.conftest import (
    BARS,
    Spool,
    archive,
    bars_archive,
    delisted_request,
    listed_request,
)

# ``{date}_{ingest}_{uuid}``: the date so a file sorts by when it landed and
# cannot be mistaken for an AppleDouble, the ingest id so an archive can scan
# what it just wrote, the uuid because APPEND requires one and a date alone
# collides the second time a partition is written in one day
_FILENAME = re.compile(r"\d{4}-\d{2}-\d{2}_[0-9a-f]{8}_[0-9a-f-]{36}\.parquet")

# Wednesday's `week`, one bar past what a `full` of BARS carries
_LATER = "2024-01-04 09:30:00,102.5,104.0,102.0,103.5,1600\n"


def _files(store: Store) -> list[str]:
    """Every parquet file in the bars tree, as a path below the bars root."""
    return sorted(
        str(written.relative_to(store._bars_directory))
        for written in store._bars_directory.rglob(PARQUET_FILES)
    )


def _partitions(store: Store) -> list[str]:
    """Return the partition each file sits in, without the filename."""
    return sorted({str(PurePosixPath(path).parent) for path in _files(store)})


class TestABarIsFiledByWhatItIs:
    def test_a_listed_stock_names_every_level(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert _partitions(store) == [
            (
                "asset_type=stock/dataset=listed/adjustment=UNADJUSTED"
                "/timeframe=1day/ticker=AAPL"
            ),
        ]

    def test_a_delisted_ticker_drops_its_suffix(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Drop the vendor's ``-DELISTED`` suffix at the ticker level.

        ``CRY-DELISTED`` is the vendor's filename, not a ticker: the dataset
        level already carries that distinction, and leaving the suffix on would
        file one company's bars under two tickers.
        """
        store.ingest_bars(
            spool(bars_archive("CRY-DELISTED", timeframe=Timeframe.MIN_1)),
            delisted_request(),
        )

        assert _partitions(store) == [
            (
                "asset_type=stock/dataset=delisted/adjustment=UNADJUSTED"
                "/timeframe=1min/ticker=CRY"
            ),
        ]

    def test_futures_land_in_the_continuous_dataset(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """File futures bars under the continuous dataset.

        The dataset is the request's to say, and a futures request says
        continuous -- it is the only futures dataset the layout carries.
        """
        ratio = ContinuousFuturesAdjustment.RATIO
        store.ingest_bars(
            spool(
                archive({f"ES_full_{Timeframe.DAY_1.value}_{ratio.value}.txt": BARS}),
            ),
            BarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1, adjustment=ratio),
                Period.FULL,
            ),
        )

        assert _partitions(store) == [
            (
                "asset_type=futures/dataset=continuous/adjustment=contin_adj_ratio"
                "/timeframe=1day/ticker=ES"
            ),
        ]

    def test_the_levels_are_the_read_selectors_in_order(self, stocked: Store) -> None:
        """Match the read selectors' depth and order to the tree's levels.

        Depth and order are not decoration: a glob mixing trees of different
        depths fails outright with a Hive partition mismatch.
        """
        levels = [
            [pair.split("=")[0] for pair in partition.split("/")]
            for partition in _partitions(stocked)
        ]

        assert levels == [BarType.fields()] * len(levels)


class TestTheBarType:
    """The bar type's five levels travel together as one path.

    The five levels travel as one value, so the order they nest in and the
    rule turning each into a path segment are the store's alone to know.
    """

    def test_a_request_says_where_its_bars_go(self) -> None:
        bar_type = delisted_request().bar_type.from_ticker("CRY")

        assert bar_type.to_dict(drop_none=True) == {
            "asset_type": "stock",
            "dataset": "delisted",
            "adjustment": "UNADJUSTED",
            "timeframe": "1min",
            "ticker": "CRY",
        }

    def test_an_index_is_filed_under_an_adjustment_it_never_sent(self) -> None:
        """File an index under an adjustment it never sent.

        ``data_file`` takes no adjustment for an index and the tree names one
        at every level, so this value is the store's word and not the vendor's.
        """
        request = BarsRequest(
            BarType(
                AssetType.INDEX,
                timeframe=Timeframe.DAY_1,
                adjustment=IndexAdjustment.UNADJUSTED,
            ),
            Period.FULL,
        )

        assert request.bar_type.to_dict(drop_none=True)["adjustment"] == "UNADJUSTED"

    def test_a_level_left_out_is_a_wildcard(self, store: Store) -> None:
        """Leave a level out of the bar type to match it as a wildcard.

        This is what lets one read span every adjustment a ticker was fetched
        under, and another narrow to one.
        """
        pattern = store._get_bar_path(BarType(AssetType.STOCK, ticker="AAPL"))

        assert str(PurePosixPath(pattern).relative_to(store._bars_directory)) == (
            "asset_type=stock/dataset=*/adjustment=*/timeframe=*/ticker=AAPL"
            f"/{PARQUET_FILES}"
        )

    def test_one_bar_type_cannot_be_a_family_of_them(self, store: Store) -> None:
        """Reject a bar type with a wildcard level for a directory replace.

        A replace removes this directory whole, and a wildcard as a literal
        path matches nothing -- so the drop would silently do nothing.
        """
        with pytest.raises(ValueError, match="ticker"):
            store._get_bar_path(listed_request().bar_type, files_regex=None)


class TestTheFilename:
    def test_it_carries_the_date_and_a_uuid(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        (written,) = _files(store)
        assert _FILENAME.fullmatch(PurePosixPath(written).name)

    def test_one_ingests_files_all_carry_its_id(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Carry one ingest id across every file an ingest writes.

        The id is what lets an ingest scan what it wrote, so every file of the
        archive must carry the same one -- across batches, across tickers.
        """
        store.ingest_bars(spool(bars_archive("AAPL", "AMZN", "ADP")), listed_request())

        ids = {PurePosixPath(written).name.split("_")[1] for written in _files(store)}

        assert len(_files(store)) == 3
        assert len(ids) == 1

    def test_two_ingests_carry_different_ids(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_bars(
            spool(bars_archive("AAPL", text=_LATER, period="week")),
            listed_request(period=Period.WEEK, ticker_range=None),
        )

        ids = {PurePosixPath(written).name.split("_")[1] for written in _files(store)}

        assert len(ids) == 2

    def test_an_increment_lands_beside_what_it_extends(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Land an increment beside the full it extends, not over it.

        Two writes into one partition on one day is exactly what an increment
        does -- a filename that collided would overwrite the full.
        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        store.ingest_bars(
            spool(bars_archive("AAPL", text=_LATER, period="week")),
            listed_request(period=Period.WEEK, ticker_range=None),
        )

        written = _files(store)
        assert len(written) == 2
        assert len(_partitions(store)) == 1


class TestTheReadSideAgreesWithTheTree:
    def test_the_projection_is_the_stores_columns_in_order(
        self,
        stocked: Store,
    ) -> None:
        """Read back the store's columns in their own order, not Hive's.

        Hive partitioning appends the key columns alphabetically, which
        nothing else in the store agrees with -- hence a named projection.
        """
        assert stocked.bars().columns == [
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "open_interest",
            *BarType.fields(),
        ]

    def test_a_read_that_matches_nothing_has_the_same_shape(
        self,
        stocked: Store,
    ) -> None:
        """Give an empty read the same shape as one that matches something.

        No data is an answer, and an answer of the wrong shape is not one:
        a caller's `.filter("ticker = ...")` has to survive the empty case.
        """
        empty = stocked.stock_bars(Timeframe.MIN_30, EquitiesAdjustment.SPLIT)

        assert empty.fetchall() == []
        assert empty.columns == stocked.bars().columns
        assert empty.types == stocked.bars().types

    def test_every_level_comes_back_as_a_column(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Read every bar type level back as a column, not a NULL.

        The tree is the only record of these values -- they are not in the
        parquet files, so a level that stopped being read would read as NULL.
        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        levels = store.bars().select(", ".join(BarType.fields())).distinct()

        assert levels.fetchall() == [("stock", "listed", "UNADJUSTED", "1day", "AAPL")]
