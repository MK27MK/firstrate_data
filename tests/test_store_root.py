"""The store owns one subdirectory of the path it takes, and everything sits there.

``Store(directory)`` keeps what it owns under ``directory/firstrate_data``: the
bars tree, the metafile tables, the scratch an ingest unzips into and the spool
a download streams to. The path it takes can be a volume root with other tenants
on it, which is what the machines running this hand over.

A store that splits across two roots doesn't fail. A bundle still downloads and
still ingests: it spools into one tree and writes into the other. The one
symptom is the wrong directory filling up while the store looks half-empty.
These tests assert that the paths share an ancestor, rather than leaving that to
a disk with 400 GB in flight.
"""

from pathlib import Path

import pytest

from firstrate_data.domain import MetafileType, Timeframe
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.store.store import PARQUET_FILES, Store
from tests.conftest import UNADJUSTED, Spool, bars_archive, listed_request

_SPLITS = b"AAPL,2020-08-31,4.0\n"
_SPLITS_REQUEST = MetafileType.SPLITS


@pytest.fixture
def given(tmp_path: Path) -> Path:
    """Return the path an operator points at through ``FIRSTRATE_DATA_PATH``.

    The path holds one other thing, so that anything the store scatters beside
    itself is visible.
    """
    (tmp_path / "not-ours").mkdir()
    return tmp_path


@pytest.fixture
def store_root(given: Path) -> Path:
    """Where that path's store directory lives."""
    return given / "firstrate_data"


class TestTheStoreIsOneSubdirectory:
    def test_it_is_the_only_thing_the_store_adds(
        self,
        given: Path,
        store_root: Path,
        spool: Spool,
    ) -> None:
        """Add a single subdirectory to the given path.

        That path isn't the store's to fill: it's a volume root on the machines
        running this, and it has other tenants.
        """
        store = Store(given)

        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_metafile(spool(_SPLITS), _SPLITS_REQUEST)

        assert sorted(given.iterdir()) == sorted([given / "not-ours", store_root])

    def test_bars_land_in_the_bars_tree_below_it(
        self,
        given: Path,
        store_root: Path,
        spool: Spool,
    ) -> None:
        store = Store(given)

        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        written = list(given.rglob(PARQUET_FILES))
        assert written
        assert all(file.is_relative_to(store_root / "bars") for file in written)

    def test_a_metafile_table_sits_beside_the_bars_tree(
        self,
        given: Path,
        store_root: Path,
        spool: Spool,
    ) -> None:
        """Write the table at the store root, not inside ``bars/``.

        A corporate action has no ``ticker=``/``timeframe=`` to sit under, and a
        bars glob must not see it.
        """
        store = Store(given)

        store.ingest_metafile(spool(_SPLITS), _SPLITS_REQUEST)

        assert (store_root / "splits.parquet").exists()
        assert store.splits().count("*").fetchone() == (1,)


class TestTheTransientsShareThatRoot:
    def test_the_spool_and_the_bars_tree_share_one_root(self, given: Path) -> None:
        """Keep the spool and the bars tree under one root.

        What's on its way in and what has arrived need the same free space.
        One volume has to measure them both, and one root puts them there.
        """
        store = Store(given)

        assert store.spool.is_relative_to(store._directory)
        assert store._bars_directory.is_relative_to(store._directory)

    def test_the_store_names_its_own_spool(
        self,
        given: Path,
        store_root: Path,
    ) -> None:
        """Tell the caller where the spool sits.

        The caller hands over a path and learns nothing about the layout inside
        it: a downloader asks the store where to spool and the store answers.
        """
        store = Store(given)

        assert store.spool == store_root / "spool"
        assert store.spool.is_dir()

    def test_a_spool_stated_to_the_store_is_the_one_a_download_uses(
        self,
        given: Path,
        tmp_path: Path,
    ) -> None:
        """Place the spool where the caller stated it.

        A caller with a reason to move the spool says so to the store, the only
        thing that places it. One answer covers where an archive waits,
        whichever way the caller got there.
        """
        elsewhere = tmp_path / "elsewhere"
        store = Store(given, elsewhere)

        stocks = StockClient("test-user", store)

        assert store.spool == elsewhere
        assert stocks.spool == elsewhere

    def test_the_spill_directory_is_inside_it(
        self,
        given: Path,
        store_root: Path,
    ) -> None:
        store = Store(given)

        setting = store._connection.sql(
            "SELECT current_setting('temp_directory')",
        ).fetchone()

        assert setting is not None
        assert Path(setting[0]).is_relative_to(store_root)

    def test_a_download_spools_into_the_store(
        self,
        given: Path,
        store_root: Path,
    ) -> None:
        """Spool into the store root.

        A split root shows one symptom: archives spool to one directory while
        the bars they become land in another.
        """
        store = Store(given)

        stocks = StockClient("test-user", store)

        assert stocks._fetcher.spool.is_relative_to(store_root)


class TestTheStoreReadsBackWhereItWrote:
    def test_what_an_ingest_wrote_is_what_a_read_finds(
        self,
        given: Path,
        spool: Spool,
    ) -> None:
        """Read back what an ingest wrote.

        The write path and the read path build the tree's location on their
        own. Agreeing on the partition levels counts for nothing if they
        disagree about the root they hang from.
        """
        store = Store(given)
        store.ingest_bars(spool(bars_archive("AAPL", "AMZN")), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED)

        assert bars.count("*").fetchone() == (6,)

    def test_a_second_store_over_the_same_path_sees_it(
        self,
        given: Path,
        spool: Spool,
    ) -> None:
        """Derive the subdirectory instead of remembering it.

        A store outlives the process that filled it: the bundle writes it and a
        notebook reads it.
        """
        Store(given).ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert Store(given).bars().count("*").fetchone() == (3,)
