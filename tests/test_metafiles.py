"""Splits, dividends and the continuous audit: their own tables at the store root.

They are not bars -- no ticker partition, no timeframe, nothing to increment --
so each is replaced whole on every fetch.

What the endpoint has actually been seen serving is an archive of one headerless
file per ticker, named for the ticker and holding nothing but the action's date
and its number. That shape is what ``_ARCHIVED_*`` stands in for, and it is why
the schema is declared rather than sniffed: with no header to read, the sniffer
takes the first action for column names and no two tickers then agree.

The container is still only half-pinned -- the docs never promise one -- so a
bare CSV is accepted too. It names no ticker, so it stays on the sniffer, which
is what ``_SPLITS`` covers. Both loose ends are issue #15.
"""

import pytest

from firstrate_data.domain import MetafileType
from firstrate_data.store.store import Store
from tests.conftest import Spool, archive

_SPLITS = b"AAPL,2020-08-31,4.0\nNVDA,2021-07-20,4.0\n"

# the vendor's own shape: the ticker is the filename, the rows are date + number
_ARCHIVED_SPLITS = {
    "AAPL.txt": b"2020-08-31,4\n2014-06-09,7\n",
    "NVDA.txt": b"2021-07-20,4\n",
}
# dividends carry a suffix the splits payloads do not; it is not the ticker
_ARCHIVED_DIVIDENDS = {"AAPL_divs.txt": b"2024-02-09,0.24\n2023-11-10,0.24\n"}

_REQUEST = MetafileType.SPLITS
_DIVIDENDS_REQUEST = MetafileType.DIVIDENDS


class TestAMetafileArrivesEitherWay:
    def test_a_bare_csv_is_ingested(self, store: Store, spool: Spool) -> None:
        ingested = store.ingest_metafile(spool(_SPLITS), _REQUEST)

        assert ingested.rows == 2
        assert store.splits().count("*").fetchone() == (2,)

    def test_an_archive_is_ingested(self, store: Store, spool: Spool) -> None:
        ingested = store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        assert ingested.rows == 3
        assert store.splits().count("*").fetchone() == (3,)

    def test_a_metafile_has_no_tickers_of_its_own(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Whatever columns the vendor gives it, it is not partitioned by one."""
        ingested = store.ingest_metafile(spool(_SPLITS), _REQUEST)

        assert ingested.tickers is None


class TestAnArchiveOfOneFilePerTicker:
    """The shape the endpoint actually serves."""

    def test_every_payload_is_read(self, store: Store, spool: Spool) -> None:
        """Read every payload in the archive under one declared schema.

        Headerless files whose first row differs cannot be sniffed: one
        declared schema is what lets a single read span every ticker.
        """
        ingested = store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        assert ingested.rows == 3
        assert ingested.rejected == 0

    def test_the_ticker_is_recovered_from_the_filename(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Take the ticker from the name of the file that holds its rows.

        The ticker is nowhere in the rows, so a metafile read without it says
        which dates had splits but never whose.
        """
        store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        by_ticker = store.splits().select("ticker").distinct().order("ticker")

        assert by_ticker.fetchall() == [("AAPL",), ("NVDA",)]

    def test_the_columns_are_named_and_typed(self, store: Store, spool: Spool) -> None:
        """Give every column its domain name and its domain type.

        ``column0`` is not a schema, and a VARCHAR date does not join against
        the bars whose adjustment it explains.
        """
        store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        splits = store.splits()

        assert splits.columns == ["ticker", "date", "ratio"]
        assert [str(kind) for kind in splits.types] == ["VARCHAR", "DATE", "DOUBLE"]

    def test_a_dividends_payload_drops_its_suffix(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """``AAPL_divs`` is the vendor's filename, not a ticker."""
        store.ingest_metafile(spool(archive(_ARCHIVED_DIVIDENDS)), _DIVIDENDS_REQUEST)

        dividends = store.dividends()

        assert dividends.columns == ["ticker", "date", "amount"]
        assert dividends.select("ticker").distinct().fetchall() == [("AAPL",)]

    def test_a_dotted_ticker_keeps_its_dots(self, store: Store, spool: Spool) -> None:
        """Strip only the last extension from the name of a payload.

        ``AA.B`` and ``AAIC.C`` are ordinary tickers here, and the store holds
        thousands of them.
        """
        store.ingest_metafile(spool(archive({"AA.B.txt": b"2015-03-02,2\n"})), _REQUEST)
        store.ingest_metafile(
            spool(archive({"AAIC.C_divs.txt": b"2019-12-30,0.225\n"})),
            _DIVIDENDS_REQUEST,
        )

        assert store.splits().select("ticker").fetchall() == [("AA.B",)]
        assert store.dividends().select("ticker").fetchall() == [("AAIC.C",)]

    def test_the_vendors_readme_is_not_a_ticker(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The archive carries ``_splits_readme.txt`` explaining the row format.

        Parsed as a payload it yields no rows and two dozen rejects -- and a
        reject count that fires on every fetch stops meaning anything, which
        costs the store the one signal it has that lines were dropped.
        """
        readme = b"FirstRate Data Splits\n=====================\nOne file per ticker\n"

        payloads = {**_ARCHIVED_SPLITS, "_splits_readme.txt": readme}
        ingested = store.ingest_metafile(spool(archive(payloads)), _REQUEST)

        assert ingested.rejected == 0
        assert ingested.rows == 3
        assert store.splits().select("ticker").distinct().order(
            "ticker",
        ).fetchall() == [("AAPL",), ("NVDA",)]

    def test_the_rows_survive_intact(self, store: Store, spool: Spool) -> None:
        store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        aapl = store.splits().filter("ticker = 'AAPL'").order("date")

        assert [(str(d), r) for _, d, r in aapl.fetchall()] == [
            ("2014-06-09", 7.0),
            ("2020-08-31", 4.0),
        ]


class TestReplacedWhole:
    def test_a_second_fetch_does_not_accumulate(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        store.ingest_metafile(
            spool(archive({**_ARCHIVED_SPLITS, "TSLA.txt": b"2022-08-25,3\n"})),
            _REQUEST,
        )

        assert store.splits().count("*").fetchone() == (4,)

    def test_a_ticker_that_stops_being_served_leaves(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Hold the last fetch alone, not the union of every fetch.

        A ticker the vendor stops serving does not linger.
        """
        store.ingest_metafile(spool(archive(_ARCHIVED_SPLITS)), _REQUEST)

        store.ingest_metafile(spool(archive({"AAPL.txt": b"2020-08-31,4\n"})), _REQUEST)

        assert store.splits().select("ticker").distinct().fetchall() == [("AAPL",)]

    def test_the_partial_file_does_not_survive(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Leave no partial file behind.

        The table is written beside the target and swapped in, so a reader sees
        one whole table or the other, never a half-written file.
        """
        store.ingest_metafile(spool(_SPLITS), _REQUEST)

        assert list(store._directory.glob("*.partial")) == []


class TestTheTablesAreKeptApart:
    def test_each_metafile_type_gets_its_own(self, store: Store, spool: Spool) -> None:
        store.ingest_metafile(spool(_SPLITS), _REQUEST)
        store.ingest_metafile(
            spool(b"AAPL,2024-02-09,0.24\n"),
            MetafileType.DIVIDENDS,
        )

        assert store.splits().count("*").fetchone() == (2,)
        assert store.dividends().count("*").fetchone() == (1,)

    def test_the_audit_file_is_the_futures_one(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_metafile(
            spool(b"ES,ESH24,2024-03-08\n"),
            MetafileType.CONTIN_AUDIT,
        )

        assert store.contin_audit().count("*").fetchone() == (1,)

    def test_reading_one_that_was_never_fetched_says_so(self, store: Store) -> None:
        """Raise rather than return an empty relation.

        Unlike bars, there is no empty relation of the right shape to return:
        the shape is whatever the sniffer reads off the file that is missing.
        """
        with pytest.raises(FileNotFoundError, match="dividends"):
            store.dividends()
