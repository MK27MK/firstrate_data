"""Reads and ingests must survive the AppleDouble files the store's own disk grows.

The store lives on an exFAT volume, and macOS makes one ``._`` sidecar per file
as it lands -- so the read path cannot depend on having cleaned them up, and an
archive zipped on a Mac can carry them in among the payloads.
"""

from pathlib import Path

import pytest

from firstrate_data.domain import BarType, Dataset, Timeframe
from firstrate_data.store.store import Store
from tests.conftest import (
    BARS,
    UNADJUSTED,
    Spool,
    archive,
    listed_request,
    payload_name,
)

# 4096 bytes of AppleDouble header, in miniature: matches the glob, is not data
_SIDECAR = b"\x00\x05\x16\x07\x00\x02\x00\x00Mac OS X" * 4


@pytest.fixture
def haunted(stocked: Store) -> Store:
    """A store with an AppleDouble beside every parquet file."""
    for written in list(stocked._bars_directory.rglob("*.parquet")):
        (written.parent / f"._{written.name}").write_bytes(_SIDECAR)

    return stocked


class TestReadsIgnoreSidecars:
    def test_stock_bars_still_reads(self, haunted: Store) -> None:
        bars = haunted.stock_bars(Timeframe.DAY_1, UNADJUSTED)

        assert bars.count("*").fetchone() == (6,)

    def test_a_whole_tree_read_still_works(self, haunted: Store) -> None:
        assert haunted.bars().count("*").fetchone() == (15,)

    def test_a_single_ticker_read_still_works(self, haunted: Store) -> None:
        bars = haunted.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")

        assert bars.count("*").fetchone() == (3,)

    def test_a_dataset_narrowed_read_still_works(self, haunted: Store) -> None:
        bars = haunted.stock_bars(Timeframe.MIN_1, UNADJUSTED, dataset=Dataset.DELISTED)

        assert bars.count("*").fetchone() == (6,)


class TestIngestIgnoresSidecars:
    def test_a_sidecar_in_the_archive_is_not_a_ticker(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """``._AAPL_full_1day_UNADJUSTED.txt`` matches the payload glob, and is
        not text. Read as one, it would invent a ticker named ``._AAPL``.

        """
        with_sidecar = archive(
            {
                payload_name("AAPL"): BARS,
                f"._{payload_name('AAPL')}": _SIDECAR,
            },
        )

        ingested = store.ingest_bars(spool(with_sidecar), listed_request())

        assert ingested.tickers == 1
        tickers = store.bars().select("ticker").distinct().fetchall()
        assert [s for (s,) in tickers] == ["AAPL"]

    def test_ingesting_into_a_haunted_tree_still_works(
        self,
        haunted: Store,
        spool: Spool,
    ) -> None:
        """The sidecars sit in the very partitions a re-ingest drops and rewrites."""
        ingested = haunted.ingest_bars(
            spool(archive({payload_name("AAPL"): BARS})),
            listed_request(),
        )

        assert ingested.rows == 3
        assert haunted.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL").count(
            "*",
        ).fetchone() == (3,)


class TestTheGlobItself:
    def test_the_bar_type_glob_cannot_match_a_dotfile(self, store: Store) -> None:
        pattern = store._get_bar_path(BarType())

        assert not Path(pattern).name.startswith("*")
