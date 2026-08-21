"""What has to be true before several archives may be pulled at once.

A parallel downloader is only worth having if an interrupted transfer resumes to
the same bytes, a truncated one is refused rather than filed, and the spool does
not grow to the size of the bundle. Each of those is asserted here against a
real socket rather than a mock.
"""

import hashlib
import io
import os
import threading
import zipfile
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from firstrate_data.domain import Period, Timeframe
from firstrate_data.download.client.fetcher import (
    ArchiveFetcher,
    Fetched,
    IncompleteDownload,
)
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED, bars_archive, listed_request
from tests.vendor import Vendor, serving

# Enough tickers that the archive is tens of kilobytes: an abort "partway
# through" needs a body with a partway.
BIG = bars_archive(*[f"TCK{n}" for n in range(200)])

# Smaller than a chunk of the real thing, so a body that dies partway leaves
# whole chunks on disk to resume from rather than nothing at all.
TEST_CHUNK = 512


@pytest.fixture
def vendor() -> Generator[Vendor]:
    yield from serving(Vendor(payload=BIG))


@pytest.fixture
def fetcher(vendor: Vendor, tmp_path: Path) -> Generator[ArchiveFetcher]:
    made = ArchiveFetcher(
        vendor.url,
        "test-user",
        tmp_path / "spool",
        chunk_size=TEST_CHUNK,
    )
    yield made
    made.close()


def a_request(ticker_range: str = "A") -> BarsRequest:
    return listed_request(ticker_range=ticker_range)


class TestAnArchiveArrivesIntactOrNotAtAll:
    def test_the_checksum_is_of_what_was_served(
        self,
        fetcher: ArchiveFetcher,
        vendor: Vendor,
    ) -> None:
        fetched = fetcher.fetch(a_request(), "bars")

        assert fetched.size == len(vendor.payload)
        assert fetched.sha256 == hashlib.sha256(vendor.payload).hexdigest()
        assert fetched.path.read_bytes() == vendor.payload

    def test_the_archive_is_never_resident(self, fetcher: ArchiveFetcher) -> None:
        """The whole point of spooling: what comes back is a path, not a body."""
        fetched = fetcher.fetch(a_request(), "bars")

        assert isinstance(fetched.path, Path)
        assert fetched.path.stat().st_size == len(BIG)
        assert not hasattr(fetched, "content")

    def test_a_body_that_never_finishes_is_refused(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """A truncated archive that got a final name would be ingested as if whole."""
        vendor.abort_after = 4096
        vendor.aborts_remaining = 99

        fetcher = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "spool",
            chunk_size=TEST_CHUNK,
            attempts=2,
        )
        try:
            with pytest.raises(IncompleteDownload):
                fetcher.fetch(a_request(), "bars")
        finally:
            fetcher.close()

        # nothing was promoted: only the partial and its record are left behind
        assert list((tmp_path / "spool").glob("*.partial")) != []
        assert [
            p for p in (tmp_path / "spool").iterdir() if "partial" not in p.name
        ] == []

    def test_a_transient_503_is_retried_rather_than_raised(
        self,
        fetcher: ArchiveFetcher,
        vendor: Vendor,
    ) -> None:
        vendor.fails_remaining = 1

        fetched = fetcher.fetch(a_request(), "bars")

        assert fetched.sha256 == hashlib.sha256(BIG).hexdigest()


class TestAnInterruptedTransferResumes:
    def test_the_resumed_file_is_byte_identical_to_an_uninterrupted_one(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """The requirement the whole partial/promote dance exists for."""
        clean = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "clean",
            chunk_size=TEST_CHUNK,
        )
        uninterrupted = clean.fetch(a_request(), "bars")
        clean.close()

        vendor.abort_after = 4096
        vendor.aborts_remaining = 1
        interrupted_fetcher = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "resumed",
            chunk_size=TEST_CHUNK,
        )
        resumed = interrupted_fetcher.fetch(a_request(), "bars")
        interrupted_fetcher.close()

        assert resumed.attempts == 2, "the transfer was supposed to be interrupted"
        assert resumed.resumed_from > 0, "the second attempt started from zero"
        assert resumed.path.read_bytes() == uninterrupted.path.read_bytes()
        assert resumed.sha256 == uninterrupted.sha256

    def test_the_second_attempt_asks_for_the_tail(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        vendor.abort_after = 4096
        vendor.aborts_remaining = 1
        fetcher = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "spool",
            chunk_size=TEST_CHUNK,
        )
        fetcher.fetch(a_request(), "bars")
        fetcher.close()

        assert vendor.asked[0].range_header is None
        assert vendor.asked[1].range_header is not None
        assert vendor.asked[1].range_header.startswith("bytes=")

    def test_a_partial_survives_the_process_that_made_it(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """A three-hour sweep that dies must not start the archive again."""
        vendor.abort_after = 4096
        vendor.aborts_remaining = 1

        died = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "spool",
            chunk_size=TEST_CHUNK,
            attempts=1,
        )
        with pytest.raises(IncompleteDownload):
            died.fetch(a_request(), "bars")
        died.close()

        restarted = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "spool",
            chunk_size=TEST_CHUNK,
        )
        finished = restarted.fetch(a_request(), "bars")
        restarted.close()

        assert finished.resumed_from > 0, "the restart re-downloaded from zero"
        assert finished.path.read_bytes() == BIG


class TestAServerThatDoesNotDoRange:
    def test_a_200_to_a_range_request_starts_over_cleanly(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """A 206 is verified, not assumed.

        A vendor that builds an archive per request may have nothing to seek
        into, and must still yield the right bytes.
        """
        vendor.honour_range = False
        vendor.abort_after = 4096
        vendor.aborts_remaining = 1

        fetcher = ArchiveFetcher(
            vendor.url,
            "test-user",
            tmp_path / "spool",
            chunk_size=TEST_CHUNK,
        )
        fetched = fetcher.fetch(a_request(), "bars")
        fetcher.close()

        assert fetched.resumed_from == 0, "bytes were kept from an ignored Range"
        assert fetched.path.read_bytes() == BIG
        assert fetched.sha256 == hashlib.sha256(BIG).hexdigest()

    def test_an_interrupted_encoded_body_still_completes(self, tmp_path: Path) -> None:
        """An encoded body restarts rather than resumes.

        A Range counts bytes of the encoded entity, and what reaches the disk
        is what the stream decoded. Resuming across that boundary asks the
        decoder to start mid-gzip-stream, which it refuses, so every retry
        fails the same way and the archive never lands.
        """
        # incompressible, so gzip cannot shrink it below the abort offset and
        # the server has a real tail to serve on the second attempt
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zipped:
            for n in range(12):
                zipped.writestr(f"TCK{n}_full_1day_UNADJUSTED.txt", os.urandom(4000))
        body = buffer.getvalue()

        vendor = Vendor(payload=body, gzip_encoded=True)
        vendor.abort_after = 8000
        vendor.aborts_remaining = 1
        served = serving(vendor)
        running = next(served)
        try:
            fetcher = ArchiveFetcher(
                running.url,
                "test-user",
                tmp_path / "spool",
                chunk_size=TEST_CHUNK,
            )
            fetched = fetcher.fetch(a_request(), "bars")
            fetcher.close()
        finally:
            next(served, None)

        assert fetched.resumed_from == 0, "decoded bytes were kept across an encoding"
        assert fetched.path.read_bytes() == body
        assert fetched.sha256 == hashlib.sha256(body).hexdigest()

    def test_a_stale_partial_is_not_spliced_onto_a_different_body(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """A partial from an earlier run belongs to no body being served.

        These archives are built per request, so a partial and the body that
        follows it are not two halves of anything.
        """
        spool = tmp_path / "spool"
        spool.mkdir()
        fetcher = ArchiveFetcher(vendor.url, "test-user", spool, chunk_size=TEST_CHUNK)
        stale = spool / "data_file_stock_full_1day_UNADJUSTED_A.partial"
        stale.write_bytes(b"not this body at all")
        # a total that cannot belong to the body being served
        (spool / f"{stale.name}.meta").write_text('{"total": 7, "etag": null}')

        fetched = fetcher.fetch(a_request(), "bars")
        fetcher.close()

        assert fetched.path.read_bytes() == BIG


class TestTheSweepPullsSeveralAtOnce:
    def test_no_more_than_max_workers_are_in_flight(
        self,
        vendor: Vendor,
        store: Store,
    ) -> None:
        vendor.dwell = 0.05
        stocks = StockClient("test-user", store, vendor.url, max_workers=3)
        requests = [a_request(letter) for letter in "ABCDEFGH"]

        _fetch_all(stocks, requests, workers=3)
        stocks.close()

        assert vendor.peak_in_flight <= 3
        assert vendor.peak_in_flight > 1, "the fetches were not actually concurrent"

    def test_every_cell_lands_and_the_spool_is_left_empty(
        self,
        vendor: Vendor,
        store: Store,
    ) -> None:
        """Parquet is the only copy -- of the vendor's zip as much as its CSV."""
        stocks = StockClient("test-user", store, vendor.url, max_workers=3)
        spool = stocks.spool
        requests = [a_request(letter) for letter in "ABCD"]

        for fetched, request in _fetch_all(stocks, requests, workers=3):
            stocks.ingest(fetched, request)
        stocks.close()

        assert list(spool.iterdir()) == []
        # the stand-in serves one payload whatever range is asked for, and a
        # period=full replaces the partitions it names -- so four cells of the
        # same 200 tickers reconcile to one copy rather than four
        assert store.bars().count("*").fetchone() == (200 * 3,)


class TestTheSpoolIsBounded:
    def test_workers_wait_rather_than_fill_the_disk(
        self,
        vendor: Vendor,
        tmp_path: Path,
    ) -> None:
        """The spool holds `queue_depth` archives however far ahead workers get.

        That bound keeps a 400 GB bundle from needing 400 GB of scratch.
        """
        spool = tmp_path / "spool"
        fetcher = ArchiveFetcher(vendor.url, "test-user", spool, chunk_size=TEST_CHUNK)
        slots = threading.Semaphore(2)
        seen: list[int] = []
        lock = threading.Lock()

        def fetch(letter: str) -> None:
            slots.acquire()
            fetched = fetcher.fetch(a_request(letter), letter, name=letter)
            with lock:
                # by archive, not by file: one in flight is a `.partial` and the
                # small `.meta` recording what it was cut from
                seen.append(len({p.name.partition(".")[0] for p in spool.iterdir()}))
            fetched.path.unlink()
            slots.release()

        threads = [
            threading.Thread(target=fetch, args=(letter,)) for letter in "ABCDEFGH"
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        fetcher.close()

        assert max(seen) <= 2, f"the spool held {max(seen)} archives at once"


def _fetch_all(
    stocks: StockClient,
    requests: list[BarsRequest],
    workers: int,
) -> list[tuple[Fetched, BarsRequest]]:
    """Every request fetched concurrently, paired back up with what asked for it."""
    with ThreadPoolExecutor(workers) as pool:
        fetched = list(pool.map(stocks.fetch, requests))
    return list(zip(fetched, requests, strict=True))


def test_a_download_still_works_when_nothing_goes_wrong(
    vendor: Vendor,
    store: Store,
) -> None:
    """The ordinary path, through the loader rather than the transport."""
    stocks = StockClient("test-user", store, vendor.url)

    ingested = stocks.download_historical_bars(
        Period.FULL,
        Timeframe.DAY_1,
        UNADJUSTED,
        ticker_range="A",
    )
    stocks.close()

    assert ingested.tickers == 200
    assert ingested.rejected == 0
