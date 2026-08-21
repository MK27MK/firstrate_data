"""The whole path an operator actually uses: ask the vendor, and it is queryable.

Served from a local HTTP server rather than a mocked transport, because the
seam this covers is the wiring between the loader, the transport and the
store -- a mock at that seam would only assert the wiring against itself.
No real credentials are involved.
"""

import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from firstrate_data.domain import EquitiesAdjustment, Period, Timeframe
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED, bars_archive

_SPLITS = b"AAPL,2020-08-31,4.0\n"


class Vendor:
    """A stand-in endpoint that serves one archive and remembers who asked."""

    def __init__(self, served: bytes) -> None:
        self.served = served
        self.url = ""
        self.asked: list[tuple[str, dict[str, list[str]]]] = []


@pytest.fixture
def vendor() -> Generator[Vendor]:
    served = Vendor(bars_archive("AAPL", "AMZN"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requested = urlparse(self.path)
            served.asked.append((requested.path.lstrip("/"), parse_qs(requested.query)))
            body = _SPLITS if requested.path.endswith("meta_file") else served.served
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - overrides BaseHTTPRequestHandler.log_message, whose signature the standard library fixes.
            """Quiet: a test suite is not a request log."""

    http = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    served.url = f"http://127.0.0.1:{http.server_port}"
    try:
        yield served
    finally:
        http.shutdown()


@pytest.fixture
def stocks(store: Store, vendor: Vendor) -> StockClient:
    return StockClient("test-user", store, vendor.url)


class TestADownloadIsQueryableAfterwards:
    def test_the_bars_are_in_the_store(self, stocks: StockClient, store: Store) -> None:
        ingested = stocks.download_historical_bars(
            Period.FULL,
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker_range="A",
        )

        assert (ingested.tickers, ingested.rows, ingested.rejected) == (2, 6, 0)
        assert store.stock_bars(Timeframe.DAY_1, UNADJUSTED).count("*").fetchone() == (
            6,
        )

    def test_the_request_carries_the_userid_and_the_parameters(
        self,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        stocks.download_historical_bars(
            Period.FULL,
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker_range="A",
        )

        endpoint, params = vendor.asked[0]
        assert endpoint == "data_file"
        assert params["userid"] == ["test-user"]
        assert params["ticker_range"] == ["A"]
        assert params["period"] == ["full"]

    def test_a_metafile_lands_in_its_own_table(
        self,
        stocks: StockClient,
        store: Store,
    ) -> None:
        stocks.download_splits()

        assert store.splits().count("*").fetchone() == (1,)

    def test_the_archive_is_not_kept(self, stocks: StockClient, store: Store) -> None:
        """Parquet is the only copy: the unzipped payloads go with the ingest."""
        stocks.download_historical_bars(
            Period.FULL,
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker_range="A",
        )

        assert list(store._directory.glob(".ingest-*")) == []
        assert list(store._directory.rglob("*.txt")) == []

    def test_re_running_the_same_download_is_idempotent(
        self,
        stocks: StockClient,
        store: Store,
    ) -> None:
        """The supported cure for an interruption, so it must not double a bar."""
        stocks.download_historical_bars(
            Period.FULL,
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker_range="A",
        )

        stocks.download_historical_bars(
            Period.FULL,
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker_range="A",
        )

        assert store.bars().count("*").fetchone() == (6,)


class TestTheVendorIsNotAskedForWhatItCannotServe:
    def test_an_unoffered_timeframe_never_reaches_the_wire(
        self,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        with pytest.raises(ValueError, match="1min and 1day"):
            stocks.download_historical_bars(
                Period.FULL,
                Timeframe.MIN_5,
                EquitiesAdjustment.UNADJUSTED,
                ticker_range="A",
            )

        assert vendor.asked == []
