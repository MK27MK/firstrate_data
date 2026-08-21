"""Fixtures building vendor archives in memory, and a store to ingest them into.

The real archives are hundreds of GB from an external disk. Everything here is
a few-row stand-in with the same shape, so the tests state what an archive
*is* rather than what one download happened to hold.
"""

import io
import zipfile
from collections.abc import Callable, Mapping
from itertools import count
from pathlib import Path

import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.requests import (
    BarsRequest,
    DelistedBarsRequest,
)
from firstrate_data.store.store import Store

# one bar per line, the vendor's own format: naive stamp then OHLCV
BARS = """2024-01-02 09:30:00,100.0,101.0,99.5,100.5,1000
2024-01-02 09:31:00,100.5,102.0,100.0,101.5,2000
2024-01-03 09:30:00,101.5,103.0,101.0,102.5,1500
"""

UNADJUSTED = EquitiesAdjustment.UNADJUSTED

# What the ``spool`` fixture hands back: bytes in, the path they landed at out
type Spool = Callable[[bytes], Path]


def archive(payloads: Mapping[str, str | bytes]) -> bytes:
    """Build a vendor zip in memory, what ``data_file`` serves."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipped:
        for name, content in payloads.items():
            zipped.writestr(name, content)
    return buffer.getvalue()


def payload_name(
    ticker: str,
    period: str = "full",
    timeframe: Timeframe = Timeframe.DAY_1,
    adjustment: EquitiesAdjustment = UNADJUSTED,
) -> str:
    """Name one ticker's file inside an archive, the way the vendor names it."""
    return f"{ticker}_{period}_{timeframe.value}_{adjustment.value}.txt"


def bars_archive(
    *tickers: str,
    text: str = BARS,
    period: str = "full",
    timeframe: Timeframe = Timeframe.DAY_1,
    adjustment: EquitiesAdjustment = UNADJUSTED,
) -> bytes:
    """Build an archive holding the same bars for each of `tickers`."""
    return archive(
        {payload_name(t, period, timeframe, adjustment): text for t in tickers},
    )


def stock_bar_type(
    timeframe: Timeframe = Timeframe.DAY_1,
    adjustment: EquitiesAdjustment = UNADJUSTED,
) -> BarType:
    """Build the tree location that holds a stock archive's bars.

    The ticker is no part of it: each payload inside the archive names its own.
    """
    return BarType(AssetType.STOCK, timeframe=timeframe, adjustment=adjustment)


def listed_request(
    period: Period = Period.FULL,
    timeframe: Timeframe = Timeframe.DAY_1,
    adjustment: EquitiesAdjustment = UNADJUSTED,
    ticker_range: str | None = "A",
) -> BarsRequest:
    return BarsRequest(
        stock_bar_type(timeframe, adjustment),
        period,
        ticker_range=ticker_range,
    )


def delisted_request(
    selector: DelistedArchive | DelistedUpdate = DelistedArchive.ARCHIVE_2,
    timeframe: Timeframe = Timeframe.MIN_1,
    adjustment: EquitiesAdjustment = UNADJUSTED,
) -> DelistedBarsRequest:
    return DelistedBarsRequest(
        stock_bar_type(timeframe, adjustment),
        selector=selector,
    )


@pytest.fixture
def spool(tmp_path_factory: pytest.TempPathFactory) -> Spool:
    """Lands an archive on disk and answers with its path.

    The store takes only a path, because that's what a download hands it: the
    vendor's zip streams to disk and never sits resident in memory. An archive
    built in memory goes through here first, so the tests cross the same seam
    production does.
    """
    # a directory of its own, not the test's ``tmp_path``: a store gets a path
    # and keeps to one subdirectory of it. An archive dropped in beside it
    # would count toward its size.
    directory = tmp_path_factory.mktemp("served")
    served = count()

    def spooled(payload: bytes) -> Path:
        path = directory / f"served-{next(served)}"
        path.write_bytes(payload)
        return path

    return spooled


@pytest.fixture
def store(tmp_path_factory: pytest.TempPathFactory) -> Store:
    """Create an empty store in a directory of its own."""
    return Store(tmp_path_factory.mktemp("store"))


@pytest.fixture
def stocked(store: Store, spool: Spool) -> Store:
    """Build a store holding what a small complete pull would leave.

    Listed bars at two timeframes, and the delisted history in both its
    halves. Fifteen bars in five partitions, enough that a read spanning
    the whole tree has something to get wrong.
    """
    store.ingest_bars(spool(bars_archive("AAPL", "AMZN")), listed_request())
    store.ingest_bars(
        spool(bars_archive("BAC", timeframe=Timeframe.MIN_1)),
        listed_request(timeframe=Timeframe.MIN_1, ticker_range="B"),
    )
    store.ingest_bars(
        spool(bars_archive("CRY-DELISTED", timeframe=Timeframe.MIN_1)),
        delisted_request(),
    )
    store.ingest_bars(
        spool(bars_archive("AAGR-DELISTED", timeframe=Timeframe.MIN_1)),
        delisted_request(DelistedUpdate.YEAR),
    )
    return store
