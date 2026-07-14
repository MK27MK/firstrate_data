from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from string import ascii_uppercase

import requests

from firstrate_data.query_parameters import (
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.stock import FirstRateStocks

# week is a strict subset of year, so a complete pull only wants year -- fetching
# both would re-download the last week's rows for nothing
COMPLETE_DELISTED: list[DelistedArchive | DelistedUpdate] = [
    *DelistedArchive,
    DelistedUpdate.YEAR,
]


@dataclass
class BundleReport:
    """The outcome of a bundle sweep, cell by cell.

    ``skipped`` and ``failed`` are different things: a skipped cell is a
    combination the API does not offer (UNADJUSTED above the timeframes it
    supports), so re-running will never produce it; a failed cell is one the API
    should have served and didn't, so it is worth retrying.
    """

    downloaded: list[Path] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, Exception]] = field(default_factory=list)

    def _record(self, cell: str, download: Callable[[], Path]) -> None:
        try:
            self.downloaded.append(download())
        except ValueError as unoffered:
            self.skipped.append((cell, str(unoffered)))
        except requests.RequestException as error:
            # a sweep is hours long; one bad response must not discard the rest
            self.failed.append((cell, error))


def download_stocks_complete(
    timeframes: list[Timeframe],
    adjustments: list[EquitiesAdjustment],
    ticker_ranges: list[str] | None = None,
    skip_existing: bool = False,
) -> BundleReport:
    """Download the complete stocks bundle: the full listed history, the delisted
    history, and the corporate actions behind both.

    For every timeframe x adjustment pair this fetches the full listed archive for
    each ticker range, the five pre-2026 delisted archives, and the 2026 delisted
    archive; splits and dividends are fetched once, being timeframe-agnostic.

    Nothing raises: every cell's outcome lands in the returned BundleReport, since
    the sweep is far too long to abandon over one bad response.

    Parameters
    ----------
    timeframes : list[Timeframe]
        Bar granularities to fetch. Note that UNADJUSTED cells are skipped for the
        timeframes where the API does not offer them (all but 1min/1day listed, all
        but 1min delisted).
    adjustments : list[EquitiesAdjustment]
        Price adjustments to fetch.
    ticker_ranges : list[str] | None
        Which first letters of the ticker to pull the listed archive for. Defaults
        to the whole alphabet, which is what makes the bundle complete.
    skip_existing : bool
        Leave already-populated request folders alone instead of re-fetching them.
        Off by default, so a bundle is always freshly built; turn it on to resume an
        interrupted sweep, accepting that anything already on disk stays as it is.
    """
    stocks = FirstRateStocks.from_data_path(skip_existing=skip_existing)
    ranges = list(ascii_uppercase) if ticker_ranges is None else ticker_ranges
    report = BundleReport()

    for timeframe in timeframes:
        for adjustment in adjustments:
            # ohlc data of currently listed stocks
            for ticker_range in ranges:
                report._record(
                    f"listed {timeframe}/{adjustment}/{ticker_range}",
                    partial(
                        stocks.download_historical_data,
                        period=Period.FULL,
                        timeframe=timeframe,
                        adjustment=adjustment,
                        ticker_range=ticker_range,
                    ),
                )

            # ohlc data of delisted stocks
            for selector in COMPLETE_DELISTED:
                report._record(
                    f"delisted {timeframe}/{adjustment}/{selector.name.lower()}",
                    partial(
                        stocks.download_delisted_historical_data,
                        selector=selector,
                        timeframe=timeframe,
                        adjustment=adjustment,
                    ),
                )

    # splits and dividends
    report._record("splits", stocks.download_splits)
    report._record("dividends", stocks.download_dividends)

    return report
