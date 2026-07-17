from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from string import ascii_uppercase

import requests

from firstrate_data.progress import ProgressReporter, TqdmProgress
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

# one named cell of the sweep, bound but not yet run
type BundleCell = tuple[str, Callable[[], Path]]


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


def _bundle_cells(
    stocks: FirstRateStocks,
    period: Period,
    timeframes: list[Timeframe],
    adjustments: list[EquitiesAdjustment],
    ticker_ranges: list[str],
) -> list[BundleCell]:
    """Every cell the sweep will fetch, named and bound but not yet run.

    Enumerated up front rather than fetched from inside the loops, because a
    cell-level ETA needs the sweep's total before the first request goes out and
    a loop that discovers its own length as it goes cannot supply one. Splitting
    "what to fetch" from "fetch it" is also what makes the plan assertable
    without a network.
    """
    cells: list[BundleCell] = []

    for timeframe in timeframes:
        for adjustment in adjustments:
            # ohlc data of currently listed stocks
            for ticker_range in ticker_ranges:
                cells.append(
                    (
                        f"listed {timeframe}/{adjustment}/{ticker_range}",
                        partial(
                            stocks.download_historical_bars,
                            period=period,
                            timeframe=timeframe,
                            adjustment=adjustment,
                            ticker_range=ticker_range,
                        ),
                    )
                )

            # ohlc data of delisted stocks
            for selector in COMPLETE_DELISTED:
                cells.append(
                    (
                        f"delisted {timeframe}/{adjustment}/{selector.name.lower()}",
                        partial(
                            stocks.download_delisted_bars_archive,
                            selector=selector,
                            timeframe=timeframe,
                            adjustment=adjustment,
                        ),
                    )
                )

    # splits and dividends
    cells.append(("splits", stocks.download_splits))
    cells.append(("dividends", stocks.download_dividends))

    return cells


def download_stocks_complete(
    period: Period,
    timeframes: list[Timeframe],
    adjustments: list[EquitiesAdjustment],
    ticker_ranges: list[str] | None = None,
    progress: ProgressReporter | None = None,
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
    progress : ProgressReporter | None
        Where to report the sweep's progress. Defaults to nested tqdm bars -- cells
        done out of cells total, with the archive in flight underneath -- because a
        sweep that runs for hours and says nothing is indistinguishable from a hung
        one. Pass ``NullProgress()`` for silence.
    """
    # the reporter is shared with the loader rather than kept local: that is what
    # makes the byte bar nest *under* this sweep's bar instead of fighting it for
    # the same terminal line
    reporter = TqdmProgress() if progress is None else progress
    stocks = FirstRateStocks.from_env(progress=reporter)
    ranges = list(ascii_uppercase) if ticker_ranges is None else ticker_ranges
    cells = _bundle_cells(stocks, period, timeframes, adjustments, ranges)
    report = BundleReport()

    with reporter.track("stocks bundle", len(cells), "cell") as advance:
        for cell, download in cells:
            report._record(cell, download)
            advance(1)

    return report
