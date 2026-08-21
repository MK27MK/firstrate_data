"""The sweep is hours long, so what it does when a cell goes wrong matters more
than what it does when one goes right.

Nothing here reaches the network for its plan, and nothing here builds a loader
to get one: a ``Bundle`` is the domain enums crossed against each other and
handed to the request constructors, which refuse the combinations the vendor
does not serve. That is what lets a dry run say exactly which cells will be
skipped before committing an evening to the ones that won't.

The sweep itself is written once and every bundle goes through it, so it is
exercised through the stocks bundle -- the largest of the three -- and the
indices and futures bundles are tested for what makes them different: their
plans.
"""

from collections.abc import Generator
from pathlib import Path
from threading import Thread

import pytest

from firstrate_data.domain import (
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.bundle import (
    COMPLETE_DELISTED,
    Bundle,
    BundleDownloader,
    SweepReport,
    _client_class,
)
from firstrate_data.download.client.futures import FuturesClient
from firstrate_data.download.client.index import IndexClient
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.download.progress import NullProgress
from firstrate_data.store.store import Store
from tests.conftest import bars_archive
from tests.vendor import Vendor, serving

ARCHIVE = bars_archive("AAPL", "AMZN")


@pytest.fixture
def vendor() -> Generator[Vendor]:
    yield from serving(Vendor(payload=ARCHIVE))


@pytest.fixture
def stocks(vendor: Vendor, store: Store) -> StockClient:
    return StockClient("test-user", store, vendor.url, max_workers=3)


@pytest.fixture
def indices(vendor: Vendor, store: Store) -> IndexClient:
    return IndexClient("test-user", store, vendor.url, max_workers=3)


@pytest.fixture
def downloader() -> BundleDownloader:
    return BundleDownloader(NullProgress(), max_workers=3, queue_depth=5)


class TestThePlanIsKnownBeforeAnythingIsAsked:
    def test_every_cell_of_a_small_stocks_bundle_is_enumerated(
        self,
        vendor: Vendor,
    ) -> None:
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A", "B"],
        )

        # two listed ranges, the delisted archives, then splits and dividends
        assert len(bundle.cells) == 2 + len(COMPLETE_DELISTED) + 2
        assert bundle.unoffered == []
        assert vendor.asked == [], "planning is not supposed to open a socket"

    def test_a_stocks_bundle_covers_the_whole_alphabet_by_default(self) -> None:
        """The ticker ranges are what make the bundle complete."""
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
        )

        assert (
            len([cell for cell in bundle.cells if cell.name.startswith("listed ")])
            == 26
        )

    def test_a_combination_the_vendor_does_not_offer_is_skipped_not_failed(
        self,
    ) -> None:
        """UNADJUSTED is not served above 1min/1day, so re-running never helps."""
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.MIN_5],
            [EquitiesAdjustment.UNADJUSTED],
            ["A"],
        )

        assert [name for name, _ in bundle.unoffered] == [
            "listed 5min/UNADJUSTED/A",
            *(
                f"delisted 5min/UNADJUSTED/{selector.name.lower()}"
                for selector in COMPLETE_DELISTED
            ),
        ]
        # the metafiles have no timeframe to be unoffered at
        assert [cell.name for cell in bundle.cells] == ["splits", "dividends"]


class TestAnIndexBundleIsOneCellPerTimeframe:
    """The index endpoint takes type, period and timeframe.

    There is no adjustment to cross the timeframes against, no ticker range to
    partition them by, no delisted dataset, and no splits or dividends to
    explain an adjustment that does not exist.
    """

    def test_one_cell_per_timeframe_and_nothing_else(self, vendor: Vendor) -> None:
        bundle = Bundle.indices(
            Period.FULL,
            [Timeframe.MIN_1, Timeframe.HOUR_1, Timeframe.DAY_1],
        )

        assert [cell.name for cell in bundle.cells] == [
            "index 1min",
            "index 1hour",
            "index 1day",
        ]
        assert bundle.unoffered == []
        assert vendor.asked == [], "planning is not supposed to open a socket"

    def test_no_cell_carries_an_adjustment_or_a_ticker_range(self) -> None:
        """Sending a parameter the endpoint does not document invites a body we
        cannot predict.
        """
        bundle = Bundle.indices(Period.FULL, [Timeframe.DAY_1])

        (asked,) = [cell.request.to_params() for cell in bundle.cells]
        assert asked == {"type": "index", "period": "full", "timeframe": "1day"}

    def test_the_bundle_does_not_grow_with_the_alphabet(self) -> None:
        """A stocks bundle is 26 listed cells per timeframe; this one is one."""
        bundle = Bundle.indices(Period.FULL, [Timeframe.DAY_1])

        assert len(bundle.cells) == 1


class TestAFuturesBundleIsTheSeriesAndWhatItWasBuiltFrom:
    """The continuous series is a construction.

    The audit file says which contracts went into it, and the contracts
    themselves are a dataset of their own -- large enough that a run has to
    ask for them.
    """

    def test_the_series_and_the_audit_file_are_the_default(
        self,
        vendor: Vendor,
    ) -> None:
        bundle = Bundle.futures(
            Period.FULL,
            [Timeframe.DAY_1],
            [ContinuousFuturesAdjustment.RATIO],
        )

        assert [cell.name for cell in bundle.cells] == [
            "continuous 1day/contin_adj_ratio",
            "contin_audit",
        ]
        assert vendor.asked == [], "planning is not supposed to open a socket"

    def test_the_contracts_are_asked_for_and_not_assumed(self) -> None:
        bundle = Bundle.futures(
            Period.FULL,
            [Timeframe.DAY_1],
            [ContinuousFuturesAdjustment.RATIO],
            contracts=True,
        )

        assert [cell.name for cell in bundle.cells] == [
            "continuous 1day/contin_adj_ratio",
            "contracts 1day/archive",
            "contracts 1day/update",
            "contin_audit",
        ]

    def test_a_short_period_leaves_the_frozen_archive_alone(self) -> None:
        """The pre-2026 contracts stopped trading.

        Re-fetching them on a daily run costs hours and brings back the same
        bars.
        """
        bundle = Bundle.futures(
            Period.DAY,
            [Timeframe.DAY_1],
            [ContinuousFuturesAdjustment.UNADJUSTED],
            contracts=True,
        )

        assert [cell.name for cell in bundle.cells] == [
            "continuous 1day/contin_UNadj",
            "contracts 1day/update",
            "contin_audit",
        ]

    def test_a_restated_series_is_skipped_rather_than_spliced(self) -> None:
        """A ratio-adjusted series is rewritten backwards by every roll, so an
        increment of it cannot be appended to what the store holds.
        """
        bundle = Bundle.futures(
            Period.WEEK,
            [Timeframe.DAY_1],
            [ContinuousFuturesAdjustment.RATIO],
        )

        assert [cell.name for cell in bundle.cells] == ["contin_audit"]
        name, reason = bundle.unoffered[0]
        assert name == "continuous 1day/contin_adj_ratio"
        assert "restated" in reason

    def test_the_contract_cells_carry_no_adjustment(self) -> None:
        """A real contract has no roll to correct for."""
        bundle = Bundle.futures(
            Period.FULL,
            [Timeframe.MIN_1],
            [ContinuousFuturesAdjustment.RATIO],
            contracts=True,
        )
        contracts = [cell for cell in bundle.cells if cell.name.startswith("contracts")]

        for cell in contracts:
            assert "adjustment" not in cell.request.to_params()


class TestTheSweepFindsTheClientAPlanDoesNotCarry:
    """A bundle is a plan, so it names no client class.

    The cells it plans name one asset type, and the sweep builds the client
    that serves it.
    """

    def test_each_bundle_kind_names_the_client_that_serves_it(self) -> None:
        stocks = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )
        indices = Bundle.indices(Period.FULL, [Timeframe.DAY_1])
        # three request types, one asset type: the contracts and the audit file
        # are served by the same client as the series
        futures = Bundle.futures(
            Period.FULL,
            [Timeframe.DAY_1],
            [ContinuousFuturesAdjustment.RATIO],
            contracts=True,
        )

        assert _client_class(stocks) is StockClient
        assert _client_class(indices) is IndexClient
        assert _client_class(futures) is FuturesClient

    def test_a_sweep_given_no_client_builds_one_from_the_environment(
        self,
        vendor: Vendor,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """The one path a scheduled run takes: a bundle and nothing else."""
        monkeypatch.setenv("FIRSTRATE_USERID", "test-user")
        monkeypatch.setenv("FIRSTRATE_DATA_PATH", str(tmp_path / "data"))
        monkeypatch.setenv("FIRSTRATE_BASE_URL", vendor.url)
        bundle = Bundle.indices(Period.FULL, [Timeframe.DAY_1])

        report = BundleDownloader(NullProgress(), max_workers=2).sweep(bundle)

        assert [name for name, _ in report.ingested] == ["index 1day"]
        assert report.failed == []


class TestASweepFinishesWhateverOneCellDoes:
    def test_every_cell_lands_and_the_spool_is_emptied(
        self,
        downloader: BundleDownloader,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )

        report = downloader.sweep(bundle, client=stocks)

        assert len(report.ingested) == len(bundle.cells)
        assert report.failed == []
        # the bars cells, plus splits and dividends, which the endpoint answers
        # with a bare CSV rather than an archive
        bars_cells = len(bundle.cells) - 2
        assert report.downloaded == bars_cells * len(ARCHIVE) + 2 * len(vendor.metafile)
        assert list(stocks.spool.iterdir()) == []

    def test_a_cell_the_vendor_will_not_serve_does_not_sink_the_rest(
        self,
        downloader: BundleDownloader,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        """One bad response must not discard the hours the others cost."""
        vendor.refuse = {"metafile_type": "splits"}
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )

        report = downloader.sweep(bundle, client=stocks)

        assert [name for name, _ in report.failed] == ["splits"]
        assert len(report.ingested) == len(bundle.cells) - 1

    def test_a_cell_that_fails_for_a_non_http_reason_is_still_only_one_cell(
        self,
        downloader: BundleDownloader,
        stocks: StockClient,
    ) -> None:
        """A full spool disk raises OSError, not a RequestException.

        Either way it is one failed cell, not a reason to return no report at
        all.
        """
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )
        doomed = bundle.cells[0].name
        original = stocks.fetch

        def fetch(request: object, name: str | None = None) -> object:
            if request is bundle.cells[0].request:
                raise OSError(28, "No space left on device")
            return original(request, name)  # type: ignore[arg-type]

        stocks.fetch = fetch  # type: ignore[method-assign,assignment]

        report = downloader.sweep(bundle, client=stocks)

        assert [name for name, _ in report.failed] == [doomed]
        assert isinstance(report.failed[0][1], OSError)
        assert len(report.ingested) == len(bundle.cells) - 1

    def test_the_report_says_how_fast_the_sweep_ran(
        self,
        downloader: BundleDownloader,
        stocks: StockClient,
    ) -> None:
        """A sweep that cannot say its own throughput cannot be tuned."""
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )

        report = downloader.sweep(bundle, client=stocks)

        assert report.seconds > 0
        assert report.megabytes_per_second > 0
        assert report.rejected == 0

    def test_an_index_bundle_goes_through_the_same_sweep(
        self,
        downloader: BundleDownloader,
        indices: IndexClient,
    ) -> None:
        bundle = Bundle.indices(Period.FULL, [Timeframe.MIN_1, Timeframe.DAY_1])

        report = downloader.sweep(bundle, client=indices)

        # a set: cells are filed in the order they land, not the order they were
        # planned in
        assert {name for name, _ in report.ingested} == {"index 1min", "index 1day"}
        assert report.failed == []
        assert list(indices.spool.iterdir()) == []


class TestTheSweepFetchesInParallelAndSpoolsWithinItsBudget:
    def test_several_archives_are_in_flight_at_once(
        self,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        """A sweep that fetched one at a time would take a third of the link."""
        vendor.dwell = 0.05
        downloader = BundleDownloader(NullProgress(), max_workers=3, queue_depth=5)
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )

        downloader.sweep(bundle, client=stocks)

        assert vendor.peak_in_flight > 1

    def test_the_spool_never_holds_more_archives_than_the_queue_allows(
        self,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        """The bound on the sweep's disk high-water mark is enforced, not hoped for.

        A worker holds a spool slot from its first byte until its archive has
        been filed and deleted, so a fetch in flight is a slot taken.
        """
        vendor.dwell = 0.05
        downloader = BundleDownloader(NullProgress(), max_workers=3, queue_depth=1)
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )

        downloader.sweep(bundle, client=stocks)

        assert vendor.peak_in_flight == 1

    def test_a_run_of_failures_does_not_starve_the_workers_that_could_succeed(
        self,
        stocks: StockClient,
        vendor: Vendor,
    ) -> None:
        """A failed fetch spools nothing, so it owes its slot back.

        Every bars cell is refused here and one slot is on offer, so a sweep
        that kept a slot on failure would hold nothing but dead slots and never
        reach the two metafiles behind them.
        """
        vendor.refuse = {"adjustment": "adj_split"}
        downloader = BundleDownloader(NullProgress(), max_workers=3, queue_depth=1)
        bundle = Bundle.stocks(
            Period.FULL,
            [Timeframe.DAY_1],
            [EquitiesAdjustment.SPLIT],
            ["A"],
        )
        swept: list[SweepReport] = []

        # off the calling thread with a deadline, so a starved sweep is a failure
        # after 60 seconds rather than a suite that never finishes
        sweeping = Thread(
            target=lambda: swept.append(downloader.sweep(bundle, client=stocks)),
            daemon=True,
        )
        sweeping.start()
        sweeping.join(timeout=60)

        assert swept, "the sweep is starved: a failed fetch kept its spool slot"
        assert {name for name, _ in swept[0].ingested} == {"splits", "dividends"}
        assert len(swept[0].failed) == len(bundle.cells) - 2
