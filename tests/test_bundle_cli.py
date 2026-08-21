"""The command line, which is the only way most runs of this are started.

A dry run is the whole reason the plan is built by validating requests rather
than by sending them: it must say exactly what an evening's sweep would fetch,
and what the vendor would refuse, without opening a socket.

Nothing here sets a user id or a store path. A bundle is built from the domain
enums alone, so listing one needs no credentials, no DuckDB connection and no
spool directory -- if a dry run ever needs those again, a loader has been
dragged back into the planning.
"""

from pathlib import Path

import pytest

from firstrate_data.download.bundle import SweepReport
from firstrate_data.scripts.bundle import _print_report
from firstrate_data.scripts.cli import _parser, main
from firstrate_data.store.store import Ingested


class TestADryRunListsTheBundleWithoutFetchingIt:
    def test_the_stocks_bundle_is_listed_cell_by_cell(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        status = main(
            [
                "bundle",
                "stocks",
                "--dry-run",
                "--timeframes",
                "1day",
                "--adjustments",
                "adj_split",
                "--ticker-ranges",
                "A",
                "B",
            ],
        )

        printed = capsys.readouterr().out
        assert status == 0
        assert "fetch  listed 1day/adj_split/A" in printed
        assert "fetch  delisted 1day/adj_split/archive_1" in printed
        assert "fetch  splits" in printed
        # two listed ranges, five delisted archives, the 2026 update, and the
        # two metafiles
        assert "10 cells to fetch, 0 not offered" in printed

    def test_the_indices_bundle_is_listed_cell_by_cell(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        status = main(
            ["bundle", "indices", "--dry-run", "--timeframes", "1min", "1day"],
        )

        printed = capsys.readouterr().out
        assert status == 0
        assert "fetch  index 1min" in printed
        assert "fetch  index 1day" in printed
        assert "2 cells to fetch, 0 not offered" in printed

    def test_the_futures_bundle_is_the_series_and_the_audit_file(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        status = main(["bundle", "futures", "--dry-run", "--timeframes", "1day"])

        printed = capsys.readouterr().out
        assert status == 0
        assert "fetch  continuous 1day/contin_adj_ratio" in printed
        assert "fetch  contin_audit" in printed
        assert "2 cells to fetch, 0 not offered" in printed

    def test_the_contracts_are_asked_for_with_a_flag(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        status = main(
            ["bundle", "futures", "--dry-run", "--timeframes", "1day", "--contracts"],
        )

        printed = capsys.readouterr().out
        assert status == 0
        assert "fetch  contracts 1day/archive" in printed
        assert "fetch  contracts 1day/update" in printed
        assert "4 cells to fetch, 0 not offered" in printed

    def test_a_short_period_fetches_the_contract_update_alone(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The pre-2026 archive is frozen, and re-pulling it costs hours."""
        status = main(
            [
                "bundle",
                "futures",
                "--dry-run",
                "--timeframes",
                "1day",
                "--period",
                "day",
                "--adjustments",
                "contin_UNadj",
                "--contracts",
            ],
        )

        printed = capsys.readouterr().out
        assert status == 0
        assert "fetch  contracts 1day/update" in printed
        assert "archive" not in printed

    def test_a_cell_the_vendor_will_not_offer_is_named_with_its_reason(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """What the dry run exists to say before an evening is committed to it."""
        status = main(
            [
                "bundle",
                "stocks",
                "--dry-run",
                "--timeframes",
                "5min",
                "--adjustments",
                "UNADJUSTED",
                "--ticker-ranges",
                "A",
            ],
        )

        printed = capsys.readouterr().out
        assert status == 0
        assert "skip   listed 5min/UNADJUSTED/A: UNADJUSTED data is only" in printed
        # only the metafiles survive: they have no timeframe to be unoffered at
        assert "2 cells to fetch, 7 not offered" in printed


class TestADryRunDescribesTheRunThatWouldHappen:
    """The dry run and the sweep must build the same bundle from the same arguments,
    or the run described and the run that happens can drift.
    """

    def test_both_paths_build_the_same_bundle(self) -> None:
        parser = _parser()
        dry = parser.parse_args(
            ["bundle", "indices", "--dry-run", "--timeframes", "1day"],
        )
        swept = parser.parse_args(["bundle", "indices", "--timeframes", "1day"])

        assert dry.plan is swept.plan
        assert dry.plan(dry).cells == swept.plan(swept).cells

    def test_the_indices_bundle_refuses_an_adjustment(self) -> None:
        """The index endpoint documents none, so accepting one and ignoring it
        would be a promise the sweep cannot keep.

        """
        with pytest.raises(SystemExit):
            main(["bundle", "indices", "--adjustments", "adj_split"])

    def test_the_indices_bundle_refuses_a_ticker_range(self) -> None:
        with pytest.raises(SystemExit):
            main(["bundle", "indices", "--ticker-ranges", "A"])

    def test_the_futures_bundle_refuses_a_ticker_range(self) -> None:
        """Futures have none: the full archive is served whole."""
        with pytest.raises(SystemExit):
            main(["bundle", "futures", "--ticker-ranges", "A"])

    def test_the_futures_bundle_refuses_an_equities_adjustment(self) -> None:
        """`adj_split` means nothing to a roll, and a subparser says so rather
        than sending it and reading whatever comes back.

        """
        with pytest.raises(SystemExit):
            main(["bundle", "futures", "--adjustments", "adj_split"])

    def test_a_bundle_must_be_named(self) -> None:
        with pytest.raises(SystemExit):
            main(["bundle"])

    def test_a_command_must_be_named(self) -> None:
        with pytest.raises(SystemExit):
            main([])


class TestTheReportTellsYouWhatToDoAboutTheDamage:
    """The counts alone are a dead end: nobody re-derives which ticker lost a
    bar, or how to look at it, from a number on a terminal.

    """

    def test_a_clean_sweep_says_nothing_about_damage(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _print_report(_report(), max_workers=4)

        printed = capsys.readouterr().out
        assert "quarantine" not in printed
        assert "suspect" not in printed

    def test_it_names_the_payloads_that_lost_lines(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _print_report(_report(damaged=(("ABC.txt", 3), ("XYZ.txt", 2))), max_workers=4)

        printed = capsys.readouterr().out
        assert "5 lines could not be parsed" in printed
        assert "ABC.txt  3" in printed
        assert "XYZ.txt  2" in printed

    def test_it_names_the_file_the_lines_went_to(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        report = _report(
            damaged=(("ABC.txt", 1),),
            quarantine=Path("/store/quarantine/x.parquet"),
        )

        _print_report(report, max_workers=4)

        assert "/store/quarantine/x.parquet" in capsys.readouterr().out

    def test_it_says_a_re_run_will_not_recover_them(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """The one thing a reader will otherwise try, and it costs an hour."""
        _print_report(_report(damaged=(("ABC.txt", 1),)), max_workers=4)

        assert "Re-running will not recover them" in capsys.readouterr().out

    def test_a_long_list_of_payloads_is_capped(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A sweep that damaged 400 payloads must not bury the advice under
        them -- the file holds every one regardless.

        """
        damaged = tuple((f"T{index:03d}.txt", 1) for index in range(400))

        _print_report(_report(damaged=damaged), max_workers=4)

        printed = capsys.readouterr().out
        assert "... and 390 more payloads" in printed
        assert "Re-running will not recover them" in printed

    def test_it_reports_the_whole_quarantine_not_just_this_sweep(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        report = _report(damaged=(("ABC.txt", 1),))
        report.quarantined_in_all = 41

        _print_report(report, max_workers=4)

        assert "quarantine now holds 41 lines" in capsys.readouterr().out

    def test_suspect_rows_get_their_own_advice(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A different kind of damage from a rejected line: these are *in* the
        store, so the reader is pointed at the store, not the quarantine.

        """
        _print_report(_report(suspect=2), max_workers=4)

        printed = capsys.readouterr().out
        assert "Found 2 suspect rows" in printed
        assert "store.suspect_bars()" in printed


def _report(
    damaged: tuple[tuple[str, int], ...] = (),
    suspect: int = 0,
    quarantine: Path | None = None,
) -> SweepReport:
    """A report of one cell that landed, damaged as the arguments say."""
    if damaged and quarantine is None:
        quarantine = Path("/store/quarantine/2026-08-03_9f3c.parquet")
    ingested = Ingested(1, 100, suspect, quarantine, damaged)
    return SweepReport(
        ingested=[("listed 1day/UNADJUSTED/A", ingested)],
        downloaded=1 << 20,
        seconds=1.0,
        quarantined_in_all=sum(lines for _, lines in damaged),
    )
