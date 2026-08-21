"""Sweep bundles from the command line.

Argparse and printing only: what a bundle *is* lives in
``firstrate_data.bundle``.
"""

import argparse
from pathlib import Path

from firstrate_data.domain import (
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.bundle import (
    QUEUE_HEADROOM,
    Bundle,
    BundleDownloader,
    SweepReport,
)
from firstrate_data.download.client.client import DEFAULT_MAX_WORKERS

# How many damaged payloads the summary names before it stops. A sweep with more
# damage than this has a vendor problem, not a payload problem, and the
# quarantine file holds every damaged line regardless.
_DAMAGED_SHOWN = 10

DESCRIPTION = "Download a complete bundle: one asset type's whole universe."


def _shared() -> argparse.ArgumentParser:
    """Build the parent parser holding the flags every bundle takes."""
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=(
            "archives to fetch at once (default: %(default)s). The useful value "
            "is a property of the link, not the machine -- see the README"
        ),
    )
    shared.add_argument(
        "--queue-depth",
        type=int,
        default=None,
        help=(
            "archives allowed in the spool at once "
            f"(default: max-workers + {QUEUE_HEADROOM}). Caps the disk the "
            "sweep needs on top of the store"
        ),
    )
    shared.add_argument(
        "--spool-dir",
        type=Path,
        default=None,
        help="where archives land before being filed (default: beside the store)",
    )
    shared.add_argument(
        "--period",
        type=Period,
        choices=list(Period),
        default=Period.FULL,
        help="span of each cell (default: %(default)s)",
    )
    shared.add_argument(
        "--timeframes",
        type=Timeframe,
        choices=list(Timeframe),
        nargs="+",
        default=[Timeframe.DAY_1],
        help="bar granularities to fetch (default: %(default)s)",
    )
    shared.add_argument(
        "--dry-run",
        action="store_true",
        help="print the cells the sweep would fetch, and exit without fetching",
    )
    return shared


def build(parser: argparse.ArgumentParser) -> None:
    """Add one subparser per bundle to `parser`.

    A subparser per bundle, rather than one parser with ``--asset-type``. Only a
    subparser refuses an equities-only flag outright, instead of accepting it and
    ignoring it at the index endpoint, where prices come unadjusted.
    """
    bundles = parser.add_subparsers(dest="bundle", required=True, metavar="BUNDLE")

    stocks = bundles.add_parser(
        "stocks",
        parents=[_shared()],
        help="the listed archive, the delisted history, and the corporate actions",
    )
    stocks.add_argument(
        "--adjustments",
        type=EquitiesAdjustment,
        choices=list(EquitiesAdjustment),
        nargs="+",
        default=[EquitiesAdjustment.SPLIT],
        help="price adjustments to fetch (default: %(default)s)",
    )
    stocks.add_argument(
        "--ticker-ranges",
        nargs="+",
        default=None,
        help="first letters to pull the listed archive for (default: A-Z)",
    )
    stocks.set_defaults(plan=_stocks_bundle)

    indices = bundles.add_parser(
        "indices",
        parents=[_shared()],
        help="the index history, one archive per timeframe",
    )
    indices.set_defaults(plan=_indices_bundle)

    futures = bundles.add_parser(
        "futures",
        parents=[_shared()],
        help="the continuous series, the audit file, and the contracts behind them",
    )
    futures.add_argument(
        "--adjustments",
        type=ContinuousFuturesAdjustment,
        choices=list(ContinuousFuturesAdjustment),
        nargs="+",
        default=[ContinuousFuturesAdjustment.RATIO],
        help="roll adjustments to fetch (default: %(default)s)",
    )
    futures.add_argument(
        "--contracts",
        action="store_true",
        help=(
            "also fetch the individual contracts the continuous series is built "
            "from. Both halves when period=full, the 2026+ update alone otherwise, "
            "the pre-2026 archive being frozen"
        ),
    )
    futures.set_defaults(plan=_futures_bundle)


def run(args: argparse.Namespace) -> int:
    """Sweep the bundle `args` names, and report what it left in the store.

    Returns a process exit status: non-zero if any cell failed, so a scheduled
    run reports half a bundle as a failure.
    """
    # one path: the bundle a dry run prints is the bundle a real run sweeps
    bundle = args.plan(args)

    if args.dry_run:
        _print_bundle(bundle)
        return 0

    report = BundleDownloader(
        max_workers=args.max_workers,
        spool_dir=args.spool_dir,
        queue_depth=args.queue_depth,
    ).sweep(bundle)
    _print_report(report, args.max_workers)
    return 1 if report.failed else 0


def _stocks_bundle(args: argparse.Namespace) -> Bundle:
    return Bundle.stocks(
        args.period,
        args.timeframes,
        args.adjustments,
        args.ticker_ranges,
    )


def _indices_bundle(args: argparse.Namespace) -> Bundle:
    return Bundle.indices(args.period, args.timeframes)


def _futures_bundle(args: argparse.Namespace) -> Bundle:
    return Bundle.futures(
        args.period,
        args.timeframes,
        args.adjustments,
        contracts=args.contracts,
    )


def _print_bundle(bundle: Bundle) -> None:
    for cell in bundle.cells:
        print(f"  fetch  {cell.name}")
    for name, reason in bundle.unoffered:
        print(f"  skip   {name}: {reason}")
    print(f"\n{len(bundle.cells)} cells to fetch, {len(bundle.unoffered)} not offered")


def _print_report(report: SweepReport, max_workers: int) -> None:
    print(
        f"\n{len(report.ingested)} cells ingested, "
        f"{len(report.skipped)} skipped, {len(report.failed)} failed",
    )
    print(
        f"{report.downloaded / (1 << 30):.1f} GiB in {report.seconds / 60:.1f} min "
        f"({report.megabytes_per_second:.1f} MB/s end to end, "
        f"{max_workers} workers)",
    )
    _print_quarantine(report)
    _print_suspect(report)
    for name, error in report.failed:
        print(f"  failed: {name}: {error}")


def _plural(count: int, noun: str) -> str:
    """``count`` and `noun`, agreeing. Every count here can legitimately be 1."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _print_quarantine(report: SweepReport) -> None:
    """Print the lines this sweep failed to parse, and where they went."""
    if not report.rejected:
        return

    print(f"\n{_plural(report.rejected, 'line')} could not be parsed. Written to")
    for path in report.quarantine_files:
        print(f"  {path}")

    print()
    for payload, lines in report.damaged[:_DAMAGED_SHOWN]:
        print(f"  {payload}  {lines}")
    hidden = len(report.damaged) - _DAMAGED_SHOWN
    if hidden > 0:
        print(f"  ... and {_plural(hidden, 'more payload')}")

    print(
        "\nRe-running will not recover them: the damage is in the vendor's file."
        f"\nThe quarantine now holds {_plural(report.quarantined_in_all, 'line')}"
        " -- read it with store.quarantined().",
    )


def _print_suspect(report: SweepReport) -> None:
    """Print the rows that parsed but fail to hold together as bars."""
    if not report.suspect:
        return

    print(
        f"\nFound {_plural(report.suspect, 'suspect row')}: a high below a low, or a "
        "negative volume."
        "\nThey are in the store. Read them with store.suspect_bars().",
    )
