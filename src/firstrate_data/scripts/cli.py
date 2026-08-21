"""``firstrate``, the one command this project installs.

The root parser holds nothing of its own. Each subcommand's module declares its
own flags through a ``build`` and does its own work through a ``run``. A
subcommand starts here or on its own, and its arguments live in one place.
"""

import argparse

from firstrate_data.scripts import benchmark, bundle


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="firstrate",
        description="Download and benchmark FirstRate Data.",
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    bundles = commands.add_parser(
        "bundle",
        help=bundle.DESCRIPTION,
        description=bundle.DESCRIPTION,
    )
    bundle.build(bundles)
    bundles.set_defaults(run=bundle.run)

    bench = commands.add_parser(
        "bench",
        help=benchmark.DESCRIPTION,
        description=benchmark.DESCRIPTION,
    )
    benchmark.build(bench)
    bench.set_defaults(run=benchmark.run)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one subcommand, parsing `argv` or ``sys.argv``.

    Returns a process exit status, so a scheduled run that half worked counts
    as a failure.

    Examples
    --------
    >>> main(["bundle", "indices", "--dry-run", "--timeframes", "1day"])
      fetch  index 1day
    <BLANKLINE>
    1 cells to fetch, 0 not offered
    0

    """
    args = _parser().parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    raise SystemExit(main())
