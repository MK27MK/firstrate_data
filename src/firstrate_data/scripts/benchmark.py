"""Measure what the downloader actually achieves, rather than asserting it.

The useful worker count is a property of the link, not of this code. A single
TCP stream caps out at ``W_tcp / RTT``, so a long fat pipe needs more than one
stream to fill and a short one needs none. That number can't be hard-coded, so
this sweeps it and reports where the curve flattens.

Two targets, because they answer different questions. ``local`` serves synthetic
archives from this machine: free, repeatable, and it measures the client and the
spool disk. ``vendor`` fetches real archives over the real link, which is the
only way to see the plateau that matters -- and it costs real bandwidth.
"""

import argparse
import os
import statistics
import threading
import time
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from string import ascii_uppercase
from tempfile import TemporaryDirectory
from typing import IO

from firstrate_data.domain import (
    AssetType,
    BarType,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.bundle import NamedRequest
from firstrate_data.download.client.fetcher import ArchiveFetcher, Fetched
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.download.progress import NullProgress
from firstrate_data.download.requests import BarsRequest, IngestibleRequest

MIB = 1 << 20

# One page of repeating filler, written over and over to make a body of any
# size. Generating rather than holding the body is the point: a harness that
# needed 2 GiB of RAM per synthetic archive couldn't measure eight at once.
_BLOCK = bytes(range(256)) * (MIB // 256)

_BINDING_SHARE = 0.9


@dataclass(frozen=True, slots=True)
class Measurement:
    """One worker count, measured."""

    workers: int
    archives: int
    downloaded: int
    seconds: float
    # summed across workers, so these exceed `seconds` when more than one ran at once
    reading: float
    writing: float

    @property
    def megabytes_per_second(self) -> float:
        return self.downloaded / self.seconds / MIB if self.seconds else 0.0

    @property
    def per_stream(self) -> float:
        return self.megabytes_per_second / self.workers if self.workers else 0.0

    @property
    def socket_share(self) -> float:
        """Fraction of worker time blocked on the network rather than the disk.

        Near 1 means the link is the constraint and more workers may still help.
        Materially below it means the workers are waiting on the spool, and
        adding more of them won't.
        """
        busy = self.reading + self.writing
        return self.reading / busy if busy else 0.0


class _SharedRate:
    """A ceiling on what every connection gets between them.

    Loopback has no link to run out of, so without this the total throughput
    rises forever with the worker count. The sweep then reports a plateau
    that's just the last setting tried. A real link runs out.
    """

    def __init__(self, bytes_per_second: float) -> None:
        self._rate = bytes_per_second
        self._lock = threading.Lock()
        self._free_at = time.monotonic()

    def take(self, size: int) -> None:
        if not self._rate:
            return
        with self._lock:
            now = time.monotonic()
            starts = max(now, self._free_at)
            self._free_at = starts + size / self._rate
            waiting = starts - now
        # slept outside the lock, or the queue would serialise on the sleeping
        if waiting > 0:
            time.sleep(waiting)


def _serve_at_rate(
    wfile: IO[bytes],
    size: int,
    per_second: float,
    link: _SharedRate,
) -> None:
    started = time.monotonic()
    sent = 0
    while sent < size:
        block = _BLOCK[: min(len(_BLOCK), size - sent)]
        link.take(len(block))
        wfile.write(block)
        sent += len(block)
        if per_second:
            owed = sent / per_second - (time.monotonic() - started)
            if owed > 0:
                time.sleep(owed)


@contextmanager
def _local_vendor(size: int, rate_mbps: float, link_mbps: float) -> Generator[str]:
    """Serve `size`-byte bodies under two ceilings.

    Reproducing the shape of the real problem needs both. A per-connection cap
    stands in for the bandwidth-delay product, which is what makes a second
    worker help at all. A shared cap stands in for the link, which is what
    makes an eighth worker not.
    """
    per_second = rate_mbps * MIB if rate_mbps else 0.0
    link = _SharedRate(link_mbps * MIB if link_mbps else 0.0)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.end_headers()
            _serve_at_rate(self.wfile, size, per_second, link)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            """Quiet: a benchmark isn't a request log.

            `format` overrides `http.server.BaseHTTPRequestHandler.log_message`.
            The standard library fixes that parameter name.
            """

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{http.server_port}"
    finally:
        http.shutdown()
        http.server_close()


def disk_write_throughput(spool: Path, size: int) -> float:
    """MB/s writing `size` bytes to `spool` and forcing them to the platter.

    This is the number every download rate compares with: a sweep can't go
    faster than the disk it spools to. On an external volume, that's
    routinely the lower of the two.
    """
    spool.mkdir(parents=True, exist_ok=True)
    probe = spool / ".write-probe"
    started = time.monotonic()
    try:
        with probe.open("wb") as writing:
            written = 0
            while written < size:
                block = _BLOCK[: min(len(_BLOCK), size - written)]
                writing.write(block)
                written += len(block)
            writing.flush()
            os.fsync(writing.fileno())
        return size / (time.monotonic() - started) / MIB
    finally:
        probe.unlink(missing_ok=True)


def _sweep_workers(
    fetch: Callable[[NamedRequest], Fetched],
    jobs: list[NamedRequest],
    workers: int,
) -> Measurement:
    """Fetch every job with `workers` in flight, and time the whole thing."""
    started = time.monotonic()
    with ThreadPoolExecutor(workers) as pool:
        fetched: list[Fetched] = list(pool.map(fetch, jobs))
    seconds = time.monotonic() - started

    for done in fetched:
        done.path.unlink(missing_ok=True)

    return Measurement(
        workers=workers,
        archives=len(fetched),
        downloaded=sum(done.size for done in fetched),
        seconds=seconds,
        reading=sum(done.reading for done in fetched),
        writing=sum(done.writing for done in fetched),
    )


def _jobs(requests: Iterable[IngestibleRequest]) -> list[NamedRequest]:
    """Pair requests with distinct spool names.

    Named per job rather than per request because the sweep fetches the same
    archives again at every worker count. A spool name derived from the
    request would have each round resuming the last one's leftovers.
    """
    return [NamedRequest(f"bench-{n}", request) for n, request in enumerate(requests)]


def _local_requests(count: int) -> list[IngestibleRequest]:
    """Build requests the synthetic endpoint will answer.

    The endpoint ignores the parameters. They exist so the transport has a
    real request to build an address from.
    """
    return [
        BarsRequest(
            BarType(
                AssetType.STOCK,
                timeframe=Timeframe.DAY_1,
                adjustment=EquitiesAdjustment.SPLIT,
            ),
            Period.FULL,
            ticker_range=ascii_uppercase[n % 26],
        )
        for n in range(count)
    ]


def plateau(measurements: Iterable[Measurement], tolerance: float = 0.05) -> int:
    """Find the fewest workers that reach within `tolerance` of the best rate seen.

    Reported rather than "the fastest setting", because the fastest is often
    1-2% higher than a much cheaper setting. A worker that buys 1% still
    costs a connection, a spool slot, and a share of the disk.
    """
    seen = list(measurements)
    if not seen:
        return 0
    best = max(m.megabytes_per_second for m in seen)
    return min(
        (m.workers for m in seen if m.megabytes_per_second >= best * (1 - tolerance)),
        default=0,
    )


def _report(measurements: list[Measurement], disk: float, spool: Path) -> None:
    print()
    print(f"{'workers':>7}  {'aggregate':>12}  {'per stream':>11}  {'on socket':>10}")
    for measured in measurements:
        print(
            f"{measured.workers:>7}  "
            f"{measured.megabytes_per_second:>8.1f} MB/s  "
            f"{measured.per_stream:>6.1f} MB/s  "
            f"{measured.socket_share:>9.0%}",
        )

    print(f"\nspool disk ({spool}), sequential + fsync: {disk:.1f} MB/s")

    best = max(measurements, key=lambda m: m.megabytes_per_second)
    print(
        f"plateau at {plateau(measurements)} workers "
        f"({best.megabytes_per_second:.1f} MB/s peak, at {best.workers})",
    )

    # the question the numbers exist to answer
    share = statistics.mean(m.socket_share for m in measurements)
    if best.megabytes_per_second >= disk * _BINDING_SHARE:
        binding = "the spool disk -- the fetches are at its sequential write rate"
    elif share >= _BINDING_SHARE:
        binding = "the network -- workers spend their time waiting on the socket"
    else:
        binding = (
            f"mixed: {share:.0%} of worker time is on the socket, the rest on disk"
        )
    print(f"binding constraint: {binding}")
    print(
        "\nThese are fetch rates: archives pulled and verified, nothing filed.\n"
        "For the end-to-end rate, which is min(wire, ingest), run a small\n"
        "`firstrate bundle stocks` -- its report carries megabytes_per_second.",
    )


DESCRIPTION = "Sweep the download worker count and report where it plateaus."


def build(parser: argparse.ArgumentParser) -> None:
    """Add the benchmark's flags to `parser`, whoever owns it.

    The root command mounts this on its ``bench`` subparser, and ``main`` below
    mounts it on a parser of its own. Either way, this declares the flags once.
    """
    parser.add_argument(
        "--target",
        choices=["local", "vendor"],
        default="local",
        help=(
            "'local' serves synthetic archives from this machine (free); "
            "'vendor' fetches real ones over the real link (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--workers",
        default="1,2,4,6,8",
        help="worker counts to sweep, comma separated (default: %(default)s)",
    )
    parser.add_argument(
        "--size-mib",
        type=int,
        default=256,
        help="size of each synthetic archive, local target only (default: %(default)s)",
    )
    parser.add_argument(
        "--archives",
        type=int,
        default=8,
        help="archives fetched per worker setting (default: %(default)s)",
    )
    parser.add_argument(
        "--rate-mbps",
        type=float,
        default=36.0,
        help=(
            "per-connection ceiling for the local target, in MB/s, standing in "
            "for the bandwidth-delay product of a real link (default: %(default)s, "
            "the measured single-stream rate from Italy to the US). 0 disables it"
        ),
    )
    parser.add_argument(
        "--link-mbps",
        type=float,
        default=95.0,
        help=(
            "shared ceiling across all connections for the local target, in "
            "MB/s, standing in for the link itself (default: %(default)s, the "
            "measured 800 Mbps line). 0 disables it"
        ),
    )
    parser.add_argument(
        "--ticker-ranges",
        nargs="+",
        default=None,
        help="which listed archives to fetch, vendor target only (default: A-H)",
    )
    parser.add_argument(
        "--spool-dir",
        type=Path,
        default=None,
        help="where archives land (default: beside the store, or a temp dir)",
    )


def run(args: argparse.Namespace) -> int:
    """Run the worker sweep and print the table."""
    counts = [int(n) for n in args.workers.split(",")]

    if args.target == "vendor":
        return _bench_vendor(args, counts)
    return _bench_local(args, counts)


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark on its own, without the ``firstrate`` command around it."""
    parser = argparse.ArgumentParser(prog="firstrate bench", description=DESCRIPTION)
    build(parser)
    return run(parser.parse_args(argv))


def _bench_local(args: argparse.Namespace, counts: list[int]) -> int:
    """Sweep the synthetic endpoint on this machine.

    Measures the client and the spool disk. It can't measure the link, which is
    why the per-connection cap exists and why the vendor target also exists.
    """
    size = args.size_mib * MIB
    with TemporaryDirectory() as scratch:
        spool = (args.spool_dir or Path(scratch)) / "bench-spool"
        disk = disk_write_throughput(spool, size)
        jobs = _jobs(_local_requests(args.archives))
        measurements = []

        with _local_vendor(size, args.rate_mbps, args.link_mbps) as url:
            for workers in counts:
                fetcher = ArchiveFetcher(
                    url,
                    "bench",
                    spool,
                    max_workers=workers,
                    progress=NullProgress(),
                )
                try:
                    measurements.append(
                        _sweep_workers(partial(_fetch_job, fetcher), jobs, workers),
                    )
                finally:
                    fetcher.close()

    _report(measurements, disk, spool)
    return 0


def _bench_vendor(args: argparse.Namespace, counts: list[int]) -> int:
    """Sweep the real endpoint. This costs bandwidth and vendor quota."""
    ranges = args.ticker_ranges or list("ABCDEFGH")
    measurements = []
    spool = args.spool_dir

    for workers in counts:
        stocks = StockClient.from_env(
            progress=NullProgress(),
            max_workers=workers,
            spool_dir=spool,
        )
        spool = stocks.spool
        jobs = _jobs(
            BarsRequest(
                BarType(
                    AssetType.STOCK,
                    timeframe=Timeframe.DAY_1,
                    adjustment=EquitiesAdjustment.SPLIT,
                ),
                Period.FULL,
                ticker_range=letter,
            )
            for letter in ranges
        )
        try:
            measurements.append(
                _sweep_workers(partial(_fetch_job, stocks), jobs, workers),
            )
        finally:
            stocks.close()

    # invariant: the loop runs at least once, and every StockClient it builds
    # assigns `spool` before this point runs
    assert spool is not None, "a loader always has a spool"  # noqa: S101
    _report(measurements, disk_write_throughput(spool, 256 * MIB), spool)
    return 0


def _fetch_job(fetcher: ArchiveFetcher | StockClient, job: NamedRequest) -> Fetched:
    """One job, through whichever of the two things can fetch it."""
    return fetcher.fetch(job.request, name=job.name)


if __name__ == "__main__":
    raise SystemExit(main())
