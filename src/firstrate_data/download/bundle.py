"""Complete bundles: one asset type's whole universe at a given timeframe.

A sweep fetches several archives at once and files them one at a time: fetching
is network-bound and parallel, filing is disk-bound and serial because the
store holds one DuckDB connection and removes bars before it writes them.
"""

import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from string import ascii_uppercase
from threading import Semaphore
from typing import Self

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    IndexAdjustment,
    MetafileType,
    Period,
    Timeframe,
)
from firstrate_data.download.client.client import DEFAULT_MAX_WORKERS, Client
from firstrate_data.download.client.fetcher import Fetched
from firstrate_data.download.client.futures import FuturesClient
from firstrate_data.download.client.index import IndexClient
from firstrate_data.download.client.stocks import StockClient
from firstrate_data.download.progress import ProgressReporter, TqdmProgress
from firstrate_data.download.requests import (
    BarsRequest,
    ContractBarsRequest,
    DelistedBarsRequest,
    IngestibleRequest,
    MetafileRequest,
    NotOfferedError,
)
from firstrate_data.store.store import Ingested

# week is a strict subset of year, so a complete pull only wants year
COMPLETE_DELISTED: list[DelistedArchive | DelistedUpdate] = [
    *DelistedArchive,
    DelistedUpdate.YEAR,
]

# spool slots over the worker count: caps the sweep's disk high-water mark at
# roughly (max_workers + this) x the size of one archive
QUEUE_HEADROOM = 2


def _contract_halves(period: Period) -> list[ContractFiles]:
    """Which halves of the individual-contract dataset a `period` pull wants.

    The archive is the contracts that stopped trading before 2026 and is frozen,
    so only a full pull has any reason to ask for it: re-fetching it on a daily
    run costs hours and brings back the same bars.

    Examples
    --------
    >>> _contract_halves(Period.FULL)
    [<ContractFiles.ARCHIVE: 'archive'>, <ContractFiles.UPDATE: 'update'>]
    >>> _contract_halves(Period.DAY)
    [<ContractFiles.UPDATE: 'update'>]

    """
    return list(ContractFiles) if period is Period.FULL else [ContractFiles.UPDATE]


@dataclass(frozen=True, slots=True)
class NamedRequest:
    """One named archive the sweep will fetch, validated but not yet asked for."""

    name: str
    request: IngestibleRequest


@dataclass(slots=True)
class _Plan:
    """A bundle's cells as they are planned, and what it cannot ask for."""

    cells: list[NamedRequest] = field(default_factory=list)
    unoffered: list[tuple[str, str]] = field(default_factory=list)

    def add(self, name: str, build: Callable[[], IngestibleRequest]) -> None:
        """Plan one cell under `name`, or record why it can never be asked for."""
        try:
            self.cells.append(NamedRequest(name, build()))
        except NotOfferedError as refused:
            # a request refuses itself on construction, which is what sorts the
            # unaskable cells out before a socket is opened rather than an hour in
            self.unoffered.append((name, str(refused)))


def _plan_continuous_futures(
    plan: _Plan,
    period: Period,
    timeframes: list[Timeframe],
    adjustments: list[ContinuousFuturesAdjustment],
) -> None:
    for timeframe in timeframes:
        for adjustment in adjustments:
            plan.add(
                f"continuous {timeframe}/{adjustment}",
                partial(
                    BarsRequest,
                    BarType(
                        AssetType.FUTURES,
                        timeframe=timeframe,
                        adjustment=adjustment,
                    ),
                    period,
                ),
            )


def _plan_futures_contracts(
    plan: _Plan,
    period: Period,
    timeframes: list[Timeframe],
) -> None:
    for timeframe in timeframes:
        for half in _contract_halves(period):
            plan.add(
                f"contracts {timeframe}/{half}",
                partial(
                    ContractBarsRequest,
                    BarType(AssetType.FUTURES, timeframe=timeframe),
                    contract_files=half,
                ),
            )


@dataclass(frozen=True, slots=True)
class Bundle:
    """One asset type's whole universe, as the cells that would fetch it.

    Built from the domain enums alone -- no client, no credentials and no
    store.

    ``label`` names it on a progress bar, ``cells`` is what the sweep will fetch
    in planning order, ``unoffered`` the (name, reason) of what it cannot.
    """

    label: str
    cells: list[NamedRequest]
    unoffered: list[tuple[str, str]]

    @classmethod
    def stocks(
        cls,
        period: Period,
        timeframes: list[Timeframe],
        adjustments: list[EquitiesAdjustment],
        ticker_ranges: list[str] | None = None,
    ) -> Self:
        """Every cell of the stocks bundle, and every cell the vendor will refuse.

        Per timeframe x adjustment: the listed archive of each ticker range (A-Z
        by default), the five pre-2026 delisted archives and the 2026 one, each
        spanning `period`. Splits and dividends are planned once, being
        timeframe-agnostic. Nothing is sent.

        Examples
        --------
        >>> bundle = Bundle.stocks(
        ...     Period.FULL, [Timeframe.MIN_5], [EquitiesAdjustment.UNADJUSTED], ["A"]
        ... )
        >>> [cell.name for cell in bundle.cells]
        ['splits', 'dividends']
        >>> name, reason = bundle.unoffered[0]
        >>> name
        'listed 5min/UNADJUSTED/A'
        >>> reason
        'UNADJUSTED data is only available in the 1min and 1day timeframes'

        """
        plan = _Plan()
        # the whole alphabet is what makes the bundle complete
        ranges = list(ascii_uppercase) if ticker_ranges is None else ticker_ranges

        for timeframe in timeframes:
            for adjustment in adjustments:
                bar_type = BarType(
                    AssetType.STOCK,
                    timeframe=timeframe,
                    adjustment=adjustment,
                )
                for ticker_range in ranges:
                    plan.add(
                        f"listed {timeframe}/{adjustment}/{ticker_range}",
                        partial(
                            BarsRequest,
                            bar_type,
                            period,
                            ticker_range=ticker_range,
                        ),
                    )

                for selector in COMPLETE_DELISTED:
                    plan.add(
                        f"delisted {timeframe}/{adjustment}/{selector.name.lower()}",
                        partial(DelistedBarsRequest, bar_type, selector=selector),
                    )

        plan.add(
            "splits",
            partial(MetafileRequest, AssetType.STOCK, MetafileType.SPLITS),
        )
        plan.add(
            "dividends",
            partial(MetafileRequest, AssetType.STOCK, MetafileType.DIVIDENDS),
        )

        return cls("stocks bundle", plan.cells, plan.unoffered)

    @classmethod
    def indices(cls, period: Period, timeframes: list[Timeframe]) -> Self:
        """Every cell of the indices bundle: one archive per timeframe.

        Each archive spans `period`. Nothing is sent.

        Examples
        --------
        >>> bundle = Bundle.indices(Period.FULL, [Timeframe.MIN_1, Timeframe.DAY_1])
        >>> [cell.name for cell in bundle.cells]
        ['index 1min', 'index 1day']
        >>> bundle.unoffered
        []

        """
        plan = _Plan()

        # UNADJUSTED is the store's word, not the vendor's: the request carries
        # no adjustment on the wire, and the bar type names one at every level
        for timeframe in timeframes:
            plan.add(
                f"index {timeframe}",
                partial(
                    BarsRequest,
                    BarType(
                        AssetType.INDEX,
                        timeframe=timeframe,
                        adjustment=IndexAdjustment.UNADJUSTED,
                    ),
                    period,
                ),
            )

        return cls("indices bundle", plan.cells, plan.unoffered)

    @classmethod
    def futures(
        cls,
        period: Period,
        timeframes: list[Timeframe],
        adjustments: list[ContinuousFuturesAdjustment],
        *,
        contracts: bool = False,
    ) -> Self:
        """Every cell of the futures bundle, and every cell the vendor will refuse.

        The continuous series, one archive per timeframe x adjustment spanning
        `period`, and the continuous audit file that says which contracts were
        stitched into it. With `contracts`, the individual contracts those series
        were built from as well: one archive per timeframe x half, the halves
        being both when `period` is full and the update alone otherwise. Nothing
        is sent.

        Examples
        --------
        >>> bundle = Bundle.futures(
        ...     Period.FULL,
        ...     [Timeframe.DAY_1],
        ...     [ContinuousFuturesAdjustment.RATIO],
        ...     contracts=True,
        ... )
        >>> [cell.name for cell in bundle.cells]
        ['continuous 1day/contin_adj_ratio', 'contracts 1day/archive', \
'contracts 1day/update', 'contin_audit']

        A restated series cannot be appended to, so a short period leaves the
        contracts and the audit file:

        >>> bundle = Bundle.futures(
        ...     Period.DAY,
        ...     [Timeframe.DAY_1],
        ...     [ContinuousFuturesAdjustment.RATIO],
        ...     contracts=True,
        ... )
        >>> [cell.name for cell in bundle.cells]
        ['contracts 1day/update', 'contin_audit']
        >>> bundle.unoffered[0][0]
        'continuous 1day/contin_adj_ratio'

        """
        plan = _Plan()
        _plan_continuous_futures(plan, period, timeframes, adjustments)
        if contracts:
            _plan_futures_contracts(plan, period, timeframes)

        # planned whatever else the bundle holds: it is the record of which
        # contracts each continuous series was stitched from, and a series
        # nobody can check against its own roll dates is a series nobody can
        # falsify
        plan.add(
            "contin_audit",
            partial(MetafileRequest, AssetType.FUTURES, MetafileType.CONTIN_AUDIT),
        )

        return cls("futures bundle", plan.cells, plan.unoffered)


@dataclass
class SweepReport:
    """The outcome of a bundle sweep, cell by cell.

    ``skipped`` and ``failed`` differ: a skipped cell is a combination the API
    does not offer, so re-running will never produce it; a failed cell is one
    the API should have served and didn't, so it is worth retrying.
    """

    ingested: list[tuple[str, Ingested]] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, Exception]] = field(default_factory=list)
    downloaded: int = 0
    seconds: float = 0.0
    # every line the quarantine holds, this sweep's and every earlier one's --
    # nothing is ever removed from it, so it only grows
    quarantined_in_all: int = 0

    @property
    def rejected(self) -> int:
        """Lines the sweep could not parse and quarantined, across every cell."""
        return sum(ingested.rejected for _, ingested in self.ingested)

    @property
    def suspect(self) -> int:
        """Rows the sweep wrote that do not hold together as bars."""
        return sum(ingested.suspect for _, ingested in self.ingested)

    @property
    def damaged(self) -> list[tuple[str, int]]:
        """Lines lost per vendor payload this sweep, worst first.

        Payloads are unique across a sweep -- one cell serves each ticker once --
        so these are concatenated rather than summed.
        """
        return sorted(
            (
                payload_lines
                for _, ingested in self.ingested
                for payload_lines in ingested.damaged
            ),
            key=lambda payload_lines: (-payload_lines[1], payload_lines[0]),
        )

    @property
    def quarantine_files(self) -> list[Path]:
        """The quarantine files this sweep wrote, in the order it wrote them."""
        return [
            ingested.quarantine
            for _, ingested in self.ingested
            if ingested.quarantine is not None
        ]

    @property
    def megabytes_per_second(self) -> float:
        """End-to-end throughput: bytes fetched over wall clock, ingest included."""
        if self.seconds <= 0:
            return 0.0
        return self.downloaded / self.seconds / (1 << 20)


def _quarantined_in_all(client: Client) -> int:
    """Every line the store's quarantine holds, this sweep's included."""
    counted = client.store.quarantined().count("*").fetchone()
    return 0 if counted is None else int(counted[0])


# a bundle is one asset type's universe, so its cells already say which client
# serves it -- which is how a sweep finds one without the plan carrying a class
# it has no use for
_CLIENTS: dict[AssetType, type[Client]] = {
    AssetType.STOCK: StockClient,
    AssetType.INDEX: IndexClient,
    AssetType.FUTURES: FuturesClient,
}


def _asset_type_of(request: IngestibleRequest) -> AssetType | None:
    """Return the asset type a cell asks for, wherever the request keeps it."""
    if isinstance(request, MetafileRequest):
        return request.asset_type
    return request.bar_type.asset_type


def _client_class(bundle: Bundle) -> type[Client]:
    """Return the client class that serves `bundle`, read off the cells it plans.

    Raises
    ------
    ValueError
        If the bundle names no single asset type.

    """
    asset_types = {_asset_type_of(cell.request) for cell in bundle.cells}
    if len(asset_types) != 1:
        msg = (
            f"a bundle is one asset type's universe, and {bundle.label} names "
            f"{len(asset_types)}: pass the client to sweep with instead"
        )
        raise ValueError(
            msg,
        )
    return _CLIENTS[asset_types.pop()]


class BundleDownloader:
    """Sweep a ``Bundle``: fetch its cells several at a time, file them one at a time.

    `progress` defaults to nested tqdm bars; pass ``NullProgress()`` for silence.
    `max_workers` is how many archives to fetch at once -- the useful value is a
    property of the link rather than of the machine, see the README. `spool_dir`
    is where archives land before they are filed, beside the store by default.
    `queue_depth` is how many may sit there at once, which caps the sweep's disk
    high-water mark; it defaults to ``max_workers + QUEUE_HEADROOM``.
    """

    def __init__(
        self,
        progress: ProgressReporter | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        spool_dir: Path | None = None,
        queue_depth: int | None = None,
    ) -> None:
        # shared with every client this builds, so the byte bars nest under the
        # sweep's bar instead of fighting it for the same terminal line
        self._progress = TqdmProgress() if progress is None else progress
        self._max_workers = max_workers
        self._spool_dir = spool_dir
        self._queue_depth = (
            max_workers + QUEUE_HEADROOM if queue_depth is None else queue_depth
        )

    def sweep(self, bundle: Bundle, *, client: Client | None = None) -> SweepReport:
        """Fetch every cell of `bundle` and file each one as it lands.

        No cell raises: every cell's outcome lands in the returned report, along
        with what the sweep pulled and how fast. The cells the vendor does not
        offer arrive already known and are reported as skipped without being
        asked for. `client` defaults to one for the bundle's asset type, built
        from the environment and wired to this sweep's settings.

        Raises
        ------
        ValueError
            If no `client` is given and the bundle names no single asset type
            to build one for.

        """
        if client is None:
            client = _client_class(bundle).from_env(
                progress=self._progress,
                max_workers=self._max_workers,
                spool_dir=self._spool_dir,
            )
        report = SweepReport(skipped=list(bundle.unoffered))
        # claimed before a cell's first byte and released once its archive has
        # been filed and deleted, so the spool holds at most `queue_depth`
        # archives however far the workers get ahead of the ingest
        spool_slots = Semaphore(max(1, self._queue_depth))
        started = time.monotonic()

        with (
            self._progress.track(bundle.label, len(bundle.cells), "cell") as advance,
            ThreadPoolExecutor(self._max_workers, thread_name_prefix="fetch") as pool,
        ):
            pending: dict[Future[Fetched], NamedRequest] = {
                pool.submit(self._fetch_cell, cell, client, spool_slots): cell
                for cell in bundle.cells
            }
            for done in as_completed(pending):
                self._file_cell(pending[done], done, client, spool_slots, report)
                advance(1)

        # read before the client goes: it leaves the store open, but a caller
        # who built one for the sweep alone has no other handle to ask through
        report.quarantined_in_all = _quarantined_in_all(client)
        client.close()
        report.seconds = time.monotonic() - started
        return report

    def _fetch_cell(
        self,
        cell: NamedRequest,
        client: Client,
        spool_slots: Semaphore,
    ) -> Fetched:
        spool_slots.acquire()
        try:
            return client.fetch(cell.request)
        except BaseException:
            # nothing was spooled, so the slot is owed back -- otherwise a
            # run of failures would starve the workers that could still succeed
            spool_slots.release()
            raise

    def _file_cell(
        self,
        cell: NamedRequest,
        fetching: Future[Fetched],
        client: Client,
        spool_slots: Semaphore,
        report: SweepReport,
    ) -> None:
        """File one cell at a time, serially, on the sweep's own thread."""
        try:
            fetched = fetching.result()
        except Exception as unserved:  # noqa: BLE001 - a failed fetch must not abort the whole sweep.
            report.failed.append((cell.name, unserved))
            return
        try:
            report.downloaded += fetched.size
            report.ingested.append((cell.name, client.ingest(fetched, cell.request)))
        except Exception as unfileable:  # noqa: BLE001 - a corrupt payload is a failed cell, not a crash.
            report.failed.append((cell.name, unfileable))
        finally:
            spool_slots.release()
