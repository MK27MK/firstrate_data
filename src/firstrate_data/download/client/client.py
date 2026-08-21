from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Self

from firstrate_data import config
from firstrate_data.domain import (
    AssetType,
    Period,
    TickerListing,
    Timeframe,
)
from firstrate_data.download.client.fetcher import ArchiveFetcher, Fetched
from firstrate_data.download.progress import NullProgress, ProgressReporter
from firstrate_data.download.requests import (
    IngestibleRequest,
    LastUpdateRequest,
    MetafileRequest,
    TickerListingRequest,
)
from firstrate_data.store.store import Ingested, Store

# re-exported: callers and the CLI import it from here
DEFAULT_BASE_URL = config.DEFAULT_BASE_URL
DEFAULT_MAX_WORKERS = 4


class Client(ABC):
    """Base loader: fetches FirstRate Data archives and ingests them into a store."""

    _asset_type: ClassVar[AssetType]

    def __init__(
        self,
        user_id: str,
        store: Store,
        base_url: str = DEFAULT_BASE_URL,
        progress: ProgressReporter | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self._user_id = user_id
        self._store = store
        self._base_url = base_url.rstrip("/")

        self._progress = NullProgress() if progress is None else progress
        # the store names its own spool, and no argument here can
        # second-guess it. Where an archive waits is a fact about the
        # store's layout, and a caller that wants it elsewhere says so to
        # the store it builds.
        self._fetcher = ArchiveFetcher(
            self._base_url,
            user_id,
            store.spool,
            max_workers=max_workers,
            progress=self._progress,
        )

    @classmethod
    def from_env(
        cls,
        progress: ProgressReporter | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        spool_dir: Path | None = None,
    ) -> Self:
        """Credentials from the environment, store via ``Store.from_env``.

        ``progress`` and ``max_workers`` pass straight through: where to draw a
        bar and how hard to pull are a caller's decisions, not a deployment's.
        ``spool_dir`` goes to the store, which is what decides where an archive
        waits.
        """
        return cls(
            config.firstrate_user_id(),
            Store.from_env(spool_dir),
            config.base_url(),
            progress,
            max_workers,
        )

    # `period` and `timeframe` are the two every asset type takes. The tail
    # carries no annotation on purpose. Stocks take an `adjustment` and a
    # `ticker_range`. Futures take an `adjustment` alone. An index takes
    # neither. A fixed tail would be a signature no subclass could honour
    # without lying about its endpoint.
    @abstractmethod
    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
    ) -> Ingested:
        """Fetch one bars archive, ingest it, discard it, report what it left.

        See the overriding subclass for the parameters its asset type accepts.
        """

    # ------------------------------------------------------------------
    # The two halves of a download
    # ------------------------------------------------------------------

    # Split rather than fused: fetching is network-bound and safe to run
    # many at once. Filing is disk-bound and holds one DuckDB connection.
    # A sweep runs many of the first for each one of the second.

    def fetch(self, request: IngestibleRequest, name: str | None = None) -> Fetched:
        """Pull one archive to the spool and say where it landed.

        The fetch neither ingests nor deletes anything: the caller owns the
        file that comes back, and ``ingest`` consumes it. Safe to call from
        many threads at once. ``name`` defaults to a spool filename derived from
        the request, which is what lets a re-run finish an interrupted run's
        partial. The ``Fetched`` carries no bytes -- an archive is never
        resident.
        """
        return self._fetcher.fetch(request, name=name)

    @property
    def spool(self) -> Path:
        """Where this loader's archives land before they're filed."""
        return self._fetcher.spool

    @property
    def store(self) -> Store:
        """The store this loader files into.

        Exposed for reading, not for lifetime: ``close`` leaves it open, because
        a store outlives any one client of it.
        """
        return self._store

    def ingest(self, fetched: Fetched, request: IngestibleRequest) -> Ingested:
        """File a fetched archive into the store, then discard it.

        Not thread-safe, by design. The store holds one DuckDB connection,
        and a replacing fetch removes bars before it writes them. This is
        the serial half of a parallel sweep.
        """
        try:
            if isinstance(request, MetafileRequest):
                return self._store.ingest_metafile(fetched.path, request.metafile_type)
            return self._store.ingest_bars(fetched.path, request)
        finally:
            # parquet is the only copy, and that goes for the vendor's zip on
            # its way in as much as for the CSV it unzips to
            fetched.path.unlink(missing_ok=True)

    # Whole downloads --------------------------------------------------

    # the request's own constructor checks what the vendor will honour, so
    # there is no guard here for a subclass to forget to route through
    def _download(self, request: IngestibleRequest) -> Ingested:
        return self.ingest(self.fetch(request), request)

    # ------------------------------------------------------------------
    # The endpoints that answer with a value rather than an archive
    # ------------------------------------------------------------------

    # Both take a `type` and appear on every asset type's docs page, so they
    # belong to the base. Neither reaches the store: a date and a listing are
    # not bars, so what comes back is a value the caller keeps.

    def download_last_update(
        self,
        *,
        is_full_update: bool | None = None,
    ) -> date | datetime:
        """When the vendor last refreshed this asset type's data.

        ``is_full_update`` left as None asks about an update of any type, full
        or partial. A ``datetime`` comes back when the endpoint states a time of
        day, a ``date`` when it states only the day.

        Raises
        ------
        ValueError
            If the body that arrived isn't a date.

        """
        return self._parse_last_update(
            self._fetcher.read(LastUpdateRequest(self._asset_type, is_full_update)),
        )

    def download_ticker_listing(self) -> list[TickerListing]:
        """List the tickers this asset type covers, in the order served.

        Raises
        ------
        ValueError
            If the body holds no rows, or a row that's not a listing.

        """
        return TickerListing.from_csv(
            self._fetcher.read(TickerListingRequest(self._asset_type)),
        )

    def close(self) -> None:
        """Release the transport's pooled connections.

        Not the store's connection: the store arrives through ``__init__`` and
        outlives any one client of it, so closing it here would shut a caller's
        store on them. Close the store yourself, or use it as a context manager.
        """
        self._fetcher.close()

    @staticmethod
    def _parse_last_update(body: str) -> date | datetime:
        r"""Parse the date ``last_update`` answered with.

        Parameters
        ----------
        body : str
            The response body, which the endpoint serves as a bare date.

        Returns
        -------
        ``date`` or ``datetime``
            The richer type when the body carries a time of day, the plainer
            one otherwise.

        Raises
        ------
        ValueError
            If the body is empty or isn't a date.

        Examples
        --------
        >>> Client._parse_last_update("2026-07-31\n")
        datetime.date(2026, 7, 31)

        A body carrying a time keeps it:

        >>> Client._parse_last_update("2026-07-31 22:00:00")
        datetime.datetime(2026, 7, 31, 22, 0)

        """
        text = body.strip()
        if not text:
            msg = "last_update answered with an empty body"
            raise ValueError(msg)

        try:
            # a bare date first: datetime.fromisoformat takes one too, and stamps it
            # with a midnight the vendor said nothing about
            return date.fromisoformat(text)
        except ValueError:
            pass

        try:
            return datetime.fromisoformat(text)
        except ValueError as unreadable:
            msg = f"last_update answered with {text!r}, which is not a date"
            raise ValueError(
                msg,
            ) from unreadable
