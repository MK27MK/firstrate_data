import http
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Self

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from firstrate_data import config
from firstrate_data.config import DEFAULT_BASE_URL
from firstrate_data.domain import (
    AssetType,
    TickerListing,
)
from firstrate_data.domain.bar_type import BarType
from firstrate_data.domain.enums import (
    ContinuousFuturesAdjustment,
    ContractFiles,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    OtherData,
    Period,
    Timeframe,
    Unadjusted,
)
from firstrate_data.download import progress
from firstrate_data.download.bundles import BundleConfig, bundle_requests
from firstrate_data.download.requests import (
    BarsRequest,
    ContractBarsRequest,
    DelistedBarsRequest,
    LastUpdateRequest,
    OtherDataRequest,
    Request,
    TickerListingRequest,
)
from firstrate_data.store.store import Ingested, Store

# 1 MiB: large enough that a multi-GB archive isn't paid for one syscall at a
# time, small enough that a progress bar still moves on a slow line
CHUNK_SIZE = 1 << 20

# a rate limit and the transient 5xx family. A 4xx is the request being wrong,
# and repeating it won't make it right.
_RETRYABLE_STATUS = (
    http.HTTPStatus.TOO_MANY_REQUESTS,
    http.HTTPStatus.INTERNAL_SERVER_ERROR,
    http.HTTPStatus.BAD_GATEWAY,
    http.HTTPStatus.SERVICE_UNAVAILABLE,
    http.HTTPStatus.GATEWAY_TIMEOUT,
)

# connecting succeeds fast or not at all. A body, though, arrives at whatever
# rate the link gives, so the read budget is per-chunk and generous
_TIMEOUT = (30, 120)


class Client:
    def __init__(
        self,
        user_id: str,
        store: Store,
        base_url: str = DEFAULT_BASE_URL,
    ) -> None:
        self._user_id = user_id
        self._store = store
        self._base_url = base_url.rstrip("/")

        self._session = _session()

    @classmethod
    def from_env(cls) -> Self:
        return cls(
            config.firstrate_user_id(),
            Store.from_env(),
            config.base_url(),
        )

    def download_bundle(
        self,
        bundle_config: BundleConfig,
        prefetch: int = 2,
        *,
        refresh: bool = False,
    ) -> list[Ingested]:
        """Download a bundle, fetching the next archives while one is written.

        A fetch waits on the vendor and a write on DuckDB, so the two overlap.
        Writes stay on this thread: the store holds one connection, and the
        spool would carry the whole bundle at once if fetches ran unbounded.

        An archive the store already holds at the vendor's last update is not
        fetched again, and is absent from what this returns. `refresh` fetches
        every archive the bundle names regardless of what is filed.
        """
        ingested: list[Ingested] = []
        fetching: deque[tuple[Request, Future[Path]]] = deque()
        # one last_update call per asset type, and only once something filed
        # turns out to be worth dating
        served: dict[AssetType, date] = {}

        # `prefetch` worker threads that run beside this one inside this same
        # process, sharing its memory -- so a worker hands back a `Path` with
        # no copying, and the session's connection pool is the one object all
        # of them use. Threads, not processes, because a fetch spends its life
        # blocked on a socket and a blocked thread costs no CPU; processes are
        # what a second CPU's worth of computation would need, and would have
        # to pickle every argument and result across the process boundary.
        # Leaving the `with` waits for every worker still running
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
        with ThreadPoolExecutor(prefetch, thread_name_prefix="fetch") as fetchers:
            for request in bundle_requests(bundle_config):
                if not refresh and self._is_current(request, served):
                    progress.note(f"filed already: {self._spool_name(request)}")
                    continue
                # hands `self._fetch(request)` to a worker and returns
                # immediately with a `Future` -- a receipt for an archive that
                # has not arrived. The download runs while this thread carries
                # on to the write below, which is the whole point. Submitting
                # more than there are workers just queues them
                # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.submit
                fetching.append((request, fetchers.submit(self._fetch, request)))
                if len(fetching) > prefetch:
                    # take the oldest receipt off the left end and write it,
                    # which is what stops the loop from submitting the whole
                    # bundle and spooling 64 archives to disk at once
                    # https://docs.python.org/3/library/collections.html#collections.deque.popleft
                    ingested.append(self._write(*fetching.popleft()))
            while fetching:
                ingested.append(self._write(*fetching.popleft()))

        return ingested

    def _is_current(self, request: Request, served: dict[AssetType, date]) -> bool:
        """Whether the store already holds this request's archive, up to date."""
        # a plain full-history archive only. A delisted archive and a contract
        # half name tickers the catalog cannot tell one selector's from
        # another's, and their tickers stopped trading, so no bar of theirs
        # ever reaches the vendor's last update; a metafile leaves no catalog
        # row at all. None of the three can be judged from what is filed
        if type(request) is not BarsRequest:
            return False

        asset_type = request.bar_type.asset_type
        if asset_type is None:
            return False

        filed = self.store.last_bar(request.bar_type, request.ticker_range)
        if filed is None:
            return False

        if asset_type not in served:
            served[asset_type] = _as_date(self.last_update(asset_type))
        return filed.date() >= served[asset_type]

    def _write(self, request: Request, fetched: Future[Path]) -> Ingested:
        # parks this thread until that worker's archive has landed, then gives
        # back the spooled path. If the worker raised -- a timeout, a 404 --
        # the exception is re-raised here, on this thread, at this line: it is
        # not lost in the worker, and it is not seen until this call
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future.result
        return self.store.write(fetched.result(), request)

    @property
    def store(self) -> Store:
        return self._store

    @property
    def spool(self) -> Path:
        return self.store.spool

    # ------------------------------------------------------------------
    # text endpoints
    # ------------------------------------------------------------------

    def download_ticker_listing(self, asset_type: AssetType) -> list[TickerListing]:
        listing = TickerListing.from_csv(
            self.read(TickerListingRequest(asset_type)),
        )
        self.store.write_ticker_listing(asset_type, listing)
        return listing

    # NOTE see docs
    # https://firstratedata.com/about/api-docs?type=stock&userID=xxx#lastupdate
    def last_update(
        self,
        asset_type: AssetType,
        *,
        is_full_update: bool | None = None,
    ) -> date | datetime:
        """The date the vendor last published data for `asset_type`."""
        return self._parse_last_update(
            self.read(LastUpdateRequest(asset_type, is_full_update)),
        )

    @staticmethod
    def _parse_last_update(body: str) -> date | datetime:
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

    # ------------------------------------------------------------------
    # Stocks
    # ------------------------------------------------------------------

    def download_stocks_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        ticker_range: str | None = None,
    ) -> Ingested:
        request = BarsRequest(
            BarType(AssetType.STOCK, timeframe=timeframe, adjustment=adjustment),
            period,
            ticker_range=ticker_range,
        )
        return self._download(request)

    # Splits / Dividends Requests --------------------------------------

    def download_splits(self) -> Ingested:
        return self._download(OtherDataRequest(AssetType.STOCK, OtherData.SPLITS))

    def download_dividends(self) -> Ingested:
        return self._download(OtherDataRequest(AssetType.STOCK, OtherData.DIVIDENDS))

    # Delisted Ticker Data ---------------------------------------------

    def download_delisted_bars(
        self,
        selector: DelistedArchive | DelistedUpdate,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
    ) -> Ingested:
        return self._download(
            DelistedBarsRequest(
                BarType(AssetType.STOCK, timeframe=timeframe, adjustment=adjustment),
                selector=selector,
            ),
        )

    # NOTE no endpoint for this
    # def download_company_profiles(self) -> ...:
    #     pass

    # ------------------------------------------------------------------
    # etf
    # ------------------------------------------------------------------

    def download_etf_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        ticker_range: str | None = None,
    ) -> Ingested:
        request = BarsRequest(
            BarType(AssetType.ETF, timeframe=timeframe, adjustment=adjustment),
            period,
            ticker_range=ticker_range,
        )
        return self._download(request)

    # ------------------------------------------------------------------
    # index
    # ------------------------------------------------------------------

    def download_index_bars(
        self,
        period: Period,
        timeframe: Timeframe,
    ) -> Ingested:
        # UNADJUSTED is the store's word for it, not the vendor's: the request
        # carries no ``adjustment`` on the wire, and the bar type path needs one
        request = BarsRequest(
            BarType(
                AssetType.INDEX,
                timeframe=timeframe,
                adjustment=Unadjusted.UNADJUSTED,
            ),
            period,
        )
        return self._download(request)

    # ------------------------------------------------------------------
    # futures
    # ------------------------------------------------------------------

    def download_futures_continuous_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: ContinuousFuturesAdjustment,
    ) -> Ingested:
        request = BarsRequest(
            BarType(AssetType.FUTURES, timeframe=timeframe, adjustment=adjustment),
            period,
        )
        return self._download(request)

    # Individual Contract Data -----------------------------------------

    def download_futures_contract_bars(
        self,
        contract_files: ContractFiles,
        timeframe: Timeframe,
    ) -> Ingested:
        return self._download(
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=timeframe),
                contract_files=contract_files,
            ),
        )

    def download_contract_dates(self) -> Ingested:
        return self._download(
            OtherDataRequest(AssetType.FUTURES, OtherData.CONTRACT_DATES)
        )

    # ------------------------------------------------------------------
    # fetch, ingest, download
    # ------------------------------------------------------------------

    def _fetch(
        self,
        request: Request,
    ) -> Path:
        """Fetch a file or archive from FirstRate and return its local path."""
        label = f"{request.endpoint} {'/'.join(request.to_params().values())}"
        destination = self.spool / self._spool_name(request)

        try:
            with self._get(request, stream=True) as response:
                response.raise_for_status()
                self._stream_body(response, destination, label)
        except (requests.RequestException, OSError):
            # a half-written archive is not an archive, and the spool must not
            # hold anything a later run could mistake for one
            destination.unlink(missing_ok=True)
            raise

        return destination

    def read(self, request: Request) -> str:
        with self._get(request) as response:
            response.raise_for_status()
            return response.text

    def close(self) -> None:
        self._session.close()

    def _download(self, request: Request) -> Ingested:
        return self.store.write(self._fetch(request), request)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _get(self, request: Request, *, stream: bool = False) -> requests.Response:
        return self._session.get(
            f"{self._base_url}/{request.endpoint}",
            params={**request.to_params(), "userid": self._user_id},
            timeout=_TIMEOUT,
            stream=stream,
        )

    def _stream_body(
        self,
        response: requests.Response,
        destination: Path,
        label: str,
    ) -> None:
        """Write responde to the spool dir."""
        declared = response.headers.get("Content-Length")
        with (
            progress.track(label, int(declared) if declared else None, "B") as advance,
            destination.open("wb") as spooled,
        ):
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                spooled.write(chunk)
                advance(len(chunk))

    @staticmethod
    def _spool_name(request: Request) -> str:
        parts = [request.endpoint, *request.to_params().values()]
        safe = (
            "".join(c if c.isalnum() or c in "-." else "_" for c in p) for p in parts
        )
        return "_".join(safe)


def _as_date(moment: date | datetime) -> date:
    # datetime is a subclass of date, so the narrower test comes first
    # https://docs.python.org/3/library/datetime.html#datetime.datetime
    return moment.date() if isinstance(moment, datetime) else moment


def _session() -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=_RETRYABLE_STATUS,
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        ),
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
