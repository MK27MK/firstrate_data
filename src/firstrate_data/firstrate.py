import os
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import ClassVar, Self

import requests
from dotenv import load_dotenv

from firstrate_data.catalog import Catalog
from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.progress import NullProgress, ProgressReporter
from firstrate_data.request import BarsRequest, MetafileRequest, Request

DEFAULT_BASE_URL = "https://firstratedata.com/api"

# small enough that the bar moves on a slow line, large enough that a multi-GB
# archive is not paid for one syscall at a time
_CHUNK_SIZE = 1 << 16


def _describe(request: Request) -> str:
    """A request as one line of bar label: ``data_file stock/full/1min/adj_split/A``."""
    return f"{request.endpoint} {'/'.join(request.to_params().values())}"


# this syntax has been introduced in 3.12 and it works like ts generics
class FirstRateData[AdjustmentT: EquitiesAdjustment | ContinuousFuturesAdjustment](ABC):
    """Base loader: fetches FirstRate Data archives and persists them into a
    managed directory. Subclasses fix ``_asset_type`` so callers never pass it,
    and bind ``AdjustmentT`` to the adjustment enum their asset type accepts --
    the API is *not* uniform across asset types, so neither is this signature."""

    _asset_type: ClassVar[AssetType]

    def __init__(
        self,
        user_id: str,
        catalog: Catalog,
        base_url: str = DEFAULT_BASE_URL,
        progress: ProgressReporter | None = None,
    ):
        self._user_id = user_id
        self._catalog = catalog
        self._base_url = base_url.rstrip("/")
        # silence by default: a loader used as a library draws nothing unasked,
        # and the bundle sweep -- the caller who wants a bar -- passes one in
        self._progress = NullProgress() if progress is None else progress

    @classmethod
    def from_env(cls, progress: ProgressReporter | None = None) -> Self:
        """Build a loader from the environment: credentials here, store via
        ``Catalog.from_env`` -- the download/persistence split, wired up.

        ``progress`` is not environment-derived and is passed straight through:
        where a bar should be drawn is a caller's decision, not a deployment's.
        """
        load_dotenv()
        user_id = os.getenv("FIRSTRATE_USERID")
        if user_id is None:
            raise KeyError("FIRSTRATE_USERID not found.")
        base_url = os.getenv("FIRSTRATE_BASE_URL", DEFAULT_BASE_URL)
        return cls(user_id, Catalog.from_env(), base_url, progress)

    @abstractmethod
    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: AdjustmentT,
    ) -> Path:
        """This function returns historical data archives (.txt files in csv format which are grouped into zip archives).

        The archive is extracted into a folder under the loader's raw
        directory, keyed by every request parameter and dated by the fetch,
        and that folder's Path is returned.

        See the overriding subclass for the parameters its asset type accepts:
        they differ (stocks take a ticker_range, futures do not; the adjustment
        enum is per-asset-type).
        """

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _snapshot_date(self) -> date:
        """The date the download about to happen will be stored under."""
        # on the loader, not the request: a request identifies a slice of the
        # dataset, and when we asked identifies neither
        return date.today()

    # Transport / persistence ------------------------------------------

    def _get(self, request: Request) -> bytes:
        # stream=True is what makes progress observable at all: without it
        # requests returns only once the last byte of a multi-GB archive has
        # landed, and there is nothing to report until there is nothing left to
        # report. The archive is still assembled whole in memory, since that is
        # what the catalog takes.
        with requests.get(
            f"{self._base_url}/{request.endpoint}",
            params={**request.to_params(), "userid": self._user_id},
            timeout=120,
            stream=True,
        ) as response:
            response.raise_for_status()

            # absent on a chunked response: a bar without an ETA, not a failure
            declared = response.headers.get("Content-Length")
            total = int(declared) if declared is not None else None

            archive = bytearray()
            with self._progress.track(_describe(request), total, "B") as advance:
                for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                    archive.extend(chunk)
                    advance(len(chunk))

        return bytes(archive)

    def _fetch_and_persist_historical_bars(
        self, request: BarsRequest[AdjustmentT]
    ) -> Path:
        """Fetch a bars archive and persist it. Shared by every asset type.

        ``BarsRequest[AdjustmentT]`` rather than ``BarsRequest`` keeps ADR 0002's
        invariant one layer deeper: a futures loader cannot hand this an
        equities-adjusted request.
        """
        zip_file = self._get(request)

        return self._catalog.write_raw_bars(zip_file, request, self._snapshot_date())

    # Meta File Requests -----------------------------------------------

    def _fetch_and_persist_metafile(self, request: MetafileRequest) -> Path:
        """Fetch a metafile and persist it under ``raw/{asset}/meta/``.

        Lives on the base rather than on the equities loader because ``meta_file``
        also serves the futures continuous-series audit file.
        """
        content = self._get(request)

        return self._catalog.write_raw_metadata(content, request, self._snapshot_date())
