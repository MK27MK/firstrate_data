import os
from abc import ABC, abstractmethod
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
from firstrate_data.request import BarsRequest, MetafileRequest, Request

DEFAULT_BASE_URL = "https://firstratedata.com/api"


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
    ):
        self._user_id = user_id
        self._catalog = catalog
        self._base_url = base_url.rstrip("/")

    @classmethod
    def from_env(cls) -> Self:
        """Build a loader from the environment: credentials here, store via
        ``Catalog.from_env`` -- the download/persistence split, wired up."""
        load_dotenv()
        user_id = os.getenv("FIRSTRATE_USERID")
        if user_id is None:
            raise KeyError("FIRSTRATE_USERID not found.")
        base_url = os.getenv("FIRSTRATE_BASE_URL", DEFAULT_BASE_URL)
        return cls(user_id, Catalog.from_env(), base_url)

    @abstractmethod
    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: AdjustmentT,
    ) -> Path:
        """This function returns historical data archives (.txt files in csv format which are grouped into zip archives).

        The archive is extracted into a request-scoped folder under the loader's
        raw directory, keyed by every request parameter, and that folder's Path
        is returned. If the folder already exists it is wiped and replaced, so it
        always reflects exactly one archive.

        See the overriding subclass for the parameters its asset type accepts:
        they differ (stocks take a ticker_range, futures do not; the adjustment
        enum is per-asset-type).
        """

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    # Transport / persistence ------------------------------------------

    def _get(self, request: Request) -> bytes:
        response = requests.get(
            f"{self._base_url}/{request.endpoint}",
            params={**request.to_params(), "userid": self._user_id},
            timeout=120,
        )
        response.raise_for_status()
        return response.content

    def _fetch_and_persist_historical_bars(
        self, request: BarsRequest[AdjustmentT]
    ) -> Path:
        """Fetch a bars archive and persist it. Shared by every asset type.

        ``BarsRequest[AdjustmentT]`` rather than ``BarsRequest`` keeps ADR 0002's
        invariant one layer deeper: a futures loader cannot hand this an
        equities-adjusted request.
        """
        zip_file = self._get(request)

        return self._catalog.write_raw_bars(zip_file, request)

    # Meta File Requests -----------------------------------------------

    def _fetch_and_persist_metafile(self, request: MetafileRequest) -> Path:
        """Fetch a metafile and persist it under ``raw/{asset}/meta/``.

        Lives on the base rather than on the equities loader because ``meta_file``
        also serves the futures continuous-series audit file.
        """
        content = self._get(request)

        return self._catalog.write_raw_metadata(content, request)
