from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import ClassVar, Self

from firstrate_data import config
from firstrate_data.config import DEFAULT_BASE_URL
from firstrate_data.domain import (
    AssetType,
    Period,
    TickerListing,
    Timeframe,
)
from firstrate_data.download.client.file_fetcher import FetchedFile, FileFetcher
from firstrate_data.download.requests import (
    IngestibleRequest,
    LastUpdateRequest,
    TickerListingRequest,
)
from firstrate_data.store.store import Ingested, Store

DEFAULT_MAX_WORKERS = 4


class Client(ABC):
    _asset_type: ClassVar[AssetType]

    def __init__(
        self,
        user_id: str,
        store: Store,
        base_url: str = DEFAULT_BASE_URL,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self._user_id = user_id
        self._store = store
        self._base_url = base_url.rstrip("/")

        self._fetcher = FileFetcher(
            self._base_url,
            user_id,
            store.spool,
            max_workers=max_workers,
        )

    @classmethod
    def from_env(cls, max_workers: int = DEFAULT_MAX_WORKERS) -> Self:
        return cls(
            config.firstrate_user_id(),
            Store.from_env(),
            config.base_url(),
            max_workers,
        )

    @abstractmethod
    def download_historical_bars(
        self,
        period: Period,
        timeframe: Timeframe,
    ) -> Ingested: ...

    @abstractmethod
    def download_bundle(self, bundle_config) -> None: ...

    @property
    def store(self) -> Store:
        return self._store

    @property
    def spool(self) -> Path:
        return self._fetcher.spool

    # ------------------------------------------------------------------
    # fetch, ingest, download
    # ------------------------------------------------------------------

    def _fetch(
        self, request: IngestibleRequest, name: str | None = None
    ) -> FetchedFile:
        return self._fetcher.fetch(request, name=name)

    def _download(self, request: IngestibleRequest) -> Ingested:
        return self.store.ingest(self._fetch(request), request)

    # ------------------------------------------------------------------
    # text endpoints
    # ------------------------------------------------------------------

    def download_ticker_listing(self) -> list[TickerListing]:
        listing = TickerListing.from_csv(
            self._fetcher.read(TickerListingRequest(self._asset_type)),
        )
        self.store.ingest_ticker_listing(self._asset_type, listing)
        return listing

    def get_last_update(
        self,
        *,
        is_full_update: bool | None = None,
    ) -> date | datetime:
        return self._parse_last_update(
            self._fetcher.read(LastUpdateRequest(self._asset_type, is_full_update)),
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
