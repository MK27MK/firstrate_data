import io
import os
import shutil
import zipfile
from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
from typing import ClassVar, Self

import requests
from dotenv import load_dotenv

from firstrate_data.query_parameters import (
    AssetType,
    EquitiesAdjustment,
    MetaFileType,
    Period,
    Timeframe,
)

DEFAULT_BASE_URL = "https://firstratedata.com/api"


# this syntax has been introduced in 3.12 and it works like ts generics
class FirstRateData[AdjustmentT: StrEnum](ABC):
    """Base loader: fetches FirstRate Data archives and persists them into a
    managed directory. Subclasses fix ``_asset_type`` so callers never pass it,
    and bind ``AdjustmentT`` to the adjustment enum their asset type accepts --
    the API is *not* uniform across asset types, so neither is this signature."""

    _asset_type: ClassVar[AssetType]

    def __init__(
        self,
        directory: Path,
        userid: str,
        base_url: str = DEFAULT_BASE_URL,
        skip_existing: bool = False,
    ):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"
        self._userid = userid
        self._base_url = base_url.rstrip("/")
        # trades freshness for time: an already-populated request folder is left
        # untouched rather than re-fetched. Only sound for archives that never
        # change (delisted pre-2026) or when resuming an interrupted sweep --
        # a listed 'full' archive is rebuilt daily, so a kept folder goes stale.
        self._skip_existing = skip_existing

    @classmethod
    def from_data_path(cls, skip_existing: bool = False) -> Self:
        load_dotenv()
        data_path = os.getenv("DATA_PATH")
        if data_path is None:
            raise FileNotFoundError("DATA_PATH not found.")
        userid = os.getenv("FIRSTRATE_USERID")
        if userid is None:
            raise KeyError("FIRSTRATE_USERID not found.")
        base_url = os.getenv("FIRSTRATE_BASE_URL", DEFAULT_BASE_URL)
        return cls(
            Path(data_path),
            userid=userid,
            base_url=base_url,
            skip_existing=skip_existing,
        )

    # Transport / persistence ------------------------------------------

    def _get(self, endpoint: str, params: dict[str, str]) -> bytes:
        response = requests.get(
            f"{self._base_url}/{endpoint}",
            params={**params, "userid": self._userid},
            timeout=120,
        )
        response.raise_for_status()
        return response.content

    def _extract_zip(self, content: bytes, target: Path) -> Path:
        # clean-and-replace: the folder always reflects exactly one archive
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            archive.extractall(target)
        return target

    def _fetch_archive(
        self, endpoint: str, params: dict[str, str], target: Path
    ) -> Path:
        if self._skip_existing and any(target.glob("*")):
            return target
        return self._extract_zip(self._get(endpoint, params), target)

    # Historical Data Requests -----------------------------------------

    def _historical_data_query(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: AdjustmentT,
        ticker_range: str | None = None,
    ) -> Path:
        # ticker_range is a stock/ETF concept -- the rules for it belong to the
        # subclass that has it, not here. This only wires it through.
        params = {
            "type": self._asset_type.value,
            "period": period.value,
            "timeframe": timeframe.value,
            "adjustment": adjustment.value,
        }
        target = (
            self._raw_directory
            / self._asset_type.value
            / period.value
            / timeframe.value
            / adjustment.value
        )
        if ticker_range is not None:
            params["ticker_range"] = ticker_range
            target = target / ticker_range

        return self._fetch_archive("data_file", params, target)

    @abstractmethod
    def download_historical_data(
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

    # Meta File Requests -----------------------------------------------

    def _download_metafile(self, metafile_type: MetaFileType) -> Path:
        """Fetch a metafile and persist it under ``raw/{asset}/meta/``.

        Lives on the base rather than on the equities loader because ``meta_file``
        also serves the futures continuous-series audit file.
        """
        params = {
            "type": self._asset_type.value,
            "metafile_type": metafile_type.value,
        }
        content = self._get("meta_file", params)

        target = self._raw_directory / self._asset_type.value / "meta"
        target.mkdir(parents=True, exist_ok=True)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            csv_path = target / f"{metafile_type.value}.csv"
            csv_path.write_bytes(content)
            return csv_path

        return self._extract_zip(content, target / metafile_type.value)


class FirstRateEquities(FirstRateData[EquitiesAdjustment]):
    """Loader for stocks and ETFs, which share three things futures do not: the
    split/dividend adjustments, the splits/dividends metafiles, and ticker_range."""

    def download_historical_data(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        ticker_range: str | None = None,
    ) -> Path:
        """This function returns historical data archives (.txt files in csv format which are grouped into zip archives).

        The archive is extracted into a request-scoped folder under the loader's
        raw directory, keyed by every request parameter, and that folder's Path
        is returned. If the folder already exists it is wiped and replaced, so it
        always reflects exactly one archive.

        Parameters
        ----------
        period : Period
            Specifies the period to request data for. 'full' requests the entire historical archive, 'month' requests the last 30 days, 'week' requests the current trading week (starting on Monday), 'day' requests the last trading day.

            To request the full historical archive you also need to specify a ticker_range parameter (see below).
        timeframe : Timeframe
            Specifies the period the timeframe of the data. '1min' will request 1-minute intraday bars, '5min' requests 5-minute bars etc.
            Note : bars with zero volumes are not included
        adjustment : EquitiesAdjustment
            Specifies the type of adjustment. 'adj_split' is data adjusted for splits only, 'adj_splitdiv' is data adjusted for both splits and dividends, 'UNADJUSTED' is raw data without any splits or dividend adjustments. UNADJUSTED data is only available in the 1min and 1day timeframes.
        ticker_range : str | None
            Only to be used when requesting the full historical dataset (ie 'period=full'). This parameter specifies the first letter of the ticker, for example 'ticker_range=C' will request all tickers beginning with the letter C

            This parameter can only be used when requesting the full historical archive (ie 'period=full')
        """
        # the delisted endpoint allows UNADJUSTED on 1min *only* -- same enum,
        # narrower rule, so each endpoint guards its own
        if adjustment is EquitiesAdjustment.UNADJUSTED and timeframe not in (
            Timeframe.MIN_1,
            Timeframe.DAY_1,
        ):
            raise ValueError(
                "UNADJUSTED data is only available in the 1min and 1day timeframes"
            )
        if period is Period.FULL and ticker_range is None:
            raise ValueError("ticker_range (A-Z) is required when period=full")
        if ticker_range is not None:
            if period is not Period.FULL:
                raise ValueError("ticker_range can only be used when period=full")
            ticker_range = ticker_range.upper()
            if len(ticker_range) != 1 or not ticker_range.isalpha():
                raise ValueError("ticker_range must be a single letter A-Z")

        return self._historical_data_query(period, timeframe, adjustment, ticker_range)

    # Splits / Dividends Requests --------------------------------------

    def download_splits(self) -> Path:
        """Historical splits: {date,split-ratio}, ratio of new to old shares."""
        return self._download_metafile(MetaFileType.SPLITS)

    def download_dividends(self) -> Path:
        """Historical dividends: {ex-dividend date,dividend amount}."""
        return self._download_metafile(MetaFileType.DIVIDENDS)
