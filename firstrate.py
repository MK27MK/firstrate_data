import io
import os
import shutil
import zipfile
from pathlib import Path
from typing import ClassVar, Self

import requests
from dotenv import load_dotenv

from query_parameters import Adjustment, AssetType, MetaFileType, Period, Timeframe

DEFAULT_BASE_URL = "https://firstratedata.com/api"


class FirstRateData:
    """Base loader: fetches FirstRate Data archives and persists them into a
    managed directory. Subclasses fix ``_asset_type`` so callers never pass it."""

    _asset_type: ClassVar[AssetType]

    def __init__(self, directory: Path, userid: str, base_url: str = DEFAULT_BASE_URL):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"
        self._userid = userid
        self._base_url = base_url.rstrip("/")

    @classmethod
    def from_data_path(cls) -> Self:
        load_dotenv()
        data_path = os.getenv("DATA_PATH")
        if data_path is None:
            raise FileNotFoundError("DATA_PATH not found.")
        userid = os.getenv("FIRSTRATE_USERID")
        if userid is None:
            raise KeyError("FIRSTRATE_USERID not found.")
        base_url = os.getenv("FIRSTRATE_BASE_URL", DEFAULT_BASE_URL)
        return cls(Path(data_path), userid=userid, base_url=base_url)

    # Historical Data Requests -----------------------------------------

    def download_historical_data(
        self,
        period: Period,
        timeframe: Timeframe,
        adjustment: Adjustment,
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
        adjustment : Adjustment
            Specifies the price adjustment applied to the data. 'adj_split' adjusts for splits only, 'adj_splitdiv' adjusts for both splits and dividends, 'UNADJUSTED' returns raw prices.
        ticker_range : str | None
            Only to be used when requesting the full historical dataset (ie 'period=full'). This parameter specifies the first letter of the ticker, for example 'ticker_range=C' will request all tickers beginning with the letter C

            This parameter can only be used when requesting the full historical archive (ie 'period=full')
        """
        if period is Period.FULL and ticker_range is None:
            raise ValueError("ticker_range (A-Z) is required when period=full")
        if ticker_range is not None:
            if period is not Period.FULL:
                raise ValueError("ticker_range can only be used when period=full")
            ticker_range = ticker_range.upper()
            if len(ticker_range) != 1 or not ticker_range.isalpha():
                raise ValueError("ticker_range must be a single letter A-Z")

        params = {
            "type": self._asset_type.value,
            "period": period.value,
            "timeframe": timeframe.value,
            "adjustment": adjustment.value,
            "userid": self._userid,
        }
        if ticker_range is not None:
            params["ticker_range"] = ticker_range

        response = requests.get(
            f"{self._base_url}/data_file", params=params, timeout=120
        )
        response.raise_for_status()

        target = (
            self._raw_directory
            / self._asset_type.value
            / period.value
            / timeframe.value
            / adjustment.value
        )
        if ticker_range is not None:
            target = target / ticker_range

        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            archive.extractall(target)
        return target


class FirstRateCorporateActions(FirstRateData):
    """Loader for asset types that have corporate actions.

    The meta_file endpoint advertises every asset type, but its own docs say
    splits and dividends only exist for stocks and ETFs -- so only those
    loaders inherit this."""

    # Splits / Dividends Requests --------------------------------------

    def _download_metafile(self, metafile_type: MetaFileType) -> Path:
        """Fetch a metafile and persist it under ``raw/{asset}/meta/``.

        Splits rows are {date,split-ratio} and dividend rows are
        {ex-dividend date,dividend amount}, both with dates as yyyy-MM-dd.
        """
        params = {
            "type": self._asset_type.value,
            "metafile_type": metafile_type.value,
            "userid": self._userid,
        }
        response = requests.get(
            f"{self._base_url}/meta_file", params=params, timeout=120
        )
        response.raise_for_status()

        target = self._raw_directory / self._asset_type.value / "meta"
        target.mkdir(parents=True, exist_ok=True)

        # ponytail: the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        body = io.BytesIO(response.content)
        if not zipfile.is_zipfile(body):
            csv_path = target / f"{metafile_type.value}.csv"
            csv_path.write_bytes(response.content)
            return csv_path

        target = target / metafile_type.value
        if target.exists():
            shutil.rmtree(target)
        target.mkdir()
        with zipfile.ZipFile(body) as archive:
            archive.extractall(target)
        return target

    def download_splits(self) -> Path:
        """Historical splits: {date,split-ratio}, ratio of new to old shares."""
        return self._download_metafile(MetaFileType.SPLITS)

    def download_dividends(self) -> Path:
        """Historical dividends: {ex-dividend date,dividend amount}."""
        return self._download_metafile(MetaFileType.DIVIDENDS)


# ----------------------------------------------------------------------
# test
# ----------------------------------------------------------------------


def _demo() -> None:
    """Self-check: no network. Feed a fake zip through the extract/persist path."""
    import tempfile

    from stock import FirstRateStocks  # local: stock.py imports this module

    class _FakeResponse:
        def __init__(self, content: bytes):
            self.content = content

        def raise_for_status(self) -> None:
            pass

    def _make_zip(names: list[str]) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for name in names:
                z.writestr(name, "t,o,h,l,c,v\n")
        return buf.getvalue()

    sent: list[dict[str, str]] = []
    names: list[str] = []
    raw: list[bytes] = []  # when set, served instead of a zip of `names`

    def _fake_get(url: str, params: dict[str, str], timeout: int) -> _FakeResponse:
        sent.append(params)
        return _FakeResponse(raw[0] if raw else _make_zip(names))

    orig_get = requests.get
    requests.get = _fake_get  # type: ignore[assignment]
    try:
        with tempfile.TemporaryDirectory() as tmp:
            loader = FirstRateStocks(Path(tmp), userid="X")

            # period=full requires ticker_range
            try:
                loader.download_historical_data(
                    Period.FULL, Timeframe.MIN_1, Adjustment.SPLIT
                )
                raise AssertionError(
                    "expected ValueError for full without ticker_range"
                )
            except ValueError:
                pass

            # ticker_range without full is rejected
            try:
                loader.download_historical_data(
                    Period.DAY, Timeframe.MIN_1, Adjustment.SPLIT, ticker_range="A"
                )
                raise AssertionError(
                    "expected ValueError for ticker_range without full"
                )
            except ValueError:
                pass

            # happy path: full + ticker_range -> nested, ticker-suffixed folder
            names[:] = ["AAPL.txt", "ABBV.txt"]
            out = loader.download_historical_data(
                Period.FULL, Timeframe.MIN_1, Adjustment.SPLIT, ticker_range="a"
            )
            assert out == Path(tmp) / "raw/stock/full/1min/adj_split/A", out
            assert (out / "AAPL.txt").exists()
            assert sent[-1]["type"] == "stock"
            assert sent[-1]["ticker_range"] == "A"

            # clean + replace: rerun with a different ticker set, stale files gone
            names[:] = ["MSFT.txt"]
            out2 = loader.download_historical_data(
                Period.FULL, Timeframe.MIN_1, Adjustment.SPLIT, ticker_range="A"
            )
            assert out2 == out
            assert (out / "MSFT.txt").exists()
            assert not (out / "AAPL.txt").exists(), "clean+replace left stale files"

            # non-full: no ticker_range segment
            names[:] = ["AAPL.txt"]
            out3 = loader.download_historical_data(
                Period.DAY, Timeframe.MIN_1, Adjustment.SPLIT
            )
            assert out3 == Path(tmp) / "raw/stock/day/1min/adj_split", out3

            # metafile, plain-csv body -> a file under meta/
            raw[:] = [b"2020-08-31,4\n"]
            splits = loader.download_splits()
            assert splits == Path(tmp) / "raw/stock/meta/splits.csv", splits
            assert splits.read_bytes() == raw[0]
            assert sent[-1]["metafile_type"] == "splits"
            assert sent[-1]["type"] == "stock"

            # rerun overwrites rather than appends
            raw[:] = [b"2020-08-31,4\n2014-06-09,7\n"]
            assert loader.download_splits().read_bytes() == raw[0]

            # metafile, zipped body -> a folder under meta/
            raw[:] = [_make_zip(["dividends.txt"])]
            dividends = loader.download_dividends()
            assert dividends == Path(tmp) / "raw/stock/meta/dividends", dividends
            assert (dividends / "dividends.txt").exists()
            assert sent[-1]["metafile_type"] == "dividends"
    finally:
        requests.get = orig_get  # type: ignore[assignment]
    print("ok")


if __name__ == "__main__":
    _demo()
