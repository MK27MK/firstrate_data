import io
import os
import shutil
import zipfile
from pathlib import Path
from typing import Self

from dotenv import load_dotenv

from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    MetaFileType,
    Period,
    Timeframe,
)


class Catalog:
    def __init__(self, directory: Path):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"

    @classmethod
    def from_env(cls) -> Self:
        load_dotenv()
        data_path = os.getenv("DATA_PATH")
        if data_path is None:
            raise FileNotFoundError("DATA_PATH not found.")
        return cls(Path(data_path))

    def get_raw_path(self, *segments: str) -> Path:
        """Build a path under the raw store from ordered segments.

        The single seam every path helper below goes through, so callers never
        touch ``_raw_directory`` and asset-specific sub-trees (contracts,
        delisted) can be keyed without a bespoke method each.
        """
        return self._raw_directory.joinpath(*segments)

    def get_bars_path(
        self,
        asset_type: AssetType,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment,
    ) -> Path:
        return self.get_raw_path(
            asset_type.value, period.value, timeframe.value, adjustment.value
        )

    def get_metadata_path(
        self, asset_type: AssetType, metafile_type: MetaFileType
    ) -> Path:
        return self.get_raw_path(asset_type.value, "meta", metafile_type.value)

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def write_raw_bars(
        self,
        zip_file: bytes,
        asset_type: AssetType,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment,
        ticker_range: str | None = None,
    ) -> Path:
        """Unzip a bars archive into its request-scoped folder and return it.

        The folder is keyed by every request parameter (``ticker_range`` too,
        when the asset type uses it), so distinct requests never share a path and
        one range's archive cannot clobber another's.
        """

        target = self.get_bars_path(asset_type, period, timeframe, adjustment)

        if ticker_range is not None:
            target = target / ticker_range

        self._unzip_and_write(zip_file, target)

        return target

    def write_raw_metadata(
        self, content: bytes, asset_type: AssetType, metafile_type: MetaFileType
    ) -> Path:

        target = self.get_metadata_path(asset_type, metafile_type)
        target.mkdir(parents=True, exist_ok=True)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            csv_path = target / f"{metafile_type.value}.csv"
            csv_path.write_bytes(content)
            return csv_path

        self._unzip_and_write(content, target)

        return target

    def write_raw_archive(self, zip_file: bytes, target: Path) -> Path:
        """Unzip an archive into an arbitrary raw sub-path and return it.

        For asset-specific zip endpoints (futures contracts, delisted stocks)
        whose folder layout has no dedicated ``get_*_path``; the caller builds
        ``target`` via :meth:`get_raw_path`.
        """
        self._unzip_and_write(zip_file, target)
        return target

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _unzip_and_write(self, zip_file: bytes, target: Path) -> None:

        # unzip into a sibling and swap it in, so `target` either holds one whole
        # archive or does not exist. Extracting in place would let a Ctrl-C land
        # mid-unzip and leave a populated-but-partial folder that a later resume
        # would read as finished and never re-fetch.
        staging = target.with_name(f"{target.name}.partial")
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(zip_file)) as archive:
            archive.extractall(staging)

        if target.exists():
            shutil.rmtree(target)
        staging.replace(target)
