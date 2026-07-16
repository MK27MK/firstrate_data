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
    MetaDataType,
    Period,
    Timeframe,
)


class Catalog:

    def __init__(self, directory: Path):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"
        self._processed_directory = directory / "processed"

    @classmethod
    def from_env(cls) -> Self:
        load_dotenv()
        data_path = os.getenv("DATA_PATH")
        if data_path is None:
            raise FileNotFoundError("DATA_PATH not found.")
        return cls(Path(data_path))

    def get_bars_path(
        self,
        asset_type: AssetType,
        period: Period,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment,
    ):
        return (
            self._raw_directory
            / asset_type
            / period.value
            / timeframe.value
            / adjustment.value
        )

    def get_metadata_path(
        self, asset_type: AssetType, metafile_type: MetaDataType
    ) -> Path:
        return self._raw_directory / asset_type.value / "meta" / metafile_type.value

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
        """Unzips `content` and writes the bars to the right path.

        Parameters
        ----------
        content : bytes
            _description_
        asset_type : AssetType
            _description_
        period : Period
            _description_
        timeframe : Timeframe
            _description_
        adjustment : EquitiesAdjustment | ContinuousFuturesAdjustment
            _description_
        ticker_range : str | None, optional
            _description_, by default None

        Returns
        -------
        Path
            _description_
        """

        target = self.get_bars_path(asset_type, period, timeframe, adjustment)

        if ticker_range is not None:
            target = target / ticker_range

        self._unzip_and_write(zip_file, target)

        return target

    def write_raw_metadata(
        self, content: bytes, asset_type: AssetType, metadata_type: MetaDataType
    ) -> Path:

        target = self.get_metadata_path(asset_type, metadata_type)
        target.mkdir(parents=True, exist_ok=True)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            csv_path = target / f"{metadata_type.value}.csv"
            csv_path.write_bytes(content)
            return csv_path

        self._unzip_and_write(content, target)

        return target

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _unzip_and_write(self, zip_file: bytes, target: Path) -> None:

        # unzip into a sibling and swap it in, so `target` either holds one whole
        # archive or does not exist. Extracting in place would let a Ctrl-C land
        # mid-unzip and leave a populated-but-partial folder, which skip_existing
        # would then read as finished and never re-fetch.
        staging = target.with_name(f"{target.name}.partial")
        if staging.exists():
            shutil.rmtree(staging)
        staging.mkdir(parents=True)
        with zipfile.ZipFile(io.BytesIO(zip_file)) as archive:
            archive.extractall(staging)

        if target.exists():
            shutil.rmtree(target)
        staging.replace(target)
