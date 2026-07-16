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
)
from firstrate_data.request import (
    BarsRequest,
    ContractsRequest,
    DelistedRequest,
    MetafileRequest,
)


class Catalog:
    """The on-disk store: where a request's archive lands, and how.

    Domain-aware on purpose -- it takes request objects and derives their layout
    rather than being handed a path -- so that the mapping from request to
    location is stated once, here, and a read side can later be added against the
    same keys. See ADR 0004.
    """

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
        request: BarsRequest[EquitiesAdjustment | ContinuousFuturesAdjustment],
    ) -> Path:
        """Where one bars request's archive lives, keyed by every field it has.

        ``ticker_range`` included: a key derived from a subset of the request is
        a key two distinct requests can share, and the one that arrives second
        overwrites the first.
        """
        segments = [
            request.asset_type.value,
            request.period.value,
            request.timeframe.value,
            request.adjustment.value,
        ]

        if request.ticker_range is not None:
            segments.append(request.ticker_range)

        return self.get_raw_path(*segments)

    def get_metadata_path(self, request: MetafileRequest) -> Path:
        return self.get_raw_path(
            request.asset_type.value, "meta", request.metadata_type.value
        )

    def get_delisted_path(self, request: DelistedRequest) -> Path:
        return self.get_raw_path(
            AssetType.STOCK,
            "delisted",
            request.kind,
            request.selector.value,
            request.timeframe.value,
            request.adjustment.value,
        )

    def get_contracts_path(self, request: ContractsRequest) -> Path:
        return self.get_raw_path(
            AssetType.FUTURES,
            "contracts",
            request.contract_files.value,
            request.timeframe.value,
        )

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def write_raw_bars(
        self,
        zip_file: bytes,
        request: BarsRequest[EquitiesAdjustment | ContinuousFuturesAdjustment],
    ) -> Path:
        target = self.get_bars_path(request)
        self._unzip_and_write(zip_file, target)

        return target

    def write_raw_metadata(self, content: bytes, request: MetafileRequest) -> Path:

        target = self.get_metadata_path(request)
        target.mkdir(parents=True, exist_ok=True)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            csv_path = target / f"{request.metadata_type.value}.csv"
            csv_path.write_bytes(content)
            return csv_path

        self._unzip_and_write(content, target)

        return target

    def write_raw_delisted(self, zip_file: bytes, request: DelistedRequest) -> Path:
        target = self.get_delisted_path(request)
        self._unzip_and_write(zip_file, target)

        return target

    def write_raw_contracts(self, zip_file: bytes, request: ContractsRequest) -> Path:
        target = self.get_contracts_path(request)
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
