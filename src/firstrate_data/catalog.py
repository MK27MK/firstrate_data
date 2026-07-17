import io
import json
import os
import shutil
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from glob import iglob
from pathlib import Path
from typing import Literal, Self

import duckdb
from dotenv import load_dotenv

from firstrate_data import ingest
from firstrate_data.manifest import Manifest
from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    Dataset,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.request import (
    AnyBarsRequest,
    BarsRequest,
    ContractsRequest,
    DelistedRequest,
    MetafileRequest,
    StoredRequest,
    request_from_params,
)

# rides in the snapshot directory it describes, recording which request
# produced it, so sync() reads provenance instead of regexing paths apart
_SNAPSHOT_FILE = "_snapshot.json"


@dataclass(frozen=True, slots=True)
class RawSnapshot:
    """One raw directory: a request's archive, dated by when it was fetched."""

    directory: Path
    source: str
    request: StoredRequest
    snapshot_date: date


@dataclass(frozen=True, slots=True)
class _Plan:
    """One raw directory, resolved to where its bars go and how they land."""

    raw: RawSnapshot
    asset_type: AssetType
    dataset: Dataset
    adjustment: str
    timeframe: Timeframe
    # whether this archive is the whole history of every ticker it names, and so
    # supersedes what the partition holds rather than extending it
    replaces: bool
    restated: bool

    @property
    def group(self) -> tuple[AssetType, Dataset, str, Timeframe]:
        return (self.asset_type, self.dataset, self.adjustment, self.timeframe)


@dataclass
class SyncReport:
    """The outcome of a sync, directory by directory.

    A skipped directory is one the layout has no place for, and re-running
    will never change that; a failed one should have ingested and didn't.
    """

    ingested: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, Exception]] = field(default_factory=list)


def _classify(raw: RawSnapshot) -> _Plan | str:
    """Where a raw directory's bars belong, or why they have nowhere to go."""
    match raw.request:
        case BarsRequest() as request:
            plan = _Plan(
                raw,
                request.asset_type,
                request.dataset,
                request.adjustment.value,
                request.timeframe,
                replaces=request.period is Period.FULL,
                restated=request.adjustment.is_restated,
            )
        case DelistedRequest() as request:
            # every delisted payload is one ticker's entire history, whichever
            # selector asked for it, so a delisted fetch replaces its
            # partitions -- which also keeps `update=week` from double-counting
            # against `update=year`, of which it is a strict subset
            plan = _Plan(
                raw,
                AssetType.STOCK,
                Dataset.DELISTED,
                request.adjustment.value,
                request.timeframe,
                replaces=True,
                restated=request.adjustment.is_restated,
            )
        case ContractsRequest():
            return "individual contracts are not in the v1 layout"
        case MetafileRequest():
            return "a metafile is corporate actions, not bars"

    if plan.restated and not plan.replaces:
        return (
            f"{plan.adjustment} is restated: appending an increment would splice "
            "two adjustment bases together. Re-fetch period=full instead."
        )

    return plan


class Catalog:
    """The on-disk store: raw archives in, a queryable parquet catalog out.

    Downloads land under ``raw/`` as dated snapshots; ``sync()`` converts them
    into a Hive-partitioned parquet tree, which the read methods serve as lazy
    DuckDB relations. While raw snapshots are on disk, the parquet tree can be
    deleted and rebuilt from them with no network.
    """

    def __init__(self, directory: Path):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"
        # the derived side: Hive-partitioned parquet
        self._parquet_directory = directory / "parquet"
        self._connection = duckdb.connect()
        self._manifest = Manifest(directory / "manifest.parquet", self._connection)

    @classmethod
    def from_env(cls) -> Self:
        load_dotenv()
        data_path = os.getenv("DATA_PATH")
        if data_path is None:
            raise FileNotFoundError("DATA_PATH not found.")
        return cls(Path(data_path))

    def _raw_path(self, *segments: str) -> Path:
        return self._raw_directory.joinpath(*segments)

    def get_bars_path(self, request: AnyBarsRequest, snapshot_date: date) -> Path:
        """Where one bars request's archive lives, keyed by every field it has,
        under the date it was fetched."""
        segments = [
            request.asset_type.value,
            request.period.value,
            request.timeframe.value,
            request.adjustment.value,
        ]

        if request.ticker_range is not None:
            segments.append(request.ticker_range)

        segments.append(snapshot_date.isoformat())

        return self._raw_path(*segments)

    def get_metadata_path(self, request: MetafileRequest, snapshot_date: date) -> Path:
        return self._raw_path(
            request.asset_type.value,
            "meta",
            request.metadata_type.value,
            snapshot_date.isoformat(),
        )

    def get_delisted_path(self, request: DelistedRequest, snapshot_date: date) -> Path:
        return self._raw_path(
            AssetType.STOCK,
            "delisted",
            request.kind,
            request.selector.value,
            request.timeframe.value,
            request.adjustment.value,
            snapshot_date.isoformat(),
        )

    def get_contracts_path(
        self, request: ContractsRequest, snapshot_date: date
    ) -> Path:
        return self._raw_path(
            AssetType.FUTURES,
            "contracts",
            request.contract_files.value,
            request.timeframe.value,
            snapshot_date.isoformat(),
        )

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def write_raw_bars(
        self, zip_file: bytes, request: AnyBarsRequest, snapshot_date: date
    ) -> Path:
        target = self.get_bars_path(request, snapshot_date)
        self._unzip_and_write(zip_file, target, request, snapshot_date)

        # retention is one number: one date for a `full`, every date for the
        # increments. A `full` wholly contains the one before it; a week of
        # 1min bars is a rounding error next to it.
        if request.period is Period.FULL:
            self._drop_superseded_snapshots(target, snapshot_date)

        return target

    def write_raw_metadata(
        self, content: bytes, request: MetafileRequest, snapshot_date: date
    ) -> Path:
        target = self.get_metadata_path(request, snapshot_date)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            target.mkdir(parents=True, exist_ok=True)
            csv_path = target / f"{request.metadata_type.value}.csv"
            csv_path.write_bytes(content)
            self._write_snapshot_record(target, request, snapshot_date)
            return csv_path

        self._unzip_and_write(content, target, request, snapshot_date)

        return target

    def write_raw_delisted(
        self, zip_file: bytes, request: DelistedRequest, snapshot_date: date
    ) -> Path:
        target = self.get_delisted_path(request, snapshot_date)
        self._unzip_and_write(zip_file, target, request, snapshot_date)

        return target

    def write_raw_contracts(
        self, zip_file: bytes, request: ContractsRequest, snapshot_date: date
    ) -> Path:
        target = self.get_contracts_path(request, snapshot_date)
        self._unzip_and_write(zip_file, target, request, snapshot_date)

        return target

    # ------------------------------------------------------------------
    # sync: raw/ -> parquet
    # ------------------------------------------------------------------

    def sync(self) -> SyncReport:
        """Bring the parquet tree up to date with ``raw/``.

        Scans the raw snapshots, diffs against the manifest, and ingests what
        is missing -- fulls first, increments after. Idempotent, so it is also
        the rebuild and the cure for an interruption.

        Returns
        -------
        SyncReport
            Every directory's outcome. Nothing raises: a sync over a
            hundreds-of-GB archive is too long to abandon over one bad file.
        """
        report = SyncReport()
        self._manifest.load()

        ingested = self._manifest.sources()
        planned, skipped = self._plan(self._scan_raw(), ingested)
        report.skipped.extend(skipped)

        # a replay's old rows describe partitions that are about to be dropped
        self._manifest.forget(
            {p.raw.source for p in planned if p.raw.source in ingested}
        )

        for plan in planned:
            try:
                self._ingest(plan)
                report.ingested.append(plan.raw.source)
            except (duckdb.Error, OSError) as error:
                report.failed.append((plan.raw.source, error))

        self._manifest.flush()

        return report

    def _scan_raw(self) -> list[RawSnapshot]:
        """Every raw snapshot on disk, read from its record file."""
        found: list[RawSnapshot] = []
        if not self._raw_directory.exists():
            return found

        for record_path in sorted(self._raw_directory.rglob(_SNAPSHOT_FILE)):
            recorded = json.loads(record_path.read_text())
            found.append(
                RawSnapshot(
                    directory=record_path.parent,
                    source=str(record_path.parent.relative_to(self._raw_directory)),
                    request=request_from_params(
                        recorded["endpoint"], recorded["params"]
                    ),
                    snapshot_date=date.fromisoformat(recorded["snapshot_date"]),
                )
            )
        return found

    def _plan(
        self, raws: list[RawSnapshot], ingested: set[str]
    ) -> tuple[list[_Plan], list[tuple[str, str]]]:
        """What to ingest, in the order the snapshot rule requires."""
        groups: dict[tuple[AssetType, Dataset, str, Timeframe], list[_Plan]] = (
            defaultdict(list)
        )
        skipped: list[tuple[str, str]] = []

        for raw in raws:
            outcome = _classify(raw)
            if isinstance(outcome, str):
                skipped.append((raw.source, outcome))
                continue
            groups[outcome.group].append(outcome)

        planned: list[_Plan] = []
        for members in groups.values():
            # a full before the increments that extend it, so that the full's
            # replace does not drop rows laid down moments earlier
            members.sort(key=lambda plan: (plan.raw.snapshot_date, not plan.replaces))

            arriving = min(
                (
                    plan.raw.snapshot_date
                    for plan in members
                    if plan.replaces and plan.raw.source not in ingested
                ),
                default=None,
            )

            for member in members:
                if member.raw.source not in ingested:
                    planned.append(member)
                elif (
                    arriving is not None
                    and not member.replaces
                    and member.raw.snapshot_date > arriving
                ):
                    # already ingested, but a full is about to land underneath it
                    # and take its rows with the partition. Lay it down again.
                    planned.append(member)

        return planned, skipped

    def _ingest(self, plan: _Plan) -> None:
        tickers = [
            row[0]
            for row in self._connection.sql(
                ingest.directory_tickers(plan.raw.directory, plan.dataset)
            ).fetchall()
        ]

        if plan.replaces:
            self._drop_partitions(plan, tickers)

        select = ingest.bars_select(
            plan.raw.directory,
            plan.asset_type,
            plan.dataset,
            plan.adjustment,
            plan.timeframe,
            self._has_open_interest(plan.raw.directory),
        )
        if not plan.replaces:
            select = self._watermarked(select, plan)

        snapshot = plan.raw.snapshot_date.isoformat()
        self._connection.sql(
            f"""
            COPY ({select})
            TO '{self._parquet_directory}'
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({", ".join(ingest.PARTITION_KEYS)}),
             FILENAME_PATTERN '{ingest.snapshot_filename(snapshot)}',
             OVERWRITE_OR_IGNORE)
            """
        )

        self._record(plan, tickers)

    def _has_open_interest(self, directory: Path) -> bool:
        """Whether this archive's payloads carry the seventh column."""
        # probed from a payload rather than ruled from (asset_type, timeframe):
        # a rule here would be a second source of truth about the vendor's own
        # file, free to drift from it. Counted off a line because the sniffer
        # cannot read these files, and empty payloads are routine -- one
        # non-empty payload speaks for the archive.
        for payload in sorted(directory.glob("*.txt")):
            with payload.open() as lines:
                for line in lines:
                    if line.strip():
                        return line.count(",") + 1 > len(ingest.BAR_COLUMNS)
        return False

    def _watermarked(self, select: str, plan: _Plan) -> str:
        """An increment, minus the rows its partition already holds."""
        # a `week` fetched on a Wednesday re-serves Monday and Tuesday, which
        # the `full` beneath it already carries; a plain append would double
        # those bars silently. The manifest's coverage is the watermark.
        return f"""
            WITH staged AS ({select}),
            watermark AS (
                SELECT ticker, max(last_ts) AS last_ts
                FROM manifest
                WHERE asset_type = '{plan.asset_type.value}'
                  AND dataset = '{plan.dataset.value}'
                  AND adjustment = '{plan.adjustment}'
                  AND timeframe = '{plan.timeframe.value}'
                GROUP BY ticker
            )
            SELECT staged.*
            FROM staged
            LEFT JOIN watermark USING (ticker)
            WHERE watermark.last_ts IS NULL OR staged.ts > watermark.last_ts
        """

    def _drop_partitions(self, plan: _Plan, tickers: list[str]) -> None:
        """Clear the partitions a `full` is about to replace."""
        # by name, rather than ``COPY ... OVERWRITE``, which removes every
        # other ticker's partition too -- the whole root, not the partitions
        # being written. The blast radius has to be ours to state.
        for ticker in tickers:
            partition = ingest.partition_directory(
                self._parquet_directory,
                plan.asset_type.value,
                plan.dataset.value,
                plan.adjustment,
                plan.timeframe.value,
                ticker,
            )
            if partition.exists():
                shutil.rmtree(partition)

    def _record(self, plan: _Plan, tickers: list[str]) -> None:
        """Record what this snapshot left in the tree, one row per partition."""
        snapshot = plan.raw.snapshot_date.isoformat()
        written = ingest.partition_glob(
            self._parquet_directory,
            plan.asset_type.value,
            plan.dataset.value,
            plan.adjustment,
            plan.timeframe.value,
            None,
            f"{snapshot}_*.parquet",
        )

        # an increment whose rows the partition already covers writes no files
        # at all: nothing to record, and the next sync finds the same nothing
        if not tickers or next(iglob(written), None) is None:
            return

        # filtered to this directory's tickers, not the bare glob: two `full`
        # snapshots of different ticker_ranges share every key and filename,
        # and without this the second would claim the first's partitions
        listed = ", ".join(ingest.sql_literal(ticker) for ticker in tickers)
        rows = self._connection.sql(
            f"""
            SELECT
                asset_type, dataset, adjustment, timeframe, ticker,
                DATE '{snapshot}' AS snapshot_date,
                '{plan.raw.source}' AS source,
                min(ts) AS first_ts,
                max(ts) AS last_ts,
                count(*) AS row_count
            FROM read_parquet('{written}', hive_partitioning = true)
            WHERE ticker IN ({listed})
            GROUP BY ALL
            """
        )
        self._manifest.record(rows)

    def _drop_superseded_snapshots(self, kept: Path, snapshot_date: date) -> None:
        """Drop the `full` snapshots this one supersedes, once it is safely on disk."""
        # older only, and dated only: dropping after the new directory lands
        # closes the mid-swap window, and refusing to touch a sibling that is
        # not a snapshot spares an interrupted fetch's staging directory
        for sibling in kept.parent.iterdir():
            if sibling == kept or not sibling.is_dir():
                continue
            try:
                superseded = date.fromisoformat(sibling.name) < snapshot_date
            except ValueError:
                continue
            if superseded:
                shutil.rmtree(sibling)

    # ------------------------------------------------------------------
    # reading methods
    # ------------------------------------------------------------------

    def stock_bars(
        self,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        *,
        dataset: Literal[Dataset.LISTED, Dataset.DELISTED] | None = None,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Stock bars, as a lazy relation.

        Parameters
        ----------
        timeframe : Timeframe
            Bar timeframe to read.
        adjustment : EquitiesAdjustment
            Adjustment the bars were fetched under.
        dataset : Dataset.LISTED or Dataset.DELISTED, optional
            Default spans both, which is the survivorship-free answer;
            narrowing to ``LISTED`` is what costs you the dead tickers.
        ticker : str, optional
            One ticker; default is all of them.

        Returns
        -------
        duckdb.DuckDBPyRelation
            Lazy relation over the matching partitions; empty if none exist.
        """
        return self._read(AssetType.STOCK, dataset, adjustment, timeframe, ticker)

    def futures_bars(
        self,
        timeframe: Timeframe,
        adjustment: ContinuousFuturesAdjustment,
        *,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Futures continuous-series bars, as a lazy relation.

        No ``dataset`` parameter: the continuous series is the only futures
        dataset the v1 layout carries.
        """
        return self._read(
            AssetType.FUTURES, Dataset.CONTINUOUS, adjustment, timeframe, ticker
        )

    def bars(
        self,
        *,
        timeframe: Timeframe | None = None,
        adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment | None = None,
        dataset: Dataset | None = None,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Bars across the whole tree, as a lazy relation.

        Every omitted selector spans all its values, so an unfiltered call
        interleaves adjustments: the same bar appears once per adjustment it
        was fetched under, distinguishable by the ``adjustment`` column.
        Narrow with the selectors rather than a ``WHERE``: they build the
        glob, which is what makes a fine slice fast.
        """
        return self._read(None, dataset, adjustment, timeframe, ticker)

    def _read(
        self,
        asset_type: AssetType | None,
        dataset: Dataset | None,
        adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment | None,
        timeframe: Timeframe | None,
        ticker: str | None,
    ) -> duckdb.DuckDBPyRelation:
        pattern = ingest.partition_glob(
            self._parquet_directory,
            None if asset_type is None else asset_type.value,
            None if dataset is None else dataset.value,
            None if adjustment is None else adjustment.value,
            None if timeframe is None else timeframe.value,
            ticker,
        )

        # a glob that matches nothing is an IOException in DuckDB, and "no
        # data" is an answer: it gets an empty relation of the right shape
        if next(iglob(pattern), None) is None:
            return self._connection.sql(ingest.empty_bars_select())

        return self._connection.sql(
            f"SELECT {ingest.bars_projection()} "
            f"FROM read_parquet('{pattern}', hive_partitioning = true)"
        )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _write_snapshot_record(
        self, directory: Path, request: StoredRequest, snapshot_date: date
    ) -> None:
        recorded = {
            "snapshot_date": snapshot_date.isoformat(),
            "endpoint": request.endpoint,
            "params": request.to_params(),
        }
        (directory / _SNAPSHOT_FILE).write_text(json.dumps(recorded, indent=2))

    def _unzip_and_write(
        self, zip_file: bytes, target: Path, request: StoredRequest, snapshot_date: date
    ) -> None:
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

        # inside the staging directory, so the record swaps in with the
        # payloads it describes and no directory is visible without provenance
        self._write_snapshot_record(staging, request, snapshot_date)

        if target.exists():
            shutil.rmtree(target)
        staging.replace(target)
