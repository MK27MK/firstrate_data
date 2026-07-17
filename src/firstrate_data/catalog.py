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
    BarsRequest,
    ContractsRequest,
    DelistedRequest,
    MetafileRequest,
    StoredRequest,
    request_from_params,
)

# Rides in the vintage directory it describes. It earns its place not as the date
# -- the path already carries that -- but as the record of *which request*
# produced a directory, so sync() reads provenance instead of inverting a path
# with regexes. See ADR 0005.
_SIDECAR = "_vintage.json"

type AnyBarsRequest = BarsRequest[EquitiesAdjustment | ContinuousFuturesAdjustment]


@dataclass(frozen=True, slots=True)
class RawVintage:
    """One raw directory, and the provenance its sidecar records."""

    directory: Path
    source: str
    request: StoredRequest
    vintage: date


@dataclass(frozen=True, slots=True)
class _Plan:
    """One raw directory, resolved to where its bars go and how they land."""

    raw: RawVintage
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

    ``skipped`` and ``failed`` are different things, as in a bundle sweep: a
    skipped directory is one the layout has no place for, or one whose vintage
    cannot be reconciled, and re-running will never change that; a failed one is
    a directory that should have ingested and didn't.
    """

    ingested: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, Exception]] = field(default_factory=list)


def _classify(raw: RawVintage) -> _Plan | str:
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
            # every delisted payload is one ticker's entire history -- the vendor
            # names them `..._full_...` whichever selector asked for them -- so a
            # delisted fetch replaces its partitions rather than extending them.
            # That is also what keeps `update=week` from double-counting against
            # `update=year`, of which it is a strict subset.
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
            return (
                "individual contracts are not in the v1 layout: a contract has no "
                "adjustment, since adjustment exists to erase roll jumps and a "
                "contract is the thing being stitched. dataset=contract is the seam."
            )
        case MetafileRequest():
            return (
                "a metafile is corporate actions, not bars: it has no ticker and no "
                "timeframe, and so no partition in a tree keyed by both."
            )
        case _:
            return "the store has no ingest rule for this request"

    if plan.restated and not plan.replaces:
        return (
            f"{plan.adjustment} is restated, and this is an increment. Appending it "
            "would weld two adjustment bases together and splice in a price move that "
            "never happened. ADR 0005 proposed a metafile guard to decide this case "
            "and its own measurement refuted the guard -- the restatement lags the "
            "metafile by an unpredictable per-ticker interval, so a metafile row "
            "cannot time the append. Re-fetch period=full instead."
        )

    return plan


class Catalog:
    """The on-disk store: where a request's archive lands, how, and how it reads.

    Domain-aware on purpose -- it takes request objects and derives their layout
    rather than being handed a path -- so that the mapping from request to
    location is stated once, here. ADR 0004 kept it that way against the day a
    read side arrived; that day is ADR 0005, and the wager paid: a blind store
    cannot build the glob that makes a fine-grained read 970x faster than a scan.

    ``raw/`` is the truth and is never deleted or rewritten in place. The parquet
    tree is a projection of it: ``rm -rf`` the tree and ``sync()`` rebuilds it
    with no network.
    """

    def __init__(self, directory: Path):
        self._directory = directory
        # where the unzipped .txt data in csv is kept
        self._raw_directory = directory / "raw"
        # the derived side: Hive-partitioned parquet, rebuildable from raw/
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

    def get_raw_path(self, *segments: str) -> Path:
        """Build a path under the raw store from ordered segments.

        The single seam every path helper below goes through, so callers never
        touch ``_raw_directory`` and asset-specific sub-trees (contracts,
        delisted) can be keyed without a bespoke method each.
        """
        return self._raw_directory.joinpath(*segments)

    def get_bars_path(self, request: AnyBarsRequest, vintage: date) -> Path:
        """Where one bars request's archive lives, keyed by every field it has,
        under the date it was fetched.

        ``ticker_range`` included: a key derived from a subset of the request is
        a key two distinct requests can share, and the one that arrives second
        overwrites the first.

        The vintage is a second argument rather than a field of the request
        because a request is the parameters identifying *a slice of the dataset*,
        and when we asked identifies neither a slice nor the dataset. It stays
        exactly the wire contract. This is not ADR 0004's clobber returning: that
        defect was a key computed from a subset by a method blind to a field, and
        ``--strict`` cannot let this argument be omitted.
        """
        segments = [
            request.asset_type.value,
            request.period.value,
            request.timeframe.value,
            request.adjustment.value,
        ]

        if request.ticker_range is not None:
            segments.append(request.ticker_range)

        segments.append(vintage.isoformat())

        return self.get_raw_path(*segments)

    def get_metadata_path(self, request: MetafileRequest, vintage: date) -> Path:
        return self.get_raw_path(
            request.asset_type.value,
            "meta",
            request.metadata_type.value,
            vintage.isoformat(),
        )

    def get_delisted_path(self, request: DelistedRequest, vintage: date) -> Path:
        return self.get_raw_path(
            AssetType.STOCK,
            "delisted",
            request.kind,
            request.selector.value,
            request.timeframe.value,
            request.adjustment.value,
            vintage.isoformat(),
        )

    def get_contracts_path(self, request: ContractsRequest, vintage: date) -> Path:
        return self.get_raw_path(
            AssetType.FUTURES,
            "contracts",
            request.contract_files.value,
            request.timeframe.value,
            vintage.isoformat(),
        )

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def write_raw_bars(
        self, zip_file: bytes, request: AnyBarsRequest, vintage: date
    ) -> Path:
        target = self.get_bars_path(request, vintage)
        self._unzip_and_write(zip_file, target, request, vintage)

        # retention is one number: one date for a `full`, every date for the
        # increments. A `full` is hundreds of GB and wholly contains the one
        # before it; a week of 1min bars is a rounding error. The asymmetry is a
        # policy over a uniform layout, not a second code path.
        if request.period is Period.FULL:
            self._drop_superseded_vintages(target, vintage)

        return target

    def write_raw_metadata(
        self, content: bytes, request: MetafileRequest, vintage: date
    ) -> Path:
        target = self.get_metadata_path(request, vintage)

        # the docs give the row format but never the container, so
        # accept either. Drop the zip branch once the live endpoint is pinned.
        if not zipfile.is_zipfile(io.BytesIO(content)):
            target.mkdir(parents=True, exist_ok=True)
            csv_path = target / f"{request.metadata_type.value}.csv"
            csv_path.write_bytes(content)
            self._write_sidecar(target, request, vintage)
            return csv_path

        self._unzip_and_write(content, target, request, vintage)

        return target

    def write_raw_delisted(
        self, zip_file: bytes, request: DelistedRequest, vintage: date
    ) -> Path:
        target = self.get_delisted_path(request, vintage)
        self._unzip_and_write(zip_file, target, request, vintage)

        return target

    def write_raw_contracts(
        self, zip_file: bytes, request: ContractsRequest, vintage: date
    ) -> Path:
        target = self.get_contracts_path(request, vintage)
        self._unzip_and_write(zip_file, target, request, vintage)

        return target

    # ------------------------------------------------------------------
    # sync: raw/ -> parquet
    # ------------------------------------------------------------------

    def sync(self) -> SyncReport:
        """Bring the parquet tree up to date with ``raw/``. The whole update surface.

        Scans raw, reads the sidecars, diffs against the manifest, and ingests
        what is missing in vintage order -- fulls first, increments after. It is
        idempotent, so it is also the rebuild and also the cure for a Ctrl-C, and
        it is what makes an archive already on disk ingestable at all, which a
        download-time hook never could. Coupling ingest into ``download_*`` would
        re-fuse the transport and the store that ADR 0004 separated, and would
        still need reconciliation after any interruption.

        Nothing raises: every directory's outcome lands in the returned report,
        since a sync over a hundreds-of-GB archive is far too long to abandon
        over one bad file.
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

    def _scan_raw(self) -> list[RawVintage]:
        """Every raw vintage on disk, read from its sidecar."""
        found: list[RawVintage] = []
        if not self._raw_directory.exists():
            return found

        for sidecar in sorted(self._raw_directory.rglob(_SIDECAR)):
            recorded = json.loads(sidecar.read_text())
            found.append(
                RawVintage(
                    directory=sidecar.parent,
                    source=str(sidecar.parent.relative_to(self._raw_directory)),
                    request=request_from_params(
                        recorded["endpoint"], recorded["params"]
                    ),
                    vintage=date.fromisoformat(recorded["vintage"]),
                )
            )
        return found

    def _plan(
        self, raws: list[RawVintage], ingested: set[str]
    ) -> tuple[list[_Plan], list[tuple[str, str]]]:
        """What to ingest, in the order the vintage rule requires."""
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
            members.sort(key=lambda plan: (plan.raw.vintage, not plan.replaces))

            arriving = min(
                (
                    plan.raw.vintage
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
                    and member.raw.vintage > arriving
                ):
                    # already ingested, but a full is about to land underneath it
                    # and take its rows with the partition. Lay it down again.
                    planned.append(member)

        return planned, skipped

    def _ingest(self, plan: _Plan) -> None:
        # which tickers this directory carries, named once and reused by the
        # partition drop and the manifest's provenance filter below
        self._connection.sql(
            f"CREATE OR REPLACE TEMP VIEW _dir_tickers AS "
            f"{ingest.directory_tickers(plan.raw.directory, plan.dataset)}"
        )

        if plan.replaces:
            self._drop_partitions(plan)

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

        self._connection.sql(
            f"""
            COPY ({select})
            TO '{self._parquet_directory}'
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({", ".join(ingest.PARTITION_KEYS)}),
             FILENAME_PATTERN '{ingest.vintage_filename(plan.raw.vintage.isoformat())}',
             OVERWRITE_OR_IGNORE)
            """
        )

        self._record(plan)

    def _has_open_interest(self, directory: Path) -> bool:
        """Whether this archive's payloads carry the seventh column.

        Probed from a payload rather than ruled from (asset_type, timeframe). The
        vendor documents open interest as futures-1day-only and the archives
        agree, but a rule written here would be a second source of truth about
        the vendor's own file, free to drift from it. The file is the truth.

        Counted off a line rather than asked of DuckDB's sniffer, which cannot
        read these files at all: the payloads mix line endings and empty ones are
        routine -- three of the 132 futures payloads in one measured archive have
        no bars. One payload speaks for the archive, since a request returns one
        shape and it is the shapes *across* requests that diverge.
        """
        for payload in sorted(directory.glob("*.txt")):
            with payload.open() as lines:
                for line in lines:
                    if line.strip():
                        return line.count(",") + 1 > len(ingest.BAR_COLUMNS)
        return False

    def _watermarked(self, select: str, plan: _Plan) -> str:
        """An increment, minus the rows its partition already holds.

        A `week` fetched on a Wednesday re-serves Monday and Tuesday, which the
        `full` beneath it already carries, so a plain append would double those
        bars -- silently, since a duplicate bar is a valid-looking bar. The
        manifest's coverage is the watermark, per ticker: this is what recording
        coverage on a manifest row is *for*.
        """
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

    def _drop_partitions(self, plan: _Plan) -> None:
        """Clear the partitions a `full` is about to replace.

        By name, rather than ``COPY ... OVERWRITE``, which was measured doing far
        more than it says: writing a single ticker under OVERWRITE removed every
        *other* ticker's partition too -- the whole root, not the partitions
        being written. A `full` for one ticker_range would have taken the other
        twenty-five with it. The blast radius has to be ours to state.
        """
        rows = self._connection.sql("SELECT ticker FROM _dir_tickers").fetchall()
        for (ticker,) in rows:
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

    def _record(self, plan: _Plan) -> None:
        """Record what this vintage left in the tree, one row per partition."""
        vintage = plan.raw.vintage.isoformat()
        written = ingest.partition_glob(
            self._parquet_directory,
            plan.asset_type.value,
            plan.dataset.value,
            plan.adjustment,
            plan.timeframe.value,
            None,
            f"v{vintage}_*.parquet",
        )

        # an increment whose rows the partition already covers writes no files at
        # all. There is no partition to describe, so there is nothing to record;
        # the next sync re-checks it and finds the same nothing, cheaply.
        if next(iglob(written), None) is None:
            return

        # SEMI JOIN, not the bare glob: two `full` vintages of different
        # ticker_ranges share every key and every filename, differing only in
        # which tickers they carry, so without this the second would claim the
        # first's partitions as its own provenance.
        basis = f"DATE '{vintage}'" if plan.restated else "NULL::DATE"
        rows = self._connection.sql(
            f"""
            SELECT
                asset_type, dataset, adjustment, timeframe, ticker,
                DATE '{vintage}' AS vintage,
                '{plan.raw.source}' AS source,
                {basis} AS basis,
                min(ts) AS first_ts,
                max(ts) AS last_ts,
                count(*) AS row_count
            FROM read_parquet('{written}', hive_partitioning = true)
            SEMI JOIN _dir_tickers USING (ticker)
            GROUP BY ALL
            """
        )
        self._manifest.record(rows)

    def _drop_superseded_vintages(self, kept: Path, vintage: date) -> None:
        """Drop the `full` vintages this one supersedes, once it is safely on disk.

        Older only, and dated only. Dropping after the new directory lands is
        what closes the window the staging swap used to leave open, and refusing
        to touch a sibling that is not a vintage means an interrupted fetch's
        staging directory is not collateral.
        """
        for sibling in kept.parent.iterdir():
            if sibling == kept or not sibling.is_dir():
                continue
            try:
                superseded = date.fromisoformat(sibling.name) < vintage
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

        A relation and not rows: rows would be ``list[Bar]``, which is what we
        refused in NT, and 400GB does not fit in a list. The caller gets the
        engine.

        The asset type is in the method name rather than a parameter, so
        ``(FUTURES, EquitiesAdjustment.SPLIT)`` is unrepresentable -- ADR 0001's
        move applied to methods. On the read side that pairing is worse than
        ADR 0002's server-side error: no server sees it, the glob simply misses,
        and an empty relation reads as "no data".

        ``dataset`` defaults to every dataset, which is the point: the plain
        question spans listed and delisted, so the unbiased answer is the one you
        get without asking, and ``Dataset.LISTED`` is what costs you survivorship.
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

        No ``dataset``: the continuous series is the only futures dataset the v1
        layout carries. ``dataset=contract`` is the seam when individual
        contracts arrive.
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

        Names no asset type, so both adjustment enums are meaningful here and
        neither is nonsense: ``EquitiesAdjustment.SPLIT`` asks for split-adjusted
        bars wherever they exist, and the futures partitions simply do not match.

        Every key is also a column, so an unfiltered call interleaves
        adjustments: the same bar appears once per adjustment it was fetched
        under, distinguishable by the ``adjustment`` column. Narrow with the
        selectors rather than a ``WHERE`` -- they build the glob, and pruning
        after enumeration is the 970x.
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

        # a glob that matches nothing is an IOException in DuckDB, and "no data"
        # is an answer. UNADJUSTED's legal timeframes differ per endpoint and
        # cannot live on the enum, so 5min UNADJUSTED is a question the vendor
        # never answered: it gets an empty relation of the right shape.
        if next(iglob(pattern), None) is None:
            return self._connection.sql(ingest.empty_bars_select())

        return self._connection.sql(
            f"SELECT {ingest.bars_projection()} "
            f"FROM read_parquet('{pattern}', hive_partitioning = true)"
        )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _write_sidecar(
        self, directory: Path, request: StoredRequest, vintage: date
    ) -> None:
        recorded = {
            "vintage": vintage.isoformat(),
            "endpoint": request.endpoint,
            "params": request.to_params(),
        }
        (directory / _SIDECAR).write_text(json.dumps(recorded, indent=2))

    def _unzip_and_write(
        self, zip_file: bytes, target: Path, request: StoredRequest, vintage: date
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

        # inside the staging directory, so the sidecar swaps in with the payloads
        # it describes and no directory is ever visible without its provenance
        self._write_sidecar(staging, request, vintage)

        if target.exists():
            shutil.rmtree(target)
        staging.replace(target)
