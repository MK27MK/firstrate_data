"""The store: vendor archives in, queryable relations out.

Parquet is the only copy. An archive is unzipped into a temporary directory
inside the store, ingested, and the temporary directory is removed.
"""

import shutil
import zipfile
from collections import defaultdict
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import date, datetime
from glob import iglob
from itertools import batched
from pathlib import Path
from tempfile import mkdtemp
from types import TracebackType
from typing import Literal, Self

import duckdb

from firstrate_data import config
from firstrate_data.domain import (
    Adjustment,
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    Dataset,
    EquitiesAdjustment,
    FuturesContractAdjustment,
    IndexAdjustment,
    MetafileType,
    Timeframe,
    TradingHours,
)
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store import _deflate64, _sql

# Where the rejected lines are kept, one parquet file per ingest that rejected
# anything. The vendor will serve the same damaged bytes on a re-fetch, so a
# line dropped here is gone unless it is written down.
_QUARANTINE_DIRECTORY = "quarantine"

# How many tickers one ``COPY`` may write. DuckDB buffers every open partition,
# so peak memory tracks distinct bar types rather than input size: 10,000 in one
# statement exhausted 12.7 GiB, 250 completed comfortably.
# See docs/notes/duckdb/partitioned-copy-ooms-past-a-few-thousand-partitions.md.
_TICKERS_PER_COPY = 250

# The store keeps to one subdirectory of the path it is handed, so that path may
# be a volume root, a home directory, or anything else already holding something.
_STORE_DIRECTORY = "firstrate_data"

# macOS drops an AppleDouble sidecar (``._name``) beside every file written to
# a non-native filesystem (exFAT, NTFS, network shares); it matches a bare
# ``*.parquet`` glob but isn't parquet. Store file names always start with the
# date that produced them, so anchoring on that digit excludes the sidecar.
PARQUET_FILES = "[0-9]*.parquet"


@dataclass(frozen=True, slots=True)
class Ingested:
    """What one archive left in the store, and what was wrong with it.

    ``tickers`` is None where the archive has no ticker dimension to count, as a
    metafile does not.

    The two damage counts are not the same kind of thing. ``rejected`` lines
    never reached the store and are only in the quarantine; ``suspect`` rows are
    in the tree and readable, and are suspect rather than wrong -- the rule is a
    heuristic, so they are counted and left alone.
    """

    tickers: int | None
    rows: int
    # rows in the tree that break a bar's own arithmetic -- see
    # ``_sql.suspect_bars_where``. Always 0 for a metafile, which holds no bars.
    suspect: int
    # where this ingest's rejected lines were written, None if it rejected none
    quarantine: Path | None = None
    # lines lost per vendor payload, worst first. The payload names the ticker.
    damaged: tuple[tuple[str, int], ...] = ()

    @property
    def rejected(self) -> int:
        """Lines DuckDB could not parse, quarantined rather than aborting the batch.

        One line counts once, however many of its columns were unreadable.
        """
        # derived rather than stored: the breakdown is read back off the
        # quarantine file, so this cannot drift from what is on disk
        return sum(lines for _, lines in self.damaged)


class Store:
    """The on-disk store: a Hive-partitioned parquet tree of bars, plus the
    corporate-action metafiles that explain their adjustments.

    Archives are ingested on arrival and not retained, so this is the only copy
    of the data -- a lost bar type is re-downloaded, not rebuilt.

    Holds a DuckDB connection for its lifetime. Use it as a context manager, or
    call ``close()`` when done; a store that is never closed holds the
    connection until it is collected.

    Parameters
    ----------
    directory : Path
        Where to keep the store. It gets a ``firstrate_data/`` subdirectory of
        its own and writes nothing beside it, so a volume root is a fine answer.
        Created if it does not exist.
    spool : Path | None
        Where incoming archives wait, if not the store's own ``spool/``.
        Overriding it puts an archive on a disk that need not have room for it,
        so state it only with a reason -- a benchmark measuring one volume, or a
        store whose own disk is full.

    """

    def __init__(self, directory: Path, spool: Path | None = None) -> None:
        # everything below is derived from the store root, not from `directory`
        # again: the two drifted apart once and split the spool from the tree
        self._directory = directory / _STORE_DIRECTORY
        # bars under their own root, so the metafile tables sit beside it
        # without a glob ever having to tell them apart
        self._bars_directory = self._directory / "bars"
        # resolved here even when overridden, so there is one answer to where
        # the spool is rather than one per caller that thought to ask
        self._spool = self._directory / "spool" if spool is None else spool
        self._connection = duckdb.connect()
        self._configure(self._connection)

    def _configure(self, connection: duckdb.DuckDBPyConnection) -> None:
        """Settings a store-sized ingest needs and a scratch query does not."""
        # DuckDB spills to `.tmp` in the working directory by default, which is
        # the machine's boot disk. We make it spill inside the store.
        # https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads#spilling-to-disk
        # https://duckdb.org/docs/stable/configuration/overview#global-configuration-options
        spill = self._directory / ".duckdb_temp"
        spill.mkdir(parents=True, exist_ok=True)
        connection.execute(f"SET temp_directory = '{spill}'")

        # nothing here reads the tree in input order, so preserving that order
        # only costs memory -- and it costs it in proportion to the archive
        connection.execute("SET preserve_insertion_order = false")

        # See docs/notes/duckdb/session-timezone-defaults-to-machine-locale.md.
        connection.execute(f"SET TimeZone = '{_sql.bar_timezone(AssetType.STOCK)}'")

    @classmethod
    def from_env(cls, spool: Path | None = None) -> Self:
        """A store at the path the environment names.

        Raises
        ------
        MissingSettingError
            If ``FIRSTRATE_DATA_PATH`` is not set.

        """
        return cls(config.firstrate_data_path(), spool)

    def close(self) -> None:
        """Release the DuckDB connection. Safe to call more than once.

        The relations the read methods returned are lazy, so they are only
        readable while the store that made them is open.
        """
        # no closed flag: DuckDB's own close() is a no-op on an already closed
        # connection, so the second call is the first one's business, not ours
        self._connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @property
    def spool(self) -> Path:
        """Where a download parks an archive on its way in. Created if absent.

        Inside the store by default, not the system temp: an incoming archive is
        the size of the store it is joining, and only one disk has room for
        both. Placed here rather than by each caller, which once put the two on
        different volumes -- a caller with a reason states it to the constructor
        instead, where that rule is written down.
        """
        self._spool.mkdir(parents=True, exist_ok=True)
        return self._spool

    @property
    def quarantine(self) -> Path:
        """Where the lines that could not be parsed are kept. Created if absent.

        One parquet file per ingest that rejected anything, holding the payload
        the line came from, its line number, the line itself and what DuckDB
        made of it. Empty until something is rejected.
        """
        quarantine = self._directory / _QUARANTINE_DIRECTORY
        quarantine.mkdir(parents=True, exist_ok=True)
        return quarantine

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def ingest_bars(self, archive: Path, source_request: BarsRequest) -> Ingested:
        """File one bars archive into the tree, and keep nothing else.

        The archive is unzipped inside the store, read, and deleted. Whether it
        replaces the bars it names or extends them is the source's to say: a
        replacing fetch carries the whole history of every ticker it names, an
        extending one is filtered down to the bars newer than the store has --
        which is what makes two overlapping fetches converge rather than
        double-count.

        A non-zero ``Ingested.rejected`` means the store is incomplete: those
        lines could not be parsed, and only the quarantine has them now. A
        non-zero ``Ingested.suspect`` means the opposite -- rows that parsed and
        are in the tree, but do not hold together as bars.
        """
        bar_type = source_request.bar_type
        if bar_type.dataset is None:
            msg = "an ingest needs a bar_type naming a dataset"
            raise ValueError(msg)

        # minted once for the whole archive, not per batch: the point of the id
        # is that one glob afterwards finds everything this archive wrote
        ingest = _sql.ingest_id()

        with self._unzipped(archive) as staging:
            payloads = self._payloads_by_ticker(staging, bar_type.dataset)
            if source_request.must_replace_existing_bars:
                self._remove_existing_bars(bar_type, payloads)

            columns_in_payload = _sql.payload_columns(
                payload for files in payloads.values() for payload in files
            )

            rows = 0
            for batch in batched(payloads.items(), _TICKERS_PER_COPY, strict=False):
                rows += self._copy_batch(
                    bar_type,
                    replaces=source_request.must_replace_existing_bars,
                    payloads=dict(batch),
                    columns_in_payload=columns_in_payload,
                    ingest=ingest,
                )

        quarantine, damaged = self._drain_rejects()
        return Ingested(
            len(payloads),
            rows,
            self._count_suspect(bar_type, ingest),
            quarantine,
            damaged,
        )

    def ingest_metafile(self, content: Path, metafile_type: MetafileType) -> Ingested:
        """Replace one metafile table -- splits, dividends, or the continuous audit.

        Replaced whole rather than reconciled: a metafile is small, the vendor
        serves the entire history each time, and it carries no key to merge on.
        ``content`` is an archive of one headerless CSV per ticker, the only
        shape observed, or a bare CSV, which the docs leave open. Both are
        accepted. ``Ingested.tickers`` is None -- a metafile has no ticker
        dimension in the layout.
        """
        target = self._metafile_path(metafile_type)

        with self._metafile_payloads(content, metafile_type) as (
            payloads,
            per_ticker,
        ):
            # written beside the target and swapped in, so a reader either sees
            # the previous table whole or the new one, never a half-written file
            staged = target.with_name(f"{target.name}.partial")
            select = _sql.metafile_select(payloads, metafile_type, per_ticker)
            rows = self._execute_counting(f"""
                COPY ({select})
                TO {_sql.sql_literal(str(staged))} (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
            staged.replace(target)

        quarantine, damaged = self._drain_rejects()
        # a metafile is a corporate action, not a bar: there is no open, high,
        # low or close for the suspect rule to hold against
        return Ingested(None, rows, 0, quarantine, damaged)

    # ------------------------------------------------------------------
    # reading methods
    # ------------------------------------------------------------------

    def stock_bars(  # noqa: PLR0913 - each selector is an independent, named query axis
        self,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        *,
        dataset: Literal[Dataset.LISTED, Dataset.DELISTED] | None = None,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
        resample: bool = False,
    ) -> duckdb.DuckDBPyRelation:
        """- The default `dataset` spans listed and delisted."""
        if dataset == Dataset.DELISTED and timeframe == Timeframe.DAY_1:
            if not resample:
                msg = "Firstrate only offers 1 minute data for delisted tickers"
                raise LookupError(
                    msg,
                )
            one_minute_delisted = self._read(
                BarType(
                    AssetType.STOCK,
                    timeframe=Timeframe.MIN_1,
                    dataset=dataset,
                    adjustment=adjustment,
                ),
                ticker,
                start=start,
                end=end,
                hours=hours,
            )
            return self.subsample_bars(one_minute_delisted, timeframe)

        return self._read(
            BarType(
                AssetType.STOCK,
                timeframe=timeframe,
                dataset=dataset,
                adjustment=adjustment,
            ),
            ticker,
            start=start,
            end=end,
            hours=hours,
        )

    def futures_bars(
        self,
        timeframe: Timeframe,
        adjustment: ContinuousFuturesAdjustment,
        *,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Futures continuous-series bars, as a lazy relation.

        No ``dataset`` parameter: individual contracts are read with
        ``futures_contract_bars`` instead. A continuous series is a
        construction, and which one you get is the adjustment's to say; a
        contract is a real instrument, with no roll inside it to correct for and
        so no adjustment to choose. The two do not share a signature, and one
        method spanning both would need a default that quietly mixed a
        construction with the instruments it was built from.
        """
        return self._read(
            BarType(
                AssetType.FUTURES,
                timeframe=timeframe,
                dataset=Dataset.CONTINUOUS,
                adjustment=adjustment,
            ),
            ticker,
            start=start,
            end=end,
        )

    def futures_contract_bars(
        self,
        timeframe: Timeframe,
        *,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Individual futures contract bars, as a lazy relation.

        No ``adjustment`` parameter: a contract is served on one basis, and no
        ``dataset`` either -- the continuous series is read with
        ``futures_bars``.
        """
        return self._read(
            BarType(
                AssetType.FUTURES,
                timeframe=timeframe,
                dataset=Dataset.CONTRACT,
                adjustment=FuturesContractAdjustment.UNADJUSTED,
            ),
            ticker,
            start=start,
            end=end,
        )

    def index_bars(
        self,
        timeframe: Timeframe,
        *,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
    ) -> duckdb.DuckDBPyRelation:
        """Index bars, as a lazy relation.

        No ``adjustment`` parameter: an index is served on one basis, and no
        ``dataset`` either -- the listed one is the only index dataset the
        layout carries.
        """
        return self._read(
            BarType(
                AssetType.INDEX,
                timeframe=timeframe,
                dataset=Dataset.LISTED,
                adjustment=IndexAdjustment.UNADJUSTED,
            ),
            ticker,
            start=start,
            end=end,
            hours=hours,
        )

    def bars(  # noqa: PLR0913 - each selector is an independent, named query axis
        self,
        *,
        timeframe: Timeframe | None = None,
        adjustment: Adjustment | None = None,
        dataset: Dataset | None = None,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Bars across the whole tree, as a lazy relation.

        Every omitted selector spans all its values, so an unfiltered call
        interleaves adjustments: the same bar appears once per adjustment it was
        fetched under. Narrow with the selectors rather than a ``WHERE`` -- they
        build the glob, which is what makes a fine slice fast.

        No ``hours``: the exchange session is defined per asset type, and this
        read spans them all. Ask the asset type's own method for that.
        """
        return self._read(
            BarType(None, timeframe=timeframe, dataset=dataset, adjustment=adjustment),
            ticker,
            start=start,
            end=end,
        )

    def suspect_bars(
        self,
        *,
        timeframe: Timeframe | None = None,
        adjustment: Adjustment | None = None,
        dataset: Dataset | None = None,
        ticker: str | Iterable[str] | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Bars in the tree that do not hold together, as a lazy relation.

        The ones a spliced line leaves behind when it happens to land on a comma:
        they parse, so nothing rejects them, and they enter the store as bars
        whose high is below their low or whose volume is negative. Selectors
        narrow the same way ``bars`` does.

        A heuristic, not a verdict -- these rows are in the store and stay there.
        """
        return self.bars(
            timeframe=timeframe,
            adjustment=adjustment,
            dataset=dataset,
            ticker=ticker,
        ).filter(_sql.suspect_bars_where())

    def quarantined(self) -> duckdb.DuckDBPyRelation:
        """Every line the store could not parse, as a lazy relation.

        The whole quarantine, not one ingest's: nothing is ever removed from it,
        so this is every line the store has ever lost, and the payload column
        names the ticker each belonged to. Empty where nothing was rejected.
        """
        files = sorted(self.quarantine.glob("*.parquet"))
        if not files:
            return self._connection.sql(_sql.empty_quarantine_select())
        return self._connection.sql(
            # each path is escaped by _sql.sql_list()
            f"SELECT * FROM read_parquet({_sql.sql_list(str(file) for file in files)})",  # noqa: S608
        )

    def splits(self) -> duckdb.DuckDBPyRelation:
        """The splits metafile, as a lazy relation."""
        return self._metafile(MetafileType.SPLITS)

    def dividends(self) -> duckdb.DuckDBPyRelation:
        """The dividends metafile, as a lazy relation."""
        return self._metafile(MetafileType.DIVIDENDS)

    def contin_audit(self) -> duckdb.DuckDBPyRelation:
        """Which individual contracts were stitched into the continuous series."""
        return self._metafile(MetafileType.CONTIN_AUDIT)

    def tickers_list(self, bar_type: BarType) -> list[str]:
        # not ``GROUP BY ticker`` over the tree, which reads no bar column but
        # still opens every file and pushes every row into the aggregate -- ~50
        # us per file plus a term per row, against a walk that costs the
        # directory entries alone. See
        # docs/notes/duckdb/grouping-by-a-hive-column-still-emits-every-row.md.
        bar_path = self._get_bar_path(bar_type)
        # the same glob a read would build, so a ticker is listed only where
        # ``bars`` will answer with rows for it
        return sorted(
            {
                level.removeprefix(_sql.TICKER_LEVEL)
                for path in iglob(bar_path)  # noqa: PTH207
                for level in Path(path).parts
                if level.startswith(_sql.TICKER_LEVEL)
            },
        )

    def subsample_bars(
        self,
        bars: duckdb.DuckDBPyRelation,
        timeframe: Timeframe,
    ) -> duckdb.DuckDBPyRelation:
        tf = self._timeframe_of(bars)
        if tf is not None and not timeframe.is_higher_than(tf):
            msg = f"a subsample only makes bars coarser: not {tf} into {timeframe}"
            raise ValueError(msg)
        return bars.aggregate(*_sql.resample_aggregate(timeframe))

    # ------------------------------------------------------------------
    # ingest internals
    # ------------------------------------------------------------------

    @contextmanager
    def _unzipped(self, archive: Path) -> Generator[Path]:
        """The archive's payloads on disk, for as long as the ingest needs them.

        Inside the store, not the system temp: rarely the same filesystem, and
        an unzipped archive runs to tens of gigabytes.
        """
        staging = Path(mkdtemp(dir=self._directory, prefix=".ingest-"))
        try:
            # the vendor's larger archives are Deflate64, which zipfile declines
            # to decode until this has run
            _deflate64.install()
            with zipfile.ZipFile(archive) as opened:
                opened.extractall(staging)
            yield staging
        finally:
            # only once the write has committed or failed -- an ingest that
            # unlinked first would be reading files it had already promised to
            # delete. A failed ingest costs a re-unzip, not a re-fetch.
            shutil.rmtree(staging, ignore_errors=True)

    def _payloads_by_ticker(
        self,
        directory: Path,
        dataset: Dataset,
    ) -> dict[str, list[Path]]:
        """Every payload one archive unzipped to, grouped by the ticker it names."""
        grouped: defaultdict[str, list[Path]] = defaultdict(list)
        for path, ticker in self._connection.sql(
            _sql.payload_tickers_select(directory, dataset),
        ).fetchall():
            grouped[ticker].append(Path(path))
        return dict(grouped)

    def _remove_existing_bars(self, bar_type: BarType, tickers: Iterable[str]) -> None:
        for ticker in tickers:
            directory = Path(
                self._get_bar_path(bar_type.from_ticker(ticker), files_regex=None),
            )
            if directory.exists():
                shutil.rmtree(directory)

    def _copy_batch(
        self,
        bar_type: BarType,
        *,
        replaces: bool,
        payloads: dict[str, list[Path]],
        columns_in_payload: int,
        ingest: str,
    ) -> int:
        """Write one batch of tickers into the tree; returns rows written."""
        select = _sql.bars_select(
            [payload for files in payloads.values() for payload in files],
            # no ticker: one COPY writes a batch of them, and each row's is
            # read back out of the payload that carried it
            bar_type,
            columns_in_payload,
        )
        if not replaces:
            select = self._only_new(select, bar_type, payloads)

        return self._execute_counting(f"""
            COPY ({select})
            TO {_sql.sql_literal(str(self._bars_directory))}
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({", ".join([f.name for f in fields(BarType)])}),
             FILENAME_PATTERN '{_sql.filename_pattern(ingest)}',
             APPEND)
            """)

    def _only_new(
        self,
        select: str,
        bar_type: BarType,
        payloads: dict[str, list[Path]],
    ) -> str:
        """An increment, minus the rows the store already holds."""
        # a `week` fetched on a Wednesday re-serves Monday and Tuesday, which the
        # `full` beneath it already carries; a plain append would double those
        # bars silently
        last_available = self._last_available_ts(bar_type, payloads)
        if not last_available:
            return select

        known = ", ".join(
            f"({_sql.sql_literal(ticker)}, "
            f"TIMESTAMPTZ {_sql.sql_literal(last_ts.isoformat())})"
            for ticker, last_ts in last_available.items()
        )
        # `select` is built by _sql.bars_select(); each value in `known` is
        # escaped by _sql.sql_literal()
        return f"""
            WITH staged AS ({select}),
            last_available(ticker, last_ts) AS (VALUES {known})
            SELECT staged.*
            FROM staged
            LEFT JOIN last_available USING (ticker)
            WHERE last_available.last_ts IS NULL OR staged.ts > last_available.last_ts
        """  # noqa: S608

    def _last_available_ts(
        self,
        bar_type: BarType,
        tickers: Iterable[str],
    ) -> dict[str, datetime]:
        """The newest bar the store already holds, per ticker, from parquet footers.

        Not ``max(ts)``: DuckDB fully reads and decodes the column for that --
        only ``count(*)`` comes from the footer -- which at this store's shape is
        one to two minutes per incremental fetch. ``parquet_metadata`` reads
        ~10.6 KB per file and scales with file count rather than row count. See
        ``docs/notes/duckdb/max-of-a-column-is-a-full-scan-not-footer-stats.md``.
        """
        globs = [self._get_bar_path(bar_type.from_ticker(ticker)) for ticker in tickers]
        # a glob matching nothing is an IOException, and a ticker the store has
        # never seen is the ordinary case for an increment. Each `glob` is a
        # full glob string built by _get_bar_path(), not decomposable into
        # Path(base).glob(pattern) at this call site
        present = [
            glob
            for glob in globs
            if next(iglob(glob), None) is not None  # noqa: PTH207
        ]
        if not present:
            return {}

        # parquet_metadata does not expose Hive bar type columns, so the ticker
        # is parsed back out of the path, and it returns one row per row-group
        # per column, so the aggregate is not optional
        footers = (
            f"FROM parquet_metadata({_sql.sql_list(present)}) "
            f"WHERE path_in_schema = 'ts'"
        )
        stated = dict(
            self._connection.sql(
                f"SELECT {_sql.hive_ticker_expression('file_name')} AS ticker, "
                # stats_max is VARCHAR ('2010-03-27 07:59:00+00'); the offset
                # makes the cast unambiguous, but the cast is mandatory
                f"max(stats_max::TIMESTAMPTZ) AS last_ts "
                f"{footers} AND stats_max IS NOT NULL GROUP BY 1",
            ).fetchall(),
        )

        # statistics are the writer's to omit, and a file without them says
        # nothing about its contents -- reading that as "no data" would silently
        # re-ingest bars the store already holds
        unstated = [
            file
            for (file,) in self._connection.sql(
                f"SELECT DISTINCT file_name {footers} AND stats_max IS NULL",
            ).fetchall()
        ]
        for ticker, last_ts in self._scanned_last_ts(unstated):
            stated[ticker] = max(last_ts, stated.get(ticker, last_ts))

        return stated

    def _scanned_last_ts(self, files: list[str]) -> list[tuple[str, datetime]]:
        """The last ts of files whose footers do not state one, the expensive way."""
        if not files:
            return []
        return self._connection.sql(
            # each path is escaped by _sql.sql_list()
            f"SELECT ticker, max(ts) FROM read_parquet({_sql.sql_list(files)}, "  # noqa: S608
            f"hive_partitioning = true) GROUP BY 1",
        ).fetchall()

    @contextmanager
    def _metafile_payloads(
        self,
        content: Path,
        metafile_type: MetafileType,
    ) -> Generator[tuple[list[Path], bool]]:
        """The metafile's CSV on disk, however the vendor wrapped it.

        Yields the payloads and whether each is named for one ticker -- an
        archive's are, a bare CSV's is not -- which decides whether the ticker
        can be read back at all.
        """
        staging = Path(mkdtemp(dir=self._directory, prefix=".ingest-"))
        try:
            if not zipfile.is_zipfile(content):
                # copied rather than read in place: the staging directory is
                # what this scope promises to clean up, and the spooled file is
                # the caller's
                bare = staging / f"{metafile_type.value}.csv"
                shutil.copyfile(content, bare)
                yield [bare], False
            else:
                _deflate64.install()
                with zipfile.ZipFile(content) as opened:
                    opened.extractall(staging)
                payloads = sorted(
                    path
                    for path in staging.iterdir()
                    if path.is_file() and _sql.is_metafile_payload(path.name)
                )
                if not payloads:
                    msg = f"{metafile_type.value} archive holds no payload"
                    raise ValueError(msg)
                yield payloads, True
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def _execute_counting(self, statement: str) -> int:
        """Run a ``COPY`` and return the rows it wrote."""
        written = self._connection.execute(statement).fetchall()
        return 0 if not written else int(written[0][0])

    def _count_suspect(self, bar_type: BarType, ingest_id: str) -> int:
        """Rows one ingest wrote that do not hold together as bars.

        Scoped to that ingest's own files, so the cost is the archive's and not
        the store's, and so an increment is not charged with the damage every
        earlier run left in the bars it is extending.
        """
        pattern = self._get_bar_path(
            bar_type,
            files_regex=_sql.parquet_file_ingest_id_glob(ingest_id),
        )
        # an archive of empty payloads writes no file at all, and DuckDB reads a
        # glob that matches nothing as an error rather than as no rows.
        # `pattern` is a full glob string built by _get_bar_path(), not
        # decomposable into Path(base).glob(pattern) at this call site
        if next(iglob(pattern), None) is None:  # noqa: PTH207
            return 0

        counted = self._connection.sql(
            # `pattern` is escaped by _sql.sql_literal(); the WHERE clause
            # comes from _sql.suspect_bars_where(), an internal builder
            f"SELECT count(*) FROM read_parquet({_sql.sql_literal(pattern)}) "  # noqa: S608
            f"WHERE {_sql.suspect_bars_where()}",
        ).fetchone()
        return 0 if counted is None else int(counted[0])

    def _drain_rejects(self) -> tuple[Path | None, tuple[tuple[str, int], ...]]:
        """Write the last ingest's unparseable lines to the quarantine, emptying
        the tally. Answers with the file written and the lines lost per payload.

        DuckDB appends to the reject tables for the life of the connection, so
        reading them without emptying them would charge every ingest with all the
        damage found before it.
        """
        try:
            counted = self._connection.sql(
                # the subquery comes from _sql.rejected_lines_select(), an
                # internal builder
                f"SELECT count(*) FROM ({_sql.rejected_lines_select()})",  # noqa: S608
            ).fetchone()
        except duckdb.CatalogException:
            # the tables are created lazily, by the first scan that rejects
            # anything -- absent means a clean store, not a broken one
            return None, ()

        # the tables outlive the ingest that filled them, so they are here and
        # empty for every clean ingest after a damaged one. Asked first so a
        # clean ingest neither writes an empty file nor makes the directory.
        if counted is None or not counted[0]:
            return None, ()

        quarantined = self.quarantine / f"{_sql.quarantine_filename()}.parquet"
        self._connection.execute(f"""
            COPY ({_sql.rejected_lines_select()})
            TO {_sql.sql_literal(str(quarantined))}
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        # both table names are internal module constants, not interpolated input
        self._connection.execute(f"DELETE FROM {_sql.REJECTS_TABLE}")  # noqa: S608
        self._connection.execute(f"DELETE FROM {_sql.REJECT_SCANS_TABLE}")  # noqa: S608

        # read back off the file rather than off the tables, which are empty by
        # now: what the caller is told and what a later reader of the quarantine
        # will find are then the same rows, not two counts of one thing
        damaged = self._connection.sql(
            _sql.quarantine_by_payload(str(quarantined)),
        ).fetchall()
        return quarantined, tuple((payload, int(lines)) for payload, lines in damaged)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _timeframe_of(bars: duckdb.DuckDBPyRelation) -> Timeframe | None:
        held = {row[0] for row in bars.aggregate("timeframe", "timeframe").fetchall()}
        # an empty relation names no timeframe, and neither does a mixed one
        if len(held) != 1:
            return None
        return Timeframe(held.pop())

    def _read(
        self,
        bar_type: BarType,
        ticker: str | Iterable[str] | None,
        *,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
    ) -> duckdb.DuckDBPyRelation:
        tickers = (
            [ticker] if ticker is None or isinstance(ticker, str) else list(ticker)
        )
        bar_paths = [self._get_bar_path(bar_type.from_ticker(one)) for one in tickers]

        # each `pattern` is a full glob string built by _get_bar_path(), not
        # decomposable into Path(base).glob(pattern) at this call site
        available_bar_paths = [
            pattern
            for pattern in bar_paths
            if next(iglob(pattern), None) is not None  # noqa: PTH207
        ]
        if not available_bar_paths:
            return self._connection.sql(_sql.empty_bars_select())

        bars_relation = self._connection.sql(
            # the projection comes from _sql.bars_projection, an internal
            # builder; each path in `available_bar_paths` is escaped by
            # _sql.sql_list
            f"SELECT {_sql.bars_projection()} "  # noqa: S608
            f"FROM read_parquet({_sql.sql_list(available_bar_paths)}, "
            "hive_partitioning = true)",
        )

        # both are ``WHERE``, not selectors: neither the clock nor the calendar
        # is a level of the tree, so no glob can narrow them
        dates = _sql.date_range_where(start, end)
        if dates is not None:
            bars_relation = bars_relation.filter(dates)
        if hours is TradingHours.REGULAR:
            if bar_type.asset_type is None:
                msg = "a session belongs to one asset type: name one"
                raise ValueError(msg)
            bars_relation = bars_relation.filter(
                _sql.regular_trading_hours_where(bar_type.asset_type),
            )
        return bars_relation

    def _metafile_path(self, metafile_type: MetafileType) -> Path:
        return self._directory / f"{metafile_type.value}.parquet"

    def _metafile(self, metafile_type: MetafileType) -> duckdb.DuckDBPyRelation:
        path = self._metafile_path(metafile_type)
        # unlike bars, a missing metafile cannot become an empty relation of the
        # right shape: the vendor's columns are whatever the sniffer read, so
        # there is no shape to return
        if not path.exists():
            msg = f"no {metafile_type.value} metafile in this store -- fetch it first"
            raise FileNotFoundError(
                msg,
            )
        return self._connection.sql(
            # `path` is escaped by _sql.sql_literal()
            f"SELECT * FROM read_parquet({_sql.sql_literal(str(path))})",  # noqa: S608
        )

    def _get_bar_path(
        self, bar_type: BarType, files_regex: str | None = PARQUET_FILES
    ) -> str:
        stated = bar_type.to_dict()

        if files_regex is None and None in stated.values():
            unset = [key for key, value in stated.items() if value is None]
            msg = f"bar type names no {', '.join(unset)}"
            raise ValueError(msg)

        levels = (
            f"{key}={'*' if value is None else value}" for key, value in stated.items()
        )
        directory = self._bars_directory.joinpath(*levels)
        path = directory if files_regex is None else directory / files_regex
        # forward slashes, which DuckDB's glob and Python's both read on every
        # platform. A backslash is the pattern language's escape character, so a
        # native Windows path would read ``\[0-9]`` as the literal ``[0-9]``.
        return path.as_posix()
