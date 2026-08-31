import shutil
import zipfile
from collections import defaultdict
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from glob import iglob
from itertools import batched
from pathlib import Path
from tempfile import mkdtemp
from types import TracebackType
from typing import Self

import duckdb

from firstrate_data import config
from firstrate_data.domain import (
    Adjustment,
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    EquitiesAdjustment,
    OtherData,
    TickerListing,
    Timeframe,
    TradingHours,
    Unadjusted,
)
from firstrate_data.download.requests import (
    BarsRequest,
    OtherDataRequest,
    Request,
)
from firstrate_data.store import _deflate64, _sql
from firstrate_data.store._catalog import Catalog, TickerSpan
from firstrate_data.store._parquet_table import ParquetTable

# How many tickers one ``COPY`` may write. DuckDB buffers every open partition,
# so peak memory tracks distinct bar types rather than input size: 10,000 in one
# statement exhausted 12.7 GiB, 250 completed comfortably.
# See docs/notes/duckdb/partitioned-copy-ooms-past-a-few-thousand-partitions.md.
_TICKERS_PER_COPY = 250

# macOS drops an AppleDouble sidecar (``._name``) beside every file written to
# a non-native filesystem (exFAT, NTFS, network shares); it matches a bare
# ``*.parquet`` glob but isn't parquet. Store file names always start with the
# date that produced them, so anchoring on that digit excludes the sidecar.
PARQUET_FILES = "[0-9]*.parquet"

TIMEZONE = "America/New_York"


@dataclass(frozen=True, slots=True)
class Ingested:
    """What one archive left in the store.

    ``tickers`` is None where the archive has no ticker dimension to count, as a
    metafile does not.
    """

    tickers: int | None
    rows: int


class ConflictingBarsError(ValueError):
    """An archive's bars are not an update of the bars already filed for a ticker."""

    def __init__(
        self,
        bar_type: BarType,
        conflicts: Iterable[tuple[TickerSpan, TickerSpan]],
    ) -> None:
        self.bar_type = bar_type
        self.conflicts = tuple(conflicts)
        named = "; ".join(
            f"{held.ticker} holds {held.first_ts:%Y-%m-%d}..{held.last_ts:%Y-%m-%d}, "
            f"archive carries {arriving.first_ts:%Y-%m-%d}..{arriving.last_ts:%Y-%m-%d}"
            for held, arriving in self.conflicts
        )
        super().__init__(
            f"{len(self.conflicts)} tickers hold bars this archive neither starts "
            f"with nor reaches past: {named}. Nothing was filed.",
        )


class Store:
    """_summary_.

    Parameters
    ----------
    directory : Path
        Store will be created at directory/firstrate_data.

    """

    def __init__(self, directory: Path) -> None:
        self._directory = directory / "firstrate_data"
        self._bars_directory = self._directory / "bars"
        self._spool = self._directory / "spool"

        self._bars_directory.mkdir(parents=True, exist_ok=True)
        self._spool.mkdir(parents=True, exist_ok=True)

        self._connection = duckdb.connect()
        self._configure(self._connection)
        self._catalog = Catalog(self._connection, self._directory / "catalog.parquet")

    def _configure(self, connection: duckdb.DuckDBPyConnection) -> None:
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

        connection.execute(f"SET TimeZone = '{TIMEZONE}'")

    @classmethod
    def from_env(cls) -> Self:
        """Return a Store with directory set from the .env file.

        Raises
        ------
        MissingSettingError
            If ``FIRSTRATE_DATA_PATH`` is not set.

        """
        return cls(config.firstrate_data_path())

    def close(self) -> None:
        """Release the DuckDB connection."""
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
        return self._spool

    # ------------------------------------------------------------------
    # writing methods
    # ------------------------------------------------------------------

    def write(self, file: Path, request: Request) -> Ingested:
        try:
            if isinstance(request, OtherDataRequest):
                return self._write_other_data(file, request.other_data)
            return self._write_bars(file, request)
        finally:
            # parquet is the only copy
            file.unlink(missing_ok=True)

    def write_ticker_listing(
        self,
        asset_type: AssetType,
        listing: Iterable[TickerListing],
    ) -> int:
        table = self._ticker_listing_table(asset_type)
        staged = table.stage(
            (
                row.ticker,
                row.full_name,
                row.start_date,
                row.end_date,
                row.is_delisted,
            )
            for row in listing
        )
        if staged is None:
            return 0

        # the staging table's name is the store's own
        table.rewrite(f"SELECT * FROM {staged}")  # noqa: S608
        return int(table.relation().shape[0])

    def _write_bars(self, archive: Path, request: BarsRequest) -> Ingested:
        bar_type = request.bar_type

        # minted once for the whole archive, not per batch: the point of the id
        # is that one glob afterwards finds everything this archive wrote
        ingest = _sql.ingest_id()

        with self._unzipped(archive) as staging:
            payloads = self._payloads_by_ticker(staging)
            held = self._catalog.spans(bar_type, payloads)
            columns_in_payload = _sql.payload_columns(
                payload for files in payloads.values() for payload in files
            )

            staged_tree = staging / "bars"
            rows = 0
            for batch in batched(payloads.items(), _TICKERS_PER_COPY, strict=False):
                rows += self._copy_batch(
                    bar_type,
                    staged_tree,
                    payloads=dict(batch),
                    columns_in_payload=columns_in_payload,
                    ingest=ingest,
                )

            arriving = self._written_spans(staged_tree, bar_type, ingest)
            self._file_over(bar_type, held, arriving, staged_tree)

        return Ingested(len(payloads), rows)

    def _write_other_data(self, content: Path, other_data: OtherData) -> Ingested:
        """Replace one metafile table -- splits, dividends, or the continuous audit.

        Replaced whole rather than reconciled: a metafile is small, the vendor
        serves the entire history each time, and it carries no key to merge on.
        ``content`` is an archive of one headerless CSV per ticker, the only
        shape observed, or a bare CSV, which the docs leave open. Both are
        accepted. ``Ingested.tickers`` is None -- a metafile has no ticker
        dimension in the layout.
        """
        target = self._other_data_path(other_data)

        with self._other_data_payloads(content, other_data) as (
            payloads,
            per_ticker,
        ):
            # written beside the target and swapped in, so a reader either sees
            # the previous table whole or the new one, never a half-written file
            staged = target.with_name(f"{target.name}.partial")
            select = _sql.other_data_select(payloads, other_data, per_ticker)
            rows = self._execute_counting(f"""
                COPY ({select})
                TO {_sql.sql_literal(str(staged))} (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
            staged.replace(target)

        return Ingested(None, rows)
    
    # ------------------------------------------------------------------
    # reading methods
    # ------------------------------------------------------------------

    def stock_bars(  # noqa: PLR0913 - each selector is an independent, named query axis
        self,
        timeframe: Timeframe,
        adjustment: EquitiesAdjustment,
        *,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
    ) -> duckdb.DuckDBPyRelation:
        """Stock bars, as a lazy relation.

        A delisted symbol keeps the vendor's ``-DELISTED`` suffix, so
        ``ticker="UTRS"`` and ``ticker="UTRS-DELISTED"`` are two instruments
        that happened to share a symbol, each read on its own.
        """
        return self._read(
            BarType(AssetType.STOCK, timeframe=timeframe, adjustment=adjustment),
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

        Individual contracts are read with ``futures_contract_bars``
        instead. A continuous series is a
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

        No ``adjustment`` parameter: a contract is served on one basis. The
        continuous series is read with ``futures_bars``.
        """
        return self._read(
            BarType(
                AssetType.FUTURES,
                timeframe=timeframe,
                adjustment=Unadjusted.UNADJUSTED,
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

        No ``adjustment`` parameter: an index is served on one basis.
        """
        return self._read(
            BarType(
                AssetType.INDEX,
                timeframe=timeframe,
                adjustment=Unadjusted.UNADJUSTED,
            ),
            ticker,
            start=start,
            end=end,
            hours=hours,
        )

    def bars(  # noqa: PLR0913 - each selector is an independent, named query axis
        self,
        *,
        asset_type: AssetType | None = None,
        timeframe: Timeframe | None = None,
        adjustment: Adjustment | None = None,
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
            BarType(asset_type, timeframe=timeframe, adjustment=adjustment),
            ticker,
            start=start,
            end=end,
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
    # what the store holds
    # ------------------------------------------------------------------

    def catalog(self) -> duckdb.DuckDBPyRelation:
        """One row per ticker per bar type, with the span of bars held for it.

        The store's own index, kept in step by every ingest: it names the bar
        type, the ticker, the first and last bar filed, and how many. Reading
        it opens no bar file.
        """
        return self._catalog.relation()

    def tickers_list(self, bar_type: BarType) -> list[str]:
        """The tickers the store holds bars for under `bar_type`, sorted.

        Off the catalog, so the cost is one small file whatever the store's
        size. A level `bar_type` leaves unstated spans all its values.
        """
        return self._catalog.tickers(bar_type)

    def ticker_listing(
        self,
        *,
        asset_type: AssetType | None = None,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Raises
        ------
        FileNotFoundError
            If no ticker listing is in the store.

        """
        asset_types = [asset_type] if asset_type is not None else list(AssetType)
        stored = [
            (one, table)
            for one in asset_types
            if (table := self._ticker_listing_table(one)).exists()
        ]
        if not stored:
            msg = "no ticker listing in this store -- download it first"
            raise FileNotFoundError(msg)

        # the asset type is a level of the path rather than a column of the
        # file, and DuckDB reads that level back as a column of the relation
        listing = self._connection.sql(
            "\nUNION ALL BY NAME\n".join(
                f"SELECT * FROM ({table.select()})"  # noqa: S608
                for _, table in stored
            ),
        )
        if ticker is not None:
            listing = listing.filter(f"ticker = {_sql.sql_literal(ticker)}")
        return listing

    # ------------------------------------------------------------------
    # other data
    # ------------------------------------------------------------------

    def splits(self) -> duckdb.DuckDBPyRelation:
        return self._other_data(OtherData.SPLITS)

    def dividends(self) -> duckdb.DuckDBPyRelation:
        return self._other_data(OtherData.DIVIDENDS)

    def contract_dates(self) -> duckdb.DuckDBPyRelation:
        """Which individual contracts were stitched into the continuous series."""
        return self._other_data(OtherData.CONTRACT_DATES)

    # ------------------------------------------------------------------
    # ingest internals
    # ------------------------------------------------------------------


    @contextmanager
    def _unzipped(self, archive: Path) -> Generator[Path]:
        staging = Path(mkdtemp(dir=self._directory, prefix=".ingest-"))
        try:
            # the vendor's larger archives are Deflate64, which zipfile declines
            # to decode until this has run
            _deflate64.install() # TODO try removing this bs
            with zipfile.ZipFile(archive) as opened:
                opened.extractall(staging)
            yield staging
        finally:
            # only once the write has committed or failed -- an ingest that
            # unlinked first would be reading files it had already promised to
            # delete. A failed ingest costs a re-unzip, not a re-fetch.
            shutil.rmtree(staging, ignore_errors=True)

    def _payloads_by_ticker(self, directory: Path) -> dict[str, list[Path]]:
        """Every payload one archive unzipped to, grouped by the ticker it names."""
        grouped: defaultdict[str, list[Path]] = defaultdict(list)
        for path, ticker in self._connection.sql(
            _sql.payload_tickers_select(directory),
        ).fetchall():
            grouped[ticker].append(Path(path))
        return dict(grouped)

    def _copy_batch(
        self,
        bar_type: BarType,
        destination: Path,
        *,
        payloads: dict[str, list[Path]],
        columns_in_payload: int,
        ingest: str,
    ) -> int:
        """Write one batch of tickers under `destination`; returns rows written."""
        select = _sql.bars_select(
            [payload for files in payloads.values() for payload in files],
            # no ticker: one COPY writes a batch of them, and each row's is
            # read back out of the payload that carried it
            bar_type,
            columns_in_payload,
        )
        levels = ", ".join(bar_type.levels())
        return self._execute_counting(f"""
            COPY ({select})
            TO {_sql.sql_literal(str(destination))}
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({levels}),
             FILENAME_PATTERN '{_sql.filename_pattern(ingest)}',
             APPEND)
            """)

    def _written_spans(
        self,
        root: Path,
        bar_type: BarType,
        ingest: str,
    ) -> dict[str, TickerSpan]:
        """What one ingest wrote under `root`, per ticker, off the files' footers.

        Scoped to that ingest's own files by the id stamped on their names, so
        the cost is the archive's and not the store's.
        """
        files = self._ingest_files(root, bar_type, ingest)
        if not files:
            return {}
        ticker = _sql.hive_level_expression("file", "ticker")
        # `ticker` is built from BarType's own field names, and
        # footer_file_spans_select() escapes every path it reads
        spans = self._connection.sql(
            f"SELECT {ticker} AS ticker, min(first_ts), max(last_ts), sum(rows) "  # noqa: S608
            f"FROM ({_sql.footer_file_spans_select(files)}) GROUP BY 1",
        ).fetchall()
        return {
            named: TickerSpan(named, first_ts, last_ts, int(rows))
            for named, first_ts, last_ts, rows in spans
        }

    def _file_over(
        self,
        bar_type: BarType,
        held: dict[str, TickerSpan],
        arriving: dict[str, TickerSpan],
        staged_tree: Path,
    ) -> None:
        """Move a staged ingest into the tree, or refuse it before it gets there.

        Raises
        ------
        ConflictingBarsError
            If any ticker's arriving bars are not an update of the filed ones.
            The tree and the catalog are untouched.

        """
        conflicts = [
            (held[ticker], span)
            for ticker, span in sorted(arriving.items())
            if ticker in held and not _updates(held[ticker], span)
        ]
        if conflicts:
            raise ConflictingBarsError(bar_type, conflicts)

        # the ticker's held bars go only once their replacement is in the tree,
        # and only the files this ingest did not write: an update carries the
        # whole history again, so what it lands beside is the previous copy of it
        landed = set(self._move_into_tree(staged_tree))
        for ticker in arriving:
            if ticker in held:
                _unlink(
                    file
                    for file in self._ticker_files(bar_type, ticker)
                    if file not in landed
                )

        self._catalog.record(
            bar_type,
            [span for _, span in sorted(arriving.items())],
        )

    def _move_into_tree(self, staged_tree: Path) -> list[str]:
        """Move a validated ingest's parquet files into the tree, level for level.

        A rename rather than a copy: the staging directory lives inside the
        store, so the bars never cross a filesystem however large the archive.
        """
        landed = []
        for staged in sorted(staged_tree.rglob(PARQUET_FILES)):
            landing = self._bars_directory / staged.relative_to(staged_tree)
            landing.parent.mkdir(parents=True, exist_ok=True)
            staged.replace(landing)
            landed.append(str(landing))
        return landed

    def _ticker_files(self, bar_type: BarType, ticker: str) -> list[str]:
        """Every parquet file the tree holds for one ticker under `bar_type`."""
        # `pattern` is a full glob string built by _get_bar_path(), not
        # decomposable into Path(base).glob(pattern) at this call site
        return sorted(iglob(self._get_bar_path(bar_type.from_ticker(ticker))))  # noqa: PTH207

    def _ingest_files(self, root: Path, bar_type: BarType, ingest: str) -> list[str]:
        """The parquet files one ingest wrote, by the id stamped on their names."""
        pattern = self._get_bar_path(
            bar_type,
            files_regex=_sql.parquet_file_ingest_id_glob(ingest),
            root=root,
        )
        # `pattern` is a full glob string built by _get_bar_path(), not
        # decomposable into Path(base).glob(pattern) at this call site
        return sorted(iglob(pattern))  # noqa: PTH207

    @contextmanager
    def _other_data_payloads(
        self,
        content: Path,
        other_data: OtherData,
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
                bare = staging / f"{other_data.value}.csv"
                shutil.copyfile(content, bare)
                yield [bare], False
            else:
                _deflate64.install()
                with zipfile.ZipFile(content) as opened:
                    opened.extractall(staging)
                payloads = sorted(
                    path
                    for path in staging.iterdir()
                    if path.is_file() and _sql.is_other_data_payload(path.name)
                )
                if not payloads:
                    msg = f"{other_data.value} archive holds no payload"
                    raise ValueError(msg)
                yield payloads, True
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def _execute_counting(self, statement: str) -> int:
        """Run a ``COPY`` and return the rows it wrote."""
        written = self._connection.execute(statement).fetchall()
        return 0 if not written else int(written[0][0])

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
        available = [
            pattern
            for one in tickers
            # each `pattern` is a full glob string built by _get_bar_path(), not
            # decomposable into Path(base).glob(pattern) at this call site
            if next(
                iglob(pattern := self._get_bar_path(bar_type.from_ticker(one))),  # noqa: PTH207
                None,
            )
            is not None
        ]
        if not available:
            return self._connection.sql(_sql.empty_bars_select())

        bars_relation = self._connection.sql(_sql.stored_bars_select(available))

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

    def _ticker_listing_table(self, asset_type: AssetType) -> ParquetTable:
        """The listing of one asset type, filed under that asset type's level."""
        return ParquetTable(
            self._connection,
            self._bars_directory
            / f"asset_type={asset_type.value}"
            / "ticker_listing.parquet",
            _sql.TICKER_LISTING_SCHEMA,
        )

    def _other_data_path(self, other_data: OtherData) -> Path:
        return self._directory / f"{other_data.value}.parquet"

    def _other_data(self, other_data: OtherData) -> duckdb.DuckDBPyRelation:
        path = self._other_data_path(other_data)
        # unlike bars, a missing metafile cannot become an empty relation of the
        # right shape: the vendor's columns are whatever the sniffer read, so
        # there is no shape to return
        if not path.exists():
            msg = f"no {other_data.value} metafile in this store -- fetch it first"
            raise FileNotFoundError(
                msg,
            )
        return self._connection.sql(
            # `path` is escaped by _sql.sql_literal()
            f"SELECT * FROM read_parquet({_sql.sql_literal(str(path))})",  # noqa: S608
        )

    def _get_bar_path(
        self,
        bar_type: BarType,
        files_regex: str = PARQUET_FILES,
        root: Path | None = None,
    ) -> str:
        """The glob `bar_type`'s parquet files sit under, in the tree or a staged one.

        `root` names a staged tree an ingest is still being validated in.
        Absent, it is the store's own.
        """
        levels = (
            f"{key}={'*' if value is None else value}"
            for key, value in bar_type.levels().items()
        )
        path = (root or self._bars_directory).joinpath(*levels) / files_regex
        # forward slashes, which DuckDB's glob and Python's both read on every
        # platform. A backslash is the pattern language's escape character, so a
        # native Windows path would read ``\[0-9]`` as the literal ``[0-9]``.
        return path.as_posix()


def _updates(held: TickerSpan, arriving: TickerSpan) -> bool:
    """Whether `arriving` is the same history as `held`, downloaded with more of it.

    The same first bar and a later last one. A different start is a different
    history under one name, and no bars past the last held are nothing to file.
    """
    return arriving.first_ts == held.first_ts and arriving.last_ts > held.last_ts


def _unlink(files: Iterable[str]) -> None:
    for file in files:
        Path(file).unlink(missing_ok=True)
