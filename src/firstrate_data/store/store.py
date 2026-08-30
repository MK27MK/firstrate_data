import shutil
import zipfile
from collections import defaultdict
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date
from functools import reduce
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
    Dataset,
    EquitiesAdjustment,
    OtherData,
    TickerListing,
    Timeframe,
    TradingHours,
)
from firstrate_data.download.client.file_fetcher import FetchedFile
from firstrate_data.download.requests import (
    BarsRequest,
    IngestibleRequest,
    OtherDataRequest,
)
from firstrate_data.store import _catalog, _deflate64, _sql
from firstrate_data.store._catalog import Catalog, TickerSpan
from firstrate_data.store._parquet_table import ParquetTable

# Where the rejected lines are kept, one parquet file per ingest that rejected
# anything. The vendor will serve the same damaged bytes on a re-fetch, so a
# line dropped here is gone unless it is written down.
_QUARANTINE_DIRECTORY = "quarantine"

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
class _FiledBars:
    """One parquet file of the tree, and the span of the bars it holds."""

    file: str
    span: TickerSpan


@dataclass(frozen=True, slots=True)
class _Settlement:
    """What one ingest leaves one ticker holding, once weighed against the tree."""

    # what the catalog records: the arriving bars and every filed span they
    # left standing
    span: TickerSpan
    # the files the arriving bars cover end to end, to be unlinked
    superseded: list[str]
    # the filed spans the arriving bars overlap without covering, as
    # (filed, arriving) pairs. Any of these refuses the whole ingest.
    collisions: list[tuple[TickerSpan, TickerSpan]]


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


class OverlappingBarsError(ValueError):
    def __init__(
        self,
        bar_type: BarType,
        collisions: Iterable[tuple[TickerSpan, TickerSpan]],
    ) -> None:
        self.bar_type = bar_type
        self.collisions = tuple(collisions)

        collisions_named = 5
        named = "; ".join(
            f"{held.ticker} holds {held.first_ts:%Y-%m-%d}..{held.last_ts:%Y-%m-%d}, "
            f"archive carries {arriving.first_ts:%Y-%m-%d}..{arriving.last_ts:%Y-%m-%d}"
            for held, arriving in self.collisions[:collisions_named]
        )
        unnamed = len(self.collisions) - collisions_named
        rest = f", and {unnamed} more" if unnamed > 0 else ""
        super().__init__(
            f"{len(self.collisions)} tickers already hold bars this archive "
            f"carries again: {named}{rest}. Nothing was filed.",
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

    def ingest(self, file: FetchedFile, request: IngestibleRequest) -> Ingested:
        try:
            if isinstance(request, OtherDataRequest):
                return self._ingest_other_data(file.path, request.other_data)
            return self._ingest_bars(file.path, request)
        finally:
            # parquet is the only copy, and that goes for the vendor's zip on
            # its way in as much as for the CSV it unzips to
            file.path.unlink(missing_ok=True)

    def ingest_ticker_listing(
        self,
        asset_type: AssetType,
        listing: Iterable[TickerListing],
    ) -> int:
        """Replace the stored ticker listing of one asset type; returns its rows.

        One file per asset type, under that asset type's level of the tree, so
        writing one leaves the others alone. Replaced rather than merged: the
        vendor serves its whole universe for an asset type each time, and the
        rows carry no key to merge on.

        The rows are stored as served. The vendor lists 169 stock symbols
        twice, and leaves the name empty on 14% of the rows, so the name is a
        label and not a key -- collapsing the rows would be the store guessing
        on the reader's behalf. Read them back with ``ticker_listing``.
        """
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

    def rebuild_catalog(self) -> int:
        """Rebuild the catalog off the tree; returns the rows it now holds.

        The tree is the only other place the catalog's answers live, so this
        is what brings a store back in step after its catalog was lost, or
        after files were moved into it by hand. It reads parquet footers
        rather than bars, so the cost is the file count and not the row count.
        """
        files = sorted(
            iglob(  # noqa: PTH207 - a recursive glob string, not a Path.glob pattern
                str(self._bars_directory / "**" / PARQUET_FILES),
                recursive=True,
            ),
        )
        if not files:
            self._catalog.rebuild_from(_sql.empty_select(_catalog.SCHEMA))
            return 0
        self._catalog.rebuild_from(
            _sql.catalog_rows_select(self._file_spans_select(files)),
        )
        return int(self._catalog.relation().shape[0])

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

        No ``dataset`` parameter: the vendor's listed and delisted bundles are
        two places it files one symbol, and the store files both under the
        bare symbol, so this answers with every bar it holds for the ticker.
        Which company held the symbol over which days is
        ``ticker_listing(ticker=...)``'s to say.
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

        No ``adjustment`` parameter: a contract is served on one basis. The
        continuous series is read with ``futures_bars``.
        """
        return self._read(
            BarType(
                AssetType.FUTURES,
                timeframe=timeframe,
                dataset=Dataset.CONTRACT,
                adjustment=Adjustment.UNADJUSTED,
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
                adjustment=Adjustment.UNADJUSTED,
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
        dataset: Dataset | None = None,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Bars across the whole tree, as a lazy relation.

        Every omitted selector spans all its values, so an unfiltered call
        interleaves adjustments: the same bar appears once per adjustment it was
        fetched under. Narrow with the selectors rather than a ``WHERE`` -- they
        build the glob, which is what makes a fine slice fast. A ``dataset``
        narrows to futures on its own: no other asset type is filed under one.

        No ``hours``: the exchange session is defined per asset type, and this
        read spans them all. Ask the asset type's own method for that.
        """
        return self._read(
            BarType(
                asset_type,
                timeframe=timeframe,
                dataset=dataset,
                adjustment=adjustment,
            ),
            ticker,
            start=start,
            end=end,
        )

    def suspect_bars(
        self,
        *,
        asset_type: AssetType | None = None,
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
            asset_type=asset_type,
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
            return self._connection.sql(_sql.empty_select(_sql.QUARANTINE_SCHEMA))
        return self._connection.sql(
            # each path is escaped by _sql.sql_list()
            f"SELECT * FROM read_parquet({_sql.sql_list(str(file) for file in files)})",  # noqa: S608
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

    def missing_tickers(self, bar_type: BarType) -> list[str]:
        """The vendor's tickers for `bar_type` that the store holds no bars for.

        The listing is the vendor's whole universe for an asset type, so this
        is what a sweep of that bar type has left to fetch. Both sides key on
        the bare symbol, the delisted suffix already parsed off.

        Raises
        ------
        ValueError
            If `bar_type` names no asset type. The listing is served per one.
        FileNotFoundError
            If no listing for that asset type is in the store.

        """
        asset_type = bar_type.asset_type
        if asset_type is None:
            msg = "the ticker listing is served per asset type: name one"
            raise ValueError(msg)
        served = {
            ticker
            for (ticker,) in self.ticker_listing(asset_type=asset_type)
            .project("ticker")
            .distinct()
            .fetchall()
        }
        return sorted(served - set(self.tickers_list(bar_type)))

    def ticker_listing(
        self,
        *,
        asset_type: AssetType | None = None,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Who held a ticker and over which days, as the vendor listed it.

        One row per row served, so a symbol two companies held is two rows,
        with a name and the days each of them held it. That is what separates
        one company's bars from another's under a symbol the store files once.

        Raises
        ------
        FileNotFoundError
            If no ticker listing is in the store; fetch it first.

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

    def company_profiles(self) -> duckdb.DuckDBPyRelation:
        ...
        # TODO

    # ------------------------------------------------------------------
    # ingest internals
    # ------------------------------------------------------------------

    def _ingest_bars(self, archive: Path, request: BarsRequest) -> Ingested:
        """File one bars archive into the tree, and keep nothing else.

        The archive is unzipped inside the store, read, and deleted. Whether
        it replaces the bars it names is the source's to say: a replacing
        fetch carries the whole history of every ticker it names, so it
        supersedes what the store holds. Any other fetch adds to it, and bars
        landing on a span already filed are a collision rather than an
        overlap to trim -- ``OverlappingBarsError``, with nothing filed.

        A non-zero ``Ingested.rejected`` means the store is incomplete: those
        lines could not be parsed, and only the quarantine has them now. A
        non-zero ``Ingested.suspect`` means the opposite -- rows that parsed and
        are in the tree, but do not hold together as bars.
        """
        bar_type = request.bar_type
        suffixed = request.payload_names_carry_delisted_suffix

        # minted once for the whole archive, not per batch: the point of the id
        # is that one glob afterwards finds everything this archive wrote
        ingest = _sql.ingest_id()

        with self._unzipped(archive) as staging:
            payloads = self._payloads_by_ticker(staging, strip_delisted_suffix=suffixed)
            # read before the write, and per file rather than per ticker: which
            # of these the archive supersedes is decided file by file, and
            # after the COPY they are no longer telling apart from its own
            held = self._filed_bars(bar_type, payloads)
            columns_in_payload = _sql.payload_columns(
                payload for files in payloads.values() for payload in files
            )

            rows = 0
            for batch in batched(payloads.items(), _TICKERS_PER_COPY, strict=False):
                rows += self._copy_batch(
                    bar_type,
                    payloads=dict(batch),
                    columns_in_payload=columns_in_payload,
                    ingest=ingest,
                    strip_delisted_suffix=suffixed,
                )

        # before the collision check: the reject tables outlive this ingest, so
        # leaving them filled would charge the next one with this one's damage
        quarantine, damaged = self._drain_rejects()

        self._file_over(
            bar_type,
            held,
            self._written_spans(bar_type, ingest),
            ingest,
            supersedes=request.must_replace_existing_bars,
        )

        return Ingested(
            len(payloads),
            rows,
            self._count_suspect(bar_type, ingest),
            quarantine,
            damaged,
        )

    def _ingest_other_data(self, content: Path, other_data: OtherData) -> Ingested:
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

        quarantine, damaged = self._drain_rejects()
        # a metafile is a corporate action, not a bar: there is no open, high,
        # low or close for the suspect rule to hold against
        return Ingested(None, rows, 0, quarantine, damaged)

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
        *,
        strip_delisted_suffix: bool,
    ) -> dict[str, list[Path]]:
        """Every payload one archive unzipped to, grouped by the ticker it names."""
        grouped: defaultdict[str, list[Path]] = defaultdict(list)
        for path, ticker in self._connection.sql(
            _sql.payload_tickers_select(
                directory,
                strip_delisted_suffix=strip_delisted_suffix,
            ),
        ).fetchall():
            grouped[ticker].append(Path(path))
        return dict(grouped)

    def _copy_batch(
        self,
        bar_type: BarType,
        *,
        payloads: dict[str, list[Path]],
        columns_in_payload: int,
        ingest: str,
        strip_delisted_suffix: bool,
    ) -> int:
        """Write one batch of tickers into the tree; returns rows written."""
        select = _sql.bars_select(
            [payload for files in payloads.values() for payload in files],
            # no ticker: one COPY writes a batch of them, and each row's is
            # read back out of the payload that carried it
            bar_type,
            columns_in_payload,
            strip_delisted_suffix=strip_delisted_suffix,
        )
        levels = ", ".join(bar_type.path_levels())
        return self._execute_counting(f"""
            COPY ({select})
            TO {_sql.sql_literal(str(self._bars_directory))}
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({levels}),
             FILENAME_PATTERN '{_sql.filename_pattern(ingest)}',
             APPEND)
            """)

    def _written_spans(self, bar_type: BarType, ingest: str) -> dict[str, TickerSpan]:
        """What one ingest put in the tree, per ticker, off the files it wrote.

        Scoped to that ingest's own files by the id stamped on their names, so
        the cost is the archive's and not the store's.
        """
        files = self._ingest_files(bar_type, ingest)
        if not files:
            return {}
        ticker = _sql.hive_level_expression("file", "ticker")
        # `ticker` is built from BarType's own field names, and
        # _file_spans_select() escapes every path it reads
        spans = self._connection.sql(
            f"SELECT {ticker} AS ticker, min(first_ts), max(last_ts), sum(rows) "  # noqa: S608
            f"FROM ({self._file_spans_select(files)}) GROUP BY 1",
        ).fetchall()
        return {
            named: TickerSpan(named, first_ts, last_ts, int(rows))
            for named, first_ts, last_ts, rows in spans
        }

    def _filed_bars(
        self,
        bar_type: BarType,
        tickers: Iterable[str],
    ) -> dict[str, list[_FiledBars]]:
        """What the tree already holds for `tickers`, one entry per parquet file."""
        files = sorted(
            file
            for ticker in tickers
            # each `pattern` is a full glob string built by _get_bar_path(),
            # not decomposable into Path(base).glob(pattern) here
            for file in iglob(  # noqa: PTH207
                self._get_bar_path(bar_type.from_ticker(ticker)),
            )
        )
        if not files:
            return {}

        ticker_level = _sql.hive_level_expression("file", "ticker")
        # `ticker_level` is built from BarType's own field names, and
        # _file_spans_select() escapes every path it reads
        rows = self._connection.sql(
            f"SELECT file, {ticker_level} AS ticker, first_ts, last_ts, rows "  # noqa: S608
            f"FROM ({self._file_spans_select(files)})",
        ).fetchall()

        held: defaultdict[str, list[_FiledBars]] = defaultdict(list)
        for file, ticker, first_ts, last_ts, filed_rows in rows:
            span = TickerSpan(ticker, first_ts, last_ts, int(filed_rows))
            held[ticker].append(_FiledBars(file, span))
        return dict(held)

    def _file_over(
        self,
        bar_type: BarType,
        held: dict[str, list[_FiledBars]],
        arriving: dict[str, TickerSpan],
        ingest: str,
        *,
        supersedes: bool,
    ) -> None:
        """Settle one ingest against the bars its tickers already had.

        A file the archive covers end to end is superseded, and only where the
        archive is the whole history of what it names -- an increment carries
        a tail, and a tail that reaches back into filed bars is a second copy
        of them. Anything else that overlaps is a collision: the ingest's own
        files go, and nothing it carried is filed.

        Raises
        ------
        OverlappingBarsError
            If any ticker's arriving bars overlap bars already filed without
            covering them whole.

        """
        settled = [
            self._settle(held.get(ticker, []), span, supersedes=supersedes)
            for ticker, span in sorted(arriving.items())
        ]

        collisions = [collision for one in settled for collision in one.collisions]
        if collisions:
            for file in self._ingest_files(bar_type, ingest):
                Path(file).unlink(missing_ok=True)
            raise OverlappingBarsError(bar_type, collisions)

        for one in settled:
            for file in one.superseded:
                Path(file).unlink(missing_ok=True)
        self._catalog.record(bar_type, [one.span for one in settled])

    @staticmethod
    def _settle(
        held: list[_FiledBars],
        arriving: TickerSpan,
        *,
        supersedes: bool,
    ) -> _Settlement:
        """Weigh one ticker's arriving bars against the files it already had."""
        standing: list[TickerSpan] = []
        superseded: list[str] = []
        collisions: list[tuple[TickerSpan, TickerSpan]] = []
        for filed in held:
            if not filed.span.overlaps(arriving):
                standing.append(filed.span)
            elif supersedes and filed.span.within(arriving):
                superseded.append(filed.file)
            else:
                collisions.append((filed.span, arriving))
        # the span the catalog keeps covers what survived here together with
        # what arrived, which by now cannot overlap
        return _Settlement(
            reduce(TickerSpan.merged_with, standing, arriving),
            superseded,
            collisions,
        )

    def _ingest_files(self, bar_type: BarType, ingest: str) -> list[str]:
        """The parquet files one ingest wrote, by the id stamped on their names."""
        pattern = self._get_bar_path(
            bar_type,
            files_regex=_sql.parquet_file_ingest_id_glob(ingest),
        )
        # `pattern` is a full glob string built by _get_bar_path(), not
        # decomposable into Path(base).glob(pattern) at this call site
        return sorted(iglob(pattern))  # noqa: PTH207

    def _file_spans_select(self, files: list[str]) -> str:
        """Build a SELECT of ``(file, first_ts, last_ts, rows)`` for `files`.

        Off the parquet footers, which is a read of ~10.6 KB per file rather
        than of every row. Statistics are the writer's to omit, and a file
        without them says nothing about its contents -- reading that as an
        empty span would leave a ticker's real span unrecorded -- so those
        files, and only those, are scanned.
        """
        footers = _sql.footer_file_spans_select(files)
        # the footers come from _sql.footer_file_spans_select(), which escapes
        # every path it reads
        unstated = [
            file
            for (file,) in self._connection.sql(
                f"SELECT file FROM ({footers}) WHERE first_ts IS NULL",  # noqa: S608
            ).fetchall()
        ]
        stated = f"SELECT * FROM ({footers}) WHERE first_ts IS NOT NULL"  # noqa: S608
        if not unstated:
            return stated
        return f"{stated} UNION ALL BY NAME {_sql.scanned_file_spans_select(unstated)}"

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

    def _count_suspect(self, bar_type: BarType, ingest: str) -> int:
        """Rows one ingest wrote that do not hold together as bars.

        Scoped to that ingest's own files, so the cost is the archive's and not
        the store's, and so an increment is not charged with the damage every
        earlier run left in the bars it is extending.
        """
        files = self._ingest_files(bar_type, ingest)
        # an archive of empty payloads writes no file at all, and DuckDB reads
        # a glob that matches nothing as an error rather than as no rows
        if not files:
            return 0

        counted = self._connection.sql(
            # each path is escaped by _sql.sql_list(); the WHERE clause comes
            # from _sql.suspect_bars_where(), an internal builder
            f"SELECT count(*) FROM read_parquet({_sql.sql_list(files)}) "  # noqa: S608
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
        available = self._available_paths(bar_type, ticker)
        if not available:
            return self._connection.sql(_sql.empty_bars_select())

        # one SELECT per asset type, unioned: DuckDB refuses a single read
        # spanning paths of different Hive levels, and futures carry a
        # ``dataset`` level the other asset types do not
        bars_relation = self._connection.sql(
            "\nUNION ALL BY NAME\n".join(
                _sql.stored_bars_select(paths, asset_type)
                for asset_type, paths in available.items()
            ),
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

    def _available_paths(
        self,
        bar_type: BarType,
        ticker: str | Iterable[str] | None,
    ) -> dict[AssetType, list[str]]:
        """The globs that match files, grouped by the asset type they are under."""
        tickers = (
            [ticker] if ticker is None or isinstance(ticker, str) else list(ticker)
        )
        available: dict[AssetType, list[str]] = {}
        for asset_type in self._asset_types_of(bar_type):
            typed = replace(bar_type, asset_type=asset_type)
            paths = [
                pattern
                for one in tickers
                # each `pattern` is a full glob string built by _get_bar_path(),
                # not decomposable into Path(base).glob(pattern) here
                if next(
                    iglob(pattern := self._get_bar_path(typed.from_ticker(one))),  # noqa: PTH207
                    None,
                )
                is not None
            ]
            if paths:
                available[asset_type] = paths
        return available

    @staticmethod
    def _asset_types_of(bar_type: BarType) -> list[AssetType]:
        """The asset types a read of `bar_type` can find bars under.

        A stated one answers for itself. An unstated one spans them all, minus
        those the other levels rule out: a stated ``dataset`` is a futures
        level, and nothing else is filed under one.
        """
        if bar_type.asset_type is not None:
            return [bar_type.asset_type]
        if bar_type.dataset is not None:
            return [AssetType.FUTURES]
        return list(AssetType)

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
        self, bar_type: BarType, files_regex: str | None = PARQUET_FILES
    ) -> str:
        """The path, or glob, the tree files `bar_type` under."""
        stated = bar_type.path_levels()

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
