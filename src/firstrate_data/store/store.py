import shutil
import zipfile
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
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
    OtherData,
    TickerListing,
    Timeframe,
    TradingHours,
)
from firstrate_data.download.requests import (
    BarsRequest,
    OtherDataRequest,
    Request,
)
from firstrate_data.store import _sql
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
    # tickers is None for a metafile, which has no ticker dimension to count
    tickers: int | None
    rows: int


@dataclass(frozen=True, slots=True)
class TickerSpan:
    """The bars the store holds for one ticker, as one unbroken span.

    ``first_ts`` and ``last_ts`` are the ends of what is filed, not of what
    the vendor has: a store holding two disjoint stretches of a recycled
    symbol reports one span covering the gap between them.
    """

    ticker: str
    first_ts: datetime
    last_ts: datetime
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
    """A parquet store of market data, created at ``directory/firstrate_data``."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory / "firstrate_data"
        self._bars_directory = self._directory / "bars"
        self._spool = self._directory / "spool"

        self._bars_directory.mkdir(parents=True, exist_ok=True)
        self._spool.mkdir(parents=True, exist_ok=True)

        # DuckDB spills to `.tmp` in the working directory by default, which is
        # the machine's boot disk. We make it spill inside the store.
        # https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads#spilling-to-disk
        # https://duckdb.org/docs/stable/configuration/overview#global-configuration-options
        spill = self._directory / ".duckdb_temp"
        spill.mkdir(parents=True, exist_ok=True)

        self._connection = duckdb.connect()
        self._connection.execute(f"SET temp_directory = '{spill}'")
        # nothing here reads the tree in input order, so preserving that order
        # only costs memory -- and it costs it in proportion to the archive
        self._connection.execute("SET preserve_insertion_order = false")
        self._connection.execute(f"SET TimeZone = '{TIMEZONE}'")

        self._catalog_table = ParquetTable(
            self._connection,
            self._directory / "catalog.parquet",
            _sql.CATALOG_SCHEMA,
        )

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
        return table.rewrite(f"SELECT * FROM {staged}")  # noqa: S608

    def _write_bars(self, archive: Path, request: BarsRequest) -> Ingested:
        bar_type = request.bar_type

        # minted once for the whole archive, not per batch: the point of the id
        # is that one glob afterwards finds everything this archive wrote
        ingest = _sql.ingest_id()

        with self._unzipped(archive) as staging:
            payloads = self._payloads_by_ticker(staging)
            held = self._held_spans(bar_type, payloads)
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
        # replaced whole rather than reconciled: a metafile is small, the vendor
        # serves the entire history each time, and it carries no key to merge on
        with self._other_data_payloads(content, other_data) as (payloads, per_ticker):
            rows = self._other_data_table(other_data).rewrite(
                _sql.other_data_select(payloads, other_data, per_ticker),
            )
        return Ingested(None, rows)

    def bars(  # noqa: PLR0913
        self,
        *,
        asset_type: AssetType | None = None,
        timeframe: Timeframe | None = None,
        adjustment: Adjustment | None = None,
        ticker: str | Iterable[str] | None = None,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
    ) -> duckdb.DuckDBPyRelation:
        """Read bars across the tree; every omitted selector spans all its values.

        Raises
        ------
        ValueError
            If ``hours`` names a session and ``asset_type`` defines none.

        """
        return self._read(
            BarType(asset_type, timeframe=timeframe, adjustment=adjustment),
            ticker,
            start=start,
            end=end,
            hours=hours,
        )

    def last_bar(
        self,
        bar_type: BarType,
        ticker_prefix: str | None = None,
    ) -> datetime | None:
        """Return the timestamp of the newest bar under `bar_type`, narrowed to `ticker_prefix`, or None."""
        # max, not min: an ingest writes an archive as one unit, so the newest
        # bar dates the whole ingest. A min would return the last bar of the
        # most long-dead ticker in the range, which no later download updates
        # https://duckdb.org/docs/stable/sql/functions/aggregates#maxarg
        named = (
            ""
            if ticker_prefix is None
            else f" AND starts_with(ticker, {_sql.sql_literal(ticker_prefix.upper())})"
        )
        latest = self._connection.sql(
            f"SELECT max(last_ts) FROM ({self._catalog_table.select()}) "  # noqa: S608
            f"WHERE {_sql.bar_type_where(bar_type)}{named}",
        ).fetchone()
        return latest[0] if latest else None

    def catalog(self) -> duckdb.DuckDBPyRelation:
        return self._catalog_table.relation()

    def ticker_listing(
        self,
        *,
        asset_type: AssetType | None = None,
        ticker: str | None = None,
    ) -> duckdb.DuckDBPyRelation:
        """Read the ticker listing.

        Raises
        ------
        FileNotFoundError
            If no ticker listing is in the store.

        """
        asset_types = [asset_type] if asset_type is not None else list(AssetType)
        stored = [
            table
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
                for table in stored
            ),
        )
        if ticker is not None:
            listing = listing.filter(f"ticker = {_sql.sql_literal(ticker)}")
        return listing

    def splits(self) -> duckdb.DuckDBPyRelation:
        return self._other_data(OtherData.SPLITS)

    def dividends(self) -> duckdb.DuckDBPyRelation:
        return self._other_data(OtherData.DIVIDENDS)

    def contract_dates(self) -> duckdb.DuckDBPyRelation:
        return self._other_data(OtherData.CONTRACT_DATES)

    @contextmanager
    def _unzipped(self, archive: Path) -> Generator[Path]:
        staging = Path(mkdtemp(dir=self._directory, prefix=".ingest-"))
        try:
            with zipfile.ZipFile(archive) as opened:
                opened.extractall(staging)
            yield staging
        finally:
            # only once the write has committed or failed -- an ingest that
            # unlinked first would be reading files it had already promised to
            # delete. A failed ingest costs a re-unzip, not a re-fetch.
            shutil.rmtree(staging, ignore_errors=True)

    def _payloads_by_ticker(self, directory: Path) -> dict[str, list[Path]]:
        return {
            ticker: [Path(file) for file in files]
            for ticker, files in self._connection.sql(
                _sql.payload_tickers_select(directory),
            ).fetchall()
        }

    def _copy_batch(
        self,
        bar_type: BarType,
        destination: Path,
        *,
        payloads: dict[str, list[Path]],
        columns_in_payload: int,
        ingest: str,
    ) -> int:
        select = _sql.bars_select(
            [payload for files in payloads.values() for payload in files],
            # no ticker: one COPY writes a batch of them, and each row's is
            # read back out of the payload that carried it
            bar_type,
            columns_in_payload,
        )
        levels = ", ".join(bar_type.levels())
        written = self._connection.execute(f"""
            COPY ({select})
            TO {_sql.sql_literal(str(destination))}
            (FORMAT PARQUET,
             COMPRESSION ZSTD,
             PARTITION_BY ({levels}),
             FILENAME_PATTERN '{_sql.filename_pattern(ingest)}',
             APPEND)
            """).fetchall()
        return int(written[0][0]) if written else 0

    def _held_spans(
        self,
        bar_type: BarType,
        tickers: Iterable[str],
    ) -> dict[str, TickerSpan]:
        """The span the catalog holds for each of `tickers` under `bar_type`."""
        named = _sql.sql_list(tickers)
        held = self._connection.sql(
            f"SELECT ticker, first_ts, last_ts, rows "  # noqa: S608
            f"FROM ({self._catalog_table.select()}) "
            f"WHERE {_sql.bar_type_where(bar_type)} AND ticker IN {named}",
        ).fetchall()
        return {
            ticker: TickerSpan(ticker, first_ts, last_ts, int(rows))
            for ticker, first_ts, last_ts, rows in held
        }

    def _written_spans(
        self,
        root: Path,
        bar_type: BarType,
        ingest: str,
    ) -> dict[str, TickerSpan]:
        # leads with the same [0-9] as PARQUET_FILES, so an ingest's files are
        # ordinary files of the tree that a read finds without knowing this exists
        files = sorted(
            str(file)
            for file in self._bar_files(
                bar_type,
                files=f"[0-9]*_{ingest}_*.parquet",
                root=root,
            )
        )
        if not files:
            return {}
        ticker = _sql.hive_ticker_expression("file")
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
        # an update is the same history downloaded again: the same first bar,
        # and a last one no earlier than the held one. A different start is a
        # different history under one name, and an archive ending before what
        # is filed is less than the store holds. An archive ending on the same
        # bar is the same archive, re-fetched -- filing it over replaces the
        # ticker's files with identical ones, which is what makes a re-run of
        # an interrupted bundle resume rather than refuse
        conflicts = [
            (held[ticker], span)
            for ticker, span in sorted(arriving.items())
            if ticker in held
            and not (
                span.first_ts == held[ticker].first_ts
                and span.last_ts >= held[ticker].last_ts
            )
        ]
        if conflicts:
            raise ConflictingBarsError(bar_type, conflicts)

        # the ticker's held bars go only once their replacement is in the tree,
        # and only the files this ingest did not write: an update carries the
        # whole history again, so what it lands beside is the previous copy of it
        landed = self._move_into_tree(staged_tree)
        for ticker in arriving.keys() & held.keys():
            for file in self._bar_files(bar_type.from_ticker(ticker)):
                if file not in landed:
                    file.unlink(missing_ok=True)

        self._record_spans(bar_type, [span for _, span in sorted(arriving.items())])

    def _record_spans(self, bar_type: BarType, spans: Iterable[TickerSpan]) -> None:
        """File `spans` under `bar_type`, over whatever the catalog held for them.

        `bar_type` names every level but the ticker, which each span carries.
        """
        levels = bar_type.from_ticker(None).levels()
        staged = self._catalog_table.stage(
            (
                *(levels[level] for level in BarType.fields() if level != "ticker"),
                span.ticker,
                span.first_ts,
                span.last_ts,
                span.rows,
            )
            for span in spans
        )
        if staged is None:
            return

        matched = " AND ".join(
            f"held.{level} IS NOT DISTINCT FROM staged.{level}"
            for level in BarType.fields()
        )
        self._catalog_table.rewrite(f"""
            SELECT * FROM ({self._catalog_table.select()}) AS held
            WHERE NOT EXISTS (
                SELECT 1 FROM {staged} AS staged WHERE {matched}
            )
            UNION ALL BY NAME
            SELECT * FROM {staged}
        """)  # noqa: S608

    def _move_into_tree(self, staged_tree: Path) -> set[Path]:
        # a rename rather than a copy: the staging directory lives inside the
        # store, so the bars never cross a filesystem however large the archive
        landed = set()
        for staged in staged_tree.rglob(PARQUET_FILES):
            landing = self._bars_directory / staged.relative_to(staged_tree)
            landing.parent.mkdir(parents=True, exist_ok=True)
            staged.replace(landing)
            landed.add(landing)
        return landed

    @contextmanager
    def _other_data_payloads(
        self,
        content: Path,
        other_data: OtherData,
    ) -> Generator[tuple[list[Path], bool]]:
        # yields the payloads and whether each is named for one ticker -- an
        # archive's are, a bare CSV's is not -- which decides whether the ticker
        # can be read back at all
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

    def _read(
        self,
        bar_type: BarType,
        ticker: str | Iterable[str] | None,
        *,
        start: date | None = None,
        end: date | None = None,
        hours: TradingHours = TradingHours.ALL,
    ) -> duckdb.DuckDBPyRelation:
        # DuckDB raises on a glob matching nothing, and an empty store, an
        # unwritten bar type, or an unheld ticker matches nothing
        glob = self._matching_glob(bar_type, ticker)
        if glob is None:
            return self._connection.sql(_sql.empty_select(_sql.STORED_BAR_SCHEMA))

        bars_relation = self._connection.sql(_sql.stored_bars_select(glob))

        # both are ``WHERE``, not selectors: neither the clock nor the calendar
        # is a level of the tree, so no glob can narrow them
        dates = _sql.date_range_where(start, end)
        if dates is not None:
            bars_relation = bars_relation.filter(dates)
        if hours is TradingHours.REGULAR:
            bars_relation = bars_relation.filter(
                _sql.regular_trading_hours_where(bar_type.asset_type),
            )
        return bars_relation

    def _ticker_listing_table(self, asset_type: AssetType) -> ParquetTable:
        return ParquetTable(
            self._connection,
            self._bars_directory
            / f"asset_type={asset_type.value}"
            / "ticker_listing.parquet",
            _sql.TICKER_LISTING_SCHEMA,
        )

    def _other_data_table(self, other_data: OtherData) -> ParquetTable:
        # no declared schema: the vendor's columns are whatever the sniffer
        # read, so `_other_data` refuses a missing metafile rather than
        # answering with an empty relation of a shape it cannot know
        return ParquetTable(
            self._connection,
            self._directory / f"{other_data.value}.parquet",
            {},
        )

    def _other_data(self, other_data: OtherData) -> duckdb.DuckDBPyRelation:
        table = self._other_data_table(other_data)
        if not table.exists():
            msg = f"no {other_data.value} metafile in this store -- fetch it first"
            raise FileNotFoundError(msg)
        return table.relation()

    def _matching_glob(
        self,
        bar_type: BarType,
        ticker: str | Iterable[str] | None,
    ) -> str | list[str] | None:
        """AI: Return the glob(s) for the files `bar_type` and `ticker` match.

        Returns ``None`` when the store has no file for any of them.
        """
        if ticker is None:
            if next(self._bar_files(bar_type), None) is None:
                return None
            return self._bar_glob(bar_type)

        named = [ticker] if isinstance(ticker, str) else list(ticker)
        scoped = [bar_type.from_ticker(one) for one in named]
        held = [one for one in scoped if next(self._bar_files(one), None) is not None]
        return [self._bar_glob(one) for one in held] or None

    def _bar_files(
        self,
        bar_type: BarType,
        files: str = PARQUET_FILES,
        root: Path | None = None,
    ) -> Generator[Path]:
        # `root` names a staged tree an ingest is still being validated in
        return (root or self._bars_directory).glob(_bar_pattern(bar_type, files))

    def _bar_glob(self, bar_type: BarType) -> str:
        path = self._bars_directory / _bar_pattern(bar_type, PARQUET_FILES)
        # forward slashes, which DuckDB's glob and Python's both read on every
        # platform. A backslash is the pattern language's escape character, so a
        # native Windows path would read ``\[0-9]`` as the literal ``[0-9]``.
        return path.as_posix()


def _bar_pattern(bar_type: BarType, files: str) -> str:
    """Build the tree-relative glob matching every file `bar_type` addresses."""
    levels = (
        f"{key}={'*' if value is None else value}"
        for key, value in bar_type.levels().items()
    )
    return "/".join([*levels, files])
