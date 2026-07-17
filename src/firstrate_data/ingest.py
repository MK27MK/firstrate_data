from pathlib import Path

from firstrate_data.query_parameters import AssetType, Dataset, Timeframe

# The seven columns every bar has, whatever it is a bar of. ``open_interest`` is
# NULL wherever the source omits it -- which is everywhere except futures at
# 1day -- rather than absent, because a tree of divergent schemas resolves its
# columns by whichever file the glob enumerates first: a stock-first glob drops
# open_interest silently, 8 columns and a million rows and no error, while a
# futures-first glob keeps it. Uniformity costs 0.043% under zstd. See ADR 0005.
BAR_COLUMNS: dict[str, str] = {
    "ts": "TIMESTAMP",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "BIGINT",
}
OPEN_INTEREST = "open_interest"

# the tree's levels, in order. Uniform depth is not tidiness: a glob mixing a
# stock tree that has `dataset=` with a futures tree that does not fails with a
# Hive partition mismatch, and union_by_name does not rescue it.
PARTITION_KEYS = ("asset_type", "dataset", "adjustment", "timeframe", "ticker")


def _literal(value: str) -> str:
    """A string as a SQL literal. Paths and tickers are not ours to trust."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def ticker_expression(column: str, dataset: Dataset) -> str:
    """The ticker, out of the vendor's filename.

    Every payload is named ``{TICKER}_{period}_{timeframe}_{adjustment}.txt``,
    and the ticker is the one field that never contains an underscore -- checked
    across all four archive shapes, including futures, whose adjustment token
    (``continuous_UNadjusted``) does contain one, so this reads from the left.

    Delisted payloads suffix the symbol: ``OLPX-DELISTED_full_1min_...``. The
    suffix is dropped, because ``dataset`` already carries that distinction as a
    column, and a ticker that answers to ``OLPX`` in one dataset and
    ``OLPX-DELISTED`` in another cannot be asked for once. See ADR 0005.
    """
    symbol = f"regexp_extract(parse_filename({column}), '^([^_]+)_', 1)"
    if dataset is Dataset.DELISTED:
        return f"regexp_replace({symbol}, '-DELISTED$', '')"
    return symbol


def bars_select(
    directory: Path,
    asset_type: AssetType,
    dataset: Dataset,
    adjustment: str,
    timeframe: Timeframe,
    has_open_interest: bool,
) -> str:
    """One raw directory of ``.txt`` payloads, as the store's uniform schema.

    ``ts`` is cast but never shifted: the vendor's docs do not state the bars'
    timezone and we have not verified it, and converting a naive local time to
    UTC cannot resolve the repeated hour at the DST fall-back -- an hour equities
    sleep through and futures trade. A guess here would corrupt silently and
    break the promise that parquet is a faithful projection of raw. See ADR 0005.
    """
    declared = dict(BAR_COLUMNS)
    if has_open_interest:
        declared[OPEN_INTEREST] = "BIGINT"
    columns = ", ".join(f"'{name}': '{kind}'" for name, kind in declared.items())

    open_interest = (
        OPEN_INTEREST if has_open_interest else f"NULL::BIGINT AS {OPEN_INTEREST}"
    )
    payloads = _literal(str(directory / "*.txt"))

    return f"""
        SELECT
            ts, open, high, low, close, volume,
            {open_interest},
            {_literal(asset_type.value)} AS asset_type,
            {_literal(dataset.value)} AS dataset,
            {_literal(adjustment)} AS adjustment,
            {_literal(timeframe.value)} AS timeframe,
            {ticker_expression("filename", dataset)} AS ticker
        FROM read_csv(
            {payloads},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings *within a single payload*: 64 of the
            -- 66 files in one measured archive carry both bare \\n and \\r\\n, and
            -- DuckDB's sniffer refuses to pick a dialect for that, failing the
            -- whole read. Strict mode off parses them; checked against Python's
            -- csv module over every payload of an archive, row for row.
            strict_mode = false
        )
    """


def directory_tickers(directory: Path, dataset: Dataset) -> str:
    """Which tickers a raw directory carries, read from its filenames alone.

    Via DuckDB's ``glob`` rather than a Python listing so that the rule for
    turning a filename into a ticker is stated once, in SQL, and the ingest and
    the partition bookkeeping cannot disagree about it.
    """
    pattern = _literal(str(directory / "*.txt"))
    return (
        f"SELECT DISTINCT {ticker_expression('file', dataset)} AS ticker "
        f"FROM glob({pattern})"
    )


def _levels(
    asset_type: str | None,
    dataset: str | None,
    adjustment: str | None,
    timeframe: str | None,
    ticker: str | None,
) -> list[str]:
    selected = (asset_type, dataset, adjustment, timeframe, ticker)
    return [
        f"{key}={'*' if value is None else value}"
        for key, value in zip(PARTITION_KEYS, selected, strict=True)
    ]


def partition_glob(
    root: Path,
    asset_type: str | None = None,
    dataset: str | None = None,
    adjustment: str | None = None,
    timeframe: str | None = None,
    ticker: str | None = None,
    filename: str = "*.parquet",
) -> str:
    """The narrowest path that answers a read, built from its selectors.

    The selectors *build the glob*; they are not a ``WHERE``. That is the whole
    shape of the read API and it is a measurement, not taste: at 3000 partitions
    a fine slice costs 291ms through ``**/*.parquet`` filtered afterwards and
    0.3ms through a narrow glob -- ~970x -- because pruning happens after
    enumeration, and this tree will hold ~150k leaves. See ADR 0005.
    """
    return str(
        root.joinpath(
            *_levels(asset_type, dataset, adjustment, timeframe, ticker), filename
        )
    )


def partition_directory(
    root: Path,
    asset_type: str,
    dataset: str,
    adjustment: str,
    timeframe: str,
    ticker: str,
) -> Path:
    """Where exactly one partition's files live. Every level named, no wildcard."""
    return root.joinpath(*_levels(asset_type, dataset, adjustment, timeframe, ticker))


def vintage_filename(vintage: str) -> str:
    """The filename pattern one vintage's files are written under.

    Keyed on the vintage, which is not tidiness either. Measured: a constant
    pattern under ``PARTITION_BY`` reuses ``data_0.parquet``, so an increment
    overwrote a full and 100 rows became 10 with no error -- ADR 0004's
    ticker_range clobber, reincarnated one layer down. Distinct names append
    correctly, leave the vintage legible on disk, and make re-ingesting one
    vintage idempotent: it lands on its own filename. See ADR 0005.
    """
    return f"v{vintage}_{{i}}"


def bars_projection() -> str:
    """The store's columns, in the store's order.

    Named explicitly rather than taken as ``SELECT *``, because Hive partitioning
    appends the key columns in *alphabetical* order -- adjustment, asset_type,
    dataset, ticker, timeframe -- which is neither the order of the tree nor the
    order an empty relation would have to invent to match. A schema that depends
    on how DuckDB happened to sort five strings is the same class of defect as a
    schema that depends on which file the glob enumerated first.
    """
    return ", ".join([*BAR_COLUMNS, OPEN_INTEREST, *PARTITION_KEYS])


def empty_bars_select() -> str:
    """The store's columns, no rows.

    A glob that matches nothing is an IOException in DuckDB, but "no data" is an
    answer, not a failure: ``UNADJUSTED``'s legal timeframes differ per endpoint
    and cannot live on the enum, so a read asking for 5min UNADJUSTED is asking
    for something the vendor never served. It gets an empty relation of the right
    shape, which composes, rather than a traceback. See ADR 0005.
    """
    bars = ", ".join(f"NULL::{kind} AS {name}" for name, kind in BAR_COLUMNS.items())
    keys = ", ".join(f"NULL::VARCHAR AS {key}" for key in PARTITION_KEYS)
    return f"SELECT {bars}, NULL::BIGINT AS {OPEN_INTEREST}, {keys} WHERE FALSE"
