from pathlib import Path

from firstrate_data.query_parameters import AssetType, Dataset, Timeframe

# The seven columns every bar has, whatever it is a bar of. ``open_interest``
# is NULL wherever the source omits it rather than absent, because a tree of
# divergent schemas resolves its columns by whichever file the glob enumerates
# first -- a stock-first glob would drop open_interest silently.
BAR_COLUMNS: dict[str, str] = {
    "ts": "TIMESTAMP",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "BIGINT",
}
OPEN_INTEREST = "open_interest"

# the tree's levels, in order. Depth must be uniform: a glob mixing trees of
# different depths fails with a Hive partition mismatch.
PARTITION_KEYS = ("asset_type", "dataset", "adjustment", "timeframe", "ticker")


def sql_literal(value: str) -> str:
    """A string as a SQL literal. Paths and tickers are not ours to trust."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def ticker_expression(column: str, dataset: Dataset) -> str:
    """A SQL expression extracting the ticker from a payload filename in `column`.

    Payloads are named ``{TICKER}_{period}_{timeframe}_{adjustment}.txt`` and
    the ticker is the one field that never contains an underscore, so this
    reads from the left. Delisted payloads suffix the symbol
    (``OLPX-DELISTED_...``); the suffix is dropped, since ``dataset`` already
    carries that distinction.
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
    """A SELECT reading one raw directory of ``.txt`` payloads as the store's schema.

    ``ts`` is cast but never shifted: the vendor does not state the bars'
    timezone, and converting a naive local time to UTC cannot resolve the
    repeated hour at the DST fall-back. Parquet stays a faithful projection
    of raw; any timezone is a view its caller chooses.
    """
    declared = dict(BAR_COLUMNS)
    if has_open_interest:
        declared[OPEN_INTEREST] = "BIGINT"
    columns = ", ".join(f"'{name}': '{kind}'" for name, kind in declared.items())

    open_interest = (
        OPEN_INTEREST if has_open_interest else f"NULL::BIGINT AS {OPEN_INTEREST}"
    )
    payloads = sql_literal(str(directory / "*.txt"))

    return f"""
        SELECT
            ts, open, high, low, close, volume,
            {open_interest},
            {sql_literal(asset_type.value)} AS asset_type,
            {sql_literal(dataset.value)} AS dataset,
            {sql_literal(adjustment)} AS adjustment,
            {sql_literal(timeframe.value)} AS timeframe,
            {ticker_expression("filename", dataset)} AS ticker
        FROM read_csv(
            {payloads},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings within a single payload, which the
            -- sniffer refuses to pick a dialect for; strict mode off parses them
            strict_mode = false
        )
    """


def directory_tickers(directory: Path, dataset: Dataset) -> str:
    """A SELECT of the distinct tickers a raw directory carries, from its filenames.

    Via DuckDB's ``glob`` so the filename-to-ticker rule is stated once, in
    SQL, and ingest and bookkeeping cannot disagree about it.
    """
    pattern = sql_literal(str(directory / "*.txt"))
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
    """The narrowest glob answering a read; ``None`` selectors become wildcards.

    The selectors build the glob rather than becoming a ``WHERE``: pruning
    happens after enumeration, so a narrow glob is ~970x faster than a wide
    one filtered afterwards at 3000 partitions.
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


def snapshot_filename(snapshot_date: str) -> str:
    """The filename pattern one snapshot's parquet files are written under."""
    # keyed on the snapshot date because a constant pattern under PARTITION_BY
    # reuses data_0.parquet: an increment overwrote a full with no error.
    # Distinct names append, and make re-ingesting one snapshot idempotent.
    return f"{snapshot_date}_{{i}}"


def bars_projection() -> str:
    """The store's columns, in the store's order."""
    # named rather than ``SELECT *``: Hive partitioning appends the key columns
    # in alphabetical order, which nothing else in the store agrees with
    return ", ".join([*BAR_COLUMNS, OPEN_INTEREST, *PARTITION_KEYS])


def empty_bars_select() -> str:
    """A SELECT with the store's columns and no rows."""
    bars = ", ".join(f"NULL::{kind} AS {name}" for name, kind in BAR_COLUMNS.items())
    keys = ", ".join(f"NULL::VARCHAR AS {key}" for key in PARTITION_KEYS)
    return f"SELECT {bars}, NULL::BIGINT AS {OPEN_INTEREST}, {keys} WHERE FALSE"
