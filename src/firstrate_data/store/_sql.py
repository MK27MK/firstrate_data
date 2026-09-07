"""SQL the store runs."""

from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from firstrate_data.domain import AssetType, BarType, OtherData

# What a metafile's rows hold, in the vendor's order. They arrive headerless,
# one file per ticker, so the sniffer has no schema to read. It takes the
# first data row for a header, and every payload then declares different
# columns from the last. This is a hard error the moment one read spans more
# than one of them.
#
# ``contin_audit`` is missing on purpose: futures-only, never yet served, so
# there is no row format to pin. It stays on the sniffer. See issue #15.
OTHER_DATA_SCHEMA: dict[OtherData, dict[str, str]] = {
    OtherData.SPLITS: {"date": "DATE", "ratio": "DOUBLE"},
    OtherData.DIVIDENDS: {"date": "DATE", "amount": "DOUBLE"},
    # TODO add company profiles and contract dates
}

# The ticker is in the payload's *name* and nowhere in its rows, so it's the
# one column a metafile can't skip. The vendor suffixes the dividends
# payloads (``AAPL_divs.txt``) and leaves the splits ones bare
# (``AAPL.txt``). The suffix isn't part of the ticker.
OTHER_DATA_SUFFIX: dict[OtherData, str] = {OtherData.DIVIDENDS: "_divs"}

# What the catalog keeps. The key columns are BarType's own fields, in
# BarType's order, so a row addresses exactly what a path does.
CATALOG_SCHEMA: dict[str, str] = {
    **dict.fromkeys(BarType.fields(), "VARCHAR"),
    "first_ts": "TIMESTAMPTZ",
    "last_ts": "TIMESTAMPTZ",
    "rows": "BIGINT",
}

TICKER_LISTING_SCHEMA: dict[str, str] = {
    "ticker": "VARCHAR",
    "full_name": "VARCHAR",
    "start_date": "DATE",
    "end_date": "DATE",
    "is_delisted": "BOOLEAN",
}

# The six columns every bar has, typed as the parquet tree holds them.
# ``open_interest`` is NULL where the source omits it rather than absent. A
# tree of divergent schemas resolves columns by whichever file the glob
# enumerates first, so a stock-first glob would drop open_interest without
# warning.
BAR_SCHEMA: dict[str, str] = {
    "ts": "TIMESTAMPTZ",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "BIGINT",
}

OPEN_INTEREST = "open_interest"

# A bar as the tree hands it back: the payload's columns, then the levels the
# path carries. Named in this order rather than taken from ``SELECT *``, which
# appends the Hive keys alphabetically and agrees with nothing else here.
STORED_BAR_SCHEMA: dict[str, str] = {
    **BAR_SCHEMA,
    OPEN_INTEREST: "BIGINT",
    **dict.fromkeys(BarType.fields(), "VARCHAR"),
}

# macOS drops these beside the payloads too, where they match ``*.txt``
SIDECAR_PREFIX = "._"

# The vendor packs a plain-text README into the metafile archives
# (``_splits_readme.txt``) explaining the row format. Read as a ticker's
# payload, it aborts the scan. No ticker starts with an underscore, which is
# what makes the prefix a safe rule.
OTHER_DATA_NOTE_PREFIX = "_"


def is_other_data_payload(name: str) -> bool:
    """Return whether `name` is a metafile payload containing one ticker's rows.

    Examples
    --------
    >>> is_other_data_payload("AAPL.txt")
    True
    >>> is_other_data_payload("_splits_readme.txt")
    False
    >>> is_other_data_payload("._AAPL.txt")
    False

    """
    return not name.startswith((SIDECAR_PREFIX, OTHER_DATA_NOTE_PREFIX))


def sql_literal(value: str) -> str:
    """Return `value` as an SQL text literal.

    Examples
    --------
    >>> sql_literal("BRK.B")
    "'BRK.B'"
    >>> sql_literal("O'Reilly")
    "'O''Reilly'"

    """
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def sql_list(values: Iterable[str]) -> str:
    """Return `values` as an SQL list literal.

    Examples
    --------
    >>> sql_list(['/tmp/AAPL.txt', '/tmp/MSFT.txt'])
    "['/tmp/AAPL.txt', '/tmp/MSFT.txt']"
    >>> sql_list([])
    '[]'

    """
    return f"[{', '.join(sql_literal(value) for value in values)}]"


def ticker_expression(column: str) -> str:
    """Return the SQL expression extracting the ticker from the filename in `column`."""
    return f"regexp_extract(parse_filename({column}), '^([^_]+)_', 1)"


def hive_ticker_expression(column: str) -> str:
    """Return the SQL expression reading the ticker level of the path in `column`."""
    # both separators: DuckDB reports a path on Windows with backslashes, and a
    # level bounded by ``/`` alone would swallow the rest of the path
    return f"regexp_extract({column}, 'ticker=([^/\\\\]+)', 1)"


def payload_tickers_select(directory: Path) -> str:
    """Return a SELECT of ``(ticker, files)`` for every payload under `directory`."""
    pattern = sql_literal((directory / "*.txt").as_posix())
    ticker = ticker_expression("file")
    return (
        f"SELECT {ticker} AS ticker, list(file) AS files "  # noqa: S608
        f"FROM glob({pattern}) "
        # an AppleDouble sidecar matches *.txt and isn't text. The vendor's own
        # archives carry them when the zip came from a Mac
        f"WHERE NOT starts_with(parse_filename(file), {sql_literal(SIDECAR_PREFIX)}) "
        f"GROUP BY 1"
    )


def bars_select(
    payloads: Iterable[Path],
    bar_type: BarType,
    columns_in_payload: int,
) -> str:
    """Return a SELECT reading the ``.txt`` `payloads` as the store's bar schema.

    `bar_type` must leave the ticker unstated, and `columns_in_payload` is the
    count `payload_columns` returns. Timestamps are returned tz-aware.

    Raises
    ------
    ValueError
        If `bar_type` names no asset type.

    """
    asset_type = bar_type.asset_type
    # every row's timestamp reflects the zone its asset type trades in, and a
    # wildcarded level answers for none of them
    if asset_type is None:
        msg = "bars need a bar type naming an asset type"
        raise ValueError(msg)

    # the declared columns are positional, so both ends of the schema move with
    # the payload's width: an index drops volume, futures add open_interest.
    # Declaring six for a five-column file aborts the scan on the sniffer.
    has_volume = columns_in_payload >= len(BAR_SCHEMA)
    has_open_interest = columns_in_payload > len(BAR_SCHEMA)

    # ``ts`` is the one column the payload doesn't hold as the store does: it
    # arrives naive, and the timezone() below is what makes it an instant
    declared = {**BAR_SCHEMA, "ts": "TIMESTAMP"}
    if not has_volume:
        del declared["volume"]
    if has_open_interest:
        declared[OPEN_INTEREST] = "BIGINT"
    columns = ", ".join(f"'{name}': '{kind}'" for name, kind in declared.items())

    volume = "volume" if has_volume else "NULL::BIGINT AS volume"
    open_interest = (
        OPEN_INTEREST if has_open_interest else f"NULL::BIGINT AS {OPEN_INTEREST}"
    )
    files = sql_list(str(payload) for payload in payloads)
    stamped = sql_literal(asset_type.timezone())
    ticker = ticker_expression("filename")
    # the tree is the only record of these -- they're nowhere in the payload,
    # so a level this stops selecting here would read back as NULL
    levels = ",\n            ".join(
        f"{sql_literal(value)} AS {key}"
        for key, value in bar_type.stated_levels().items()
    )

    return f"""
        SELECT
            -- the DST fall-back repeats an hour of naive stamps; both copies
            -- land on the standard-time offset, which is ICU's pick
            timezone({stamped}, ts) AS ts,
            open, high, low, close, {volume},
            {open_interest},
            {levels},
            {ticker} AS ticker
        FROM read_csv(
            {files},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings within a single payload, which the
            -- sniffer refuses to pick a dialect for; strict mode off parses them
            strict_mode = false
        )
    """  # noqa: S608


def other_data_ticker_expression(column: str, other_data: OtherData) -> str:
    """Return the SQL expression extracting `other_data`'s ticker from `column`."""
    stem = f"parse_filename({column}, true)"
    suffix = OTHER_DATA_SUFFIX.get(other_data)
    if suffix is None:
        return stem
    return f"regexp_replace({stem}, {sql_literal(f'{suffix}$')}, '')"


def other_data_select(
    payloads: Iterable[Path],
    other_data: OtherData,
    per_ticker: bool,  # noqa: FBT001 - store.py's only caller passes it positionally
) -> str:
    """Return a SELECT reading `other_data`'s `payloads` as its own table.

    `per_ticker` reads one file per ticker and adds a ``ticker`` column taken
    from each filename; otherwise the payloads are read as they come.
    """
    files = sql_list(str(payload) for payload in payloads)
    declared = OTHER_DATA_SCHEMA.get(other_data)
    if declared is None or not per_ticker:
        return f"SELECT * FROM read_csv({files})"  # noqa: S608

    columns = ", ".join(f"'{name}': '{kind}'" for name, kind in declared.items())
    return f"""
        SELECT
            {other_data_ticker_expression("filename", other_data)} AS ticker,
            {", ".join(declared)}
        FROM read_csv(
            {files},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings here as it does in the bars
            strict_mode = false
        )
    """  # noqa: S608


def ingest_id() -> str:
    """Return a short id naming one ingest."""
    return uuid4().hex[:8]


def filename_pattern(ingest_id: str) -> str:
    """Return the filename pattern `ingest_id` writes its parquet files under."""
    # APPEND under PARTITION_BY requires {uuid}: a constant pattern reuses
    # data_0.parquet and overwrites with no error. The date leads so the file
    # sorts by when it landed and no glob mistakes it for an AppleDouble
    # sidecar. The ingest id between them is how an ingest finds its own files.
    return f"{datetime.now(tz=UTC).date().isoformat()}_{ingest_id}_{{uuid}}"


def empty_select(schema: dict[str, str]) -> str:
    """Return a SELECT with `schema`'s columns and no rows."""
    columns = ", ".join(f"NULL::{kind} AS {name}" for name, kind in schema.items())
    return f"SELECT {columns} WHERE FALSE"


# keyed by an optional asset type so a read that named none falls into the
# same "has no session" answer as one that trades around the clock
EXCHANGE_SESSION: dict[AssetType | None, tuple[time, time]] = dict.fromkeys(
    (AssetType.STOCK, AssetType.ETF, AssetType.INDEX),
    (time(9, 30), time(16, 0)),
)


def regular_trading_hours_where(asset_type: AssetType | None) -> str:
    """Return the predicate keeping `asset_type`'s exchange session.

    The predicate compares in the connection's timezone.

    Raises
    ------
    ValueError
        If `asset_type` is None or names no exchange session.

    """
    session = EXCHANGE_SESSION.get(asset_type)
    if session is None:
        msg = (
            f"{asset_type} has no exchange session: it trades around the clock, "
            f"or the read named no asset type to define one"
        )
        raise ValueError(msg)

    opens, closes = session
    return (
        f"ts::TIMESTAMP::TIME >= '{opens:%H:%M:%S}' "
        f"AND ts::TIMESTAMP::TIME < '{closes:%H:%M:%S}'"
    )


def date_range_where(start: date | None, end: date | None) -> str | None:
    """Return the predicate keeping days from `start` to `end`, or None for neither.

    Both bounds name a whole day and both are kept.

    Examples
    --------
    >>> date_range_where(date(2024, 1, 2), date(2024, 1, 3))
    "ts >= '2024-01-02' AND ts < '2024-01-04'"
    >>> date_range_where(None, None) is None
    True

    """
    bounds = []
    if start is not None:
        bounds.append(f"ts >= '{start:%Y-%m-%d}'")
    if end is not None:
        # half-open on the right, so the whole of ``end`` stays in
        bounds.append(f"ts < '{end + timedelta(days=1):%Y-%m-%d}'")
    return " AND ".join(bounds) or None


def stored_bars_select(glob: str | list[str]) -> str:
    """Return a SELECT reading the parquet files at `glob` as the store's columns.

    A single glob reads every ticker under it; a list reads exactly the globs
    named.
    """
    source = sql_literal(glob) if isinstance(glob, str) else sql_list(glob)
    return (
        f"SELECT {', '.join(STORED_BAR_SCHEMA)} "  # noqa: S608
        f"FROM read_parquet({source}, hive_partitioning = true)"
    )


def footer_file_spans_select(files: Iterable[str]) -> str:
    """Return a SELECT of ``(file, first_ts, last_ts, rows)`` from `files`' footers."""
    return f"""
        SELECT
            file_name AS file,
            -- stats_min is VARCHAR ('2010-03-27 07:59:00+00'); the offset
            -- makes the cast unambiguous, but the cast is mandatory
            min(stats_min::TIMESTAMPTZ) AS first_ts,
            max(stats_max::TIMESTAMPTZ) AS last_ts,
            -- one row per row group per column, so the aggregate is not optional
            sum(row_group_num_rows)::BIGINT AS rows
        FROM parquet_metadata({sql_list(files)})
        WHERE path_in_schema = 'ts'
        GROUP BY 1
    """  # noqa: S608


def bar_type_where(bar_type: BarType) -> str:
    """Return the predicate matching the catalog rows `bar_type` names.

    A level left unstated matches every value of it.
    """
    tests = [
        f"{level} = {sql_literal(value)}"
        for level, value in bar_type.stated_levels().items()
    ]
    return " AND ".join(tests) or "TRUE"


def payload_columns(payloads: Iterable[Path]) -> int:
    """Return the number of columns the `payloads` have.

    The first non-empty line of the first non-empty payload decides.
    """
    # probed rather than ruled from (asset_type, timeframe), which would be a
    # second source of truth about the vendor's file. Empty payloads are
    # routine, so one non-empty payload speaks for the archive.
    for payload in payloads:
        # bytes, not text: a spliced payload holds bytes that aren't a
        # character in the machine's locale encoding
        with payload.open("rb") as lines:
            for line in lines:
                if line.strip():
                    return line.count(b",") + 1
    return len(BAR_SCHEMA)
