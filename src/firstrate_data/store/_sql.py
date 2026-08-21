"""The SQL the store runs, assembled in one place.

Every statement ``Store`` runs comes from here, so the rules it depends on --
the bar schema, how a filename becomes a ticker -- exist once. The write path
and the read path share them. Where a bar goes in the tree is ``layout``'s.
"""

from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4

from firstrate_data.domain import AssetType, BarType, Dataset, MetafileType, Timeframe

# Where ``store_rejects`` puts the lines it couldn't parse. DuckDB creates both
# as temp tables on the first scan that rejects anything, then *appends* to
# them after that. They're a running total for the connection, not for one
# scan -- read them and empty them per ingest, or every later ingest inherits
# the earlier ones' damage. ``reject_errors`` holds one row per *column* that
# failed, so a line broken in two places is two rows there.
REJECTS_TABLE = "reject_errors"
REJECT_SCANS_TABLE = "reject_scans"

# What a quarantined line keeps. The payload is the vendor's filename, which is
# all that survives the staging directory the ingest deletes -- and it names the
# ticker. ``line`` is the line number within that payload.
QUARANTINE_SCHEMA: dict[str, str] = {
    "payload": "VARCHAR",
    "line": "BIGINT",
    "csv_line": "VARCHAR",
    "errors": "VARCHAR",
}

# What a metafile's rows hold, in the vendor's order. They arrive headerless,
# one file per ticker, so the sniffer has no schema to read. It takes the
# first data row for a header, and every payload then declares different
# columns from the last. This is a hard error the moment one read spans more
# than one of them.
#
# ``contin_audit`` is missing on purpose: futures-only, never yet served, so
# there is no row format to pin. It stays on the sniffer. See issue #15.
METAFILE_SCHEMA: dict[MetafileType, dict[str, str]] = {
    MetafileType.SPLITS: {"date": "DATE", "ratio": "DOUBLE"},
    MetafileType.DIVIDENDS: {"date": "DATE", "amount": "DOUBLE"},
}

# The ticker is in the payload's *name* and nowhere in its rows, so it's the
# one column a metafile can't skip. The vendor suffixes the dividends
# payloads (``AAPL_divs.txt``) and leaves the splits ones bare
# (``AAPL.txt``). The suffix isn't part of the ticker.
METAFILE_SUFFIX: dict[MetafileType, str] = {MetafileType.DIVIDENDS: "_divs"}

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

# How the ticker reads as a path segment of the tree, wherever BarType puts it
TICKER_LEVEL = "ticker="


# macOS drops these beside the payloads too, where they match ``*.txt``
SIDECAR_PREFIX = "._"

# The vendor packs a plain-text README into the metafile archives
# (``_splits_readme.txt``) explaining the row format. Read as a ticker's
# payload, it yields no rows and two dozen rejects. A non-zero reject count is
# the only signal the store has that ingest lost lines, and one that fires on
# every fetch is worse than no signal at all. No ticker starts with an
# underscore, which is what makes the prefix a safe rule.
METAFILE_NOTE_PREFIX = "_"


def is_metafile_payload(name: str) -> bool:
    """Whether a file in a metafile archive holds one ticker's rows.

    Examples
    --------
    >>> is_metafile_payload("AAPL.txt")
    True
    >>> is_metafile_payload("_splits_readme.txt")
    False
    >>> is_metafile_payload("._AAPL.txt")
    False

    """
    return not name.startswith((SIDECAR_PREFIX, METAFILE_NOTE_PREFIX))


def sql_literal(value: str) -> str:
    """Turn a Python string into a safe SQL text value.

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
    """Strings as an SQL list literal, for the functions that take many paths.

    Examples
    --------
    >>> sql_list(['/tmp/AAPL.txt', '/tmp/MSFT.txt'])
    "['/tmp/AAPL.txt', '/tmp/MSFT.txt']"
    >>> sql_list([])
    '[]'

    """
    return f"[{', '.join(sql_literal(value) for value in values)}]"


def ticker_expression(column: str, dataset: Dataset) -> str:
    """Build the SQL expression extracting a payload filename's ticker in `column`.

    The vendor names payloads ``{TICKER}_{period}_{timeframe}_{adjustment}.txt``,
    and the ticker is the one field that never contains an underscore, so this
    reads from the left. Delisted payloads suffix the ticker
    (``OLPX-DELISTED_...``). This expression drops the suffix, since
    ``dataset`` already carries that distinction.
    """
    ticker = f"regexp_extract(parse_filename({column}), '^([^_]+)_', 1)"
    if dataset is Dataset.DELISTED:
        return f"regexp_replace({ticker}, '-DELISTED$', '')"
    return ticker


def hive_ticker_expression(column: str) -> str:
    # both separators: DuckDB reports a path on Windows with backslashes, and
    # a level bounded by ``/`` alone would swallow the rest of the path into
    # the ticker
    return f"regexp_extract({column}, '{TICKER_LEVEL}([^/\\\\]+)', 1)"


def bar_timezone(asset_type: AssetType) -> str:
    """Name the IANA timezone the vendor stamps `asset_type`'s bars in."""
    # the vendor's stated rule: U.S. Eastern for everything except crypto,
    # which trades around the clock and carries a UTC stamp instead
    if asset_type is AssetType.CRYPTO:
        return "UTC"
    return "America/New_York"


def payload_tickers_select(directory: Path, dataset: Dataset) -> str:
    """Build a SELECT of ``(file, ticker)`` for every payload one archive unzipped to.

    Via DuckDB's ``glob``, so the filename-to-ticker rule exists once, in
    SQL, and the ingest and the removal it does first can't disagree about it.
    """
    pattern = sql_literal((directory / "*.txt").as_posix())
    # pattern and SIDECAR_PREFIX go through sql_literal(), and
    # ticker_expression() only assembles internal SQL fragments. Neither comes
    # from user input.
    return (
        f"SELECT file, {ticker_expression('file', dataset)} AS ticker "  # noqa: S608
        f"FROM glob({pattern}) "
        # an AppleDouble sidecar matches *.txt and isn't text. The vendor's own
        # archives carry them when the zip came from a Mac
        f"WHERE NOT starts_with(parse_filename(file), {sql_literal(SIDECAR_PREFIX)})"
    )


def bars_select(
    payloads: Iterable[Path],
    bar_type: BarType,
    columns_in_payload: int,
) -> str:
    """Build a SELECT reading unzipped ``.txt`` payloads as the store's schema.

    ``ts`` arrives naive in the vendor's clock and leaves tz-aware.
    `bar_type` must leave the ticker unnamed: one file per ticker.
    `columns_in_payload` comes from `payload_columns`.
    """
    asset_type, dataset = bar_type.asset_type, bar_type.dataset
    # every row's timestamp reflects the zone its asset type trades in, and
    # its key reflects the ticker rule its dataset uses. A wildcarded level
    # answers neither.
    if asset_type is None or dataset is None:
        msg = "bars need a bar type naming an asset type and a dataset"
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
    stamped = sql_literal(bar_timezone(asset_type))
    # the tree is the only record of these -- they're nowhere in the payload,
    # so a level this stops selecting here would read back as NULL
    levels = ",\n            ".join(
        f"{sql_literal(value)} AS {key}"
        for key, value in bar_type.to_dict(drop_none=True).items()
    )

    # stamped, levels and files go through sql_literal() and sql_list().
    # volume, open_interest and columns come from this module's own schema
    # constants. ticker_expression() only assembles internal SQL fragments.
    # None of this comes from user input.
    return f"""
        SELECT
            -- the DST fall-back repeats an hour of naive stamps; both copies
            -- land on the standard-time offset, which is ICU's pick
            timezone({stamped}, ts) AS ts,
            open, high, low, close, {volume},
            {open_interest},
            {levels},
            {ticker_expression("filename", dataset)} AS ticker
        FROM read_csv(
            {files},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings within a single payload, which the
            -- sniffer refuses to pick a dialect for; strict mode off parses them
            strict_mode = false,
            -- a handful of payloads in the archive have spliced bytes: a bar run
            -- cut off mid-stamp with a bar from days later running straight into
            -- it. Without this the first such line aborts the scan, and since a
            -- scan covers a whole batch, one bad line costs several hundred
            -- healthy tickers. Quarantined instead, into the reject table, which
            -- the ingest reports -- dropping rows quietly would leave the store
            -- looking complete while it silently was not.
            store_rejects = true
        )
    """  # noqa: S608


def metafile_ticker_expression(column: str, metafile_type: MetafileType) -> str:
    """Build an SQL expression extracting the ticker from a metafile payload's name."""
    stem = f"parse_filename({column}, true)"
    suffix = METAFILE_SUFFIX.get(metafile_type)
    if suffix is None:
        return stem
    return f"regexp_replace({stem}, {sql_literal(f'{suffix}$')}, '')"


def metafile_select(
    payloads: Iterable[Path],
    metafile_type: MetafileType,
    per_ticker: bool,  # noqa: FBT001 - store.py's only caller passes it positionally
) -> str:
    """Build a SELECT reading one metafile's payloads as its own table.

    ``per_ticker`` says the payloads came out of an archive, one file per
    ticker: this expression recovers the ticker from the filename because the
    rows don't carry it. A bare CSV names none, so it stays on the sniffer
    (issue #15).
    """
    files = sql_list(str(payload) for payload in payloads)
    declared = METAFILE_SCHEMA.get(metafile_type)
    if declared is None or not per_ticker:
        # sql_list() escapes files, which never comes from user input
        return f"SELECT * FROM read_csv({files}, store_rejects = true)"  # noqa: S608

    columns = ", ".join(f"'{name}': '{kind}'" for name, kind in declared.items())

    # sql_list() escapes files. columns and declared come from this module's
    # METAFILE_SCHEMA constant. metafile_ticker_expression() only assembles
    # internal SQL fragments. None of this comes from user input.
    return f"""
        SELECT
            {metafile_ticker_expression("filename", metafile_type)} AS ticker,
            {", ".join(declared)}
        FROM read_csv(
            {files},
            header = false,
            filename = true,
            columns = {{{columns}}},
            -- the vendor mixes line endings here as it does in the bars
            strict_mode = false,
            -- quarantined rather than aborting a read that spans every ticker
            store_rejects = true
        )
    """  # noqa: S608


def ingest_id() -> str:
    """Generate a short id naming one ingest, to stamp on the files it writes."""
    # eight hex digits: enough that two ingests of one sweep can't collide,
    # short enough that the filename stays readable
    return uuid4().hex[:8]


def filename_pattern(ingest_id: str) -> str:
    """Build the filename pattern under which one ingest writes its parquet files."""
    # the uuid isn't decoration. A constant pattern under PARTITION_BY reuses
    # data_0.parquet, so an increment overwrote a full with no error. A date
    # alone collides as soon as the same partition takes two writes in one
    # day -- exactly what appending increments do. APPEND mode requires
    # {uuid} regardless. The date leads so the file sorts and reads by when it
    # landed, and so nothing mistakes it for an AppleDouble sidecar.
    #
    # The ingest id sits between them so an ingest can find its own files
    # afterward. Without it, scanning what one archive wrote means scanning
    # every file its partitions hold -- every earlier run's included.
    return f"{datetime.now(tz=UTC).date().isoformat()}_{ingest_id}_{{uuid}}"


def parquet_file_ingest_id_glob(ingest_id: str) -> str:
    # leads with the same [0-9] as PARQUET_FILES, so an ingest's files are
    # ordinary files of the tree that a read finds without knowing this exists
    return f"[0-9]*_{ingest_id}_*.parquet"


def suspect_bars_where() -> str:
    """Build the predicate matching rows that break a bar's own arithmetic.

    A spliced line that happens to land on a comma parses cleanly and enters the
    store as a bar, so nothing counts it as damaged. These four orderings and the
    volume floor are what such a bar tends to break, and what no genuine bar can.
    They hold for every asset type, because back-adjusting shifts prices without
    reordering them.
    """
    # NULLs aren't suspect. Open_interest aside, a NULL price is the store's
    # answer for a column the source doesn't carry, and every comparison here
    # would swallow it into neither camp anyway
    return """
        high < low
        OR high < open OR high < close
        OR low > open OR low > close
        OR volume < 0
    """


def quarantine_filename() -> str:
    """Build the filename under which one ingest writes its quarantined lines."""
    # the uuid keeps two ingests on the same day from overwriting each other,
    # which is routine: a sweep files sixty archives in an afternoon. The date
    # leads so the files sort by when the ingest found the damage.
    return f"{datetime.now(tz=UTC).date().isoformat()}_{uuid4()}"


def empty_quarantine_select() -> str:
    """Build a SELECT with the quarantine's columns and no rows."""
    columns = ", ".join(
        f"NULL::{kind} AS {name}" for name, kind in QUARANTINE_SCHEMA.items()
    )
    return f"SELECT {columns} WHERE FALSE"


def quarantine_by_payload(files: str) -> str:
    """How many lines each payload lost, worst first."""
    # sql_literal() escapes files, which never comes from user input
    return f"""
        SELECT payload, count(*) AS lines
        FROM read_parquet({sql_literal(files)})
        GROUP BY payload
        ORDER BY lines DESC, payload
    """  # noqa: S608


def rejected_lines_select() -> str:
    """Build a SELECT of the lines the reject tables hold, one row per line.

    ``reject_errors`` carries one row per *column* that failed to parse, so a
    line broken in two places appears twice. Grouped back to one row per line
    here, which is what a dropped bar actually costs.
    """
    # the payload is a staging path that ingest deletes, so the basename is
    # what's worth keeping: it names the ticker the line belonged to
    #
    # REJECTS_TABLE and REJECT_SCANS_TABLE are this module's own constants,
    # not user input
    return f"""
        SELECT
            parse_filename(scans.file_path) AS payload,
            errors.line AS line,
            any_value(errors.csv_line) AS csv_line,
            string_agg(DISTINCT errors.error_message, ' | ') AS errors
        FROM {REJECTS_TABLE} AS errors
        JOIN {REJECT_SCANS_TABLE} AS scans USING (scan_id, file_id)
        GROUP BY ALL
    """  # noqa: S608


# The bar type levels a resample groups by. The timeframe is missing on purpose:
# the resample changes it, so this restates it as the target instead.
_RESAMPLE_KEYS = tuple(key for key in BarType.fields() if key != "timeframe")


def regular_trading_hours_where(asset_type: AssetType) -> str:
    """Build the predicate keeping ``asset_type``'s exchange session.

    The cast to ``TIMESTAMP`` reads the connection's timezone, which the store
    sets to the zone the bars carry a stamp in. This predicate runs in local
    clock time and holds across daylight saving.

    This uses one cast rather than ``hour`` and ``minute`` tests because that
    conversion is the whole cost of the filter. DuckDB shares the repeated
    ``ts::TIMESTAMP::TIME`` between the two bounds but doesn't share a
    repeated ``hour(ts)``. The four-call form converts every row four times:
    2.28s versus 0.88s over 30M minute bars, for the same rows.

    Raises
    ------
    ValueError
        If the asset type trades around the clock and has no session.

    """
    rth: dict[AssetType, tuple[time, time]] = {
        AssetType.STOCK: (time(9, 30), time(16, 0)),
        AssetType.ETF: (time(9, 30), time(16, 0)),
        AssetType.INDEX: (time(9, 30), time(16, 0)),
    }

    session = rth.get(asset_type)
    if session is None:
        msg = f"{asset_type} trades around the clock: it has no session"
        raise ValueError(msg)

    opens, closes = session
    return (
        f"ts::TIMESTAMP::TIME >= '{opens:%H:%M:%S}' "
        f"AND ts::TIMESTAMP::TIME < '{closes:%H:%M:%S}'"
    )


def date_range_where(start: date | None, end: date | None) -> str | None:
    """Build the predicate keeping days from ``start`` to ``end``, or None for neither.

    Both ends name a whole day, and this keeps both, so a bar stamped 15:59 on
    ``end`` is inside the range. This ignores the time of day of either bound,
    which is what makes a ``datetime`` and the ``date`` under it the same
    request.

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


def resample_aggregate(timeframe: Timeframe) -> tuple[str, str]:
    """Build the projection and grouping that resample bars into ``timeframe`` buckets.

    One bar per bucket per ticker, in the store's column order: the first
    bar's open, the extremes across the bucket, and the last bar's close.
    The volume sums across the bucket. The ``timeframe`` column restates the
    target, so a resampled relation doesn't still claim to be minutes.
    """
    # ``date_trunc`` reads the session timezone but ``time_bucket`` doesn't: a day
    # sampled by ``time_bucket`` starts from the UTC epoch. Sub-hour bins are safe
    # either way.
    buckets = [
        "time_bucket(INTERVAL 1 MINUTE, ts)",
        "time_bucket(INTERVAL 5 MINUTE, ts)",
        "time_bucket(INTERVAL 30 MINUTE, ts)",
        "date_trunc('hour', ts)",
        "date_trunc('day', ts)",
    ]
    timeframe_bucket_sql = dict(zip(list(Timeframe), buckets, strict=False))

    projection = f"""
        {timeframe_bucket_sql[timeframe]} AS ts,
        arg_min(open, ts) AS open,
        max(high) AS high,
        min(low) AS low,
        arg_max(close, ts) AS close,
        sum(volume) AS volume,
        -- a bucket's open interest is the position left standing at its end,
        -- never the sum of the readings inside it
        arg_max({OPEN_INTEREST}, ts) AS {OPEN_INTEREST},
        {sql_literal(timeframe.value)} AS timeframe,
        {", ".join(_RESAMPLE_KEYS)}
    """
    grouping = ", ".join([timeframe_bucket_sql[timeframe], *_RESAMPLE_KEYS])
    return projection, grouping


def bars_projection() -> str:
    """List the store's columns, in the store's order."""
    # named rather than ``SELECT *``: Hive partitioning appends the key columns
    # in alphabetical order, which nothing else in the store agrees with
    return ", ".join([*BAR_SCHEMA, OPEN_INTEREST, *BarType.fields()])


def empty_bars_select() -> str:
    """Build a SELECT with the store's columns and no rows."""
    bars = ", ".join(f"NULL::{kind} AS {name}" for name, kind in BAR_SCHEMA.items())
    keys = ", ".join(f"NULL::VARCHAR AS {key}" for key in BarType.fields())
    return f"SELECT {bars}, NULL::BIGINT AS {OPEN_INTEREST}, {keys} WHERE FALSE"


def payload_columns(payloads: Iterable[Path]) -> int:
    """How many columns this archive's payloads carry.

    Six is the common bar. Futures add ``open_interest`` as a seventh. An index
    payload has five: a published level has no volume behind it.
    """
    # probed rather than ruled from (asset_type, timeframe): a rule would be a
    # second source of truth about the vendor's file, free to drift from it.
    # Empty payloads are routine, so one non-empty payload speaks for the archive.
    for payload in payloads:
        # bytes, not text. A spliced payload holds bytes that aren't a
        # character in the machine's locale encoding, and this counts a width
        # from the commas rather than reading it
        with payload.open("rb") as lines:
            for line in lines:
                if line.strip():
                    return line.count(b",") + 1
    return len(BAR_SCHEMA)
