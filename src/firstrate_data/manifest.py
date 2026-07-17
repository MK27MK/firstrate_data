from pathlib import Path

import duckdb

# One row per partition-vintage. ``source`` is the raw directory the rows came
# from, relative to the raw root: it is what makes sync()'s diff exact rather
# than inferred, since two `full` vintages of different ticker_ranges share every
# other key and differ only in which tickers they carry.
_SCHEMA: dict[str, str] = {
    "asset_type": "VARCHAR",
    "dataset": "VARCHAR",
    "adjustment": "VARCHAR",
    "timeframe": "VARCHAR",
    "ticker": "VARCHAR",
    "vintage": "DATE",
    "source": "VARCHAR",
    # the corporate actions this partition's prices already account for, in
    # effect the date they were computed as of. NULL for unadjusted series:
    # nothing restates them, so they have no basis. See ADR 0005.
    "basis": "DATE",
    "first_ts": "TIMESTAMP",
    "last_ts": "TIMESTAMP",
    "row_count": "BIGINT",
}

_TABLE = "manifest"


def _empty_select() -> str:
    columns = ", ".join(
        f"NULL::{sql_type} AS {name}" for name, sql_type in _SCHEMA.items()
    )
    return f"SELECT {columns} WHERE FALSE"


class Manifest:
    """What the parquet tree holds, one row per partition-vintage.

    A cache and nothing more: every column here is recoverable by scanning the
    tree and the raw sidecars, and ``rm``-ing this file costs a rebuild, not
    data. It exists because that scan costs ~291ms at 3000 partitions against a
    tree that will hold ~150k leaves, and ``sync()`` must not pay it per call.

    Rewritten wholesale rather than edited in place. It is a rounding error next
    to the tree it describes, and a half-written index is worse than none: it
    would claim partitions that do not exist and hide ones that do.
    """

    def __init__(self, path: Path, connection: duckdb.DuckDBPyConnection):
        self._path = path
        self._con = connection

    def load(self) -> None:
        """Read the manifest into a table for this session to work against."""
        source = (
            f"SELECT * FROM read_parquet('{self._path}')"
            if self._path.exists()
            else _empty_select()
        )
        self._con.sql(f"CREATE OR REPLACE TEMP TABLE {_TABLE} AS {source}")

    def flush(self) -> None:
        """Write the table back, whole, and swap it in.

        Via a sibling and a rename so that a Ctrl-C leaves either the old
        manifest or the new one, never half of either -- the same reason the raw
        side stages its unzips.
        """
        staging = self._path.with_suffix(".parquet.partial")
        self._con.sql(
            f"COPY (SELECT * FROM {_TABLE}) TO '{staging}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        staging.replace(self._path)

    def relation(self) -> duckdb.DuckDBPyRelation:
        return self._con.sql(f"SELECT * FROM {_TABLE}")

    def sources(self) -> set[str]:
        """Which raw directories have already been ingested."""
        rows = self._con.sql(f"SELECT DISTINCT source FROM {_TABLE}").fetchall()
        return {row[0] for row in rows}

    def forget(self, sources: set[str]) -> None:
        """Drop the rows a re-ingest is about to make untrue."""
        if not sources:
            return
        listed = ", ".join(f"'{source}'" for source in sorted(sources))
        self._con.sql(f"DELETE FROM {_TABLE} WHERE source IN ({listed})")

    def record(self, rows: duckdb.DuckDBPyRelation) -> None:
        rows.insert_into(_TABLE)
