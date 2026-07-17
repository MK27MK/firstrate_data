from pathlib import Path

import duckdb

# One row per partition-snapshot. ``source`` is the raw directory the rows came
# from, relative to the raw root: two `full` snapshots of different
# ticker_ranges share every other key and differ only in which tickers they
# carry, so it is what makes sync()'s diff exact.
_SCHEMA: dict[str, str] = {
    "asset_type": "VARCHAR",
    "dataset": "VARCHAR",
    "adjustment": "VARCHAR",
    "timeframe": "VARCHAR",
    "ticker": "VARCHAR",
    "snapshot_date": "DATE",
    "source": "VARCHAR",
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
    """An index of the parquet tree, one row per partition-snapshot.

    A cache: every column is recoverable by scanning the tree and the
    snapshot records, so deleting the file costs a rebuild, not data.
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
        """Write the table back to disk, whole."""
        # via a sibling and a rename so that a Ctrl-C leaves either the old
        # manifest or the new one, never half of either
        staging = self._path.with_suffix(".parquet.partial")
        self._con.sql(
            f"COPY (SELECT * FROM {_TABLE}) TO '{staging}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        staging.replace(self._path)

    def sources(self) -> set[str]:
        """Which raw directories have already been ingested."""
        rows = self._con.sql(f"SELECT DISTINCT source FROM {_TABLE}").fetchall()
        return {row[0] for row in rows}

    def forget(self, sources: set[str]) -> None:
        """Drop the rows recorded for `sources`."""
        if not sources:
            return
        listed = ", ".join(f"'{source}'" for source in sorted(sources))
        self._con.sql(f"DELETE FROM {_TABLE} WHERE source IN ({listed})")

    def record(self, rows: duckdb.DuckDBPyRelation) -> None:
        rows.insert_into(_TABLE)
