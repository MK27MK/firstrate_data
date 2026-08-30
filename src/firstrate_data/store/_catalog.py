"""The catalog: which tickers the store holds, and the bars it holds for each.

One row per ticker per bar type, kept in step with the tree by every ingest
and every removal. It answers "which tickers are in the store" and "what does
the store already hold for this one" without opening a bar file, and it is
what lets an ingest refuse a span the store already holds instead of filing a
second copy of it.

Parquet, beside the bars, because parquet is the only copy. The file is small
enough -- one row per ticker per bar type -- to be rewritten whole on each
change, which is what makes a reader see either the previous catalog or the
new one and never a half-written file.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import duckdb

from firstrate_data.domain import AssetType, BarType
from firstrate_data.store import _sql
from firstrate_data.store._parquet_table import ParquetTable

# What the catalog keeps. The five key columns are BarType's own fields, in
# BarType's order, so a row addresses exactly what a path does -- ``dataset``
# included, which is NULL for the asset types the tree carries no such level
# under.
SCHEMA: dict[str, str] = {
    **dict.fromkeys(BarType.fields(), "VARCHAR"),
    "first_ts": "TIMESTAMPTZ",
    "last_ts": "TIMESTAMPTZ",
    "rows": "BIGINT",
}


@dataclass(frozen=True, slots=True)
class TickerSpan:
    """The bars the store holds for one ticker, as one unbroken span.

    ``first_ts`` and ``last_ts`` are the ends of what is filed, not of what
    the vendor has: a store holding two disjoint stretches of a recycled
    symbol reports one span covering the gap between them. That is what the
    collision rule needs -- anything landing inside those ends is a second
    copy of bars already filed.
    """

    ticker: str
    first_ts: datetime
    last_ts: datetime
    rows: int

    def overlaps(self, other: Self) -> bool:
        """Whether the two spans share any instant. Both ends are inclusive."""
        return self.first_ts <= other.last_ts and other.first_ts <= self.last_ts

    def within(self, other: Self) -> bool:
        """Whether `other` covers this span whole, both ends included."""
        return other.first_ts <= self.first_ts and self.last_ts <= other.last_ts

    def merged_with(self, other: Self) -> Self:
        """The span covering both, with their rows summed.

        Sound only where the two do not overlap, which is what the ingest
        checks before it merges.
        """
        return type(self)(
            self.ticker,
            min(self.first_ts, other.first_ts),
            max(self.last_ts, other.last_ts),
            self.rows + other.rows,
        )


class Catalog:
    """The store's index of tickers, read and written as one parquet file."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, path: Path) -> None:
        self._connection = connection
        self._table = ParquetTable(connection, path, SCHEMA)

    def relation(self) -> duckdb.DuckDBPyRelation:
        """The whole catalog, as a lazy relation. Empty in an empty store."""
        return self._table.relation()

    def tickers(self, bar_type: BarType) -> list[str]:
        """The tickers the store holds bars for under `bar_type`, sorted.

        A level `bar_type` leaves unstated spans all its values, the way it
        does in a read.
        """
        return [
            ticker
            for (ticker,) in self._connection.sql(
                # `where_bar_type` escapes each level, and `_select` names
                # this store's own catalog file
                f"SELECT DISTINCT ticker FROM ({self._table.select()}) "  # noqa: S608
                f"WHERE {where_bar_type(bar_type)} ORDER BY 1",
            ).fetchall()
        ]

    def record(self, bar_type: BarType, spans: Iterable[TickerSpan]) -> None:
        """File `spans` under `bar_type`, over whatever the catalog held for them.

        `bar_type` names every level but the ticker, which each span carries.
        """
        staged = self._stage(bar_type, spans)
        if staged is None:
            return
        matched = " AND ".join(
            f"held.{level} IS NOT DISTINCT FROM staged.{level}"
            for level in BarType.fields()
        )
        # the staging table's name is the store's own and `matched` is
        # assembled from BarType's field names, never from user input
        self._table.rewrite(f"""
            SELECT * FROM ({self._table.select()}) AS held
            WHERE NOT EXISTS (
                SELECT 1 FROM {staged} AS staged WHERE {matched}
            )
            UNION ALL BY NAME
            SELECT * FROM {staged}
        """)  # noqa: S608

    def rebuild_from(self, rows: str) -> None:
        """Replace the whole catalog with what `rows` selects.

        For a store whose catalog was lost or never written: `rows` is read
        off the tree itself, which is the only other place the answer exists.
        """
        self._table.rewrite(rows)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _stage(self, bar_type: BarType, spans: Iterable[TickerSpan]) -> str | None:
        """Load the rows of one update into the staging table, or None for no rows."""
        return self._table.stage(
            tuple(
                {
                    **bar_type.to_dict(),
                    "ticker": span.ticker,
                    "first_ts": span.first_ts,
                    "last_ts": span.last_ts,
                    "rows": span.rows,
                }[column]
                for column in SCHEMA
            )
            for span in spans
        )


def where_bar_type(bar_type: BarType) -> str:
    """Build the predicate matching the catalog rows `bar_type` addresses.

    A level left unstated matches every value of it. ``dataset`` outside
    futures is not unstated but absent: the tree carries no such level there
    and the catalog holds NULL, which is what this matches it against.
    """
    tests = [
        f"{level} = {_sql.sql_literal(value)}"
        for level, value in bar_type.to_dict(drop_none=True).items()
    ]
    asset_type = bar_type.asset_type
    if asset_type is not None and asset_type is not AssetType.FUTURES:
        tests.append("dataset IS NULL")
    return " AND ".join(tests) or "TRUE"
