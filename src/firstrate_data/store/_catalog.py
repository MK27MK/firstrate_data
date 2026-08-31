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

import duckdb

from firstrate_data.domain import BarType
from firstrate_data.store import _sql
from firstrate_data.store._parquet_table import ParquetTable

# What the catalog keeps. The key columns are BarType's own fields, in
# BarType's order, so a row addresses exactly what a path does.
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
    symbol reports one span covering the gap between them.
    """

    ticker: str
    first_ts: datetime
    last_ts: datetime
    rows: int


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

    def spans(self, bar_type: BarType, tickers: Iterable[str]) -> dict[str, TickerSpan]:
        """The span the catalog holds for each of `tickers` under `bar_type`."""
        named = _sql.sql_list(tickers)
        # `where_bar_type` escapes each level, `sql_list` escapes each ticker,
        # and `select` names this store's own catalog file
        held = self._connection.sql(
            f"SELECT ticker, first_ts, last_ts, rows FROM ({self._table.select()}) "  # noqa: S608
            f"WHERE {where_bar_type(bar_type)} AND ticker IN {named}",
        ).fetchall()
        return {
            ticker: TickerSpan(ticker, first_ts, last_ts, int(rows))
            for ticker, first_ts, last_ts, rows in held
        }

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

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _stage(self, bar_type: BarType, spans: Iterable[TickerSpan]) -> str | None:
        """Load the rows of one update into the staging table, or None for no rows."""
        levels = bar_type.from_ticker(None).levels()
        return self._table.stage(
            (
                *(levels[level] for level in BarType.fields() if level != "ticker"),
                span.ticker,
                span.first_ts,
                span.last_ts,
                span.rows,
            )
            for span in spans
        )


def where_bar_type(bar_type: BarType) -> str:
    """Build the predicate matching the catalog rows `bar_type` addresses.

    A level left unstated matches every value of it.
    """
    tests = [
        f"{level} = {_sql.sql_literal(value)}"
        for level, value in bar_type.stated_levels().items()
    ]
    return " AND ".join(tests) or "TRUE"
