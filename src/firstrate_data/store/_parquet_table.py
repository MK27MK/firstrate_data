"""A small relation the store keeps as one parquet file.

The catalog and the ticker listing are both this shape: a handful of columns,
one row per ticker or per row the vendor served, small enough to be rewritten
whole on every change. Rewriting whole is what keeps parquet the only copy
without a reader ever seeing a half-written file -- the new table is written
beside the old one and swapped in.
"""

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from firstrate_data.store import _sql


class ParquetTable:
    """One parquet file, read as a relation and written whole."""

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        path: Path,
        schema: dict[str, str],
    ) -> None:
        self._connection = connection
        self._path = path
        self._schema = schema
        # named after the file, so two tables staging an update at once cannot
        # write into each other's rows
        self._staging_table = f"staged_{path.stem}"

    @property
    def path(self) -> Path:
        return self._path

    def exists(self) -> bool:
        return self._path.exists()

    def select(self) -> str:
        """A SELECT of the table as it stands, of the right shape when absent."""
        if not self.exists():
            return _sql.empty_select(self._schema)
        # the path is the store's own, escaped all the same
        return f"SELECT * FROM read_parquet({_sql.sql_literal(str(self._path))})"  # noqa: S608

    def relation(self) -> duckdb.DuckDBPyRelation:
        """The whole table, as a lazy relation. Empty where nothing is written."""
        return self._connection.sql(self.select())

    def stage(self, rows: Iterable[Sequence[Any]]) -> str | None:
        """Load the rows of one update into a temp table, named. None for no rows.

        A temp table rather than a VALUES list: an ingest of a full stock
        archive stages sixteen thousand rows at once.
        """
        staged = list(rows)
        if not staged:
            return None

        declared = ", ".join(f"{name} {kind}" for name, kind in self._schema.items())
        # the table name and the column declarations are the store's own, and
        # every value goes in through a bound parameter
        self._connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {self._staging_table} ({declared})",
        )
        self._connection.executemany(
            f"INSERT INTO {self._staging_table} "  # noqa: S608
            f"VALUES ({', '.join('?' * len(self._schema))})",
            staged,
        )
        return self._staging_table

    def rewrite(self, select: str) -> None:
        """Write what `select` answers with over the table, whole."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        staged = self._path.with_name(f"{self._path.name}.partial")
        self._connection.execute(f"""
            COPY ({select})
            TO {_sql.sql_literal(str(staged))} (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        staged.replace(self._path)
