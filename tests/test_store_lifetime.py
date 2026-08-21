"""When the store's DuckDB connection is released.

A ``Store`` holds one connection for its lifetime -- the ingest settings are set
on it, and every relation the read methods return is lazy against it. So the
store is what decides when it goes, and a caller that opens more than one of them
in a process needs a way to say when.
"""

from pathlib import Path

import duckdb
import pytest

from firstrate_data.store.store import Store


class TestClosing:
    def test_the_connection_is_released(self, tmp_path: Path) -> None:
        store = Store(tmp_path)

        store.close()

        with pytest.raises(duckdb.ConnectionException):
            store._connection.sql("SELECT 1")

    def test_closing_twice_is_not_an_error(self, tmp_path: Path) -> None:
        """Closing twice is not an error.

        A store closed by its ``with`` block and again by a caller's cleanup
        is the ordinary case, not a mistake worth raising over.
        """
        store = Store(tmp_path)

        store.close()
        store.close()


class TestTheContextManager:
    def test_it_yields_the_store(self, tmp_path: Path) -> None:
        with Store(tmp_path) as store:
            assert store.bars().count("*").fetchone() == (0,)

    def test_leaving_the_block_closes(self, tmp_path: Path) -> None:
        with Store(tmp_path) as store:
            pass

        with pytest.raises(duckdb.ConnectionException):
            store._connection.sql("SELECT 1")

    def test_an_exception_still_closes(self, tmp_path: Path) -> None:
        """The case the ``with`` is for: a bundle that fails mid-ingest."""
        with pytest.raises(RuntimeError), Store(tmp_path) as store:  # noqa: PT012 - the two-line raise satisfies this project's EM101/TRY003 rules.
            msg = "ingest failed"
            raise RuntimeError(msg)

        with pytest.raises(duckdb.ConnectionException):
            store._connection.sql("SELECT 1")

    def test_it_does_not_swallow_the_exception(self, tmp_path: Path) -> None:
        """``__exit__`` returns None, so the failure reaches the caller."""
        with pytest.raises(RuntimeError, match="ingest failed"), Store(tmp_path):  # noqa: PT012 - the two-line raise satisfies this project's EM101/TRY003 rules.
            msg = "ingest failed"
            raise RuntimeError(msg)
