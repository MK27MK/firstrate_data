r"""The paths the store hands DuckDB, on a machine whose separator is not ``/``.

Two of them are read back rather than only written: the glob is a pattern
language where ``\\`` escapes, and the ticker is parsed out of the path DuckDB
reports. A Windows separator changes what both mean.
"""

from dataclasses import replace

from firstrate_data.domain import BarType, Dataset, Timeframe
from firstrate_data.store import _sql
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED, stock_bar_type

_POSIX_FILE = "/store/bars/adjustment=UNADJUSTED/ticker=AAPL/2024-01-02_3f2a_0.parquet"
_WINDOWS_FILE = (
    r"D:\store\bars\adjustment=UNADJUSTED\ticker=AAPL\2024-01-02_3f2a_0.parquet"
)


class TestTheTickerComesBackOutOfThePath:
    def _extracted(self, store: Store, path: str) -> str:
        expression = _sql.hive_ticker_expression(_sql.sql_literal(path))
        read = store._connection.sql(f"SELECT {expression}").fetchone()

        assert read is not None
        return read[0]

    def test_a_posix_path_names_its_ticker(self, store: Store) -> None:
        assert self._extracted(store, _POSIX_FILE) == "AAPL"

    def test_a_windows_path_names_its_ticker(self, store: Store) -> None:
        """A level bounded by ``/`` alone swallows the rest of the path, and the
        ticker it invents keys nothing the store holds -- so an increment reads
        every ticker as unseen and appends the bars it already has.

        """
        assert self._extracted(store, _WINDOWS_FILE) == "AAPL"


class TestTheGlobStaysAPattern:
    def test_a_bar_path_carries_no_backslash(self, store: Store) -> None:
        r"""``\[0-9]`` is the literal ``[0-9]``, so a native Windows separator
        ahead of the file pattern matches nothing the store ever wrote.

        """
        pattern = store._get_bar_path(stock_bar_type(Timeframe.DAY_1, UNADJUSTED))

        assert "\\" not in pattern

    def test_a_partition_directory_carries_no_backslash(self, store: Store) -> None:
        directory = store._get_bar_path(
            replace(stock_bar_type(), dataset=Dataset.LISTED).from_ticker("AAPL"),
            files_regex=None,
        )

        assert "\\" not in directory

    def test_a_wildcarded_path_carries_no_backslash(self, store: Store) -> None:
        assert "\\" not in store._get_bar_path(BarType())
