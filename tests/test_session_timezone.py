"""What a bare timestamp literal means in a query against this store.

``ts`` is a TIMESTAMPTZ, so it holds an instant and is never ambiguous. What
*is* ambiguous is the other side of a comparison: DuckDB reads a naive literal
in the session's timezone, and that defaults to the machine's locale. On a
laptop set to Europe/Rome, ``ts >= '2024-01-02 09:30:00'`` asks for 03:30 in New
York -- the query is wrong, silently, and differently wrong on a colleague's
machine.
"""

from datetime import UTC, datetime
from pathlib import Path

from firstrate_data.domain import Timeframe
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED


class TestSessionTimezone:
    def test_is_pinned_to_the_market_zone(self, tmp_path: Path) -> None:
        store = Store(tmp_path)

        setting = store._connection.sql("SELECT current_setting('TimeZone')").fetchone()

        assert setting is not None
        assert setting[0] == "America/New_York"

    def test_a_naive_literal_means_market_time(self, stocked: Store) -> None:
        """09:31 in a filter is 09:31 on the exchange, whatever the laptop says."""
        bars = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")

        after = bars.filter("ts >= '2024-01-02 09:31:00'")

        # the 09:30 bar is excluded, the 09:31 and the next day's remain
        assert after.count("*").fetchone() == (2,)

    def test_a_naive_literal_is_not_read_as_utc(self, stocked: Store) -> None:
        """The guard against 'fix it by pinning UTC', which is wrong differently.

        09:30 ET is 14:30 UTC. If the session were UTC, this filter would keep
        every bar instead of excluding the first.
        """
        bars = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")

        after = bars.filter("ts >= '2024-01-02 14:30:00'")

        assert after.count("*").fetchone() == (1,)

    def test_the_stored_instant_is_unchanged_by_any_of_this(
        self,
        stocked: Store,
    ) -> None:
        """Rendering is a view; the instant is the data."""
        bars = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")

        first = bars.order("ts").select("ts").fetchone()
        assert first is not None
        assert first[0] == datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
