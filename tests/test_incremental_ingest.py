"""An increment always re-serves bars the store already holds.

A ``week`` fetched on a Wednesday starts from Monday, not from where the
store left off, so every incremental fetch overlaps the one before it. What
keeps the overlap from double-counting is the *last available ts*, read from
the parquet footers of the tickers the archive names.
"""

from datetime import UTC, datetime

from firstrate_data.domain import Period, Timeframe
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED, Spool, bars_archive, listed_request

# Wednesday's `week`: Tuesday's last bar again, then two the store has not seen
_WEEK = """2024-01-03 09:30:00,101.5,103.0,101.0,102.5,1500
2024-01-04 09:30:00,102.5,104.0,102.0,103.5,1600
2024-01-05 09:30:00,103.5,105.0,103.0,104.5,1700
"""

_WEEKLY = listed_request(period=Period.WEEK, ticker_range=None)


def _week_archive(*tickers: str) -> bytes:
    return bars_archive(*tickers, text=_WEEK, period="week")


class TestAnIncrementKeepsOnlyWhatIsNew:
    def test_the_overlapping_bar_is_not_stored_twice(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        ingested = store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        assert ingested.rows == 2
        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")
        assert bars.count("*").fetchone() == (5,)

    def test_fetching_the_same_week_twice_changes_nothing(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        again = store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        assert again.rows == 0
        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")
        assert bars.count("*").fetchone() == (5,)

    def test_the_kept_bars_are_the_new_ones(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")
        stamps = bars.order("ts").select("ts").fetchall()

        # naive by design: compared against ts.replace(tzinfo=None) above
        assert [ts.replace(tzinfo=None) for (ts,) in stamps] == [
            datetime(2024, 1, 2, 9, 30),  # noqa: DTZ001
            datetime(2024, 1, 2, 9, 31),  # noqa: DTZ001
            datetime(2024, 1, 3, 9, 30),  # noqa: DTZ001
            datetime(2024, 1, 4, 9, 30),  # noqa: DTZ001
            datetime(2024, 1, 5, 9, 30),  # noqa: DTZ001
        ]

    def test_a_ticker_the_store_has_never_seen_lands_whole(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The filter is per ticker: an unknown one has no last available ts,
        and dropping its bars for want of one would lose a new listing.

        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        ingested = store.ingest_bars(spool(_week_archive("AAPL", "NVDA")), _WEEKLY)

        assert ingested.rows == 5
        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="NVDA")
        assert bars.count("*").fetchone() == (3,)

    def test_an_increment_into_an_empty_store_keeps_everything(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        ingested = store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        assert ingested.rows == 3

    def test_the_last_available_ts_is_read_per_partition(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Another adjustment's bars are a different series, not this one's past."""
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        elsewhere = store.ingest_bars(
            spool(bars_archive("AAPL", timeframe=Timeframe.MIN_1, period="week")),
            listed_request(
                period=Period.WEEK,
                timeframe=Timeframe.MIN_1,
                ticker_range=None,
            ),
        )

        assert elsewhere.rows == 3


class TestTheFooterFallback:
    """``stats_max`` is NULL whenever the writer omitted statistics, and reading
    that as "no data" would re-ingest bars the store already holds. Those files
    fall back to a real scan -- a path no file DuckDB writes will take, so it is
    exercised directly rather than left to rot.

    """

    def test_the_scan_agrees_with_the_footers(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(spool(bars_archive("AAPL", "AMZN")), listed_request())
        written = [str(path) for path in store._bars_directory.rglob("[0-9]*.parquet")]

        scanned = store._scanned_last_ts(written)

        assert sorted(scanned) == [
            ("AAPL", datetime(2024, 1, 3, 14, 30, tzinfo=UTC)),
            ("AMZN", datetime(2024, 1, 3, 14, 30, tzinfo=UTC)),
        ]

    def test_no_files_is_not_a_query(self, store: Store) -> None:
        """An empty list would be an empty glob, which DuckDB raises on."""
        assert store._scanned_last_ts([]) == []


class TestAFullReplaces:
    def test_a_second_full_does_not_double_the_bars(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """A full carries the whole history of every ticker it names, and there
        is no record of which archive contributed which row -- so it drops the
        partitions it is about to write rather than merging into them.

        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")
        assert bars.count("*").fetchone() == (3,)

    def test_a_full_takes_the_increments_laid_on_top_of_it(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The re-fetched full is authoritative: the increment's rows are inside
        it, or the vendor no longer serves them.

        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_bars(spool(_week_archive("AAPL")), _WEEKLY)

        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL")
        assert bars.count("*").fetchone() == (3,)

    def test_it_leaves_the_tickers_it_does_not_name_alone(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bars_archive("AAPL", "AMZN")), listed_request())

        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AMZN").count(
            "*",
        ).fetchone() == (3,)
