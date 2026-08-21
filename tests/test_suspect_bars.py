"""The damage the reject table cannot see.

A spliced line that happens to break on a comma parses cleanly: DuckDB rejects
nothing, the ingest reports nothing, and the store gains a bar whose high is
below its low. Counting rejected lines will never find one of these, so every
ingest scans what it just wrote for rows that break a bar's own arithmetic.

The rule is a heuristic, so what these tests pin is that a suspect row is
*counted and reachable*, never that it is removed: the store is the only copy of
the data, and a false positive deleted here is gone for good.
"""

import pytest

from firstrate_data.domain import MetafileType, Period, Timeframe
from firstrate_data.store.store import Store
from tests.conftest import (
    BARS,
    UNADJUSTED,
    Spool,
    archive,
    bars_archive,
    listed_request,
    payload_name,
)

# high below low. What a splice leaves when it lands between two price fields.
_HIGH_BELOW_LOW = """2024-01-02 09:30:00,10.0,11.0,9.5,10.5,100
2024-01-02 09:31:00,10.0,9.0,11.0,10.5,200
2024-01-02 09:32:00,10.5,12.0,10.0,11.5,300
"""

# close outside the range its own high and low claim
_CLOSE_ABOVE_HIGH = """2024-01-02 09:30:00,20.0,21.0,19.5,20.5,400
2024-01-02 09:31:00,20.0,21.0,19.5,99.0,500
"""

# a volume no trade can produce
_NEGATIVE_VOLUME = """2024-01-02 09:30:00,30.0,31.0,29.5,30.5,600
2024-01-02 09:31:00,30.0,31.0,29.5,30.5,-7
"""

# one bar past what BARS carries, so an increment has something to add
_LATER = "2024-01-04 09:30:00,102.5,104.0,102.0,103.5,1600\n"


@pytest.fixture
def bent() -> bytes:
    """An archive with one bar of each damaged shape, beside a healthy ticker."""
    return archive(
        {
            payload_name("AAPL"): BARS,
            payload_name("ABC"): _HIGH_BELOW_LOW,
            payload_name("ADP"): _CLOSE_ABOVE_HIGH,
            payload_name("AMZN"): _NEGATIVE_VOLUME,
        },
    )


class TestTheIngestCountsThem:
    def test_a_clean_archive_reports_none(self, store: Store, spool: Spool) -> None:
        ingested = store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert ingested.suspect == 0

    def test_each_damaged_shape_is_counted(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        """One per damaged ticker, and none for the healthy one beside them."""
        ingested = store.ingest_bars(spool(bent), listed_request())

        assert ingested.suspect == 3

    def test_the_rows_are_still_written(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        """Counted, not dropped: the rule is a heuristic and the store is the
        only copy, so a suspect row stays where a reader can judge it.

        """
        ingested = store.ingest_bars(spool(bent), listed_request())

        # three healthy bars, plus every bar of the three damaged payloads
        assert ingested.rows == 10

    def test_a_metafile_is_never_suspect(self, store: Store, spool: Spool) -> None:
        """It holds no open, high, low or close for the rule to hold against."""
        ingested = store.ingest_metafile(
            spool(archive({"AAPL.txt": "2024-01-02,2.0\n"})),
            MetafileType.SPLITS,
        )

        assert ingested.suspect == 0


class TestTheCountIsThisIngestsAlone:
    """The scan is scoped by the ingest id stamped on the files. Unscoped, every
    archive would be charged with all the damage its partitions already held.

    """

    def test_a_later_clean_ingest_reports_none(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bent), listed_request())

        clean = store.ingest_bars(
            spool(bars_archive("BAC", timeframe=Timeframe.MIN_1)),
            listed_request(timeframe=Timeframe.MIN_1, ticker_range="B"),
        )

        assert clean.suspect == 0

    def test_an_increment_is_not_charged_with_what_it_extends(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """An increment appends beside the full it extends, in the same
        partition -- so an unscoped scan would re-count the full's damage.

        """
        store.ingest_bars(
            spool(archive({payload_name("ABC"): _HIGH_BELOW_LOW})),
            listed_request(),
        )

        increment = store.ingest_bars(
            spool(bars_archive("ABC", text=_LATER, period="week")),
            listed_request(period=Period.WEEK, ticker_range=None),
        )

        assert increment.suspect == 0

    def test_the_store_still_holds_the_earlier_damage(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The increment reporting 0 must not mean the bar went anywhere."""
        store.ingest_bars(
            spool(archive({payload_name("ABC"): _HIGH_BELOW_LOW})),
            listed_request(),
        )
        store.ingest_bars(
            spool(bars_archive("ABC", text=_LATER, period="week")),
            listed_request(period=Period.WEEK, ticker_range=None),
        )

        assert store.suspect_bars().count("*").fetchone() == (1,)


class TestTheyCanBeReadBack:
    """`store.suspect_bars()` is the whole point: without it the count tells you
    something is wrong and gives you no way to look at it.

    """

    def test_it_returns_the_damaged_rows(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bent), listed_request())

        tickers = store.suspect_bars().select("ticker").fetchall()

        assert sorted(ticker for (ticker,) in tickers) == ["ABC", "ADP", "AMZN"]

    def test_it_narrows_like_the_other_reads(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bent), listed_request())

        assert store.suspect_bars(ticker="ABC").count("*").fetchone() == (1,)

    def test_a_clean_store_answers_with_no_rows(self, store: Store) -> None:
        """Not an error: 'nothing is wrong' is an answer, and a caller that has
        to catch an exception to hear it will stop asking.

        """
        assert store.suspect_bars().count("*").fetchone() == (0,)

    def test_the_healthy_bars_are_not_returned(
        self,
        store: Store,
        bent: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bent), listed_request())

        assert store.stock_bars(Timeframe.DAY_1, UNADJUSTED).count("*").fetchone() == (
            10,
        )
        assert store.suspect_bars().count("*").fetchone() == (3,)
