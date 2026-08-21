"""What one malformed line in one vendor file does to the archive around it.

The real store holds three payloads with spliced bytes -- a truncated bar run
straight into a bar from ten days later, mid-line. Each sits in an archive of
thousands of tickers, and each one aborted the whole scan: a single bad
line cost every ticker beside it.

The rule these tests pin: a bad line loses its own row, and the ingest counts
and reports that row rather than dropping it.
"""

import duckdb
import pytest

from firstrate_data.domain import Timeframe
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

# the real corruption, reproduced: a stamp cut off mid-field with a later bar
# spliced onto it. The splice leaves the bars either side intact, and they
# must survive.
_SPLICED = """2024-01-02 09:30:00,10.0,11.0,9.5,10.5,100
2024-01-02 09:2024-,0.424,0.424,0.424,0.424,2600
2024-01-02 09:32:00,10.5,12.0,10.0,11.5,200
"""

# the other shape the store holds: a payload that opens with an empty line
_LEADING_BLANK = """
2024-01-02 09:30:00,20.0,21.0,19.5,20.5,300
2024-01-02 09:31:00,20.5,22.0,20.0,21.5,400
"""

# one line, two unreadable columns. DuckDB files a reject row per column that
# failed, so this line lands in the reject table twice. Counting reject rows
# charges the ingest with two lost bars where it lost one.
_TWICE_BROKEN = """2024-01-02 09:30:00,30.0,31.0,29.5,30.5,500
2024-01-02 09:31:00,30.5,2024-,32.0,31.5,2024-01-12
2024-01-02 09:32:00,31.5,33.0,31.0,32.5,700
"""


def _quarantined(store: Store) -> list[dict[str, str]]:
    """Every quarantined line in the store, read back off disk.

    On its own connection, not the store's. The tests check that the lines
    survive the ingest that dropped them, and reading them back through the
    connection that wrote them proves nothing.
    """
    files = list(store.quarantine.glob("*.parquet"))
    if not files:
        return []
    with duckdb.connect() as connection:
        paths = [str(f) for f in files]
        rows = connection.sql(
            # test fixture paths from a temp directory the test itself
            # created, not external input
            f"SELECT payload, line, csv_line, errors FROM read_parquet({paths})",  # noqa: S608
        )
        columns = [description[0] for description in rows.description]
        return [dict(zip(columns, row, strict=True)) for row in rows.fetchall()]


@pytest.fixture
def corrupted() -> bytes:
    """Build an archive with two damaged payloads beside two healthy ones."""
    return archive(
        {
            payload_name("AAPL"): BARS,
            payload_name("AMZN"): BARS,
            payload_name("ABC"): _SPLICED,
            payload_name("ADP"): _LEADING_BLANK,
        },
    )


class TestABadLineDoesNotCostTheArchive:
    def test_the_archive_still_ingests(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        ingested = store.ingest_bars(spool(corrupted), listed_request())

        # three bars each from the healthy pair, two from each damaged file
        assert ingested.tickers == 4
        assert ingested.rows == 10

    def test_the_healthy_tickers_beside_it_are_all_there(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """AAPL and AMZN share the archive with the damaged files."""
        store.ingest_bars(spool(corrupted), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED)
        tickers = bars.select("ticker").distinct().fetchall()

        assert sorted(t for (t,) in tickers) == ["AAPL", "ABC", "ADP", "AMZN"]

    def test_the_good_rows_of_the_damaged_file_survive(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """The ingest loses the spliced line and keeps the bars either side."""
        store.ingest_bars(spool(corrupted), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="ABC")

        assert bars.count("*").fetchone() == (2,)

    def test_a_leading_blank_line_costs_nothing_at_all(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(corrupted), listed_request())

        bars = store.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="ADP")

        assert bars.count("*").fetchone() == (2,)


class TestTheLostRowsAreReported:
    """The ingest counts and reports the rows it rejects.

    Dropping them without a word would be worse than failing: the store would
    look complete while missing rows nobody ever hears about.
    """

    def test_the_rejected_row_is_counted_against_its_ingest(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        ingested = store.ingest_bars(spool(corrupted), listed_request())

        assert ingested.rejected == 1

    def test_a_line_broken_twice_is_one_lost_bar_not_two(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The count is lines, not reject rows.

        DuckDB files one row per column that failed, and a splice routinely
        ruins more than one column.
        """
        ingested = store.ingest_bars(
            spool(archive({payload_name("ABC"): _TWICE_BROKEN})),
            listed_request(),
        )

        assert ingested.rejected == 1
        assert ingested.rows == 2

    def test_a_clean_archive_reports_nothing(self, store: Store, spool: Spool) -> None:
        ingested = store.ingest_bars(
            spool(bars_archive("AAPL", "AMZN")),
            listed_request(),
        )

        assert ingested.rejected == 0

    def test_rejects_do_not_leak_from_one_ingest_into_the_next(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """Each ingest drains the reject table.

        The reject table belongs to the connection and accumulates. Untended,
        every later ingest inherits this one's count.
        """
        store.ingest_bars(spool(corrupted), listed_request())

        clean = store.ingest_bars(
            spool(bars_archive("BAC", timeframe=Timeframe.MIN_1)),
            listed_request(timeframe=Timeframe.MIN_1, ticker_range="B"),
        )

        assert clean.rejected == 0


class TestTheLostLinesAreKept:
    """The store quarantines the lines it loses rather than merely counting them.

    The vendor serves the same damaged bytes on a re-fetch, so a line the store
    only counts is a line nobody can ever look at.
    """

    def test_the_line_is_written_to_the_quarantine(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(corrupted), listed_request())

        quarantined = _quarantined(store)

        assert len(quarantined) == 1
        assert quarantined[0]["csv_line"].startswith("2024-01-02 09:2024-")

    def test_the_line_says_which_payload_it_came_from(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """The quarantined line names the payload it came from.

        The archive takes the staging directory with it, so the payload's
        name is the only thing left that says which ticker lost the bar.
        """
        store.ingest_bars(spool(corrupted), listed_request())

        assert _quarantined(store)[0]["payload"] == payload_name("ABC")

    def test_a_line_broken_twice_is_one_quarantined_row(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(
            spool(archive({payload_name("ABC"): _TWICE_BROKEN})),
            listed_request(),
        )

        assert len(_quarantined(store)) == 1

    def test_a_clean_ingest_writes_no_file(self, store: Store, spool: Spool) -> None:
        """No file at all, not merely an empty one.

        A sweep of sixty clean archives must not leave sixty empty files
        behind saying nothing happened.
        """
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert list(store.quarantine.glob("*.parquet")) == []

    def test_a_clean_ingest_after_a_damaged_one_writes_no_file(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """A clean ingest sees the prior reject table, empty rather than absent.

        The reject tables outlive the ingest that filled them, so the second
        ingest here finds them present and empty -- not absent.
        """
        store.ingest_bars(spool(corrupted), listed_request())
        store.ingest_bars(
            spool(bars_archive("BAC", timeframe=Timeframe.MIN_1)),
            listed_request(timeframe=Timeframe.MIN_1, ticker_range="B"),
        )

        assert len(list(store.quarantine.glob("*.parquet"))) == 1

    def test_two_damaged_ingests_do_not_overwrite_each_other(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(corrupted), listed_request())
        store.ingest_bars(
            spool(archive({payload_name("ABC"): _TWICE_BROKEN})),
            listed_request(),
        )

        assert len(list(store.quarantine.glob("*.parquet"))) == 2
        assert len(_quarantined(store)) == 2


class TestTheyCanBeReadBackWithoutSql:
    """Quarantined lines come back through the store's own query, not raw SQL.

    A count nobody can act on is barely better than silence, and a caller
    forced to write `read_parquet` over a path the store chose has no API.
    """

    def test_the_store_hands_them_back(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(corrupted), listed_request())

        (line,) = store.quarantined().fetchall()

        assert line[0] == payload_name("ABC")

    def test_it_spans_every_ingest_not_just_the_last(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """The quarantine query spans every ingest, not just the last.

        Nothing ever leaves the quarantine, so this is the whole history of
        what the store has lost -- the point of keeping it.
        """
        store.ingest_bars(spool(corrupted), listed_request())
        store.ingest_bars(
            spool(archive({payload_name("ABC"): _TWICE_BROKEN})),
            listed_request(),
        )

        assert store.quarantined().count("*").fetchone() == (2,)

    def test_a_clean_store_answers_with_no_rows(self, store: Store) -> None:
        """Not an error and not a crash on a missing directory."""
        assert store.quarantined().count("*").fetchone() == (0,)

    def test_the_empty_answer_has_the_same_columns(self, store: Store) -> None:
        """Otherwise a caller's projection breaks on exactly the happy path."""
        assert store.quarantined().columns == ["payload", "line", "csv_line", "errors"]

    def test_the_ingest_reports_which_payloads_lost_lines(
        self,
        store: Store,
        corrupted: bytes,
        spool: Spool,
    ) -> None:
        """What the end-of-sweep summary prints, without re-reading the file."""
        ingested = store.ingest_bars(spool(corrupted), listed_request())

        assert ingested.damaged == ((payload_name("ABC"), 1),)
        assert ingested.quarantine is not None
        assert ingested.quarantine.exists()

    def test_a_clean_ingest_names_no_quarantine_file(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        ingested = store.ingest_bars(spool(bars_archive("AAPL")), listed_request())

        assert ingested.quarantine is None
        assert ingested.damaged == ()
