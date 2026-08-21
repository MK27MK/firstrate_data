"""Narrowing a read, and rolling one up.

A caller wants one instrument over one date range, in the session it trades and
at the timeframe it thinks in. All four of those are the store's to answer:
written outside it they become a second spelling of the tree's own rules, free
to disagree with them.
"""

from datetime import date

import duckdb
import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    Dataset,
    DelistedUpdate,
    Timeframe,
    TradingHours,
)
from firstrate_data.store import _sql
from firstrate_data.store.store import Store
from tests.conftest import (
    UNADJUSTED,
    Spool,
    bars_archive,
    delisted_request,
    listed_request,
)

# a session's worth of minutes around its edges: two before the open, the open
# and close themselves, and one after the close
MINUTES = """2024-01-02 09:28:00,99.0,99.2,98.8,99.1,10
2024-01-02 09:29:00,99.1,99.3,98.9,99.2,20
2024-01-02 09:30:00,100.0,101.0,99.5,100.5,1000
2024-01-02 12:00:00,100.5,103.0,97.0,101.5,2000
2024-01-02 15:59:00,101.5,102.0,101.0,101.8,3000
2024-01-02 16:00:00,101.8,101.9,101.7,101.85,40
2024-01-03 09:30:00,102.0,104.0,101.0,103.0,5000
2024-01-03 15:59:00,103.0,103.5,102.5,103.4,6000
"""


@pytest.fixture
def minutes(store: Store, spool: Spool) -> Store:
    """Hold one listed ticker's minutes, extended hours included."""
    store.ingest_bars(
        spool(bars_archive("AAPL", text=MINUTES, timeframe=Timeframe.MIN_1)),
        listed_request(timeframe=Timeframe.MIN_1),
    )
    return store


def stamps(relation: duckdb.DuckDBPyRelation) -> list[str]:
    """List the relation's timestamps, in order, as the local clock reads them."""
    frame = relation.df().sort_values("ts")
    return [
        ts.strftime("%Y-%m-%d %H:%M") for ts in frame["ts"].dt.tz_convert("US/Eastern")
    ]


class TestTheDateRange:
    def test_keeps_both_end_days_whole(self, minutes: Store) -> None:
        kept = minutes.stock_bars(
            Timeframe.MIN_1,
            UNADJUSTED,
            ticker="AAPL",
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
        )
        # 16:00 is on the end day and stays: the bound names a day, not an instant
        assert stamps(kept)[-1] == "2024-01-02 16:00"

    def test_drops_the_days_outside_it(self, minutes: Store) -> None:
        kept = minutes.stock_bars(
            Timeframe.MIN_1,
            UNADJUSTED,
            ticker="AAPL",
            start=date(2024, 1, 3),
        )
        assert {stamp[:10] for stamp in stamps(kept)} == {"2024-01-03"}

    def test_is_the_whole_series_when_neither_end_is_named(
        self,
        minutes: Store,
    ) -> None:
        every = minutes.stock_bars(Timeframe.MIN_1, UNADJUSTED, ticker="AAPL")
        assert len(stamps(every)) == 8


class TestTheTradingSession:
    def test_regular_hours_drop_the_prints_outside_them(self, minutes: Store) -> None:
        session = minutes.stock_bars(
            Timeframe.MIN_1,
            UNADJUSTED,
            ticker="AAPL",
            hours=TradingHours.REGULAR,
        )
        assert stamps(session) == [
            "2024-01-02 09:30",
            "2024-01-02 12:00",
            "2024-01-02 15:59",
            "2024-01-03 09:30",
            "2024-01-03 15:59",
        ]

    def test_all_hours_is_the_default(self, minutes: Store) -> None:
        every = minutes.stock_bars(Timeframe.MIN_1, UNADJUSTED, ticker="AAPL")
        assert "2024-01-02 09:28" in stamps(every)

    def test_an_asset_type_that_never_closes_has_no_session(self) -> None:
        # crypto and futures trade around the clock, so no read of them offers
        # ``hours`` at all -- the rule they would need does not exist
        with pytest.raises(ValueError, match="around the clock"):
            _sql.regular_trading_hours_where(AssetType.CRYPTO)


class TestTheResample:
    def test_makes_one_daily_bar_from_a_session_of_minutes(
        self,
        minutes: Store,
    ) -> None:
        session = minutes.stock_bars(
            Timeframe.MIN_1,
            UNADJUSTED,
            ticker="AAPL",
            start=date(2024, 1, 2),
            end=date(2024, 1, 2),
            hours=TradingHours.REGULAR,
        )
        daily = minutes.subsample_bars(session, Timeframe.DAY_1).df()

        assert len(daily) == 1
        bar = daily.iloc[0]
        assert (bar["open"], bar["high"], bar["low"], bar["close"]) == (
            100.0,
            103.0,
            97.0,
            101.8,
        )
        assert bar["volume"] == 6000
        # the bucket opens at midnight local, not 19:00 the evening before
        assert bar["ts"].tz_convert("US/Eastern").strftime("%H:%M") == "00:00"

    def test_restates_the_timeframe_it_produced(self, minutes: Store) -> None:
        daily = minutes.subsample_bars(
            minutes.stock_bars(Timeframe.MIN_1, UNADJUSTED, ticker="AAPL"),
            Timeframe.DAY_1,
        ).df()
        assert set(daily["timeframe"]) == {Timeframe.DAY_1.value}

    def test_keeps_the_tickers_apart(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(
            spool(
                bars_archive("AAPL", "AMZN", text=MINUTES, timeframe=Timeframe.MIN_1),
            ),
            listed_request(timeframe=Timeframe.MIN_1),
        )
        daily = store.subsample_bars(
            store.stock_bars(Timeframe.MIN_1, UNADJUSTED),
            Timeframe.DAY_1,
        ).df()
        # two tickers, two sessions each -- not one bar per session across both
        assert len(daily) == 4
        assert sorted(set(daily["ticker"])) == ["AAPL", "AMZN"]

    def test_refuses_a_timeframe_that_is_not_coarser(self, minutes: Store) -> None:
        held = minutes.stock_bars(Timeframe.MIN_1, UNADJUSTED, ticker="AAPL")
        with pytest.raises(ValueError, match="only makes bars coarser"):
            minutes.subsample_bars(held, Timeframe.MIN_1)


class TestTheTickerList:
    def test_answers_the_selection_and_nothing_else(self, stocked: Store) -> None:
        listed = stocked.tickers_list(
            BarType(
                AssetType.STOCK,
                timeframe=Timeframe.DAY_1,
                dataset=Dataset.LISTED,
                adjustment=UNADJUSTED,
            ),
        )
        assert listed == ["AAPL", "AMZN"]

    def test_spans_the_selectors_it_is_not_given(self, stocked: Store) -> None:
        every = stocked.tickers_list(BarType(AssetType.STOCK))
        assert every == ["AAGR", "AAPL", "AMZN", "BAC", "CRY"]

    def test_is_empty_where_the_tree_holds_nothing(self, store: Store) -> None:
        assert store.tickers_list(BarType()) == []

    def test_reads_the_delisted_tickers_without_their_suffix(
        self,
        stocked: Store,
    ) -> None:
        delisted = stocked.tickers_list(BarType(dataset=Dataset.DELISTED))
        assert delisted == ["AAGR", "CRY"]


def test_a_delisted_daily_bar_is_built_from_its_minutes(
    store: Store,
    spool: Spool,
) -> None:
    """A delisted ticker's daily bar comes from a resample of its minutes.

    The vendor serves unadjusted delisted bars at 1min only, so a daily bar
    for a dead ticker is a resample or nothing.
    """
    store.ingest_bars(
        spool(bars_archive("CRY-DELISTED", text=MINUTES, timeframe=Timeframe.MIN_1)),
        delisted_request(DelistedUpdate.YEAR),
    )
    with pytest.raises(LookupError, match="1 minute"):
        store.stock_bars(Timeframe.DAY_1, UNADJUSTED, dataset=Dataset.DELISTED)

    daily = store.subsample_bars(
        store.stock_bars(
            Timeframe.MIN_1,
            UNADJUSTED,
            dataset=Dataset.DELISTED,
            ticker="CRY",
            hours=TradingHours.REGULAR,
        ),
        Timeframe.DAY_1,
    ).df()
    assert len(daily) == 2


class TestReadingManyTickersAtOnce:
    """A list of tickers builds one glob per ticker.

    The ticker level of the tree is enumerated before any filter can prune it,
    so a caller that names its tickers must not pay for a walk of every other
    ticker in the store.
    """

    def test_answers_every_ticker_it_names(self, stocked: Store) -> None:
        held = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker=["AAPL", "AMZN"])
        assert sorted(set(held.df()["ticker"])) == ["AAPL", "AMZN"]

    def test_leaves_out_the_tickers_it_does_not_name(self, stocked: Store) -> None:
        held = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker=["AAPL"])
        assert set(held.df()["ticker"]) == {"AAPL"}

    def test_reads_the_same_bars_as_the_bare_ticker(self, stocked: Store) -> None:
        one = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker="AAPL").df()
        listed = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker=["AAPL"]).df()
        assert len(one) == len(listed)

    def test_skips_a_ticker_the_store_never_held(self, stocked: Store) -> None:
        # an absent name is not an error: the read answers for the tickers the
        # store has, rather than failing whole over one it was never sent
        held = stocked.stock_bars(
            Timeframe.DAY_1,
            UNADJUSTED,
            ticker=["AAPL", "NOSUCH"],
        )
        assert set(held.df()["ticker"]) == {"AAPL"}

    def test_is_empty_where_it_names_no_ticker_the_store_holds(
        self,
        stocked: Store,
    ) -> None:
        held = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker=["NOSUCH"])
        assert held.df().empty

    def test_an_empty_list_asks_for_no_ticker_rather_than_all_of_them(
        self,
        stocked: Store,
    ) -> None:
        # the list is the whole of what was asked for, and it asked for
        # nothing. Spanning the tree here would answer the narrowest possible
        # question with the widest possible read
        held = stocked.stock_bars(Timeframe.DAY_1, UNADJUSTED, ticker=[])
        assert held.df().empty
