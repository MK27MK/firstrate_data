"""Futures daily bars carry a seventh column, and intraday ones do not.

The tree holds one schema for every bar it stores, so ``open_interest`` is NULL
where the source omits it rather than absent: a glob that enumerated a stock
file first would otherwise drop the column for the futures files behind it.
"""

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store.store import Store
from tests.conftest import BARS, Spool, archive, bars_archive, listed_request

_RATIO = ContinuousFuturesAdjustment.RATIO

# the vendor's 1day futures format: { DateTime, O, H, L, C, Volume, OpenInterest }
_WITH_OPEN_INTEREST = """2024-01-02 00:00:00,100.0,101.0,99.5,100.5,1000,54321
2024-01-03 00:00:00,101.5,103.0,101.0,102.5,1500,54800
"""


def _futures_request(timeframe: Timeframe) -> BarsRequest:
    return BarsRequest(
        BarType(AssetType.FUTURES, timeframe=timeframe, adjustment=_RATIO),
        Period.FULL,
    )


def _futures_archive(timeframe: Timeframe, text: str) -> bytes:
    return archive({f"ES_full_{timeframe.value}_{_RATIO.value}.txt": text})


class TestOpenInterest:
    def test_a_daily_archive_keeps_it(self, store: Store, spool: Spool) -> None:
        store.ingest_bars(
            spool(_futures_archive(Timeframe.DAY_1, _WITH_OPEN_INTEREST)),
            _futures_request(Timeframe.DAY_1),
        )

        bars = store.futures_bars(Timeframe.DAY_1, _RATIO, ticker="ES")

        assert bars.order("ts").select("open_interest").fetchall() == [
            (54321,),
            (54800,),
        ]

    def test_an_intraday_archive_stores_it_as_null(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Absent from the source, so absent here -- but the column stays."""
        store.ingest_bars(
            spool(_futures_archive(Timeframe.MIN_1, BARS)),
            _futures_request(Timeframe.MIN_1),
        )

        bars = store.futures_bars(Timeframe.MIN_1, _RATIO, ticker="ES")

        assert bars.select("open_interest").distinct().fetchall() == [(None,)]

    def test_the_column_survives_a_read_across_asset_types(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """A stock-first glob is what would drop it, so the stocks go in first."""
        store.ingest_bars(spool(bars_archive("AAPL")), listed_request())
        store.ingest_bars(
            spool(_futures_archive(Timeframe.DAY_1, _WITH_OPEN_INTEREST)),
            _futures_request(Timeframe.DAY_1),
        )

        everything = store.bars()

        assert "open_interest" in everything.columns
        assert everything.filter("open_interest IS NOT NULL").count("*").fetchone() == (
            2,
        )
