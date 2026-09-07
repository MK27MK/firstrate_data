"""One runnable check of what a read returns off the tree."""

import zipfile
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from firstrate_data.domain import (
    AssetType,
    BarType,
    EquitiesAdjustment,
    Period,
    Timeframe,
    TradingHours,
)
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store._sql import STORED_BAR_SCHEMA
from firstrate_data.store.store import Store

BAR_TYPE = BarType(AssetType.STOCK, EquitiesAdjustment.SPLIT, Timeframe.DAY_1)


def _archive(directory: Path, ticker: str, start: date, days: int) -> Path:
    rows = "\n".join(
        f"{start + timedelta(days=day)} 16:00:00,1.0,2.0,0.5,1.5,100"
        for day in range(days)
    )
    path = directory / f"{ticker}_{start}_{days}.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{ticker}_full_1day_adj_split.txt", rows)
    return path


def _read(store: Store, **selectors: object) -> list[tuple]:
    return store.bars(
        asset_type=AssetType.STOCK,
        timeframe=Timeframe.DAY_1,
        adjustment=EquitiesAdjustment.SPLIT,
        **selectors,
    ).fetchall()


def main() -> None:
    with TemporaryDirectory() as tmp:
        spool = Path(tmp)
        with Store(spool) as store:
            empty = store.bars()
            assert empty.fetchall() == [], "an empty store reads back no bars"
            assert empty.columns == list(STORED_BAR_SCHEMA), (
                "an empty store still reads back the store's columns"
            )

            listed = BarsRequest(BAR_TYPE, Period.FULL, ticker_range="A")
            store.write(_archive(spool, "AAPL", date(2020, 1, 1), 5), listed)
            store.write(_archive(spool, "MSFT", date(2020, 1, 1), 5), listed)

            assert store.bars().columns == list(STORED_BAR_SCHEMA)
            assert len(_read(store)) == 10, "no selector spans every ticker"
            assert len(_read(store, ticker="AAPL")) == 5
            assert len(_read(store, ticker=["AAPL", "MSFT"])) == 10
            assert _read(store, ticker="NVDA") == [], (
                "a ticker the store does not hold reads back nothing, not an error"
            )
            assert len(_read(store, start=date(2020, 1, 3))) == 6, (
                "both ends of a date range are kept"
            )
            assert len(_read(store, ticker="AAPL", end=date(2020, 1, 1))) == 1

            # every bar is stamped 16:00, which the session excludes on the right
            assert _read(store, hours=TradingHours.REGULAR) == []
            try:
                store.bars(hours=TradingHours.REGULAR)
            except ValueError:
                pass
            else:
                raise AssertionError("a session needs an asset type to define it")

    print("ok")


if __name__ == "__main__":
    main()
