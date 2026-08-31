"""One runnable check of what an ingest is allowed to file over."""

import zipfile
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from firstrate_data.domain import (
    AssetType,
    BarType,
    DelistedUpdate,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.requests import BarsRequest, DelistedBarsRequest
from firstrate_data.store.store import ConflictingBarsError, Store

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


def _tree(directory: Path) -> set[str]:
    """Every parquet file the bars tree holds, relative to it."""
    bars = directory / "firstrate_data" / "bars"
    return {str(path.relative_to(bars)) for path in bars.rglob("*.parquet")}


def _span(store: Store, ticker: str) -> tuple[date, date, int]:
    ((first, last, rows),) = (
        store.catalog()
        .filter(f"ticker = '{ticker}'")
        .project("first_ts, last_ts, rows")
        .fetchall()
    )
    return first.date(), last.date(), rows


def main() -> None:
    with TemporaryDirectory() as tmp:
        spool = Path(tmp)
        with Store(spool) as store:
            listed = BarsRequest(BAR_TYPE, Period.FULL, ticker_range="A")

            store.write(_archive(spool, "AAPL", date(2020, 1, 1), 5), listed)
            assert _span(store, "AAPL") == (date(2020, 1, 1), date(2020, 1, 5), 5)

            # same start, further end: the same history downloaded again
            store.write(_archive(spool, "AAPL", date(2020, 1, 1), 8), listed)
            assert _span(store, "AAPL") == (date(2020, 1, 1), date(2020, 1, 8), 8), (
                "an update must file over, not add to, the bars it restates"
            )

            # no bars past what is held
            try:
                store.write(_archive(spool, "AAPL", date(2020, 1, 1), 8), listed)
            except ConflictingBarsError:
                pass
            else:
                raise AssertionError("re-filing an unchanged span must raise")

            # a different start is a different history under one name
            filed = _tree(spool)
            try:
                store.write(_archive(spool, "AAPL", date(2020, 1, 3), 20), listed)
            except ConflictingBarsError:
                pass
            else:
                raise AssertionError("a shifted start must raise")

            assert _span(store, "AAPL") == (date(2020, 1, 1), date(2020, 1, 8), 8), (
                "a refused ingest must leave the catalog as it was"
            )
            assert _tree(spool) == filed, (
                "no bar of a refused ingest may reach the tree"
            )

            # the delisted suffix is part of the ticker the store files under
            store.write(
                _archive(spool, "UTRS-DELISTED", date(2010, 6, 1), 3),
                DelistedBarsRequest(BAR_TYPE, selector=DelistedUpdate.YEAR),
            )
            assert _span(store, "UTRS-DELISTED") == (
                date(2010, 6, 1),
                date(2010, 6, 3),
                3,
            )
            assert store.tickers_list(BAR_TYPE) == ["AAPL", "UTRS-DELISTED"]

    print("ok")


if __name__ == "__main__":
    main()
