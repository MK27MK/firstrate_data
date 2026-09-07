"""One runnable check of what happens when a filed archive is fetched again."""

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
)
from firstrate_data.download.requests import BarsRequest
from firstrate_data.store.store import ConflictingBarsError, Store

REQUEST = BarsRequest(
    BarType(AssetType.STOCK, EquitiesAdjustment.UNADJUSTED, Timeframe.DAY_1),
    Period.FULL,
    ticker_range="C",
)


def _archive(directory: Path, name: str, start: date, days: int) -> Path:
    rows = "\n".join(
        f"{start + timedelta(days=day)} 16:00:00,1.0,2.0,0.5,1.5,100"
        for day in range(days)
    )
    path = directory / name
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("CAT_full_1day_UNADJUSTED.txt", rows)
    return path


def main() -> None:
    start = date(2020, 1, 1)
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        with Store(root) as store:
            store.write(_archive(root, "first.zip", start, 3), REQUEST)

            refiled = store.write(_archive(root, "same.zip", start, 3), REQUEST)
            assert refiled.rows == 3, "the same archive files again"
            assert store.bars(ticker="CAT").fetchall() == store.bars().fetchall()
            assert len(store.bars(ticker="CAT").fetchall()) == 3, (
                "re-filing replaces the ticker's bars rather than doubling them"
            )

            longer = store.write(_archive(root, "longer.zip", start, 5), REQUEST)
            assert longer.rows == 5, "an archive carrying more bars files over"

            try:
                store.write(_archive(root, "other.zip", date(2021, 1, 1), 5), REQUEST)
            except ConflictingBarsError:
                pass
            else:
                msg = "a different first bar is a different history, and is refused"
                raise AssertionError(msg)

    print("refile OK")


if __name__ == "__main__":
    main()
