"""One runnable check that a bundle skips what the store already holds."""

import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from firstrate_data.domain import BarType
from firstrate_data.domain.enums import (
    AssetType,
    EquitiesAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.bundles import StocksBundleConfig
from firstrate_data.download.client import Client
from firstrate_data.download.requests import BarsRequest, Request
from firstrate_data.store.store import TIMEZONE, Ingested, Store

SERVED = date(2026, 1, 30)
BUNDLE = StocksBundleConfig(
    asset_type=AssetType.STOCK,
    timeframes=[Timeframe.DAY_1],
    adjustment=EquitiesAdjustment.UNADJUSTED,
    ticker_range=["A", "B", "C"],
    include_splits=True,
    include_dividends=False,
    include_company_profiles=False,
    include_delisted_archives=True,
)


def _at(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 16, tzinfo=ZoneInfo(TIMEZONE))


class _StubClient(Client):
    """A client whose store holds A current, B stale, and nothing for C."""

    def __init__(self) -> None:
        self.fetched: list[str] = []
        self.last_update_calls = 0
        filed = {"A": _at(SERVED), "B": _at(date(2025, 6, 1))}
        store = SimpleNamespace(
            write=lambda _file, _request: Ingested(tickers=1, rows=1),
            last_bar=lambda _bar_type, prefix=None: filed.get(prefix),
        )
        super().__init__("user", store)  # type: ignore[arg-type]

    def last_update(self, asset_type, *, is_full_update=None) -> date:  # noqa: ANN001, ARG002
        self.last_update_calls += 1
        return SERVED

    def _fetch(self, request: Request) -> Path:
        self.fetched.append(self._spool_name(request))
        return Path(self._spool_name(request))


BAR_TYPE = BarType(AssetType.STOCK, EquitiesAdjustment.UNADJUSTED, Timeframe.DAY_1)


def _archive(directory: Path, name: str, ticker: str, start: date, days: int) -> Path:
    rows = "\n".join(
        f"{start + timedelta(days=day)} 16:00:00,1.0,2.0,0.5,1.5,100"
        for day in range(days)
    )
    path = directory / name
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{ticker}_full_1day_UNADJUSTED.txt", rows)
    return path


def check_last_bar() -> None:
    """Check the catalog answers what a range holds, which is what the skip reads."""
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        with Store(root) as store:
            assert store.last_bar(BAR_TYPE) is None, "an empty store dates nothing"
            assert store.last_bar(BAR_TYPE, "C") is None

            for name, ticker, letter, days in (
                ("c.zip", "CAT", "C", 3),
                ("a.zip", "AAPL", "A", 10),
            ):
                store.write(
                    _archive(root, name, ticker, date(2020, 1, 1), days),
                    BarsRequest(BAR_TYPE, Period.FULL, ticker_range=letter),
                )

            held = store.last_bar(BAR_TYPE, "C")
            assert held is not None
            assert held.date() == date(2020, 1, 3), held

            assert store.last_bar(BAR_TYPE, "Z") is None, (
                "an unfiled range dates nothing"
            )

            folded = store.last_bar(BAR_TYPE, "c")
            assert folded is not None, "a lowercase prefix reads the same rows"
            assert folded.date() == date(2020, 1, 3), folded

            spanning = store.last_bar(BAR_TYPE)
            assert spanning is not None, "a bar type with rows dates them"
            assert spanning.date() == date(2020, 1, 10), (
                "no prefix takes the newest bar of the whole bar type"
            )

            other = BarType(
                AssetType.ETF, EquitiesAdjustment.UNADJUSTED, Timeframe.DAY_1
            )
            assert store.last_bar(other) is None, "another bar type dates nothing"


def main() -> None:
    check_last_bar()

    client = _StubClient()
    ingested = client.download_bundle(BUNDLE, prefetch=2)

    ranges = [name for name in client.fetched if name.startswith("data_file")]
    assert len(ranges) == 2, f"only the stale and the absent range, got {ranges}"
    assert not any(name.endswith("_A") for name in ranges), "A is current, so skipped"
    assert any(name.endswith("_B") for name in ranges), "B is stale, so fetched"
    assert any(name.endswith("_C") for name in ranges), "C is absent, so fetched"

    assert client.last_update_calls == 1, "the vendor is dated once per asset type"

    assert any(name.startswith("delisted_data_file") for name in client.fetched), (
        "a delisted archive is never judged from the catalog, so it always fetches"
    )
    assert any(name.startswith("meta_file") for name in client.fetched), (
        "a metafile leaves no catalog row, so it always fetches"
    )

    assert len(ingested) == len(client.fetched), "a skipped request writes nothing"

    refetched = _StubClient()
    refetched.download_bundle(BUNDLE, prefetch=2, refresh=True)
    assert len([n for n in refetched.fetched if n.startswith("data_file")]) == 3, (
        "refresh fetches every range the bundle names"
    )
    assert refetched.last_update_calls == 0, "refresh never dates the vendor"

    print("skip-what-is-filed OK")


if __name__ == "__main__":
    main()
