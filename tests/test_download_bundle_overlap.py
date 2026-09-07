"""One runnable check that a bundle fetches ahead while the store writes."""

import time
from pathlib import Path
from types import SimpleNamespace

from firstrate_data.domain.enums import AssetType, Timeframe
from firstrate_data.download.bundles import BundleConfig
from firstrate_data.download.client import Client
from firstrate_data.download.requests import Request
from firstrate_data.store.store import Ingested

BUNDLE = BundleConfig(
    asset_type=AssetType.CRYPTO,
    timeframes=[Timeframe.DAY_1, Timeframe.HOUR_1, Timeframe.MIN_1],
)
STEP = 0.05


class _TimedClient(Client):
    """A client whose fetch and write take time and say when they ran."""

    def __init__(self) -> None:
        self.events: list[str] = []
        store = SimpleNamespace(
            write=self._timed_write,
            # an empty store, so every request the bundle names is fetched
            last_bar=lambda *_args: None,
        )
        super().__init__("user", store)  # type: ignore[arg-type]

    def _fetch(self, request: Request) -> Path:
        self.events.append(f"fetch {request.to_params()['timeframe']}")
        time.sleep(STEP)
        return Path(request.to_params()["timeframe"])

    def _timed_write(self, file: Path, _request: Request) -> Ingested:
        self.events.append(f"write {file}")
        time.sleep(STEP)
        return Ingested(tickers=1, rows=1)


def main() -> None:
    client = _TimedClient()
    ingested = client.download_bundle(BUNDLE, prefetch=2)

    assert len(ingested) == 3, "one write per planned request"
    assert [e for e in client.events if e.startswith("write")] == [
        "write 1day",
        "write 1hour",
        "write 1min",
    ], "writes stay in the bundle's order"
    assert client.events.index("fetch 1min") < client.events.index("write 1day"), (
        "the last fetch starts before the first write, so the two overlap"
    )

    print("bundle overlap OK")


if __name__ == "__main__":
    main()
