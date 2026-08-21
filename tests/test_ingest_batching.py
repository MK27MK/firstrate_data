"""One ``COPY`` cannot write the whole universe.

DuckDB buffers every open partition, so peak memory tracks the number of
distinct partition values rather than the size of the input: 10,000 ticker
partitions in a single statement exhausted 12.7 GiB. Ingest writes in batches,
and the batching must not be visible in what lands.
"""

import pytest

from firstrate_data.domain import Period, Timeframe
from firstrate_data.store import store as store_module
from firstrate_data.store.store import Store
from tests.conftest import UNADJUSTED, Spool, bars_archive, listed_request

_TICKERS = ("AA", "AB", "AC", "AD", "AE", "AF", "AG")


@pytest.fixture
def small_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two tickers per COPY, so seven of them take four statements."""
    monkeypatch.setattr(store_module, "_TICKERS_PER_COPY", 2)


@pytest.fixture
def copies(
    monkeypatch: pytest.MonkeyPatch,
    store: Store,
    small_batches: None,  # noqa: ARG001 - fixture applied for its side effect: shrinks the batch size.
) -> list[str]:
    """Every statement the store counts rows off, in order."""
    issued: list[str] = []
    executed = store._execute_counting

    def spy(statement: str) -> int:
        issued.append(statement)
        return executed(statement)

    monkeypatch.setattr(store, "_execute_counting", spy)
    return issued


@pytest.mark.usefixtures("small_batches")
class TestIngestBatches:
    def test_it_issues_one_copy_per_batch(
        self,
        store: Store,
        copies: list[str],
        spool: Spool,
    ) -> None:
        store.ingest_bars(spool(bars_archive(*_TICKERS)), listed_request())

        assert len([s for s in copies if "PARTITION_BY" in s]) == 4

    def test_every_ticker_lands_anyway(self, store: Store, spool: Spool) -> None:
        ingested = store.ingest_bars(spool(bars_archive(*_TICKERS)), listed_request())

        assert ingested.tickers == len(_TICKERS)
        assert ingested.rows == 3 * len(_TICKERS)

        stored = store.stock_bars(Timeframe.DAY_1, UNADJUSTED)
        tickers = stored.select("ticker").distinct().fetchall()
        assert sorted(t for (t,) in tickers) == sorted(_TICKERS)

    def test_the_batches_do_not_overwrite_each_other(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Every batch writes into the same root under the same date, so a
        filename pattern without the uuid would have them collide.

        """
        store.ingest_bars(spool(bars_archive(*_TICKERS)), listed_request())

        assert store.bars().count("*").fetchone() == (3 * len(_TICKERS),)

    def test_an_increment_is_filtered_batch_by_batch(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The last available ts is read per batch, so a batching bug shows up
        as bars doubled for every ticker outside the first batch.

        """
        store.ingest_bars(spool(bars_archive(*_TICKERS)), listed_request())

        store.ingest_bars(
            spool(bars_archive(*_TICKERS, period="week")),
            listed_request(period=Period.WEEK, ticker_range=None),
        )

        assert store.bars().count("*").fetchone() == (3 * len(_TICKERS),)
