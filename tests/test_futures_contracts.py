"""Individual futures contracts are their own dataset, beside the continuous one.

A continuous series is a construction stitched from contracts; a contract is a
real instrument that expired. They share an asset type and a ticker space that
looks alike (``ES`` against ``ESH2024``), so nothing but the ``dataset`` level
keeps them apart -- and a read that mixed them would double-count the same
trades, once raw and once through whatever roll adjustment built the series.
That separation is what most of this module asserts.

The request side is asserted here too, because ``futures_contract`` takes a
different set of parameters from every other bars endpoint: no ``type``, no
``period``, no ``adjustment``. The store still files these bars under an
adjustment -- ``FuturesContractAdjustment.UNADJUSTED``, the tree naming one at
every level -- and that value must not leak back into the query string.

Note: the payload naming used below, ``{CONTRACT_TICKER}_{timeframe}.txt``,
is inferred from the vendor's docs rather than confirmed against a real
download. The store's rule -- ticker is the first underscore-delimited field --
holds for any name of that shape, so a correction would change these fixtures,
not the assertions.
"""

import pytest

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    Dataset,
    FuturesContractAdjustment,
    Period,
    Timeframe,
)
from firstrate_data.download.requests import BarsRequest, ContractBarsRequest
from firstrate_data.store.store import Store
from tests.conftest import BARS, Spool, archive

_ARCHIVE = ContractFiles.ARCHIVE
_UPDATE = ContractFiles.UPDATE
_RATIO = ContinuousFuturesAdjustment.RATIO

# the vendor's 1day futures format: { DateTime, O, H, L, C, Volume, OpenInterest }
_WITH_OPEN_INTEREST = """2024-01-02 00:00:00,100.0,101.0,99.5,100.5,1000,54321
2024-01-03 00:00:00,101.5,103.0,101.0,102.5,1500,54800
"""


def _continuous_request(timeframe: Timeframe) -> BarsRequest:
    return BarsRequest(
        BarType(AssetType.FUTURES, timeframe=timeframe, adjustment=_RATIO),
        Period.FULL,
    )


def _continuous_archive(timeframe: Timeframe, text: str = BARS) -> bytes:
    return archive({f"ES_full_{timeframe.value}_{_RATIO.value}.txt": text})


def _contract_archive(
    *tickers: str,
    timeframe: Timeframe = Timeframe.MIN_1,
    text: str = BARS,
) -> bytes:
    """Build a contract archive holding the same bars for each contract ticker."""
    return archive({f"{ticker}_{timeframe.value}.txt": text for ticker in tickers})


class TestTheRequestSendsOnlyWhatTheEndpointTakes:
    """``futures_contract`` documents exactly two parameters.

    A third parameter would either be ignored or would change the body, and
    that is not something to discover at ingest time.
    """

    @pytest.mark.parametrize("half", [_ARCHIVE, _UPDATE])
    def test_both_halves_send_exactly_the_two_documented_parameters(
        self,
        half: ContractFiles,
    ) -> None:
        request = ContractBarsRequest(
            BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_5),
            contract_files=half,
        )

        assert request.to_params() == {
            "contract_files": half.value,
            "timeframe": "5min",
        }

    @pytest.mark.parametrize("half", [_ARCHIVE, _UPDATE])
    def test_neither_half_sends_type_period_or_adjustment(
        self,
        half: ContractFiles,
    ) -> None:
        """Spells out the three parameters the equality above omits.

        These three are what every *other* bars request sends, so they are
        what would be copied in by mistake.
        """
        sent = ContractBarsRequest(
            BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1),
            contract_files=half,
        ).to_params()

        assert "type" not in sent
        assert "period" not in sent
        assert "adjustment" not in sent

    def test_the_stated_adjustment_is_never_sent(self) -> None:
        """Confirms the stated adjustment stops at the store's edge.

        The tree names an adjustment at every level and the vendor has none
        to give, so this value must not reach the request.
        """
        request = ContractBarsRequest(
            BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1),
            contract_files=_ARCHIVE,
        )

        assert request.bar_type.adjustment is FuturesContractAdjustment.UNADJUSTED
        assert request.bar_type.to_dict(drop_none=True)["adjustment"] == "UNADJUSTED"
        assert "UNADJUSTED" not in request.to_params().values()

    def test_it_goes_to_the_contract_endpoint(self) -> None:
        """Confirms the endpoint is ``futures_contract``, not ``data_file``.

        The same asset type is served by both, and the endpoint is the
        request type's to say rather than a caller's.
        """
        assert ContractBarsRequest.endpoint == "futures_contract"
        assert (
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.HOUR_1),
                contract_files=_UPDATE,
            ).endpoint
            == "futures_contract"
        )


class TestAContractLandsInItsOwnDataset:
    def test_an_archive_is_read_back_by_contract_ticker(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(
            spool(_contract_archive("ESH2024", "ESM2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )

        contracts = store.futures_contract_bars(Timeframe.MIN_1)

        assert contracts.select("ticker").distinct().order("ticker").fetchall() == [
            ("ESH2024",),
            ("ESM2024",),
        ]

    def test_a_ticker_narrows_the_read_to_one_contract(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(
            spool(_contract_archive("ESH2024", "ESM2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )

        one = store.futures_contract_bars(Timeframe.MIN_1, ticker="ESH2024")

        assert one.count("*").fetchone() == (3,)
        assert one.select("ticker").distinct().fetchall() == [("ESH2024",)]

    def test_the_dataset_level_says_contract(self, store: Store, spool: Spool) -> None:
        """The one level that tells a contract from the series built out of it."""
        store.ingest_bars(
            spool(_contract_archive("ESH2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )

        levels = (
            store.bars().select("dataset, adjustment, asset_type").distinct().fetchall()
        )

        assert levels == [("contract", "UNADJUSTED", "futures")]

    def test_both_halves_land_in_one_dataset(self, store: Store, spool: Spool) -> None:
        """Confirms archive and update land in one dataset.

        Archive and update are a fetch-side split, not two bodies of data:
        they name different contracts, and a reader wants the pair as one.
        """
        store.ingest_bars(
            spool(_contract_archive("ESH2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )
        store.ingest_bars(
            spool(_contract_archive("ESH2026")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_UPDATE,
            ),
        )

        contracts = store.futures_contract_bars(Timeframe.MIN_1)

        assert contracts.select("ticker").distinct().order("ticker").fetchall() == [
            ("ESH2024",),
            ("ESH2026",),
        ]


class TestTheTwoFuturesDatasetsDoNotMix:
    """Explains why the dataset exists.

    Both are futures, both are bars, and the same trade is in each -- once
    raw in the contract, once rolled into the series -- so a read that
    spanned both would count it twice and call it volume.
    """

    @pytest.fixture
    def both(self, store: Store, spool: Spool) -> Store:
        """One continuous series and one of the contracts behind it."""
        store.ingest_bars(
            spool(_continuous_archive(Timeframe.DAY_1)),
            _continuous_request(Timeframe.DAY_1),
        )
        store.ingest_bars(
            spool(_contract_archive("ESH2024", timeframe=Timeframe.DAY_1)),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1),
                contract_files=_ARCHIVE,
            ),
        )
        return store

    def test_the_continuous_read_does_not_see_contracts(self, both: Store) -> None:
        series = both.futures_bars(Timeframe.DAY_1, _RATIO)

        assert series.select("ticker").distinct().fetchall() == [("ES",)]

    def test_the_contract_read_does_not_see_the_continuous_series(
        self,
        both: Store,
    ) -> None:
        contracts = both.futures_contract_bars(Timeframe.DAY_1)

        assert contracts.select("ticker").distinct().fetchall() == [("ESH2024",)]

    def test_narrowing_the_contract_read_to_the_series_ticker_finds_nothing(
        self,
        both: Store,
    ) -> None:
        """``ES`` names the continuous series and prefixes its contracts.

        The dataset level, not the ticker, keeps the two apart.
        """
        assert both.futures_contract_bars(Timeframe.DAY_1, ticker="ES").fetchall() == []

    def test_a_read_across_the_whole_tree_still_holds_both(self, both: Store) -> None:
        """Separate is not hidden.

        The split lives in the dataset level, so a caller who asks for
        everything gets both bodies, each labelled.
        """
        labelled = (
            both.bars().select("dataset, ticker").distinct().order("dataset").fetchall()
        )

        assert labelled == [("continuous", "ES"), ("contract", "ESH2024")]

    def test_one_datasets_replace_leaves_the_other_alone(
        self,
        both: Store,
        spool: Spool,
    ) -> None:
        """A contract fetch replaces only its own partitions.

        Every level names a partition, so the continuous bars sit outside the
        path the fetch drops.
        """
        both.ingest_bars(
            spool(_contract_archive("ESH2024", timeframe=Timeframe.DAY_1)),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1),
                contract_files=_ARCHIVE,
            ),
        )

        assert both.futures_bars(Timeframe.DAY_1, _RATIO).count("*").fetchone() == (3,)


class TestOpenInterest:
    """The tree holds one schema for every bar, as for the continuous series.

    The seventh column is NULL where the source omits it, never absent.
    """

    def test_a_daily_contract_archive_keeps_it(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        store.ingest_bars(
            spool(
                _contract_archive(
                    "ESH2024",
                    timeframe=Timeframe.DAY_1,
                    text=_WITH_OPEN_INTEREST,
                ),
            ),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.DAY_1),
                contract_files=_ARCHIVE,
            ),
        )

        bars = store.futures_contract_bars(Timeframe.DAY_1, ticker="ESH2024")

        assert bars.order("ts").select("open_interest").fetchall() == [
            (54321,),
            (54800,),
        ]

    def test_an_intraday_contract_archive_stores_it_as_null(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """Absent from the source, so absent here -- but the column stays."""
        store.ingest_bars(
            spool(_contract_archive("ESH2024", timeframe=Timeframe.MIN_30)),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_30),
                contract_files=_ARCHIVE,
            ),
        )

        bars = store.futures_contract_bars(Timeframe.MIN_30, ticker="ESH2024")

        assert bars.select("open_interest").distinct().fetchall() == [(None,)]


class TestRefetchingAContract:
    """Every payload carries one contract's whole life.

    The archive's contracts have expired and the update's are re-served each
    day, so a fetch supersedes its partitions. Appending would double the
    update's rows every day.
    """

    def test_the_same_archive_twice_does_not_double_the_rows(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        for _ in range(2):
            store.ingest_bars(
                spool(_contract_archive("ESH2024")),
                ContractBarsRequest(
                    BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                    contract_files=_ARCHIVE,
                ),
            )

        bars = store.futures_contract_bars(Timeframe.MIN_1, ticker="ESH2024")

        assert bars.count("*").fetchone() == (3,)

    def test_the_request_says_so(self) -> None:
        assert (
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ).must_replace_existing_bars
            is True
        )
        assert (
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_UPDATE,
            ).must_replace_existing_bars
            is True
        )

    def test_a_refetch_does_not_touch_another_contract(
        self,
        store: Store,
        spool: Spool,
    ) -> None:
        """The ticker is a level of the tree, so a replace is one contract's."""
        store.ingest_bars(
            spool(_contract_archive("ESH2024", "ESM2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )
        store.ingest_bars(
            spool(_contract_archive("ESH2024")),
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ),
        )

        contracts = store.futures_contract_bars(Timeframe.MIN_1)

        assert contracts.count("*").fetchone() == (6,)


class TestTheDatasetIsTheRequestsToSay:
    def test_a_contract_request_names_the_contract_dataset(self) -> None:
        assert (
            ContractBarsRequest(
                BarType(AssetType.FUTURES, timeframe=Timeframe.MIN_1),
                contract_files=_ARCHIVE,
            ).bar_type.dataset
            is Dataset.CONTRACT
        )

    def test_a_futures_bars_request_still_names_the_continuous_one(self) -> None:
        """The pair that would collide if either moved."""
        assert (
            _continuous_request(Timeframe.MIN_1).bar_type.dataset is Dataset.CONTINUOUS
        )
