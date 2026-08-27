from dataclasses import dataclass, field, replace
from typing import ClassVar, Literal, Protocol

from firstrate_data.domain import (
    EQUITIES,
    AssetType,
    BarType,
    ContractFiles,
    Dataset,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    FuturesContractAdjustment,
    MetafileType,
    Period,
    Timeframe,
    dataset_of,
)


class Request(Protocol):
    """One request to one endpoint.

    The endpoint is a property of the request type, not an argument the
    caller supplies, so a request can't reach the wrong address.
    """

    endpoint: ClassVar[str]

    def to_params(self) -> dict[str, str]:
        """Return the query string minus ``userid``, which the transport adds."""
        ...


class NotOfferedError(ValueError):
    """A combination of parameters the vendor doesn't serve."""


@dataclass(frozen=True, slots=True)
class BarsRequest:
    """One archive of bars, and where the store files what comes back.

    ``bar_type`` leaves the ticker unnamed: an archive carries many, and each
    payload names its own. The dataset is this endpoint's to say, so this
    request states it here, whatever the caller passed.

    ``period`` is None at the endpoints below, which serve their dataset whole.
    """

    bar_type: BarType
    period: Period | None = None
    endpoint: ClassVar[str] = "data_file"
    ticker_range: str | None = field(default=None, kw_only=True)

    # __post_init__ checks what the vendor will honour, rather than each
    # loader's plan_* method. A rule that depends on the caller routing
    # through the right method stops running the moment someone adds a
    # fourth asset type. No path can send an unconstructible request.
    def __post_init__(self) -> None:
        asset_type = self._stated(self.bar_type.asset_type, "asset type")
        self._file_under(dataset_of(asset_type))
        self._stated(self.period, "period")

        if asset_type in EQUITIES:
            self._check_equities_offer()
        elif self.ticker_range is not None:
            msg = (
                f"ticker_range is equities-only: a {asset_type.value} "
                "full archive is served whole"
            )
            raise NotOfferedError(
                msg,
            )

        # refused before the request goes out rather than at ingest: a restated
        # increment is unusable no matter how long it took to arrive, and this
        # one takes an hour
        adjustment = self.bar_type.adjustment
        if (
            adjustment is not None
            and adjustment.is_restated
            and self.period is not Period.FULL
        ):
            msg = (
                f"{adjustment.value} is restated: the vendor rewrites "
                "its history backwards when an action lands, so appending a "
                f"{self._stated(self.period, 'period').value} would splice two "
                "adjustment bases together. "
                "Re-fetch it with period=full instead."
            )
            raise NotOfferedError(
                msg,
            )

    def _file_under(self, dataset: Dataset) -> None:
        """State the dataset on ``bar_type``, over whatever it arrived with."""
        # the supported way to normalise a field of a frozen dataclass
        object.__setattr__(self, "bar_type", replace(self.bar_type, dataset=dataset))

    def _stated[Named](self, value: Named | None, what: str) -> Named:
        """``value``, or a refusal naming what this endpoint wasn't told.

        Raises
        ------
        NotOfferedError
            If ``value`` is None.

        """
        if value is None:
            msg = f"{self.endpoint} is served per {what}: name one"
            raise NotOfferedError(msg)
        return value

    def _check_equities_offer(self) -> None:
        """Check the rules only stocks and ETFs have, and normalise ticker_range."""
        self._check_unadjusted_timeframe()
        self._check_and_normalise_ticker_range()

    def _check_unadjusted_timeframe(self) -> None:
        # the delisted endpoint serves UNADJUSTED on 1min *only* -- same Enum,
        # narrower rule, so each endpoint guards its own
        if self.bar_type.adjustment is EquitiesAdjustment.UNADJUSTED and self._stated(
            self.bar_type.timeframe, "timeframe"
        ) not in (
            Timeframe.MIN_1,
            Timeframe.DAY_1,
        ):
            msg = "UNADJUSTED data is only available in the 1min and 1day timeframes"
            raise NotOfferedError(
                msg,
            )

    def _check_and_normalise_ticker_range(self) -> None:
        if self.period is Period.FULL and self.ticker_range is None:
            msg = "ticker_range (A-Z) is required when period=full"
            raise NotOfferedError(msg)
        if self.ticker_range is not None:
            if self.period is not Period.FULL:
                msg = "ticker_range can only be used when period=full"
                raise NotOfferedError(msg)
            letter = self.ticker_range.upper()
            if len(letter) != 1 or not letter.isalpha():
                msg = "ticker_range must be a single letter A-Z"
                raise NotOfferedError(msg)
            # the supported way to normalise a field of a frozen dataclass
            object.__setattr__(self, "ticker_range", letter)

    @property
    def must_replace_existing_bars(self) -> bool:
        """Whether this archive is the whole history of every ticker it names.

        A ``full`` is, so it supersedes what its bar types hold rather than
        extending them. Every shorter period starts from its own beginning,
        so it carries only the tail of a history the store already has.
        """
        return self.period is Period.FULL

    def to_params(self) -> dict[str, str]:
        asset_type = self._stated(self.bar_type.asset_type, "asset type")
        params = {
            "type": asset_type.value,
            # __post_init__ states period before anything can send this
            "period": self._stated(self.period, "period").value,
            "timeframe": self._stated(self.bar_type.timeframe, "timeframe").value,
        }

        # the index docs page lists no adjustment at all, and sending one the
        # endpoint doesn't document risks an unpredictable response body.
        # IndexAdjustment exists for the bar type path, which names one at
        # every level, and stops at the store's edge
        if asset_type is not AssetType.INDEX and self.bar_type.adjustment is not None:
            params["adjustment"] = self.bar_type.adjustment.value

        # equities-only, and the rules for it live on StockClient -- absent
        # here means the asset type doesn't have one, not that someone forgot it
        if self.ticker_range is not None:
            params["ticker_range"] = self.ticker_range

        return params


@dataclass(frozen=True, slots=True)
class DelistedBarsRequest(BarsRequest):
    """Bars for tickers that no longer trade. Stock-only, hence no ``type``.

    Raises
    ------
    NotOfferedError
        If the vendor doesn't serve this combination.

    """

    endpoint: ClassVar[str] = "delisted_data_file"

    selector: DelistedArchive | DelistedUpdate = field(kw_only=True)

    def __post_init__(self) -> None:
        self._file_under(Dataset.DELISTED)
        timeframe = self._stated(self.bar_type.timeframe, "timeframe")
        # UNADJUSTED reaches only 1min here, where the listed rule takes 1day too
        if (
            self.bar_type.adjustment is EquitiesAdjustment.UNADJUSTED
            and timeframe is not Timeframe.MIN_1
        ):
            msg = "UNADJUSTED delisted data is only available in the 1min timeframe"
            raise NotOfferedError(
                msg,
            )
        # no restated guard: a delisted fetch has no period and is always whole,
        # so there is no increment to splice onto a rewritten history

    @property
    def must_replace_existing_bars(self) -> bool:
        # every delisted payload is one ticker's entire history, whichever
        # selector asked for it, so a delisted fetch replaces its bar types.
        # That keeps `update=week` from double-counting with `update=year`,
        # since `update=week` is a strict subset of it
        return True

    @property
    def kind(self) -> Literal["archive", "update"]:
        """Which half of the delisted dataset the selector names."""
        return "archive" if isinstance(self.selector, DelistedArchive) else "update"

    def to_params(self) -> dict[str, str]:
        selector_param = "archive_number" if self.kind == "archive" else "update"

        return {
            selector_param: self.selector.value,
            "timeframe": self._stated(self.bar_type.timeframe, "timeframe").value,
            "adjustment": self._stated(self.bar_type.adjustment, "adjustment").value,
        }


@dataclass(frozen=True, slots=True)
class ContractBarsRequest(BarsRequest):
    """Bars for individual futures contracts. Futures-only, hence no ``type``.

    The continuous series is a construction. It draws from these contracts,
    each one a real instrument with its own ticker. They carry no
    ``adjustment`` and no ``period``: there is no roll inside a single
    contract to correct for, and each half of the dataset arrives whole.
    """

    endpoint: ClassVar[str] = "futures_contract"

    contract_files: ContractFiles = field(kw_only=True)

    def __post_init__(self) -> None:
        self._file_under(Dataset.CONTRACT)
        # the endpoint serves all five timeframes on both halves, so there is
        # nothing else here the vendor refuses
        object.__setattr__(
            self,
            "bar_type",
            replace(self.bar_type, adjustment=FuturesContractAdjustment.UNADJUSTED),
        )

    @property
    def must_replace_existing_bars(self) -> bool:
        # every payload is one contract's entire life, both halves alike: the
        # archive's contracts stopped trading before 2026 and the update's are
        # re-served whole each day. The two name different contracts, so
        # replacing one never drops the other's bar types
        return True

    def to_params(self) -> dict[str, str]:
        return {
            "contract_files": self.contract_files.value,
            "timeframe": self._stated(self.bar_type.timeframe, "timeframe").value,
        }


@dataclass(frozen=True, slots=True)
class LastUpdateRequest:
    """When the vendor last refreshed one asset type's data."""

    endpoint: ClassVar[str] = "last_update"

    asset_type: AssetType
    is_full_update: bool | None = None

    def to_params(self) -> dict[str, str]:
        params = {"type": self.asset_type.value}

        # documented optional, and the docs don't say which way it defaults.
        # Leaving the question unanswered beats guessing on the caller's
        # behalf
        if self.is_full_update is not None:
            params["is_full_update"] = "true" if self.is_full_update else "false"

        return params


@dataclass(frozen=True, slots=True)
class TickerListingRequest:
    """Which tickers one asset type covers, and over what dates."""

    endpoint: ClassVar[str] = "ticker_listing"

    asset_type: AssetType

    def to_params(self) -> dict[str, str]:
        # 'html=true' serves the same rows wrapped in a page. Nothing here reads
        # a page, so this fixes the flag rather than exposing it
        return {"type": self.asset_type.value, "html": "false"}


@dataclass(frozen=True, slots=True)
class MetafileRequest:
    """A per-asset-type metafile: splits, dividends, or the continuous audit."""

    endpoint: ClassVar[str] = "meta_file"

    asset_type: AssetType
    metafile_type: MetafileType

    def to_params(self) -> dict[str, str]:
        return {
            "type": self.asset_type.value,
            "metafile_type": self.metafile_type.value,
        }


# every request a sweep can fetch and then file, whichever table it lands in.
# A metafile is wider than BarsRequest: it replaces a table of its own
# rather than joining the bars tree. The same workers fetch it over the
# same wire, though, so the download side has no reason to tell them apart.
type IngestibleRequest = BarsRequest | MetafileRequest
