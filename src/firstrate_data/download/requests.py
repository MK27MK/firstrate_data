from dataclasses import dataclass, field, replace
from typing import ClassVar, Literal, Protocol

from firstrate_data.domain import (
    AssetType,
    BarType,
    ContractFiles,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    OtherData,
    Period,
    Timeframe,
)
from firstrate_data.domain.enums import Unadjusted


class Request(Protocol):
    endpoint: ClassVar[str]

    def to_params(self) -> dict[str, str]:
        """Return the query string minus ``userid``, which the transport adds."""
        ...


class NotOfferedError(ValueError):
    """A combination of parameters the vendor doesn't serve."""


@dataclass(frozen=True, slots=True)
class BarsRequest:
    endpoint: ClassVar[str] = "data_file"
    bar_type: BarType
    period: Period | None = None
    ticker_range: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        asset_type = self._raise_if_none(self.bar_type.asset_type, "asset type")
        self._raise_if_none(self.period, "period")

        equities = (AssetType.STOCK, AssetType.ETF)
        if asset_type in equities:
            self._check_equities_offer()
        elif self.ticker_range is not None:
            msg = (
                f"ticker_range is equities-only: a {asset_type.value} "
                "full archive is served whole"
            )
            raise NotOfferedError(msg)

        # refused before the request goes out rather than at ingest: a restated
        # increment is unusable no matter how long it took to arrive, and this
        # one takes an hour
        adjustment = self.bar_type.adjustment
        if (
            adjustment is not None
            and adjustment.changes_past
            and self.period is not Period.FULL
        ):
            msg = (
                f"{adjustment.value} is restated: the vendor rewrites "
                "its history backwards when there's a split or dividend, so appending a "
                f"{self._raise_if_none(self.period, 'period').value} would splice two "
                "adjustment bases together. "
                "Re-fetch it with period=full instead."
            )
            raise NotOfferedError(msg)

    def to_params(self) -> dict[str, str]:
        asset_type = self._raise_if_none(self.bar_type.asset_type, "asset type")
        params = {
            "type": asset_type.value,
            # __post_init__ states period before anything can send this
            "period": self._raise_if_none(self.period, "period").value,
            "timeframe": self._raise_if_none(
                self.bar_type.timeframe, "timeframe"
            ).value,
        }

        # index, FX and crypto list no adjustment at all, and sending one the
        # endpoint doesn't document risks an unpredictable response body.
        # ``Unadjusted`` exists for the bar type path, which names one at
        # every level, and stops at the store's edge
        if self.bar_type.adjustment is not None and not isinstance(
            self.bar_type.adjustment, Unadjusted
        ):
            params["adjustment"] = self.bar_type.adjustment.value

        # equities-only, and the rules for it live on StockClient -- absent
        # here means the asset type doesn't have one, not that someone forgot it
        if self.ticker_range is not None:
            params["ticker_range"] = self.ticker_range

        return params

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _raise_if_none[Named](self, value: Named | None, what: str) -> Named:
        if value is None:
            msg = f"{self.endpoint} is served per {what}: name one"
            raise NotOfferedError(msg)
        return value

    def _check_equities_offer(self) -> None:
        self._check_unadjusted_timeframe()
        self._check_and_normalise_ticker_range()

    def _check_unadjusted_timeframe(self) -> None:
        # unadjusted data for ETFs and stock is only available in the 1min and daily tf
        if self.bar_type.adjustment is not EquitiesAdjustment.UNADJUSTED:
            return

        tf = self._raise_if_none(self.bar_type.timeframe, "timeframe")

        if tf not in (Timeframe.MIN_1, Timeframe.DAY_1):
            msg = "UNADJUSTED data is only available in the 1min and 1day timeframes"
            raise NotOfferedError(msg)

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
        self.raise_on_unavalaible_timeframes()

    def raise_on_unavalaible_timeframes(self) -> None:
        if self.bar_type.adjustment is not EquitiesAdjustment.UNADJUSTED:
            return

        timeframe = self._raise_if_none(self.bar_type.timeframe, "timeframe")
        if timeframe not in (Timeframe.MIN_1, Timeframe.DAY_1):
            msg = "UNADJUSTED delisted data is only available in the 1min and daily timeframe"
            raise NotOfferedError(msg)

    @property
    def kind(self) -> Literal["archive", "update"]:
        """Which half of the delisted dataset the selector names."""
        return "archive" if isinstance(self.selector, DelistedArchive) else "update"

    def to_params(self) -> dict[str, str]:
        selector_param = "archive_number" if self.kind == "archive" else "update"

        return {
            selector_param: self.selector.value,
            "timeframe": self._raise_if_none(
                self.bar_type.timeframe, "timeframe"
            ).value,
            "adjustment": self._raise_if_none(
                self.bar_type.adjustment, "adjustment"
            ).value,
        }


@dataclass(frozen=True, slots=True)
class ContractBarsRequest(BarsRequest):
    endpoint: ClassVar[str] = "futures_contract"
    contract_files: ContractFiles = field(kw_only=True)

    def __post_init__(self) -> None:
        # the endpoint serves all five timeframes on both halves, so there is
        # nothing else here the vendor refuses
        object.__setattr__(
            self,
            "bar_type",
            replace(self.bar_type, adjustment=Unadjusted.UNADJUSTED),
        )

    def to_params(self) -> dict[str, str]:
        return {
            "contract_files": self.contract_files.value,
            "timeframe": self._raise_if_none(
                self.bar_type.timeframe, "timeframe"
            ).value,
        }


@dataclass(frozen=True, slots=True)
class LastUpdateRequest:
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
    endpoint: ClassVar[str] = "ticker_listing"
    asset_type: AssetType

    def to_params(self) -> dict[str, str]:
        # 'html=true' serves the same rows wrapped in a page. Nothing here reads
        # a page, so this fixes the flag rather than exposing it
        return {"type": self.asset_type.value, "html": "false"}


@dataclass(frozen=True, slots=True)
class OtherDataRequest:
    endpoint: ClassVar[str] = "meta_file"
    asset_type: AssetType
    other_data: OtherData

    def to_params(self) -> dict[str, str]:
        return {
            "type": self.asset_type.value,
            "other_data": self.other_data.value,
        }
