from dataclasses import dataclass
from typing import ClassVar, Literal, Protocol

from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    MetaDataType,
    Period,
    Timeframe,
)


class Request(Protocol):
    """One request to one endpoint, with the endpoint bound to the shape.

    ``_get`` takes a Request rather than an ``(endpoint, params)`` pair so that a
    request cannot be sent to the wrong URL: the endpoint is not an argument the
    caller supplies, it is a property of the type being sent. Two live bugs
    predate this -- delisted bars were fetched from ``data_file`` and futures
    contracts from ``meta_file`` -- and both were a free-form string away from
    correct.
    """

    endpoint: ClassVar[str]

    def to_params(self) -> dict[str, str]:
        """The query string minus ``userid``, which the transport adds."""
        ...


@dataclass(frozen=True, slots=True)
class BarsRequest[AdjT: EquitiesAdjustment | ContinuousFuturesAdjustment]:
    """Historical bars for one asset type.

    Generic on the adjustment for the same reason the loader is (ADR 0002): the
    accepted values are per-asset-type, so ``BarsRequest[EquitiesAdjustment]``
    and ``BarsRequest[ContinuousFuturesAdjustment]`` are different requests and
    the base's ``AdjustmentT`` carries the distinction all the way down.
    """

    endpoint: ClassVar[str] = "data_file"

    asset_type: AssetType
    period: Period
    timeframe: Timeframe
    adjustment: AdjT
    ticker_range: str | None = None

    def to_params(self) -> dict[str, str]:
        params = {
            "type": self.asset_type.value,
            "period": self.period.value,
            "timeframe": self.timeframe.value,
            "adjustment": self.adjustment.value,
        }

        # equities-only, and the rules for it live on FirstRateEquities -- absent
        # here means the asset type does not have one, not that it was forgotten
        if self.ticker_range is not None:
            params["ticker_range"] = self.ticker_range

        return params


@dataclass(frozen=True, slots=True)
class DelistedRequest:
    """Bars for tickers that no longer trade. Stock-only, hence no ``type``."""

    endpoint: ClassVar[str] = "delisted_data_file"

    selector: DelistedArchive | DelistedUpdate
    timeframe: Timeframe
    adjustment: EquitiesAdjustment

    @property
    def kind(self) -> Literal["archive", "update"]:
        """Which half of the delisted dataset the selector names.

        The wire parameter name and the store's path segment are the same
        decision (ADR 0003's XOR, resolved), so it is made once here rather than
        by an ``isinstance`` in each of them that could drift apart.
        """
        return "archive" if isinstance(self.selector, DelistedArchive) else "update"

    def to_params(self) -> dict[str, str]:
        selector_param = "archive_number" if self.kind == "archive" else "update"

        return {
            selector_param: self.selector.value,
            "timeframe": self.timeframe.value,
            "adjustment": self.adjustment.value,
        }


@dataclass(frozen=True, slots=True)
class ContractsRequest:
    """Individual futures contracts. Futures-only, hence no ``type``."""

    endpoint: ClassVar[str] = "futures_contract"

    contract_files: ContractFiles
    timeframe: Timeframe

    def to_params(self) -> dict[str, str]:
        return {
            "contract_files": self.contract_files.value,
            "timeframe": self.timeframe.value,
        }


@dataclass(frozen=True, slots=True)
class MetafileRequest:
    """A per-asset-type metafile: splits, dividends, or the continuous audit."""

    endpoint: ClassVar[str] = "meta_file"

    asset_type: AssetType
    metadata_type: MetaDataType

    def to_params(self) -> dict[str, str]:
        return {
            "type": self.asset_type.value,
            "metafile_type": self.metadata_type.value,
        }
