from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, Literal, Protocol

from firstrate_data.query_parameters import (
    AssetType,
    ContinuousFuturesAdjustment,
    ContractFiles,
    Dataset,
    DelistedArchive,
    DelistedUpdate,
    EquitiesAdjustment,
    MetaDataType,
    Period,
    Timeframe,
)


class Request(Protocol):
    """One request to one endpoint.

    The endpoint is a property of the request type, not an argument the
    caller supplies, so a request cannot be sent to the wrong URL.
    """

    endpoint: ClassVar[str]

    def to_params(self) -> dict[str, str]:
        """The query string minus ``userid``, which the transport adds."""
        ...


@dataclass(frozen=True, slots=True)
class BarsRequest[AdjT: EquitiesAdjustment | ContinuousFuturesAdjustment]:
    """Historical bars for one asset type.

    Generic on the adjustment because the accepted values are
    per-asset-type: ``BarsRequest[EquitiesAdjustment]`` and
    ``BarsRequest[ContinuousFuturesAdjustment]`` are different requests.
    """

    endpoint: ClassVar[str] = "data_file"

    asset_type: AssetType
    period: Period
    timeframe: Timeframe
    adjustment: AdjT
    ticker_range: str | None = None

    @property
    def dataset(self) -> Dataset:
        """Which body of data these bars join once at rest."""
        return (
            Dataset.CONTINUOUS
            if self.asset_type is AssetType.FUTURES
            else Dataset.LISTED
        )

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
    def dataset(self) -> Dataset:
        return Dataset.DELISTED

    @property
    def kind(self) -> Literal["archive", "update"]:
        """Which half of the delisted dataset the selector names."""
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

    @property
    def dataset(self) -> Dataset:
        return Dataset.CONTRACT

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


type AnyBarsRequest = BarsRequest[EquitiesAdjustment | ContinuousFuturesAdjustment]

# every request whose archive the store keeps
type StoredRequest = (
    AnyBarsRequest | DelistedRequest | ContractsRequest | MetafileRequest
)


def request_from_params(endpoint: str, params: Mapping[str, str]) -> StoredRequest:
    """Rebuild the request recorded in a snapshot record.

    Parameters
    ----------
    endpoint : str
        The endpoint the original request was sent to.
    params : Mapping[str, str]
        The query parameters the original request carried.

    Returns
    -------
    StoredRequest
        The request that produced the snapshot.

    Raises
    ------
    ValueError
        If `endpoint` names no known request type.
    """
    match endpoint:
        case BarsRequest.endpoint:
            asset_type = AssetType(params["type"])
            # the adjustment enum is per-asset-type (ADR 0002), so which one to
            # parse the string with is decided by ``type``, not guessed by trying
            adjustment: EquitiesAdjustment | ContinuousFuturesAdjustment = (
                ContinuousFuturesAdjustment(params["adjustment"])
                if asset_type is AssetType.FUTURES
                else EquitiesAdjustment(params["adjustment"])
            )
            return BarsRequest(
                asset_type,
                Period(params["period"]),
                Timeframe(params["timeframe"]),
                adjustment,
                params.get("ticker_range"),
            )
        case DelistedRequest.endpoint:
            selector: DelistedArchive | DelistedUpdate = (
                DelistedArchive(params["archive_number"])
                if "archive_number" in params
                else DelistedUpdate(params["update"])
            )
            return DelistedRequest(
                selector,
                Timeframe(params["timeframe"]),
                EquitiesAdjustment(params["adjustment"]),
            )
        case ContractsRequest.endpoint:
            return ContractsRequest(
                ContractFiles(params["contract_files"]), Timeframe(params["timeframe"])
            )
        case MetafileRequest.endpoint:
            return MetafileRequest(
                AssetType(params["type"]), MetaDataType(params["metafile_type"])
            )
        case _:
            raise ValueError(f"snapshot record names an unknown endpoint: {endpoint!r}")
