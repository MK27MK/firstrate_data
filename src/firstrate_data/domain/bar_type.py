from dataclasses import dataclass, fields, replace
from typing import Literal, Self, overload

from firstrate_data.domain import Adjustment, AssetType, Dataset, Timeframe


@dataclass(frozen=True, slots=True)
class BarType:
    # declared in the same order used as the store's paths.
    asset_type: AssetType | None = None
    dataset: Dataset | None = None
    adjustment: Adjustment | None = None
    timeframe: Timeframe | None = None
    ticker: str | None = None

    def from_ticker(self, ticker: str | None) -> Self:
        """Return a copy of this `BarType` with `ticker` swapped in."""
        return replace(self, ticker=ticker)

    @overload
    def to_dict(self, *, drop_none: Literal[True]) -> dict[str, str]: ...
    @overload
    def to_dict(
        self, *, drop_none: Literal[False] = False
    ) -> dict[str, str | None]: ...
    def to_dict(
        self,
        *,
        drop_none: bool = False,
    ) -> dict[str, str] | dict[str, str | None]:
        pairs = ((f.name, getattr(self, f.name)) for f in fields(self))
        return {
            name: None if value is None else str(value)
            for name, value in pairs
            if value is not None or not drop_none
        }

    @classmethod
    def fields(cls) -> list[str]:
        return [f.name for f in fields(cls)]
