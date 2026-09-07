from dataclasses import dataclass, fields, replace
from typing import Self

from firstrate_data.domain.enums import Adjustment, AssetType, Timeframe


@dataclass(frozen=True, slots=True)
class BarType:
    # declared in the same order used as the store's paths.
    asset_type: AssetType | None = None
    adjustment: Adjustment | None = None
    timeframe: Timeframe | None = None
    ticker: str | None = None

    def from_ticker(self, ticker: str | None) -> Self:
        """Return a copy of this `BarType` with `ticker` swapped in."""
        return replace(self, ticker=ticker)

    def levels(self) -> dict[str, str | None]:
        """Every level of the tree, in the tree's order. None where unstated."""
        return {
            f.name: None if (value := getattr(self, f.name)) is None else str(value)
            for f in fields(self)
        }

    def stated_levels(self) -> dict[str, str]:
        """The levels this bar type names, in the tree's order."""
        return {
            name: value for name, value in self.levels().items() if value is not None
        }

    @classmethod
    def fields(cls) -> list[str]:
        return [f.name for f in fields(cls)]
