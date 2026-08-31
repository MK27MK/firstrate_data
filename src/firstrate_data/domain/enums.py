from __future__ import annotations

from enum import StrEnum, auto
from typing import Self


class AssetType(StrEnum):
    STOCK = auto()
    ETF = auto()
    INDEX = auto()
    FUTURES = auto()
    CRYPTO = auto()
    FX = auto()
    OPTIONS = auto()

    def timezone(self) -> str:
        if self is AssetType.CRYPTO:
            return "UTC"
        return "America/New_York"



# adjustments ----------------------------------------------------------


class Adjustment(StrEnum):
    @property
    def changes_past(self) -> bool:
        """Return `True` for all adjustments which are not `UNADJUSTED`.

        A series holding these kind of adjustments need to be fully replaced
        when a new adjustment comes.
        """
        return self.name != "UNADJUSTED"


class Unadjusted(Adjustment):
    UNADJUSTED = "UNADJUSTED"


class EquitiesAdjustment(Adjustment):
    SPLIT = "adj_split"
    SPLIT_AND_DIVIDEND = "adj_splitdiv"
    UNADJUSTED = "UNADJUSTED"


class ContinuousFuturesAdjustment(Adjustment):
    RATIO = "contin_adj_ratio"
    ABSOLUTE = "contin_adj_absolute"
    UNADJUSTED = "contin_UNadj"


class Timeframe(StrEnum):
    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_30 = "30min"
    HOUR_1 = "1hour"
    DAY_1 = "1day"

    def is_higher_than(self, other_timeframe: Self) -> bool:
        order = list(Timeframe)
        return order.index(self) > order.index(other_timeframe)


# ----------------------------------------------------------------------


class TradingHours(StrEnum):
    ALL = auto()
    REGULAR = auto()


class Period(StrEnum):
    FULL = auto()
    MONTH = auto()
    WEEK = auto()
    DAY = auto()


class OtherData(StrEnum):
    SPLITS = auto()
    DIVIDENDS = auto()
    COMPANY_PROFILES = auto()
    CONTRACT_DATES = "contin_audit"


class ContractFiles(StrEnum):
    """Which half of the individual-contract dataset a request names.

    The vendor splits it in two: the archive freezes everything up to 2025,
    and the update carries the contracts trading from 2026, refreshed daily.
    They name different contracts, so neither contains the other.
    """

    ARCHIVE = auto()
    UPDATE = auto()


# ----------------------------------------------------------------------
# delisted data
# ----------------------------------------------------------------------


class DelistedArchive(StrEnum):
    """One slice of the pre-2026 delisted history, downloaded on its own."""

    # the docs list accepted values 1-5 but say "the four historical archives"
    # in the same breath. The values win: a fifth archive that doesn't exist
    # fails on the request, while omitting one that does loses data unseen.
    ARCHIVE_1 = "1"
    ARCHIVE_2 = "2"
    ARCHIVE_3 = "3"
    ARCHIVE_4 = "4"
    ARCHIVE_5 = "5"


class DelistedUpdate(StrEnum):
    """The 2026+ delisted data, refreshed each Sunday at 11 PM, Eastern."""

    WEEK = auto()
    YEAR = auto()
