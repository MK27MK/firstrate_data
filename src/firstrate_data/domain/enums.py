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


EQUITIES = (AssetType.STOCK, AssetType.ETF)


class Dataset(StrEnum):
    LISTED = auto()
    DELISTED = auto()
    CONTINUOUS = auto()
    CONTRACT = auto()

    @classmethod
    def default_from_asset_type(cls, asset_type: AssetType) -> "Dataset":
        return cls.CONTINUOUS if asset_type is AssetType.FUTURES else cls.LISTED


# adjustments ----------------------------------------------------------


class EquitiesAdjustment(StrEnum):
    SPLIT = "adj_split"
    SPLIT_AND_DIVIDEND = "adj_splitdiv"
    UNADJUSTED = "UNADJUSTED"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past when an action lands.

        Every split or dividend rescales all bars before it, so two
        fetches of a restated series sit on different bases that no join
        reconciles. Unadjusted prices are never restated.
        """
        return self is not EquitiesAdjustment.UNADJUSTED


class ContinuousFuturesAdjustment(StrEnum):
    RATIO = "contin_adj_ratio"
    ABSOLUTE = "contin_adj_absolute"
    UNADJUSTED = "contin_UNadj"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past when a roll lands.

        A ratio- or absolute-adjusted continuous series rescales or shifts
        the history behind each new roll, exactly as a split does.
        ``contin_UNadj`` is raw trade data and is never restated.
        """
        return self is not ContinuousFuturesAdjustment.UNADJUSTED


class FuturesContractAdjustment(StrEnum):
    """The one basis the vendor serves an individual contract on.

    ``futures_contract`` takes no ``adjustment``: a real contract has no
    roll to correct for, the roll being a property of the continuous series
    built from many of them. A store-side value, like ``IndexAdjustment``,
    and never sent.
    """

    UNADJUSTED = "UNADJUSTED"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past. Never, for a contract.

        A contract's prices are the trades that happened in it. Nothing later
        rebases them, so a fetch can extend a contract rather than replace it.
        """
        return False


class IndexAdjustment(StrEnum):
    """The one basis the vendor serves an index on.

    ``data_file`` takes no ``adjustment`` for ``type=index``, so this is
    a store-side value: the bar type names an ``adjustment`` at every
    level, and an index's is none.
    """

    UNADJUSTED = "UNADJUSTED"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past. Never, for an index.

        An index level is a published number, not a price the vendor rebases:
        there is no corporate action or roll behind it to restate it.
        """
        return False


type Adjustment = (
    EquitiesAdjustment
    | ContinuousFuturesAdjustment
    | FuturesContractAdjustment
    | IndexAdjustment
)


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


class MetafileType(StrEnum):
    SPLITS = auto()
    DIVIDENDS = auto()
    CONTIN_AUDIT = "contin_audit"


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
