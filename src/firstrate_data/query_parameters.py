from enum import StrEnum, auto


class AssetType(StrEnum):
    STOCK = auto()
    ETF = auto()
    INDEX = auto()
    FUTURES = auto()


class Period(StrEnum):
    FULL = auto()
    MONTH = auto()
    WEEK = auto()
    DAY = auto()


class Timeframe(StrEnum):
    MIN_1 = "1min"
    MIN_5 = "5min"
    MIN_30 = "30min"
    HOUR_1 = "1hour"
    DAY_1 = "1day"


class MetaDataType(StrEnum):
    # never user-facing: reached only through the download_* method of the
    # loader whose asset type supports it
    SPLITS = auto()
    DIVIDENDS = auto()
    CONTIN_AUDIT = "contin_audit"


class Dataset(StrEnum):
    """Which body of data within an asset type a bar belongs to, once it is at rest.

    A key of the store rather than a filter over it: the plain query spans every
    dataset, so the unbiased question is the one you ask by default and
    survivorship is what you have to opt into. It sits at a fixed level of the
    tree because the depth must be uniform -- a glob mixing a stock tree that has
    this key with a futures tree that does not fails to bind. See ADR 0005.
    """

    LISTED = auto()
    DELISTED = auto()
    CONTINUOUS = auto()
    CONTRACT = auto()


class EquitiesAdjustment(StrEnum):
    SPLIT = "adj_split"
    SPLIT_AND_DIVIDEND = "adj_splitdiv"
    UNADJUSTED = "UNADJUSTED"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past when an action lands.

        An adjusted price is computed as of a date: every split or dividend
        rescales all the bars before it, so two vintages of the same series sit
        on different bases and joining them end-to-end splices in a price move
        that never happened. Unadjusted prices have no basis -- nothing restates
        them -- which is why they are the only ones an increment can extend.
        See ADR 0005.
        """
        return self is not EquitiesAdjustment.UNADJUSTED


class ContinuousFuturesAdjustment(StrEnum):
    RATIO = "contin_adj_ratio"
    ABSOLUTE = "contin_adj_absolute"
    UNADJUSTED = "contin_UNadj"

    @property
    def is_restated(self) -> bool:
        """Whether the vendor rewrites this series' past when a roll lands.

        The same property the equities adjustments have, arrived at by a
        different mechanism: a ratio- or absolute-adjusted continuous series
        exists to erase roll jumps, so each new roll rescales or shifts the
        history behind it exactly as a split does. ``contin_UNadj`` is raw trade
        data and is not restated. See ADR 0005.
        """
        return self is not ContinuousFuturesAdjustment.UNADJUSTED


class ContractFiles(StrEnum):
    ARCHIVE = auto()  # pre-2026
    UPDATE = auto()  # 2026+, refreshed daily


class DelistedArchive(StrEnum):
    """One slice of the pre-2026 delisted history, downloaded on its own.

    The docs list the accepted values as 1-5 but call them "the four historical
    archives" in the same breath. We follow the values: a fifth archive that does
    not exist fails loudly on the first request, whereas omitting one that does
    exist would silently cost us a fifth of the delisted history.
    """

    ARCHIVE_1 = "1"
    ARCHIVE_2 = "2"
    ARCHIVE_3 = "3"
    ARCHIVE_4 = "4"
    ARCHIVE_5 = "5"


class DelistedUpdate(StrEnum):
    """The 2026+ delisted data, refreshed at the end of each week (Sunday 11pm EST)."""

    WEEK = auto()
    YEAR = auto()
