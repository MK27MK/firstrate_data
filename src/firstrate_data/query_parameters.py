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
    """Which body of data within an asset type a bar belongs to, once at rest."""

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

        Every split or dividend rescales all bars before it, so two
        snapshots of a restated series sit on different bases and cannot be
        joined end-to-end. Unadjusted prices are never restated.
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


class ContractFiles(StrEnum):
    ARCHIVE = auto()  # pre-2026
    UPDATE = auto()  # 2026+, refreshed daily


class DelistedArchive(StrEnum):
    """One slice of the pre-2026 delisted history, downloaded on its own."""

    # the docs list accepted values 1-5 but say "the four historical archives"
    # in the same breath. We follow the values: a fifth archive that does not
    # exist fails loudly; omitting one that does would silently lose data.
    ARCHIVE_1 = "1"
    ARCHIVE_2 = "2"
    ARCHIVE_3 = "3"
    ARCHIVE_4 = "4"
    ARCHIVE_5 = "5"


class DelistedUpdate(StrEnum):
    """The 2026+ delisted data, refreshed at the end of each week (Sunday 11pm EST)."""

    WEEK = auto()
    YEAR = auto()
