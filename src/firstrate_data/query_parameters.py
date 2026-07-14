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


class MetaFileType(StrEnum):
    # never user-facing: reached only through the download_* method of the
    # loader whose asset type supports it
    SPLITS = auto()
    DIVIDENDS = auto()
    CONTIN_AUDIT = "contin_audit"


class EquitiesAdjustment(StrEnum):
    SPLIT = "adj_split"
    SPLIT_AND_DIVIDEND = "adj_splitdiv"
    UNADJUSTED = "UNADJUSTED"


class ContinuousFuturesAdjustment(StrEnum):
    RATIO = "contin_adj_ratio"
    ABSOLUTE = "contin_adj_absolute"
    UNADJUSTED = "contin_UNadj"


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
