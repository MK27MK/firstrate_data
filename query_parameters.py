from enum import StrEnum, auto


class AssetType(StrEnum):
    STOCK = auto()
    ETF = auto()
    INDEX = auto()


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
    SPLITS = auto()
    DIVIDENDS = auto()


class Adjustment(StrEnum):
    SPLIT = "adj_split"
    SPLIT_DIVIDEND = "adj_splitdiv"
    UNADJUSTED = "UNADJUSTED"
