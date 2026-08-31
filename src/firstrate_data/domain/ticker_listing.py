import csv
from dataclasses import dataclass
from datetime import date
from typing import Self

_DELISTED_SUFFIX = "-DELISTED"


@dataclass(frozen=True, slots=True)
class TickerListing:
    # example structure of a ticker listing file: https://firstratedata.com/api_ticker_files/etf_ticker_dates_listing.txt
    # Ticker,Name,First Date,Last Date
    # AAA,Listed Funds Trust Aaf First Priority Clo Bond ETF,2020-09-09,2026-08-26
    ticker: str
    full_name: str
    start_date: date
    end_date: date
    is_delisted: bool

    @classmethod
    def from_csv(cls, csv_body: str) -> list[Self]:
        # get one TickerListing for each row of file_body; the vendor heads the
        # file with "Ticker,Name,First Date,Last Date"
        rows = csv.reader(csv_body.splitlines())
        listed_tickers = [
            cls._from_row(row)
            for row in rows
            if any(row) and row[0].strip().casefold() != "ticker"
        ]
        if not listed_tickers:
            msg = f"ticker_listing answered with no rows: {csv_body}"
            raise ValueError(msg)
        return listed_tickers

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    _ROW_FIELDS = 4

    @classmethod
    def _from_row(cls, row: list[str]) -> Self:
        if len(row) < cls._ROW_FIELDS:
            msg = (
                f"ticker_listing row {','.join(row)!r} is not "
                f"{{ticker}},{{name}},{{startDate}},{{endDate}}"
            )
            raise ValueError(
                msg,
            )

        # from both ends rather than by position: an unquoted comma in a name
        # ("Dow Jones Industrial Average, Total Return") splits into extra fields.
        # Those fields belong to the name.
        symbol, *full_name, start_date, end_date = row
        is_delisted = symbol.endswith(_DELISTED_SUFFIX)
        return cls(
            symbol.removesuffix(_DELISTED_SUFFIX),
            # strip the name whole rather than field by field: the space after
            # the comma in "S&P 500, Total Return" belongs to the name
            ",".join(full_name).strip(),
            cls._listing_date_from_str(start_date.strip(), row),
            cls._listing_date_from_str(end_date.strip(), row),
            is_delisted,
        )

    @staticmethod
    def _listing_date_from_str(field: str, row: list[str]) -> date:
        try:
            return date.fromisoformat(field)
        except ValueError as unreadable:
            msg = (
                f"ticker_listing row {','.join(row)!r} carries {field!r} "
                "where a date belongs"
            )
            raise ValueError(
                msg,
            ) from unreadable
