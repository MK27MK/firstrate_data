"""Futures data wrapper around the FirstRate Data API.

FirstRate returns zip archives (data/meta) or plain csv text (last_update,
ticker_listing). We validate params with Pydantic enums and proxy through.
"""

from enum import Enum
from typing import Annotated

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse, StreamingResponse

from .config import settings

router = APIRouter(prefix="/futures", tags=["futures"])

TYPE = "futures"


class Period(str, Enum):
    full = "full"
    month = "month"
    week = "week"
    day = "day"


class Timeframe(str, Enum):
    m1 = "1min"
    m5 = "5min"
    m30 = "30min"
    h1 = "1hour"
    d1 = "1day"


class Adjustment(str, Enum):
    adj_split = "adj_split"
    adj_splitdiv = "adj_splitdiv"
    unadjusted = "UNADJUSTED"


class MetafileType(str, Enum):
    splits = "splits"
    dividends = "dividends"


async def _get(path: str, params: dict[str, str]) -> httpx.Response:
    params = {**params, "type": TYPE, "userid": settings.userid}
    async with httpx.AsyncClient(base_url=settings.base_url, timeout=120) as client:
        r = await client.get(path, params=params)
    if r.is_error:
        raise HTTPException(r.status_code, f"FirstRate error: {r.text[:500]}")
    return r


@router.get("/data")
async def data_file(
    period: Period,
    timeframe: Timeframe,
    adjustment: Adjustment,
    ticker_range: Annotated[str | None, Query(pattern="^[A-Z]$")] = None,
) -> StreamingResponse:
    """Historical futures archive (zip of csv). ticker_range required for period=full."""
    if period is Period.full and ticker_range is None:
        raise HTTPException(422, "ticker_range (A-Z) is required when period=full")

    params = {
        "period": period.value,
        "timeframe": timeframe.value,
        "adjustment": adjustment.value,
    }
    if ticker_range:
        params["ticker_range"] = ticker_range

    r = await _get("/data_file", params)
    filename = f"futures_{period.value}_{timeframe.value}.zip"
    return StreamingResponse(
        iter([r.content]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/meta")
async def meta_file(metafile_type: MetafileType) -> StreamingResponse:
    """Splits/dividends archive (zip of csv)."""
    r = await _get("/meta_file", {"metafile_type": metafile_type.value})
    return StreamingResponse(
        iter([r.content]),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="futures_{metafile_type.value}.zip"'
        },
    )


@router.get("/last-update", response_class=PlainTextResponse)
async def last_update(is_full_update: bool | None = None) -> str:
    """Latest available data date, to decide whether to re-download."""
    params = {} if is_full_update is None else {"is_full_update": str(is_full_update).lower()}
    r = await _get("/last_update", params)
    return r.text


@router.get("/tickers", response_class=PlainTextResponse)
async def ticker_listing() -> str:
    """CSV listing: {ticker},{name},{startDate},{endDate} per line."""
    r = await _get("/ticker_listing", {})
    return r.text
