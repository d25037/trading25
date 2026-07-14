from __future__ import annotations

from pydantic import BaseModel


class WatchlistStockPrice(BaseModel):
    code: str
    close: float
    prevClose: float | None = None
    changePercent: float | None = None
    volume: int
    date: str


class WatchlistPricesResponse(BaseModel):
    prices: list[WatchlistStockPrice]
