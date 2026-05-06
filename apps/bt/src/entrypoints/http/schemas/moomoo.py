"""Read-only moomoo OpenD response schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class MoomooStatusResponse(BaseModel):
    """moomoo OpenD read-only integration status."""

    enabled: bool
    mode: Literal["read_only"] = "read_only"
    tradeApiEnabled: Literal[False] = False
    sdkInstalled: bool
    openDReachable: bool
    quoteContextReady: bool
    host: str
    port: int
    supportedMarkets: list[Literal["US"]] = Field(default_factory=lambda: ["US"])
    message: str | None = None


class MoomooUsStockInfo(BaseModel):
    """US stock static information from moomoo OpenD."""

    code: str
    symbol: str
    name: str | None = None
    lotSize: int | None = None
    stockType: str | None = None
    exchangeType: str | None = None
    listingDate: str | None = None
    delisting: bool | None = None


class MoomooUsStockSearchResponse(BaseModel):
    """US stock search response."""

    query: str
    items: list[MoomooUsStockInfo]
    count: int
    lastUpdated: str


class MoomooUsKlineItem(BaseModel):
    """US historical daily candlestick row from moomoo OpenD."""

    code: str
    timeKey: str
    name: str | None = None
    open: float | None = None
    close: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    turnover: float | None = None
    peRatio: float | None = None
    turnoverRate: float | None = None
    changeRate: float | None = None
    lastClose: float | None = None


class MoomooUsHistoryResponse(BaseModel):
    """US historical candlestick response."""

    symbol: str
    code: str
    timeframe: Literal["1d"]
    adjustment: Literal["qfq"]
    rows: list[MoomooUsKlineItem]
    count: int
    hasMore: bool
    lastUpdated: str


class MoomooUsSnapshotItem(BaseModel):
    """US market snapshot row from moomoo OpenD."""

    code: str
    symbol: str
    name: str | None = None
    updateTime: str | None = None
    lastPrice: float | None = None
    openPrice: float | None = None
    highPrice: float | None = None
    lowPrice: float | None = None
    prevClosePrice: float | None = None
    volume: float | None = None
    turnover: float | None = None
    turnoverRate: float | None = None
    peRatio: float | None = None
    pbRatio: float | None = None
    totalMarketValue: float | None = None
    suspension: bool | None = None


class MoomooUsSnapshotResponse(BaseModel):
    """US market snapshot response."""

    symbols: list[str]
    items: list[MoomooUsSnapshotItem]
    count: int
    lastUpdated: str
