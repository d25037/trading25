"""
Market Ranking Schemas

Hono MarketRankingResponse 互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RankingItem(BaseModel):
    """ランキング項目"""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    currentPrice: float
    volume: float
    tradingValue: float | None = None
    tradingValueAverage: float | None = None
    previousPrice: float | None = None
    basePrice: float | None = None
    changeAmount: float | None = None
    changePercentage: float | None = None
    lookbackDays: int | None = None


class Rankings(BaseModel):
    """5種類のランキング"""

    tradingValue: list[RankingItem] = Field(default_factory=list)
    gainers: list[RankingItem] = Field(default_factory=list)
    losers: list[RankingItem] = Field(default_factory=list)
    periodHigh: list[RankingItem] = Field(default_factory=list)
    periodLow: list[RankingItem] = Field(default_factory=list)


class MarketRankingResponse(BaseModel):
    """マーケットランキングレスポンス"""

    date: str
    markets: list[str]
    lookbackDays: int
    periodDays: int
    rankings: Rankings
    lastUpdated: str
