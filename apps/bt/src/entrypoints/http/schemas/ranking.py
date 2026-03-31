"""
Market Ranking Schemas

Hono MarketRankingResponse 互換のレスポンススキーマ。
"""

from __future__ import annotations

from typing import Literal

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


class IndexPerformanceItem(BaseModel):
    """指数パフォーマンス項目"""

    code: str
    name: str
    category: str
    currentDate: str
    baseDate: str
    currentClose: float
    baseClose: float
    changeAmount: float
    changePercentage: float
    lookbackDays: int


class MarketRankingResponse(BaseModel):
    """マーケットランキングレスポンス"""

    date: str
    markets: list[str]
    lookbackDays: int
    periodDays: int
    rankings: Rankings
    indexPerformance: list[IndexPerformanceItem] = Field(default_factory=list)
    lastUpdated: str


Topix100RankingMetric = Literal["price_vs_sma_gap", "price_sma_20_80"]
Topix100PriceSmaWindow = Literal[20, 50, 100]


class Topix100RankingItem(BaseModel):
    """TOPIX100 SMA ranking item."""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    scaleCategory: str
    currentPrice: float
    volume: float
    priceVsSmaGap: float
    priceSma20_80: float
    volumeSma5_20: float
    priceDecile: int
    priceBucket: Literal["q1", "q10", "q456", "other"]
    volumeBucket: Literal["high", "low"] | None = None


class Topix100RankingResponse(BaseModel):
    """TOPIX100 SMA ranking response."""

    date: str
    rankingMetric: Topix100RankingMetric
    smaWindow: Topix100PriceSmaWindow
    itemCount: int
    items: list[Topix100RankingItem] = Field(default_factory=list)
    lastUpdated: str


class FundamentalRankingItem(BaseModel):
    """ファンダメンタルランキング項目"""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    currentPrice: float
    volume: float
    epsValue: float  # latest forecast EPS / latest actual EPS
    disclosedDate: str
    periodType: str
    source: Literal["revised", "fy"]


class FundamentalRankings(BaseModel):
    """比率ベースのファンダメンタルランキング"""

    ratioHigh: list[FundamentalRankingItem] = Field(default_factory=list)
    ratioLow: list[FundamentalRankingItem] = Field(default_factory=list)


class MarketFundamentalRankingResponse(BaseModel):
    """ファンダメンタルランキングレスポンス"""

    date: str
    markets: list[str]
    metricKey: str
    rankings: FundamentalRankings
    lastUpdated: str
