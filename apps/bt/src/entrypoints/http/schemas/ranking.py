"""
Market Ranking Schemas

Hono MarketRankingResponse 互換のレスポンススキーマ。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ValueCompositeScoreMethod = Literal[
    "standard_pbr_tilt",
    "prime_size_tilt",
    "equal_weight",
]
ValueCompositeForwardEpsMode = Literal["latest", "fy"]
ValueCompositeScoreUnavailableReason = Literal[
    "not_found",
    "unsupported_market",
    "forward_eps_missing",
    "bps_missing",
    "not_rankable",
]


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


class ValueCompositeTechnicalMetrics(BaseModel):
    """Entry-as-of raw technical metrics for value-composite ranking."""

    featureDate: str | None = None
    reboundFrom252dLowPct: float | None = None
    return252dPct: float | None = None
    volatility20dPct: float | None = None
    volatility60dPct: float | None = None
    downsideVolatility60dPct: float | None = None


class ValueCompositeRankingItem(BaseModel):
    """Value-composite ranking item."""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    currentPrice: float
    volume: float
    score: float
    lowPbrScore: float
    smallMarketCapScore: float
    lowForwardPerScore: float
    pbr: float
    forwardPer: float
    marketCapBilJpy: float
    bps: float | None = None
    forwardEps: float | None = None
    latestFyDisclosedDate: str | None = None
    forwardEpsDisclosedDate: str | None = None
    forwardEpsSource: Literal["revised", "fy"] | None = None
    technicalMetrics: ValueCompositeTechnicalMetrics | None = None


class ValueCompositeRankingResponse(BaseModel):
    """Value-composite ranking response."""

    date: str
    markets: list[str]
    metricKey: Literal["standard_value_composite"] = "standard_value_composite"
    scoreMethod: ValueCompositeScoreMethod
    forwardEpsMode: ValueCompositeForwardEpsMode
    scorePolicy: str
    weights: dict[str, float]
    itemCount: int
    items: list[ValueCompositeRankingItem] = Field(default_factory=list)
    lastUpdated: str


class ValueCompositeScoreResponse(BaseModel):
    """Single-symbol value-composite score response."""

    date: str
    code: str
    companyName: str | None = None
    marketCode: str | None = None
    market: str | None = None
    metricKey: Literal["standard_value_composite"] = "standard_value_composite"
    scoreMethod: ValueCompositeScoreMethod | None = None
    forwardEpsMode: ValueCompositeForwardEpsMode
    scorePolicy: str | None = None
    weights: dict[str, float] = Field(default_factory=dict)
    universeCount: int = 0
    scoreAvailable: bool
    unsupportedReason: ValueCompositeScoreUnavailableReason | None = None
    item: ValueCompositeRankingItem | None = None
    lastUpdated: str
