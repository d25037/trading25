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


Topix100RankingMetric = Literal["price_vs_sma_gap", "price_sma_20_80"]
Topix100PriceSmaWindow = Literal[20, 50, 100]
Topix100StudyMode = Literal["intraday", "swing_5d"]
Topix100ScoreTarget = Literal[
    "next_session_open_close",
    "next_session_open_to_close_5d",
    "next_session_open_to_open_5d",
]


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
    priceBucket: Literal["q1", "q10", "q234", "other"]
    longScore5d: float | None = None
    shortScore1d: float | None = None
    longScore5dRank: int | None = None
    shortScore1dRank: int | None = None
    intradayScore: float | None = None
    intradayLongRank: int | None = None
    intradayShortRank: int | None = None
    nextSessionDate: str | None = None
    nextSessionIntradayReturn: float | None = None
    swingEntryDate: str | None = None
    swingExitDate: str | None = None
    openToOpen5dReturn: float | None = None


class Topix100RankingResponse(BaseModel):
    """TOPIX100 SMA ranking response."""

    date: str
    studyMode: Topix100StudyMode = "intraday"
    rankingMetric: Topix100RankingMetric
    smaWindow: Topix100PriceSmaWindow
    shortWindowStreaks: int
    longWindowStreaks: int
    longScoreHorizonDays: int = 5
    shortScoreHorizonDays: int = 1
    scoreTarget: Topix100ScoreTarget = "next_session_open_close"
    intradayScoreTarget: Topix100ScoreTarget = "next_session_open_close"
    scoreModelType: Literal["walkforward_frozen_split", "daily_refit"] = "daily_refit"
    scoreTrainWindowDays: int | None = None
    scoreTestWindowDays: int | None = None
    scoreStepDays: int | None = None
    scoreSplitTrainStart: str | None = None
    scoreSplitTrainEnd: str | None = None
    scoreSplitTestStart: str | None = None
    scoreSplitTestEnd: str | None = None
    scoreSplitPartialTail: bool = False
    scoreSourceRunId: str | None = None
    primaryBenchmark: Literal["topix"] | None = None
    secondaryBenchmark: Literal["topix100_universe"] | None = None
    primaryBenchmarkReturn: float | None = None
    secondaryBenchmarkReturn: float | None = None
    benchmarkEntryDate: str | None = None
    benchmarkExitDate: str | None = None
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
