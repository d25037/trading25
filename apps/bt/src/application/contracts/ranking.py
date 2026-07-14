"""
Market Ranking Schemas

Market Ranking API のレスポンススキーマ。
"""

from typing import Literal

from pydantic import BaseModel, Field

ValueCompositeScoreMethod = Literal[
    "standard_pbr_tilt",
    "prime_size_tilt",
    "prime_size75_forward_per25",
    "equal_weight",
]
ValueCompositeProfileId = Literal[
    "standard_breakout_120d20",
    "prime_size75_forward_per25",
]
ValueCompositeForwardEpsMode = Literal["latest", "fy"]
ValueCompositeScoreUnavailableReason = Literal[
    "not_found",
    "unsupported_market",
    "forward_eps_missing",
    "bps_missing",
    "not_rankable",
]
LiquidityRegime = Literal[
    "neutral_rerating",
    "crowded_rerating",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
]
RankingRiskFlag = Literal["overheat", "stale_rally_fade"]
RankingTechnicalFlag = Literal["atr20_acceleration", "momentum_20_60_top20"]
RankingRegimeStateFilter = LiquidityRegime
RankingRiskStateFilter = RankingRiskFlag
RankingTechnicalStateFilter = RankingTechnicalFlag
RankingFundamentalStateFilter = Literal[
    "deep_value",
    "value_confirmed",
    "undervalued",
    "expensive_or",
    "overvalued",
    "very_overvalued",
    "no_earnings",
]
SectorStrengthBucket = Literal["sector_strong", "sector_neutral", "sector_weak"]
SectorStrengthFamily = Literal["balanced_sector_strength", "long_hybrid_leadership"]


def normalize_sector_strength_family(value: str) -> SectorStrengthFamily:
    if value == "balanced_sector_strength":
        return "balanced_sector_strength"
    if value == "long_hybrid_leadership":
        return "long_hybrid_leadership"
    raise ValueError(f"Unsupported sectorStrengthFamily: {value}")


class RankingItem(BaseModel):
    """ランキング項目"""

    rank: int
    code: str
    companyName: str
    marketCode: str
    sector33Name: str
    sectorStrengthScore: float | None = None
    sectorStrengthBucket: SectorStrengthBucket | None = None
    currentPrice: float
    volume: float
    tradingValue: float | None = None
    tradingValueAverage: float | None = None
    previousPrice: float | None = None
    basePrice: float | None = None
    changeAmount: float | None = None
    changePercentage: float | None = None
    lookbackDays: int | None = None
    sma5AboveCount5d: int | None = None
    sma5BelowStreak: int | None = None
    per: float | None = None
    perPercentile: float | None = None
    forwardPer: float | None = None
    forwardPerPercentile: float | None = None
    pOp: float | None = None
    forwardPOp: float | None = None
    forwardPOpPercentile: float | None = None
    forecastOperatingProfitGrowthRatio: float | None = Field(
        None,
        description="Forward operating profit divided by actual operating profit, derived from p_op / forward_p_op",
    )
    forecastOperatingProfitGrowthPct: float | None = Field(
        None,
        description="Forward operating profit growth percentage, derived from forecastOperatingProfitGrowthRatio",
    )
    psr: float | None = None
    psrPercentile: float | None = None
    forwardPsr: float | None = None
    forwardPsrPercentile: float | None = None
    forwardEpsDisclosedDate: str | None = None
    forwardEpsSource: Literal["revised", "fy"] | None = None
    pbr: float | None = None
    pbrPercentile: float | None = None
    valueCompositeScore: float | None = Field(
        None,
        description=(
            "Equal-weight low forward PER percentile and low PBR percentile score; "
            "higher means stronger low forward PER and low PBR value confirmation."
        ),
    )
    overvaluationCompositeScore: float | None = Field(
        None,
        description=(
            "Equal-weight high forward PER percentile and high PBR percentile score; "
            "higher means stronger high forward PER and high PBR overvaluation confirmation."
        ),
    )
    marketCap: float | None = None
    liquidityResidualZ: float | None = None
    liquidityRegime: LiquidityRegime | None = None
    adv60ToFreeFloatPct: float | None = None
    riskFlags: list[RankingRiskFlag] = Field(default_factory=list)
    technicalFlags: list[RankingTechnicalFlag] = Field(default_factory=list)


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
    sectorStrengthScore: float | None = None
    sectorStrengthBucket: SectorStrengthBucket | None = None
    sector20dTopixExcessPct: float | None = None
    sector60dTopixExcessPct: float | None = None
    sectorBreadth20dPct: float | None = None
    sectorStockCount: int | None = None


class MarketRankingResponse(BaseModel):
    """マーケットランキングレスポンス"""

    date: str
    markets: list[str]
    lookbackDays: int
    periodDays: int
    sectorStrengthFamily: SectorStrengthFamily = "balanced_sector_strength"
    rankings: Rankings
    indexPerformance: list[IndexPerformanceItem] = Field(default_factory=list)
    lastUpdated: str


class MarketRankingSymbolResponse(BaseModel):
    """単一銘柄の最新マーケットランキングレスポンス"""

    date: str | None
    item: RankingItem | None
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
    breakoutFeatureDate: str | None = None
    reboundFrom252dLowPct: float | None = None
    return252dPct: float | None = None
    volatility20dPct: float | None = None
    volatility60dPct: float | None = None
    downsideVolatility60dPct: float | None = None
    avgTradingValue60dMilJpy: float | None = None
    avgTradingValue60dSourceSessions: int | None = None
    newHigh20d: bool | None = None
    daysSinceNewHigh20d: int | None = None
    closeToPriorHigh20dPct: float | None = None
    newHigh120d: bool | None = None
    daysSinceNewHigh120d: int | None = None
    closeToPriorHigh120dPct: float | None = None


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
    scoreBeforeBoost: float | None = None
    breakoutBoost: float | None = None
    liquidityEligible: bool | None = None
    avgTradingValue60dMilJpy: float | None = None
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
    profileId: ValueCompositeProfileId | None = None
    profileLabel: str | None = None
    scoreMethod: ValueCompositeScoreMethod
    forwardEpsMode: ValueCompositeForwardEpsMode
    rebalanceMonths: int | None = None
    breakoutWindow: int | None = None
    breakoutLookbackSessions: int | None = None
    breakoutScoreBoost: float | None = None
    applyLiquidityFilter: bool = True
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
