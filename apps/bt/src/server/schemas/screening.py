"""
Market Screening Schemas

Hono MarketScreeningResponse 互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RangeBreakDetails(BaseModel):
    """レンジブレイク詳細"""

    breakDate: str
    currentHigh: float
    maxHighInLookback: float
    breakPercentage: float
    volumeRatio: float
    avgVolume20Days: float
    avgVolume100Days: float


class ScreeningDetails(BaseModel):
    """スクリーニング詳細"""

    rangeBreak: RangeBreakDetails | None = None


class FutureReturnPoint(BaseModel):
    """将来リターンの1データポイント"""

    date: str
    price: float
    changePercent: float


class FutureReturns(BaseModel):
    """将来リターン（履歴モード用）"""

    day5: FutureReturnPoint | None = None
    day20: FutureReturnPoint | None = None
    day60: FutureReturnPoint | None = None


class ScreeningResultItem(BaseModel):
    """スクリーニング結果項目"""

    stockCode: str
    companyName: str
    scaleCategory: str | None = None
    sector33Name: str | None = None
    screeningType: str  # "rangeBreakFast" | "rangeBreakSlow"
    matchedDate: str
    details: ScreeningDetails
    futureReturns: FutureReturns | None = None


class ScreeningSummary(BaseModel):
    """スクリーニングサマリー"""

    totalStocksScreened: int
    matchCount: int
    skippedCount: int = 0
    byScreeningType: dict[str, int] = Field(default_factory=dict)


class MarketScreeningResponse(BaseModel):
    """マーケットスクリーニングレスポンス"""

    results: list[ScreeningResultItem]
    summary: ScreeningSummary
    markets: list[str]
    recentDays: int
    referenceDate: str | None = None
    lastUpdated: str
