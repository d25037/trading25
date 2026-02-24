"""
Market Screening Schemas

Backtest連動の動的YAMLスクリーニング向けレスポンススキーマ。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ScreeningSortBy = Literal[
    "bestStrategyScore",
    "matchedDate",
    "stockCode",
    "matchStrategyCount",
]
SortOrder = Literal["asc", "desc"]
ScreeningDataSource = Literal["market", "dataset"]


class MatchedStrategyItem(BaseModel):
    """同一銘柄でヒットした戦略情報"""

    strategyName: str
    matchedDate: str
    strategyScore: float | None = None


class ScreeningResultItem(BaseModel):
    """銘柄集約済みスクリーニング結果項目"""

    stockCode: str
    companyName: str
    scaleCategory: str | None = None
    sector33Name: str | None = None
    matchedDate: str
    bestStrategyName: str
    bestStrategyScore: float | None = None
    matchStrategyCount: int
    matchedStrategies: list[MatchedStrategyItem] = Field(default_factory=list)


class ScreeningSummary(BaseModel):
    """スクリーニングサマリー"""

    totalStocksScreened: int
    matchCount: int
    skippedCount: int = 0
    byStrategy: dict[str, int] = Field(default_factory=dict)
    strategiesEvaluated: list[str] = Field(default_factory=list)
    strategiesWithoutBacktestMetrics: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class MarketScreeningResponse(BaseModel):
    """マーケットスクリーニングレスポンス"""

    results: list[ScreeningResultItem]
    summary: ScreeningSummary
    markets: list[str]
    recentDays: int
    referenceDate: str | None = None
    sortBy: ScreeningSortBy
    order: SortOrder
    lastUpdated: str
