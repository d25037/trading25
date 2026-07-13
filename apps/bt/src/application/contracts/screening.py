"""Application-owned screening contracts."""

from typing import Any

from pydantic import BaseModel, Field

from src.application.contracts import analytics as analytics_contracts
from src.domains.analytics.screening_results import ScreeningSortBy, SortOrder
from src.domains.strategy.runtime.screening_profile import EntryDecidability


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
    entry_decidability: EntryDecidability = Field(default="pre_open_decidable")
    markets: list[str]
    scopeLabel: str | None = None
    recentDays: int
    referenceDate: str | None = None
    sortBy: ScreeningSortBy
    order: SortOrder
    lastUpdated: str
    provenance: analytics_contracts.DataProvenance
    diagnostics: analytics_contracts.ResponseDiagnostics = Field(
        default_factory=analytics_contracts.ResponseDiagnostics
    )


class ScreeningJobRequest(BaseModel):
    """Screening ジョブ作成リクエスト"""

    entry_decidability: EntryDecidability = Field(default="pre_open_decidable")
    markets: str | None = Field(default=None)
    strategies: str | None = Field(default=None)
    recentDays: int = Field(default=10, ge=1, le=90)
    date: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    sortBy: ScreeningSortBy = Field(default="matchedDate")
    order: SortOrder = Field(default="desc")
    limit: int | None = Field(default=None, ge=1)

    model_config = {"extra": "forbid"}


class ScreeningJobPayload(BaseModel):
    """JobInfo.raw_result へ保持する payload"""

    response: dict[str, Any]
