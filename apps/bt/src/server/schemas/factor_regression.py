"""
Factor Regression Schemas

Hono FactorRegressionResponse 互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class DateRange(BaseModel):
    """分析期間"""

    from_: str = Field(alias="from")
    to: str

    model_config = {"populate_by_name": True}


class IndexMatch(BaseModel):
    """指数マッチ結果"""

    indexCode: str
    indexName: str
    category: str
    rSquared: float
    beta: float


class FactorRegressionResponse(BaseModel):
    """ファクター回帰分析レスポンス"""

    stockCode: str
    companyName: str | None = None
    marketBeta: float
    marketRSquared: float
    sector17Matches: list[IndexMatch]
    sector33Matches: list[IndexMatch]
    topixStyleMatches: list[IndexMatch]
    analysisDate: str
    dataPoints: int
    dateRange: DateRange
