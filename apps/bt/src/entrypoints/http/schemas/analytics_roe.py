"""
ROE Analytics Response Schemas

Hono hono-openapi-baseline.json と完全互換の ROE 分析レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ROEMetadata(BaseModel):
    """ROE 計算メタデータ"""

    code: str
    periodType: str = Field(description="Period type (FY, Q1, Q2, Q3)")
    periodEnd: str
    isConsolidated: bool
    accountingStandard: str | None = None
    isAnnualized: bool | None = None


class ROEResultItem(BaseModel):
    """ROE 計算結果"""

    roe: float = Field(description="Return on Equity percentage")
    netProfit: float = Field(description="Net profit in millions of yen")
    equity: float = Field(description="Shareholders' equity in millions of yen")
    metadata: ROEMetadata


class ROESummary(BaseModel):
    """ROE 集計統計"""

    averageROE: float
    maxROE: float
    minROE: float
    totalCompanies: int


class ROEResponse(BaseModel):
    """ROE 分析レスポンス"""

    results: list[ROEResultItem]
    summary: ROESummary
    lastUpdated: str
