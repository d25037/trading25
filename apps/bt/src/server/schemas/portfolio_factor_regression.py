"""
Portfolio Factor Regression Schemas

Hono Portfolio Factor Regression API 互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class StockWeight(BaseModel):
    code: str
    companyName: str
    weight: float
    latestPrice: float
    marketValue: float
    quantity: int


class ExcludedStock(BaseModel):
    code: str
    companyName: str
    reason: str


class IndexMatch(BaseModel):
    code: str
    name: str
    rSquared: float


class DateRange(BaseModel):
    from_: str = Field(alias="from")
    to: str

    model_config = {"populate_by_name": True}


class PortfolioFactorRegressionResponse(BaseModel):
    portfolioId: int
    portfolioName: str
    weights: list[StockWeight]
    totalValue: float
    stockCount: int
    includedStockCount: int
    marketBeta: float
    marketRSquared: float
    sector17Matches: list[IndexMatch]
    sector33Matches: list[IndexMatch]
    topixStyleMatches: list[IndexMatch]
    analysisDate: str
    dataPoints: int
    dateRange: DateRange
    excludedStocks: list[ExcludedStock]
