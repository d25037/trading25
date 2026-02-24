"""
Portfolio Performance Schemas

Hono Portfolio Performance API 互換のレスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class PerformanceSummary(BaseModel):
    totalCost: float
    currentValue: float
    totalPnL: float
    returnRate: float


class HoldingDetail(BaseModel):
    code: str
    companyName: str
    quantity: int
    purchasePrice: float
    currentPrice: float
    cost: float
    marketValue: float
    pnl: float
    returnRate: float
    weight: float
    purchaseDate: str
    account: str | None = None


class TimeSeriesPoint(BaseModel):
    date: str
    dailyReturn: float
    cumulativeReturn: float


class BenchmarkResult(BaseModel):
    code: str
    name: str
    beta: float
    alpha: float
    correlation: float
    rSquared: float
    benchmarkReturn: float
    relativeReturn: float


class BenchmarkTimeSeriesPoint(BaseModel):
    date: str
    portfolioReturn: float
    benchmarkReturn: float


class DateRange(BaseModel):
    from_: str = Field(alias="from")
    to: str

    model_config = {"populate_by_name": True}


class PortfolioPerformanceResponse(BaseModel):
    portfolioId: int
    portfolioName: str
    portfolioDescription: str | None = None
    summary: PerformanceSummary
    holdings: list[HoldingDetail]
    timeSeries: list[TimeSeriesPoint]
    benchmark: BenchmarkResult | None = None
    benchmarkTimeSeries: list[BenchmarkTimeSeriesPoint] | None = None
    analysisDate: str
    dateRange: DateRange | None = None
    dataPoints: int
    warnings: list[str]


class WatchlistStockPrice(BaseModel):
    code: str
    close: float
    prevClose: float | None = None
    changePercent: float | None = None
    volume: int
    date: str


class WatchlistPricesResponse(BaseModel):
    prices: list[WatchlistStockPrice]
