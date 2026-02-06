"""
Fundamentals API Schemas

Pydantic models for fundamentals calculation request/response.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class FundamentalsComputeRequest(BaseModel):
    """Request body for fundamentals computation."""

    symbol: str = Field(..., description="Stock code (4-5 digits)", json_schema_extra={"example": "7203"})
    from_date: str | None = Field(
        None, description="Start date (YYYY-MM-DD)", json_schema_extra={"example": "2020-01-01"}
    )
    to_date: str | None = Field(
        None, description="End date (YYYY-MM-DD)", json_schema_extra={"example": "2025-12-31"}
    )
    period_type: Literal["all", "FY", "1Q", "2Q", "3Q"] = Field(
        default="all", description="Filter by period type (FY, 1Q, 2Q, 3Q)"
    )
    prefer_consolidated: bool = Field(
        default=True, description="Prefer consolidated statements"
    )


class FundamentalDataPoint(BaseModel):
    """Single fundamental data point."""

    # Period information
    date: str = Field(..., description="Period end date (YYYY-MM-DD)")
    disclosedDate: str = Field(..., description="Disclosure date (YYYY-MM-DD)")
    periodType: str = Field(..., description="Period type (FY, 1Q, 2Q, 3Q)")
    isConsolidated: bool = Field(..., description="Whether data is consolidated")
    accountingStandard: str | None = Field(
        None, description="Accounting standard (IFRS, US GAAP, JGAAP)"
    )

    # Core metrics
    roe: float | None = Field(None, description="Return on Equity (%)")
    eps: float | None = Field(None, description="Earnings per share (JPY)")
    dilutedEps: float | None = Field(None, description="Diluted EPS (JPY)")
    bps: float | None = Field(None, description="Book value per share (JPY)")
    adjustedEps: float | None = Field(
        None, description="Adjusted EPS using share count (JPY)"
    )
    adjustedForecastEps: float | None = Field(
        None, description="Adjusted forecast EPS using share count (JPY)"
    )
    adjustedBps: float | None = Field(
        None, description="Adjusted BPS using share count (JPY)"
    )
    per: float | None = Field(None, description="Price to earnings ratio")
    pbr: float | None = Field(None, description="Price to book ratio")

    # Profitability metrics
    roa: float | None = Field(None, description="Return on Assets (%)")
    operatingMargin: float | None = Field(None, description="Operating margin (%)")
    netMargin: float | None = Field(None, description="Net profit margin (%)")

    # Financial data (millions of JPY)
    stockPrice: float | None = Field(None, description="Stock price at disclosure")
    netProfit: float | None = Field(None, description="Net profit (millions JPY)")
    equity: float | None = Field(None, description="Equity (millions JPY)")
    totalAssets: float | None = Field(None, description="Total assets (millions JPY)")
    netSales: float | None = Field(None, description="Net sales (millions JPY)")
    operatingProfit: float | None = Field(
        None, description="Operating profit (millions JPY)"
    )

    # Cash flow data (millions of JPY)
    cashFlowOperating: float | None = Field(
        None, description="Cash flow from operating activities (millions JPY)"
    )
    cashFlowInvesting: float | None = Field(
        None, description="Cash flow from investing activities (millions JPY)"
    )
    cashFlowFinancing: float | None = Field(
        None, description="Cash flow from financing activities (millions JPY)"
    )
    cashAndEquivalents: float | None = Field(
        None, description="Cash and equivalents (millions JPY)"
    )

    # FCF metrics
    fcf: float | None = Field(None, description="Free cash flow (millions JPY)")
    fcfYield: float | None = Field(None, description="FCF yield (%)")
    fcfMargin: float | None = Field(None, description="FCF margin (%)")

    # Forecast EPS
    forecastEps: float | None = Field(None, description="Forecast EPS (JPY)")
    forecastEpsChangeRate: float | None = Field(
        None, description="Forecast EPS change rate (%)"
    )

    # Revised forecast (from latest Q)
    revisedForecastEps: float | None = Field(
        None, description="Revised forecast EPS from latest Q (JPY)"
    )
    revisedForecastSource: str | None = Field(
        None, description="Source of revised forecast (1Q, 2Q, 3Q)"
    )

    # Previous period CF data
    prevCashFlowOperating: float | None = Field(
        None, description="Previous period CFO (millions JPY)"
    )
    prevCashFlowInvesting: float | None = Field(
        None, description="Previous period CFI (millions JPY)"
    )
    prevCashFlowFinancing: float | None = Field(
        None, description="Previous period CFF (millions JPY)"
    )
    prevCashAndEquivalents: float | None = Field(
        None, description="Previous period cash (millions JPY)"
    )


class DailyValuationDataPoint(BaseModel):
    """Daily valuation data point."""

    date: str = Field(..., description="Date (YYYY-MM-DD)")
    close: float = Field(..., description="Closing price")
    per: float | None = Field(None, description="PER at this date")
    pbr: float | None = Field(None, description="PBR at this date")
    marketCap: float | None = Field(None, description="Market cap at this date (JPY)")


class FundamentalsComputeResponse(BaseModel):
    """Response for fundamentals computation."""

    symbol: str = Field(..., description="Stock code")
    companyName: str | None = Field(None, description="Company name")
    data: list[FundamentalDataPoint] = Field(
        ..., description="Fundamental data points sorted by date descending"
    )
    latestMetrics: FundamentalDataPoint | None = Field(
        None, description="Latest metrics with daily valuation"
    )
    dailyValuation: list[DailyValuationDataPoint] | None = Field(
        None, description="Daily PER/PBR time-series"
    )
    lastUpdated: str = Field(..., description="Last updated timestamp (ISO 8601)")
