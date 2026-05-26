"""
Fundamentals API Schemas

Pydantic models for fundamentals calculation request/response.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from src.entrypoints.http.schemas.analytics_common import (
    DataProvenance,
    ResponseDiagnostics,
)


class FundamentalsComputeRequest(BaseModel):
    """Request body for fundamentals computation."""

    symbol: str = Field(
        ...,
        description="Stock code (4-5 digits)",
        json_schema_extra={"example": "7203"},
    )
    from_date: str | None = Field(
        None,
        description="Start date (YYYY-MM-DD)",
        json_schema_extra={"example": "2020-01-01"},
    )
    to_date: str | None = Field(
        None,
        description="End date (YYYY-MM-DD)",
        json_schema_extra={"example": "2025-12-31"},
    )
    period_type: Literal["all", "FY", "1Q", "2Q", "3Q"] = Field(
        default="all", description="Filter by period type (FY, 1Q, 2Q, 3Q)"
    )
    prefer_consolidated: bool = Field(
        default=True, description="Prefer consolidated statements"
    )
    trading_value_period: int = Field(
        default=15,
        ge=1,
        le=250,
        description="Rolling period (days) for market cap to trading value ratio",
    )
    forecast_eps_lookback_fy_count: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Lookback FY count for forecast EPS vs recent actual EPS comparison",
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
    dividendFy: float | None = Field(
        None, description="Dividend per share for FY (JPY)"
    )
    adjustedDividendFy: float | None = Field(
        None, description="Adjusted FY dividend per share using share count (JPY)"
    )
    forecastDividendFy: float | None = Field(
        None, description="Forecast dividend per share for FY (JPY)"
    )
    adjustedForecastDividendFy: float | None = Field(
        None,
        description="Adjusted forecast FY dividend per share using share count (JPY)",
    )
    forecastDividendFyChangeRate: float | None = Field(
        None, description="Forecast dividend change rate from actual dividend (%)"
    )
    payoutRatio: float | None = Field(None, description="Payout ratio (%)")
    forecastPayoutRatio: float | None = Field(
        None, description="Forecast payout ratio (%)"
    )
    forecastPayoutRatioChangeRate: float | None = Field(
        None,
        description="Forecast payout ratio change rate from actual payout ratio (%)",
    )
    per: float | None = Field(None, description="Price to earnings ratio")
    forwardPer: float | None = Field(None, description="Forward price to earnings ratio")
    pOp: float | None = Field(None, description="Price to operating profit ratio")
    forwardPOp: float | None = Field(
        None, description="Forward price to operating profit ratio"
    )
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
    cfoYield: float | None = Field(None, description="CFO yield (%)")
    cfoMargin: float | None = Field(None, description="CFO margin (%)")
    cfoToNetProfitRatio: float | None = Field(
        None, description="Operating cash flow / net profit ratio (x)"
    )
    tradingValueToMarketCapRatio: float | None = Field(
        None, description="Market cap / N-day average trading value ratio (x)"
    )
    marketCap: float | None = Field(None, description="Market cap (JPY)")
    freeFloatMarketCap: float | None = Field(
        None, description="Free-float market cap (JPY)"
    )

    # Forecast EPS
    forecastEps: float | None = Field(None, description="Forecast EPS (JPY)")
    forecastEpsChangeRate: float | None = Field(
        None, description="Forecast EPS change rate (%)"
    )
    forecastOperatingProfit: float | None = Field(
        None, description="Forecast operating profit (millions JPY)"
    )
    forecastOperatingProfitChangeRate: float | None = Field(
        None, description="Forecast operating profit change rate (%)"
    )
    forecastEpsAboveRecentFyActuals: bool | None = Field(
        None,
        description="Whether latest forecast EPS is greater than recent FY actual EPS values (lookback window)",
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
    eps: float | None = Field(
        None, description="Adjusted actual EPS used for valuation"
    )
    bps: float | None = Field(
        None, description="Adjusted BPS used for valuation"
    )
    per: float | None = Field(None, description="PER at this date")
    forwardPer: float | None = Field(None, description="Forward PER at this date")
    pOp: float | None = Field(None, description="P/OP at this date")
    forwardPOp: float | None = Field(None, description="Forward P/OP at this date")
    pbr: float | None = Field(None, description="PBR at this date")
    marketCap: float | None = Field(
        None,
        description="Market cap at this date using shares outstanding (JPY)",
    )
    freeFloatMarketCap: float | None = Field(
        None,
        description="Market cap at this date using free-float shares (JPY)",
    )
    statementDisclosedDate: str | None = Field(
        None, description="Disclosure date of the FY actual EPS/BPS source"
    )
    forwardEps: float | None = Field(None, description="Forward EPS used for valuation")
    forwardEpsDisclosedDate: str | None = Field(
        None, description="Disclosure date of the forward EPS source"
    )
    forwardEpsSource: Literal["revised", "fy"] | None = Field(
        None, description="Forward EPS source"
    )
    priceBasisDate: str | None = Field(
        None, description="Adjusted price basis date for this valuation row"
    )
    basisVersion: str | None = Field(
        None, description="Adjusted valuation materialization basis version"
    )


class LiquidityProfileWindow(BaseModel):
    """Free-float liquidity profile for one median ADV window."""

    advWindow: int = Field(..., description="ADV window in trading sessions")
    averageTradingValue: float | None = Field(
        None,
        description=(
            "N-day median trading value (JPY). Field name is kept for backwards "
            "compatibility with existing clients."
        ),
    )
    freeFloatTradingValueRatioPct: float | None = Field(
        None, description="Median ADV / free-float market cap (%)"
    )
    liquidityResidualZ: float | None = Field(
        None,
        description="Z-score of log median ADV residual against Prime free-float market cap regression",
    )
    liquidityImpliedFreeFloatMarketCap: float | None = Field(
        None,
        description="Free-float market cap implied by current median ADV using Prime regression (JPY)",
    )
    liquidityImpliedPrice: float | None = Field(
        None,
        description="Price implied by liquidity-implied free-float market cap (JPY)",
    )
    liquidityImpliedPriceGapPct: float | None = Field(
        None, description="Liquidity-implied price gap versus latest close (%)"
    )
    liquidityRegime: str | None = Field(
        None, description="Prime-only liquidity regime label"
    )
    regressionAlpha: float | None = Field(None, description="Regression intercept")
    regressionBeta: float | None = Field(None, description="Regression slope")
    regressionRSquared: float | None = Field(None, description="Regression R-squared")
    regressionObservationCount: int | None = Field(
        None, description="Number of Prime observations used in regression"
    )


class LiquidityProfile(BaseModel):
    """Prime-only free-float liquidity diagnostic."""

    supported: bool = Field(
        ..., description="Whether the profile is supported for this symbol"
    )
    unsupportedReason: str | None = Field(None, description="Reason when unsupported")
    modelScope: str = Field(default="prime", description="Regression model scope")
    date: str | None = Field(None, description="Observation date")
    currentPrice: float | None = Field(None, description="Latest close (JPY)")
    freeFloatMarketCap: float | None = Field(
        None, description="Latest free-float market cap (JPY)"
    )
    recentReturn20dPct: float | None = Field(
        None, description="Recent 20-session return (%)"
    )
    recentReturn60dPct: float | None = Field(
        None, description="Recent 60-session return (%)"
    )
    windows: list[LiquidityProfileWindow] = Field(default_factory=list)


class LatestMetricsSourceItem(BaseModel):
    """Source metadata for one part of latestMetrics."""

    table: Literal["daily_valuation", "statements"] = Field(
        ..., description="Source table used for this latestMetrics part"
    )
    date: str | None = Field(None, description="Trading or period date of the source")
    periodType: str | None = Field(None, description="Statement period type when applicable")
    disclosedDate: str | None = Field(None, description="Statement disclosure date when applicable")
    source: str | None = Field(None, description="Source classifier such as fy or revised")


class LatestMetricsSource(BaseModel):
    """Source metadata for composed latestMetrics summary values."""

    actualPerShare: LatestMetricsSourceItem = Field(
        ..., description="Source for EPS/BPS displayed in latestMetrics"
    )
    valuation: LatestMetricsSourceItem = Field(
        ..., description="Source for PER/PBR/market-cap values"
    )
    forecast: LatestMetricsSourceItem | None = Field(
        None, description="Source for forecast EPS and forward valuation values"
    )
    latestDisclosure: LatestMetricsSourceItem | None = Field(
        None, description="Latest statement disclosure used for operating and cash-flow metrics"
    )


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
    latestMetricsSource: LatestMetricsSource | None = Field(
        None, description="Source tables and dates used to compose latestMetrics"
    )
    dailyValuation: list[DailyValuationDataPoint] | None = Field(
        None, description="Daily PER/PBR time-series"
    )
    priceBasisDate: str | None = Field(
        None, description="Adjusted price basis date used by daily valuation"
    )
    valuationBasisVersion: str | None = Field(
        None, description="Adjusted valuation materialization basis version"
    )
    liquidityProfile: LiquidityProfile | None = Field(
        None,
        description="Prime-only free-float liquidity diagnostic for Symbol Workbench",
    )
    tradingValuePeriod: int = Field(
        ..., description="Rolling period used for market cap to trading value ratio"
    )
    forecastEpsLookbackFyCount: int = Field(
        default=3,
        description="Lookback FY count used for forecast EPS comparison indicator",
    )
    lastUpdated: str = Field(..., description="Last updated timestamp (ISO 8601)")
    provenance: DataProvenance
    diagnostics: ResponseDiagnostics = Field(default_factory=ResponseDiagnostics)
