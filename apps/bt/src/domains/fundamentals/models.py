"""Domain models for fundamentals calculations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel


class FundamentalDataPoint(BaseModel):
    # Period information
    date: str
    disclosedDate: str
    periodType: str
    isConsolidated: bool
    accountingStandard: str | None = None

    # Core metrics
    roe: float | None = None
    eps: float | None = None
    dilutedEps: float | None = None
    bps: float | None = None
    adjustedEps: float | None = None
    adjustedForecastEps: float | None = None
    adjustedBps: float | None = None
    dividendFy: float | None = None
    adjustedDividendFy: float | None = None
    forecastDividendFy: float | None = None
    adjustedForecastDividendFy: float | None = None
    forecastDividendFyChangeRate: float | None = None
    payoutRatio: float | None = None
    forecastPayoutRatio: float | None = None
    forecastPayoutRatioChangeRate: float | None = None
    per: float | None = None
    forwardPer: float | None = None
    psr: float | None = None
    forwardPsr: float | None = None
    pOp: float | None = None
    forwardPOp: float | None = None
    pbr: float | None = None

    # Profitability metrics
    roa: float | None = None
    operatingMargin: float | None = None
    netMargin: float | None = None

    # Financial data (millions of JPY)
    stockPrice: float | None = None
    netProfit: float | None = None
    equity: float | None = None
    totalAssets: float | None = None
    netSales: float | None = None
    operatingProfit: float | None = None

    # Cash flow data (millions of JPY)
    cashFlowOperating: float | None = None
    cashFlowInvesting: float | None = None
    cashFlowFinancing: float | None = None
    cashAndEquivalents: float | None = None

    # FCF metrics
    fcf: float | None = None
    fcfYield: float | None = None
    fcfMargin: float | None = None
    cfoYield: float | None = None
    cfoMargin: float | None = None
    cfoToNetProfitRatio: float | None = None
    tradingValueToMarketCapRatio: float | None = None
    marketCap: float | None = None
    freeFloatMarketCap: float | None = None

    # Forecast EPS
    forecastEps: float | None = None
    forecastEpsChangeRate: float | None = None
    forecastOperatingProfit: float | None = None
    forecastOperatingProfitChangeRate: float | None = None
    forecastEpsAboveRecentFyActuals: bool | None = None

    # Revised forecast (from latest Q)
    revisedForecastEps: float | None = None
    revisedForecastOperatingProfit: float | None = None
    revisedForecastSource: str | None = None

    # Previous period CF data
    prevCashFlowOperating: float | None = None
    prevCashFlowInvesting: float | None = None
    prevCashFlowFinancing: float | None = None
    prevCashAndEquivalents: float | None = None


class DailyValuationDataPoint(BaseModel):
    date: str
    close: float
    eps: float | None = None
    bps: float | None = None
    per: float | None = None
    forwardPer: float | None = None
    sales: float | None = None
    forwardSales: float | None = None
    psr: float | None = None
    forwardPsr: float | None = None
    pOp: float | None = None
    forwardPOp: float | None = None
    pbr: float | None = None
    marketCap: float | None = None
    freeFloatMarketCap: float | None = None
    statementDisclosedDate: str | None = None
    forwardEps: float | None = None
    forwardEpsDisclosedDate: str | None = None
    forwardEpsSource: Literal["revised", "fy"] | None = None
    forwardSalesDisclosedDate: str | None = None
    forwardSalesSource: Literal["revised", "fy"] | None = None
    priceBasisDate: str | None = None
    basisVersion: str | None = None


@dataclass
class FYDataPoint:
    disclosed_date: str
    eps: float | None
    bps: float | None
    operating_profit: float | None = None
    sales: float | None = None
    forward_eps: float | None = None
    forward_operating_profit: float | None = None
    forward_sales: float | None = None
    forward_eps_disclosed_date: str | None = None
    forward_eps_source: Literal["revised", "fy"] | None = None
    forward_sales_disclosed_date: str | None = None
    forward_sales_source: Literal["revised", "fy"] | None = None


EMPTY_PREV_CASH_FLOW: dict[str, float | None] = {
    "prevCashFlowOperating": None,
    "prevCashFlowInvesting": None,
    "prevCashFlowFinancing": None,
    "prevCashAndEquivalents": None,
}
