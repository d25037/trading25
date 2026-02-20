"""
Dataset Data Schemas

Dataset Data エンドポイント（15 EP）のレスポンススキーマ。
camelCase フィールド名は Hono 互換。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- OHLCV ---


class OHLCVRecord(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


class OHLCRecord(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float


# --- Stock List ---


class StockListItem(BaseModel):
    stockCode: str = Field(description="Stock code (4-digit)")
    record_count: int = Field(description="Number of OHLCV records")
    start_date: str | None = Field(default=None, description="First date")
    end_date: str | None = Field(default=None, description="Last date")


# --- Index List ---


class IndexListItem(BaseModel):
    indexCode: str = Field(description="Index code")
    indexName: str | None = Field(default=None, description="Index/sector name")
    record_count: int = Field(description="Number of records")
    start_date: str | None = Field(default=None, description="First date")
    end_date: str | None = Field(default=None, description="Last date")


# --- Margin ---


class MarginRecord(BaseModel):
    date: str
    longMarginVolume: float | None = None
    shortMarginVolume: float | None = None


class MarginListItem(BaseModel):
    stockCode: str = Field(description="Stock code (4-digit)")
    record_count: int = Field(description="Number of margin records")
    start_date: str | None = Field(default=None)
    end_date: str | None = Field(default=None)
    avg_long_margin: float | None = Field(default=None)
    avg_short_margin: float | None = Field(default=None)


# --- Statements ---


class StatementRecord(BaseModel):
    code: str
    disclosedDate: str
    earningsPerShare: float | None = None
    profit: float | None = None
    equity: float | None = None
    typeOfCurrentPeriod: str | None = None
    typeOfDocument: str | None = None
    nextYearForecastEarningsPerShare: float | None = None
    bps: float | None = None
    sales: float | None = None
    operatingProfit: float | None = None
    ordinaryProfit: float | None = None
    operatingCashFlow: float | None = None
    dividendFy: float | None = None
    forecastDividendFy: float | None = None
    nextYearForecastDividendFy: float | None = None
    payoutRatio: float | None = None
    forecastPayoutRatio: float | None = None
    nextYearForecastPayoutRatio: float | None = None
    forecastEps: float | None = None
    investingCashFlow: float | None = None
    financingCashFlow: float | None = None
    cashAndEquivalents: float | None = None
    totalAssets: float | None = None
    sharesOutstanding: float | None = None
    treasuryShares: float | None = None


# --- Sectors ---


class SectorWithCount(BaseModel):
    sectorName: str
    count: int
