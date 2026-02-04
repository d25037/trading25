"""Pydantic models for API responses."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


# =============================================================================
# Stock Data Models
# =============================================================================


class StockOHLCVRecord(BaseModel):
    """Stock OHLCV record from API response."""

    date: date
    open: float = Field(alias="open")
    high: float = Field(alias="high")
    low: float = Field(alias="low")
    close: float = Field(alias="close")
    volume: int = Field(alias="volume")

    class Config:
        populate_by_name = True


class StockListItem(BaseModel):
    """Stock list item with metadata."""

    stock_code: str = Field(alias="stockCode")
    record_count: int = Field(alias="record_count")
    start_date: Optional[date] = Field(default=None, alias="start_date")
    end_date: Optional[date] = Field(default=None, alias="end_date")

    class Config:
        populate_by_name = True


class StockInfo(BaseModel):
    """Stock basic information."""

    code: str
    company_name: str = Field(alias="company_name")

    class Config:
        populate_by_name = True


# =============================================================================
# Index Data Models
# =============================================================================


class TopixRecord(BaseModel):
    """TOPIX data record."""

    date: date
    open: float
    high: float
    low: float
    close: float


class IndexRecord(BaseModel):
    """Index data record."""

    date: date
    open: float
    high: float
    low: float
    close: float


class IndexListItem(BaseModel):
    """Index list item with metadata."""

    index_code: str = Field(alias="indexCode")
    index_name: str = Field(alias="indexName")
    record_count: int = Field(alias="record_count")
    start_date: Optional[date] = Field(default=None, alias="start_date")
    end_date: Optional[date] = Field(default=None, alias="end_date")

    class Config:
        populate_by_name = True


# =============================================================================
# Margin Data Models
# =============================================================================


class MarginRecord(BaseModel):
    """Margin data record."""

    date: date
    long_margin_volume: float = Field(alias="longMarginVolume")
    short_margin_volume: float = Field(alias="shortMarginVolume")

    class Config:
        populate_by_name = True


class MarginListItem(BaseModel):
    """Margin data list item with metadata."""

    stock_code: str = Field(alias="stockCode")
    record_count: int = Field(alias="record_count")
    start_date: Optional[date] = Field(default=None, alias="start_date")
    end_date: Optional[date] = Field(default=None, alias="end_date")
    avg_long_margin: Optional[float] = Field(default=None, alias="avg_long_margin")
    avg_short_margin: Optional[float] = Field(default=None, alias="avg_short_margin")

    class Config:
        populate_by_name = True


# =============================================================================
# Financial Statement Models
# =============================================================================


class StatementsRecord(BaseModel):
    """Financial statements record."""

    disclosed_date: date = Field(alias="disclosedDate")
    earnings_per_share: Optional[float] = Field(default=None, alias="earningsPerShare")
    profit: Optional[float] = Field(default=None, alias="profit")
    equity: Optional[float] = Field(default=None, alias="equity")
    # Period type: "FY" (本決算), "1Q", "2Q", "3Q" (四半期)
    type_of_current_period: Optional[str] = Field(
        default=None, alias="typeOfCurrentPeriod"
    )
    # Extended financial metrics (added 2026-01)
    next_year_forecast_eps: Optional[float] = Field(
        default=None, alias="nextYearForecastEarningsPerShare"
    )
    bps: Optional[float] = Field(default=None, alias="bps")
    sales: Optional[float] = Field(default=None, alias="sales")
    operating_profit: Optional[float] = Field(default=None, alias="operatingProfit")
    ordinary_profit: Optional[float] = Field(default=None, alias="ordinaryProfit")
    operating_cash_flow: Optional[float] = Field(default=None, alias="operatingCashFlow")
    investing_cash_flow: Optional[float] = Field(default=None, alias="investingCashFlow")
    dividend_fy: Optional[float] = Field(default=None, alias="dividendFY")
    forecast_eps: Optional[float] = Field(default=None, alias="forecastEps")
    # Share count data (added 2026-01)
    shares_outstanding: Optional[int] = Field(default=None, alias="sharesOutstanding")
    treasury_shares: Optional[int] = Field(default=None, alias="treasuryShares")

    class Config:
        populate_by_name = True


# =============================================================================
# Sector Mapping Models
# =============================================================================


class SectorMappingRecord(BaseModel):
    """Sector to index mapping record."""

    sector_code: str = Field(alias="sector_code")
    sector_name: str = Field(alias="sector_name")
    index_code: str = Field(alias="index_code")
    index_name: str = Field(alias="index_name")

    class Config:
        populate_by_name = True


# =============================================================================
# Portfolio Models
# =============================================================================


class PortfolioItem(BaseModel):
    """Portfolio item (holding)."""

    id: int
    portfolio_id: int = Field(alias="portfolio_id")
    code: str
    company_name: str = Field(alias="company_name")
    quantity: int
    purchase_price: float = Field(alias="purchase_price")
    purchase_date: Optional[date] = Field(default=None, alias="purchase_date")
    account: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime = Field(alias="created_at")
    updated_at: datetime = Field(alias="updated_at")

    class Config:
        populate_by_name = True


class Portfolio(BaseModel):
    """Portfolio with basic info."""

    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(alias="created_at")
    updated_at: datetime = Field(alias="updated_at")
    items: list[PortfolioItem] = Field(default_factory=list)

    class Config:
        populate_by_name = True


# =============================================================================
# API Response Wrappers
# =============================================================================


class PaginatedResponse(BaseModel):
    """Paginated API response wrapper."""

    data: list[dict]  # type: ignore[type-arg]
    total: int
    page: int
    per_page: int = Field(alias="perPage")
    has_next: bool = Field(alias="hasNext")

    class Config:
        populate_by_name = True


class APIHealthResponse(BaseModel):
    """Health check response."""

    status: str
    timestamp: datetime
