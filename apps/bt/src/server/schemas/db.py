"""
Database Schemas

DB Stats / Validate / Sync / Refresh のスキーマ。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# --- Common ---


class DateRange(BaseModel):
    min: str
    max: str


# --- Stats ---


class TopixStats(BaseModel):
    count: int
    dateRange: DateRange | None = None


class StockStats(BaseModel):
    total: int
    byMarket: dict[str, int] = Field(default_factory=dict)


class StockDataStats(BaseModel):
    count: int
    dateCount: int = 0
    dateRange: DateRange | None = None
    averageStocksPerDay: float = 0


class IndicesStats(BaseModel):
    masterCount: int = 0
    dataCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    byCategory: dict[str, int] = Field(default_factory=dict)


class MarketStatsResponse(BaseModel):
    initialized: bool
    lastSync: str | None = None
    databaseSize: int
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataStats
    indices: IndicesStats
    lastUpdated: str


# --- Validate ---


class AdjustmentEvent(BaseModel):
    code: str
    date: str
    adjustmentFactor: float
    close: float
    eventType: str


class IntegrityIssue(BaseModel):
    code: str
    count: int


class StockDataValidation(BaseModel):
    count: int
    dateRange: DateRange | None = None
    missingDates: list[str] = Field(default_factory=list)
    missingDatesCount: int = 0


class MarketValidationResponse(BaseModel):
    status: Literal["healthy", "warning", "error"]
    initialized: bool
    lastSync: str | None = None
    lastStocksRefresh: str | None = None
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataValidation
    failedDates: list[str] = Field(default_factory=list)
    failedDatesCount: int = 0
    adjustmentEvents: list[AdjustmentEvent] = Field(default_factory=list)
    adjustmentEventsCount: int = 0
    stocksNeedingRefresh: list[str] = Field(default_factory=list)
    stocksNeedingRefreshCount: int = 0
    integrityIssues: list[IntegrityIssue] = Field(default_factory=list)
    integrityIssuesCount: int = 0
    recommendations: list[str] = Field(default_factory=list)
    lastUpdated: str


# --- Sync ---


class SyncProgress(BaseModel):
    stage: str
    current: int
    total: int
    percentage: float
    message: str


class SyncResult(BaseModel):
    success: bool
    totalApiCalls: int = 0
    stocksUpdated: int = 0
    datesProcessed: int = 0
    failedDates: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class CreateSyncJobResponse(BaseModel):
    jobId: str
    status: str = "pending"
    mode: str
    estimatedApiCalls: int
    message: str = "Sync job started"


class SyncJobResponse(BaseModel):
    jobId: str
    status: str
    mode: str
    progress: SyncProgress | None = None
    result: SyncResult | None = None
    startedAt: str
    completedAt: str | None = None
    error: str | None = None


# --- Refresh ---


class RefreshStockResult(BaseModel):
    code: str
    success: bool
    recordsFetched: int = 0
    recordsStored: int = 0
    error: str | None = None


class RefreshRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=50)


class RefreshResponse(BaseModel):
    totalStocks: int
    successCount: int
    failedCount: int
    totalApiCalls: int
    totalRecordsStored: int
    results: list[RefreshStockResult]
    errors: list[str] = Field(default_factory=list)
    lastUpdated: str
