"""
Database Schemas

DB Stats / Validate / Sync / Refresh のスキーマ。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


# --- Common ---


class DateRange(BaseModel):
    min: str
    max: str


class StorageStats(BaseModel):
    duckdbBytes: int = 0
    parquetBytes: int = 0
    totalBytes: int = 0


IntradayFreshnessStatusLiteral = Literal["idle", "up_to_date", "stale"]


class IntradayFreshness(BaseModel):
    status: IntradayFreshnessStatusLiteral
    expectedDate: str
    latestDate: str | None = None
    latestTime: str | None = None
    lastIntradaySync: str | None = None
    readyTimeJst: str
    evaluatedAtJst: str
    calendarBasis: str = "weekday_cutoff"


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


class StockMinuteDataStats(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    latestTime: str | None = None
    averageBarsPerDay: float = 0


class IndicesStats(BaseModel):
    masterCount: int = 0
    dataCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    byCategory: dict[str, int] = Field(default_factory=dict)


class Options225Stats(BaseModel):
    count: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None


class MarginStats(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None


class ListedMarketCoverage(BaseModel):
    listedMarketStocks: int = 0
    coveredStocks: int = 0
    missingStocks: int = 0
    coverageRatio: float = 0
    issuerAliasCoveredCount: int = 0
    emptySkippedCount: int = 0


class FundamentalsStats(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    latestDisclosedDate: str | None = None
    listedMarketCoverage: ListedMarketCoverage


class MarketStatsResponse(BaseModel):
    initialized: bool
    lastSync: str | None = None
    lastIntradaySync: str | None = None
    timeSeriesSource: str = "duckdb-parquet"
    databaseSize: int
    storage: StorageStats
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataStats
    stockMinuteData: StockMinuteDataStats
    indices: IndicesStats
    options225: Options225Stats
    margin: MarginStats
    fundamentals: FundamentalsStats
    intradayFreshness: IntradayFreshness
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


class StockMinuteDataValidation(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    latestTime: str | None = None


class MarginValidation(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    orphanCount: int = 0
    emptySkippedCount: int = 0
    emptySkippedCodes: list[str] = Field(default_factory=list)


class Options225Validation(BaseModel):
    count: int = 0
    dateCount: int = 0
    dateRange: DateRange | None = None
    missingTopixCoverageDatesCount: int = 0
    missingTopixCoverageDates: list[str] = Field(default_factory=list)
    missingUnderlyingPriceDatesCount: int = 0
    missingUnderlyingPriceDates: list[str] = Field(default_factory=list)
    conflictingUnderlyingPriceDatesCount: int = 0
    conflictingUnderlyingPriceDates: list[str] = Field(default_factory=list)


class FundamentalsValidation(BaseModel):
    count: int = 0
    uniqueStockCount: int = 0
    latestDisclosedDate: str | None = None
    missingListedMarketStocksCount: int = 0
    missingListedMarketStocks: list[str] = Field(default_factory=list)
    issuerAliasCoveredCount: int = 0
    emptySkippedCount: int = 0
    emptySkippedCodes: list[str] = Field(default_factory=list)
    failedDatesCount: int = 0
    failedCodesCount: int = 0


class ValidationSampleWindow(BaseModel):
    returnedCount: int = 0
    totalCount: int = 0
    limit: int = 0
    truncated: bool = False


class ValidationSampleWindows(BaseModel):
    stockDataMissingDates: ValidationSampleWindow
    failedDates: ValidationSampleWindow
    adjustmentEvents: ValidationSampleWindow
    stocksNeedingRefresh: ValidationSampleWindow
    options225MissingTopixCoverageDates: ValidationSampleWindow
    options225MissingUnderlyingPriceDates: ValidationSampleWindow
    options225ConflictingUnderlyingPriceDates: ValidationSampleWindow
    missingListedMarketStocks: ValidationSampleWindow
    fundamentalsEmptySkippedCodes: ValidationSampleWindow
    marginEmptySkippedCodes: ValidationSampleWindow


class MarketValidationResponse(BaseModel):
    status: Literal["healthy", "warning", "error"]
    initialized: bool
    lastSync: str | None = None
    lastIntradaySync: str | None = None
    lastStocksRefresh: str | None = None
    timeSeriesSource: str = "duckdb-parquet"
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataValidation
    stockMinuteData: StockMinuteDataValidation
    options225: Options225Validation
    margin: MarginValidation
    fundamentals: FundamentalsValidation
    failedDates: list[str] = Field(default_factory=list)
    failedDatesCount: int = 0
    adjustmentEvents: list[AdjustmentEvent] = Field(default_factory=list)
    adjustmentEventsCount: int = 0
    stocksNeedingRefresh: list[str] = Field(default_factory=list)
    stocksNeedingRefreshCount: int = 0
    integrityIssues: list[IntegrityIssue] = Field(default_factory=list)
    integrityIssuesCount: int = 0
    sampleWindows: ValidationSampleWindows
    recommendations: list[str] = Field(default_factory=list)
    intradayFreshness: IntradayFreshness
    lastUpdated: str


# --- Sync ---


SyncModeLiteral = Literal["auto", "initial", "incremental", "repair"]
SyncDataBackendLiteral = Literal["duckdb-parquet"]
IntradaySyncModeLiteral = Literal["auto", "bulk", "rest"]


class SyncDataPlaneRequest(BaseModel):
    backend: SyncDataBackendLiteral = "duckdb-parquet"


class SyncRequest(BaseModel):
    mode: SyncModeLiteral = "auto"
    dataPlane: SyncDataPlaneRequest | None = None
    enforceBulkForStockData: bool = False
    resetBeforeSync: bool = False

    @model_validator(mode="after")
    def validate_reset_before_sync(self) -> "SyncRequest":
        if self.resetBeforeSync and self.mode != "initial":
            raise ValueError("resetBeforeSync is supported only when mode='initial'")
        return self


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
    fundamentalsUpdated: int = 0
    fundamentalsDatesProcessed: int = 0
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
    enforceBulkForStockData: bool = False
    progress: SyncProgress | None = None
    result: SyncResult | None = None
    startedAt: str
    completedAt: str | None = None
    error: str | None = None


class SyncFetchDetail(BaseModel):
    eventType: Literal["strategy", "execution"]
    stage: str
    endpoint: str
    method: Literal["rest", "bulk"]
    targetLabel: str | None = None
    reason: str | None = None
    reasonDetail: str | None = None
    estimatedRestCalls: int | None = None
    estimatedBulkCalls: int | None = None
    plannerApiCalls: int | None = None
    fallback: bool = False
    fallbackReason: str | None = None
    timestamp: str


class SyncFetchDetailsResponse(BaseModel):
    jobId: str
    status: str
    mode: str
    latest: SyncFetchDetail | None = None
    items: list[SyncFetchDetail] = Field(default_factory=list)


# --- Intraday Sync ---


class IntradaySyncRequest(BaseModel):
    mode: IntradaySyncModeLiteral = "auto"
    date: str | None = None
    dateFrom: str | None = None
    dateTo: str | None = None
    codes: list[str] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def validate_date_inputs(self) -> "IntradaySyncRequest":
        if self.date and (self.dateFrom or self.dateTo):
            raise ValueError("date cannot be combined with dateFrom/dateTo")
        if not self.date and not self.dateFrom and not self.dateTo:
            raise ValueError("date or dateFrom/dateTo is required")
        if self.dateFrom and self.dateTo and self.dateFrom > self.dateTo:
            raise ValueError("dateFrom must be before or equal to dateTo")
        if self.mode == "rest" and not self.codes:
            raise ValueError("codes is required when mode='rest'")
        return self


class IntradaySyncResponse(BaseModel):
    success: bool
    mode: IntradaySyncModeLiteral
    requestedCodes: int = 0
    storedCodes: int = 0
    datesProcessed: int = 0
    recordsFetched: int = 0
    recordsStored: int = 0
    apiCalls: int = 0
    selectedFiles: int = 0
    cacheHits: int = 0
    cacheMisses: int = 0
    skippedRows: int = 0
    lastUpdated: str


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
