"""Application-owned Market data-plane contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.shared.contracts import market_maintenance as maintenance_contracts


# --- Common ---


class DateRange(BaseModel):
    min: str
    max: str


class StorageStats(BaseModel):
    duckdbBytes: int = 0
    parquetBytes: int = 0
    totalBytes: int = 0
    duckdbBlocksTotal: int = 0
    duckdbBlocksUsed: int = 0
    duckdbBlocksFree: int = 0
    duckdbBytesFree: int = 0
    duckdbWalBytes: int = 0
    tempDirectory: str | None = None
    tempBytes: int = 0
    staleArtifactCount: int = 0
    staleArtifacts: list[str] = Field(default_factory=list)


IntradayFreshnessStatusLiteral = Literal["idle", "up_to_date", "stale"]
ValidationHealthStatusLiteral = Literal["healthy", "info", "warning", "error"]
Options225CoverageStatusLiteral = Literal[
    "in_sync",
    "missing",
    "pending",
    "stale",
    "partial",
]
AdjustedMetricsStatusLiteral = Literal[
    "ready",
    "missing",
    "stale",
    "incomplete_coverage",
    "invalid_lineage",
    "empty_source",
]


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


class MarketSchemaStats(BaseModel):
    version: int | None = None
    requiredVersion: int = 4
    current: bool = False


class StockMasterCoverageStats(BaseModel):
    dailyCount: int = 0
    intervalCount: int = 0
    latestCount: int = 0
    indexMembershipDailyCount: int = 0
    dateRange: DateRange | None = None
    dateCount: int = 0
    codeCount: int = 0
    missingTopixDatesCount: int = 0
    missingTopixDates: list[str] = Field(default_factory=list)


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


class AdjustedMetricsStats(BaseModel):
    currentBasisStatementCount: int = 0
    dailyValuationRows: int = 0
    dailyTechnicalMetricRows: int = 0
    dailyValuationLatestDate: str | None = None
    dailyValuationLatestCodeCount: int = 0
    dailyValuationPreviousCodeCount: int = 0
    fundamentalsAdjustmentBasisDate: str | None = None
    providerWindowCount: int = 0
    readyProviderWindowCount: int = 0
    providerWindowCoverageFrontier: str | None = None
    pendingCurrentBasisCodeCount: int = 0
    orphanAdjustedStatementRows: int = 0
    orphanDailyValuationRows: int = 0
    sourceStatementKeyCount: int = 0
    expectedAdjustedStatementRows: int = 0
    missingAdjustedStatementRows: int = 0
    extraAdjustedStatementRows: int = 0
    staleAdjustedStatementRows: int = 0
    wrongBasisAdjustedStatementRows: int = 0
    missingDailyValuationRows: int = 0
    extraDailyValuationRows: int = 0
    wrongBasisDailyValuationRows: int = 0
    status: AdjustedMetricsStatusLiteral = "empty_source"


class MarketStatsResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    initialized: bool
    lastSync: str | None = None
    lastIntradaySync: str | None = None
    timeSeriesSource: str = "duckdb-parquet"
    databaseSize: int
    storage: StorageStats
    maintenance: maintenance_contracts.MarketMaintenanceRecord = Field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )
    schema_: MarketSchemaStats = Field(
        default_factory=MarketSchemaStats, alias="schema"
    )
    stockMaster: StockMasterCoverageStats = Field(
        default_factory=StockMasterCoverageStats
    )
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataStats
    stockMinuteData: StockMinuteDataStats
    indices: IndicesStats
    options225: Options225Stats
    margin: MarginStats
    fundamentals: FundamentalsStats
    adjustedMetrics: AdjustedMetricsStats = Field(default_factory=AdjustedMetricsStats)
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
    coverageStatus: Options225CoverageStatusLiteral = "in_sync"
    allowedTopixLagDates: int = 0
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
    stockMasterMissingTopixDates: ValidationSampleWindow
    failedDates: ValidationSampleWindow
    adjustmentEvents: ValidationSampleWindow
    stocksNeedingRefresh: ValidationSampleWindow
    options225MissingTopixCoverageDates: ValidationSampleWindow
    options225MissingUnderlyingPriceDates: ValidationSampleWindow
    options225ConflictingUnderlyingPriceDates: ValidationSampleWindow
    missingListedMarketStocks: ValidationSampleWindow
    fundamentalsEmptySkippedCodes: ValidationSampleWindow
    marginEmptySkippedCodes: ValidationSampleWindow


class ValidationHealthDomains(BaseModel):
    coreDailyStatus: ValidationHealthStatusLiteral = "healthy"
    derivativesStatus: ValidationHealthStatusLiteral = "healthy"
    intradayStatus: ValidationHealthStatusLiteral = "healthy"
    sourceQualityStatus: ValidationHealthStatusLiteral = "healthy"


class MarketValidationResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    status: Literal["healthy", "warning", "error"]
    healthDomains: ValidationHealthDomains
    initialized: bool
    lastSync: str | None = None
    lastIntradaySync: str | None = None
    lastStocksRefresh: str | None = None
    timeSeriesSource: str = "duckdb-parquet"
    schema_: MarketSchemaStats = Field(
        default_factory=MarketSchemaStats, alias="schema"
    )
    stockMaster: StockMasterCoverageStats = Field(
        default_factory=StockMasterCoverageStats
    )
    topix: TopixStats
    stocks: StockStats
    stockData: StockDataValidation
    stockMinuteData: StockMinuteDataValidation
    options225: Options225Validation
    margin: MarginValidation
    fundamentals: FundamentalsValidation
    adjustedMetrics: AdjustedMetricsStats = Field(default_factory=AdjustedMetricsStats)
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
IntradaySyncModeLiteral = Literal["auto", "bulk", "rest"]


class SyncProgress(BaseModel):
    stage: str
    current: int
    total: int
    percentage: float
    message: str
    completedCodes: int | None = None
    totalCodes: int | None = None
    currentCode: str | None = None
    currentBasisStatementCount: int | None = None
    stockRowsAppended: int = 0
    affectedStockCodes: int = 0
    stockCodesReplaced: int = 0
    stockRowsReplaced: int = 0


class SyncResult(BaseModel):
    success: bool
    totalApiCalls: int = 0
    stocksUpdated: int = 0
    stockRowsAppended: int = 0
    affectedStockCodes: int = 0
    stockCodesReplaced: int = 0
    stockRowsReplaced: int = 0
    stockRecomputationErrors: list[str] = Field(default_factory=list)
    datesProcessed: int = 0
    fundamentalsUpdated: int = 0
    fundamentalsDatesProcessed: int = 0
    failedDates: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class AdjustedMetricsMaterializeResult(BaseModel):
    success: bool
    completedCodes: int
    totalCodes: int
    currentBasisStatementCount: int
    pendingCurrentBasisCodeCount: int
    dailyValuationRows: int
    dailyTechnicalMetricRows: int
    dailyValuationLatestDate: str | None
    fundamentalsAdjustmentBasisDate: str | None


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
    maintenance: maintenance_contracts.MarketMaintenanceRecord = Field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )
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


class RefreshResponse(BaseModel):
    maintenance: maintenance_contracts.MarketMaintenanceRecord = Field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )
    totalStocks: int
    successCount: int
    failedCount: int
    totalApiCalls: int
    totalRecordsStored: int
    results: list[RefreshStockResult]
    errors: list[str] = Field(default_factory=list)
    lastUpdated: str
