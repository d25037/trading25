"""
Dataset Management Schemas

Dataset 管理エンドポイントのリクエスト/レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


# --- List ---


class DatasetListItem(BaseModel):
    name: str = Field(description="Dataset name (without .db)")
    path: str = Field(description="Full file path")
    fileSize: int = Field(description="File size in bytes")
    lastModified: str = Field(description="Last modified ISO datetime")
    preset: str | None = Field(default=None, description="Preset name used to create dataset")
    createdAt: str | None = Field(default=None, description="Created datetime stored in dataset_info")


# --- Info ---


class DatasetSnapshotDateRange(BaseModel):
    min: str
    max: str


class DatasetStatsDateRange(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    from_: str = Field(alias="from")
    to: str


class DatasetSnapshotValidation(BaseModel):
    isValid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DatasetSnapshot(BaseModel):
    preset: str | None = Field(default=None, description="Preset name used")
    createdAt: str | None = Field(default=None, description="Dataset created datetime")
    totalStocks: int = Field(default=0, description="Number of stocks")
    stocksWithQuotes: int = Field(default=0, description="Stocks with OHLCV data")
    dateRange: DatasetSnapshotDateRange | None = Field(default=None)
    validation: DatasetSnapshotValidation | None = None


class DatasetStatementsFieldCoverage(BaseModel):
    total: int = 0
    totalFY: int = 0
    totalHalf: int = 0
    hasExtendedFields: bool = False
    hasCashFlowFields: bool = False
    earningsPerShare: int = 0
    profit: int = 0
    equity: int = 0
    nextYearForecastEps: int = 0
    bps: int = 0
    sales: int = 0
    operatingProfit: int = 0
    ordinaryProfit: int = 0
    operatingCashFlow: int = 0
    dividendFY: int = 0
    forecastEps: int = 0
    investingCashFlow: int = 0
    financingCashFlow: int = 0
    cashAndEquivalents: int = 0
    totalAssets: int = 0
    sharesOutstanding: int = 0
    treasuryShares: int = 0


class DatasetStats(BaseModel):
    totalStocks: int = 0
    totalQuotes: int = 0
    dateRange: DatasetStatsDateRange
    hasMarginData: bool = False
    hasTOPIXData: bool = False
    hasSectorData: bool = False
    hasStatementsData: bool = False
    statementsFieldCoverage: DatasetStatementsFieldCoverage | None = None


class DatasetFkIntegrity(BaseModel):
    stockDataOrphans: int = 0
    marginDataOrphans: int = 0
    statementsOrphans: int = 0


class DatasetExpectedRange(BaseModel):
    min: int
    max: int


class DatasetStockCountValidation(BaseModel):
    preset: str | None = None
    expected: DatasetExpectedRange | None = None
    actual: int = 0
    isWithinRange: bool = True


class DatasetDataCoverage(BaseModel):
    totalStocks: int = 0
    stocksWithQuotes: int = 0
    stocksWithStatements: int = 0
    stocksWithMargin: int = 0


class DatasetValidationDetails(BaseModel):
    dateGapsCount: int | None = None
    fkIntegrity: DatasetFkIntegrity | None = None
    orphanStocksCount: int | None = None
    stockCountValidation: DatasetStockCountValidation | None = None
    dataCoverage: DatasetDataCoverage | None = None


class DatasetValidation(BaseModel):
    isValid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    details: DatasetValidationDetails | None = None


class DatasetInfoResponse(BaseModel):
    name: str
    path: str
    fileSize: int
    lastModified: str
    snapshot: DatasetSnapshot
    stats: DatasetStats
    validation: DatasetValidation


# --- Sample ---


class DatasetSampleResponse(BaseModel):
    codes: list[str] = Field(description="Random sample of stock codes")


# --- Search ---


class SearchResultItem(BaseModel):
    code: str
    name: str
    match_type: str = Field(description="Match type: exact/partial")


class DatasetSearchResponse(BaseModel):
    results: list[SearchResultItem]


# --- Create / Resume ---


class DatasetCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255, description="Dataset filename (e.g. 'prime.db')")
    preset: str = Field(description="Preset config name")
    overwrite: bool = Field(default=False, description="Overwrite existing dataset")
    timeoutMinutes: int = Field(default=35, ge=1, le=120, description="Build timeout in minutes")


class DatasetCreateResponse(BaseModel):
    jobId: str
    status: str
    name: str
    preset: str
    message: str
    estimatedTime: str | None = None


# --- Job ---


class DatasetJobResult(BaseModel):
    success: bool
    totalStocks: int = 0
    processedStocks: int = 0
    warnings: list[str] | None = None
    errors: list[str] | None = None
    outputPath: str = ""


class DatasetJobResponse(BaseModel):
    jobId: str
    status: str
    preset: str
    name: str
    progress: dict[str, object] | None = None
    result: DatasetJobResult | None = None
    startedAt: str
    completedAt: str | None = None
    error: str | None = None
