"""Application-owned dataset response and value contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


DatasetStorageBackend = Literal["duckdb-parquet"]


class DatasetStorageInfo(BaseModel):
    backend: DatasetStorageBackend = Field(description="Resolved dataset storage backend")
    primaryPath: str = Field(description="Primary artifact path for the dataset")
    duckdbPath: str | None = Field(default=None, description="DuckDB snapshot path")
    manifestPath: str | None = Field(default=None, description="Dataset snapshot manifest path")


# --- List ---


class DatasetListItem(BaseModel):
    name: str = Field(description="Dataset snapshot name")
    path: str = Field(description="Full file path")
    fileSize: int = Field(description="File size in bytes")
    lastModified: str = Field(description="Last modified ISO datetime")
    preset: str | None = Field(default=None, description="Preset name used to create dataset")
    createdAt: str | None = Field(default=None, description="Created datetime stored in dataset_info")
    backend: DatasetStorageBackend = Field(description="Resolved storage backend")


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
    schemaVersion: Literal[3] = Field(description="Dataset manifest payload schema version")
    sourceMarketSchemaVersion: Literal[4] = Field(description="Source Market schema version")
    stockPriceAdjustmentMode: Literal["local_projection_v2_event_time"] = Field(
        description="Source stock price adjustment mode"
    )
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
    storage: DatasetStorageInfo
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
