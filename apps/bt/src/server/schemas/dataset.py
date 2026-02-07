"""
Dataset Management Schemas

Dataset 管理エンドポイントのリクエスト/レスポンススキーマ。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- List ---


class DatasetListItem(BaseModel):
    name: str = Field(description="Dataset name (without .db)")
    path: str = Field(description="Full file path")
    fileSize: int = Field(description="File size in bytes")
    lastModified: str = Field(description="Last modified ISO datetime")


# --- Info ---


class DateRange(BaseModel):
    min: str
    max: str


class DatasetValidation(BaseModel):
    isValid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DatasetSnapshot(BaseModel):
    preset: str | None = Field(default=None, description="Preset name used")
    totalStocks: int = Field(description="Number of stocks")
    stocksWithQuotes: int = Field(description="Stocks with OHLCV data")
    dateRange: DateRange | None = Field(default=None)
    validation: DatasetValidation


class DatasetInfoResponse(BaseModel):
    name: str
    path: str
    fileSize: int
    lastModified: str
    snapshot: DatasetSnapshot


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
