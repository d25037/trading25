"""Dataset HTTP request and job envelope schemas."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class DatasetCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        min_length=1,
        max_length=255,
        description=(
            "Export/repro dataset snapshot name (not normal backtest SoT; "
            "e.g. 'primeMarket')"
        ),
    )
    preset: str = Field(description="Export/repro preset config name")
    overwrite: bool = Field(default=False, description="Overwrite existing dataset")


class DatasetCreateResponse(BaseModel):
    jobId: str
    status: str
    name: str
    preset: str
    message: str
    estimatedTime: str | None = None


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
