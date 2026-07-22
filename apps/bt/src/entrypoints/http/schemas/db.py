"""Market data-plane HTTP request and job envelope schemas."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from src.application.contracts import market_data_plane as market_contracts
from src.shared.contracts import market_maintenance as maintenance_contracts


SyncDataBackendLiteral = Literal["duckdb-parquet"]


class SyncDataPlaneRequest(BaseModel):
    backend: SyncDataBackendLiteral = "duckdb-parquet"


class SyncRequest(BaseModel):
    mode: market_contracts.SyncModeLiteral = "incremental"
    dataPlane: SyncDataPlaneRequest | None = None
    enforceBulkForStockData: bool = False
    resetBeforeSync: bool = False

    @model_validator(mode="after")
    def validate_reset_before_sync(self) -> SyncRequest:
        if self.mode == "initial" and not self.resetBeforeSync:
            raise ValueError("initial sync requires resetBeforeSync=true")
        if self.mode == "incremental" and self.resetBeforeSync:
            raise ValueError("incremental sync requires resetBeforeSync=false")
        return self


class CreateSyncJobResponse(BaseModel):
    jobId: str
    status: str = "pending"
    mode: market_contracts.SyncModeLiteral
    estimatedApiCalls: int
    message: str = "Sync job started"


class SyncJobResponse(BaseModel):
    jobId: str
    status: str
    mode: market_contracts.SyncModeLiteral
    enforceBulkForStockData: bool = False
    maintenance: maintenance_contracts.MarketMaintenanceRecord = Field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )
    progress: market_contracts.SyncProgress | None = None
    result: market_contracts.SyncResult | None = None
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
    mode: market_contracts.SyncModeLiteral
    latest: SyncFetchDetail | None = None
    items: list[SyncFetchDetail] = Field(default_factory=list)


class RefreshRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=50)
