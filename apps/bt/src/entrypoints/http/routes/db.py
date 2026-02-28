"""
Database Routes

GET    /api/db/stats                 — DB 統計
GET    /api/db/validate              — DB 検証
POST   /api/db/sync                  — Sync 開始
GET    /api/db/sync/jobs/{jobId}     — Sync ジョブ状態
DELETE /api/db/sync/jobs/{jobId}     — Sync ジョブキャンセル
POST   /api/db/stocks/refresh        — 銘柄データ再取得
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger

from src.shared.config.settings import get_settings
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.time_series_store import (
    MarketTimeSeriesStore,
    create_time_series_store,
)
from src.entrypoints.http.schemas.db import (
    CreateSyncJobResponse,
    MarketStatsResponse,
    MarketValidationResponse,
    RefreshRequest,
    RefreshResponse,
    SyncRequest,
    SyncJobResponse,
)
from src.entrypoints.http.schemas.job import CancelJobResponse, JobStatus
from src.application.services import db_stats_service, db_validation_service, stock_refresh_service
from src.application.services.sync_service import SyncMode, sync_job_manager, start_sync

router = APIRouter(tags=["Database"])


def _get_market_db(request: Request) -> MarketDb:
    market_db = getattr(request.app.state, "market_db", None)
    if market_db is None:
        raise HTTPException(status_code=422, detail="Database not initialized. Please run sync first.")
    return market_db


def _get_jquants_client(request: Request) -> JQuantsAsyncClient:
    client = getattr(request.app.state, "jquants_client", None)
    if client is None:
        raise HTTPException(status_code=422, detail="JQuants client not initialized")
    return client


# --- Stats ---


@router.get(
    "/api/db/stats",
    response_model=MarketStatsResponse,
    summary="Market database statistics",
)
def get_db_stats(request: Request) -> MarketStatsResponse:
    market_db = _get_market_db(request)
    return db_stats_service.get_market_stats(market_db)


# --- Validate ---


@router.get(
    "/api/db/validate",
    response_model=MarketValidationResponse,
    summary="Market database validation",
)
def get_db_validate(request: Request) -> MarketValidationResponse:
    market_db = _get_market_db(request)
    time_series_store = getattr(request.app.state, "market_time_series_store", None)
    return db_validation_service.validate_market_db(
        market_db,
        time_series_store=time_series_store,
    )


# --- Sync ---


def _resolve_time_series_store(
    request: Request,
    market_db: MarketDb,
    body: SyncRequest,
) -> tuple[MarketTimeSeriesStore | None, bool]:
    """Sync request に応じた time-series store を解決する。"""
    default_store = getattr(request.app.state, "market_time_series_store", None)
    data_plane = body.dataPlane
    if data_plane is None:
        return default_store, False

    if data_plane.backend == "default" and data_plane.sqliteMirror is None:
        return default_store, False

    settings = get_settings()
    timeseries_base = Path(settings.market_timeseries_dir)
    resolved_backend = settings.market_timeseries_backend if data_plane.backend == "default" else data_plane.backend

    if resolved_backend == "sqlite":
        resolved_sqlite_mirror = True
    elif data_plane.sqliteMirror is None:
        resolved_sqlite_mirror = settings.market_timeseries_sqlite_mirror
    else:
        resolved_sqlite_mirror = data_plane.sqliteMirror

    if (
        resolved_backend == settings.market_timeseries_backend
        and resolved_sqlite_mirror == settings.market_timeseries_sqlite_mirror
    ):
        return default_store, False

    store = create_time_series_store(
        backend=resolved_backend,
        duckdb_path=str(timeseries_base / "market.duckdb"),
        parquet_dir=str(timeseries_base / "parquet"),
        sqlite_mirror=resolved_sqlite_mirror,
        market_db=market_db,
        allow_sqlite_fallback=resolved_sqlite_mirror,
    )
    requires_duckdb = resolved_backend in {"duckdb", "duckdb-parquet", "dual"} and not resolved_sqlite_mirror
    if requires_duckdb and store is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "DuckDB backend is unavailable for this request. "
                "Install duckdb or enable sqliteMirror."
            ),
        )
    return store, store is not None


def _close_time_series_store_safely(store: MarketTimeSeriesStore | None) -> None:
    if store is None:
        return
    try:
        store.close()
    except Exception as exc:
        logger.warning("Failed to close override time-series store: {}", exc)


@router.post(
    "/api/db/sync",
    response_model=CreateSyncJobResponse,
    status_code=202,
    summary="Start database sync job",
)
async def start_sync_job(request: Request, body: SyncRequest) -> JSONResponse:
    market_db = _get_market_db(request)
    jquants_client = _get_jquants_client(request)
    time_series_store, close_store_on_finish = _resolve_time_series_store(request, market_db, body)
    sync_mode = SyncMode(body.mode)

    try:
        job = await start_sync(
            sync_mode,
            market_db,
            jquants_client,
            time_series_store=time_series_store,
            close_time_series_store=close_store_on_finish,
        )
    except Exception:
        if close_store_on_finish and time_series_store is not None:
            _close_time_series_store_safely(time_series_store)
        raise

    if job is None:
        if close_store_on_finish and time_series_store is not None:
            _close_time_series_store_safely(time_series_store)
        raise HTTPException(status_code=409, detail="Another sync job is already running")

    from src.application.services.sync_strategies import get_strategy
    strategy = get_strategy(job.data.resolved_mode)

    return JSONResponse(
        status_code=202,
        content=CreateSyncJobResponse(
            jobId=job.job_id,
            status="pending",
            mode=job.data.resolved_mode,
            estimatedApiCalls=strategy.estimate_api_calls(),
            message="Sync job started",
        ).model_dump(),
    )


@router.get(
    "/api/db/sync/jobs/{jobId}",
    response_model=SyncJobResponse,
    summary="Get sync job status",
)
def get_sync_job(jobId: str) -> SyncJobResponse:
    job = sync_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")

    return SyncJobResponse(
        jobId=job.job_id,
        status=job.status.value,
        mode=job.data.resolved_mode or job.data.mode.value,
        progress=job.progress,
        result=job.result,
        startedAt=(job.started_at or job.created_at).isoformat(),
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
    )


@router.delete(
    "/api/db/sync/jobs/{jobId}",
    response_model=CancelJobResponse,
    summary="Cancel sync job",
)
async def cancel_sync_job(jobId: str) -> CancelJobResponse:
    job = sync_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(status_code=400, detail=f"Job {jobId} cannot be cancelled (status: {job.status.value})")
    await sync_job_manager.cancel_job(jobId)
    return CancelJobResponse(success=True, jobId=jobId, message="Job cancelled")


# --- Refresh ---


@router.post(
    "/api/db/stocks/refresh",
    response_model=RefreshResponse,
    summary="Refresh stock data for specific codes",
)
async def refresh_stocks(request: Request, body: RefreshRequest) -> RefreshResponse:
    market_db = _get_market_db(request)
    jquants_client = _get_jquants_client(request)

    if not market_db.is_initialized():
        raise HTTPException(status_code=422, detail="Database not initialized. Please run sync first.")

    return await stock_refresh_service.refresh_stocks(body.codes, market_db, jquants_client)
