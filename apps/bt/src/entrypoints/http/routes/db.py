"""
Database Routes

GET    /api/db/stats                 — DB 統計
GET    /api/db/validate              — DB 検証
POST   /api/db/sync                  — Sync 開始
GET    /api/db/sync/jobs/active      — 実行中 Sync ジョブ状態
GET    /api/db/sync/jobs/{jobId}     — Sync ジョブ状態
GET    /api/db/sync/jobs/{jobId}/stream — Sync SSE stream
DELETE /api/db/sync/jobs/{jobId}     — Sync ジョブキャンセル
POST   /api/db/stocks/refresh        — 銘柄データ再取得
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from src.application.services.chart_service import ChartService
from src.application.services.margin_analytics_service import create_market_margin_analytics_service
from src.application.services.market_data_service import MarketDataService
from src.application.services.roe_service import create_market_roe_service
from src.shared.config.settings import get_settings
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_reader import MarketDbReader
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
    SyncFetchDetail,
    SyncFetchDetailsResponse,
    SyncProgress,
    SyncRequest,
    SyncJobResponse,
    SyncResult,
)
from src.entrypoints.http.schemas.job import CancelJobResponse, JobStatus
from src.application.services import db_stats_service, db_validation_service, stock_refresh_service
from src.application.services.generic_job_manager import JobInfo
from src.application.services.sync_service import SyncJobData, SyncMode, sync_job_manager, start_sync
from src.application.services.sync_stream_manager import SyncStreamEvent, sync_stream_manager
from src.infrastructure.data_access.clients import close_all_cached_data_access_clients

router = APIRouter(tags=["Database"])


def _get_market_db(request: Request) -> MarketDb:
    market_db = getattr(request.app.state, "market_db", None)
    if market_db is None:
        raise HTTPException(status_code=422, detail="Database not initialized. Please run sync first.")
    return market_db


def _market_timeseries_paths() -> tuple[Path, Path]:
    settings = get_settings()
    timeseries_base = Path(settings.market_timeseries_dir)
    return timeseries_base / "market.duckdb", timeseries_base / "parquet"


def _create_market_resources() -> tuple[MarketDb, MarketTimeSeriesStore]:
    duckdb_path, parquet_dir = _market_timeseries_paths()
    market_db = MarketDb(str(duckdb_path), read_only=False)
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
    )
    if store is None:
        market_db.close()
        raise RuntimeError("DuckDB market time-series store is unavailable. Install duckdb and retry.")
    return market_db, store


def _close_resource(resource: object | None, *, label: str) -> None:
    if resource is None:
        return
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception as exc:  # noqa: BLE001 - reset should remain best-effort
        logger.warning("Failed to close {} during market DB reset: {}", label, exc)


def _install_market_reader_services(request: Request, duckdb_path: str) -> None:
    reader: MarketDbReader | None = None
    market_data_service: MarketDataService | None = None

    try:
        reader = MarketDbReader(duckdb_path)
        market_data_service = MarketDataService(reader)
    except Exception as exc:  # noqa: BLE001 - keep API alive with degraded read services
        logger.warning("Failed to initialize market reader after market DB reset: {}", exc)

    request.app.state.market_reader = reader
    request.app.state.market_data_service = market_data_service
    request.app.state.roe_service = create_market_roe_service(reader)
    request.app.state.margin_analytics_service = create_market_margin_analytics_service(reader)
    request.app.state.chart_service = ChartService(reader)


def _reset_market_resources(request: Request) -> tuple[MarketDb, MarketTimeSeriesStore]:
    duckdb_path, parquet_dir = _market_timeseries_paths()
    wal_path = Path(f"{duckdb_path}.wal")
    current_market_db = getattr(request.app.state, "market_db", None)
    current_store = getattr(request.app.state, "market_time_series_store", None)
    current_reader = getattr(request.app.state, "market_reader", None)

    request.app.state.market_reader = None
    request.app.state.market_data_service = None
    request.app.state.roe_service = create_market_roe_service(None)
    request.app.state.margin_analytics_service = create_market_margin_analytics_service(None)
    request.app.state.chart_service = ChartService(None)
    request.app.state.market_db = None
    request.app.state.market_time_series_store = None

    _close_resource(current_reader, label="market_reader")
    _close_resource(current_store, label="market_time_series_store")
    _close_resource(current_market_db, label="market_db")
    close_all_cached_data_access_clients()

    if duckdb_path.exists():
        duckdb_path.unlink()
    if wal_path.exists():
        wal_path.unlink()
    shutil.rmtree(parquet_dir, ignore_errors=True)

    market_db, store = _create_market_resources()
    request.app.state.market_db = market_db
    request.app.state.market_time_series_store = store
    _install_market_reader_services(request, str(duckdb_path))
    return market_db, store


def _get_market_time_series_store(request: Request) -> MarketTimeSeriesStore:
    store = getattr(request.app.state, "market_time_series_store", None)
    if store is not None:
        return store

    duckdb_path, parquet_dir = _market_timeseries_paths()
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
    )
    if store is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "DuckDB market time-series store is unavailable. "
                "Install duckdb and retry."
            ),
        )
    request.app.state.market_time_series_store = store
    return store


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
    time_series_store = _get_market_time_series_store(request)
    return db_stats_service.get_market_stats(
        market_db,
        time_series_store=time_series_store,
    )


# --- Validate ---


@router.get(
    "/api/db/validate",
    response_model=MarketValidationResponse,
    summary="Market database validation",
)
def get_db_validate(request: Request) -> MarketValidationResponse:
    market_db = _get_market_db(request)
    time_series_store = _get_market_time_series_store(request)
    return db_validation_service.validate_market_db(
        market_db,
        time_series_store=time_series_store,
    )


# --- Sync ---


def _resolve_time_series_store(
    request: Request,
    body: SyncRequest,
) -> MarketTimeSeriesStore:
    """Sync request に応じた DuckDB time-series store を解決する。"""
    default_store = _get_market_time_series_store(request)
    data_plane = body.dataPlane
    if data_plane is None:
        return default_store

    if data_plane.backend != "duckdb-parquet":
        raise HTTPException(
            status_code=422,
            detail=(
                "Unsupported dataPlane backend. "
                "Only duckdb-parquet is available."
            ),
        )
    return default_store


@router.post(
    "/api/db/sync",
    response_model=CreateSyncJobResponse,
    status_code=202,
    summary="Start database sync job",
)
async def start_sync_job(request: Request, body: SyncRequest) -> JSONResponse:
    market_db = _get_market_db(request)
    jquants_client = _get_jquants_client(request)
    time_series_store = _resolve_time_series_store(request, body)
    sync_mode = SyncMode(body.mode)
    job = await start_sync(
        sync_mode,
        market_db,
        jquants_client,
        time_series_store=time_series_store,
        close_time_series_store=False,
        enforce_bulk_for_stock_data=body.enforceBulkForStockData,
        reset_before_sync=body.resetBeforeSync,
        reset_market_snapshot=(lambda: _reset_market_resources(request)) if body.resetBeforeSync else None,
    )

    if job is None:
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


def _to_sync_job_response(job: JobInfo[SyncJobData, SyncProgress, SyncResult]) -> SyncJobResponse:
    return SyncJobResponse(
        jobId=job.job_id,
        status=job.status.value,
        mode=job.data.resolved_mode or job.data.mode.value,
        enforceBulkForStockData=job.data.enforce_bulk_for_stock_data,
        progress=job.progress,
        result=job.result,
        startedAt=(job.started_at or job.created_at).isoformat(),
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
    )


def _to_sync_fetch_details_response(job: JobInfo[SyncJobData, SyncProgress, SyncResult]) -> SyncFetchDetailsResponse:
    # Snapshot first to avoid concurrent append side effects while serializing.
    snapshot = list(job.data.fetch_details)
    items = [SyncFetchDetail.model_validate(item) for item in snapshot]
    return SyncFetchDetailsResponse(
        jobId=job.job_id,
        status=job.status.value,
        mode=job.data.resolved_mode or job.data.mode.value,
        latest=items[-1] if items else None,
        items=items,
    )


def _build_sync_stream_snapshot_payload(
    job: JobInfo[SyncJobData, SyncProgress, SyncResult],
) -> str:
    return json.dumps(
        {
            "job": _to_sync_job_response(job).model_dump(mode="json"),
            "fetchDetails": _to_sync_fetch_details_response(job).model_dump(mode="json"),
        },
        ensure_ascii=False,
    )


def _build_sync_fetch_detail_payload(
    job: JobInfo[SyncJobData, SyncProgress, SyncResult],
    event: SyncStreamEvent,
) -> str:
    return json.dumps(
        {
            "jobId": job.job_id,
            "status": job.status.value,
            "mode": job.data.resolved_mode or job.data.mode.value,
            "detail": SyncFetchDetail.model_validate(event.payload or {}).model_dump(mode="json"),
        },
        ensure_ascii=False,
    )


async def _sync_job_event_generator(job_id: str):
    queue = sync_stream_manager.subscribe(job_id)
    try:
        job = sync_job_manager.get_job(job_id)
        if job is None:
            yield {
                "event": "error",
                "data": json.dumps({"jobId": job_id, "message": f"Job {job_id} not found"}, ensure_ascii=False),
            }
            return

        yield {
            "event": "snapshot",
            "data": _build_sync_stream_snapshot_payload(job),
        }
        if job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            return

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                yield {"event": "heartbeat", "data": "{}"}
                continue

            if event is None:
                return

            latest_job = sync_job_manager.get_job(job_id)
            if latest_job is None:
                return

            if event.event == "job":
                yield {
                    "event": "job",
                    "data": _to_sync_job_response(latest_job).model_dump_json(),
                }
                if latest_job.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                    return
                continue

            if event.event == "fetch-detail":
                yield {
                    "event": "fetch-detail",
                    "data": _build_sync_fetch_detail_payload(latest_job, event),
                }
    finally:
        sync_stream_manager.unsubscribe(job_id, queue)


@router.get(
    "/api/db/sync/jobs/active",
    response_model=SyncJobResponse | None,
    summary="Get active sync job status",
)
def get_active_sync_job() -> SyncJobResponse | None:
    job = sync_job_manager.get_active_job()
    if job is None:
        return None
    return _to_sync_job_response(job)


@router.get(
    "/api/db/sync/jobs/{jobId}",
    response_model=SyncJobResponse,
    summary="Get sync job status",
)
def get_sync_job(jobId: str) -> SyncJobResponse:
    job = sync_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    return _to_sync_job_response(job)


@router.get(
    "/api/db/sync/jobs/{jobId}/fetch-details",
    response_model=SyncFetchDetailsResponse,
    summary="Get sync job fetch details",
)
def get_sync_job_fetch_details(jobId: str) -> SyncFetchDetailsResponse:
    job = sync_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    return _to_sync_fetch_details_response(job)


@router.get(
    "/api/db/sync/jobs/{jobId}/stream",
    operation_id="stream_sync_job",
    response_class=EventSourceResponse,
    responses={
        200: {
            "content": {"text/event-stream": {"schema": {"type": "string"}}},
            "description": "Sync job events stream",
        }
    },
    summary="Stream sync job events",
)
async def stream_sync_job(jobId: str) -> EventSourceResponse:
    job = sync_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    return EventSourceResponse(_sync_job_event_generator(jobId))


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
    cancelled = await sync_job_manager.cancel_job(jobId)
    if not cancelled:
        latest_job = sync_job_manager.get_job(jobId)
        latest_status = latest_job.status.value if latest_job is not None else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"Job {jobId} was already finished while cancelling (status: {latest_status})",
        )
    return CancelJobResponse(success=True, jobId=jobId, message="Job cancelled")


# --- Refresh ---


@router.post(
    "/api/db/stocks/refresh",
    response_model=RefreshResponse,
    summary="Refresh stock data for specific codes",
)
async def refresh_stocks(request: Request, body: RefreshRequest) -> RefreshResponse:
    market_db = _get_market_db(request)
    time_series_store = _get_market_time_series_store(request)
    jquants_client = _get_jquants_client(request)

    if not market_db.is_initialized():
        raise HTTPException(status_code=422, detail="Database not initialized. Please run sync first.")
    if market_db.is_legacy_stock_price_snapshot():
        raise HTTPException(
            status_code=422,
            detail=(
                "Legacy market.duckdb detected. Reset market-timeseries/market.duckdb "
                "and market-timeseries/parquet, then run initial sync."
            ),
        )

    return await stock_refresh_service.refresh_stocks(
        body.codes,
        market_db,
        time_series_store,
        jquants_client,
    )
