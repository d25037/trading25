"""
Database Routes

GET    /api/db/stats                 — DB 統計
GET    /api/db/validate              — DB 検証
POST   /api/db/sync                  — Sync 開始
GET    /api/db/sync/jobs/active      — 実行中 Sync ジョブ状態
GET    /api/db/sync/jobs/{jobId}     — Sync ジョブ状態
GET    /api/db/sync/jobs/{jobId}/stream — Sync SSE stream
DELETE /api/db/sync/jobs/{jobId}     — Sync ジョブキャンセル
POST   /api/db/adjusted-metrics/materialize — adjusted_metrics_pit recovery job 開始
POST   /api/db/intraday/sync         — Intraday minute data sync
POST   /api/db/stocks/refresh        — 銘柄データ再取得
"""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from src.application.contracts.jobs import JobStatus
from src.application.contracts.market_maintenance import (
    MaintenanceOutcome,
    MarketMaintenanceRecord,
    MarketOperationOutcome,
)
from src.application.services.market_maintenance_finalizer import (
    MarketFinalizationDecision,
    MarketMaintenanceFinalizer,
    finalize_market_operation_joined,
)
from src.application.services.chart_service import ChartService
from src.application.services.margin_analytics_service import (
    create_market_margin_analytics_service,
)
from src.application.services.market_data_service import MarketDataService
from src.application.services.roe_service import create_market_roe_service
from src.shared.config.settings import get_settings
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.market_operation_lease import (
    MarketOperationLease,
    MarketOperationLeaseError,
)
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    MarketWriterSession,
)
from src.infrastructure.db.market.time_series_store import (
    MarketTimeSeriesStore,
    create_time_series_store,
)
from src.entrypoints.http.schemas.db import (
    AdjustedMetricsMaterializeJobResponse,
    AdjustedMetricsMaterializeResult,
    CreateSyncJobResponse,
    CreateAdjustedMetricsMaterializeJobResponse,
    IntradaySyncRequest,
    IntradaySyncResponse,
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
from src.entrypoints.http.schemas.job import CancelJobResponse
from src.application.services import (
    db_stats_service,
    db_validation_service,
    intraday_sync_service,
    stock_refresh_service,
)
from src.application.services.generic_job_manager import JobInfo
from src.application.services.sync_service import (
    AdjustedMetricsMaterializeJobData,
    SyncJobData,
    SyncMode,
    adjusted_metrics_materialize_job_manager,
    start_adjusted_metrics_materialization,
    sync_job_manager,
    start_sync,
)
from src.application.services.sync_stream_manager import (
    SyncStreamEvent,
    sync_stream_manager,
)
from src.infrastructure.data_access.clients import close_all_cached_data_access_clients

router = APIRouter(tags=["Database"])
_MARKET_RESOURCE_LOCK = threading.RLock()


def _get_market_db(request: Request) -> MarketDb:
    market_db = getattr(request.app.state, "market_db", None)
    if market_db is None:
        raise HTTPException(
            status_code=422, detail="Database not initialized. Please run sync first."
        )
    return market_db


def _market_timeseries_paths() -> tuple[Path, Path]:
    settings = get_settings()
    timeseries_base = Path(settings.market_timeseries_dir)
    return timeseries_base / "market.duckdb", timeseries_base / "parquet"


def _remember_market_paths(request: Request) -> tuple[Path, Path]:
    market_db = getattr(request.app.state, "market_db", None)
    db_path = getattr(market_db, "db_path", None)
    if isinstance(db_path, str) and db_path.strip():
        duckdb_path = Path(db_path)
        parquet_dir = duckdb_path.parent / "parquet"
    else:
        duckdb_path, parquet_dir = _market_timeseries_paths()
    request.app.state.market_duckdb_path = str(duckdb_path)
    request.app.state.market_parquet_dir = str(parquet_dir)
    return duckdb_path, parquet_dir


def _remembered_market_paths(request: Request) -> tuple[Path, Path]:
    duckdb_path = getattr(request.app.state, "market_duckdb_path", None)
    parquet_dir = getattr(request.app.state, "market_parquet_dir", None)
    if isinstance(duckdb_path, str) and duckdb_path.strip():
        resolved_duckdb_path = Path(duckdb_path)
        if isinstance(parquet_dir, str) and parquet_dir.strip():
            return resolved_duckdb_path, Path(parquet_dir)
        return resolved_duckdb_path, resolved_duckdb_path.parent / "parquet"
    return _market_timeseries_paths()


def _create_market_resources(
    *,
    read_only: bool = True,
    duckdb_path: Path | None = None,
    parquet_dir: Path | None = None,
) -> tuple[MarketDb, MarketTimeSeriesStore]:
    if duckdb_path is None or parquet_dir is None:
        duckdb_path, parquet_dir = _market_timeseries_paths()
    if not read_only:
        raise PermissionError(
            "Writable Market resources require MarketWriterResourceFactory"
        )
    resources = MarketWriterResourceFactory(
        data_root=duckdb_path.parent.parent,
        market_root=duckdb_path.parent,
    ).read_only_factory.open_existing()
    return resources.market_db, resources.time_series_store


def _writer_factory(duckdb_path: Path) -> MarketWriterResourceFactory:
    return MarketWriterResourceFactory(
        data_root=duckdb_path.parent.parent,
        market_root=duckdb_path.parent,
    )


def _shared_operation_lease(request: Request) -> MarketOperationLease | None:
    return getattr(request.app.state, "market_operation_lease", None)


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


def _clear_market_resources(request: Request) -> None:
    with _MARKET_RESOURCE_LOCK:
        current_market_db = getattr(request.app.state, "market_db", None)
        current_store = getattr(request.app.state, "market_time_series_store", None)
        current_reader = getattr(request.app.state, "market_reader", None)

        request.app.state.market_reader = None
        request.app.state.market_data_service = None
        request.app.state.roe_service = create_market_roe_service(None)
        request.app.state.margin_analytics_service = (
            create_market_margin_analytics_service(None)
        )
        request.app.state.chart_service = ChartService(None)
        request.app.state.market_db = None
        request.app.state.market_time_series_store = None

        _close_resource(current_reader, label="market_reader")
        _close_resource(current_store, label="market_time_series_store")
        _close_resource(current_market_db, label="market_db")
        close_all_cached_data_access_clients()


def _install_market_reader_services(request: Request, duckdb_path: str) -> None:
    reader: MarketDbReader | None = None
    market_data_service: MarketDataService | None = None

    try:
        reader = MarketDbReader(duckdb_path)
        market_data_service = MarketDataService(reader)
    except Exception as exc:  # noqa: BLE001 - keep API alive with degraded read services
        logger.warning(
            "Failed to initialize market reader after market DB reset: {}", exc
        )

    request.app.state.market_reader = reader
    request.app.state.market_data_service = market_data_service
    request.app.state.roe_service = create_market_roe_service(reader)
    request.app.state.margin_analytics_service = create_market_margin_analytics_service(
        reader
    )
    request.app.state.chart_service = ChartService(reader)


def _attach_finalized_market_resources(
    app: object,
    owner: object,
    resources: object,
    evidence: MarketMaintenanceRecord,
) -> None:
    """Install one verified generation while the writer lease is still exclusive."""
    with _MARKET_RESOURCE_LOCK:
        state = getattr(app, "state")
        if getattr(state, "market_writer_owner", None) is not owner:
            raise RuntimeError("Market writer finalizer ownership changed")
        reader: MarketDbReader | None = None
        try:
            identity = getattr(resources, "identity")
            reader = MarketDbReader(str(identity.path))
            market_data_service = MarketDataService(reader)
            roe_service = create_market_roe_service(reader)
            margin_service = create_market_margin_analytics_service(reader)
            chart_service = ChartService(reader)
        except BaseException as construction_error:
            state.market_db = None
            state.market_time_series_store = None
            state.market_reader = None
            state.market_data_service = None
            cleanup_errors: list[BaseException] = []
            if reader is not None:
                try:
                    reader.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            close = getattr(resources, "close", None)
            if callable(close):
                try:
                    close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            for cleanup_error in cleanup_errors:
                construction_error.add_note(
                    f"Finalized resource cleanup failed: {cleanup_error}"
                )
            if cleanup_errors:
                writer_session = getattr(state, "market_writer_session", None)
                if isinstance(writer_session, MarketWriterSession):
                    writer_session.fenced = True
            raise construction_error

        state.market_db = getattr(resources, "market_db")
        state.market_time_series_store = getattr(resources, "time_series_store")
        state.market_reader = reader
        state.market_data_service = market_data_service
        state.roe_service = roe_service
        state.margin_analytics_service = margin_service
        state.chart_service = chart_service
        state.market_maintenance = evidence
        close_all_cached_data_access_clients()


def _release_finalized_market_ownership(
    app: object,
    owner: object,
    session: MarketWriterSession,
) -> None:
    """Forget writer ownership only after terminal publication and lease release."""
    with _MARKET_RESOURCE_LOCK:
        state = getattr(app, "state")
        if (
            getattr(state, "market_writer_owner", None) is owner
            and getattr(state, "market_writer_session", None) is session
        ):
            state.market_writer_session = None
            state.market_writer_owner = None


def _build_market_finalizer(
    request: Request, operation: str
) -> MarketMaintenanceFinalizer:
    with _MARKET_RESOURCE_LOCK:
        session = getattr(request.app.state, "market_writer_session", None)
        owner = getattr(request.app.state, "market_writer_owner", None)
        if not isinstance(session, MarketWriterSession) or owner is None:
            raise RuntimeError("Market writer session is missing at finalization")
        app = request.app
    return MarketMaintenanceFinalizer(
        session=session,
        operation=operation,
        attach=lambda resources, evidence: _attach_finalized_market_resources(
            app,
            owner,
            resources,
            evidence,
        ),
        release_complete=lambda: _release_finalized_market_ownership(
            app,
            owner,
            session,
        ),
    )


async def _finalize_direct_market_write(
    request: Request,
    *,
    operation: str,
    operation_outcome: MarketOperationOutcome,
    operation_error: str | None = None,
) -> MarketFinalizationDecision:
    decision: list[MarketFinalizationDecision] = []
    finalizer = _build_market_finalizer(request, operation)

    def replace_terminal(updated: MarketFinalizationDecision) -> None:
        decision[:] = [updated]
        with _MARKET_RESOURCE_LOCK:
            request.app.state.market_maintenance = updated.maintenance

    await finalize_market_operation_joined(
        finalizer,
        operation_outcome=operation_outcome,
        operation_error=operation_error,
        publish_terminal=decision.append,
        replace_terminal=replace_terminal,
    )
    if not decision:
        raise RuntimeError("Market finalizer did not publish a terminal decision")
    return decision[0]


def _restore_unreserved_read_only_resources(request: Request) -> None:
    with _MARKET_RESOURCE_LOCK:
        if isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            raise RuntimeError("Reserved Market writer requires the common finalizer")
        duckdb_path, parquet_dir = _remembered_market_paths(request)
        _clear_market_resources(request)
        if not duckdb_path.exists():
            return
        market_db, store = _create_market_resources(
            read_only=True,
            duckdb_path=duckdb_path,
            parquet_dir=parquet_dir,
        )
        request.app.state.market_db = market_db
        request.app.state.market_time_series_store = store
        _install_market_reader_services(request, str(duckdb_path))


def _prepare_market_write_resources(
    request: Request,
) -> tuple[MarketDb, MarketTimeSeriesStore]:
    with _MARKET_RESOURCE_LOCK:
        if isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            raise HTTPException(
                status_code=409, detail="Another Market write is already running"
            )
        duckdb_path, _parquet_dir = _remember_market_paths(request)
        _clear_market_resources(request)
        try:
            session = _writer_factory(duckdb_path).open_existing(
                blocking=False,
                lease=_shared_operation_lease(request),
            )
        except MarketOperationLeaseError as exc:
            _restore_unreserved_read_only_resources(request)
            raise HTTPException(
                status_code=409,
                detail="Another Market write is already running",
            ) from exc
        except BaseException:
            _restore_unreserved_read_only_resources(request)
            raise
        owner = object()
        request.state.market_writer_owner = owner
        request.app.state.market_writer_owner = owner
        request.app.state.market_writer_session = session
        request.app.state.market_db = session.handles.market_db
        request.app.state.market_time_series_store = session.handles.time_series_store
        return session.handles.market_db, session.handles.time_series_store


def _reset_market_resources(request: Request) -> tuple[MarketDb, MarketTimeSeriesStore]:
    with _MARKET_RESOURCE_LOCK:
        active = getattr(request.app.state, "market_writer_session", None)
        if isinstance(active, MarketWriterSession):
            raise HTTPException(
                status_code=409, detail="Another Market write is already running"
            )
        duckdb_path, _parquet_dir = _market_timeseries_paths()
        _clear_market_resources(request)
        try:
            session = _writer_factory(duckdb_path).reset_and_open_v4(
                blocking=False,
                lease=_shared_operation_lease(request),
            )
        except MarketOperationLeaseError as exc:
            _restore_unreserved_read_only_resources(request)
            raise HTTPException(
                status_code=409,
                detail="Another Market write is already running",
            ) from exc
        except BaseException:
            _restore_unreserved_read_only_resources(request)
            raise
        new_owner = object()
        request.state.market_writer_owner = new_owner
        request.app.state.market_writer_owner = new_owner
        request.app.state.market_writer_session = session
        request.app.state.market_db = session.handles.market_db
        request.app.state.market_time_series_store = session.handles.time_series_store
        return session.handles.market_db, session.handles.time_series_store


def _get_market_time_series_store(request: Request) -> MarketTimeSeriesStore:
    store = getattr(request.app.state, "market_time_series_store", None)
    if store is not None:
        return store

    duckdb_path, parquet_dir = _market_timeseries_paths()
    store = create_time_series_store(
        backend="duckdb-parquet",
        duckdb_path=str(duckdb_path),
        parquet_dir=str(parquet_dir),
        read_only=True,
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
    with _MARKET_RESOURCE_LOCK:
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
    with _MARKET_RESOURCE_LOCK:
        market_db = _get_market_db(request)
        time_series_store = _get_market_time_series_store(request)
        return db_validation_service.validate_market_db(
            market_db,
            time_series_store=time_series_store,
        )


# --- Sync ---


def _job_maintenance(data: object) -> MarketMaintenanceRecord:
    value = getattr(data, "maintenance", None)
    return (
        value
        if isinstance(value, MarketMaintenanceRecord)
        else MarketMaintenanceRecord.never_run()
    )


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
            detail=("Unsupported dataPlane backend. Only duckdb-parquet is available."),
        )
    return default_store


@router.post(
    "/api/db/sync",
    response_model=CreateSyncJobResponse,
    status_code=202,
    summary="Start database sync job",
)
async def start_sync_job(request: Request, body: SyncRequest) -> JSONResponse:
    if adjusted_metrics_materialize_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409,
            detail="Adjusted metrics materialization is already running",
        )
    jquants_client = _get_jquants_client(request)
    sync_mode = SyncMode(body.mode)
    market_db: MarketDb
    time_series_store: MarketTimeSeriesStore

    if body.resetBeforeSync:
        market_db = _get_market_db(request)
        time_series_store = _resolve_time_series_store(request, body)
    else:
        if body.dataPlane is not None and body.dataPlane.backend != "duckdb-parquet":
            raise HTTPException(
                status_code=422,
                detail=(
                    "Unsupported dataPlane backend. Only duckdb-parquet is available."
                ),
            )
        try:
            market_db, time_series_store = _prepare_market_write_resources(request)
        except RuntimeError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        job = await start_sync(
            sync_mode,
            market_db,
            jquants_client,
            time_series_store=time_series_store,
            enforce_bulk_for_stock_data=body.enforceBulkForStockData,
            reset_before_sync=body.resetBeforeSync,
            reset_market_snapshot=(lambda: _reset_market_resources(request))
            if body.resetBeforeSync
            else None,
            market_finalizer=lambda: _build_market_finalizer(
                request,
                f"{sync_mode.value}_sync",
            ),
        )
    except asyncio.CancelledError:
        if not body.resetBeforeSync and isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            await _finalize_direct_market_write(
                request,
                operation=f"{sync_mode.value}_sync",
                operation_outcome=MarketOperationOutcome.CANCELLED,
                operation_error="Request cancelled while creating Market job",
            )
        raise
    except Exception as exc:
        if not body.resetBeforeSync and isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            await _finalize_direct_market_write(
                request,
                operation=f"{sync_mode.value}_sync",
                operation_outcome=MarketOperationOutcome.FAILED,
                operation_error=str(exc),
            )
        raise

    if job is None:
        if not body.resetBeforeSync:
            await _finalize_direct_market_write(
                request,
                operation=f"{sync_mode.value}_sync",
                operation_outcome=MarketOperationOutcome.FAILED,
                operation_error="Another sync job is already running",
            )
        raise HTTPException(
            status_code=409, detail="Another sync job is already running"
        )

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


def _to_sync_job_response(
    job: JobInfo[SyncJobData, SyncProgress, SyncResult],
) -> SyncJobResponse:
    return SyncJobResponse(
        jobId=job.job_id,
        status=job.status.value,
        mode=job.data.resolved_mode or job.data.mode.value,
        enforceBulkForStockData=job.data.enforce_bulk_for_stock_data,
        maintenance=_job_maintenance(job.data),
        progress=job.progress,
        result=job.result,
        startedAt=(job.started_at or job.created_at).isoformat(),
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
    )


def _to_sync_fetch_details_response(
    job: JobInfo[SyncJobData, SyncProgress, SyncResult],
) -> SyncFetchDetailsResponse:
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
            "fetchDetails": _to_sync_fetch_details_response(job).model_dump(
                mode="json"
            ),
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
            "detail": SyncFetchDetail.model_validate(event.payload or {}).model_dump(
                mode="json"
            ),
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
                "data": json.dumps(
                    {"jobId": job_id, "message": f"Job {job_id} not found"},
                    ensure_ascii=False,
                ),
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
                if latest_job.status in (
                    JobStatus.COMPLETED,
                    JobStatus.FAILED,
                    JobStatus.CANCELLED,
                ):
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
        raise HTTPException(
            status_code=400,
            detail=f"Job {jobId} cannot be cancelled (status: {job.status.value})",
        )
    cancelled = await sync_job_manager.cancel_job(jobId)
    if not cancelled:
        latest_job = sync_job_manager.get_job(jobId)
        latest_status = latest_job.status.value if latest_job is not None else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"Job {jobId} was already finished while cancelling (status: {latest_status})",
        )
    return CancelJobResponse(success=True, jobId=jobId, message="Job cancelled")


def _to_adjusted_metrics_materialize_job_response(
    job: JobInfo[
        AdjustedMetricsMaterializeJobData,
        SyncProgress,
        AdjustedMetricsMaterializeResult,
    ],
) -> AdjustedMetricsMaterializeJobResponse:
    return AdjustedMetricsMaterializeJobResponse(
        jobId=job.job_id,
        status=job.status.value,
        mode=job.data.mode,
        maintenance=_job_maintenance(job.data),
        progress=job.progress,
        result=job.result,
        startedAt=(job.started_at or job.created_at).isoformat(),
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
    )


@router.post(
    "/api/db/adjusted-metrics/materialize",
    response_model=CreateAdjustedMetricsMaterializeJobResponse,
    status_code=202,
    summary="Start event-time PIT adjusted metrics materialization job",
)
async def start_adjusted_metrics_materialize_job(request: Request) -> JSONResponse:
    if sync_job_manager.get_active_job() is not None:
        raise HTTPException(status_code=409, detail="Database sync is already running")
    if adjusted_metrics_materialize_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409,
            detail="Adjusted metrics materialization is already running",
        )

    try:
        market_db, _time_series_store = _prepare_market_write_resources(request)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    try:
        job = await start_adjusted_metrics_materialization(
            market_db,
            market_finalizer=lambda: _build_market_finalizer(
                request,
                "adjusted_metrics_materialization",
            ),
        )
    except asyncio.CancelledError:
        if isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            await _finalize_direct_market_write(
                request,
                operation="adjusted_metrics_materialization",
                operation_outcome=MarketOperationOutcome.CANCELLED,
                operation_error="Request cancelled while creating Market job",
            )
        raise
    except Exception as exc:
        if isinstance(
            getattr(request.app.state, "market_writer_session", None),
            MarketWriterSession,
        ):
            await _finalize_direct_market_write(
                request,
                operation="adjusted_metrics_materialization",
                operation_outcome=MarketOperationOutcome.FAILED,
                operation_error=str(exc),
            )
        raise

    if job is None:
        await _finalize_direct_market_write(
            request,
            operation="adjusted_metrics_materialization",
            operation_outcome=MarketOperationOutcome.FAILED,
            operation_error="Adjusted metrics materialization is already running",
        )
        raise HTTPException(
            status_code=409,
            detail="Adjusted metrics materialization is already running",
        )

    return JSONResponse(
        status_code=202,
        content=CreateAdjustedMetricsMaterializeJobResponse(
            jobId=job.job_id,
            status="pending",
            mode=job.data.mode,
        ).model_dump(),
    )


@router.get(
    "/api/db/adjusted-metrics/materialize/jobs/active",
    response_model=AdjustedMetricsMaterializeJobResponse | None,
    summary="Get active adjusted metrics materialization job status",
)
def get_active_adjusted_metrics_materialize_job() -> (
    AdjustedMetricsMaterializeJobResponse | None
):
    job = adjusted_metrics_materialize_job_manager.get_active_job()
    if job is None:
        return None
    return _to_adjusted_metrics_materialize_job_response(job)


@router.get(
    "/api/db/adjusted-metrics/materialize/jobs/{jobId}",
    response_model=AdjustedMetricsMaterializeJobResponse,
    summary="Get adjusted metrics materialization job status",
)
def get_adjusted_metrics_materialize_job(
    jobId: str,
) -> AdjustedMetricsMaterializeJobResponse:
    job = adjusted_metrics_materialize_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    return _to_adjusted_metrics_materialize_job_response(job)


@router.delete(
    "/api/db/adjusted-metrics/materialize/jobs/{jobId}",
    response_model=CancelJobResponse,
    summary="Cancel adjusted metrics materialization job",
)
async def cancel_adjusted_metrics_materialize_job(jobId: str) -> CancelJobResponse:
    job = adjusted_metrics_materialize_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(
            status_code=400,
            detail=(f"Job {jobId} cannot be cancelled (status: {job.status.value})"),
        )
    cancelled = await adjusted_metrics_materialize_job_manager.cancel_job(
        jobId,
        wait=True,
    )
    if not cancelled:
        latest_job = adjusted_metrics_materialize_job_manager.get_job(jobId)
        latest_status = latest_job.status.value if latest_job is not None else "unknown"
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job {jobId} was already finished while cancelling "
                f"(status: {latest_status})"
            ),
        )
    return CancelJobResponse(success=True, jobId=jobId, message="Job cancelled")


# --- Refresh ---


@router.post(
    "/api/db/intraday/sync",
    response_model=IntradaySyncResponse,
    summary="Sync intraday minute bars into local DuckDB",
)
async def sync_intraday(
    request: Request, body: IntradaySyncRequest
) -> IntradaySyncResponse:
    if sync_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409, detail="Another sync job is already running"
        )
    if adjusted_metrics_materialize_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409,
            detail="Adjusted metrics materialization is already running",
        )
    _get_market_db(request)
    jquants_client = _get_jquants_client(request)
    market_db, time_series_store = _prepare_market_write_resources(request)
    result: IntradaySyncResponse | None = None
    operation_error: BaseException | None = None
    try:
        result = await intraday_sync_service.sync_intraday_data(
            body,
            market_db=market_db,
            time_series_store=time_series_store,
            jquants_client=jquants_client,
        )
    except BaseException as exc:
        operation_error = exc
    decision = await _finalize_direct_market_write(
        request,
        operation="intraday_sync",
        operation_outcome=(
            MarketOperationOutcome.CANCELLED
            if isinstance(operation_error, asyncio.CancelledError)
            else MarketOperationOutcome.FAILED
            if operation_error is not None
            else MarketOperationOutcome.SUCCEEDED
        ),
        operation_error=str(operation_error) if operation_error is not None else None,
    )
    if decision.maintenance.outcome is MaintenanceOutcome.FAILED:
        raise HTTPException(
            status_code=503,
            detail=(
                f"{decision.error}. Published data remains available; run "
                f"{decision.maintenance.recoveryCommand}."
            ),
        )
    if operation_error is not None:
        raise operation_error
    if result is None:
        raise RuntimeError("Intraday sync completed without a result")
    return result.model_copy(update={"maintenance": decision.maintenance})


@router.post(
    "/api/db/stocks/refresh",
    response_model=RefreshResponse,
    summary="Refresh stock data for specific codes",
)
async def refresh_stocks(request: Request, body: RefreshRequest) -> RefreshResponse:
    if sync_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409, detail="Another sync job is already running"
        )
    if adjusted_metrics_materialize_job_manager.get_active_job() is not None:
        raise HTTPException(
            status_code=409,
            detail="Adjusted metrics materialization is already running",
        )
    _get_market_db(request)
    jquants_client = _get_jquants_client(request)

    market_db, time_series_store = _prepare_market_write_resources(request)
    result: RefreshResponse | None = None
    operation_error: BaseException | None = None
    try:
        if not market_db.is_initialized():
            raise HTTPException(
                status_code=422,
                detail="Database not initialized. Please run sync first.",
            )
        if market_db.is_legacy_stock_price_snapshot():
            raise HTTPException(
                status_code=422,
                detail=(
                    "Legacy market.duckdb detected. Reset market-timeseries/market.duckdb "
                    "and market-timeseries/parquet, then run initial sync."
                ),
            )

        result = await stock_refresh_service.refresh_stocks(
            body.codes,
            market_db,
            time_series_store,
            jquants_client,
        )
    except BaseException as exc:
        operation_error = exc
    decision = await _finalize_direct_market_write(
        request,
        operation="stock_refresh",
        operation_outcome=(
            MarketOperationOutcome.CANCELLED
            if isinstance(operation_error, asyncio.CancelledError)
            else MarketOperationOutcome.FAILED
            if operation_error is not None
            else MarketOperationOutcome.SUCCEEDED
        ),
        operation_error=str(operation_error) if operation_error is not None else None,
    )
    if decision.maintenance.outcome is MaintenanceOutcome.FAILED:
        raise HTTPException(
            status_code=503,
            detail=(
                f"{decision.error}. Published data remains available; run "
                f"{decision.maintenance.recoveryCommand}."
            ),
        )
    if operation_error is not None:
        raise operation_error
    if result is None:
        raise RuntimeError("Stock refresh completed without a result")
    return result.model_copy(update={"maintenance": decision.maintenance})
