"""
Dataset Management Routes

GET    /api/dataset                    — データセット一覧
GET    /api/dataset/{name}/info        — データセット詳細
GET    /api/dataset/{name}/sample      — ランダムサンプル
GET    /api/dataset/{name}/search      — 銘柄検索
DELETE /api/dataset/{name}             — データセット削除
POST   /api/dataset                    — データセット作成（バックグラウンド）
POST   /api/dataset/resume             — 再開（バックグラウンド）
GET    /api/dataset/jobs/{jobId}       — ジョブ状態
DELETE /api/dataset/jobs/{jobId}       — ジョブキャンセル
"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.entrypoints.http.schemas.dataset import (
    DatasetCreateRequest,
    DatasetCreateResponse,
    DatasetInfoResponse,
    DatasetJobResponse,
    DatasetJobResult,
    DatasetListItem,
    DatasetSampleResponse,
    DatasetSearchResponse,
)
from src.entrypoints.http.schemas.job import CancelJobResponse, JobStatus
from src.application.services import dataset_service
from src.application.services.dataset_builder_service import (
    DatasetJobData,
    dataset_job_manager,
    start_dataset_build,
)
from src.application.services.dataset_presets import get_preset, list_presets
from src.application.services.dataset_resolver import DatasetResolver

router = APIRouter(tags=["Dataset"])


def _get_resolver(request: Request) -> DatasetResolver:
    resolver = getattr(request.app.state, "dataset_resolver", None)
    if resolver is None:
        raise HTTPException(status_code=422, detail="Dataset resolver not initialized")
    return resolver


def _get_jquants_client(request: Request) -> JQuantsAsyncClient:
    client = getattr(request.app.state, "jquants_client", None)
    if client is None:
        raise HTTPException(status_code=422, detail="JQuants client not initialized")
    return client


# --- List ---


@router.get(
    "/api/dataset",
    response_model=list[DatasetListItem],
    summary="List available datasets",
)
def list_datasets(request: Request) -> list[DatasetListItem]:
    resolver = _get_resolver(request)
    return dataset_service.list_datasets(resolver)


# --- Info ---


@router.get(
    "/api/dataset/{name}/info",
    response_model=DatasetInfoResponse,
    summary="Dataset detailed information",
)
def get_dataset_info(request: Request, name: str) -> DatasetInfoResponse:
    resolver = _get_resolver(request)
    result = dataset_service.get_dataset_info(resolver, name)
    if result is None:
        raise HTTPException(status_code=404, detail=f'Dataset "{name}" not found')
    return result


# --- Sample ---


@router.get(
    "/api/dataset/{name}/sample",
    response_model=DatasetSampleResponse,
    summary="Random sample of stock codes",
)
def get_dataset_sample(
    request: Request,
    name: str,
    count: int = Query(default=10, ge=1, le=100),
    seed: int | None = Query(default=None),
) -> DatasetSampleResponse:
    resolver = _get_resolver(request)
    db = resolver.resolve(name)
    if db is None:
        raise HTTPException(status_code=404, detail=f'Dataset "{name}" not found')
    return dataset_service.get_dataset_sample(db, count=count, seed=seed)


# --- Search ---


@router.get(
    "/api/dataset/{name}/search",
    response_model=DatasetSearchResponse,
    summary="Search stocks in dataset",
)
def search_dataset(
    request: Request,
    name: str,
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(default=50, ge=1, le=200),
) -> DatasetSearchResponse:
    resolver = _get_resolver(request)
    db = resolver.resolve(name)
    if db is None:
        raise HTTPException(status_code=404, detail=f'Dataset "{name}" not found')
    return dataset_service.search_dataset(db, q=q, limit=limit)


# --- Jobs (registered BEFORE /{name} routes to avoid path conflicts) ---


@router.get(
    "/api/dataset/jobs/{jobId}",
    response_model=DatasetJobResponse,
    summary="Get dataset build job status",
)
def get_dataset_job(jobId: str) -> DatasetJobResponse:
    job = dataset_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")

    progress_dict = None
    if job.progress is not None:
        progress_dict = {
            "stage": job.progress.stage,
            "current": job.progress.current,
            "total": job.progress.total,
            "percentage": job.progress.percentage,
            "message": job.progress.message,
        }

    result = None
    if job.result is not None:
        result = DatasetJobResult(
            success=job.result.success,
            totalStocks=job.result.totalStocks,
            processedStocks=job.result.processedStocks,
            warnings=job.result.warnings,
            errors=job.result.errors,
            outputPath=job.result.outputPath,
        )

    return DatasetJobResponse(
        jobId=job.job_id,
        status=job.status.value,
        preset=job.data.preset,
        name=job.data.name,
        progress=progress_dict,
        result=result,
        startedAt=(job.started_at or job.created_at).isoformat(),
        completedAt=job.completed_at.isoformat() if job.completed_at else None,
        error=job.error,
    )


@router.delete(
    "/api/dataset/jobs/{jobId}",
    response_model=CancelJobResponse,
    summary="Cancel dataset build job",
)
async def cancel_dataset_job(jobId: str) -> CancelJobResponse:
    job = dataset_job_manager.get_job(jobId)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {jobId} not found")
    if job.status not in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(
            status_code=400,
            detail=f"Job {jobId} cannot be cancelled (status: {job.status.value})",
        )
    await dataset_job_manager.cancel_job(jobId)
    return CancelJobResponse(success=True, jobId=jobId, message="Job cancelled")


# --- Create ---


def _estimate_time(preset_name: str) -> str:
    """プリセットに基づいて推定時間を返す"""
    if preset_name == "quickTesting":
        return "1-2 minutes"
    if preset_name in ("topix100", "mid400"):
        return "5-15 minutes"
    if preset_name in ("topix500", "primeMarket"):
        return "10-30 minutes"
    return "15-35 minutes"


@router.post(
    "/api/dataset",
    response_model=DatasetCreateResponse,
    status_code=202,
    summary="Create a new dataset (background job)",
)
async def create_dataset(request: Request, body: DatasetCreateRequest) -> JSONResponse:
    resolver = _get_resolver(request)
    jquants_client = _get_jquants_client(request)

    # Validate preset
    if get_preset(body.preset) is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown preset: {body.preset}. Available: {', '.join(list_presets())}",
        )

    # Check existing dataset
    name_stem = body.name.removesuffix(".db")
    db_path = resolver.get_db_path(f"{name_stem}.db")

    if os.path.exists(db_path) and not body.overwrite:
        raise HTTPException(
            status_code=409,
            detail=f'Dataset "{name_stem}" already exists. Use overwrite=true to replace.',
        )

    data = DatasetJobData(
        name=name_stem,
        preset=body.preset,
        overwrite=body.overwrite,
        timeout_minutes=body.timeoutMinutes,
    )
    job = await start_dataset_build(data, resolver, jquants_client)
    if job is None:
        raise HTTPException(status_code=409, detail="Another dataset build job is already running")

    return JSONResponse(
        status_code=202,
        content=DatasetCreateResponse(
            jobId=job.job_id,
            status="pending",
            name=name_stem,
            preset=body.preset,
            message="Dataset creation job started",
            estimatedTime=_estimate_time(body.preset),
        ).model_dump(),
    )


# --- Resume ---


@router.post(
    "/api/dataset/resume",
    response_model=DatasetCreateResponse,
    status_code=202,
    summary="Resume an incomplete dataset build",
)
async def resume_dataset(request: Request, body: DatasetCreateRequest) -> JSONResponse:
    resolver = _get_resolver(request)
    jquants_client = _get_jquants_client(request)

    # Validate preset
    if get_preset(body.preset) is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown preset: {body.preset}. Available: {', '.join(list_presets())}",
        )

    # Check dataset exists
    name_stem = body.name.removesuffix(".db")
    db_path = resolver.get_db_path(f"{name_stem}.db")

    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f'Dataset "{name_stem}" not found')

    data = DatasetJobData(
        name=name_stem,
        preset=body.preset,
        resume=True,
        timeout_minutes=body.timeoutMinutes,
    )
    job = await start_dataset_build(data, resolver, jquants_client)
    if job is None:
        raise HTTPException(status_code=409, detail="Another dataset build job is already running")

    return JSONResponse(
        status_code=202,
        content=DatasetCreateResponse(
            jobId=job.job_id,
            status="pending",
            name=name_stem,
            preset=body.preset,
            message="Dataset resume job started",
            estimatedTime="Depends on missing data",
        ).model_dump(),
    )


# --- Delete ---


@router.delete(
    "/api/dataset/{name}",
    summary="Delete a dataset",
)
def delete_dataset(request: Request, name: str) -> dict[str, object]:
    resolver = _get_resolver(request)
    if not dataset_service.delete_dataset(resolver, name):
        raise HTTPException(status_code=404, detail=f'Dataset "{name}" not found')
    return {"success": True, "message": f'Dataset "{name}" deleted'}
