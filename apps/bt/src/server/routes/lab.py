"""
Lab API Endpoints

戦略自動生成・GA進化・Optuna最適化・戦略改善
"""

from collections.abc import Awaitable, Callable
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from src.server.schemas.lab import (
    LabEvolveRequest,
    LabEvolveResult,
    LabGenerateRequest,
    LabGenerateResult,
    LabImproveRequest,
    LabImproveResult,
    LabJobResponse,
    LabOptimizeRequest,
    LabOptimizeResult,
    LabResultData,
)
from src.server.services.job_manager import JobInfo, job_manager
from src.server.services.lab_service import lab_service
from src.server.services.sse_manager import sse_manager

router = APIRouter(tags=["Lab"])

LabType = Literal["generate", "evolve", "optimize", "improve"]

_LAB_TYPE_MAP: dict[str, LabType] = {
    "lab_generate": "generate",
    "lab_evolve": "evolve",
    "lab_optimize": "optimize",
    "lab_improve": "improve",
}

_RESULT_CLASS_MAP: dict[str, type[LabResultData]] = {
    "generate": LabGenerateResult,
    "evolve": LabEvolveResult,
    "optimize": LabOptimizeResult,
    "improve": LabImproveResult,
}


def _get_lab_job_or_404(job_id: str) -> JobInfo:
    """Labジョブを取得、存在しなければ404"""
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ジョブが見つかりません: {job_id}")
    if job.job_type not in _LAB_TYPE_MAP:
        raise HTTPException(status_code=400, detail=f"Labジョブではありません: {job.job_type}")
    return job


def _build_lab_job_response(job: JobInfo) -> LabJobResponse:
    """JobInfoからLabJobResponseを構築"""
    lab_type = _LAB_TYPE_MAP.get(job.job_type)

    result_data = None
    if job.raw_result and lab_type:
        result_cls = _RESULT_CLASS_MAP.get(lab_type)
        if result_cls:
            try:
                result_data = result_cls.model_validate(job.raw_result)
            except Exception as e:
                logger.warning(f"Lab結果のパースに失敗: {e}")

    return LabJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        message=job.message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        lab_type=lab_type,
        strategy_name=job.strategy_name,
        result_data=result_data,
    )


async def _submit_and_respond(
    submit_fn: Callable[..., Awaitable[str]],
    submit_kwargs: dict[str, Any],
    error_label: str,
) -> LabJobResponse:
    """共通のサブミット処理: ジョブ作成 -> レスポンス構築"""
    try:
        job_id = await submit_fn(**submit_kwargs)
        job = _get_lab_job_or_404(job_id)
        return _build_lab_job_response(job)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Lab {error_label} サブミットエラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


# ============================================
# Job Submission Endpoints
# ============================================


@router.post("/api/lab/generate", response_model=LabJobResponse)
async def run_lab_generate(request: LabGenerateRequest) -> LabJobResponse:
    """戦略自動生成ジョブをサブミット"""
    return await _submit_and_respond(
        lab_service.submit_generate,
        {
            "count": request.count,
            "top": request.top,
            "seed": request.seed,
            "save": request.save,
            "direction": request.direction,
            "timeframe": request.timeframe,
            "dataset": request.dataset,
            "entry_filter_only": request.entry_filter_only,
            "allowed_categories": request.allowed_categories,
        },
        error_label="generate",
    )


@router.post("/api/lab/evolve", response_model=LabJobResponse)
async def run_lab_evolve(request: LabEvolveRequest) -> LabJobResponse:
    """GA進化ジョブをサブミット"""
    return await _submit_and_respond(
        lab_service.submit_evolve,
        {
            "strategy_name": request.strategy_name,
            "generations": request.generations,
            "population": request.population,
            "structure_mode": request.structure_mode,
            "random_add_entry_signals": request.random_add_entry_signals,
            "random_add_exit_signals": request.random_add_exit_signals,
            "seed": request.seed,
            "save": request.save,
            "entry_filter_only": request.entry_filter_only,
            "allowed_categories": request.allowed_categories,
        },
        error_label="evolve",
    )


@router.post("/api/lab/optimize", response_model=LabJobResponse)
async def run_lab_optimize(request: LabOptimizeRequest) -> LabJobResponse:
    """Optuna最適化ジョブをサブミット"""
    return await _submit_and_respond(
        lab_service.submit_optimize,
        {
            "strategy_name": request.strategy_name,
            "trials": request.trials,
            "sampler": request.sampler,
            "structure_mode": request.structure_mode,
            "random_add_entry_signals": request.random_add_entry_signals,
            "random_add_exit_signals": request.random_add_exit_signals,
            "seed": request.seed,
            "save": request.save,
            "entry_filter_only": request.entry_filter_only,
            "allowed_categories": request.allowed_categories,
            "scoring_weights": request.scoring_weights,
        },
        error_label="optimize",
    )


@router.post("/api/lab/improve", response_model=LabJobResponse)
async def run_lab_improve(request: LabImproveRequest) -> LabJobResponse:
    """戦略改善ジョブをサブミット"""
    return await _submit_and_respond(
        lab_service.submit_improve,
        {
            "strategy_name": request.strategy_name,
            "auto_apply": request.auto_apply,
            "entry_filter_only": request.entry_filter_only,
            "allowed_categories": request.allowed_categories,
        },
        error_label="improve",
    )


# ============================================
# Job Management Endpoints
# ============================================


@router.get("/api/lab/jobs/{job_id}", response_model=LabJobResponse)
async def get_lab_job_status(job_id: str) -> LabJobResponse:
    """Labジョブのステータスを取得"""
    job = _get_lab_job_or_404(job_id)
    return _build_lab_job_response(job)


@router.get("/api/lab/jobs/{job_id}/stream")
async def stream_lab_job_events(job_id: str) -> EventSourceResponse:
    """LabジョブのSSEストリーミング"""
    _get_lab_job_or_404(job_id)
    return EventSourceResponse(sse_manager.job_event_generator(job_id))


@router.post("/api/lab/jobs/{job_id}/cancel", response_model=LabJobResponse)
async def cancel_lab_job(job_id: str) -> LabJobResponse:
    """Labジョブをキャンセル"""
    _get_lab_job_or_404(job_id)

    cancelled_job = await job_manager.cancel_job(job_id)
    if cancelled_job is None:
        job = job_manager.get_job(job_id)
        status = job.status if job else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"ジョブは既に終了しています（状態: {status}）",
        )

    return _build_lab_job_response(cancelled_job)
