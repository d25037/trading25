"""
Backtest Execution Endpoints
"""

import base64
from pathlib import Path

from fastapi import APIRouter, HTTPException
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from src.paths.resolver import get_backtest_attribution_dir, get_backtest_results_dir
from src.server.routes.attribution_file_utils import (
    list_attribution_files_in_dir,
    read_attribution_file,
)
from src.server.routes.html_file_utils import (
    delete_html_file,
    list_html_files_in_dir,
    read_html_file,
    rename_html_file,
)
from src.server.schemas.backtest import (
    AttributionArtifactContentResponse,
    AttributionArtifactInfo,
    AttributionArtifactListResponse,
    BacktestJobResponse,
    BacktestRequest,
    BacktestResultResponse,
    HtmlFileContentResponse,
    HtmlFileDeleteResponse,
    HtmlFileInfo,
    HtmlFileListResponse,
    HtmlFileMetrics,
    HtmlFileRenameRequest,
    HtmlFileRenameResponse,
    JobStatus,
    SignalAttributionJobResponse,
    SignalAttributionRequest,
    SignalAttributionResult,
    SignalAttributionResultResponse,
)
from src.server.services.backtest_attribution_service import backtest_attribution_service
from src.server.services.backtest_service import backtest_service
from src.server.services.job_manager import JobInfo, job_manager
from src.server.services.sse_manager import sse_manager

router = APIRouter(tags=["Backtest"])
_ATTRIBUTION_JOB_TYPE = "backtest_attribution"


def _build_backtest_job_response(job: JobInfo) -> BacktestJobResponse:
    """JobInfoからBacktestJobResponseを構築"""
    return BacktestJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        message=job.message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        result=job.result,
    )


def _get_job_or_404(job_id: str) -> JobInfo:
    """ジョブを取得、存在しなければ404"""
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ジョブが見つかりません: {job_id}")
    return job


def _get_attribution_job_or_404(job_id: str) -> JobInfo:
    """寄与分析ジョブを取得し、存在しなければ404 / 種別不一致は400"""
    job = _get_job_or_404(job_id)
    if job.job_type != _ATTRIBUTION_JOB_TYPE:
        raise HTTPException(
            status_code=400,
            detail=f"シグナル寄与分析ジョブではありません: {job.job_type}",
        )
    return job


def _build_signal_attribution_job_response(job: JobInfo) -> SignalAttributionJobResponse:
    """JobInfoからSignalAttributionJobResponseを構築"""
    result_data = None
    if job.raw_result is not None and job.status == JobStatus.COMPLETED:
        try:
            result_data = SignalAttributionResult.model_validate(job.raw_result)
        except Exception as e:
            logger.warning(f"寄与分析結果のパースに失敗: {e}")

    return SignalAttributionJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        message=job.message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        result_data=result_data,
    )


@router.post("/api/backtest/run", response_model=BacktestJobResponse)
async def run_backtest(request: BacktestRequest) -> BacktestJobResponse:
    """
    バックテストを実行

    非同期でバックテストをサブミットし、ジョブIDを返却
    結果は /api/backtest/jobs/{job_id} で確認
    """
    try:
        job_id = await backtest_service.submit_backtest(
            strategy_name=request.strategy_name,
            config_override=request.strategy_config_override,
        )

        job = _get_job_or_404(job_id)
        return _build_backtest_job_response(job)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("バックテストサブミットエラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/backtest/jobs/{job_id}", response_model=BacktestJobResponse)
async def get_job_status(job_id: str) -> BacktestJobResponse:
    """
    ジョブステータスを取得

    Args:
        job_id: ジョブID
    """
    job = _get_job_or_404(job_id)
    return _build_backtest_job_response(job)


@router.get("/api/backtest/jobs", response_model=list[BacktestJobResponse])
async def list_jobs(limit: int = 50) -> list[BacktestJobResponse]:
    """
    ジョブ一覧を取得

    Args:
        limit: 取得件数上限（デフォルト50）
    """
    jobs = job_manager.list_jobs(limit=limit)
    return [_build_backtest_job_response(job) for job in jobs]


@router.post("/api/backtest/jobs/{job_id}/cancel", response_model=BacktestJobResponse)
async def cancel_job(job_id: str) -> BacktestJobResponse:
    """
    ジョブをキャンセル

    PENDING/RUNNING → キャンセル実行 → 200
    CANCELLED → 200（冪等）
    COMPLETED/FAILED → 409 Conflict
    """
    _get_job_or_404(job_id)

    cancelled_job = await job_manager.cancel_job(job_id)
    if cancelled_job is None:
        # cancel_jobがNoneを返すのはCOMPLETED/FAILEDの場合のみ
        job = job_manager.get_job(job_id)
        status = job.status if job else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"ジョブは既に終了しています（状態: {status}）",
        )

    return _build_backtest_job_response(cancelled_job)


@router.get("/api/backtest/result/{job_id}", response_model=BacktestResultResponse)
async def get_result(job_id: str, include_html: bool = False) -> BacktestResultResponse:
    """
    バックテスト結果を取得

    Args:
        job_id: ジョブID
        include_html: HTMLコンテンツを含めるか（base64エンコード）
    """
    job = _get_job_or_404(job_id)

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"ジョブが完了していません（状態: {job.status}）",
        )

    if job.result is None:
        raise HTTPException(status_code=500, detail="結果がありません")

    # HTMLコンテンツを読み込み（オプション）
    html_content = None
    if include_html and job.html_path:
        try:
            html_path = Path(job.html_path)
            if html_path.exists():
                with open(html_path, "rb") as f:
                    html_content = base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.warning(f"HTML読み込みエラー: {e}")

    return BacktestResultResponse(
        job_id=job.job_id,
        strategy_name=job.strategy_name,
        dataset_name=job.dataset_name or "unknown",
        summary=job.result,
        execution_time=job.execution_time or 0.0,
        html_content=html_content,
        created_at=job.created_at,
    )


@router.post(
    "/api/backtest/attribution/run",
    response_model=SignalAttributionJobResponse,
)
async def run_signal_attribution(
    request: SignalAttributionRequest,
) -> SignalAttributionJobResponse:
    """シグナル寄与分析ジョブをサブミット"""
    try:
        job_id = await backtest_attribution_service.submit_attribution(
            strategy_name=request.strategy_name,
            config_override=request.strategy_config_override,
            shapley_top_n=request.shapley_top_n,
            shapley_permutations=request.shapley_permutations,
            random_seed=request.random_seed,
        )
        job = _get_attribution_job_or_404(job_id)
        return _build_signal_attribution_job_response(job)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("シグナル寄与分析サブミットエラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get(
    "/api/backtest/attribution/jobs/{job_id}",
    response_model=SignalAttributionJobResponse,
)
async def get_signal_attribution_job(job_id: str) -> SignalAttributionJobResponse:
    """シグナル寄与分析ジョブの状態を取得"""
    job = _get_attribution_job_or_404(job_id)
    return _build_signal_attribution_job_response(job)


@router.post(
    "/api/backtest/attribution/jobs/{job_id}/cancel",
    response_model=SignalAttributionJobResponse,
)
async def cancel_signal_attribution_job(job_id: str) -> SignalAttributionJobResponse:
    """シグナル寄与分析ジョブをキャンセル"""
    _get_attribution_job_or_404(job_id)

    cancelled_job = await job_manager.cancel_job(job_id)
    if cancelled_job is None:
        job = job_manager.get_job(job_id)
        status = job.status if job else "unknown"
        raise HTTPException(
            status_code=409,
            detail=f"ジョブは既に終了しています（状態: {status}）",
        )

    return _build_signal_attribution_job_response(cancelled_job)


@router.get(
    "/api/backtest/attribution/jobs/{job_id}/stream",
)
async def stream_signal_attribution_events(job_id: str) -> EventSourceResponse:
    """シグナル寄与分析ジョブの進捗をSSEでストリーミング"""
    _get_attribution_job_or_404(job_id)
    return EventSourceResponse(sse_manager.job_event_generator(job_id))


@router.get(
    "/api/backtest/attribution/result/{job_id}",
    response_model=SignalAttributionResultResponse,
)
async def get_signal_attribution_result(job_id: str) -> SignalAttributionResultResponse:
    """シグナル寄与分析結果を取得"""
    job = _get_attribution_job_or_404(job_id)

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(
            status_code=400,
            detail=f"ジョブが完了していません（状態: {job.status}）",
        )
    if not job.raw_result:
        raise HTTPException(status_code=500, detail="結果がありません")

    try:
        result = SignalAttributionResult.model_validate(job.raw_result)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"結果の復元に失敗しました: {e}",
        ) from e

    return SignalAttributionResultResponse(
        job_id=job.job_id,
        strategy_name=job.strategy_name,
        result=result,
        created_at=job.created_at,
    )


@router.get(
    "/api/backtest/attribution-files",
    response_model=AttributionArtifactListResponse,
)
async def list_attribution_files(
    strategy: str | None = None,
    limit: int = 100,
) -> AttributionArtifactListResponse:
    """
    保存済みシグナル寄与分析JSONファイル一覧を取得

    Args:
        strategy: 戦略名でフィルタ（階層パス対応、オプション）
        limit: 取得件数上限（デフォルト100）
    """
    results_dir = get_backtest_attribution_dir()
    file_dicts, total = list_attribution_files_in_dir(results_dir, strategy, limit)

    files = [
        AttributionArtifactInfo(
            strategy_name=f["strategy_name"],
            filename=f["filename"],
            created_at=f["created_at"],
            size_bytes=f["size_bytes"],
            job_id=f.get("job_id"),
        )
        for f in file_dicts
    ]

    return AttributionArtifactListResponse(files=files, total=total)


@router.get(
    "/api/backtest/attribution-files/content",
    response_model=AttributionArtifactContentResponse,
)
async def get_attribution_file_content(
    strategy: str,
    filename: str,
) -> AttributionArtifactContentResponse:
    """
    保存済みシグナル寄与分析JSONファイル内容を取得

    Args:
        strategy: 戦略名（階層パス対応）
        filename: ファイル名（.json）
    """
    results_dir = get_backtest_attribution_dir()
    artifact = read_attribution_file(results_dir, strategy, filename)

    return AttributionArtifactContentResponse(
        strategy_name=strategy,
        filename=filename,
        artifact=artifact,
    )


@router.get("/api/backtest/html-files", response_model=HtmlFileListResponse)
async def list_html_files(
    strategy: str | None = None,
    limit: int = 100,
) -> HtmlFileListResponse:
    """
    バックテスト結果HTMLファイル一覧を取得

    Args:
        strategy: 戦略名でフィルタ（オプション）
        limit: 取得件数上限（デフォルト100）
    """
    results_dir = get_backtest_results_dir()
    file_dicts, total = list_html_files_in_dir(results_dir, strategy, limit)

    files = [
        HtmlFileInfo(
            strategy_name=f["strategy_name"],
            filename=f["filename"],
            dataset_name=f["dataset_name"],
            created_at=f["created_at"],
            size_bytes=f["size_bytes"],
        )
        for f in file_dicts
    ]

    return HtmlFileListResponse(files=files, total=total)


@router.get(
    "/api/backtest/html-files/{strategy}/{filename}",
    response_model=HtmlFileContentResponse,
)
async def get_html_file_content(strategy: str, filename: str) -> HtmlFileContentResponse:
    """
    特定のHTMLファイルのコンテンツを取得

    Args:
        strategy: 戦略名
        filename: ファイル名
    """
    results_dir = get_backtest_results_dir()
    html_content = read_html_file(results_dir, strategy, filename)

    # メトリクス抽出
    metrics: HtmlFileMetrics | None = None
    try:
        from src.data.metrics_extractor import extract_metrics_from_html

        html_path = results_dir / strategy / filename
        bt_metrics = extract_metrics_from_html(html_path)
        metrics = HtmlFileMetrics(
            total_return=bt_metrics.total_return,
            max_drawdown=bt_metrics.max_drawdown,
            sharpe_ratio=bt_metrics.sharpe_ratio,
            sortino_ratio=bt_metrics.sortino_ratio,
            calmar_ratio=bt_metrics.calmar_ratio,
            win_rate=bt_metrics.win_rate,
            profit_factor=bt_metrics.profit_factor,
            total_trades=bt_metrics.total_trades,
        )
    except Exception as e:
        logger.warning(f"メトリクス抽出エラー: {e}")

    return HtmlFileContentResponse(
        strategy_name=strategy,
        filename=filename,
        html_content=html_content,
        metrics=metrics,
    )


@router.post(
    "/api/backtest/html-files/{strategy}/{filename}/rename",
    response_model=HtmlFileRenameResponse,
)
async def rename_html_file_endpoint(
    strategy: str,
    filename: str,
    request: HtmlFileRenameRequest,
) -> HtmlFileRenameResponse:
    """
    HTMLファイルをリネーム

    Args:
        strategy: 戦略名
        filename: 現在のファイル名
        request: リネームリクエスト（新しいファイル名）
    """
    results_dir = get_backtest_results_dir()
    rename_html_file(results_dir, strategy, filename, request.new_filename)

    return HtmlFileRenameResponse(
        success=True,
        strategy_name=strategy,
        old_filename=filename,
        new_filename=request.new_filename,
    )


@router.delete(
    "/api/backtest/html-files/{strategy}/{filename}",
    response_model=HtmlFileDeleteResponse,
)
async def delete_html_file_endpoint(strategy: str, filename: str) -> HtmlFileDeleteResponse:
    """
    HTMLファイルを削除

    Args:
        strategy: 戦略名
        filename: ファイル名
    """
    results_dir = get_backtest_results_dir()
    delete_html_file(results_dir, strategy, filename)

    return HtmlFileDeleteResponse(
        success=True,
        strategy_name=strategy,
        filename=filename,
    )


# ============================================
# SSE Stream Endpoint
# ============================================


@router.get("/api/backtest/jobs/{job_id}/stream")
async def stream_job_events(job_id: str) -> EventSourceResponse:
    """
    ジョブの進捗をSSEでストリーミング

    Args:
        job_id: ジョブID
    """
    _get_job_or_404(job_id)
    return EventSourceResponse(sse_manager.job_event_generator(job_id))
