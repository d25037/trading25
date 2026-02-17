"""
Parameter Optimization Endpoints
"""

import os
from functools import reduce
from operator import mul
from pathlib import Path

from fastapi import APIRouter, HTTPException
from loguru import logger
from ruamel.yaml import YAML
from sse_starlette.sse import EventSourceResponse

from src.paths.resolver import get_all_optimization_grid_dirs, get_optimization_results_dir
from src.server.routes.html_file_utils import (
    delete_html_file,
    list_html_files_in_dir,
    read_html_file,
    rename_html_file,
)
from src.server.routes.utils import validate_path_param
from src.server.schemas.backtest import (
    HtmlFileDeleteResponse,
    HtmlFileRenameRequest,
    HtmlFileRenameResponse,
)
from src.server.schemas.optimize import (
    OptimizationGridConfig,
    OptimizationGridDeleteResponse,
    OptimizationGridListResponse,
    OptimizationGridSaveRequest,
    OptimizationGridSaveResponse,
    OptimizationHtmlFileContentResponse,
    OptimizationHtmlFileInfo,
    OptimizationHtmlFileListResponse,
    OptimizationJobResponse,
    OptimizationRequest,
)
from src.server.services.job_manager import job_manager
from src.server.services.optimization_service import optimization_service
from src.server.services.sse_manager import sse_manager

router = APIRouter(tags=["Optimization"])


def _build_optimization_job_response(job_id: str) -> OptimizationJobResponse:
    """JobInfoからOptimizationJobResponseを構築"""
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ジョブが見つかりません: {job_id}")

    return OptimizationJobResponse(
        job_id=job.job_id,
        status=job.status,
        progress=job.progress,
        message=job.message,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        best_score=job.best_score,
        best_params=job.best_params,
        worst_score=job.worst_score,
        worst_params=job.worst_params,
        total_combinations=job.total_combinations,
        notebook_path=job.notebook_path,
    )


# ============================================
# Optimization Job Endpoints
# ============================================


@router.post("/api/optimize/run", response_model=OptimizationJobResponse)
async def run_optimization(request: OptimizationRequest) -> OptimizationJobResponse:
    """パラメータ最適化を実行"""
    try:
        job_id = await optimization_service.submit_optimization(
            strategy_name=request.strategy_name,
        )
        return _build_optimization_job_response(job_id)
    except Exception as e:
        logger.exception("最適化サブミットエラー")
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/optimize/jobs/{job_id}", response_model=OptimizationJobResponse)
async def get_optimization_status(job_id: str) -> OptimizationJobResponse:
    """最適化ジョブのステータスを取得"""
    return _build_optimization_job_response(job_id)


@router.get("/api/optimize/jobs/{job_id}/stream")
async def stream_optimization_events(job_id: str) -> EventSourceResponse:
    """
    最適化ジョブの進捗をSSEでストリーミング

    Args:
        job_id: ジョブID
    """
    job = job_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"ジョブが見つかりません: {job_id}")

    return EventSourceResponse(sse_manager.job_event_generator(job_id))



# ============================================
# Grid Config Endpoints
# ============================================


def _parse_grid_yaml(content: str) -> tuple[int, int]:
    """
    Grid YAML文字列をパースしてパラメータ数と組み合わせ数を計算

    Returns:
        (param_count, combinations)
    """
    ruamel_yaml = YAML()
    data = ruamel_yaml.load(content)
    if not data or "parameter_ranges" not in data:
        return 0, 0

    ranges = data["parameter_ranges"]

    def count_params(d: dict, prefix: str = "") -> list[int]:
        """ネストされたパラメータ範囲から各パラメータの値数を取得"""
        counts: list[int] = []
        for key, value in d.items():
            if isinstance(value, list):
                counts.append(len(value))
            elif isinstance(value, dict):
                counts.extend(count_params(value, f"{prefix}{key}."))
        return counts

    param_counts = count_params(ranges)
    param_count = len(param_counts)
    combinations = reduce(mul, param_counts, 1) if param_counts else 0

    return param_count, combinations


def _find_grid_file(strategy_name: str) -> Path | None:
    """戦略名からGrid設定ファイルを検索"""
    # カテゴリ付きの場合、ベース名を抽出
    basename = strategy_name.split("/")[-1] if "/" in strategy_name else strategy_name
    grid_filename = f"{basename}_grid.yaml"

    for search_dir in get_all_optimization_grid_dirs():
        candidate = search_dir / grid_filename
        if candidate.exists():
            return candidate

    return None


def _get_grid_write_path(strategy_name: str) -> Path:
    """Grid設定の書き込み先パスを取得（外部ディレクトリ優先）"""
    from src.paths.resolver import get_optimization_grid_dir

    basename = strategy_name.split("/")[-1] if "/" in strategy_name else strategy_name
    grid_dir = get_optimization_grid_dir()
    grid_dir.mkdir(parents=True, exist_ok=True)
    return grid_dir / f"{basename}_grid.yaml"


@router.get("/api/optimize/grid-configs", response_model=OptimizationGridListResponse)
async def list_grid_configs() -> OptimizationGridListResponse:
    """Grid設定一覧を取得"""
    configs: list[OptimizationGridConfig] = []

    seen: set[str] = set()
    for search_dir in get_all_optimization_grid_dirs():
        if not search_dir.exists():
            continue
        for yaml_file in search_dir.glob("*_grid.yaml"):
            strategy_name = yaml_file.stem.replace("_grid", "")
            if strategy_name in seen:
                continue
            seen.add(strategy_name)

            try:
                content = yaml_file.read_text(encoding="utf-8")
                param_count, combinations = _parse_grid_yaml(content)
                configs.append(
                    OptimizationGridConfig(
                        strategy_name=strategy_name,
                        content=content,
                        param_count=param_count,
                        combinations=combinations,
                    )
                )
            except Exception as e:
                logger.warning(f"Grid設定読み込みエラー: {yaml_file}: {e}")

    configs.sort(key=lambda c: c.strategy_name)

    return OptimizationGridListResponse(configs=configs, total=len(configs))


@router.get("/api/optimize/grid-configs/{strategy}", response_model=OptimizationGridConfig)
async def get_grid_config(strategy: str) -> OptimizationGridConfig:
    """Grid設定を取得"""
    validate_path_param(strategy, "戦略名")
    grid_path = _find_grid_file(strategy)
    if grid_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Grid設定が見つかりません: {strategy}",
        )

    content = grid_path.read_text(encoding="utf-8")
    param_count, combinations = _parse_grid_yaml(content)

    return OptimizationGridConfig(
        strategy_name=strategy,
        content=content,
        param_count=param_count,
        combinations=combinations,
    )


@router.put("/api/optimize/grid-configs/{strategy}", response_model=OptimizationGridSaveResponse)
async def save_grid_config(strategy: str, request: OptimizationGridSaveRequest) -> OptimizationGridSaveResponse:
    """Grid設定を保存"""
    validate_path_param(strategy, "戦略名")
    # YAML検証
    try:
        param_count, combinations = _parse_grid_yaml(request.content)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"無効なYAML: {e}",
        ) from e

    grid_path = _get_grid_write_path(strategy)

    try:
        grid_path.write_text(request.content, encoding="utf-8")
        logger.info(f"Grid設定保存: {strategy} ({grid_path})")
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail=f"ファイル保存エラー: {e}",
        ) from e

    return OptimizationGridSaveResponse(
        success=True,
        strategy_name=strategy,
        param_count=param_count,
        combinations=combinations,
    )


@router.delete("/api/optimize/grid-configs/{strategy}", response_model=OptimizationGridDeleteResponse)
async def delete_grid_config(strategy: str) -> OptimizationGridDeleteResponse:
    """Grid設定を削除"""
    validate_path_param(strategy, "戦略名")
    grid_path = _find_grid_file(strategy)
    if grid_path is None:
        raise HTTPException(
            status_code=404,
            detail=f"Grid設定が見つかりません: {strategy}",
        )

    try:
        os.remove(grid_path)
        logger.info(f"Grid設定削除: {strategy} ({grid_path})")
    except OSError as e:
        raise HTTPException(
            status_code=500,
            detail=f"ファイル削除エラー: {e}",
        ) from e

    return OptimizationGridDeleteResponse(success=True, strategy_name=strategy)


# ============================================
# Optimization HTML File Endpoints
# ============================================


@router.get("/api/optimize/html-files", response_model=OptimizationHtmlFileListResponse)
async def list_optimization_html_files(
    strategy: str | None = None,
    limit: int = 100,
) -> OptimizationHtmlFileListResponse:
    """最適化結果HTMLファイル一覧を取得"""
    results_dir = get_optimization_results_dir()
    file_dicts, total = list_html_files_in_dir(results_dir, strategy, limit)

    files = [
        OptimizationHtmlFileInfo(
            strategy_name=f["strategy_name"],
            filename=f["filename"],
            dataset_name=f["dataset_name"],
            created_at=f["created_at"],
            size_bytes=f["size_bytes"],
        )
        for f in file_dicts
    ]

    return OptimizationHtmlFileListResponse(files=files, total=total)


@router.get(
    "/api/optimize/html-files/{strategy}/{filename}",
    response_model=OptimizationHtmlFileContentResponse,
)
async def get_optimization_html_file_content(strategy: str, filename: str) -> OptimizationHtmlFileContentResponse:
    """最適化結果HTMLファイルのコンテンツを取得"""
    results_dir = get_optimization_results_dir()
    html_content = read_html_file(results_dir, strategy, filename)

    return OptimizationHtmlFileContentResponse(
        strategy_name=strategy,
        filename=filename,
        html_content=html_content,
    )


@router.post(
    "/api/optimize/html-files/{strategy}/{filename}/rename",
    response_model=HtmlFileRenameResponse,
)
async def rename_optimization_html_file(
    strategy: str,
    filename: str,
    request: HtmlFileRenameRequest,
) -> HtmlFileRenameResponse:
    """
    最適化結果HTMLファイルをリネーム

    Args:
        strategy: 戦略名
        filename: 現在のファイル名
        request: リネームリクエスト（新しいファイル名）
    """
    results_dir = get_optimization_results_dir()
    rename_html_file(results_dir, strategy, filename, request.new_filename, log_prefix="最適化")

    return HtmlFileRenameResponse(
        success=True,
        strategy_name=strategy,
        old_filename=filename,
        new_filename=request.new_filename,
    )


@router.delete(
    "/api/optimize/html-files/{strategy}/{filename}",
    response_model=HtmlFileDeleteResponse,
)
async def delete_optimization_html_file(strategy: str, filename: str) -> HtmlFileDeleteResponse:
    """
    最適化結果HTMLファイルを削除

    Args:
        strategy: 戦略名
        filename: ファイル名
    """
    results_dir = get_optimization_results_dir()
    delete_html_file(results_dir, strategy, filename, log_prefix="最適化")

    return HtmlFileDeleteResponse(
        success=True,
        strategy_name=strategy,
        filename=filename,
    )
