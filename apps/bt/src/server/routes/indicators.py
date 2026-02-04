"""
Indicator API Endpoints

インジケーター計算API
"""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, HTTPException
from loguru import logger

from src.server.schemas.indicators import (
    IndicatorComputeRequest,
    IndicatorComputeResponse,
    MarginIndicatorRequest,
    MarginIndicatorResponse,
)
from src.server.services.indicator_service import indicator_service

router = APIRouter(tags=["Indicators"])

# ThreadPoolExecutor（モジュールレベルで1つ生成）
_executor = ThreadPoolExecutor(max_workers=5)

TIMEOUT_SECONDS = 10


async def _run_in_executor(
    fn: Callable[..., dict[str, Any]],
    *args: Any,
    label: str,
) -> dict[str, Any]:
    """スレッドプールで計算を実行し、共通エラーハンドリングを適用"""
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_executor, fn, *args),
            timeout=TIMEOUT_SECONDS,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"{label}がタイムアウトしました ({TIMEOUT_SECONDS}秒)",
        )
    except ValueError as e:
        status = 404 if "取得できません" in str(e) else 422
        raise HTTPException(status_code=status, detail=str(e))
    except Exception as e:
        logger.exception(f"{label}エラー: {e}")
        raise HTTPException(status_code=500, detail=f"計算エラー: {e}")


@router.post(
    "/api/indicators/compute",
    response_model=IndicatorComputeResponse,
)
async def compute_indicators(request: IndicatorComputeRequest) -> IndicatorComputeResponse:
    """複数インジケーターを一括計算

    output='ohlcv'の場合、インジケーター計算をスキップし、
    変換後のOHLCVのみを返却する。
    """
    logger.info(
        f"インジケーター計算: {request.stock_code} "
        f"({len(request.indicators)} indicators, {request.timeframe}, output={request.output})"
    )
    relative_opts = (
        request.relative_options.model_dump() if request.relative_options else None
    )
    result = await _run_in_executor(
        indicator_service.compute_indicators,
        request.stock_code,
        request.source,
        request.timeframe,
        [spec.model_dump() for spec in request.indicators],
        request.start_date,
        request.end_date,
        request.nan_handling,
        request.benchmark_code,
        relative_opts,
        request.output,
        label="インジケーター計算",
    )
    return IndicatorComputeResponse(**result)


@router.post(
    "/api/indicators/margin",
    response_model=MarginIndicatorResponse,
)
async def compute_margin_indicators(
    request: MarginIndicatorRequest,
) -> MarginIndicatorResponse:
    """信用指標を計算"""
    logger.info(
        f"信用指標計算: {request.stock_code} source={request.source} "
        f"({len(request.indicators)} indicators)"
    )
    result = await _run_in_executor(
        indicator_service.compute_margin_indicators,
        request.stock_code,
        request.source,
        list(request.indicators),
        request.average_period,
        request.start_date,
        request.end_date,
        label="信用指標計算",
    )
    return MarginIndicatorResponse(**result)
