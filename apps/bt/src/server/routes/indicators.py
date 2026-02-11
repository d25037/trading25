"""
Indicator API Endpoints

インジケーター計算API
"""

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from loguru import logger

from src.api.exceptions import APIError, APINotFoundError

from src.server.schemas.indicators import (
    IndicatorComputeRequest,
    IndicatorComputeResponse,
    MarginIndicatorRequest,
    MarginIndicatorResponse,
)
from src.server.services.indicator_service import IndicatorService

router = APIRouter(tags=["Indicators"])

# ThreadPoolExecutor（モジュールレベルで1つ生成）
_executor = ThreadPoolExecutor(max_workers=5)


def _get_executor() -> ThreadPoolExecutor:
    """shutdown 済みの場合は再作成して返す"""
    global _executor
    if getattr(_executor, "_shutdown", False):
        _executor = ThreadPoolExecutor(max_workers=5)
    return _executor

TIMEOUT_SECONDS = 10


def _get_indicator_service(request: Request) -> IndicatorService:
    market_reader = getattr(request.app.state, "market_reader", None)
    return IndicatorService(market_reader=market_reader)


async def _run_in_executor(
    fn: Callable[..., dict[str, Any]],
    *args: Any,
    label: str,
) -> dict[str, Any]:
    """スレッドプールで計算を実行し、共通エラーハンドリングを適用"""
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_get_executor(), fn, *args),
            timeout=TIMEOUT_SECONDS,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"{label}がタイムアウトしました ({TIMEOUT_SECONDS}秒)",
        )
    except APINotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except APIError as e:
        logger.error(f"{label} APIエラー: {e}")
        raise HTTPException(
            status_code=e.status_code or 500, detail=str(e)
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
async def compute_indicators(
    request: Request,
    payload: IndicatorComputeRequest,
) -> IndicatorComputeResponse:
    """複数インジケーターを一括計算

    output='ohlcv'の場合、インジケーター計算をスキップし、
    変換後のOHLCVのみを返却する。
    """
    logger.info(
        f"インジケーター計算: {payload.stock_code} "
        f"({len(payload.indicators)} indicators, {payload.timeframe}, output={payload.output})"
    )
    service = _get_indicator_service(request)
    try:
        relative_opts = (
            payload.relative_options.model_dump() if payload.relative_options else None
        )
        result = await _run_in_executor(
            service.compute_indicators,
            payload.stock_code,
            payload.source,
            payload.timeframe,
            [spec.model_dump() for spec in payload.indicators],
            payload.start_date,
            payload.end_date,
            payload.nan_handling,
            payload.benchmark_code,
            relative_opts,
            payload.output,
            label="インジケーター計算",
        )
        return IndicatorComputeResponse(**result)
    finally:
        service.close()


@router.post(
    "/api/indicators/margin",
    response_model=MarginIndicatorResponse,
)
async def compute_margin_indicators(
    request: Request,
    payload: MarginIndicatorRequest,
) -> MarginIndicatorResponse:
    """信用指標を計算"""
    logger.info(
        f"信用指標計算: {payload.stock_code} source={payload.source} "
        f"({len(payload.indicators)} indicators)"
    )
    service = _get_indicator_service(request)
    try:
        result = await _run_in_executor(
            service.compute_margin_indicators,
            payload.stock_code,
            payload.source,
            list(payload.indicators),
            payload.average_period,
            payload.start_date,
            payload.end_date,
            label="信用指標計算",
        )
        return MarginIndicatorResponse(**result)
    finally:
        service.close()
