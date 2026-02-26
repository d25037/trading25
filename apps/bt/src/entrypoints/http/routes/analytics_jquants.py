"""
Analytics Routes (JQuants-dependent)

ROE、margin-pressure、margin-ratio、fundamentals の 4+1 エンドポイント。
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from src.entrypoints.http.schemas.analytics_margin import (
    MarginPressureIndicatorsResponse,
    MarginVolumeRatioResponse,
)
from src.entrypoints.http.schemas.analytics_roe import ROEResponse
from src.entrypoints.http.schemas.fundamentals import (
    FundamentalsComputeRequest,
    FundamentalsComputeResponse,
)
from src.application.services.fundamentals_service import fundamentals_service
from src.application.services.margin_analytics_service import MarginAnalyticsService
from src.application.services.roe_service import ROEService

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])

# ThreadPoolExecutor for blocking operations (fundamentals)
_executor = ThreadPoolExecutor(max_workers=4)


def _get_executor() -> ThreadPoolExecutor:
    """shutdown 済みの場合は再作成して返す"""
    global _executor
    if getattr(_executor, "_shutdown", False):
        _executor = ThreadPoolExecutor(max_workers=4)
    return _executor


def _get_roe_service(request: Request) -> ROEService:
    service = getattr(request.app.state, "roe_service", None)
    if service is None:
        raise HTTPException(status_code=422, detail="ROE service not initialized")
    return service


def _get_margin_service(request: Request) -> MarginAnalyticsService:
    service = getattr(request.app.state, "margin_analytics_service", None)
    if service is None:
        raise HTTPException(status_code=422, detail="Margin analytics service not initialized")
    return service


@router.get("/roe", response_model=ROEResponse)
async def get_roe(
    request: Request,
    code: str | None = Query(None, description="Stock codes (comma-separated)"),
    date: str | None = Query(None, description="Specific date (YYYYMMDD or YYYY-MM-DD)"),
    annualize: str = Query("true", description="Annualize quarterly data"),
    preferConsolidated: str = Query("true", description="Prefer consolidated data"),
    minEquity: str = Query("1000", description="Minimum equity threshold (millions)"),
    sortBy: str = Query("roe", description="Sort by (roe, code, date)"),
    limit: str = Query("50", description="Max results"),
) -> ROEResponse:
    """ROE (自己資本利益率) を計算"""
    if not code and not date:
        raise HTTPException(status_code=400, detail="Either 'code' or 'date' parameter is required")

    service = _get_roe_service(request)
    return await service.calculate_roe(
        code=code,
        date=date,
        annualize=annualize.lower() != "false",
        prefer_consolidated=preferConsolidated.lower() != "false",
        min_equity=float(minEquity),
        sort_by=sortBy,
        limit=int(limit),
    )


@router.get(
    "/stocks/{symbol}/margin-pressure",
    response_model=MarginPressureIndicatorsResponse,
)
async def get_margin_pressure(
    request: Request,
    symbol: str,
    period: int = Query(15, ge=5, le=60, description="Rolling average period in days"),
) -> MarginPressureIndicatorsResponse:
    """マージンプレッシャー指標を取得"""
    service = _get_margin_service(request)
    result = await service.get_margin_pressure(symbol, period)

    if not result.longPressure and not result.flowPressure and not result.turnoverDays:
        raise HTTPException(
            status_code=404,
            detail=f"Margin pressure data for stock symbol '{symbol}' not found",
        )
    return result


@router.get(
    "/stocks/{symbol}/margin-ratio",
    response_model=MarginVolumeRatioResponse,
)
async def get_margin_ratio(
    request: Request,
    symbol: str,
) -> MarginVolumeRatioResponse:
    """マージン出来高比率を取得"""
    service = _get_margin_service(request)
    result = await service.get_margin_ratio(symbol)

    if not result.longRatio and not result.shortRatio:
        raise HTTPException(
            status_code=404,
            detail=f"Margin ratio data for stock symbol '{symbol}' not found",
        )
    return result


@router.get(
    "/fundamentals/{symbol}",
    response_model=FundamentalsComputeResponse,
    summary="Get fundamental analysis metrics for a stock",
)
async def get_fundamentals(
    symbol: str,
    from_date: str | None = Query(None, alias="from"),
    to_date: str | None = Query(None, alias="to"),
    periodType: Literal["all", "FY", "1Q", "2Q", "3Q"] = Query("all"),
    preferConsolidated: bool = Query(True),
    tradingValuePeriod: int = Query(
        15,
        ge=1,
        le=250,
        description="Rolling average period in days for trading value to market cap ratio",
    ),
    forecastEpsLookbackFyCount: int = Query(
        3,
        ge=1,
        le=20,
        description="Lookback FY count for forecast EPS vs recent actual EPS comparison",
    ),
) -> FundamentalsComputeResponse:
    """ファンダメンタルズ分析指標を取得"""
    req = FundamentalsComputeRequest(
        symbol=symbol,
        from_date=from_date,
        to_date=to_date,
        period_type=periodType,
        prefer_consolidated=preferConsolidated,
        trading_value_period=tradingValuePeriod,
        forecast_eps_lookback_fy_count=forecastEpsLookbackFyCount,
    )
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        _get_executor(),
        fundamentals_service.compute_fundamentals,
        req,
    )
    if not result.data:
        raise HTTPException(
            status_code=404,
            detail=f"No financial statements found for stock {symbol}",
        )
    return result
