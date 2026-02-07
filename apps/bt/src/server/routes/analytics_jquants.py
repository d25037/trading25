"""
Analytics Routes (JQuants-dependent)

ROE、margin-pressure、margin-ratio の 4 エンドポイント。
fundamentals はプロキシ済みなので含まない。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from src.server.schemas.analytics_margin import (
    MarginPressureIndicatorsResponse,
    MarginVolumeRatioResponse,
)
from src.server.schemas.analytics_roe import ROEResponse

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


def _get_roe_service(request: Request):
    return request.app.state.roe_service


def _get_margin_service(request: Request):
    return request.app.state.margin_analytics_service


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
