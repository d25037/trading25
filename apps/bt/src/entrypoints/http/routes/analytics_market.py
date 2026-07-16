"""
Analytics Routes (market.duckdb SoT)

ROE、margin-pressure、margin-ratio、fundamentals の検算系エンドポイント。
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from src.application.contracts import fundamentals as fundamentals_contracts
from src.application.contracts import margin_analytics as margin_contracts
from src.application.contracts import roe as roe_contracts
from src.application.contracts.fundamentals_pit import FundamentalsPitSnapshotError
from src.application.services.fundamentals_service import fundamentals_service
from src.application.services.margin_analytics_service import MarginAnalyticsService
from src.application.services.roe_service import ROEService
from src.domains.analytics.market_bubble_footprint_monitor import get_latest_market_bubble_footprint
from src.entrypoints.http.schemas.analytics_common import MarketBubbleFootprintLatestResponse
from src.entrypoints.http.routes.fundamentals_error_mapping import (
    FUNDAMENTALS_ERROR_RESPONSES,
    raise_fundamentals_http_error,
)

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])

_executor = ThreadPoolExecutor(max_workers=4)


def _get_executor() -> ThreadPoolExecutor:
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


@router.get(
    "/market-bubble-footprint/latest",
    response_model=MarketBubbleFootprintLatestResponse,
    summary="Get latest market bubble footprint",
)
async def get_market_bubble_footprint_latest(
    markets: str = Query("prime,standard,growth"),
    date: str | None = Query(None),
) -> MarketBubbleFootprintLatestResponse:
    market_scopes = tuple(item.strip() for item in markets.split(",") if item.strip())
    loop = asyncio.get_event_loop()
    try:
        payload = await loop.run_in_executor(
            _get_executor(),
            lambda: get_latest_market_bubble_footprint(markets=market_scopes, date=date),
        )
        return MarketBubbleFootprintLatestResponse.model_validate(payload)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/roe", response_model=roe_contracts.ROEResponse)
async def get_roe(
    request: Request,
    code: str | None = Query(None, description="Stock codes (comma-separated)"),
    date: str | None = Query(None, description="Specific date (YYYYMMDD or YYYY-MM-DD)"),
    annualize: str = Query("true", description="Annualize quarterly data"),
    preferConsolidated: str = Query("true", description="Prefer consolidated data"),
    minEquity: str = Query("1000", description="Minimum equity threshold (millions)"),
    sortBy: str = Query("roe", description="Sort by (roe, code, date)"),
    limit: str = Query("50", description="Max results"),
) -> roe_contracts.ROEResponse:
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
    response_model=margin_contracts.MarginPressureIndicatorsResponse,
)
async def get_margin_pressure(
    request: Request,
    symbol: str,
    period: int = Query(15, ge=5, le=60, description="Rolling average period in days"),
) -> margin_contracts.MarginPressureIndicatorsResponse:
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
    response_model=margin_contracts.MarginVolumeRatioResponse,
)
async def get_margin_ratio(
    request: Request,
    symbol: str,
) -> margin_contracts.MarginVolumeRatioResponse:
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
    response_model=fundamentals_contracts.FundamentalsComputeResponse,
    responses=FUNDAMENTALS_ERROR_RESPONSES,
    summary="Get fundamental analysis metrics for a stock",
)
async def get_fundamentals(
    symbol: str,
    from_date: fundamentals_contracts.StrictIsoDate | None = Query(
        None,
        alias="from",
        description=fundamentals_contracts.FUNDAMENTALS_FROM_DATE_DESCRIPTION,
    ),
    to_date: fundamentals_contracts.StrictIsoDate | None = Query(
        None,
        alias="to",
        description=fundamentals_contracts.FUNDAMENTALS_TO_DATE_DESCRIPTION,
    ),
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
) -> fundamentals_contracts.FundamentalsComputeResponse:
    try:
        req = fundamentals_contracts.FundamentalsComputeQuery(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date,
            period_type=periodType,
            prefer_consolidated=preferConsolidated,
            trading_value_period=tradingValuePeriod,
            forecast_eps_lookback_fy_count=forecastEpsLookbackFyCount,
        )
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from exc
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            _get_executor(),
            fundamentals_service.compute_fundamentals,
            req,
        )
    except FundamentalsPitSnapshotError as exc:
        raise_fundamentals_http_error(exc)
    return result
