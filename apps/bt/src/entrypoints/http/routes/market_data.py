"""
Market Data Routes

market.db から株式・TOPIX データを提供する 4 エンドポイント。
Hono /api/market/ ルートと互換。
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from src.entrypoints.http.schemas.market_data import (
    MarketOHLCRecord,
    MarketOHLCVRecord,
    MarketStockData,
    StockInfo,
)
from src.application.services.market_data_service import MarketDataService

router = APIRouter(tags=["Market Data"])


def _get_market_data_service(request: Request) -> MarketDataService:
    service = getattr(request.app.state, "market_data_service", None)
    if service is None:
        raise HTTPException(status_code=422, detail="Market database not initialized")
    return service


@router.get(
    "/api/market/stocks",
    response_model=list[MarketStockData],
    summary="全銘柄データ取得（スクリーニング用）",
)
def get_all_stocks(
    request: Request,
    market: Literal["prime", "standard"] = Query(default="prime", description="市場コード"),
    history_days: int = Query(default=300, ge=1, le=1000, description="履歴日数"),
) -> list[MarketStockData]:
    service = _get_market_data_service(request)
    result = service.get_all_stocks(market=market, history_days=history_days)
    if result is None:
        raise HTTPException(status_code=404, detail="Market database not found")
    return result


@router.get(
    "/api/market/stocks/{code}",
    response_model=StockInfo,
    summary="単一銘柄情報取得",
)
def get_stock_info(
    request: Request,
    code: str,
) -> StockInfo:
    service = _get_market_data_service(request)
    result = service.get_stock_info(code)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return result


@router.get(
    "/api/market/stocks/{code}/ohlcv",
    response_model=list[MarketOHLCVRecord],
    summary="銘柄 OHLCV データ取得",
)
def get_stock_ohlcv(
    request: Request,
    code: str,
    start_date: str | None = Query(default=None, description="開始日 (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="終了日 (YYYY-MM-DD)"),
) -> list[MarketOHLCVRecord]:
    service = _get_market_data_service(request)
    result = service.get_stock_ohlcv(code, start_date=start_date, end_date=end_date)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return result


@router.get(
    "/api/market/topix",
    response_model=list[MarketOHLCRecord],
    summary="TOPIX データ取得",
)
def get_topix(
    request: Request,
    start_date: str | None = Query(default=None, description="開始日 (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="終了日 (YYYY-MM-DD)"),
) -> list[MarketOHLCRecord]:
    service = _get_market_data_service(request)
    result = service.get_topix(start_date=start_date, end_date=end_date)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Market database not found or TOPIX data not available",
        )
    return result
