"""
Chart Routes

chart/indices, chart/stocks 系 5 ルート + analytics/sector-stocks 1 ルート。
Hono chart ルートと互換。

NOTE: /api/chart/stocks/search は /api/chart/stocks/{symbol} より先に登録。
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from src.entrypoints.http.schemas.chart import (
    IndexDataResponse,
    IndicesListResponse,
    SectorStocksResponse,
    StockDataResponse,
    StockSearchResponse,
    TopixDataResponse,
)
from src.application.services.chart_service import ChartService

router = APIRouter(tags=["Chart"])


def _get_chart_service(request: Request) -> ChartService:
    service = getattr(request.app.state, "chart_service", None)
    if service is None:
        raise HTTPException(status_code=422, detail="Chart service not initialized")
    return service


# --- Indices ---


@router.get(
    "/api/chart/indices",
    response_model=IndicesListResponse,
    summary="指数一覧取得",
)
def get_indices_list(request: Request) -> IndicesListResponse:
    service = _get_chart_service(request)
    result = service.get_indices_list()
    if result is None:
        raise HTTPException(status_code=500, detail="Market database not available")
    return result


@router.get(
    "/api/chart/indices/topix",
    response_model=TopixDataResponse,
    summary="TOPIX チャートデータ取得",
)
async def get_topix_data(
    request: Request,
    from_date: str | None = Query(default=None, alias="from", description="開始日 (YYYY-MM-DD)"),
    to_date: str | None = Query(default=None, alias="to", description="終了日 (YYYY-MM-DD)"),
    date: str | None = Query(default=None, description="特定日 (YYYY-MM-DD)"),
) -> TopixDataResponse:
    # date パラメータで from/to を上書き
    effective_from = date or from_date
    effective_to = date or to_date

    # 日付範囲バリデーション
    if effective_from and effective_to and effective_from > effective_to:
        raise HTTPException(
            status_code=422,
            detail='Invalid date range: "from" date must be before or equal to "to" date',
        )

    service = _get_chart_service(request)
    result = await service.get_topix_data(from_date=effective_from, to_date=effective_to)
    if result is None:
        raise HTTPException(status_code=500, detail="TOPIX data not available")
    return result


@router.get(
    "/api/chart/indices/{code}",
    response_model=IndexDataResponse,
    summary="指数チャートデータ取得",
)
def get_index_data(request: Request, code: str) -> IndexDataResponse:
    service = _get_chart_service(request)
    result = service.get_index_data(code)
    if result is None:
        raise HTTPException(status_code=404, detail="Index not found")
    return result


# --- Stock Search (MUST be registered BEFORE /api/chart/stocks/{symbol}) ---


@router.get(
    "/api/chart/stocks/search",
    response_model=StockSearchResponse,
    summary="銘柄検索",
)
def search_stocks(
    request: Request,
    q: str = Query(min_length=1, max_length=100, description="検索クエリ"),
    limit: int = Query(default=20, ge=1, le=100, description="最大件数"),
) -> StockSearchResponse:
    service = _get_chart_service(request)
    return service.search_stocks(q, limit)


# --- Stock Chart ---


@router.get(
    "/api/chart/stocks/{symbol}",
    response_model=StockDataResponse,
    summary="銘柄チャートデータ取得",
)
async def get_stock_data(
    request: Request,
    symbol: str,
    timeframe: Literal["daily", "weekly", "monthly"] = Query(default="daily"),
    adjusted: Literal["true", "false"] = Query(default="true"),
) -> StockDataResponse:
    service = _get_chart_service(request)
    result = await service.get_stock_data(
        symbol=symbol,
        timeframe=timeframe,
        adjusted=(adjusted == "true"),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Stock symbol not found")
    return result


# --- Sector Stocks ---


@router.get(
    "/api/analytics/sector-stocks",
    response_model=SectorStocksResponse,
    summary="セクター別銘柄データ取得",
)
def get_sector_stocks(
    request: Request,
    sector33Name: str | None = Query(default=None, description="33業種名"),
    sector17Name: str | None = Query(default=None, description="17業種名"),
    markets: str = Query(default="prime,standard", description="市場フィルタ"),
    lookbackDays: int = Query(default=5, ge=1, le=100, description="振り返り日数"),
    sortBy: Literal["tradingValue", "changePercentage", "code"] = Query(default="tradingValue"),
    sortOrder: Literal["asc", "desc"] = Query(default="desc"),
    limit: int = Query(default=100, ge=1, le=500, description="最大件数"),
) -> SectorStocksResponse:
    service = _get_chart_service(request)
    result = service.get_sector_stocks(
        sector33_name=sector33Name,
        sector17_name=sector17Name,
        markets=markets,
        lookback_days=lookbackDays,
        sort_by=sortBy,
        sort_order=sortOrder,
        limit=limit,
    )
    if result is None:
        raise HTTPException(
            status_code=422,
            detail='Market database not initialized. Please run "bun cli db sync" first.',
        )
    return result
