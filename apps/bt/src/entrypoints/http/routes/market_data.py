"""
Market Data Routes

DuckDB market time-series から株式・TOPIX データを提供する 4 エンドポイント。
Hono /api/market/ ルートと互換。
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request

from src.application.services.market_data_errors import MarketDataError
from src.application.services.options_225 import normalize_options_225_date
from src.entrypoints.http.error_utils import market_data_http_exception
from src.entrypoints.http.schemas.jquants import N225OptionsExplorerResponse
from src.entrypoints.http.schemas.market_data import (
    MarketMinuteBarRecord,
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
    market: Literal["prime", "standard", "growth", "0111", "0112", "0113"] = Query(
        default="prime",
        description="市場コード（legacy/current 同義語対応: prime/standard/growth, 0111/0112/0113）",
    ),
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
    asOfDate: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$", description="PIT master date (YYYY-MM-DD); omitted uses stocks_latest"),
) -> StockInfo:
    service = _get_market_data_service(request)
    result = service.get_stock_info(code, as_of_date=asOfDate)
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
    "/api/market/stocks/{code}/minute-bars",
    response_model=list[MarketMinuteBarRecord],
    summary="銘柄 分足データ取得",
)
def get_stock_minute_bars(
    request: Request,
    code: str,
    date: str = Query(..., description="対象日 (YYYY-MM-DD)"),
    start_time: str | None = Query(default=None, description="開始時刻 (HH:MM)"),
    end_time: str | None = Query(default=None, description="終了時刻 (HH:MM)"),
) -> list[MarketMinuteBarRecord]:
    if start_time and end_time and start_time > end_time:
        raise HTTPException(
            status_code=422,
            detail="'start_time' must be before or equal to 'end_time'",
        )

    service = _get_market_data_service(request)
    result = service.get_stock_minute_bars(
        code,
        date=date,
        start_time=start_time,
        end_time=end_time,
    )
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


@router.get(
    "/api/market/options/225",
    response_model=N225OptionsExplorerResponse,
    summary="日経225オプション四本値取得（DuckDB）",
)
def get_options_225(
    request: Request,
    date: str | None = Query(default=None, description="取引日 (YYYY-MM-DD or YYYYMMDD)"),
) -> N225OptionsExplorerResponse:
    normalized_date: str | None = None
    if date is not None:
        try:
            normalized_date = normalize_options_225_date(date)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    service = _get_market_data_service(request)
    try:
        return service.get_options_225(normalized_date)
    except MarketDataError as exc:
        raise market_data_http_exception(exc) from exc
