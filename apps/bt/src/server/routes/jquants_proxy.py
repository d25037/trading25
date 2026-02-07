"""
JQuants Proxy Routes

Hono Layer 1: JQuants Proxy API の移植。
7 エンドポイント: auth/status, daily-quotes, indices, listed-info,
                  margin-interest, statements, statements/raw, topix
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from src.server.schemas.jquants import (
    ApiIndicesResponse,
    ApiListedInfoResponse,
    ApiMarginInterestResponse,
    AuthStatusResponse,
    DailyQuotesResponse,
    RawStatementsResponse,
    StatementsResponse,
    TopixRawResponse,
)

router = APIRouter(prefix="/api/jquants", tags=["JQuants Proxy"])


def _get_proxy_service(request: Request):
    """Request state から JQuantsProxyService を取得"""
    return request.app.state.jquants_proxy_service


@router.get("/auth/status", response_model=AuthStatusResponse)
async def get_auth_status(request: Request) -> AuthStatusResponse:
    """JQuants API v2 認証ステータスを取得"""
    service = _get_proxy_service(request)
    return service.get_auth_status()


@router.get("/daily-quotes", response_model=DailyQuotesResponse)
async def get_daily_quotes(
    request: Request,
    code: str = Query(..., description="Stock code"),
    date_from: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    date: str | None = Query(None, description="Specific date (YYYY-MM-DD)"),
) -> DailyQuotesResponse:
    """日足クォートデータを取得（JQuants 生フォーマット）"""
    service = _get_proxy_service(request)
    return await service.get_daily_quotes(code, date_from, date_to, date)


@router.get("/indices", response_model=ApiIndicesResponse)
async def get_indices(
    request: Request,
    code: str | None = Query(None, description="Index code (e.g., 0000 for Nikkei 225)"),
    date_from: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    date: str | None = Query(None, description="Specific date (YYYY-MM-DD)"),
) -> ApiIndicesResponse:
    """指数データを取得"""
    # Date range validation (Hono 互換)
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="'from' date must be before or equal to 'to' date")
    service = _get_proxy_service(request)
    return await service.get_indices(code, date_from, date_to, date)


@router.get("/listed-info", response_model=ApiListedInfoResponse)
async def get_listed_info(
    request: Request,
    code: str | None = Query(None, min_length=4, max_length=4, description="Stock code (4 characters)"),
    date: str | None = Query(None, description="Date (YYYY-MM-DD)"),
) -> ApiListedInfoResponse:
    """上場銘柄情報を取得"""
    service = _get_proxy_service(request)
    return await service.get_listed_info(code, date)


@router.get(
    "/stocks/{symbol}/margin-interest",
    response_model=ApiMarginInterestResponse,
)
async def get_margin_interest(
    request: Request,
    symbol: str,
    date_from: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    date: str | None = Query(None, description="Specific date (YYYY-MM-DD)"),
) -> ApiMarginInterestResponse:
    """週次信用取引データを取得"""
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="'from' date must be before or equal to 'to' date")
    service = _get_proxy_service(request)
    return await service.get_margin_interest(symbol, date_from, date_to, date)


@router.get("/statements", response_model=StatementsResponse)
async def get_statements(
    request: Request,
    code: str = Query(..., description="Stock code (4-5 digits)"),
) -> StatementsResponse:
    """財務諸表データを取得（EPS サブセット）"""
    service = _get_proxy_service(request)
    return await service.get_statements(code)


@router.get("/statements/raw", response_model=RawStatementsResponse)
async def get_statements_raw(
    request: Request,
    code: str = Query(..., description="Stock code (4-5 digits)"),
) -> RawStatementsResponse:
    """財務諸表データを取得（完全版）"""
    service = _get_proxy_service(request)
    return await service.get_statements_raw(code)


@router.get("/topix", response_model=TopixRawResponse)
async def get_topix(
    request: Request,
    date_from: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    date: str | None = Query(None, description="Specific date (YYYY-MM-DD)"),
) -> TopixRawResponse:
    """TOPIX 指数データを取得（生フォーマット）"""
    service = _get_proxy_service(request)
    return await service.get_topix(date_from, date_to, date)
