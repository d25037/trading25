"""Read-only moomoo OpenD market data routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from src.application.services.moomoo_market_data_service import MoomooMarketDataService
from src.entrypoints.http.schemas.moomoo import (
    MoomooStatusResponse,
    MoomooUsHistoryResponse,
    MoomooUsSnapshotResponse,
    MoomooUsStockSearchResponse,
)
from src.infrastructure.external_api.clients.moomoo_quote_client import MoomooOpenDError

router = APIRouter(prefix="/api/moomoo", tags=["Moomoo OpenD"])


def _get_moomoo_service(request: Request) -> MoomooMarketDataService:
    service = getattr(request.app.state, "moomoo_market_data_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="moomoo OpenD service not initialized")
    return service


def _as_http_error(exc: MoomooOpenDError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.message)


@router.get("/status", response_model=MoomooStatusResponse)
async def get_moomoo_status(request: Request) -> MoomooStatusResponse:
    """Return read-only moomoo OpenD integration status."""
    service = _get_moomoo_service(request)
    return MoomooStatusResponse.model_validate(await service.get_status())


@router.get("/us/stocks/search", response_model=MoomooUsStockSearchResponse)
async def search_us_stocks(
    request: Request,
    query: str = Query(..., min_length=1, max_length=64, description="US ticker or company name fragment"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of matches"),
) -> MoomooUsStockSearchResponse:
    """Search US stocks through moomoo OpenD static information."""
    service = _get_moomoo_service(request)
    try:
        return MoomooUsStockSearchResponse.model_validate(await service.search_us_stocks(query, limit))
    except MoomooOpenDError as exc:
        raise _as_http_error(exc) from exc


@router.get("/us/history", response_model=MoomooUsHistoryResponse)
async def get_us_history(
    request: Request,
    symbol: str = Query(..., min_length=1, max_length=32, description="US symbol, with or without US. prefix"),
    date_from: str | None = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    date_to: str | None = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
    max_rows: int | None = Query(None, ge=1, le=5000, description="Maximum rows to return"),
) -> MoomooUsHistoryResponse:
    """Fetch US daily historical candlesticks from moomoo OpenD."""
    if date_from and date_to and date_from > date_to:
        raise HTTPException(status_code=422, detail="'from' date must be before or equal to 'to' date")
    service = _get_moomoo_service(request)
    try:
        return MoomooUsHistoryResponse.model_validate(
            await service.get_us_history(symbol, date_from, date_to, max_rows)
        )
    except MoomooOpenDError as exc:
        raise _as_http_error(exc) from exc


@router.get("/us/snapshot", response_model=MoomooUsSnapshotResponse)
async def get_us_snapshot(
    request: Request,
    symbols: list[str] = Query(..., min_length=1, max_length=50, description="US symbols"),
) -> MoomooUsSnapshotResponse:
    """Fetch US market snapshots from moomoo OpenD."""
    service = _get_moomoo_service(request)
    try:
        return MoomooUsSnapshotResponse.model_validate(await service.get_us_snapshot(symbols))
    except MoomooOpenDError as exc:
        raise _as_http_error(exc) from exc
