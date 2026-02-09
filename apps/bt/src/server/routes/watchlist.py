"""
Watchlist API Routes

Hono Watchlist API 互換の CRUD + Prices エンドポイント。
Phase 3E-1: 7 CRUD EP, Phase 3E-2: 1 Prices EP
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy.exc import IntegrityError

from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.portfolio_db import PortfolioDb
from src.server.schemas.portfolio import DeleteResponse
from src.server.schemas.portfolio_performance import WatchlistPricesResponse
from src.server.services.watchlist_prices_service import WatchlistPricesService
from src.server.schemas.watchlist import (
    WatchlistCreateRequest,
    WatchlistDetailResponse,
    WatchlistItemCreateRequest,
    WatchlistItemResponse,
    WatchlistResponse,
    WatchlistSummaryResponse,
    WatchlistUpdateRequest,
)

router = APIRouter(tags=["Watchlist"])


def _get_portfolio_db(request: Request) -> PortfolioDb:
    pdb = getattr(request.app.state, "portfolio_db", None)
    if pdb is None:
        raise HTTPException(status_code=422, detail="Portfolio database not initialized")
    return pdb


def _row_to_watchlist(row: Any) -> dict[str, Any]:
    """Row → WatchlistResponse 用 dict"""
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "createdAt": row.created_at or "",
        "updatedAt": row.updated_at or "",
    }


def _row_to_watchlist_item(row: Any) -> dict[str, Any]:
    """Row → WatchlistItemResponse 用 dict"""
    return {
        "id": row.id,
        "watchlistId": row.watchlist_id,
        "code": row.code,
        "companyName": row.company_name,
        "memo": row.memo,
        "createdAt": row.created_at or "",
    }


# ===== Watchlist CRUD (5 EP) =====


@router.get(
    "/api/watchlist",
    summary="List all watchlists",
    description="ウォッチリスト一覧を stockCount 付きで取得",
)
def list_watchlists(request: Request) -> dict[str, list[WatchlistSummaryResponse]]:
    pdb = _get_portfolio_db(request)
    summaries = pdb.list_watchlist_summaries()
    return {
        "watchlists": [
            WatchlistSummaryResponse(
                id=s["id"],
                name=s["name"],
                description=s["description"],
                stockCount=s["stock_count"],
                createdAt=s["created_at"] or "",
                updatedAt=s["updated_at"] or "",
            )
            for s in summaries
        ]
    }


@router.post(
    "/api/watchlist",
    response_model=WatchlistResponse,
    status_code=201,
    summary="Create a watchlist",
)
def create_watchlist(request: Request, body: WatchlistCreateRequest) -> JSONResponse:
    pdb = _get_portfolio_db(request)
    try:
        row = pdb.create_watchlist(body.name, body.description)
    except IntegrityError as e:
        msg = str(e.orig)
        if "watchlists.name" in msg:
            raise HTTPException(status_code=409, detail="Watchlist name already exists") from e
        raise  # pragma: no cover
    return JSONResponse(status_code=201, content=_row_to_watchlist(row))


@router.get(
    "/api/watchlist/{id}",
    response_model=WatchlistDetailResponse,
    summary="Get watchlist with items",
)
def get_watchlist(request: Request, id: int) -> WatchlistDetailResponse:
    pdb = _get_portfolio_db(request)
    row = pdb.get_watchlist(id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Watchlist {id} not found")
    items = pdb.list_watchlist_items(id)
    data = _row_to_watchlist(row)
    data["items"] = [_row_to_watchlist_item(item) for item in items]
    return WatchlistDetailResponse(**data)


@router.put(
    "/api/watchlist/{id}",
    response_model=WatchlistResponse,
    summary="Update a watchlist",
)
def update_watchlist(request: Request, id: int, body: WatchlistUpdateRequest) -> WatchlistResponse:
    pdb = _get_portfolio_db(request)
    kwargs: dict[str, Any] = {}
    if body.name is not None:
        kwargs["name"] = body.name
    if "description" in body.model_fields_set:
        kwargs["description"] = body.description
    try:
        row = pdb.update_watchlist(id, **kwargs)
    except IntegrityError as e:
        msg = str(e.orig)
        if "watchlists.name" in msg:
            raise HTTPException(status_code=409, detail="Watchlist name already exists") from e
        raise  # pragma: no cover
    if row is None:
        raise HTTPException(status_code=404, detail=f"Watchlist {id} not found")
    return WatchlistResponse(**_row_to_watchlist(row))


@router.delete(
    "/api/watchlist/{id}",
    response_model=DeleteResponse,
    summary="Delete a watchlist",
)
def delete_watchlist(request: Request, id: int) -> DeleteResponse:
    pdb = _get_portfolio_db(request)
    if not pdb.delete_watchlist(id):
        raise HTTPException(status_code=404, detail=f"Watchlist {id} not found")
    return DeleteResponse(message="Watchlist deleted successfully")


# ===== Watchlist Items (2 EP) =====


@router.post(
    "/api/watchlist/{id}/items",
    response_model=WatchlistItemResponse,
    status_code=201,
    summary="Add item to watchlist",
)
def add_watchlist_item(request: Request, id: int, body: WatchlistItemCreateRequest) -> JSONResponse:
    pdb = _get_portfolio_db(request)
    # Check watchlist exists
    if pdb.get_watchlist(id) is None:
        raise HTTPException(status_code=404, detail=f"Watchlist {id} not found")
    try:
        row = pdb.add_watchlist_item(id, body.code, body.companyName, memo=body.memo)
    except IntegrityError as e:
        msg = str(e.orig)
        if "watchlist_items" in msg and "code" in msg:
            raise HTTPException(status_code=409, detail="Stock already in watchlist") from e
        raise  # pragma: no cover
    return JSONResponse(status_code=201, content=_row_to_watchlist_item(row))


@router.delete(
    "/api/watchlist/{id}/items/{itemId}",
    response_model=DeleteResponse,
    summary="Delete watchlist item",
)
def delete_watchlist_item(request: Request, id: int, itemId: int) -> DeleteResponse:
    pdb = _get_portfolio_db(request)
    # Verify watchlist exists
    if pdb.get_watchlist(id) is None:
        raise HTTPException(status_code=404, detail=f"Watchlist {id} not found")
    if not pdb.delete_watchlist_item(itemId):
        raise HTTPException(status_code=404, detail=f"Item {itemId} not found in watchlist")
    return DeleteResponse(message="Stock removed from watchlist")


# ===== Watchlist Prices (Phase 3E-2) =====


def _get_market_reader(request: Request) -> MarketDbReader:
    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Market database not initialized")
    return reader


@router.get(
    "/api/watchlist/{id}/prices",
    response_model=WatchlistPricesResponse,
    summary="Get watchlist stock prices",
    description="ウォッチリスト銘柄の最新価格と前日比を取得",
)
def get_watchlist_prices(request: Request, id: int) -> WatchlistPricesResponse:
    pdb = _get_portfolio_db(request)
    reader = _get_market_reader(request)
    service = WatchlistPricesService(reader, pdb)
    try:
        return service.get_prices(id)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from e
        raise HTTPException(status_code=422, detail=msg) from e
    except Exception as e:
        logger.exception("Watchlist prices error")
        raise HTTPException(status_code=500, detail=str(e)) from e
