"""
Portfolio API Routes

Hono Portfolio API 互換の CRUD + Performance エンドポイント。
Phase 3E-1: 11 CRUD EP, Phase 3E-2: 1 Performance EP
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy.exc import IntegrityError

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.portfolio_db import PortfolioDb
from src.entrypoints.http.schemas.portfolio import (
    DeleteResponse,
    PortfolioCodesResponse,
    PortfolioCreateRequest,
    PortfolioDetailResponse,
    PortfolioItemCreateRequest,
    PortfolioItemResponse,
    PortfolioItemUpdateRequest,
    PortfolioResponse,
    PortfolioSummaryResponse,
    PortfolioUpdateRequest,
    StockDeleteResponse,
    StockUpdateRequest,
)
from src.entrypoints.http.schemas.portfolio_performance import PortfolioPerformanceResponse
from src.application.services.portfolio_performance_service import PortfolioPerformanceService

router = APIRouter(tags=["Portfolio"])


def _get_portfolio_db(request: Request) -> PortfolioDb:
    pdb = getattr(request.app.state, "portfolio_db", None)
    if pdb is None:
        raise HTTPException(status_code=422, detail="Portfolio database not initialized")
    return pdb


def _row_to_portfolio(row: Any) -> dict[str, Any]:
    """Row → PortfolioResponse 用 dict"""
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "createdAt": row.created_at or "",
        "updatedAt": row.updated_at or "",
    }


def _row_to_item(row: Any) -> dict[str, Any]:
    """Row → PortfolioItemResponse 用 dict"""
    return {
        "id": row.id,
        "portfolioId": row.portfolio_id,
        "code": row.code,
        "companyName": row.company_name,
        "quantity": row.quantity,
        "purchasePrice": row.purchase_price,
        "purchaseDate": row.purchase_date,
        "account": row.account,
        "notes": row.notes,
        "createdAt": row.created_at or "",
        "updatedAt": row.updated_at or "",
    }


# ===== Portfolio CRUD (5 EP) =====


@router.get(
    "/api/portfolio",
    summary="List all portfolios",
    description="ポートフォリオ一覧を stockCount/totalShares 付きで取得",
)
def list_portfolios(request: Request) -> dict[str, list[PortfolioSummaryResponse]]:
    pdb = _get_portfolio_db(request)
    summaries = pdb.list_portfolio_summaries()
    return {
        "portfolios": [
            PortfolioSummaryResponse(
                id=s["id"],
                name=s["name"],
                description=s["description"],
                stockCount=s["stock_count"],
                totalShares=s["total_shares"],
                createdAt=s["created_at"] or "",
                updatedAt=s["updated_at"] or "",
            )
            for s in summaries
        ]
    }


@router.post(
    "/api/portfolio",
    response_model=PortfolioResponse,
    status_code=201,
    summary="Create a portfolio",
)
def create_portfolio(request: Request, body: PortfolioCreateRequest) -> JSONResponse:
    pdb = _get_portfolio_db(request)
    try:
        row = pdb.create_portfolio(body.name, body.description)
    except IntegrityError as e:
        msg = str(e.orig)
        if "portfolios.name" in msg:
            raise HTTPException(status_code=409, detail="Portfolio name already exists") from e
        raise  # pragma: no cover
    return JSONResponse(status_code=201, content=_row_to_portfolio(row))


@router.get(
    "/api/portfolio/{id}",
    response_model=PortfolioDetailResponse,
    summary="Get portfolio with items",
)
def get_portfolio(request: Request, id: int) -> PortfolioDetailResponse:
    pdb = _get_portfolio_db(request)
    row = pdb.get_portfolio(id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")
    items = pdb.list_items(id)
    data = _row_to_portfolio(row)
    data["items"] = [_row_to_item(item) for item in items]
    return PortfolioDetailResponse(**data)


@router.put(
    "/api/portfolio/{id}",
    response_model=PortfolioResponse,
    summary="Update a portfolio",
)
def update_portfolio(request: Request, id: int, body: PortfolioUpdateRequest) -> PortfolioResponse:
    pdb = _get_portfolio_db(request)
    # Determine which fields to update
    kwargs: dict[str, Any] = {}
    if body.name is not None:
        kwargs["name"] = body.name
    # Use model_fields_set to distinguish "description not sent" vs "description: null"
    if "description" in body.model_fields_set:
        kwargs["description"] = body.description
    try:
        row = pdb.update_portfolio(id, **kwargs)
    except IntegrityError as e:
        msg = str(e.orig)
        if "portfolios.name" in msg:
            raise HTTPException(status_code=409, detail="Portfolio name already exists") from e
        raise  # pragma: no cover
    if row is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")
    return PortfolioResponse(**_row_to_portfolio(row))


@router.delete(
    "/api/portfolio/{id}",
    response_model=DeleteResponse,
    summary="Delete a portfolio",
)
def delete_portfolio(request: Request, id: int) -> DeleteResponse:
    pdb = _get_portfolio_db(request)
    if not pdb.delete_portfolio(id):
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")
    return DeleteResponse(message="Portfolio deleted successfully")


# ===== Portfolio Items (3 EP) =====


@router.post(
    "/api/portfolio/{id}/items",
    response_model=PortfolioItemResponse,
    status_code=201,
    summary="Add item to portfolio",
)
def add_item(request: Request, id: int, body: PortfolioItemCreateRequest) -> JSONResponse:
    pdb = _get_portfolio_db(request)
    # Check portfolio exists
    if pdb.get_portfolio(id) is None:
        raise HTTPException(status_code=404, detail=f"Portfolio {id} not found")
    try:
        row = pdb.add_item(
            id,
            body.code,
            body.companyName,
            body.quantity,
            body.purchasePrice,
            body.purchaseDate,
            account=body.account,
            notes=body.notes,
        )
    except IntegrityError as e:
        msg = str(e.orig)
        if "portfolio_items" in msg and "code" in msg:
            raise HTTPException(status_code=409, detail="Stock already exists in portfolio") from e
        raise  # pragma: no cover
    return JSONResponse(status_code=201, content=_row_to_item(row))


@router.put(
    "/api/portfolio/{id}/items/{itemId}",
    response_model=PortfolioItemResponse,
    summary="Update portfolio item",
)
def update_item(request: Request, id: int, itemId: int, body: PortfolioItemUpdateRequest) -> PortfolioItemResponse:
    pdb = _get_portfolio_db(request)
    # Verify item belongs to this portfolio
    existing = pdb.get_item(itemId)
    if existing is None or existing.portfolio_id != id:
        raise HTTPException(status_code=404, detail=f"Item {itemId} not found in portfolio {id}")
    kwargs: dict[str, Any] = {}
    for field in ("quantity", "purchasePrice", "purchaseDate", "account", "notes"):
        val = getattr(body, field)
        if val is not None:
            # Convert camelCase to snake_case for DB
            db_field = {
                "purchasePrice": "purchase_price",
                "purchaseDate": "purchase_date",
            }.get(field, field)
            kwargs[db_field] = val
    row = pdb.update_item(itemId, **kwargs)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Item {itemId} not found")  # pragma: no cover
    return PortfolioItemResponse(**_row_to_item(row))


@router.delete(
    "/api/portfolio/{id}/items/{itemId}",
    response_model=DeleteResponse,
    summary="Delete portfolio item",
)
def delete_item(request: Request, id: int, itemId: int) -> DeleteResponse:
    pdb = _get_portfolio_db(request)
    # Verify item belongs to this portfolio
    existing = pdb.get_item(itemId)
    if existing is None or existing.portfolio_id != id:
        raise HTTPException(status_code=404, detail=f"Item {itemId} not found in portfolio {id}")
    pdb.delete_item(itemId)
    return DeleteResponse(message="Item deleted successfully")


# ===== Portfolio Name-based Ops (3 EP) =====


@router.put(
    "/api/portfolio/{portfolioName}/stocks/{code}",
    response_model=PortfolioItemResponse,
    summary="Upsert stock by portfolio name",
)
def upsert_stock(
    request: Request, portfolioName: str, code: str, body: StockUpdateRequest
) -> PortfolioItemResponse:
    pdb = _get_portfolio_db(request)
    # Build kwargs for upsert
    company_name = body.companyName or ""
    quantity = body.quantity or 0
    purchase_price = body.purchasePrice or 0.0
    purchase_date = body.purchaseDate or ""
    kwargs: dict[str, Any] = {}
    if body.account is not None:
        kwargs["account"] = body.account
    if body.notes is not None:
        kwargs["notes"] = body.notes
    try:
        row = pdb.upsert_stock(
            portfolioName, code, company_name, quantity, purchase_price, purchase_date, **kwargs
        )
    except IntegrityError as e:
        logger.error(f"IntegrityError in upsert_stock: {e}")
        raise HTTPException(status_code=409, detail="Stock already exists in portfolio") from e
    return PortfolioItemResponse(**_row_to_item(row))


@router.delete(
    "/api/portfolio/{portfolioName}/stocks/{code}",
    response_model=StockDeleteResponse,
    summary="Delete stock by portfolio name",
)
def delete_stock(request: Request, portfolioName: str, code: str) -> StockDeleteResponse:
    pdb = _get_portfolio_db(request)
    # Need to get the item before deletion for deletedItem response
    portfolio = pdb.get_portfolio_by_name(portfolioName)
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio '{portfolioName}' not found")
    item = pdb.get_item_by_code(portfolio.id, code)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Stock {code} not found in portfolio '{portfolioName}'")
    item_data = _row_to_item(item)
    pdb.delete_item(item.id)
    return StockDeleteResponse(
        message="Stock removed successfully",
        deletedItem=PortfolioItemResponse(**item_data),
    )


@router.get(
    "/api/portfolio/{portfolioName}/codes",
    response_model=PortfolioCodesResponse,
    summary="Get stock codes by portfolio name",
)
def get_portfolio_codes(request: Request, portfolioName: str) -> PortfolioCodesResponse:
    pdb = _get_portfolio_db(request)
    codes = pdb.get_portfolio_codes(portfolioName)
    return PortfolioCodesResponse(name=portfolioName, codes=codes)


# ===== Performance (Phase 3E-2) =====


def _get_market_reader(request: Request) -> MarketDbReader:
    reader = getattr(request.app.state, "market_reader", None)
    if reader is None:
        raise HTTPException(status_code=422, detail="Market database not initialized")
    return reader


@router.get(
    "/api/portfolio/{id}/performance",
    response_model=PortfolioPerformanceResponse,
    summary="Get portfolio performance",
    description="P&L、ベンチマーク比較、時系列リターンを計算",
)
def get_performance(
    request: Request,
    id: int,
    benchmarkCode: str = "0000",
    lookbackDays: int = 252,
) -> PortfolioPerformanceResponse:
    pdb = _get_portfolio_db(request)
    reader = _get_market_reader(request)
    service = PortfolioPerformanceService(reader, pdb)
    try:
        return service.analyze(id, benchmark_code=benchmarkCode, lookback_days=lookbackDays)
    except ValueError as e:
        msg = str(e)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from e
        raise HTTPException(status_code=422, detail=msg) from e
    except Exception as e:
        logger.exception("Portfolio performance error")
        raise HTTPException(status_code=500, detail=str(e)) from e
