"""
Dataset Data Routes (15 EP)

GET /api/dataset/{name}/stocks                      — 銘柄一覧
GET /api/dataset/{name}/stocks/{code}/ohlcv         — 個別 OHLCV
GET /api/dataset/{name}/stocks/ohlcv/batch          — バッチ OHLCV
GET /api/dataset/{name}/topix                       — TOPIX
GET /api/dataset/{name}/indices                     — 指数一覧
GET /api/dataset/{name}/indices/{code}              — 個別指数
GET /api/dataset/{name}/margin                      — 信用取引一覧
GET /api/dataset/{name}/margin/batch                — バッチ信用取引
GET /api/dataset/{name}/margin/{code}               — 個別信用取引
GET /api/dataset/{name}/statements/batch            — バッチ財務諸表
GET /api/dataset/{name}/statements/{code}           — 個別財務諸表
GET /api/dataset/{name}/sectors                     — セクター一覧
GET /api/dataset/{name}/sectors/mapping             — セクターマッピング
GET /api/dataset/{name}/sectors/stock-mapping       — 銘柄→セクター
GET /api/dataset/{name}/sectors/{sectorName}/stocks — セクター銘柄

NOTE: /batch を /{code} より先に登録（ルーティング順序）
"""

from __future__ import annotations

from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Query, Request

from src.models.types import StatementsPeriodType
from src.server.schemas.dataset_data import (
    IndexListItem,
    MarginListItem,
    MarginRecord,
    OHLCRecord,
    OHLCVRecord,
    SectorWithCount,
    StatementRecord,
    StockListItem,
)
from src.server.services.dataset_data_service import (
    batch_to_margin,
    batch_to_ohlcv,
    batch_to_statements,
    rows_to_index_list,
    rows_to_margin_list,
    rows_to_ohlc,
    rows_to_ohlcv,
    rows_to_sector_with_count,
    rows_to_statements,
    rows_to_stock_list,
)
from src.server.services.dataset_resolver import DatasetResolver

router = APIRouter(tags=["Dataset Data"])


def _get_resolver(request: Request) -> DatasetResolver:
    resolver = getattr(request.app.state, "dataset_resolver", None)
    if resolver is None:
        raise HTTPException(status_code=422, detail="Dataset resolver not initialized")
    return resolver


def _resolve_dataset(request: Request, name: str):  # noqa: ANN202
    resolver = _get_resolver(request)
    db = resolver.resolve(name)
    if db is None:
        raise HTTPException(status_code=404, detail=f'Dataset "{name}" not found')
    return db


# --- Stocks ---


@router.get(
    "/api/dataset/{name}/stocks",
    response_model=list[StockListItem],
    summary="Dataset stock list with record counts",
)
def get_dataset_stocks(
    request: Request,
    name: str,
    min_records: int = Query(default=100, ge=0),
) -> list[StockListItem]:
    db = _resolve_dataset(request, name)
    rows = db.get_stock_list_with_counts(min_records=min_records)
    return rows_to_stock_list(rows)


# NOTE: /ohlcv/batch MUST be registered before /{code}/ohlcv
@router.get(
    "/api/dataset/{name}/stocks/ohlcv/batch",
    response_model=dict[str, list[OHLCVRecord]],
    summary="Batch OHLCV data for multiple stocks",
)
def get_dataset_ohlcv_batch(
    request: Request,
    name: str,
    codes: str = Query(..., description="Comma-separated stock codes (max 100)"),
) -> dict[str, list[OHLCVRecord]]:
    db = _resolve_dataset(request, name)
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if len(code_list) > 100:
        raise HTTPException(status_code=400, detail="Too many codes (max 100)")
    batch = db.get_ohlcv_batch(code_list)
    return batch_to_ohlcv(batch)


@router.get(
    "/api/dataset/{name}/stocks/{code}/ohlcv",
    response_model=list[OHLCVRecord],
    summary="OHLCV data for a single stock",
)
def get_dataset_stock_ohlcv(
    request: Request,
    name: str,
    code: str,
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[OHLCVRecord]:
    db = _resolve_dataset(request, name)
    rows = db.get_stock_ohlcv(code, start=start_date, end=end_date)
    return rows_to_ohlcv(rows)


# --- TOPIX ---


@router.get(
    "/api/dataset/{name}/topix",
    response_model=list[OHLCRecord],
    summary="TOPIX data",
)
def get_dataset_topix(
    request: Request,
    name: str,
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[OHLCRecord]:
    db = _resolve_dataset(request, name)
    rows = db.get_topix(start=start_date, end=end_date)
    return rows_to_ohlc(rows)


# --- Indices ---


@router.get(
    "/api/dataset/{name}/indices",
    response_model=list[IndexListItem],
    summary="Available index list with record counts",
)
def get_dataset_indices(
    request: Request,
    name: str,
    min_records: int = Query(default=100, ge=0),
) -> list[IndexListItem]:
    db = _resolve_dataset(request, name)
    rows = db.get_index_list_with_counts(min_records=min_records)
    return rows_to_index_list(rows)


@router.get(
    "/api/dataset/{name}/indices/{code}",
    response_model=list[OHLCRecord],
    summary="Index OHLC data",
)
def get_dataset_index_data(
    request: Request,
    name: str,
    code: str,
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[OHLCRecord]:
    db = _resolve_dataset(request, name)
    rows = db.get_index_data(code, start=start_date, end=end_date)
    return rows_to_ohlc(rows)


# --- Margin ---


# NOTE: /margin/batch MUST be registered before /margin/{code}
@router.get(
    "/api/dataset/{name}/margin/batch",
    response_model=dict[str, list[MarginRecord]],
    summary="Batch margin data",
)
def get_dataset_margin_batch(
    request: Request,
    name: str,
    codes: str = Query(..., description="Comma-separated stock codes (max 100)"),
) -> dict[str, list[MarginRecord]]:
    db = _resolve_dataset(request, name)
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if len(code_list) > 100:
        raise HTTPException(status_code=400, detail="Too many codes (max 100)")
    batch = db.get_margin_batch(code_list)
    return batch_to_margin(batch)


@router.get(
    "/api/dataset/{name}/margin/{code}",
    response_model=list[MarginRecord],
    summary="Margin data for a single stock",
)
def get_dataset_margin(
    request: Request,
    name: str,
    code: str,
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
) -> list[MarginRecord]:
    db = _resolve_dataset(request, name)
    rows = db.get_margin(code=code, start=start_date, end=end_date)
    return [
        MarginRecord(
            date=r.date,
            longMarginVolume=r.long_margin_volume,
            shortMarginVolume=r.short_margin_volume,
        )
        for r in rows
    ]


@router.get(
    "/api/dataset/{name}/margin",
    response_model=list[MarginListItem],
    summary="Margin data summary list",
)
def get_dataset_margin_list(
    request: Request,
    name: str,
    min_records: int = Query(default=10, ge=0),
) -> list[MarginListItem]:
    db = _resolve_dataset(request, name)
    rows = db.get_margin_list(min_records=min_records)
    return rows_to_margin_list(rows)


# --- Statements ---


# NOTE: /statements/batch MUST be registered before /statements/{code}
@router.get(
    "/api/dataset/{name}/statements/batch",
    response_model=dict[str, list[StatementRecord]],
    summary="Batch financial statements",
)
def get_dataset_statements_batch(
    request: Request,
    name: str,
    codes: str = Query(..., description="Comma-separated stock codes (max 100)"),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    period_type: StatementsPeriodType = Query(default="all"),
    actual_only: bool = Query(default=True),
) -> dict[str, list[StatementRecord]]:
    db = _resolve_dataset(request, name)
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if len(code_list) > 100:
        raise HTTPException(status_code=400, detail="Too many codes (max 100)")
    batch = db.get_statements_batch(
        code_list,
        start=start_date,
        end=end_date,
        period_type=period_type,
        actual_only=actual_only,
    )
    return batch_to_statements(batch)


@router.get(
    "/api/dataset/{name}/statements/{code}",
    response_model=list[StatementRecord],
    summary="Financial statements for a single stock",
)
def get_dataset_statements(
    request: Request,
    name: str,
    code: str,
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    period_type: StatementsPeriodType = Query(default="all"),
    actual_only: bool = Query(default=True),
) -> list[StatementRecord]:
    db = _resolve_dataset(request, name)
    rows = db.get_statements(
        code,
        start=start_date,
        end=end_date,
        period_type=period_type,
        actual_only=actual_only,
    )
    return rows_to_statements(rows)


# --- Sectors ---


@router.get(
    "/api/dataset/{name}/sectors",
    response_model=list[SectorWithCount],
    summary="Sector list with stock counts",
)
def get_dataset_sectors(
    request: Request,
    name: str,
) -> list[SectorWithCount]:
    db = _resolve_dataset(request, name)
    rows = db.get_sectors_with_count()
    return rows_to_sector_with_count(rows)


@router.get(
    "/api/dataset/{name}/sectors/mapping",
    response_model=dict[str, str],
    summary="Sector code to name mapping",
)
def get_dataset_sector_mapping(
    request: Request,
    name: str,
) -> dict[str, str]:
    db = _resolve_dataset(request, name)
    return db.get_sector_mapping()


@router.get(
    "/api/dataset/{name}/sectors/stock-mapping",
    response_model=dict[str, list[str]],
    summary="Sector name to stock codes mapping",
)
def get_dataset_sector_stock_mapping(
    request: Request,
    name: str,
) -> dict[str, list[str]]:
    db = _resolve_dataset(request, name)
    return db.get_sector_stock_mapping()


@router.get(
    "/api/dataset/{name}/sectors/{sectorName}/stocks",
    response_model=list[str],
    summary="Stock codes in a sector",
)
def get_dataset_sector_stocks(
    request: Request,
    name: str,
    sectorName: str,
) -> list[str]:
    db = _resolve_dataset(request, name)
    try:
        decoded = unquote(sectorName)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid URI encoding for sector name")
    rows = db.get_sector_stocks(decoded)
    return [r.code for r in rows]
