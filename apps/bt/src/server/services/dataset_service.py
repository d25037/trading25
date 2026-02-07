"""
Dataset Service

Dataset 管理操作（list, info, sample, search, delete）のサービス層。
"""

from __future__ import annotations

import os
from datetime import datetime

from src.server.db.dataset_db import DatasetDb
from src.server.schemas.dataset import (
    DatasetInfoResponse,
    DatasetListItem,
    DatasetSampleResponse,
    DatasetSearchResponse,
    DatasetSnapshot,
    DatasetValidation,
    DateRange,
    SearchResultItem,
)
from src.server.services.dataset_resolver import DatasetResolver


def list_datasets(resolver: DatasetResolver) -> list[DatasetListItem]:
    """利用可能なデータセット一覧を取得"""
    items: list[DatasetListItem] = []
    for name in resolver.list_datasets():
        db_path = resolver.get_db_path(name)
        try:
            stat = os.stat(db_path)
            items.append(
                DatasetListItem(
                    name=name,
                    path=db_path,
                    fileSize=stat.st_size,
                    lastModified=datetime.fromtimestamp(stat.st_mtime).isoformat(),
                )
            )
        except OSError:
            continue
    return items


def get_dataset_info(resolver: DatasetResolver, name: str) -> DatasetInfoResponse | None:
    """データセット詳細情報を取得"""
    db = resolver.resolve(name)
    if db is None:
        return None

    db_path = resolver.get_db_path(name)
    stat = os.stat(db_path)

    info = db.get_dataset_info()
    table_counts = db.get_table_counts()
    date_range = db.get_date_range()
    stock_count = db.get_stock_count()
    stocks_with_quotes = db.get_stocks_with_quotes_count()

    errors: list[str] = []
    warnings: list[str] = []

    if stock_count == 0:
        errors.append("No stocks found")
    if table_counts.get("stock_data", 0) == 0:
        errors.append("No stock data found")
    if stocks_with_quotes == 0:
        warnings.append("No stocks have OHLCV data")
    if table_counts.get("topix_data", 0) == 0:
        warnings.append("No TOPIX data")

    return DatasetInfoResponse(
        name=name,
        path=db_path,
        fileSize=stat.st_size,
        lastModified=datetime.fromtimestamp(stat.st_mtime).isoformat(),
        snapshot=DatasetSnapshot(
            preset=info.get("preset"),
            totalStocks=stock_count,
            stocksWithQuotes=stocks_with_quotes,
            dateRange=DateRange(**date_range) if date_range else None,
            validation=DatasetValidation(
                isValid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
            ),
        ),
    )


def get_dataset_sample(db: DatasetDb, count: int = 10, seed: int | None = None) -> DatasetSampleResponse:
    """ランダムサンプル取得"""
    codes = db.get_sample_codes(size=count, seed=seed)
    return DatasetSampleResponse(codes=codes)


def search_dataset(db: DatasetDb, q: str, limit: int = 50) -> DatasetSearchResponse:
    """銘柄検索"""
    # 完全一致を先に試す
    exact_rows = db.search_stocks(term=q, exact=True, limit=limit)
    partial_rows = db.search_stocks(term=q, exact=False, limit=limit)

    # 重複排除（exact が優先）
    seen: set[str] = set()
    results: list[SearchResultItem] = []

    for row in exact_rows:
        if row.code not in seen:
            seen.add(row.code)
            results.append(SearchResultItem(code=row.code, name=row.company_name, match_type="exact"))

    for row in partial_rows:
        if row.code not in seen and len(results) < limit:
            seen.add(row.code)
            results.append(SearchResultItem(code=row.code, name=row.company_name, match_type="partial"))

    return DatasetSearchResponse(results=results[:limit])


def delete_dataset(resolver: DatasetResolver, name: str) -> bool:
    """データセット削除"""
    db_path = resolver.get_db_path(name)
    if not os.path.exists(db_path):
        return False
    resolver.evict(name)
    os.remove(db_path)
    return True
