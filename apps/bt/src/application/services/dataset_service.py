"""
Dataset Service

Dataset 管理操作（list, info, sample, search, delete）のサービス層。
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from src.application.services.dataset_presets import get_preset
from src.application.services.dataset_resolver import DatasetResolver
from src.entrypoints.http.schemas.dataset import (
    DatasetDataCoverage,
    DatasetExpectedRange,
    DatasetFkIntegrity,
    DatasetInfoResponse,
    DatasetListItem,
    DatasetSampleResponse,
    DatasetSearchResponse,
    DatasetSnapshot,
    DatasetSnapshotDateRange,
    DatasetSnapshotValidation,
    DatasetStats,
    DatasetStatsDateRange,
    DatasetStockCountValidation,
    DatasetValidation,
    DatasetValidationDetails,
    SearchResultItem,
)
class DatasetReadable(Protocol):
    def get_dataset_info(self) -> dict[str, str]: ...
    def get_table_counts(self) -> dict[str, int]: ...
    def get_date_range(self) -> dict[str, str] | None: ...
    def get_stock_count(self) -> int: ...
    def get_stocks_with_quotes_count(self) -> int: ...
    def get_stocks_with_margin_count(self) -> int: ...
    def get_stocks_with_statements_count(self) -> int: ...
    def get_fk_orphan_counts(self) -> dict[str, int]: ...
    def get_stocks_without_quotes_count(self) -> int: ...
    def get_sample_codes(self, size: int = 10, seed: int | None = None) -> list[str]: ...
    def search_stocks(
        self, term: str, exact: bool = False, limit: int = 50
    ) -> Sequence[object]: ...


def _read_dataset_metadata(db: DatasetReadable | None) -> tuple[str | None, str | None]:
    if db is None:
        return None, None
    try:
        metadata = db.get_dataset_info()
    except Exception:
        # metadata is optional; keep listing even if dataset_info is missing/corrupted
        return None, None
    return metadata.get("preset"), metadata.get("created_at")


def list_datasets(resolver: DatasetResolver) -> list[DatasetListItem]:
    """利用可能なデータセット一覧を取得"""
    items: list[DatasetListItem] = []
    for name in resolver.list_datasets():
        dataset_path = resolver.get_dataset_path(name)
        try:
            last_modified = _resolve_last_modified(dataset_path)
            preset, created_at = _read_dataset_metadata(resolver.resolve(name))

            items.append(
                DatasetListItem(
                    name=name,
                    path=dataset_path,
                    fileSize=_resolve_path_size(dataset_path),
                    lastModified=datetime.fromtimestamp(last_modified).isoformat(),
                    preset=preset,
                    createdAt=created_at,
                )
            )
        except OSError:
            continue
    return items


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def get_dataset_info(resolver: DatasetResolver, name: str) -> DatasetInfoResponse | None:
    """データセット詳細情報を取得"""
    db = resolver.resolve(name)
    if db is None:
        return None

    dataset_path = resolver.get_dataset_path(name)
    last_modified = _resolve_last_modified(dataset_path)

    info = db.get_dataset_info()
    table_counts = db.get_table_counts()
    date_range = db.get_date_range()
    stock_count = db.get_stock_count()
    stocks_with_quotes = db.get_stocks_with_quotes_count()
    stocks_with_margin = db.get_stocks_with_margin_count()
    stocks_with_statements = db.get_stocks_with_statements_count()
    fk_orphans = db.get_fk_orphan_counts()
    orphan_stocks_count = db.get_stocks_without_quotes_count()

    errors: list[str] = []
    warnings: list[str] = []
    preset_name = info.get("preset")
    preset_config = get_preset(preset_name) if preset_name else None
    expected_stock_count = _parse_int(info.get("stock_count"))
    is_within_expected_stock_count = expected_stock_count is None or expected_stock_count == stock_count
    total_quotes = table_counts.get("stock_data", 0)
    topix_count = table_counts.get("topix_data", 0)
    margin_count = table_counts.get("margin_data", 0)
    statements_count = table_counts.get("statements", 0)
    indices_count = table_counts.get("indices_data", 0)
    has_margin_data = margin_count > 0
    has_topix_data = topix_count > 0
    has_sector_data = indices_count > 0
    has_statements_data = statements_count > 0

    if stock_count == 0:
        errors.append("No stocks found")
    if total_quotes == 0:
        errors.append("No stock data found")
    if stocks_with_quotes == 0:
        warnings.append("No stocks have OHLCV data")
    if not is_within_expected_stock_count:
        warnings.append(
            "Stock count mismatch: "
            f"expected={expected_stock_count}, actual={stock_count}"
        )
    if not has_topix_data and (preset_config is None or preset_config.include_topix):
        warnings.append("No TOPIX data")
    if not has_margin_data and preset_config is not None and preset_config.include_margin:
        warnings.append("No margin data")
    if not has_statements_data and preset_config is not None and preset_config.include_statements:
        warnings.append("No statements data")
    if not has_sector_data and preset_config is not None and preset_config.include_sector_indices:
        warnings.append("No sector index data")
    if orphan_stocks_count > 0:
        warnings.append(f"{orphan_stocks_count} stocks have no OHLCV records")

    if any(count > 0 for count in fk_orphans.values()):
        errors.append("Foreign key integrity issues detected")

    top_level_validation = DatasetValidation(
        isValid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        details=DatasetValidationDetails(
            fkIntegrity=DatasetFkIntegrity(**fk_orphans),
            orphanStocksCount=orphan_stocks_count,
            stockCountValidation=DatasetStockCountValidation(
                preset=preset_name,
                expected=(
                    DatasetExpectedRange(min=expected_stock_count, max=expected_stock_count)
                    if expected_stock_count is not None
                    else None
                ),
                actual=stock_count,
                isWithinRange=is_within_expected_stock_count,
            ),
            dataCoverage=DatasetDataCoverage(
                totalStocks=stock_count,
                stocksWithQuotes=stocks_with_quotes,
                stocksWithStatements=stocks_with_statements,
                stocksWithMargin=stocks_with_margin,
            ),
        ),
    )

    return DatasetInfoResponse(
        name=name,
        path=dataset_path,
        fileSize=_resolve_path_size(dataset_path),
        lastModified=datetime.fromtimestamp(last_modified).isoformat(),
        snapshot=DatasetSnapshot(
            preset=preset_name,
            createdAt=info.get("created_at"),
            totalStocks=stock_count,
            stocksWithQuotes=stocks_with_quotes,
            dateRange=DatasetSnapshotDateRange(**date_range) if date_range else None,
            validation=DatasetSnapshotValidation(
                isValid=top_level_validation.isValid,
                errors=top_level_validation.errors,
                warnings=top_level_validation.warnings,
            ),
        ),
        stats=DatasetStats(
            totalStocks=stock_count,
            totalQuotes=total_quotes,
            dateRange=(
                DatasetStatsDateRange(from_=date_range["min"], to=date_range["max"])
                if date_range
                else DatasetStatsDateRange(from_="-", to="-")
            ),
            hasMarginData=has_margin_data,
            hasTOPIXData=has_topix_data,
            hasSectorData=has_sector_data,
            hasStatementsData=has_statements_data,
            statementsFieldCoverage=None,
        ),
        validation=top_level_validation,
    )


def get_dataset_sample(db: DatasetReadable, count: int = 10, seed: int | None = None) -> DatasetSampleResponse:
    """ランダムサンプル取得"""
    codes = db.get_sample_codes(size=count, seed=seed)
    return DatasetSampleResponse(codes=codes)


def search_dataset(db: DatasetReadable, q: str, limit: int = 50) -> DatasetSearchResponse:
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
    artifact_paths = resolver.get_artifact_paths(name)
    if not artifact_paths:
        return False
    resolver.evict(name)
    for path in artifact_paths:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
    return True


def _resolve_path_size(path: str) -> int:
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _dirs, files in os.walk(path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.isfile(file_path):
                total += os.path.getsize(file_path)
    return total


def _resolve_last_modified(path: str) -> float:
    if os.path.isfile(path):
        return os.path.getmtime(path)

    latest_mtime = os.path.getmtime(path)
    for root, _dirs, files in os.walk(path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.isfile(file_path):
                latest_mtime = max(latest_mtime, os.path.getmtime(file_path))
    return latest_mtime
