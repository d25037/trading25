"""
Stock Refresh Service

POST /api/db/stocks/refresh のビジネスロジック。
指定銘柄の株価データを JQuants API から再取得して DuckDB time-series を更新する。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from loguru import logger

from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code
from src.entrypoints.http.schemas.db import RefreshResponse, RefreshStockResult
from src.application.services.stock_data_row_builder import build_stock_data_row


class StockRefreshMarketDbLike(Protocol):
    def set_sync_metadata(self, key: str, value: str) -> None: ...
    def mark_stock_adjustments_resolved(self, codes: list[str] | None = None) -> int: ...


class StockRefreshClientLike(Protocol):
    async def get_paginated(
        self,
        path: str,
        params: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]: ...


class StockRefreshTimeSeriesStoreLike(Protocol):
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> Any: ...

    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int: ...
    def index_stock_data(self) -> None: ...


async def refresh_stocks(
    codes: list[str],
    market_db: StockRefreshMarketDbLike,
    time_series_store: StockRefreshTimeSeriesStoreLike,
    jquants_client: StockRefreshClientLike,
    *,
    progress_callback: Callable[[int, int, str], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> RefreshResponse:
    """銘柄データを再取得"""
    total_calls = 0
    total_stored = 0
    results: list[RefreshStockResult] = []
    errors: list[str] = []
    any_rows_published = False
    cancelled = False
    resolved_codes: list[str] = []
    unique_codes = list(dict.fromkeys(codes))
    total_codes = len(unique_codes)

    # TOPIX 日付範囲を取得（フィルタ用）
    inspection = await asyncio.to_thread(time_series_store.inspect)
    min_date = inspection.topix_min
    max_date = inspection.topix_max

    for index, code in enumerate(unique_codes, start=1):
        if cancel_check is not None and cancel_check():
            cancelled = True
            if progress_callback is not None:
                progress_callback(index - 1, total_codes, f"Cancelled stock refresh before stock {index}/{total_codes}")
            break
        normalized = normalize_stock_code(code)
        expanded = expand_stock_code(normalized)
        if progress_callback is not None:
            progress_callback(index - 1, total_codes, f"Refreshing stock {index}/{total_codes}: {normalized}")
        try:
            data = await jquants_client.get_paginated(
                "/equities/bars/daily",
                params={"code": expanded},
            )
            total_calls += 1

            if cancel_check is not None and cancel_check():
                cancelled = True
                if progress_callback is not None:
                    progress_callback(index - 1, total_codes, f"Cancelled stock refresh after fetching stock {index}/{total_codes}: {normalized}")
                break

            # TOPIX 日付範囲でフィルタ
            rows: list[dict[str, Any]] = []
            skipped_rows = 0
            created_at = datetime.now(UTC).isoformat()
            for d in data:
                row = build_stock_data_row(
                    d,
                    normalized_code=normalized,
                    created_at=created_at,
                )
                if row is None:
                    skipped_rows += 1
                    continue

                date = row["date"]
                if min_date and date < min_date:
                    continue
                if max_date and date > max_date:
                    continue
                rows.append(row)

            if skipped_rows > 0:
                logger.warning(
                    "Skipped {} rows with incomplete OHLCV during stock refresh: {}",
                    skipped_rows,
                    normalized,
                )

            stored = 0
            if cancel_check is not None and cancel_check():
                cancelled = True
                if progress_callback is not None:
                    progress_callback(index - 1, total_codes, f"Cancelled stock refresh before publishing stock {index}/{total_codes}: {normalized}")
                break
            elif rows:
                stored = await asyncio.to_thread(time_series_store.publish_stock_data, rows)
                if stored > 0:
                    any_rows_published = True
                    resolved_codes.append(normalized)
                else:
                    failure_message = "No rows were published to the local market snapshot"
                    errors.append(f"{normalized}: {failure_message}")
                    results.append(RefreshStockResult(
                        code=normalized,
                        success=False,
                        recordsFetched=len(data),
                        recordsStored=0,
                        error=failure_message,
                    ))
                    if progress_callback is not None:
                        progress_callback(index, total_codes, f"Refresh failed for stock {index}/{total_codes}: {normalized}")
                    continue
            else:
                failure_message = "No publishable rows matched the local market snapshot date range"
                errors.append(f"{normalized}: {failure_message}")
                results.append(RefreshStockResult(
                    code=normalized,
                    success=False,
                    recordsFetched=len(data),
                    recordsStored=0,
                    error=failure_message,
                ))
                if progress_callback is not None:
                    progress_callback(index, total_codes, f"Refresh failed for stock {index}/{total_codes}: {normalized}")
                continue
            total_stored += stored
            results.append(RefreshStockResult(
                code=normalized,
                success=True,
                recordsFetched=len(data),
                recordsStored=stored,
            ))
            if progress_callback is not None:
                progress_callback(index, total_codes, f"Refreshed stock {index}/{total_codes}: {normalized}")
        except Exception as e:
            logger.warning(f"Refresh failed for {code}: {e}")
            errors.append(f"{code}: {e}")
            results.append(RefreshStockResult(
                code=normalized,
                success=False,
                error=str(e),
            ))
            if progress_callback is not None:
                progress_callback(index, total_codes, f"Refresh failed for stock {index}/{total_codes}: {normalized}")

    if any_rows_published:
        try:
            await asyncio.to_thread(time_series_store.index_stock_data)
        except Exception as e:
            logger.warning("Stock refresh index failed: {}", e)
            errors.append(f"stock_data index: {e}")
        await asyncio.to_thread(
            market_db.mark_stock_adjustments_resolved,
            resolved_codes,
        )
    if cancelled:
        errors.append("Cancelled")

    # Update metadata
    now_iso = datetime.now(UTC).isoformat()
    market_db.set_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"], now_iso)

    return RefreshResponse(
        totalStocks=total_codes,
        successCount=sum(1 for r in results if r.success),
        failedCount=sum(1 for r in results if not r.success),
        totalApiCalls=total_calls,
        totalRecordsStored=total_stored,
        results=results,
        errors=errors,
        lastUpdated=now_iso,
    )
