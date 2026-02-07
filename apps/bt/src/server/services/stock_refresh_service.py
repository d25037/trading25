"""
Stock Refresh Service

POST /api/db/stocks/refresh のビジネスロジック。
指定銘柄の株価データを JQuants API から再取得して market.db を更新する。
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.db.market_db import METADATA_KEYS, MarketDb
from src.server.db.query_helpers import expand_stock_code, normalize_stock_code
from src.server.schemas.db import RefreshResponse, RefreshStockResult


async def refresh_stocks(
    codes: list[str],
    market_db: MarketDb,
    jquants_client: JQuantsAsyncClient,
) -> RefreshResponse:
    """銘柄データを再取得"""
    total_calls = 0
    total_stored = 0
    results: list[RefreshStockResult] = []
    errors: list[str] = []

    # TOPIX 日付範囲を取得（フィルタ用）
    topix_range = market_db.get_topix_date_range()
    min_date = topix_range["min"] if topix_range else None
    max_date = topix_range["max"] if topix_range else None

    for code in codes:
        normalized = normalize_stock_code(code)
        expanded = expand_stock_code(normalized)
        try:
            data = await jquants_client.get_paginated(
                "/equities/bars/daily",
                params={"code": expanded},
            )
            total_calls += 1

            # TOPIX 日付範囲でフィルタ
            rows: list[dict[str, Any]] = []
            for d in data:
                date = d.get("Date", "")
                if min_date and date < min_date:
                    continue
                if max_date and date > max_date:
                    continue
                rows.append({
                    "code": normalized,
                    "date": date,
                    "open": d.get("Open", 0),
                    "high": d.get("High", 0),
                    "low": d.get("Low", 0),
                    "close": d.get("Close", 0),
                    "volume": d.get("Volume", 0),
                    "adjustment_factor": d.get("AdjustmentFactor"),
                    "created_at": datetime.now(UTC).isoformat(),
                })

            stored = market_db.upsert_stock_data(rows) if rows else 0
            total_stored += stored
            results.append(RefreshStockResult(
                code=normalized,
                success=True,
                recordsFetched=len(data),
                recordsStored=stored,
            ))
        except Exception as e:
            logger.warning(f"Refresh failed for {code}: {e}")
            errors.append(f"{code}: {e}")
            results.append(RefreshStockResult(
                code=normalized,
                success=False,
                error=str(e),
            ))

    # Update metadata
    now_iso = datetime.now(UTC).isoformat()
    market_db.set_sync_metadata(METADATA_KEYS["LAST_STOCKS_REFRESH"], now_iso)

    return RefreshResponse(
        totalStocks=len(codes),
        successCount=sum(1 for r in results if r.success),
        failedCount=sum(1 for r in results if not r.success),
        totalApiCalls=total_calls,
        totalRecordsStored=total_stored,
        results=results,
        errors=errors,
        lastUpdated=now_iso,
    )
