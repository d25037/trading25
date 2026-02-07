"""
Sync Strategies

market.db 同期のための 3 つの戦略。
Hono sync-strategies.ts からの移植。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

from loguru import logger

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.db.market_db import METADATA_KEYS, MarketDb
from src.server.db.query_helpers import normalize_stock_code
from src.server.schemas.db import SyncResult


@dataclass
class SyncContext:
    client: JQuantsAsyncClient
    market_db: MarketDb
    cancelled: asyncio.Event
    on_progress: Callable[[str, int, int, str], None]


class SyncStrategy(Protocol):
    async def execute(self, ctx: SyncContext) -> SyncResult: ...
    def estimate_api_calls(self) -> int: ...


class IndicesOnlySyncStrategy:
    """指数のみ同期: 指数マスタ + 指数データ（~52 API calls）"""

    def estimate_api_calls(self) -> int:
        return 52

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        errors: list[str] = []

        try:
            # 1. 指数マスタ取得
            ctx.on_progress("indices_master", 0, 2, "Fetching index master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            body = await ctx.client.get("/indices")
            total_calls += 1
            indices_list = body.get("indices", [])

            # index_master に保存
            master_rows = []
            for idx in indices_list:
                master_rows.append({
                    "code": idx.get("code", ""),
                    "name": idx.get("name", ""),
                    "name_english": idx.get("name_english"),
                    "category": idx.get("category", ""),
                    "data_start_date": idx.get("data_start_date"),
                    "created_at": datetime.now(UTC).isoformat(),
                })
            if master_rows:
                await asyncio.to_thread(ctx.market_db.upsert_index_master, master_rows)

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, 2, f"Fetching data for {len(indices_list)} indices...")
            for _i, idx in enumerate(indices_list):
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                code = idx.get("code", "")
                try:
                    data = await ctx.client.get_paginated("/indices/bars/daily", params={"code": code})
                    total_calls += 1
                    rows = []
                    for d in data:
                        rows.append({
                            "code": code,
                            "date": d.get("Date", ""),
                            "open": d.get("Open"),
                            "high": d.get("High"),
                            "low": d.get("Low"),
                            "close": d.get("Close"),
                            "sector_name": d.get("SectorName"),
                            "created_at": datetime.now(UTC).isoformat(),
                        })
                    if rows:
                        await asyncio.to_thread(ctx.market_db.upsert_indices_data, rows)
                except Exception as e:
                    errors.append(f"Index {code}: {e}")
                    logger.warning(f"Index {code} sync error: {e}")

            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class InitialSyncStrategy:
    """初回同期: TOPIX + 全銘柄 + 株価データ + 指数データ"""

    def estimate_api_calls(self) -> int:
        return 552

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        dates_processed = 0
        failed_dates: list[str] = []
        errors: list[str] = []

        try:
            # Step 1: TOPIX
            ctx.on_progress("topix", 0, 5, "Fetching TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            topix_data = await ctx.client.get_paginated("/indices/topix")
            total_calls += 1
            topix_rows = [
                {
                    "date": d.get("Date", ""),
                    "open": d.get("Open", 0),
                    "high": d.get("High", 0),
                    "low": d.get("Low", 0),
                    "close": d.get("Close", 0),
                    "created_at": datetime.now(UTC).isoformat(),
                }
                for d in topix_data
            ]
            if topix_rows:
                await asyncio.to_thread(ctx.market_db.upsert_topix_data, topix_rows)
            dates_processed = len(topix_rows)

            # Step 2: 銘柄マスタ
            ctx.on_progress("stocks", 1, 5, "Fetching stock master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data = await ctx.client.get_paginated("/equities/master")
            total_calls += 1
            stock_rows = _convert_stock_rows(stocks_data)
            if stock_rows:
                await asyncio.to_thread(ctx.market_db.upsert_stocks, stock_rows)

            # Step 3: 株価データ（日付ベース、TOPIX 日付を使用）
            ctx.on_progress("stock_data", 2, 5, "Fetching daily stock prices...")
            trading_dates = sorted({r["date"] for r in topix_rows})
            consecutive_failures = 0
            for i, date in enumerate(trading_dates):
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                if i % 50 == 0:
                    ctx.on_progress("stock_data", 2, 5, f"Fetching stock data: {i}/{len(trading_dates)} dates...")
                try:
                    data = await ctx.client.get_paginated("/equities/bars/daily", params={"date": date})
                    total_calls += 1
                    rows = _convert_stock_data_rows(data)
                    if rows:
                        await asyncio.to_thread(ctx.market_db.upsert_stock_data, rows)
                        stocks_updated += len(rows)
                    consecutive_failures = 0
                except Exception:
                    failed_dates.append(date)
                    consecutive_failures += 1
                    if consecutive_failures >= 5:
                        errors.append(f"Too many consecutive failures at {date}")
                        break

            # Step 4: 指数データ
            ctx.on_progress("indices", 3, 5, "Fetching index data...")
            indices_strategy = IndicesOnlySyncStrategy()
            indices_result = await indices_strategy.execute(ctx)
            total_calls += indices_result.totalApiCalls
            errors.extend(indices_result.errors)

            # Step 5: メタデータ更新
            ctx.on_progress("finalize", 4, 5, "Finalizing sync...")
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["INIT_COMPLETED"], "true")
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)
            if failed_dates:
                import json
                await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FAILED_DATES"], json.dumps(failed_dates))

            ctx.on_progress("complete", 5, 5, "Sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=dates_processed,
                failedDates=failed_dates,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class IncrementalSyncStrategy:
    """増分同期: 最終同期日以降のデータのみ取得"""

    def estimate_api_calls(self) -> int:
        return 5

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        errors: list[str] = []

        try:
            last_sync = ctx.market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
            if not last_sync:
                return SyncResult(success=False, errors=["No last_sync_date found. Run initial sync first."])

            # Step 1: TOPIX（増分）
            ctx.on_progress("topix", 0, 3, "Fetching incremental TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            last_date = ctx.market_db.get_latest_trading_date()
            params: dict[str, Any] = {}
            if last_date:
                params["from"] = last_date

            topix_data = await ctx.client.get_paginated("/indices/topix", params=params)
            total_calls += 1
            topix_rows = [
                {
                    "date": d.get("Date", ""),
                    "open": d.get("Open", 0),
                    "high": d.get("High", 0),
                    "low": d.get("Low", 0),
                    "close": d.get("Close", 0),
                    "created_at": datetime.now(UTC).isoformat(),
                }
                for d in topix_data
            ]
            if topix_rows:
                await asyncio.to_thread(ctx.market_db.upsert_topix_data, topix_rows)

            # Step 2: 銘柄マスタ更新
            ctx.on_progress("stocks", 1, 3, "Updating stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data = await ctx.client.get_paginated("/equities/master")
            total_calls += 1
            stock_rows = _convert_stock_rows(stocks_data)
            if stock_rows:
                await asyncio.to_thread(ctx.market_db.upsert_stocks, stock_rows)

            # Step 3: 新しい日付の株価データ
            ctx.on_progress("stock_data", 2, 3, "Fetching new stock data...")
            new_dates = sorted({r["date"] for r in topix_rows if last_date and r["date"] > last_date})
            for date in new_dates:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                try:
                    data = await ctx.client.get_paginated("/equities/bars/daily", params={"date": date})
                    total_calls += 1
                    rows = _convert_stock_data_rows(data)
                    if rows:
                        await asyncio.to_thread(ctx.market_db.upsert_stock_data, rows)
                        stocks_updated += len(rows)
                except Exception as e:
                    errors.append(f"Date {date}: {e}")

            # メタデータ更新
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)

            ctx.on_progress("complete", 3, 3, "Incremental sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=len(new_dates),
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


def get_strategy(resolved_mode: str) -> SyncStrategy:
    """モード名から戦略インスタンスを返す"""
    if resolved_mode == "initial":
        return InitialSyncStrategy()
    elif resolved_mode == "incremental":
        return IncrementalSyncStrategy()
    elif resolved_mode == "indices-only":
        return IndicesOnlySyncStrategy()
    return InitialSyncStrategy()


def _convert_stock_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 銘柄マスタ → DB 行"""
    rows = []
    for d in data:
        code = normalize_stock_code(d.get("Code", ""))
        if not code:
            continue
        rows.append({
            "code": code,
            "company_name": d.get("CompanyName", ""),
            "company_name_english": d.get("CompanyNameEnglish"),
            "market_code": d.get("MarketCode", ""),
            "market_name": d.get("MarketCodeName", ""),
            "sector_17_code": d.get("Sector17Code", ""),
            "sector_17_name": d.get("Sector17CodeName", ""),
            "sector_33_code": d.get("Sector33Code", ""),
            "sector_33_name": d.get("Sector33CodeName", ""),
            "scale_category": d.get("ScaleCategory"),
            "listed_date": d.get("Date", ""),
            "created_at": datetime.now(UTC).isoformat(),
        })
    return rows


def _convert_stock_data_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 株価データ → DB 行"""
    rows = []
    for d in data:
        code = normalize_stock_code(d.get("Code", ""))
        if not code:
            continue
        rows.append({
            "code": code,
            "date": d.get("Date", ""),
            "open": d.get("Open", 0),
            "high": d.get("High", 0),
            "low": d.get("Low", 0),
            "close": d.get("Close", 0),
            "volume": d.get("Volume", 0),
            "adjustment_factor": d.get("AdjustmentFactor"),
            "created_at": datetime.now(UTC).isoformat(),
        })
    return rows
