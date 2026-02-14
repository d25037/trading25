"""
Sync Strategies

market.db 同期のための 3 つの戦略。
Hono sync-strategies.ts からの移植。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Callable, Protocol

from loguru import logger

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.lib.market_db.market_db import METADATA_KEYS, MarketDb
from src.lib.market_db.query_helpers import normalize_stock_code
from src.server.schemas.db import SyncResult
from src.server.services.stock_data_row_builder import build_stock_data_row


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
            indices_list = _extract_list_items(body, preferred_keys=("data", "indices"))

            # index_master に保存
            master_rows = _convert_index_master_rows(indices_list)
            if master_rows:
                await asyncio.to_thread(ctx.market_db.upsert_index_master, master_rows)

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, 2, f"Fetching data for {len(indices_list)} indices...")
            for idx in indices_list:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                code = _extract_index_code(idx)
                if not code:
                    continue
                try:
                    data = await ctx.client.get_paginated("/indices/bars/daily", params={"code": code})
                    total_calls += 1
                    rows = _convert_indices_data_rows(data, code)
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

            topix_data = await ctx.client.get_paginated("/indices/bars/daily/topix")
            total_calls += 1
            topix_rows = [
                {
                    "date": d.get("Date", ""),
                    "open": d.get("O", 0),
                    "high": d.get("H", 0),
                    "low": d.get("L", 0),
                    "close": d.get("C", 0),
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
        return 60

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        errors: list[str] = []

        try:
            last_sync = ctx.market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
            if not last_sync:
                return SyncResult(success=False, errors=["No last_sync_date found. Run initial sync first."])

            # Step 1: TOPIX（増分）
            ctx.on_progress("topix", 0, 4, "Fetching incremental TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # NOTE:
            # stock_data が一部日付で取りこぼされると topix_data より遅れる場合があるため、
            # 増分同期の基準日は stock_data の最新日を優先する。
            last_topix_date = ctx.market_db.get_latest_trading_date()
            last_stock_date = ctx.market_db.get_latest_stock_data_date()
            last_date = last_stock_date or last_topix_date
            params: dict[str, Any] = {}
            if last_date:
                # J-Quants は YYYYMMDD 形式が安定しているため、既存データ形式（YYYY-MM-DD / YYYYMMDD）を吸収する
                params["from"] = _to_jquants_date_param(last_date)

            topix_data = await ctx.client.get_paginated("/indices/bars/daily/topix", params=params)
            total_calls += 1
            topix_rows = [
                {
                    "date": d.get("Date", ""),
                    "open": d.get("O", 0),
                    "high": d.get("H", 0),
                    "low": d.get("L", 0),
                    "close": d.get("C", 0),
                    "created_at": datetime.now(UTC).isoformat(),
                }
                for d in topix_data
            ]
            if topix_rows:
                await asyncio.to_thread(ctx.market_db.upsert_topix_data, topix_rows)

            # Step 2: 銘柄マスタ更新
            ctx.on_progress("stocks", 1, 4, "Updating stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data = await ctx.client.get_paginated("/equities/master")
            total_calls += 1
            stock_rows = _convert_stock_rows(stocks_data)
            if stock_rows:
                await asyncio.to_thread(ctx.market_db.upsert_stocks, stock_rows)

            # Step 3: 新しい日付の株価データ
            ctx.on_progress("stock_data", 2, 4, "Fetching new stock data...")
            if last_date:
                new_dates = sorted(
                    {
                        r["date"]
                        for r in topix_rows
                        if r.get("date") and _is_date_after(r["date"], last_date)
                    },
                    key=_date_sort_key,
                )
            else:
                new_dates = sorted({r["date"] for r in topix_rows if r.get("date")}, key=_date_sort_key)
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

            # Step 4: 指数データ（増分）
            ctx.on_progress("indices", 3, 4, "Fetching incremental index data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            indices_body = await ctx.client.get("/indices")
            total_calls += 1
            indices_list = _extract_list_items(indices_body, preferred_keys=("data", "indices"))

            master_rows = _convert_index_master_rows(indices_list)
            if master_rows:
                await asyncio.to_thread(ctx.market_db.upsert_index_master, master_rows)

            latest_index_dates = ctx.market_db.get_latest_indices_data_dates()
            for idx in indices_list:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

                code = _extract_index_code(idx)
                if not code:
                    continue

                params: dict[str, Any] = {"code": code}
                last_index_date = latest_index_dates.get(code)
                if last_index_date:
                    params["from"] = _to_jquants_date_param(last_index_date)

                try:
                    data = await ctx.client.get_paginated("/indices/bars/daily", params=params)
                    total_calls += 1

                    rows = _convert_indices_data_rows(data, code)
                    if last_index_date:
                        rows = [r for r in rows if _is_date_after(r["date"], last_index_date)]

                    if rows:
                        await asyncio.to_thread(ctx.market_db.upsert_indices_data, rows)
                except Exception as e:
                    errors.append(f"Index {code}: {e}")
                    logger.warning(f"Index {code} incremental sync error: {e}")

            # メタデータ更新
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)

            ctx.on_progress("complete", 4, 4, "Incremental sync complete!")
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
            "company_name": d.get("CoName", ""),
            "company_name_english": d.get("CoNameEn"),
            "market_code": d.get("Mkt", ""),
            "market_name": d.get("MktNm", ""),
            "sector_17_code": d.get("S17", ""),
            "sector_17_name": d.get("S17Nm", ""),
            "sector_33_code": d.get("S33", ""),
            "sector_33_name": d.get("S33Nm", ""),
            "scale_category": d.get("ScaleCat"),
            "listed_date": d.get("Date", ""),
            "created_at": datetime.now(UTC).isoformat(),
        })
    return rows


def _convert_stock_data_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 株価データ → DB 行"""
    rows: list[dict[str, Any]] = []
    skipped = 0
    sample_codes: list[str] = []
    created_at = datetime.now(UTC).isoformat()

    for d in data:
        row = build_stock_data_row(d, created_at=created_at)
        if row is None:
            skipped += 1
            code = normalize_stock_code(d.get("Code", ""))
            if code and code not in sample_codes and len(sample_codes) < 5:
                sample_codes.append(code)
            continue
        rows.append(row)

    if skipped > 0:
        sample = ", ".join(sample_codes) if sample_codes else "unknown"
        logger.warning(
            "Skipped {} daily quotes with incomplete OHLCV data (sample codes: {})",
            skipped,
            sample,
        )
    return rows


def _extract_list_items(
    body: dict[str, Any],
    *,
    preferred_keys: tuple[str, ...] = ("data",),
) -> list[dict[str, Any]]:
    """レスポンスの配列ペイロードをキー揺れ込みで取り出す。"""
    def _coerce_dict_items(value: Any) -> list[dict[str, Any]] | None:
        if not isinstance(value, list):
            return None
        return [item for item in value if isinstance(item, dict)]

    for key in preferred_keys:
        dict_items = _coerce_dict_items(body.get(key))
        if dict_items is not None:
            return dict_items

    for value in body.values():
        dict_items = _coerce_dict_items(value)
        if dict_items is not None:
            return dict_items

    return []


def _extract_index_code(index_info: dict[str, Any]) -> str:
    """指数コードをキー揺れを吸収して取得。"""
    code = index_info.get("code") or index_info.get("Code") or ""
    return str(code).strip()


def _convert_index_master_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 指数マスタ → DB 行。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    for idx in data:
        code = _extract_index_code(idx)
        if not code:
            continue
        rows.append({
            "code": code,
            "name": idx.get("name") or idx.get("Name") or "",
            "name_english": idx.get("name_english") or idx.get("nameEnglish"),
            "category": idx.get("category") or idx.get("Category") or "",
            "data_start_date": idx.get("data_start_date") or idx.get("dataStartDate"),
            "created_at": created_at,
        })
    return rows


def _convert_indices_data_rows(data: list[dict[str, Any]], code: str) -> list[dict[str, Any]]:
    """JQuants 指数データ → DB 行。日付欠損行はスキップ。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    skipped = 0

    for d in data:
        row_date = d.get("Date") or d.get("date") or ""
        if not row_date:
            skipped += 1
            continue
        rows.append({
            "code": code,
            "date": row_date,
            "open": d.get("O", d.get("open")),
            "high": d.get("H", d.get("high")),
            "low": d.get("L", d.get("low")),
            "close": d.get("C", d.get("close")),
            "sector_name": d.get("SectorName", d.get("sector_name")),
            "created_at": created_at,
        })

    if skipped > 0:
        logger.warning("Skipped {} index rows with missing date (code={})", skipped, code)
    return rows


def _parse_date(value: str) -> date | None:
    """YYYY-MM-DD / YYYYMMDD を date に正規化して返す。"""
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()  # noqa: DTZ007
        return datetime.strptime(text, "%Y-%m-%d").date()  # noqa: DTZ007
    except ValueError:
        return None


def _to_jquants_date_param(value: str) -> str:
    """J-Quants 向けの日付パラメータ（YYYYMMDD）に変換。"""
    parsed = _parse_date(value)
    if parsed is None:
        return value
    return parsed.strftime("%Y%m%d")


def _is_date_after(lhs: str, rhs: str) -> bool:
    """日付文字列（YYYY-MM-DD / YYYYMMDD）の大小比較。"""
    left = _parse_date(lhs)
    right = _parse_date(rhs)
    if left is None or right is None:
        return lhs > rhs
    return left > right


def _date_sort_key(value: str) -> tuple[int, str]:
    """日付ソート用キー（parse 失敗時は末尾に回す）。"""
    parsed = _parse_date(value)
    if parsed is None:
        return (1, value)
    return (0, parsed.isoformat())
