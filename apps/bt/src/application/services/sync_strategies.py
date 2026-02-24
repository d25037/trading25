"""
Sync Strategies

market.db 同期のための 3 つの戦略。
Hono sync-strategies.ts からの移植。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

from loguru import logger

from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.market.market_db import METADATA_KEYS, MarketDb
from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
    stock_code_candidates,
)
from src.entrypoints.http.schemas.db import SyncResult
from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.index_master_catalog import (
    build_index_master_seed_rows,
    get_index_catalog_codes,
)
from src.application.services.stock_data_row_builder import build_stock_data_row


@dataclass
class SyncContext:
    client: JQuantsAsyncClient
    market_db: MarketDb
    cancelled: asyncio.Event
    on_progress: Callable[[str, int, int, str], None]


class SyncStrategy(Protocol):
    async def execute(self, ctx: SyncContext) -> SyncResult: ...
    def estimate_api_calls(self) -> int: ...


_JST = ZoneInfo("Asia/Tokyo")
_PRIME_MARKET_CODES = {"0111", "prime"}
_MAX_FINS_SUMMARY_PAGES = 2000


class IndicesOnlySyncStrategy:
    """指数のみ同期: 指数マスタ + 指数データ（~52 API calls）"""

    def estimate_api_calls(self) -> int:
        return 52

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        errors: list[str] = []

        try:
            # 1. 指数マスタ（ローカルカタログ）を補完
            ctx.on_progress("indices_master", 0, 2, "Syncing index master catalog...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            target_codes = sorted(get_index_catalog_codes() | known_master_codes)

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, 2, f"Fetching data for {len(target_codes)} indices...")
            for code in target_codes:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                try:
                    data = await ctx.client.get_paginated("/indices/bars/daily", params={"code": code})
                    total_calls += 1
                    rows = _convert_indices_data_rows(data, code)
                    if rows:
                        await _upsert_indices_rows_with_master_backfill(
                            ctx,
                            rows,
                            known_master_codes,
                        )
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
        # TOPIX/株価/指数に加えて Prime 全銘柄の /fins/summary(code=...) を含む。
        return 2500

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        dates_processed = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        failed_dates: list[str] = []
        errors: list[str] = []
        stock_rows: list[dict[str, Any]] = []

        try:
            # Step 1: TOPIX
            ctx.on_progress("topix", 0, 6, "Fetching TOPIX data...")
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
            ctx.on_progress("stocks", 1, 6, "Fetching stock master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data = await ctx.client.get_paginated("/equities/master")
            total_calls += 1
            stock_rows = _convert_stock_rows(stocks_data)
            if stock_rows:
                await asyncio.to_thread(ctx.market_db.upsert_stocks, stock_rows)

            # Step 3: Prime fundamentals（初回: code指定フル取得）
            ctx.on_progress("fundamentals", 2, 6, "Fetching Prime fundamentals (full)...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            prime_codes = _extract_prime_codes_from_stock_rows(stock_rows)
            if not prime_codes:
                prime_codes = await asyncio.to_thread(ctx.market_db.get_prime_codes)

            fundamentals_sync = await _sync_fundamentals_initial(ctx, sorted(prime_codes))
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 4: 株価データ（日付ベース、TOPIX 日付を使用）
            ctx.on_progress("stock_data", 3, 6, "Fetching daily stock prices...")
            trading_dates = sorted({r["date"] for r in topix_rows})
            consecutive_failures = 0
            for i, date in enumerate(trading_dates):
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                if i % 50 == 0:
                    ctx.on_progress("stock_data", 3, 6, f"Fetching stock data: {i}/{len(trading_dates)} dates...")
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

            # Step 5: 指数データ
            ctx.on_progress("indices", 4, 6, "Fetching index data...")
            indices_strategy = IndicesOnlySyncStrategy()
            indices_result = await indices_strategy.execute(ctx)
            total_calls += indices_result.totalApiCalls
            errors.extend(indices_result.errors)

            # Step 6: メタデータ更新
            ctx.on_progress("finalize", 5, 6, "Finalizing sync...")
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["INIT_COMPLETED"], "true")
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)
            if failed_dates:
                await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FAILED_DATES"], json.dumps(failed_dates))
            else:
                await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FAILED_DATES"], "[]")

            ctx.on_progress("complete", 6, 6, "Sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=dates_processed,
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                failedDates=failed_dates,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class IncrementalSyncStrategy:
    """増分同期: 最終同期日以降のデータのみ取得"""

    def estimate_api_calls(self) -> int:
        return 120

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        errors: list[str] = []
        stock_rows: list[dict[str, Any]] = []

        try:
            last_sync = ctx.market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
            if not last_sync:
                return SyncResult(success=False, errors=["No last_sync_date found. Run initial sync first."])

            # Step 1: TOPIX（増分）
            ctx.on_progress("topix", 0, 5, "Fetching incremental TOPIX data...")
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
            ctx.on_progress("stocks", 1, 5, "Updating stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data = await ctx.client.get_paginated("/equities/master")
            total_calls += 1
            stock_rows = _convert_stock_rows(stocks_data)
            if stock_rows:
                await asyncio.to_thread(ctx.market_db.upsert_stocks, stock_rows)

            # Step 3: 新しい日付の株価データ
            ctx.on_progress("stock_data", 2, 5, "Fetching new stock data...")
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
            ctx.on_progress("indices", 3, 5, "Fetching incremental index data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            latest_index_dates = ctx.market_db.get_latest_indices_data_dates()
            target_codes = sorted(
                get_index_catalog_codes()
                | set(latest_index_dates.keys())
                | known_master_codes
            )

            for code in target_codes:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

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
                        await _upsert_indices_rows_with_master_backfill(
                            ctx,
                            rows,
                            known_master_codes,
                            discovery_log="Inserted {} discovered index master rows while syncing by code.",
                        )
                except Exception as e:
                    errors.append(f"Index {code}: {e}")
                    logger.warning(f"Index {code} incremental sync error: {e}")

            # code 指定同期の補完として、日付指定で新規コードを探索する。
            latest_index_date = _latest_date(list(latest_index_dates.values()))
            fallback_dates = _extract_dates_after(
                topix_rows,
                latest_index_date,
                include_anchor=True,
            )

            # indices_data が遅れている場合、topix を indices 側アンカーで再取得して候補日を補完する。
            if (
                latest_index_date
                and last_date
                and _is_date_after(last_date, latest_index_date)
            ):
                topix_for_indices = await ctx.client.get_paginated(
                    "/indices/bars/daily/topix",
                    params={"from": _to_jquants_date_param(latest_index_date)},
                )
                total_calls += 1
                topix_dates = [
                    {"date": d.get("Date", "")}
                    for d in topix_for_indices
                    if d.get("Date")
                ]
                fallback_dates = sorted(
                    set(fallback_dates) | set(
                        _extract_dates_after(topix_dates, latest_index_date, include_anchor=True)
                    ),
                    key=_date_sort_key,
                )

            for index_date in fallback_dates:
                if ctx.cancelled.is_set():
                    return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

                try:
                    data = await ctx.client.get_paginated(
                        "/indices/bars/daily",
                        params={"date": _to_jquants_date_param(index_date)},
                    )
                    total_calls += 1
                    rows = _convert_indices_data_rows(data, None)
                    if rows:
                        await _upsert_indices_rows_with_master_backfill(
                            ctx,
                            rows,
                            known_master_codes,
                            discovery_log="Inserted {} discovered index master rows while syncing by date.",
                        )
                except Exception as e:
                    errors.append(f"Index date {index_date}: {e}")
                    logger.warning("Index date {} incremental sync error: {}", index_date, e)

            # Step 5: Prime fundamentals（増分: date 指定 + 欠損補完）
            ctx.on_progress("fundamentals", 4, 5, "Fetching incremental Prime fundamentals...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            prime_codes = _extract_prime_codes_from_stock_rows(stock_rows)
            if not prime_codes:
                prime_codes = await asyncio.to_thread(ctx.market_db.get_prime_codes)

            fundamentals_sync = await _sync_fundamentals_incremental(ctx, sorted(prime_codes))
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # メタデータ更新
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)

            ctx.on_progress("complete", 5, 5, "Incremental sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=len(new_dates),
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


async def _sync_fundamentals_initial(
    ctx: SyncContext,
    prime_codes: list[str],
) -> dict[str, Any]:
    """Prime 銘柄を code 指定でフル同期"""
    api_calls = 0
    updated = 0
    failed_codes: list[str] = []
    errors: list[str] = []

    for idx, code in enumerate(prime_codes):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": 0,
                "errors": errors,
                "cancelled": True,
            }

        if idx > 0 and idx % 100 == 0:
            ctx.on_progress("fundamentals", 2, 6, f"Fetching fundamentals: {idx}/{len(prime_codes)} codes...")

        try:
            data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
            api_calls += page_calls
            rows = convert_fins_summary_rows(data, default_code=code)
            if rows:
                await asyncio.to_thread(ctx.market_db.upsert_statements, rows)
                updated += len(rows)
        except Exception as e:
            failed_codes.append(code)
            errors.append(f"Fundamentals code {code}: {e}")

    latest_disclosed = await asyncio.to_thread(ctx.market_db.get_latest_statement_disclosed_date)
    now_iso = datetime.now(UTC).isoformat()

    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"],
        now_iso,
    )
    if latest_disclosed:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            latest_disclosed,
        )
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
        "[]",
    )
    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
        failed_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": 0,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_fundamentals_incremental(
    ctx: SyncContext,
    prime_codes: list[str],
) -> dict[str, Any]:
    """date 指定増分 + 欠損 Prime 補完"""
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_dates: list[str] = []
    failed_codes: list[str] = []
    prime_code_set = set(prime_codes)

    previous_failed_dates = _normalize_date_list(
        _load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"])
    )
    previous_failed_codes = _collect_unique_codes(
        _load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"])
    )

    anchor = (
        ctx.market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or ctx.market_db.get_latest_statement_disclosed_date()
    )
    date_targets = _build_incremental_date_targets(anchor, previous_failed_dates)

    for idx, disclosed_date in enumerate(date_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": idx,
                "errors": errors,
                "cancelled": True,
            }

        if idx > 0 and idx % 30 == 0:
            ctx.on_progress("fundamentals", 4, 5, f"Fetching fundamentals dates: {idx}/{len(date_targets)}...")

        try:
            data, page_calls = await _fetch_fins_summary_paginated(
                ctx.client,
                {"date": _to_jquants_date_param(disclosed_date)},
            )
            api_calls += page_calls
            rows = convert_fins_summary_rows(data)
            rows = [row for row in rows if row.get("code") in prime_code_set]
            if rows:
                await asyncio.to_thread(ctx.market_db.upsert_statements, rows)
                updated += len(rows)
        except Exception as e:
            failed_dates.append(disclosed_date)
            errors.append(f"Fundamentals date {disclosed_date}: {e}")

    statement_codes = await asyncio.to_thread(ctx.market_db.get_statement_codes)
    missing_prime_codes = sorted(set(prime_codes) - set(statement_codes))
    code_targets = _collect_unique_codes(previous_failed_codes + missing_prime_codes)

    for idx, code in enumerate(code_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": len(date_targets),
                "errors": errors,
                "cancelled": True,
            }

        if idx > 0 and idx % 100 == 0:
            ctx.on_progress("fundamentals", 4, 5, f"Backfilling fundamentals: {idx}/{len(code_targets)} codes...")

        try:
            data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
            api_calls += page_calls
            rows = convert_fins_summary_rows(data, default_code=code)
            rows = [row for row in rows if row.get("code") in prime_code_set]
            if rows:
                await asyncio.to_thread(ctx.market_db.upsert_statements, rows)
                updated += len(rows)
        except Exception as e:
            failed_codes.append(code)
            errors.append(f"Fundamentals code {code}: {e}")

    latest_disclosed = await asyncio.to_thread(ctx.market_db.get_latest_statement_disclosed_date)
    now_iso = datetime.now(UTC).isoformat()
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"],
        now_iso,
    )
    if latest_disclosed:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            latest_disclosed,
        )

    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"],
        failed_dates,
    )
    await _save_metadata_json_list(
        ctx,
        METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"],
        failed_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": len(date_targets),
        "errors": errors,
        "cancelled": False,
    }


async def _fetch_fins_summary_paginated(
    client: JQuantsAsyncClient,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    """`/fins/summary` を pagination_key が尽きるまで取得する。"""
    current_params = dict(params)
    all_rows: list[dict[str, Any]] = []
    api_calls = 0

    while True:
        body = await client.get("/fins/summary", params=current_params)
        api_calls += 1

        page_rows = _extract_list_items(body, preferred_keys=("data",))
        all_rows.extend(page_rows)

        pagination_key = body.get("pagination_key")
        if not pagination_key:
            break

        if api_calls >= _MAX_FINS_SUMMARY_PAGES:
            raise RuntimeError("fins/summary pagination exceeded safety limit")

        current_params = {**current_params, "pagination_key": pagination_key}

    return all_rows, api_calls


async def _fetch_fins_summary_by_code(
    client: JQuantsAsyncClient,
    code: str,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch /fins/summary by trying both 5-digit and 4-digit code formats.

    dataset builder は 5桁コードで fetch しているため、
    market sync も 5桁優先で試行し、空結果やエラー時のみ 4桁へフォールバックする。
    """
    normalized_code = normalize_stock_code(code)
    candidates = list(
        dict.fromkeys(
            (
                expand_stock_code(normalized_code),
                *stock_code_candidates(normalized_code),
            )
        )
    )

    total_calls = 0
    last_error: Exception | None = None
    saw_empty_payload = False

    for candidate in candidates:
        try:
            data, page_calls = await _fetch_fins_summary_paginated(
                client,
                {"code": candidate},
            )
            total_calls += page_calls
            if data:
                return data, total_calls
            saw_empty_payload = True
            continue
        except Exception as exc:
            last_error = exc
            continue

    if saw_empty_payload:
        return [], total_calls

    if last_error is None:
        raise RuntimeError(f"fins/summary code fetch failed for {code}")
    raise last_error


def _extract_prime_codes_from_stock_rows(stock_rows: list[dict[str, Any]]) -> set[str]:
    codes: set[str] = set()
    for row in stock_rows:
        market_code = row.get("market_code")
        if not _is_prime_market_code(market_code):
            continue
        code = normalize_stock_code(str(row.get("code", "")))
        if code:
            codes.add(code)
    return codes


def _is_prime_market_code(value: Any) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in _PRIME_MARKET_CODES


def _load_metadata_json_list(market_db: MarketDb, key: str) -> list[str]:
    raw = market_db.get_sync_metadata(key)
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(v) for v in loaded if isinstance(v, str) or isinstance(v, int)]


async def _save_metadata_json_list(
    ctx: SyncContext,
    key: str,
    values: list[str],
) -> None:
    deduped = _dedupe_preserve_order(values)
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        key,
        json.dumps(deduped, ensure_ascii=False),
    )


def _collect_unique_codes(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = normalize_stock_code(str(value).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _normalize_date_list(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        parsed = _parse_date(value)
        if parsed is None:
            continue
        normalized = parsed.isoformat()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return sorted(deduped, key=_date_sort_key)


def _build_incremental_date_targets(anchor: str | None, retry_dates: list[str]) -> list[str]:
    targets: list[str] = list(retry_dates)
    seen = set(targets)

    anchor_date = _parse_date(anchor) if anchor else None
    today_jst = datetime.now(_JST).date()

    if anchor_date is not None:
        current = anchor_date + timedelta(days=1)
        while current <= today_jst:
            value = current.isoformat()
            if value not in seen:
                seen.add(value)
                targets.append(value)
            current += timedelta(days=1)

    return targets


def get_strategy(resolved_mode: str) -> SyncStrategy:
    """モード名から戦略インスタンスを返す"""
    if resolved_mode == "initial":
        return InitialSyncStrategy()
    elif resolved_mode == "incremental":
        return IncrementalSyncStrategy()
    elif resolved_mode == "indices-only":
        return IndicesOnlySyncStrategy()
    return InitialSyncStrategy()


async def _seed_index_master_from_catalog(ctx: SyncContext) -> set[str]:
    seed_rows = build_index_master_seed_rows()
    if seed_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, seed_rows)
    return ctx.market_db.get_index_master_codes()


async def _upsert_indices_rows_with_master_backfill(
    ctx: SyncContext,
    rows: list[dict[str, Any]],
    known_master_codes: set[str],
    *,
    discovery_log: str | None = None,
) -> None:
    missing_master_rows = _build_fallback_index_master_rows(rows, known_master_codes)
    if missing_master_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, missing_master_rows)
        known_master_codes.update(
            str(row["code"])
            for row in missing_master_rows
            if row.get("code")
        )
        if discovery_log:
            logger.warning(discovery_log, len(missing_master_rows))

    await asyncio.to_thread(ctx.market_db.upsert_indices_data, rows)


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
    code = (
        index_info.get("code")
        or index_info.get("Code")
        or index_info.get("index_code")
        or index_info.get("indexCode")
    )
    return _normalize_index_code(code)


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


def _convert_indices_data_rows(data: list[dict[str, Any]], code: str | None) -> list[dict[str, Any]]:
    """JQuants 指数データ → DB 行。日付欠損行はスキップ。"""
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    skipped_missing_date = 0
    skipped_missing_code = 0

    for d in data:
        row_code = _extract_index_code(d) or _normalize_index_code(code)
        if not row_code:
            skipped_missing_code += 1
            continue

        row_date = d.get("Date") or d.get("date") or ""
        if not row_date:
            skipped_missing_date += 1
            continue

        rows.append({
            "code": row_code,
            "date": row_date,
            "open": d.get("O", d.get("open")),
            "high": d.get("H", d.get("high")),
            "low": d.get("L", d.get("low")),
            "close": d.get("C", d.get("close")),
            "sector_name": d.get("SectorName", d.get("sector_name")),
            "created_at": created_at,
        })

    if skipped_missing_date > 0:
        logger.warning("Skipped {} index rows with missing date (code={})", skipped_missing_date, code)
    if skipped_missing_code > 0:
        logger.warning("Skipped {} index rows with missing code", skipped_missing_code)
    return rows


def _build_fallback_index_master_rows(
    rows: list[dict[str, Any]],
    known_codes: set[str],
) -> list[dict[str, Any]]:
    """index_master 欠損コード向けに最小プレースホルダ行を作る。"""
    missing_master_items_by_code: dict[str, dict[str, Any]] = {}

    for row in rows:
        code = _normalize_index_code(row.get("code"))
        if not code or code in known_codes:
            continue

        row_name = str(row.get("sector_name") or "").strip()
        placeholder_name = row_name or code
        row_date = str(row.get("date") or "").strip() or None

        existing = missing_master_items_by_code.get(code)
        if existing is None:
            missing_master_items_by_code[code] = {
                "code": code,
                "name": placeholder_name,
                "category": "unknown",
                "data_start_date": row_date,
            }
            continue

        if existing["name"] == code and row_name:
            existing["name"] = row_name
        if existing["data_start_date"] is None and row_date:
            existing["data_start_date"] = row_date

    return _convert_index_master_rows(list(missing_master_items_by_code.values()))


def _normalize_index_code(value: Any) -> str:
    """指数コードを文字列化し、数字コードは 4 桁に正規化する。"""
    text = str(value).strip() if value is not None else ""
    if not text:
        return ""
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text.upper()


def _latest_date(values: list[str]) -> str | None:
    """日付文字列配列から最新日を返す。"""
    latest: str | None = None
    for value in values:
        if not value:
            continue
        if latest is None or _is_date_after(value, latest):
            latest = value
    return latest


def _extract_dates_after(
    rows: list[dict[str, Any]],
    anchor_date: str | None,
    *,
    include_anchor: bool = False,
) -> list[str]:
    """行配列から anchor_date 以降（またはより後）の日付を抽出して昇順化する。"""
    dates: set[str] = set()
    for row in rows:
        row_date = row.get("date")
        if not row_date:
            continue
        if anchor_date:
            if include_anchor:
                if _is_date_after(anchor_date, row_date):
                    continue
            elif not _is_date_after(row_date, anchor_date):
                continue
        dates.add(row_date)
    return sorted(dates, key=_date_sort_key)


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
