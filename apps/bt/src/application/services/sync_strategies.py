"""
Sync Strategies

market.db 同期のための 3 つの戦略。
Hono sync-strategies.ts からの移植。
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Awaitable, Callable, Literal, Protocol, cast
from zoneinfo import ZoneInfo

from loguru import logger

from src.application.services.jquants_bulk_service import (
    BulkFetchPlan,
    BulkFetchResult,
    JQuantsBulkService,
)
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.market.market_db import METADATA_KEYS, MarketDb
from src.infrastructure.db.market.time_series_store import (
    MarketTimeSeriesStore,
    TimeSeriesInspection,
)
from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
    stock_code_candidates,
)
from src.entrypoints.http.schemas.db import SyncResult
from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.ingestion_pipeline import (
    run_ingestion_batch,
    validate_rows_required_fields,
)
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
    time_series_store: MarketTimeSeriesStore | None = None
    bulk_service: JQuantsBulkService | None = None


class SyncStrategy(Protocol):
    async def execute(self, ctx: SyncContext) -> SyncResult: ...
    def estimate_api_calls(self) -> int: ...


_JST = ZoneInfo("Asia/Tokyo")
_PRIME_MARKET_CODES = {"0111", "prime"}
_MAX_FINS_SUMMARY_PAGES = 2000

_FetchMethod = Literal["rest", "bulk"]


@dataclass(frozen=True)
class _StageFetchDecision:
    method: _FetchMethod
    planner_api_calls: int
    estimated_rest_calls: int
    estimated_bulk_calls: int | None
    plan: BulkFetchPlan | None = None


_BULK_STOCK_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "date": "Date",
    "o": "O",
    "open": "O",
    "h": "H",
    "high": "H",
    "l": "L",
    "low": "L",
    "c": "C",
    "close": "C",
    "vo": "Vo",
    "volume": "Vo",
    "adjo": "AdjO",
    "adjopen": "AdjO",
    "adjh": "AdjH",
    "adjhigh": "AdjH",
    "adjl": "AdjL",
    "adjlow": "AdjL",
    "adjc": "AdjC",
    "adjclose": "AdjC",
    "adjvo": "AdjVo",
    "adjvolume": "AdjVo",
    "adjfactor": "AdjFactor",
}

_BULK_INDEX_KEY_ALIASES: dict[str, str] = {
    **_BULK_STOCK_KEY_ALIASES,
    "sectorname": "SectorName",
    "sector_name": "SectorName",
    "indexcode": "Code",
    "index_code": "Code",
}

_BULK_FINS_KEY_ALIASES: dict[str, str] = {
    "code": "Code",
    "discdate": "DiscDate",
    "eps": "EPS",
    "np": "NP",
    "eq": "Eq",
    "curpertype": "CurPerType",
    "doctype": "DocType",
    "nxfeps": "NxFEPS",
    "bps": "BPS",
    "sales": "Sales",
    "op": "OP",
    "odp": "OdP",
    "cfo": "CFO",
    "divann": "DivAnn",
    "divfy": "DivFY",
    "fdivann": "FDivAnn",
    "fdivfy": "FDivFY",
    "nxfdivann": "NxFDivAnn",
    "nxfdivfy": "NxFDivFY",
    "payoutratioann": "PayoutRatioAnn",
    "fpayoutratioann": "FPayoutRatioAnn",
    "nxfpayoutratioann": "NxFPayoutRatioAnn",
    "feps": "FEPS",
    "cfi": "CFI",
    "cff": "CFF",
    "casheq": "CashEq",
    "ta": "TA",
    "shoutfy": "ShOutFY",
    "trshfy": "TrShFY",
}


def _supports_bulk_sync(client: JQuantsAsyncClient) -> bool:
    plan = str(getattr(client, "plan", "")).strip().lower()
    return plan in {"light", "standard", "premium"}


def _get_bulk_service(ctx: SyncContext) -> JQuantsBulkService:
    if ctx.bulk_service is None:
        ctx.bulk_service = JQuantsBulkService(ctx.client)
    return ctx.bulk_service


async def _get_paginated_rows_with_call_count(
    client: JQuantsAsyncClient,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    get_with_meta = getattr(client, "get_paginated_with_meta", None)
    if callable(get_with_meta):
        get_with_meta_callable = cast(
            Callable[..., Awaitable[tuple[list[dict[str, Any]], int]]],
            get_with_meta,
        )
        rows, calls = await get_with_meta_callable(path, params=params)
        return rows, int(calls)
    rows = await client.get_paginated(path, params=params)
    return rows, 1


def _to_iso_date_text(value: str | None) -> str | None:
    parsed = _parse_date(value or "")
    if parsed is None:
        return None
    return parsed.isoformat()


def _select_bulk_candidates_from_dates(dates: list[str]) -> tuple[str | None, str | None]:
    parsed = [_parse_date(value) for value in dates]
    normalized = [d for d in parsed if d is not None]
    if not normalized:
        return None, None
    return min(normalized).isoformat(), max(normalized).isoformat()


async def _plan_fetch_method(
    ctx: SyncContext,
    *,
    stage: str,
    endpoint: str,
    estimated_rest_calls: int,
    date_from: str | None = None,
    date_to: str | None = None,
    exact_dates: list[str] | None = None,
    min_rest_calls_to_probe_bulk: int = 3,
) -> _StageFetchDecision:
    if not _supports_bulk_sync(ctx.client):
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="plan_not_supported",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
        )

    if estimated_rest_calls < min_rest_calls_to_probe_bulk:
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="rest_estimate_too_small",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
        )

    bulk_service = _get_bulk_service(ctx)
    plan = await bulk_service.build_plan(
        endpoint=endpoint,
        date_from=date_from,
        date_to=date_to,
        exact_dates=exact_dates,
    )
    selected: _FetchMethod = "bulk" if plan.estimated_api_calls < estimated_rest_calls else "rest"
    reason = "bulk_estimate_lower" if selected == "bulk" else "rest_estimate_lower_or_equal"

    logger.info(
        "sync fetch strategy selected",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=selected,
        reason=reason,
        estimatedRestCalls=estimated_rest_calls,
        estimatedBulkCalls=plan.estimated_api_calls,
        plannerApiCalls=plan.list_api_calls,
        estimatedCacheHits=plan.estimated_cache_hits,
        estimatedCacheMisses=plan.estimated_cache_misses,
        selectedFiles=len(plan.files),
    )
    return _StageFetchDecision(
        method=selected,
        planner_api_calls=plan.list_api_calls,
        estimated_rest_calls=estimated_rest_calls,
        estimated_bulk_calls=plan.estimated_api_calls,
        plan=plan,
    )


def _canonicalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def _normalize_bulk_row_keys(
    rows: list[dict[str, Any]],
    aliases: dict[str, str],
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized = dict(row)
        for raw_key, raw_value in row.items():
            canonical = _canonicalize_key(str(raw_key))
            target = aliases.get(canonical)
            if not target:
                continue
            existing = normalized.get(target)
            if existing is None or (isinstance(existing, str) and not existing.strip()):
                normalized[target] = raw_value
        normalized_rows.append(normalized)
    return normalized_rows


def _normalize_bulk_stock_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_STOCK_KEY_ALIASES)


def _normalize_bulk_indices_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_INDEX_KEY_ALIASES)


def _normalize_bulk_fins_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _normalize_bulk_row_keys(rows, _BULK_FINS_KEY_ALIASES)


def _is_incremental_cold_start(
    ctx: SyncContext,
    inspection: TimeSeriesInspection | None,
) -> bool:
    if inspection is None or ctx.time_series_store is None:
        return False
    has_anchor_signal = bool(
        inspection.topix_max
        or inspection.stock_max
        or inspection.indices_max
        or inspection.latest_indices_dates
    )
    if has_anchor_signal:
        return False
    return (
        inspection.topix_count == 0
        and inspection.stock_count == 0
        and inspection.indices_count == 0
    )


def _log_sync_fetch_execution(
    *,
    stage: str,
    endpoint: str,
    decision: _StageFetchDecision,
    executed: _FetchMethod,
    actual_api_calls: int,
    fallback: bool,
    bulk_result: BulkFetchResult | None = None,
) -> None:
    cache_hit_rate: float | None = None
    cache_hits = 0
    cache_misses = 0
    if bulk_result is not None:
        cache_hits = bulk_result.cache_hits
        cache_misses = bulk_result.cache_misses
        total = cache_hits + cache_misses
        cache_hit_rate = (cache_hits / total) if total > 0 else None

    logger.info(
        "sync fetch strategy execution",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=decision.method,
        executed=executed,
        fallbackUsed=fallback,
        estimatedRestCalls=decision.estimated_rest_calls,
        estimatedBulkCalls=decision.estimated_bulk_calls,
        plannerApiCalls=decision.planner_api_calls,
        actualApiCalls=actual_api_calls,
        cacheHits=cache_hits,
        cacheMisses=cache_misses,
        cacheHitRate=cache_hit_rate,
    )


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
            target_code_set = {_normalize_index_code(code) for code in target_codes}

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, 2, f"Fetching data for {len(target_codes)} indices...")
            decision = await _plan_fetch_method(
                ctx,
                stage="indices_data",
                endpoint="/indices/bars/daily",
                estimated_rest_calls=len(target_codes),
            )
            total_calls += decision.planner_api_calls

            used_rest_fallback = False
            stage_api_calls = 0
            bulk_result: BulkFetchResult | None = None
            if decision.method == "bulk" and decision.plan is not None:
                try:
                    bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision.plan)
                    total_calls += bulk_result.api_calls
                    stage_api_calls += bulk_result.api_calls
                    rows = validate_rows_required_fields(
                        _convert_indices_data_rows(_normalize_bulk_indices_rows(bulk_result.rows), None),
                        required_fields=("code", "date"),
                        dedupe_keys=("code", "date"),
                        stage="indices_data",
                    )
                    rows = [
                        row
                        for row in rows
                        if _normalize_index_code(row.get("code")) in target_code_set
                    ]
                    if rows:
                        await _upsert_indices_rows_with_master_backfill(
                            ctx,
                            rows,
                            known_master_codes,
                        )
                    _log_sync_fetch_execution(
                        stage="indices_data",
                        endpoint="/indices/bars/daily",
                        decision=decision,
                        executed="bulk",
                        actual_api_calls=stage_api_calls,
                        fallback=False,
                        bulk_result=bulk_result,
                    )
                except Exception as e:
                    used_rest_fallback = True
                    logger.warning("indices-only bulk fetch failed, falling back to REST: {}", e)

            if decision.method == "rest" or used_rest_fallback:
                for code in target_codes:
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params={"code": code},
                        )
                        total_calls += page_calls
                        stage_api_calls += page_calls
                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, code),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
                        if rows:
                            await _upsert_indices_rows_with_master_backfill(
                                ctx,
                                rows,
                                known_master_codes,
                            )
                    except Exception as e:
                        errors.append(f"Index {code}: {e}")
                        logger.warning(f"Index {code} sync error: {e}")
                _log_sync_fetch_execution(
                    stage="indices_data",
                    endpoint="/indices/bars/daily",
                    decision=decision,
                    executed="rest",
                    actual_api_calls=stage_api_calls,
                    fallback=used_rest_fallback,
                    bulk_result=bulk_result,
                )

            await _index_indices_rows(ctx)

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

            topix_data_raw, topix_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily/topix",
            )

            async def _prefetched_topix_rows() -> list[dict[str, Any]]:
                return topix_data_raw

            topix_batch = await run_ingestion_batch(
                stage="topix",
                fetch=_prefetched_topix_rows,
                normalize=_convert_topix_rows,
                validate=lambda rows: validate_rows_required_fields(
                    rows,
                    required_fields=("date", "open", "high", "low", "close"),
                    dedupe_keys=("date",),
                    stage="topix",
                ),
                publish=lambda rows: _publish_topix_rows(ctx, rows),
                index=lambda _rows: _index_topix_rows(ctx),
            )
            total_calls += topix_calls
            topix_rows = topix_batch.rows
            dates_processed = len(topix_rows)

            # Step 2: 銘柄マスタ
            ctx.on_progress("stocks", 1, 6, "Fetching stock master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data, stocks_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/equities/master",
            )
            total_calls += stocks_calls
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
            from_date, to_date = _select_bulk_candidates_from_dates(trading_dates)
            decision = await _plan_fetch_method(
                ctx,
                stage="stock_data_initial",
                endpoint="/equities/bars/daily",
                estimated_rest_calls=max(len(trading_dates), 1),
                date_from=from_date,
                date_to=to_date,
                exact_dates=trading_dates,
            )
            total_calls += decision.planner_api_calls

            used_rest_fallback = False
            stage_api_calls = 0
            bulk_result: BulkFetchResult | None = None
            if decision.method == "bulk" and decision.plan is not None:
                try:
                    bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision.plan)
                    total_calls += bulk_result.api_calls
                    stage_api_calls += bulk_result.api_calls
                    bulk_rows = _normalize_bulk_stock_rows(bulk_result.rows)
                    if trading_dates:
                        trading_date_set = {_to_iso_date_text(value) for value in trading_dates}
                        bulk_rows = [
                            row
                            for row in bulk_rows
                            if _to_iso_date_text(str(row.get("Date") or "")) in trading_date_set
                        ]
                    rows = validate_rows_required_fields(
                        _convert_stock_data_rows(bulk_rows),
                        required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                        dedupe_keys=("code", "date"),
                        stage="stock_data",
                    )
                    if rows:
                        stocks_updated += await _publish_stock_data_rows(ctx, rows)
                    _log_sync_fetch_execution(
                        stage="stock_data_initial",
                        endpoint="/equities/bars/daily",
                        decision=decision,
                        executed="bulk",
                        actual_api_calls=stage_api_calls,
                        fallback=False,
                        bulk_result=bulk_result,
                    )
                except Exception as e:
                    used_rest_fallback = True
                    logger.warning("Initial stock_data bulk fetch failed, falling back to REST: {}", e)

            if decision.method == "rest" or used_rest_fallback:
                consecutive_failures = 0
                for i, date in enumerate(trading_dates):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if i % 50 == 0:
                        ctx.on_progress("stock_data", 3, 6, f"Fetching stock data: {i}/{len(trading_dates)} dates...")
                    try:
                        payload, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/equities/bars/daily",
                            params={"date": date},
                        )
                        total_calls += page_calls
                        stage_api_calls += page_calls

                        async def _prefetched_stock_rows() -> list[dict[str, Any]]:
                            return payload

                        batch = await run_ingestion_batch(
                            stage="stock_data",
                            fetch=_prefetched_stock_rows,
                            normalize=_convert_stock_data_rows,
                            validate=lambda rows: validate_rows_required_fields(
                                rows,
                                required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                                dedupe_keys=("code", "date"),
                                stage="stock_data",
                            ),
                            publish=lambda rows: _publish_stock_data_rows(ctx, rows),
                        )
                        stocks_updated += batch.published_count
                        consecutive_failures = 0
                    except Exception:
                        failed_dates.append(date)
                        consecutive_failures += 1
                        if consecutive_failures >= 5:
                            errors.append(f"Too many consecutive failures at {date}")
                            break
                _log_sync_fetch_execution(
                    stage="stock_data_initial",
                    endpoint="/equities/bars/daily",
                    decision=decision,
                    executed="rest",
                    actual_api_calls=stage_api_calls,
                    fallback=used_rest_fallback,
                    bulk_result=bulk_result,
                )

            await _index_stock_data_rows(ctx)

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
            inspection = _inspect_time_series(ctx)
            cold_start_bootstrap = _is_incremental_cold_start(ctx, inspection)
            last_topix_date = (
                inspection.topix_max if inspection and inspection.topix_max else ctx.market_db.get_latest_trading_date()
            )
            last_stock_date = (
                inspection.stock_max
                if inspection and inspection.stock_max
                else ctx.market_db.get_latest_stock_data_date()
            )
            last_date = last_stock_date or last_topix_date
            if cold_start_bootstrap:
                logger.info(
                    "Incremental sync detected empty time-series SoT; switching to bootstrap path",
                    event="sync_fetch_strategy",
                    stage="incremental_bootstrap",
                    selected="bootstrap",
                    reason="empty_timeseries",
                )
                last_date = None
            params: dict[str, Any] = {}
            if last_date:
                # J-Quants は YYYYMMDD 形式が安定しているため、既存データ形式（YYYY-MM-DD / YYYYMMDD）を吸収する
                params["from"] = _to_jquants_date_param(last_date)

            topix_payload, topix_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily/topix",
                params=params,
            )

            async def _prefetched_incremental_topix_rows() -> list[dict[str, Any]]:
                return topix_payload

            topix_batch = await run_ingestion_batch(
                stage="topix",
                fetch=_prefetched_incremental_topix_rows,
                normalize=_convert_topix_rows,
                validate=lambda rows: validate_rows_required_fields(
                    rows,
                    required_fields=("date", "open", "high", "low", "close"),
                    dedupe_keys=("date",),
                    stage="topix",
                ),
                publish=lambda rows: _publish_topix_rows(ctx, rows),
                index=lambda _rows: _index_topix_rows(ctx),
            )
            total_calls += topix_calls
            topix_rows = topix_batch.rows

            # Step 2: 銘柄マスタ更新
            ctx.on_progress("stocks", 1, 5, "Updating stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            stocks_data, stocks_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/equities/master",
            )
            total_calls += stocks_calls
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
            from_date_new, to_date_new = _select_bulk_candidates_from_dates(new_dates)
            decision_stock_data = await _plan_fetch_method(
                ctx,
                stage="stock_data_incremental",
                endpoint="/equities/bars/daily",
                estimated_rest_calls=max(len(new_dates), 1),
                date_from=from_date_new,
                date_to=to_date_new,
                exact_dates=new_dates,
            )
            total_calls += decision_stock_data.planner_api_calls

            used_stock_rest_fallback = False
            stock_stage_api_calls = 0
            stock_bulk_result: BulkFetchResult | None = None
            if decision_stock_data.method == "bulk" and decision_stock_data.plan is not None:
                try:
                    stock_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision_stock_data.plan)
                    total_calls += stock_bulk_result.api_calls
                    stock_stage_api_calls += stock_bulk_result.api_calls
                    bulk_rows = _normalize_bulk_stock_rows(stock_bulk_result.rows)
                    if new_dates:
                        new_date_set = {_to_iso_date_text(value) for value in new_dates}
                        bulk_rows = [
                            row
                            for row in bulk_rows
                            if _to_iso_date_text(str(row.get("Date") or "")) in new_date_set
                        ]
                    rows = validate_rows_required_fields(
                        _convert_stock_data_rows(bulk_rows),
                        required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                        dedupe_keys=("code", "date"),
                        stage="stock_data",
                    )
                    if rows:
                        stocks_updated += await _publish_stock_data_rows(ctx, rows)
                    _log_sync_fetch_execution(
                        stage="stock_data_incremental",
                        endpoint="/equities/bars/daily",
                        decision=decision_stock_data,
                        executed="bulk",
                        actual_api_calls=stock_stage_api_calls,
                        fallback=False,
                        bulk_result=stock_bulk_result,
                    )
                except Exception as e:
                    used_stock_rest_fallback = True
                    logger.warning("Incremental stock_data bulk fetch failed, falling back to REST: {}", e)

            if decision_stock_data.method == "rest" or used_stock_rest_fallback:
                for date in new_dates:
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    try:
                        payload, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/equities/bars/daily",
                            params={"date": date},
                        )
                        total_calls += page_calls
                        stock_stage_api_calls += page_calls

                        async def _prefetched_new_date_rows() -> list[dict[str, Any]]:
                            return payload

                        batch = await run_ingestion_batch(
                            stage="stock_data",
                            fetch=_prefetched_new_date_rows,
                            normalize=_convert_stock_data_rows,
                            validate=lambda rows: validate_rows_required_fields(
                                rows,
                                required_fields=("code", "date", "open", "high", "low", "close", "volume"),
                                dedupe_keys=("code", "date"),
                                stage="stock_data",
                            ),
                            publish=lambda rows: _publish_stock_data_rows(ctx, rows),
                        )
                        stocks_updated += batch.published_count
                    except Exception as e:
                        errors.append(f"Date {date}: {e}")
                _log_sync_fetch_execution(
                    stage="stock_data_incremental",
                    endpoint="/equities/bars/daily",
                    decision=decision_stock_data,
                    executed="rest",
                    actual_api_calls=stock_stage_api_calls,
                    fallback=used_stock_rest_fallback,
                    bulk_result=stock_bulk_result,
                )

            await _index_stock_data_rows(ctx)

            # Step 4: 指数データ（増分）
            ctx.on_progress("indices", 3, 5, "Fetching incremental index data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            raw_latest_index_dates = (
                dict(inspection.latest_indices_dates)
                if inspection and inspection.latest_indices_dates
                else ctx.market_db.get_latest_indices_data_dates()
            )
            latest_index_dates = {
                _normalize_index_code(code): value
                for code, value in raw_latest_index_dates.items()
                if _normalize_index_code(code)
            }
            target_codes = sorted(
                get_index_catalog_codes()
                | set(latest_index_dates.keys())
                | known_master_codes
            )
            target_code_set = {_normalize_index_code(code) for code in target_codes}

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
                topix_for_indices, topix_for_indices_calls = await _get_paginated_rows_with_call_count(
                    ctx.client,
                    "/indices/bars/daily/topix",
                    params={"from": _to_jquants_date_param(latest_index_date)},
                )
                total_calls += topix_for_indices_calls
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

            all_code_has_anchor = all(latest_index_dates.get(_normalize_index_code(code)) for code in target_codes)
            decision_indices = await _plan_fetch_method(
                ctx,
                stage="indices_incremental",
                endpoint="/indices/bars/daily",
                estimated_rest_calls=max(len(target_codes) + len(fallback_dates), 1),
                date_from=latest_index_date if all_code_has_anchor else None,
            )
            total_calls += decision_indices.planner_api_calls

            used_indices_rest_fallback = False
            indices_stage_api_calls = 0
            indices_bulk_result: BulkFetchResult | None = None
            if decision_indices.method == "bulk" and decision_indices.plan is not None:
                try:
                    indices_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision_indices.plan)
                    total_calls += indices_bulk_result.api_calls
                    indices_stage_api_calls += indices_bulk_result.api_calls
                    rows = validate_rows_required_fields(
                        _convert_indices_data_rows(_normalize_bulk_indices_rows(indices_bulk_result.rows), None),
                        required_fields=("code", "date"),
                        dedupe_keys=("code", "date"),
                        stage="indices_data",
                    )
                    fallback_date_set = {
                        normalized
                        for normalized in (_to_iso_date_text(value) for value in fallback_dates)
                        if normalized is not None
                    }
                    filtered_rows: list[dict[str, Any]] = []
                    for row in rows:
                        code = _normalize_index_code(row.get("code"))
                        row_date = _to_iso_date_text(str(row.get("date") or ""))
                        if not code or row_date is None:
                            continue

                        include = False
                        code_anchor = latest_index_dates.get(code)
                        if code in target_code_set:
                            if code_anchor is None:
                                include = True
                            else:
                                include = _is_date_after(row_date, code_anchor)

                        if not include and row_date in fallback_date_set:
                            include = True

                        if include:
                            filtered_rows.append(row)

                    if filtered_rows:
                        await _upsert_indices_rows_with_master_backfill(
                            ctx,
                            filtered_rows,
                            known_master_codes,
                            discovery_log="Inserted {} discovered index master rows while syncing by bulk.",
                        )
                    _log_sync_fetch_execution(
                        stage="indices_incremental",
                        endpoint="/indices/bars/daily",
                        decision=decision_indices,
                        executed="bulk",
                        actual_api_calls=indices_stage_api_calls,
                        fallback=False,
                        bulk_result=indices_bulk_result,
                    )
                except Exception as e:
                    used_indices_rest_fallback = True
                    logger.warning("Incremental indices bulk fetch failed, falling back to REST: {}", e)

            if decision_indices.method == "rest" or used_indices_rest_fallback:
                for code in target_codes:
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

                    params: dict[str, Any] = {"code": code}
                    normalized_code = _normalize_index_code(code)
                    last_index_date = latest_index_dates.get(normalized_code)
                    if last_index_date:
                        params["from"] = _to_jquants_date_param(last_index_date)

                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params=params,
                        )
                        total_calls += page_calls
                        indices_stage_api_calls += page_calls

                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, code),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
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

                for index_date in fallback_dates:
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

                    try:
                        data, page_calls = await _get_paginated_rows_with_call_count(
                            ctx.client,
                            "/indices/bars/daily",
                            params={"date": _to_jquants_date_param(index_date)},
                        )
                        total_calls += page_calls
                        indices_stage_api_calls += page_calls
                        rows = validate_rows_required_fields(
                            _convert_indices_data_rows(data, None),
                            required_fields=("code", "date"),
                            dedupe_keys=("code", "date"),
                            stage="indices_data",
                        )
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
                _log_sync_fetch_execution(
                    stage="indices_incremental",
                    endpoint="/indices/bars/daily",
                    decision=decision_indices,
                    executed="rest",
                    actual_api_calls=indices_stage_api_calls,
                    fallback=used_indices_rest_fallback,
                    bulk_result=indices_bulk_result,
                )

            await _index_indices_rows(ctx)

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
    prime_code_set = set(prime_codes)
    bulk_succeeded = False
    stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None

    decision = await _plan_fetch_method(
        ctx,
        stage="fundamentals_initial",
        endpoint="/fins/summary",
        estimated_rest_calls=max(len(prime_codes), 1),
    )
    api_calls += decision.planner_api_calls

    if decision.method == "bulk" and decision.plan is not None:
        try:
            bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision.plan)
            api_calls += bulk_result.api_calls
            stage_api_calls += bulk_result.api_calls
            bulk_rows = convert_fins_summary_rows(_normalize_bulk_fins_rows(bulk_result.rows))
            rows = [row for row in bulk_rows if row.get("code") in prime_code_set]
            rows = validate_rows_required_fields(
                rows,
                required_fields=("code", "disclosed_date"),
                dedupe_keys=("code", "disclosed_date"),
                stage="fundamentals",
            )
            if rows:
                updated += await _publish_statement_rows(ctx, rows)
            bulk_succeeded = True
            _log_sync_fetch_execution(
                stage="fundamentals_initial",
                endpoint="/fins/summary",
                decision=decision,
                executed="bulk",
                actual_api_calls=stage_api_calls,
                fallback=False,
                bulk_result=bulk_result,
            )
        except Exception as e:
            logger.warning("Initial fundamentals bulk fetch failed, falling back to REST: {}", e)

    if not bulk_succeeded:
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
                stage_api_calls += page_calls
                rows = validate_rows_required_fields(
                    convert_fins_summary_rows(data, default_code=code),
                    required_fields=("code", "disclosed_date"),
                    dedupe_keys=("code", "disclosed_date"),
                    stage="fundamentals",
                )
                if rows:
                    updated += await _publish_statement_rows(ctx, rows)
            except Exception as e:
                failed_codes.append(code)
                errors.append(f"Fundamentals code {code}: {e}")
        _log_sync_fetch_execution(
            stage="fundamentals_initial",
            endpoint="/fins/summary",
            decision=decision,
            executed="rest",
            actual_api_calls=stage_api_calls,
            fallback=decision.method == "bulk",
            bulk_result=bulk_result,
        )

    await _index_statement_rows(ctx)

    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
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
        or _get_latest_statement_disclosed_date(ctx)
    )
    date_targets = _build_incremental_date_targets(anchor, previous_failed_dates)
    normalized_targets = {
        normalized
        for normalized in (_to_iso_date_text(value) for value in date_targets)
        if normalized is not None
    }
    dates_phase_completed = 0
    bulk_dates_succeeded = False
    date_phase_api_calls = 0
    date_phase_bulk_result: BulkFetchResult | None = None
    if date_targets:
        decision = await _plan_fetch_method(
            ctx,
            stage="fundamentals_incremental_dates",
            endpoint="/fins/summary",
            estimated_rest_calls=max(len(date_targets), 1),
            exact_dates=date_targets,
        )
        api_calls += decision.planner_api_calls

        if decision.method == "bulk" and decision.plan is not None:
            try:
                date_phase_bulk_result = await _get_bulk_service(ctx).fetch_with_plan(decision.plan)
                api_calls += date_phase_bulk_result.api_calls
                date_phase_api_calls += date_phase_bulk_result.api_calls
                bulk_rows = convert_fins_summary_rows(_normalize_bulk_fins_rows(date_phase_bulk_result.rows))
                rows = [row for row in bulk_rows if row.get("code") in prime_code_set]
                if normalized_targets:
                    rows = [
                        row
                        for row in rows
                        if _to_iso_date_text(str(row.get("disclosed_date") or "")) in normalized_targets
                    ]
                rows = validate_rows_required_fields(
                    rows,
                    required_fields=("code", "disclosed_date"),
                    dedupe_keys=("code", "disclosed_date"),
                    stage="fundamentals",
                )
                if rows:
                    updated += await _publish_statement_rows(ctx, rows)
                bulk_dates_succeeded = True
                dates_phase_completed = len(date_targets)
                _log_sync_fetch_execution(
                    stage="fundamentals_incremental_dates",
                    endpoint="/fins/summary",
                    decision=decision,
                    executed="bulk",
                    actual_api_calls=date_phase_api_calls,
                    fallback=False,
                    bulk_result=date_phase_bulk_result,
                )
            except Exception as e:
                logger.warning("Incremental fundamentals bulk date fetch failed, falling back to REST: {}", e)

        if not bulk_dates_succeeded:
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
                    date_phase_api_calls += page_calls
                    rows = convert_fins_summary_rows(data)
                    rows = [row for row in rows if row.get("code") in prime_code_set]
                    rows = validate_rows_required_fields(
                        rows,
                        required_fields=("code", "disclosed_date"),
                        dedupe_keys=("code", "disclosed_date"),
                        stage="fundamentals",
                    )
                    if rows:
                        updated += await _publish_statement_rows(ctx, rows)
                except Exception as e:
                    failed_dates.append(disclosed_date)
                    errors.append(f"Fundamentals date {disclosed_date}: {e}")
            dates_phase_completed = len(date_targets)
            _log_sync_fetch_execution(
                stage="fundamentals_incremental_dates",
                endpoint="/fins/summary",
                decision=decision,
                executed="rest",
                actual_api_calls=date_phase_api_calls,
                fallback=decision.method == "bulk",
                bulk_result=date_phase_bulk_result,
            )

    statement_codes = _get_statement_codes(ctx)
    missing_prime_codes = sorted(set(prime_codes) - set(statement_codes))
    code_targets = _collect_unique_codes(previous_failed_codes + missing_prime_codes)

    for idx, code in enumerate(code_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": dates_phase_completed,
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
            rows = validate_rows_required_fields(
                rows,
                required_fields=("code", "disclosed_date"),
                dedupe_keys=("code", "disclosed_date"),
                stage="fundamentals",
            )
            if rows:
                updated += await _publish_statement_rows(ctx, rows)
        except Exception as e:
            failed_codes.append(code)
            errors.append(f"Fundamentals code {code}: {e}")

    await _index_statement_rows(ctx)

    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
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
        "dates_processed": dates_phase_completed,
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


def _inspect_time_series(ctx: SyncContext) -> TimeSeriesInspection | None:
    if ctx.time_series_store is None:
        return None
    try:
        return ctx.time_series_store.inspect()
    except Exception as e:  # noqa: BLE001 - inspection failure should not block sync
        logger.warning("Failed to inspect time-series store during sync: {}", e)
        return None


def _get_latest_statement_disclosed_date(ctx: SyncContext) -> str | None:
    inspection = _inspect_time_series(ctx)
    if inspection and inspection.latest_statement_disclosed_date:
        return inspection.latest_statement_disclosed_date
    return ctx.market_db.get_latest_statement_disclosed_date()


def _get_statement_codes(ctx: SyncContext) -> set[str]:
    inspection = _inspect_time_series(ctx)
    if inspection and inspection.statement_codes:
        return set(inspection.statement_codes)
    return ctx.market_db.get_statement_codes()


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

    await _publish_indices_rows(ctx, rows)


async def _publish_topix_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if ctx.time_series_store is None:
        return await asyncio.to_thread(ctx.market_db.upsert_topix_data, rows)
    return await asyncio.to_thread(ctx.time_series_store.publish_topix_data, rows)


async def _publish_stock_data_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if ctx.time_series_store is None:
        return await asyncio.to_thread(ctx.market_db.upsert_stock_data, rows)
    return await asyncio.to_thread(ctx.time_series_store.publish_stock_data, rows)


async def _publish_indices_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if ctx.time_series_store is None:
        return await asyncio.to_thread(ctx.market_db.upsert_indices_data, rows)
    return await asyncio.to_thread(ctx.time_series_store.publish_indices_data, rows)


async def _publish_statement_rows(ctx: SyncContext, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if ctx.time_series_store is None:
        return await asyncio.to_thread(ctx.market_db.upsert_statements, rows)
    return await asyncio.to_thread(ctx.time_series_store.publish_statements, rows)


async def _index_topix_rows(ctx: SyncContext) -> None:
    if ctx.time_series_store is None:
        return
    await asyncio.to_thread(ctx.time_series_store.index_topix_data)


async def _index_stock_data_rows(ctx: SyncContext) -> None:
    if ctx.time_series_store is None:
        return
    await asyncio.to_thread(ctx.time_series_store.index_stock_data)


async def _index_indices_rows(ctx: SyncContext) -> None:
    if ctx.time_series_store is None:
        return
    await asyncio.to_thread(ctx.time_series_store.index_indices_data)


async def _index_statement_rows(ctx: SyncContext) -> None:
    if ctx.time_series_store is None:
        return
    await asyncio.to_thread(ctx.time_series_store.index_statements)


def _convert_topix_rows(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    created_at = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for d in data:
        rows.append(
            {
                "date": d.get("Date", d.get("date", "")),
                "open": d.get("O", d.get("open")),
                "high": d.get("H", d.get("high")),
                "low": d.get("L", d.get("low")),
                "close": d.get("C", d.get("close")),
                "created_at": created_at,
            }
        )
    return rows


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
