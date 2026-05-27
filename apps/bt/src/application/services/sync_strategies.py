"""
Sync Strategies

DuckDB market data 同期のための 3 つの戦略。
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, Iterable, Protocol, cast

from loguru import logger

from src.application.services.jquants_bulk_service import (
    BulkFileInfo,
    BulkFetchPlan,
    BulkFetchResult,
)
from src.application.services.sync_paginated_fetch import get_paginated_rows_with_call_count
from src.application.services.listed_market_targets import (
    extract_listed_market_codes,
    is_listed_market_code,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import (
    TimeSeriesInspection,
)
from src.application.services.options_225 import (
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
)
from src.infrastructure.db.market.query_helpers import (
    normalize_stock_code,
)
from src.entrypoints.http.schemas.db import SyncResult
from src.application.services.ingestion_pipeline import (
    run_ingestion_batch,
    validate_rows_required_fields,
)
from src.application.services.index_master_catalog import (
    build_index_master_seed_rows,
    get_index_catalog_codes,
)
from src.application.services import sync_fetch_planner
from src.application.services.sync_index_master_backfill import (
    build_fallback_index_master_rows,
    upsert_indices_rows_with_master_backfill as _upsert_indices_rows_with_master_backfill,
)
from src.application.services.sync_indices_data import (
    ingest_incremental_indices_bulk_batch as _ingest_incremental_indices_bulk_batch,
    sync_incremental_indices_rest as _sync_incremental_indices_rest,
    sync_incremental_indices_stage as _run_incremental_indices_stage,
)
from src.application.services.sync_row_converters import (
    convert_index_master_rows,
    convert_indices_data_rows as _convert_indices_data_rows,
    convert_margin_rows as _convert_margin_rows,
    convert_stock_data_rows as _convert_stock_data_rows,
    convert_stock_rows,
    convert_topix_rows as _convert_topix_rows,
    extract_dates_after as _extract_dates_after,
    extract_list_items as _extract_list_items,
    latest_date as _latest_date,
    normalize_bulk_indices_rows as _normalize_bulk_indices_rows,
    to_jquants_date_param as _to_jquants_date_param,
    _date_sort_key,
    _is_date_after,
    _normalize_index_code,
    _normalize_iso_date_text,
    _parse_date,
    _to_iso_date_text,
)
from src.application.services import sync_publish_helpers
from src.application.services.sync_fins_fetch import (
    _fetch_fins_summary_by_code,
)
from src.application.services.sync_fundamentals_data import (
    sync_fundamentals_incremental as _sync_fundamentals_incremental,
    sync_fundamentals_initial as _sync_fundamentals_initial,
)
from src.application.services.sync_margin_data import (
    _fetch_margin_by_code,
    sync_margin_data as _sync_margin_data,
)
from src.application.services.sync_stock_master import (
    sync_daily_stock_master,
)
from src.application.services.sync_stock_data_fetch import execute_stock_data_bulk_fetch
from src.application.services.sync_options_225_data import (
    resolve_incremental_options_date_targets as _resolve_incremental_options_date_targets,
    sync_options_225_dates as _sync_options_225_dates,
)
from src.application.services import sync_state_helpers

__all__ = (
    "_convert_margin_rows",
    "_extract_list_items",
    "_extract_dates_after",
    "_date_sort_key",
    "_fetch_fins_summary_by_code",
    "_fetch_margin_by_code",
    "_ingest_incremental_indices_bulk_batch",
    "_normalize_iso_date_text",
    "_sync_incremental_indices_rest",
    "_to_iso_date_text",
)


class SyncClientLike(Protocol):  # pragma: no cover
    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...

    async def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...


_LOCAL_SYNTHETIC_INDEX_CODES = {
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
}

_build_fallback_index_master_rows = build_fallback_index_master_rows
_convert_index_master_rows = convert_index_master_rows
_convert_stock_rows = convert_stock_rows


class SyncMarketDbLike(Protocol):  # pragma: no cover
    def get_sync_metadata(self, key: str) -> str | None: ...
    def set_sync_metadata(self, key: str, value: str) -> None: ...
    def upsert_stocks(self, rows: list[dict[str, Any]]) -> Any: ...
    def upsert_stock_master_daily(self, snapshot_date: str, rows: list[dict[str, Any]]) -> Any: ...
    def upsert_stock_master_daily_rows(self, rows: list[dict[str, Any]]) -> int: ...
    def rebuild_stock_master_intervals(self) -> int: ...
    def rebuild_stocks_latest(self) -> int: ...
    def get_topix_dates(self, *, start_date: str | None = None, end_date: str | None = None) -> list[str]: ...
    def get_missing_stock_master_dates(self, *, limit: int | None = 20) -> list[str]: ...
    def get_fundamentals_target_codes(self) -> set[str]: ...
    def get_fundamentals_target_stock_rows(self) -> list[dict[str, str]]: ...
    def get_stocks_needing_refresh(self, limit: int | None = None) -> list[str]: ...
    def upsert_index_master(self, rows: list[dict[str, Any]]) -> Any: ...
    def get_index_master_codes(self) -> set[str]: ...


class BulkServiceLike(Protocol):  # pragma: no cover
    async def build_plan(
        self,
        *,
        endpoint: str,
        date_from: str | None = None,
        date_to: str | None = None,
        exact_dates: list[str] | None = None,
    ) -> BulkFetchPlan: ...

    async def fetch_with_plan(
        self,
        plan: BulkFetchPlan,
        *,
        on_rows_batch: Callable[[list[dict[str, Any]], BulkFileInfo], Awaitable[None]] | None = None,
        accumulate_rows: bool = True,
    ) -> BulkFetchResult: ...


class SyncTimeSeriesStoreLike(Protocol):  # pragma: no cover
    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection: ...

    def publish_topix_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_stock_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_indices_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_options_225_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_margin_data(self, rows: list[dict[str, Any]]) -> int: ...
    def publish_statements(self, rows: list[dict[str, Any]]) -> int: ...
    def index_topix_data(self) -> None: ...
    def index_stock_data(self) -> None: ...
    def index_indices_data(self) -> None: ...
    def index_options_225_data(self) -> None: ...
    def index_margin_data(self) -> None: ...
    def index_statements(self) -> None: ...


@dataclass
class SyncContext:
    client: SyncClientLike
    market_db: SyncMarketDbLike
    cancelled: asyncio.Event
    on_progress: Callable[[str, int, int, str], None]
    on_fetch_detail: Callable[[dict[str, Any]], None] | None = None
    time_series_store: SyncTimeSeriesStoreLike | None = None
    bulk_service: BulkServiceLike | None = None
    bulk_probe_disabled: bool = False
    bulk_probe_failure_reason: str | None = None
    enforce_bulk_for_stock_data: bool = False


class SyncStrategy(Protocol):  # pragma: no cover
    async def execute(self, ctx: SyncContext) -> SyncResult: ...
    def estimate_api_calls(self) -> int: ...


@dataclass(frozen=True)
class _SyncStageOutcome:
    api_calls: int
    errors: list[str]
    cancelled: bool = False


def _select_bulk_candidates_from_dates(dates: list[str]) -> tuple[str | None, str | None]:
    parsed = [_parse_date(value) for value in dates]
    normalized = [d for d in parsed if d is not None]
    if not normalized:
        return None, None
    return min(normalized).isoformat(), max(normalized).isoformat()


async def _ingest_indices_only_bulk_batch(
    ctx: SyncContext,
    *,
    batch_rows: list[dict[str, Any]],
    target_code_set: set[str],
    known_master_codes: set[str],
) -> None:
    rows = validate_rows_required_fields(
        _convert_indices_data_rows(_normalize_bulk_indices_rows(batch_rows), None),
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


def _is_incremental_cold_start(
    inspection: TimeSeriesInspection,
) -> bool:
    has_anchor_signal = bool(
        inspection.topix_max
        or inspection.stock_max
        or inspection.indices_max
        or inspection.margin_max
        or inspection.latest_indices_dates
    )
    if has_anchor_signal:
        return False
    return (
        inspection.topix_count == 0
        and inspection.stock_count == 0
        and inspection.indices_count == 0
        and inspection.margin_count == 0
    )


def _extract_listed_market_target_rows(
    stock_rows: list[dict[str, Any]],
) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in stock_rows:
        if not is_listed_market_code(row.get("market_code")):
            continue
        code = normalize_stock_code(str(row.get("code", "")).strip())
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(
            {
                "code": code,
                "company_name": str(row.get("company_name", "") or ""),
                "market_code": str(row.get("market_code", "") or ""),
            }
        )
    return deduped

class IndicesOnlySyncStrategy:
    """指数同期: 指数マスタ + 指数データ + optional N225 options sync."""

    def __init__(self, *, include_options: bool = True) -> None:
        self._include_options = include_options

    def estimate_api_calls(self) -> int:
        return 70 if self._include_options else 52

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        errors: list[str] = []
        total_steps = 3 if self._include_options else 2

        try:
            # 1. 指数マスタ（ローカルカタログ）を補完
            ctx.on_progress("indices_master", 0, total_steps, "Syncing index master catalog...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            known_master_codes = await _seed_index_master_from_catalog(ctx)
            target_codes = _jquants_index_fetch_codes(get_index_catalog_codes() | known_master_codes)
            target_code_set = set(target_codes)

            # 2. 各指数のデータ取得
            ctx.on_progress("indices_data", 1, total_steps, f"Fetching data for {len(target_codes)} indices...")
            decision = await sync_fetch_planner._plan_fetch_method(
                ctx,
                stage="indices_data",
                endpoint="/indices/bars/daily",
                estimated_rest_calls=len(target_codes),
            )
            total_calls += decision.planner_api_calls
            sync_fetch_planner._emit_fetch_strategy_progress(
                ctx,
                progress_stage="indices_data",
                current=1,
                total=total_steps,
                endpoint="/indices/bars/daily",
                decision=decision,
                target_label=f"{len(target_codes)} codes",
            )

            used_rest_fallback = False
            bulk_fallback_reason: str | None = None
            stage_api_calls = 0
            bulk_result: BulkFetchResult | None = None
            if decision.method == "bulk":
                async def _consume_indices_only_bulk_rows(
                    batch_rows: list[dict[str, Any]],
                    _file_info: BulkFileInfo,
                ) -> None:
                    await _ingest_indices_only_bulk_batch(
                        ctx,
                        batch_rows=batch_rows,
                        target_code_set=target_code_set,
                        known_master_codes=known_master_codes,
                    )

                bulk_outcome = await sync_fetch_planner._execute_bulk_fetch_stage(
                    ctx,
                    decision=decision,
                    stage_name="indices_data",
                    progress_stage="indices_data",
                    current=1,
                    total=total_steps,
                    endpoint="/indices/bars/daily",
                    target_label=f"{len(target_codes)} codes",
                    on_rows_batch=_consume_indices_only_bulk_rows,
                    fallback_log_message="indices-only bulk fetch unavailable, falling back to REST: {}",
                )
                total_calls += bulk_outcome.api_calls
                stage_api_calls += bulk_outcome.api_calls
                bulk_result = bulk_outcome.bulk_result
                used_rest_fallback = bulk_outcome.used_rest_fallback
                bulk_fallback_reason = bulk_outcome.fallback_reason

            if decision.method == "rest" or used_rest_fallback:
                sync_fetch_planner._emit_fetch_execution_progress(
                    ctx,
                    progress_stage="indices_data",
                    current=1,
                    total=total_steps,
                    endpoint="/indices/bars/daily",
                    method="rest",
                    target_label=f"{len(target_codes)} codes",
                    fallback=used_rest_fallback,
                    fallback_reason=bulk_fallback_reason,
                )
                for i, code in enumerate(target_codes, start=1):
                    if ctx.cancelled.is_set():
                        return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
                    if i > 1 and i % 50 == 0:
                        ctx.on_progress(
                            "indices_data",
                            1,
                            total_steps,
                            f"Fetching /indices/bars/daily via REST: {i}/{len(target_codes)} codes...",
                        )
                    try:
                        data, page_calls = await get_paginated_rows_with_call_count(
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
                sync_fetch_planner._log_sync_fetch_execution(
                    stage="indices_data",
                    endpoint="/indices/bars/daily",
                    decision=decision,
                    executed="rest",
                    actual_api_calls=stage_api_calls,
                    fallback=used_rest_fallback,
                    bulk_result=bulk_result,
                )

            if not self._include_options:
                await sync_publish_helpers._index_indices_rows(ctx)
                return SyncResult(
                    success=len(errors) == 0,
                    totalApiCalls=total_calls,
                    errors=errors,
                )

            options_dates = await asyncio.to_thread(ctx.market_db.get_topix_dates)
            ctx.on_progress("options_225", 2, total_steps, f"Fetching N225 options for {len(options_dates)} dates...")
            options_result = await _sync_options_225_dates(
                ctx,
                date_targets=options_dates,
                progress_stage="options_225",
                progress_current=2,
                progress_total=total_steps,
                stage_name="options_225_indices_only",
            )
            total_calls += int(options_result["api_calls"])
            errors.extend(cast(list[str], options_result["errors"]))
            if bool(options_result["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


async def _sync_initial_topix_stage(ctx: SyncContext) -> tuple[list[dict[str, Any]], int]:
    topix_data_raw, topix_calls = await get_paginated_rows_with_call_count(
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
        publish=lambda rows: sync_publish_helpers._publish_topix_rows(ctx, rows),
        index=lambda _rows: sync_publish_helpers._index_topix_rows(ctx),
    )
    return topix_batch.rows, topix_calls


async def _sync_initial_stock_master_stage(
    ctx: SyncContext,
    *,
    trading_dates: list[str],
) -> dict[str, Any]:
    master_sync = await sync_daily_stock_master(
        ctx,
        target_dates=trading_dates,
        progress_current=1,
        progress_total=8,
    )
    return {
        "api_calls": master_sync["api_calls"],
        "stock_rows": master_sync["latest_rows"],
        "errors": master_sync["errors"],
        "cancelled": master_sync["cancelled"],
    }


async def _sync_initial_fundamentals_stage(
    ctx: SyncContext,
    *,
    stock_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    fundamentals_target_rows = _extract_listed_market_target_rows(stock_rows)
    if not fundamentals_target_rows:
        fundamentals_target_rows = await asyncio.to_thread(
            ctx.market_db.get_fundamentals_target_stock_rows
        )

    fundamentals_sync = await _sync_fundamentals_initial(
        ctx,
        fundamentals_target_rows,
        progress_current=2,
        progress_total=8,
    )
    return {
        "api_calls": fundamentals_sync["api_calls"],
        "updated": fundamentals_sync["updated"],
        "dates_processed": fundamentals_sync["dates_processed"],
        "errors": fundamentals_sync["errors"],
        "cancelled": fundamentals_sync["cancelled"],
    }


async def _sync_initial_stock_data_stage(
    ctx: SyncContext,
    *,
    trading_dates: list[str],
) -> dict[str, Any]:
    api_calls = 0
    stocks_updated = 0
    failed_dates: list[str] = []
    errors: list[str] = []
    from_date, to_date = _select_bulk_candidates_from_dates(trading_dates)
    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="stock_data_initial",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=max(len(trading_dates), 1),
        date_from=from_date,
        date_to=to_date,
        exact_dates=trading_dates,
        require_bulk=ctx.enforce_bulk_for_stock_data,
    )
    api_calls += decision.planner_api_calls
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="stock_data",
        current=3,
        total=8,
        endpoint="/equities/bars/daily",
        decision=decision,
        target_label=f"{len(trading_dates)} dates",
    )
    sync_fetch_planner._enforce_stock_bulk_plan_available(
        ctx,
        decision=decision,
        endpoint="/equities/bars/daily",
        progress_stage="stock_data",
        current=3,
        total=8,
        target_count=len(trading_dates),
    )

    used_rest_fallback = False
    stock_bulk_fallback_reason: str | None = None
    bulk_result: BulkFetchResult | None = None
    bulk_outcome = await execute_stock_data_bulk_fetch(
        ctx,
        decision=decision,
        target_dates=trading_dates,
        stage_name="stock_data_initial",
        progress_stage="stock_data",
        current=3,
        total=8,
        fallback_log_message="Initial stock_data bulk fetch failed, falling back to REST: {}",
    )
    api_calls += bulk_outcome.api_calls
    stocks_updated += bulk_outcome.stocks_updated
    bulk_result = bulk_outcome.bulk_result
    used_rest_fallback = bulk_outcome.used_rest_fallback
    stock_bulk_fallback_reason = bulk_outcome.fallback_reason

    if decision.method == "rest" or used_rest_fallback:
        rest_result = await _sync_initial_stock_data_rest(
            ctx,
            trading_dates=trading_dates,
            decision=decision,
            used_rest_fallback=used_rest_fallback,
            fallback_reason=stock_bulk_fallback_reason,
            bulk_result=bulk_result,
        )
        api_calls += rest_result["api_calls"]
        stocks_updated += rest_result["stocks_updated"]
        failed_dates.extend(rest_result["failed_dates"])
        errors.extend(rest_result["errors"])
        if rest_result["cancelled"]:
            return {
                "api_calls": api_calls,
                "stocks_updated": stocks_updated,
                "failed_dates": failed_dates,
                "errors": errors,
                "cancelled": True,
            }

    await sync_publish_helpers._index_stock_data_rows(ctx)
    return {
        "api_calls": api_calls,
        "stocks_updated": stocks_updated,
        "failed_dates": failed_dates,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_initial_stock_data_rest(
    ctx: SyncContext,
    *,
    trading_dates: list[str],
    decision: Any,
    used_rest_fallback: bool,
    fallback_reason: str | None,
    bulk_result: BulkFetchResult | None,
) -> dict[str, Any]:
    api_calls = 0
    stocks_updated = 0
    failed_dates: list[str] = []
    errors: list[str] = []
    stage_api_calls = 0
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="stock_data",
        current=3,
        total=8,
        endpoint="/equities/bars/daily",
        method="rest",
        target_label=f"{len(trading_dates)} dates",
        fallback=used_rest_fallback,
        fallback_reason=fallback_reason,
    )
    consecutive_failures = 0
    for i, date in enumerate(trading_dates):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "stocks_updated": stocks_updated,
                "failed_dates": failed_dates,
                "errors": errors,
                "cancelled": True,
            }
        if i % 50 == 0:
            ctx.on_progress(
                "stock_data",
                3,
                8,
                f"Fetching /equities/bars/daily via REST: {i}/{len(trading_dates)} dates...",
            )
        try:
            payload, page_calls = await get_paginated_rows_with_call_count(
                ctx.client,
                "/equities/bars/daily",
                params={"date": date},
            )
            api_calls += page_calls
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
                publish=lambda rows: sync_publish_helpers._publish_stock_data_rows(ctx, rows),
            )
            stocks_updated += batch.published_count
            consecutive_failures = 0
        except Exception:
            failed_dates.append(date)
            consecutive_failures += 1
            if consecutive_failures >= 5:
                errors.append(f"Too many consecutive failures at {date}")
                break
    sync_fetch_planner._log_sync_fetch_execution(
        stage="stock_data_initial",
        endpoint="/equities/bars/daily",
        decision=decision,
        executed="rest",
        actual_api_calls=stage_api_calls,
        fallback=used_rest_fallback,
        bulk_result=bulk_result,
    )
    return {
        "api_calls": api_calls,
        "stocks_updated": stocks_updated,
        "failed_dates": failed_dates,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_initial_margin_stage(
    ctx: SyncContext,
    *,
    stock_rows: list[dict[str, Any]],
    topix_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    all_stock_codes = sync_state_helpers._collect_unique_codes(
        [str(row.get("code", "")) for row in stock_rows if row.get("code")]
    )
    margin_target_codes = extract_listed_market_codes(stock_rows)
    margin_frontier = (
        _latest_date([str(row.get("date")) for row in topix_rows if row.get("date")])
        or ctx.market_db.get_latest_trading_date()
        or ctx.market_db.get_latest_stock_data_date()
        or ctx.market_db.get_latest_margin_date()
    )
    margin_sync = await _sync_margin_data(
        ctx,
        margin_target_codes,
        progress_current=6,
        progress_total=8,
        stage_name="margin_initial",
        trading_frontier=margin_frontier,
        skipped_market_count=max(len(all_stock_codes) - len(margin_target_codes), 0),
    )
    return {
        "api_calls": margin_sync["api_calls"],
        "errors": margin_sync["errors"],
        "cancelled": margin_sync["cancelled"],
    }


async def _finalize_initial_sync_metadata(ctx: SyncContext, *, failed_dates: list[str]) -> None:
    now_iso = datetime.now(UTC).isoformat()
    await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["INIT_COMPLETED"], "true")
    await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["FAILED_DATES"],
        json.dumps(failed_dates) if failed_dates else "[]",
    )


class InitialSyncStrategy:
    """初回同期: TOPIX + 全銘柄 + 株価データ + 指数データ"""

    def estimate_api_calls(self) -> int:
        # TOPIX/株価/指数/信用残に加えて Prime 全銘柄の /fins/summary(code=...) を含む。
        return 3200

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
            ctx.on_progress("topix", 0, 8, "Fetching TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
            topix_rows, topix_calls = await _sync_initial_topix_stage(ctx)
            total_calls += topix_calls
            dates_processed = len(topix_rows)

            ctx.on_progress("stock_master_daily", 1, 8, "Fetching daily stock master data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
            trading_dates = sorted({r["date"] for r in topix_rows})
            master_sync = await _sync_initial_stock_master_stage(ctx, trading_dates=trading_dates)
            total_calls += master_sync["api_calls"]
            stock_rows = master_sync["stock_rows"]
            errors.extend(master_sync["errors"])
            if master_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            ctx.on_progress("fundamentals", 2, 8, "Fetching listed-market fundamentals (full)...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
            fundamentals_sync = await _sync_initial_fundamentals_stage(ctx, stock_rows=stock_rows)
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            ctx.on_progress("stock_data", 3, 8, "Fetching daily stock prices...")
            stock_sync = await _sync_initial_stock_data_stage(ctx, trading_dates=trading_dates)
            total_calls += stock_sync["api_calls"]
            stocks_updated += stock_sync["stocks_updated"]
            failed_dates.extend(stock_sync["failed_dates"])
            errors.extend(stock_sync["errors"])
            if stock_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            ctx.on_progress("indices", 4, 8, "Fetching index data...")
            indices_strategy = IndicesOnlySyncStrategy(include_options=False)
            indices_result = await indices_strategy.execute(ctx)
            total_calls += indices_result.totalApiCalls
            errors.extend(indices_result.errors)

            ctx.on_progress("options_225", 5, 8, f"Fetching N225 options for {len(trading_dates)} dates...")
            options_sync = await _sync_options_225_dates(
                ctx,
                date_targets=trading_dates,
                progress_stage="options_225",
                progress_current=5,
                progress_total=8,
                stage_name="options_225_initial",
            )
            total_calls += int(options_sync["api_calls"])
            errors.extend(cast(list[str], options_sync["errors"]))
            if bool(options_sync["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            ctx.on_progress("margin", 6, 8, "Fetching margin data...")
            margin_sync = await _sync_initial_margin_stage(ctx, stock_rows=stock_rows, topix_rows=topix_rows)
            total_calls += margin_sync["api_calls"]
            errors.extend(margin_sync["errors"])
            if margin_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            ctx.on_progress("finalize", 7, 8, "Finalizing sync...")
            await _finalize_initial_sync_metadata(ctx, failed_dates=failed_dates)

            ctx.on_progress("complete", 8, 8, "Sync complete!")
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
        except sync_fetch_planner.BulkFetchRequiredError:
            raise
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


async def _sync_incremental_stock_data_stage(
    ctx: SyncContext,
    *,
    topix_rows: list[dict[str, Any]],
    anchor: str | None,
) -> dict[str, Any]:
    api_calls = 0
    stocks_updated = 0
    errors: list[str] = []
    stock_target_dates = await _resolve_incremental_stock_date_targets(ctx, topix_rows=topix_rows, anchor=anchor)
    from_date_new, to_date_new = _select_bulk_candidates_from_dates(stock_target_dates)
    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="stock_data_incremental",
        endpoint="/equities/bars/daily",
        estimated_rest_calls=max(len(stock_target_dates), 1),
        date_from=from_date_new,
        date_to=to_date_new,
        exact_dates=stock_target_dates,
        require_bulk=ctx.enforce_bulk_for_stock_data,
    )
    api_calls += decision.planner_api_calls
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="stock_data",
        current=2,
        total=7,
        endpoint="/equities/bars/daily",
        decision=decision,
        target_label=f"{len(stock_target_dates)} dates",
    )
    sync_fetch_planner._enforce_stock_bulk_plan_available(
        ctx,
        decision=decision,
        endpoint="/equities/bars/daily",
        progress_stage="stock_data",
        current=2,
        total=7,
        target_count=len(stock_target_dates),
    )

    used_rest_fallback = False
    bulk_fallback_reason: str | None = None
    bulk_result: BulkFetchResult | None = None
    bulk_outcome = await execute_stock_data_bulk_fetch(
        ctx,
        decision=decision,
        target_dates=stock_target_dates,
        stage_name="stock_data_incremental",
        progress_stage="stock_data",
        current=2,
        total=7,
        fallback_log_message="Incremental stock_data bulk fetch failed, falling back to REST: {}",
    )
    api_calls += bulk_outcome.api_calls
    stocks_updated += bulk_outcome.stocks_updated
    bulk_result = bulk_outcome.bulk_result
    used_rest_fallback = bulk_outcome.used_rest_fallback
    bulk_fallback_reason = bulk_outcome.fallback_reason

    if decision.method == "rest" or used_rest_fallback:
        rest_result = await _sync_incremental_stock_data_rest(
            ctx,
            stock_target_dates=stock_target_dates,
            decision=decision,
            used_rest_fallback=used_rest_fallback,
            fallback_reason=bulk_fallback_reason,
            bulk_result=bulk_result,
        )
        api_calls += rest_result["api_calls"]
        stocks_updated += rest_result["stocks_updated"]
        errors.extend(rest_result["errors"])
        if rest_result["cancelled"]:
            return {
                "api_calls": api_calls,
                "stocks_updated": stocks_updated,
                "stock_target_dates": stock_target_dates,
                "errors": errors,
                "cancelled": True,
            }

    await sync_publish_helpers._index_stock_data_rows(ctx)
    return {
        "api_calls": api_calls,
        "stocks_updated": stocks_updated,
        "stock_target_dates": stock_target_dates,
        "errors": errors,
        "cancelled": False,
    }


async def _sync_incremental_stock_data_rest(
    ctx: SyncContext,
    *,
    stock_target_dates: list[str],
    decision: Any,
    used_rest_fallback: bool,
    fallback_reason: str | None,
    bulk_result: BulkFetchResult | None,
) -> dict[str, Any]:
    api_calls = 0
    stocks_updated = 0
    errors: list[str] = []
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="stock_data",
        current=2,
        total=7,
        endpoint="/equities/bars/daily",
        method="rest",
        target_label=f"{len(stock_target_dates)} dates",
        fallback=used_rest_fallback,
        fallback_reason=fallback_reason,
    )
    for index, date in enumerate(stock_target_dates, start=1):
        if ctx.cancelled.is_set():
            return {"api_calls": api_calls, "stocks_updated": stocks_updated, "errors": errors, "cancelled": True}
        if index > 1 and index % 50 == 0:
            ctx.on_progress(
                "stock_data",
                2,
                7,
                f"Fetching /equities/bars/daily via REST: {index}/{len(stock_target_dates)} dates...",
            )
        try:
            payload, page_calls = await get_paginated_rows_with_call_count(
                ctx.client,
                "/equities/bars/daily",
                params={"date": date},
            )
            api_calls += page_calls

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
                publish=lambda rows: sync_publish_helpers._publish_stock_data_rows(ctx, rows),
            )
            stocks_updated += batch.published_count
        except Exception as e:
            errors.append(f"Date {date}: {e}")
    sync_fetch_planner._log_sync_fetch_execution(
        stage="stock_data_incremental",
        endpoint="/equities/bars/daily",
        decision=decision,
        executed="rest",
        actual_api_calls=api_calls,
        fallback=used_rest_fallback,
        bulk_result=bulk_result,
    )
    return {"api_calls": api_calls, "stocks_updated": stocks_updated, "errors": errors, "cancelled": False}


async def _sync_incremental_indices_stage(
    ctx: SyncContext,
    *,
    inspection: TimeSeriesInspection,
    topix_rows: list[dict[str, Any]],
    last_date: str | None,
    progress_current: int = 3,
    progress_total: int = 7,
) -> _SyncStageOutcome:
    known_master_codes = await _seed_index_master_from_catalog(ctx)
    outcome = await _run_incremental_indices_stage(
        ctx,
        inspection=inspection,
        topix_rows=topix_rows,
        last_date=last_date,
        known_master_codes=known_master_codes,
        catalog_codes=get_index_catalog_codes(),
        progress_current=progress_current,
        progress_total=progress_total,
    )
    return _SyncStageOutcome(
        api_calls=outcome.api_calls,
        errors=outcome.errors,
        cancelled=outcome.cancelled,
    )


async def _sync_incremental_margin_stage(
    ctx: SyncContext,
    *,
    inspection: TimeSeriesInspection,
    listed_market_target_rows: list[dict[str, str]],
    stock_rows: list[dict[str, Any]],
    topix_rows: list[dict[str, Any]],
    progress_current: int = 6,
    progress_total: int = 7,
) -> _SyncStageOutcome:
    ctx.on_progress("margin", progress_current, progress_total, "Fetching incremental margin data...")
    if ctx.cancelled.is_set():
        return _SyncStageOutcome(api_calls=0, errors=[], cancelled=True)

    all_stock_codes = sync_state_helpers._collect_unique_codes(
        [
            str(row.get("code", ""))
            for row in [*listed_market_target_rows, *stock_rows]
            if row.get("code")
        ]
    )
    margin_target_codes = extract_listed_market_codes(listed_market_target_rows)
    margin_frontier = (
        _latest_date([str(row.get("date")) for row in topix_rows if row.get("date")])
        or inspection.topix_max
        or inspection.stock_max
        or inspection.margin_max
    )
    margin_sync = await _sync_margin_data(
        ctx,
        margin_target_codes,
        progress_current=progress_current,
        progress_total=progress_total,
        stage_name="margin_incremental",
        anchor=inspection.margin_max,
        existing_margin_codes=set(inspection.margin_codes),
        trading_frontier=margin_frontier,
        skipped_market_count=max(len(all_stock_codes) - len(margin_target_codes), 0),
    )
    return _SyncStageOutcome(
        api_calls=margin_sync["api_calls"],
        errors=margin_sync["errors"],
        cancelled=margin_sync["cancelled"],
    )


class IncrementalSyncStrategy:
    """増分同期: 最終同期日以降のデータのみ取得"""

    def estimate_api_calls(self) -> int:
        return 180

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        errors: list[str] = []
        stock_rows: list[dict[str, Any]] = []

        try:
            if not ctx.market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"]):
                logger.warning(
                    "LAST_SYNC_DATE metadata is missing; proceeding incremental sync with DuckDB inspection anchors",
                    event="sync_fetch_strategy",
                    stage="incremental_bootstrap",
                    selected="incremental",
                    reason="missing_last_sync_metadata",
                )

            # Step 1: TOPIX（増分）
            ctx.on_progress("topix", 0, 7, "Fetching incremental TOPIX data...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # NOTE:
            # stock_data が一部日付で取りこぼされると topix_data より遅れる場合があるため、
            # 増分同期の基準日は stock_data の最新日を優先する。
            inspection = sync_state_helpers._inspect_time_series(ctx)
            cold_start_bootstrap = _is_incremental_cold_start(inspection)
            last_topix_date = inspection.topix_max
            last_stock_date = inspection.stock_max
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

            topix_payload, topix_calls = await get_paginated_rows_with_call_count(
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
                publish=lambda rows: sync_publish_helpers._publish_topix_rows(ctx, rows),
                index=lambda _rows: sync_publish_helpers._index_topix_rows(ctx),
            )
            total_calls += topix_calls
            topix_rows = topix_batch.rows

            # Step 2: 日次銘柄マスタ更新
            ctx.on_progress("stock_master_daily", 1, 7, "Updating daily stock master...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            missing_master_dates = await asyncio.to_thread(
                ctx.market_db.get_missing_stock_master_dates,
                limit=None,
            )
            master_sync = await sync_daily_stock_master(
                ctx,
                target_dates=missing_master_dates,
                progress_current=1,
                progress_total=7,
                allow_large_rest_fallback=False,
            )
            total_calls += master_sync["api_calls"]
            stock_rows = master_sync["latest_rows"]
            errors.extend(master_sync["errors"])
            if master_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])
            listed_market_target_rows = await asyncio.to_thread(
                ctx.market_db.get_fundamentals_target_stock_rows
            )
            if not listed_market_target_rows:
                listed_market_target_rows = stock_rows

            # Step 3: 新しい日付の株価データ
            ctx.on_progress("stock_data", 2, 7, "Fetching new stock data...")
            stock_sync = await _sync_incremental_stock_data_stage(
                ctx,
                topix_rows=topix_rows,
                anchor=last_date,
            )
            total_calls += stock_sync["api_calls"]
            stocks_updated += stock_sync["stocks_updated"]
            stock_target_dates = cast(list[str], stock_sync["stock_target_dates"])
            errors.extend(cast(list[str], stock_sync["errors"]))
            if stock_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            indices_sync = await _sync_incremental_indices_stage(
                ctx,
                inspection=inspection,
                topix_rows=topix_rows,
                last_date=last_date,
                progress_current=3,
                progress_total=7,
            )
            total_calls += indices_sync.api_calls
            errors.extend(indices_sync.errors)
            if indices_sync.cancelled:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 5: N225 options（増分 + 欠損履歴補完）
            options_new_dates = await _resolve_incremental_options_date_targets(
                ctx,
                inspection=inspection,
                topix_rows=topix_rows,
            )
            ctx.on_progress("options_225", 4, 7, f"Fetching N225 options for {len(options_new_dates)} dates...")
            options_sync = await _sync_options_225_dates(
                ctx,
                date_targets=options_new_dates,
                progress_stage="options_225",
                progress_current=4,
                progress_total=7,
                stage_name="options_225_incremental",
            )
            total_calls += int(options_sync["api_calls"])
            errors.extend(cast(list[str], options_sync["errors"]))
            if bool(options_sync["cancelled"]):
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # Step 6: listed markets fundamentals（増分: date 指定 + 欠損補完）
            ctx.on_progress("fundamentals", 5, 7, "Fetching incremental listed-market fundamentals...")
            if ctx.cancelled.is_set():
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            fundamentals_target_rows = _extract_listed_market_target_rows(listed_market_target_rows)

            fundamentals_sync = await _sync_fundamentals_incremental(
                ctx,
                fundamentals_target_rows,
                progress_current=5,
                progress_total=7,
            )
            total_calls += fundamentals_sync["api_calls"]
            fundamentals_updated += fundamentals_sync["updated"]
            fundamentals_dates_processed += fundamentals_sync["dates_processed"]
            errors.extend(fundamentals_sync["errors"])
            if fundamentals_sync["cancelled"]:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            margin_sync = await _sync_incremental_margin_stage(
                ctx,
                inspection=inspection,
                listed_market_target_rows=listed_market_target_rows,
                stock_rows=stock_rows,
                topix_rows=topix_rows,
                progress_current=6,
                progress_total=7,
            )
            total_calls += margin_sync.api_calls
            errors.extend(margin_sync.errors)
            if margin_sync.cancelled:
                return SyncResult(success=False, totalApiCalls=total_calls, errors=["Cancelled"])

            # メタデータ更新
            now_iso = datetime.now(UTC).isoformat()
            await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["LAST_SYNC_DATE"], now_iso)

            ctx.on_progress("complete", 7, 7, "Incremental sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=len(stock_target_dates),
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                errors=errors,
            )
        except sync_fetch_planner.BulkFetchRequiredError:
            raise
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


class RepairSyncStrategy:
    """warning 解消向け: fundamentals / metadata warnings の backfill"""

    def estimate_api_calls(self) -> int:
        return 200

    async def execute(self, ctx: SyncContext) -> SyncResult:
        total_calls = 0
        stocks_updated = 0
        fundamentals_updated = 0
        fundamentals_dates_processed = 0
        errors: list[str] = []

        try:
            ctx.on_progress("repair", 0, 200, "Inspecting DuckDB warning repair targets...")
            if ctx.cancelled.is_set():
                return _cancelled_sync_result(total_calls)

            fundamentals_target_rows = await asyncio.to_thread(
                ctx.market_db.get_fundamentals_target_stock_rows
            )

            if fundamentals_target_rows:
                def _on_fundamentals_progress(stage: str, current: int, total: int, message: str) -> None:
                    ctx.on_progress(
                        stage,
                        _scale_repair_progress(current, total, base=0),
                        200,
                        message,
                    )

                fundamentals_ctx = replace(ctx, on_progress=_on_fundamentals_progress)
                fundamentals_sync = await _sync_fundamentals_incremental(
                    fundamentals_ctx,
                    fundamentals_target_rows,
                )
                total_calls += fundamentals_sync["api_calls"]
                fundamentals_updated += fundamentals_sync["updated"]
                fundamentals_dates_processed += fundamentals_sync["dates_processed"]
                errors.extend(fundamentals_sync["errors"])
                if fundamentals_sync["cancelled"]:
                    return _cancelled_sync_result(total_calls)
            else:
                ctx.on_progress("fundamentals", 200, 200, "No listed-market fundamentals repair needed.")

            ctx.on_progress("complete", 200, 200, "Repair sync complete!")
            return SyncResult(
                success=len(errors) == 0,
                totalApiCalls=total_calls,
                stocksUpdated=stocks_updated,
                datesProcessed=0,
                fundamentalsUpdated=fundamentals_updated,
                fundamentalsDatesProcessed=fundamentals_dates_processed,
                errors=errors,
            )
        except Exception as e:
            return SyncResult(success=False, totalApiCalls=total_calls, errors=[str(e)])


def _scale_repair_progress(current: int, total: int, *, base: int) -> int:
    ratio = current / total if total > 0 else 1.0
    return base + round(min(max(ratio, 0.0), 1.0) * 100)


def _cancelled_sync_result(total_api_calls: int) -> SyncResult:
    return SyncResult(success=False, totalApiCalls=total_api_calls, errors=["Cancelled"])


async def _resolve_incremental_stock_date_targets(
    ctx: SyncContext,
    *,
    topix_rows: list[dict[str, Any]],
    anchor: str | None,
) -> list[str]:
    inspection = sync_state_helpers._inspect_time_series(ctx)
    topix_dates = sync_state_helpers._normalize_date_list(
        [
            str(r["date"])
            for r in topix_rows
            if r.get("date") and (anchor is None or _is_date_after(str(r["date"]), anchor))
        ]
    )
    if inspection.missing_stock_dates_count <= 0:
        return topix_dates

    missing_coverage = sync_state_helpers._inspect_time_series(
        ctx,
        missing_stock_dates_limit=inspection.missing_stock_dates_count,
    )
    return sync_state_helpers._normalize_date_list(topix_dates + list(missing_coverage.missing_stock_dates))


def get_strategy(resolved_mode: str) -> SyncStrategy:
    """モード名から戦略インスタンスを返す"""
    if resolved_mode == "initial":
        return InitialSyncStrategy()
    elif resolved_mode == "incremental":
        return IncrementalSyncStrategy()
    elif resolved_mode == "repair":
        return RepairSyncStrategy()
    return InitialSyncStrategy()


async def _seed_index_master_from_catalog(ctx: SyncContext) -> set[str]:
    seed_rows = build_index_master_seed_rows()
    if seed_rows:
        await asyncio.to_thread(ctx.market_db.upsert_index_master, seed_rows)
    return ctx.market_db.get_index_master_codes()


def _jquants_index_fetch_codes(codes: Iterable[str]) -> list[str]:
    return sorted(
        {
            normalized
            for normalized in (_normalize_index_code(code) for code in codes)
            if normalized and normalized not in _LOCAL_SYNTHETIC_INDEX_CODES
        }
    )
