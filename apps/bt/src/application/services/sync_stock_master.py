"""Daily stock-master sync stage helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, cast

from loguru import logger

from src.application.services import sync_fetch_planner
from src.application.services.jquants_bulk_service import BulkFileInfo
from src.application.services.sync_row_converters import (
    build_target_date_set as _build_target_date_set,
    convert_stock_rows as _convert_stock_rows,
    group_stock_master_bulk_rows_by_date as _group_stock_master_bulk_rows_by_date,
    to_jquants_date_param as _to_jquants_date_param,
    _date_sort_key,
    _is_date_after,
    _parse_date,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS

_STOCK_MASTER_REST_PAGES_PER_DATE_ESTIMATE = 4
_STOCK_MASTER_REST_FALLBACK_MAX_ESTIMATED_CALLS = 12


@dataclass(frozen=True)
class _StockMasterSyncParams:
    target_dates: list[str]
    progress_current: int
    progress_total: int
    allow_large_rest_fallback: bool


@dataclass
class _StockMasterSyncState:
    api_calls: int = 0
    rows_updated: int = 0
    latest_rows: list[dict[str, Any]] | None = None
    latest_snapshot_date: str | None = None
    updated_dates: set[str] | None = None

    def __post_init__(self) -> None:
        if self.latest_rows is None:
            self.latest_rows = []
        if self.updated_dates is None:
            self.updated_dates = set()

    def record_rows(self, snapshot_date: str, rows: list[dict[str, Any]]) -> None:
        assert self.latest_rows is not None
        assert self.updated_dates is not None
        self.updated_dates.add(snapshot_date)
        if self.latest_snapshot_date is None or _is_date_after(snapshot_date, self.latest_snapshot_date):
            self.latest_snapshot_date = snapshot_date
            self.latest_rows = list(rows)
        elif snapshot_date == self.latest_snapshot_date:
            self.latest_rows.extend(rows)


async def _get_paginated_rows_with_call_count(
    client: Any,
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


def _select_bulk_candidates_from_dates(dates: list[str]) -> tuple[str | None, str | None]:
    parsed = [_parse_date(value) for value in dates]
    normalized = [d for d in parsed if d is not None]
    if not normalized:
        return None, None
    return min(normalized).isoformat(), max(normalized).isoformat()


def _estimate_stock_master_rest_calls(date_count: int) -> int:
    return max(date_count, 1) * _STOCK_MASTER_REST_PAGES_PER_DATE_ESTIMATE


def _empty_stock_master_sync_result() -> dict[str, Any]:
    return {
        "api_calls": 0,
        "updated": 0,
        "latest_rows": [],
        "errors": [],
        "cancelled": False,
    }


def _stock_master_sync_result(
    state: _StockMasterSyncState,
    normalized_dates: list[str],
    *,
    cancelled: bool = False,
) -> dict[str, Any]:
    latest_rows = state.latest_rows if state.latest_rows is not None else []
    if cancelled:
        errors: list[str] = []
    else:
        updated_dates = state.updated_dates if state.updated_dates is not None else set()
        failed_dates = [date for date in normalized_dates if date not in updated_dates]
        errors = [
            f"No stock master rows returned for {len(failed_dates)} TOPIX dates: {', '.join(failed_dates[:10])}"
        ] if failed_dates else []
    return {
        "api_calls": state.api_calls,
        "updated": state.rows_updated,
        "latest_rows": latest_rows,
        "errors": errors,
        "cancelled": cancelled,
    }


async def _plan_stock_master_fetch(
    ctx: Any,
    params: _StockMasterSyncParams,
    *,
    total_dates: int,
) -> Any:
    date_from, date_to = _select_bulk_candidates_from_dates(params.target_dates)
    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="stock_master_daily",
        endpoint="/equities/master",
        estimated_rest_calls=_estimate_stock_master_rest_calls(total_dates),
        date_from=date_from,
        date_to=date_to,
        exact_dates=params.target_dates,
        min_rest_calls_to_probe_bulk=1,
        disable_future_bulk_on_probe_failure=False,
    )
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="stock_master_daily",
        current=params.progress_current,
        total=params.progress_total,
        endpoint="/equities/master",
        decision=decision,
        target_label=f"{total_dates} dates",
    )
    return decision


async def _publish_stock_master_daily_rows(
    ctx: Any,
    params: _StockMasterSyncParams,
    *,
    rows_to_upsert: list[dict[str, Any]],
    message: str,
) -> int:
    ctx.on_progress(
        "stock_master_daily",
        params.progress_current,
        params.progress_total,
        message,
    )
    return await asyncio.to_thread(
        ctx.market_db.upsert_stock_master_daily_rows,
        rows_to_upsert,
    )


async def _execute_stock_master_bulk(
    ctx: Any,
    params: _StockMasterSyncParams,
    state: _StockMasterSyncState,
    decision: Any,
    *,
    target_date_set: set[str] | None,
    total_dates: int,
) -> tuple[list[str], bool, str | None]:
    if decision.method != "bulk":
        return params.target_dates, False, None
    if decision.plan is None or not decision.plan.files:
        fallback_reason = sync_fetch_planner._resolve_bulk_fallback_reason(decision.plan)
        return params.target_dates, True, fallback_reason

    try:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="stock_master_daily",
            current=params.progress_current,
            total=params.progress_total,
            endpoint="/equities/master",
            method="bulk",
            target_label=f"{total_dates} dates",
        )

        async def _consume_stock_master_bulk_rows(
            batch_rows: list[dict[str, Any]],
            file_info: BulkFileInfo,
        ) -> None:
            default_snapshot_date = (
                file_info.range_start.isoformat()
                if file_info.range_start is not None and file_info.range_start == file_info.range_end
                else None
            )
            rows_by_date = _group_stock_master_bulk_rows_by_date(
                batch_rows,
                target_dates=target_date_set,
                default_snapshot_date=default_snapshot_date,
            )
            rows_to_upsert: list[dict[str, Any]] = []
            for snapshot_date in sorted(rows_by_date, key=_date_sort_key):
                rows = rows_by_date[snapshot_date]
                rows_to_upsert.extend(dict(row, date=snapshot_date) for row in rows)
                state.record_rows(snapshot_date, rows)
            if rows_to_upsert:
                state.rows_updated += await _publish_stock_master_daily_rows(
                    ctx,
                    params,
                    rows_to_upsert=rows_to_upsert,
                    message=f"Publishing daily stock master bulk batch ({len(rows_to_upsert)} rows)...",
                )

        bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
            decision.plan,
            on_rows_batch=_consume_stock_master_bulk_rows,
            accumulate_rows=False,
        )
        state.api_calls += bulk_result.api_calls
        sync_fetch_planner._log_sync_fetch_execution(
            stage="stock_master_daily",
            endpoint="/equities/master",
            decision=decision,
            executed="bulk",
            actual_api_calls=decision.planner_api_calls + bulk_result.api_calls,
            fallback=False,
            bulk_result=bulk_result,
        )
        updated_dates = state.updated_dates if state.updated_dates is not None else set()
        rest_target_dates = [date for date in params.target_dates if date not in updated_dates]
        if not rest_target_dates:
            return rest_target_dates, False, None
        return (
            rest_target_dates,
            True,
            f"bulk returned no rows for {len(rest_target_dates)} target dates",
        )
    except Exception as e:
        fallback_reason = sync_fetch_planner._summarize_exception(e)
        logger.exception(
            "stock master bulk fetch failed, falling back to REST: {}",
            fallback_reason,
        )
        return params.target_dates, True, fallback_reason


def _enforce_stock_master_rest_fallback_budget(
    ctx: Any,
    params: _StockMasterSyncParams,
    *,
    decision: Any,
    rest_target_dates: list[str],
    rest_fallback_reason: str | None,
) -> None:
    rest_estimated_calls = _estimate_stock_master_rest_calls(len(rest_target_dates))
    if params.allow_large_rest_fallback or rest_estimated_calls <= _STOCK_MASTER_REST_FALLBACK_MAX_ESTIMATED_CALLS:
        return
    reason = rest_fallback_reason or decision.reason
    message = (
        "Refusing stock_master_daily REST fallback for "
        f"{len(rest_target_dates)} dates "
        f"(estimated REST calls={rest_estimated_calls}, reason={reason}). "
        "Run with bulk available or reduce the stock master backfill window."
    )
    ctx.on_progress(
        "stock_master_daily",
        params.progress_current,
        params.progress_total,
        message,
    )
    raise RuntimeError(message)


async def _execute_stock_master_rest(
    ctx: Any,
    params: _StockMasterSyncParams,
    state: _StockMasterSyncState,
    decision: Any,
    *,
    rest_target_dates: list[str],
    used_rest_fallback: bool,
    rest_fallback_reason: str | None,
) -> bool:
    _enforce_stock_master_rest_fallback_budget(
        ctx,
        params,
        decision=decision,
        rest_target_dates=rest_target_dates,
        rest_fallback_reason=rest_fallback_reason,
    )
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="stock_master_daily",
        current=params.progress_current,
        total=params.progress_total,
        endpoint="/equities/master",
        method="rest",
        target_label=f"{len(rest_target_dates)} dates",
        fallback=used_rest_fallback,
        fallback_reason=rest_fallback_reason,
    )
    rest_calls = 0
    for index, snapshot_date in enumerate(rest_target_dates, start=1):
        if ctx.cancelled.is_set():
            return True
        ctx.on_progress(
            "stock_master_daily",
            params.progress_current,
            params.progress_total,
            f"Fetching daily stock master {index}/{len(rest_target_dates)}: {snapshot_date}",
        )
        payload, calls = await _get_paginated_rows_with_call_count(
            ctx.client,
            "/equities/master",
            params={"date": _to_jquants_date_param(snapshot_date)},
        )
        state.api_calls += calls
        rest_calls += calls
        rows = _convert_stock_rows(payload)
        if not rows:
            continue
        dated_rows = [dict(row, date=snapshot_date) for row in rows]
        state.rows_updated += await _publish_stock_master_daily_rows(
            ctx,
            params,
            rows_to_upsert=dated_rows,
            message=(
                f"Publishing daily stock master {index}/{len(rest_target_dates)}: "
                f"{snapshot_date} ({len(rows)} rows)"
            ),
        )
        state.record_rows(snapshot_date, rows)
    sync_fetch_planner._log_sync_fetch_execution(
        stage="stock_master_daily",
        endpoint="/equities/master",
        decision=decision,
        executed="rest",
        actual_api_calls=decision.planner_api_calls + rest_calls,
        fallback=used_rest_fallback,
    )
    return False


async def _finalize_stock_master_sync(
    ctx: Any,
    params: _StockMasterSyncParams,
    *,
    rows_updated: int,
) -> None:
    if rows_updated <= 0:
        return
    ctx.on_progress(
        "stock_master_daily",
        params.progress_current,
        params.progress_total,
        "Rebuilding stock master intervals...",
    )
    await asyncio.to_thread(ctx.market_db.rebuild_stock_master_intervals)
    ctx.on_progress(
        "stock_master_daily",
        params.progress_current,
        params.progress_total,
        "Rebuilding latest stock master snapshot...",
    )
    await asyncio.to_thread(ctx.market_db.rebuild_stocks_latest)
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        METADATA_KEYS["LAST_STOCKS_REFRESH"],
        datetime.now(UTC).isoformat(),
    )


async def sync_daily_stock_master(
    ctx: Any,
    *,
    target_dates: list[str],
    progress_current: int,
    progress_total: int,
    allow_large_rest_fallback: bool = True,
) -> dict[str, Any]:
    """Fetch daily stock master snapshots and rebuild derived master tables."""
    normalized_dates = sorted({date for date in target_dates if date})
    if not normalized_dates:
        return _empty_stock_master_sync_result()

    params = _StockMasterSyncParams(
        target_dates=normalized_dates,
        progress_current=progress_current,
        progress_total=progress_total,
        allow_large_rest_fallback=allow_large_rest_fallback,
    )
    state = _StockMasterSyncState()
    total_dates = len(normalized_dates)
    target_date_set = _build_target_date_set(normalized_dates)
    decision = await _plan_stock_master_fetch(
        ctx,
        params,
        total_dates=total_dates,
    )
    state.api_calls += decision.planner_api_calls
    rest_target_dates, used_rest_fallback, rest_fallback_reason = await _execute_stock_master_bulk(
        ctx,
        params,
        state,
        decision,
        target_date_set=target_date_set,
        total_dates=total_dates,
    )

    if decision.method == "rest" or used_rest_fallback:
        cancelled = await _execute_stock_master_rest(
            ctx,
            params,
            state,
            decision,
            rest_target_dates=rest_target_dates,
            used_rest_fallback=used_rest_fallback,
            rest_fallback_reason=rest_fallback_reason,
        )
        if cancelled:
            return _stock_master_sync_result(state, normalized_dates, cancelled=True)

    await _finalize_stock_master_sync(ctx, params, rows_updated=state.rows_updated)
    return _stock_master_sync_result(state, normalized_dates)
