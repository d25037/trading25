"""N225 options sync stage helpers."""

from __future__ import annotations

import asyncio
from typing import Any

from loguru import logger

from src.application.services import sync_fetch_planner, sync_publish_helpers, sync_state_helpers
from src.application.services.ingestion_pipeline import validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.sync_paginated_fetch import get_paginated_rows_with_call_count
from src.application.services.sync_row_converters import (
    build_target_date_set,
    convert_options_225_rows,
    normalize_bulk_options_225_rows,
    to_jquants_date_param,
    _date_sort_key,
    _is_date_after,
    _normalize_iso_date_text,
    _parse_date,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


def _select_bulk_candidates_from_dates(dates: list[str]) -> tuple[str | None, str | None]:
    parsed = [_parse_date(value) for value in dates]
    normalized = [d for d in parsed if d is not None]
    if not normalized:
        return None, None
    return min(normalized).isoformat(), max(normalized).isoformat()


async def sync_options_225_dates(
    ctx: Any,
    *,
    date_targets: list[str],
    progress_stage: str,
    progress_current: int,
    progress_total: int,
    stage_name: str,
) -> dict[str, Any]:
    target_dates = sorted(
        {
            normalized
            for normalized in (_normalize_iso_date_text(value) for value in date_targets)
            if normalized is not None
        },
        key=_date_sort_key,
    )
    if not target_dates:
        ctx.on_progress(progress_stage, progress_current, progress_total, "No N225 options dates to sync.")
        return {"api_calls": 0, "errors": [], "cancelled": False}

    from_date, to_date = _select_bulk_candidates_from_dates(target_dates)
    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage=stage_name,
        endpoint="/derivatives/bars/daily/options/225",
        estimated_rest_calls=max(len(target_dates), 1),
        date_from=from_date,
        date_to=to_date,
        exact_dates=target_dates,
    )
    api_calls = decision.planner_api_calls
    errors: list[str] = []
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage=progress_stage,
        current=progress_current,
        total=progress_total,
        endpoint="/derivatives/bars/daily/options/225",
        decision=decision,
        target_label=f"{len(target_dates)} dates",
    )

    target_date_set = build_target_date_set(target_dates)
    stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    if decision.method == "bulk":

        async def _consume_options_bulk_rows(
            batch_rows: list[dict[str, Any]],
            _file_info: BulkFileInfo,
        ) -> None:
            normalized_rows = convert_options_225_rows(normalize_bulk_options_225_rows(batch_rows))
            if target_date_set is not None:
                normalized_rows[:] = [row for row in normalized_rows if row.get("date") in target_date_set]
            rows = validate_rows_required_fields(
                normalized_rows,
                required_fields=("code", "date"),
                dedupe_keys=("code", "date"),
                stage="options_225",
            )
            if not rows:
                return
            await sync_publish_helpers._publish_options_225_rows(ctx, rows)
            await sync_publish_helpers._publish_synthetic_nikkei_rows(ctx, rows)

        bulk_outcome = await sync_fetch_planner._execute_bulk_fetch_stage(
            ctx,
            decision=decision,
            stage_name=stage_name,
            progress_stage=progress_stage,
            current=progress_current,
            total=progress_total,
            endpoint="/derivatives/bars/daily/options/225",
            target_label=f"{len(target_dates)} dates",
            on_rows_batch=_consume_options_bulk_rows,
            fallback_log_message="options_225 bulk fetch unavailable, falling back to REST: {}",
        )
        api_calls += bulk_outcome.api_calls
        stage_api_calls += bulk_outcome.api_calls
        bulk_result = bulk_outcome.bulk_result
        used_rest_fallback = bulk_outcome.used_rest_fallback
        bulk_fallback_reason = bulk_outcome.fallback_reason
    else:
        used_rest_fallback = False
        bulk_fallback_reason = None

    if decision.method == "rest" or used_rest_fallback:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage=progress_stage,
            current=progress_current,
            total=progress_total,
            endpoint="/derivatives/bars/daily/options/225",
            method="rest",
            target_label=f"{len(target_dates)} dates",
            fallback=used_rest_fallback,
            fallback_reason=bulk_fallback_reason,
        )
        for index, target_date in enumerate(target_dates, start=1):
            if ctx.cancelled.is_set():
                return {"api_calls": api_calls, "errors": errors, "cancelled": True}
            if index > 1 and index % 50 == 0:
                ctx.on_progress(
                    progress_stage,
                    progress_current,
                    progress_total,
                    f"Fetching /derivatives/bars/daily/options/225 via REST: {index}/{len(target_dates)} dates...",
                )
            try:
                payload, page_calls = await get_paginated_rows_with_call_count(
                    ctx.client,
                    "/derivatives/bars/daily/options/225",
                    params={"date": to_jquants_date_param(target_date)},
                )
                api_calls += page_calls
                stage_api_calls += page_calls
                rows = validate_rows_required_fields(
                    convert_options_225_rows(payload),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="options_225",
                )
                if rows:
                    await sync_publish_helpers._publish_options_225_rows(ctx, rows)
                    await sync_publish_helpers._publish_synthetic_nikkei_rows(ctx, rows)
            except Exception as e:
                errors.append(f"Options {target_date}: {e}")
                logger.warning("Options date {} sync error: {}", target_date, e)
        sync_fetch_planner._log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/derivatives/bars/daily/options/225",
            decision=decision,
            executed="rest",
            actual_api_calls=stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=bulk_result,
        )

    await sync_publish_helpers._index_options_225_rows(ctx)
    await sync_publish_helpers._index_indices_rows(ctx)
    return {"api_calls": api_calls, "errors": errors, "cancelled": False}


async def resolve_incremental_options_date_targets(
    ctx: Any,
    *,
    inspection: TimeSeriesInspection,
    topix_rows: list[dict[str, Any]],
) -> list[str]:
    last_options_225_date = inspection.latest_options_225_date
    if not last_options_225_date:
        options_dates = await asyncio.to_thread(ctx.market_db.get_topix_dates)
        if options_dates:
            return options_dates
        return sorted(
            {str(r["date"]) for r in topix_rows if r.get("date")},
            key=_date_sort_key,
        )

    options_dates = sync_state_helpers._normalize_date_list(
        [
            str(r["date"])
            for r in topix_rows
            if r.get("date") and _is_date_after(str(r["date"]), last_options_225_date)
        ]
    )
    if inspection.missing_options_225_dates_count <= 0:
        return options_dates

    missing_coverage = sync_state_helpers._inspect_time_series(
        ctx,
        missing_options_225_dates_limit=inspection.missing_options_225_dates_count,
    )
    return sync_state_helpers._normalize_date_list(options_dates + list(missing_coverage.missing_options_225_dates))
