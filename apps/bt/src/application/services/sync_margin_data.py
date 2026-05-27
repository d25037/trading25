"""Margin-interest sync stage for market DB sync."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, cast

from loguru import logger

from src.application.services import sync_bulk_ingest_helpers, sync_fetch_planner, sync_publish_helpers, sync_state_helpers
from src.application.services.ingestion_pipeline import validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.listed_market_targets import (
    normalize_frontier_date,
    resolve_frontier_cache_codes,
    serialize_frontier_code_cache,
)
from src.application.services.sync_row_converters import (
    convert_margin_rows as _convert_margin_rows,
    to_jquants_date_param as _to_jquants_date_param,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code, stock_code_candidates

_MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS = 250


def _format_target_label(
    count: int,
    unit: str,
    *,
    skipped_empty: int = 0,
    skipped_market: int = 0,
) -> str:
    parts = [f"{count} {unit}"]
    if skipped_market > 0:
        parts.append(f"skipped_market={skipped_market}")
    if skipped_empty > 0:
        parts.append(f"skipped_empty={skipped_empty}")
    return ", ".join(parts)


def _load_frontier_code_cache(market_db: Any, key: str, frontier: str | None) -> set[str]:
    return resolve_frontier_cache_codes(market_db.get_sync_metadata(key), frontier)


async def _save_frontier_code_cache(
    ctx: Any,
    key: str,
    frontier: str | None,
    codes: set[str] | list[str],
) -> None:
    await asyncio.to_thread(
        ctx.market_db.set_sync_metadata,
        key,
        serialize_frontier_code_cache(frontier, codes),
    )


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


async def _fetch_margin_by_code(
    client: Any,
    code: str,
    *,
    date_from: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch /markets/margin-interest by trying 5-digit then 4-digit code formats."""
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
        params: dict[str, Any] = {"code": candidate}
        if date_from:
            params["from"] = _to_jquants_date_param(date_from)
        try:
            data, page_calls = await _get_paginated_rows_with_call_count(
                client,
                "/markets/margin-interest",
                params=params,
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
        raise RuntimeError(f"margin-interest code fetch failed for {code}")
    raise last_error


async def sync_margin_data(
    ctx: Any,
    target_codes: list[str],
    *,
    progress_current: int,
    progress_total: int,
    stage_name: str,
    anchor: str | None = None,
    existing_margin_codes: set[str] | None = None,
    trading_frontier: str | None = None,
    skipped_market_count: int = 0,
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []

    normalized_codes = sync_state_helpers._collect_unique_codes(target_codes)
    if not normalized_codes:
        return {
            "api_calls": api_calls,
            "updated": updated,
            "errors": errors,
            "cancelled": False,
        }

    has_existing_margin_snapshot = anchor is not None and existing_margin_codes is not None
    existing_margin_code_set = set(existing_margin_codes or set())
    current_frontier = normalize_frontier_date(trading_frontier)
    current_empty_codes = _load_frontier_code_cache(
        ctx.market_db,
        METADATA_KEYS["MARGIN_EMPTY_CODES"],
        current_frontier,
    )
    all_backfill_codes = (
        sorted(set(normalized_codes) - existing_margin_code_set)
        if has_existing_margin_snapshot
        else []
    )
    skipped_empty_codes = sorted(code for code in all_backfill_codes if code in current_empty_codes)
    backfill_codes = [code for code in all_backfill_codes if code not in current_empty_codes]
    rest_codes = normalized_codes
    if has_existing_margin_snapshot:
        rest_codes = [code for code in normalized_codes if code in existing_margin_code_set]
    target_code_set = set(rest_codes) | set(backfill_codes)
    target_label = _format_target_label(
        len(normalized_codes),
        "codes",
        skipped_empty=len(skipped_empty_codes),
        skipped_market=skipped_market_count,
    )

    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage=stage_name,
        endpoint="/markets/margin-interest",
        estimated_rest_calls=max(len(rest_codes) + len(backfill_codes), 1),
        date_from=anchor,
    )
    api_calls += decision.planner_api_calls
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="margin",
        current=progress_current,
        total=progress_total,
        endpoint="/markets/margin-interest",
        decision=decision,
        target_label=target_label,
    )

    used_rest_fallback = False
    bulk_fallback_reason: str | None = None
    bulk_stage_api_calls = 0
    rest_stage_api_calls = 0
    backfill_stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    empty_fetch_codes: set[str] = set()

    if decision.method == "bulk":
        effective_plan = decision.plan
        skipped_anchor_files = 0
        if effective_plan is not None:
            effective_plan, skipped_anchor_files = sync_fetch_planner._filter_bulk_plan_after_exclusive_anchor(
                effective_plan,
                anchor=anchor,
            )
            if skipped_anchor_files > 0:
                logger.info(
                    "margin bulk files at or before anchor skipped",
                    event="sync_fetch_strategy",
                    stage=stage_name,
                    endpoint="/markets/margin-interest",
                    skippedFiles=skipped_anchor_files,
                    remainingFiles=len(effective_plan.files),
                    anchor=anchor,
                )
        if effective_plan is not None and skipped_anchor_files > 0 and len(effective_plan.files) == 0:
            ctx.on_progress(
                "margin",
                progress_current,
                progress_total,
                (
                    "No new /markets/margin-interest bulk files after "
                    f"{anchor}; skipping margin bulk fetch."
                ),
            )
            sync_fetch_planner._emit_fetch_detail(
                ctx,
                {
                    "eventType": "execution",
                    "stage": "margin",
                    "endpoint": "/markets/margin-interest",
                    "method": "bulk",
                    "targetLabel": target_label,
                    "reason": None,
                    "reasonDetail": "no_new_bulk_files_after_anchor",
                    "estimatedRestCalls": None,
                    "estimatedBulkCalls": None,
                    "plannerApiCalls": None,
                    "fallback": False,
                    "fallbackReason": None,
                },
            )
            sync_fetch_planner._log_sync_fetch_execution(
                stage=stage_name,
                endpoint="/markets/margin-interest",
                decision=decision,
                executed="bulk",
                actual_api_calls=0,
                fallback=False,
                bulk_result=BulkFetchResult(
                    rows=[],
                    api_calls=0,
                    cache_hits=0,
                    cache_misses=0,
                    selected_files=0,
                ),
            )
        elif effective_plan is None or len(effective_plan.files) == 0:
            used_rest_fallback = True
            bulk_fallback_reason = sync_fetch_planner._resolve_bulk_fallback_reason(effective_plan)
            logger.warning(
                "{} bulk fetch selected but no bulk files were available, falling back to REST: {}",
                stage_name,
                bulk_fallback_reason,
            )
        else:
            try:
                sync_fetch_planner._emit_fetch_execution_progress(
                    ctx,
                    progress_stage="margin",
                    current=progress_current,
                    total=progress_total,
                    endpoint="/markets/margin-interest",
                    method="bulk",
                    target_label=target_label,
                )

                async def _consume_margin_bulk_rows(
                    batch_rows: list[dict[str, Any]],
                    _file_info: BulkFileInfo,
                ) -> None:
                    nonlocal updated
                    updated += await sync_bulk_ingest_helpers._ingest_margin_bulk_batch(
                        ctx,
                        batch_rows=batch_rows,
                        target_codes=target_code_set,
                        min_date_exclusive=anchor,
                    )

                bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
                    effective_plan,
                    on_rows_batch=_consume_margin_bulk_rows,
                    accumulate_rows=False,
                )
                api_calls += bulk_result.api_calls
                bulk_stage_api_calls += bulk_result.api_calls
                sync_fetch_planner._log_sync_fetch_execution(
                    stage=stage_name,
                    endpoint="/markets/margin-interest",
                    decision=decision,
                    executed="bulk",
                    actual_api_calls=bulk_stage_api_calls,
                    fallback=False,
                    bulk_result=bulk_result,
                )
            except Exception as e:
                used_rest_fallback = True
                bulk_fallback_reason = sync_fetch_planner._summarize_exception(e)
                logger.warning(
                    "{} bulk fetch failed, falling back to REST: {}",
                    stage_name,
                    bulk_fallback_reason,
                )

    if decision.method == "rest" or used_rest_fallback:
        rest_estimated_calls = max(len(rest_codes) + len(backfill_codes), 1)
        if rest_estimated_calls > _MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS:
            reason = bulk_fallback_reason or decision.reason
            message = (
                "Refusing margin_data REST fallback for "
                f"{len(rest_codes)} refresh codes and {len(backfill_codes)} backfill codes "
                f"(estimated REST calls={rest_estimated_calls}, reason={reason}). "
                "Run with /markets/margin-interest bulk available or narrow the margin target universe."
            )
            ctx.on_progress(
                "margin",
                progress_current,
                progress_total,
                message,
            )
            errors.append(message)
            return {
                "api_calls": api_calls,
                "updated": updated,
                "errors": errors,
                "cancelled": False,
            }

    if (decision.method == "rest" or used_rest_fallback) and rest_codes:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="margin",
            current=progress_current,
            total=progress_total,
            endpoint="/markets/margin-interest",
            method="rest",
            target_label=target_label,
            fallback=used_rest_fallback,
            fallback_reason=bulk_fallback_reason,
        )
        for idx, code in enumerate(rest_codes, start=1):
            if ctx.cancelled.is_set():
                return {
                    "api_calls": api_calls,
                    "updated": updated,
                    "errors": errors,
                    "cancelled": True,
                }

            if idx > 1 and idx % 100 == 0:
                ctx.on_progress(
                    "margin",
                    progress_current,
                    progress_total,
                    f"Fetching /markets/margin-interest via REST: {idx}/{len(rest_codes)} codes...",
                )

            try:
                data, page_calls = await _fetch_margin_by_code(
                    ctx.client,
                    code,
                    date_from=anchor,
                )
                api_calls += page_calls
                rest_stage_api_calls += page_calls
                rows = validate_rows_required_fields(
                    _convert_margin_rows(
                        data,
                        default_code=code,
                        min_date_exclusive=anchor,
                    ),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="margin_data",
                )
                if rows:
                    updated += await sync_publish_helpers._publish_margin_rows(ctx, rows)
            except Exception as e:
                errors.append(f"Margin code {code}: {e}")

        sync_fetch_planner._log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/markets/margin-interest",
            decision=decision,
            executed="rest",
            actual_api_calls=rest_stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=bulk_result,
        )

    if (
        backfill_codes
        and len(backfill_codes) > _MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS
    ):
        message = (
            "Skipping margin_data REST backfill for "
            f"{len(backfill_codes)} missing codes after bulk phase "
            f"(limit={_MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS}). "
            "Run with complete /markets/margin-interest bulk coverage or narrow the margin target universe."
        )
        ctx.on_progress(
            "margin",
            progress_current,
            progress_total,
            message,
        )
        errors.append(message)
        backfill_codes = []

    if backfill_codes:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="margin",
            current=progress_current,
            total=progress_total,
            endpoint="/markets/margin-interest",
            method="rest",
            target_label=_format_target_label(
                len(backfill_codes),
                "backfill codes",
                skipped_empty=len(skipped_empty_codes),
            ),
        )
        for idx, code in enumerate(backfill_codes, start=1):
            if ctx.cancelled.is_set():
                return {
                    "api_calls": api_calls,
                    "updated": updated,
                    "errors": errors,
                    "cancelled": True,
                }

            if idx > 1 and idx % 100 == 0:
                ctx.on_progress(
                    "margin",
                    progress_current,
                    progress_total,
                    (
                        "Fetching /markets/margin-interest via REST: "
                        f"{idx}/{len(backfill_codes)} backfill codes..."
                    ),
                )

            try:
                data, page_calls = await _fetch_margin_by_code(ctx.client, code)
                api_calls += page_calls
                backfill_stage_api_calls += page_calls
                if not data:
                    empty_fetch_codes.add(code)
                    continue
                rows = validate_rows_required_fields(
                    _convert_margin_rows(data, default_code=code),
                    required_fields=("code", "date"),
                    dedupe_keys=("code", "date"),
                    stage="margin_data",
                )
                if rows:
                    updated += await sync_publish_helpers._publish_margin_rows(ctx, rows)
                else:
                    empty_fetch_codes.add(code)
            except Exception as e:
                errors.append(f"Margin backfill code {code}: {e}")

        sync_fetch_planner._log_sync_fetch_execution(
            stage=f"{stage_name}_backfill",
            endpoint="/markets/margin-interest",
            decision=decision,
            executed="rest",
            actual_api_calls=backfill_stage_api_calls,
            fallback=used_rest_fallback,
            bulk_result=None,
        )

    if updated == 0:
        if empty_fetch_codes:
            next_empty_codes = set(current_empty_codes)
            next_empty_codes.update(empty_fetch_codes)
            if has_existing_margin_snapshot:
                next_empty_codes -= existing_margin_code_set
            await _save_frontier_code_cache(
                ctx,
                METADATA_KEYS["MARGIN_EMPTY_CODES"],
                current_frontier,
                next_empty_codes,
            )
        ctx.on_progress(
            "margin",
            progress_current,
            progress_total,
            "No margin_data rows changed; skipping margin index/export.",
        )
        return {
            "api_calls": api_calls,
            "updated": updated,
            "errors": errors,
            "cancelled": False,
        }

    await sync_publish_helpers._index_margin_rows(ctx)
    next_empty_codes = set(current_empty_codes)
    next_empty_codes.update(empty_fetch_codes)
    latest_margin_codes = set(sync_state_helpers._inspect_time_series(ctx).margin_codes)
    next_empty_codes = {
        code for code in next_empty_codes
        if code not in latest_margin_codes
    }
    await _save_frontier_code_cache(
        ctx,
        METADATA_KEYS["MARGIN_EMPTY_CODES"],
        current_frontier,
        next_empty_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "cancelled": False,
    }
