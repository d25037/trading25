"""Margin-interest sync stage for market DB sync."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

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
from src.application.services.sync_paginated_fetch import get_paginated_rows_with_call_count
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code, stock_code_candidates

_MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS = 250


@dataclass(frozen=True)
class MarginSyncTargets:
    normalized_codes: list[str]
    rest_codes: list[str]
    backfill_codes: list[str]
    skipped_empty_codes: list[str]
    target_code_set: set[str]
    current_empty_codes: set[str]
    current_frontier: str | None
    has_existing_margin_snapshot: bool
    existing_margin_code_set: set[str]


@dataclass
class MarginStageResult:
    api_calls: int = 0
    updated: int = 0
    errors: list[str] | None = None
    cancelled: bool = False
    empty_fetch_codes: set[str] | None = None
    used_rest_fallback: bool = False
    fallback_reason: str | None = None
    bulk_result: BulkFetchResult | None = None

    def error_list(self) -> list[str]:
        if self.errors is None:
            self.errors = []
        return self.errors

    def empty_codes(self) -> set[str]:
        if self.empty_fetch_codes is None:
            self.empty_fetch_codes = set()
        return self.empty_fetch_codes


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


def _margin_result(
    *,
    api_calls: int,
    updated: int,
    errors: list[str],
    cancelled: bool = False,
) -> dict[str, Any]:
    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "cancelled": cancelled,
    }


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
            data, page_calls = await get_paginated_rows_with_call_count(
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


def _resolve_margin_targets(
    ctx: Any,
    normalized_codes: list[str],
    *,
    anchor: str | None,
    existing_margin_codes: set[str] | None,
    trading_frontier: str | None,
) -> MarginSyncTargets:
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
    rest_codes = (
        [code for code in normalized_codes if code in existing_margin_code_set]
        if has_existing_margin_snapshot
        else normalized_codes
    )
    return MarginSyncTargets(
        normalized_codes=normalized_codes,
        rest_codes=rest_codes,
        backfill_codes=backfill_codes,
        skipped_empty_codes=skipped_empty_codes,
        target_code_set=set(rest_codes) | set(backfill_codes),
        current_empty_codes=current_empty_codes,
        current_frontier=current_frontier,
        has_existing_margin_snapshot=has_existing_margin_snapshot,
        existing_margin_code_set=existing_margin_code_set,
    )


async def _sync_margin_bulk_stage(
    ctx: Any,
    *,
    decision: Any,
    targets: MarginSyncTargets,
    target_label: str,
    progress_current: int,
    progress_total: int,
    stage_name: str,
    anchor: str | None,
) -> MarginStageResult:
    result = MarginStageResult()
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
            f"No new /markets/margin-interest bulk files after {anchor}; skipping margin bulk fetch.",
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
            bulk_result=BulkFetchResult(rows=[], api_calls=0, cache_hits=0, cache_misses=0, selected_files=0),
        )
        return result

    if effective_plan is None or len(effective_plan.files) == 0:
        result.used_rest_fallback = True
        result.fallback_reason = sync_fetch_planner._resolve_bulk_fallback_reason(effective_plan)
        logger.warning(
            "{} bulk fetch selected but no bulk files were available, falling back to REST: {}",
            stage_name,
            result.fallback_reason,
        )
        return result

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
            result.updated += await sync_bulk_ingest_helpers._ingest_margin_bulk_batch(
                ctx,
                batch_rows=batch_rows,
                target_codes=targets.target_code_set,
                min_date_exclusive=anchor,
            )

        bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
            effective_plan,
            on_rows_batch=_consume_margin_bulk_rows,
            accumulate_rows=False,
        )
        result.api_calls += bulk_result.api_calls
        result.bulk_result = bulk_result
        sync_fetch_planner._log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/markets/margin-interest",
            decision=decision,
            executed="bulk",
            actual_api_calls=bulk_result.api_calls,
            fallback=False,
            bulk_result=bulk_result,
        )
    except Exception as e:
        sync_fetch_planner._raise_if_bulk_rate_limited(e, stage_name=stage_name)
        result.used_rest_fallback = True
        result.fallback_reason = sync_fetch_planner._summarize_exception(e)
        logger.warning("{} bulk fetch failed, falling back to REST: {}", stage_name, result.fallback_reason)
    return result


async def _sync_margin_rest_codes(
    ctx: Any,
    *,
    codes: list[str],
    decision: Any,
    target_label: str,
    progress_current: int,
    progress_total: int,
    stage_name: str,
    anchor: str | None,
    used_rest_fallback: bool,
    fallback_reason: str | None,
    bulk_result: BulkFetchResult | None,
) -> MarginStageResult:
    result = MarginStageResult(errors=[], empty_fetch_codes=set())
    if not codes:
        return result
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="margin",
        current=progress_current,
        total=progress_total,
        endpoint="/markets/margin-interest",
        method="rest",
        target_label=target_label,
        fallback=used_rest_fallback,
        fallback_reason=fallback_reason,
    )
    for idx, code in enumerate(codes, start=1):
        if ctx.cancelled.is_set():
            result.cancelled = True
            return result
        if idx > 1 and idx % 100 == 0:
            ctx.on_progress(
                "margin",
                progress_current,
                progress_total,
                f"Fetching /markets/margin-interest via REST: {idx}/{len(codes)} codes...",
            )
        try:
            data, page_calls = await _fetch_margin_by_code(ctx.client, code, date_from=anchor)
            result.api_calls += page_calls
            rows = validate_rows_required_fields(
                _convert_margin_rows(data, default_code=code, min_date_exclusive=anchor),
                required_fields=("code", "date"),
                dedupe_keys=("code", "date"),
                stage="margin_data",
            )
            if rows:
                result.updated += await sync_publish_helpers._publish_margin_rows(ctx, rows)
        except Exception as e:
            result.error_list().append(f"Margin code {code}: {e}")

    sync_fetch_planner._log_sync_fetch_execution(
        stage=stage_name,
        endpoint="/markets/margin-interest",
        decision=decision,
        executed="rest",
        actual_api_calls=result.api_calls,
        fallback=used_rest_fallback,
        bulk_result=bulk_result,
    )
    return result


async def _sync_margin_backfill_codes(
    ctx: Any,
    *,
    codes: list[str],
    skipped_empty_codes: list[str],
    decision: Any,
    progress_current: int,
    progress_total: int,
    stage_name: str,
    used_rest_fallback: bool,
) -> MarginStageResult:
    result = MarginStageResult(errors=[], empty_fetch_codes=set())
    if not codes:
        return result
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="margin",
        current=progress_current,
        total=progress_total,
        endpoint="/markets/margin-interest",
        method="rest",
        target_label=_format_target_label(len(codes), "backfill codes", skipped_empty=len(skipped_empty_codes)),
    )
    for idx, code in enumerate(codes, start=1):
        if ctx.cancelled.is_set():
            result.cancelled = True
            return result
        if idx > 1 and idx % 100 == 0:
            ctx.on_progress(
                "margin",
                progress_current,
                progress_total,
                f"Fetching /markets/margin-interest via REST: {idx}/{len(codes)} backfill codes...",
            )
        try:
            data, page_calls = await _fetch_margin_by_code(ctx.client, code)
            result.api_calls += page_calls
            if not data:
                result.empty_codes().add(code)
                continue
            rows = validate_rows_required_fields(
                _convert_margin_rows(data, default_code=code),
                required_fields=("code", "date"),
                dedupe_keys=("code", "date"),
                stage="margin_data",
            )
            if rows:
                result.updated += await sync_publish_helpers._publish_margin_rows(ctx, rows)
            else:
                result.empty_codes().add(code)
        except Exception as e:
            result.error_list().append(f"Margin backfill code {code}: {e}")

    sync_fetch_planner._log_sync_fetch_execution(
        stage=f"{stage_name}_backfill",
        endpoint="/markets/margin-interest",
        decision=decision,
        executed="rest",
        actual_api_calls=result.api_calls,
        fallback=used_rest_fallback,
        bulk_result=None,
    )
    return result


def _merge_margin_stage(
    stage: MarginStageResult,
    *,
    api_calls: int,
    updated: int,
    errors: list[str],
    empty_fetch_codes: set[str],
) -> tuple[int, int]:
    errors.extend(stage.error_list())
    empty_fetch_codes.update(stage.empty_codes())
    return api_calls + stage.api_calls, updated + stage.updated


def _narrow_margin_backfill_targets_after_bulk(
    ctx: Any,
    targets: MarginSyncTargets,
) -> MarginSyncTargets:
    if not targets.backfill_codes:
        return targets
    latest_margin_codes = set(sync_state_helpers._inspect_time_series(ctx).margin_codes)
    remaining_backfill_codes = [
        code for code in targets.backfill_codes if code not in latest_margin_codes
    ]
    if len(remaining_backfill_codes) == len(targets.backfill_codes):
        return targets
    return replace(
        targets,
        backfill_codes=remaining_backfill_codes,
        existing_margin_code_set=latest_margin_codes,
        target_code_set=set(targets.rest_codes) | set(remaining_backfill_codes),
    )


async def _finish_margin_sync(
    ctx: Any,
    *,
    targets: MarginSyncTargets,
    api_calls: int,
    updated: int,
    errors: list[str],
    empty_fetch_codes: set[str],
    progress_current: int,
    progress_total: int,
) -> dict[str, Any]:
    if updated == 0:
        if empty_fetch_codes:
            next_empty_codes = set(targets.current_empty_codes)
            next_empty_codes.update(empty_fetch_codes)
            if targets.has_existing_margin_snapshot:
                next_empty_codes -= targets.existing_margin_code_set
            await _save_frontier_code_cache(
                ctx,
                METADATA_KEYS["MARGIN_EMPTY_CODES"],
                targets.current_frontier,
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

    await sync_publish_helpers._index_margin_rows(
        ctx,
        progress_current=progress_current,
        progress_total=progress_total,
    )
    next_empty_codes = set(targets.current_empty_codes)
    next_empty_codes.update(empty_fetch_codes)
    latest_margin_codes = set(sync_state_helpers._inspect_time_series(ctx).margin_codes)
    next_empty_codes = {
        code for code in next_empty_codes
        if code not in latest_margin_codes
    }
    await _save_frontier_code_cache(
        ctx,
        METADATA_KEYS["MARGIN_EMPTY_CODES"],
        targets.current_frontier,
        next_empty_codes,
    )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "cancelled": False,
    }


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
    empty_fetch_codes: set[str] = set()

    normalized_codes = sync_state_helpers._collect_unique_codes(target_codes)
    if not normalized_codes:
        return _margin_result(api_calls=api_calls, updated=updated, errors=errors)

    targets = _resolve_margin_targets(
        ctx,
        normalized_codes,
        anchor=anchor,
        existing_margin_codes=existing_margin_codes,
        trading_frontier=trading_frontier,
    )
    target_label = _format_target_label(
        len(targets.normalized_codes),
        "codes",
        skipped_empty=len(targets.skipped_empty_codes),
        skipped_market=skipped_market_count,
    )

    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage=stage_name,
        endpoint="/markets/margin-interest",
        estimated_rest_calls=max(len(targets.rest_codes) + len(targets.backfill_codes), 1),
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
    fallback_reason: str | None = None
    bulk_result: BulkFetchResult | None = None

    if decision.method == "bulk":
        bulk_stage = await _sync_margin_bulk_stage(
            ctx,
            decision=decision,
            targets=targets,
            target_label=target_label,
            progress_current=progress_current,
            progress_total=progress_total,
            stage_name=stage_name,
            anchor=anchor,
        )
        api_calls, updated = _merge_margin_stage(
            bulk_stage,
            api_calls=api_calls,
            updated=updated,
            errors=errors,
            empty_fetch_codes=empty_fetch_codes,
        )
        used_rest_fallback = bulk_stage.used_rest_fallback
        fallback_reason = bulk_stage.fallback_reason
        bulk_result = bulk_stage.bulk_result
        if bulk_stage.updated > 0:
            targets = _narrow_margin_backfill_targets_after_bulk(ctx, targets)

    if decision.method == "rest" or used_rest_fallback:
        rest_estimated_calls = max(len(targets.rest_codes) + len(targets.backfill_codes), 1)
        if rest_estimated_calls > _MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS:
            reason = fallback_reason or decision.reason
            message = (
                "Refusing margin_data REST fallback for "
                f"{len(targets.rest_codes)} refresh codes and {len(targets.backfill_codes)} backfill codes "
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

    if decision.method == "rest" or used_rest_fallback:
        rest_stage = await _sync_margin_rest_codes(
            ctx,
            codes=targets.rest_codes,
            decision=decision,
            target_label=target_label,
            progress_current=progress_current,
            progress_total=progress_total,
            stage_name=stage_name,
            anchor=anchor,
            used_rest_fallback=used_rest_fallback,
            fallback_reason=fallback_reason,
            bulk_result=bulk_result,
        )
        if rest_stage.cancelled:
            return {
                "api_calls": api_calls + rest_stage.api_calls,
                "updated": updated + rest_stage.updated,
                "errors": [*errors, *rest_stage.error_list()],
                "cancelled": True,
            }
        api_calls, updated = _merge_margin_stage(
            rest_stage,
            api_calls=api_calls,
            updated=updated,
            errors=errors,
            empty_fetch_codes=empty_fetch_codes,
        )

    if (
        targets.backfill_codes
        and len(targets.backfill_codes) > _MARGIN_REST_FALLBACK_MAX_ESTIMATED_CALLS
    ):
        message = (
            "Skipping margin_data REST backfill for "
            f"{len(targets.backfill_codes)} missing codes after bulk phase "
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
    else:
        backfill_stage = await _sync_margin_backfill_codes(
            ctx,
            codes=targets.backfill_codes,
            skipped_empty_codes=targets.skipped_empty_codes,
            decision=decision,
            progress_current=progress_current,
            progress_total=progress_total,
            stage_name=stage_name,
            used_rest_fallback=used_rest_fallback,
        )
        if backfill_stage.cancelled:
            return {
                "api_calls": api_calls + backfill_stage.api_calls,
                "updated": updated + backfill_stage.updated,
                "errors": [*errors, *backfill_stage.error_list()],
                "cancelled": True,
            }
        api_calls, updated = _merge_margin_stage(
            backfill_stage,
            api_calls=api_calls,
            updated=updated,
            errors=errors,
            empty_fetch_codes=empty_fetch_codes,
        )

    return await _finish_margin_sync(
        ctx,
        targets=targets,
        api_calls=api_calls,
        updated=updated,
        errors=errors,
        empty_fetch_codes=empty_fetch_codes,
        progress_current=progress_current,
        progress_total=progress_total,
    )
