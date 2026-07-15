"""Fundamentals sync stages for market DB sync."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.application.services import sync_bulk_ingest_helpers, sync_fetch_planner, sync_publish_helpers, sync_state_helpers
from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.ingestion_pipeline import validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.listed_market_targets import (
    build_fundamentals_fetch_codes,
    build_fundamentals_target_map,
    group_target_codes_by_canonical,
    normalize_frontier_date,
    resolve_frontier_cache_codes,
    serialize_frontier_code_cache,
)
from src.application.services.sync_fins_fetch import (
    _fetch_fins_summary_by_code,
    _fetch_fins_summary_paginated,
)
from src.application.services.sync_row_converters import (
    build_target_date_set as _build_target_date_set,
    latest_date as _latest_date,
    to_jquants_date_param as _to_jquants_date_param,
    _normalize_iso_date_text,
)
from src.infrastructure.db.market.market_db import METADATA_KEYS


_FUNDAMENTALS_REST_BACKFILL_MAX_ESTIMATED_CALLS = 250


def _format_target_label(
    count: int,
    unit: str,
    *,
    skipped_empty: int = 0,
    issuer_alias: int = 0,
) -> str:
    parts = [f"{count} {unit}"]
    if skipped_empty > 0:
        parts.append(f"skipped_empty={skipped_empty}")
    if issuer_alias > 0:
        parts.append(f"issuer_alias={issuer_alias}")
    return ", ".join(parts)


def _load_frontier_code_cache(market_db: Any, key: str, frontier: str | None) -> set[str]:
    return resolve_frontier_cache_codes(market_db.get_sync_metadata(key), frontier)


async def _save_frontier_code_cache(
    ctx: Any,
    key: str,
    frontier: str | None,
    codes: set[str] | list[str],
) -> None:
    await asyncio.to_thread(ctx.market_db.set_sync_metadata, key, serialize_frontier_code_cache(frontier, codes))


def _get_latest_statement_disclosed_date(ctx: Any) -> str | None:
    inspection = sync_state_helpers._inspect_time_series(ctx)
    return inspection.latest_statement_disclosed_date


def _get_statement_codes(ctx: Any) -> set[str]:
    inspection = sync_state_helpers._inspect_time_series(ctx)
    return set(inspection.statement_codes)


def _fundamentals_result(
    *,
    api_calls: int,
    updated: int,
    dates_processed: int,
    errors: list[str],
    cancelled: bool = False,
) -> dict[str, Any]:
    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": dates_processed,
        "errors": errors,
        "cancelled": cancelled,
    }


async def _complete_initial_fundamentals_residuals(
    ctx: Any,
    *,
    target_map: dict[str, str],
    allowed_statement_codes: set[str],
    issuer_alias_count: int,
    progress_current: int,
    progress_total: int,
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_codes: list[str] = []
    final_empty_fetch_codes: set[str] = set()
    target_groups = group_target_codes_by_canonical(target_map)
    empty_codes_by_frontier: dict[str | None, set[str]] = {}
    seen_states: set[tuple[str | None, frozenset[str], frozenset[str]]] = set()
    target_budget_used = 0
    pass_number = 0

    while True:
        effective_frontier = normalize_frontier_date(_get_latest_statement_disclosed_date(ctx))
        statement_codes = _get_statement_codes(ctx)
        valid_empty_exact_codes = _load_frontier_code_cache(
            ctx.market_db,
            METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"],
            effective_frontier,
        )
        for fetch_code in empty_codes_by_frontier.get(effective_frontier, set()):
            valid_empty_exact_codes.update(target_groups.get(fetch_code, ()))

        residual_codes = build_fundamentals_fetch_codes(
            target_map,
            statement_codes,
            empty_skipped_codes=valid_empty_exact_codes,
        )
        if not residual_codes:
            final_empty_fetch_codes.update(empty_codes_by_frontier.get(effective_frontier, set()))
            break

        state = (
            effective_frontier,
            frozenset(statement_codes),
            frozenset(valid_empty_exact_codes),
        )
        if state in seen_states:
            message = (
                "Initial fundamentals residual completion did not converge; "
                f"frontier={effective_frontier or 'none'}, remaining={len(residual_codes)} codes."
            )
            ctx.on_progress("fundamentals", progress_current, progress_total, message)
            errors.append(message)
            break
        seen_states.add(state)

        next_budget_used = target_budget_used + len(residual_codes)
        if next_budget_used > _FUNDAMENTALS_REST_BACKFILL_MAX_ESTIMATED_CALLS:
            message = (
                "Refusing fundamentals REST residual completion for "
                f"{len(residual_codes)} codes: total residual REST budget would be "
                f"{next_budget_used} "
                f"(limit={_FUNDAMENTALS_REST_BACKFILL_MAX_ESTIMATED_CALLS})."
            )
            ctx.on_progress("fundamentals", progress_current, progress_total, message)
            errors.append(message)
            break
        target_budget_used = next_budget_used
        pass_number += 1

        pass_result = await _sync_fundamentals_backfill_codes(
            ctx,
            code_targets=residual_codes,
            allowed_statement_codes=allowed_statement_codes,
            skipped_empty_exact_codes=valid_empty_exact_codes,
            issuer_alias_count=issuer_alias_count,
            progress_current=progress_current,
            progress_total=progress_total,
            stage_name="fundamentals_initial_residual_completion",
            target_unit=f"residual completion codes (pass={pass_number})",
            reason="bulk_coverage_gap",
        )
        api_calls += pass_result["api_calls"]
        updated += pass_result["updated"]
        errors.extend(pass_result["errors"])
        failed_codes.extend(pass_result["failed_codes"])
        if pass_result["cancelled"]:
            return {
                "api_calls": api_calls,
                "updated": updated,
                "errors": errors,
                "failed_codes": failed_codes,
                "empty_fetch_codes": final_empty_fetch_codes,
                "cancelled": True,
            }
        if pass_result["errors"]:
            break

        post_pass_frontier = normalize_frontier_date(_get_latest_statement_disclosed_date(ctx))
        empty_codes_by_frontier.setdefault(post_pass_frontier, set()).update(
            pass_result["empty_fetch_codes"]
        )

    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "failed_codes": failed_codes,
        "empty_fetch_codes": final_empty_fetch_codes,
        "cancelled": False,
    }


async def sync_fundamentals_initial(
    ctx: Any,
    target_rows: list[dict[str, Any]],
    *,
    progress_current: int = 2,
    progress_total: int = 6,
) -> dict[str, Any]:
    """fundamentals 対象市場を code 指定でフル同期"""
    api_calls = 0
    updated = 0
    failed_codes: list[str] = []
    errors: list[str] = []
    target_map = build_fundamentals_target_map(target_rows)
    if not target_map:
        return _fundamentals_result(api_calls=api_calls, updated=updated, dates_processed=0, errors=errors)
    allowed_statement_codes = set(target_map) | set(target_map.values())
    target_groups = group_target_codes_by_canonical(target_map)
    statement_codes = _get_statement_codes(ctx)
    current_frontier = normalize_frontier_date(
        ctx.market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or _get_latest_statement_disclosed_date(ctx)
    )
    current_empty_codes = _load_frontier_code_cache(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"], current_frontier)
    target_codes = build_fundamentals_fetch_codes(
        target_map,
        statement_codes,
        empty_skipped_codes=current_empty_codes,
    )
    skipped_empty_exact_codes = {
        code
        for code, canonical in target_map.items()
        if canonical not in statement_codes and code in current_empty_codes
    }
    issuer_alias_count = sum(1 for code, canonical in target_map.items() if canonical != code)
    bulk_succeeded = False
    stage_api_calls = 0
    bulk_result: BulkFetchResult | None = None
    empty_fetch_codes: set[str] = set()

    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="fundamentals_initial",
        endpoint="/fins/summary",
        estimated_rest_calls=max(len(target_codes), 1),
    )
    api_calls += decision.planner_api_calls
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="fundamentals",
        current=progress_current,
        total=progress_total,
        endpoint="/fins/summary",
        decision=decision,
        target_label=_format_target_label(
            len(target_codes),
            "listed-market fetch codes",
            skipped_empty=len(skipped_empty_exact_codes),
            issuer_alias=issuer_alias_count,
        ),
    )

    if decision.method == "bulk" and decision.plan is not None:
        try:
            sync_fetch_planner._emit_fetch_execution_progress(
                ctx,
                progress_stage="fundamentals",
                current=progress_current,
                total=progress_total,
                endpoint="/fins/summary",
                method="bulk",
                target_label=_format_target_label(
                    len(target_codes),
                    "listed-market fetch codes",
                    skipped_empty=len(skipped_empty_exact_codes),
                    issuer_alias=issuer_alias_count,
                ),
            )

            async def _consume_initial_fundamentals_bulk_rows(
                batch_rows: list[dict[str, Any]],
                _file_info: BulkFileInfo,
            ) -> None:
                nonlocal updated
                updated += await sync_bulk_ingest_helpers._ingest_fins_bulk_batch(
                    ctx,
                    batch_rows=batch_rows,
                    allowed_codes=allowed_statement_codes,
                )

            bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
                decision.plan,
                on_rows_batch=_consume_initial_fundamentals_bulk_rows,
                accumulate_rows=False,
            )
            api_calls += bulk_result.api_calls
            stage_api_calls += bulk_result.api_calls
            bulk_succeeded = True
            sync_fetch_planner._log_sync_fetch_execution(
                stage="fundamentals_initial",
                endpoint="/fins/summary",
                decision=decision,
                executed="bulk",
                actual_api_calls=stage_api_calls,
                fallback=False,
                bulk_result=bulk_result,
            )
        except Exception as e:
            sync_fetch_planner._raise_if_bulk_rate_limited(e, stage_name="fundamentals_initial")
            logger.warning("Initial fundamentals bulk fetch failed, falling back to REST: {}", e)

    if bulk_succeeded:
        residual_result = await _complete_initial_fundamentals_residuals(
            ctx,
            target_map=target_map,
            allowed_statement_codes=allowed_statement_codes,
            issuer_alias_count=issuer_alias_count,
            progress_current=progress_current,
            progress_total=progress_total,
        )
        api_calls += residual_result["api_calls"]
        updated += residual_result["updated"]
        errors.extend(residual_result["errors"])
        failed_codes.extend(residual_result["failed_codes"])
        empty_fetch_codes.update(residual_result["empty_fetch_codes"])
        if residual_result["cancelled"]:
            return _fundamentals_result(
                api_calls=api_calls,
                updated=updated,
                dates_processed=0,
                errors=errors,
                cancelled=True,
            )

    if not bulk_succeeded:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="fundamentals",
            current=progress_current,
            total=progress_total,
            endpoint="/fins/summary",
            method="rest",
            target_label=_format_target_label(
                len(target_codes),
                "listed-market fetch codes",
                skipped_empty=len(skipped_empty_exact_codes),
                issuer_alias=issuer_alias_count,
            ),
            fallback=decision.method == "bulk",
        )
        for idx, code in enumerate(target_codes):
            if ctx.cancelled.is_set():
                return _fundamentals_result(
                    api_calls=api_calls,
                    updated=updated,
                    dates_processed=0,
                    errors=errors,
                    cancelled=True,
                )
            if idx > 0 and idx % 100 == 0:
                ctx.on_progress(
                    "fundamentals",
                    progress_current,
                    progress_total,
                    f"Fetching /fins/summary via REST: {idx}/{len(target_codes)} codes...",
                )
            try:
                data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
                api_calls += page_calls
                stage_api_calls += page_calls
                if not data:
                    empty_fetch_codes.add(code)
                    continue
                rows = validate_rows_required_fields(
                    convert_fins_summary_rows(data, default_code=code),
                    required_fields=("code", "disclosed_date"),
                    dedupe_keys=("code", "disclosed_date"),
                    stage="fundamentals",
                )
                rows = [row for row in rows if row.get("code") in allowed_statement_codes]
                if rows:
                    updated += await sync_publish_helpers._publish_statement_rows(ctx, rows)
                else:
                    empty_fetch_codes.add(code)
            except Exception as e:
                failed_codes.append(code)
                errors.append(f"Fundamentals code {code}: {e}")
        sync_fetch_planner._log_sync_fetch_execution(
            stage="fundamentals_initial",
            endpoint="/fins/summary",
            decision=decision,
            executed="rest",
            actual_api_calls=stage_api_calls,
            fallback=decision.method == "bulk",
            bulk_result=bulk_result,
        )

    await sync_publish_helpers._index_statement_rows(
        ctx,
        progress_current=progress_current,
        progress_total=progress_total,
    )
    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
    normalized_latest_disclosed = normalize_frontier_date(latest_disclosed)
    now_iso = datetime.now(UTC).isoformat()

    await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"], now_iso)
    if latest_disclosed:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            latest_disclosed,
        )
    empty_cache_frontier = normalized_latest_disclosed or current_frontier
    next_empty_codes = set(current_empty_codes) if empty_cache_frontier == current_frontier else set()
    for fetch_code in empty_fetch_codes:
        next_empty_codes.update(target_groups.get(fetch_code, ()))
    latest_statement_codes = _get_statement_codes(ctx)
    next_empty_codes = {
        code for code in next_empty_codes
        if code in target_map and target_map[code] not in latest_statement_codes
    }
    await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"], "[]")
    await _save_frontier_code_cache(ctx, METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"], empty_cache_frontier, next_empty_codes)
    await sync_state_helpers._save_metadata_json_list(ctx, METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"], failed_codes)

    return _fundamentals_result(api_calls=api_calls, updated=updated, dates_processed=0, errors=errors)


async def sync_fundamentals_incremental(
    ctx: Any,
    target_rows: list[dict[str, Any]],
    *,
    progress_current: int = 4,
    progress_total: int = 5,
) -> dict[str, Any]:
    """date 指定増分 + 欠損 listed-market 補完"""
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_dates: list[str] = []
    failed_codes: list[str] = []
    target_map = build_fundamentals_target_map(target_rows)
    if not target_map:
        return _fundamentals_result(api_calls=api_calls, updated=updated, dates_processed=0, errors=errors)
    allowed_statement_codes = set(target_map) | set(target_map.values())
    target_groups = group_target_codes_by_canonical(target_map)
    issuer_alias_count = sum(1 for code, canonical in target_map.items() if canonical != code)

    previous_failed_dates = sync_state_helpers._normalize_date_list(
        sync_state_helpers._load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"])
    )
    previous_failed_codes = sync_state_helpers._collect_unique_codes(
        sync_state_helpers._load_metadata_json_list(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"])
    )

    anchor = (
        ctx.market_db.get_sync_metadata(METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"])
        or _get_latest_statement_disclosed_date(ctx)
    )
    date_targets = sync_state_helpers._build_incremental_date_targets(anchor, previous_failed_dates)
    dates_phase_completed = 0
    date_phase_disclosed_dates: set[str] = set()
    if date_targets:
        date_result = await _sync_fundamentals_incremental_dates(
            ctx,
            date_targets=date_targets,
            allowed_statement_codes=allowed_statement_codes,
            progress_current=progress_current,
            progress_total=progress_total,
        )
        api_calls += date_result["api_calls"]
        updated += date_result["updated"]
        errors.extend(date_result["errors"])
        failed_dates.extend(date_result["failed_dates"])
        dates_phase_completed = date_result["dates_processed"]
        date_phase_disclosed_dates.update(date_result["disclosed_dates"])
        if date_result["cancelled"]:
            return _fundamentals_result(
                api_calls=api_calls,
                updated=updated,
                dates_processed=dates_phase_completed,
                errors=errors,
                cancelled=True,
            )

    current_frontier = normalize_frontier_date(_get_latest_statement_disclosed_date(ctx) or anchor)
    current_empty_codes = _load_frontier_code_cache(ctx.market_db, METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"], current_frontier)
    statement_codes = _get_statement_codes(ctx)
    skipped_empty_exact_codes = {
        code for code, canonical in target_map.items()
        if canonical not in statement_codes and code in current_empty_codes
    }
    code_targets = build_fundamentals_fetch_codes(
        target_map,
        statement_codes,
        previous_failed_codes=previous_failed_codes,
        empty_skipped_codes=current_empty_codes,
    )
    code_result = await _sync_fundamentals_backfill_codes(
        ctx,
        code_targets=code_targets,
        allowed_statement_codes=allowed_statement_codes,
        skipped_empty_exact_codes=skipped_empty_exact_codes,
        issuer_alias_count=issuer_alias_count,
        progress_current=progress_current,
        progress_total=progress_total,
    )
    api_calls += code_result["api_calls"]
    updated += code_result["updated"]
    errors.extend(code_result["errors"])
    failed_codes.extend(code_result["failed_codes"])
    empty_fetch_codes = set(code_result["empty_fetch_codes"])
    if code_result["cancelled"]:
        return _fundamentals_result(
            api_calls=api_calls,
            updated=updated,
            dates_processed=dates_phase_completed,
            errors=errors,
            cancelled=True,
        )

    await sync_publish_helpers._index_statement_rows(
        ctx,
        progress_current=progress_current,
        progress_total=progress_total,
    )
    latest_disclosed = _get_latest_statement_disclosed_date(ctx)
    normalized_latest_disclosed = normalize_frontier_date(latest_disclosed)
    date_phase_frontier = _latest_date(list(date_phase_disclosed_dates))
    now_iso = datetime.now(UTC).isoformat()
    await asyncio.to_thread(ctx.market_db.set_sync_metadata, METADATA_KEYS["FUNDAMENTALS_LAST_SYNC_DATE"], now_iso)
    metadata_frontier = date_phase_frontier
    if metadata_frontier is None and anchor is None:
        metadata_frontier = normalized_latest_disclosed
    if metadata_frontier:
        await asyncio.to_thread(
            ctx.market_db.set_sync_metadata,
            METADATA_KEYS["FUNDAMENTALS_LAST_DISCLOSED_DATE"],
            metadata_frontier,
        )
    empty_cache_frontier = metadata_frontier or normalized_latest_disclosed or current_frontier
    next_empty_codes = set(current_empty_codes) if empty_cache_frontier == current_frontier else set()
    for fetch_code in empty_fetch_codes:
        next_empty_codes.update(target_groups.get(fetch_code, ()))
    latest_statement_codes = _get_statement_codes(ctx)
    next_empty_codes = {
        code for code in next_empty_codes
        if code in target_map and target_map[code] not in latest_statement_codes
    }

    await sync_state_helpers._save_metadata_json_list(ctx, METADATA_KEYS["FUNDAMENTALS_FAILED_DATES"], failed_dates)
    await _save_frontier_code_cache(ctx, METADATA_KEYS["FUNDAMENTALS_EMPTY_CODES"], empty_cache_frontier, next_empty_codes)
    await sync_state_helpers._save_metadata_json_list(ctx, METADATA_KEYS["FUNDAMENTALS_FAILED_CODES"], failed_codes)
    return _fundamentals_result(
        api_calls=api_calls,
        updated=updated,
        dates_processed=dates_phase_completed,
        errors=errors,
    )


async def _sync_fundamentals_incremental_dates(
    ctx: Any,
    *,
    date_targets: list[str],
    allowed_statement_codes: set[str],
    progress_current: int,
    progress_total: int,
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_dates: list[str] = []
    disclosed_dates: set[str] = set()
    normalized_target_dates = _build_target_date_set(date_targets)
    decision = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="fundamentals_incremental_dates",
        endpoint="/fins/summary",
        estimated_rest_calls=max(len(date_targets), 1),
        exact_dates=date_targets,
    )
    api_calls += decision.planner_api_calls
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="fundamentals",
        current=progress_current,
        total=progress_total,
        endpoint="/fins/summary",
        decision=decision,
        target_label=f"{len(date_targets)} dates",
    )
    bulk_succeeded = False
    bulk_result: BulkFetchResult | None = None
    stage_api_calls = 0
    if decision.method == "bulk" and decision.plan is not None:
        try:
            sync_fetch_planner._emit_fetch_execution_progress(
                ctx,
                progress_stage="fundamentals",
                current=progress_current,
                total=progress_total,
                endpoint="/fins/summary",
                method="bulk",
                target_label=f"{len(date_targets)} dates",
            )

            async def _consume_incremental_fundamentals_bulk_rows(
                batch_rows: list[dict[str, Any]],
                _file_info: BulkFileInfo,
            ) -> None:
                nonlocal updated
                updated += await sync_bulk_ingest_helpers._ingest_fins_bulk_batch(
                    ctx,
                    batch_rows=batch_rows,
                    allowed_codes=allowed_statement_codes,
                    target_dates=normalized_target_dates,
                    published_dates=disclosed_dates,
                )

            bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
                decision.plan,
                on_rows_batch=_consume_incremental_fundamentals_bulk_rows,
                accumulate_rows=False,
            )
            api_calls += bulk_result.api_calls
            stage_api_calls += bulk_result.api_calls
            bulk_succeeded = True
            sync_fetch_planner._log_sync_fetch_execution(
                stage="fundamentals_incremental_dates",
                endpoint="/fins/summary",
                decision=decision,
                executed="bulk",
                actual_api_calls=stage_api_calls,
                fallback=False,
                bulk_result=bulk_result,
            )
        except Exception as e:
            sync_fetch_planner._raise_if_bulk_rate_limited(
                e,
                stage_name="fundamentals_incremental_dates",
            )
            logger.warning("Incremental fundamentals bulk date fetch failed, falling back to REST: {}", e)

    if bulk_succeeded:
        return {
            "api_calls": api_calls,
            "updated": updated,
            "dates_processed": len(date_targets),
            "errors": errors,
            "failed_dates": failed_dates,
            "disclosed_dates": disclosed_dates,
            "cancelled": False,
        }
    rest_result = await _sync_fundamentals_incremental_dates_rest(
        ctx,
        date_targets=date_targets,
        decision=decision,
        allowed_statement_codes=allowed_statement_codes,
        progress_current=progress_current,
        progress_total=progress_total,
        bulk_result=bulk_result,
    )
    rest_result["api_calls"] += api_calls
    rest_result["updated"] += updated
    return rest_result


async def _sync_fundamentals_incremental_dates_rest(
    ctx: Any,
    *,
    date_targets: list[str],
    decision: Any,
    allowed_statement_codes: set[str],
    progress_current: int,
    progress_total: int,
    bulk_result: BulkFetchResult | None,
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_dates: list[str] = []
    disclosed_dates: set[str] = set()
    sync_fetch_planner._emit_fetch_execution_progress(
        ctx,
        progress_stage="fundamentals",
        current=progress_current,
        total=progress_total,
        endpoint="/fins/summary",
        method="rest",
        target_label=f"{len(date_targets)} dates",
        fallback=decision.method == "bulk",
    )
    for idx, disclosed_date in enumerate(date_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "dates_processed": idx,
                "errors": errors,
                "failed_dates": failed_dates,
                "disclosed_dates": disclosed_dates,
                "cancelled": True,
            }
        if idx > 0 and idx % 30 == 0:
            ctx.on_progress(
                "fundamentals",
                progress_current,
                progress_total,
                f"Fetching /fins/summary via REST: {idx}/{len(date_targets)} dates...",
            )
        try:
            data, page_calls = await _fetch_fins_summary_paginated(
                ctx.client,
                {"date": _to_jquants_date_param(disclosed_date)},
            )
            api_calls += page_calls
            rows = convert_fins_summary_rows(data)
            rows = [row for row in rows if row.get("code") in allowed_statement_codes]
            rows = validate_rows_required_fields(
                rows,
                required_fields=("code", "disclosed_date"),
                dedupe_keys=("code", "disclosed_date"),
                stage="fundamentals",
            )
            if rows:
                disclosed_dates.update(
                    normalized
                    for normalized in (_normalize_iso_date_text(row.get("disclosed_date")) for row in rows)
                    if normalized is not None
                )
                updated += await sync_publish_helpers._publish_statement_rows(ctx, rows)
        except Exception as e:
            failed_dates.append(disclosed_date)
            errors.append(f"Fundamentals date {disclosed_date}: {e}")
    sync_fetch_planner._log_sync_fetch_execution(
        stage="fundamentals_incremental_dates",
        endpoint="/fins/summary",
        decision=decision,
        executed="rest",
        actual_api_calls=api_calls,
        fallback=decision.method == "bulk",
        bulk_result=bulk_result,
    )
    return {
        "api_calls": api_calls,
        "updated": updated,
        "dates_processed": len(date_targets),
        "errors": errors,
        "failed_dates": failed_dates,
        "disclosed_dates": disclosed_dates,
        "cancelled": False,
    }


async def _sync_fundamentals_backfill_codes(
    ctx: Any,
    *,
    code_targets: list[str],
    allowed_statement_codes: set[str],
    skipped_empty_exact_codes: set[str],
    issuer_alias_count: int,
    progress_current: int,
    progress_total: int,
    stage_name: str = "fundamentals_incremental_backfill",
    target_unit: str = "backfill codes",
    reason: str = "code_backfill",
) -> dict[str, Any]:
    api_calls = 0
    updated = 0
    errors: list[str] = []
    failed_codes: list[str] = []
    empty_fetch_codes: set[str] = set()
    if len(code_targets) > _FUNDAMENTALS_REST_BACKFILL_MAX_ESTIMATED_CALLS:
        message = (
            "Refusing fundamentals REST backfill for "
            f"{len(code_targets)} codes "
            f"(limit={_FUNDAMENTALS_REST_BACKFILL_MAX_ESTIMATED_CALLS}). "
            "Run with /fins/summary bulk available or narrow the fundamentals target universe."
        )
        ctx.on_progress("fundamentals", progress_current, progress_total, message)
        return {
            "api_calls": api_calls,
            "updated": updated,
            "errors": [message],
            "failed_codes": failed_codes,
            "empty_fetch_codes": empty_fetch_codes,
            "cancelled": False,
        }
    if code_targets:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="fundamentals",
            current=progress_current,
            total=progress_total,
            endpoint="/fins/summary",
            method="rest",
            target_label=_format_target_label(
                len(code_targets),
                target_unit,
                skipped_empty=len(skipped_empty_exact_codes),
                issuer_alias=issuer_alias_count,
            ),
            reason=reason,
            reason_detail=f"count={len(code_targets)}",
        )

    for idx, code in enumerate(code_targets):
        if ctx.cancelled.is_set():
            return {
                "api_calls": api_calls,
                "updated": updated,
                "errors": errors,
                "failed_codes": failed_codes,
                "empty_fetch_codes": empty_fetch_codes,
                "cancelled": True,
            }
        if idx > 0 and idx % 100 == 0:
            ctx.on_progress(
                "fundamentals",
                progress_current,
                progress_total,
                f"Fetching /fins/summary via REST: {idx}/{len(code_targets)} backfill codes...",
            )
        try:
            data, page_calls = await _fetch_fins_summary_by_code(ctx.client, code)
            api_calls += page_calls
            if not data:
                empty_fetch_codes.add(code)
                continue
            rows = convert_fins_summary_rows(data, default_code=code)
            rows = [row for row in rows if row.get("code") in allowed_statement_codes]
            rows = validate_rows_required_fields(
                rows,
                required_fields=("code", "disclosed_date"),
                dedupe_keys=("code", "disclosed_date"),
                stage="fundamentals",
            )
            if rows:
                updated += await sync_publish_helpers._publish_statement_rows(ctx, rows)
            else:
                empty_fetch_codes.add(code)
        except Exception as e:
            if sync_fetch_planner._is_bulk_rate_limited(e):
                raise RuntimeError(
                    f"fundamentals {target_unit} REST fetch was rate-limited after retries; "
                    "refusing remaining requests to avoid request amplification. "
                    "Retry after the shared J-Quants cooldown."
                ) from e
            failed_codes.append(code)
            errors.append(f"Fundamentals code {code}: {e}")

    if code_targets:
        sync_fetch_planner._log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/fins/summary",
            decision=sync_fetch_planner._StageFetchDecision(
                method="rest",
                planner_api_calls=0,
                estimated_rest_calls=len(code_targets),
                estimated_bulk_calls=None,
                reason=reason,
            ),
            executed="rest",
            actual_api_calls=api_calls,
            fallback=False,
            bulk_result=None,
        )
    return {
        "api_calls": api_calls,
        "updated": updated,
        "errors": errors,
        "failed_codes": failed_codes,
        "empty_fetch_codes": empty_fetch_codes,
        "cancelled": False,
    }
