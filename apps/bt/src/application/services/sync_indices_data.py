"""Index data sync stage helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, cast

from loguru import logger

from src.application.services import sync_fetch_planner, sync_publish_helpers
from src.application.services.ingestion_pipeline import validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.options_225 import OPTIONS_225_SYNTHETIC_INDEX_CODE
from src.application.services.sync_index_master_backfill import (
    upsert_indices_rows_with_master_backfill,
)
from src.application.services.sync_row_converters import (
    convert_indices_data_rows,
    extract_dates_after,
    latest_date,
    normalize_bulk_indices_rows,
    to_jquants_date_param,
    _date_sort_key,
    _is_date_after,
    _normalize_index_code,
    _to_iso_date_text,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


_LOCAL_SYNTHETIC_INDEX_CODES = {
    OPTIONS_225_SYNTHETIC_INDEX_CODE,
}


@dataclass(frozen=True)
class IndicesSyncStageOutcome:
    api_calls: int
    errors: list[str]
    cancelled: bool = False


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


def jquants_index_fetch_codes(codes: Iterable[str]) -> list[str]:
    return sorted(
        {
            normalized
            for normalized in (_normalize_index_code(code) for code in codes)
            if normalized and normalized not in _LOCAL_SYNTHETIC_INDEX_CODES
        }
    )


async def ingest_incremental_indices_bulk_batch(
    ctx: Any,
    *,
    batch_rows: list[dict[str, Any]],
    target_code_set: set[str],
    known_master_codes: set[str],
    latest_index_dates: dict[str, str],
    fallback_date_set: set[str],
) -> None:
    rows = validate_rows_required_fields(
        convert_indices_data_rows(normalize_bulk_indices_rows(batch_rows), None),
        required_fields=("code", "date"),
        dedupe_keys=("code", "date"),
        stage="indices_data",
    )
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
        await upsert_indices_rows_with_master_backfill(
            ctx,
            filtered_rows,
            known_master_codes,
            discovery_log="Inserted {} discovered index master rows while syncing by bulk.",
        )


async def sync_incremental_indices_stage(
    ctx: Any,
    *,
    inspection: TimeSeriesInspection,
    topix_rows: list[dict[str, Any]],
    last_date: str | None,
    known_master_codes: set[str],
    catalog_codes: set[str],
    progress_current: int = 3,
    progress_total: int = 7,
) -> IndicesSyncStageOutcome:
    ctx.on_progress(
        "indices",
        progress_current,
        progress_total,
        "Fetching incremental index data...",
    )
    if ctx.cancelled.is_set():
        return IndicesSyncStageOutcome(api_calls=0, errors=[], cancelled=True)

    api_calls = 0
    errors: list[str] = []
    raw_latest_index_dates = dict(inspection.latest_indices_dates)
    latest_index_dates = {
        _normalize_index_code(code): value
        for code, value in raw_latest_index_dates.items()
        if _normalize_index_code(code)
    }
    target_codes = sorted(
        jquants_index_fetch_codes(
            catalog_codes
            | set(latest_index_dates.keys())
            | known_master_codes
        )
    )
    target_code_set = set(target_codes)

    latest_index_date = latest_date(list(latest_index_dates.values()))
    fallback_dates = extract_dates_after(
        topix_rows,
        latest_index_date,
        include_anchor=True,
    )
    if latest_index_date and last_date and _is_date_after(last_date, latest_index_date):
        topix_for_indices, topix_for_indices_calls = await _get_paginated_rows_with_call_count(
            ctx.client,
            "/indices/bars/daily/topix",
            params={"from": to_jquants_date_param(latest_index_date)},
        )
        api_calls += topix_for_indices_calls
        topix_dates = [
            {"date": d.get("Date", "")}
            for d in topix_for_indices
            if d.get("Date")
        ]
        fallback_dates = sorted(
            set(fallback_dates)
            | set(extract_dates_after(topix_dates, latest_index_date, include_anchor=True)),
            key=_date_sort_key,
        )

    all_code_has_anchor = all(
        latest_index_dates.get(_normalize_index_code(code))
        for code in target_codes
    )
    decision_indices = await sync_fetch_planner._plan_fetch_method(
        ctx,
        stage="indices_incremental",
        endpoint="/indices/bars/daily",
        estimated_rest_calls=max(len(target_codes) + len(fallback_dates), 1),
        date_from=latest_index_date if all_code_has_anchor else None,
    )
    api_calls += decision_indices.planner_api_calls
    target_label = f"{len(target_codes)} codes + {len(fallback_dates)} dates"
    sync_fetch_planner._emit_fetch_strategy_progress(
        ctx,
        progress_stage="indices",
        current=progress_current,
        total=progress_total,
        endpoint="/indices/bars/daily",
        decision=decision_indices,
        target_label=target_label,
    )

    used_indices_rest_fallback = False
    indices_bulk_fallback_reason: str | None = None
    indices_stage_api_calls = 0
    indices_bulk_result: BulkFetchResult | None = None
    if decision_indices.method == "bulk":
        fallback_date_set = {
            normalized
            for normalized in (_to_iso_date_text(value) for value in fallback_dates)
            if normalized is not None
        }

        async def _consume_incremental_indices_bulk_rows(
            batch_rows: list[dict[str, Any]],
            _file_info: BulkFileInfo,
        ) -> None:
            await ingest_incremental_indices_bulk_batch(
                ctx,
                batch_rows=batch_rows,
                target_code_set=target_code_set,
                known_master_codes=known_master_codes,
                latest_index_dates=latest_index_dates,
                fallback_date_set=fallback_date_set,
            )

        indices_bulk_outcome = await sync_fetch_planner._execute_bulk_fetch_stage(
            ctx,
            decision=decision_indices,
            stage_name="indices_incremental",
            progress_stage="indices",
            current=progress_current,
            total=progress_total,
            endpoint="/indices/bars/daily",
            target_label=target_label,
            on_rows_batch=_consume_incremental_indices_bulk_rows,
            fallback_log_message="Incremental indices bulk fetch unavailable, falling back to REST: {}",
        )
        api_calls += indices_bulk_outcome.api_calls
        indices_stage_api_calls += indices_bulk_outcome.api_calls
        indices_bulk_result = indices_bulk_outcome.bulk_result
        used_indices_rest_fallback = indices_bulk_outcome.used_rest_fallback
        indices_bulk_fallback_reason = indices_bulk_outcome.fallback_reason

    if decision_indices.method == "rest" or used_indices_rest_fallback:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage="indices",
            current=progress_current,
            total=progress_total,
            endpoint="/indices/bars/daily",
            method="rest",
            target_label=target_label,
            fallback=used_indices_rest_fallback,
            fallback_reason=indices_bulk_fallback_reason,
        )
        rest_outcome = await sync_incremental_indices_rest(
            ctx,
            target_codes=target_codes,
            fallback_dates=fallback_dates,
            latest_index_dates=latest_index_dates,
            known_master_codes=known_master_codes,
            progress_current=progress_current,
            progress_total=progress_total,
        )
        api_calls += rest_outcome.api_calls
        indices_stage_api_calls += rest_outcome.api_calls
        errors.extend(rest_outcome.errors)
        if rest_outcome.cancelled:
            return IndicesSyncStageOutcome(api_calls=api_calls, errors=errors, cancelled=True)
        sync_fetch_planner._log_sync_fetch_execution(
            stage="indices_incremental",
            endpoint="/indices/bars/daily",
            decision=decision_indices,
            executed="rest",
            actual_api_calls=indices_stage_api_calls,
            fallback=used_indices_rest_fallback,
            bulk_result=indices_bulk_result,
        )

    await sync_publish_helpers._index_indices_rows(ctx)
    return IndicesSyncStageOutcome(api_calls=api_calls, errors=errors)


async def sync_incremental_indices_rest(
    ctx: Any,
    *,
    target_codes: list[str],
    fallback_dates: list[str],
    latest_index_dates: dict[str, str],
    known_master_codes: set[str],
    progress_current: int,
    progress_total: int,
) -> IndicesSyncStageOutcome:
    api_calls = 0
    errors: list[str] = []
    for code_idx, code in enumerate(target_codes, start=1):
        if ctx.cancelled.is_set():
            return IndicesSyncStageOutcome(api_calls=api_calls, errors=errors, cancelled=True)
        if code_idx > 1 and code_idx % 50 == 0:
            ctx.on_progress(
                "indices",
                progress_current,
                progress_total,
                f"Fetching /indices/bars/daily via REST: {code_idx}/{len(target_codes)} codes...",
            )

        params: dict[str, Any] = {"code": code}
        normalized_code = _normalize_index_code(code)
        last_index_date = latest_index_dates.get(normalized_code)
        if last_index_date:
            params["from"] = to_jquants_date_param(last_index_date)

        try:
            data, page_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily",
                params=params,
            )
            api_calls += page_calls
            rows = validate_rows_required_fields(
                convert_indices_data_rows(data, code),
                required_fields=("code", "date"),
                dedupe_keys=("code", "date"),
                stage="indices_data",
            )
            if last_index_date:
                rows = [r for r in rows if _is_date_after(r["date"], last_index_date)]
            if rows:
                await upsert_indices_rows_with_master_backfill(
                    ctx,
                    rows,
                    known_master_codes,
                    discovery_log="Inserted {} discovered index master rows while syncing by code.",
                )
        except Exception as e:
            errors.append(f"Index {code}: {e}")
            logger.warning(f"Index {code} incremental sync error: {e}")

    for date_idx, index_date in enumerate(fallback_dates, start=1):
        if ctx.cancelled.is_set():
            return IndicesSyncStageOutcome(api_calls=api_calls, errors=errors, cancelled=True)
        if date_idx > 1 and date_idx % 50 == 0:
            ctx.on_progress(
                "indices",
                progress_current,
                progress_total,
                f"Fetching /indices/bars/daily via REST: {date_idx}/{len(fallback_dates)} dates...",
            )

        try:
            data, page_calls = await _get_paginated_rows_with_call_count(
                ctx.client,
                "/indices/bars/daily",
                params={"date": to_jquants_date_param(index_date)},
            )
            api_calls += page_calls
            rows = validate_rows_required_fields(
                convert_indices_data_rows(data, None),
                required_fields=("code", "date"),
                dedupe_keys=("code", "date"),
                stage="indices_data",
            )
            if rows:
                await upsert_indices_rows_with_master_backfill(
                    ctx,
                    rows,
                    known_master_codes,
                    discovery_log="Inserted {} discovered index master rows while syncing by date.",
                )
        except Exception as e:
            errors.append(f"Index date {index_date}: {e}")
            logger.warning("Index date {} incremental sync error: {}", index_date, e)
    return IndicesSyncStageOutcome(api_calls=api_calls, errors=errors)
