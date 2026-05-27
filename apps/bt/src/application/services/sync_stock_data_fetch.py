"""Stock daily bulk fetch helper for market sync strategies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.application.services import sync_bulk_ingest_helpers, sync_fetch_planner
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.sync_row_converters import build_target_date_set


@dataclass(frozen=True)
class StockDataBulkFetchOutcome:
    api_calls: int = 0
    stocks_updated: int = 0
    bulk_result: BulkFetchResult | None = None
    used_rest_fallback: bool = False
    fallback_reason: str | None = None


async def execute_stock_data_bulk_fetch(
    ctx: Any,
    *,
    decision: Any,
    target_dates: list[str],
    stage_name: str,
    progress_stage: str,
    current: int,
    total: int,
    fallback_log_message: str,
) -> StockDataBulkFetchOutcome:
    if decision.method != "bulk" or decision.plan is None:
        return StockDataBulkFetchOutcome()

    stocks_updated = 0
    try:
        sync_fetch_planner._emit_fetch_execution_progress(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint="/equities/bars/daily",
            method="bulk",
            target_label=f"{len(target_dates)} dates",
        )
        target_date_set = build_target_date_set(target_dates)

        async def _consume_stock_bulk_rows(
            batch_rows: list[dict[str, Any]],
            _file_info: BulkFileInfo,
        ) -> None:
            nonlocal stocks_updated
            stocks_updated += await sync_bulk_ingest_helpers._ingest_stock_bulk_batch(
                ctx,
                batch_rows=batch_rows,
                target_dates=target_date_set,
            )

        bulk_result = await sync_fetch_planner._get_bulk_service(ctx).fetch_with_plan(
            decision.plan,
            on_rows_batch=_consume_stock_bulk_rows,
            accumulate_rows=False,
        )
        sync_fetch_planner._log_sync_fetch_execution(
            stage=stage_name,
            endpoint="/equities/bars/daily",
            decision=decision,
            executed="bulk",
            actual_api_calls=bulk_result.api_calls,
            fallback=False,
            bulk_result=bulk_result,
        )
        return StockDataBulkFetchOutcome(
            api_calls=bulk_result.api_calls,
            stocks_updated=stocks_updated,
            bulk_result=bulk_result,
        )
    except Exception as e:
        if ctx.enforce_bulk_for_stock_data and len(target_dates) > 0:
            sync_fetch_planner._raise_stock_bulk_required_error(
                ctx,
                progress_stage=progress_stage,
                current=current,
                total=total,
                endpoint="/equities/bars/daily",
                reason="bulk_fetch_failed",
                reason_detail=sync_fetch_planner._summarize_exception(e),
            )
        fallback_reason = sync_fetch_planner._summarize_exception(e)
        logger.exception(fallback_log_message, fallback_reason)
        return StockDataBulkFetchOutcome(
            used_rest_fallback=True,
            fallback_reason=fallback_reason,
        )
