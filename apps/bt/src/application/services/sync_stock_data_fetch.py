"""Stock daily fetch helpers for market sync strategies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger

from src.application.services import sync_bulk_ingest_helpers, sync_fetch_planner, sync_publish_helpers
from src.application.services.ingestion_pipeline import run_ingestion_batch, validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.sync_paginated_fetch import get_paginated_rows_with_call_count
from src.application.services.sync_row_converters import build_target_date_set
from src.application.services.sync_row_converters import convert_stock_data_rows as _convert_stock_data_rows


@dataclass(frozen=True)
class StockDataBulkFetchOutcome:
    api_calls: int = 0
    stocks_updated: int = 0
    bulk_result: BulkFetchResult | None = None
    used_rest_fallback: bool = False
    fallback_reason: str | None = None


@dataclass(frozen=True)
class StockDataRestDateOutcome:
    api_calls: int
    stocks_updated: int


class StockDataRestDateIngestionError(RuntimeError):
    def __init__(self, original: Exception, *, api_calls: int) -> None:
        super().__init__(str(original))
        self.api_calls = api_calls
        self.original = original


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
        staged_count = await sync_bulk_ingest_helpers._flush_staged_stock_bulk_rows(ctx)
        if staged_count is not None:
            stocks_updated = staged_count
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


async def execute_stock_data_rest_date(ctx: Any, *, date: str) -> StockDataRestDateOutcome:
    payload, page_calls = await get_paginated_rows_with_call_count(
        ctx.client,
        "/equities/bars/daily",
        params={"date": date},
    )

    async def _prefetched_stock_rows() -> list[dict[str, Any]]:
        return payload

    try:
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
    except Exception as exc:
        raise StockDataRestDateIngestionError(exc, api_calls=page_calls) from exc

    return StockDataRestDateOutcome(
        api_calls=page_calls,
        stocks_updated=batch.published_count,
    )
