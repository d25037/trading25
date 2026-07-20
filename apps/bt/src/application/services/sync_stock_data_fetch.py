"""Stock daily fetch helpers for market sync strategies."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast

from loguru import logger

from src.application.services import sync_fetch_planner, sync_publish_helpers
from src.application.services.ingestion_pipeline import run_ingestion_batch, validate_rows_required_fields
from src.application.services.jquants_bulk_service import BulkFetchResult, BulkFileInfo
from src.application.services.sync_paginated_fetch import get_paginated_rows_with_call_count
from src.application.services.sync_row_converters import build_target_date_set
from src.application.services.stock_refresh_service import refresh_stocks
from src.shared.config.settings import get_settings
from src.infrastructure.db.market.market_mutations import SemanticDeltaResult
from src.application.services.sync_row_converters import convert_stock_data_rows as _convert_stock_data_rows
from src.application.services.sync_row_converters import (
    convert_stock_bulk_rows as _convert_stock_bulk_rows,
)


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


class AffectedStockRefreshError(RuntimeError):
    """Raised when any affected code cannot be fully refreshed."""


@dataclass(frozen=True, slots=True)
class StockDataCommitOutcome:
    api_calls: int = 0
    appended_rows: int = 0
    affected_codes: frozenset[str] = frozenset()
    replaced_codes: int = 0
    replaced_rows: int = 0


class _ProviderPlanClient:
    def __init__(self, delegate: Any, *, plan: str) -> None:
        self._delegate = delegate
        self.plan = plan

    async def get_paginated_with_meta(
        self,
        path: str,
        params: dict[str, str] | None = None,
        max_pages: int = 10,
    ) -> tuple[list[dict[str, Any]], int]:
        get_with_meta = getattr(self._delegate, "get_paginated_with_meta", None)
        if not callable(get_with_meta):
            raise RuntimeError(
                "Affected stock refresh requires terminal pagination proof before publication"
            )
        fetch = cast(
            Callable[..., Awaitable[tuple[list[dict[str, Any]], int]]],
            get_with_meta,
        )
        rows, calls = await fetch(path, params=params, max_pages=max_pages)
        return rows, int(calls)


def _provider_plan(ctx: Any) -> str:
    for candidate in (
        getattr(ctx, "provider_plan", None),
        getattr(ctx.client, "plan", None),
        get_settings().jquants_plan,
    ):
        normalized = str(candidate).strip().lower() if candidate is not None else ""
        if normalized:
            return normalized
    raise RuntimeError("JQUANTS_PLAN is required for provider stock refresh")


@dataclass(slots=True)
class StockDataIngestionSession:
    """Stage all dates, then append normal rows and replace affected codes."""

    affected_codes: set[str] = field(default_factory=set)
    staged_rows: int = 0

    async def stage(self, ctx: Any, rows: list[dict[str, Any]]) -> None:
        result = await sync_publish_helpers._stage_stock_data_rows(ctx, rows)
        self.affected_codes.update(result.affected_codes)
        self.staged_rows += result.staged_rows

    async def discard(self, ctx: Any) -> None:
        await sync_publish_helpers._discard_staged_stock_data_rows(ctx)
        self.affected_codes.clear()
        self.staged_rows = 0

    async def commit(self, ctx: Any) -> StockDataCommitOutcome:
        affected_codes = frozenset(self.affected_codes)
        api_calls = 0
        replaced_codes = 0
        replaced_rows = 0
        if affected_codes:
            ctx.on_progress(
                "stock_data_refresh",
                0,
                len(affected_codes),
                f"Refreshing {len(affected_codes)} affected stock provider windows...",
            )

            def _on_refresh_progress(
                completed: int, total: int, message: str
            ) -> None:
                ctx.on_progress("stock_data_refresh", completed, total, message)

            try:
                response = await refresh_stocks(
                    sorted(affected_codes),
                    ctx.market_db,
                    ctx.time_series_store,
                    _ProviderPlanClient(ctx.client, plan=_provider_plan(ctx)),
                    progress_callback=_on_refresh_progress,
                    cancel_check=ctx.cancelled.is_set,
                )
            except BaseException:
                await sync_publish_helpers._discard_staged_stock_data_rows(ctx)
                raise
            api_calls = response.totalApiCalls
            replaced_codes = response.successCount
            replaced_rows = response.totalRecordsStored
            if response.failedCount or response.errors:
                await sync_publish_helpers._discard_staged_stock_data_rows(ctx)
                detail = "; ".join(response.errors) or "affected stock refresh failed"
                raise AffectedStockRefreshError(detail)

        append_result = await sync_publish_helpers._flush_staged_stock_data_rows(
            ctx,
            exclude_codes=affected_codes,
        )
        recompute = getattr(ctx, "recompute_affected_stock_codes", None)
        if affected_codes and callable(recompute):
            await cast(
                Callable[[frozenset[str]], Awaitable[None]], recompute
            )(affected_codes)
        outcome = StockDataCommitOutcome(
            api_calls=api_calls,
            appended_rows=append_result.stats.inserted,
            affected_codes=affected_codes,
            replaced_codes=replaced_codes,
            replaced_rows=replaced_rows,
        )
        on_stock_commit = getattr(ctx, "on_stock_commit", None)
        if callable(on_stock_commit):
            on_stock_commit(
                outcome.appended_rows,
                len(outcome.affected_codes),
                outcome.replaced_codes,
                outcome.replaced_rows,
            )
        self.affected_codes.clear()
        self.staged_rows = 0
        return outcome


async def execute_stock_data_bulk_fetch(
    ctx: Any,
    *,
    session: StockDataIngestionSession,
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
            del _file_info
            rows = _convert_stock_bulk_rows(
                batch_rows,
                target_dates=target_date_set,
            )
            await session.stage(ctx, rows)

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
        await session.discard(ctx)
        sync_fetch_planner._raise_if_bulk_rate_limited(e, stage_name=stage_name)
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


async def execute_stock_data_rest_date(
    ctx: Any,
    *,
    session: StockDataIngestionSession,
    date: str,
) -> StockDataRestDateOutcome:
    payload, page_calls = await get_paginated_rows_with_call_count(
        ctx.client,
        "/equities/bars/daily",
        params={"date": date},
    )

    async def _prefetched_stock_rows() -> list[dict[str, Any]]:
        return payload

    async def _stage_stock_rows(
        rows: list[dict[str, Any]],
    ) -> SemanticDeltaResult:
        await session.stage(ctx, rows)
        return SemanticDeltaResult.empty(input_count=len(rows))

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
            publish=_stage_stock_rows,
        )
    except Exception as exc:
        raise StockDataRestDateIngestionError(exc, api_calls=page_calls) from exc

    return StockDataRestDateOutcome(
        api_calls=page_calls,
        stocks_updated=batch.published_count,
    )
