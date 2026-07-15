"""Bulk/REST fetch planning helpers for market sync strategies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Literal, NoReturn, Protocol

from loguru import logger

from src.infrastructure.external_api.clients.jquants_client import JQuantsApiError

from src.application.services.jquants_bulk_service import (
    BulkFileInfo,
    BulkFetchPlan,
    BulkFetchResult,
    JQuantsBulkService,
)
from src.application.services.sync_row_converters import _parse_date


_SyncClientLike = Any


class _BulkServiceLike(Protocol):  # pragma: no cover
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


_SyncFetchContext = Any


_FetchMethod = Literal["rest", "bulk"]


@dataclass(frozen=True)
class _StageFetchDecision:
    method: _FetchMethod
    planner_api_calls: int
    estimated_rest_calls: int
    estimated_bulk_calls: int | None
    plan: BulkFetchPlan | None = None
    reason: str = "unspecified"
    reason_detail: str | None = None


@dataclass(frozen=True)
class _BulkFetchStageOutcome:
    api_calls: int
    bulk_result: BulkFetchResult | None
    used_rest_fallback: bool
    fallback_reason: str | None


class BulkFetchRequiredError(RuntimeError):
    """Raised when stock_data sync requires bulk but planner/execution cannot use it."""


def _get_plan_hint(client: _SyncClientLike) -> str:
    return str(getattr(client, "plan", "")).strip().lower()


def _get_bulk_service(ctx: _SyncFetchContext) -> _BulkServiceLike:
    if ctx.bulk_service is None:
        ctx.bulk_service = JQuantsBulkService(ctx.client)
    return ctx.bulk_service


async def _plan_fetch_method(
    ctx: _SyncFetchContext,
    *,
    stage: str,
    endpoint: str,
    estimated_rest_calls: int,
    date_from: str | None = None,
    date_to: str | None = None,
    exact_dates: list[str] | None = None,
    min_rest_calls_to_probe_bulk: int = 3,
    require_bulk: bool = False,
    disable_future_bulk_on_probe_failure: bool = True,
) -> _StageFetchDecision:
    endpoint_probe_disabled = endpoint in getattr(ctx, "bulk_probe_disabled_endpoints", set())
    if ctx.bulk_probe_disabled or endpoint_probe_disabled:
        plan_hint = _get_plan_hint(ctx.client)
        reason_detail = (
            ctx.bulk_probe_failure_reason
            if ctx.bulk_probe_disabled
            else f"bulk probe disabled for endpoint {endpoint}"
        )
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="bulk_probe_disabled",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="bulk_probe_disabled",
            reason_detail=reason_detail,
        )

    if not require_bulk and estimated_rest_calls < min_rest_calls_to_probe_bulk:
        plan_hint = _get_plan_hint(ctx.client)
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="rest_estimate_too_small",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=0,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=0,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="rest_estimate_too_small",
        )

    bulk_service = _get_bulk_service(ctx)
    plan_hint = _get_plan_hint(ctx.client)
    try:
        plan = await bulk_service.build_plan(
            endpoint=endpoint,
            date_from=date_from,
            date_to=date_to,
            exact_dates=exact_dates,
        )
    except Exception as e:
        _raise_if_bulk_rate_limited(e, stage_name=stage)
        # Free/unknown plans and transient /bulk/list failures fall back to REST
        # so the sync job can continue. Some stages also suppress further probes.
        probe_failure_reason = _summarize_exception(e)
        if disable_future_bulk_on_probe_failure:
            disabled_endpoints = getattr(ctx, "bulk_probe_disabled_endpoints", None)
            if disabled_endpoints is None:
                ctx.bulk_probe_disabled = True
            else:
                disabled_endpoints.add(endpoint)
            ctx.bulk_probe_failure_reason = probe_failure_reason
        logger.warning(
            "sync bulk plan probe failed, falling back to REST for this job: {}",
            e,
        )
        logger.info(
            "sync fetch strategy selected",
            event="sync_fetch_strategy",
            stage=stage,
            endpoint=endpoint,
            selected="rest",
            reason="bulk_probe_failed",
            estimatedRestCalls=estimated_rest_calls,
            estimatedBulkCalls=None,
            plannerApiCalls=1,
            planHint=plan_hint or None,
            requireBulk=require_bulk,
        )
        return _StageFetchDecision(
            method="rest",
            planner_api_calls=1,
            estimated_rest_calls=estimated_rest_calls,
            estimated_bulk_calls=None,
            plan=None,
            reason="bulk_probe_failed",
            reason_detail=probe_failure_reason,
        )

    if require_bulk:
        selected: _FetchMethod = "bulk"
        reason = "bulk_required"
    else:
        selected = "bulk" if plan.estimated_api_calls < estimated_rest_calls else "rest"
        reason = "bulk_estimate_lower" if selected == "bulk" else "rest_estimate_lower_or_equal"

    logger.info(
        "sync fetch strategy selected",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=selected,
        reason=reason,
        estimatedRestCalls=estimated_rest_calls,
        estimatedBulkCalls=plan.estimated_api_calls,
        plannerApiCalls=plan.list_api_calls,
        estimatedCacheHits=plan.estimated_cache_hits,
        estimatedCacheMisses=plan.estimated_cache_misses,
        selectedFiles=len(plan.files),
        planHint=plan_hint or None,
        requireBulk=require_bulk,
    )
    return _StageFetchDecision(
        method=selected,
        planner_api_calls=plan.list_api_calls,
        estimated_rest_calls=estimated_rest_calls,
        estimated_bulk_calls=plan.estimated_api_calls,
        plan=plan,
        reason=reason,
    )


def _log_sync_fetch_execution(
    *,
    stage: str,
    endpoint: str,
    decision: _StageFetchDecision,
    executed: _FetchMethod,
    actual_api_calls: int,
    fallback: bool,
    bulk_result: BulkFetchResult | None = None,
) -> None:
    cache_hit_rate: float | None = None
    cache_hits = 0
    cache_misses = 0
    if bulk_result is not None:
        cache_hits = bulk_result.cache_hits
        cache_misses = bulk_result.cache_misses
        total = cache_hits + cache_misses
        cache_hit_rate = (cache_hits / total) if total > 0 else None

    logger.info(
        "sync fetch strategy execution",
        event="sync_fetch_strategy",
        stage=stage,
        endpoint=endpoint,
        selected=decision.method,
        executed=executed,
        fallbackUsed=fallback,
        estimatedRestCalls=decision.estimated_rest_calls,
        estimatedBulkCalls=decision.estimated_bulk_calls,
        plannerApiCalls=decision.planner_api_calls,
        actualApiCalls=actual_api_calls,
        cacheHits=cache_hits,
        cacheMisses=cache_misses,
        cacheHitRate=cache_hit_rate,
    )


def _format_fetch_estimate(value: int | None) -> str:
    return str(value) if value is not None else "n/a"


def _summarize_exception(exc: Exception, *, limit: int = 200) -> str:
    text = str(exc).replace("\n", " ").strip() or exc.__class__.__name__
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _describe_bulk_unavailable_reason(
    *,
    reason: str,
    reason_detail: str | None = None,
) -> str:
    reason_map = {
        "bulk_probe_disabled": "bulk probe is disabled after a previous probe failure",
        "bulk_probe_failed": "bulk plan probe failed",
        "rest_estimate_too_small": "rest estimate is below bulk probe threshold",
        "rest_estimate_lower_or_equal": "planner selected REST based on API-call estimate",
        "bulk_plan_missing": "bulk plan is missing",
        "bulk_plan_empty": "bulk/list returned no matching files for requested dates",
        "bulk_fetch_failed": "bulk fetch execution failed",
    }
    base = reason_map.get(reason, f"bulk unavailable ({reason})")
    if not reason_detail:
        return base
    return f"{base}: {reason_detail}"


def _raise_stock_bulk_required_error(
    ctx: _SyncFetchContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    reason: str,
    reason_detail: str | None = None,
) -> NoReturn:
    detail = _describe_bulk_unavailable_reason(reason=reason, reason_detail=reason_detail)
    message = (
        f"Bulk fetch required for {endpoint} but unavailable ({detail}). "
        "REST fallback is disabled for stock_data sync."
    )
    ctx.on_progress(progress_stage, current, total, message)
    raise BulkFetchRequiredError(message)


def _enforce_stock_bulk_plan_available(
    ctx: _SyncFetchContext,
    *,
    decision: _StageFetchDecision,
    endpoint: str,
    progress_stage: str,
    current: int,
    total: int,
    target_count: int,
) -> None:
    if not ctx.enforce_bulk_for_stock_data or target_count <= 0:
        return

    if decision.method != "bulk":
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason=decision.reason,
            reason_detail=decision.reason_detail,
        )

    if decision.plan is None:
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason="bulk_plan_missing",
        )

    if len(decision.plan.files) == 0:
        _raise_stock_bulk_required_error(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            reason="bulk_plan_empty",
            reason_detail=f"targets={target_count} dates",
        )


def _resolve_bulk_fallback_reason(plan: BulkFetchPlan | None) -> str:
    if plan is None:
        return "bulk_plan_missing"
    if len(plan.files) == 0:
        return "bulk_plan_empty"
    return "bulk_plan_unavailable"


def _raise_if_bulk_rate_limited(error: Exception, *, stage_name: str) -> None:
    if isinstance(error, JQuantsApiError) and error.upstream_status_code == 429:
        raise RuntimeError(
            f"{stage_name} bulk fetch was rate-limited after retries; "
            "refusing REST fallback to avoid request amplification. "
            "Retry after the shared J-Quants cooldown."
        ) from error


async def _execute_bulk_fetch_stage(
    ctx: _SyncFetchContext,
    *,
    decision: _StageFetchDecision,
    stage_name: str,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    target_label: str | None,
    on_rows_batch: Callable[[list[dict[str, Any]], BulkFileInfo], Awaitable[None]],
    fallback_log_message: str,
) -> _BulkFetchStageOutcome:
    if decision.method != "bulk":
        return _BulkFetchStageOutcome(
            api_calls=0,
            bulk_result=None,
            used_rest_fallback=False,
            fallback_reason=None,
        )

    if decision.plan is None or len(decision.plan.files) == 0:
        fallback_reason = _resolve_bulk_fallback_reason(decision.plan)
        logger.warning(fallback_log_message, fallback_reason)
        return _BulkFetchStageOutcome(
            api_calls=0,
            bulk_result=None,
            used_rest_fallback=True,
            fallback_reason=fallback_reason,
        )

    try:
        _emit_fetch_execution_progress(
            ctx,
            progress_stage=progress_stage,
            current=current,
            total=total,
            endpoint=endpoint,
            method="bulk",
            target_label=target_label,
        )
        bulk_result = await _get_bulk_service(ctx).fetch_with_plan(
            decision.plan,
            on_rows_batch=on_rows_batch,
            accumulate_rows=False,
        )
        _log_sync_fetch_execution(
            stage=stage_name,
            endpoint=endpoint,
            decision=decision,
            executed="bulk",
            actual_api_calls=bulk_result.api_calls,
            fallback=False,
            bulk_result=bulk_result,
        )
        return _BulkFetchStageOutcome(
            api_calls=bulk_result.api_calls,
            bulk_result=bulk_result,
            used_rest_fallback=False,
            fallback_reason=None,
        )
    except Exception as e:
        _raise_if_bulk_rate_limited(e, stage_name=stage_name)
        fallback_reason = _summarize_exception(e)
        logger.warning(fallback_log_message, fallback_reason)
        return _BulkFetchStageOutcome(
            api_calls=0,
            bulk_result=None,
            used_rest_fallback=True,
            fallback_reason=fallback_reason,
        )


def _filter_bulk_plan_after_exclusive_anchor(
    plan: BulkFetchPlan,
    *,
    anchor: str | None,
) -> tuple[BulkFetchPlan, int]:
    """Drop bulk files whose inferred range cannot contain rows after anchor."""
    anchor_date = _parse_date(anchor or "")
    if anchor_date is None or not plan.files:
        return plan, 0

    files: list[BulkFileInfo] = []
    skipped = 0
    for file_info in plan.files:
        if file_info.range_end is not None and file_info.range_end <= anchor_date:
            skipped += 1
            continue
        files.append(file_info)

    if skipped == 0:
        return plan, 0
    return replace(plan, files=files), skipped


def _emit_fetch_strategy_progress(
    ctx: _SyncFetchContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    decision: _StageFetchDecision,
    target_label: str | None = None,
) -> None:
    target_text = f", targets={target_label}" if target_label else ""
    ctx.on_progress(
        progress_stage,
        current,
        total,
        (
            f"Fetch strategy: {endpoint} -> {decision.method.upper()} "
            f"(REST est={decision.estimated_rest_calls}, "
            f"BULK est={_format_fetch_estimate(decision.estimated_bulk_calls)}{target_text})"
        ),
    )
    _emit_fetch_detail(
        ctx,
        {
            "eventType": "strategy",
            "stage": progress_stage,
            "endpoint": endpoint,
            "method": decision.method,
            "targetLabel": target_label,
            "reason": decision.reason,
            "reasonDetail": decision.reason_detail,
            "estimatedRestCalls": decision.estimated_rest_calls,
            "estimatedBulkCalls": decision.estimated_bulk_calls,
            "plannerApiCalls": decision.planner_api_calls,
            "fallback": False,
            "fallbackReason": None,
        },
    )


def _emit_fetch_execution_progress(
    ctx: _SyncFetchContext,
    *,
    progress_stage: str,
    current: int,
    total: int,
    endpoint: str,
    method: _FetchMethod,
    target_label: str | None = None,
    fallback: bool = False,
    fallback_reason: str | None = None,
) -> None:
    target_text = f", targets={target_label}" if target_label else ""
    fallback_text = ""
    if fallback:
        fallback_text = (
            f" (bulk fallback: {fallback_reason})"
            if fallback_reason
            else " (bulk fallback)"
        )
    ctx.on_progress(
        progress_stage,
        current,
        total,
        f"Fetching {endpoint} via {method.upper()}{fallback_text}{target_text}...",
    )
    _emit_fetch_detail(
        ctx,
        {
            "eventType": "execution",
            "stage": progress_stage,
            "endpoint": endpoint,
            "method": method,
            "targetLabel": target_label,
            "reason": None,
            "reasonDetail": None,
            "estimatedRestCalls": None,
            "estimatedBulkCalls": None,
            "plannerApiCalls": None,
            "fallback": fallback,
            "fallbackReason": fallback_reason,
        },
    )


def _emit_fetch_detail(ctx: _SyncFetchContext, detail: dict[str, Any]) -> None:
    if ctx.on_fetch_detail is None:
        return
    try:
        ctx.on_fetch_detail(detail)
    except Exception as e:  # noqa: BLE001 - fetch detail failures should not abort sync
        logger.warning("Failed to emit sync fetch detail: {}", e)


__all__ = [
    "BulkFetchRequiredError",
    "_BulkFetchStageOutcome",
    "_StageFetchDecision",
    "_emit_fetch_detail",
    "_emit_fetch_execution_progress",
    "_emit_fetch_strategy_progress",
    "_enforce_stock_bulk_plan_available",
    "_execute_bulk_fetch_stage",
    "_filter_bulk_plan_after_exclusive_anchor",
    "_get_bulk_service",
    "_get_plan_hint",
    "_log_sync_fetch_execution",
    "_plan_fetch_method",
    "_raise_stock_bulk_required_error",
    "_resolve_bulk_fallback_reason",
    "_summarize_exception",
]
