"""Response assembly helpers for market screening."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from src.application.contracts.analytics import ResponseDiagnostics
from src.application.contracts.screening import (
    MarketScreeningResponse,
    MatchedStrategyItem,
    ScreeningResultItem,
    ScreeningSummary,
)
from src.application.services.analytics_provenance import build_market_provenance
from src.domains.analytics.screening_results import ScreeningSortBy, SortOrder
from src.domains.strategy.runtime.screening_profile import EntryDecidability

SCREENING_USED_DOMAINS = (
    "stock_data",
    "stock_master_daily",
    "topix_data",
    "indices_data",
    "margin_data",
    "statements",
)


@dataclass(frozen=True)
class ScreeningAggregation:
    results: list[ScreeningResultItem]
    by_strategy: dict[str, int]
    processed_codes: set[str]
    warnings: list[str]


def aggregate_screening_results(
    *,
    strategy_runtimes: Iterable[Any],
    strategy_results: Iterable[Any],
    strategy_scores: Mapping[str, float | None],
    build_result_item: Callable[[dict[str, Any]], ScreeningResultItem],
) -> ScreeningAggregation:
    """Merge per-strategy screening matches into per-stock response rows."""
    by_strategy = {strategy.response_name: 0 for strategy in strategy_runtimes}
    processed_codes: set[str] = set()
    aggregated: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []

    for strategy_result in strategy_results:
        strategy_name = strategy_result.strategy.response_name
        processed_codes |= strategy_result.processed_codes
        warnings.extend(strategy_result.warnings)

        strategy_score = strategy_scores.get(strategy_name)
        for stock, matched_date in strategy_result.matched_rows:
            by_strategy[strategy_name] += 1

            existing = aggregated.get(stock.code)
            if existing is None:
                existing = {
                    "stock": stock,
                    "matchedDate": matched_date,
                    "matchedStrategies": [],
                }
                aggregated[stock.code] = existing
            elif matched_date > existing["matchedDate"]:
                existing["matchedDate"] = matched_date

            existing["matchedStrategies"].append(
                MatchedStrategyItem(
                    strategyName=strategy_name,
                    matchedDate=matched_date,
                    strategyScore=strategy_score,
                )
            )

    return ScreeningAggregation(
        results=[build_result_item(item) for item in aggregated.values()],
        by_strategy=by_strategy,
        processed_codes=processed_codes,
        warnings=warnings,
    )


def dedupe_screening_warnings(warnings: list[str], *, limit: int) -> list[str]:
    """Deduplicate warnings while preserving order and keeping payload bounded."""
    deduped: list[str] = []
    seen: set[str] = set()

    for warning in warnings:
        if warning in seen:
            continue
        seen.add(warning)
        deduped.append(warning)
        if len(deduped) >= limit:
            break

    if len(warnings) > len(deduped):
        deduped.append("additional warnings were truncated")

    return deduped


def build_screening_response(
    *,
    results: list[ScreeningResultItem],
    summary: ScreeningSummary,
    entry_decidability: EntryDecidability,
    requested_market_codes: list[str],
    scope_label: str,
    recent_days: int,
    reference_date: str,
    sort_by: ScreeningSortBy,
    order: SortOrder,
    last_updated: str,
) -> MarketScreeningResponse:
    return MarketScreeningResponse(
        results=results,
        summary=summary,
        entry_decidability=entry_decidability,
        markets=requested_market_codes,
        scopeLabel=scope_label,
        recentDays=recent_days,
        referenceDate=reference_date,
        sortBy=sort_by,
        order=order,
        lastUpdated=last_updated,
        provenance=build_market_provenance(
            reference_date=reference_date,
            loaded_domains=SCREENING_USED_DOMAINS,
            warnings=summary.warnings,
        ),
        diagnostics=ResponseDiagnostics(
            missing_required_data=[],
            used_fields=list(SCREENING_USED_DOMAINS),
            effective_period_type="multi",
            warnings=summary.warnings,
        ),
    )
