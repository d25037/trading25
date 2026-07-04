"""In-place collection helpers for market ranking items."""

from __future__ import annotations

from datetime import date as calendar_date, datetime, timedelta

from src.application.services.ranking_query_helpers import normalize_equity_code
from src.entrypoints.http.schemas.ranking import (
    RankingFundamentalStateFilter,
    RankingItem,
    RankingRegimeStateFilter,
    RankingRiskStateFilter,
    RankingTechnicalStateFilter,
)


def filter_ranking_collections_by_forward_eps_source_date(
    collections: tuple[list[RankingItem], ...],
    *,
    target_date: str,
    forward_eps_disclosed_within_days: int,
) -> None:
    if forward_eps_disclosed_within_days <= 0:
        return

    try:
        max_date = datetime.fromisoformat(target_date).date()
    except ValueError:
        return
    min_date = max_date - timedelta(days=forward_eps_disclosed_within_days)

    for collection in collections:
        collection[:] = [
            item
            for item in collection
            if is_forward_eps_source_date_in_window(
                item.forwardEpsDisclosedDate,
                min_date=min_date,
                max_date=max_date,
            )
        ]


def is_forward_eps_source_date_in_window(
    disclosed_date: str | None,
    *,
    min_date: calendar_date,
    max_date: calendar_date,
) -> bool:
    if disclosed_date is None:
        return False
    try:
        source_date = datetime.fromisoformat(disclosed_date).date()
    except ValueError:
        return False
    return min_date <= source_date <= max_date


def limit_and_rerank_ranking_collections(
    collections: tuple[list[RankingItem], ...],
    limit: int,
) -> None:
    for collection in collections:
        if limit > 0:
            del collection[limit:]
        for rank, item in enumerate(collection, start=1):
            item.rank = rank


def group_ranking_items_by_normalized_code(
    collections: tuple[list[RankingItem], ...],
) -> dict[str, list[RankingItem]]:
    items_by_code: dict[str, list[RankingItem]] = {}
    for collection in collections:
        for item in collection:
            items_by_code.setdefault(normalize_equity_code(item.code), []).append(item)
    return items_by_code


def filter_ranking_collections_by_regime_state(
    collections: tuple[list[RankingItem], ...],
    *,
    regime_state: RankingRegimeStateFilter | None,
) -> None:
    if regime_state is None:
        return

    for collection in collections:
        collection[:] = [
            item for item in collection if item.liquidityRegime == regime_state
        ]


def filter_ranking_collections_by_fundamental_state(
    collections: tuple[list[RankingItem], ...],
    *,
    fundamental_state: RankingFundamentalStateFilter | None,
) -> None:
    if fundamental_state is None:
        return

    for collection in collections:
        collection[:] = [
            item
            for item in collection
            if matches_fundamental_state(item, fundamental_state=fundamental_state)
        ]


def filter_ranking_collections_by_risk_state(
    collections: tuple[list[RankingItem], ...],
    *,
    risk_state: RankingRiskStateFilter | None,
) -> None:
    if risk_state is None:
        return

    for collection in collections:
        collection[:] = [item for item in collection if risk_state in item.riskFlags]


def matches_fundamental_state(
    item: RankingItem,
    *,
    fundamental_state: RankingFundamentalStateFilter,
) -> bool:
    if fundamental_state == "deep_value":
        return _has_deep_value_confirmation(item)
    if fundamental_state == "value_confirmed":
        return _has_value_confirmation(item)
    if fundamental_state == "undervalued":
        return (
            not _has_deep_value_confirmation(item)
            and not _has_very_expensive_valuation_warning(item)
            and not _has_expensive_valuation_warning(item)
            and not _has_no_earnings_valuation_warning(item)
            and _has_value_confirmation(item)
        )
    if fundamental_state == "expensive_or":
        return _has_expensive_per_or_psr(item)
    if fundamental_state == "overvalued":
        return (
            not _has_very_expensive_valuation_warning(item)
            and _has_expensive_valuation_warning(item)
        )
    if fundamental_state == "very_overvalued":
        return _has_very_expensive_valuation_warning(item)
    if fundamental_state == "no_earnings":
        return _has_no_earnings_valuation_warning(item)
    return False


def _has_deep_value_confirmation(item: RankingItem) -> bool:
    return _has_low_pbr_and_low_forward_per(
        item
    ) or _has_low_per_forward_per_improvement(item, max_ratio=0.8)


def _has_value_confirmation(item: RankingItem) -> bool:
    return (
        _has_deep_value_confirmation(item)
        or _has_low_pbr(item)
        or _has_low_per_forward_per_improvement(item, max_ratio=1.0)
    )


def _has_low_pbr(item: RankingItem) -> bool:
    return _is_percentile_at_or_below(item.pbrPercentile, 0.2)


def _has_low_pbr_and_low_forward_per(item: RankingItem) -> bool:
    return _is_percentile_at_or_below(
        item.pbrPercentile, 0.2
    ) and _is_percentile_at_or_below(item.forwardPerPercentile, 0.2)


def _has_low_per_forward_per_improvement(
    item: RankingItem,
    *,
    max_ratio: float,
) -> bool:
    if not _is_percentile_at_or_below(item.perPercentile, 0.2):
        return False
    if item.forwardPer is None or item.per is None:
        return False
    if item.forwardPer <= 0 or item.per <= 0:
        return False
    return item.forwardPer / item.per <= max_ratio


def _is_percentile_at_or_below(value: float | None, threshold: float) -> bool:
    return value is not None and value <= threshold


def _has_expensive_per_or_psr(item: RankingItem) -> bool:
    return any(
        _is_percentile_at_or_above(value, 0.8)
        for value in (
            item.perPercentile,
            item.forwardPerPercentile,
            item.psrPercentile,
            item.forwardPsrPercentile,
        )
    )


def _has_expensive_valuation_warning(item: RankingItem) -> bool:
    return any(
        _is_percentile_at_or_above(value, 0.8)
        for value in (
            item.perPercentile,
            item.forwardPerPercentile,
            item.forwardPOpPercentile,
            item.pbrPercentile,
        )
    )


def _has_very_expensive_valuation_warning(item: RankingItem) -> bool:
    return any(
        _is_percentile_at_or_above(value, 0.9)
        for value in (
            item.perPercentile,
            item.forwardPerPercentile,
            item.forwardPOpPercentile,
            item.pbrPercentile,
        )
    )


def _has_no_earnings_valuation_warning(item: RankingItem) -> bool:
    return item.perPercentile is None and item.forwardPerPercentile is None


def _is_percentile_at_or_above(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def filter_ranking_collections_by_technical_state(
    collections: tuple[list[RankingItem], ...],
    *,
    technical_state: RankingTechnicalStateFilter | None,
) -> None:
    if technical_state is None:
        return

    for collection in collections:
        collection[:] = [
            item for item in collection if technical_state in item.technicalFlags
        ]
