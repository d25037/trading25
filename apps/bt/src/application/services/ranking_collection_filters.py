"""In-place collection helpers for market ranking items."""

from __future__ import annotations

from datetime import date as calendar_date, datetime, timedelta

from src.application.services.ranking_query_helpers import normalize_equity_code
from src.application.services.ranking_state_flags import RISK_FLAG_STATE_FILTERS
from src.entrypoints.http.schemas.ranking import (
    RankingItem,
    RankingRegimeStateFilter,
    RankingRiskStateFilter,
    RankingStateFilter,
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


def filter_ranking_collections_by_liquidity_state(
    collections: tuple[list[RankingItem], ...],
    *,
    liquidity_state: RankingStateFilter | None,
) -> None:
    if liquidity_state is None:
        return

    for collection in collections:
        if liquidity_state in RISK_FLAG_STATE_FILTERS:
            collection[:] = [
                item for item in collection if liquidity_state in item.riskFlags
            ]
        else:
            collection[:] = [
                item for item in collection if item.liquidityRegime == liquidity_state
            ]


def filter_ranking_collections_by_regime_state(
    collections: tuple[list[RankingItem], ...],
    *,
    regime_state: RankingRegimeStateFilter | None,
) -> None:
    if regime_state is None:
        return

    if regime_state == "neutral_rerating_good":
        target_regime = "neutral_rerating"
        require_good = True
    elif regime_state == "crowded_rerating_good":
        target_regime = "crowded_rerating"
        require_good = True
    else:
        target_regime = regime_state
        require_good = False

    for collection in collections:
        collection[:] = [
            item
            for item in collection
            if item.liquidityRegime == target_regime
            and (
                not require_good
                or has_rerating_good_confirmation(item, target_regime=target_regime)
            )
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


def has_rerating_good_confirmation(
    item: RankingItem,
    *,
    target_regime: str,
) -> bool:
    if target_regime == "neutral_rerating":
        return _has_low_pbr_and_low_forward_per(
            item
        ) or _has_low_per_forward_per_improvement(item, max_ratio=0.8)
    if target_regime == "crowded_rerating":
        return (
            _has_low_pbr_and_low_forward_per(item)
            or _has_low_per_forward_per_improvement(item, max_ratio=0.8)
            or _has_low_pbr(item)
            or _has_low_per_forward_per_improvement(item, max_ratio=1.0)
        )
    return False


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
