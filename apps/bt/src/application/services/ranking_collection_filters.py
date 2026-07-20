"""In-place collection helpers for market ranking items."""

from __future__ import annotations

from datetime import date as calendar_date, datetime, timedelta

from src.application.contracts import ranking as ranking_contracts
from src.application.services.ranking_query_helpers import normalize_equity_code
from src.domains.analytics.daily_ranking_core import (
    DailyRankingValuationMetrics,
    DailyRankingValuationState,
    classify_valuation_state,
)
from src.shared.utils.market_code_alias import normalize_market_scope


def filter_ranking_collections_by_forward_eps_source_date(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
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
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    limit: int,
) -> None:
    for collection in collections:
        if limit > 0:
            del collection[limit:]
        for rank, item in enumerate(collection, start=1):
            item.rank = rank


def group_ranking_items_by_normalized_code(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
) -> dict[str, list[ranking_contracts.RankingItem]]:
    items_by_code: dict[str, list[ranking_contracts.RankingItem]] = {}
    for collection in collections:
        for item in collection:
            items_by_code.setdefault(normalize_equity_code(item.code), []).append(item)
    return items_by_code


def filter_ranking_collections_by_regime_state(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    regime_state: ranking_contracts.RankingRegimeStateFilter | None,
) -> None:
    if regime_state is None:
        return

    for collection in collections:
        collection[:] = [
            item for item in collection if item.liquidityRegime == regime_state
        ]


def filter_ranking_collections_by_fundamental_state(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    fundamental_state: ranking_contracts.RankingFundamentalStateFilter | None,
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
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    risk_state: ranking_contracts.RankingRiskStateFilter | None,
) -> None:
    if risk_state is None:
        return

    for collection in collections:
        collection[:] = [item for item in collection if risk_state in item.riskFlags]


def matches_fundamental_state(
    item: ranking_contracts.RankingItem,
    *,
    fundamental_state: ranking_contracts.RankingFundamentalStateFilter,
) -> bool:
    state = _valuation_state_for_item(item)
    if fundamental_state == "deep_value":
        return state.strong_value_confirmation
    if fundamental_state == "value_confirmed":
        return state.medium_value_confirmation
    if fundamental_state == "undervalued":
        return (
            not state.strong_value_confirmation
            and not state.very_overvalued_warning
            and not state.overvalued_warning
            and not state.no_positive_earnings_valuation
            and state.medium_value_confirmation
        )
    if fundamental_state == "expensive_or":
        return state.expensive_per_or_psr
    if fundamental_state == "overvalued":
        return not state.very_overvalued_warning and state.overvalued_warning
    if fundamental_state == "very_overvalued":
        return state.very_overvalued_warning
    if fundamental_state == "no_earnings":
        return state.no_positive_earnings_valuation
    return False


def _valuation_state_for_item(
    item: ranking_contracts.RankingItem,
) -> DailyRankingValuationState:
    market_scope = normalize_market_scope(item.marketCode, default="unknown")
    population = "prime" if market_scope == "prime" else "non_prime_unsupported"
    return classify_valuation_state(
        DailyRankingValuationMetrics(
            percentile_population=population,
            per_percentile=item.perPercentile,
            forward_per_percentile=item.forwardPerPercentile,
            forward_p_op_percentile=item.forwardPOpPercentile,
            pbr_percentile=item.pbrPercentile,
            per=item.per,
            forward_per=item.forwardPer,
            psr_percentile=item.psrPercentile,
            forward_psr_percentile=item.forwardPsrPercentile,
        )
    )


def filter_ranking_collections_by_technical_state(
    collections: tuple[list[ranking_contracts.RankingItem], ...],
    *,
    technical_state: ranking_contracts.RankingTechnicalStateFilter | None,
) -> None:
    if technical_state is None:
        return

    for collection in collections:
        collection[:] = [
            item for item in collection if technical_state in item.technicalFlags
        ]
