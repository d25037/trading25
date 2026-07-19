"""Technical Fit compatibility exports for generic event-time price relations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from src.domains.analytics.daily_ranking_event_time_prices import (
    DailyRankingPriceLineage,
    DailyRankingPriceRequest,
    build_daily_ranking_event_time_prices,
)


SIGNAL_FEATURE_RELATION = "ranking_technical_fit_signal_price_features"
FORWARD_OUTCOME_RELATION = "ranking_technical_fit_forward_price_outcomes"

EventTimePriceAudit = DailyRankingPriceLineage


@dataclass(frozen=True)
class EventTimePriceRelations:
    signal_features: str = SIGNAL_FEATURE_RELATION
    forward_outcomes: str = FORWARD_OUTCOME_RELATION


def create_event_time_price_relations(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: Sequence[int],
) -> tuple[EventTimePriceRelations, EventTimePriceAudit]:
    """Build the generic projection under Technical Fit's legacy namespace."""

    built = build_daily_ranking_event_time_prices(
        conn,
        DailyRankingPriceRequest(
            namespace="ranking_technical_fit",
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            horizons=tuple(int(value) for value in horizons),
        ),
    )
    return (
        EventTimePriceRelations(
            signal_features=built.signal_features,
            forward_outcomes=built.forward_outcomes,
        ),
        built.lineage,
    )
