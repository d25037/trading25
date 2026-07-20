"""Technical Fit compatibility exports for generic event-time price relations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from src.domains.analytics.daily_ranking_event_time_prices import (
    DAILY_RANKING_SIGNAL_FEATURE_COLUMNS,
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

    prior_generation_tables = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE starts_with(table_name, 'ranking_technical_fit_g_')
              AND (
                  ends_with(table_name, '_signal_price_features')
                  OR ends_with(table_name, '_forward_price_outcomes')
                  OR ends_with(table_name, '_price_history')
              )
            """
        ).fetchall()
    }
    prior_price_history_tables = {
        name for name in prior_generation_tables if name.endswith("_price_history")
    }
    try:
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
    except Exception:
        for relation_name in prior_price_history_tables:
            conn.execute(f"DROP TABLE IF EXISTS {relation_name}")
        raise
    signal_columns = ", ".join(DAILY_RANKING_SIGNAL_FEATURE_COLUMNS)
    legacy_outcome_columns = ", ".join(
        column
        for horizon in tuple(sorted({int(value) for value in horizons}))
        for column in (
            f"forward_outcome_completion_date_{horizon}d",
            f"forward_close_return_{horizon}d_pct",
            f"forward_close_excess_return_{horizon}d_pct",
            f"completion_basis_id_{horizon}d",
        )
    )
    transaction_started = False
    try:
        conn.execute("BEGIN TRANSACTION")
        transaction_started = True
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW {SIGNAL_FEATURE_RELATION} AS
            SELECT {signal_columns}
            FROM {built.signal_features}
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW {FORWARD_OUTCOME_RELATION} AS
            SELECT code, date, {legacy_outcome_columns}
            FROM {built.forward_outcomes}
            """
        )
        conn.execute("COMMIT")
        transaction_started = False
    except Exception:
        if transaction_started:
            conn.execute("ROLLBACK")
        conn.execute(f"DROP TABLE IF EXISTS {built.signal_features}")
        conn.execute(f"DROP TABLE IF EXISTS {built.forward_outcomes}")
        conn.execute(f"DROP TABLE IF EXISTS {built.price_history}")
        for relation_name in prior_price_history_tables:
            conn.execute(f"DROP TABLE IF EXISTS {relation_name}")
        raise
    for relation_name in prior_generation_tables:
        conn.execute(f"DROP TABLE IF EXISTS {relation_name}")
    conn.execute(f"DROP TABLE IF EXISTS {built.price_history}")
    return (
        EventTimePriceRelations(),
        built.lineage,
    )
