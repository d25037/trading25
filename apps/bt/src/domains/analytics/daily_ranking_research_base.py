"""Reusable Daily Ranking research-state panel builders.

This module is the public research base for Daily Ranking parameter studies.
It exposes the production Ranking state used by existing readouts while keeping
the legacy temp-table names available for older runners.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from src.domains.analytics.ranking_color_evidence import (
    _assert_required_tables,
    _create_observation_panel,
    _normalize_market_scopes,
    _offset_calendar_date,
)

DAILY_RANKING_RESEARCH_PANEL_TABLE = "daily_ranking_research_panel"
DAILY_RANKING_RESEARCH_RANKED_TABLE = "daily_ranking_research_ranked"
DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE = (
    "daily_ranking_research_liquidity_ranked"
)
DAILY_RANKING_RESEARCH_SCOPED_TABLE = "daily_ranking_research_scoped"
DAILY_RANKING_RESEARCH_RELATIONS_TABLE = "daily_ranking_research_relations"


@dataclass(frozen=True)
class DailyRankingResearchPanelSpec:
    """Created temp-table names and normalized query parameters."""

    panel_table: str
    ranked_table: str
    liquidity_ranked_table: str
    scoped_table: str
    relations_table: str
    legacy_panel_table: str
    legacy_ranked_table: str
    legacy_liquidity_ranked_table: str
    market_source: str
    market_scopes: tuple[str, ...]
    horizons: tuple[int, ...]
    query_start: str | None
    query_end: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    include_relation_percentiles: bool
    event_time_basis_only: bool


def normalize_daily_ranking_market_scopes(
    market_scopes: Sequence[str],
) -> tuple[str, ...]:
    """Normalize Ranking research market scopes using the production aliases."""

    return _normalize_market_scopes(market_scopes)


def daily_ranking_query_start_date(
    start_date: str | None,
    *,
    warmup_calendar_days: int = 720,
) -> str | None:
    """Resolve the lower query bound needed for lagged Ranking state features."""

    return _offset_calendar_date(start_date, days=-int(warmup_calendar_days))


def daily_ranking_query_end_date(
    end_date: str | None,
    *,
    max_horizon: int,
) -> str | None:
    """Resolve the upper query bound needed for forward return horizons."""

    return _offset_calendar_date(end_date, days=int(max_horizon) * 4 + 30)


def assert_daily_ranking_research_tables(conn: Any) -> None:
    """Assert the baseline tables required for the Daily Ranking state panel."""

    _assert_required_tables(conn)


def create_daily_ranking_research_panel(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: Sequence[int],
    market_scopes: Sequence[str],
    market_source: str = "stock_master_daily_exact_date",
    include_liquidity_ranked: bool = True,
    include_relation_percentiles: bool = True,
    event_time_basis_only: bool = False,
    price_feature_relation: str | None = None,
    price_outcome_relation: str | None = None,
) -> DailyRankingResearchPanelSpec:
    """Create the reusable Daily Ranking research panel temp tables.

    The generated public temp views are:

    - `daily_ranking_research_panel`
    - `daily_ranking_research_relations`
    - `daily_ranking_research_scoped`
    - `daily_ranking_research_ranked`
    - `daily_ranking_research_liquidity_ranked`

    Existing legacy temp tables from `ranking_color_evidence` are also created
    for compatibility with older research modules.
    """

    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    if not resolved_horizons or any(horizon <= 0 for horizon in resolved_horizons):
        raise ValueError("horizons must contain positive integers")
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)

    _create_observation_panel(
        conn,
        query_start=query_start,
        query_end=query_end,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        horizons=resolved_horizons,
        market_source=market_source,
        market_scopes=resolved_market_scopes,
        include_liquidity_ranked=include_liquidity_ranked,
        include_relation_percentiles=include_relation_percentiles,
        event_time_basis_only=event_time_basis_only,
        price_feature_relation=price_feature_relation,
        price_outcome_relation=price_outcome_relation,
    )
    _create_public_aliases(conn, include_liquidity_ranked=include_liquidity_ranked)

    return DailyRankingResearchPanelSpec(
        panel_table=DAILY_RANKING_RESEARCH_PANEL_TABLE,
        ranked_table=DAILY_RANKING_RESEARCH_RANKED_TABLE,
        liquidity_ranked_table=DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
        scoped_table=DAILY_RANKING_RESEARCH_SCOPED_TABLE,
        relations_table=DAILY_RANKING_RESEARCH_RELATIONS_TABLE,
        legacy_panel_table="ranking_color_panel",
        legacy_ranked_table="ranking_color_ranked",
        legacy_liquidity_ranked_table="ranking_color_liquidity_ranked",
        market_source=market_source,
        market_scopes=resolved_market_scopes,
        horizons=resolved_horizons,
        query_start=query_start,
        query_end=query_end,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        include_relation_percentiles=include_relation_percentiles,
        event_time_basis_only=event_time_basis_only,
    )


def _create_public_aliases(conn: Any, *, include_liquidity_ranked: bool) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_PANEL_TABLE} AS
        SELECT * FROM ranking_color_panel
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_RELATIONS_TABLE} AS
        SELECT * FROM ranking_color_panel_relations
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_SCOPED_TABLE} AS
        SELECT * FROM ranking_color_scoped
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_RANKED_TABLE} AS
        SELECT
            *,
            {_valuation_signal_select_sql()}
        FROM ranking_color_ranked
        """
    )
    if include_liquidity_ranked:
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE} AS
            SELECT
                *,
                {_valuation_signal_select_sql()}
            FROM ranking_color_liquidity_ranked
            """
        )


def _valuation_signal_select_sql() -> str:
    strong_value = _strong_value_confirmation_sql()
    medium_value = _medium_value_confirmation_sql()
    very_overvalued = _very_overvalued_warning_sql()
    overvalued = _overvalued_warning_sql()
    no_positive_earnings = _no_positive_earnings_valuation_sql()
    return f"""
            ({strong_value}) AS strong_value_confirmation,
            ({medium_value}) AS medium_value_confirmation,
            ({overvalued}) AS overvalued_warning,
            ({very_overvalued}) AS very_overvalued_warning,
            ({no_positive_earnings}) AS no_positive_earnings_valuation,
            (NOT ({medium_value})) AS no_value_confirmation,
            CASE
                WHEN ({strong_value}) THEN 'strong_value_confirmation'
                WHEN ({very_overvalued}) THEN 'very_overvalued_warning'
                WHEN ({overvalued}) THEN 'overvalued_warning'
                WHEN ({no_positive_earnings}) THEN 'no_positive_earnings_valuation'
                WHEN ({medium_value}) THEN 'medium_value_confirmation'
            END AS valuation_signal
    """


def _strong_value_confirmation_sql() -> str:
    return """
        coalesce(
            (pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2)
            OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8),
            FALSE
        )
    """


def _medium_value_confirmation_sql() -> str:
    return """
        coalesce(
            pbr_percentile <= 0.2
            OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0),
            FALSE
        )
    """


def _overvalued_warning_sql() -> str:
    return """
        coalesce(
            per_percentile >= 0.8
            OR forward_per_percentile >= 0.8
            OR forward_p_op_percentile >= 0.8
            OR pbr_percentile >= 0.8,
            FALSE
        )
    """


def _very_overvalued_warning_sql() -> str:
    return """
        coalesce(
            per_percentile >= 0.9
            OR forward_per_percentile >= 0.9
            OR forward_p_op_percentile >= 0.9
            OR pbr_percentile >= 0.9,
            FALSE
        )
    """


def _no_positive_earnings_valuation_sql() -> str:
    return "per_percentile IS NULL AND forward_per_percentile IS NULL"
