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
    )
    _create_public_aliases(conn)

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
    )


def _create_public_aliases(conn: Any) -> None:
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
        SELECT * FROM ranking_color_ranked
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE} AS
        SELECT * FROM ranking_color_liquidity_ranked
        """
    )
