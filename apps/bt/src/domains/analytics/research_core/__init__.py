"""Thin internal primitives for runner-first analytics research."""

from src.domains.analytics.research_core.parameters import (
    normalize_positive_int_sequence,
    warmup_start_date,
)
from src.domains.analytics.research_core.portfolio import (
    PricePath,
    build_event_portfolio_daily_df,
    build_price_path_lookup,
)
from src.domains.analytics.research_core.tables import sort_research_table
from src.domains.analytics.research_core.universe import (
    GROWTH_MARKET_CODES,
    PRIME_MARKET_CODES,
    STANDARD_MARKET_CODES,
    TOPIX500_SCALE_CATEGORIES,
    UNIVERSE_LABELS,
    UNIVERSE_ORDER,
    build_market_universe_case_sql,
    research_universe_market_codes,
    sql_string_list,
)

__all__ = [
    "GROWTH_MARKET_CODES",
    "PRIME_MARKET_CODES",
    "PricePath",
    "STANDARD_MARKET_CODES",
    "TOPIX500_SCALE_CATEGORIES",
    "UNIVERSE_LABELS",
    "UNIVERSE_ORDER",
    "build_event_portfolio_daily_df",
    "build_market_universe_case_sql",
    "build_price_path_lookup",
    "normalize_positive_int_sequence",
    "research_universe_market_codes",
    "sort_research_table",
    "sql_string_list",
    "warmup_start_date",
]
