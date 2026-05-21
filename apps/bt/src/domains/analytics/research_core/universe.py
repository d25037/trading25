"""Trading25 research universe primitives.

These helpers are intentionally repo-local. They encode the market-behavior
research universe used by runner-first studies, not a generic screening DSL.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.shared.utils.market_code_alias import expand_market_codes

PRIME_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["prime"]))
STANDARD_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["standard"]))
GROWTH_MARKET_CODES: tuple[str, ...] = tuple(expand_market_codes(["growth"]))
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
UNIVERSE_ORDER: tuple[str, ...] = (
    "topix500",
    "prime_ex_topix500",
    "standard",
    "growth",
)
UNIVERSE_LABELS: dict[str, str] = {
    "topix500": "TOPIX500",
    "prime_ex_topix500": "Prime ex TOPIX500",
    "standard": "Standard",
    "growth": "Growth",
}


def sql_string_list(values: Iterable[str]) -> str:
    """Return a quoted SQL literal list for static repo-owned identifiers."""
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)


def research_universe_market_codes() -> tuple[str, ...]:
    return tuple(dict.fromkeys([*PRIME_MARKET_CODES, *STANDARD_MARKET_CODES, *GROWTH_MARKET_CODES]))


def build_market_universe_case_sql(
    *,
    market_code_column: str,
    scale_category_column: str,
) -> str:
    return f"""
                CASE
                    WHEN {scale_category_column} IN ({sql_string_list(TOPIX500_SCALE_CATEGORIES)})
                        THEN 'topix500'
                    WHEN {market_code_column} IN ({sql_string_list(PRIME_MARKET_CODES)})
                        THEN 'prime_ex_topix500'
                    WHEN {market_code_column} IN ({sql_string_list(STANDARD_MARKET_CODES)})
                        THEN 'standard'
                    WHEN {market_code_column} IN ({sql_string_list(GROWTH_MARKET_CODES)})
                        THEN 'growth'
                END
    """.strip()
