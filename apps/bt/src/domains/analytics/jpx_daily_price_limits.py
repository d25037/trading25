"""JPX standard daily price-limit reference shared across analytics studies."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL = (
    "JPX Daily Price Limits page (updated Apr. 17, 2026)"
)
JPX_DAILY_PRICE_LIMITS_DEFINITION_NOTE = (
    "The standard JPX/TSE daily price-limit width is determined from the previous "
    "session close. Research that uses daily OHLC only can classify exact hits "
    "inside that standard band, but broadened-limit and special-quote cases "
    "cannot always be disambiguated exactly without intraday quote context."
)

# Standard TSE daily price limits. The final ``None`` means "or more".
STANDARD_DAILY_LIMIT_BANDS: tuple[tuple[int | None, int], ...] = (
    (100, 30),
    (200, 50),
    (500, 80),
    (700, 100),
    (1_000, 150),
    (1_500, 300),
    (2_000, 400),
    (3_000, 500),
    (5_000, 700),
    (7_000, 1_000),
    (10_000, 1_500),
    (15_000, 3_000),
    (20_000, 4_000),
    (30_000, 5_000),
    (50_000, 7_000),
    (70_000, 10_000),
    (100_000, 15_000),
    (150_000, 30_000),
    (200_000, 40_000),
    (300_000, 50_000),
    (500_000, 70_000),
    (700_000, 100_000),
    (1_000_000, 150_000),
    (1_500_000, 300_000),
    (2_000_000, 400_000),
    (3_000_000, 500_000),
    (5_000_000, 700_000),
    (7_000_000, 1_000_000),
    (10_000_000, 1_500_000),
    (15_000_000, 3_000_000),
    (20_000_000, 4_000_000),
    (30_000_000, 5_000_000),
    (50_000_000, 7_000_000),
    (None, 10_000_000),
)


def build_standard_daily_limit_width_case_sql(base_price_sql: str) -> str:
    conditions: list[str] = [
        f"WHEN {base_price_sql} IS NULL OR {base_price_sql} <= 0 THEN NULL"
    ]
    for upper_bound, limit_width in STANDARD_DAILY_LIMIT_BANDS:
        if upper_bound is None:
            conditions.append(f"ELSE {limit_width}")
        else:
            conditions.append(f"WHEN {base_price_sql} < {upper_bound} THEN {limit_width}")
    joined = "\n                ".join(conditions)
    return f"CASE\n                {joined}\n            END"


def resolve_standard_daily_limit_width(base_price: float | int | None) -> float | None:
    if base_price is None:
        return None
    numeric_base = float(base_price)
    if not math.isfinite(numeric_base) or numeric_base <= 0:
        return None
    for upper_bound, limit_width in STANDARD_DAILY_LIMIT_BANDS:
        if upper_bound is None or numeric_base < upper_bound:
            return float(limit_width)
    return None


def build_standard_daily_limit_table_df() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    previous_upper_bound: int | None = None
    for upper_bound, limit_width in STANDARD_DAILY_LIMIT_BANDS:
        if previous_upper_bound is None:
            base_price_rule = (
                f"base < {upper_bound:,}" if upper_bound is not None else "base >= 0"
            )
        elif upper_bound is None:
            base_price_rule = f"{previous_upper_bound:,} <= base"
        else:
            base_price_rule = f"{previous_upper_bound:,} <= base < {upper_bound:,}"
        rows.append(
            {
                "base_price_rule": base_price_rule,
                "daily_limit_width": limit_width,
            }
        )
        previous_upper_bound = upper_bound
    return pd.DataFrame(rows)
