"""Stable table ordering helpers for internal analytics research outputs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

from src.domains.analytics.research_core.universe import UNIVERSE_ORDER


def sort_research_table(
    df: pd.DataFrame,
    *,
    sort_columns: Sequence[str],
    universe_column: str = "universe_key",
    universe_order: Sequence[str] = UNIVERSE_ORDER,
    extra_order_columns: Mapping[str, Mapping[object, int]] | None = None,
) -> pd.DataFrame:
    """Sort research output tables without leaking temporary ordering columns."""
    if df.empty:
        return df
    result = df.copy()
    effective_sort_columns: list[str] = []
    temporary_columns: list[str] = []
    if universe_column in result.columns:
        order_column = f"_{universe_column}_order"
        result[order_column] = result[universe_column].map(
            {key: index for index, key in enumerate(universe_order)}
        )
        temporary_columns.append(order_column)
        effective_sort_columns.append(order_column)
    for column, order_map in (extra_order_columns or {}).items():
        if column not in result.columns:
            continue
        order_column = f"_{column}_order"
        result[order_column] = result[column].map(order_map)
        temporary_columns.append(order_column)
        effective_sort_columns.append(order_column)
    effective_sort_columns.extend(column for column in sort_columns if column in result.columns)
    if effective_sort_columns:
        result = result.sort_values(effective_sort_columns, kind="stable").reset_index(drop=True)
    return result.drop(columns=temporary_columns)
