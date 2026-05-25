"""Valuation enrichment helpers for market rankings."""

from __future__ import annotations

import math

import pandas as pd

from src.application.services.ranking_query_helpers import canonical_market_label
from src.application.services.ranking_value_composite_config import (
    PRIME_VALUATION_PERCENTILE_COLUMNS,
)


def with_prime_valuation_percentiles(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    for _, percentile_column in PRIME_VALUATION_PERCENTILE_COLUMNS:
        result[percentile_column] = None
    if "market_code" not in result.columns:
        return result

    prime_mask = result["market_code"].map(
        lambda value: canonical_market_label(str(value)) == "prime"
    )
    if not bool(prime_mask.any()):
        return result

    for value_column, percentile_column in PRIME_VALUATION_PERCENTILE_COLUMNS:
        if value_column not in result.columns:
            continue
        values = pd.to_numeric(result.loc[prime_mask, value_column], errors="coerce")
        valid_mask = values.map(
            lambda value: pd.notna(value)
            and math.isfinite(float(value))
            and float(value) > 0
        )
        valid_values = values[valid_mask]
        if valid_values.empty:
            continue
        if len(valid_values) == 1:
            percentiles = pd.Series(0.0, index=valid_values.index)
        else:
            percentiles = (valid_values.rank(method="min") - 1.0) / (
                len(valid_values) - 1.0
            )
        result.loc[percentiles.index, percentile_column] = percentiles.astype(float)
    return result
