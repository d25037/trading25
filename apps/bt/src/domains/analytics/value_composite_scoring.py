"""Production-safe value-composite scoring helpers.

This module intentionally contains only snapshot/panel scoring primitives used
by API-backed ranking surfaces. Research runners, bundle I/O, and experiment
readout code live in the annual value-composite research modules.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import math

import numpy as np
import pandas as pd

VALUE_COMPOSITE_SCORE_COLUMN = "value_composite_score"
FIXED_VALUE_COMPOSITE_SCORE_COLUMN = "fixed_55_25_20_score"
VALUE_COMPOSITE_CORE_SCORE_COLUMNS: tuple[str, ...] = (
    "low_pbr_score",
    "small_market_cap_score",
    "low_forward_per_score",
)
FIXED_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    "small_market_cap_score": 0.55,
    "low_pbr_score": 0.25,
    "low_forward_per_score": 0.20,
}
STANDARD_PBR_TILT_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    "small_market_cap_score": 0.35,
    "low_pbr_score": 0.40,
    "low_forward_per_score": 0.25,
}
PRIME_SIZE_TILT_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    "small_market_cap_score": 0.465,
    "low_pbr_score": 0.05,
    "low_forward_per_score": 0.485,
}
EQUAL_VALUE_COMPOSITE_WEIGHTS: dict[str, float] = {
    column: 1.0 for column in VALUE_COMPOSITE_CORE_SCORE_COLUMNS
}
VALUE_COMPOSITE_REQUIRED_POSITIVE_COLUMNS: tuple[str, ...] = ("pbr", "forward_per")


def normalize_required_positive_columns(columns: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    supported_columns = {"pbr", "forward_per"}
    for raw_column in columns:
        column = str(raw_column).strip()
        if not column:
            raise ValueError("required positive columns must not be empty")
        if column not in supported_columns:
            raise ValueError(f"Unsupported required positive column: {column}")
        if column not in normalized:
            normalized.append(column)
    return tuple(normalized)


def _normalize_score_group_columns(columns: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_column in columns:
        column = str(raw_column).strip()
        if not column:
            raise ValueError("score group columns must not be empty")
        if column not in normalized:
            normalized.append(column)
    return tuple(normalized)


def _score_factor_within_groups(
    frame: pd.DataFrame,
    source_column: str,
    *,
    group_columns: Sequence[str],
    prefer_low: bool,
) -> pd.Series:
    values = pd.to_numeric(frame[source_column], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    normalized_groups = _normalize_score_group_columns(group_columns)
    groups = (
        frame.groupby(list(normalized_groups), dropna=False, sort=False)
        if normalized_groups
        else ((None, frame),)
    )
    for _, group in groups:
        valid = values.loc[group.index].dropna()
        count = len(valid)
        if count == 0:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        if prefer_low:
            ranked = 1.0 - ranked
        scores.loc[ranked.index] = ranked.astype(float)
    return scores


def build_value_composite_score_frame(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str] = ("year", "market"),
    required_positive_columns: Sequence[str] = (),
    score_column: str = FIXED_VALUE_COMPOSITE_SCORE_COLUMN,
    weights: Mapping[str, float] = FIXED_VALUE_COMPOSITE_WEIGHTS,
) -> pd.DataFrame:
    """Apply the value-composite factor scores to a panel or snapshot."""

    result = frame.copy()
    for column in ("pbr", "forward_per", "market_cap_bil_jpy", *group_columns):
        if column not in result.columns:
            result[column] = np.nan

    normalized_positive_columns = normalize_required_positive_columns(required_positive_columns)
    for column in normalized_positive_columns:
        result = result[pd.to_numeric(result[column], errors="coerce") > 0].copy()

    for score_name, source_column in (
        ("low_pbr_score", "pbr"),
        ("small_market_cap_score", "market_cap_bil_jpy"),
        ("low_forward_per_score", "forward_per"),
    ):
        result[score_name] = _score_factor_within_groups(
            result,
            source_column,
            group_columns=group_columns,
            prefer_low=True,
        )

    normalized_weights = {str(column): float(weight) for column, weight in weights.items()}
    missing_weight_columns = sorted(
        set(normalized_weights) - set(VALUE_COMPOSITE_CORE_SCORE_COLUMNS)
    )
    if missing_weight_columns:
        raise ValueError(f"Unsupported value composite score column(s): {missing_weight_columns}")
    weight_sum = sum(normalized_weights.values())
    if not math.isfinite(weight_sum) or weight_sum <= 0:
        raise ValueError("value composite weights must sum to a positive finite value")

    composite = pd.Series(0.0, index=result.index, dtype="float64")
    for column, raw_weight in normalized_weights.items():
        composite = composite + pd.to_numeric(result[column], errors="coerce") * (raw_weight / weight_sum)
    missing = result[list(normalized_weights)].apply(pd.to_numeric, errors="coerce").isna().any(axis=1)
    composite.loc[missing] = np.nan
    result[score_column] = composite
    return result.reset_index(drop=True)
