"""Shared validation helpers for TOPIX100 streak LightGBM studies."""

from __future__ import annotations

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import DECILE_ORDER

DEFAULT_TOPIX100_STREAK_LIGHTGBM_TOP_K_VALUES: tuple[int, ...] = (1, 3, 5, 10, 20)


def build_topix100_streak_baseline_selector_value_key(
    selector_kind: str,
    values: dict[str, str],
) -> str:
    if selector_kind == "universe":
        return "universe"
    if selector_kind == "bucket":
        return values["bucket"]
    raise ValueError(f"Unsupported selector kind: {selector_kind}")


def build_topix100_streak_validation_score_decile_df(
    validation_prediction_df: pd.DataFrame,
) -> pd.DataFrame:
    ranked_df = validation_prediction_df.copy()
    ranked_df["date_constituent_count"] = ranked_df.groupby(
        ["model_name", "date"],
        observed=True,
    )["code"].transform("size")
    ranked_df["score_rank_desc"] = ranked_df.groupby(
        ["model_name", "date"],
        observed=True,
    )["score"].rank(method="first", ascending=False)
    ranked_df["score_decile_index"] = (
        ((ranked_df["score_rank_desc"] - 1) * len(DECILE_ORDER))
        // ranked_df["date_constituent_count"]
    ) + 1
    ranked_df["score_decile_index"] = ranked_df["score_decile_index"].clip(
        1, len(DECILE_ORDER)
    )
    ranked_df["score_decile"] = ranked_df["score_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    return (
        ranked_df.groupby(
            ["model_name", "score_decile_index", "score_decile"],
            observed=True,
            sort=False,
        )
        .agg(
            mean_realized_return=("realized_return", "mean"),
            stock_count=("code", "count"),
            date_count=("date", "nunique"),
        )
        .reset_index()
        .sort_values(["model_name", "score_decile_index"], kind="stable")
        .reset_index(drop=True)
    )
