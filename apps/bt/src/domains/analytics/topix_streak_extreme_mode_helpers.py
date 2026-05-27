"""Small helpers for TOPIX streak extreme mode summaries."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def lookup_mode_forward_row(
    summary_df: pd.DataFrame,
    *,
    mode: str,
) -> pd.Series | None:
    mode_df = summary_df[summary_df["mode"] == mode]
    if mode_df.empty:
        return None
    return mode_df.iloc[0]


def format_int_sequence(values: Sequence[int]) -> str:
    if not values:
        return ""
    if len(values) > 10:
        return f"{values[0]}..{values[-1]} ({len(values)} values)"
    return ",".join(str(value) for value in values)


def format_return(value: float) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{value * 100:+.2f}%"


def select_best_window_streaks(window_score_df: pd.DataFrame) -> int:
    candidates_df = window_score_df[
        (window_score_df["sample_split"] == "discovery")
        & window_score_df["selection_eligible"]
        & window_score_df["selection_score"].notna()
    ].copy()
    if candidates_df.empty:
        raise ValueError("No eligible discovery windows were available for selection")
    candidates_df = candidates_df.sort_values(
        [
            "selection_score",
            "directional_consistency",
            "balance_ratio",
            "window_streaks",
        ],
        ascending=[False, False, False, True],
        kind="stable",
    )
    return int(candidates_df.iloc[0]["window_streaks"])
