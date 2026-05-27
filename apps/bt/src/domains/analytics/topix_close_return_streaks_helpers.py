"""Small helpers for TOPIX close-return streak summaries."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd


def select_mode_bucket_summary(
    summary_df: pd.DataFrame,
    *,
    mode: str,
    bucket_column: str,
) -> pd.Series | None:
    mode_df = summary_df[summary_df["mode"] == mode].copy()
    if mode_df.empty:
        return None
    mode_df = mode_df.sort_values([bucket_column], ascending=[True], kind="stable")
    return mode_df.iloc[0]


def select_extreme_future_row(
    summary_df: pd.DataFrame,
    *,
    mode: str,
    largest: bool,
) -> pd.Series | None:
    mode_df = summary_df[summary_df["mode"] == mode].copy()
    if mode_df.empty:
        return None
    mode_df = mode_df.sort_values(
        ["mean_future_return", "sample_count", "segment_length_bucket"],
        ascending=[not largest, False, True],
        kind="stable",
    )
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
