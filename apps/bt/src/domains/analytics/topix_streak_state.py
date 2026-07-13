"""Neutral streak-state construction for retrospective analytics."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

MODE_ORDER: tuple[str, ...] = ("bullish", "bearish")
MULTI_TIMEFRAME_STATE_ORDER: tuple[str, ...] = (
    "long_bullish__short_bullish",
    "long_bullish__short_bearish",
    "long_bearish__short_bullish",
    "long_bearish__short_bearish",
)


def prepare_streak_candle_frame(
    streak_segment_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
    validation_ratio: float,
) -> pd.DataFrame:
    max_window = max(candidate_windows)
    required_columns = [f"future_return_{horizon}d" for horizon in future_horizons]
    complete_df = (
        streak_segment_df[streak_segment_df["is_complete"]]
        .copy()
        .sort_values("segment_id", kind="stable")
        .reset_index(drop=True)
    )
    complete_df["analysis_eligible"] = False
    complete_df["sample_split"] = "excluded"
    eligible_mask = complete_df[required_columns].notna().all(axis=1)
    eligible_index = complete_df.index[
        eligible_mask & (complete_df.index >= (max_window - 1))
    ]
    if len(eligible_index) == 0:
        raise ValueError("No comparable streak candles remained after warmup/horizon trimming")

    split_labels = build_sample_split_labels(
        len(eligible_index),
        validation_ratio=validation_ratio,
    )
    complete_df.loc[eligible_index, "analysis_eligible"] = True
    complete_df.loc[eligible_index, "sample_split"] = split_labels
    return complete_df


def build_sample_split_labels(
    sample_count: int,
    *,
    validation_ratio: float,
) -> list[str]:
    if sample_count <= 0:
        raise ValueError("sample_count must be positive")

    validation_count = int(round(sample_count * validation_ratio))
    if validation_ratio > 0.0 and validation_count == 0 and sample_count >= 2:
        validation_count = 1
    if validation_count >= sample_count and sample_count >= 2:
        validation_count = sample_count - 1
    discovery_count = sample_count - validation_count
    if discovery_count <= 0:
        raise ValueError("discovery split would be empty; reduce validation_ratio")
    return (["discovery"] * discovery_count) + (["validation"] * validation_count)


def build_mode_assignments_df(
    prepared_streak_df: pd.DataFrame,
    *,
    candidate_windows: Sequence[int],
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    eligible_indices = prepared_streak_df.index[
        prepared_streak_df["analysis_eligible"]
    ].to_list()
    if not eligible_indices:
        raise ValueError("No comparable streak candles were marked as analysis eligible")

    assignment_rows: list[dict[str, Any]] = []
    for window_streaks in candidate_windows:
        for segment_index in eligible_indices:
            window_start = segment_index - window_streaks + 1
            if window_start < 0:
                continue
            window_df = prepared_streak_df.iloc[window_start : segment_index + 1]
            dominant_position = int(window_df["segment_return"].abs().argmax())
            dominant_row = window_df.iloc[dominant_position]
            current_row = prepared_streak_df.iloc[segment_index]
            row = {
                "segment_id": int(current_row["segment_id"]),
                "sample_split": str(current_row["sample_split"]),
                "window_streaks": int(window_streaks),
                "segment_start_date": str(current_row["start_date"]),
                "segment_end_date": str(current_row["end_date"]),
                "synthetic_open": float(current_row["synthetic_open"]),
                "synthetic_close": float(current_row["synthetic_close"]),
                "segment_return": float(current_row["segment_return"]),
                "segment_day_count": int(current_row["segment_day_count"]),
                "base_streak_mode": str(current_row["mode"]),
                "mode": "bullish"
                if float(dominant_row["segment_return"]) >= 0.0
                else "bearish",
                "dominant_segment_id": int(dominant_row["segment_id"]),
                "dominant_segment_return": float(dominant_row["segment_return"]),
                "dominant_abs_segment_return": float(
                    abs(dominant_row["segment_return"])
                ),
                "dominant_segment_start_date": str(dominant_row["start_date"]),
                "dominant_segment_end_date": str(dominant_row["end_date"]),
                "dominant_segment_day_count": int(dominant_row["segment_day_count"]),
            }
            for horizon in future_horizons:
                row[f"future_return_{horizon}d"] = float(
                    current_row[f"future_return_{horizon}d"]
                )
                row[f"future_diff_{horizon}d"] = float(
                    current_row[f"future_diff_{horizon}d"]
                )
            assignment_rows.append(row)

    mode_assignments_df = pd.DataFrame(assignment_rows)
    if mode_assignments_df.empty:
        raise ValueError("Failed to build any streak extreme mode assignments")
    mode_assignments_df["mode"] = pd.Categorical(
        mode_assignments_df["mode"],
        categories=list(MODE_ORDER),
        ordered=True,
    )
    return mode_assignments_df


def build_multi_timeframe_state_streak_df(
    mode_assignments_df: pd.DataFrame,
    *,
    short_window_streaks: int,
    long_window_streaks: int,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    short_df = (
        mode_assignments_df[mode_assignments_df["window_streaks"] == short_window_streaks]
        .copy()
        .rename(
            columns={
                "mode": "short_mode",
                "dominant_segment_return": "short_dominant_segment_return",
                "dominant_abs_segment_return": "short_dominant_abs_segment_return",
                "dominant_segment_start_date": "short_dominant_segment_start_date",
                "dominant_segment_end_date": "short_dominant_segment_end_date",
                "dominant_segment_day_count": "short_dominant_segment_day_count",
            }
        )
    )
    long_df = (
        mode_assignments_df[mode_assignments_df["window_streaks"] == long_window_streaks]
        .copy()
        .rename(
            columns={
                "mode": "long_mode",
                "dominant_segment_return": "long_dominant_segment_return",
                "dominant_abs_segment_return": "long_dominant_abs_segment_return",
                "dominant_segment_start_date": "long_dominant_segment_start_date",
                "dominant_segment_end_date": "long_dominant_segment_end_date",
                "dominant_segment_day_count": "long_dominant_segment_day_count",
            }
        )
    )
    if short_df.empty or long_df.empty:
        raise ValueError("Short/long streak rows were not found in mode_assignments_df")

    short_columns = [
        "segment_id",
        "sample_split",
        "segment_start_date",
        "segment_end_date",
        "synthetic_open",
        "synthetic_close",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
        "short_mode",
        "short_dominant_segment_return",
        "short_dominant_abs_segment_return",
        "short_dominant_segment_start_date",
        "short_dominant_segment_end_date",
        "short_dominant_segment_day_count",
    ]
    short_columns.extend(f"future_return_{horizon}d" for horizon in future_horizons)
    short_columns.extend(f"future_diff_{horizon}d" for horizon in future_horizons)
    long_columns = [
        "segment_id",
        "sample_split",
        "long_mode",
        "long_dominant_segment_return",
        "long_dominant_abs_segment_return",
        "long_dominant_segment_start_date",
        "long_dominant_segment_end_date",
        "long_dominant_segment_day_count",
    ]
    merged_df = short_df[short_columns].merge(
        long_df[long_columns],
        on=["segment_id", "sample_split"],
        how="inner",
        validate="one_to_one",
    )
    merged_df["short_window_streaks"] = short_window_streaks
    merged_df["long_window_streaks"] = long_window_streaks
    merged_df["state_key"] = merged_df.apply(
        lambda row: _build_multi_timeframe_state_key(
            long_mode=str(row["long_mode"]),
            short_mode=str(row["short_mode"]),
        ),
        axis=1,
    )
    merged_df["state_label"] = merged_df["state_key"].map(
        _format_multi_timeframe_state_label
    )
    merged_df["state_key"] = pd.Categorical(
        merged_df["state_key"],
        categories=list(MULTI_TIMEFRAME_STATE_ORDER),
        ordered=True,
    )
    return merged_df.sort_values("segment_id", kind="stable").reset_index(drop=True)


def _build_multi_timeframe_state_key(*, long_mode: str, short_mode: str) -> str:
    return f"long_{long_mode}__short_{short_mode}"


def _format_multi_timeframe_state_label(state_key: str) -> str:
    return state_key.replace("long_", "Long ").replace(
        "__short_", " / Short "
    ).replace("_", " ").title()
