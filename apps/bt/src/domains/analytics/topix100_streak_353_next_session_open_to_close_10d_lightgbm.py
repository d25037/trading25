"""
Feature-panel helper for TOPIX100 streak 3/53 next-session open-to-close 10D research.

This keeps the leak-free point-in-time snapshot discipline:

- features are built using information available up to day X
- entry is at X+1 open
- exit is at X+10 close
- target is X+10 close / X+1 open - 1
"""

from __future__ import annotations

import pandas as pd

from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    _build_price_feature_frame,
    _coerce_signal_state_panel_df,
)

TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_CLOSE_10D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-open-to-close-10d-lightgbm-walkforward"
)


def build_feature_panel_from_state_event_df(
    *,
    event_panel_df: pd.DataFrame,
    state_event_df: pd.DataFrame,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    if event_panel_df.empty or state_event_df.empty:
        raise ValueError("Base price/state inputs were empty")

    price_df = _build_price_feature_frame(
        event_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    price_df = price_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    grouped = price_df.groupby("code", observed=True, sort=False)
    price_df["swing_entry_date"] = grouped["date"].shift(-1)
    price_df["swing_entry_open"] = grouped["open"].shift(-1).astype(float)
    price_df["swing_exit_date"] = grouped["date"].shift(-10)
    price_df["swing_exit_close"] = grouped["close"].shift(-10).astype(float)
    price_df["next_session_open_to_close_10d_return"] = price_df["swing_exit_close"].div(
        price_df["swing_entry_open"].replace(0, pd.NA)
    ).sub(1.0)

    state_df = _coerce_signal_state_panel_df(state_event_df)
    state_columns = [
        "state_event_id",
        "code",
        "company_name",
        "sample_split",
        "segment_id",
        "date",
        "segment_return",
        "segment_day_count",
        "base_streak_mode",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
    ]
    missing_state_columns = [
        column for column in state_columns if column not in state_df.columns
    ]
    if missing_state_columns:
        raise ValueError(f"Missing state event columns: {missing_state_columns}")

    state_df = state_df[state_columns].copy()
    state_df["date"] = state_df["date"].astype(str)
    state_df["code"] = state_df["code"].astype(str).str.zfill(4)

    merged_df = price_df.merge(
        state_df,
        on=["date", "code", "company_name"],
        how="inner",
        validate="one_to_one",
    )
    merged_df = merged_df.dropna(subset=["next_session_open_to_close_10d_return"]).copy()
    if merged_df.empty:
        raise ValueError("No swing targets remained after joining price and state rows")

    merged_df["segment_abs_return"] = merged_df["segment_return"].astype(float).abs()
    ordered_columns = [
        "date",
        "code",
        "company_name",
        "sample_split",
        "state_event_id",
        "segment_id",
        "decile_num",
        "decile",
        "volume_bucket",
        "short_mode",
        "long_mode",
        "state_key",
        "state_label",
        price_feature,
        volume_feature,
        "recent_return_1d",
        "recent_return_3d",
        "recent_return_5d",
        "intraday_return",
        "range_pct",
        "segment_return",
        "segment_abs_return",
        "segment_day_count",
        "swing_entry_date",
        "swing_exit_date",
        "next_session_open_to_close_10d_return",
    ]
    return merged_df[ordered_columns].copy()
