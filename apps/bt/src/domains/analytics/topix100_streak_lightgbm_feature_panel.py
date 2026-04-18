"""Shared feature-panel helpers for TOPIX100 streak LightGBM studies."""

from __future__ import annotations

from typing import Any, Sequence, cast

import pandas as pd

from src.domains.analytics.topix_rank_future_close_core import DECILE_ORDER

STREAK_STATE_PANEL_COLUMNS: tuple[str, ...] = (
    "state_event_id",
    "code",
    "company_name",
    "sample_split",
    "segment_id",
    "date",
    "segment_return",
    "segment_day_count",
)


def coerce_topix100_streak_state_panel_df(state_source: Any) -> pd.DataFrame:
    if isinstance(state_source, pd.DataFrame):
        state_df = state_source.copy()
    elif hasattr(state_source, "daily_state_panel_df") and isinstance(
        getattr(state_source, "daily_state_panel_df"),
        pd.DataFrame,
    ):
        state_df = cast(pd.DataFrame, getattr(state_source, "daily_state_panel_df")).copy()
    elif hasattr(state_source, "state_event_df") and isinstance(
        getattr(state_source, "state_event_df"),
        pd.DataFrame,
    ):
        state_df = cast(pd.DataFrame, getattr(state_source, "state_event_df")).copy()
    else:
        raise ValueError("Unable to resolve a state panel dataframe")

    if "segment_end_date" in state_df.columns and "date" not in state_df.columns:
        state_df = state_df.rename(columns={"segment_end_date": "date"})
    return state_df


def build_topix100_streak_price_feature_frame(
    event_panel_df: pd.DataFrame,
    *,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    required_event_columns = [
        "date",
        "code",
        "company_name",
        "open",
        "high",
        "low",
        "close",
        price_feature,
        volume_feature,
        "date_constituent_count",
    ]
    missing_event_columns = [
        column for column in required_event_columns if column not in event_panel_df.columns
    ]
    if missing_event_columns:
        raise ValueError(f"Missing event panel columns: {missing_event_columns}")

    optional_future_columns = [
        column
        for column in event_panel_df.columns
        if column.startswith("t_plus_") and column.endswith("_return")
    ]
    price_df = event_panel_df[[*required_event_columns, *optional_future_columns]].copy()
    price_df["date"] = price_df["date"].astype(str)
    price_df["code"] = price_df["code"].astype(str).str.zfill(4)
    price_df = price_df.dropna(subset=[price_feature, volume_feature]).copy()
    if price_df.empty:
        raise ValueError("No event rows remained after dropping missing feature values")

    price_df = price_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    close_group = price_df.groupby("code", observed=True)["close"]
    price_df["recent_return_1d"] = close_group.pct_change(1)
    price_df["recent_return_3d"] = close_group.pct_change(3)
    price_df["recent_return_5d"] = close_group.pct_change(5)
    price_df["intraday_return"] = price_df["close"].astype(float).div(
        price_df["open"].replace(0, pd.NA).astype(float)
    ).sub(1.0)
    price_df["range_pct"] = (
        price_df["high"].astype(float).sub(price_df["low"].astype(float))
    ).div(price_df["close"].replace(0, pd.NA).astype(float))
    price_df["price_rank_desc"] = (
        price_df.groupby("date", observed=True)[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    price_df["decile_num"] = (
        ((price_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // price_df["date_constituent_count"]
    ) + 1
    price_df["decile_num"] = price_df["decile_num"].clip(1, len(DECILE_ORDER))
    price_df["decile"] = price_df["decile_num"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    return price_df


def join_topix100_streak_state_panel_df(
    price_df: pd.DataFrame,
    *,
    state_source: Any,
    empty_join_error_message: str = "Joining price rows with state rows produced no overlap",
    dropna_target_columns: Sequence[str] | None = None,
    empty_target_error_message: str | None = None,
) -> pd.DataFrame:
    state_df = coerce_topix100_streak_state_panel_df(state_source)
    missing_state_columns = [
        column for column in STREAK_STATE_PANEL_COLUMNS if column not in state_df.columns
    ]
    if missing_state_columns:
        raise ValueError(f"Missing state event columns: {missing_state_columns}")

    state_df = state_df[list(STREAK_STATE_PANEL_COLUMNS)].copy()
    state_df["date"] = state_df["date"].astype(str)
    state_df["code"] = state_df["code"].astype(str).str.zfill(4)
    merged_df = price_df.merge(
        state_df,
        on=["date", "code", "company_name"],
        how="inner",
        validate="one_to_one",
    )
    if merged_df.empty:
        raise ValueError(empty_join_error_message)

    if dropna_target_columns:
        merged_df = merged_df.dropna(subset=list(dropna_target_columns)).copy()
        if merged_df.empty:
            raise ValueError(empty_target_error_message or empty_join_error_message)

    merged_df["segment_abs_return"] = merged_df["segment_return"].astype(float).abs()
    return merged_df
