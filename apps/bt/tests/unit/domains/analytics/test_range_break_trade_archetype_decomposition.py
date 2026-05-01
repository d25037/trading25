"""Tests for range-break trade-archetype decomposition helpers."""

from __future__ import annotations

import math

import pandas as pd

from src.domains.analytics.range_break_trade_archetype_decomposition import (
    _build_optional_fundamental_features,
    _build_overlay_candidate_summary_df,
    _build_return_bucket_summary_df,
)


def test_build_overlay_candidate_summary_df_compares_bad_tail_candidate_to_baseline() -> None:
    trades = pd.DataFrame(
        {
            "dataset_name": ["ds"] * 5,
            "window_label": ["full"] * 5,
            "market_scope": ["prime"] * 5,
            "trade_return_pct": [18.0, -14.0, 8.0, -12.0, 5.0],
            "breakout_60d_runup_pct": [10.0, 80.0, 20.0, 70.0, 15.0],
            "breakout_120d_runup_pct": [30.0, 150.0, 40.0, 130.0, 35.0],
            "rsi10": [50.0, 86.0, 55.0, 82.0, 60.0],
            "volume_ratio_value": [1.8, 4.0, 2.0, 3.8, 2.2],
            "topix_return_60d_pct": [3.0, 2.0, 4.0, -1.0, 5.0],
            "topix_close_vs_sma200_pct": [5.0, 4.0, 6.0, -2.0, 7.0],
            "trading_value_ma_15_oku": [10.0, 20.0, 12.0, 18.0, 14.0],
            "rolling_beta_50": [0.8, 2.5, 1.0, 2.2, 1.1],
        }
    )

    summary_df = _build_overlay_candidate_summary_df(
        enriched_trade_df=trades,
        severe_loss_threshold_pct=-10.0,
    )

    baseline = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["candidate_name"] == "baseline_all")
    ].iloc[0]
    pruned = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["candidate_name"] == "overheat_overlap_ge2_excluded")
    ].iloc[0]

    assert baseline["trade_count"] == 5
    assert pruned["trade_count"] == 4
    assert math.isclose(pruned["delta_severe_loss_rate_pct"], -15.0)


def test_build_return_bucket_summary_df_profiles_high_and_low_return_tails() -> None:
    trades = pd.DataFrame(
        {
            "dataset_name": ["ds"] * 5,
            "window_label": ["full"] * 5,
            "market_scope": ["prime"] * 5,
            "trade_return_pct": [-20.0, -5.0, 1.0, 10.0, 30.0],
            "breakout_60d_runup_pct": [80.0, 50.0, 40.0, 30.0, 20.0],
            "rsi10": [85.0, 70.0, 60.0, 55.0, 50.0],
            "volume_ratio_value": [3.5, 2.6, 2.2, 2.0, 1.8],
            "rolling_beta_50": [2.4, 1.8, 1.5, 1.2, 0.9],
            "topix_return_60d_pct": [-2.0, 0.0, 1.0, 3.0, 5.0],
            "trading_value_ma_15_oku": [8.0, 10.0, 12.0, 14.0, 16.0],
            "pbr": [2.0, 1.5, 1.0, 0.8, 0.6],
            "forward_per": [30.0, 20.0, 15.0, 12.0, 8.0],
            "market_cap_bil_jpy": [500.0, 300.0, 200.0, 100.0, 50.0],
        }
    )

    summary_df = _build_return_bucket_summary_df(
        enriched_trade_df=trades,
        severe_loss_threshold_pct=-10.0,
    )

    low = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["return_bucket"] == "low_return_q20")
    ].iloc[0]
    high = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["return_bucket"] == "high_return_q80")
    ].iloc[0]

    assert low["trade_count"] == 1
    assert high["trade_count"] == 1
    assert low["median_breakout_60d_runup_pct"] > high["median_breakout_60d_runup_pct"]
    assert low["median_topix_return_60d_pct"] < high["median_topix_return_60d_pct"]


def test_build_optional_fundamental_features_allows_missing_statements() -> None:
    stock_index = pd.DatetimeIndex(pd.to_datetime(["2026-01-05", "2026-01-06"]))

    features = _build_optional_fundamental_features(
        statements_df=pd.DataFrame(index=stock_index),
        stock_index=stock_index,
        parameters={"entry_filter_params": {}},
    )

    assert features.index.equals(stock_index)
    assert set(features.columns) == {
        "forward_forecast_eps",
        "adjusted_bps",
        "raw_eps",
        "adjusted_eps",
        "shares_outstanding",
    }
    assert features.isna().all().all()
