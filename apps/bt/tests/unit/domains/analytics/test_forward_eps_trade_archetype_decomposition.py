"""Tests for forward EPS trade-archetype decomposition helpers."""

from __future__ import annotations

import math

import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    _assign_quantile_bucket,
    _build_feature_bucket_summary_df,
    _build_overlay_candidate_summary_df,
)


def test_assign_quantile_bucket_spreads_ranked_values() -> None:
    series = pd.Series([10.0, 20.0, 30.0, 40.0])

    buckets = _assign_quantile_bucket(series, bucket_count=2)

    assert buckets.tolist() == [1, 1, 2, 2]


def test_build_feature_bucket_summary_df_summarizes_per_quantile() -> None:
    trades = pd.DataFrame(
        {
            "window_label": ["full"] * 4,
            "trade_return_pct": [2.0, -12.0, 8.0, 20.0],
            "forward_eps_growth_value": [0.10, 0.20, 0.30, 0.40],
            "forward_eps_growth_margin": [0.00, 0.10, 0.20, 0.30],
            "risk_adjusted_return_value": [1.2, 1.1, 1.4, 1.8],
            "risk_adjusted_return_margin": [0.0, -0.1, 0.2, 0.6],
            "volume_ratio_value": [1.7, 1.8, 2.0, 2.2],
            "volume_ratio_margin": [0.0, 0.1, 0.3, 0.5],
            "rsi10": [41.0, 43.0, 46.0, 49.0],
            "stock_return_20d_pct": [1.0, 2.0, 3.0, 4.0],
            "stock_return_60d_pct": [5.0, 6.0, 7.0, 8.0],
            "stock_volatility_20d_pct": [1.0, 2.0, 3.0, 4.0],
            "days_since_disclosed": [1.0, 2.0, 3.0, 4.0],
            "topix_return_20d_pct": [1.0, 1.0, 1.0, 1.0],
            "topix_return_60d_pct": [2.0, 2.0, 2.0, 2.0],
            "topix_risk_adjusted_return_60": [0.6, 0.6, 0.6, 0.6],
            "topix_close_vs_sma200_pct": [1.0, 1.0, 1.0, 1.0],
        }
    )

    summary_df = _build_feature_bucket_summary_df(
        enriched_trade_df=trades,
        quantile_bucket_count=2,
        severe_loss_threshold_pct=-10.0,
    )
    feps_buckets = summary_df[
        (summary_df["feature_name"] == "forward_eps_growth_value")
        & (summary_df["window_label"] == "full")
    ].sort_values("bucket_rank")

    assert feps_buckets["trade_count"].tolist() == [2, 2]
    assert feps_buckets["bucket_label"].tolist() == ["Q1/2", "Q2/2"]
    assert math.isclose(feps_buckets.iloc[0]["avg_trade_return_pct"], -5.0)
    assert math.isclose(feps_buckets.iloc[0]["severe_loss_rate_pct"], 50.0)
    assert math.isclose(feps_buckets.iloc[1]["avg_trade_return_pct"], 14.0)


def test_build_overlay_candidate_summary_df_computes_baseline_deltas() -> None:
    trades = pd.DataFrame(
        {
            "window_label": ["holdout_6m"] * 4,
            "trade_return_pct": [12.0, -15.0, 9.0, 6.0],
            "days_since_disclosed": [1.0, 9.0, 2.0, 7.0],
            "topix_return_20d_pct": [1.0, -1.0, 2.0, 2.0],
            "topix_return_60d_pct": [2.0, -2.0, 3.0, 1.0],
            "topix_close_vs_sma200_pct": [1.0, -1.0, 2.0, 1.0],
            "forward_eps_growth_margin": [0.2, 0.0, 0.3, 0.05],
            "risk_adjusted_return_margin": [0.4, -0.2, 0.5, 0.1],
            "volume_ratio_margin": [0.3, 0.0, 0.4, 0.1],
        }
    )

    summary_df = _build_overlay_candidate_summary_df(
        enriched_trade_df=trades,
        severe_loss_threshold_pct=-10.0,
    )

    baseline = summary_df[summary_df["candidate_name"] == "baseline_all"].iloc[0]
    fresh = summary_df[summary_df["candidate_name"] == "fresh_disclosure_3d"].iloc[0]
    combo = summary_df[summary_df["candidate_name"] == "topix_supportive_combo"].iloc[0]

    assert baseline["trade_count"] == 4
    assert math.isclose(baseline["avg_trade_return_pct"], 3.0)
    assert math.isclose(fresh["coverage_pct"], 50.0)
    assert math.isclose(fresh["avg_trade_return_pct"], 10.5)
    assert math.isclose(fresh["delta_avg_trade_return_pct"], 7.5)
    assert math.isclose(fresh["delta_severe_loss_rate_pct"], -25.0)
    assert combo["trade_count"] == 3
