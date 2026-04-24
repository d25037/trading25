"""Tests for forward EPS trade-archetype decomposition helpers."""

from __future__ import annotations

import math

import pandas as pd

from src.domains.analytics.forward_eps_trade_archetype_decomposition import (
    _assign_quantile_bucket,
    _build_feature_bucket_summary_df,
    _build_market_scope_summary_df,
    _build_overlay_candidate_summary_df,
    _build_value_feature_bucket_summary_df,
    _build_value_overlay_summary_df,
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


def test_build_market_scope_summary_df_splits_actual_trades_by_market() -> None:
    trades = pd.DataFrame(
        {
            "window_label": ["full", "full", "full", "full"],
            "dataset_name": ["ds"] * 4,
            "market_scope": ["prime", "prime", "standard", "standard"],
            "trade_return_pct": [8.0, -2.0, 15.0, -5.0],
            "pbr": [0.8, 1.4, 0.5, 1.2],
            "forward_per": [8.0, 18.0, 6.0, 15.0],
            "market_cap_bil_jpy": [80.0, 300.0, 20.0, 120.0],
            "forward_eps_growth_value": [0.5, 0.4, 0.8, 0.36],
        }
    )

    summary_df = _build_market_scope_summary_df(
        enriched_trade_df=trades,
        severe_loss_threshold_pct=-10.0,
    )

    prime = summary_df[summary_df["market_scope"] == "prime"].iloc[0]
    standard = summary_df[summary_df["market_scope"] == "standard"].iloc[0]

    assert prime["trade_count"] == 2
    assert standard["trade_count"] == 2
    assert math.isclose(prime["avg_trade_return_pct"], 3.0)
    assert math.isclose(standard["avg_trade_return_pct"], 5.0)
    assert math.isclose(standard["median_pbr"], 0.85)


def test_build_value_feature_bucket_summary_df_uses_value_axes_not_adv() -> None:
    trades = pd.DataFrame(
        {
            "window_label": ["full"] * 6,
            "dataset_name": ["ds"] * 6,
            "market_scope": ["prime", "prime", "prime", "standard", "standard", "standard"],
            "trade_return_pct": [12.0, 8.0, -4.0, 18.0, 9.0, -6.0],
            "pbr": [0.5, 0.8, 1.8, 0.4, 0.7, 1.5],
            "forward_per": [5.0, 7.0, 25.0, 4.0, 6.0, 20.0],
            "market_cap_bil_jpy": [20.0, 50.0, 400.0, 10.0, 30.0, 200.0],
            "avg_trading_value_60d_mil_jpy": [5.0, 50.0, 20.0, 3.0, 40.0, 15.0],
        }
    )

    summary_df = _build_value_feature_bucket_summary_df(
        enriched_trade_df=trades,
        quantile_bucket_count=2,
        severe_loss_threshold_pct=-10.0,
    )

    assert set(summary_df["feature_name"].unique()) == {
        "pbr",
        "forward_per",
        "market_cap_bil_jpy",
        "value_composite_score",
    }
    prime_pbr_q1 = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["feature_name"] == "pbr")
        & (summary_df["bucket_rank"] == 1)
    ].iloc[0]
    standard_mcap_q1 = summary_df[
        (summary_df["market_scope"] == "standard")
        & (summary_df["feature_name"] == "market_cap_bil_jpy")
        & (summary_df["bucket_rank"] == 1)
    ].iloc[0]

    assert prime_pbr_q1["trade_count"] == 2
    assert math.isclose(prime_pbr_q1["avg_trade_return_pct"], 10.0)
    assert standard_mcap_q1["trade_count"] == 2
    assert math.isclose(standard_mcap_q1["avg_trade_return_pct"], 13.5)


def test_build_value_overlay_summary_df_compares_value_core_to_baseline() -> None:
    trades = pd.DataFrame(
        {
            "window_label": ["holdout_6m"] * 6,
            "dataset_name": ["ds"] * 6,
            "market_scope": ["prime", "prime", "prime", "standard", "standard", "standard"],
            "trade_return_pct": [15.0, 10.0, -9.0, 20.0, 12.0, -8.0],
            "pbr": [0.5, 0.8, 1.9, 0.4, 0.7, 1.7],
            "forward_per": [5.0, 8.0, 24.0, 4.0, 7.0, 22.0],
            "market_cap_bil_jpy": [20.0, 60.0, 500.0, 15.0, 35.0, 220.0],
        }
    )

    summary_df = _build_value_overlay_summary_df(
        enriched_trade_df=trades,
        severe_loss_threshold_pct=-10.0,
    )

    prime_baseline = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["candidate_name"] == "baseline_all")
    ].iloc[0]
    prime_core = summary_df[
        (summary_df["market_scope"] == "prime")
        & (summary_df["candidate_name"] == "value_core_low_pbr_low_fper_small_cap")
    ].iloc[0]
    standard_core = summary_df[
        (summary_df["market_scope"] == "standard")
        & (summary_df["candidate_name"] == "value_core_low_pbr_low_fper_small_cap")
    ].iloc[0]

    assert prime_baseline["trade_count"] == 3
    assert prime_core["trade_count"] == 1
    assert math.isclose(prime_core["avg_trade_return_pct"], 15.0)
    assert math.isclose(prime_core["delta_avg_trade_return_pct"], 9.666666666666666)
    assert standard_core["trade_count"] == 1
