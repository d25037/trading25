"""Tests for intraday discrete-feature ablation walk-forward research."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.domains.analytics.topix100_streak_353_next_session_intraday_discrete_ablation_walkforward import (
    _build_variant_vs_full_df,
    run_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research,
)


def _build_fake_feature_panel() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "code": ["1001", "1002"],
            "company_name": ["Stock 1001", "Stock 1002"],
        }
    )


def _build_fake_variant_result(
    *,
    lightgbm_long: float,
    lightgbm_short_edge: float,
    lightgbm_spread: float,
    feature_name: str,
) -> SimpleNamespace:
    model_summary_df = pd.DataFrame(
        [
            {
                "model_name": "baseline",
                "top_k": 3,
                "date_count": 12,
                "avg_long_return": 0.002,
                "long_hit_rate_positive_return": 0.55,
                "avg_short_edge": 0.001,
                "short_hit_rate_positive_edge": 0.53,
                "avg_short_return": -0.001,
                "avg_long_short_spread": 0.003,
                "spread_hit_rate_positive": 0.58,
                "avg_gross_edge": 0.004,
                "avg_spread_vs_universe": 0.002,
                "avg_universe_return": 0.0005,
                "avg_long_stock_count": 3.0,
                "avg_short_stock_count": 3.0,
                "avg_long_score": 0.01,
                "avg_short_score": -0.01,
            },
            {
                "model_name": "lightgbm",
                "top_k": 3,
                "date_count": 12,
                "avg_long_return": lightgbm_long,
                "long_hit_rate_positive_return": 0.65,
                "avg_short_edge": lightgbm_short_edge,
                "short_hit_rate_positive_edge": 0.62,
                "avg_short_return": -lightgbm_short_edge,
                "avg_long_short_spread": lightgbm_spread,
                "spread_hit_rate_positive": 0.7,
                "avg_gross_edge": lightgbm_long + lightgbm_short_edge,
                "avg_spread_vs_universe": lightgbm_spread - 0.0005,
                "avg_universe_return": 0.0005,
                "avg_long_stock_count": 3.0,
                "avg_short_stock_count": 3.0,
                "avg_long_score": 0.02,
                "avg_short_score": -0.02,
            },
        ]
    )
    model_comparison_df = pd.DataFrame(
        [
            {
                "top_k": 3,
                "baseline_avg_long_return": 0.002,
                "lightgbm_avg_long_return": lightgbm_long,
                "long_return_lift_vs_baseline": lightgbm_long - 0.002,
                "baseline_avg_short_edge": 0.001,
                "lightgbm_avg_short_edge": lightgbm_short_edge,
                "short_edge_lift_vs_baseline": lightgbm_short_edge - 0.001,
                "baseline_avg_long_short_spread": 0.003,
                "lightgbm_avg_long_short_spread": lightgbm_spread,
                "spread_lift_vs_baseline": lightgbm_spread - 0.003,
                "baseline_spread_hit_rate_positive": 0.58,
                "lightgbm_spread_hit_rate_positive": 0.7,
                "spread_hit_rate_lift_vs_baseline": 0.12,
            }
        ]
    )
    split_comparison_df = pd.DataFrame(
        [
            {
                "split_index": 1,
                "top_k": 3,
                "baseline_avg_long_return": 0.002,
                "lightgbm_avg_long_return": lightgbm_long,
                "long_return_lift_vs_baseline": lightgbm_long - 0.002,
                "baseline_avg_short_edge": 0.001,
                "lightgbm_avg_short_edge": lightgbm_short_edge,
                "short_edge_lift_vs_baseline": lightgbm_short_edge - 0.001,
                "baseline_avg_long_short_spread": 0.003,
                "lightgbm_avg_long_short_spread": lightgbm_spread,
                "spread_lift_vs_baseline": lightgbm_spread - 0.003,
                "baseline_spread_hit_rate_positive": 0.58,
                "lightgbm_spread_hit_rate_positive": 0.7,
                "spread_hit_rate_lift_vs_baseline": 0.12,
            }
        ]
    )
    score_decile_df = pd.DataFrame(
        [
            {
                "model_name": "lightgbm",
                "score_decile_index": 1,
                "score_decile": "Top 10%",
                "mean_realized_return": lightgbm_long,
                "stock_count": 20,
                "date_count": 12,
            }
        ]
    )
    feature_importance_df = pd.DataFrame(
        [
            {
                "model_name": "lightgbm",
                "feature_name": feature_name,
                "mean_importance_gain": 100.0,
                "mean_importance_share": 0.4,
                "split_count": 1,
                "importance_rank": 1,
            }
        ]
    )
    return SimpleNamespace(
        split_count=1,
        walkforward_model_summary_df=model_summary_df,
        walkforward_model_comparison_df=model_comparison_df,
        walkforward_split_comparison_df=split_comparison_df,
        walkforward_score_decile_df=score_decile_df,
        walkforward_feature_importance_df=feature_importance_df,
    )


def test_build_variant_vs_full_df_computes_deltas() -> None:
    model_summary_df = pd.DataFrame(
        [
            {
                "variant_key": "full",
                "variant_label": "Full",
                "top_k": 3,
                "avg_long_return": 0.01,
                "avg_short_edge": 0.011,
                "avg_long_short_spread": 0.021,
                "spread_hit_rate_positive": 0.75,
            },
            {
                "variant_key": "decile_only",
                "variant_label": "Decile Only",
                "top_k": 3,
                "avg_long_return": 0.009,
                "avg_short_edge": 0.01,
                "avg_long_short_spread": 0.019,
                "spread_hit_rate_positive": 0.7,
            },
        ]
    )

    comparison_df = _build_variant_vs_full_df(model_summary_df)

    assert len(comparison_df) == 1
    row = comparison_df.iloc[0]
    assert row["variant_key"] == "decile_only"
    assert row["spread_delta_vs_full"] == 0.019 - 0.021
    assert row["spread_retention_vs_full"] == 0.019 / 0.021


def test_run_intraday_discrete_ablation_walkforward_aggregates_variants(monkeypatch) -> None:
    fake_feature_panel_df = _build_fake_feature_panel()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_discrete_ablation_walkforward.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: SimpleNamespace(
            source_mode="snapshot",
            source_detail="synthetic",
            event_panel_df=pd.DataFrame(),
        ),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_discrete_ablation_walkforward.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_discrete_ablation_walkforward._build_feature_panel_df",
        lambda **kwargs: fake_feature_panel_df,
    )

    variant_map = {
        ("decile", "volume_bucket", "short_mode", "long_mode"): _build_fake_variant_result(
            lightgbm_long=0.010,
            lightgbm_short_edge=0.011,
            lightgbm_spread=0.021,
            feature_name="range_pct",
        ),
        ("decile", "volume_bucket"): _build_fake_variant_result(
            lightgbm_long=0.0098,
            lightgbm_short_edge=0.0107,
            lightgbm_spread=0.0205,
            feature_name="price_vs_sma_50_gap",
        ),
        ("decile", "short_mode", "long_mode"): _build_fake_variant_result(
            lightgbm_long=0.0097,
            lightgbm_short_edge=0.0106,
            lightgbm_spread=0.0203,
            feature_name="volume_sma_5_20",
        ),
        ("decile",): _build_fake_variant_result(
            lightgbm_long=0.0094,
            lightgbm_short_edge=0.0102,
            lightgbm_spread=0.0196,
            feature_name="decile",
        ),
        (): _build_fake_variant_result(
            lightgbm_long=0.0088,
            lightgbm_short_edge=0.0091,
            lightgbm_spread=0.0179,
            feature_name="recent_return_5d",
        ),
    }

    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_next_session_intraday_discrete_ablation_walkforward._run_walkforward_from_panel",
        lambda **kwargs: variant_map[tuple(kwargs["categorical_feature_columns"])],
    )

    result = run_topix100_streak_353_next_session_intraday_discrete_ablation_walkforward_research(
        "/tmp/market.duckdb",
        top_k_values=(3,),
        train_window=6,
        test_window=3,
        step=3,
    )

    assert result.variant_keys == (
        "full",
        "no_modes",
        "no_volume_bucket",
        "decile_only",
        "continuous_only",
    )
    assert not result.variant_config_df.empty
    assert set(result.variant_model_summary_df["variant_key"]) == set(result.variant_keys)
    assert not result.variant_vs_full_df.empty
    best_simplified = result.variant_model_summary_df[
        result.variant_model_summary_df["variant_key"] == "no_modes"
    ].iloc[0]
    assert best_simplified["avg_long_short_spread"] == 0.0205
