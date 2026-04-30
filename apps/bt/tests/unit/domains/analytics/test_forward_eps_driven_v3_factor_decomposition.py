"""Tests for forward EPS driven v3 factor decomposition helpers."""

from __future__ import annotations

import math
from types import SimpleNamespace

import pandas as pd
import pytest

import src.domains.analytics.forward_eps_driven_v3_factor_decomposition as module
from src.domains.analytics.forward_eps_driven_v3_factor_decomposition import (
    ForwardEpsDrivenV3FactorDecompositionResult,
    _build_published_summary,
    _build_summary_markdown,
    _build_action_candidate_summary_df,
    _build_factor_bucket_summary_df,
    _build_factor_contrast_summary_df,
    _build_tail_profile_df,
    _prepare_factor_frame,
    get_forward_eps_driven_v3_factor_decomposition_bundle_path_for_run_id,
    get_forward_eps_driven_v3_factor_decomposition_latest_bundle_path,
    load_forward_eps_driven_v3_factor_decomposition_bundle,
    run_forward_eps_driven_v3_factor_decomposition,
    write_forward_eps_driven_v3_factor_decomposition_bundle,
)


def _sample_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "window_label": ["full"] * 6,
            "market_scope": ["prime", "prime", "prime", "standard", "standard", "standard"],
            "symbol": ["1001", "1002", "1003", "2001", "2002", "2003"],
            "trade_return_pct": [12.0, -14.0, 6.0, 18.0, -12.0, 4.0],
            "forward_eps_growth_value": [0.4, 0.6, 0.8, 0.5, 0.7, 1.0],
            "forward_eps_growth_margin": [0.05, 0.2, 0.45, 0.1, 0.3, 0.6],
            "pbr": [0.5, 1.2, 0.8, 0.4, 1.5, 0.7],
            "forward_per": [8.0, 16.0, 10.0, 5.0, 22.0, 7.0],
            "market_cap_bil_jpy": [10.0, 50.0, 20.0, 5.0, 80.0, 12.0],
            "risk_adjusted_return_value": [1.1, 4.2, 3.9, 1.5, 4.5, 2.1],
            "risk_adjusted_return_margin": [-0.1, 3.0, 2.7, 0.3, 3.3, 0.9],
            "volume_ratio_value": [1.5, 2.3, 2.1, 1.7, 2.5, 1.9],
            "volume_ratio_margin": [-0.2, 0.6, 0.4, 0.0, 0.8, 0.2],
            "rsi10": [42.0, 70.0, 65.0, 45.0, 75.0, 55.0],
            "stock_return_20d_pct": [5.0, 45.0, 40.0, 6.0, 50.0, 12.0],
            "stock_return_60d_pct": [12.0, 70.0, 62.0, 15.0, 80.0, 25.0],
            "stock_volatility_20d_pct": [2.0, 8.0, 5.0, 2.5, 9.0, 3.0],
            "topix_return_20d_pct": [1.0, 2.0, -1.0, 1.0, 2.0, 3.0],
            "topix_return_60d_pct": [2.0, 3.0, -2.0, 2.0, 3.0, 4.0],
            "topix_risk_adjusted_return_60": [0.5, 0.7, -0.3, 0.5, 0.7, 0.8],
            "topix_close_vs_sma200_pct": [1.0, 2.0, -1.0, 1.0, 2.0, 3.0],
            "days_since_disclosed": [1.0, 10.0, 5.0, 2.0, 12.0, 4.0],
        }
    )


def test_factor_bucket_and_contrast_tables_compare_low_and_high_buckets() -> None:
    frame = _prepare_factor_frame(_sample_trades())

    buckets = _build_factor_bucket_summary_df(
        frame=frame,
        quantile_bucket_count=2,
        severe_loss_threshold_pct=-10.0,
    )
    contrasts = _build_factor_contrast_summary_df(buckets)

    pbr_prime = contrasts[
        (contrasts["market_scope"] == "prime") & (contrasts["feature_name"] == "pbr")
    ].iloc[0]

    assert pbr_prime["low_trade_count"] == 1
    assert pbr_prime["high_trade_count"] == 2
    assert math.isclose(pbr_prime["low_avg_trade_return_pct"], 12.0)
    assert math.isclose(pbr_prime["high_avg_trade_return_pct"], -4.0)
    assert math.isclose(pbr_prime["delta_high_minus_low_avg_trade_return_pct"], -16.0)


def test_tail_profile_keeps_severe_and_right_tail_feature_medians() -> None:
    frame = _prepare_factor_frame(_sample_trades())

    tail = _build_tail_profile_df(
        frame=frame,
        severe_loss_threshold_pct=-10.0,
    )
    severe_all = tail[
        (tail["market_scope"] == "all") & (tail["tail_cohort"] == "severe_loss")
    ].iloc[0]
    right_tail_all = tail[
        (tail["market_scope"] == "all") & (tail["tail_cohort"] == "right_tail_p90")
    ].iloc[0]

    assert severe_all["trade_count"] == 2
    assert math.isclose(severe_all["median_stock_return_60d_pct"], 75.0)
    assert right_tail_all["trade_count"] == 1
    assert math.isclose(right_tail_all["median_forward_eps_growth_margin"], 0.1)


def test_action_candidates_report_exclude_and_haircut_metrics() -> None:
    frame = _prepare_factor_frame(_sample_trades())

    actions = _build_action_candidate_summary_df(
        frame=frame,
        severe_loss_threshold_pct=-10.0,
        size_haircut=0.5,
    )
    overheat_exclude = actions[
        (actions["market_scope"] == "all")
        & (actions["candidate_name"] == "exclude_overheated_old_threshold_overlap_ge2")
    ].iloc[0]
    overheat_haircut = actions[
        (actions["market_scope"] == "all")
        & (actions["candidate_name"] == "haircut_overheated_v3_q80_overlap_ge2")
    ].iloc[0]

    assert overheat_exclude["selected_trade_count"] == 3
    assert math.isclose(overheat_exclude["selected_severe_loss_rate_pct"], 66.66666666666666)
    assert overheat_exclude["kept_trade_count"] == 3
    assert math.isclose(overheat_exclude["kept_severe_loss_rate_pct"], 0.0)
    assert overheat_haircut["haircut_worst_trade_return_pct"] > -14.0


def test_empty_factor_tables_keep_bundle_serialization_columns() -> None:
    frame = _prepare_factor_frame(
        pd.DataFrame(
            columns=[
                "window_label",
                "market_scope",
                "trade_return_pct",
            ]
        )
    )

    buckets = _build_factor_bucket_summary_df(
        frame=frame,
        quantile_bucket_count=2,
        severe_loss_threshold_pct=-10.0,
    )
    contrasts = _build_factor_contrast_summary_df(buckets)
    tail = _build_tail_profile_df(
        frame=frame,
        severe_loss_threshold_pct=-10.0,
    )
    actions = _build_action_candidate_summary_df(
        frame=frame,
        severe_loss_threshold_pct=-10.0,
        size_haircut=0.5,
    )

    assert buckets.empty
    assert "feature_name" in buckets.columns
    assert contrasts.empty
    assert "delta_high_minus_low_avg_trade_return_pct" in contrasts.columns
    assert tail.empty
    assert "tail_cohort" in tail.columns
    assert actions.empty
    assert "candidate_name" in actions.columns


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"holdout_months": 0}, "holdout_months"),
        ({"severe_loss_threshold_pct": 0.0}, "severe_loss_threshold_pct"),
        ({"quantile_bucket_count": 1}, "quantile_bucket_count"),
        ({"size_haircut": 1.5}, "size_haircut"),
    ],
)
def test_run_validates_input_boundaries(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        run_forward_eps_driven_v3_factor_decomposition(**kwargs)


def test_public_run_summary_and_bundle_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    sample = _sample_trades()
    scenario_summary = pd.DataFrame(
        {
            "window_label": ["full", "holdout_6m"],
            "trade_count": [6, 2],
            "avg_trade_return_pct": [2.3333333333, 3.0],
        }
    )
    market_scope_summary = pd.DataFrame(
        {
            "window_label": ["full", "holdout_6m", "full"],
            "market_scope": ["all", "all", "standard"],
            "severe_loss_rate_pct": [33.3333333333, 50.0, 33.3333333333],
        }
    )
    base_result = SimpleNamespace(
        strategy_name="production/forward_eps_driven",
        dataset_name="primeExTopix500",
        holdout_months=6,
        severe_loss_threshold_pct=-10.0,
        quantile_bucket_count=2,
        analysis_start_date="2020-01-01",
        analysis_end_date="2020-12-31",
        dataset_summary_df=pd.DataFrame({"dataset_name": ["primeExTopix500"]}),
        scenario_summary_df=scenario_summary,
        market_scope_summary_df=market_scope_summary,
        enriched_trade_df=sample,
    )
    monkeypatch.setattr(
        module,
        "run_forward_eps_trade_archetype_decomposition",
        lambda **_: base_result,
    )

    result = run_forward_eps_driven_v3_factor_decomposition(
        holdout_months=6,
        quantile_bucket_count=2,
        size_haircut=0.5,
    )
    summary = _build_summary_markdown(result)
    published = _build_published_summary(result)

    assert result.factor_bucket_summary_df.shape[0] > 0
    assert result.factor_contrast_summary_df.shape[0] > 0
    assert "Full-history trades: `6`" in summary
    assert published["tradeCount"] == 6

    bundle = write_forward_eps_driven_v3_factor_decomposition_bundle(
        result,
        output_root=tmp_path,
        run_id="unit_roundtrip",
        notes="unit test",
    )
    loaded = load_forward_eps_driven_v3_factor_decomposition_bundle(bundle.bundle_dir)

    assert isinstance(loaded, ForwardEpsDrivenV3FactorDecompositionResult)
    assert loaded.strategy_name == result.strategy_name
    assert loaded.action_candidate_summary_df.shape[0] == result.action_candidate_summary_df.shape[0]
    assert (
        get_forward_eps_driven_v3_factor_decomposition_bundle_path_for_run_id(
            "unit_roundtrip",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_forward_eps_driven_v3_factor_decomposition_latest_bundle_path(
        output_root=tmp_path
    ) == bundle.bundle_dir
