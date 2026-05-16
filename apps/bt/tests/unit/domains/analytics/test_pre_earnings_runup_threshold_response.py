from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.pre_earnings_runup_threshold_response import (
    PreEarningsRunupThresholdResponseResult,
    _build_joint_runup_response_df,
    _build_percentile_response_df,
    _build_threshold_response_df,
    build_summary_markdown,
    run_pre_earnings_runup_threshold_response_research,
    write_pre_earnings_runup_threshold_response_bundle,
)


def test_runup_threshold_response_summarizes_window_specific_thresholds() -> None:
    frame = _sample_scoped_frame()

    result = _build_threshold_response_df(
        frame,
        pre_windows=(20, 60),
        horizons=(1,),
        thresholds_by_window={20: (5.0, 20.0), 60: (10.0, 30.0)},
        severe_loss_threshold_pct=-10.0,
        min_events=1,
    )

    row = result[
        (result["pre_window"] == 20)
        & (result["direction"] == "ge")
        & (result["threshold_pct"] == 20.0)
        & (result["market_scope"] == "prime")
        & (result["is_fy"] == False)  # noqa: E712
    ].iloc[0]
    assert row["event_count"] == 1
    assert row["eps120_target_rate_pct"] == pytest.approx(100.0)
    assert row["holdthrough_median_excess_return_pct"] == pytest.approx(-5.0)
    assert row["post_entry_median_excess_return_pct"] == pytest.approx(2.0)


def test_runup_threshold_response_emits_joint_and_percentile_tables() -> None:
    frame = _sample_scoped_frame()

    joint = _build_joint_runup_response_df(
        frame,
        horizons=(1,),
        thresholds_20d=(5.0, 20.0),
        thresholds_60d=(10.0, 30.0),
        severe_loss_threshold_pct=-10.0,
        min_events=1,
    )
    percentile = _build_percentile_response_df(
        frame,
        pre_windows=(20, 60),
        horizons=(1,),
        severe_loss_threshold_pct=-10.0,
        min_events=1,
    )

    assert {"threshold_20d_pct", "threshold_60d_pct", "post_entry_win_rate_pct"}.issubset(
        joint.columns
    )
    assert {"percentile_bucket", "pre_window", "holdthrough_win_rate_pct"}.issubset(
        percentile.columns
    )


def test_runup_threshold_response_bundle_and_summary(tmp_path: Path) -> None:
    frame = _sample_scoped_frame()
    threshold = _build_threshold_response_df(
        frame,
        pre_windows=(20, 60),
        horizons=(1,),
        thresholds_by_window={20: (5.0,), 60: (10.0,)},
        severe_loss_threshold_pct=-10.0,
        min_events=1,
    )
    result = PreEarningsRunupThresholdResponseResult(
        db_path="/tmp/market.duckdb",
        source_mode="live",
        source_detail="unit",
        market_source="unit",
        analysis_start_date="2024-01-10",
        analysis_end_date="2024-01-10",
        pre_windows=(20, 60),
        horizons=(1,),
        liquidity_window=60,
        severe_loss_threshold_pct=-10.0,
        thresholds_20d=(5.0,),
        thresholds_60d=(10.0,),
        min_events=1,
        event_feature_df=frame,
        threshold_response_df=threshold,
        joint_runup_response_df=pd.DataFrame({"condition_family": []}),
        percentile_response_df=pd.DataFrame({"condition_family": []}),
        coverage_diagnostics_df=pd.DataFrame({"market_scope": []}),
    )

    summary = build_summary_markdown(result)
    assert "Threshold Response" in summary

    bundle = write_pre_earnings_runup_threshold_response_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
    )
    assert bundle.manifest_path.exists()
    assert bundle.results_db_path.exists()


def test_runup_threshold_response_rejects_invalid_params(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_pre_earnings_runup_threshold_response_research(tmp_path / "missing.duckdb")


def _sample_scoped_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "code": "1111",
                "market_scope": "prime",
                "is_fy": False,
                "liquidity_scope": "all_liquidity",
                "pre_event_date": "2024-01-09",
                "pre_return_20d_pct": 22.0,
                "pre_return_60d_pct": 35.0,
                "eps120_target_eligible": True,
                "eps120_positive_target": True,
                "entry_executable": True,
                "execution_label": "executable_open",
                "holdthrough_excess_return_1d_pct": -5.0,
                "forward_excess_return_1d_pct": 2.0,
            },
            {
                "code": "2222",
                "market_scope": "prime",
                "is_fy": False,
                "liquidity_scope": "all_liquidity",
                "pre_event_date": "2024-01-09",
                "pre_return_20d_pct": 8.0,
                "pre_return_60d_pct": 12.0,
                "eps120_target_eligible": False,
                "eps120_positive_target": False,
                "entry_executable": False,
                "execution_label": "limit_up_no_fill",
                "holdthrough_excess_return_1d_pct": 3.0,
                "forward_excess_return_1d_pct": float("nan"),
            },
            {
                "code": "3333",
                "market_scope": "prime",
                "is_fy": True,
                "liquidity_scope": "all_liquidity",
                "pre_event_date": "2024-01-09",
                "pre_return_20d_pct": -12.0,
                "pre_return_60d_pct": -18.0,
                "eps120_target_eligible": True,
                "eps120_positive_target": False,
                "entry_executable": False,
                "execution_label": "limit_down_no_fill",
                "holdthrough_excess_return_1d_pct": -15.0,
                "forward_excess_return_1d_pct": float("nan"),
            },
        ]
    )
