from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.falling_knife_bad_tail_pruning import (
    FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID,
    get_falling_knife_bad_tail_pruning_bundle_path_for_run_id,
    get_falling_knife_bad_tail_pruning_latest_bundle_path,
    load_falling_knife_bad_tail_pruning_bundle,
    run_falling_knife_bad_tail_pruning,
    write_falling_knife_bad_tail_pruning_bundle,
)
from src.domains.analytics.falling_knife_reversal_study import (
    FallingKnifeReversalStudyResult,
    write_falling_knife_reversal_study_bundle,
)


def _build_input_result() -> FallingKnifeReversalStudyResult:
    event_df = pd.DataFrame(
        {
            "signal_date": [f"2026-01-{day:02d}" for day in range(1, 9)],
            "code": [f"10{day:02d}" for day in range(1, 9)],
            "market_name": [
                "プライム",
                "プライム",
                "スタンダード",
                "グロース",
                "グロース",
                "プライム",
                "スタンダード",
                "グロース",
            ],
            "risk_adjusted_bucket": [
                "Q1_lowest",
                "Q5_highest",
                "Q3",
                "Q5_highest",
                "Q2",
                "Q4",
                "Q5_highest",
                "unbucketed",
            ],
            "condition_count": [2, 2, 3, 4, 2, 2, 5, 3],
            "deep_60d_drawdown": [False, False, True, True, False, False, True, True],
            "deep_20d_drop": [False, False, False, True, False, False, True, False],
            "sharp_5d_drop": [False, False, True, True, False, False, True, True],
            "catch_return_20d": [0.04, -0.12, 0.03, -0.25, 0.08, 0.02, -0.18, -0.11],
            "catch_return_5d": [0.01, -0.03, 0.02, -0.08, 0.04, 0.01, -0.05, -0.02],
        }
    )
    empty_trade_summary = pd.DataFrame(columns=["strategy_family"])
    empty_paired_delta = pd.DataFrame(columns=["horizon_days"])
    empty_condition_profile = pd.DataFrame(columns=["condition_name"])
    return FallingKnifeReversalStudyResult(
        db_path="synthetic://falling-knife",
        source_mode="live",
        source_detail="synthetic",
        available_start_date="2026-01-01",
        available_end_date="2026-01-31",
        analysis_start_date="2026-01-01",
        analysis_end_date="2026-01-08",
        market_codes=("0111", "0112", "0113"),
        forward_horizons=(5, 20),
        risk_adjusted_lookback=60,
        condition_ratio_type="sortino",
        five_day_drop_threshold=-0.10,
        twenty_day_drop_threshold=-0.20,
        sixty_day_drawdown_threshold=-0.25,
        risk_adjusted_threshold=0.0,
        min_condition_count=2,
        max_wait_days=10,
        signal_cooldown_days=20,
        severe_loss_threshold=-0.10,
        source_row_count=8,
        event_count=8,
        wait_candidate_count=0,
        research_note="synthetic",
        event_df=event_df,
        trade_summary_df=empty_trade_summary,
        paired_delta_df=empty_paired_delta,
        condition_profile_df=empty_condition_profile,
    )


def test_falling_knife_bad_tail_pruning_scores_exclusion_rules(tmp_path: Path) -> None:
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(),
        output_root=tmp_path,
        run_id="20260427_120000_inputabcd",
    )

    result = run_falling_knife_bad_tail_pruning(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
    )

    assert result.baseline_count == 8
    assert not result.rule_summary_df.empty
    growth_rule = result.rule_summary_df[
        result.rule_summary_df["rule_name"] == "exclude_growth"
    ].iloc[0]
    assert int(growth_rule["removed_count"]) == 3
    assert float(growth_rule["kept_severe_loss_rate_pct"]) < float(
        growth_rule["baseline_severe_loss_rate_pct"]
    )
    assert set(result.segment_summary_df["segment_name"]) == {
        "market_name",
        "risk_adjusted_bucket",
        "condition_count",
    }


def test_falling_knife_bad_tail_pruning_bundle_roundtrip(tmp_path: Path) -> None:
    input_bundle = write_falling_knife_reversal_study_bundle(
        _build_input_result(),
        output_root=tmp_path,
        run_id="20260427_120000_inputabcd",
    )
    result = run_falling_knife_bad_tail_pruning(
        input_bundle.bundle_dir,
        output_root=tmp_path,
        horizon_days=20,
    )

    bundle = write_falling_knife_bad_tail_pruning_bundle(
        result,
        output_root=tmp_path,
        run_id="20260427_120500_testabcd",
    )
    reloaded = load_falling_knife_bad_tail_pruning_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == FALLING_KNIFE_BAD_TAIL_PRUNING_EXPERIMENT_ID
    assert (
        get_falling_knife_bad_tail_pruning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert get_falling_knife_bad_tail_pruning_latest_bundle_path(
        output_root=tmp_path,
    ) == bundle.bundle_dir
    assert reloaded.horizon_days == 20
    pdt.assert_frame_equal(
        reloaded.rule_summary_df,
        result.rule_summary_df,
        check_dtype=False,
    )
