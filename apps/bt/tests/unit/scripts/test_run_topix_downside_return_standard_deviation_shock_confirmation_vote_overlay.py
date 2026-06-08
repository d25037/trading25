"""Tests for the TOPIX downside shock-confirmation vote overlay runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay.py"
    )


def test_parse_args_accepts_shock_confirmation_vote_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--downside-return-standard-deviation-window-days",
            "5",
            "--downside-return-standard-deviation-mean-window-days",
            "1,2",
            "--high-annualized-downside-return-standard-deviation-thresholds",
            "0.24,0.25",
            "--low-annualized-downside-return-standard-deviation-thresholds",
            "0.20,0.22",
            "--reduced-exposure-ratios",
            "0.0,0.1",
            "--trend-family-rules",
            "close_below_sma20,drawdown_63d_le_neg0p05",
            "--breadth-family-rules",
            "topix100_above_sma20_le_0p40,topix100_at_20d_low_ge_0p20",
            "--trend-vote-thresholds",
            "1,2",
            "--breadth-vote-thresholds",
            "1",
            "--confirmation-modes",
            "stress_and_trend_and_breadth,two_of_three_vote",
            "--min-constituents-per-day",
            "90",
            "--rank-top-ks",
            "10,20",
            "--discovery-window-days",
            "756",
            "--validation-window-days",
            "252",
            "--step-window-days",
            "126",
            "--run-id",
            "20260413_120000_testabcd",
        ]
    )

    assert args.downside_return_standard_deviation_window_days == (5,)
    assert args.downside_return_standard_deviation_mean_window_days == (1, 2)
    assert args.high_annualized_downside_return_standard_deviation_thresholds == (
        0.24,
        0.25,
    )
    assert args.low_annualized_downside_return_standard_deviation_thresholds == (
        0.2,
        0.22,
    )
    assert args.reduced_exposure_ratios == (0.0, 0.1)
    assert args.trend_family_rules == ("close_below_sma20", "drawdown_63d_le_neg0p05")
    assert args.breadth_family_rules == (
        "topix100_above_sma20_le_0p40",
        "topix100_at_20d_low_ge_0p20",
    )
    assert args.trend_vote_thresholds == (1, 2)
    assert args.breadth_vote_thresholds == (1,)
    assert args.confirmation_modes == (
        "stress_and_trend_and_breadth",
        "two_of_three_vote",
    )
    assert args.min_constituents_per_day == 90
    assert args.rank_top_ks == (10, 20)


def test_main_runs_shock_confirmation_vote_overlay_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix-downside-return-standard-deviation-shock-confirmation-vote-overlay",
        run_id="20260413_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_downside_return_standard_deviation_shock_confirmation_vote_overlay_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_120000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260413_120000_testabcd")
