"""Tests for the TOPIX downside family committee walk-forward runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix_downside_return_standard_deviation_family_committee_walkforward.py"
    )


def test_parse_args_accepts_family_committee_walkforward_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--family-downside-return-standard-deviation-window-days",
            "5",
            "--family-downside-return-standard-deviation-mean-window-days",
            "1,2",
            "--family-high-annualized-downside-return-standard-deviation-thresholds",
            "0.22,0.25",
            "--family-low-annualized-downside-return-standard-deviation-thresholds",
            "0.05,0.10,0.20",
            "--family-reduced-exposure-ratios",
            "0.0,0.1",
            "--committee-sizes",
            "1,3,5",
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

    assert args.family_downside_return_standard_deviation_window_days == (5,)
    assert args.family_downside_return_standard_deviation_mean_window_days == (1, 2)
    assert (
        args.family_high_annualized_downside_return_standard_deviation_thresholds
        == (
            0.22,
            0.25,
        )
    )
    assert args.family_low_annualized_downside_return_standard_deviation_thresholds == (
        0.05,
        0.1,
        0.2,
    )
    assert args.family_reduced_exposure_ratios == (0.0, 0.1)
    assert args.committee_sizes == (1, 3, 5)
    assert args.rank_top_ks == (10, 20)
    assert args.discovery_window_days == 756
    assert args.validation_window_days == 252
    assert args.step_window_days == 126


def test_main_runs_family_committee_walkforward_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix-downside-return-standard-deviation-family-committee-walkforward",
        run_id="20260413_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix_downside_return_standard_deviation_family_committee_walkforward_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_downside_return_standard_deviation_family_committee_walkforward_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_120000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260413_120000_testabcd")
