"""Tests for the TOPIX return-standard-deviation exposure timing runner."""

from __future__ import annotations


import pytest

from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix_return_standard_deviation_exposure_timing.py"
    )


def test_parse_args_accepts_topix_return_standard_deviation_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--return-standard-deviation-window-days",
            "5,20",
            "--return-standard-deviation-mean-window-days",
            "1,3",
            "--high-annualized-return-standard-deviation-thresholds",
            "0.20,0.30",
            "--low-annualized-return-standard-deviation-thresholds",
            "0.15,0.25",
            "--reduced-exposure-ratios",
            "0.25,0.50",
            "--validation-ratio",
            "0.4",
            "--run-id",
            "20260413_120000_testabcd",
        ]
    )

    assert args.return_standard_deviation_window_days == (5, 20)
    assert args.return_standard_deviation_mean_window_days == (1, 3)
    assert args.high_annualized_return_standard_deviation_thresholds == (0.20, 0.30)
    assert args.low_annualized_return_standard_deviation_thresholds == (0.15, 0.25)
    assert args.reduced_exposure_ratios == (0.25, 0.50)
    assert args.validation_ratio == 0.4
    assert args.run_id == "20260413_120000_testabcd"


def test_main_runs_topix_return_standard_deviation_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix-return-standard-deviation-exposure-timing",
        run_id="20260413_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix_return_standard_deviation_exposure_timing_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_return_standard_deviation_exposure_timing_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_120000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260413_120000_testabcd")


@pytest.mark.parametrize(
    ("func_name", "raw"),
    [
        ("_parse_positive_int_sequence", "0"),
        ("_parse_positive_int_sequence", "abc"),
        ("_parse_positive_int_sequence", " , "),
        ("_parse_non_negative_float_sequence", "-0.1"),
        ("_parse_non_negative_float_sequence", "abc"),
        ("_parse_non_negative_float_sequence", " , "),
    ],
)
def test_parse_helpers_reject_invalid_values(func_name: str, raw: str) -> None:
    module = _load_module()

    with pytest.raises(Exception):
        getattr(module, func_name)(raw)
