"""Tests for the TOPIX extreme-mode mean-reversion comparison runner."""

from __future__ import annotations

import argparse

import pytest

from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix_extreme_mode_mean_reversion_comparison.py"
    )


def test_parse_args_accepts_window_horizon_and_hold_specs() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--normal-candidate-windows",
            "2:6:2,9",
            "--streak-candidate-windows",
            "3,5:9:2",
            "--future-horizons",
            "1,3,5",
            "--hold-days",
            "1,4,7",
            "--validation-ratio",
            "0.25",
            "--min-normal-mode-days",
            "12",
            "--min-streak-mode-candles",
            "9",
            "--run-id",
            "20260405_020100_testabcd",
        ]
    )

    assert args.normal_candidate_windows == (2, 4, 6, 9)
    assert args.streak_candidate_windows == (3, 5, 7, 9)
    assert args.future_horizons == (1, 3, 5)
    assert args.hold_days == (1, 4, 7)
    assert args.validation_ratio == 0.25
    assert args.min_normal_mode_days == 12
    assert args.min_streak_mode_candles == 9


@pytest.mark.parametrize(
    ("raw", "expected_message"),
    [
        ("2:3:4:5", "Invalid range token"),
        ("2:a", "Invalid integer in range token"),
        ("3:2", "Invalid positive range token"),
        ("abc", "Invalid integer token"),
        ("0", "Values must be positive integers"),
        (", ,", "Provide at least one positive integer"),
    ],
)
def test_parse_positive_int_sequence_rejects_invalid_specs(
    raw: str,
    expected_message: str,
) -> None:
    module = _load_module()

    with pytest.raises(argparse.ArgumentTypeError, match=expected_message):
        module._parse_positive_int_sequence(raw)


def test_main_runs_comparison_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix-extreme-mode-mean-reversion-comparison",
        run_id="20260405_020100_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix_extreme_mode_mean_reversion_comparison_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_extreme_mode_mean_reversion_comparison_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--normal-candidate-windows",
            "2:10:2",
            "--streak-candidate-windows",
            "3:9:3",
            "--future-horizons",
            "1,3",
            "--hold-days",
            "1,5",
            "--run-id",
            "20260405_020100_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260405_020100_testabcd")
