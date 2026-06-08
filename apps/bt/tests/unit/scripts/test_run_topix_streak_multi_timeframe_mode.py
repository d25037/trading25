"""Tests for the TOPIX streak multi-timeframe mode runner script."""

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
    return load_research_runner_module("run_topix_streak_multi_timeframe_mode.py")


def test_parse_args_accepts_pair_scan_specs() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--candidate-windows",
            "2:6:2,21",
            "--future-horizons",
            "1,5,10",
            "--stability-horizons",
            "5,10",
            "--validation-ratio",
            "0.25",
            "--min-mode-candles",
            "8",
            "--min-state-observations",
            "12",
            "--run-id",
            "20260406_110000_testabcd",
        ]
    )

    assert args.candidate_windows == (2, 4, 6, 21)
    assert args.future_horizons == (1, 5, 10)
    assert args.stability_horizons == (5, 10)
    assert args.validation_ratio == 0.25
    assert args.min_mode_candles == 8
    assert args.min_state_observations == 12
    assert args.run_id == "20260406_110000_testabcd"


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


def test_main_runs_streak_multi_timeframe_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix-streak-multi-timeframe-mode",
        run_id="20260406_110000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix_streak_multi_timeframe_mode_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_streak_multi_timeframe_mode_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--candidate-windows",
            "2:10:2,53",
            "--future-horizons",
            "1,5,10,20",
            "--stability-horizons",
            "5,10,20",
            "--run-id",
            "20260406_110000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260406_110000_testabcd")
