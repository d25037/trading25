"""Tests for the TOPIX100 next-session open-to-open 5D LightGBM walk-forward runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward.py"
    )


def test_parse_args_accepts_walkforward_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--short-window-streaks",
            "3",
            "--long-window-streaks",
            "53",
            "--top-k-values",
            "1",
            "3",
            "5",
            "--train-window",
            "756",
            "--test-window",
            "126",
            "--step",
            "126",
            "--purge-signal-dates",
            "1",
            "--run-id",
            "20260407_130000_testabcd",
        ]
    )

    assert args.short_window_streaks == 3
    assert args.long_window_streaks == 53
    assert args.top_k_values == [1, 3, 5]
    assert args.train_window == 756
    assert args.test_window == 126
    assert args.step == 126
    assert args.purge_signal_dates == 1
    assert args.run_id == "20260407_130000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-streak-3-53-next-session-open-to-open-5d-lightgbm-walkforward",
        run_id="20260407_130000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260407_130000_testabcd"])

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260407_130000_testabcd")
