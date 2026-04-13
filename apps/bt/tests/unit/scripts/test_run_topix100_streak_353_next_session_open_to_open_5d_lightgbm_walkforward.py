"""Tests for the TOPIX100 next-session open-to-open 5D LightGBM walk-forward runner."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load open-to-open 5D LightGBM walk-forward runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward"] = (
        module
    )
    spec.loader.exec_module(module)
    return module


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
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-next-session-open-to-open-5d-lightgbm-walkforward",
        run_id="20260407_130000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
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

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260407_130000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
