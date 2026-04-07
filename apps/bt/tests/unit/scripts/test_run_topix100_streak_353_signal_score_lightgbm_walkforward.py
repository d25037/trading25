"""Tests for the walk-forward TOPIX100 streak 3/53 LightGBM runner."""

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
        / "run_topix100_streak_353_signal_score_lightgbm_walkforward.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_signal_score_lightgbm_walkforward",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load walk-forward score runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_streak_353_signal_score_lightgbm_walkforward"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_walkforward_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--train-window",
            "756",
            "--test-window",
            "126",
            "--step",
            "126",
            "--top-k-values",
            "5",
            "10",
            "20",
        ]
    )

    assert args.train_window == 756
    assert args.test_window == 126
    assert args.step == 126
    assert args.top_k_values == [5, 10, 20]


def test_main_runs_walkforward_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-signal-score-lightgbm-walkforward",
        run_id="20260406_230000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_signal_score_lightgbm_walkforward_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_signal_score_lightgbm_walkforward_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260406_230000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260406_230000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
