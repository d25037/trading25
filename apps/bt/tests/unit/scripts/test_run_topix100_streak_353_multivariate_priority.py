"""Tests for the TOPIX100 streak 3/53 multivariate priority runner."""

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
        / "run_topix100_streak_353_multivariate_priority.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_multivariate_priority",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX100 multivariate priority runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_streak_353_multivariate_priority"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_priority_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--price-feature",
            "price_vs_sma_50_gap",
            "--volume-feature",
            "volume_sma_5_20",
            "--future-horizons",
            "1",
            "5",
            "10",
            "--short-window-streaks",
            "3",
            "--long-window-streaks",
            "53",
            "--min-discovery-date-count",
            "80",
            "--min-validation-date-count",
            "60",
            "--run-id",
            "20260406_180000_testabcd",
        ]
    )

    assert args.price_feature == "price_vs_sma_50_gap"
    assert args.volume_feature == "volume_sma_5_20"
    assert args.future_horizons == [1, 5, 10]
    assert args.short_window_streaks == 3
    assert args.long_window_streaks == 53
    assert args.min_discovery_date_count == 80
    assert args.min_validation_date_count == 60
    assert args.run_id == "20260406_180000_testabcd"


def test_main_runs_priority_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-multivariate-priority",
        run_id="20260406_180000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_multivariate_priority_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_multivariate_priority_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--run-id",
            "20260406_180000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260406_180000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
