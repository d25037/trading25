"""Tests for the TOPIX100 short-side streak 3/53 scan runner."""

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
        / "run_topix100_short_side_streak_353_scan.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_short_side_streak_353_scan",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX100 short-side runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_short_side_streak_353_scan"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_short_side_options() -> None:
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
            "--strongest-lower-decile",
            "10",
            "--strongest-upper-decile",
            "10",
            "--strongest-state-key",
            "long_bearish__short_bearish",
            "--strongest-volume-bucket",
            "volume_low",
            "--min-validation-date-count",
            "120",
            "--min-pair-overlap-dates",
            "120",
            "--run-id",
            "20260406_170000_testabcd",
        ]
    )

    assert args.price_feature == "price_vs_sma_50_gap"
    assert args.volume_feature == "volume_sma_5_20"
    assert args.future_horizons == [1, 5, 10]
    assert args.short_window_streaks == 3
    assert args.long_window_streaks == 53
    assert args.strongest_lower_decile == 10
    assert args.strongest_upper_decile == 10
    assert args.strongest_state_key == "long_bearish__short_bearish"
    assert args.strongest_volume_bucket == "volume_low"
    assert args.min_validation_date_count == 120
    assert args.min_pair_overlap_dates == 120
    assert args.run_id == "20260406_170000_testabcd"


def test_main_runs_short_side_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-short-side-streak-3-53-scan",
        run_id="20260406_170000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_short_side_streak_353_scan_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_short_side_streak_353_scan_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--run-id",
            "20260406_170000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260406_170000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
