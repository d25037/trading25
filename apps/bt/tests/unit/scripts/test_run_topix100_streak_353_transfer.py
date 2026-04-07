"""Tests for the TOPIX100 streak 3/53 transfer runner script."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_topix100_streak_353_transfer.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_transfer",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX100 streak 3/53 transfer runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_streak_353_transfer"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_fixed_pair_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--future-horizons",
            "1,5,10",
            "--validation-ratio",
            "0.25",
            "--short-window-streaks",
            "3",
            "--long-window-streaks",
            "53",
            "--min-stock-events-per-state",
            "4",
            "--min-constituents-per-date-state",
            "9",
            "--run-id",
            "20260406_130000_testabcd",
        ]
    )

    assert args.future_horizons == (1, 5, 10)
    assert args.validation_ratio == 0.25
    assert args.short_window_streaks == 3
    assert args.long_window_streaks == 53
    assert args.min_stock_events_per_state == 4
    assert args.min_constituents_per_date_state == 9
    assert args.run_id == "20260406_130000_testabcd"


@pytest.mark.parametrize(
    ("raw", "expected_message"),
    [
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


def test_main_runs_transfer_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-transfer",
        run_id="20260406_130000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_transfer_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--future-horizons",
            "1,5,10,20",
            "--short-window-streaks",
            "3",
            "--long-window-streaks",
            "53",
            "--run-id",
            "20260406_130000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260406_130000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
