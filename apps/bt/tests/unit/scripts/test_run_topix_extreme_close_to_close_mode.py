"""Tests for the TOPIX extreme close-to-close mode runner script."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import argparse
import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_topix_extreme_close_to_close_mode.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix_extreme_close_to_close_mode",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX extreme close-to-close mode runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix_extreme_close_to_close_mode"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_window_and_horizon_specs() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--candidate-windows",
            "2:6:2,9",
            "--future-horizons",
            "1,3,5",
            "--validation-ratio",
            "0.25",
            "--min-mode-days",
            "12",
            "--run-id",
            "20260404_121500_testabcd",
        ]
    )

    assert args.candidate_windows == (2, 4, 6, 9)
    assert args.future_horizons == (1, 3, 5)
    assert args.validation_ratio == 0.25
    assert args.min_mode_days == 12
    assert args.run_id == "20260404_121500_testabcd"


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
def test_parse_positive_int_sequence_rejects_invalid_specs(raw: str, expected_message: str) -> None:
    module = _load_module()

    with pytest.raises(argparse.ArgumentTypeError, match=expected_message):
        module._parse_positive_int_sequence(raw)


def test_main_runs_topix_mode_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix-extreme-close-to-close-mode",
        run_id="20260404_121500_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix_extreme_close_to_close_mode_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_extreme_close_to_close_mode_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--candidate-windows",
            "2:10:2",
            "--future-horizons",
            "1,3",
            "--run-id",
            "20260404_121500_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260404_121500_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
