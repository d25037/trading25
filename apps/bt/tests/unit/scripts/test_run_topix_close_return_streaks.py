"""Tests for the TOPIX close return streak runner script."""

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
        / "run_topix_close_return_streaks.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix_close_return_streaks",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX close return streak runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix_close_return_streaks"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_horizon_and_bucket_params() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--future-horizons",
            "1,3,5",
            "--validation-ratio",
            "0.25",
            "--max-streak-day-bucket",
            "6",
            "--max-segment-length-bucket",
            "7",
            "--run-id",
            "20260405_000100_testabcd",
        ]
    )

    assert args.future_horizons == (1, 3, 5)
    assert args.validation_ratio == 0.25
    assert args.max_streak_day_bucket == 6
    assert args.max_segment_length_bucket == 7
    assert args.run_id == "20260405_000100_testabcd"


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


def test_main_runs_streak_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix-close-return-streaks",
        run_id="20260405_000100_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix_close_return_streaks_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_close_return_streaks_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--future-horizons",
            "1,3",
            "--run-id",
            "20260405_000100_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260405_000100_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
