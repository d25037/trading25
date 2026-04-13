"""Tests for the TOPIX return-standard-deviation exposure timing runner."""

from __future__ import annotations

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
        / "run_topix_return_standard_deviation_exposure_timing.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix_return_standard_deviation_exposure_timing",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load TOPIX return-standard-deviation runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix_return_standard_deviation_exposure_timing"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_topix_return_standard_deviation_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--return-standard-deviation-window-days",
            "5,20",
            "--return-standard-deviation-mean-window-days",
            "1,3",
            "--high-annualized-return-standard-deviation-thresholds",
            "0.20,0.30",
            "--low-annualized-return-standard-deviation-thresholds",
            "0.15,0.25",
            "--reduced-exposure-ratios",
            "0.25,0.50",
            "--validation-ratio",
            "0.4",
            "--run-id",
            "20260413_120000_testabcd",
        ]
    )

    assert args.return_standard_deviation_window_days == (5, 20)
    assert args.return_standard_deviation_mean_window_days == (1, 3)
    assert args.high_annualized_return_standard_deviation_thresholds == (0.20, 0.30)
    assert args.low_annualized_return_standard_deviation_thresholds == (0.15, 0.25)
    assert args.reduced_exposure_ratios == (0.25, 0.50)
    assert args.validation_ratio == 0.4
    assert args.run_id == "20260413_120000_testabcd"


def test_main_runs_topix_return_standard_deviation_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix-return-standard-deviation-exposure-timing",
        run_id="20260413_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix_return_standard_deviation_exposure_timing_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix_return_standard_deviation_exposure_timing_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_120000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260413_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"


@pytest.mark.parametrize(
    ("func_name", "raw"),
    [
        ("_parse_positive_int_sequence", "0"),
        ("_parse_positive_int_sequence", "abc"),
        ("_parse_positive_int_sequence", " , "),
        ("_parse_non_negative_float_sequence", "-0.1"),
        ("_parse_non_negative_float_sequence", "abc"),
        ("_parse_non_negative_float_sequence", " , "),
    ],
)
def test_parse_helpers_reject_invalid_values(func_name: str, raw: str) -> None:
    module = _load_module()

    with pytest.raises(Exception):
        getattr(module, func_name)(raw)
