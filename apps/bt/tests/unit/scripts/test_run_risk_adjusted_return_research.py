"""Tests for the synthetic risk-adjusted-return runner script."""

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
        / "run_risk_adjusted_return_research.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_risk_adjusted_return_research",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load risk-adjusted-return runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_risk_adjusted_return_research"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_risk_adjusted_return_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--lookback-period",
            "40",
            "--ratio-type",
            "sharpe",
            "--seed",
            "7",
            "--n-days",
            "300",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260401_123500_testabcd",
        ]
    )

    assert args.lookback_period == 40
    assert args.ratio_type == "sharpe"
    assert args.seed == 7
    assert args.n_days == 300
    assert args.run_id == "20260401_123500_testabcd"


def test_main_runs_risk_adjusted_return_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/risk-adjusted-return-playground",
        run_id="20260401_123500_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_risk_adjusted_return_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_risk_adjusted_return_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--run-id",
            "20260401_123500_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260401_123500_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
