"""Tests for the value-breakout overheat filter research runner."""

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
        / "run_value_breakout_overheat_filter.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_value_breakout_overheat_filter",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load value-breakout overheat runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_value_breakout_overheat_filter"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_overheat_filter_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/value-breakout/run",
            "--db-path",
            "/tmp/market.duckdb",
            "--market-scope",
            "standard",
            "--score-method",
            "prime_size_tilt",
            "--liquidity-scenario",
            "adv10m",
            "--breakout-policy",
            "breakout_additive",
            "--breakout-window",
            "120",
            "--breakout-lookback-sessions",
            "20",
            "--rebalance-months",
            "3",
            "--selection-count",
            "10",
            "--holdout-months",
            "0",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.input_bundle == "/tmp/value-breakout/run"
    assert args.db_path == "/tmp/market.duckdb"
    assert args.market_scope == "standard"
    assert args.breakout_window == 120
    assert args.holdout_months == 0
    assert args.run_id == "20260509_test"


def test_main_runs_overheat_filter_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/value-breakout-overheat-filter",
        run_id="20260509_test",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_value_breakout_overheat_filter",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_value_breakout_overheat_filter_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/value-breakout/run",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260509_test"
    assert payload["bundlePath"] == "/tmp/research/run"
