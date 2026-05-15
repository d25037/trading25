"""Tests for the earnings hold-through expectancy runner."""

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
        / "run_earnings_holdthrough_expectancy.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_earnings_holdthrough_expectancy",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load earnings hold-through runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_earnings_holdthrough_expectancy"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_earnings_holdthrough_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--pre-windows",
            "20,60",
            "--horizons",
            "1,5,20",
            "--liquidity-window",
            "60",
            "--run-id",
            "20260515_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-12-31"
    assert args.pre_windows == "20,60"
    assert args.horizons == "1,5,20"
    assert args.liquidity_window == 60
    assert args.run_id == "20260515_test"


def test_main_runs_earnings_holdthrough_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/earnings-holdthrough-expectancy",
        run_id="20260515_test",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_earnings_holdthrough_expectancy_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_earnings_holdthrough_expectancy_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260515_test",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260515_test"
    assert payload["bundlePath"] == "/tmp/research/run"
