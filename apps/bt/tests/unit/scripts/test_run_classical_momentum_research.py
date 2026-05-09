"""Tests for the classical momentum research runner script."""

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
        / "run_classical_momentum_research.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_classical_momentum_research",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load classical momentum runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_classical_momentum_research"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_classical_momentum_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--lookback-specs",
            "126:20,252:20",
            "--hold-sessions",
            "20,60",
            "--selection-fractions",
            "0.05,0.10",
            "--rebalance-interval-sessions",
            "20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_specs == ((126, 20), (252, 20))
    assert args.hold_sessions == (20, 60)
    assert args.selection_fractions == (0.05, 0.10)
    assert args.rebalance_interval_sessions == 20
    assert args.run_id == "20260509_test"


def test_main_runs_classical_momentum_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/classical-momentum-research",
        run_id="20260509_test",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_classical_momentum_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_classical_momentum_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260509_test"
    assert payload["bundlePath"] == "/tmp/research/run"
