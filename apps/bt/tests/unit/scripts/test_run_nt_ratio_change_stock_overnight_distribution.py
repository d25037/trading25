"""Tests for the NT ratio change / stock overnight runner script."""

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
        / "run_nt_ratio_change_stock_overnight_distribution.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_nt_ratio_change_stock_overnight_distribution",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load NT ratio overnight runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_nt_ratio_change_stock_overnight_distribution"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_nt_ratio_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--sigma-threshold-1",
            "1.5",
            "--sigma-threshold-2",
            "2.5",
            "--selected-groups",
            "PRIME,TOPIX100",
            "--sample-size",
            "500",
            "--clip-lower",
            "5",
            "--clip-upper",
            "95",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_181000_testabcd",
            "--notes",
            "nt ratio bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.sigma_threshold_1 == 1.5
    assert args.sigma_threshold_2 == 2.5
    assert args.selected_groups == "PRIME,TOPIX100"
    assert args.run_id == "20260331_181000_testabcd"


def test_main_runs_nt_ratio_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/nt-ratio-change-stock-overnight-distribution",
        run_id="20260331_181000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_nt_ratio_change_stock_overnight_distribution",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_nt_ratio_change_stock_overnight_distribution_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_181000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260331_181000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
