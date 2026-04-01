"""Tests for the 1357 x NT ratio / TOPIX hedge runner script."""

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
        / "run_1357_nt_ratio_topix_hedge.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_1357_nt_ratio_topix_hedge",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load hedge runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_1357_nt_ratio_topix_hedge"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_hedge_runner_options() -> None:
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
            "TOPIX100,TOPIX500",
            "--fixed-weights",
            "0.2,0.4",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260401_122000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.sigma_threshold_1 == 1.5
    assert args.sigma_threshold_2 == 2.5
    assert args.selected_groups == "TOPIX100,TOPIX500"
    assert args.fixed_weights == "0.2,0.4"
    assert args.run_id == "20260401_122000_testabcd"


def test_main_runs_hedge_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/hedge-1357-nt-ratio-topix",
        run_id="20260401_122000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_1357_nt_ratio_topix_hedge_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_1357_nt_ratio_topix_hedge_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260401_122000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260401_122000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
