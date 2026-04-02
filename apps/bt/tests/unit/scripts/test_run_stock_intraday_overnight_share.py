"""Tests for the stock intraday / overnight share runner script."""

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
        / "run_stock_intraday_overnight_share.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_stock_intraday_overnight_share",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load stock intraday runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_stock_intraday_overnight_share"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_stock_intraday_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--selected-groups",
            "TOPIX100,TOPIX500",
            "--min-session-count",
            "15",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_182000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.selected_groups == "TOPIX100,TOPIX500"
    assert args.min_session_count == 15
    assert args.run_id == "20260331_182000_testabcd"


def test_main_runs_stock_intraday_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/stock-intraday-overnight-share",
        run_id="20260331_182000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_stock_intraday_overnight_share_analysis",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_stock_intraday_overnight_share_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_182000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260331_182000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
