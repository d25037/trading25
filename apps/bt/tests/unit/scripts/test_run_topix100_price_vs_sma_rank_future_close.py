"""Tests for the runner-first TOPIX100 price-vs-SMA research script."""

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
        / "run_topix100_price_vs_sma_rank_future_close.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_price_vs_sma_rank_future_close",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load run_topix100_price_vs_sma_rank_future_close module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_price_vs_sma_rank_future_close"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_research_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--lookback-years",
            "7",
            "--min-constituents-per-day",
            "70",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_120000_testabcd",
            "--notes",
            "pilot bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-12-31"
    assert args.lookback_years == 7
    assert args.min_constituents_per_day == 70
    assert args.output_root == "/tmp/research"
    assert args.run_id == "20260331_120000_testabcd"
    assert args.notes == "pilot bundle"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-price-vs-sma-rank-future-close",
        run_id="20260331_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_price_vs_sma_rank_future_close_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_120000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260331_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
