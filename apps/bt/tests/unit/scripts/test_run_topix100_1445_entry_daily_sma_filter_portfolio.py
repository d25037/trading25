"""Tests for the TOPIX100 14:45 daily-SMA filter portfolio runner."""

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
        / "run_topix100_1445_entry_daily_sma_filter_portfolio.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_1445_entry_daily_sma_filter_portfolio",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_1445_entry_daily_sma_filter_portfolio module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_1445_entry_daily_sma_filter_portfolio"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-18",
            "--end-date",
            "2026-04-17",
            "--interval-minutes",
            "15",
            "--signal-family",
            "previous_open_vs_open",
            "--exit-label",
            "next_open",
            "--market-regime-bucket-key",
            "weak",
            "--subgroup-key",
            "losers",
            "--sma-window",
            "50",
            "--sma-filter-state",
            "at_or_below",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--entry-time",
            "14:45",
            "--next-session-exit-time",
            "10:30",
            "--tail-fraction",
            "0.20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260419_130000_testabcd",
            "--notes",
            "14:45 filtered portfolio bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.market_regime_bucket_key == "weak"
    assert args.subgroup_key == "losers"
    assert args.sma_window == 50
    assert args.sma_filter_state == "at_or_below"
    assert args.tail_fraction == 0.20


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-1445-entry-daily-sma-filter-portfolio",
        run_id="20260419_130000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_1445_entry_daily_sma_filter_portfolio_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--interval-minutes",
            "15",
            "--signal-family",
            "previous_open_vs_open",
            "--exit-label",
            "next_open",
            "--market-regime-bucket-key",
            "weak",
            "--subgroup-key",
            "losers",
            "--sma-window",
            "50",
            "--sma-filter-state",
            "at_or_below",
            "--tail-fraction",
            "0.20",
            "--run-id",
            "20260419_130000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260419_130000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
