"""Tests for the TOPIX100 prev-open-vs-open entry/exit profit runner."""

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
        / "run_topix100_prev_open_vs_open_entry_exit_profit.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_prev_open_vs_open_entry_exit_profit",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_prev_open_vs_open_entry_exit_profit module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_prev_open_vs_open_entry_exit_profit"] = module
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
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--round-trip-cost-bps",
            "12.5",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260418_150000_testabcd",
            "--notes",
            "prev open vs open entry exit bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.intervals == "5,15,30"
    assert args.bucket_count == 4
    assert args.period_months == 6
    assert args.round_trip_cost_bps == 12.5
    assert args.run_id == "20260418_150000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-prev-open-vs-open-entry-exit-profit",
        run_id="20260418_150000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_prev_open_vs_open_entry_exit_profit_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--round-trip-cost-bps",
            "8",
            "--run-id",
            "20260418_150000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260418_150000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
