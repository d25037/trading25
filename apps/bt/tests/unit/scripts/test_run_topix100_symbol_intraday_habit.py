"""Tests for the runner-first TOPIX100 symbol intraday habit script."""

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
        / "run_topix100_symbol_intraday_habit.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_symbol_intraday_habit",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_symbol_intraday_habit module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_symbol_intraday_habit"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_symbol_intraday_habit_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-15",
            "--end-date",
            "2026-04-14",
            "--interval-minutes",
            "30",
            "--sample-seed",
            "42",
            "--random-sample-size",
            "4",
            "--anchor-code",
            "6857",
            "--analysis-period-months",
            "6",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260415_150000_testabcd",
            "--notes",
            "symbol habit bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.interval_minutes == 30
    assert args.sample_seed == 42
    assert args.run_id == "20260415_150000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-symbol-intraday-habit",
        run_id="20260415_150000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_symbol_intraday_habit_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_symbol_intraday_habit_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--interval-minutes",
            "30",
            "--sample-seed",
            "42",
            "--run-id",
            "20260415_150000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260415_150000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
